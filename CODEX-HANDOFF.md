# Codex Handoff — Kubernetes-Native Architecture Refactor

> Historical implementation handoff. Paths and deployment status below describe the refactor stages. The current layout uses `placement/`, `autoscaler/`, and `utils/`; see README.md for current operation.

**Date:** 2026-11-15  
**Local repo:** `/home/yogarn/projects/lab/5g-mec-placement-controller`  
**`HEAD` / remote sync state:** `origin/main` is at `fc269aa` (`Update campaign schedule to 13 phases at 60s duration`). Both `core-5g` (`member@10.34.211.140`) and UE host (`member@10.34.211.6`) are at `fc269aa`; **all changes below are local uncommitted work and have not been pushed or applied to the cluster yet.**

---

## 1. Objective & Architecture

Refactored the controller and experiment harness to follow Kubernetes separation of responsibilities, eliminate per-node child `Deployment`s and per-node `nodeAffinity` pinning, and separate placement-only comparisons from full-system comparisons:

1. **Single Application Deployment:** One `apps/v1` `Deployment` (`ReplicaSet` owns replica lifecycle, scale-in termination, and post-eviction replacement Pod creation). Removed `OWNER_LABEL`, `GROUP_LABEL`, `NODE_ANNOTATION`, `MIGRATION_ANNOTATION`, and per-node child Deployments.
2. **Autoscaler (`Deployment.spec.replicas`):**
   - **Native HPA (default):** `experiments/hpa-proposed.yaml` (`autoscaling/v2` `HorizontalPodAutoscaler` targeting `External` metric `ue_p90_latency_seconds`, `target.value: 500m`, `scaleUp.stabilizationWindowSeconds: 0`, `scaleDown.stabilizationWindowSeconds: 300`) backed by `telemetry/05-prometheus-adapter.yaml`.
   - **Python controller fallback (`--placement-engine python --autoscaler controller`):** `controller/reconcile.py` `Reconciler.reconcile_autoscaler()` (also aligned `LatencyRatioPolicy` in `controller/policy.py` to HPA-style 300s rolling-maximum scale-down stabilization in `9a44922`, already on `origin/main`).
3. **Proposed Scheduler (`schedulerName: mec-placement-scheduler`):**
   - **Go in-cluster Deployment (default, `placement-go/`):** Uses `k8s.io/client-go` `SharedInformerFactory` (event-driven + fallback resync ticker) to watch unscheduled experiment Pods (`spec.nodeName == ""`, `spec.schedulerName == settings.SchedulerName`), filters `NodeReady`, `!Unschedulable`, `cpu_utilization < cpu_saturation`, allocatable resources (`scheduled + inFlightBindings`), `nodeSelector`, `affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution` (skipping empty `nodeSelectorTerms: [{}]`), and `taints`/`tolerations` (failing closed on `podAffinity`/`podAntiAffinity`/`DoNotSchedule` topologySpread), scores nodes using the preserved headroom + traffic locality formula, and binds via **Pod Binding API** (`CoreV1().Pods(ns).Bind`). Never modifies `Deployment.spec.replicas` or already-bound Pods.
   - **Python fallback (`controller/reconcile.py`):** Retained for explicit `--placement-engine python`.
4. **Descheduler:**
   - **Go & Python:** Runs every `deschedule_interval_seconds` only when all Deployment Pods are settled (`active == replicas`, `ready == replicas`, no unbound Pods, no `inFlightBindings`) and (in Go) skips missing Deployments or `default-scheduler` (`baseline`) Deployments/Pods. Picks the largest positive score-improvement `delta` via `SelectDescheduleCandidate` (verifying against the `SubtractNodeAllocation` post-removal snapshot so eviction never bounces back to the same source node) and evicts via **`policy/v1` Eviction API**. Respects `PodDisruptionBudget` (`429`) and handles `404`/`409`.
5. **Separated Experiment Configurations (`experiments/campaign.py`):**
   - `--comparison placement` (**Comparison A**): Same latency HPA for both methods (`default-scheduler` + latency HPA vs `mec-placement-scheduler` + in-cluster Go scheduler/descheduler + latency HPA).
   - `--comparison full` (**Comparison B**): `default-scheduler` + CPU HPA (`experiments/hpa-baseline.yaml`) vs full proposed system; attribution warnings saved in `plan.yaml` and `campaign.yaml`.
   - `--autoscaler {hpa,controller}` (default: `hpa`) and `--placement-engine {go,python}` (default: `go`, running in-cluster via `placement/placement-controller` with no host Python or Go controller process).

---

## 2. Files Changed / Added in This Refactor

### New Files
- `experiments/hpa-proposed.yaml` — `autoscaling/v2` HPA template using `External` metric `ue_p90_latency_seconds`.
- `telemetry/05-prometheus-adapter.yaml` — RBAC, ConfigMap (`externalRules`), Deployment (`registry.k8s.io/prometheus-adapter/prometheus-adapter:v0.12.0`), Service, and `v1beta1.external.metrics.k8s.io` `APIService` querying `placement-prometheus.placement.svc:9090`.
- `placement-go/` — Self-contained Go 1.24 module (`k8s.io/client-go v0.33.0`):
  - `placement-go/go.mod`, `placement-go/go.sum`
  - `placement-go/Dockerfile`, `placement-go/.dockerignore` — Multi-stage static Go build on `gcr.io/distroless/static-debian12:nonroot`.
  - `placement-go/cmd/placement-controller/main.go`
  - `placement-go/internal/config/config.go` & `config_test.go`
  - `placement-go/internal/telemetry/prometheus.go`
  - `placement-go/internal/policy/policy.go` & `policy_test.go`
  - `placement-go/internal/controller/controller.go` & `controller_test.go`
  - `placement-go/deploy.yaml` — Complete in-cluster manifest (`ServiceAccount`, `ClusterRole`, `ClusterRoleBinding`, `ConfigMap` `placement-controller-config` with `config.yaml` + `manifest.yaml`, and `apps/v1` `Deployment` `placement/placement-controller` with `strategy.type: Recreate` and `/etc/placement` ConfigMap mount).

### Modified Files (Refactor & Review Fixes)
- `.gitignore` — Added `placement-go/placement-controller` binary.
- `telemetry/05-prometheus-adapter.yaml` — Added `resources: {namespaced: false}` on `externalRules[0]` so prometheus-adapter `v0.12.0` does not inject a namespace label matcher onto the global collector metric.
- `controller/config.py` — Added `scheduler_name` (`mec-placement-scheduler`) and `scheduler_interval_seconds` (`1.0`).
- `controller/policy.py` — Added `score_candidate_node()` and `select_deschedule_candidate()`.
- `controller/reconcile.py` — Replaced per-node child Deployment state machine with `reconcile_autoscaler()`, `reconcile_scheduler()`, `reconcile_descheduler()`.
- `controller/__main__.py` — Added `--placement-only` (when HPA owns `spec.replicas`) and `--autoscaler-only`; runs `reconcile_scheduler()` at `scheduler_interval_seconds` in `mobility-aware` mode.
- `controller/tests/test_placement_reconcile.py` — Rewritten for initial scheduling, in-flight binding dedup/capacity, scale-out, scale-in, eviction + PDB `429` + binding `409`, post-eviction rescoring, and restart recovery.
- `controller/tests/test_gpu_reconcile.py` — Updated for refactored `Reconciler` and `select_deschedule_candidate`.
- `experiments/preflight.py`, `experiments/test_preflight.py` — Removed legacy `replicas == 0` / `mec-placement-controller.io/owner` child-Deployment branch.
- `experiments/campaign.py`, `experiments/test_campaign.py` — Switched defaults to `--autoscaler hpa --placement-engine go`, rejected mixed `--placement-engine go --autoscaler controller`, removed host Go `Popen`, added `verify_incluster_components()` (`placement/prometheus-adapter`, `APIService`, `placement/placement-controller`), synced `placement/placement-controller-config` ConfigMap + rollout restart/status + in-cluster log capture (`controller.log`) for `proposed`, and kept `reset()` cleanup for both `*-baseline` and `*-proposed` HPAs.

*(Note: `EXPERIMENTS.md`, `README.md`, `experiments/campaign-19-phase.yaml`, and `pedestrian-workload.patch` were already modified/untracked in the working tree prior to this refactor.)*

---

## 3. Local Verification Results (Passed After Final Edit)

- **Go toolchain:** Installed locally at `~/.local/go` (`go1.27.4 linux/amd64`).
- **Go unit tests & binary build:**
  ```bash
  cd placement-go
  ~/.local/go/bin/go test -count=1 -v ./...
  ~/.local/go/bin/go build -o placement-controller ./cmd/placement-controller
  ```
  - `internal/config`: `2/2` PASS (`TestRepoConfigAndManifestLoad`, `TestDeployConfigMapLoadsCleanly`)
  - `internal/controller`: `5/5` PASS (`TestSchedulerBindingInFlightDeduplicationAndCapacity`, `TestDeschedulerEvictionAndPostEvictionRescoringAndPDBRejection`, `TestRestartAndBindingConflictRecovery`, `TestSchedulerAndDeschedulerEnforceNodeConstraints`, `TestSchedulerAndDeschedulerIgnoreMissingAndOtherScheduler`)
  - `internal/policy`: `5/5` PASS (`TestRankAndScoreCandidateNodeParity`, `TestSelectDescheduleCandidatePicksHighestPositiveDelta`, `TestPodRequestsAndSelectorMatches`, `TestSelectDescheduleCandidateRejectsBounceBackToSource`, `TestPodFitsNodeConstraints` including empty `nodeSelectorTerms: [{}]`)
  - Binary built at `placement-go/placement-controller`.
- **Python unit tests:**
  ```bash
  python3 -m pytest -q --ignore=experiments/test_plot_autoscaler.py --ignore=apps/pedestrian-detection/tests/test_app.py
  python3 -m unittest discover -s controller/tests
  python3 -m unittest experiments/test_preflight.py experiments/test_pedestrian_workload.py experiments/test_traffic_control.py
  ```
  - `pytest`: `206 passed, 68 subtests passed in 3.76s` (`matplotlib` and `flask` are not installed in local system Python).
  - `unittest`: `46` controller tests + `6` experiment tests passed cleanly.
- **Offline plan preview (defaults: `hpa` + `go`):**
  ```bash
  python3 -m experiments.campaign --plan-only /tmp/plan-smoke-incluster
  ```
  - Exited `0` and generated offline plan artifacts cleanly (`--plan-only` directory cleaned up afterward).

---

## 4. Known Behavioral Differences & Cluster Findings

1. **Cluster APIService conflict (`core-5g`, K3s `v1.35.4+k3s1`):**
   - `kubectl get apiservices` currently shows `v1beta1.external.metrics.k8s.io` served by `keda/keda-metrics-apiserver` (`Available=True`), with 3 pods running in namespace `keda` (`scaledobjects` empty). Because KEDA is no longer used, KEDA's `APIService` / `keda` namespace **must be removed before** applying `telemetry/05-prometheus-adapter.yaml`.
2. **External-metric HPA during unready Pod scale-out:**
   - Python `LatencyRatioPolicy` waited for `ready == replicas`; Kubernetes `autoscaling/v2` HPA with `External` `Value` metrics does not wait for Pod `Ready` before recomputing ratios on subsequent 15s ticks. In `--comparison placement --autoscaler hpa`, both methods use the exact same HPA controller so this applies symmetrically.
3. **`prometheus-adapter` staleness:**
   - `telemetry/05-prometheus-adapter.yaml` guards against partial/empty windows using `and on() (ue_request_window_complete{job="ue-telemetry"} == 1)`, and sets `resources: {namespaced: false}` for global collector matching, but does not enforce explicit `max_metric_age_seconds` beyond Prometheus's built-in staleness delta (~5m if `ue-telemetry` stops scraping cleanly).
4. **Placement constraint parity & fail-closed behavior:**
   - Go scheduler and descheduler evaluate `nodeSelector`, `affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution` (`matchExpressions` including empty `nodeSelectorTerms: [{}]` returning no match, and `metadata.name` `matchFields`), and node `taints` / Pod `tolerations`, and fail closed on unsupported `podAffinity`, `podAntiAffinity`, `DoNotSchedule` `topologySpreadConstraints`, or unknown `matchFields`.
   - Go descheduler ignores missing Deployments (cleanly during campaign resets) and `default-scheduler` Deployments/Pods (`baseline`), and evaluates `ScoreCandidateNode(postRemoval, ...)` against `SubtractNodeAllocation(nodes, source, requests)` without `exclude` so an eviction is never triggered if the freed `source` node would immediately win replacement scheduling.
   - Python placement (`controller/policy.py` / `controller/reconcile.py`) does not implement `PodFitsNodeConstraints` or post-removal bounce prevention and is only used if `--placement-engine python` is explicitly requested.
5. **Container image build & digest pinning:**
   - `placement-go/deploy.yaml` uses `10.34.211.140:30500/placement-controller:latest` with `imagePullPolicy: Always` so rebuilt images never reuse cached node layers; after pushing to `10.34.211.140:30500`, you can also pin the immutable `@sha256:...` digest in `deploy.yaml`.
6. **Preflight APIService backing-service & metric probe:**
   - `verify_incluster_components` validates that `APIService/v1beta1.external.metrics.k8s.io` has `spec.service` equal to `placement/prometheus-adapter`, `Available=True`, `/apis/external.metrics.k8s.io/v1beta1` returning `APIResourceList`, and probes `/apis/external.metrics.k8s.io/v1beta1/namespaces/services/ue_p90_latency_seconds?labelSelector=job%3Due-telemetry` (tolerating `NotFound` only while idle; rejecting `5xx`/config errors). During active campaign warmup in `run_method`, `ue_p90_latency_seconds` must return non-empty items via `prometheus-adapter`.

---

## 5. Concrete Migration Plan Before Cluster Experiments

1. **Review, commit, and push local refactor** (leaving out unrelated files if needed), then sync `core-5g` (`10.34.211.140`) and UE host (`10.34.211.6`) via `git fetch origin && git reset --hard origin/main`.
2. **Build and push Go scheduler container image on `core-5g`:**
   ```bash
   docker build -t 10.34.211.140:30500/placement-controller:latest placement-go
   docker push 10.34.211.140:30500/placement-controller:latest
   ```
   (Optionally pin the resulting `@sha256:...` digest in `placement-go/deploy.yaml`.)
3. **Remove unused KEDA external metrics provider and deploy `prometheus-adapter` + `placement-controller`:**
   - Verify no `ScaledObject` is needed (`kubectl get scaledobjects -A`), then delete KEDA's `APIService` / namespace (`keda`) so it does not collide with `v1beta1.external.metrics.k8s.io`.
   - Apply `kubectl apply -f telemetry/05-prometheus-adapter.yaml` and `kubectl apply -f placement-go/deploy.yaml`.
   - Verify `kubectl get apiservices v1beta1.external.metrics.k8s.io` reports `AVAILABLE=True` backed by `placement/prometheus-adapter` and `kubectl -n placement rollout status deployment/placement-controller` succeeds (`readyReplicas: 1`).
4. **Verify external metric & preflight on `core-5g`:**
   - Run `python -m experiments.campaign --check` (verifies UE probes, collector, `placement/prometheus-adapter`, `APIService`, and `placement/placement-controller`).
   - During a test probe, confirm `/apis/external.metrics.k8s.io/v1beta1/namespaces/services/ue_p90_latency_seconds?labelSelector=job%3Due-telemetry` returns values when `ue_request_window_complete == 1` and returns empty/404 when idle.
5. **Run separated experiments (defaulting to in-cluster Go + native HPA, zero host controller processes):**
   - **Comparison A (Placement isolation, shared Latency HPA):**
     `python -m experiments.campaign <name-a> --comparison placement`
   - **Comparison B (Full system vs CPU HPA, do not attribute solely to placement):**
     `python -m experiments.campaign <name-b> --comparison full`
6. **Constraints to preserve:** Do not edit LaTeX files or historical `results/` data unless explicitly asked.

---

## 6. Fixed Arrival-Rate Load Generation (Ready for Codex Audit)

1. **Implementation choices:**
   - **Monotonic pacer (`utils/experiment.py` `run_fixed_rate_phase`):** Schedules request starts every `1 / rps_per_ue` seconds (`--rps-per-ue`, default `5.0`) using `time.monotonic()`, staggered deterministically per active UE by `(pos / num_ues) * period` across `selected` so all sites share the same rate without synchronized bursts.
   - **Bounded concurrency & no catch-up bursts:** Each UE uses a bounded `concurrent.futures.ThreadPoolExecutor(max_workers=max_in_flight_per_ue)` (`--max-in-flight-per-ue`, default `8`) and an `in_flight` counter. When `in_flight >= max_in_flight_per_ue` or `now >= slot_time + period` (or `now >= phase_end_mono`), the slot is recorded as `missed` and skipped without catch-up bursts.
   - **Phase-end drain & ordering:** New slot scheduling stops at `phase_end_mono` (or `cancelled.set()`), all in-flight futures drain before `TelemetryPublisher.close()`, `phase_end`, and UE reattachment, and `attempt` numbers are assigned sequentially under `ue_lock` on emission so `requests.jsonl` stays strictly ordered even when overlapping requests complete out of order.
   - **Propagation & metadata (`experiments/campaign.py`, `experiments/campaign_ue.py`, `utils/fixed_replica_harness.py`, `experiments/run.sh`):** `--rps-per-ue` and `--max-in-flight-per-ue` propagate through `plan.yaml`, `campaign.yaml`, `remote_settings`, `campaign_ue.py`, `run.json`, and `effective-config.json` for standard scaling, `mobility9`, `--fixed-replicas`, and `fixed_replica_harness.py`.
   - **Coverage & site reporting (`utils/summarize.py`, `utils/plot_autoscaler.py`):** `summarize.py` reports `scheduled`, `started`, `completed`, `failures`, `missed`, `actual_start_rate`, `request_coverage`, and `slo_attainment_scheduled` per phase (`summary.csv`), per site (`summary-sites.csv`), and per UE (`summary-ues.csv`), keeping `missed` separate from HTTP `failures` and maintaining backward compatibility with legacy closed-loop JSONL. `plot_autoscaler.py` adds `load_coverage` and a request-coverage subplot beneath SLO violations in `slo-violations.{png,pdf}`.
2. **Tests verified locally:**
   - `utils/test_experiment.py`: fake-clock tests for pacing independent of response duration, bounded concurrency + missed slots without catch-up bursts, scheduling lag skip without catch-up, deterministic staggered UE offsets, phase-end draining, immediate cancellation, and invalid rate/concurrency rejection.
   - `experiments/test_campaign.py`: propagation across standard and `mobility9` (`--fixed-replicas`) plans and `campaign_ue.py` forwarding.
   - `utils/test_summarize.py`: phase/site/UE arrival-rate counters, missed vs HTTP failure separation, and `summary-sites.{csv,json}` generation.
   - `utils/test_plot_autoscaler.py`: `load_coverage` test added and `py_compile` verified (`matplotlib` is not installed in the local system Python environment).
3. **Limitations:**
   - `5` RPS/UE is an initial uncalibrated rate (`120` RPS at 24 UEs) and requires calibration on the live cluster before drawing capacity conclusions.
   - `benchmark/client.py` standalone CLI was left unchanged because all campaign and harness paths dispatch through `utils/experiment.py`.
   - Local `placement/internal/controller/controller.go` and `controller_test.go` have an earlier uncommitted descheduler-ticker jitter fix outside these commits; do not sync/deploy until audited.

## 7. Scaler-Only Balanced Scenario (`--comparison scaling --scenario scaler-balanced`)

1. **Changes:**
   - **`experiments/campaign.py`, `experiments/run.sh`:** Added `--comparison scaling`, `--scenario scaler-balanced`, and `--final-hold`. Renders both `baseline` and `proposed` workloads with `default-scheduler`, `replicas: 1`, and required `nodeAffinity` restricted to `gnb-3` (`min=1`, default `max=24`, preserving `--max-replicas` and `--tc-ms`, rejecting `--fixed-replicas`, `--debug`, `--autoscaler controller`, `--placement-engine python`, `--deschedule-interval`, and custom scoring weights before cluster access).
   - **No `placement-controller` interaction:** In `scaling` mode, `verify_incluster_components` still requires `placement/prometheus-adapter` (`v1beta1.external.metrics.k8s.io`) but skips `placement/placement-controller`, and `run_method` never mutates `placement-controller-config`, restarts `placement-controller`, or launches host `python -m controller`.
   - **Pre-traffic capacity guard:** Added `check_node_capacity` to verify `gnb-3` allocatable CPU/memory minus non-terminated pods (excluding the reset workload) fits `max_replicas` before starting traffic.
   - **Accurate optional 360s final hold (`utils/experiment.py`, `experiments/campaign_ue.py`, `utils/plot_autoscaler.py`):** Added `--phase-durations` so `--final-hold` extends the final `3:1:1:1` phase to `420s` continuously without extra stop-and-reattach phases; `run.json`, `phase_start`/`phase_end`, `summarize.py`, and `plot_autoscaler.py` reflect actual per-phase durations.
   - **Metadata:** `plan.yaml` and `campaign.yaml` persist `schedule`, `phase_durations_s`, `final_hold_seconds`, `node_restriction`, `hpa_targets`, `replica_bounds`, `pacing`, and `tc_ms`.
2. **Validation:**
   - `utils/test_experiment.py` & `utils/test_summarize.py`: verified `--phase-durations` accuracy (`generation_duration_s`, `scheduled`, `actual_start_rate`) and early rejection of invalid durations.
   - `experiments/test_campaign.py`: verified `--plan-only` manifests, HPAs, bounds, `--final-hold`, `gnb-3` capacity check pass/fail, skipping `placement-controller` preflight/mutations, and early rejection of incompatible flags before cluster access.
   - `bash -n experiments/run.sh`, `py_compile` on `utils/plot_autoscaler.py` / `utils/test_plot_autoscaler.py`, and offline `--plan-only` smoke check passed.
3. **Limitations:**
   - Do not push, sync, or run cluster tests while experiments are running on `core`.
   - `24` replicas at `500m` CPU / `256Mi` memory each requires `12000m` CPU and `6144Mi` memory free on `gnb-3`; `check_node_capacity` will fail before traffic if `gnb-3` has less available capacity unless `--max-replicas` is lowered.

## 8. Scaler Capacity Guard & Resource Churn Reporting Fixes

1. **Changes:**
   - **Effective Pod requests (`experiments/campaign.py`):** Replaced container-only request summing with `_pod_effective_requests(spec)` applied identically to the Deployment template and existing non-terminal Pods on `gnb-3`. Accounts for ordinary containers, sequential + restartable (`restartPolicy: Always`) init containers, Pod-level `spec.resources`, Pod `spec.overhead`, `limits` fallback when `requests` is omitted, and keeps terminating Pods (`deletionTimestamp` set) reserved while ignoring `Succeeded`/`Failed` Pods. Unsupported configurations (`resourceClaims`, container `resources.claims`, unsupported init `restartPolicy`, unsupported `overhead`/`resources` keys) fail clearly with `ValueError`.
   - **Churn-aware resource sampling & summarization (`utils/monitor_resources.py`):** `sample()` now retains metrics for all workload-prefixed Pods (including recently terminated/terminating Pods). `summarize()` clusters deduplicated Pod metrics into simultaneous rounds (`<= 10s` timestamp skew, `<= 5s` CPU window spread, stable Pod membership across `[min_t, max_t]`) instead of zipping arbitrary timestamps across Pods. Exports `complete_samples`, `partial_samples`, `rejected_samples`, `missing_samples`, `total_observations`, `empty_observations`, `error_observations`, `round_completeness_ratio`, `availability_ratio`, `coverage_ratio`, `partial_cpu_millicores`, `partial_memory_mib`, `completeness_status`, and `missing_reasons`. Incomplete or missing phases remain blank (`''`), never zero-filled or interpolated.
   - **Coverage-aware plotting (`utils/plot_autoscaler.py`):** Added `load_resource_coverage`, missing/partial annotations on CPU/RAM panels, and a resource-coverage bar panel in `resource-usage.{png,pdf}`.
   - **Docs (`EXPERIMENTS.md`):** Documented exact resource summary semantics/limitations and a proposed low-load TC-on/TC-off path diagnostic (not executed; 100 ms objective unchanged).
2. **Validation:**
   - `python3 /tmp/audit_scaler_capacity.py`: failed as expected with `RuntimeError: node gnb-3 cannot fit 1 replicas: needs 500m CPU / 256Mi memory, available 0m CPU / 3968Mi memory`.
   - `python3 -m pytest -q --ignore=utils/test_plot_autoscaler.py --ignore=apps/pedestrian-detection/tests/test_app.py`: all tests passed (including new tests in `experiments/test_campaign.py` and `utils/test_monitor_resources.py`).
   - `python3 -m py_compile utils/plot_autoscaler.py utils/test_plot_autoscaler.py` and `bash -n experiments/run.sh`: passed.
3. **Unresolved limitations:**
   - `utils/test_plot_autoscaler.py` could not be executed locally because `matplotlib` is not installed in `/usr/bin/python3` (only `py_compile` ran locally; run `pytest utils/test_plot_autoscaler.py` in `.venv` after syncing).
   - Metrics Server scrapes asynchronously (~15–30s windows), so rapid Pod replacement churn can still leave phases with `0` complete simultaneous rounds; those gaps are reported as missing rather than fabricated.

## 9. Prometheus/cAdvisor Resource Monitoring

1. **Changes:**
   - **`telemetry/03-prometheus.yaml`:** Added `kubelet-cadvisor` 5s scrape job (`scrape_timeout: 4s`) using Kubernetes API server `nodes/proxy` (`/api/v1/nodes/<node>/proxy/metrics/cadvisor`), ServiceAccount CA + token, eligible-node filter (`core-5g|gnb1|gnb-2|gnb-3`), and explicit `metric_relabel_configs` keeping only `container_cpu_usage_seconds_total` and `container_memory_working_set_bytes` for `namespace="services"` while dropping `container=""` (pod-cgroup aggregates) and `container="POD"` (pause containers). Added `"/metrics/cadvisor"` to `ClusterRole` `nonResourceURLs`. Metrics Server and native CPU HPA are unchanged.
   - **`utils/cadvisor_resources.py`:** Exports raw Prometheus range queries (`resources-prometheus.json` with queries, timestamps, step, rate window, units, and `fallback_used: false`) and summarizes per-container-instance CPU rates (`30s` window, counter-reset handling, `>= 2` samples required) and working-set memory (`MiB`) independently against `metrics.jsonl` Pod lifecycle observations, without zero-filling, interpolating, or substituting Metrics Server.
   - **`experiments/campaign.py`, `utils/monitor_resources.py`, `utils/plot_autoscaler.py`:** New runs keep `monitor_resources sample` for Pod placement (`pods-by-ramp.csv`) and lifecycle (`metrics.jsonl`), pass `--skip-resource-csv` to `monitor_resources summarize`, and generate `resource-by-ramp.csv` via `utils.cadvisor_resources run`. `plot_autoscaler.py` displays the resource source, CPU rate window, sample counts, and missingness while preserving legacy Metrics Server CSV compatibility.
2. **Validation:**
   - `python3 -m pytest -q --ignore=utils/test_plot_autoscaler.py --ignore=apps/pedestrian-detection/tests/test_app.py`: `237 passed, 23 subtests passed` (including 7 new synthetic tests in `utils/test_cadvisor_resources.py`).
   - `python3 -m py_compile utils/plot_autoscaler.py utils/test_plot_autoscaler.py`, `bash -n experiments/run.sh`, and YAML validation of `telemetry/03-prometheus.yaml`: passed.
3. **Deployment assumptions & unresolved live checks:**
   - Assumes K3s API server `nodes/proxy` routes `/api/v1/nodes/<node>/proxy/metrics/cadvisor` with ServiceAccount auth and exposes `container_cpu_usage_seconds_total` and `container_memory_working_set_bytes` with `container` and `id`/`container_id` labels.
   - Requires live verification after applying `telemetry/03-prometheus.yaml`: endpoint access, metric/label names, `up{job="kubelet-cadvisor"} == 1`, absence of duplicate targets, 5s scrape overhead, and 24h TSDB retention.
   - `utils/test_plot_autoscaler.py` was only `py_compile`-checked locally (`matplotlib` is not installed in `/usr/bin/python3`).
