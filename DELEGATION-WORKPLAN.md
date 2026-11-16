# Delegation Workplan: Instrumentation, Baselines and Replication

Prepared: 16 November 2026. Audience: agy (implementation, diagnostic experiments, and evaluation) and Codex (checkpoint audit).

This is a plan, not a record of completed work. Read it together with `AGENTS.md`, `EXPERIMENTS.md`, `AGY-EXPERIMENT-RUNBOOK.md` and the runner code before acting. Where this file and the code disagree, trust the code and report the mismatch.

## 1. What the thesis is arguing

The contribution is an autoscaler that scales on measured response time (RT) against an SLO, combined with mobility-aware placement. The line of argument the experiments must support:

- A CPU-threshold or request-rate autoscaler requires per-workload threshold calibration against CPU or request-rate capacity to meet a latency SLO. The RT autoscaler uses the latency objective directly without requiring a CPU-utilization target (though exploratory parameters such as `--latency-floor-ms` and `--rt-target-factor` are still empirical tuning choices).
- The RT autoscaler is reactive. It cannot see saturation coming, so it needs a capacity floor (`minReplicas`) and pays a known transient cost on large load steps.
- Violations that come from the network path cannot be removed by replicas. That is what placement is for.

Do not write results text that goes beyond what a run shows. Mark observations, interpretations, hypotheses and open questions separately. Treat causal explanations as hypotheses until measured.

## 2. Evidence so far

All numbers below are from single runs on core (`member@10.34.211.140`, `~/skripsi_yoga/5g-mec-controller/results/`). None is replicated.

### 2.1 Runs

| Run | Config | Purpose |
|---|---|---|
| `scaler_floor_01` | staircase, SLO 150 ms, floor 130 ms, min 1, 180 s phases | first floor-adjusted trial |
| `scaler_floor_min2_01` | same, min 2, 60 s phases, no final hold | capacity-floor trial |

Common settings: workload `pedestrian-detection`, all pods on gnb-3, 5 requests/s per UE, max 8 in flight per UE, `--tc-ms 10`, max 24 replicas, CPU baseline at 60% of a 500m request. UE steps 3, 6, 9, 12, 15, 18, 24, 18, 15, 12, 9, 6, 3, split equally across three gNBs.

### 2.2 Findings

1. **Phase 2 spike in `scaler_floor_01` (p90 2685 ms, 458 missed slots).** One pod saturates at about 16.5 requests/s. Phase 1 (15 requests/s) ran it at about 91% with the HPA metric at 11 to 13 ms against a 20 ms target, so nothing triggered. When load doubled, the HPA took about 20 s to react and new pods took about 20 s more to become ready.
2. **Phase 7 failures in `scaler_floor_01` (81 failures) are not scaling.** 80 are 10 s timeouts from one UE (`172.16.50.248`, gnb-1) while 18 to 20 pods were ready. Cause not determined.
3. **`minReplicas: 2` removed the spike.** In `scaler_floor_min2_01` phase 2 p90 is 152 ms with no missed slots.
4. **Violations over all phases in `scaler_floor_min2_01`:** baseline 24.2%, proposed 8.5%.
5. **Violations track load per pod, for both methods.** Roughly 11 to 13 requests/s per pod gives 13 to 19% violations, about 7 gives 10 to 13%, 4 to 5 gives 4 to 6%. Halving violations costs about double the pods.
6. **Violations are concentrated on remote UEs.** Proposed, phases 3 to 7: gnb-3 (local) 1 to 2%, gnb-1 10 to 14%, gnb-2 19 to 25%. The fastest 10% of requests take 25 to 28 ms locally and 126 to 130 ms remotely. The roughly 100 ms remote penalty is unexplained (shaped delay is only 10 ms).
7. **The pod is busy but not computing most of the time.** Wall-clock occupancy is about 60 ms per request; cAdvisor CPU is about 13 to 19 ms per request. A saturated pod burns about 260 to 310m CPU, right at the CPU HPA's 300m threshold. Hypothesis, not measured: the single Gunicorn sync worker is blocked on upload and response I/O.
8. **The proposed scaler's pod count on the down-ramp is rate-limited, not signal-driven.** With 60 s phases, 60 s stabilisation and 1 pod per 30 s, it holds 19 to 8 pods while the baseline holds 7 to 3.
9. **`--rt-target-factor 0.90` is expected to run away.** With floor 130 ms it gives a 5 ms excess target. At low load the metric reads about 10 ms regardless of pod count (phase 13: 8 pods, 15 requests/s, p90 140.5 ms, 0.2% violations). Factor 0.95 gives 12.5 ms, which low-load phases clear by about 2 ms.

### 2.3 Related work to compare against

A senior thesis in the same lab (request-rate-aware autoscaler and scheduler extender) compared against a CPU HPA and a request-rate HPA through Prometheus Adapter. It set its threshold at 85% of a measured per-pod capacity and found no significant response-time change, attributed to sufficient capacity, low inter-site latency and random kube-proxy routing. Two things are taken from it here: a request-rate HPA baseline (task A6) and a processed-versus-incoming-rate-per-site metric (task A8).

## 3. Decisions already made

Do not reopen these without asking the user.

- The RT signal stays the primary scaling metric for the proposed method. Do not add a request-rate or CPU metric to the proposed HPA.
- `minReplicas: 2` is applied to both methods, justified on availability grounds, and must be recorded in the plan before a run.
- Scaling stays on the native Kubernetes HPA. No Python or custom replica writer.
- The Service keeps cluster-wide routing with no locality preference (`EXPERIMENTS.md`, routing section). Do not change it.
- Baseline and proposed must share every setting except the scaling metric: replica bounds, HPA behaviour, workload image, load, traffic shaping.
- Existing result folders are frozen. New work goes in new folders.
- Commit rules follow `AGENTS.md`: Conventional Commits, stage only relevant files.

## 4. Part A: Code changes (Codex)

### Priority

| Tasks | Priority | Reason |
|---|---|---|
| A1 to A5 | **Do first. Required before any run that will be kept.** | They record data that cannot be reconstructed afterwards. A1 and A2 change the workload image, so runs from before and after them must not be pooled. A5 is needed for the scale-down settings in B3. |
| A6, A7 | Later. Needed only for B5. | They add extra baselines and do not affect B1 to B4. |
| A8 | Later. Can be written after the runs exist. | It works on saved data. |
| A9 | Not now. | Gated on evidence from B1 and B3. |

Do A1 and A2 together so the image is rebuilt and pinned once.

Each task lists why it exists, where to look, and what "done" means. Keep every new behaviour off by default unless stated, so historical commands reproduce historical results. Add or update tests alongside each change. No live traffic during development; use `--plan-only` and unit tests.

### A1. [PRIORITY] Record request ID, serving pod, node and server time per request

- **Why:** Without this, RT cannot be split into network, queueing and inference, and the hypothesis that requests collide on busy pods (finding 5) cannot be tested.
- **Where:** `apps/pedestrian-detection/app.py` (`detect()` already returns `processing_ms`), `apps/pedestrian-detection/manifest.yaml` (expose pod name and node name through the Downward API), `benchmark/client.py` (currently calls `response.read()` and discards the body).
- **Do:** Accept or generate a `request_id` (passed by `benchmark/client.py` via header/payload and echoed in JSON), and return `server_pod` and `server_node` in the JSON response. Record `request_id`, `server_pod`, `server_node` and `server_processing_ms` in each `requests.jsonl` record. Failed or non-JSON responses record nulls for server fields, never a guess. Preserve client-side latency measurement boundaries (timestamps taken before request send and immediately after `response.read()`, before JSON parsing).
- **Done when:** `requests.jsonl` has those fields for successful requests; client-side latency measurement is unchanged; tests cover success, HTTP error and malformed body.
- **Note:** This changes the image. Follow the existing rebuild and digest-pinning flow (`utils/rebuild_pedestrian.py`). Apply the same change to `apps/face-detection` only if the user asks.

### A2. [PRIORITY] Separate server timings into upload/parse, decode, lock wait and prediction

- **Why:** `processing_ms` starts at handler entry and mixes upload parsing/body reading, image decoding, lock wait and inference. Finding 7 needs these separated before any concurrency change is considered.
- **Where:** `apps/pedestrian-detection/app.py`, `detect()`, and `benchmark/client.py`.
- **Do:** Add timestamps at handler entry, after upload parsing/body reading (`body_read_ms`), after image decode (`decode_ms`), at inference-lock acquisition (`lock_wait_ms`), and after prediction (`inference_ms`) beside `processing_ms`. Record `server_body_read_ms`, `server_decode_ms`, `server_lock_wait_ms`, and `server_inference_ms` in `requests.jsonl` (nulls on failed/non-JSON responses).
- **Done when:** All five server timing values are present per request and `body_read_ms + decode_ms + lock_wait_ms + inference_ms` is within a small tolerance of `processing_ms` in a unit test.
- **Limit to state in docs:** Time spent before the Flask handler is entered (TCP accept, Gunicorn socket read / pre-handler queueing) is not visible from inside the app.

### A3. [PRIORITY] Save CPU throttling and node CPU with each campaign

- **Why:** Pods have a 1-CPU limit, so an examiner can ask whether tail latency is CFS throttling. All pods share gnb-3, so node contention must be ruled out. Node CPU is in Prometheus but not copied into the campaign folder, so it depends on retention.
- **Where:** `telemetry/03-prometheus.yaml` (Prometheus `kubelet-cadvisor` `metric_relabel_configs` allowlist) and `utils/cadvisor_resources.py`.
- **Do:**
  - Extend the `kubelet-cadvisor` `metric_relabel_configs` allowlist in `telemetry/03-prometheus.yaml` to retain `container_cpu_cfs_throttled_periods_total`, `container_cpu_cfs_periods_total`, and `container_cpu_cfs_throttled_seconds_total`.
  - Query those counters for workload containers and per-node CPU utilisation from node-exporter for the eligible nodes, saved as raw series for the method window.
  - Compute per-phase aggregate throttled-period ratio as `sum(throttled_period_deltas) / sum(period_deltas)` across container instances (handling counter resets cleanly), rather than averaging per-container ratios.
- **Done when:** Prometheus retains the throttling counters, a campaign folder contains the raw series and the per-phase aggregate ratio, and missing metrics or zero-period denominators are reported as missing (`None`/blank), never as zero.

### A4. [PRIORITY] Record provenance in `plan.yaml` and `campaign.yaml`

- **Why:** The campaign metadata has no code revision, Kubernetes version or kube-proxy mode. The runner already reads the revision (`experiments/campaign.py`, near line 226) but does not save it.
- **Do:** Save git revision and dirty-tree flag for core and UE host, Kubernetes server version, kube-proxy mode, workload image digest, and the method execution order.
- **Done when:** Those keys appear in `plan.yaml` for a `--plan-only` render (with cluster-only fields marked null/offline) and in `campaign.yaml` for a live run.

### A5. [PRIORITY] Make HPA scale-down behaviour and method order configurable

- **Why:** Finding 8. Scale-down behaviour is hard-coded for the staircase profile (`experiments/campaign.py`, `_render_hpa`). Method order is fixed, which confounds the comparison with warm-up and drift.
- **Do:**
  - Add flags for scale-down stabilisation seconds (`--scale-down-stabilization-seconds`), scale-down pods per period (`--scale-down-pods`), and period seconds (`--scale-down-period-seconds`). Apply to both HPAs identically. Defaults reproduce today's values.
  - Add `--method-order` (`baseline-first`, `proposed-first`, `random`) so matched pairs can use balanced randomized method order, and record the realized order in `plan.yaml` and `campaign.yaml`.
  - Record the effective values in `plan.yaml` and `campaign.yaml`.
- **Done when:** `--plan-only` renders both HPAs with the requested behaviour and they are identical between methods; tests cover defaults, overrides, and method-order selection.

### A6. Request-rate HPA baseline

- **Why:** This is the strongest competitor to the RT scaler: proactive, but it requires calibrating a per-pod request-rate threshold. Comparing against it supports the argument that RT scales directly in latency-objective units without requiring a capacity-threshold search.
- **Do:** Add a baseline variant that scales on total request rate per pod through Prometheus Adapter, with the per-pod target passed as a flag (no default; it must come from the capacity measurement in B2). Reuse the adapter-rule mechanism already used for the latency metric, preserving unrelated rules and restoring on failure. Same replica bounds and HPA behaviour as the other methods.
- **Done when:** `--plan-only` renders the HPA and adapter rule; the campaign refuses to start this variant without an explicit target; `campaign.yaml` records the target and where it came from.

### A7. Configurable CPU target for the CPU baseline

- **Why:** Finding 5 implies a CPU HPA with a lower target would reach similar violations at similar pod counts. Showing that curve makes the comparison fair and shows the threshold has to be searched for.
- **Do:** Add a flag for the CPU utilisation target, default 60. Record it in `campaign.yaml`.
- **Done when:** `--plan-only` renders the requested target; default output is unchanged.

### A8. Analysis outputs

- **Why:** The pooled violation number hides the per-site structure and the cost.
- **Where:** `utils/summarize.py`, `utils/phase_windows.py`, `utils/plot_autoscaler.py`.
- **Do:** For each method and phase, write:
  - violations, p50, p90 per UE site;
  - **ready-pod-seconds** (integral of ready pods over time) and **active-pod-seconds** (integral of running/terminating or total active pods over time) reported separately, plus violations per ready-pod-second;
  - **offered**, **dispatched**, and **completed** request rates distinguished explicitly, plus requests/s per ready pod against violation rate;
  - share of requests served on the node of the UE's own site, and processed-versus-incoming rate per site (needs A1);
  - mean and p90 of `lock_wait_ms` (needs A2);
  - time from phase start to first scale-up, and to the new pods becoming ready.
- **Done when:** Running the analysis on an existing result folder produces the outputs it can and marks the rest unavailable, without failing.

### A9. Not now: concurrent request handling in the pod

Do not change Gunicorn worker or thread settings yet. This is gated on B1 and B3 showing where the 60 ms goes. If the user approves it later, it is a new workload configuration: new image digest, new capacity measurement, both methods rerun, results never pooled with the single-thread runs.

## 5. Part B: Experiments (agy)

Run in order. Stop and report if a step fails its check. Each run gets a fresh folder, and the plan (settings, reasoning, expected outcome) is written into that folder before traffic starts. One campaign at a time; use the existing campaign lock. Do not tune a setting after seeing which method wins.

### B0. Preconditions

- Priority tasks A1 to A5 merged, `telemetry/03-prometheus.yaml` applied on the cluster if throttling counters are not yet scraped, workload image rebuilt and pinned, core and UE host on the same revision. Runs made before this point are exploratory only and must not be pooled with later runs.
- `bash experiments/run.sh check --workload pedestrian-detection` passes.
- Confirm the new request fields, throttling series and provenance keys appear in a short smoke run before spending time on full campaigns.

### B1. Path baseline per site

- **Question:** Where does the roughly 100 ms remote penalty come from (finding 6)?
- **Do:** Adapt `utils/fixed_replica_harness.py` explicitly for `pedestrian-detection` so B1/B2 collect the same request timing, provenance, and cAdvisor/throttling resource evidence as campaign runs. With 2 fixed replicas on `gnb-3` and one UE per site at low rate, run **UE-bound probes of the actual service path** (from each UE namespace/tun interface to the NodePort service URL) alongside `requests.jsonl` and server timings from A1/A2. Repeat with traffic shaping off (`--tc-ms 0` / omitted) vs `--tc-ms 10`.
- **Estimated runtime:** ~5–8 minutes per tc condition (2–3 minutes traffic + setup/teardown).
- **Report:** A per-site table of offered/dispatched/completed rates, client RT, server timings (`body_read_ms`, `decode_ms`, `lock_wait_ms`, `inference_ms`, `processing_ms`), UE-to-NodePort probe RTT, and the remainder. Call the remainder "network and transport residual", not propagation delay.

### B2. Single-pod capacity

- **Question:** What is one pod's sustainable request rate? The 16.5 figure comes from one saturated 30 s window.
- **Do:** Using the adapted `utils/fixed_replica_harness.py` (`pedestrian-detection`, 1 fixed replica on `gnb-3`, full timing/provenance/resource collection), run stepped load for three repetitions. Record the rate at which p90 leaves the flat region and the rate at which slots start being missed.
- **Estimated runtime:** ~15–20 minutes across 3 repetitions (depending on step count).
- **Report:** Capacity with its spread. This value sets the A6 request-rate target (85% of capacity, matching the related work) and is itself evidence of the calibration cost.

### B3. Proposed-scaler pilots (factor and scale-down separated)

- **Question:** (B3a) Does `--rt-target-factor 0.95` alone cut mid-ramp violations without creeping upward at low load? (B3b) Does faster scale-down alone remove the down-ramp pod surplus?
- **Settings (common phase duration 60 s, `--final-hold` disabled, `--min-replicas 2`):**
  - **B3a (factor only):** `--min-replicas 2 --rt-target-factor 0.95` with existing scale-down (`60s` stabilization, `1` pod per `30s`).
  - **B3b (scale-down only):** `--min-replicas 2 --rt-target-factor 1.0` with faster scale-down (`--scale-down-stabilization-seconds 30 --scale-down-pods 2 --scale-down-period-seconds 30`).
  - Both methods in each run share the exact same HPA scale-down behaviour and replica bounds.
- **Estimated runtime:** ~30–35 minutes per matched pair (13 phases × 60 s = 13 min traffic per method, 26 min total, plus setup/drain/reattachment; `--final-hold` disabled). If 180 s phases are chosen instead, runtime is ~85–90 minutes per pair.
- **Watch:** Phases 1 and 13 for replica creep above 2 with near-zero violations. If that happens, report it and stop; do not try other factors without asking.
- **Do not run factor 0.90** with floor 130 ms (finding 9).

### B4. Freeze, then replicate

- Freeze SLO, floor, factor, replica bounds, HPA behaviour, phase duration (`60s` or `180s`, `--final-hold` explicitly stated), and image digest in writing.
- Run at least three matched pairs (target 4 pairs for balanced `baseline-first` / `proposed-first` ordering), using **balanced randomized method order** recorded in `campaign.yaml` before traffic starts.
- **Predefined invalidation rules:** A run may be invalidated and rerun as an extra pair **only** with independent evidence of an infrastructure/harness fault (e.g., UE tunnel drop, Prometheus/adapter outage, node failure, or preflight/teardown abort). **Do not exclude timeout runs (such as finding 2) without independent evidence of an infrastructure fault.**
- **Analysis:** Treat matched campaign pairs as replicates. Report per-phase means with range/spread across pairs, keeping violation rates alongside ready-pod-seconds and active-pod-seconds.

### B5. Additional baselines under the frozen configuration

- Request-rate HPA (A6) with the target from B2.
- CPU HPA at one or two lower targets (A7), chosen before running, to show the pods-versus-violations curve.
- Same number of repetitions as B4 if time allows; otherwise state clearly that these are single runs.

### B6. Out of scope for this plan

Full-system runs with placement and descheduling, and the locality-weight sensitivity (G4 in the runbook). The placement controller's own logging was not audited for gaps; do that before those runs.

## 6. Reporting rules

- Every report states: what ran, exact command, revision, what failed, what was observed, what is interpretation/hypothesis, what is still unknown, and the next action.
- Report violations per site as well as pooled.
- Report **ready-pod-seconds** and **active-pod-seconds** separately beside every violation result.
- Distinguish **offered**, **dispatched**, and **completed** request rates.
- With 60 s phases, do not describe any number as steady state.
- Count failures and missed slots separately from slow requests.
- Keep credentials, tokens and subscriber data out of reports.

## 7. Questions for the user

1. Scale-down values for B3: is stabilisation 30 s and 2 pods per 30 s acceptable?
2. Should A1 and A2 also be applied to `face-detection`?
3. Which CPU targets for B5?
4. Is a percentile other than p90 (for example p95) wanted as a second proposed variant? It is not planned above.
