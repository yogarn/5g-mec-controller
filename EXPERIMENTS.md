# gNB3-hotspot experiments

## Run an experiment

On **core-5g**, in tmux, from the project root:

```bash
cd /home/member/skripsi_yoga/5g-mec-controller
tmux new -s mec-experiment
```

Inside that session:

```bash
bash experiments/run.sh run_2
```

Traffic control is **off by default**. To enable a 10 ms one-way inter-node delay and set a 750 ms controller/reporting SLO:

```bash
bash experiments/run.sh run_2 --tc-ms 10 --slo-ms 750
```

For faster debugging, use a separate campaign name and `--debug`:

```bash
bash experiments/run.sh debug_01 --debug --tc-ms 10 --slo-ms 300 --max-replicas 30
```

The debug profile retains the configured 13 phases at 60 seconds each: 13 minutes per method or 26 minutes per pair, plus UE reattachment, startup and transfer. `--phase-duration` can increase the phase duration; the CLI requires at least 60 seconds. A duration override alone does not change controller timers. Normal runs use the configured 60-second phases and standard HPA and descheduler timers.

| Timing | Normal | Debug |
|---|---:|---:|
| Controller reconciliation | 15 s | 15 s |
| Proposed scale-up cooldown | none (0 s) | none (0 s) |
| HPA scale-up stabilization | 0 s | 0 s |
| Placement-only / descheduling cooldown (`desired == replicas`) | 120 s | 15 s |
| Proposed scale-down stabilization (sustained-lower) | 300 s | 30 s |
| Descheduler check | 120 s | 30 s |
| Node CPU rate window | 120 s | 60 s |
| HPA scale-down stabilization (rolling-max) | 300 s | 30 s |
| Collector p90/site-count window | 15 s | unchanged |
| UE telemetry scrape | 15 s | unchanged |
| Node metrics scrape | 15 s | unchanged |
| Prometheus rule evaluation (p90 is not a recording rule) | 1 s | unchanged |

The scrape values above are the agreed experiment configuration; the profile does not reconfigure shared Prometheus or the collector. The p90 is exported directly by the collector, rather than computed by a Prometheus recording rule. After switching phases, allow roughly 15 seconds for old observations to leave its window, up to 15 seconds for the next telemetry scrape, and up to 15 seconds for a debug reconciliation. These are indicative sampling delays, not a response deadline; Pod readiness, CPU metrics collection, processing and UE reattachment add time. HPA sync and metrics-server collection remain cluster settings and are not accelerated by this flag. Removal-first relocation can still take tens of seconds. Short phases show transitions and need not reach steady state or complete scale-down. Debug runs change the policies' timing and must be labeled separately from evaluation runs.

The effective phase duration and debug flag are saved in `campaign.yaml`; generated controller and HPA manifests retain the exact policy settings. Traffic shaping, request timeouts, UE cleanup deadlines and Pod readiness checks are unchanged.

Omit `--tc-ms` for no added delay. Omit `--slo-ms` to use `slo_seconds` from `config.yaml` (currently 500 ms). Both methods use the same settings, saved in the campaign's `campaign.yaml`.

To compare placement under a shared replica budget, pass `--max-replicas`:

```bash
bash experiments/run.sh cap_3 --max-replicas 3 --tc-ms 10 --slo-ms 500
```

The flag sets both the baseline HPA's `maxReplicas` and the proposed controller's `max_replicas`. It must be a positive integer at least the configured minimum. Omit it to use `max_replicas` from `config.yaml` (currently 100) for both methods. Each campaign records the effective cap in `campaign.yaml`, both controller configurations, and the generated `baseline-hpa.yaml` that is actually applied. Use a fresh campaign name for each cap; keep the workload and other settings fixed. Reaching the cap does not disable score-improving descheduling: proposed can relocate one replica at a time without increasing the ceiling. The cap limits desired replicas, not Kubernetes' instantaneous count of terminating Pods during scale-down.

Use a **new name** for each campaign (`run_2`, `run_3`, etc.). Existing result directories are never reused. You do not need another tmux session, manually started samplers, manual UE commands, file transfers, or owner-label commands.

The command performs the complete sequence:

1. Checks dependencies, Kubernetes, collector health/scraping, SSH, passwordless sudo, reserved UE inventory and matching project revisions.
2. Removes the old experiment's app and HPA. It never deletes the collector, gNBs, UPFs or other services.
3. **Baseline:** applies the face-detection manifest (one fresh pod) and HPA, waits for readiness, runs preflight, starts resource sampling and remotely starts the UE workload.
4. Verifies active latency telemetry appears, waits for UE completion and cleanup, stops the sampler, transfers results and verifies their checksums. Produces the baseline summaries.
5. Deletes baseline HPA and app resources, including any managed groups from an earlier experiment.
6. **Proposed:** starts the controller with `--deploy`; the controller creates its own missing face-detection Deployment and Service. Waits for readiness, runs preflight, starts a fresh sampler and the identical UE workload.
7. Stops the controller and sampler, transfers/checksums and summarizes proposed results, generates PNG/PDF plots including per-phase SLO-violation percentages and a p90 target line, and removes the experiment app/HPA resources.

Progress is printed in the terminal. Per-method controller, sampler and UE logs are retained under `results/NAME/baseline/` and `results/NAME/proposed/`. UE console logs and original UE results also remain on the UE host. Final plots are in `results/NAME/plots/` on core.

Detach with `Ctrl-b d`; reattach with:

```bash
tmux attach -t mec-experiment
```

**Ctrl-C stops the campaign.** The runner signals only its recorded UE process, lets the UE lifecycle clean up, stops its controller/sampler and removes the isolated app resources. Errors stop the campaign rather than silently continuing. Logs and any partial data remain; use a new campaign name after fixing the error. Do not rerun old summary commands on the same directory.

## One-time setup

Both machines need the same updated project revision. On core and the UE host, update the checkout with:

```bash
cd /home/member/skripsi_yoga/5g-mec-controller
git pull --ff-only
```

If Git reports local changes, preserve them before updating; do not force-reset the checkout. Review `experiments/campaign.yaml` once. It contains the UE host/address and paths, the reserved inventory path, collector URL, schedule and phase duration. It contains **no authentication secrets**. The defaults are for:

- Core Kubernetes host: `10.34.211.140`.
- UE host: `member@10.34.211.6`, project `/home/member/skripsi_yoga/5g-mec-controller`.
- Reserved pool: `.private/ue-paper-24/inventory.yaml` on the UE host.
- UERANSIM: `/home/member/5g-ai-edge/ueransim/build/nr-ue`.
- Token: `/home/member/.config/5g-mec/ue-telemetry-token` on the UE host.

From core, set up dependencies on both machines once:

```bash
bash experiments/run.sh setup
```

This installs project requirements and Matplotlib on core, project requirements on the UE host, and checks noninteractive SSH/sudo access. It does **not** deploy or restart shared infrastructure. Core must be able to SSH to the configured UE host without a password, and that account must be authorized to use `sudo -n` for the UE runner. Configure that access before running setup; the script does not change sudoers or copy credentials.

Check readiness before running a campaign. This now includes an **active connectivity probe**:

```bash
bash experiments/run.sh check
```

## Infrastructure that must already exist

These are installation/provisioning tasks, **not per-run commands**:

- A working Open5GS core, three registered gNBs, routes and three IPv4 DNNs with correct UPF anchors.
- The 24 reserved subscribers, with static IPs and private UE configurations for all three sites. No other UE workload may use these reservations during a campaign.
- One ready collector in `placement/ue-telemetry`, NodePort **31669**, using the same token as the UE host.
- Prometheus in `placement`, reachable at `http://10.34.211.140:30990`, scraping `ue-telemetry.placement.svc.cluster.local:8080/metrics` and the configured node/GPU metrics.
- Metrics Server, the face-detection image registry and all three eligible Kubernetes nodes.

Reuse this infrastructure. Do not restart or reapply the shared monitoring or 5G manifests for every experiment. If the collector must be installed, review `collector/manifest.yaml`, create its token Secret in **placement**, and merge only `collector/prometheus-scrape.yaml` into the existing Prometheus configuration. The collector image is digest-pinned; a build alone does not update its running image. Never replace other Prometheus jobs.

For **first-time subscriber provisioning only**, create the private inventory using the schema in [README.md](README.md#provisioning-ue-subscribers). Each subscriber needs three configurations sharing the same SUPI/authentication values, with site-specific DNN, gNB address and static IP. On a trusted host with MongoDB access:

```bash
python3 -m utils.provision_ues --inventory /secure/ue-inventory.yaml --verbose
python3 -m utils.provision_ues --inventory /secure/ue-inventory.yaml --apply
```

The tool inserts only new subscribers; do not reprovision the existing pool. Writes are not transactional: inspect partial inserts before retrying. Subscriber secrets and telemetry tokens stay outside Git. Provisioning does not configure DNNs, UPFs or gNBs; independently verify registration, assigned static IPs and anchors when provisioning or changing the network.

## Fixed experiment conditions

The application remains in namespace **services**. The runner deletes only the named `mec-placement-face-detection` Deployment/Service and `mec-placement-face-detection-baseline` HPA before each method and after completion. Kubernetes removes the app Deployment's dependent managed groups. It never deletes all resources in `services`, never touches the collector in `placement`, and never deletes historical data.

The baseline uses Kubernetes' **default scheduler** with the same required eligibility (`gnb1`, `gnb-2`, `gnb-3`, `core-5g`) as proposed. It has no preference or special fallback rule for any node. The proposed controller scores all four nodes using the same placement algorithm; core has zero gNB-local traffic because it is not a gNB site, but can be selected on its resource score. Both methods use the same digest-pinned app image, photo set, 500m CPU / 256 MiB requests and 1 CPU / 512 MiB per-pod limits (limits are twice requests), shared replica ceiling (`--max-replicas`, default 100) and starting replica count of one. The CPU baseline HPA uses `averageUtilization: 60`, targeting 300m per pod (60% of the 500m request, or 30% of the 1-CPU limit). Placement-only comparisons use the same latency HPA for both methods. Treat runs with different resource limits or HPA targets as separate experimental configurations. The scheduler and proposed placement policy also use the CPU request when checking node capacity; this can reduce the number of pods that fit. Descheduling removes one source replica before creating its replacement, so there is no temporary surge above the desired ceiling. During relocation, available capacity may fall by one, including a temporary service gap at one replica. The application NodePort is discovered automatically. Requests remain lower than limits, so monitor node load and CPU throttling.

For both methods, each UE group sends to its own gNB's NodePort, using `site_node_ips` from `experiments/campaign.yaml`. The runner checks these against Kubernetes node InternalIPs. The Service uses `externalTrafficPolicy: Cluster`, `internalTrafficPolicy: Cluster`, no `trafficDistribution` preference, and `sessionAffinity: None`. Verify that the live kube-proxy uses iptables mode: the Service settings alone do not select the proxy mode. In iptables mode, each new connection randomly selects a ready endpoint across the cluster. A node with k of R ready Pods therefore receives approximately k/R of new connections, including core. This is probabilistic balancing, not strict round robin or load-based spillover; reused connections stay on their selected backend. The runner rejects locality preferences and client affinity before starting UE traffic. Routing from each UE through its UPF to its configured NodePort must be verified on the live network; preflight does not prove those paths.

To cancel a foreground campaign, press Ctrl+C once and allow cleanup to finish. The runner attempts to stop remote UEs and local processes, remove experiment workloads, and clean up campaign traffic shaping. Avoid a forced kill or repeated interrupts during cleanup. Interrupted methods skip the normal result transfer and summaries; retain their local logs and UE-host raw results. Label campaigns using the previous `PreferSameNode` Service as a separate routing condition and do not pool them with cluster-wide routing results.

When `--tc-ms` is specified, both methods use the reserved Calico pool `mec-campaign-reserved` (`172.16.40.0/23`) and the runner applies the requested one-way netem delay per inter-node egress interface, in both directions, only to TCP port 5000 traffic to or from that pool. It checks the filters during traffic and removes its filters and IFB devices after deleting the experiment pods. The pool and empty `clsact` hooks remain reserved after the run so that unrelated pods cannot inherit stale rules after a crash and existing filters are never removed. Explicitly assigning unrelated pods to this pool defeats isolation. A forced process kill can leave filters and IFBs; the next campaign with `--tc-ms` refuses to overwrite them and requires manual cleanup. Without `--tc-ms`, the runner adds no shaping or pool annotation.

The controller computes `ceil(current replicas × all-attempt p90 / target)`, within its configured bounds. `--slo-ms` sets that target and the threshold for both methods' summaries and plots. The collector's separately configured SLO-attainment metric is not changed by the flag; the campaign plots calculate attainment from raw attempts. The controller plans added replicas individually against projected capacity and applies the resulting per-node counts without a per-step cooldown. The descheduler checks every 120 seconds when the workload is settled and scaling is not needed. It evaluates all occupied nodes, selects the largest strictly positive target-minus-current score delta, and removes one source replica before creating its replacement. Placement and replacement remain enforced through node-specific Deployments; this uses Deployment scale-down rather than the Kubernetes Eviction API and does not enforce PodDisruptionBudgets. In-flight progress is stored durably; old surge-first migration state must be completed before upgrading. Missing required metrics make reconciliation fail closed.

The objective is **all-attempt p90 <=500 ms** by default, not a p95 objective. Per-attempt violation rates and p95 remain supplementary diagnostics.

Each phase generates traffic for **60 seconds**. The 13-phase schedule covers balanced and gNB-3 hotspot demand at 3, 9, 15, and 24 UEs, followed by recovery. Traffic takes **13 minutes per method**, or **26 minutes per paired campaign**, plus UE switching, setup, and transfer.

The 300-second HPA scale-down stabilization spans multiple phases. Interpret this campaign as a transient workload experiment; extend phase durations if settled scale-down is an endpoint. The schedule is `total:gnb1:gnb2:gnb3`:

```text
3:1:1:1
9:3:3:3
9:1:1:7
15:5:5:5
15:1:1:13
24:8:8:8
24:1:1:22
24:8:8:8
15:1:1:13
15:5:5:5
9:1:1:7
9:3:3:3
3:1:1:1
```

The original 19-phase schedule is preserved in `experiments/campaign-19-phase.yaml` for explicit coverage runs with `--config`. For the pedestrian experiment, pass `--slo-ms 100` so the controller, summaries and plots share the candidate deadline.

UE switching is stop-and-reattach, **not handover**. Traffic uses **fixed arrival-rate (open-loop) pacing** (`5` RPS/UE by default via `--rps-per-ue`, bounded by `--max-in-flight-per-ue`, default `8`) rather than closed-loop pacing; `5` RPS/UE (`15` RPS at 3 UEs, `45` at 9, `75` at 15, `120` at 24) is an **initial rate requiring calibration, not proven capacity**. Preflight and active collector checks do not independently prove every UE's attachment or physical route. Check those after network/provisioning changes. Record failures, missed starts, request coverage, telemetry drops and mapping coverage; a fast failure or omitted request can otherwise make latency look better. The response-time plots use **successful requests only**; SLO attainment uses all attempts and is reported alongside request coverage (`completed / scheduled`). Resource CPU windows can overlap phase boundaries. One sequential baseline/proposed pair is exploratory; repeat campaigns before drawing a causal conclusion.

Locality is relative request-attempt demand: `site attempts / total attempts`. Its 40% score contribution is unchanged by assigning another replica until observed demand changes; resource headroom is recomputed after each proposed addition. This is a site-demand preference, not residual demand per replica. The CPU-only resource score uses 50% CPU and 50% RAM, with GPU and VRAM weights zero.

The Go descheduler evaluates positive score improvements using a post-removal resource snapshot. It skips eviction if the freed source node would win replacement scheduling, waits for settled Pods, and evicts at most one Pod per interval through the Eviction API. The ReplicaSet creates its replacement and the Go scheduler scores current conditions before binding it. PodDisruptionBudget rejection defers eviction.

Both methods use native HPA for replica management. In the full-system comparison, baseline uses CPU HPA and proposed uses latency HPA; in the placement comparison, both use the same latency HPA. Normal scale-down stabilization is 300 seconds (30 seconds with `--debug`). There are no per-node child Deployments or Python controller processes in the default path.

Placement scoring can be configured per campaign without editing `config.yaml`:

```bash
# CPU-only resource component, retaining the default 40% locality contribution:
bash experiments/run.sh debug_cpu_01 --debug --slo-ms 500 --tc-ms 10 --max-replicas 30 --resource-weights 1 0 0 0
# CPU/RAM component with no locality contribution:
bash experiments/run.sh debug_resource_01 --debug --slo-ms 500 --tc-ms 10 --max-replicas 30 --resource-weights 0.5 0.5 0 0 --locality-weight 0
```

`--resource-weights CPU MEMORY GPU VRAM` requires four finite nonnegative numbers summing to 1. These are weights *inside* the resource component. `--locality-weight L` selects the traffic-share contribution; the resource contribution is `1-L`. Both flags affect proposed placement/descheduling, not the baseline HPA or default scheduler. Enabling GPU/VRAM weights requires valid exporter telemetry. Effective weights are printed and saved in campaign metadata and generated controller configs. Weights cannot make an infeasible node eligible or guarantee equal Pod counts. Keep matched comparisons and change one scoring factor per experiment.

Controller reconciliation is 15 seconds in both profiles to match Kubernetes' documented default HPA sync period (`--horizontal-pod-autoscaler-sync-period`, 15 s). Read-only inspection on `core` (K3s `v1.35.4+k3s1`) found no `--horizontal-pod-autoscaler-sync-period` or `kube-controller-manager-arg` in `/etc/systemd/system/k3s.service` or `/etc/rancher/k3s/config.yaml` and no `config.yaml.d` fragments, though `/etc/systemd/system/k3s.service.env` was unreadable, so the 15 s runtime sync period is strongly indicated rather than fully verified. The agreed UE telemetry and node-exporter scrape intervals are both 15 seconds; other jobs and global rule evaluation retain their existing settings. Metrics Server was inspected with `--metric-resolution=15s`. The debug CPU rate window is 60 seconds to retain about four samples at the new scrape interval. Verify live timing before evaluation; the campaign runner does not deploy monitoring changes. The controller currently sleeps 15 seconds after each reconciliation, so its actual start-to-start interval also includes processing time. Matching configured periods reduces a timing difference but does not establish identical metric age, phase alignment, or stabilization algorithms. This is configured timing, not phase-lock synchronization: a controller check can still precede a scrape. The runner does not change the live Prometheus scrape configuration or cluster controller-manager settings.

`bash experiments/run.sh check` and automatic campaign preflight create an isolated temporary namespace with the same image-inference application and a cluster-wide NodePort. They register one reserved UE at each gNB, submit one image inference request per site, then repeat after tearing down and reattaching the three UEs. Both cycles must succeed before the campaign starts. This tests data-session setup and UE-to-NodePort/application connectivity, not physical attachment independently or all 24 subscriber configurations. It does not reproduce campaign load or apply the campaign's `tc` shaping. A passing probe cannot rule out a later intermittent storage or network failure.

Probe traffic is not submitted to UE latency telemetry, and temporary sessions, routes, application and Service are cleaned up on completion/failure. UE logs and request records remain under `results/mec-preflight-<id>/` on the UE host; failure messages identify the expected UE address and registration/PDU setup log. Checks refuse to interfere with existing UE workloads and use the campaign lock. Probes add startup, registration and cleanup time; they are not read-only checks. Do not force-kill them during cleanup.

Campaign names are checked on core and the configured UE host before active preflight. A duplicate name stops immediately with the host and absolute path; SSH/check failures are reported separately. Both paths must be absent. UE results can be root-owned because the runner requires root for UE interfaces. Archive rather than overwrite previous results. The runner rechecks name availability after preflight and does not create an empty core directory when it finds a remote collision.

## Nine-UE mobility scenario

Run from core with a new result name:

```bash
bash experiments/run.sh mobility9_01 --scenario mobility9
```

This scenario keeps nine UEs active per traffic phase, with splits `3:3:3 → 7:1:1 → 1:7:1 → 1:1:7 → 1:7:1 → 7:1:1 → 3:3:3`. Each phase lasts 60 seconds: seven minutes of traffic per method, fourteen minutes per pair, plus setup, UE reattachment and transfer. Movement means stop-and-reattach rather than seamless handover.

For a longer convergence test, add `--long-run` to use seven **120-second phases** per method (28 minutes of traffic per pair, plus setup and UE switching). This is a shortcut for `--phase-duration 120`; the two flags are mutually exclusive. It retains the normal 30-second descheduler interval and HPA timers. Record the first and second minute separately when assessing initial response versus convergence; the existing phase plots average the full 120 seconds.

```bash
bash experiments/run.sh mobility9_fixed9_tc10_150ms_long_01 --scenario mobility9 --long-run --fixed-replicas 9 --tc-ms 10 --slo-ms 150
```

The default comparison is `placement`: both methods use the same native latency HPA at 100 ms. Proposed uses the Go scheduler/descheduler. The descheduler checks every 30 seconds; HPA stabilization and the CPU measurement window retain their normal settings. Override the check interval independently with `--deschedule-interval SECONDS`; `--phase-duration`, `--slo-ms`, and `--locality-weight` remain available. `--debug` is rejected for this scenario to avoid changing other timers.

The shorter descheduler interval gives each hotspot opportunities for eviction, but does not force a move. Pods must be settled, the destination must be feasible, and the score improvement must be positive; PDB rejection also defers eviction. In particular, gNB-1 may lack capacity. Inspect `proposed/controller.log` for `descheduler evicted`, `descheduler noop`, and `descheduler deferred`, alongside placement and latency plots. HPA remains active, so new-Pod scheduling also affects placement; this is a diagnostic of the placement system, not an isolated descheduler-only test. Repeat with reversed hotspot order before treating an observed effect as general.

Preview locally without traffic or cluster access:

```bash
bash experiments/run.sh --scenario mobility9 --plan-only /tmp/mobility9-plan
```

For a placement comparison at a fixed replica budget with inter-node traffic shaping:

```bash
bash experiments/run.sh mobility9_fixed6_tc10_01 --scenario mobility9 --fixed-replicas 6 --tc-ms 10
```

`--fixed-replicas 6` sets both methods' initial Deployment replicas and HPA minimum/maximum to six. HPA cannot change this budget; an eviction can temporarily reduce the number of ready Pods while the ReplicaSet creates a replacement. The scenario retains nine UEs, 60-second phases, 30-second descheduler checks and the 100 ms reporting objective. Do not combine `--fixed-replicas` with `--max-replicas`.

`--tc-ms 10` adds 10 ms one-way delay on the selected inter-node workload interfaces in both directions (about 20 ms added RTT per shaped round trip). Same-node traffic is not delayed by this rule. Both methods use the reserved workload pool, and campaign cleanup removes its shaping filters. Use a fresh result name for each run; remove `--tc-ms` for a separate unshaped comparison.

## Fixed arrival-rate load generation & calibration

All scenarios (`--comparison full`, `--comparison placement`, `--scenario mobility9`, `--fixed-replicas`, and `utils/fixed_replica_harness.py`) schedule per-UE request starts on a monotonic clock every `1 / rps` seconds with deterministic per-UE phase offsets `(ue_pos / active_ues) * period` and bounded in-flight concurrency (`--max-in-flight-per-ue`, default `8`). If concurrency is saturated or the pacer lags past a slot boundary, missed slots are counted separately from HTTP failures and skipped without catch-up bursts. At phase end, new scheduling stops and all in-flight requests drain to completion before UEs reattach.

Treat the default `--rps-per-ue 5` as an **initial rate requiring calibration, not proven capacity** (`3` UEs = `15` RPS, `9` UEs = `45` RPS, `15` UEs = `75` RPS, `24` UEs = `120` RPS). Calibrate single-replica and multi-replica capacity before full comparisons:

```bash
# Fixed-replica calibration run (adjust --rps-per-ue / --max-in-flight-per-ue as needed):
.venv/bin/python -m utils.fixed_replica_harness \
  --campaign-dir results/cal_fixed1 --stage cal --block 1 --run-id r1_5rps \
  --replicas 1 --node gnb-2 --schedule 6:0:6:0 --rps-per-ue 5 --max-in-flight-per-ue 8

# Standard scaling campaign at explicit rate:
bash experiments/run.sh run_5rps_01 --comparison full --slo-ms 100 --rps-per-ue 5 --max-in-flight-per-ue 8

# Nine-UE mobility comparison (or fixed-replica mobility9) at the same rate:
bash experiments/run.sh mobility9_5rps_01 --scenario mobility9 --rps-per-ue 5 --max-in-flight-per-ue 8
bash experiments/run.sh mobility9_fixed6_5rps_01 --scenario mobility9 --fixed-replicas 6 --rps-per-ue 5 --max-in-flight-per-ue 8
```

`utils/summarize.py` writes `scheduled`, `started`, `completed`, `failures`, `missed`, `actual_start_rate`, `request_coverage`, and `slo_attainment_scheduled` across `summary.csv` (by phase), `summary-sites.csv` (by phase and gNB site), and `summary-ues.csv` (by phase and UE). `utils/plot_autoscaler.py` plots request coverage alongside SLO violations in `slo-violations.{png,pdf}`.

### Short calibration command

On core, use `.venv/bin/python -m utils.calibrate_mobility calib_01` to run one balanced `9:3:3:3` phase per method with six replicas, 10 ms TC, 5 RPS/UE, and 60-second phases. This reuses campaign preflight, collection, and cleanup. Choose a fresh name on each run. Other campaign flags can override the defaults. Preview without traffic using `.venv/bin/python -m utils.calibrate_mobility --plan-only /tmp/calibration-plan`.

## Scaler-only balanced scenario (`scaler-balanced`)

Use `--comparison scaling --scenario scaler-balanced` to compare native CPU HPA (`60%` target) against native latency HPA (`100` ms p90 target) on `gnb-3` alone with `default-scheduler` for both methods, no custom `placement-controller` interaction, `1..24` replica bounds (override with `--max-replicas`), 60-second balanced phases (`3:1:1:1,9:3:3:3,15:5:5:5,24:8:8:8,15:5:5:5,9:3:3:3,3:1:1:1`), and fixed `5` RPS/UE (`--max-in-flight-per-ue 8`). Before traffic starts, the runner verifies `gnb-3` allocatable CPU/memory minus active pods can fit `max_replicas`.

```bash
# Offline preview (no cluster or network access):
bash experiments/run.sh --comparison scaling --scenario scaler-balanced --final-hold --plan-only /tmp/scaler-balanced-plan

# Live run (60s phases; add --final-hold for a 420s final 3:1:1:1 phase to observe 300s HPA scale-down):
bash experiments/run.sh scaler_01 --comparison scaling --scenario scaler-balanced --final-hold
```

## Resource summaries & limitations (`utils/monitor_resources.py`)

`utils/monitor_resources.py` polls workload Pod lists (`kubectl get pods`) with a 5-second delay between collection rounds. Campaigns use `--pods-only`, which skips Metrics Server requests and records Pod lifecycle and placement independently of resource collection. Without `--pods-only`, the sampler also collects Kubernetes Metrics Server Pod metrics (`/apis/metrics.k8s.io/v1beta1/namespaces/<ns>/pods`), keeping metrics for all workload Pods (including recently terminated or terminating Pods) so churned measurements are not dropped before summarization.

- **What CPU and memory numbers measure:** `cpu_millicores` is the sum of container CPU usage averages over each Pod's Metrics Server reporting window, and `memory_mib` is the sum of container memory working sets, averaged across **complete simultaneous rounds** in each phase. A round groups per-Pod metric timestamps within `10` seconds of each other, requires consistent CPU windows (`<= 5s` spread), and requires an exact match to the active `Running` Pod set observed near the round (`<= 8s` age).
- **Incomplete, skewed, or missing observations:** Skewed Pod timestamps (`> 10s` apart) are never combined into a simultaneous total. Rounds missing one or more running Pods are recorded separately as `partial_samples` (and `partial_cpu_millicores` / `partial_memory_mib`, never mixed into total `cpu_millicores` / `memory_mib`). Rounds with churn/window mismatches are counted in `rejected_samples`. Phases with zero complete rounds leave `cpu_millicores` and `memory_mib` blank (`missing`), never zero-filled or interpolated.
- **Coverage & plots:** `resource-by-ramp.csv` exports `complete_samples`, `partial_samples`, `rejected_samples`, `missing_samples`, `round_completeness_ratio` (complete among observed rounds), `availability_ratio` (non-empty metric observations over total valid + error observations), `coverage_ratio`, `completeness_status`, and `missing_reasons`. Rounds whose Pod membership changes across their `[min_t, max_t]` interval are rejected (`pod_membership_changed_within_round`). `utils/plot_autoscaler.py` plots `resource-usage.{png,pdf}` with a dedicated resource-coverage bar panel and missing/partial annotations so churn-induced gaps are never presented as unbiased full-phase means. Metrics Server scrapes asynchronously (~15–30s windows), so rapid Pod replacement churn can still yield gaps; those gaps are retained rather than fabricated.

## Prometheus/cAdvisor resource monitoring (`utils/cadvisor_resources.py`)

New experiment runs use **Prometheus/cAdvisor** as the primary source for `resource-by-ramp.csv` and `resource-usage.{png,pdf}`, while **Metrics Server remains unchanged for native CPU HPA** (`autoscaler/hpa-baseline.yaml`). Application Pods require no sidecars or manifest changes: cAdvisor is exposed per node by kubelet (`/metrics/cadvisor`) and scraped by `placement-prometheus` via the Kubernetes API server proxy (`nodes/proxy` RBAC, ServiceAccount TLS + bearer token) every `5s` (`scrape_timeout: 4s`) with explicit cardinality filtering (`container_cpu_usage_seconds_total` and `container_memory_working_set_bytes` only, `namespace="services"`, `container!=""` and `container!="POD"`).

- **Deploy / reload Prometheus config (once after syncing):**
  ```bash
  kubectl apply -f telemetry/03-prometheus.yaml && kubectl -n placement rollout status deployment/placement-prometheus --timeout=120s
  ```
- **Verify metric availability on the live cluster:**
  ```bash
  curl -fsS 'http://10.34.211.140:30990/api/v1/query?query=up{job="kubelet-cadvisor"}'
  curl -fsS 'http://10.34.211.140:30990/api/v1/query?query=count(container_cpu_usage_seconds_total{job="kubelet-cadvisor",namespace="services"})'
  ```
- **One-line offline reprocessing from saved `resources-prometheus.json` into a new output directory (no Prometheus connection needed; never overwrite historical results):**
  ```bash
  .venv/bin/python -m utils.cadvisor_resources summarize results/RUN/baseline/resources-prometheus.json results/RUN/baseline/requests.jsonl results/RUN/baseline/summary.csv results/RUN_reprocessed/baseline --metrics-jsonl results/RUN/baseline/metrics.jsonl --rate-window 45 --freshness 25
  ```
- **One-line offline 10-second transient analysis for Phase 2 (writes to a new directory, never modifies original results):**
  ```bash
  .venv/bin/python -m utils.transient_analysis results/RUN results/RUN_transient --phase phase_02 --bin-seconds 10
  ```
- **Workload health-listener rebuild & deployment (required before testing the `/health` port `5001` starvation fix; existing deployed images do not contain it):**
  ```bash
  bash experiments/run.sh rebuild
  ```
  This builds a unique image tag, checks the loaded model and dedicated health/readiness endpoints in a local container, pushes it, and pins the published digest in both workload manifests. It shares the campaign lock and stops before updating manifests if build, smoke testing or digest validation fails. The campaign deploys the image. Preview with `bash experiments/run.sh rebuild --plan-only`.
- **Short scaler ramp command (60s phases, no extra final hold, TC 10ms, SLO 150ms, 5 RPS/UE, maximum 8 in flight):**
  ```bash
  bash experiments/run.sh ramp scaler_floor_short_01
  ```
  Supply a fresh result name. Additional campaign options may follow the name. This selects the floor-adjusted native latency HPA described below; append `--latency-floor-ms 0` for the raw RT-ratio variant.
- **Live checks still required before live evaluation:** verify `nodes/proxy` `/metrics/cadvisor` access on all 4 nodes (`core-5g`, `gnb1`, `gnb-2`, `gnb-3`), confirm container-instance identity labels (`id`/`container_id`), verify `up{job="kubelet-cadvisor"} == 1` with no duplicate targets, and check 5s scrape CPU/memory overhead and 24h retention.
- **Boundary effects:** CPU rates use a `45s` rate window (with `25s` freshness to accommodate 10–20s cAdvisor container timestamp spacing while keeping Prometheus scrape and Pod lifecycle polling at `5s`) per container instance (with counter-reset handling). A `45s` CPU rate window can cross the `60s` phase boundaries, and newly started containers require `>= 2` samples in the window before a rate exists. Resource summaries are averages of valid observations, not exact phase CPU consumption. If cAdvisor data is missing, `resource-by-ramp.csv` leaves totals blank rather than substituting Metrics Server.

## Proposed low-load TC-on / TC-off path diagnostic (not executed)

To isolate fixed UPF/inter-node path latency from queueing and autoscaling effects without altering the `100` ms all-attempt p90 objective, run a short low-load diagnostic pair (e.g. `1`–`3` UEs at `1`–`2` RPS/UE on a fixed replica count) with `--tc-ms 10` vs. without `--tc-ms`:

```bash
# Diagnostic only (low load, fixed replicas; compare p50/p90 baseline path delay):
.venv/bin/python -m utils.fixed_replica_harness --campaign-dir results/diag_tc_off --stage diag --block 1 --run-id tc_off --replicas 6 --node gnb-2 --schedule 3:1:1:1 --rps-per-ue 1 --max-in-flight-per-ue 4
.venv/bin/python -m utils.fixed_replica_harness --campaign-dir results/diag_tc_on  --stage diag --block 1 --run-id tc_on  --replicas 6 --node gnb-2 --schedule 3:1:1:1 --rps-per-ue 1 --max-in-flight-per-ue 4 --tc-ms 10
```


## Gradual scaler staircase (new configuration)

Run `bash experiments/run.sh ramp scaler_03` inside tmux. This shortcut now selects
`--scenario scaler-staircase`, 60s phases, no extra final hold, TC 10ms, SLO 150ms,
5 RPS/UE, max 8 in flight/UE, and an exploratory latency reference of 130ms.
Both methods use default-scheduler on gnb-3,
replica bounds 1..24, requests 500m CPU / 256Mi and limits 1 CPU / 512Mi.
The rebuilt image digest from core is pinned in both manifests.

UE steps are 3, 6, 9, 12, 15, 18, 24, 18, 15, 12, 9, 6, 3, equally divided
between the three gNBs. All thirteen phases last 60s by default (13 minutes of
traffic per method, 26 minutes per pair, plus setup, request drain and UE
reattachment). This default also applies to direct staircase invocation.
`--phase-duration` overrides the regular phases. Opt into `--final-hold` to extend
the last phase to at least 600s; it is no longer part of the shortcut. To reproduce
the older 46-minute-per-method timing, append `--phase-duration 180 --final-hold`.
The old `scaler-balanced` flag retains its seven-phase schedule
and original HPA behavior for reproducibility. Mobility is unchanged.

The staircase gives both HPAs matched behavior: scale up by at most 2 Pods/30s,
scale down by at most 1 Pod/30s, scale-down stabilization 60s, and scale-down
tolerance 0.01. This requires Kubernetes support for per-direction HPA tolerance
(v1.35 cluster used here). CPU baseline still targets 60% of requests. The `ramp` shortcut now opts into the floor-adjusted variant described below;
direct scenario invocation keeps raw RT/SLO unless `--latency-floor-ms` is set.
These settings do not guarantee SLO attainment or full replica convergence;
integer rounding, metric delay, and floor choice remain limitations.
Do not label this configuration as the unchanged default-HPA comparison.

Plots preserve the full-range `response-time` figure. `response-time-zoom` adds
a 0–300ms whole-phase panel and a final-60s panel, with all values above 300ms
marked and labelled. `late-phase` also shows final-60s latency on an unrestricted
axis and all-attempt violation percentages. `{method}-phase-windows.csv` records
whole-phase and late-phase latency, failures, exact scheduled-slot coverage and
scheduled SLO attainment when slot metadata exists. Request latency is grouped
by dispatch time; coverage is grouped by scheduled slot. Missing slot metadata
leaves coverage unknown. Late-phase measurements are not automatically steady
state. Check the retained pod/HPA timeline for convergence; do not average away
or discard the transition costs. These request windows do not change cAdvisor
resource-window semantics. With 60s phases, the final-60s request window covers
the entire phase and gives no separate evidence of settling.

Frozen results and `scaler_02` retain their original parameters. This is a new
exploratory configuration following inspection of those runs; preserve both.


## Exploratory floor-adjusted latency HPA

The user authorized trying this variant after examining scaler_02. The `ramp`
shortcut now supplies `--latency-floor-ms 130`. With SLO 150ms, proposed HPA uses
`ceil(currentReplicas * max(RT - 130ms, 0) / 20ms)`, subject to the existing
replica bounds, stabilization, tolerance and rate limits. CPU baseline stays
at 60% of requests, with the matched staircase rate/stabilization settings.
Reporting still uses the full response time against 150ms, including failures
and scheduled-request coverage. No Python replica controller is introduced.

130ms is a fixed exploratory reference below the roughly 140ms low-load p90
observed in prior runs. It is not a measured physical minimum, an identified
queueing delay, or a Kubernetes recommendation. The difference is a heuristic
load-sensitive latency signal. This variant needs validation and independent
replication; neither smoothness nor eventual minimum replica count is assured.
An overestimated reference can make the signal zero and encourage excessive
downscaling. A reference close to the SLO increases sensitivity. Record changes
as distinct experimental configurations rather than tuning an ongoing run.

`--latency-floor-ms` accepts nonnegative whole milliseconds, requires the
staircase scenario when nonzero, and must be below the SLO. Use
`bash experiments/run.sh ramp scaler_floor_01` to try 130ms, or append
`--latency-floor-ms 0` to retain raw RT/SLO. Direct campaign invocation defaults
to zero, so legacy and mobility scenarios keep their original formula.

For each nonzero reference the campaign renders a separate adapter metric, e.g.
`ue_p90_excess_latency_130ms_seconds`, using
`clamp_min(ue_request_attempt_duration_p90_seconds{job="ue-telemetry"} - 0.13, 0)`
with the existing complete-telemetry-window guard. Missing/incomplete source
telemetry stays absent, while an observed latency below the reference produces
a legitimate zero. The HPA target is `20m` seconds for the 130/150ms example.

`--plan-only` writes the proposed HPA, adapter ConfigMap preview and run metadata
without cluster access. During an actual campaign, after capacity checks and
before either method starts, the runner appends/updates only the selected
floor-specific rule in the live ConfigMap and restarts/waits for the adapter.
Other adapter rules and data keys are preserved; the raw metric stays available.
The pre-change ConfigMap is saved in `adapter-before.json`, and the desired
configuration in `floor-adapter-configmap.yaml`. A failed adapter rollout
attempts to restore the prior config and aborts before UE traffic. Rules for
previous reference values remain available. The active-telemetry check queries
the exact floor-specific metric during the proposed run. Syncing Git alone
neither installs the rule nor restarts the adapter.

Implementation references: [Prometheus clamp_min](https://prometheus.io/docs/prometheus/latest/querying/functions/#clamp_min)
and [Prometheus Adapter configuration](https://github.com/kubernetes-sigs/prometheus-adapter/blob/master/docs/config.md).


## Exploratory internal RT target factor (`--rt-target-factor`)

`--rt-target-factor` (default `1.0`, finite `0 < factor <= 1`) separates the
**reporting SLO** (`--slo-ms`) from the **internal RT scaling target**
(`slo_ms * rt_target_factor`) used only by the proposed RT HPA:

- **Raw mode (`--latency-floor-ms 0`):** proposed HPA scales on
  `RT / internal_target` (for example, `150 * 0.95 = 142.5ms`).
- **Floor mode (`--latency-floor-ms > 0`):** proposed HPA scales on
  `max(RT - floor, 0) / (internal_target - floor)` (for example, with SLO
  `150`, floor `130`, and factor `0.95`, internal target is `142.5ms` and excess
  target is `12.5ms`). Configurations where `internal_target <= latency_floor_ms`
  are rejected.
- **Exact fractional targets in Kubernetes HPA:** Kubernetes HPA reads external
  metric values and targets via `Quantity.MilliValue()`, which rounds fractional
  milli-quantities (e.g. `142.5m` → `143m`, `12.5m` → `13m`). When the effective
  target has fractional milliseconds, the campaign automatically exposes a
  millisecond-unit adapter metric (`ue_p90_latency_milliseconds` or
  `ue_p90_excess_latency_<floor>ms_milliseconds`, `* 1000`) and sets the HPA
  target directly in milliseconds (`142.5` or `12.5`, so `MilliValue()` is
  `142500` or `12500`). Whole-millisecond targets (e.g. `135m`, `5m`, `20m`)
  retain the seconds-unit metrics.
- **Unchanged evaluation and baseline:** request SLO classification, violation
  plots, attainment summaries, baseline CPU target (`60%`), replica bounds, and
  HPA behavior remain evaluated against `--slo-ms`. Legacy `--autoscaler
  controller` and `--comparison placement` reject `rt_target_factor != 1.0`
  (placement-only comparisons must share the same latency autoscaler).

`--rt-target-factor` is an **experimental headroom parameter**, not a
theoretically established constant. The `ramp` shortcut keeps the default
`1.0` because unloaded p90 is roughly `140`–`142` ms; a `0.90` factor (`135` ms
internal target) may be unattainable under this workload and requires
experimental validation.

Example `--plan-only` commands (no cluster access or traffic):

```bash
# Raw mode with 0.95 headroom factor (150ms reporting SLO -> 142.5ms internal target):
.venv/bin/python -m experiments.campaign --comparison scaling --scenario scaler-staircase --slo-ms 150 --rt-target-factor 0.95 --plan-only /tmp/plan_raw_095

# Floor mode with 0.95 headroom factor (150ms SLO, 130ms floor -> 142.5ms internal target, 12.5ms excess target):
.venv/bin/python -m experiments.campaign --comparison scaling --scenario scaler-staircase --slo-ms 150 --latency-floor-ms 130 --rt-target-factor 0.95 --plan-only /tmp/plan_floor_095

# Floor mode with 0.90 headroom factor via ramp preset forwarding (135ms internal target, 5ms excess target):
bash experiments/run.sh ramp --rt-target-factor 0.90 --plan-only /tmp/plan_floor_090
```
