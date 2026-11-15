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

The baseline uses Kubernetes' **default scheduler** with the same required eligibility (`gnb1`, `gnb-2`, `gnb-3`, `core-5g`) as proposed. It has no preference or special fallback rule for any node. The proposed controller scores all four nodes using the same placement algorithm; core has zero gNB-local traffic because it is not a gNB site, but can be selected on its resource score. Both methods use the same digest-pinned app image, photo set, 500m CPU / 256 MiB requests and 2 CPU / 2 GiB per-pod limits, shared replica ceiling (`--max-replicas`, default 100) and starting replica count of one. The HPA uses an absolute average CPU target of 1000m per pod (`AverageValue`), equivalent to 200% of the 500m request and half the 2-CPU limit. This replaces the previous 80%-of-request target (400m); treat runs with different HPA targets as separate baseline configurations. The scheduler and proposed placement policy also use the CPU request when checking node capacity; this can reduce the number of pods that fit. Descheduling removes one source replica before creating its replacement, so there is no temporary surge above the desired ceiling. During relocation, available capacity may fall by one, including a temporary service gap at one replica. The application NodePort is discovered automatically. Requests remain lower than limits, so monitor node load and CPU throttling.

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

UE switching is stop-and-reattach, **not handover**. This is closed-loop traffic, not a fixed offered request rate. Preflight and active collector checks do not independently prove every UE's attachment or physical route. Check those after network/provisioning changes. Record failures, telemetry drops and mapping coverage; a fast failure can lower all-attempt p90. The response-time plots use **successful requests only**; SLO attainment uses all attempts. Resource CPU windows can overlap phase boundaries. One sequential baseline/proposed pair is exploratory; repeat campaigns before drawing a causal conclusion.

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
