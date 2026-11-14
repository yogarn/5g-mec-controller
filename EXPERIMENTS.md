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

You can check readiness at any time **without starting a campaign or modifying Kubernetes resources**:

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
python3 -m experiments.provision_ues --inventory /secure/ue-inventory.yaml --verbose
python3 -m experiments.provision_ues --inventory /secure/ue-inventory.yaml --apply
```

The tool inserts only new subscribers; do not reprovision the existing pool. Writes are not transactional: inspect partial inserts before retrying. Subscriber secrets and telemetry tokens stay outside Git. Provisioning does not configure DNNs, UPFs or gNBs; independently verify registration, assigned static IPs and anchors when provisioning or changing the network.

## Fixed experiment conditions

The application remains in namespace **services**. The runner deletes only the named `mec-placement-face-detection` Deployment/Service and `mec-placement-face-detection-baseline` HPA before each method and after completion. Kubernetes removes the app Deployment's dependent managed groups. It never deletes all resources in `services`, never touches the collector in `placement`, and never deletes historical data.

The baseline uses Kubernetes' **default scheduler** with the same required eligibility (`gnb1`, `gnb-2`, `gnb-3`, `core-5g`) as proposed. It has no preference or special fallback rule for any node. The proposed controller scores all four nodes using the same placement algorithm; core has zero gNB-local traffic because it is not a gNB site, but can be selected on its resource score. Both methods use the same digest-pinned app image, photo set, 500m CPU / 256 MiB requests and 2 CPU / 2 GiB per-pod limits, shared replica ceiling (`--max-replicas`, default 100) and starting replica count of one. The HPA's 80% CPU target is based on the 500m request (400m per pod), not the CPU limit. The scheduler and proposed placement policy also use the CPU request when checking node capacity; this can reduce the number of pods that fit. Descheduling removes one source replica before creating its replacement, so there is no temporary surge above the desired ceiling. During relocation, available capacity may fall by one, including a temporary service gap at one replica. The application NodePort is discovered automatically. Requests remain lower than limits, so monitor node load and CPU throttling.

For both methods, each UE group sends to its own gNB's NodePort, using `site_node_ips` from `experiments/campaign.yaml`. The runner checks these against Kubernetes node InternalIPs. The Service requests `PreferSameNode`: kube-proxy prefers a ready endpoint on the receiving node and falls back to another node when none is local. It does not spill over based on endpoint load. Routing from each UE through its UPF to its configured NodePort must be verified on the live network; preflight does not prove those paths.

When `--tc-ms` is specified, both methods use the reserved Calico pool `mec-campaign-reserved` (`172.16.40.0/23`) and the runner applies the requested one-way netem delay per inter-node egress interface, in both directions, only to TCP port 5000 traffic to or from that pool. It checks the filters during traffic and removes its filters and IFB devices after deleting the experiment pods. The pool and empty `clsact` hooks remain reserved after the run so that unrelated pods cannot inherit stale rules after a crash and existing filters are never removed. Explicitly assigning unrelated pods to this pool defeats isolation. A forced process kill can leave filters and IFBs; the next campaign with `--tc-ms` refuses to overwrite them and requires manual cleanup. Without `--tc-ms`, the runner adds no shaping or pool annotation.

The controller computes `ceil(current replicas × all-attempt p90 / target)`, within its configured bounds. `--slo-ms` sets that target and the threshold for both methods' summaries and plots. The collector's separately configured SLO-attainment metric is not changed by the flag; the campaign plots calculate attainment from raw attempts. The controller plans added replicas individually against projected capacity and applies the resulting per-node counts without a per-step cooldown. The descheduler checks every 120 seconds when the workload is settled and scaling is not needed. It evaluates all occupied nodes, selects the largest strictly positive target-minus-current score delta, and removes one source replica before creating its replacement. Placement and replacement remain enforced through node-specific Deployments; this uses Deployment scale-down rather than the Kubernetes Eviction API and does not enforce PodDisruptionBudgets. In-flight progress is stored durably; old surge-first migration state must be completed before upgrading. Missing required metrics make reconciliation fail closed.

The objective is **all-attempt p90 <=500 ms** by default, not a p95 objective. Per-attempt violation rates and p95 remain supplementary diagnostics.

Each phase generates traffic for **300 seconds**. The 19 phases traverse the original ascending schedule and then reverse it, with the peak included once. Traffic takes **95 minutes per method**, or **190 minutes per paired campaign**, plus UE switching, setup, and transfer. The longer phases allow observation of the 180-second scale-down stabilization and 120-second cooldown, but do not guarantee full convergence at every tier. The wrapper prints each phase number, ramp direction, UE split, and traffic-start/completion messages, with a progress reminder every 30 seconds. The schedule is `total:gnb1:gnb2:gnb3`:

```text
3:1:1:1
9:3:3:3
9:1:2:6
9:1:1:7
15:5:5:5
15:1:4:10
15:1:1:13
24:8:8:8
24:1:7:16
24:1:1:22
24:1:7:16
24:8:8:8
15:1:1:13
15:1:4:10
15:5:5:5
9:1:1:7
9:1:2:6
9:3:3:3
3:1:1:1
```

UE switching is stop-and-reattach, **not handover**. This is closed-loop traffic, not a fixed offered request rate. Preflight and active collector checks do not independently prove every UE's attachment or physical route. Check those after network/provisioning changes. Record failures, telemetry drops and mapping coverage; a fast failure can lower all-attempt p90. The response-time plots use **successful requests only**; SLO attainment uses all attempts. Resource CPU windows can overlap phase boundaries. One sequential baseline/proposed pair is exploratory; repeat campaigns before drawing a causal conclusion.

Locality is relative request-attempt demand: `site attempts / total attempts`. Its 40% score contribution is unchanged by assigning another replica until observed demand changes; resource headroom is recomputed after each proposed addition. This is a site-demand preference, not residual demand per replica. The CPU-only resource score uses 50% CPU and 50% RAM, with GPU and VRAM weights zero.

Descheduling uses `delta = best feasible target score after adding the Pod - current node score`, and acts only when the largest delta is positive. Node-name order breaks ties. This adapts the maximum-score-improvement selection described by Marchese and Tomarchio in [Enhancing the Kubernetes Platform with a Load-Aware Orchestration Strategy](https://doi.org/10.1007/s42979-025-03712-z); our resource/locality score and Deployment-based removal/replacement mechanism differ from their scheduler/descheduler implementation. Replicas of this homogeneous service on the same node have equal scores. The original default-scheduled Pod is retained unless another node offers a positive delta; it is not recreated merely to normalize ownership. If original replicas span multiple nodes, only the pinned groups are eligible for descheduling, because Deployment scale-down cannot select a particular original Pod. After removal, an unavailable destination is retried without clearing the durable replacement obligation.
