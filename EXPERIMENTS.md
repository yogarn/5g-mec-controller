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

The baseline uses Kubernetes' **default scheduler** with the same required eligibility (`gnb1`, `gnb-2`, `gnb-3`) as proposed. Both methods use the same digest-pinned app image, photo set, 500m CPU / 256 MiB requests and 2 CPU / 2 GiB per-pod limits, 100-replica ceiling and starting replica count of one. The HPA's 80% CPU target is based on the 500m request (400m per pod), not the CPU limit. The scheduler and proposed placement policy also use the larger CPU request when checking node capacity; this can reduce the number of pods that fit. A temporary migration replacement can exceed the desired ceiling by one. The application NodePort is discovered automatically; the UE command receives the actual allocated port. Requests remain lower than limits, so monitor node load and CPU throttling.

The controller computes `ceil(current replicas × all-attempt p90 / 0.5 seconds)`, within its configured bounds. It plans added replicas individually against projected capacity and applies the resulting per-node counts without a per-step cooldown. Migration is one replacement at a time. Missing required metrics make reconciliation fail closed.

Each phase generates traffic for **180 seconds**. The schedule is `total:gnb1:gnb2:gnb3`:

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
```

UE switching is stop-and-reattach, **not handover**. This is closed-loop traffic, not a fixed offered request rate. Preflight and active collector checks do not independently prove every UE's attachment or physical route. Check those after network/provisioning changes. Record failures, telemetry drops and mapping coverage; a fast failure can lower all-attempt p90. The response-time plots use **successful requests only**; SLO attainment uses all attempts. Resource CPU windows can overlap phase boundaries. One sequential baseline/proposed pair is exploratory; repeat campaigns before drawing a causal conclusion.
