# Two-gNB experiment runbook

Run commands from the **repository root**, not from `experiments/`. This runbook separates the controller on the core host from the UE workload generator on the UE host. It does not start or transfer anything over SSH automatically.

## Stop: verify readiness first

The last inspection found gNB-2 Envoy routing to the face-detection service, but gNB-3 Envoy routing elsewhere. The configured controller latency query covers only gNB-2. The live Deployment was pinned to gNB-2. Treat those observations as historical and check current state; do not run a two-site comparison until both UE paths, complete service-wide latency telemetry, and placement constraints are verified. The 500 ms SLO is exploratory. The face image in the Deployment predates the local source changes; verify the deployed image and workload identity before comparing runs.

The default schedule is two 30-second phases with one UE per site. It is a **connectivity smoke test**, not a justified research campaign. The runner stops and reattaches UEs between phases; it does not perform 5G handover. Closed-loop UE counts are not fixed offered request rates.

Never run the HPA and a mutating controller, KEDA, or any other replica writer at the same time. Approve a campaign and recovery plan first. Keep the same workload, image, input photo, routes, SLO, starting replicas, node constraints, UE schedule and phase duration across matched runs. Repeat runs and counterbalance method order. The runner does not automatically collect pod resources, controller events, or prove actual gNB attachment.

## 1. Core host setup and preflight

**On core**, with Kubernetes access:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
git pull --ff-only
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Check each UPF. An interactive mismatch may offer a rollout; answer N
# unless the UPF interruption and route replacement are authorized.
python -m experiments.preflight
python -m experiments.preflight --node gnb-3

kubectl -n open5gs get deployment mec-placement-face-detection -o wide
kubectl -n open5gs get hpa
kubectl -n open5gs top pods -l app=mec-placement-face-detection
```

The preflight defaults to gNB-2; `--node gnb-3` checks the other UPF. Both currently use the configured latency query, so a successful gNB-3 preflight alone does not prove gNB-3 telemetry. `--json` gives the detailed report and disables interactive prompts. A NaN p95 with a confirmed zero request count in the last two minutes is an idle warning, not a preflight failure; other telemetry errors still fail. The controller waits for a finite p95 from real requests before making latency-based decisions. No dummy traffic or synthetic zero latency is needed, but an idle warning does not verify the request path.

If the proxy configuration differs, preflight can offer to back up the current ConfigMap under `results/` and restart the shared UPF. **Restarting interrupts 5G traffic and applying the configuration replaces existing proxy routes.** Only confirm after reviewing the impact and recovery plan. A matching config is not a reason to restart.

Before the first run, capture the current Deployment and HPA state. Use a new directory for each campaign; if `mkdir` fails, stop and choose a different name:

```sh
mkdir -p results
mkdir results/campaign-01
kubectl -n open5gs get deployment mec-placement-face-detection -o yaml \
  > results/campaign-01/deployment-before.yaml
kubectl -n open5gs get hpa -o yaml > results/campaign-01/hpas-before.yaml
kubectl -n open5gs get pods -l app=mec-placement-face-detection -o wide \
  > results/campaign-01/pods-before.txt
```

Review existing HPAs, controller processes, Deployment replicas, node selectors, affinity, and annotations. Save the actual desired initial state and identify who owns each resource before changing anything. The controller's HPA check does not prove another writer is absent.

## 2. UE host setup

**On the intended UE host**, log in separately. If the checkout does not exist, create its parent and clone it; `git clone` refuses an existing destination:

```sh
mkdir -p ~/skripsi_yoga
git clone https://github.com/yogarn/5g-mec-controller.git ~/skripsi_yoga/5g-mec-controller
cd ~/skripsi_yoga/5g-mec-controller
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If the checkout already exists, `cd` into it and use `git pull --ff-only` instead of cloning. Use the same revision on both hosts. Install UERANSIM and prepare the existing subscriber pool and a private inventory; the expected default paths are:

```sh
ls -l ~/.config/5g-mec/ue-inventory.yaml
ls -l ~/.config/5g-mec/face.jpg
ls -l ~/ueransim/build/nr-ue
```

If any path differs, set the corresponding `--ue-config`, `--image` or `--nr-ue` option. The inventory schema and first-time subscriber provisioning instructions are in `README.md`. The insert-only provisioning tool refuses existing IMSIs/IPs; **do not re-provision an existing pool**. Confirm the UE host has no unrelated running `nr-ue` processes, the chosen subscribers are free, and both site's registered sessions and packet paths work before load testing.

## 3. Baseline: Kubernetes HPA

**On core**, stop the placement controller and all other replica writers. Confirm the HPA list has no HPA targeting the workload. The following creates the experiment's HPA; it fails if an HPA with that name already exists:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
kubectl -n open5gs get hpa
kubectl create -f experiments/hpa-baseline.yaml
kubectl -n open5gs describe hpa mec-placement-face-detection-baseline
```

The baseline targets 80% of CPU **request**, has 1–3 replicas, and uses a 180-second scale-down window. With a 100m CPU request, 80% is 80m per pod, not 80% of the 500m CPU limit. Confirm Metrics Server reports valid data and all pods have CPU requests. This threshold is an experimental choice, not a calibrated latency SLO.

**On the UE host**, from the checkout root, run an initial smoke test only after both paths are ready:

```sh
cd ~/skripsi_yoga/5g-mec-controller
sudo .venv/bin/python -m experiments.experiment --output-dir results/hpa-smoke-01
```

The command previews the defaults and asks for confirmation. `--armed` is the explicit non-interactive alternative. The output directory must not exist. For a research run, specify an approved, pilot-derived schedule and duration; do not substitute the smoke test. Use the same options for HPA and controller. Example syntax (illustrative only):

```sh
sudo .venv/bin/python -m experiments.experiment \
  --phase-schedule '2:2:0,4:3:1,4:1:3,2:0:2' \
  --phase-duration 180 --output-dir results/hpa-run-01
```

Capture HPA status, pod placement, resource use and events during the run. The runner does not collect them. Retain its complete `results/hpa-run-01/` directory, including failed attempts and UE lifecycle journal.

## 4. Reset before switching methods

**On core**, first stop the UE run and wait for its cleanup. Stop all replica writers. Confirm the baseline HPA is still the one created for this experiment, then capture its state and remove **only that HPA**:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
kubectl -n open5gs get hpa mec-placement-face-detection-baseline -o yaml \
  > results/campaign-01/hpa-after.yaml
kubectl delete -f experiments/hpa-baseline.yaml
kubectl -n open5gs get hpa
```

If the approved starting state is one replica and no other writer is running:

```sh
kubectl -n open5gs scale deployment mec-placement-face-detection --replicas=1
kubectl -n open5gs rollout status deployment/mec-placement-face-detection
kubectl -n open5gs get deployment mec-placement-face-detection -o yaml \
  > results/campaign-01/deployment-after-reset.yaml
```

Compare `deployment-after-reset.yaml` with `deployment-before.yaml` and restore only approved changed fields. Scaling does **not** restore affinity, annotations, selectors, routes or the prior replica count automatically. In particular, stopping the mobility-aware controller or changing it to scaling-only does not remove its recorded preference. Do not blindly reapply an old Deployment snapshot or delete another experiment's resources. If ownership or recovery is unclear, stop and inspect before proceeding.

## 5. Proposed controller condition

**On core**, ensure the HPA is gone and all other replica writers are stopped. Edit the controller settings:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
source .venv/bin/activate
nano config.yaml
```

Set a verified service-wide `latency_query`, SLO and bounds. `mutate: false` is observation-only; set `mutate: true` only after authorization and validation. `mode: scaling-only` compares latency-ratio scaling with HPA; it does not test mobility-aware placement. `mode: mobility-aware` additionally requires verified `eligible_nodes`, `cpu_utilization_query`, and `mobility_query`, with one correctly labeled series per node. The provided configuration is not ready for that mode.

Check one observation **while `mutate: false`**:

```sh
python -m controller --once
```

Once ready, set `mutate: true` and run in a dedicated core terminal:

```sh
python -m controller
```

With `mutate: true`, even `--once` can change replicas. The controller needs finite recent latency data and otherwise fails closed. Its in-memory cooldown and stabilization state reset on restart.

**On the UE host**, run the identical approved workload using a new output directory:

```sh
cd ~/skripsi_yoga/5g-mec-controller
sudo .venv/bin/python -m experiments.experiment \
  --phase-schedule '2:2:0,4:3:1,4:1:3,2:0:2' \
  --phase-duration 180 --output-dir results/controller-run-01
```

The shown schedule is the same illustrative example, not a recommended research design. Capture controller logs, pod placement, events and resources separately. Stop the controller with Ctrl+C in its terminal when finished. With every writer stopped, repeat the reset checks in section 4 and restore approved initial state before the next condition.

## 6. Summarize and retain results

**On the UE host**, from the repository root:

```sh
sudo .venv/bin/python -m experiments.summarize results/hpa-run-01/requests.jsonl --slo-ms 500
sudo .venv/bin/python -m experiments.summarize results/controller-run-01/requests.jsonl --slo-ms 500
```

The output directories are created by the root-run experiment, so `sudo` is needed to read and write them unless permissions are deliberately changed. Each command exclusively creates `summary.json`, `summary.csv`, `summary-ues.json`, and `summary-ues.csv` next to the input. It refuses to overwrite existing summaries. Latency percentiles cover successful requests; SLO attainment includes **all attempts** in the denominator. Throughput includes phase drain time. There is no plotter or automatic transfer to core; retain and transfer raw results and logs using your approved process, then verify checksums before removing anything.

If interrupted cleanup did not finish, preserve the run directory and inspect its journal. After confirming ownership, recover only that run on the UE host:

```sh
sudo .venv/bin/python -m experiments.ue_lifecycle --recover results/hpa-run-01/owned_ues
```

Use the matching run's actual directory. Recovery fails closed on ambiguous ownership; never globally kill `nr-ue` processes or delete interfaces as a substitute.
