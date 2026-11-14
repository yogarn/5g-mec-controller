# Three-gNB experiment runbook

Run commands from the **repository root**, not from `experiments/`. This runbook separates the controller on the core host from the UE workload generator on the UE host. It does not start or transfer anything over SSH automatically.

This runbook covers the current gNB3-hotspot campaign. Verify the direct NodePort path independently from all three gNBs. The controller targets the elapsed-time p90 of all UE attempts, including failures and timeouts, from one telemetry collector. Fast failures can lower this p90; monitor failures separately. Keep mutation disabled until all UE paths, telemetry coverage, and policy are validated. Inspect the live Services before each run because allocated NodePorts can change.

The default schedule is three 30-second phases with one UE at each site in turn. It is a **connectivity smoke test**, not a justified research campaign. The runner stops and reattaches UEs between phases; it does not perform 5G handover. Closed-loop UE counts are not fixed offered request rates.

Never run the HPA and a mutating controller, KEDA, or any other replica writer at the same time. Approve a campaign and recovery plan first. Keep the same workload, image, input photo, routes, SLO, starting replicas, node constraints, UE schedule and phase duration across matched runs. Repeat runs and counterbalance method order. The runner does not automatically collect pod resources, controller events, or prove actual gNB attachment.

## When to run each section

| Frequency | Required action |
| --- | --- |
| First installation or when assets/configuration change | Sections 1–2: install dependencies; configure the workload, collector and Prometheus only if missing or changing; provision **new** UE subscribers only if necessary. Existing resources and reservations must not be recreated for each run. |
| Before every matched campaign | Section 3.1: check ownership and infrastructure, save the current state, clear only the experiment application's old pods and start one fresh ready application pod. |
| For each baseline run | Section 3.2: create the baseline HPA, run preflight and the three-site smoke test, reset to one fresh pod if needed, then start the sampler and measured UE workload. |
| Before each proposed run | Section 3.3: remove only the campaign HPA, clear the experiment application's pods, start one fresh ready pod, and validate the controller in non-mutating mode with active smoke traffic before the measured run. |
| After each proposed run | Stop controller and sampler, recover owned resources, restore the original affinity and replica count, and verify the resulting cluster state. |
| After each matched pair | Section 3.4: summarize, transfer and checksum raw data, produce plots and retain logs. Use new output directories for repeats. |

The collector Deployment and Service are in namespace `placement` (NodePort 31669), as is Prometheus (NodePort 30990). Verify them again before a run:

```sh
kubectl -n placement get deployment,service ue-telemetry
kubectl -n placement get deployment,service placement-prometheus
```

If they are healthy and already configured, **do not redeploy, rebuild or reprovision them**. Setup and repair require authorization and are not routine per-run steps.

## 1. Core host and collector setup — first installation or change

Verification commands in this section, including preflight, still apply before every campaign even when installation is skipped.

**On core**, with Kubernetes access, prepare the software without changing active experiments. Do not create, replace, restart, or reload shared resources until their owner authorizes a maintenance window.

After authorization, inspect the `services` namespace and project Deployment and Service. If both workload resources are absent, create them with `kubectl create -f apps/face-detection/manifest.yaml`; if either exists, inspect ownership and state rather than overwriting it. The desired replica count is 1; cold start can temporarily have zero ready pods. Inspect the app NodePort with `kubectl -n services get service mec-placement-face-detection` and choose a node IP reachable from each UE path. Requests use `http://NODE_IP:APP_NODEPORT/detect` through Kubernetes kube-proxy.

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
# Preserve local changes before updating an existing checkout.
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# The Python Kubernetes client does not automatically use k3s's default path.
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

# After the collector and Prometheus are configured as described below:
python -m experiments.preflight
kubectl -n services get deployment mec-placement-face-detection -o wide
kubectl -n services get hpa
kubectl -n services top pods -l app=mec-placement-face-detection
```

Preflight checks the workload, NodePort configuration and global collector telemetry; it does not check UPFs or prove reachability or telemetry coverage at any site. Verify all three UE paths separately. Idle is unverified only when the collector scrape is up, its window complete, and attempts are zero; scrape failures and overflow remain errors.

### Collector and Prometheus startup

The collector runs as **one Kubernetes pod**, not on each UE. Its image must be accessible to the Kubernetes node that runs the pod; UE hosts only need the Python workload code and token file. The collector and copied Prometheus Deployment select `site: core`. The current cluster's `core-5g` node has that label and is schedulable; recheck labels, taints and available resources before deploying. The registry avoids manually copying the image to the node.

The `collector/manifest.yaml` Service uses NodePort 31669 and the Deployment is pinned to an image digest. Build and push after collector code changes, then update the manifest to the new verified digest; building alone does not change the running Deployment:

```sh
docker build -f collector/Dockerfile -t 10.34.211.140:30500/ue-telemetry-collector:latest .
docker push 10.34.211.140:30500/ue-telemetry-collector:latest
```

If Docker is not configured for the testbed's HTTP registry, an operator may instead run `docker buildx build -f collector/Dockerfile --output type=image,name=10.34.211.140:30500/ue-telemetry-collector:latest,push=true,registry.insecure=true .` on the trusted testbed network. Publishing `latest` does not update a digest-pinned Deployment. Coordinate image changes with other users.

After authorization, copy the token securely to core if necessary. Set `TOKEN_FILE` to the protected file containing the same token used by the UE; do not put its contents in a command, manifest, or repository. If the Secret and collector resources do not already exist, create them:

```sh
TOKEN_FILE="$HOME/.config/5g-mec/ue-telemetry-token"
kubectl -n placement create secret generic ue-telemetry-token --from-file=token="$TOKEN_FILE"
kubectl create -f collector/manifest.yaml
kubectl -n placement rollout status deployment/ue-telemetry
kubectl -n placement get service ue-telemetry -o wide
```

If any resource already exists, stop and coordinate with its owner rather than overwriting it. Review `collector/manifest.yaml`, including its ConfigMap settings, before creation; the image uses `/etc/ue-telemetry/config.yaml`. Confirm NodePort 31669 is free before creating the Service and inspect it afterward. Choose a reachable node IP for `http://NODE_IP:COLLECTOR_NODEPORT/observations`. Protect the token file on both hosts; never commit it.

The shared monitoring manifests copied under `telemetry/` are **reference snapshots**, not authoritative copies of live resources; the Prometheus copy additionally selects `site: core`. Prometheus retains the other team's scrape jobs and recording rules. Do not apply these copies over existing installations.

If shared `placement/placement-prometheus` is already running, reuse it; do not start a second instance or restart it during another team's experiment. After authorization, an operator must back up and inspect the live `placement/placement-prometheus-config` ConfigMap, merge only the `ue-telemetry` job from `collector/prometheus-scrape.yaml` into its existing `scrape_configs`, validate the complete configuration, and use the owner's approved reload procedure. Do not replace other jobs or recording rules. The copied Prometheus manifest enables `--web.enable-lifecycle`, but verify the live Deployment before using `POST /-/reload` and wait for the updated ConfigMap to reach the pod. Reloading is a shared operational change.

The copied Grafana manifest provisions the existing placement dashboard and exposes NodePort 30031; it is optional for the collector and controller. It enables **anonymous Admin access and disables the login form**. Treat it as unsafe on untrusted networks: an authorized operator must review authentication and network restrictions before using it. Reuse the existing shared Grafana instead of replacing it during another team's experiments.

For a new, isolated installation only, review the copied namespace and monitoring manifests, reconcile their existing jobs, rules, RBAC and NodePorts with that environment, and create the required resources in namespace-first order. Merge the collector scrape job before starting Prometheus. Do not use this procedure to replace the shared instance at `10.34.211.140:30990`.

For example, after creating and validating a reviewed copy of `telemetry/03-prometheus.yaml` at `PROMETHEUS_MANIFEST` with the collector job merged, and confirming none of its resources exist:

```sh
kubectl create -f telemetry/00-namespace.yaml
kubectl create -f telemetry/01-node-exporter.yaml
# Only if this environment uses NVIDIA DCGM monitoring:
kubectl create -f telemetry/02-dcgm-exporter.yaml
kubectl create -f "$PROMETHEUS_MANIFEST"
kubectl -n placement rollout status deployment/placement-prometheus
kubectl -n placement get service placement-prometheus
```

The copied Prometheus manifest uses NodePort 30990; resolve any port collision in the reviewed copy before creation. These commands create shared cluster resources and must not be run during another team's experiment without authorization.

Supply both `--telemetry-url http://NODE_IP:COLLECTOR_NODEPORT/observations` and `--telemetry-token-file PATH` to the UE runner. Without both options, telemetry is disabled. Delivery and drop totals appear on stderr; compare them with JSONL attempts.

The rolling window uses collector arrival time, not request start time. Delivery loss, queue overflow, and collector restart skew coverage; counts are partial while `ue_request_window_complete` is zero, and both p90 gauges are omitted until the window is complete. `ue_request_attempt_duration_p90_seconds` is nearest-rank over all attempts, using their actual duration without penalties; `ue_request_duration_p90_seconds` remains successful-only. Failures are attempts minus successes. One replica is essential: per-replica p90 values cannot be aggregated into a global p90.

Each UE observation contains `ue_ip`, full-response `duration_ms`, `http_status` (null if no response status was received), and `success`. Success remains separate because reading a response can fail after a 2xx status. The collector converts milliseconds to seconds for its Prometheus p90 and SLO metrics. Update collector and UE code together.

Protect the collector network and token: payload IPs are client asserted and do not authenticate UEs or prove attachment. Before any live mutation, reverify NodePort reachability and telemetry source/payload at all three gNBs, the SMF-IP-to-AMF-gNB join, scrape health, delivery loss, and p90 against raw JSONL. Unknown or stale mappings contribute only to global metrics. Keep mutation disabled if any check fails.


## 2. UE host setup — first installation or change

**On the UE host** (`member@10.34.211.6`), log in separately. The former `10.34.7.38` host is retired. If the checkout does not exist, create its parent and clone it; `git clone` refuses an existing destination:

```sh
mkdir -p ~/skripsi_yoga
git clone https://github.com/yogarn/5g-mec-controller.git ~/skripsi_yoga/5g-mec-controller
cd ~/skripsi_yoga/5g-mec-controller
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If the checkout is a Git clone, inspect and preserve local changes before `git pull --ff-only`; a transferred copy without `.git` must be synchronized deliberately instead. Use the same revision on both hosts. Inspect private assets and the campaign inventory before starting:

```sh
ls -l ~/.config/5g-mec/ue-inventory.yaml
ls -l ~/.config/5g-mec/smoke.png
ls -l ~/5g-ai-edge/ueransim/build/nr-ue
```

On the current UE host, pass `/home/member/5g-ai-edge/ueransim/build/nr-ue` with `--nr-ue` and `~/.config/5g-mec/smoke.png` with `--image` for connectivity checks. The 64×64 synthetic image is not representative performance data. Protect the private inventory and expand it only using verified reservations; the campaign below requires 24 subscribers with all three site configurations.

For a representative workload, pass `--image /path/to/image-directory`. Supported image filenames are sorted and assigned by subscriber index modulo the image count, so images are reused when UEs outnumber them and a subscriber retains its image across phases. `run.json` records names, hashes and assignments; each request record identifies its image. Use the same directory and assignments in matched runs.

The eight selected images in `apps/face-detection/images` are from [Syn-Vis-v0](https://huggingface.co/datasets/retowyss/Syn-Vis-v0), Reto Wyss (2025), revision `100262732989e77f38cd831d70a376a93735006a`. Its dataset card dedicates individual images to CC0 and licenses the dataset compilation/documentation under CC-BY-SA 4.0; retain provenance and check the source terms. These 1024×1024 headshots are synthetic, female-presenting CGI portraits and are not representative of all real-world faces. The images are unchanged from the source; record their hashes and keep the image set fixed across compared runs.

If any path differs, set the corresponding `--ue-config`, `--image` or `--nr-ue` option. For first-time provisioning, use the detailed inventory schema and instructions in `README.md`:

1. Ensure Open5GS, three registered gNBs, UERANSIM, and three IPv4 DNNs with nonoverlapping pools are configured. Provisioning does not configure network infrastructure.
2. For each subscriber, create three private UE YAMLs with the same unique SUPI and authentication values, and site-specific gNB search list and DNN. Create an inventory with `gnb1`, `gnb2`, and `gnb3` config paths and unique reserved static IPs outside dynamic allocation; protect these files.
3. On a trusted host with MongoDB access, install requirements and set `OPEN5GS_MONGO_URI` securely if needed. Stop other provisioning writers. Check the plan, then explicitly insert only after review:

   ```sh
   python3 -m experiments.provision_ues --inventory /secure/ue-inventory.yaml --verbose
   python3 -m experiments.provision_ues --inventory /secure/ue-inventory.yaml --apply
   ```

The insert-only tool refuses existing IMSIs/IPs; **do not re-provision an existing pool**. Writes are not transactional: if insertion fails, inspect MongoDB and resolve partial inserts before retrying. Verify registration, assigned IP, routing and connectivity separately. Confirm no unrelated `nr-ue` processes are running and chosen subscribers are free.

## 3. Three-gNB campaign: tmux walkthrough

This is a **new** run, not a continuation of `gnb3-hotspot-20261114T0500Z`. The proposed condition calculates a replica target from all-attempt p90 / 0.5 s, then plans each extra replica in sequence against projected node allocations. The controller applies the resulting per-node counts as a batch; the cooldown is between decisions, not between pods. Migration remains surge-first and one replica at a time. Both methods have a maximum of 100 desired replicas; a migration can temporarily use one more. A cap is not a capacity guarantee: check CPU, memory, pod slots, node saturation and other workloads before starting and monitor them during the trial.

The same 24 reserved subscribers, routes, image set, node eligibility, 180-second phases, and starting replica count must be used for both methods. The runner stops and reattaches UEs between phases; this is not a handover experiment. Do not restart shared gNBs, UPFs, Prometheus or other teams' services without their approval. Check that the 24 reserved UEs have their static IP and DNN/UPF anchor for each of gNB1, gNB-2 and gNB-3 and are not in use; do not reprovision existing subscribers. Verify all three paths and telemetry association during an active smoke test before starting. If a check fails, stop; do not assume an idle or incomplete telemetry window is valid.

### 3.1 Prepare both machines — before each matched campaign

On **both core and the UE host**, make sure this updated checkout is present. The core and UE host may have uncommitted local changes: do not blindly overwrite them or use `git pull` until you have checked and preserved them. Verify the controller has batch-scale-up support and `config.yaml` and `experiments/hpa-baseline.yaml` both specify 100. The UE host needs the experiment runner and private inventory; core needs the controller, sampler, Kubernetes access and Prometheus. Install the existing project requirements on each host. For plots, install `matplotlib` in the core virtual environment if not already installed.

Open two persistent sessions on **core** and one on **UE host**:

```sh
# On core, in two terminals:
tmux new -s mec-control
tmux new -s mec-metrics
# On UE host:
tmux new -s mec-ue
```

Detach with `Ctrl-b d`; reattach with `tmux attach -t mec-control` (or the other session name). Commands below are entered in the named session, not inside another running command. Use the same new campaign name on both hosts:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
export CAMPAIGN=gnb3-100-01
```

Choose a unique name; result directories must not already exist. In **core `mec-control`**, save state and inspect it:

```sh
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
mkdir -p results
mkdir "results/$CAMPAIGN"
kubectl -n services get deployment mec-placement-face-detection -o yaml > "results/$CAMPAIGN/deployment-before.yaml"
kubectl -n services get hpa -o yaml > "results/$CAMPAIGN/hpas-before.yaml"
kubectl -n services get pods -l app=mec-placement-face-detection -o wide > "results/$CAMPAIGN/pods-before.txt"
kubectl -n services get svc,deploy mec-placement-face-detection
kubectl -n placement get svc,deploy ue-telemetry
kubectl get nodes
kubectl top nodes
kubectl describe nodes gnb1 gnb-2 gnb-3
```

Confirm the collector is ready and no workload HPA, controller or other replica writer is active. Identify any controller-owned groups and migration annotation; if present, stop and follow ownership-aware recovery below before proceeding. Inspect the saved Deployment's original affinity; the following temporary constraint is appropriate **only if it has no existing affinity**. Otherwise stop and design an equivalent constraint with the owner; do not overwrite existing affinity.

Clear only this experiment's application pods by scaling its Deployment to zero. This temporarily makes the application unavailable. Do **not** delete the collector, gNB, UPF, core or other teams' pods. If the HPA or another writer is active, stop here until its owner stops it; otherwise it can recreate pods during cleanup.

```sh
kubectl -n services scale deployment/mec-placement-face-detection --replicas=0
kubectl -n services wait --for=delete pod -l app=mec-placement-face-detection --timeout=180s
kubectl -n services get pods -l app=mec-placement-face-detection -o wide
```

Confirm that no experiment application pods remain. Apply the common node constraint while the application is stopped, then start exactly one fresh application pod:

```sh
kubectl -n services patch deployment mec-placement-face-detection --type=merge -p \
  '{"spec":{"template":{"spec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["gnb1","gnb-2","gnb-3"]}]}]}}}}}}}'
```

**Stop if the patch reports an error.** Confirm the required hostname affinity is present before starting the pod; otherwise the baseline and proposed node eligibility will differ.

```sh
kubectl -n services get deployment mec-placement-face-detection -o jsonpath='{.spec.template.spec.affinity}{"\n"}'
kubectl -n services scale deployment/mec-placement-face-detection --replicas=1
kubectl -n services rollout status deployment/mec-placement-face-detection --timeout=180s
kubectl -n services get deployment mec-placement-face-detection -o yaml > "results/$CAMPAIGN/deployment-matched.yaml"
```

Wait until exactly one new application pod is running and ready. The Deployment creates the pod; the HPA introduced next changes its desired replica count but does not create the Deployment. Keep this same constraint for both conditions. Verify live Services still use app NodePort 30998 and collector NodePort 31669; substitute verified addresses if they changed. Verify the collector token file on the UE host and do not print its contents.


### 3.2 Baseline HPA — each baseline run

In **core `mec-control`**, after confirming no other writer:

```sh
kubectl create -f experiments/hpa-baseline.yaml
kubectl -n services get hpa mec-placement-face-detection-baseline -o yaml > "results/$CAMPAIGN/hpa-created.yaml"
mkdir "results/$CAMPAIGN/baseline"
```
With the new application pod ready, the HPA active, and the collector available, run the read-only preflight on **core**:

```sh
.venv/bin/python -m experiments.preflight
```

With no UE traffic, an idle collector is reported as **unverified**, not as validated latency. The next smoke test starts temporary UEs; run preflight again while its traffic is active. It is not the measured experiment.

For a three-site connectivity smoke test, run this on the **UE host** after confirming the reserved subscriber is free and the paths, token and ports are correct:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
sudo .venv/bin/python -u -m experiments.experiment --armed \
  --url http://10.34.211.140:30998/detect \
  --image /home/member/.config/5g-mec/smoke.png \
  --ue-config .private/ue-paper-24/inventory.yaml \
  --nr-ue /home/member/5g-ai-edge/ueransim/build/nr-ue \
  --phase-schedule '1:1:0:0,1:0:1:0,1:0:0:1' --phase-duration 30 \
  --telemetry-url http://10.34.211.140:31669/observations \
  --telemetry-token-file /home/member/.config/5g-mec/ue-telemetry-token \
  --output-dir "results/$CAMPAIGN/smoke"
```

While the smoke test is sending traffic, run this on **core `mec-control`**:

```sh
.venv/bin/python -m experiments.preflight
```

Separately verify each site's UE IP, session, DNN, UPF anchor, route and telemetry association. The smoke reuses one subscriber at each site in turn; it does not validate every reserved subscriber or all three sites simultaneously. Verify the full pool independently. Wait for cleanup before starting the campaign.

The smoke test can cause the HPA to scale, so reset its state and start the **measured baseline** from one fresh pod. On **core**, after smoke cleanup and after confirming this is the campaign-owned HPA:

```sh
kubectl -n services get hpa mec-placement-face-detection-baseline -o yaml > "results/$CAMPAIGN/hpa-smoke-after.yaml"
kubectl -n services delete hpa mec-placement-face-detection-baseline
kubectl -n services scale deployment/mec-placement-face-detection --replicas=0
kubectl -n services wait --for=delete pod -l app=mec-placement-face-detection --timeout=180s
kubectl -n services scale deployment/mec-placement-face-detection --replicas=1
kubectl -n services rollout status deployment/mec-placement-face-detection --timeout=180s
kubectl -n services get pods -l app=mec-placement-face-detection -o wide
kubectl create -f experiments/hpa-baseline.yaml
kubectl -n services get hpa mec-placement-face-detection-baseline
```

Confirm exactly one fresh pod is ready before starting the measured UE workload. If the HPA has already scaled above one, stop and reset again; never scale while another replica writer is active.


The baseline targets 80% of CPU request, with 1–100 replicas and a 180-second scale-down window. For a 200m request, the target is 160m per pod, not 80% of the CPU limit. Confirm Metrics Server reports valid data and all pods have CPU requests; the threshold is not a calibrated latency SLO.

In **core `mec-metrics`**, set the same campaign name and start the sampler. Leave it running until the UE command has finished, then stop it with `Ctrl-C`:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
export CAMPAIGN=gnb3-100-01
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
.venv/bin/python -u -m experiments.monitor_resources sample "results/$CAMPAIGN/baseline/metrics.jsonl"
```

In **UE `mec-ue`** run the approved schedule. `--armed` starts it without an interactive confirmation. The command creates the output directory and cleans up its own UE processes on completion; do not reuse a result directory.

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
export CAMPAIGN=gnb3-100-01
set -o pipefail
export SCHEDULE='3:1:1:1,9:3:3:3,9:1:2:6,9:1:1:7,15:5:5:5,15:1:4:10,15:1:1:13,24:8:8:8,24:1:7:16,24:1:1:22'
sudo .venv/bin/python -u -m experiments.experiment --armed \
  --url http://10.34.211.140:30998/detect \
  --image apps/face-detection/images \
  --ue-config .private/ue-paper-24/inventory.yaml \
  --nr-ue /home/member/5g-ai-edge/ueransim/build/nr-ue \
  --phase-schedule "$SCHEDULE" --phase-duration 180 \
  --telemetry-url http://10.34.211.140:31669/observations \
  --telemetry-token-file /home/member/.config/5g-mec/ue-telemetry-token \
  --output-dir "results/$CAMPAIGN/baseline" 2>&1 | tee "baseline-$CAMPAIGN.log"
```

After all ten phases finish and UE cleanup completes, stop the **baseline sampler** in **core `mec-metrics`** by pressing `Ctrl-C`. Wait until the shell prompt returns; do not leave the baseline sampler running into the proposed run. Verify no experiment-owned `nr-ue` processes remain. Preserve the raw JSONL, run metadata, UE lifecycle journal, telemetry delivery/drop counts, sampler and console log.

### 3.3 Reset and proposed condition — before and after each proposed run

In **core `mec-control`**:

Before deleting the HPA, confirm it is still the experiment-owned instance and has not been replaced or taken over. Stop if ownership is unclear. Before scaling, stop all writers and confirm there are no controller-owned placement groups or migration annotation; if present, follow the ownership-aware recovery below instead.

```sh
kubectl -n services get hpa mec-placement-face-detection-baseline -o yaml > "results/$CAMPAIGN/hpa-after.yaml"
kubectl -n services delete hpa mec-placement-face-detection-baseline
kubectl -n services get hpa
kubectl -n services scale deployment/mec-placement-face-detection --replicas=0
kubectl -n services wait --for=delete pod -l app=mec-placement-face-detection --timeout=180s
kubectl -n services scale deployment/mec-placement-face-detection --replicas=1
kubectl -n services rollout status deployment/mec-placement-face-detection --timeout=180s
kubectl -n services get pods -l app=mec-placement-face-detection -o wide
```

Confirm the old application pods have terminated and the single newly created pod is ready. The temporary matched node constraint must still be present; there must be no HPA, other replica writer, controller-owned group or migration annotation.

Create a campaign-specific controller config from the validated `config.yaml`; retain `max_replicas: 100`, `interval_seconds: 10`, `mode: mobility-aware`, and the verified metrics and eligible nodes. Set an absolute path to `apps/face-detection/manifest.yaml`, because relative paths are resolved relative to the config file. First use `mutate: false` and run `--once` while the three-site smoke workload is active; an idle telemetry window cannot validate p90. Check the target, per-node metrics and logs. Only after checks pass and the trial is authorized, set `mutate: true` in this campaign copy; keep the checked-in default `config.yaml` non-mutating.

```sh
.venv/bin/python - <<'PY'
import os
from pathlib import Path
import yaml
root = Path.cwd()
path = root / "results" / os.environ["CAMPAIGN"] / "controller.yaml"
data = yaml.safe_load((root / "config.yaml").read_text())
data["manifest"] = str(root / "apps/face-detection/manifest.yaml")
data["mutate"] = False
with path.open("x") as stream:
    yaml.safe_dump(data, stream, sort_keys=False)
PY
```

On **UE `mec-ue`**, start a fresh proposed-condition smoke test:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
export CAMPAIGN=gnb3-100-01
sudo .venv/bin/python -u -m experiments.experiment --armed \
  --url http://10.34.211.140:30998/detect \
  --image /home/member/.config/5g-mec/smoke.png \
  --ue-config .private/ue-paper-24/inventory.yaml \
  --nr-ue /home/member/5g-ai-edge/ueransim/build/nr-ue \
  --phase-schedule '1:1:0:0,1:0:1:0,1:0:0:1' --phase-duration 30 \
  --telemetry-url http://10.34.211.140:31669/observations \
  --telemetry-token-file /home/member/.config/5g-mec/ue-telemetry-token \
  --output-dir "results/$CAMPAIGN/smoke-proposed"
```

While that smoke test is **actively sending traffic**, run the non-mutating controller observation on **core `mec-control`**:

```sh
.venv/bin/python -m controller --config "results/$CAMPAIGN/controller.yaml" --once
```

Check its output and wait for the smoke test and UE cleanup to finish. Only after validation and authorization, change `mutate` to `true` in the campaign copy:

```sh
nano "results/$CAMPAIGN/controller.yaml"
```

In **core `mec-control`**, create the output directory and run the controller in the foreground, retaining its log:

```sh
mkdir "results/$CAMPAIGN/proposed"
set -o pipefail
.venv/bin/python -u -m controller --config "results/$CAMPAIGN/controller.yaml" 2>&1 | tee "results/$CAMPAIGN/proposed/controller.log"
```

In **core `mec-metrics`**, confirm the baseline sampler has been stopped and the shell prompt is available. Start a **separate** sampler for the proposed run:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
export CAMPAIGN=gnb3-100-01
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
.venv/bin/python -u -m experiments.monitor_resources sample "results/$CAMPAIGN/proposed/metrics.jsonl"
```

In **UE `mec-ue`**, start the measured proposed run:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
export CAMPAIGN=gnb3-100-01
set -o pipefail
export SCHEDULE='3:1:1:1,9:3:3:3,9:1:2:6,9:1:1:7,15:5:5:5,15:1:4:10,15:1:1:13,24:8:8:8,24:1:7:16,24:1:1:22'
sudo .venv/bin/python -u -m experiments.experiment --armed \
  --url http://10.34.211.140:30998/detect \
  --image apps/face-detection/images \
  --ue-config .private/ue-paper-24/inventory.yaml \
  --nr-ue /home/member/5g-ai-edge/ueransim/build/nr-ue \
  --phase-schedule "$SCHEDULE" --phase-duration 180 \
  --telemetry-url http://10.34.211.140:31669/observations \
  --telemetry-token-file /home/member/.config/5g-mec/ue-telemetry-token \
  --output-dir "results/$CAMPAIGN/proposed" 2>&1 | tee "proposed-$CAMPAIGN.log"
```

Monitor controller errors, pending pods, node eligibility, resource pressure and telemetry while the run proceeds. A temporarily missing metric can make the controller fail closed; record it, do not silently replace data.

After all phases and UE cleanup finish, stop the controller and sampler with `Ctrl-C` in their own tmux sessions and verify their processes have exited. Save the original Deployment, its owned groups, pods, endpoints and events. Follow the ownership-aware recovery below before restoring the original Deployment to one ready replica. Remove the temporary affinity **only if it was added by this campaign and the saved original had none**:

```sh
kubectl -n services patch deployment mec-placement-face-detection --type=json \
  -p='[{"op":"remove","path":"/spec/template/spec/affinity"}]'
kubectl -n services rollout status deployment/mec-placement-face-detection --timeout=180s
kubectl -n services get hpa
kubectl -n services get pods -l app=mec-placement-face-detection -o wide
```

Confirm exactly one ready app pod remains, no temporary HPA, owned groups or migration annotation remain, and the saved original affinity is restored. Do not delete unrelated resources or globally kill UE processes.

### Ownership-aware recovery

Stop all replica writers and capture the original Deployment, managed child Deployments, pods and Service endpoints. Identify children by the original Deployment UID (`mec-placement-controller.io/owner=<UID>`) and verify their owner references. If the original has a `mec-placement-controller.io/migration` annotation, inspect its kind and recorded replica counts; a batch scale-up records per-node targets, while a migration records a source and destination. Stop if ownership or recovery state is unclear.

Scale the original up one replica at a time; wait for readiness and healthy Service endpoints before scaling a verified owned child down by one and waiting for termination. Do not exceed the configured maximum plus one temporary replacement. Repeat until the approved desired replicas are on the original. Once healthy, remove only verified zero-replica owned groups and clear the migration annotation. Confirm neither remains before enabling the HPA or scaling-only mode. Compare the Deployment with the saved original and restore only approved changed fields; do not blindly reapply snapshots.

If interrupted UE cleanup did not finish, preserve the run directory and inspect its journal. After confirming ownership, recover only that run on the UE host:

```sh
sudo .venv/bin/python -m experiments.ue_lifecycle --recover "results/$CAMPAIGN/baseline/owned_ues"
```

Use the actual affected method's journal. Recovery fails closed on ambiguous ownership; never globally kill `nr-ue` processes or delete interfaces as a substitute.

### 3.4 Summarize, transfer and plot — after each matched pair

On the **UE host**, summarize each completed run once:

```sh
for method in baseline proposed; do
  sudo .venv/bin/python -m experiments.summarize "results/$CAMPAIGN/$method/requests.jsonl" --slo-ms 500
  sudo sha256sum "results/$CAMPAIGN/$method/requests.jsonl"
done
```

Transfer each run's `requests.jsonl`, `summary.csv`, remaining summaries, `run.json`, lifecycle journal and console log to the corresponding core campaign directory without overwriting sampler files. Check the `requests.jsonl` SHA-256 on core against the UE host before proceeding; retain the originals. On **core**:

For example, from the **UE host**, with SSH access to `core` and the same `CAMPAIGN` on both machines:

```sh
for method in baseline proposed; do
  sudo tar -C "results/$CAMPAIGN/$method" -czf - \
    requests.jsonl summary.csv summary.json summary-ues.csv summary-ues.json run.json owned_ues |
    ssh core "tar -xzf - -C /home/member/skripsi_yoga/5g-mec-controller/results/$CAMPAIGN/$method"
  scp "$method-$CAMPAIGN.log" "core:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMPAIGN/$method/runner.log"
done
```

The destination run directories must already exist and must not contain files with those names. `tar` will overwrite matching names; stop if they already exist. Transfer over a trusted channel because the lifecycle journal contains private subscriber information. Verify checksums after transfer.

```sh
for method in baseline proposed; do
  D="results/$CAMPAIGN/$method"
  sha256sum "$D/requests.jsonl"
  .venv/bin/python -m experiments.monitor_resources summarize "$D/metrics.jsonl" "$D/requests.jsonl" "$D/summary.csv" "$D"
  .venv/bin/python -m experiments.telemetry_history "$D/requests.jsonl" "$D"
done
.venv/bin/python -m experiments.plot_autoscaler "results/$CAMPAIGN" "results/$CAMPAIGN/plots"
```

Retain both methods' raw files, configuration, snapshots, controller/HPA logs, phase coverage and generated PNG/PDF plots. Resource CPU windows can overlap phases. Successful-request p90 is not the same as the controller's all-attempt p90 or the share of all attempts meeting the 500 ms SLO. Report failures and telemetry drops separately, and do not treat one sequential pair as a causal or repeatability claim.
