# Two-gNB experiment runbook

Run commands from the **repository root**, not from `experiments/`. This runbook separates the controller on the core host from the UE workload generator on the UE host. It does not start or transfer anything over SSH automatically.

## Stop: verify readiness first

Verify the direct NodePort path independently from both gNBs. The controller targets the actual elapsed-time p90 of all UE attempts, including failures and timeouts, from the single telemetry collector. Fast failures can lower this p90; monitor failures separately. Keep mutation disabled until both UE paths, telemetry coverage, and policy are validated.

The current testbed's core, gNB-2 and gNB-3 UPFs were rolled out without their Envoy sidecars and 8000/30090 redirect rules. The old Envoy ConfigMaps and Prometheus scrape job remain as unused references; applying older UPF manifests can restore the sidecars and redirects. Recovery snapshots from the smoke test are under `/home/member/5g-mec-telemetry-recovery/smoke-39TIpOuh` on core. The currently allocated application and collector NodePorts are 30998 and 31669 respectively; inspect the live Services before each run because allocated ports can change.

The default schedule is two 30-second phases with one UE per site. It is a **connectivity smoke test**, not a justified research campaign. The runner stops and reattaches UEs between phases; it does not perform 5G handover. Closed-loop UE counts are not fixed offered request rates.

Never run the HPA and a mutating controller, KEDA, or any other replica writer at the same time. Approve a campaign and recovery plan first. Keep the same workload, image, input photo, routes, SLO, starting replicas, node constraints, UE schedule and phase duration across matched runs. Repeat runs and counterbalance method order. The runner does not automatically collect pod resources, controller events, or prove actual gNB attachment.

## 1. Core host setup

**On core**, with Kubernetes access, prepare the software without changing active experiments. Do not create, replace, restart, or reload shared resources until their owner authorizes a maintenance window.

After authorization, inspect the `services` namespace and project Deployment and Service. If both workload resources are absent, create them with `kubectl create -f apps/face-detection/manifest.yaml`; if either exists, inspect ownership and state rather than overwriting it. The desired replica count is 1; cold start can temporarily have zero ready pods. Inspect the app NodePort with `kubectl -n services get service mec-placement-face-detection` and choose a node IP reachable from each UE path. Requests use `http://NODE_IP:APP_NODEPORT/detect` through Kubernetes kube-proxy.

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
git pull --ff-only
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

Preflight checks the workload, NodePort configuration and global collector telemetry; it does not check UPFs or prove reachability or telemetry coverage at either site. Verify both UE paths separately. Idle is unverified only when the collector scrape is up, its window complete, and attempts are zero; scrape failures and overflow remain errors.

## Collector and Prometheus startup

The collector runs as **one Kubernetes pod**, not on each UE. Its image must be accessible to the Kubernetes node that runs the pod; UE hosts only need the Python workload code and token file. The collector and copied Prometheus Deployment select `site: core`. The current cluster's `core-5g` node has that label and is schedulable; recheck labels, taints and available resources before deploying. The registry avoids manually copying the image to the node.

The `collector/manifest.yaml` Service already has `type: NodePort`. Kubernetes allocates the port; no fixed port is assumed. The same Service also has a ClusterIP for Prometheus. Build and push after collector code changes:

```sh
docker build -f collector/Dockerfile -t 10.34.211.140:30500/ue-telemetry-collector:latest .
docker push 10.34.211.140:30500/ue-telemetry-collector:latest
```

If Docker is not configured for the testbed's HTTP registry, an operator may instead run `docker buildx build -f collector/Dockerfile --output type=image,name=10.34.211.140:30500/ue-telemetry-collector:latest,push=true,registry.insecure=true .` on the trusted testbed network. Publishing `latest` does not update a running pod; it may affect a future pull or restart. Coordinate image changes with other users.

After authorization, copy the token securely to core if necessary. Set `TOKEN_FILE` to the protected file containing the same token used by the UE; do not put its contents in a command, manifest, or repository. If the Secret and collector resources do not already exist, create them:

```sh
TOKEN_FILE="$HOME/.config/5g-mec/ue-telemetry-token"
kubectl -n services create secret generic ue-telemetry-token --from-file=token="$TOKEN_FILE"
kubectl create -f collector/manifest.yaml
kubectl -n services rollout status deployment/ue-telemetry
kubectl -n services get service ue-telemetry -o wide
```

If any resource already exists, stop and coordinate with its owner rather than overwriting it. Review `collector/manifest.yaml`, including its ConfigMap settings, before creation; the image uses `/etc/ue-telemetry/config.yaml`. Inspect the allocated `nodePort` in the Service output. Choose a reachable node IP for `http://NODE_IP:COLLECTOR_NODEPORT/observations`. Protect the token file on both hosts; never commit it.

The original shared monitoring manifests are copied under `telemetry/`: namespace, node-exporter, DCGM exporter, Prometheus, and Grafana. They are **reference snapshots**, not authoritative copies of live shared resources; the Prometheus copy additionally selects `site: core`. Prometheus retains the other team's scrape jobs and recording rules. Do not apply these copies over existing installations. vLLM and UPF/Envoy manifests are not required for this face-detection telemetry path.

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

Supply both `--telemetry-url http://NODE_IP:COLLECTOR_NODEPORT/observations` and `--telemetry-token-file PATH` to either UE runner. Without both options, telemetry is disabled. Delivery and drop totals appear on stderr; compare them with JSONL attempts.

The rolling window uses collector arrival time, not request start time. Delivery loss, queue overflow, and collector restart skew coverage; counts are partial while `ue_request_window_complete` is zero, and both p90 gauges are omitted until the window is complete. `ue_request_attempt_duration_p90_seconds` is nearest-rank over all attempts, using their actual duration without penalties; `ue_request_duration_p90_seconds` remains successful-only. Failures are attempts minus successes. One replica is essential: per-replica p90 values cannot be aggregated into a global p90.

Each UE observation contains `ue_ip`, full-response `duration_ms`, `http_status` (null if no response status was received), and `success`. Success remains separate because reading a response can fail after a 2xx status. The collector converts milliseconds to seconds for its existing Prometheus p90 and SLO metrics. The observation format is not compatible with the previous collector image; update the collector and UE code together.

Protect the collector network and token: payload IPs are client asserted and do not authenticate UEs or prove attachment. Before any live mutation, reverify NodePort reachability and telemetry source/payload at both gNBs, the SMF-IP-to-AMF-gNB join, scrape health, delivery loss, and p90 against raw JSONL. Unknown or stale mappings contribute only to global metrics. Keep mutation disabled if any check fails.


Before the first run, capture the current Deployment and HPA state. Use a new directory for each campaign; if `mkdir` fails, stop and choose a different name:

```sh
mkdir -p results
mkdir results/campaign-01
kubectl -n services get deployment mec-placement-face-detection -o yaml \
  > results/campaign-01/deployment-before.yaml
kubectl -n services get hpa -o yaml > results/campaign-01/hpas-before.yaml
kubectl -n services get pods -l app=mec-placement-face-detection -o wide \
  > results/campaign-01/pods-before.txt
```

Review existing HPAs, controller processes, Deployment replicas, node selectors, affinity, and annotations. Save the actual desired initial state and identify who owns each resource before changing anything. The controller's HPA check does not prove another writer is absent.

## 2. UE host setup

**On the UE host** (`member@10.34.211.6`), log in separately. The former `10.34.7.38` host is retired. If the checkout does not exist, create its parent and clone it; `git clone` refuses an existing destination:

```sh
mkdir -p ~/skripsi_yoga
git clone https://github.com/yogarn/5g-mec-controller.git ~/skripsi_yoga/5g-mec-controller
cd ~/skripsi_yoga/5g-mec-controller
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If the checkout is a Git clone, use `git pull --ff-only`; the smoke-test copy was transferred without `.git`, so synchronize it deliberately instead. Use the same revision on both hosts. The current UE host's private assets can be inspected with:

```sh
ls -l ~/.config/5g-mec/ue-inventory.yaml
ls -l ~/.config/5g-mec/smoke.png
ls -l ~/5g-ai-edge/ueransim/build/nr-ue
```

On the current UE host, pass `/home/member/5g-ai-edge/ueransim/build/nr-ue` with `--nr-ue` and `~/.config/5g-mec/smoke.png` with `--image`. The 64×64 synthetic image is for connectivity checks, not representative performance measurements. The private inventory contains one reserved subscriber with paired site configurations; protect it and expand it only using verified reservations.

For a representative workload, pass `--image /path/to/image-directory`. Supported image filenames are sorted and assigned by subscriber index modulo the image count, so images are reused when UEs outnumber them and a subscriber retains its image across phases. `run.json` records names, hashes and assignments; each request record identifies its image. Use the same directory and assignments in matched runs.

The eight selected images in `apps/face-detection/images` are from [Syn-Vis-v0](https://huggingface.co/datasets/retowyss/Syn-Vis-v0), Reto Wyss (2025), revision `100262732989e77f38cd831d70a376a93735006a`. Its dataset card dedicates individual images to CC0 and licenses the dataset compilation/documentation under CC-BY-SA 4.0; retain provenance and check the source terms. These 1024×1024 headshots are synthetic, female-presenting CGI portraits and are not representative of all real-world faces. The images are unchanged from the source; record their hashes and keep the image set fixed across compared runs.

If any path differs, set the corresponding `--ue-config`, `--image` or `--nr-ue` option. The inventory schema and first-time subscriber provisioning instructions are in `README.md`. The insert-only provisioning tool refuses existing IMSIs/IPs; **do not re-provision an existing pool**. Confirm the UE host has no unrelated running `nr-ue` processes, the chosen subscribers are free, and both site's registered sessions and packet paths work before load testing.

## 3. Baseline: Kubernetes HPA

**On core**, stop the placement controller and all other replica writers. Confirm the HPA list has no HPA targeting the workload. The following creates the experiment's HPA; it fails if an HPA with that name already exists:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
kubectl -n services get hpa
kubectl create -f experiments/hpa-baseline.yaml
kubectl -n services describe hpa mec-placement-face-detection-baseline
```

The baseline targets 80% of CPU **request**, has 1–3 replicas, and uses a 180-second scale-down window. With a 100m CPU request, 80% is 80m per pod, not 80% of the 500m CPU limit. Confirm Metrics Server reports valid data and all pods have CPU requests. This threshold is an experimental choice, not a calibrated latency SLO.

**On the UE host**, from the checkout root, run an initial smoke test only after both paths are ready:

```sh
cd ~/skripsi_yoga/5g-mec-controller
sudo .venv/bin/python -m experiments.experiment \
  --url http://NODE_IP:APP_NODEPORT/detect --output-dir results/hpa-smoke-01
```

The command previews the schedule and asks for confirmation. `--url` is required; there is no default app address. `--armed` is the explicit non-interactive alternative. The output directory must not exist. For a research run, specify an approved, pilot-derived schedule and duration; do not substitute the smoke test. Use the same options for HPA and controller. Example syntax (illustrative only):

```sh
sudo .venv/bin/python -m experiments.experiment \
  --url http://NODE_IP:APP_NODEPORT/detect \
  --phase-schedule '2:2:0,4:3:1,4:1:3,2:0:2' \
  --phase-duration 180 --output-dir results/hpa-run-01
```

Capture HPA status, pod placement, resource use and events during the run. The runner does not collect them. Retain its complete `results/hpa-run-01/` directory, including failed attempts and UE lifecycle journal.

## 4. Reset before switching methods

**On core**, first stop the UE run and wait for its cleanup. Stop all replica writers. Confirm the baseline HPA is still the one created for this experiment, then capture its state and remove **only that HPA**:

```sh
cd /home/member/skripsi_yoga/5g-mec-controller
kubectl -n services get hpa mec-placement-face-detection-baseline -o yaml \
  > results/campaign-01/hpa-after.yaml
kubectl delete -f experiments/hpa-baseline.yaml
kubectl -n services get hpa
```

If the approved starting state is one replica and no other writer is running:

```sh
kubectl -n services scale deployment mec-placement-face-detection --replicas=1
kubectl -n services rollout status deployment/mec-placement-face-detection
kubectl -n services get deployment mec-placement-face-detection -o yaml \
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
  --url http://NODE_IP:APP_NODEPORT/detect \
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
