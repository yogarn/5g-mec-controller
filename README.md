# 5G MEC placement controller

A research project for placement and scaling of a containerized service on a 5G MEC testbed. A face-detection workload is included; other services can be supplied as Kubernetes manifests. See [RESEARCH.md](RESEARCH.md) for comparisons, measurement limits and operational safety requirements.

## Face-detection workload

`apps/face-detection/` contains the app adapted from the Open5GS testbed's `05-edge-apps/face-detection`, its Dockerfile, and a dedicated Deployment and Service manifest. The manifest removes the original edge node selector and uses the digest-pinned image and resource values from the testbed's face benchmark draft. The image predates the local source changes; build and publish a new image to deploy this source. Verify registry reachability on eligible nodes. The Dockerfile installs unpinned dependencies and runs Flask's development server.

The controller reads the configured manifest but does not deploy it. Review the namespace, image, resource settings, routing and node eligibility before a separate, authorized deployment. The app accepts `POST /detect` with a multipart `image` field, returns face bounding boxes, and exposes `/health`.

Prometheus can supply proxy-observed response time; app-side inference timing is not required. Configure `latency_query` from the actual Envoy metrics and labels for detection traffic, checking units and aggregation. Proxy time excludes client-to-proxy network latency and may include upstream waiting and network time; validate the exact Envoy metric's boundaries. Avoid duplicate proxy observations and health traffic. Measure end-to-end latency independently at the UE.

For the gnb-2 calibration, `apps/face-detection/envoy-gnb2.yaml` replaces the UPF's static Envoy configuration and routes port 8000 to this Service; it does not preserve the previous proxy routes. The live UPF was restarted to activate it. The pre-change ConfigMap and StatefulSet snapshot are on `core` under `/home/member/mec-face-calibration-14UkXB/`; restoring the ConfigMap and restarting the UPF requires a planned interruption. The live workload was temporarily pinned to gnb-2 for calibration, unlike the unpinned manifest.

The supplied Prometheus query observes the gnb-2 face Envoy listener only, including any non-detection requests sent to that listener; other proxies and direct requests are not counted. Envoy's duration histogram is in milliseconds; the query converts its p95 to seconds. Scrape and histogram windows introduce lag and quantile approximation; no traffic can produce a non-finite p95. This query does not measure UE-to-proxy time or all-attempt SLO attainment.

The 500 ms SLO in `config.yaml` is an exploratory proxy-path target, not a validated production objective; mutation remains disabled.

## Controller

Install dependencies with `python3 -m pip install -r requirements.txt`. Edit `config.yaml` to set `manifest` to the path of the selected service's YAML, `prometheus_url`, the latency Prometheus query, and the response-time SLO. The manifest must contain exactly one `apps/v1` Deployment; it may also contain a Service. Set the Deployment's namespace in the manifest or specify `namespace` in the config. Relative manifest paths are resolved relative to the config file. The controller reads the manifest for identity; it does not apply it.

Run `python3 -m controller --config config.yaml --once` for one observation, or omit `--once` for the continuous loop. Mutation is disabled by default; `mutate: true` enables it. The Deployment must already exist. The controller rejects a detected HPA targeting it, but this does not establish exclusive ownership: stop KEDA and every other replica writer before enabling mutation.

`mode: scaling-only` leaves scheduling to Kubernetes. `mode: mobility-aware` additionally requires `eligible_nodes`, `cpu_utilization_query` and `mobility_query`. Each node query must return one series per eligible node with a `node` label; CPU utilization is a fraction and mobility values are arrival rates. The latency query must return exactly one series in seconds. Missing or invalid required data stops the reconciliation cycle. Prometheus query evaluation timestamps alone do not prove the underlying samples are fresh.

Scaling computes `ceil(replicas * latency / SLO)` within the configured replica bounds. Scale-down requires a sustained lower target for `scale_down_seconds`; scaling waits for all replicas to be ready, and mutations respect the cooldown. Latency alone cannot distinguish demand from a slow dependency or network, and it reacts only after latency rises. Mobility-aware placement adds a *soft* node preference; it does not pin pods to one node or guarantee relocation. Changing the preference triggers a Deployment rollout, and switching to scaling-only does not remove an earlier preference. Resource filtering uses Kubernetes requests and CPU saturation, not GPU or network latency. Do not run an experiment before validating the selected service, telemetry and policy.

Optional settings include `interval_seconds`, `cpu_saturation`, `locality_weight`, `improvement_factor`, `cooldown_seconds`, `scale_down_seconds`, and `max_metric_age_seconds`. Cooldown and stabilization state are in memory and reset on restart.

## Default commands

From the repository root, `python3 -m experiments.preflight` checks gNB-2; add `--node gnb-3` for gNB-3. `python3 -m experiments.provision_ues` checks the inventory without writing to MongoDB. `python3 -m experiments.experiment` previews a two-phase smoke test and asks for confirmation before creating UE sessions. `python3 -m experiments.summarize` summarizes the only run under `results/`; if there are multiple runs, specify the input explicitly.

Defaults are in `experiments/defaults.py`: UE inventory `~/.config/5g-mec/ue-inventory.yaml`, image `~/.config/5g-mec/face.jpg`, UERANSIM `~/ueransim/build/nr-ue`, URL `http://10.34.211.196:30090/detect`, two 30-second phases (`1:1:0,1:0:1`), and a 500 ms summary SLO. With sudo, `~` refers to the invoking user's home. Supply the private inventory, image and executable and verify the URL on both UE paths before running. The default schedule is a connectivity smoke test, **not** a justified research campaign; gNB-3's route and complete latency metrics are not yet validated.

All options remain overridable. MongoDB writes still require `--apply`; proxy changes and UPF restarts still require their explicit flags and backup. Non-interactive experiments still require `--armed`, and UE recovery requires the exact journal path to avoid touching another run. The generic benchmark also requires explicit existing UE addresses.

## Experimental HPA baseline

`experiments/hpa-baseline.yaml` is a standalone Kubernetes HPA for the same face-detection Deployment; it is not part of the controller or its production configuration. It uses CPU utilization at 80% of each container's CPU **request**, with 1–3 replicas and a 180-second scale-down stabilization window. With the manifest's `100m` request, the target is 80m average CPU per pod, not 80% of its `500m` limit. This is a proposed experimental setting, not a calibrated equivalent of the latency SLO. Metrics Server and CPU requests for all containers are required.

Before an authorized run, use the same workload, image, resources, replica bounds, routes, node constraints, initial replica count and UE schedule for both conditions. The live calibration workload was pinned to gnb-2: inspect and match placement constraints before comparison. Stop the placement controller and all other replica writers; its HPA detection is not a substitute for exclusive ownership. Check for existing HPAs targeting the Deployment, and do not overwrite or delete another experiment's HPA. Capture the pre-run Deployment and HPA state in a new directory:

```sh
mkdir baseline-state
kubectl -n open5gs get deployment mec-placement-face-detection -o yaml > baseline-state/deployment-before.yaml
kubectl -n open5gs get hpa -o yaml > baseline-state/hpas-before.yaml
kubectl -n open5gs get hpa
kubectl -n open5gs top pods -l app=mec-placement-face-detection
kubectl -n open5gs get hpa mec-placement-face-detection-baseline
```

The last command must report NotFound; if it exists, stop and inspect ownership. After checking Metrics Server, workload readiness and the preflight results, create the baseline HPA with `kubectl create -f experiments/hpa-baseline.yaml`. Inspect `kubectl -n open5gs describe hpa mec-placement-face-detection-baseline` for valid CPU metrics and scaling conditions. Run the same `experiments.experiment` schedule and `experiments.summarize` commands documented below; retain HPA status/events and pod resources separately.

After the run, capture the HPA status, then delete **only this experiment's** HPA with `kubectl delete -f experiments/hpa-baseline.yaml` after confirming it has not been replaced or taken over. Deleting the HPA does not restore the replica count or placement. With all writers stopped, compare the live Deployment with the captured state and restore only approved changed fields, then verify readiness. Remove any controller-added node preference before the baseline only as part of an approved, captured setup; do not assume disabling the controller removes it.

## Generic UE HTTP benchmark

The client uses **already running** UERANSIM UE IP addresses; it does not launch, move, or terminate UEs. The supplied address groups must correspond to actual UE attachments. It issues closed-loop HTTP requests and writes per-attempt JSONL with phase boundaries and per-UE counts. It does not measure pod resources or collect controller events.

```sh
python3 -m benchmark.client --help
python3 -m benchmark.client \
  --url http://SERVICE_ADDRESS/PATH \
  --phase-schedule '2:1:1,4:2:2,2:1:1' \
  --gnb2-addresses 'UE_IP_1,UE_IP_2' \
  --gnb3-addresses 'UE_IP_3,UE_IP_4' \
  --phase-duration 300 --output results.jsonl
```

The example is illustrative, not a runnable campaign. Output creation is exclusive: choose a new path. Request generation stops at each phase deadline; outstanding requests drain before the next phase. Configure method, body, headers and timeout for the selected service. Establish connectivity and routing separately before any campaign.

## Provisioning UE subscribers

The experiment requires an existing Open5GS core, two registered gNBs, UERANSIM and two configured IPv4 DNNs with nonoverlapping address pools. Provisioning does not configure the core, DNNs, UPFs, routing or gNBs.

1. Install the project dependencies on a trusted host with access to the Open5GS MongoDB. Restrict the database connection to trusted hosts; set `OPEN5GS_MONGO_URI` in the environment if it is not `mongodb://127.0.0.1:27017/`. Do not put database credentials in shell history or Git.
2. For each subscriber, make two private copies of a working UERANSIM UE YAML configuration, one per gNB. Use a unique 15-digit `imsi-...` SUPI per subscriber; the two copies must share the same SUPI, `key`, `op` (OPC), `opType: OPC`, and `amf`. Set `gnbSearchList` and the single IPv4 `sessions` entry's `apn` for the intended site; use the same numeric `slice.sst` in both. Provisioning supports one SST without SD and one session per site configuration.
3. Create the private inventory described below, with unique per-site static IPs inside the configured DNN pools. Reserve them outside dynamic allocation and ensure they are not already assigned. Keep inventory and UE YAMLs accessible to the UE host and restrict permissions.
4. Stop other subscriber provisioning writers. Run the check first, review the redacted IMSI/DNN/IP plan, then explicitly insert:

```sh
python3 -m experiments.provision_ues --inventory /secure/ue-inventory.yaml
python3 -m experiments.provision_ues --inventory /secure/ue-inventory.yaml --apply
```

The command checks the database for existing IMSIs and static IPs and inserts only new subscribers; it never updates or deletes existing records. Authentication values come from the private UE configurations and are not printed. **MongoDB writes are not transactional:** if insertion fails, some subscribers may already have been created. Inspect the database before retrying; resolve partial results manually under the site's recovery procedures. The conflict check cannot prevent races with other writers, so exclusive provisioning is required. Successful insertion does not prove UE registration, IP pool correctness or connectivity; verify those separately.

Provisioning prints a short subscriber count by default; use `--verbose` for the IMSI/DNN/IP list or `--json` for machine-readable output. No authentication keys are printed.

## Preflight

From a host with `kubectl` access to the testbed, check the workload, proxy, and Prometheus without changing the cluster:

```sh
python3 -m experiments.preflight --upf-statefulset upf-gnb-2 \
  --envoy-container envoy --envoy-configmap upf-gnb-2-envoy-config \
  --envoy-config-key envoy.yaml --envoy-config-path /etc/envoy-config/envoy.yaml
```

The command prints concise status checks and exits nonzero on failure; use `--json` for the full machine-readable report. In an interactive terminal, if the Envoy configuration differs and other prerequisites are satisfied, it offers to back up the ConfigMap under `results/` and redeploy the UPF after confirmation. Redeployment replaces proxy routes and interrupts 5G traffic. Non-interactive changes require `--apply-envoy-config --recovery-dir NEW_DIRECTORY`; replacing the UPF pod additionally requires `--restart-upf`. `OnDelete` does not reload the static configuration automatically.

This example checks only gnb-2. For a two-site campaign, also check gnb-3 with its own StatefulSet and ConfigMap arguments. The current controller latency query only covers gnb-2; the gnb-3 proxy route and complete service-wide telemetry must be configured and verified before interpreting a two-site run.

An idle service can yield no finite latency histogram; preflight reports that as a failed telemetry check rather than inventing a value. For the supplied face-proxy query, it checks whether the request counter has increased in the last two minutes and reports confirmed idle traffic separately. A confirmed idle latency query does not block an explicitly approved Envoy redeployment; other failed prerequisites do. Send a real face-detection request through the intended proxy path, then recheck. The controller cannot scale from a missing or non-finite latency value; idle traffic is not evidence of readiness for a campaign.

## Automated UE ramp and switching

Run `python3 -m experiments.experiment` on the UERANSIM host as root, after preflight and an approved recovery plan. It creates owned UE sessions, tears them down and reattaches them to realize each ordered gNB split, then drives closed-loop face requests from the resulting source IPs. This is stop-and-reattach, **not a 5G handover**. Phase changes include registration and routing setup; traffic pauses during switching.

Supply a private UE inventory YAML outside the repository. Each `subscribers` entry has `gnb2` and `gnb3` mappings with `config` (path to that subscriber's existing UERANSIM YAML) and `ip` (its expected IPv4 address at that site). The two configurations for each subscriber must have the same SUPI; SUPIs and expected IPs must be unique across the inventory. Paths are relative to the inventory. Keep subscriber keys out of Git.

For example, one subscriber entry has this shape (replace paths and addresses with the provisioned testbed values):

```yaml
subscribers:
  - gnb2: {config: /secure/ue-01-gnb2.yaml, ip: 10.63.1.101}
    gnb3: {config: /secure/ue-01-gnb3.yaml, ip: 10.64.1.101}
```

```sh
sudo python3 -m experiments.experiment --armed \
  --ue-config /secure/ue-inventory.yaml --nr-ue /path/to/nr-ue \
  --url http://SERVICE_ADDRESS:8000/detect --image /path/to/image.jpg \
  --phase-schedule '2:2:0,4:3:1,4:1:3,2:0:2' \
  --phase-duration 180 --timeout 10 --output-dir results/run-001
python3 -m experiments.summarize results/run-001/requests.jsonl \
  --slo-ms 500 --output-prefix results/run-001/summary
```

The output directory must not exist; its parent must already exist. `run.json` records the image hash, schedule and request settings; `requests.jsonl` records phase boundaries and every attempt. UE lifecycle logs and recovery evidence are retained in `owned_ues/`. On interruption, preserve the directory; use `python3 -m experiments.ue_lifecycle --recover RESULTS_DIR/owned_ues` as root if cleanup did not finish. Review recovery output rather than killing unrelated UE processes.

The summary command creates `summary.json`, `summary.csv`, `summary-ues.json` and `summary-ues.csv` without overwriting existing output. Percentiles use successful request latencies; SLO attainment divides successful attempts within the threshold by **all** attempts. Throughput uses elapsed phase time including request drain. These are client-observed latencies, not Envoy-only timings. Source IP and configured site are not independent proof of attachment: verify session, route, and traffic path before interpreting results. The runner does not collect pod resource usage or controller events; collect those separately for a placement or scaling comparison.

Do not run an experiment or change cluster state without an approved campaign and recovery plan. Never run two replica writers against the same Deployment.
