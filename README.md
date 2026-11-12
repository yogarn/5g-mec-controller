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

Do not run an experiment or change cluster state without an approved campaign and recovery plan. Never run two replica writers against the same Deployment.
