# 5G MEC Placement & Autoscaling Controller

Latency-aware Kubernetes pod placement (Go scheduler + descheduler) and SLO-driven autoscaling (Kubernetes HPA + Prometheus Adapter) for 5G MEC edge workloads.

## Repository Layout

```text
apps/          Edge workloads (face-detection/, pedestrian-detection/)
autoscaler/    Kubernetes HPA manifests (hpa-baseline.yaml, hpa-proposed.yaml)
benchmark/     UE request client and telemetry publisher
collector/     UE telemetry HTTP-to-Prometheus collector (manifest.yaml)
experiments/   Campaign orchestrator (campaign.py, campaign_ue.py, campaign.yaml, run.sh)
placement/     Go in-cluster scheduler & descheduler (cmd/, internal/, Dockerfile, deploy.yaml)
telemetry/     Prometheus, Node/DCGM exporters, Grafana, Prometheus Adapter (00..05-*.yaml)
utils/         Shared config, Prometheus client, preflight, UE lifecycle, tc, monitoring, plotting
```

---

## 1. One-Time Setup (Launched Once)

These components are cluster-wide or environment-level prerequisites. You only deploy them once (or when their manifests/images change).

### Check whether they are already running

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

# 1. Check telemetry, collector, prometheus-adapter, and placement-controller pods
kubectl -n placement get deploy,pods

# 2. Check who owns the external metrics API (must be placement/prometheus-adapter, NOT keda)
kubectl get apiservices v1beta1.external.metrics.k8s.io -o jsonpath='{.spec.service}{"\n"}'

# 3. Probe the external metric directly
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/default/ue_p90_latency_seconds"
```

If all deployments in `placement` are `1/1 Ready`, `v1beta1.external.metrics.k8s.io` points to `placement/prometheus-adapter`, and the raw query returns a `MetricValueList`, skip the cluster apply steps below.

### Apply if missing or outdated

1. **Remove KEDA (if `v1beta1.external.metrics.k8s.io` is still owned by KEDA):**
   ```bash
   helm uninstall keda -n keda 2>/dev/null || true
   kubectl delete apiservice v1beta1.external.metrics.k8s.io --ignore-not-found
   ```
2. **Deploy telemetry & Prometheus Adapter:**
   ```bash
   kubectl apply -f telemetry/
   ```
3. **Deploy UE telemetry collector:**
   ```bash
   kubectl apply -f collector/manifest.yaml
   ```
4. **Build, push, and deploy the Go placement controller** (push via `localhost:30500` on `core-5g` because Docker treats `localhost:*` as HTTP without `/etc/docker/daemon.json`; `k3s` pulls from `10.34.211.140:30500`):
   ```bash
   docker build --network=host -t localhost:30500/placement-controller:latest placement
   docker push localhost:30500/placement-controller:latest
   kubectl apply -f placement/deploy.yaml
   ```
5. **Set up Python virtual environments (on `core` and remote `ue_host`):**
   ```bash
   bash experiments/run.sh setup
   ```
6. **Provision UE subscribers in Open5GS MongoDB (first time only, see `EXPERIMENTS.md`):**
   ```bash
   .venv/bin/python -m utils.provision_ues --inventory /secure/ue-inventory.yaml --apply
   ```

---

## 2. Running Experiments

When running an evaluation campaign, **do not manually apply workloads or HPAs**—`experiments/campaign.py` handles workload deployment/reset, baseline vs. proposed HPA switching, `placement-controller-config` updates, UE traffic ramps, telemetry collection, and plotting.

1. **Run preflight & UE connectivity checks:**
   ```bash
   bash experiments/run.sh check
   # or for pedestrian-detection:
   bash experiments/run.sh check --workload pedestrian-detection
   ```
2. **Run a campaign:**
   ```bash
   # Full comparison (default-scheduler + CPU HPA vs mec-placement-scheduler + latency HPA)
   bash experiments/run.sh run_1 --workload face-detection --comparison full

   # Placement-only comparison (both use latency HPA)
   bash experiments/run.sh run_2 --workload pedestrian-detection --comparison placement

   # Quick 60s-phase debug run
   bash experiments/run.sh debug_1 --debug
   ```
   Results, rendered manifests, summaries, and plots are saved to `results/<campaign_name>/`. See `EXPERIMENTS.md` for fixed experimental conditions and plotting commands.

---

## 3. Production Proposed Deployment

To run the proposed system outside an experiment campaign (assuming Section 1 one-time infrastructure is running):

1. **Deploy the edge workload** (ensure `spec.template.spec.schedulerName: mec-placement-scheduler` is set in the Deployment):
   ```bash
   kubectl apply -f apps/face-detection/manifest.yaml
   # or: kubectl apply -f apps/pedestrian-detection/manifest.yaml
   ```
2. **Deploy the proposed latency HPA** (edit `scaleTargetRef`, `minReplicas`/`maxReplicas`, and `target.value` SLO in seconds if needed):
   ```bash
   kubectl apply -f autoscaler/hpa-proposed.yaml
   ```
3. **Verify scaling and placement:**
   ```bash
   kubectl get hpa -w
   kubectl -n placement logs -f deploy/placement-controller
   ```
