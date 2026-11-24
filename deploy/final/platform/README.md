# Platform: what the arm runbooks assume is already running

The arm runbooks (`../joint`, `../mobility`, `../scaling`) only add and remove the per-arm pieces
(workload, anchors, HPA, adapter rules, placement config). They do **not** deploy the platform below.
The campaign does not either; it fails its preflight if a piece is missing. Run this once, or to rebuild
from nothing. Every command runs on core in a terminal where you did
`source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh`.

| layer | namespace | manifest | what it is |
|---|---|---|---|
| telemetry | placement | `telemetry/00-namespace.yaml`, `01-node-exporter.yaml`, `03-prometheus.yaml`, `05-prometheus-adapter.yaml` | namespace, node-exporter, Prometheus (all scrape jobs), adapter + `external.metrics.k8s.io` APIService |
| collector | placement | `collector/manifest.yaml` | `ue-telemetry` Deployment and Service (UEs POST latency here) |
| scheduler | placement | `placement/deploy.yaml` | `placement-controller` (the `mec-placement-scheduler` and descheduler) |
| offloader | placement | `offloader/manifest.yaml` | `vertical-offloader` (xDS + admin API) and Services `vertical-offloader`, `vertical-offloader-admin` |
| Core backend | services | `offloader/core-backend.yaml` | `mec-placement-pedestrian-detection-core`, 3 replicas on `core-5g` |
| Envoy ingress | services | `offloader/envoy-ingress.yaml` | `mec-envoy-gnb-1/2/3` and their Services (NodePorts 31801 to 31803) |
| secret | placement | created by hand | `ue-telemetry-token` (key `token`), used by the collector and the offloader |

Not part of the platform: Grafana (`telemetry/04-grafana.yaml`, optional) and
`collector/prometheus-scrape.yaml`. That file is a merge snippet; the same jobs are already inside
`telemetry/03-prometheus.yaml`, so do **not** apply it. The Calico pools (`mec-campaign-reserved`
172.16.40.0/23, `edge-pool`) and node labels are cluster setup and are not touched here.

## 1. Check what is running now

```bash
kubectl -n placement get deploy,ds,svc -o wide
kubectl -n services get deploy,svc | grep -E 'envoy|core|pedestrian'
kubectl get apiservice v1beta1.external.metrics.k8s.io
kubectl -n placement get secret ue-telemetry-token
```

Expected in `placement`: `node-exporter` (DaemonSet), `placement-prometheus`, `prometheus-adapter`,
`ue-telemetry`, `placement-controller`, `vertical-offloader` all `1/1`, plus Services
`vertical-offloader` (8085) and `vertical-offloader-admin` (8086). Expected in `services`:
`mec-envoy-gnb-1/2/3` `1/1` and `mec-placement-pedestrian-detection-core` `3/3`. The APIService must say
`True` under AVAILABLE. Then the offloader itself (needs the T3 port-forward):

```bash
curl -s $A/health; echo
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Three sites, `core_ready_endpoints: 3` each. The pinned images (from the manifests) are
`placement-controller@sha256:1164526f...`, `pedestrian-detection@sha256:5245d82f...`,
`vertical-offloader:backoff-20261121T180558Z`, `ue-telemetry-collector:prune-20261121T152448Z`,
`envoyproxy/envoy:v1.31-latest`, all from `10.34.211.140:30500`.

## 2. Tear the platform down (reverse order)

Do the arm cleanup from the runbook first (offloader to LOCAL, remove the arm). Then, in this order so
nothing is left pointing at something that is already gone:

```bash
# Envoys first: they consume the offloader's xDS
kubectl delete -f offloader/envoy-ingress.yaml
# Core backend and the offloader itself
kubectl delete -f offloader/core-backend.yaml
kubectl delete -f offloader/manifest.yaml
# scheduler: while it is gone, pods with schedulerName mec-placement-scheduler stay Pending
kubectl delete -f placement/deploy.yaml
# collector, then the telemetry stack
kubectl delete -f collector/manifest.yaml
kubectl delete -f telemetry/05-prometheus-adapter.yaml
kubectl delete -f telemetry/03-prometheus.yaml
kubectl delete -f telemetry/01-node-exporter.yaml
```

This keeps the `placement` and `services` namespaces and the `ue-telemetry-token` Secret. To remove
those as well (not needed for a rebuild): `kubectl -n placement delete secret ue-telemetry-token`, then
`kubectl delete -f telemetry/00-namespace.yaml` (this also deletes Grafana if it is deployed there).
Prometheus stores its data in the pod, so tearing it down drops the history.

## 3. Bring the platform up from nothing

```bash
# 3.1 namespaces
kubectl apply -f telemetry/00-namespace.yaml
kubectl get namespace services >/dev/null 2>&1 || kubectl create namespace services

# 3.2 token secret: the key must be "token" and the value must be the file content exactly
#     (the secret on core is byte-identical to this file, trailing newline included)
kubectl -n placement get secret ue-telemetry-token >/dev/null 2>&1 || \
  kubectl -n placement create secret generic ue-telemetry-token \
    --from-file=token=/home/member/.config/5g-mec/ue-telemetry-token

# 3.3 telemetry
kubectl apply -f telemetry/01-node-exporter.yaml
kubectl apply -f telemetry/03-prometheus.yaml
kubectl -n placement rollout status deployment/placement-prometheus --timeout=180s
kubectl apply -f telemetry/05-prometheus-adapter.yaml
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=180s
kubectl get apiservice v1beta1.external.metrics.k8s.io   # AVAILABLE must become True

# 3.4 collector (do NOT apply collector/prometheus-scrape.yaml)
kubectl apply -f collector/manifest.yaml
kubectl -n placement rollout status deployment/ue-telemetry --timeout=180s

# 3.5 scheduler / descheduler
kubectl apply -f placement/deploy.yaml
kubectl -n placement rollout status deployment/placement-controller --timeout=180s

# 3.6 offloader, Core backend, Envoys (in this order)
kubectl apply -f offloader/manifest.yaml
kubectl -n placement rollout status deployment/vertical-offloader --timeout=180s
kubectl apply -f offloader/core-backend.yaml
kubectl -n services rollout status deployment/mec-placement-pedestrian-detection-core --timeout=240s
kubectl apply -f offloader/envoy-ingress.yaml
kubectl -n services rollout status deployment/mec-envoy-gnb-1 --timeout=180s
kubectl -n services rollout status deployment/mec-envoy-gnb-2 --timeout=180s
kubectl -n services rollout status deployment/mec-envoy-gnb-3 --timeout=180s
```

Optional Grafana: `kubectl apply -f telemetry/04-grafana.yaml`.

Then open the T3 port-forward (see the scenario runbook) and run the checks from section 1. The offloader
config starts in `mode: vertical`; every arm's runbook resets it to `local-only` before doing anything
else, so that is fine. Give Prometheus about a minute to scrape before the first arm: the adapter
metrics only appear once the `ue-telemetry` and `kubelet-cadvisor` jobs have data.

## Status

The manifest list, order and expected state come from the repo manifests and from what is running on core
now (read-only). The registry on `10.34.211.140:30500` has the pinned image tags. Neither the teardown nor
the bring-up was executed; the teardown is destructive for Prometheus history, so read section 2 before
running it.
