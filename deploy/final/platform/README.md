# Platform: what the arm runbooks assume is already running

Stand-alone reference. The same steps are in each scenario runbook (`../joint`, `../mobility`, `../scaling`),
tailored to what that scenario needs: section A builds the images, section B removes and deploys the platform.

The arm runbooks (`../joint`, `../mobility`, `../scaling`) only add and remove the per-arm pieces
(workload, anchors, HPA, adapter rules, placement config). They do **not** deploy the platform below.
The campaign does not either; it fails its preflight if a piece is missing. Run this once, or to rebuild
from nothing. Every command runs on core; each block starts with
`source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh` (it also `cd`s into the project,
which the relative manifest paths need), so a block can be pasted into any terminal on core.

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
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement get deploy,ds,svc -o wide
kubectl -n services get deploy,svc | grep -E 'envoy|core|pedestrian'
kubectl get apiservice v1beta1.external.metrics.k8s.io
kubectl -n placement get secret ue-telemetry-token
```

Expected in `placement`: `node-exporter` (DaemonSet), `placement-prometheus`, `prometheus-adapter`,
`ue-telemetry`, `placement-controller`, `vertical-offloader` all `1/1`, plus Services
`vertical-offloader` (8085) and `vertical-offloader-admin` (8086). Expected in `services`:
`mec-envoy-gnb-1/2/3` `1/1` and `mec-placement-pedestrian-detection-core` `3/3`. The APIService must say
`True` under AVAILABLE. Then the offloader itself (needs the offloader port-forward running in another terminal, `kubectl -n placement port-forward --address 127.0.0.1 svc/vertical-offloader-admin 18086:8086`):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s $A/health; echo
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Three sites, `core_ready_endpoints: 3` each.

Images (all from the registry `10.34.211.140:30500`):

| component | manifest image now | final runs used |
|---|---|---|
| vertical-offloader | `:latest` | tag `backoff-20261121T180558Z` |
| ue-telemetry-collector | `:latest` | tag `prune-20261121T152448Z` |
| placement-controller | `:latest` | `@sha256:1164526f...` |
| pedestrian-detection (system under test) | `@sha256:5245d82f...` (unchanged on purpose) | same |
| Envoy | `envoyproxy/envoy:v1.31-latest` | same |

The three `:latest` images must be built and pushed first (section A of each scenario runbook:
`docker build` on core, push to `localhost:30500`). The `latest` tags in the registry before such a build are
older images, not the ones the final runs used. To pin the running Deployments back to the final-run images,
see section A4 of the runbooks. `.dockerignore` at the repo root keeps the build context small (the repo is
about 30 GB with results and archives).

<!-- rebuild-safety:start -->
## Rebuild safety: which images are safe to rebuild
**Short answer:** the offloader, the collector and `placement-controller` (what we build ourselves) are safe to rebuild. The workload is not. Envoy and the telemetry images are upstream and not built.
Same source does not mean same image: every `docker build` produces a new digest (layer timestamps, and base images and unpinned dependencies can move). What matters is whether that can change the **measured** result. Status of what was checked is stated per row.
| image | built by you? | safe to rebuild? | why / what was checked |
|---|---|---|---|
| `vertical-offloader` | yes (A2) | **yes** | It makes the offloading decisions of the vertical arms, but its logic is identical: checked on core that every non-test `.py` file in the final-run image (`backoff-20261121T180558Z`) has the same SHA-256 as the file in this repo (17 files). Only the digest, the base image `python:3.12-slim` and the unpinned `pip install pyyaml` (config parsing only) can differ. Observed on core on 2026-11-24: the rebuild has PyYAML 6.0.4 (final-run image: 6.0.3) and Debian 13.8 (13.7); Python 3.12.16 is the same. |
| `ue-telemetry-collector` | yes (A2) | **yes** | It computes the p90 the HPA reads. Checked on core: every non-test `.py` file in the final-run image (`prune-20261121T152448Z`) is identical to the repo (6 files). `PyYAML==6.0.2` is pinned; only the base image can move (observed on 2026-11-24: Debian 13.8 instead of 13.7, PyYAML and Python unchanged). |
| `placement-controller` | yes (A2) | **yes** | It is the scheduler and descheduler of the proposed arms. A Go binary cannot be compared to its source directly, so identity rests on the timeline below: the image `sha256:1164526f...` is tag `prune-20261121T152448Z`, built 2026-11-21 15:25 UTC right after commit `fda7bfb`, and `git diff fda7bfb HEAD -- placement/` shows that **no Go source changed since**; only `deploy.yaml` (the image reference) differs. Checked: `go vet ./...` and `go test ./...` pass on the frozen source (config, controller and policy packages, in `golang:1.24-alpine`), and a local build from `placement/` starts and prints its flags; that shows the logic is sound, not that it is the same binary. Only the digest, the moving `golang:1.24-alpine` toolchain and the base image can differ; modules are pinned by `go.sum`. **Observed on core on 2026-11-24:** the rebuild reported every step `CACHED` (the build cache still held the layers of the original build), and the pushed `latest` has the same 13 layers, the same top layer and the same `created` time as the final-run image, i.e. it is the same binary; the manifest digest differs only because BuildKit now wraps it in an index with an attestation. On a machine or after a `docker builder prune` without that cache you get a fresh build of the same source instead. |
| `envoyproxy/envoy:v1.31-latest` | no (upstream) | **do not re-pull it** | It sits on the request path and the tag is floating. The nodes run `sha256:caa5b411...`, which is also the digest recorded in the results; `imagePullPolicy: IfNotPresent` keeps it cached. A fresh pull could bring a newer 1.31.x. Pinning it (`envoyproxy/envoy@sha256:caa5b411be1633b90023592a34a7e010c933d6e60206c758f631485e53006865`) would remove that risk; the manifests do not do this yet. |
| `pedestrian-detection` (the workload) | **no** | **no, keep the digest** | It is the system under test and the digest `sha256:5245d82f...` (tag `steep-spillover`) is recorded in the results. `requirements.txt` pins only 5 packages (Flask, gunicorn, numpy, Pillow, onnxruntime); their dependencies and the `python:3.11-slim` base are not pinned, so a rebuild can change inference time by a few ms, which goes straight into p90 and SLO violations. With `:latest` and `imagePullPolicy: IfNotPresent` each node would also keep whichever build it pulled first, so replicas on different nodes could silently run different images. A rebuild means a new tag and digest, every YAML with the old digest replaced, and the experiment rerun. |
| Prometheus `v2.53.0`, prometheus-adapter `v0.12.0`, node-exporter `v1.8.1`, Grafana `11.1.0` | no (upstream) | n/a | Fixed version tags, pulled not built; nothing to do. |
**When the final-run images were built, and what changed in the code afterwards** (commit times in this repo are WIB, UTC+7; image times are UTC). The first final run started 2026-11-22 12:33 UTC.

| image | built (UTC) | built after | code changed since? |
|---|---|---|---|
| `ue-telemetry-collector` | 2026-11-21 15:25 | `fda7bfb` "prune unreachable edge cases", which came after the big refactors (`fc05563` modularize, `a477e95` split offloader/collector into modules) | no: every non-test `.py` file is identical to the repo |
| `placement-controller` | 2026-11-21 15:25 | the same commit `fda7bfb` | no Go source change (`git diff fda7bfb HEAD -- placement/` is only `deploy.yaml`) |
| `vertical-offloader` | tag 2026-11-21 18:05 (image created 18:50) | `820195f` "exponential backoff of retreat retries" (18:02 UTC), the last offloader logic change | no: every non-test `.py` file is identical to the repo |
| `pedestrian-detection` | tag `steep-spillover` | its last source commit `84c4beb` (2026-11-19) | no app source change since; the image is pinned by digest |
So the images were built **after** the large refactors and after the last change to these components. What changed after that is the experiment harness and the plots (`experiments/`, `utils/`), which run outside these images (the campaign on core, the UE runner on the UE host).
Rule of thumb: the three control components are safe to rebuild from this checkout (their source is unchanged since the final-run images were built), the workload and Envoy are never rebuilt or re-pulled during a comparison. After any rebuild, section B3 shows the digest each pod really runs; write it down next to the results.
<!-- rebuild-safety:end -->

## 2. Tear the platform down (reverse order)

Do the arm cleanup from the runbook first (offloader to LOCAL, remove the arm). Then, in this order so
nothing is left pointing at something that is already gone:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
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
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
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
now (read-only). The registry on `10.34.211.140:30500` has the final-run image tags and digests. Neither the teardown nor
the bring-up was executed; the teardown is destructive for Prometheus history, so read section 2 before
running it.
