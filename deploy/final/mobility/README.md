# Mobility experiment (placement only, no autoscaling)
Mobility scenario: 7 phases x 120 s, 24 UEs the whole time, schedule `24:1:11:12,24:1:17:6,24:1:22:1,24:1:16:7,24:1:11:12,24:1:6:17,24:1:1:22`. Replica counts are fixed (no HPA): 3 anchors + 6 dynamic for local arms, 3 anchors + 3 dynamic + 3 Core for vertical arms.

## Order of work and rebuild verdict
1. **A. Build and push the images we make** (the offloader, the collector and `placement-controller`): `docker build` on core, push to the registry as `:latest` plus an immutable `build-<timestamp>` tag. Skip it if the images in the registry are already the ones you want.

2. **B. Platform:** remove the old one (optional) and deploy it from scratch, then check it and save the image digests that actually run.

3. **Scenario setup:** `tc` delay, zone labels, offloader port-forward.

4. **The arms, one at a time**, in the order given below, each with its own clean-up.

5. **Scenario teardown.**

**Rebuild verdict:** everything we build ourselves (the offloader, the collector and `placement-controller`) is **safe to rebuild** from this checkout; its source has not changed since the final-run image was built (details and checks in A0). The workload `pedestrian-detection` is **not** rebuilt: it stays pinned by digest. Envoy and the telemetry images (Prometheus, adapter, node-exporter, Grafana) are upstream images, pulled and not built.

Terminals (all on core unless noted): **T1** = commands, **T2** = tc delay, **T3** = offloader port-forward, **T4** = monitoring.

**Every code block below starts with `source .../deploy/final/env.sh`** (project directory, `KUBECONFIG`, `NS`, `APP`, `A`, `H`, `UE`), so a block can be pasted into any terminal on core, in any order. State shared between blocks lives in files (`/tmp/final-camp`, `/tmp/ue-payload.json`), not in shell variables.

Run one arm at a time, in this order: `baseline-local`, `proposed-local`, `baseline-vertical`, `proposed-vertical`. Each arm section is self-contained.

## A. Build and push the images
The platform manifests now use `:latest` for the offloader, the collector and `placement-controller` (`imagePullPolicy: Always`). Build on core with Docker (no sudo needed), push to the cluster registry through `localhost:30500`; the nodes pull the same registry as `10.34.211.140:30500`.

**Important:** the `latest` tags that exist in the registry today are *not* the images used in the final runs. Pushing a fresh build replaces them with a build of the current checkout, whose digest will differ from the ones recorded for the results:

| image | final runs used | `latest` in the registry before you rebuild |
|---|---|---|
| vertical-offloader | tag `backoff-20261121T180558Z` (`sha256:5c0a3478...`) | `sha256:4471c7df...` |
| ue-telemetry-collector | tag `prune-20261121T152448Z` (`sha256:a39107ee...`) | `sha256:121b56b5...` |
| placement-controller | `sha256:1164526f...` | `sha256:948f3b1e...` |
| pedestrian-detection (the application under test) | `sha256:5245d82f...` | no `latest` tag |

The application image is deliberately **not** moved to `latest`: it is the system under test, its digest is recorded in the results, and it is already in the registry. It is pinned by digest in `apps/pedestrian-detection/manifest.yaml`, `offloader/core-backend.yaml` and every rendered manifest here. No build step is needed for it.

### A0. Which images are safe to rebuild (read first)
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

### A1. Check the checkout and the build tools
The build uses whatever is checked out, so confirm it is the frozen commit and clean:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
git log --oneline -1
git status -s    # must print nothing
docker info --format 'docker {{.ServerVersion}}, root {{.DockerRootDir}}'
curl -s -o /dev/null -w 'registry http %{http_code}\n' http://localhost:30500/v2/
```

### A2. Build and push
Each image is pushed twice: as `latest` and as an immutable `build-<timestamp>` tag, so you can always say which build ran. The base images (`python:3.12-slim`, `golang:1.24-alpine`, `distroless`) are pulled from the internet, so core needs outbound access.

Offloader (build context = repo root, it copies `offloader/` and the probe image):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export TS=$(date -u +%Y%m%dT%H%M%SZ) R=localhost:30500
docker build --network=host -f offloader/Dockerfile -t $R/vertical-offloader:latest -t $R/vertical-offloader:build-$TS .
docker push $R/vertical-offloader:latest
docker push $R/vertical-offloader:build-$TS
echo built vertical-offloader:build-$TS
```

Collector (build context = repo root):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export TS=$(date -u +%Y%m%dT%H%M%SZ) R=localhost:30500
docker build --network=host -f collector/Dockerfile -t $R/ue-telemetry-collector:latest -t $R/ue-telemetry-collector:build-$TS .
docker push $R/ue-telemetry-collector:latest
docker push $R/ue-telemetry-collector:build-$TS
echo built ue-telemetry-collector:build-$TS
```

`placement-controller` (Go; build context = `placement/`, downloads Go modules):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export TS=$(date -u +%Y%m%dT%H%M%SZ) R=localhost:30500
docker build --network=host -t $R/placement-controller:latest -t $R/placement-controller:build-$TS placement
docker push $R/placement-controller:latest
docker push $R/placement-controller:build-$TS
echo built placement-controller:build-$TS
```

### A3. Verify what is in the registry now

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
for r in vertical-offloader ue-telemetry-collector placement-controller; do
  echo "$r latest = $(curl -s -I -H 'Accept: application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json' http://localhost:30500/v2/$r/manifests/latest | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest"{print $2}')"
  curl -s http://localhost:30500/v2/$r/tags/list; echo
done
```

Compare each rebuilt `latest` with the final-run image layer by layer. **Same top layer and same `created` time means the same bytes** (a fully `CACHED` build); a different top layer means a fresh build of the same source:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
python3 - <<'EOF'
import json, urllib.request
R = "http://localhost:30500/v2/"
ACC = ("application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, "
       "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json")
def get(path):
    return json.load(urllib.request.urlopen(urllib.request.Request(R + path, headers={"Accept": ACC})))
def image(repo, ref):
    m = get(f"{repo}/manifests/{ref}")
    for x in m.get("manifests", []):
        p = x.get("platform", {})
        if p.get("architecture") == "amd64" and p.get("os") == "linux":
            return get(f"{repo}/manifests/{x['digest']}")
    return m
for repo, final in [('vertical-offloader', 'backoff-20261121T180558Z'), ('ue-telemetry-collector', 'prune-20261121T152448Z'), ('placement-controller', 'prune-20261121T152448Z')]:
    for ref in ("latest", final):
        m = image(repo, ref); cfg = get(f"{repo}/blobs/{m['config']['digest']}")
        print(f"{repo:24} {ref[:26]:26} layers={len(m['layers'])} top-layer={cfg['rootfs']['diff_ids'][-1][7:19]} created={cfg.get('created', '')[:19]}")
EOF
```

The `latest` digest must now differ from the one in the table and the newest `build-...` tag must be listed. Section B deploys these images; section B3 shows the digest each pod really runs.

### A4. Go back to the exact final-run images (only if you need them)
Run this *after* section B. It pins the running Deployments to the images the results were produced with (the old tags still exist in the registry):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement set image deployment/vertical-offloader offloader=10.34.211.140:30500/vertical-offloader:backoff-20261121T180558Z
kubectl -n placement rollout status deployment/vertical-offloader --timeout=180s
kubectl -n placement set image deployment/ue-telemetry collector=10.34.211.140:30500/ue-telemetry-collector:prune-20261121T152448Z
kubectl -n placement rollout status deployment/ue-telemetry --timeout=180s
kubectl -n placement set image deployment/placement-controller placement-controller=10.34.211.140:30500/placement-controller@sha256:1164526f35a20a1a1afdccf538d9fd0f6b49169f851db31670d656c5d2afab1a
kubectl -n placement rollout status deployment/placement-controller --timeout=180s
```

## B. Platform: remove the old one and deploy from scratch
This scenario needs the whole platform: telemetry (node-exporter, Prometheus, adapter), the collector, `placement-controller`, the offloader, the Core backend and the three Envoys. The arm sections below assume it exists; this section builds it. All `apply` commands are idempotent, so skip B1 if you only want to make sure everything is present.

### B1. Remove the old platform (optional, only to start from nothing)
Prometheus keeps its history inside the pod, so this drops it. Reverse order, so nothing points at something already gone. Finish or clean up any running arm first.

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
# Envoys first (they consume the offloader's xDS), then the Core backend, then the offloader
kubectl delete -f offloader/envoy-ingress.yaml --ignore-not-found=true
kubectl delete -f offloader/core-backend.yaml --ignore-not-found=true
kubectl delete -f offloader/manifest.yaml --ignore-not-found=true
# scheduler: while it is gone, pods with schedulerName mec-placement-scheduler stay Pending
kubectl delete -f placement/deploy.yaml --ignore-not-found=true
# collector, then telemetry
kubectl delete -f collector/manifest.yaml --ignore-not-found=true
kubectl delete -f telemetry/05-prometheus-adapter.yaml --ignore-not-found=true
kubectl delete -f telemetry/03-prometheus.yaml --ignore-not-found=true
kubectl delete -f telemetry/01-node-exporter.yaml --ignore-not-found=true
```

The namespaces and the `ue-telemetry-token` Secret are kept.

### B2. Deploy
Namespaces and the token Secret (the Secret value must be the token file content exactly; the one on core is byte-identical to this file):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f telemetry/00-namespace.yaml
kubectl get namespace services >/dev/null 2>&1 || kubectl create namespace services
kubectl -n placement get secret ue-telemetry-token >/dev/null 2>&1 || \
  kubectl -n placement create secret generic ue-telemetry-token \
    --from-file=token=/home/member/.config/5g-mec/ue-telemetry-token
```

Telemetry and collector (do **not** apply `collector/prometheus-scrape.yaml`, it is only a merge snippet; the scrape jobs are already in `telemetry/03-prometheus.yaml`):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f telemetry/01-node-exporter.yaml
kubectl apply -f telemetry/03-prometheus.yaml
kubectl -n placement rollout status deployment/placement-prometheus --timeout=180s
kubectl apply -f telemetry/05-prometheus-adapter.yaml
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=180s
kubectl apply -f collector/manifest.yaml
kubectl -n placement rollout status deployment/ue-telemetry --timeout=180s
kubectl get apiservice v1beta1.external.metrics.k8s.io   # AVAILABLE must become True
```

Scheduler, offloader, Core backend and Envoys (in this order):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f placement/deploy.yaml
kubectl -n placement rollout status deployment/placement-controller --timeout=180s
kubectl apply -f offloader/manifest.yaml
kubectl -n placement rollout status deployment/vertical-offloader --timeout=180s
kubectl apply -f offloader/core-backend.yaml
kubectl -n services rollout status deployment/mec-placement-pedestrian-detection-core --timeout=240s
kubectl apply -f offloader/envoy-ingress.yaml
kubectl -n services rollout status deployment/mec-envoy-gnb-1 --timeout=180s
kubectl -n services rollout status deployment/mec-envoy-gnb-2 --timeout=180s
kubectl -n services rollout status deployment/mec-envoy-gnb-3 --timeout=180s
```

If the Deployments were already running (you skipped B1) and you rebuilt an image in section A, `apply` does nothing for an unchanged `:latest` tag. Restart them so they pull the new image:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement rollout restart deployment/ue-telemetry deployment/placement-controller deployment/vertical-offloader
kubectl -n placement rollout status deployment/ue-telemetry --timeout=180s
kubectl -n placement rollout status deployment/placement-controller --timeout=180s
kubectl -n placement rollout status deployment/vertical-offloader --timeout=180s
```

The offloader starts in `mode: vertical`; every arm resets it to `local-only` first, so that is fine. The Core backend comes up with 3 replicas.

### B3. Check
Give Prometheus about a minute to scrape before the first arm; the adapter metrics only appear once the `ue-telemetry` and `kubelet-cadvisor` jobs have data.

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement get deploy,svc
kubectl -n services get deploy | grep -E 'envoy|core'
kubectl get apiservice v1beta1.external.metrics.k8s.io
# which image (and exact digest) each platform pod is really running:
kubectl -n placement get pods -o custom-columns=POD:.metadata.name,IMAGE:.spec.containers[0].image,IMAGEID:.status.containerStatuses[0].imageID
```

Expect `1/1` for `placement-prometheus`, `prometheus-adapter`, `ue-telemetry`, `placement-controller`, `vertical-offloader`; `1/1` for each `mec-envoy-gnb-N`; `3/3` for `mec-placement-pedestrian-detection-core`; Services `vertical-offloader` (8085) and `vertical-offloader-admin` (8086); APIService `True`. The offloader itself is checked in the setup below, once the port-forward is up.

Save the digests that are actually running next to the results. After a rebuild they differ from the digests recorded for the final runs, so write down which build you measured:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
mkdir -p results
OUT=results/platform-images-$(date -u +%Y%m%dT%H%M%SZ).txt
{ echo "commit: $(git rev-parse --short HEAD)"; kubectl -n placement get pods -o custom-columns=POD:.metadata.name,IMAGE:.spec.containers[0].image,IMAGEID:.status.containerStatuses[0].imageID; kubectl -n services get pods -o custom-columns=POD:.metadata.name,IMAGE:.spec.containers[0].image,IMAGEID:.status.containerStatuses[0].imageID; } | tee $OUT
```

## Scenario setup (once per scenario, before the first arm)
**T2 (core), keep open for the whole scenario:** inter-node path delay of 10 ms for the experiment pods (`tc_ms: 10`, as in every final run).

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
.venv/bin/python deploy/final/tools/hold_tc_delay.py 10
# prints "tc delay 10 ms active"; Ctrl-C at the very end removes it
```

**PreferClose zones** (T1): each edge node is its own zone so `trafficDistribution: PreferClose` keeps traffic on the same node. The final runs recorded these labels as already set before the run (`node-zones-before.yaml`), so this is idempotent.

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl label node gnb1 topology.kubernetes.io/zone=gnb1 --overwrite
kubectl label node gnb-2 topology.kubernetes.io/zone=gnb-2 --overwrite
kubectl label node gnb-3 topology.kubernetes.io/zone=gnb-3 --overwrite
kubectl get nodes -L topology.kubernetes.io/zone
```

**Offloader admin API** (**T3**, core, keep open): the offloader is used by all four arms.

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement port-forward --address 127.0.0.1 svc/vertical-offloader-admin 18086:8086
```

Check from T1:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s $A/health; echo
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

The Core fallback pool was 3 replicas before the final runs. Record it now:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get deployment $APP-core -o jsonpath='{.spec.replicas}{"\n"}'   # expect 3
```

---

## 1. `baseline-local`: default scheduler, all replicas local, no offload
Files: `deploy/final/mobility/baseline-local/`. Run in terminal **T1** on core (see the setup section) unless stated.

#### 1. Start from a clean slate (offloader to LOCAL first, then remove the previous arm)
Reset the offloader before deleting anything, so no site is still routing to the Core pool or to pods that are about to disappear (the campaign guarantees this through the previous arm's teardown; after an aborted run you must do it yourself).

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Edge anchors (one pod pinned on each edge node)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/mobility/baseline-local/01-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 3. Workload (managed Deployment + Service)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/mobility/baseline-local/02-workload.yaml
kubectl -n $NS rollout status deployment/$APP --timeout=240s
```

#### 4. Routing check before traffic

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 5. Offloader and Core fallback pool (local arm: Core pool 0, offloader stays local-only)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
# reset every site to LOCAL and wait for the Envoy ACKs
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# Core fallback pool: 0 replicas
kubectl -n $NS scale deployment/$APP-core --replicas=0
kubectl -n $NS wait --for=delete pod -l app=$APP-core --timeout=180s 2>/dev/null || true
# wait until the offloader sees 0 Core endpoints at every site
for i in $(seq 1 90); do [ "$(curl -s -H "$H" $A/status | jq -r '[.sites[].core_ready_endpoints]|unique|join(",")')" = "0" ] && break; sleep 2; done
[ "$i" -lt 90 ] || echo "TIMEOUT: expected 0 core endpoints at every site, see the counts below. A managed pod already running on core-5g counts as a core endpoint: scale the managed Deployment to 0 and repeat."
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

#### 6. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=manual_mobility_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
echo "$CAMP" > /tmp/final-camp                              # later blocks (abort, collect) read it from here
PORT=$(kubectl -n $NS get svc $APP -o jsonpath='{.spec.ports[0].nodePort}')
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/mobility/baseline-local/ue-payload.json > /tmp/ue-payload.json
# the check must pass before traffic starts (&&), then this terminal blocks for the whole run
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json \
  && $UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`24:1:11:12,24:1:17:6,24:1:22:1,24:1:16:7,24:1:11:12,24:1:6:17,24:1:1:22`). Traffic starts when the UE log prints a line beginning with `[RUN]`. Open a second terminal (T4) for the next step.

#### 7. While the UE runs (terminal T4 on core). Each block is independent; the `-w` ones stay open until Ctrl-C
Pod placement (stays open):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get pods -o wide -w
```

Offload routes per site (run repeatedly):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Abort the UE early:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
$UE "cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign $CAMP --method baseline-local"
```

#### 8. After the UE finishes: collect and clean up

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
# 1) offloader back to LOCAL first (waits for the Envoy ACKs), same order as the campaign's teardown
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# 2) only then remove the arm
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
# The Core pool is left as this arm set it; the next arm sets its own size, and the scenario teardown restores it to 3.
```

---

## 2. `proposed-local`: placement scheduler + descheduler + latency HPA, no offload
Files: `deploy/final/mobility/proposed-local/`. Run in terminal **T1** on core (see the setup section) unless stated.

#### 1. Start from a clean slate (offloader to LOCAL first, then remove the previous arm)
Reset the offloader before deleting anything, so no site is still routing to the Core pool or to pods that are about to disappear (the campaign guarantees this through the previous arm's teardown; after an aborted run you must do it yourself).

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Placement scheduler/descheduler config (in-cluster Go controller)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/mobility/proposed-local/01-placement-configmap.yaml
kubectl -n placement rollout restart deployment/placement-controller
kubectl -n placement rollout status deployment/placement-controller --timeout=120s
kubectl -n placement get deployment placement-controller
```

#### 3. Edge anchors (one pod pinned on each edge node)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/mobility/proposed-local/02-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 4. Workload (managed Deployment + Service)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/mobility/proposed-local/03-workload.yaml
kubectl -n $NS rollout status deployment/$APP --timeout=240s
```

#### 5. Routing check before traffic

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 6. Offloader and Core fallback pool (local arm: Core pool 0, offloader stays local-only)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
# reset every site to LOCAL and wait for the Envoy ACKs
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# Core fallback pool: 0 replicas
kubectl -n $NS scale deployment/$APP-core --replicas=0
kubectl -n $NS wait --for=delete pod -l app=$APP-core --timeout=180s 2>/dev/null || true
# wait until the offloader sees 0 Core endpoints at every site
for i in $(seq 1 90); do [ "$(curl -s -H "$H" $A/status | jq -r '[.sites[].core_ready_endpoints]|unique|join(",")')" = "0" ] && break; sleep 2; done
[ "$i" -lt 90 ] || echo "TIMEOUT: expected 0 core endpoints at every site, see the counts below. A managed pod already running on core-5g counts as a core endpoint: scale the managed Deployment to 0 and repeat."
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

#### 7. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=manual_mobility_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
echo "$CAMP" > /tmp/final-camp                              # later blocks (abort, collect) read it from here
PORT=$(kubectl -n $NS get svc $APP -o jsonpath='{.spec.ports[0].nodePort}')
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/mobility/proposed-local/ue-payload.json > /tmp/ue-payload.json
# the check must pass before traffic starts (&&), then this terminal blocks for the whole run
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json \
  && $UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`24:1:11:12,24:1:17:6,24:1:22:1,24:1:16:7,24:1:11:12,24:1:6:17,24:1:1:22`). Traffic starts when the UE log prints a line beginning with `[RUN]`. Open a second terminal (T4) for the next step.

#### 8. While the UE runs (terminal T4 on core). Each block is independent; the `-w` ones stay open until Ctrl-C
Pod placement (stays open):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get pods -o wide -w
```

Offload routes per site (run repeatedly):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Scheduler / descheduler decisions:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement logs deployment/placement-controller --tail=30
```

Abort the UE early:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
$UE "cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign $CAMP --method proposed-local"
```

#### 9. After the UE finishes: collect and clean up

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
# 1) offloader back to LOCAL first (waits for the Envoy ACKs), same order as the campaign's teardown
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# 2) only then remove the arm
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
# The Core pool is left as this arm set it; the next arm sets its own size, and the scenario teardown restores it to 3.
```

---

## 3. `baseline-vertical`: default scheduler, offload to the Core pool allowed
Files: `deploy/final/mobility/baseline-vertical/`. Run in terminal **T1** on core (see the setup section) unless stated.

#### 1. Start from a clean slate (offloader to LOCAL first, then remove the previous arm)
Reset the offloader before deleting anything, so no site is still routing to the Core pool or to pods that are about to disappear (the campaign guarantees this through the previous arm's teardown; after an aborted run you must do it yourself).

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Edge anchors (one pod pinned on each edge node)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/mobility/baseline-vertical/01-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 3. Workload (managed Deployment + Service)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/mobility/baseline-vertical/02-workload.yaml
kubectl -n $NS rollout status deployment/$APP --timeout=240s
```

#### 4. Routing check before traffic

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 5. Offloader and Core fallback pool (vertical arm: Core pool 3, then switch to vertical)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
# reset every site to LOCAL and wait for the Envoy ACKs
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# Core fallback pool: 3 replicas
kubectl -n $NS scale deployment/$APP-core --replicas=3
kubectl -n $NS rollout status deployment/$APP-core --timeout=180s
kubectl -n $NS get pods -l app=$APP-core -o wide    # expect 3 Ready on core-5g
# wait until the offloader sees 3 Core endpoints at every site
for i in $(seq 1 90); do [ "$(curl -s -H "$H" $A/status | jq -r '[.sites[].core_ready_endpoints]|unique|join(",")')" = "3" ] && break; sleep 2; done
[ "$i" -lt 90 ] || echo "TIMEOUT: expected 3 core endpoints at every site, see the counts below. A managed pod already running on core-5g counts as a core endpoint: scale the managed Deployment to 0 and repeat."
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Only if every site shows `active_route: LOCAL`, `local_ready_endpoints >= 1` and `core_ready_endpoints: 3` (no TIMEOUT above; the offloader counts a managed pod that runs on `core-5g` as a Core endpoint, which is why the managed Deployment is seeded only after this gate), activate vertical mode:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"vertical"}' $A/mode | jq .
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Expect `mode: vertical`.

#### 6. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=manual_mobility_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
echo "$CAMP" > /tmp/final-camp                              # later blocks (abort, collect) read it from here
PORT=0   # unused: vertical arms use the fixed Envoy NodePorts 31801-31803
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/mobility/baseline-vertical/ue-payload.json > /tmp/ue-payload.json
# the check must pass before traffic starts (&&), then this terminal blocks for the whole run
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json \
  && $UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`24:1:11:12,24:1:17:6,24:1:22:1,24:1:16:7,24:1:11:12,24:1:6:17,24:1:1:22`). Traffic starts when the UE log prints a line beginning with `[RUN]`. Open a second terminal (T4) for the next step.

#### 7. While the UE runs (terminal T4 on core). Each block is independent; the `-w` ones stay open until Ctrl-C
Pod placement (stays open):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get pods -o wide -w
```

Offload routes per site (run repeatedly):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Abort the UE early:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
$UE "cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign $CAMP --method baseline-vertical"
```

#### 8. After the UE finishes: collect and clean up

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
# 1) offloader back to LOCAL first (waits for the Envoy ACKs), same order as the campaign's teardown
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# 2) only then remove the arm
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
# The Core pool is left as this arm set it; the next arm sets its own size, and the scenario teardown restores it to 3.
```

---

## 4. `proposed-vertical`: placement scheduler + descheduler + latency HPA + offload
Files: `deploy/final/mobility/proposed-vertical/`. Run in terminal **T1** on core (see the setup section) unless stated.

#### 1. Start from a clean slate (offloader to LOCAL first, then remove the previous arm)
Reset the offloader before deleting anything, so no site is still routing to the Core pool or to pods that are about to disappear (the campaign guarantees this through the previous arm's teardown; after an aborted run you must do it yourself).

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Placement scheduler/descheduler config (in-cluster Go controller)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/mobility/proposed-vertical/01-placement-configmap.yaml
kubectl -n placement rollout restart deployment/placement-controller
kubectl -n placement rollout status deployment/placement-controller --timeout=120s
kubectl -n placement get deployment placement-controller
```

#### 3. Edge anchors (one pod pinned on each edge node)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/mobility/proposed-vertical/02-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 4. Workload (managed Deployment + Service)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/mobility/proposed-vertical/03-workload.yaml
kubectl -n $NS rollout status deployment/$APP --timeout=240s
```

#### 5. Routing check before traffic

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 6. Offloader and Core fallback pool (vertical arm: Core pool 3, then switch to vertical)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
# reset every site to LOCAL and wait for the Envoy ACKs
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# Core fallback pool: 3 replicas
kubectl -n $NS scale deployment/$APP-core --replicas=3
kubectl -n $NS rollout status deployment/$APP-core --timeout=180s
kubectl -n $NS get pods -l app=$APP-core -o wide    # expect 3 Ready on core-5g
# wait until the offloader sees 3 Core endpoints at every site
for i in $(seq 1 90); do [ "$(curl -s -H "$H" $A/status | jq -r '[.sites[].core_ready_endpoints]|unique|join(",")')" = "3" ] && break; sleep 2; done
[ "$i" -lt 90 ] || echo "TIMEOUT: expected 3 core endpoints at every site, see the counts below. A managed pod already running on core-5g counts as a core endpoint: scale the managed Deployment to 0 and repeat."
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Only if every site shows `active_route: LOCAL`, `local_ready_endpoints >= 1` and `core_ready_endpoints: 3` (no TIMEOUT above; the offloader counts a managed pod that runs on `core-5g` as a Core endpoint, which is why the managed Deployment is seeded only after this gate), activate vertical mode:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"vertical"}' $A/mode | jq .
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Expect `mode: vertical`.

#### 7. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=manual_mobility_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
echo "$CAMP" > /tmp/final-camp                              # later blocks (abort, collect) read it from here
PORT=0   # unused: vertical arms use the fixed Envoy NodePorts 31801-31803
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/mobility/proposed-vertical/ue-payload.json > /tmp/ue-payload.json
# the check must pass before traffic starts (&&), then this terminal blocks for the whole run
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json \
  && $UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`24:1:11:12,24:1:17:6,24:1:22:1,24:1:16:7,24:1:11:12,24:1:6:17,24:1:1:22`). Traffic starts when the UE log prints a line beginning with `[RUN]`. Open a second terminal (T4) for the next step.

#### 8. While the UE runs (terminal T4 on core). Each block is independent; the `-w` ones stay open until Ctrl-C
Pod placement (stays open):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get pods -o wide -w
```

Offload routes per site (run repeatedly):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Scheduler / descheduler decisions:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement logs deployment/placement-controller --tail=30
```

Abort the UE early:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
$UE "cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign $CAMP --method proposed-vertical"
```

#### 9. After the UE finishes: collect and clean up

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
# 1) offloader back to LOCAL first (waits for the Envoy ACKs), same order as the campaign's teardown
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# 2) only then remove the arm
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
# The Core pool is left as this arm set it; the next arm sets its own size, and the scenario teardown restores it to 3.
```

---

## Scenario teardown

**Option 1: keep the platform, restore the pool.** Offloader to LOCAL first, then the Core pool back to its original 3 replicas (as the campaign does at the very end):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl -n $NS scale deployment/$APP-core --replicas=3
kubectl -n $NS rollout status deployment/$APP-core --timeout=180s
kubectl -n $NS get deployment $APP-core    # expect 3/3
```

**Option 2: remove the offloader stack as well** (offloader to LOCAL first, then Envoys, Core backend, offloader, scheduler; the telemetry and collector stay, remove them with section B1 if you want a bare cluster):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl delete -f offloader/envoy-ingress.yaml --ignore-not-found=true
kubectl delete -f offloader/core-backend.yaml --ignore-not-found=true
kubectl delete -f offloader/manifest.yaml --ignore-not-found=true
kubectl delete -f placement/deploy.yaml --ignore-not-found=true
```

Then stop the tc helper in T2 (Ctrl-C removes the delay) and the port-forward in T3, and check the zones:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl get nodes -L topology.kubernetes.io/zone
```

