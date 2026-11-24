# Scaling experiment (autoscaler only)
Scaling staircase: 7 phases x 120 s, schedule `3:1:1:1,9:3:3:3,15:5:5:5,21:7:7:7,24:8:8:8,12:4:4:4,3:1:1:1`, `core-5g` only, default scheduler for both arms. `baseline` uses the native CPU HPA (80%), `proposed` the latency HPA (excess over 120 ms, target 30m).

## Order of work and rebuild verdict
1. **A. Build and push the images we make** (the collector): `docker build` on core, push to the registry as `:latest` plus an immutable `build-<timestamp>` tag. Skip it if the images in the registry are already the ones you want.

2. **B. Platform:** remove the old one (optional) and deploy it from scratch, then check it and save the image digests that actually run.

3. **Scenario setup:** `tc` delay, zone labels.

4. **The arms, one at a time**, in the order given below, each with its own clean-up.

5. **Scenario teardown.**

**Rebuild verdict:** everything we build ourselves (the collector) is **safe to rebuild** from this checkout; its source has not changed since the final-run image was built (details and checks in A0). The workload `pedestrian-detection` is **not** rebuilt: it stays pinned by digest. Envoy and the telemetry images (Prometheus, adapter, node-exporter, Grafana) are upstream images, pulled and not built.

Terminals (all on core unless noted): **T1** = commands, **T2** = tc delay, **T4** = monitoring.

**Every code block below starts with `source .../deploy/final/env.sh`** (project directory, `KUBECONFIG`, `NS`, `APP`, `A`, `H`, `UE`), so a block can be pasted into any terminal on core, in any order. State shared between blocks lives in files (`/tmp/final-camp`, `/tmp/ue-payload.json`), not in shell variables.

Run one arm at a time, in this order: `baseline`, `proposed`. Each arm section is self-contained.

## A. Build and push the images
The platform manifests now use `:latest` for the collector (`imagePullPolicy: Always`). Build on core with Docker (no sudo needed), push to the cluster registry through `localhost:30500`; the nodes pull the same registry as `10.34.211.140:30500`.

**Important:** the `latest` tags that exist in the registry today are *not* the images used in the final runs. Pushing a fresh build replaces them with a build of the current checkout, whose digest will differ from the ones recorded for the results:

| image | final runs used | `latest` in the registry before you rebuild |
|---|---|---|
| ue-telemetry-collector | tag `prune-20261121T152448Z` (`sha256:a39107ee...`) | `sha256:121b56b5...` |
| pedestrian-detection (the application under test) | `sha256:5245d82f...` | no `latest` tag |

The application image is deliberately **not** moved to `latest`: it is the system under test, its digest is recorded in the results, and it is already in the registry. It is pinned by digest in `apps/pedestrian-detection/manifest.yaml`, `offloader/core-backend.yaml` and every rendered manifest here. No build step is needed for it.

### A0. Which images are safe to rebuild (read first)
**Short answer:** the collector (what we build ourselves) are safe to rebuild. The workload is not. Envoy and the telemetry images are upstream and not built.

Same source does not mean same image: every `docker build` produces a new digest (layer timestamps, and base images and unpinned dependencies can move). What matters is whether that can change the **measured** result. Status of what was checked is stated per row.

| image | built by you? | safe to rebuild? | why / what was checked |
|---|---|---|---|
| `ue-telemetry-collector` | yes (A2) | **yes** | It computes the p90 the HPA reads. Checked on core: every non-test `.py` file in the final-run image (`prune-20261121T152448Z`) is identical to the repo (6 files). `PyYAML==6.0.2` is pinned; only the base image can move. |
| `pedestrian-detection` (the workload) | **no** | **no, keep the digest** | It is the system under test and the digest `sha256:5245d82f...` (tag `steep-spillover`) is recorded in the results. `requirements.txt` pins only 5 packages (Flask, gunicorn, numpy, Pillow, onnxruntime); their dependencies and the `python:3.11-slim` base are not pinned, so a rebuild can change inference time by a few ms, which goes straight into p90 and SLO violations. With `:latest` and `imagePullPolicy: IfNotPresent` each node would also keep whichever build it pulled first, so replicas on different nodes could silently run different images. A rebuild means a new tag and digest, every YAML with the old digest replaced, and the experiment rerun. |
| Prometheus `v2.53.0`, prometheus-adapter `v0.12.0`, node-exporter `v1.8.1`, Grafana `11.1.0` | no (upstream) | n/a | Fixed version tags, pulled not built; nothing to do. |

**When the final-run images were built, and what changed in the code afterwards** (commit times in this repo are WIB, UTC+7; image times are UTC). The first final run started 2026-11-22 12:33 UTC.

| image | built (UTC) | built after | code changed since? |
|---|---|---|---|
| `ue-telemetry-collector` | 2026-11-21 15:25 | `fda7bfb` "prune unreachable edge cases", which came after the big refactors (`fc05563` modularize, `a477e95` split offloader/collector into modules) | no: every non-test `.py` file is identical to the repo |
| `pedestrian-detection` | tag `steep-spillover` | its last source commit `84c4beb` (2026-11-19) | no app source change since; the image is pinned by digest |

So the images were built **after** the large refactors and after the last change to these components. What changed after that is the experiment harness and the plots (`experiments/`, `utils/`), which run outside these images (the campaign on core, the UE runner on the UE host).

Rule of thumb: the collector is safe to rebuild from this checkout (proven identical in logic), the workload is never rebuilt during a comparison. This scenario does not use the offloader, `placement-controller` or the Envoys. After any rebuild, section B3 shows the digest each pod really runs; write it down next to the results.

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
Each image is pushed twice: as `latest` and as an immutable `build-<timestamp>` tag, so you can always say which build ran. The base images (`python:3.12-slim`) are pulled from the internet, so core needs outbound access.

Collector (build context = repo root):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export TS=$(date -u +%Y%m%dT%H%M%SZ) R=localhost:30500
docker build --network=host -f collector/Dockerfile -t $R/ue-telemetry-collector:latest -t $R/ue-telemetry-collector:build-$TS .
docker push $R/ue-telemetry-collector:latest
docker push $R/ue-telemetry-collector:build-$TS
echo built ue-telemetry-collector:build-$TS
```

### A3. Verify what is in the registry now

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
for r in ue-telemetry-collector; do
  echo "$r latest = $(curl -s -I -H 'Accept: application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json' http://localhost:30500/v2/$r/manifests/latest | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest"{print $2}')"
  curl -s http://localhost:30500/v2/$r/tags/list; echo
done
```

The `latest` digest must now differ from the one in the table and the newest `build-...` tag must be listed. Section B deploys these images; section B3 shows the digest each pod really runs.

### A4. Go back to the exact final-run images (only if you need them)
Run this *after* section B. It pins the running Deployments to the images the results were produced with (the old tags still exist in the registry):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement set image deployment/ue-telemetry collector=10.34.211.140:30500/ue-telemetry-collector:prune-20261121T152448Z
kubectl -n placement rollout status deployment/ue-telemetry --timeout=180s
```

## B. Platform: remove the old one and deploy from scratch
This scenario needs telemetry (node-exporter, Prometheus, adapter) and the collector. It does **not** use `placement-controller`, the offloader, the Envoys or the Core backend. The arm sections below assume it exists; this section builds it. All `apply` commands are idempotent, so skip B1 if you only want to make sure everything is present.

### B1. Remove the old platform (optional, only to start from nothing)
Prometheus keeps its history inside the pod, so this drops it. Reverse order, so nothing points at something already gone. Finish or clean up any running arm first.

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
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

If the collector was already running and you rebuilt it in section A, restart it so it pulls the new `:latest`:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement rollout restart deployment/ue-telemetry
kubectl -n placement rollout status deployment/ue-telemetry --timeout=180s
```

### B3. Check
Give Prometheus about a minute to scrape before the first arm; the adapter metrics only appear once the `ue-telemetry` and `kubelet-cadvisor` jobs have data.

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement get deploy,svc
kubectl get apiservice v1beta1.external.metrics.k8s.io
kubectl -n placement get pods -o custom-columns=POD:.metadata.name,IMAGE:.spec.containers[0].image,IMAGEID:.status.containerStatuses[0].imageID
```

Expect `1/1` for `placement-prometheus`, `prometheus-adapter`, `ue-telemetry` and APIService `True`.

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

This scenario runs on `core-5g` only with the default scheduler: **no zone labels, no offloader, no anchors, no placement controller config.**

---

## 1. `baseline`: native CPU HPA (80%), default scheduler, core-5g only
Files: `deploy/final/scaling/baseline/`. Run in terminal **T1** on core (see the setup section) unless stated.

#### 1. Start from a clean slate (what the campaign does before every arm)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Prometheus adapter rules

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/scaling/baseline/01-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 3. Workload (managed Deployment + Service)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/scaling/baseline/02-workload.yaml
kubectl -n $NS rollout status deployment/$APP --timeout=240s
```

#### 4. Autoscaler

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/scaling/baseline/03-hpa.yaml
kubectl -n $NS get hpa
```

#### 5. Routing check before traffic

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'
kubectl -n $NS get pods -o wide
```

#### 6. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=manual_scaling_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
echo "$CAMP" > /tmp/final-camp                              # later blocks (abort, collect) read it from here
PORT=$(kubectl -n $NS get svc $APP -o jsonpath='{.spec.ports[0].nodePort}')
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/scaling/baseline/ue-payload.json > /tmp/ue-payload.json
# the check must pass before traffic starts (&&), then this terminal blocks for the whole run
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json \
  && $UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,9:3:3:3,15:5:5:5,21:7:7:7,24:8:8:8,12:4:4:4,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`. Open a second terminal (T4) for the next step.

#### 7. While the UE runs (terminal T4 on core). Each block is independent; the `-w` ones stay open until Ctrl-C
HPA decisions (stays open):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get hpa -w
```

Pod placement (stays open):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get pods -o wide -w
```

Abort the UE early:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
$UE "cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign $CAMP --method baseline"
```

#### 8. After the UE finishes: collect and clean up

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

---

## 2. `proposed`: latency HPA (excess over 120 ms, target 30m), default scheduler, core-5g only
Files: `deploy/final/scaling/proposed/`. Run in terminal **T1** on core (see the setup section) unless stated.

#### 1. Start from a clean slate (what the campaign does before every arm)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Prometheus adapter rules

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/scaling/proposed/01-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 3. Workload (managed Deployment + Service)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/scaling/proposed/02-workload.yaml
kubectl -n $NS rollout status deployment/$APP --timeout=240s
```

#### 4. Autoscaler

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/scaling/proposed/03-hpa.yaml
kubectl -n $NS get hpa
```

#### 5. Routing check before traffic

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'
kubectl -n $NS get pods -o wide
```

#### 6. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=manual_scaling_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
echo "$CAMP" > /tmp/final-camp                              # later blocks (abort, collect) read it from here
PORT=$(kubectl -n $NS get svc $APP -o jsonpath='{.spec.ports[0].nodePort}')
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/scaling/proposed/ue-payload.json > /tmp/ue-payload.json
# the check must pass before traffic starts (&&), then this terminal blocks for the whole run
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json \
  && $UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,9:3:3:3,15:5:5:5,21:7:7:7,24:8:8:8,12:4:4:4,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`. Open a second terminal (T4) for the next step.

#### 7. While the UE runs (terminal T4 on core). Each block is independent; the `-w` ones stay open until Ctrl-C
Latency metric visible to the HPA (should appear within ~90 s of `[RUN]`; prints a value and returns):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
for i in $(seq 1 24); do kubectl get --raw /apis/external.metrics.k8s.io/v1beta1/namespaces/$NS/ue_p90_excess_latency_120ms_seconds | jq -e '.items[0].value' && break; sleep 5; done
```

HPA decisions (stays open):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get hpa -w
```

Pod placement (stays open):

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get pods -o wide -w
```

Abort the UE early:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
$UE "cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign $CAMP --method proposed"
```

#### 8. After the UE finishes: collect and clean up

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=$(cat /tmp/final-camp)
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

---

## Scenario teardown

Stop the tc helper in T2 (Ctrl-C removes the delay). The telemetry stack and collector stay; remove them with section B1 if you want a bare cluster. Check the zones were not touched:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl get nodes -L topology.kubernetes.io/zone
```

