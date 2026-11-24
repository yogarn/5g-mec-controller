# Scaling experiment (autoscaler only)
Scaling staircase: 7 phases x 120 s, schedule `3:1:1:1,9:3:3:3,15:5:5:5,21:7:7:7,24:8:8:8,12:4:4:4,3:1:1:1`, `core-5g` only, default scheduler for both arms. `baseline` uses the native CPU HPA (80%), `proposed` the latency HPA (excess over 120 ms, target 30m).

Terminals (all on core unless noted): **T1** = commands, **T2** = tc delay, **T4** = monitoring.

**Every code block below starts with `source .../deploy/final/env.sh`** (project directory, `KUBECONFIG`, `NS`, `APP`, `A`, `H`, `UE`), so a block can be pasted into any terminal on core, in any order. State shared between blocks lives in files (`/tmp/final-camp`, `/tmp/ue-payload.json`), not in shell variables.

Run one arm at a time, in this order: `baseline`, `proposed`. Each arm section is self-contained.

## 0. Platform: remove the old one and deploy from scratch
This scenario needs telemetry (node-exporter, Prometheus, adapter) and the collector. It does **not** use `placement-controller`, the offloader, the Envoys or the Core backend. The arm sections below assume it exists; this section builds it. All `apply` commands are idempotent, so skip 0a if you only want to make sure everything is present.

### 0a. Remove the old platform (optional, only to start from nothing)
Prometheus keeps its history inside the pod, so this drops it. Reverse order, so nothing points at something already gone. Finish or clean up any running arm first.

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl delete -f collector/manifest.yaml --ignore-not-found=true
kubectl delete -f telemetry/05-prometheus-adapter.yaml --ignore-not-found=true
kubectl delete -f telemetry/03-prometheus.yaml --ignore-not-found=true
kubectl delete -f telemetry/01-node-exporter.yaml --ignore-not-found=true
```

The namespaces and the `ue-telemetry-token` Secret are kept.

### 0b. Deploy
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

### 0c. Check
Give Prometheus about a minute to scrape before the first arm; the adapter metrics only appear once the `ue-telemetry` and `kubelet-cadvisor` jobs have data.

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement get deploy,svc
kubectl get apiservice v1beta1.external.metrics.k8s.io
```

Expect `1/1` for `placement-prometheus`, `prometheus-adapter`, `ue-telemetry` and APIService `True`.

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

Stop the tc helper in T2 (Ctrl-C removes the delay). The telemetry stack and collector stay; remove them with section 0a if you want a bare cluster. Check the zones were not touched:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl get nodes -L topology.kubernetes.io/zone
```

