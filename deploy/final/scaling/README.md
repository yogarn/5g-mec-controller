# Scaling experiment (autoscaler only)
Scaling staircase: 7 phases x 120 s, schedule `3:1:1:1,9:3:3:3,15:5:5:5,21:7:7:7,24:8:8:8,12:4:4:4,3:1:1:1`, `core-5g` only, default scheduler for both arms. `baseline` uses the native CPU HPA (80%), `proposed` the latency HPA (excess over 120 ms, target 30m).

Terminals (all on core unless noted): **T1** = commands, **T2** = tc delay, **T4** = monitoring.

**Run this first in EVERY terminal (T1, T2, T3, T4); it sets the project directory, `KUBECONFIG` and the variables used below:**

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
```

Run one arm at a time, in this order: `baseline`, `proposed`. Each arm section is self-contained.

## Platform (must already be running)
These sections only add and remove the per-arm pieces. The platform (telemetry, collector, `placement-controller`, token Secret) is deployed once, see [`../platform/README.md`](../platform/README.md) to check it, tear it down or rebuild it from nothing. Quick check:

```bash
kubectl -n placement get deploy
kubectl get apiservice v1beta1.external.metrics.k8s.io
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
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Prometheus adapter rules

```bash
kubectl apply -f deploy/final/scaling/baseline/01-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 3. Workload (managed Deployment + Service)

```bash
kubectl apply -f deploy/final/scaling/baseline/02-workload.yaml
kubectl -n $NS rollout status deployment/$APP --timeout=240s
```

#### 4. Autoscaler

```bash
kubectl apply -f deploy/final/scaling/baseline/03-hpa.yaml
kubectl -n $NS get hpa
```

#### 5. Routing check before traffic

```bash
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'
kubectl -n $NS get pods -o wide
```

#### 6. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
export CAMP=manual_scaling_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
PORT=$(kubectl -n $NS get svc $APP -o jsonpath='{.spec.ports[0].nodePort}')
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/scaling/baseline/ue-payload.json > /tmp/ue-payload.json
UE="ssh -o BatchMode=yes -o ConnectTimeout=10 member@10.34.211.6"
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json
# when the check prints "UE prerequisites ready", start traffic (open a second terminal T4 now):
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,9:3:3:3,15:5:5:5,21:7:7:7,24:8:8:8,12:4:4:4,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`.

#### 7. While the UE runs (terminal T4 on core, after `source .../env.sh`)

```bash
kubectl -n $NS get hpa -w            # HPA decisions
kubectl -n $NS get pods -o wide -w    # pod placement (Ctrl-C to stop watching)
```

To abort the UE early: `ssh member@10.34.211.6 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign '"$CAMP"' --method baseline'`.

#### 8. After the UE finishes: collect and clean up

```bash
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
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Prometheus adapter rules

```bash
kubectl apply -f deploy/final/scaling/proposed/01-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 3. Workload (managed Deployment + Service)

```bash
kubectl apply -f deploy/final/scaling/proposed/02-workload.yaml
kubectl -n $NS rollout status deployment/$APP --timeout=240s
```

#### 4. Autoscaler

```bash
kubectl apply -f deploy/final/scaling/proposed/03-hpa.yaml
kubectl -n $NS get hpa
```

#### 5. Routing check before traffic

```bash
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'
kubectl -n $NS get pods -o wide
```

#### 6. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
export CAMP=manual_scaling_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
PORT=$(kubectl -n $NS get svc $APP -o jsonpath='{.spec.ports[0].nodePort}')
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/scaling/proposed/ue-payload.json > /tmp/ue-payload.json
UE="ssh -o BatchMode=yes -o ConnectTimeout=10 member@10.34.211.6"
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json
# when the check prints "UE prerequisites ready", start traffic (open a second terminal T4 now):
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,9:3:3:3,15:5:5:5,21:7:7:7,24:8:8:8,12:4:4:4,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`.

#### 7. While the UE runs (terminal T4 on core, after `source .../env.sh`)

```bash
# the latency metric must show a value within ~90 s of traffic starting:
until kubectl get --raw /apis/external.metrics.k8s.io/v1beta1/namespaces/$NS/ue_p90_excess_latency_120ms_seconds | jq -e '.items[0].value'; do sleep 5; done
kubectl -n $NS get hpa -w            # HPA decisions
kubectl -n $NS get pods -o wide -w    # pod placement (Ctrl-C to stop watching)
```

To abort the UE early: `ssh member@10.34.211.6 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign '"$CAMP"' --method proposed'`.

#### 8. After the UE finishes: collect and clean up

```bash
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

---

## Scenario teardown

```bash
# now stop the tc helper in T2 with Ctrl-C (removes the delay) and, for joint/mobility, the port-forward in T3
kubectl get nodes -L topology.kubernetes.io/zone
```

