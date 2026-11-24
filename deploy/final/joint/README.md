# Joint experiment (placement + autoscaling + offloading)
Joint scenario: 7 phases x 120 s, 3 to 24 UEs, schedule `3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1`. Local arms use no offloading; vertical arms may offload to the Core pool (3 replicas). Baseline arms use an external CPU HPA, proposed arms the latency HPA.

Terminals (all on core unless noted): **T1** = commands, **T2** = tc delay, **T3** = offloader port-forward, **T4** = monitoring.

**Every code block below starts with `source .../deploy/final/env.sh`** (project directory, `KUBECONFIG`, `NS`, `APP`, `A`, `H`, `UE`), so a block can be pasted into any terminal on core, in any order. State shared between blocks lives in files (`/tmp/final-camp`, `/tmp/ue-payload.json`), not in shell variables.

Run one arm at a time, in this order: `baseline-local`, `proposed-local`, `baseline-vertical`, `proposed-vertical`. Each arm section is self-contained.

## Platform (must already be running)
These sections only add and remove the per-arm pieces. The platform (telemetry, collector, `placement-controller`, `vertical-offloader`, Envoys, Core backend, token Secret) is deployed once, see [`../platform/README.md`](../platform/README.md) to check it, tear it down or rebuild it from nothing. Quick check:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n placement get deploy
kubectl -n services get deploy | grep -E "envoy|core"
kubectl get apiservice v1beta1.external.metrics.k8s.io
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
Files: `deploy/final/joint/baseline-local/`. Run in terminal **T1** on core (see the setup section) unless stated.

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

#### 2. Prometheus adapter rules

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/baseline-local/01-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 3. Edge anchors (one pod pinned on each edge node)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/baseline-local/02-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 4. Workload (managed Deployment + Service)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/baseline-local/03-workload.yaml
# seed the managed Deployment to the HPA minimum (1) before traffic (the HPA does not act at 0 replicas)
kubectl -n $NS scale deployment/$APP --current-replicas=0 --replicas=1
kubectl -n $NS rollout status deployment/$APP --timeout=240s
# Alternative that matches the recorded final runs exactly (they started with 0 dynamic pods):
# skip the two lines above and run the scale line once the UE log shows [RUN]
```

#### 5. Autoscaler

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
# the baseline HPA reads an external metric; it must exist before the HPA is created (campaign waits up to 45 s)
for i in $(seq 1 15); do
  kubectl get --raw /apis/external.metrics.k8s.io/v1beta1/namespaces/$NS/mec_baseline_cpu_dynamic_replicas_fixed3 | jq -e '.items[0].value' && break
  sleep 3
done
kubectl apply -f deploy/final/joint/baseline-local/04-hpa.yaml
kubectl -n $NS get hpa
```

#### 6. Routing check before traffic

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 7. Offloader and Core fallback pool (local arm: Core pool 0, offloader stays local-only)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
# reset every site to LOCAL and wait for the Envoy ACKs
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# Core fallback pool: 0 replicas
kubectl -n $NS scale deployment/$APP-core --replicas=0
kubectl -n $NS wait --for=delete pod -l app=$APP-core --timeout=180s 2>/dev/null || true
# wait until the offloader sees 0 Core endpoints at every site
for i in $(seq 1 90); do [ "$(curl -s -H "$H" $A/status | jq -r '[.sites[].core_ready_endpoints]|unique|join(",")')" = "0" ] && break; sleep 2; done
[ "$i" -lt 90 ] || echo "TIMEOUT: offloader never reported 0 core endpoints at every site (is the T3 port-forward running?)"
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

#### 8. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=manual_joint_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
echo "$CAMP" > /tmp/final-camp                              # later blocks (abort, collect) read it from here
PORT=$(kubectl -n $NS get svc $APP -o jsonpath='{.spec.ports[0].nodePort}')
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/joint/baseline-local/ue-payload.json > /tmp/ue-payload.json
# the check must pass before traffic starts (&&), then this terminal blocks for the whole run
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json \
  && $UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`. Open a second terminal (T4) for the next step.

#### 9. While the UE runs (terminal T4 on core). Each block is independent; the `-w` ones stay open until Ctrl-C
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

#### 10. After the UE finishes: collect and clean up

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
Files: `deploy/final/joint/proposed-local/`. Run in terminal **T1** on core (see the setup section) unless stated.

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
kubectl apply -f deploy/final/joint/proposed-local/01-placement-configmap.yaml
kubectl -n placement rollout restart deployment/placement-controller
kubectl -n placement rollout status deployment/placement-controller --timeout=120s
kubectl -n placement get deployment placement-controller
```

#### 3. Prometheus adapter rules

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/proposed-local/02-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 4. Edge anchors (one pod pinned on each edge node)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/proposed-local/03-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 5. Workload (managed Deployment + Service)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/proposed-local/04-workload.yaml
# seed the managed Deployment to the HPA minimum (1) before traffic (the HPA does not act at 0 replicas)
kubectl -n $NS scale deployment/$APP --current-replicas=0 --replicas=1
kubectl -n $NS rollout status deployment/$APP --timeout=240s
# Alternative that matches the recorded final runs exactly (they started with 0 dynamic pods):
# skip the two lines above and run the scale line once the UE log shows [RUN]
```

#### 6. Autoscaler

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/proposed-local/05-hpa.yaml
kubectl -n $NS get hpa
```

#### 7. Routing check before traffic

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 8. Offloader and Core fallback pool (local arm: Core pool 0, offloader stays local-only)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
# reset every site to LOCAL and wait for the Envoy ACKs
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# Core fallback pool: 0 replicas
kubectl -n $NS scale deployment/$APP-core --replicas=0
kubectl -n $NS wait --for=delete pod -l app=$APP-core --timeout=180s 2>/dev/null || true
# wait until the offloader sees 0 Core endpoints at every site
for i in $(seq 1 90); do [ "$(curl -s -H "$H" $A/status | jq -r '[.sites[].core_ready_endpoints]|unique|join(",")')" = "0" ] && break; sleep 2; done
[ "$i" -lt 90 ] || echo "TIMEOUT: offloader never reported 0 core endpoints at every site (is the T3 port-forward running?)"
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

#### 9. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=manual_joint_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
echo "$CAMP" > /tmp/final-camp                              # later blocks (abort, collect) read it from here
PORT=$(kubectl -n $NS get svc $APP -o jsonpath='{.spec.ports[0].nodePort}')
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/joint/proposed-local/ue-payload.json > /tmp/ue-payload.json
# the check must pass before traffic starts (&&), then this terminal blocks for the whole run
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json \
  && $UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`. Open a second terminal (T4) for the next step.

#### 10. While the UE runs (terminal T4 on core). Each block is independent; the `-w` ones stay open until Ctrl-C
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

#### 11. After the UE finishes: collect and clean up

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
Files: `deploy/final/joint/baseline-vertical/`. Run in terminal **T1** on core (see the setup section) unless stated.

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

#### 2. Prometheus adapter rules

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/baseline-vertical/01-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 3. Edge anchors (one pod pinned on each edge node)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/baseline-vertical/02-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 4. Workload (managed Deployment + Service)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/baseline-vertical/03-workload.yaml
# seed the managed Deployment to the HPA minimum (1) before traffic (the HPA does not act at 0 replicas)
kubectl -n $NS scale deployment/$APP --current-replicas=0 --replicas=1
kubectl -n $NS rollout status deployment/$APP --timeout=240s
# Alternative that matches the recorded final runs exactly (they started with 0 dynamic pods):
# skip the two lines above and run the scale line once the UE log shows [RUN]
```

#### 5. Autoscaler

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
# the baseline HPA reads an external metric; it must exist before the HPA is created (campaign waits up to 45 s)
for i in $(seq 1 15); do
  kubectl get --raw /apis/external.metrics.k8s.io/v1beta1/namespaces/$NS/mec_baseline_cpu_dynamic_replicas_fixed6 | jq -e '.items[0].value' && break
  sleep 3
done
kubectl apply -f deploy/final/joint/baseline-vertical/04-hpa.yaml
kubectl -n $NS get hpa
```

#### 6. Routing check before traffic

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 7. Offloader and Core fallback pool (vertical arm: Core pool 3, then switch to vertical)

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
[ "$i" -lt 90 ] || echo "TIMEOUT: offloader never reported 3 core endpoints at every site (is the T3 port-forward running?)"
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Only if every site shows `active_route: LOCAL`, `local_ready_endpoints >= 1` and `core_ready_endpoints: 3` (no TIMEOUT above), activate vertical mode:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"vertical"}' $A/mode | jq .
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Expect `mode: vertical`.

#### 8. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=manual_joint_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
echo "$CAMP" > /tmp/final-camp                              # later blocks (abort, collect) read it from here
PORT=0   # unused: vertical arms use the fixed Envoy NodePorts 31801-31803
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/joint/baseline-vertical/ue-payload.json > /tmp/ue-payload.json
# the check must pass before traffic starts (&&), then this terminal blocks for the whole run
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json \
  && $UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`. Open a second terminal (T4) for the next step.

#### 9. While the UE runs (terminal T4 on core). Each block is independent; the `-w` ones stay open until Ctrl-C
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

#### 10. After the UE finishes: collect and clean up

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
Files: `deploy/final/joint/proposed-vertical/`. Run in terminal **T1** on core (see the setup section) unless stated.

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
kubectl apply -f deploy/final/joint/proposed-vertical/01-placement-configmap.yaml
kubectl -n placement rollout restart deployment/placement-controller
kubectl -n placement rollout status deployment/placement-controller --timeout=120s
kubectl -n placement get deployment placement-controller
```

#### 3. Prometheus adapter rules

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/proposed-vertical/02-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 4. Edge anchors (one pod pinned on each edge node)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/proposed-vertical/03-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 5. Workload (managed Deployment + Service)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/proposed-vertical/04-workload.yaml
# seed the managed Deployment to the HPA minimum (1) before traffic (the HPA does not act at 0 replicas)
kubectl -n $NS scale deployment/$APP --current-replicas=0 --replicas=1
kubectl -n $NS rollout status deployment/$APP --timeout=240s
# Alternative that matches the recorded final runs exactly (they started with 0 dynamic pods):
# skip the two lines above and run the scale line once the UE log shows [RUN]
```

#### 6. Autoscaler

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl apply -f deploy/final/joint/proposed-vertical/05-hpa.yaml
kubectl -n $NS get hpa
```

#### 7. Routing check before traffic

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 8. Offloader and Core fallback pool (vertical arm: Core pool 3, then switch to vertical)

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
[ "$i" -lt 90 ] || echo "TIMEOUT: offloader never reported 3 core endpoints at every site (is the T3 port-forward running?)"
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Only if every site shows `active_route: LOCAL`, `local_ready_endpoints >= 1` and `core_ready_endpoints: 3` (no TIMEOUT above), activate vertical mode:

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"vertical"}' $A/mode | jq .
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

Expect `mode: vertical`.

#### 9. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
export CAMP=manual_joint_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
echo "$CAMP" > /tmp/final-camp                              # later blocks (abort, collect) read it from here
PORT=0   # unused: vertical arms use the fixed Envoy NodePorts 31801-31803
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/joint/proposed-vertical/ue-payload.json > /tmp/ue-payload.json
# the check must pass before traffic starts (&&), then this terminal blocks for the whole run
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json \
  && $UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`. Open a second terminal (T4) for the next step.

#### 10. While the UE runs (terminal T4 on core). Each block is independent; the `-w` ones stay open until Ctrl-C
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

#### 11. After the UE finishes: collect and clean up

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

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
# offloader to LOCAL first, then restore the Core pool to its original 3 replicas (as the campaign does at the very end)
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl -n $NS scale deployment/$APP-core --replicas=3
kubectl -n $NS rollout status deployment/$APP-core --timeout=180s
kubectl -n $NS get deployment $APP-core    # expect 3/3
# now stop the tc helper in T2 with Ctrl-C (removes the delay) and, for joint/mobility, the port-forward in T3
kubectl get nodes -L topology.kubernetes.io/zone
```

