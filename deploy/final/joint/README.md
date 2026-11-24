# Joint experiment (placement + autoscaling + offloading)
Joint scenario: 7 phases x 120 s, 3 to 24 UEs, schedule `3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1`. Local arms use no offloading; vertical arms may offload to the Core pool (3 replicas). Baseline arms use an external CPU HPA, proposed arms the latency HPA.

Terminals (all on core unless noted): **T1** = commands, **T2** = tc delay, **T3** = offloader port-forward, **T4** = monitoring.

**Paste this in every terminal that runs `kubectl` or `curl` (T1 and T4):**

```bash
cd /home/member/skripsi_yoga/5g-mec-controller
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
export NS=services APP=mec-placement-pedestrian-detection
export A=http://127.0.0.1:18086
export TOKEN=$(cat /home/member/.config/5g-mec/ue-telemetry-token)
export H="Authorization: Bearer $TOKEN"
```

Run one arm at a time, in this order: `baseline-local`, `proposed-local`, `baseline-vertical`, `proposed-vertical`. Each arm section is self-contained.

## Scenario setup (once per scenario, before the first arm)
**T2 (core), keep open for the whole scenario:** inter-node path delay of 10 ms for the experiment pods (`tc_ms: 10`, as in every final run).

```bash
cd /home/member/skripsi_yoga/5g-mec-controller
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
.venv/bin/python deploy/final/tools/hold_tc_delay.py 10
# prints "tc delay 10 ms active"; Ctrl-C at the very end removes it
```

**PreferClose zones** (T1): each edge node is its own zone so `trafficDistribution: PreferClose` keeps traffic on the same node. The final runs recorded these labels as already set before the run (`node-zones-before.yaml`), so this is idempotent.

```bash
kubectl label node gnb1 topology.kubernetes.io/zone=gnb1 --overwrite
kubectl label node gnb-2 topology.kubernetes.io/zone=gnb-2 --overwrite
kubectl label node gnb-3 topology.kubernetes.io/zone=gnb-3 --overwrite
kubectl get nodes -L topology.kubernetes.io/zone
```

**Offloader admin API** (**T3**, core, keep open): the offloader is used by all four arms.

```bash
kubectl -n placement port-forward --address 127.0.0.1 svc/vertical-offloader-admin 18086:8086
```

Check from T1:

```bash
curl -s $A/health; echo
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

The Core fallback pool was 3 replicas before the final runs. Record it now:

```bash
kubectl -n $NS get deployment $APP-core -o jsonpath='{.spec.replicas}{"\n"}'   # expect 3
```

---

## 1. `baseline-local`: default scheduler, all replicas local, no offload
Files: `deploy/final/joint/baseline-local/`. Run in terminal **T1** on core (see the setup section) unless stated.

#### 1. Start from a clean slate (what the campaign does before every arm)

```bash
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Prometheus adapter rules

```bash
kubectl apply -f deploy/final/joint/baseline-local/01-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 3. Edge anchors (one pod pinned on each edge node)

```bash
kubectl apply -f deploy/final/joint/baseline-local/02-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 4. Workload (managed Deployment + Service)

```bash
kubectl apply -f deploy/final/joint/baseline-local/03-workload.yaml
# replicas: 0 on purpose; it is seeded after traffic starts (step "While the UE runs")
```

#### 5. Autoscaler

```bash
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
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 7. Offloader and Core fallback pool (local arm: Core pool 0, offloader stays local-only)

```bash
# reset every site to LOCAL and wait for the Envoy ACKs
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# Core fallback pool: 0 replicas
kubectl -n $NS scale deployment/$APP-core --replicas=0
kubectl -n $NS wait --for=delete pod -l app=$APP-core --timeout=180s 2>/dev/null || true
# wait until the offloader sees 0 Core endpoints at every site
until [ "$(curl -s -H "$H" $A/status | jq -r '[.sites[].core_ready_endpoints]|unique|join(",")')" = "0" ]; do sleep 2; done
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

#### 8. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
export CAMP=manual_joint_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
PORT=$(kubectl -n $NS get svc $APP -o jsonpath='{.spec.ports[0].nodePort}')
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/joint/baseline-local/ue-payload.json > /tmp/ue-payload.json
UE="ssh -o BatchMode=yes -o ConnectTimeout=10 member@10.34.211.6"
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json
# when the check prints "UE prerequisites ready", start traffic (open a second terminal T4 now):
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`.

#### 9. While the UE runs (terminal T4 on core, same environment block)

```bash
# a) as soon as the UE log shows [RUN], seed the managed Deployment up to the HPA minimum (1):
kubectl -n $NS scale deployment/$APP --current-replicas=0 --replicas=1
kubectl -n $NS get hpa -w            # HPA decisions
kubectl -n $NS get pods -o wide -w    # pod placement (Ctrl-C to stop watching)
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'   # offload routes per site
```

To abort the UE early: `ssh member@10.34.211.6 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign '"$CAMP"' --method baseline-local'`.

#### 10. After the UE finishes: collect and clean up

```bash
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl -n $NS scale deployment/$APP-core --replicas=3    # the pool was 3 before the run
```

---

## 2. `proposed-local`: placement scheduler + descheduler + latency HPA, no offload
Files: `deploy/final/joint/proposed-local/`. Run in terminal **T1** on core (see the setup section) unless stated.

#### 1. Start from a clean slate (what the campaign does before every arm)

```bash
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Placement scheduler/descheduler config (in-cluster Go controller)

```bash
kubectl apply -f deploy/final/joint/proposed-local/01-placement-configmap.yaml
kubectl -n placement rollout restart deployment/placement-controller
kubectl -n placement rollout status deployment/placement-controller --timeout=120s
kubectl -n placement get deployment placement-controller
```

#### 3. Prometheus adapter rules

```bash
kubectl apply -f deploy/final/joint/proposed-local/02-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 4. Edge anchors (one pod pinned on each edge node)

```bash
kubectl apply -f deploy/final/joint/proposed-local/03-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 5. Workload (managed Deployment + Service)

```bash
kubectl apply -f deploy/final/joint/proposed-local/04-workload.yaml
# replicas: 0 on purpose; it is seeded after traffic starts (step "While the UE runs")
```

#### 6. Autoscaler

```bash
kubectl apply -f deploy/final/joint/proposed-local/05-hpa.yaml
kubectl -n $NS get hpa
```

#### 7. Routing check before traffic

```bash
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 8. Offloader and Core fallback pool (local arm: Core pool 0, offloader stays local-only)

```bash
# reset every site to LOCAL and wait for the Envoy ACKs
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# Core fallback pool: 0 replicas
kubectl -n $NS scale deployment/$APP-core --replicas=0
kubectl -n $NS wait --for=delete pod -l app=$APP-core --timeout=180s 2>/dev/null || true
# wait until the offloader sees 0 Core endpoints at every site
until [ "$(curl -s -H "$H" $A/status | jq -r '[.sites[].core_ready_endpoints]|unique|join(",")')" = "0" ]; do sleep 2; done
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
```

#### 9. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
export CAMP=manual_joint_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
PORT=$(kubectl -n $NS get svc $APP -o jsonpath='{.spec.ports[0].nodePort}')
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/joint/proposed-local/ue-payload.json > /tmp/ue-payload.json
UE="ssh -o BatchMode=yes -o ConnectTimeout=10 member@10.34.211.6"
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json
# when the check prints "UE prerequisites ready", start traffic (open a second terminal T4 now):
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`.

#### 10. While the UE runs (terminal T4 on core, same environment block)

```bash
# a) as soon as the UE log shows [RUN], seed the managed Deployment up to the HPA minimum (1):
kubectl -n $NS scale deployment/$APP --current-replicas=0 --replicas=1
# b) the latency metric must show a value within ~90 s of traffic starting:
until kubectl get --raw /apis/external.metrics.k8s.io/v1beta1/namespaces/$NS/ue_p90_excess_latency_120ms_seconds | jq -e '.items[0].value'; do sleep 5; done
kubectl -n $NS get hpa -w            # HPA decisions
kubectl -n $NS get pods -o wide -w    # pod placement (Ctrl-C to stop watching)
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'   # offload routes per site
kubectl -n placement logs deployment/placement-controller --tail=30   # scheduler/descheduler decisions
```

To abort the UE early: `ssh member@10.34.211.6 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign '"$CAMP"' --method proposed-local'`.

#### 11. After the UE finishes: collect and clean up

```bash
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl -n $NS scale deployment/$APP-core --replicas=3    # the pool was 3 before the run
```

---

## 3. `baseline-vertical`: default scheduler, offload to the Core pool allowed
Files: `deploy/final/joint/baseline-vertical/`. Run in terminal **T1** on core (see the setup section) unless stated.

#### 1. Start from a clean slate (what the campaign does before every arm)

```bash
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Prometheus adapter rules

```bash
kubectl apply -f deploy/final/joint/baseline-vertical/01-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 3. Edge anchors (one pod pinned on each edge node)

```bash
kubectl apply -f deploy/final/joint/baseline-vertical/02-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 4. Workload (managed Deployment + Service)

```bash
kubectl apply -f deploy/final/joint/baseline-vertical/03-workload.yaml
# replicas: 0 on purpose; it is seeded after traffic starts (step "While the UE runs")
```

#### 5. Autoscaler

```bash
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
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 7. Offloader and Core fallback pool (vertical arm: Core pool 3, then switch to vertical)

```bash
# reset every site to LOCAL and wait for the Envoy ACKs
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# Core fallback pool: 3 replicas
kubectl -n $NS scale deployment/$APP-core --replicas=3
kubectl -n $NS rollout status deployment/$APP-core --timeout=180s
kubectl -n $NS get pods -l app=$APP-core -o wide    # expect 3 Ready on core-5g
# wait until the offloader sees 3 Core endpoints at every site
until [ "$(curl -s -H "$H" $A/status | jq -r '[.sites[].core_ready_endpoints]|unique|join(",")')" = "3" ]; do sleep 2; done
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
# readiness is satisfied when every site shows active_route LOCAL, local_ready_endpoints >= 1 and core_ready_endpoints = 3.
# only then activate vertical mode:
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"vertical"}' $A/mode | jq .
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'   # expect mode: vertical
```

#### 8. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
export CAMP=manual_joint_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
PORT=0   # unused: vertical arms use the fixed Envoy NodePorts 31801-31803
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/joint/baseline-vertical/ue-payload.json > /tmp/ue-payload.json
UE="ssh -o BatchMode=yes -o ConnectTimeout=10 member@10.34.211.6"
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json
# when the check prints "UE prerequisites ready", start traffic (open a second terminal T4 now):
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`.

#### 9. While the UE runs (terminal T4 on core, same environment block)

```bash
# a) as soon as the UE log shows [RUN], seed the managed Deployment up to the HPA minimum (1):
kubectl -n $NS scale deployment/$APP --current-replicas=0 --replicas=1
kubectl -n $NS get hpa -w            # HPA decisions
kubectl -n $NS get pods -o wide -w    # pod placement (Ctrl-C to stop watching)
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'   # offload routes per site
```

To abort the UE early: `ssh member@10.34.211.6 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign '"$CAMP"' --method baseline-vertical'`.

#### 10. After the UE finishes: collect and clean up

```bash
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl -n $NS scale deployment/$APP-core --replicas=3    # the pool was 3 before the run
```

---

## 4. `proposed-vertical`: placement scheduler + descheduler + latency HPA + offload
Files: `deploy/final/joint/proposed-vertical/`. Run in terminal **T1** on core (see the setup section) unless stated.

#### 1. Start from a clean slate (what the campaign does before every arm)

```bash
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
```

#### 2. Placement scheduler/descheduler config (in-cluster Go controller)

```bash
kubectl apply -f deploy/final/joint/proposed-vertical/01-placement-configmap.yaml
kubectl -n placement rollout restart deployment/placement-controller
kubectl -n placement rollout status deployment/placement-controller --timeout=120s
kubectl -n placement get deployment placement-controller
```

#### 3. Prometheus adapter rules

```bash
kubectl apply -f deploy/final/joint/proposed-vertical/02-adapter-configmap.yaml
kubectl -n placement rollout restart deployment/prometheus-adapter
kubectl -n placement rollout status deployment/prometheus-adapter --timeout=120s
```

#### 4. Edge anchors (one pod pinned on each edge node)

```bash
kubectl apply -f deploy/final/joint/proposed-vertical/03-site-anchors.yaml
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-1 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-2 --timeout=180s
kubectl -n $NS rollout status deployment/$APP-anchor-gnb-3 --timeout=180s
```

#### 5. Workload (managed Deployment + Service)

```bash
kubectl apply -f deploy/final/joint/proposed-vertical/04-workload.yaml
# replicas: 0 on purpose; it is seeded after traffic starts (step "While the UE runs")
```

#### 6. Autoscaler

```bash
kubectl apply -f deploy/final/joint/proposed-vertical/05-hpa.yaml
kubectl -n $NS get hpa
```

#### 7. Routing check before traffic

```bash
kubectl -n $NS get svc $APP -o jsonpath='{.spec.trafficDistribution}{"\n"}'   # expect: PreferClose
kubectl -n $NS get pods -o wide
```

#### 8. Offloader and Core fallback pool (vertical arm: Core pool 3, then switch to vertical)

```bash
# reset every site to LOCAL and wait for the Envoy ACKs
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
# Core fallback pool: 3 replicas
kubectl -n $NS scale deployment/$APP-core --replicas=3
kubectl -n $NS rollout status deployment/$APP-core --timeout=180s
kubectl -n $NS get pods -l app=$APP-core -o wide    # expect 3 Ready on core-5g
# wait until the offloader sees 3 Core endpoints at every site
until [ "$(curl -s -H "$H" $A/status | jq -r '[.sites[].core_ready_endpoints]|unique|join(",")')" = "3" ]; do sleep 2; done
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'
# readiness is satisfied when every site shows active_route LOCAL, local_ready_endpoints >= 1 and core_ready_endpoints = 3.
# only then activate vertical mode:
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"vertical"}' $A/mode | jq .
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'   # expect mode: vertical
```

#### 9. UE: prerequisites check, then run (blocks for the whole schedule)

```bash
export CAMP=manual_joint_$(date -u +%Y%m%dT%H%M%SZ)   # new name per run, results land in results/$CAMP on the UE host
PORT=0   # unused: vertical arms use the fixed Envoy NodePorts 31801-31803
jq --arg c "$CAMP" --arg p "$PORT" '.campaign=$c | .site_urls |= with_entries(.value |= sub("NODEPORT";$p))' \
  deploy/final/joint/proposed-vertical/ue-payload.json > /tmp/ue-payload.json
UE="ssh -o BatchMode=yes -o ConnectTimeout=10 member@10.34.211.6"
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue check' < /tmp/ue-payload.json
# when the check prints "UE prerequisites ready", start traffic (open a second terminal T4 now):
$UE 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < /tmp/ue-payload.json
```

The run is 7 phases x 120 s (`3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1`). Traffic starts when the UE log prints a line beginning with `[RUN]`.

#### 10. While the UE runs (terminal T4 on core, same environment block)

```bash
# a) as soon as the UE log shows [RUN], seed the managed Deployment up to the HPA minimum (1):
kubectl -n $NS scale deployment/$APP --current-replicas=0 --replicas=1
# b) the latency metric must show a value within ~90 s of traffic starting:
until kubectl get --raw /apis/external.metrics.k8s.io/v1beta1/namespaces/$NS/ue_p90_excess_latency_120ms_seconds | jq -e '.items[0].value'; do sleep 5; done
kubectl -n $NS get hpa -w            # HPA decisions
kubectl -n $NS get pods -o wide -w    # pod placement (Ctrl-C to stop watching)
curl -s -H "$H" $A/status | jq '{mode, sites: (.sites | map_values({active_route, local_ready_endpoints, core_ready_endpoints}))}'   # offload routes per site
kubectl -n placement logs deployment/placement-controller --tail=30   # scheduler/descheduler decisions
```

To abort the UE early: `ssh member@10.34.211.6 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -m experiments.campaign_ue stop --campaign '"$CAMP"' --method proposed-vertical'`.

#### 11. After the UE finishes: collect and clean up

```bash
# optional: copy the UE-side results (request logs, summaries) to core
rsync -a member@10.34.211.6:/home/member/skripsi_yoga/5g-mec-controller/results/$CAMP/ results/$CAMP-ue/
kubectl -n $NS delete hpa/$APP-baseline hpa/$APP-proposed deployment/$APP service/$APP \
  deployment/$APP-anchor-gnb-1 deployment/$APP-anchor-gnb-2 deployment/$APP-anchor-gnb-3 \
  --ignore-not-found=true --cascade=foreground --wait=true --timeout=180s
kubectl -n $NS wait --for=delete pod -l app=$APP --timeout=180s 2>/dev/null || true
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset | jq .
kubectl -n $NS scale deployment/$APP-core --replicas=3    # the pool was 3 before the run
```

---

## Scenario teardown

```bash
echo "Ctrl-C the tc helper in T2 (removes the delay) and the port-forward in T3"
kubectl get nodes -L topology.kubernetes.io/zone
kubectl -n $NS get deployment $APP-core    # should be 3/3 again
```

