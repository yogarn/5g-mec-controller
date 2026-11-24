# Pre-rendered manifests for the final experiments

Every file here is exactly what `experiments.campaign` rendered for the final runs
(`--plan-only` output of the `vertical-joint*` and `vertical-mobility*` profiles, plus the
in-cluster placement ConfigMap the campaign builds at run time). Nothing is generated when
you use them: apply the numbered files in order.

```
deploy/final/<scenario>/<arm>/NN-*.yaml     apply in numeric order
deploy/final/<scenario>/<arm>/ue-payload.json   stdin for the UE runner (see "UE" below)
```

Scenarios: `joint` (7 phases x 120 s, 3 to 24 UEs, HPA) and `mobility` (7 phases x 120 s,
24 UEs, 9 fixed replicas, no HPA). Arms: `baseline-local`, `proposed-local`,
`baseline-vertical`, `proposed-vertical`.

Run everything on **core** (`ssh core`, then
`cd /home/member/skripsi_yoga/5g-mec-controller; export KUBECONFIG=/etc/rancher/k3s/k3s.yaml`)
except the UE step, which runs on the UE host `member@10.34.211.6`.

Run one arm at a time and clean up (last section) before the next.

## 1. Prerequisites that are not YAML

The campaign does these automatically; do them by hand only if you skip the campaign:

- **PreferClose zones** (joint and mobility, all arms): label each eligible node as its own zone.
  `kubectl label node gnb1 topology.kubernetes.io/zone=gnb1 --overwrite` (same for `gnb-2`, `gnb-3`,
  and `core-5g` on vertical arms). Save the old values first with
  `kubectl get node <n> -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}'`.
- **10 ms path delay (`tc_ms: 10`)**: the final runs shaped inter-node traffic to/from the Calico pool
  `mec-campaign-reserved`. There is no manifest for it. It is `utils.traffic_control.TrafficDelay(10.0)`
  with `.start()` before traffic and `.close()` after, run from the project root on core.
  Without it, latencies are not comparable to the results.
- **Vertical arms only**: the offloader admin API (port-forward and token below).

```
kubectl -n placement port-forward --address 127.0.0.1 svc/vertical-offloader-admin 18086:8086 &
TOKEN=$(cat /home/member/.config/5g-mec/ue-telemetry-token)   # offloader_token_file in the campaign config
```

## 2. Apply order per arm

```
D=deploy/final/<scenario>/<arm>
for f in $(ls $D/*.yaml | sort); do kubectl apply -f $f; done
```

| file | what | notes |
|---|---|---|
| `01-placement-configmap.yaml` | `placement-controller-config` for the in-cluster Go scheduler/descheduler | proposed arms only. Then `kubectl -n placement rollout restart deployment/placement-controller` and wait for `rollout status`. |
| `NN-adapter-configmap.yaml` | `prometheus-adapter-config` (excess-latency rule, plus the external CPU rule for the baseline joint arms) | joint arms only. Then `kubectl -n placement rollout restart deployment/prometheus-adapter` and wait for `rollout status`. |
| `NN-site-anchors.yaml` | 3 anchor Deployments, one per edge site | wait until Ready |
| `NN-workload.yaml` | managed Deployment and Service (`mec-placement-pedestrian-detection`) | the campaign waits for Ready where `replicas > 0` |
| `NN-hpa.yaml` | HPA | joint arms only. Mobility arms have no HPA (fixed 9 replicas). |

Joint arms: the managed Deployment starts at `replicas: 0` and the HPA does not act at zero. Once
UE traffic has started, seed it exactly like the campaign:
`kubectl -n services scale deployment/mec-placement-pedestrian-detection --current-replicas=0 --replicas=1`.

## 3. Vertical arms: offloader and Core pool (API, no YAML)

The campaign runs this before traffic (`experiments/vertical.py: prepare_vertical_arm`):

```
A=http://127.0.0.1:18086
H="Authorization: Bearer $TOKEN"
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset
kubectl -n services scale deployment/mec-placement-pedestrian-detection-core --replicas=3   # 0 for local arms
kubectl -n services get pods -l app=mec-placement-pedestrian-detection-core -o wide        # wait: 3 Ready on core-5g
# wait for the core endpoints in the offloader status (admin API), then
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"vertical"}' $A/mode
```

The readiness gate between these steps probes the three Envoys; check
`curl -s -H "$H" $A/status` shows 3 core endpoints and all sites LOCAL before switching to `vertical`.
Local arms use no offloader traffic path: reset to `local-only` and keep the Core pool at 0.

## 4. UE (on the UE host)

`ue-payload.json` is the stdin the campaign sends over SSH. From core:

```
ssh member@10.34.211.6 'cd /home/member/skripsi_yoga/5g-mec-controller && sudo -n .venv/bin/python -u -m experiments.campaign_ue run' < $D/ue-payload.json
```

- `vertical` arms: `site_urls` are the Envoy NodePorts (31801 to 31803) and are already filled in.
- `local` arms: `site_urls` is `null` because the Service NodePort is assigned when the Service is created.
  Fill it in first:
  ```
  PORT=$(kubectl -n services get svc mec-placement-pedestrian-detection -o jsonpath='{.spec.ports[0].nodePort}')
  python3 - $D/ue-payload.json $PORT > /tmp/ue-payload.json <<'PY'
  import json, sys
  p = json.load(open(sys.argv[1])); port = sys.argv[2]
  ips = {'gnb-1': '10.34.211.157', 'gnb-2': '10.34.211.196', 'gnb-3': '10.34.210.3'}
  p['site_urls'] = {s: f'http://{a}:{port}/detect' for s, a in ips.items()}
  print(json.dumps(p))
  PY
  ```
- The `campaign` name (`manual_<scenario>`) must be new each time; results go to
  `results/<campaign>/` on the UE host.
- Telemetry must be live before judging the run: `ue_p90_latency_seconds` (proposed arms:
  `ue_p90_excess_latency_120ms_seconds`) should appear in the adapter within ~90 s.

## 5. Cleanup after each arm

```
kubectl -n services delete hpa --all
kubectl -n services delete deployment -l app=mec-placement-pedestrian-detection   # managed Deployment and the 3 anchors
kubectl -n services scale deployment/mec-placement-pedestrian-detection-core --replicas=<value it had before>
curl -s -X POST -H "$H" -H 'Content-Type: application/json' -d '{"mode":"local-only"}' $A/reset    # vertical arms
```

Then restore the node zone labels and stop the `TrafficDelay` filters. If a UE process is still
running on the UE host, stop it before the next arm.

## Status of these instructions

The manifests are the campaign's own output. The steps around them were written from reading
`experiments/campaign.py`, `experiments/vertical.py` and `utils/traffic_control.py`; they have
not been executed by hand end to end. If a manual arm and `./demo.sh <arm>` disagree, `demo.sh`
is the reference.
