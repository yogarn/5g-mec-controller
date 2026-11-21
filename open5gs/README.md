# Open5GS testbed patch

Changes this research makes on top of the base K3s/Calico Open5GS testbed
(https://github.com/rayhanegar/Open5GS-Testbed, `open5gs/open5gs-k3s-calico`, deployed with
`setup-k3s-environment-calico.sh`, `build-import-containers.sh`, and
`deploy-k3s-calico.sh`). Only added or modified parts are kept here; the
control-plane functions, MongoDB, and the base core UPF are not repeated.

These patches describe the Core-DNN topology reconstructed on 2026-11-19.
The minimal gNB2 and gNB3 manifests were unapplied at that snapshot. The
scaling stage uses the Core-DNN profile for gNB1. Joint and mobility use the
local PDU session (`mec.icn-2.testbed`, `172.16.50.0/24`) and eligible AN-MEC
node `gnb1`, as defined in `experiments/campaign-vertical.yaml`. UPF addresses
must fall inside an existing Calico IP pool.

| Path | Content |
|---|---|
| `user-plane/upf-gnb-{2,3}.yaml` | Local UPF for the gnb-2 and gnb-3 sites: ConfigMap (one DNN each) and StatefulSet with a fixed Calico address. |
| `patches/smf-dnn-excerpt.yaml` | Excerpt only: UPF `dnn` entries and `session` subnets added to the SMF configuration. |
| `patches/svc-amf-metrics.yaml`, `patches/svc-smf-externalip.yaml` | `kubectl patch` files: `metrics` port 9005 on the AMF Service and `externalIPs` on the SMF Service. The base testbed already has the `metrics` blocks in the AMF/SMF ConfigMaps and the SMF `metrics` port. |

| gNB site | UPF | Address | DNN | Session subnet | TUN device |
|---|---|---|---|---|---|
| gnb-1 (Core-DNN profile) | `upf-0` (core, base testbed) | 172.16.10.7 | `embb.testbed` | 172.16.45.0/24 | `ogstun` |
| gnb-2 | `upf-gnb-2` | 172.16.20.70 | `embb.gnb2.testbed` | 10.63.1.0/24 | `ogstun2s` |
| gnb-3 | `upf-gnb-3` | 172.16.20.200 | `embb.gnb3.testbed` | 10.64.1.0/24 | `ogstun3s` |

Notes:
- The UPFs of the running cluster for gnb-2 and gnb-3 also contain a DNS traffic
  shaper, a `tc` exporter, and an Envoy proxy from other work. Those parts and the
  `urllc` and `mmtc` DNNs are left out here because this research does not use them.
- The per-experiment pool `mec-campaign-reserved` is created by
  `utils/traffic_control.py`.
