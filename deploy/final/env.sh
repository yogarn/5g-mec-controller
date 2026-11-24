# Source this in EVERY terminal on core before running the runbook commands:
#   source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
cd /home/member/skripsi_yoga/5g-mec-controller || return
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
export NS=services APP=mec-placement-pedestrian-detection
export A=http://127.0.0.1:18086                       # offloader admin API (needs the T3 port-forward)
export TOKEN=$(cat /home/member/.config/5g-mec/ue-telemetry-token)
export H="Authorization: Bearer $TOKEN"
echo "env ready: NS=$NS APP=$APP KUBECONFIG=$KUBECONFIG"
