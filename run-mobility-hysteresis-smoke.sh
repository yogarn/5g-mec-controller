#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

if (( $# > 2 )); then
  echo "Usage: bash $0 [CAMPAIGN_NAME [PLAN_DIR]]" >&2
  exit 2
fi

campaign=${1:-mobility18_hysteresis05_smoke01}
plan_dir=${2:-}
options=(
  --config experiments/campaign-mobility-core.yaml
  --scenario mobility9 --comparison placement --autoscaler hpa
  --fixed-replicas=6 --prefer-close --site-anchors
  --phase-schedule=18:6:6:6,18:14:2:2,18:2:14:2
  --phase-duration=60 --phase-transition=continuous
  --slo-ms=150 --tc-ms=10 --rps-per-ue=5
  --locality-weight=0.4 --deschedule-interval=5 --method-order=proposed-first
)

if [[ -n $plan_dir ]]; then
  exec bash experiments/run.sh "$campaign" "${options[@]}" --plan-only "$plan_dir"
fi

.venv/bin/python -m utils.redeploy_placement
exec bash experiments/run.sh "$campaign" "${options[@]}"
