#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

smoke=false
default_name=final_mobility18
if [[ ${1:-} == --prefer-close-smoke ]]; then
  smoke=true
  default_name=mobility18_podshare_smoke01
  shift
fi

name=${1:-$default_name}
plan_root=${2:-}
common=(
  --config experiments/campaign-mobility-core.yaml --workload pedestrian-detection
  --scenario mobility9 --comparison placement --autoscaler hpa
  --site-anchors --fixed-replicas=6 --phase-duration=120 --deschedule-interval=5
  --phase-schedule=18:14:2:2,18:10:6:2,18:6:10:2,18:2:14:2,18:2:10:6,18:2:6:10,18:2:2:14
  --rps-per-ue=5 --slo-ms=150 --tc-ms=10 --locality-weight=0.4 --method-order=proposed-first
)

run() {
  local suffix=$1 routing=$2
  local plan=()
  if [[ -n $plan_root ]]; then
    plan=(--plan-only "$plan_root/$suffix")
  fi
  bash experiments/run.sh "${name}_$suffix" "${common[@]}" "$routing" "${plan[@]}"
}

if $smoke; then
  common+=(--phase-duration=60 --phase-schedule=18:6:6:6,18:14:2:2,18:2:14:2,18:2:2:14)
  run prefer --prefer-close
else
  run wide --cluster-wide
  run prefer --prefer-close-routing-only
fi
