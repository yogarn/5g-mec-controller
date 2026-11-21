#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if (( $# > 2 )); then
  echo "Usage: bash $0 [CAMPAIGN_PREFIX [PLAN_DIR]]" >&2
  exit 2
fi

prefix=${1:-smoke_suite_$(date -u +%Y%m%dT%H%M%SZ)}
plan_dir=${2:-}
common=(
  --config experiments/campaign-mobility-core.yaml
  --autoscaler hpa --phase-duration=60 --phase-transition=continuous
  --slo-ms=150 --tc-ms=10 --rps-per-ue=5 --method-order=proposed-first
)
placement=(--scenario mobility9 --site-anchors --locality-weight=0.4 --deschedule-interval=5)
joint=(--comparison full --min-replicas=6 --max-replicas=12 --cpu-hpa-target=80
       --phase-schedule=6:2:2:2,18:14:2:2,18:2:14:2,6:2:2:2)

run() {
  local suffix=$1
  shift
  local plan=()
  if [[ -n $plan_dir ]]; then
    plan=(--plan-only "$plan_dir/$suffix")
  fi
  bash experiments/run.sh "${prefix}_$suffix" "${common[@]}" "$@" "${plan[@]}"
}

run mobility_wide "${placement[@]}" --comparison placement --cluster-wide \
  --fixed-replicas=6 --phase-schedule=18:6:6:6,18:14:2:2,18:2:14:2
run autoscaling --scenario scaler-staircase --comparison scaling --cpu-hpa-target=80 \
  --min-replicas=3 --max-replicas=12 --latency-floor-ms=125 --phase-schedule=3:1:1:1,12:4:4:4,3:1:1:1
run joint_wide "${placement[@]}" "${joint[@]}" --cluster-wide
run joint_prefer "${placement[@]}" "${joint[@]}" --prefer-close
