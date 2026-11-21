#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

skip_mobility=false
if [[ ${1:-} == --skip-mobility ]]; then
  skip_mobility=true
  shift
fi

if (( $# > 2 )); then
  echo "Usage: bash $0 [--skip-mobility] [CAMPAIGN_PREFIX [PLAN_DIR]]" >&2
  exit 2
fi

prefix=${1:-long_suite_$(date -u +%Y%m%dT%H%M%SZ)}
plan_dir=${2:-}
common=(
  --config experiments/campaign-mobility-core.yaml
  --autoscaler hpa --phase-duration=120 --phase-transition=continuous
  --slo-ms=150 --tc-ms=10 --rps-per-ue=5 --method-order=proposed-first
)
placement=(--scenario mobility9 --pod-share-scoring --locality-weight=0.4 --deschedule-interval=5)
mobility=(--comparison placement --fixed-replicas=6 --no-hpa
          --phase-schedule=18:6:6:6,18:14:2:2,18:10:6:2,18:6:10:2,18:2:14:2,18:2:10:6,18:2:6:10,18:2:2:14)
joint=(--comparison full --asymmetric-latency --bounded-hpa-behavior --scale-down-stabilization-seconds=300 --latency-floor-ms=125 --min-replicas=3 --max-replicas=12 --cpu-hpa-target=80
       --phase-schedule=3:1:1:1,6:4:1:1,9:6:2:1,12:6:4:2,15:6:6:3,18:5:8:5,24:4:12:8,24:2:16:6,18:2:8:8,15:1:5:9,12:1:3:8,9:1:2:6,6:1:1:4,3:1:1:1)

run() {
  local suffix=$1
  shift
  local plan=()
  if [[ -n $plan_dir ]]; then
    plan=(--plan-only "$plan_dir/$suffix")
  fi
  bash experiments/run.sh "${prefix}_$suffix" "${common[@]}" "$@" "${plan[@]}"
}

run joint_prefer "${placement[@]}" "${joint[@]}" --prefer-close --site-anchors
run joint_wide "${placement[@]}" "${joint[@]}" --cluster-wide
if ! $skip_mobility; then
  run mobility_wide "${placement[@]}" "${mobility[@]}" --cluster-wide
  run mobility_prefer "${placement[@]}" "${mobility[@]}" --prefer-close --site-anchors
fi
run autoscaling --scenario scaler-staircase --comparison scaling --asymmetric-latency --cluster-wide --cpu-hpa-target=80 \
  --min-replicas=3 --max-replicas=12 --latency-floor-ms=125 --final-hold
