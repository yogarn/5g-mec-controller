#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

skip_joint=false
if [[ ${1:-} == --skip-joint ]]; then
  skip_joint=true
  shift
fi

plan_root=
if [[ $# == 2 && $1 == --plan-only ]]; then
  plan_root=$(realpath -m "$2")
elif (( $# != 0 )); then
  printf 'Usage: %s [--skip-joint] [--plan-only DIRECTORY]\n' "$0" >&2
  exit 2
fi

run_stage() {
  local stage=$1
  shift
  if [[ -n $plan_root ]]; then
    "$@" --plan-only "$plan_root/$stage"
  else
    "$@"
  fi
}

if ! $skip_joint; then
  run_stage joint bash run-vertical-full.sh
fi
scaling="vertical_suite_scaling_$(date -u +%Y%m%dT%H%M%SZ)"
run_stage scaling bash experiments/run.sh "$scaling" \
  --config experiments/campaign-mobility-core.yaml --workload pedestrian-detection \
  --comparison scaling --scenario scaler-staircase --autoscaler hpa \
  --cpu-hpa-target 80 --slo-ms 150 --asymmetric-latency \
  --min-replicas 3 --max-replicas 18 --scale-down-stabilization-seconds 60 \
  --phase-duration 120 --phase-transition continuous --rps-per-ue 5 --max-in-flight-per-ue 8 --tc-ms 10
run_stage mobility bash run-vertical-full.sh --mobility
