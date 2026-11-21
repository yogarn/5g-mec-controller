#!/usr/bin/env bash
# Retry joint_floor125_02 from phase 1, then run the two remaining explorations.
# Dry run (no cluster/UE access):  PLAN_ONLY=/tmp/plancheck bash run-experiments-repeat.sh
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

run() {
  local campaign=$1
  local -a plan_args=()
  if [[ -n ${PLAN_ONLY:-} ]]; then
    plan_args=(--plan-only "$PLAN_ONLY/$campaign-$RANDOM")
  fi
  echo "=== $(date -u +%FT%TZ) START $*"
  bash experiments/run.sh "$@" "${plan_args[@]}"
  echo "=== $(date -u +%FT%TZ) DONE  $campaign"
}

# Reuse the cleared campaign name with the original proposed-first settings.
run joint_floor125_02 --comparison full --cpu-hpa-target 80 \
  --slo-ms 150 --latency-floor-ms 125 --max-replicas 12 --method-order proposed-first

# Exploration: locality weight 0.3 (default 0.4), same order as _01
run mobility_grace_lw03_01 --scenario mobility9 --fixed-replicas 3 \
  --comparison full --cpu-hpa-target 80 --locality-weight 0.3
run joint_floor125_lw03_01 --comparison full --cpu-hpa-target 80 \
  --slo-ms 150 --latency-floor-ms 125 --max-replicas 12 --locality-weight 0.3
