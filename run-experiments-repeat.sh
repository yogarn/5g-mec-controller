#!/usr/bin/env bash
# Repeat of *_01 results (same configs) plus locality-weight 0.3 exploration.
# Dry run (no cluster/UE access):  PLAN_ONLY=/tmp/plancheck bash run-experiments-repeat.sh
set -euo pipefail
cd /home/member/skripsi_yoga/5g-mec-controller

run() {
  echo "=== $(date -u +%FT%TZ) START $*"
  bash experiments/run.sh "$@" ${PLAN_ONLY:+--plan-only "$PLAN_ONLY/plan-$RANDOM"}
  echo "=== $(date -u +%FT%TZ) DONE  $1"
}

# Repeats of _01 (proposed-first to test method-order effect)
run mobility_grace_02 --scenario mobility9 --fixed-replicas 3 \
  --comparison full --cpu-hpa-target 80 --method-order proposed-first
run ramp scaling_floor125_02 --cpu-hpa-target 80 --slo-ms 150 \
  --max-replicas 12 --method-order proposed-first
run joint_floor125_02 --comparison full --cpu-hpa-target 80 \
  --slo-ms 150 --latency-floor-ms 125 --max-replicas 12 --method-order proposed-first

# PreferClose repeat (same order as _01)
run joint_prefer_close_02 --comparison full --cpu-hpa-target 80 \
  --slo-ms 150 --latency-floor-ms 125 --max-replicas 12 --prefer-close

# Exploration: locality weight 0.3 (default 0.4), same order as _01
run mobility_grace_lw03_01 --scenario mobility9 --fixed-replicas 3 \
  --comparison full --cpu-hpa-target 80 --locality-weight 0.3
run joint_floor125_lw03_01 --comparison full --cpu-hpa-target 80 \
  --slo-ms 150 --latency-floor-ms 125 --max-replicas 12 --locality-weight 0.3
