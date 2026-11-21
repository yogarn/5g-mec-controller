#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

campaign=mobility_cluster_wide_fast_01
if (( $# )) && [[ $1 != -* ]]; then
  campaign=$1
  shift
fi

exec bash experiments/run.sh "$campaign" \
  --config experiments/campaign-steep.yaml --workload pedestrian-detection \
  --scenario mobility9 --comparison full --autoscaler hpa \
  --fixed-replicas=3 --phase-duration=60 --deschedule-interval=5 \
  --slo-ms=150 --cpu-hpa-target=80 --tc-ms=10 --rps-per-ue=5 --locality-weight=0.4 \
  --method-order=proposed-first "$@"
