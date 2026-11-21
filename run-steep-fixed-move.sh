#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

campaign=steep_fixed_move_cap8_01
if (( $# )) && [[ $1 != -* ]]; then
  campaign=$1
  shift
fi
config=$(mktemp /tmp/steep-fixed-move.XXXXXX.yaml)
trap 'rm -f -- "$config"' EXIT
sed "s/^schedule:.*/schedule: '3:1:1:1,24:2:20:2,24:2:2:20'/" \
  experiments/campaign-steep.yaml > "$config"

bash experiments/run.sh "$campaign" \
  --config "$config" --comparison placement --autoscaler hpa \
  --edge-only --prefer-close --site-anchors \
  --min-replicas 8 --max-replicas 8 --phase-duration 90 \
  --slo-ms 150 --latency-floor-ms 125 --tc-ms 10 \
  --rps-per-ue 5 --locality-weight 0.4 --method-order baseline-first "$@"
