#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

campaign_prefix=vertical_joint_full
phase_seconds=120
schedule='3:1:1:1,6:3:2:1,9:6:2:1,12:6:4:2,15:6:6:3,18:5:8:5,24:4:12:8,24:2:16:6,18:2:8:8,15:1:5:9,3:1:1:1'
comparison=(
  --comparison full --cpu-hpa-target 80 --scale-down-stabilization-seconds 60
  --asymmetric-latency --rt-target-factor 1 --proposed-scale-down-period-seconds 15
)
if [[ ${1:-} == --smoke ]]; then
  campaign_prefix=vertical_joint_smoke
  phase_seconds=60
  schedule='3:1:1:1,6:4:1:1,3:1:1:1'
  shift
elif [[ ${1:-} == --mobility ]]; then
  campaign_prefix=vertical_mobility
  schedule='18:8:5:5,18:2:14:2,18:2:12:4,18:2:8:8,18:2:4:12,18:2:2:14'
  comparison=(--comparison placement --fixed-replicas 9 --no-hpa)
  shift
fi

campaign="${campaign_prefix}_$(date -u +%Y%m%dT%H%M%SZ)"
runner=(
  .venv/bin/python -u -m experiments.campaign "$campaign"
  --config experiments/campaign-vertical.yaml --vertical-mode four-arm
  "${comparison[@]}" --scenario mobility9 --phase-schedule "$schedule"
  --phase-duration "$phase_seconds" --phase-transition continuous
  --rps-per-ue 5 --max-in-flight-per-ue 8 --slo-ms 150 --tc-ms 10
  --autoscaler hpa
  --placement-engine go --pod-share-scoring --deschedule-interval 30
  --locality-weight 0.4 --resource-weights 0.5 0.5 0 0
  --prefer-close --site-anchors
)
if [[ $# == 2 && $1 == --plan-only ]]; then
  exec "${runner[@]}" --plan-only "$2"
elif (( $# != 0 )); then
  printf 'Usage: %s [--smoke|--mobility] [--plan-only DIRECTORY]\n' "$0" >&2
  exit 2
fi

mkdir -p results
log="results/$campaign.log"
printf 'Campaign %s\nLog %s\n' "$campaign" "$log" | tee "$log"

admin_health=http://127.0.0.1:18086/health
if ! curl -fsS --max-time 2 "$admin_health" >/dev/null; then
  kubectl -n placement port-forward --address 127.0.0.1 svc/vertical-offloader-admin 18086:8086 >>"$log" 2>&1 &
  forward_pid=$!
  trap 'kill "$forward_pid" 2>/dev/null || true; wait "$forward_pid" 2>/dev/null || true' EXIT
  for ((i = 0; i < 15; i++)); do
    if curl -fsS --max-time 2 "$admin_health" >/dev/null 2>&1; then
      break
    fi
    if ! kill -0 "$forward_pid" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  if ! curl -fsS --max-time 2 "$admin_health" >/dev/null 2>&1; then
    printf 'Offloader admin API is unavailable. See %s\n' "$log" | tee -a "$log" >&2
    exit 1
  fi
fi

if "${runner[@]}" 2>&1 | tee -ai "$log"; then
  printf 'Results results/%s\n' "$campaign" | tee -a "$log"
else
  status=$?
  printf 'Campaign exited with status %s. See %s\n' "$status" "$log" | tee -a "$log" >&2
  exit "$status"
fi
