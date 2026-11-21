#!/usr/bin/env bash
# Shared engine for the vertical campaigns; use run-vertical-full.sh, run-vertical-smoke.sh or
# run-vertical-mobility.sh. All experiment constants live in experiments/campaign.py (PROFILES).
# Usage: vertical_campaign.sh PROFILE PREFIX [--plan-only DIRECTORY]
set -euo pipefail
cd "$(dirname "$0")/.."

if (( $# < 2 )); then
  printf 'Usage: %s PROFILE PREFIX [--plan-only DIRECTORY]\n' "$0" >&2
  exit 2
fi
profile=$1
campaign_prefix=$2
shift 2

campaign="${campaign_prefix}_$(date -u +%Y%m%dT%H%M%SZ)"
runner=(.venv/bin/python -u -m experiments.campaign "$campaign" --profile "$profile")
if [[ $# == 2 && $1 == --plan-only ]]; then
  exec "${runner[@]}" --plan-only "$2"
elif (( $# != 0 )); then
  printf 'Usage: %s PROFILE PREFIX [--plan-only DIRECTORY]\n' "$0" >&2
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
