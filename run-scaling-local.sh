#!/usr/bin/env bash
# Rerun of the scaling comparison (CPU HPA vs SLO HPA, both arms, 7 x 120 s) with gnb-1 on its local UPF
# (DNN mec.icn-2.testbed, NodePort on gnb1) instead of the legacy core DNN. Settings: PROFILES['scaling-staircase-local'].
# The result folder keeps the vertical_suite_scaling_ prefix so it can replace the 24 Nov scaling folder.
# Usage: run-scaling-local.sh [--plan-only DIRECTORY]
set -euo pipefail
cd "$(dirname "$0")"
campaign="vertical_suite_scaling_localupf_$(date -u +%Y%m%dT%H%M%SZ)"
if [[ $# == 2 && $1 == --plan-only ]]; then
  exec bash experiments/run.sh "$campaign" --profile scaling-staircase-local --plan-only "$2"
elif (( $# != 0 )); then
  printf 'Usage: %s [--plan-only DIRECTORY]\n' "$0" >&2
  exit 2
fi
mkdir -p results
log="results/$campaign.log"
printf 'Campaign %s\nLog %s\n' "$campaign" "$log" | tee "$log"
bash experiments/run.sh "$campaign" --profile scaling-staircase-local 2>&1 | tee -ai "$log"
.venv/bin/python -m utils.verify_scaling_paths "results/$campaign" 2>&1 | tee -a "$log"
printf 'Results results/%s\n' "$campaign" | tee -a "$log"
