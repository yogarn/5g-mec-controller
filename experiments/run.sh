#!/usr/bin/env bash
# One-time setup (venv on core + UE host): bash experiments/run.sh setup
# Prerequisites + temporary workload and UE connectivity probes: bash experiments/run.sh check [--workload face-detection|pedestrian-detection]
# Render manifests without cluster access: bash experiments/run.sh --plan-only /tmp/plan [--workload ...] [--comparison full|placement|scaling]
# Nine-UE mobility: bash experiments/run.sh mobility9_01 --scenario mobility9
# Scaler-only balanced: bash experiments/run.sh scaler_01 --comparison scaling --scenario scaler-balanced [--final-hold]
# Whole campaign: bash experiments/run.sh run_2 [--workload face-detection|pedestrian-detection] [--comparison full|placement]
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
if [[ "${1:-}" == setup ]]; then
  python3 -m venv .venv
  .venv/bin/python -m pip install -r requirements.txt matplotlib
  .venv/bin/python - <<'PY'
import shlex
import subprocess
import yaml
from pathlib import Path
config = yaml.safe_load(Path('experiments/campaign.yaml').read_text())
project = shlex.quote(config['ue_project'])
script = f'cd {project} && python3 -m venv .venv && .venv/bin/python -m pip install -r requirements.txt && sudo -n true'
subprocess.run(['ssh', '-o', 'BatchMode=yes', config['ue_host'], script], check=True)
PY
  echo 'Setup complete. Run: bash experiments/run.sh check'
  exit 0
fi
if [[ ! -x .venv/bin/python ]]; then
  echo 'Missing virtual environment. Run: bash experiments/run.sh setup' >&2
  exit 1
fi
if [[ "${1:-}" == rebuild ]]; then
  shift
  exec .venv/bin/python -u -m utils.rebuild_pedestrian "$@"
fi
if [[ "${1:-}" == ramp ]]; then
  shift
  exec .venv/bin/python -u -m experiments.campaign --comparison scaling --scenario scaler-staircase --phase-duration 60 --tc-ms 10 --slo-ms 150 --latency-floor-ms 130 --rps-per-ue 5 --max-in-flight-per-ue 8 "$@"
fi
if [[ "${1:-}" == check ]]; then
  shift
  exec .venv/bin/python -u -m experiments.campaign --check "$@"
fi
if [[ $# -eq 0 ]]; then
  echo 'Usage: bash experiments/run.sh setup | check [--workload NAME] | rebuild [--plan-only] | ramp NEW_CAMPAIGN_NAME [OPTIONS] | NEW_CAMPAIGN_NAME [--workload face-detection|pedestrian-detection] [--comparison full|placement|scaling] [--scenario mobility9|scaler-balanced|scaler-staircase] [--final-hold] [--long-run] [--fixed-replicas COUNT] [--deschedule-interval SECONDS] [--rps-per-ue RATE] [--max-in-flight-per-ue COUNT] [--tc-ms DELAY] [--slo-ms TARGET] [--rt-target-factor FACTOR] [--latency-floor-ms MILLISECONDS] [--max-replicas COUNT] [--debug] [--phase-duration SECONDS] [--resource-weights CPU MEMORY GPU VRAM] [--locality-weight WEIGHT] [--plan-only DIR]' >&2
  exit 2
fi
exec .venv/bin/python -u -m experiments.campaign "$@"
