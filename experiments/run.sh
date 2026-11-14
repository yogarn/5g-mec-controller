#!/usr/bin/env bash
# One-time setup: bash experiments/run.sh setup
# Read-only checks: bash experiments/run.sh check
# Whole campaign: bash experiments/run.sh run_2
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
if [[ "${1:-}" == check ]]; then
  shift
  exec .venv/bin/python -u -m experiments.campaign --check "$@"
fi
if [[ $# -eq 0 ]]; then
  echo 'Usage: bash experiments/run.sh setup | check | NEW_CAMPAIGN_NAME [--tc-ms DELAY] [--slo-ms TARGET] [--max-replicas COUNT]' >&2
  exit 2
fi
exec .venv/bin/python -u -m experiments.campaign "$@"
