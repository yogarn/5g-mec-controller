#!/usr/bin/env bash
# Smoke test of the joint run, three 60-second phases. Settings: experiments/campaign.py PROFILES['vertical-smoke'].
# Optional: --plan-only DIRECTORY renders the plan without touching the cluster.
set -euo pipefail
exec "$(dirname "$0")/experiments/vertical_campaign.sh" vertical-smoke vertical_joint_smoke "$@"
