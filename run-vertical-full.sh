#!/usr/bin/env bash
# Joint scaling and placement, four arms, 11 phases. Settings: experiments/campaign.py PROFILES['vertical-joint'].
# Optional: --plan-only DIRECTORY renders the plan without touching the cluster.
set -euo pipefail
exec "$(dirname "$0")/experiments/vertical_campaign.sh" vertical-joint vertical_joint_full "$@"
