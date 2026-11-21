#!/usr/bin/env bash
# Mobility placement comparison, four arms, 24 UEs. Settings: experiments/campaign.py PROFILES['vertical-mobility'].
# Optional: --plan-only DIRECTORY renders the plan without touching the cluster.
set -euo pipefail
exec "$(dirname "$0")/experiments/vertical_campaign.sh" vertical-mobility vertical_mobility "$@"
