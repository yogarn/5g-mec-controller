#!/usr/bin/env bash
# The two local-only arms (baseline-local, proposed-local) of the mobility sweep (9 fixed replicas, 7 phases).
# Settings: experiments/profiles.py PROFILES['vertical-mobility-local'].
# Optional: --plan-only DIRECTORY renders the plan without touching the cluster.
set -euo pipefail
exec "$(dirname "$0")/experiments/vertical_campaign.sh" vertical-mobility-local vertical_mobility_local "$@"
