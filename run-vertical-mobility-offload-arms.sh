#!/usr/bin/env bash
# The two offloading arms (baseline-vertical, proposed-vertical) of the mobility sweep (9 fixed replicas, 7 phases).
# Settings: experiments/profiles.py PROFILES['vertical-mobility-vertical'].
# Optional: --plan-only DIRECTORY renders the plan without touching the cluster.
set -euo pipefail
exec "$(dirname "$0")/experiments/vertical_campaign.sh" vertical-mobility-vertical vertical_mobility_offload "$@"
