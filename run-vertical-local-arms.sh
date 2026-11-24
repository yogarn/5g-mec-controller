#!/usr/bin/env bash
# The two local-only arms (baseline-local, proposed-local) of the joint run, 7-phase gnb-2 hotspot schedule.
# Settings: experiments/profiles.py PROFILES['vertical-joint-local'].
# Optional: --plan-only DIRECTORY renders the plan without touching the cluster.
set -euo pipefail
exec "$(dirname "$0")/experiments/vertical_campaign.sh" vertical-joint-local vertical_joint_local "$@"
