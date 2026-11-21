#!/usr/bin/env bash
# The two offloading arms (baseline-vertical, proposed-vertical) of the joint run, full 11-phase schedule.
# Settings: experiments/profiles.py PROFILES['vertical-joint-vertical'].
# Optional: --plan-only DIRECTORY renders the plan without touching the cluster.
set -euo pipefail
exec "$(dirname "$0")/experiments/vertical_campaign.sh" vertical-joint-vertical vertical_joint_offload "$@"
