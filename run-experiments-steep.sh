#!/usr/bin/env bash
# Copy one command at a time from the repository root on core.
# The two spill-off commands are the edge-only comparison. The spill-on
# commands below are for a later two-tier comparison with core-5g eligible.
# Phase 1 has four pods after the initial seed: one fixed anchor each on
# gnb1, gnb-2, and gnb-3, plus one managed pod seeded when traffic starts.
# The default scheduler or Go controller places that managed pod.
# Both arms use PreferClose and the same latency HPA (managed 1–13, total 4–16).

# bash experiments/run.sh steep_pl_off_01 --config experiments/campaign-steep.yaml --comparison placement \
#   --autoscaler hpa --prefer-close --site-anchors --edge-only --min-replicas 3 --max-replicas 16 \
#   --slo-ms 150 --latency-floor-ms 125 --tc-ms 10 --locality-weight 0.4 --method-order baseline-first

# bash experiments/run.sh steep_pl_off_02 --config experiments/campaign-steep.yaml --comparison placement \
#   --autoscaler hpa --prefer-close --site-anchors --edge-only --min-replicas 3 --max-replicas 16 \
#   --slo-ms 150 --latency-floor-ms 125 --tc-ms 10 --locality-weight 0.4 --method-order proposed-first

# bash experiments/run.sh steep_pl_spill_01 --config experiments/campaign-steep.yaml --comparison placement \
#   --autoscaler hpa --prefer-close --site-anchors --min-replicas 3 --max-replicas 16 \
#   --slo-ms 150 --latency-floor-ms 125 --tc-ms 10 --locality-weight 0.4 \
#   --spill-high 5 --spill-low 3 --method-order baseline-first

# bash experiments/run.sh steep_pl_spill_02 --config experiments/campaign-steep.yaml --comparison placement \
#   --autoscaler hpa --prefer-close --site-anchors --min-replicas 3 --max-replicas 16 \
#   --slo-ms 150 --latency-floor-ms 125 --tc-ms 10 --locality-weight 0.4 \
#   --spill-high 5 --spill-low 3 --method-order proposed-first
