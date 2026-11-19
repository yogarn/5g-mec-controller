#!/usr/bin/env bash
# Copy one command at a time and run it from the repository root.
# Each arm starts with one fixed anchor on gnb1, gnb-2, and gnb-3.
# Phase 1 starts with three serving pods. The runner then seeds the managed
# Deployment with one pod so native HPA can scale it. HPA manages 1–13 extra
# pods in each arm, for 4–16 total once scaling is active. Baseline CPU HPA
# measures the managed pods. Both Services route to anchors and managed pods.

# bash experiments/run.sh steep_pc_01 --config experiments/campaign-steep.yaml --comparison full \
#   --prefer-close --site-anchors --min-replicas 3 --max-replicas 16 --cpu-hpa-target 80 --slo-ms 150 \
#   --latency-floor-ms 125 --tc-ms 10 --locality-weight 0.4 --method-order baseline-first

# bash experiments/run.sh steep_pc_02 --config experiments/campaign-steep.yaml --comparison full \
#   --prefer-close --site-anchors --min-replicas 3 --max-replicas 16 --cpu-hpa-target 80 --slo-ms 150 \
#   --latency-floor-ms 125 --tc-ms 10 --locality-weight 0.4 --method-order proposed-first

# bash experiments/run.sh steep_pc_03 --config experiments/campaign-steep.yaml --comparison full \
#   --prefer-close --site-anchors --min-replicas 3 --max-replicas 16 --cpu-hpa-target 80 --slo-ms 150 \
#   --latency-floor-ms 125 --tc-ms 10 --locality-weight 0.4 --method-order baseline-first

# bash experiments/run.sh steep_pc_04 --config experiments/campaign-steep.yaml --comparison full \
#   --prefer-close --site-anchors --min-replicas 3 --max-replicas 16 --cpu-hpa-target 80 --slo-ms 150 \
#   --latency-floor-ms 125 --tc-ms 10 --locality-weight 0.4 --method-order proposed-first
