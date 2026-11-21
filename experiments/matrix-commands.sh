# Experiment matrix commands for review. Every line is a comment.
# Run from the repository root on core when live execution is authorized.
# For an offline preview, append --plan-only /tmp/mec-matrix-plans/<run-name> to a command.
# Use a fresh plan directory for each name. Do not use the wrapper’s check action for a read-only review.
# Pin all 16 campaigns to the pedestrian manifest image digest:
# sha256:5245d82fcec53c68db02844135a38eed05e7ee263b987571d38e16d1f4e0d269
# Each command is one paired campaign. 01 runs baseline first and 02 runs proposed first.
# Traffic total is 592 min (9 h 52 min), including the optional load pair.
# Without load the traffic total is 512 min (8 h 32 min). Scaling uses no final hold.
# Setup estimates are per paired campaign and include preflight, both arm startups and between-arm reset.
# Estimates exclude traffic, phase transitions, result transfer, plotting and final cleanup.
# Mobility uses the same seven-phase schedule in all regimes with at most 3 UEs on gnb1.
# The live placement-controller image differed from placement/deploy.yaml during review.
# Verify the deployed controller build against the reviewed Go code before placement campaigns.
# Anchored runs seed the managed Deployment at traffic start. Tier2 commands use the current
# readiness implementation and provide no guaranteed core endpoint or readiness-independent scale-up.
# The optional load commands match steep_pl_off_01 and steep_pl_off_02 apart from campaign names.
#
# mob_pool | 1680 s traffic and approximately 180–300 s setup per paired run.
# bash experiments/run.sh mob_pool_01 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison placement --scenario mobility9 --fixed-replicas 6 --phase-schedule 9:3:5:1,9:2:6:1,9:1:7:1,9:1:5:3,9:1:3:5,9:1:2:6,9:1:1:7 --method-order baseline-first
# bash experiments/run.sh mob_pool_02 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison placement --scenario mobility9 --fixed-replicas 6 --phase-schedule 9:3:5:1,9:2:6:1,9:1:7:1,9:1:5:3,9:1:3:5,9:1:2:6,9:1:1:7 --method-order proposed-first
#
# mob_local | 1680 s traffic and approximately 240–420 s setup per paired run.
# bash experiments/run.sh mob_local_01 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison placement --scenario mobility9 --fixed-replicas 6 --phase-schedule 9:3:5:1,9:2:6:1,9:1:7:1,9:1:5:3,9:1:3:5,9:1:2:6,9:1:1:7 --prefer-close --site-anchors --edge-only --method-order baseline-first
# bash experiments/run.sh mob_local_02 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison placement --scenario mobility9 --fixed-replicas 6 --phase-schedule 9:3:5:1,9:2:6:1,9:1:7:1,9:1:5:3,9:1:3:5,9:1:2:6,9:1:1:7 --prefer-close --site-anchors --edge-only --method-order proposed-first
#
# mob_tier2 | 1680 s traffic and approximately 240–420 s setup per paired run.
# bash experiments/run.sh mob_tier2_01 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison placement --scenario mobility9 --fixed-replicas 6 --phase-schedule 9:3:5:1,9:2:6:1,9:1:7:1,9:1:5:3,9:1:3:5,9:1:2:6,9:1:1:7 --prefer-close --site-anchors --spill-high 5 --spill-low 3 --method-order baseline-first
# bash experiments/run.sh mob_tier2_02 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison placement --scenario mobility9 --fixed-replicas 6 --phase-schedule 9:3:5:1,9:2:6:1,9:1:7:1,9:1:5:3,9:1:3:5,9:1:2:6,9:1:1:7 --prefer-close --site-anchors --spill-high 5 --spill-low 3 --method-order proposed-first
#
# scal | 3120 s traffic and approximately 120–240 s setup per paired run.
# bash experiments/run.sh scal_01 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison scaling --scenario scaler-staircase --cpu-hpa-target 80 --latency-floor-ms 125 --min-replicas 3 --max-replicas 24 --method-order baseline-first
# bash experiments/run.sh scal_02 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison scaling --scenario scaler-staircase --cpu-hpa-target 80 --latency-floor-ms 125 --min-replicas 3 --max-replicas 24 --method-order proposed-first
#
# joint_pool | 2400 s traffic and approximately 180–300 s setup per paired run.
# bash experiments/run.sh joint_pool_01 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison full --cpu-hpa-target 80 --latency-floor-ms 125 --min-replicas 3 --max-replicas 16 --method-order baseline-first
# bash experiments/run.sh joint_pool_02 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison full --cpu-hpa-target 80 --latency-floor-ms 125 --min-replicas 3 --max-replicas 16 --method-order proposed-first
#
# joint_local | 2400 s traffic and approximately 240–420 s setup per paired run.
# bash experiments/run.sh joint_local_01 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison full --cpu-hpa-target 80 --latency-floor-ms 125 --min-replicas 3 --max-replicas 16 --prefer-close --site-anchors --edge-only --method-order baseline-first
# bash experiments/run.sh joint_local_02 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison full --cpu-hpa-target 80 --latency-floor-ms 125 --min-replicas 3 --max-replicas 16 --prefer-close --site-anchors --edge-only --method-order proposed-first
#
# joint_tier2 | 2400 s traffic and approximately 240–420 s setup per paired run.
# bash experiments/run.sh joint_tier2_01 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison full --cpu-hpa-target 80 --latency-floor-ms 125 --min-replicas 3 --max-replicas 16 --prefer-close --site-anchors --spill-high 5 --spill-low 3 --method-order baseline-first
# bash experiments/run.sh joint_tier2_02 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison full --cpu-hpa-target 80 --latency-floor-ms 125 --min-replicas 3 --max-replicas 16 --prefer-close --site-anchors --spill-high 5 --spill-low 3 --method-order proposed-first
#
# load_local | 2400 s traffic and approximately 240–420 s setup per paired run.
# bash experiments/run.sh load_local_01 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison placement --latency-floor-ms 125 --min-replicas 3 --max-replicas 16 --locality-weight 0.4 --prefer-close --site-anchors --edge-only --method-order baseline-first
# bash experiments/run.sh load_local_02 --config experiments/campaign-steep.yaml --workload pedestrian-detection --autoscaler hpa --slo-ms 150 --tc-ms 10 --comparison placement --latency-floor-ms 125 --min-replicas 3 --max-replicas 16 --locality-weight 0.4 --prefer-close --site-anchors --edge-only --method-order proposed-first
