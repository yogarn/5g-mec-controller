"""Command-line argument parser and profile expansion for experiments.campaign.

Builds the argparse CLI for `python -m experiments.campaign` and expands named
profiles from experiments.profiles before applying explicit CLI overrides.
"""
import argparse
from pathlib import Path

from utils.experiment import DEFAULT_MAX_IN_FLIGHT_PER_UE, DEFAULT_RPS_PER_UE

from experiments.profiles import (
    ARM_SELECTIONS,
    AUTOSCALERS,
    COMPARISONS,
    DEFAULT_CPU_HPA_TARGET,
    DEFAULT_DESCHEDULE_INTERVAL_S,
    DEFAULT_MIN_REPLICAS,
    DEFAULT_PHASE_SECONDS,
    DEFAULT_SLO_MS,
    DEFAULT_TC_MS,
    PLACEMENT_ENGINES,
    PROFILES,
    ROOT,
    SCALER_SCENARIOS,
    VERTICAL_MODES,
    WORKLOADS,
)


def _build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', choices=sorted(PROFILES),
                        help='named experiment profile with all of its constants; the run scripts select one')
    parser.add_argument('campaign', nargs='?', help='new result name, for example run_2')
    parser.add_argument('--workload', choices=WORKLOADS, default=None,
                        help='workload to deploy and monitor (default: campaign config or pedestrian-detection)')
    parser.add_argument('--scenario', choices=['mobility9', *SCALER_SCENARIOS],
                        help='mobility9: 9 UEs shifting across gNBs (7 phases); scaler-balanced: legacy steps; scaler-staircase: gradual balanced steps on core-5g')
    parser.add_argument('--mobility-direction', choices=('forward', 'reverse'), default=None,
                        help='mobility9 only: UE hotspot trajectory direction (forward or reverse; default: forward)')
    parser.add_argument('--phase-transition', choices=('continuous', 'restart'), default='continuous',
                        help='continuous preserves unaffected UE traffic at phase boundaries; restart reproduces legacy runs')
    parser.add_argument('--phase-schedule',
                        help='override the scenario with explicit total:gnb1:gnb2:gnb3 phases (recorded in metadata)')
    parser.add_argument('--final-hold', action='store_true',
                        help='extend final scaling phase: +360s for legacy balanced, or to at least 600s for staircase')
    parser.add_argument('--deschedule-interval', type=float,
                        help='seconds between proposed descheduler checks (default: 30; disabled for scaling)')
    parser.add_argument('--comparison', choices=COMPARISONS, default=None,
                        help='placement: same latency autoscaler; full: CPU HPA vs full proposed; scaling: CPU HPA vs latency HPA on core-5g')
    parser.add_argument('--autoscaler', choices=AUTOSCALERS, default='hpa',
                        help='proposed latency autoscaler engine: hpa (Kubernetes HPA + prometheus-adapter)')
    parser.add_argument('--placement-engine', choices=PLACEMENT_ENGINES, default='go',
                        help='scheduler/descheduler runtime for proposed method: go (in-cluster Deployment)')
    parser.add_argument('--plan-only', type=Path, metavar='DIRECTORY',
                        help='write workload/controller/HPA manifests locally without any probes or cluster access')
    parser.add_argument('--config', default=str(ROOT / 'experiments/campaign.yaml'))
    parser.add_argument('--check', action='store_true', help='dependency checks plus temporary workload and UE connectivity probes')
    parser.add_argument('--tc-ms', type=float, default=DEFAULT_TC_MS,
                        help='workload link shaping one-way delay in milliseconds (default: 10)')
    parser.add_argument('--slo-ms', type=float,
                        help='reporting p90 SLO threshold in milliseconds (also base RT target before --rt-target-factor; default: 150)')
    parser.add_argument('--cpu-hpa-target', type=int, metavar='PERCENT',
                        help='baseline CPU HPA target as a positive integer percent of CPU requests (default: 80; full/scaling comparisons only)')
    parser.add_argument('--rt-target-factor', type=float, default=1.0,
                        help='experimental headroom multiplier in (0, 1] applied only to proposed RT autoscaler internal target (default: 1.0)')
    parser.add_argument('--latency-floor-ms', type=int,
                        help='reference latency in ms (default: 0.8 x --slo-ms, i.e. 120 for the 150 ms SLO, for native HPA full and staircase comparisons; 0 uses raw RT/SLO)')
    parser.add_argument('--asymmetric-latency', action='store_true',
                        help='use raw p90 below target and floor-adjusted scaling above target')
    parser.add_argument('--fixed-replicas', type=int,
                        help='fixed replica count for both mobility9 methods; clamps HPA min/max and initial Deployment replicas')
    parser.add_argument('--no-hpa', action='store_true',
                        help='run a fixed-replica Go placement comparison without an HPA')
    parser.add_argument('--bounded-hpa-behavior', action='store_true',
                        help='apply the staircase HPA rate limits and stabilization to both full-comparison arms')
    parser.add_argument('--pod-share-scoring', action='store_true',
                        help='use managed pod share for Go placement (already the default)')
    parser.add_argument('--min-replicas', type=int,
                        help='shared minimum replica count (default: 3; under --vertical-mode, vertical-arm total budget including core_backend_replicas, e.g. 6 -> 3 edge + 3 Core)')
    parser.add_argument('--max-replicas', type=int,
                        help='shared replica ceiling (default: 18 for staircase, 24 for balanced, config.yaml for other non-vertical runs; under --vertical-mode, vertical-arm total budget including core_backend_replicas, e.g. 18 -> 15 edge + 3 Core)')
    parser.add_argument('--scale-down-stabilization-seconds', type=int,
                        help='shared HPA scale-down stabilization window in seconds (default: 60 for staircase, config.yaml otherwise)')
    parser.add_argument('--proposed-scale-down-period-seconds', type=int,
                        help='rate-limit the proposed HPA to one pod removal per configured interval')
    parser.add_argument('--scale-down-pods', type=int,
                        help='shared HPA scale-down pods per period (default: none, i.e. the Kubernetes default rate)')
    parser.add_argument('--scale-down-period-seconds', type=int,
                        help='shared HPA scale-down period in seconds (default: none, i.e. the Kubernetes default rate)')
    parser.add_argument('--method-order', choices=('baseline-first', 'proposed-first', 'random'),
                        default='baseline-first',
                        help='order in which baseline and proposed run (default: baseline-first)')
    parser.add_argument('--arms', choices=sorted(ARM_SELECTIONS), default='all',
                        help='four-arm only: run all arms, or only the vertical arms (smoke profile)')
    parser.add_argument('--vertical-mode', choices=VERTICAL_MODES, default='off',
                        help='vertical offloading mode: off (default), local-only (0 Core), vertical (Core fallback), or four-arm (both modes and methods, same total ceiling)')
    parser.add_argument('--debug', action='store_true',
                        help='60s phases and shorter controller/HPA timers for debugging; separate from evaluation runs')
    duration_options = parser.add_mutually_exclusive_group()
    duration_options.add_argument('--long-run', action='store_true',
                                  help='mobility9 only: use 120s phases (default for mobility9; kept for compatibility)')
    duration_options.add_argument('--phase-duration', type=float,
                                  help='traffic seconds per phase (minimum 60; default: 120 for all full experiments; joint reads campaign config; 60 with --debug)')
    parser.add_argument('--rps-per-ue', type=float, default=DEFAULT_RPS_PER_UE,
                        help='target request arrival rate per active UE in requests/second (default: 5)')
    parser.add_argument('--max-in-flight-per-ue', type=int, default=DEFAULT_MAX_IN_FLIGHT_PER_UE,
                        help='maximum concurrent in-flight requests per UE (default: 8)')
    parser.add_argument('--resource-weights', nargs=2, type=float,
                        metavar=('CPU', 'MEMORY'),
                        help='proposed resource-score weights, nonnegative and summing to 1')
    parser.add_argument('--locality-weight', type=float,
                        help='proposed traffic-share weight between 0 and 1 (default: config.yaml)')
    routing_options = parser.add_mutually_exclusive_group()
    routing_options.add_argument('--prefer-close', action='store_true', help='enable PreferClose Service routing')
    routing_options.add_argument('--prefer-close-routing-only', action='store_true',
                                 help='enable PreferClose Service routing (alias for --prefer-close)')
    routing_options.add_argument('--cluster-wide', action='store_true',
                                 help='use cluster-wide Service routing even when site anchors are enabled')
    parser.add_argument('--edge-only', action='store_true',
                        help='place workload replicas only on the three gNB nodes')
    parser.add_argument('--spill-high', type=int, metavar='N', help='mark a pod unready at N pending detections')
    parser.add_argument('--spill-low', type=int, metavar='N', help='restore readiness at N or fewer pending detections')
    parser.add_argument('--site-anchors', action='store_true',
                        help='start both arms with one fixed serving pod per gNB within the total replica budget')
    return parser


def _parse_args(argv=None):
    """Parse arguments; a profile supplies defaults that explicit flags may still override."""
    parser = _build_parser()
    selected, _ = parser.parse_known_args(argv)
    if selected.profile:
        parser.set_defaults(**PROFILES[selected.profile])
    return parser, parser.parse_args(argv)
