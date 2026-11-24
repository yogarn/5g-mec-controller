"""Experiment profile definitions, schedules, and shared campaign constants.

Centralizes named experiment profiles (vertical-joint, vertical-smoke,
vertical-mobility, scaling-staircase), site/node topology constants, and
autoscaler defaults loaded from autoscaler/autoscaler.yaml.
"""
from pathlib import Path

from autoscaler.config import load_autoscaler_config


ROOT = Path(__file__).resolve().parent.parent
NAMESPACE = 'services'
APP = 'mec-placement-pedestrian-detection'
WORKLOADS = ('pedestrian-detection',)
SITE_NODES = {'gnb-1': 'core-5g', 'gnb-2': 'gnb-2', 'gnb-3': 'gnb-3'}
LOCAL_SITE_NODES = {**SITE_NODES, "gnb-1": "gnb1"}
CORE_GNB1_SESSION = ('embb.testbed', '172.16.45.0/24')
LOCAL_GNB1_SESSION = ('mec.icn-2.testbed', '172.16.50.0/24')
LOCAL_GNB2_SESSION = ('embb.gnb2.testbed', '10.63.1.0/24')
LOCAL_GNB3_SESSION = ('embb.gnb3.testbed', '10.64.1.0/24')
VERTICAL_MODES = ('off', 'local-only', 'vertical', 'four-arm')
FOUR_ARM_SPECS = (
    ('baseline-local', 'baseline', 'local-only'),
    ('proposed-local', 'proposed', 'local-only'),
    ('baseline-vertical', 'baseline', 'vertical'),
    ('proposed-vertical', 'proposed', 'vertical'),
)
# 'vertical' runs only the two offloading arms; used by the smoke profile because the local-only arms are
# unaffected by offloader changes. Full runs always use 'all'.
ARM_SELECTIONS = {
    'all': FOUR_ARM_SPECS,
    'local': tuple(spec for spec in FOUR_ARM_SPECS if spec[2] == 'local-only'),
    'vertical': tuple(spec for spec in FOUR_ARM_SPECS if spec[2] == 'vertical'),
    # One arm per name; used by the demo profiles to show a single configuration live.
    **{spec[0]: (spec,) for spec in FOUR_ARM_SPECS},
}
UPF_PLACEMENT_QUERY = (
    'ue_request_upf_placement_window_attempts{job="ue-telemetry-placement",node=~"core-5g|gnb-2|gnb-3"} / 5'
)
MOBILITY9_SCHEDULE = '9:7:1:1,9:5:3:1,9:3:5:1,9:1:7:1,9:1:5:3,9:1:3:5,9:1:1:7'
SCALER_BALANCED_SCHEDULE = '3:1:1:1,9:3:3:3,15:5:5:5,24:8:8:8,15:5:5:5,9:3:3:3,3:1:1:1'
SCALER_STAIRCASE_SCHEDULE = ','.join(f'{n}:{n//3}:{n//3}:{n//3}' for n in (3, 9, 15, 21, 24, 12, 3))
SCALER_SCENARIOS = ('scaler-balanced', 'scaler-staircase')
AUTOSCALER_CONFIG = load_autoscaler_config()
SCALER_NODE = 'core-5g'
SCALER_MIN_REPLICAS = AUTOSCALER_CONFIG['min_replicas']
SCALER_DEFAULT_MAX_REPLICAS = AUTOSCALER_CONFIG['max_replicas']
SCALER_STAIRCASE_MAX_REPLICAS = AUTOSCALER_CONFIG['staircase_max_replicas']
DEFAULT_CPU_HPA_TARGET = AUTOSCALER_CONFIG['cpu_hpa_target']
DEFAULT_SLO_MS = AUTOSCALER_CONFIG['slo_ms']
# Reference latency L0 = 0.8 x SLO: replicas double when p90 reaches 1.2 x SLO, ((RT-L0)/(SLO-L0) = 2).
DEFAULT_LATENCY_FLOOR_FRACTION = AUTOSCALER_CONFIG['latency_floor_fraction']
DEFAULT_LATENCY_FLOOR_MS = round(DEFAULT_LATENCY_FLOOR_FRACTION * DEFAULT_SLO_MS)
DEFAULT_TC_MS = 10
DEFAULT_MIN_REPLICAS = AUTOSCALER_CONFIG['min_replicas']
DEFAULT_DESCHEDULE_INTERVAL_S = 30
DEFAULT_PHASE_SECONDS = 120
MOBILITY_PHASE_SECONDS = DEFAULT_PHASE_SECONDS
SCALER_PHASE_SECONDS = DEFAULT_PHASE_SECONDS

# Named experiment profiles. The run scripts only select a profile; every experiment constant lives here.
# Smoke and the full joint run share all settings except the schedule and phase length.
# Joint schedule (total:gnb1:gnb2:gnb3). Focused 7-phase gnb-2 hotspot ramp (1->6->12->18->22 UEs on gnb-2, peak 24 UEs total)
# with gnb-1 and gnb-3 held at 1 UE during the ramp so baseline CPU HPA cannot over-provision gnb-2 by accident, followed by
# a partial rebalance onto gnb-3 (12:1:5:6) for the descheduler and cooldown (3:1:1:1).
VERTICAL_JOINT_SCHEDULE = '3:1:1:1,8:1:6:1,14:1:12:1,20:1:18:1,24:1:22:1,12:1:5:6,3:1:1:1'
VERTICAL_SMOKE_SCHEDULE = '3:1:1:1,6:4:1:1,3:1:1:1'
# 24 UEs (the reserved subscribers); 9 fixed replicas give baseline-local 3:3:3 (saturating above ~16-17 UEs/site)
# while proposed-local can concentrate up to 7 pods on the hot site (1:7:1 / 1:1:7).
# Symmetric sweep: gnb-1 holds 1 UE throughout; hotspot builds on gnb-2 to 22 UEs and sweeps across to gnb-3 (22 UEs).
VERTICAL_MOBILITY_SCHEDULE = '24:1:11:12,24:1:17:6,24:1:22:1,24:1:16:7,24:1:11:12,24:1:6:17,24:1:1:22'
_VERTICAL_COMMON = {
    'config': 'experiments/campaign-vertical.yaml',
    'vertical_mode': 'four-arm',
    'scenario': 'mobility9',
    'phase_duration': 120.0,
    'rps_per_ue': 5.0,
    'slo_ms': 150.0,
    'tc_ms': 10.0,
    'pod_share_scoring': True,
    'deschedule_interval': 30.0,
    'locality_weight': 0.4,
    'resource_weights': [0.5, 0.5],
    'prefer_close': True,
    'site_anchors': True,
}
_VERTICAL_FULL = {
    'scenario': 'joint',
    'comparison': 'full',
    'cpu_hpa_target': 80,
    'scale_down_stabilization_seconds': 60,
    'scale_down_pods': 1,
    'scale_down_period_seconds': 15,
    'asymmetric_latency': False,
}
PROFILES = {
    'vertical-joint': {**_VERTICAL_COMMON, **_VERTICAL_FULL, 'phase_schedule': VERTICAL_JOINT_SCHEDULE},
    # Demo: the final joint schedule (7 phases x 120 s, 14 min of traffic plus setup) for ONE arm; see demo.sh.
    **{f'demo-{spec[0]}': {**_VERTICAL_COMMON, **_VERTICAL_FULL, 'phase_schedule': VERTICAL_JOINT_SCHEDULE,
                           'arms': spec[0]} for spec in FOUR_ARM_SPECS},
    # The two local-only arms (baseline-local, proposed-local) of the joint run with the default schedule.
    'vertical-joint-local': {**_VERTICAL_COMMON, **_VERTICAL_FULL, 'phase_schedule': VERTICAL_JOINT_SCHEDULE,
                             'arms': 'local'},
    # The two offloading arms of the joint run with the default schedule (the local arms do not use the offloader).
    'vertical-joint-vertical': {**_VERTICAL_COMMON, **_VERTICAL_FULL, 'phase_schedule': VERTICAL_JOINT_SCHEDULE,
                                'arms': 'vertical'},
    'vertical-smoke': {**_VERTICAL_COMMON, **_VERTICAL_FULL, 'phase_schedule': VERTICAL_SMOKE_SCHEDULE,
                       'phase_duration': 60.0, 'arms': 'vertical'},
    'vertical-mobility': {**_VERTICAL_COMMON, 'comparison': 'placement', 'fixed_replicas': 9, 'no_hpa': True,
                          'phase_schedule': VERTICAL_MOBILITY_SCHEDULE},
    'vertical-mobility-local': {**_VERTICAL_COMMON, 'comparison': 'placement', 'fixed_replicas': 9, 'no_hpa': True,
                                'phase_schedule': VERTICAL_MOBILITY_SCHEDULE, 'arms': 'local'},
    'vertical-mobility-vertical': {**_VERTICAL_COMMON, 'comparison': 'placement', 'fixed_replicas': 9, 'no_hpa': True,
                                   'phase_schedule': VERTICAL_MOBILITY_SCHEDULE, 'arms': 'vertical'},
    'scaling-staircase': {
        'config': 'experiments/campaign-mobility-core.yaml',
        'workload': 'pedestrian-detection',
        'comparison': 'scaling',
        'scenario': 'scaler-staircase',
        'cpu_hpa_target': 80,
        'slo_ms': 150.0,
        # Full floor formula (p90 - floor) / (SLO - floor) in both directions: this stage has a ~140 ms latency
        # floor above L0, so the below-target raw p90 branch of the asymmetric metric never allowed a scale-down.
        'asymmetric_latency': False,
        'min_replicas': 3,
        'max_replicas': 18,
        'scale_down_stabilization_seconds': 60,
        'scale_down_pods': 1,
        'scale_down_period_seconds': 15,
        'phase_duration': 120.0,
        'rps_per_ue': 5.0,
        'tc_ms': 10.0,
    },
}
FINAL_HOLD_SECONDS = 360
COMPARISONS = ('full', 'placement', 'scaling')
AUTOSCALERS = ('hpa',)
PLACEMENT_ENGINES = ('go',)
PLACEMENT_NAMESPACE = 'placement'
PLACEMENT_PROMETHEUS_URL = 'http://placement-prometheus.placement.svc:9090'
COMPARISON_NOTES = {
    'placement': 'Comparison A (placement): same latency-ratio autoscaler for both methods; isolates scheduler/descheduler placement.',
    'full': 'Comparison B (full system): default-scheduler + CPU HPA vs full proposed system (autoscaler + scheduler + descheduler); do not attribute results solely to placement.',
    'scaling': 'Comparison C (scaling): default-scheduler + core-5g-only required affinity for both methods; native CPU HPA vs native latency HPA (targets recorded in hpa_targets).',
}


SPILL_READINESS_PROBE = {
    'periodSeconds': 2, 'failureThreshold': 1, 'successThreshold': 2, 'timeoutSeconds': 1,
}

_GO_PLACEMENT_OMIT_KEYS = (
    'interval_seconds',
    'cooldown_seconds',
    'scale_down_seconds',
    'slo_seconds',
    'latency_query',
    'min_replicas',
    'max_replicas',
)
