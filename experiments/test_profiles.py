"""Each named profile must parse to exactly what the former run-script flag lists produced."""
import pytest

from experiments import campaign

_SHARED = [
    '--config', 'experiments/campaign-vertical.yaml', '--vertical-mode', 'four-arm',
    '--scenario', 'mobility9', '--phase-transition', 'continuous', '--rps-per-ue', '5',
    '--max-in-flight-per-ue', '8', '--slo-ms', '150', '--tc-ms', '10', '--autoscaler', 'hpa',
    '--placement-engine', 'go', '--pod-share-scoring', '--deschedule-interval', '30',
    '--locality-weight', '0.4', '--resource-weights', '0.5', '0.5', '0', '0',
    '--prefer-close', '--site-anchors',
]
_FULL = [
    '--comparison', 'full', '--cpu-hpa-target', '80', '--scale-down-stabilization-seconds', '60',
    '--asymmetric-latency', '--rt-target-factor', '1', '--proposed-scale-down-period-seconds', '15',
]
LEGACY = {
    'vertical-joint': [
        *_FULL, *_SHARED, '--phase-schedule', campaign.VERTICAL_JOINT_SCHEDULE, '--phase-duration', '120',
    ],
    'vertical-smoke': [
        *_FULL, *_SHARED, '--phase-schedule', campaign.VERTICAL_SMOKE_SCHEDULE, '--phase-duration', '60',
    ],
    'vertical-mobility': [
        '--comparison', 'placement', '--fixed-replicas', '9', '--no-hpa', *_SHARED,
        '--phase-schedule', campaign.VERTICAL_MOBILITY_SCHEDULE, '--phase-duration', '120',
    ],
    'scaling-staircase': [
        '--config', 'experiments/campaign-mobility-core.yaml', '--workload', 'pedestrian-detection',
        '--comparison', 'scaling', '--scenario', 'scaler-staircase', '--autoscaler', 'hpa',
        '--cpu-hpa-target', '80', '--slo-ms', '150', '--asymmetric-latency',
        '--min-replicas', '3', '--max-replicas', '18', '--scale-down-stabilization-seconds', '60',
        '--phase-duration', '120', '--phase-transition', 'continuous', '--rps-per-ue', '5',
        '--max-in-flight-per-ue', '8', '--tc-ms', '10',
    ],
}


@pytest.mark.parametrize('name', sorted(LEGACY))
def test_profile_matches_former_flag_list(name):
    _, legacy = campaign._parse_args(['run_x', *LEGACY[name]])
    _, profile = campaign._parse_args(['run_x', '--profile', name])
    expected = vars(legacy)
    actual = vars(profile)
    assert actual.pop('profile') == name
    expected.pop('profile')
    assert actual == expected


def test_profile_flags_can_still_be_overridden():
    _, args = campaign._parse_args(['run_x', '--profile', 'vertical-joint', '--phase-duration', '30'])
    assert args.phase_duration == 30


def test_mobility_profile_uses_24_ues_with_hotspot_at_most_18():
    phases = campaign.parse_schedule(campaign.VERTICAL_MOBILITY_SCHEDULE)
    assert {p['workload_ue'] for p in phases} == {24}
    assert max(max(p['gnb1_ues'], p['gnb2_ues'], p['gnb3_ues']) for p in phases[1:]) == 18
