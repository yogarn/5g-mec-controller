"""Each named profile must parse to exactly what the former run-script flag lists produced."""
import pytest

from experiments import campaign

_SHARED = [
    '--config', 'experiments/campaign-vertical.yaml', '--vertical-mode', 'four-arm',
    '--phase-transition', 'continuous', '--rps-per-ue', '5',
    '--slo-ms', '150', '--tc-ms', '10', '--autoscaler', 'hpa',
    '--placement-engine', 'go', '--pod-share-scoring', '--deschedule-interval', '30',
    '--locality-weight', '0.4', '--resource-weights', '0.5', '0.5',
    '--prefer-close', '--site-anchors',
]
_FULL = [
    '--scenario', 'joint', '--comparison', 'full', '--cpu-hpa-target', '80', '--scale-down-stabilization-seconds', '60',
    '--scale-down-pods', '1', '--scale-down-period-seconds', '15',
    '--rt-target-factor', '1',
]
LEGACY = {
    'vertical-joint': [
        *_FULL, *_SHARED, '--phase-schedule', campaign.VERTICAL_JOINT_SCHEDULE, '--phase-duration', '120',
    ],
    'vertical-smoke': [
        *_FULL, *_SHARED, '--phase-schedule', campaign.VERTICAL_SMOKE_SCHEDULE, '--phase-duration', '60',
        '--arms', 'vertical',
    ],
    'vertical-mobility': [
        '--scenario', 'mobility9', '--comparison', 'placement', '--fixed-replicas', '9', '--no-hpa', *_SHARED,
        '--phase-schedule', campaign.VERTICAL_MOBILITY_SCHEDULE, '--phase-duration', '120',
    ],
    'scaling-staircase': [
        '--config', 'experiments/campaign-mobility-core.yaml', '--workload', 'pedestrian-detection',
        '--comparison', 'scaling', '--scenario', 'scaler-staircase', '--autoscaler', 'hpa',
        '--cpu-hpa-target', '80', '--slo-ms', '150',  # full floor formula (no --asymmetric-latency), see PROFILES
        '--min-replicas', '3', '--max-replicas', '18', '--scale-down-stabilization-seconds', '60',
        '--scale-down-pods', '1', '--scale-down-period-seconds', '15',
        '--phase-duration', '120', '--phase-transition', 'continuous', '--rps-per-ue', '5',
        '--tc-ms', '10',
    ],
}
LEGACY['scaling-staircase-local'] = [
    *LEGACY['scaling-staircase'][:1], 'experiments/campaign-scaling-local.yaml', *LEGACY['scaling-staircase'][2:],
]


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


def test_mobility_profile_uses_24_ues_with_hotspot_peak_22():
    phases = campaign.parse_schedule(campaign.VERTICAL_MOBILITY_SCHEDULE)
    assert {p['workload_ue'] for p in phases} == {24}
    assert max(max(p['gnb1_ues'], p['gnb2_ues'], p['gnb3_ues']) for p in phases[1:]) == 22


def test_only_smoke_and_demo_profiles_select_arms():
    assert {n: p.get('arms', 'all') for n, p in campaign.PROFILES.items()} == {
        'vertical-joint': 'all', 'vertical-smoke': 'vertical',
        'vertical-joint-local': 'local', 'vertical-joint-vertical': 'vertical',
        'vertical-mobility': 'all',
        'vertical-mobility-local': 'local', 'vertical-mobility-vertical': 'vertical',
        'scaling-staircase': 'all',
        'scaling-staircase-local': 'all',
        'demo-baseline-local': 'baseline-local', 'demo-proposed-local': 'proposed-local',
        'demo-baseline-vertical': 'baseline-vertical', 'demo-proposed-vertical': 'proposed-vertical',
    }
    assert [a for a, _m, _v in campaign.ARM_SELECTIONS['local']] == ['baseline-local', 'proposed-local']
    assert [a for a, _m, _v in campaign.ARM_SELECTIONS['vertical']] == ['baseline-vertical', 'proposed-vertical']
    assert [a for a, _m, _v in campaign.ARM_SELECTIONS['all']] == [
        'baseline-local', 'proposed-local', 'baseline-vertical', 'proposed-vertical']


def test_demo_profiles_are_the_joint_profile_for_a_single_arm():
    from experiments.profiles import ARM_SELECTIONS, FOUR_ARM_SPECS, PROFILES
    for arm, _method, _mode in FOUR_ARM_SPECS:
        demo = dict(PROFILES[f'demo-{arm}'])
        assert demo.pop('arms') == arm
        assert [spec[0] for spec in ARM_SELECTIONS[arm]] == [arm]
        assert demo == {k: v for k, v in PROFILES['vertical-joint'].items() if k != 'arms'}


def test_local_scaling_profile_puts_gnb1_on_its_local_upf():
    _, args = campaign._parse_args(['run_x', '--profile', 'scaling-staircase-local'])
    assert args.comparison == 'scaling'
    settings = campaign.load(args.config)
    assert settings['radio_only_gnb1'] is False
    assert settings['site_node_ips']['gnb-1'] == '10.34.211.157'
    remote = campaign.remote_settings(settings)
    assert (remote['gnb1_dnn'], remote['gnb1_subnet']) == ('mec.icn-2.testbed', '172.16.50.0/24')
    assert (remote['gnb2_dnn'], remote['gnb3_dnn']) == ('embb.gnb2.testbed', 'embb.gnb3.testbed')


def test_legacy_scaling_profile_still_uses_the_core_dnn_for_gnb1():
    _, args = campaign._parse_args(['run_x', '--profile', 'scaling-staircase'])
    settings = campaign.load(args.config)
    assert campaign.remote_settings(settings)['gnb1_dnn'] == 'embb.testbed'
