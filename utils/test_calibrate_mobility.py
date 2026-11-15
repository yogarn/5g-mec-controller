import yaml

from utils import calibrate_mobility


def test_calibration_plan_is_balanced_fixed_six_and_does_not_change_mobility(tmp_path):
    original = calibrate_mobility.campaign.MOBILITY9_SCHEDULE
    output = tmp_path / 'plan'
    calibrate_mobility.main(['--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['campaign_settings']['schedule'] == '9:3:3:3'
    assert plan['campaign_settings']['phase_duration'] == 60
    assert plan['campaign_settings']['rps_per_ue'] == 5
    assert plan['fixed_replicas'] == 6
    for method in ('baseline', 'proposed'):
        hpa = yaml.safe_load((output / f'{method}-hpa.yaml').read_text())
        assert hpa['spec']['minReplicas'] == hpa['spec']['maxReplicas'] == 6
        assert 'mec-campaign-reserved' in (output / f'{method}-workload.yaml').read_text()
    assert calibrate_mobility.campaign.MOBILITY9_SCHEDULE == original
