"""Verify effective defaults in rendered plans without touching the cluster."""
import json
import subprocess

import pytest
import yaml

from experiments import campaign


@pytest.mark.parametrize('flags,comparison,durations,ceiling,deschedule', [
    (['--scenario', 'mobility9', '--fixed-replicas', '3'], 'full', [120] * 7, 3, 30),
    (['--scenario', 'scaler-staircase'], 'scaling', [120] * 13, 18, None),
    (['--comparison', 'full', '--max-replicas', '12'], 'full', [120] * 13, 12, 30),
])
def test_short_campaign_commands_render_effective_defaults(tmp_path, flags, comparison, durations, ceiling, deschedule):
    output = tmp_path / 'plan'
    campaign.main(['default_test', *flags, '--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['comparison'] == comparison
    if comparison == 'full' and '--scenario' not in flags:
        settings = yaml.safe_load((campaign.ROOT / 'experiments/campaign.yaml').read_text())
        assert plan['schedule'] == settings['schedule']
    assert plan['phase_durations_s'] == durations
    assert plan['tc_ms'] == 10
    assert plan['slo_ms'] == 150
    assert plan['pacing'] == {'rps_per_ue': 5, 'max_in_flight_per_ue': 8}
    assert plan['replica_bounds'] == {'min': 3, 'max': ceiling}
    assert plan['deschedule_interval_seconds'] == deschedule
    for method in ('baseline', 'proposed'):
        config = yaml.safe_load((output / f'{method}-controller.yaml').read_text())
        hpa = yaml.safe_load((output / f'{method}-hpa.yaml').read_text())
        deployment = next(d for d in yaml.safe_load_all((output / f'{method}-workload.yaml').read_text())
                          if d and d.get('kind') == 'Deployment')
        assert config['min_replicas'] == hpa['spec']['minReplicas'] == deployment['spec']['replicas'] == 3
        assert hpa['spec']['maxReplicas'] == ceiling
        assert config['slo_seconds'] == .15
        if deschedule is not None:
            assert config['deschedule_interval_seconds'] == deschedule


def test_explicit_options_override_all_shared_defaults(tmp_path):
    output = tmp_path / 'overrides'
    campaign.main(['override_test', '--comparison', 'full', '--phase-duration', '90',
                   '--deschedule-interval', '45', '--tc-ms', '15', '--slo-ms', '200',
                   '--rps-per-ue', '4', '--min-replicas', '2', '--max-replicas', '10',
                   '--max-in-flight-per-ue', '6', '--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['phase_durations_s'] == [90] * 13
    assert plan['deschedule_interval_seconds'] == 45
    assert plan['tc_ms'] == 15 and plan['slo_ms'] == 200
    assert plan['replica_bounds'] == {'min': 2, 'max': 10}
    assert plan['pacing'] == {'rps_per_ue': 4, 'max_in_flight_per_ue': 6}
    config = yaml.safe_load((output / 'proposed-controller.yaml').read_text())
    assert config['deschedule_interval_seconds'] == 45
    assert config['min_replicas'] == 2 and config['max_replicas'] == 10


def test_ramp_shortcut_renders_full_schedule_with_inherited_defaults(tmp_path):
    script = tmp_path / 'repo/experiments/run.sh'
    script.parent.mkdir(parents=True)
    script.write_text((campaign.ROOT / 'experiments/run.sh').read_text())
    python = script.parent.parent / '.venv/bin/python'
    python.parent.mkdir(parents=True)
    python.write_text('#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n')
    python.chmod(0o755)
    output = tmp_path / 'ramp-plan'
    result = subprocess.check_output(['bash', str(script), 'ramp', 'scaling_full_01',
                                      '--max-replicas', '12', '--plan-only', str(output)], text=True)
    argv = json.loads(result)
    assert argv[:3] == ['-u', '-m', 'experiments.campaign']
    campaign.main(argv[3:])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['comparison'] == 'scaling' and plan['scenario'] == 'scaler-staircase'
    assert plan['phase_durations_s'] == [120] * 13
    assert plan['final_hold_seconds'] == 0
    assert plan['latency_floor_ms'] == 120
    assert plan['effective_rt_metric_target_ms'] == 30
    assert plan['tc_ms'] == 10 and plan['slo_ms'] == 150
    assert plan['replica_bounds'] == {'min': 3, 'max': 12}
    assert plan['pacing'] == {'rps_per_ue': 5, 'max_in_flight_per_ue': 8}
