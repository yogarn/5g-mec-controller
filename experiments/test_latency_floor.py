import json
import subprocess

import pytest
import yaml

from experiments import campaign


def test_floor_plan_changes_only_proposed_signal_and_records_reference(tmp_path):
    out = tmp_path / 'plan'
    campaign.main(['--scenario', 'scaler-staircase', '--slo-ms', '150',
                   '--latency-floor-ms', '130', '--plan-only', str(out)])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    base = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())
    prop = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    assert base['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 60
    assert base['spec']['behavior'] == prop['spec']['behavior']
    assert prop['spec']['metrics'][0]['external']['metric']['name'] == 'ue_p90_excess_latency_130ms_seconds'
    assert prop['spec']['metrics'][0]['external']['target']['value'] == '20m'
    assert plan['slo_ms'] == 150
    assert plan['latency_floor_ms'] == 130
    assert plan['latency_scaling_formula'] == 'max(RT-floor, 0)/(SLO-floor)'
    cm = yaml.safe_load((out / 'floor-adapter-configmap.yaml').read_text())
    rules = yaml.safe_load(cm['data']['config.yaml'])['externalRules']
    assert rules[0]['name']['as'] == 'ue_p90_latency_seconds'
    assert rules[-1]['resources'] == {'namespaced': False}
    assert '- 0.13, 0)' in rules[-1]['metricsQuery']
    assert 'ue_request_window_complete{job="ue-telemetry"} == 1' in rules[-1]['metricsQuery']


@pytest.mark.parametrize('flags', [
    ['--latency-floor-ms', '-1'],
    ['--latency-floor-ms', 'nan'],
    ['--latency-floor-ms', '1.5'],
    ['--latency-floor-ms', '150'],
    ['--latency-floor-ms', '151'],
    ['--latency-floor-ms', '130', '--scenario', 'mobility9'],
    ['--latency-floor-ms', '130', '--scenario', 'scaler-balanced'],
])
def test_invalid_floor_fails_before_writing_plan(tmp_path, flags):
    out = tmp_path / 'plan'
    with pytest.raises(SystemExit):
        campaign.main(['--scenario', 'scaler-staircase', '--slo-ms', '150', '--plan-only', str(out), *flags])
    assert not out.exists()


def test_rules_preserve_unrelated_config_and_multiple_floors():
    first = campaign._floor_adapter_config(120)
    first['data']['unrelated'] = 'keep'
    cfg = yaml.safe_load(first['data']['config.yaml'])
    cfg['rules'] = [{'seriesQuery': 'unrelated'}]
    first['data']['config.yaml'] = yaml.safe_dump(cfg)
    second = campaign._floor_adapter_config(130, first)
    assert campaign._floor_adapter_config(130, second) == second
    cfg2 = yaml.safe_load(second['data']['config.yaml'])
    assert cfg2['rules'] == cfg['rules']
    assert second['data']['unrelated'] == 'keep'
    assert [r['name']['as'] for r in cfg2['externalRules']] == [
        'ue_p90_latency_seconds', 'ue_p90_excess_latency_120ms_seconds', 'ue_p90_excess_latency_130ms_seconds']


@pytest.mark.parametrize('fail_rollout', [False, True])
def test_adapter_install_saves_config_and_rolls_back_failed_rollout(tmp_path, monkeypatch, fail_rollout):
    current = campaign._floor_adapter_config(120)
    current['metadata']['resourceVersion'] = '42'
    monkeypatch.setattr(campaign, 'get', lambda *a: current)
    calls = []

    def kube(*args, **kwargs):
        calls.append(args)
        if fail_rollout and 'status' in args and sum('status' in c for c in calls) == 1:
            raise subprocess.CalledProcessError(1, args)

    monkeypatch.setattr(campaign, 'kube', kube)
    if fail_rollout:
        with pytest.raises(subprocess.CalledProcessError):
            campaign._prepare_floor_adapter(tmp_path, 130)
        patches = [json.loads(c[-1]) for c in calls if 'patch' in c]
        assert patches[-1]['data']['config.yaml'] == current['data']['config.yaml']
    else:
        campaign._prepare_floor_adapter(tmp_path, 130)
    assert json.loads((tmp_path / 'adapter-before.json').read_text()) == current
    assert not any('apply' in c for c in calls)
    first_patch = json.loads(next(c[-1] for c in calls if 'patch' in c))
    assert first_patch['metadata']['resourceVersion'] == '42'
