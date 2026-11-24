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
    assert base['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 80
    assert base['spec']['behavior'] == prop['spec']['behavior']
    assert prop['spec']['metrics'][0]['external']['metric']['name'] == 'ue_p90_excess_latency_130ms_seconds'
    assert prop['spec']['metrics'][0]['external']['target']['value'] == '20m'
    assert plan['slo_ms'] == 150
    assert plan['latency_floor_ms'] == 130
    assert plan['latency_scaling_formula'] == 'max(RT-floor, 0)/(SLO-floor)'
    assert plan['phase_durations_s'] == [120.0] * 7
    assert plan['final_hold_seconds'] == 0
    cm = yaml.safe_load((out / 'floor-adapter-configmap.yaml').read_text())
    rules = yaml.safe_load(cm['data']['config.yaml'])['externalRules']
    assert rules[0]['name']['as'] == 'ue_p90_latency_seconds'
    assert rules[-1]['resources'] == {'namespaced': False}
    assert '- 0.13, 0)' in rules[-1]['metricsQuery']
    assert 'ue_request_window_complete' not in rules[-1]['metricsQuery']


@pytest.mark.parametrize('factor,excess_target', [('1.0', '20m'), ('0.95', '12.5')])
def test_joint_floor_preserves_placement_and_reporting(tmp_path, factor, excess_target):
    out = tmp_path / 'plan'
    campaign.main(['--comparison', 'full', '--slo-ms', '150', '--tc-ms', '10',
                   '--cpu-hpa-target', '80', '--latency-floor-ms', '130',
                   '--rt-target-factor', factor, '--max-replicas', '12',
                   '--plan-only', str(out)])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    base = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())
    prop = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    assert base['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 80
    assert prop['spec']['metrics'][0]['external']['target']['value'] == excess_target
    unit = 'seconds' if factor == '1.0' else 'milliseconds'
    assert prop['spec']['metrics'][0]['external']['metric']['name'] == f'ue_p90_excess_latency_130ms_{unit}'
    assert plan['reporting_slo_ms'] == 150
    assert plan['latency_floor_ms'] == plan['campaign_settings']['latency_floor_ms'] == 130
    assert plan['tc_ms'] == 10
    assert plan['node_restriction'] is None
    assert plan['replica_bounds'] == {'min': 3, 'max': 12}
    assert plan['phase_durations_s'] == [120.0] * 13
    assert plan['deschedule_interval_seconds'] == 30
    for method, scheduler in (('baseline', 'default-scheduler'), ('proposed', 'mec-placement-scheduler')):
        dep = next(d for d in yaml.safe_load_all((out / f'{method}-workload.yaml').read_text())
                   if d and d.get('kind') == 'Deployment')
        spec = dep['spec']['template']['spec']
        assert spec['schedulerName'] == scheduler
        terms = spec['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms']
        assert {value for term in terms for match in term['matchFields']
                for value in match['values']} == {'gnb-2', 'gnb-3', 'core-5g'}
    cfg = yaml.safe_load((out / 'proposed-controller.yaml').read_text())
    assert cfg['slo_seconds'] == .15


@pytest.mark.parametrize('flags', [
    ['--latency-floor-ms', '-1'],
    ['--latency-floor-ms', 'nan'],
    ['--latency-floor-ms', '1.5'],
    ['--latency-floor-ms', '150'],
    ['--latency-floor-ms', '151'],
    ['--latency-floor-ms', '130', '--scenario', 'mobility9', '--comparison', 'placement'],
    ['--latency-floor-ms', '130', '--scenario', 'scaler-balanced'],
    ['--latency-floor-ms', '130', '--comparison', 'full', '--autoscaler', 'controller'],
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


@pytest.mark.parametrize('flags,stabilization', [
    (['--comparison', 'full', '--scale-down-stabilization-seconds', '300'], 300),
    (['--comparison', 'scaling', '--scenario', 'scaler-staircase'], 60),
])
def test_asymmetric_hpa_plan(tmp_path, flags, stabilization):
    out = tmp_path / 'asymmetric'
    campaign.main([*flags, '--asymmetric-latency', '--plan-only', str(out)])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    proposed = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())['spec']
    baseline = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())['spec']
    external = proposed['metrics'][0]['external']
    assert external['target']['value'] == '150m'
    assert baseline['metrics'][0]['resource']['target']['averageUtilization'] == 80
    assert proposed['behavior']['scaleDown']['stabilizationWindowSeconds'] == stabilization
    name = external['metric']['name']
    assert plan['rt_metric_name'] == name
    adapter = yaml.safe_load((out / 'floor-adapter-configmap.yaml').read_text())
    rules = yaml.safe_load(adapter['data']['config.yaml'])['externalRules']
    query = next(rule['metricsQuery'] for rule in rules if rule['name']['as'] == name)
    assert '(120 / 30' in query
    assert '- 0.15, 0)' in query
    assert 'ue_request_window_complete' not in query


def test_floor_adapter_config_prunes_orphan_asymmetric_rules():
    base = campaign._floor_adapter_config(125, asymmetric_target_ms=150)
    with_both = campaign._floor_adapter_config(120, base, asymmetric_target_ms=150)
    cleaned = campaign._floor_adapter_config(120, with_both)
    names = [r['name']['as'] for r in yaml.safe_load(cleaned['data']['config.yaml'])['externalRules']]
    assert names == ['ue_p90_latency_seconds', 'ue_p90_excess_latency_120ms_seconds']
