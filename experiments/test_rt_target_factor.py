from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from experiments import campaign


def test_default_factor_backward_compatible(tmp_path):
    out = tmp_path / 'plan_default'
    campaign.main([
        '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--latency-floor-ms', '130', '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    prop = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    base = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())

    assert plan['slo_ms'] == 150
    assert plan['reporting_slo_ms'] == 150
    assert plan['rt_target_factor'] == 1.0
    assert plan['internal_rt_target_ms'] == 150
    assert plan['latency_floor_ms'] == 130
    assert plan['effective_rt_metric_target_ms'] == 20
    assert plan['latency_scaling_formula'] == 'max(RT-floor, 0)/(SLO-floor)'
    assert plan['hpa_targets']['baseline'] == {'type': 'cpu_utilization_percent', 'target': 60}
    assert plan['hpa_targets']['proposed'] == {
        'type': 'excess_latency_p90_ms',
        'target': 20,
        'floor_ms': 130,
        'slo_ms': 150,
        'metric': 'ue_p90_excess_latency_130ms_seconds',
    }
    assert prop['spec']['metrics'][0]['external']['target']['value'] == '20m'
    assert base['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 60


def test_raw_mode_slo_150_factor_095(tmp_path):
    out = tmp_path / 'plan_raw_095'
    campaign.main([
        '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--rt-target-factor', '0.95', '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    prop = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    base = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())

    assert plan['slo_ms'] == 150
    assert plan['reporting_slo_ms'] == 150
    assert plan['rt_target_factor'] == 0.95
    assert plan['internal_rt_target_ms'] == 142.5
    assert plan['latency_floor_ms'] == 0
    assert plan['effective_rt_metric_target_ms'] == 142.5
    assert plan['rt_metric_name'] == 'ue_p90_latency_milliseconds'
    assert plan['rt_metric_unit'] == 'milliseconds'
    assert plan['rt_hpa_target_value'] == '142.5'
    assert plan['latency_scaling_formula'] == 'RT/internal_target'
    assert plan['campaign_settings']['rt_target_factor'] == 0.95
    assert plan['campaign_settings']['rt_metric_unit'] == 'milliseconds'
    assert prop['spec']['metrics'][0]['external']['metric']['name'] == 'ue_p90_latency_milliseconds'
    assert prop['spec']['metrics'][0]['external']['target']['value'] == '142.5'
    cm = yaml.safe_load((out / 'latency-adapter-configmap.yaml').read_text())
    rules = yaml.safe_load(cm['data']['config.yaml'])['externalRules']
    assert rules[-1]['name']['as'] == 'ue_p90_latency_milliseconds'
    assert '* 1000' in rules[-1]['metricsQuery']
    assert base['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 60
    assert base['spec']['behavior'] == prop['spec']['behavior']
    assert base['spec']['minReplicas'] == prop['spec']['minReplicas'] == 1
    assert base['spec']['maxReplicas'] == prop['spec']['maxReplicas'] == 24


def test_floor_mode_slo_150_floor_130_factor_095(tmp_path):
    out = tmp_path / 'plan_floor_095'
    campaign.main([
        '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--latency-floor-ms', '130',
        '--rt-target-factor', '0.95', '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    prop = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    base = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())

    assert plan['slo_ms'] == 150
    assert plan['reporting_slo_ms'] == 150
    assert plan['rt_target_factor'] == 0.95
    assert plan['internal_rt_target_ms'] == 142.5
    assert plan['latency_floor_ms'] == 130
    assert plan['effective_rt_metric_target_ms'] == 12.5
    assert plan['rt_metric_name'] == 'ue_p90_excess_latency_130ms_milliseconds'
    assert plan['rt_metric_unit'] == 'milliseconds'
    assert plan['rt_hpa_target_value'] == '12.5'
    assert plan['latency_scaling_formula'] == 'max(RT-floor, 0)/(internal_target-floor)'
    assert plan['hpa_targets']['proposed'] == {
        'type': 'excess_latency_p90_ms',
        'target': 12.5,
        'floor_ms': 130,
        'slo_ms': 150,
        'internal_target_ms': 142.5,
        'rt_target_factor': 0.95,
        'metric': 'ue_p90_excess_latency_130ms_milliseconds',
        'metric_unit': 'milliseconds',
        'hpa_target_value': '12.5',
    }
    assert prop['spec']['metrics'][0]['external']['metric']['name'] == 'ue_p90_excess_latency_130ms_milliseconds'
    assert prop['spec']['metrics'][0]['external']['target']['value'] == '12.5'
    cm = yaml.safe_load((out / 'floor-adapter-configmap.yaml').read_text())
    rules = yaml.safe_load(cm['data']['config.yaml'])['externalRules']
    assert rules[-1]['name']['as'] == 'ue_p90_excess_latency_130ms_milliseconds'
    assert '(clamp_min(ue_request_attempt_duration_p90_seconds{job="ue-telemetry"} - 0.13, 0)) * 1000' in rules[-1]['metricsQuery']
    assert base['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 60
    assert base['spec']['behavior'] == prop['spec']['behavior']


def test_floor_mode_slo_150_floor_130_factor_090(tmp_path):
    out = tmp_path / 'plan_floor_090'
    campaign.main([
        '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--latency-floor-ms', '130',
        '--rt-target-factor', '0.90', '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    prop = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())

    assert plan['slo_ms'] == 150
    assert plan['internal_rt_target_ms'] == 135
    assert plan['effective_rt_metric_target_ms'] == 5
    assert prop['spec']['metrics'][0]['external']['target']['value'] == '5m'


@pytest.mark.parametrize('flags', [
    ['--rt-target-factor', '0'],
    ['--rt-target-factor', '-0.1'],
    ['--rt-target-factor', '1.01'],
    ['--rt-target-factor', 'nan'],
    ['--rt-target-factor', 'inf'],
    ['--rt-target-factor', '-inf'],
    # 150 * 0.85 = 127.5 <= 130
    ['--latency-floor-ms', '130', '--rt-target-factor', '0.85'],
    # 200 * 0.65 = 130 <= 130 (at floor)
    ['--slo-ms', '200', '--latency-floor-ms', '130', '--rt-target-factor', '0.65'],
])
def test_invalid_factor_or_target_at_or_below_floor_rejected(tmp_path, flags):
    out = tmp_path / 'plan_invalid'
    base_args = ['--comparison', 'scaling', '--scenario', 'scaler-staircase', '--slo-ms', '150', '--plan-only', str(out)]
    # Allow --slo-ms override when present in flags
    if '--slo-ms' in flags:
        base_args = ['--comparison', 'scaling', '--scenario', 'scaler-staircase', '--plan-only', str(out)]
    with pytest.raises(SystemExit):
        campaign.main([*base_args, *flags])
    assert not out.exists()


@pytest.mark.parametrize('factor', [0, -0.5, 1.1, float('nan'), float('inf'), True])
def test_method_configs_rejects_invalid_factor(tmp_path, factor):
    with pytest.raises(ValueError, match='rt_target_factor'):
        campaign._method_configs(
            tmp_path, 'proposed', 150, 24,
            comparison='scaling', autoscaler='hpa', staircase=True,
            rt_target_factor=factor,
        )


def test_controller_autoscaler_rejects_non_unit_factor(tmp_path):
    out = tmp_path / 'plan_ctrl'
    with pytest.raises(SystemExit):
        campaign.main([
            '--comparison', 'full', '--autoscaler', 'controller',
            '--slo-ms', '150', '--rt-target-factor', '0.95', '--plan-only', str(out),
        ])
    assert not out.exists()

    with pytest.raises(ValueError, match='native HPA'):
        campaign._method_configs(
            tmp_path, 'proposed', 150, 10,
            comparison='full', autoscaler='controller',
            rt_target_factor=0.95,
        )


def test_placement_comparison_rejects_non_unit_factor(tmp_path):
    out = tmp_path / 'plan_placement'
    with pytest.raises(SystemExit):
        campaign.main([
            '--comparison', 'placement', '--slo-ms', '150',
            '--rt-target-factor', '0.95', '--plan-only', str(out),
        ])
    assert not out.exists()

    with pytest.raises(ValueError, match='--comparison placement'):
        campaign._method_configs(
            tmp_path, 'proposed', 150, 10,
            comparison='placement', autoscaler='hpa',
            rt_target_factor=0.95,
        )


def test_campaign_yaml_console_and_plot_slo_unchanged(tmp_path, monkeypatch, capsys):
    fake_root = tmp_path / 'repo'
    fake_root.mkdir()
    for name in ('config.yaml', 'experiments', 'apps', 'autoscaler'):
        (fake_root / name).symlink_to(campaign.ROOT / name)
    (fake_root / 'results').mkdir()

    commands = []
    monkeypatch.setattr(campaign, 'ROOT', fake_root)
    monkeypatch.setattr(campaign, 'kube', lambda *a, **kw: SimpleNamespace(stdout='services\n'))
    monkeypatch.setattr(campaign, 'reset', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'check_node_capacity', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'prerequisites', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'check_result_name', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, '_prepare_floor_adapter', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'command', lambda args, **kw: commands.append(args))
    monkeypatch.setattr(campaign.fcntl, 'flock', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'run_method', lambda *a, **kw: None)

    campaign.main([
        'camp_095', '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--latency-floor-ms', '130', '--rt-target-factor', '0.95',
    ])

    camp_yaml = yaml.safe_load((fake_root / 'results' / 'camp_095' / 'campaign.yaml').read_text())
    assert camp_yaml['slo_ms'] == 150
    assert camp_yaml['reporting_slo_ms'] == 150
    assert camp_yaml['rt_target_factor'] == 0.95
    assert camp_yaml['internal_rt_target_ms'] == 142.5
    assert camp_yaml['latency_floor_ms'] == 130
    assert camp_yaml['effective_rt_metric_target_ms'] == 12.5
    assert camp_yaml['latency_scaling_formula'] == 'max(RT-floor, 0)/(internal_target-floor)'
    assert any('utils.plot_autoscaler' in map(str, c) and '--slo-ms' in c
               and float(c[c.index('--slo-ms') + 1]) == 150.0
               for c in commands)

    out = capsys.readouterr().out
    assert 'internal scaling target 142.5 ms p90 = 150 ms * 0.95; reporting SLO 150 ms' in out
    assert 'excess target 12.5ms (metric ue_p90_excess_latency_130ms_milliseconds, target 12.5). Reporting SLO remains 150ms.' in out


def test_fractional_slo_with_default_factor_installs_millisecond_adapter(tmp_path, monkeypatch):
    fake_root = tmp_path / 'repo'
    fake_root.mkdir()
    for name in ('config.yaml', 'experiments', 'apps', 'autoscaler'):
        (fake_root / name).symlink_to(campaign.ROOT / name)
    (fake_root / 'results').mkdir()

    adapter_calls = []
    monkeypatch.setattr(campaign, 'ROOT', fake_root)
    monkeypatch.setattr(campaign, 'kube', lambda *a, **kw: SimpleNamespace(stdout='services\n'))
    monkeypatch.setattr(campaign, 'reset', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'check_node_capacity', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'prerequisites', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'check_result_name', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, '_prepare_floor_adapter',
                        lambda root, floor_ms, unit='seconds': adapter_calls.append((root, floor_ms, unit)))
    monkeypatch.setattr(campaign, 'command', lambda *a, **kw: None)
    monkeypatch.setattr(campaign.fcntl, 'flock', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'run_method', lambda *a, **kw: None)

    campaign.main([
        'camp_150_5', '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150.5', '--latency-floor-ms', '0',
    ])

    assert adapter_calls == [(fake_root / 'results' / 'camp_150_5', 0, 'milliseconds')]
    camp_yaml = yaml.safe_load((fake_root / 'results' / 'camp_150_5' / 'campaign.yaml').read_text())
    assert camp_yaml['rt_target_factor'] == 1.0
    assert camp_yaml['rt_metric_name'] == 'ue_p90_latency_milliseconds'
    assert camp_yaml['rt_metric_unit'] == 'milliseconds'
    assert camp_yaml['rt_hpa_target_value'] == '150.5'


def test_run_method_forwards_reporting_slo_to_summarize(tmp_path, monkeypatch):
    import io
    import json
    import subprocess
    summarize_cmds = []

    class FakeProc:
        def __init__(self, is_ue=False):
            self.returncode = 0
            self.stdin = io.StringIO()
            self._is_ue = is_ue
            self._polls = 0
        def poll(self):
            if not self._is_ue:
                return None
            self._polls += 1
            return None if self._polls == 1 else 0
        def wait(self, timeout=None):
            return 0

    def fake_kube(*args, **kwargs):
        if args[:2] == ('get', '--raw') and 'ue_p90_excess_latency_130ms_milliseconds' in args[2]:
            return subprocess.CompletedProcess(
                args, 0,
                stdout=json.dumps({'items': [{'metricName': 'ue_p90_excess_latency_130ms_milliseconds', 'value': '10'}]}),
                stderr='',
            )
        return subprocess.CompletedProcess(args, 0, stdout='deployment.apps/mec-placement-face-detection\n', stderr='')

    def fake_get(kind, name, namespace=campaign.NAMESPACE):
        if kind == 'service':
            return {'spec': {'ports': [{'nodePort': 30080}]}}
        raise AssertionError(f'unexpected get {kind} {name} {namespace}')

    class DummyProgress:
        def __init__(self, *a): pass
        def poll(self, final=False): pass

    monkeypatch.setattr(campaign.subprocess, 'Popen', lambda *a, **kw: FakeProc(is_ue=('campaign_ue' in str(a))))
    monkeypatch.setattr(campaign, 'kube', fake_kube)
    monkeypatch.setattr(campaign, 'get', fake_get)
    monkeypatch.setattr(campaign, 'command', lambda args, **kw: summarize_cmds.append(args))
    monkeypatch.setattr(campaign, 'active_telemetry', lambda url: True)
    monkeypatch.setattr(campaign, 'stop_process', lambda p: None)
    monkeypatch.setattr(campaign, 'check_service_routing', lambda s: None)
    monkeypatch.setattr(campaign, 'transfer', lambda *a: None)
    monkeypatch.setattr(campaign, 'RunnerProgress', DummyProgress)
    monkeypatch.setattr(campaign, 'time', type('T', (), {'monotonic': lambda: 0.0, 'sleep': lambda _: None}))

    settings = yaml.safe_load((campaign.ROOT / 'experiments/campaign.yaml').read_text())
    settings.update({
        'schedule': '3:1:1:1',
        'phase_duration': 60,
        'phase_durations': [0.0],
        'staircase': True,
        'latency_floor_ms': 130,
        'rt_target_factor': 0.95,
        'rt_metric_unit': 'milliseconds',
    })

    run_root = tmp_path / 'run'
    run_root.mkdir()
    (run_root / 'workload.yaml').write_text(campaign.workload_manifest('face-detection').read_text())
    campaign.run_method(
        settings, 'camp', run_root, 'proposed', delay=None, slo_ms=150, max_replicas=24,
        comparison='scaling', autoscaler='hpa', placement_engine='go',
    )
    prop_hpa = yaml.safe_load((run_root / 'proposed-hpa.yaml').read_text())
    assert prop_hpa['spec']['metrics'][0]['external']['metric']['name'] == 'ue_p90_excess_latency_130ms_milliseconds'
    assert prop_hpa['spec']['metrics'][0]['external']['target']['value'] == '12.5'
    assert any('utils.summarize' in map(str, c) and '--slo-ms' in c
               and float(c[c.index('--slo-ms') + 1]) == 150.0
               for c in summarize_cmds)
