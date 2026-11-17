import io
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from experiments import campaign


def test_omitted_min_replicas_preserves_defaults(tmp_path):
    out = tmp_path / 'plan_default'
    campaign.main([
        '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--latency-floor-ms', '130', '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    base_hpa = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())
    prop_hpa = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    base_dep = next(d for d in yaml.safe_load_all((out / 'baseline-workload.yaml').read_text())
                    if d and d.get('kind') == 'Deployment')
    prop_dep = next(d for d in yaml.safe_load_all((out / 'proposed-workload.yaml').read_text())
                    if d and d.get('kind') == 'Deployment')

    assert plan['replica_bounds'] == {'min': 3, 'max': 24}
    assert plan['http_threads'] == 5
    assert base_hpa['spec']['minReplicas'] == 3
    assert prop_hpa['spec']['minReplicas'] == 3
    assert base_dep['spec']['replicas'] == 3
    assert prop_dep['spec']['replicas'] == 3
    for dep in (base_dep, prop_dep):
        env = {e['name']: e.get('value') for e in dep['spec']['template']['spec']['containers'][0]['env']}
        assert env['HTTP_THREADS'] == '5'
        assert env['ORT_THREADS'] == '1'


def test_min_replicas_2_applies_to_both_methods_and_preserves_scaling_semantics(tmp_path):
    out = tmp_path / 'plan_min2'
    campaign.main([
        '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--latency-floor-ms', '130',
        '--rt-target-factor', '1.0', '--min-replicas', '2',
        '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    base_hpa = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())
    prop_hpa = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    base_dep = next(d for d in yaml.safe_load_all((out / 'baseline-workload.yaml').read_text())
                    if d and d.get('kind') == 'Deployment')
    prop_dep = next(d for d in yaml.safe_load_all((out / 'proposed-workload.yaml').read_text())
                    if d and d.get('kind') == 'Deployment')
    base_cfg = yaml.safe_load((out / 'baseline-controller.yaml').read_text())
    prop_cfg = yaml.safe_load((out / 'proposed-controller.yaml').read_text())

    assert base_hpa['spec']['minReplicas'] == 2
    assert prop_hpa['spec']['minReplicas'] == 2
    assert base_hpa['spec']['maxReplicas'] == 24
    assert prop_hpa['spec']['maxReplicas'] == 24
    assert base_dep['spec']['replicas'] == 2
    assert prop_dep['spec']['replicas'] == 2
    assert base_cfg['min_replicas'] == 2
    assert prop_cfg['min_replicas'] == 2

    # Baseline CPU, proposed RT-only floor metric, factor, behavior, pacing, and timing preserved.
    assert base_hpa['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 60
    assert prop_hpa['spec']['metrics'][0]['external']['metric']['name'] == 'ue_p90_excess_latency_130ms_seconds'
    assert prop_hpa['spec']['metrics'][0]['external']['target']['value'] == '20m'
    assert base_hpa['spec']['behavior'] == prop_hpa['spec']['behavior']
    assert plan['replica_bounds'] == {'min': 2, 'max': 24}
    assert plan['rt_target_factor'] == 1.0
    assert plan['slo_ms'] == 150
    assert plan['pacing'] == {'rps_per_ue': 5.0, 'max_in_flight_per_ue': 8}
    assert plan['phase_durations_s'] == [120.0] * 13


def test_min_replicas_2_with_rt_target_factor_and_campaign_yaml(tmp_path, monkeypatch):
    fake_root = tmp_path / 'repo'
    fake_root.mkdir()
    for name in ('config.yaml', 'experiments', 'apps', 'autoscaler'):
        (fake_root / name).symlink_to(campaign.ROOT / name)
    (fake_root / 'results').mkdir()

    monkeypatch.setattr(campaign, 'ROOT', fake_root)
    monkeypatch.setattr(campaign, 'kube', lambda *a, **kw: SimpleNamespace(stdout='services\n'))
    monkeypatch.setattr(campaign, 'reset', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'check_node_capacity', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'prerequisites', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'check_result_name', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, '_prepare_floor_adapter', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'command', lambda *a, **kw: None)
    monkeypatch.setattr(campaign.fcntl, 'flock', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'TrafficDelay', lambda *a, **kw: SimpleNamespace(start=lambda: None, close=lambda: None))
    monkeypatch.setattr(campaign, 'run_method', lambda *a, **kw: None)

    campaign.main([
        'camp_min2', '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--latency-floor-ms', '130',
        '--rt-target-factor', '0.95', '--min-replicas', '2',
    ])

    camp_yaml = yaml.safe_load((fake_root / 'results' / 'camp_min2' / 'campaign.yaml').read_text())
    assert camp_yaml['min_replicas'] == 2
    assert camp_yaml['max_replicas'] == 24
    assert camp_yaml['replica_bounds'] == {'min': 2, 'max': 24}
    assert camp_yaml['rt_target_factor'] == 0.95
    assert camp_yaml['effective_rt_metric_target_ms'] == 12.5


def test_run_sh_forwards_min_replicas_without_changing_ramp_default(tmp_path):
    repo = tmp_path / 'repo'
    (repo / 'experiments').mkdir(parents=True)
    (repo / '.venv' / 'bin').mkdir(parents=True)
    shutil_target = repo / 'experiments' / 'run.sh'
    shutil_target.write_text((campaign.ROOT / 'experiments' / 'run.sh').read_text())
    py_stub = repo / '.venv' / 'bin' / 'python'
    py_stub.write_text('#!/usr/bin/env python3\nimport json, sys\nprint(json.dumps(sys.argv[1:]))\n')
    os.chmod(py_stub, 0o755)

    res = subprocess.run(
        ['bash', str(shutil_target), 'ramp', 'scaler_floor_min2_01', '--rt-target-factor', '1.0', '--min-replicas', '2'],
        capture_output=True, text=True, check=True,
    )
    argv = json.loads(res.stdout.strip())
    assert argv[:3] == ['-u', '-m', 'experiments.campaign']
    assert 'scaler_floor_min2_01' in argv
    assert '--rt-target-factor' in argv
    assert argv[argv.index('--rt-target-factor') + 1] == '1.0'
    assert '--min-replicas' in argv
    assert argv[argv.index('--min-replicas') + 1] == '2'

    res_default = subprocess.run(
        ['bash', str(shutil_target), 'ramp', 'scaler_default'],
        capture_output=True, text=True, check=True,
    )
    argv_default = json.loads(res_default.stdout.strip())
    assert '--min-replicas' not in argv_default


@pytest.mark.parametrize('flags', [
    ['--min-replicas', '0'],
    ['--min-replicas', '-1'],
    ['--min-replicas', '1.5'],
    ['--min-replicas', 'abc'],
    ['--min-replicas', '5', '--max-replicas', '4'],
])
def test_invalid_min_replicas_rejected_before_writing_plan(tmp_path, flags):
    out = tmp_path / 'plan_invalid'
    with pytest.raises(SystemExit):
        campaign.main([
            '--comparison', 'scaling', '--scenario', 'scaler-staircase',
            '--slo-ms', '150', '--plan-only', str(out), *flags,
        ])
    assert not out.exists()


def test_min_replicas_conflicts_with_fixed_replicas(tmp_path):
    out = tmp_path / 'plan_conflict'
    with pytest.raises(SystemExit):
        campaign.main([
            '--scenario', 'mobility9', '--fixed-replicas', '3', '--min-replicas', '2',
            '--plan-only', str(out),
        ])
    assert not out.exists()

    with pytest.raises(ValueError, match='min_replicas'):
        campaign._method_configs(
            tmp_path, 'proposed', 150, 10,
            comparison='placement', autoscaler='hpa',
            fixed_replicas=3, min_replicas=2,
        )


def test_fixed_replicas_behavior_intact(tmp_path):
    out = tmp_path / 'plan_fixed'
    campaign.main([
        '--scenario', 'mobility9', '--fixed-replicas', '3',
        '--slo-ms', '150', '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    prop_hpa = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    prop_dep = next(d for d in yaml.safe_load_all((out / 'proposed-workload.yaml').read_text())
                    if d and d.get('kind') == 'Deployment')

    assert plan['replica_bounds'] == {'min': 3, 'max': 3}
    assert prop_hpa['spec']['minReplicas'] == 3
    assert prop_hpa['spec']['maxReplicas'] == 3
    assert prop_dep['spec']['replicas'] == 3


def test_startup_waits_for_initial_replicas_and_aborts_on_timeout(tmp_path, monkeypatch):
    # Case 1: readyReplicas transitions 1 -> 2 before traffic starts.
    ready_sequence = iter([
        {'spec': {'replicas': 2}, 'status': {'readyReplicas': 1}},
        {'spec': {'replicas': 2}, 'status': {'readyReplicas': 2}},
    ])
    monkeypatch.setattr(campaign, 'kube', lambda *a, **kw: subprocess.CompletedProcess(a, 0, stdout='', stderr=''))
    monkeypatch.setattr(campaign, 'get', lambda *a, **kw: next(ready_sequence))
    monkeypatch.setattr(campaign.time, 'sleep', lambda _: None)
    campaign._wait_initial_replicas_ready('mec-placement-face-detection', 2, method='baseline', timeout=10)

    # Case 2: rollout status times out -> raises clear RuntimeError.
    def fail_rollout(*args, **kwargs):
        raise subprocess.CalledProcessError(1, args)

    monkeypatch.setattr(campaign, 'kube', fail_rollout)
    with pytest.raises(RuntimeError, match='did not reach 2 ready replicas within 10s'):
        campaign._wait_initial_replicas_ready('mec-placement-face-detection', 2, method='proposed', timeout=10)

    # Case 3: readyReplicas stays below 2 until timeout -> raises clear RuntimeError.
    now = {'t': 0.0}
    monkeypatch.setattr(campaign, 'kube', lambda *a, **kw: subprocess.CompletedProcess(a, 0, stdout='', stderr=''))
    monkeypatch.setattr(campaign, 'get', lambda *a, **kw: {'spec': {'replicas': 2}, 'status': {'readyReplicas': 1}})
    monkeypatch.setattr(campaign.time, 'monotonic', lambda: now.__setitem__('t', now['t'] + 6.0) or now['t'])
    with pytest.raises(RuntimeError, match='1/2 ready replicas after 10s'):
        campaign._wait_initial_replicas_ready('mec-placement-face-detection', 2, method='proposed', timeout=10)


def test_run_method_aborts_before_traffic_when_initial_replicas_not_ready(tmp_path, monkeypatch):
    monkeypatch.setattr(campaign, 'kube', lambda *a, **kw: subprocess.CompletedProcess(a, 0, stdout='', stderr=''))
    monkeypatch.setattr(campaign, 'get', lambda *a, **kw: {'spec': {'replicas': 2}, 'status': {'readyReplicas': 1}})
    now = {'t': 0.0}
    monkeypatch.setattr(campaign.time, 'monotonic', lambda: now.__setitem__('t', now['t'] + 100.0) or now['t'])
    monkeypatch.setattr(campaign.time, 'sleep', lambda _: None)

    ue_started = []
    monkeypatch.setattr(campaign.subprocess, 'Popen', lambda *a, **kw: ue_started.append(a))

    settings = yaml.safe_load((campaign.ROOT / 'experiments/campaign.yaml').read_text())
    settings.update({
        'schedule': '3:1:1:1',
        'phase_duration': 60,
        'phase_durations': [60.0],
        'staircase': True,
        'latency_floor_ms': 130,
        'rt_target_factor': 1.0,
    })
    run_root = tmp_path / 'run'
    run_root.mkdir()
    (run_root / 'workload.yaml').write_text(campaign.workload_manifest('face-detection').read_text())

    with pytest.raises(RuntimeError, match='1/2 ready replicas'):
        campaign.run_method(
            settings, 'camp', run_root, 'proposed', delay=None, slo_ms=150, max_replicas=24,
            comparison='scaling', autoscaler='hpa', placement_engine='go', min_replicas=2,
        )
    assert ue_started == []
