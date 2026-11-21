from types import SimpleNamespace

import pytest
import yaml

from experiments import campaign


def test_a4_a5_defaults_and_plan_provenance(tmp_path):
    out = tmp_path / 'plan_defaults'
    campaign.main([
        '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--latency-floor-ms', '130', '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    base_hpa = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())
    prop_hpa = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())

    assert plan['hpa_scale_down'] == {
        'stabilization_window_seconds': 60,
        'pods': None,
        'period_seconds': None,
    }
    assert plan['method_order_requested'] == 'baseline-first'
    assert plan['method_order'] == ['baseline', 'proposed']
    assert plan['provenance']['core_git'] is not None
    assert plan['provenance']['workload_image']
    assert plan['provenance']['method_order'] == ['baseline', 'proposed']
    assert base_hpa['spec']['behavior']['scaleDown'] == prop_hpa['spec']['behavior']['scaleDown']


def test_a5_custom_scale_down_applied_identically_to_both_hpas(tmp_path):
    out = tmp_path / 'plan_sd'
    campaign.main([
        '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--latency-floor-ms', '130', '--min-replicas', '2',
        '--scale-down-stabilization-seconds', '30',
        '--scale-down-pods', '2',
        '--scale-down-period-seconds', '30',
        '--method-order', 'proposed-first',
        '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    base_hpa = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())
    prop_hpa = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())

    expected_sd = {
        'stabilizationWindowSeconds': 30,
        'policies': [{'type': 'Pods', 'value': 2, 'periodSeconds': 30}],
    }
    assert base_hpa['spec']['behavior']['scaleDown'] == expected_sd
    assert prop_hpa['spec']['behavior']['scaleDown'] == expected_sd
    assert plan['hpa_scale_down'] == {
        'stabilization_window_seconds': 30,
        'pods': 2,
        'period_seconds': 30,
    }
    assert plan['method_order'] == ['proposed', 'baseline']


def test_a5_method_order_random_and_live_execution_order(tmp_path, monkeypatch):
    fake_root = tmp_path / 'repo'
    fake_root.mkdir()
    for name in ('config.yaml', 'experiments', 'apps', 'autoscaler'):
        (fake_root / name).symlink_to(campaign.ROOT / name)
    (fake_root / 'results').mkdir()

    executed = []
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
    monkeypatch.setattr(campaign, 'run_method', lambda _s, _c, _r, method, *a, **kw: executed.append(method))
    monkeypatch.setattr(campaign, '_collect_live_provenance', lambda _s: {
        'core_git': {'revision': 'abc', 'dirty': False},
        'ue_git': {'revision': 'abc', 'dirty': False},
        'kubernetes_server_version': 'v1.35.0',
        'kube_proxy_mode': 'iptables',
    })
    monkeypatch.setattr(campaign.random, 'choice', lambda seq: False)

    campaign.main([
        'camp_rand', '--comparison', 'scaling', '--scenario', 'scaler-staircase',
        '--slo-ms', '150', '--latency-floor-ms', '130', '--min-replicas', '2',
        '--method-order', 'random',
    ])

    assert executed == ['proposed', 'baseline']
    camp_yaml = yaml.safe_load((fake_root / 'results' / 'camp_rand' / 'campaign.yaml').read_text())
    assert camp_yaml['method_order_requested'] == 'random'
    assert camp_yaml['method_order'] == ['proposed', 'baseline']
    assert camp_yaml['provenance']['kubernetes_server_version'] == 'v1.35.0'
    assert camp_yaml['provenance']['kube_proxy_mode'] == 'iptables'
    assert camp_yaml['provenance']['ue_git'] == {'revision': 'abc', 'dirty': False}
    assert camp_yaml['provenance']['method_order'] == ['proposed', 'baseline']


@pytest.mark.parametrize('flags', [
    ['--scale-down-stabilization-seconds', '-1'],
    ['--scale-down-pods', '0'],
    ['--scale-down-period-seconds', '0'],
])
def test_a5_invalid_scale_down_flags_rejected(tmp_path, flags):
    out = tmp_path / 'plan_bad_sd'
    with pytest.raises(SystemExit):
        campaign.main([
            '--comparison', 'scaling', '--scenario', 'scaler-staircase',
            '--slo-ms', '150', '--plan-only', str(out), *flags,
        ])
    assert not out.exists()


def test_collect_live_provenance_detects_k3s_kube_proxy_mode_from_metrics(monkeypatch):
    monkeypatch.setattr(campaign, 'command', lambda *a, **kw: SimpleNamespace(stdout='abc\n'))

    def fake_kube(*args, **kwargs):
        if args[:4] == ('version', '-o', 'json'):
            return SimpleNamespace(stdout='{"serverVersion": {"gitVersion": "v1.35.4+k3s1"}}')
        if args[:4] == ('-n', 'kube-system', 'get', 'configmap'):
            return SimpleNamespace(stdout='')
        if args[:3] == ('get', '--raw', '/metrics'):
            return SimpleNamespace(stdout='kubeproxy_iptables_ct_state_invalid_dropped_packets_total 0\n')
        raise AssertionError(f'unexpected kube call: {args}')

    monkeypatch.setattr(campaign, 'kube', fake_kube)
    prov = campaign._collect_live_provenance({'ue_host': 'ue', 'ue_project': '/tmp'})
    assert prov['kubernetes_server_version'] == 'v1.35.4+k3s1'
    assert prov['kube_proxy_mode'] == 'iptables'
