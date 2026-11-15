import json
import subprocess

import pytest
import yaml

from experiments import campaign


def test_pedestrian_configs_target_new_app_and_preserve_face_default(tmp_path):
    docs = list(yaml.safe_load_all(campaign._campaign_manifest(
        tmp_path, False, 'pedestrian-detection').read_text()))
    deployment, service = docs
    app = 'mec-placement-pedestrian-detection'
    assert deployment['metadata']['name'] == service['metadata']['name'] == app
    assert deployment['spec']['template']['metadata']['labels']['app'] == app
    assert service['spec']['selector']['app'] == app
    campaign.check_service_routing(service)
    config, _, hpa_path = campaign._method_configs(
        tmp_path, 'baseline', 100, 10, workload='pedestrian-detection')
    hpa = yaml.safe_load(hpa_path.read_text())
    assert hpa['spec']['scaleTargetRef']['name'] == app
    assert hpa['metadata']['name'] == app + '-baseline'
    assert config['slo_seconds'] == .1
    assert config['interval_seconds'] == 15
    assert campaign.workload_identity() == campaign.APP


def test_pedestrian_reset_never_targets_face_app(monkeypatch):
    calls = []
    def fake_kube(*args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, stdout=json.dumps({'items': []}))
    monkeypatch.setattr(campaign, 'kube', fake_kube)
    campaign.reset('pedestrian-detection')
    assert all('mec-placement-face-detection' not in ' '.join(args) for args in calls)
    assert 'hpa/mec-placement-pedestrian-detection-baseline' in calls[0]


def test_pedestrian_requires_separate_config_and_explicit_target_before_probes(monkeypatch):
    monkeypatch.setattr(campaign, 'prerequisites', lambda _: pytest.fail('live probe attempted'))
    with pytest.raises(SystemExit):
        campaign.main(['--check', '--workload', 'pedestrian-detection'])


def test_workload_rejects_arbitrary_manifest_paths():
    with pytest.raises(ValueError):
        campaign.workload_manifest('../../other')


def test_plan_only_does_not_probe_network_or_touch_campaign_lock(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('offline planning attempted an external operation')
    monkeypatch.setattr(campaign, 'kube', forbidden)
    monkeypatch.setattr(campaign, 'command', forbidden)
    monkeypatch.setattr(campaign, 'prerequisites', forbidden)
    monkeypatch.setattr(campaign.fcntl, 'flock', forbidden)
    output = tmp_path / 'offline'
    campaign.main(['--workload', 'pedestrian-detection', '--config',
                   str(campaign.ROOT / 'experiments/campaign-pedestrian.example.yaml'),
                   '--slo-ms', '100', '--max-replicas', '10', '--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['calibrated'] is False
    assert plan['workload'] == 'pedestrian-detection'
    hpa = yaml.safe_load((output / 'baseline-hpa.yaml').read_text())
    assert hpa['spec']['scaleTargetRef']['name'] == 'mec-placement-pedestrian-detection'


def test_resource_sampler_selects_pedestrian_pods_only(tmp_path, monkeypatch):
    from experiments import monitor_resources as monitor
    app = 'mec-placement-pedestrian-detection'
    calls = []
    def kube(namespace, *args):
        calls.append(args)
        if '--raw' in args:
            return {'items': [{'metadata': {'name': app + '-test'}},
                              {'metadata': {'name': 'mec-placement-face-detection-test'}}]}
        return {'items': [{'metadata': {'name': app + '-test'}, 'status': {'phase': 'Running'}}]}
    def stop(_):
        raise KeyboardInterrupt
    monkeypatch.setattr(monitor, 'kubectl', kube)
    monkeypatch.setattr(monitor.signal, 'signal', lambda *args: None)
    monkeypatch.setattr(monitor.time, 'sleep', stop)
    output = tmp_path / 'metrics.jsonl'
    with pytest.raises(KeyboardInterrupt):
        monitor.sample(output, app=app)
    assert 'app=' + app in calls[0]
    assert set(json.loads(output.read_text())['metrics']) == {app + '-test'}
