import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import types

import pytest
import yaml

from experiments import campaign
from benchmark.client import parse_schedule


def test_workload_service_accepts_cluster_wide_routing():
    documents = yaml.safe_load_all((campaign.ROOT / 'apps/face-detection/manifest.yaml').read_text())
    service = next(document for document in documents if document['kind'] == 'Service')
    campaign.check_service_routing(service)


@pytest.mark.parametrize('field,value', [
    ('trafficDistribution', 'PreferSameNode'),
    ('trafficDistribution', 'PreferSameZone'),
    ('externalTrafficPolicy', 'Local'),
    ('internalTrafficPolicy', 'Local'),
    ('sessionAffinity', 'ClientIP'),
])
def test_campaign_rejects_restricted_or_sticky_routing(field, value):
    with pytest.raises(RuntimeError, match='cluster-wide routing'):
        campaign.check_service_routing({'spec': {field: value}})


@pytest.mark.parametrize('key', ['service.kubernetes.io/topology-mode',
                                 'service.kubernetes.io/topology-aware-hints'])
def test_campaign_rejects_legacy_topology_hints(key):
    with pytest.raises(RuntimeError, match='cluster-wide routing'):
        campaign.check_service_routing({'spec': {}, 'metadata': {'annotations': {key: 'Auto'}}})


def archive_bytes(method='baseline', unsafe=False):
    data = b'{"event":"request","success":true}\n'
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode='w:gz') as archive:
        directory = tarfile.TarInfo(method)
        directory.type = tarfile.DIRTYPE
        archive.addfile(directory)
        member = tarfile.TarInfo('../outside' if unsafe else method + '/requests.jsonl')
        member.size = len(data)
        archive.addfile(member, io.BytesIO(data))
        log = tarfile.TarInfo(method + '-runner.log')
        log.size = 3
        archive.addfile(log, io.BytesIO(b'ok\n'))
    return stream.getvalue(), data


def transfer_command(monkeypatch, payload, checksum):
    def command(args, **kwargs):
        if 'tar' in args[-1]:
            kwargs['stdout'].write(payload)
            return subprocess.CompletedProcess(args, 0)
        return subprocess.CompletedProcess(args, 0, stdout=checksum + '  requests.jsonl\n')
    monkeypatch.setattr(campaign, 'command', command)


def settings():
    return {'ue_host': 'ue', 'ue_project': '/project'}


def test_transfer_preserves_sampler_and_verifies_real_archive(tmp_path, monkeypatch):
    output = tmp_path / 'baseline'
    output.mkdir()
    (output / 'metrics.jsonl').write_text('sample\n')
    payload, raw = archive_bytes()
    transfer_command(monkeypatch, payload, hashlib.sha256(raw).hexdigest())
    campaign.transfer(settings(), 'run_2', 'baseline', output)
    assert (output / 'requests.jsonl').read_bytes() == raw
    assert (output / 'metrics.jsonl').read_text() == 'sample\n'
    assert (tmp_path / 'baseline-runner.log').read_text() == 'ok\n'


def test_transfer_rejects_traversal_before_extracting(tmp_path, monkeypatch):
    output = tmp_path / 'baseline'
    output.mkdir()
    payload, _ = archive_bytes(unsafe=True)
    transfer_command(monkeypatch, payload, 'unused')
    with pytest.raises(ValueError, match='unsafe'):
        campaign.transfer(settings(), 'run_2', 'baseline', output)
    assert not (tmp_path / 'outside').exists()
    assert not (output / 'requests.jsonl').exists()


def test_transfer_refuses_existing_raw_results(tmp_path, monkeypatch):
    output = tmp_path / 'baseline'
    output.mkdir()
    (output / 'requests.jsonl').write_text('original')
    payload, _ = archive_bytes()
    transfer_command(monkeypatch, payload, 'unused')
    with pytest.raises(FileExistsError):
        campaign.transfer(settings(), 'run_2', 'baseline', output)
    assert (output / 'requests.jsonl').read_text() == 'original'


def test_transfer_detects_corruption(tmp_path, monkeypatch):
    output = tmp_path / 'baseline'
    output.mkdir()
    payload, _ = archive_bytes()
    transfer_command(monkeypatch, payload, 'wrong')
    with pytest.raises(RuntimeError, match='checksum'):
        campaign.transfer(settings(), 'run_2', 'baseline', output)


def test_campaign_name_cannot_escape_results(monkeypatch):
    monkeypatch.setattr(campaign, 'load', lambda _: {})
    with pytest.raises(SystemExit) as error:
        campaign.main(['../other'])
    assert error.value.code == 2


@pytest.mark.parametrize('code', [0, 17, 255])
def test_result_name_check_distinguishes_collision_from_ssh_failure(tmp_path, monkeypatch, code):
    monkeypatch.setattr(campaign, 'ROOT', tmp_path)
    monkeypatch.setattr(campaign.subprocess, 'run', lambda *a, **kw:
                        subprocess.CompletedProcess(a[0], code, stdout='', stderr='SSH diagnostic'))
    config = {'ue_host': 'ue', 'ue_project': '/project'}
    if code == 17:
        with pytest.raises(FileExistsError, match='UE host ue: /project/results/reused'):
            campaign.check_result_name(config, 'reused')
    elif code:
        with pytest.raises(RuntimeError, match='SSH diagnostic'):
            campaign.check_result_name(config, 'reused')
    else:
        campaign.check_result_name(config, 'reused')
    assert not (tmp_path / 'results').exists()


def test_duplicate_name_stops_before_preflight(monkeypatch):
    def duplicate(*args):
        raise FileExistsError('already exists on UE host')
    monkeypatch.setattr(campaign, 'check_result_name', duplicate)
    monkeypatch.setattr(campaign, 'prerequisites', lambda _: pytest.fail('active probe ran'))
    with pytest.raises(FileExistsError, match='UE host'):
        campaign.main(['reused'])


def test_progress_relays_phase_lines_without_duplicates_or_partial_lines(tmp_path, capsys):
    path = tmp_path / 'runner.log'
    path.write_text('[RUN] phase_02: 2/19 ramp-up; 9 UEs\n[--] phase_02: 10')
    progress = campaign.RunnerProgress(path, 'proposed')
    progress.poll()
    first = capsys.readouterr().out
    assert 'proposed: [RUN] phase_02: 2/19 ramp-up; 9 UEs' in first
    assert '[--]' not in first
    progress.poll()
    assert capsys.readouterr().out == ''
    with path.open('a') as stream:
        stream.write(' requests\n[RUN] phase_03: ramp-down')
    progress.poll()
    assert capsys.readouterr().out == 'proposed: [--] phase_02: 10 requests\n'
    progress.poll(final=True)
    assert capsys.readouterr().out == 'proposed: [RUN] phase_03: ramp-down\n'


def test_default_campaign_has_matched_ascending_and_descending_phases():
    settings = yaml.safe_load((campaign.ROOT / 'experiments/campaign.yaml').read_text())
    phases = parse_schedule(settings['schedule'])
    counts = [tuple(p[f'gnb{i}_ues'] for i in (1, 2, 3)) for p in phases]
    assert len(phases) == 13
    assert counts[:6] == list(reversed(counts[7:]))
    assert settings['phase_duration'] == 60


@pytest.mark.parametrize('cap', [1, 3, 8, 100])
def test_replica_cap_is_identical_in_saved_baseline_hpa_and_proposed_config(tmp_path, cap):
    baseline, _, hpa_path = campaign._method_configs(tmp_path, 'baseline', 500, cap)
    proposed, proposed_path, no_hpa = campaign._method_configs(tmp_path, 'proposed', 500, cap)
    hpa = yaml.safe_load(hpa_path.read_text())
    saved = yaml.safe_load(proposed_path.read_text())
    assert baseline['max_replicas'] == proposed['max_replicas'] == saved['max_replicas'] == hpa['spec']['maxReplicas'] == cap
    assert baseline['min_replicas'] == proposed['min_replicas'] == hpa['spec']['minReplicas']
    assert no_hpa is None
    assert hpa['spec']['metrics'][0]['resource']['target'] == {
        'type': 'Utilization', 'averageUtilization': 60}


@pytest.mark.parametrize('cap', [0, -1, 1.5, True])
def test_invalid_cap_does_not_write_method_configs(tmp_path, cap):
    with pytest.raises(ValueError, match='max replicas'):
        campaign._method_configs(tmp_path, 'baseline', 500, cap)
    assert list(tmp_path.iterdir()) == []


def test_debug_configs_shorten_policy_timers_and_preserve_measurement_target(tmp_path):
    baseline, _, path = campaign._method_configs(tmp_path, 'baseline', 300, 30, debug=True)
    proposed, config_path, _ = campaign._method_configs(tmp_path, 'proposed', 300, 30, debug=True)
    saved = yaml.safe_load(config_path.read_text())
    hpa = yaml.safe_load(path.read_text())
    for config in (baseline, proposed, saved):
        assert 'scale_up_cooldown_seconds' not in config
        assert config['interval_seconds'] == 15
        assert config['cooldown_seconds'] == 15
        assert config['scale_down_seconds'] == 30
        assert config['deschedule_interval_seconds'] == 30
        assert '[1m]' in config['cpu_utilization_query']
        assert config['slo_seconds'] == .3
        assert config['max_replicas'] == 30
        assert config['mobility_query'].endswith('/ 15')
    assert hpa['spec']['behavior']['scaleUp']['stabilizationWindowSeconds'] == 0
    assert hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 30
    assert hpa['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 60


def test_normal_hpa_and_proposed_share_15s_interval_and_300s_stabilization(tmp_path):
    config, _, path = campaign._method_configs(tmp_path, 'baseline', 500, 30, debug=False)
    hpa = yaml.safe_load(path.read_text())
    assert 'scale_up_cooldown_seconds' not in config
    assert hpa['spec']['behavior']['scaleUp']['stabilizationWindowSeconds'] == 0
    assert hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == config['scale_down_seconds'] == 300
    proposed, proposed_path, _ = campaign._method_configs(tmp_path, 'proposed', 500, 30)
    from utils.config import load_settings
    effective = load_settings(proposed_path)
    assert proposed['interval_seconds'] == effective.interval_seconds == config['interval_seconds'] == 15
    assert effective.scale_down_seconds == 300


def test_debug_cli_uses_60_second_phases(monkeypatch):
    observed = []
    monkeypatch.setattr(campaign, 'prerequisites', lambda settings: observed.append(settings.copy()))
    campaign.main(['--check', '--debug'])
    assert observed[0]['phase_duration'] == 60


def test_custom_weights_are_saved_without_changing_source_config(tmp_path):
    before = (campaign.ROOT / 'config.yaml').read_bytes()
    weights = {'cpu': 1., 'memory': 0., 'gpu': 0., 'vram': 0.}
    config, path, _ = campaign._method_configs(tmp_path, 'proposed', 500, 30,
                                              resource_weights=weights, locality_weight=0.)
    saved = yaml.safe_load(path.read_text())
    assert saved['resource_weights'] == config['resource_weights'] == weights
    assert saved['locality_weight'] == 0
    assert saved['interval_seconds'] == 15
    assert (campaign.ROOT / 'config.yaml').read_bytes() == before


@pytest.mark.parametrize('options', [
    ['--resource-weights', '0', '0', '0', '0'],
    ['--resource-weights', '1', '1', '0', '0'],
    ['--resource-weights', 'nan', '0', '0', '0'],
    ['--resource-weights', '-1', '2', '0', '0'],
    ['--locality-weight', '1.1'], ['--locality-weight', 'nan'],
])
def test_invalid_weights_fail_before_preflight(monkeypatch, options):
    monkeypatch.setattr(campaign, 'prerequisites', lambda _: pytest.fail('preflight ran'))
    with pytest.raises(SystemExit) as error:
        campaign.main(['debug_test', *options])
    assert error.value.code == 2


@pytest.mark.parametrize('duration', ['0', '59', 'nan', 'inf'])
def test_cli_rejects_short_or_invalid_phases_before_preflight(monkeypatch, duration):
    monkeypatch.setattr(campaign, 'prerequisites', lambda _: pytest.fail('preflight ran'))
    with pytest.raises(SystemExit) as error:
        campaign.main(['debug_test', '--debug', '--phase-duration', duration])
    assert error.value.code == 2


@pytest.mark.parametrize('cap', ['0', '-1', '1.5'])
def test_cli_rejects_invalid_caps_before_preflight(monkeypatch, cap):
    monkeypatch.setattr(campaign, 'prerequisites', lambda _: pytest.fail('preflight ran'))
    with pytest.raises(SystemExit) as error:
        campaign.main(['run_test', '--max-replicas', cap])
    assert error.value.code == 2


@pytest.mark.parametrize('use_tc', [False, True])
def test_campaign_manifest_reserves_pool_only_with_tc(tmp_path, use_tc):
    path = campaign._campaign_manifest(tmp_path, use_tc)
    deployment = next(item for item in yaml.safe_load_all(path.read_text())
                      if item and item.get('kind') == 'Deployment')
    annotations = deployment['spec']['template']['metadata'].get('annotations', {})
    key = 'cni.projectcalico.org/ipv4pools'
    if use_tc:
        assert json.loads(annotations[key]) == [campaign.POOL_NAME]
    else:
        assert key not in annotations


def test_separated_comparison_a_placement_vs_comparison_b_full(tmp_path):
    placement_dir = tmp_path / 'placement'
    placement_dir.mkdir()
    base_p, _, no_hpa = campaign._method_configs(
        placement_dir, 'baseline', 500, 10, comparison='placement')
    prop_p, _, prop_hpa = campaign._method_configs(
        placement_dir, 'proposed', 500, 10, comparison='placement')
    assert no_hpa is None
    assert prop_hpa is None
    assert base_p['mode'] == 'scaling-only' and base_p['mutate'] is True
    assert prop_p['mode'] == 'mobility-aware' and prop_p['mutate'] is True
    base_dep = next(d for d in yaml.safe_load_all(Path(base_p['manifest']).read_text()) if d['kind'] == 'Deployment')
    prop_dep = next(d for d in yaml.safe_load_all(Path(prop_p['manifest']).read_text()) if d['kind'] == 'Deployment')
    assert base_dep['spec']['template']['spec']['schedulerName'] == 'default-scheduler'
    assert prop_dep['spec']['template']['spec']['schedulerName'] == 'mec-placement-scheduler'

    full_dir = tmp_path / 'full'
    full_dir.mkdir()
    base_f, _, hpa_f = campaign._method_configs(
        full_dir, 'baseline', 500, 10, comparison='full')
    assert hpa_f is not None
    assert base_f['mutate'] is False

    hpa_dir = tmp_path / 'hpa_placement'
    hpa_dir.mkdir()
    _, _, base_lat_hpa = campaign._method_configs(
        hpa_dir, 'baseline', 450, 12, comparison='placement', autoscaler='hpa')
    _, _, prop_lat_hpa = campaign._method_configs(
        hpa_dir, 'proposed', 450, 12, comparison='placement', autoscaler='hpa')
    assert base_lat_hpa is not None and prop_lat_hpa is not None
    prop_hpa = yaml.safe_load(prop_lat_hpa.read_text())
    assert prop_hpa['spec']['metrics'][0]['external']['target']['value'] == '450m'
    assert prop_hpa['spec']['maxReplicas'] == 12
    assert prop_hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 300


def test_prometheus_adapter_external_rule_sets_namespaced_false():
    docs = list(yaml.safe_load_all((campaign.ROOT / 'telemetry/05-prometheus-adapter.yaml').read_text()))
    cm = next(d for d in docs if d and d.get('kind') == 'ConfigMap'
              and d.get('metadata', {}).get('name') == 'prometheus-adapter-config')
    adapter_cfg = yaml.safe_load(cm['data']['config.yaml'])
    assert adapter_cfg['externalRules'][0]['resources'] == {'namespaced': False}


def test_campaign_defaults_to_hpa_and_incluster_go_and_rejects_mixed_controller(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    out_dir = tmp_path / 'plan'
    campaign.main([
        '--config', str(campaign.ROOT / 'experiments' / 'campaign-pedestrian.example.yaml'),
        '--slo-ms', '500',
        '--plan-only', str(out_dir),
    ])
    plan = yaml.safe_load((out_dir / 'plan.yaml').read_text())
    assert plan['autoscaler'] == 'hpa'
    assert plan['placement_engine'] == 'go'

    monkeypatch.setattr(campaign, 'prerequisites', lambda _: pytest.fail('preflight ran'))
    with pytest.raises(SystemExit) as error:
        campaign.main([
            'run_test',
            '--config', str(campaign.ROOT / 'experiments' / 'campaign-pedestrian.example.yaml'),
            '--placement-engine', 'go',
            '--autoscaler', 'controller',
        ])
    assert error.value.code == 2


def test_verify_incluster_components_checks_adapter_and_go_deployment(monkeypatch):
    def fake_get(kind, name, namespace=campaign.NAMESPACE):
        if kind == 'deployment' and name == 'prometheus-adapter' and namespace == 'placement':
            return {'spec': {'replicas': 1}, 'status': {'readyReplicas': 1}}
        if kind == 'deployment' and name == 'placement-controller' and namespace == 'placement':
            return {'spec': {'replicas': 1}, 'status': {'readyReplicas': 1}}
        raise AssertionError(f'unexpected get {kind} {name} {namespace}')

    apiservice_payload = {
        'spec': {'service': {'namespace': 'placement', 'name': 'prometheus-adapter'}},
        'status': {'conditions': [{'type': 'Available', 'status': 'True'}]},
    }
    queried_paths = []

    def fake_kube(*args, **kwargs):
        if args[:2] == ('get', 'apiservice'):
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(apiservice_payload), stderr='')
        if args[:2] == ('get', '--raw'):
            queried_paths.append(args[2])
            if args[2] == '/apis/external.metrics.k8s.io/v1beta1':
                return subprocess.CompletedProcess(args, 0, stdout=json.dumps({'kind': 'APIResourceList'}), stderr='')
            if 'ue_p90_latency_seconds' in args[2]:
                raise subprocess.CalledProcessError(1, ['kubectl', *args], stderr='Error from server (NotFound)')
        raise AssertionError(f'unexpected kube {args}')

    monkeypatch.setattr(campaign, 'get', fake_get)
    monkeypatch.setattr(campaign, 'kube', fake_kube)
    campaign.verify_incluster_components('hpa', 'go')
    assert any('ue_p90_latency_seconds' in p for p in queried_paths)

    # KEDA owning v1beta1.external.metrics.k8s.io must be rejected even with Available=True.
    apiservice_payload['spec']['service'] = {'namespace': 'keda', 'name': 'keda-metrics-apiserver'}
    with pytest.raises(RuntimeError, match='placement/prometheus-adapter'):
        campaign.verify_incluster_components('hpa', 'go')
    apiservice_payload['spec']['service'] = {'namespace': 'placement', 'name': 'prometheus-adapter'}

    # Non-NotFound adapter query failure (e.g. 500 namespaced error) must fail preflight.
    def fake_kube_500(*args, **kwargs):
        if args[:2] == ('get', 'apiservice'):
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(apiservice_payload), stderr='')
        if args[:2] == ('get', '--raw') and args[2] == '/apis/external.metrics.k8s.io/v1beta1':
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps({'kind': 'APIResourceList'}), stderr='')
        if args[:2] == ('get', '--raw') and 'ue_p90_latency_seconds' in args[2]:
            raise subprocess.CalledProcessError(1, ['kubectl', *args], stderr='InternalError: unable to find a resource')
        raise AssertionError(f'unexpected kube {args}')

    monkeypatch.setattr(campaign, 'kube', fake_kube_500)
    with pytest.raises(subprocess.CalledProcessError):
        campaign.verify_incluster_components('hpa', 'go')

    monkeypatch.setattr(campaign, 'kube', fake_kube)
    monkeypatch.setattr(
        campaign, 'get',
        lambda kind, name, ns=campaign.NAMESPACE: {'spec': {'replicas': 1}, 'status': {'readyReplicas': 0}},
    )
    with pytest.raises(RuntimeError, match='prometheus-adapter|placement-controller'):
        campaign.verify_incluster_components('hpa', 'go')


def test_incluster_go_with_hpa_applies_manifests_without_host_controller_processes(monkeypatch, tmp_path):
    spawned = []
    kube_calls = []

    class FakeProc:
        def __init__(self, cmd):
            self.cmd = cmd
            self.pid = 1000 + len(spawned)
            self.returncode = None
            self._polls = 0
            self.stdin = io.StringIO()

        def poll(self):
            if 'experiments.campaign_ue' in ' '.join(self.cmd):
                self._polls += 1
                if self._polls >= 2:
                    self.returncode = 0
                    return 0
            return None

        def wait(self, timeout=None):
            return 0

    def fake_popen(cmd, **kwargs):
        proc = FakeProc(cmd)
        spawned.append(cmd)
        return proc

    def fake_kube(*args, **kwargs):
        kube_calls.append(args)
        if 'logs' in args and 'stdout' in kwargs:
            kwargs['stdout'].write('scheduler bound pod=test node=gnb-2\n')
        if args[:2] == ('get', '--raw') and 'ue_p90_latency_seconds' in args[2]:
            return subprocess.CompletedProcess(
                args, 0,
                stdout=json.dumps({'items': [{'metricName': 'ue_p90_latency_seconds', 'value': '100m'}]}),
                stderr='',
            )
        return subprocess.CompletedProcess(args, 0, stdout='deployment.apps/mec-placement-pedestrian-detection\n', stderr='')

    def fake_get(kind, name, namespace=campaign.NAMESPACE):
        if kind == 'deployment' and name == 'placement-controller' and namespace == 'placement':
            return {'spec': {'replicas': 1}, 'status': {'readyReplicas': 1}}
        if kind == 'service':
            return {'spec': {'ports': [{'nodePort': 30080}]}}
        raise AssertionError(f'unexpected get {kind} {name} {namespace}')

    monkeypatch.setattr(campaign.subprocess, 'Popen', fake_popen)
    monkeypatch.setattr(campaign, 'kube', fake_kube)
    monkeypatch.setattr(campaign, 'get', fake_get)
    monkeypatch.setattr(campaign, 'command', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'active_telemetry', lambda url: True)
    monkeypatch.setattr(campaign, 'stop_process', lambda p: None)
    monkeypatch.setattr(campaign, 'check_service_routing', lambda s: None)
    monkeypatch.setattr(campaign, 'transfer', lambda *a: None)
    monkeypatch.setattr(campaign, 'time', type('T', (), {'monotonic': lambda: 0.0, 'sleep': lambda _: None}))

    class DummyProgress:
        def __init__(self, *a): pass
        def poll(self, final=False): pass

    monkeypatch.setattr(campaign, 'RunnerProgress', DummyProgress)

    settings = {
        'workload': 'pedestrian-detection',
        'site_node_ips': {'gnb-1': '127.0.0.1', 'gnb-2': '127.0.0.2', 'gnb-3': '127.0.0.3'},
        'schedule': '3:1:1:1',
        'phase_duration': 60,
        'prometheus_url': 'http://prom',
        'ue_host': 'ue',
        'ue_project': '/tmp',
        'ue_ssh_port': 22,
        'image': 'img:latest',
        'ssh_key': '/dev/null',
        'ue_user': 'root',
        'ue_password': '',
        'inventory': '/dev/null',
        'nr_ue': 'nr-ue',
        'token': 'tok',
        'telemetry_url': 'http://tel',
    }
    run_root = tmp_path / 'run'
    run_root.mkdir()
    campaign.run_method(
        settings, 'camp', run_root, 'proposed', delay=None, slo_ms=500, max_replicas=10,
        comparison='full', autoscaler='hpa', placement_engine='go',
    )

    assert not any('controller' in ' '.join(map(str, c)) for c in spawned)
    cm_file = run_root / 'proposed' / 'placement-configmap.yaml'
    assert any(args[:2] == ('apply', '-f') and str(cm_file) in args for args in kube_calls)
    assert any(args[:4] == ('-n', 'placement', 'rollout', 'restart') for args in kube_calls)
    assert any(args[:4] == ('-n', 'placement', 'rollout', 'status') for args in kube_calls)
    assert any(args[:2] == ('apply', '-f') and str(run_root / 'proposed-workload.yaml') in map(str, args) for args in kube_calls)
    assert any(args[:2] == ('apply', '-f') and str(run_root / 'proposed-hpa.yaml') in map(str, args) for args in kube_calls)
    assert any(args[:2] == ('get', '--raw') and 'ue_p90_latency_seconds' in args[2] for args in kube_calls)
    cm = yaml.safe_load(cm_file.read_text())
    cfg = yaml.safe_load(cm['data']['config.yaml'])
    assert cfg['manifest'] == '/etc/placement/manifest.yaml'
    assert cfg['prometheus_url'] == 'http://placement-prometheus.placement.svc:9090'
    assert cfg['mutate'] is True
    assert 'scheduler bound pod=test' in (run_root / 'proposed' / 'controller.log').read_text()


def test_mobility9_plan_rotates_fixed_demand_with_matched_hpa(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    output = tmp_path / 'mobility-plan'
    campaign.main(['--scenario', 'mobility9', '--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['comparison'] == 'placement'
    assert plan['slo_ms'] == 100
    settings = plan['campaign_settings']
    assert settings['phase_duration'] == 60
    assert settings['schedule'] == '9:3:3:3,9:7:1:1,9:1:7:1,9:1:1:7,9:1:7:1,9:7:1:1,9:3:3:3'
    baseline = yaml.safe_load((output / 'baseline-hpa.yaml').read_text())
    proposed = yaml.safe_load((output / 'proposed-hpa.yaml').read_text())
    assert baseline['spec'] == proposed['spec']
    assert proposed['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 300
    assert proposed['spec']['metrics'][0]['external']['target']['value'] == '100m'
    cfg = yaml.safe_load((output / 'proposed-controller.yaml').read_text())
    assert cfg['deschedule_interval_seconds'] == 30
    original = yaml.safe_load((campaign.ROOT / 'config.yaml').read_text())
    assert cfg['cpu_utilization_query'] == original['cpu_utilization_query']
    cm = campaign._render_incluster_placement_configmap(cfg, cfg['manifest'])
    assert yaml.safe_load(cm['data']['config.yaml'])['deschedule_interval_seconds'] == 30


def test_mobility9_preserves_explicit_timer_and_slo_overrides(tmp_path):
    output = tmp_path / 'plan'
    campaign.main(['--scenario', 'mobility9', '--deschedule-interval', '45',
                   '--phase-duration', '90', '--slo-ms', '150', '--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['campaign_settings']['phase_duration'] == 90
    assert plan['slo_ms'] == 150
    cfg = yaml.safe_load((output / 'proposed-controller.yaml').read_text())
    assert cfg['deschedule_interval_seconds'] == 45


@pytest.mark.parametrize('flags', [
    ['--deschedule-interval', '0'], ['--deschedule-interval', 'nan'],
    ['--deschedule-interval', '-1'], ['--deschedule-interval', 'inf'],
    ['--scenario', 'mobility9', '--debug'],
    ['--scenario', 'mobility9', '--placement-engine', 'python'],
])
def test_mobility_rejects_invalid_timing_and_runtime_before_cluster_access(flags, monkeypatch):
    monkeypatch.setattr(campaign, 'prerequisites', lambda _: pytest.fail('cluster accessed'))
    with pytest.raises(SystemExit) as exc:
        campaign.main(['test_mobility', *flags])
    assert exc.value.code == 2


def test_fixed_mobility_plan_clamps_both_deployments_and_hpas_and_enables_tc(tmp_path):
    output = tmp_path / 'fixed-plan'
    campaign.main(['--scenario', 'mobility9', '--fixed-replicas', '6',
                   '--tc-ms', '10', '--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['fixed_replicas'] == 6
    for method, scheduler in [('baseline', 'default-scheduler'), ('proposed', 'mec-placement-scheduler')]:
        docs = list(yaml.safe_load_all((output / f'{method}-workload.yaml').read_text()))
        dep = next(d for d in docs if d and d['kind'] == 'Deployment')
        assert dep['spec']['replicas'] == 6
        assert dep['spec']['template']['spec']['schedulerName'] == scheduler
        assert dep['spec']['template']['metadata']['annotations']['cni.projectcalico.org/ipv4pools'] == '["mec-campaign-reserved"]'
        hpa = yaml.safe_load((output / f'{method}-hpa.yaml').read_text())
        assert hpa['spec']['minReplicas'] == hpa['spec']['maxReplicas'] == 6
        cfg = yaml.safe_load((output / f'{method}-controller.yaml').read_text())
        assert cfg['min_replicas'] == cfg['max_replicas'] == 6
    assert plan['campaign_settings']['phase_duration'] == 60


@pytest.mark.parametrize('flags', [
    ['--scenario', 'mobility9', '--fixed-replicas', '0'],
    ['--scenario', 'mobility9', '--fixed-replicas', '-2'],
    ['--fixed-replicas', '6'],
    ['--scenario', 'mobility9', '--fixed-replicas', '6', '--max-replicas', '10'],
])
def test_fixed_mobility_rejects_invalid_or_conflicting_options(flags, monkeypatch):
    monkeypatch.setattr(campaign, 'prerequisites', lambda _: pytest.fail('cluster accessed'))
    with pytest.raises(SystemExit) as exc:
        campaign.main(['fixed_test', *flags])
    assert exc.value.code == 2


def test_arrival_rate_settings_propagate_across_standard_and_mobility9_plans(tmp_path):
    std_out = tmp_path / 'std'
    campaign.main(['--plan-only', str(std_out)])
    std_plan = yaml.safe_load((std_out / 'plan.yaml').read_text())
    assert std_plan['campaign_settings']['rps_per_ue'] == 5.0
    assert std_plan['campaign_settings']['max_in_flight_per_ue'] == 8
    remote_std = campaign.remote_settings(std_plan['campaign_settings'])
    assert remote_std['rps_per_ue'] == 5.0
    assert remote_std['max_in_flight_per_ue'] == 8

    mob_out = tmp_path / 'mob'
    campaign.main(['--scenario', 'mobility9', '--fixed-replicas', '6',
                   '--rps-per-ue', '3.5', '--max-in-flight-per-ue', '4',
                   '--plan-only', str(mob_out)])
    mob_plan = yaml.safe_load((mob_out / 'plan.yaml').read_text())
    assert mob_plan['campaign_settings']['rps_per_ue'] == 3.5
    assert mob_plan['campaign_settings']['max_in_flight_per_ue'] == 4
    remote_mob = campaign.remote_settings(mob_plan['campaign_settings'])
    assert remote_mob['rps_per_ue'] == 3.5
    assert remote_mob['max_in_flight_per_ue'] == 4


@pytest.mark.parametrize('flags', [
    ['--rps-per-ue', '0'],
    ['--rps-per-ue', '-1'],
    ['--rps-per-ue', 'nan'],
    ['--max-in-flight-per-ue', '0'],
])
def test_invalid_arrival_rate_flags_rejected(flags, monkeypatch):
    monkeypatch.setattr(campaign, 'prerequisites', lambda _: pytest.fail('cluster accessed'))
    with pytest.raises(SystemExit) as exc:
        campaign.main(['rate_test', *flags])
    assert exc.value.code == 2


def test_scaler_balanced_plan_only_renders_workloads_hpas_and_metadata(tmp_path):
    out = tmp_path / 'scaler_plan'
    campaign.main(['--comparison', 'scaling', '--scenario', 'scaler-balanced', '--plan-only', str(out)])

    base_docs = [d for d in yaml.safe_load_all((out / 'baseline-workload.yaml').read_text()) if d]
    prop_docs = [d for d in yaml.safe_load_all((out / 'proposed-workload.yaml').read_text()) if d]
    base_dep = next(d for d in base_docs if d['kind'] == 'Deployment')
    prop_dep = next(d for d in prop_docs if d['kind'] == 'Deployment')
    expected_affinity = {
        'nodeAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchFields': [{
                        'key': 'metadata.name',
                        'operator': 'In',
                        'values': ['gnb-3'],
                    }]
                }]
            }
        }
    }
    for dep in (base_dep, prop_dep):
        assert dep['spec']['replicas'] == 1
        assert dep['spec']['template']['spec']['schedulerName'] == 'default-scheduler'
        assert dep['spec']['template']['spec']['affinity'] == expected_affinity

    base_hpa = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())
    prop_hpa = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    assert base_hpa['spec']['minReplicas'] == 1
    assert base_hpa['spec']['maxReplicas'] == 24
    assert base_hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 300
    assert base_hpa['spec']['metrics'][0]['resource']['name'] == 'cpu'
    assert base_hpa['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 60

    assert prop_hpa['spec']['minReplicas'] == 1
    assert prop_hpa['spec']['maxReplicas'] == 24
    assert prop_hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 300
    assert prop_hpa['spec']['metrics'][0]['external']['metric']['name'] == 'ue_p90_latency_seconds'
    assert prop_hpa['spec']['metrics'][0]['external']['target']['value'] == '100m'

    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    assert plan['comparison'] == 'scaling'
    assert plan['scenario'] == 'scaler-balanced'
    assert plan['schedule'] == '3:1:1:1,9:3:3:3,15:5:5:5,24:8:8:8,15:5:5:5,9:3:3:3,3:1:1:1'
    assert plan['phase_durations_s'] == [60.0] * 7
    assert plan['final_hold_seconds'] == 0
    assert plan['node_restriction'] == 'gnb-3'
    assert plan['replica_bounds'] == {'min': 1, 'max': 24}
    assert plan['pacing'] == {'rps_per_ue': 5.0, 'max_in_flight_per_ue': 8}
    assert plan['hpa_targets'] == {
        'baseline': {'type': 'cpu_utilization_percent', 'target': 60},
        'proposed': {'type': 'latency_p90_ms', 'target': 100},
    }
    assert plan['tc_ms'] is None


def test_scaler_balanced_final_hold_and_overrides(tmp_path):
    out = tmp_path / 'scaler_hold'
    campaign.main([
        '--comparison', 'scaling', '--scenario', 'scaler-balanced', '--final-hold',
        '--max-replicas', '12', '--tc-ms', '15', '--rps-per-ue', '4', '--max-in-flight-per-ue', '6',
        '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    assert plan['phase_durations_s'] == [60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 420.0]
    assert plan['final_hold_seconds'] == 360
    assert plan['replica_bounds'] == {'min': 1, 'max': 12}
    assert plan['tc_ms'] == 15.0
    assert plan['pacing'] == {'rps_per_ue': 4.0, 'max_in_flight_per_ue': 6}
    assert yaml.safe_load((out / 'baseline-hpa.yaml').read_text())['spec']['maxReplicas'] == 12
    assert yaml.safe_load((out / 'proposed-hpa.yaml').read_text())['spec']['maxReplicas'] == 12


def test_scaling_mode_skips_placement_controller_preflight_and_mutations(tmp_path, monkeypatch):
    kube_calls = []

    def fake_kube(*args, **kwargs):
        kube_calls.append(args)
        if args == ('get', 'apiservice', 'v1beta1.external.metrics.k8s.io', '-o', 'json'):
            return types.SimpleNamespace(stdout=json.dumps({
                'spec': {'service': {'namespace': 'placement', 'name': 'prometheus-adapter'}},
                'status': {'conditions': [{'type': 'Available', 'status': 'True'}]},
            }))
        if args == ('get', '--raw', '/apis/external.metrics.k8s.io/v1beta1'):
            return types.SimpleNamespace(stdout=json.dumps({'kind': 'APIResourceList'}))
        if len(args) >= 3 and args[:2] == ('get', '--raw') and args[2].startswith('/apis/external.metrics.k8s.io/v1beta1/namespaces/services/ue_p90_latency_seconds'):
            return types.SimpleNamespace(stdout=json.dumps({'items': []}))
        if args[0] == 'apply' and 'placement-configmap' not in str(args):
            return types.SimpleNamespace(stdout='')
        if args[:3] == ('-n', 'services', 'rollout') and args[3] == 'status':
            return types.SimpleNamespace(stdout='')
        if args[:4] == ('-n', 'services', 'get', 'deployments,pods,hpa'):
            return types.SimpleNamespace(stdout='{}')
        raise AssertionError(f'unexpected kube call: {args}')

    def fake_get(kind, name, namespace='services'):
        if namespace == 'placement' and name == 'placement-controller':
            raise AssertionError('placement-controller must not be queried in scaling mode')
        if namespace == 'placement' and name == 'prometheus-adapter':
            return {'spec': {'replicas': 1}, 'status': {'readyReplicas': 1}}
        if kind == 'service':
            return {'spec': {'ports': [{'nodePort': 30000}], 'externalTrafficPolicy': 'Local'}}
        raise AssertionError(f'unexpected get({kind}, {name}, {namespace})')

    monkeypatch.setattr(campaign, 'kube', fake_kube)
    monkeypatch.setattr(campaign, 'get', fake_get)
    campaign.verify_incluster_components('hpa', 'go', comparison='scaling')

    monkeypatch.setattr(campaign, 'command', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'check_service_routing', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'active_telemetry', lambda *a, **kw: True)
    monkeypatch.setattr(campaign, '_query_external_latency_metric', lambda allow_not_found=True: True)
    monkeypatch.setattr(campaign, 'RunnerProgress', lambda *a, **kw: types.SimpleNamespace(poll=lambda final=False: None))

    popen_cmds = []
    written_payloads = []

    class FakePopen:
        def __init__(self, cmd, **kwargs):
            popen_cmds.append(cmd)
            self.is_ue = 'campaign_ue' in ' '.join(map(str, cmd))
            self.ue_polls = 0
            self.returncode = 0
            self.stdin = types.SimpleNamespace(write=lambda data: written_payloads.append(json.loads(data)), close=lambda: None)

        def poll(self):
            if not self.is_ue:
                return None
            self.ue_polls += 1
            return None if self.ue_polls == 1 else 0

        def terminate(self):
            pass

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(campaign.subprocess, 'Popen', FakePopen)
    monkeypatch.setattr(campaign.time, 'sleep', lambda _: None)
    monkeypatch.setattr(campaign, 'stop_process', lambda p: None)
    monkeypatch.setattr(campaign, 'transfer', lambda *a, **kw: None)

    settings = {
        'inventory': 'inv.yaml', 'nr_ue': '/bin/nr-ue', 'token': 'tok', 'image': 'img',
        'telemetry_url': 'http://127.0.0.1/obs', 'site_node_ips': {'gnb-1': '10.0.0.1', 'gnb-2': '10.0.0.2', 'gnb-3': '10.0.0.3'},
        'schedule': campaign.SCALER_BALANCED_SCHEDULE, 'phase_duration': 60,
        'phase_durations': [60.0] * 6 + [420.0], 'workload': 'face-detection',
        'ue_host': 'ue', 'ue_project': '/ue',
    }
    kube_calls.clear()
    root = tmp_path / 'run'
    root.mkdir()
    campaign.run_method(
        settings, 'scaler_01', root, 'proposed', None, 100, 24,
        comparison='scaling', autoscaler='hpa', placement_engine='go',
    )
    assert not (root / 'proposed' / 'placement-configmap.yaml').exists()
    assert not any('placement-controller' in ' '.join(map(str, c)) for c in kube_calls)
    assert not any('-m controller' in ' '.join(map(str, cmd)) for cmd in popen_cmds)
    assert written_payloads[0]['phase_durations'] == [60.0] * 6 + [420.0]


def test_check_node_capacity_passes_and_fails_clearly(tmp_path, monkeypatch):
    manifest = tmp_path / 'workload.yaml'
    manifest.write_text(yaml.safe_dump({
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'metadata': {'name': 'mec-placement-face-detection', 'namespace': 'services'},
        'spec': {'template': {'spec': {'containers': [{
            'name': 'app',
            'resources': {'requests': {'cpu': '500m', 'memory': '256Mi'}},
        }]}}},
    }))

    def mock_node_and_pods(alloc_cpu, alloc_mem):
        def fake_kube(*args, **kwargs):
            if args[:3] == ('get', 'node', 'gnb-3'):
                return types.SimpleNamespace(stdout=json.dumps({
                    'status': {'allocatable': {'cpu': alloc_cpu, 'memory': alloc_mem}},
                }))
            if args[:3] == ('get', 'pods', '-A'):
                return types.SimpleNamespace(stdout=json.dumps({'items': [
                    {'metadata': {'namespace': 'kube-system'}, 'status': {'phase': 'Running'},
                     'spec': {'containers': [{'resources': {'requests': {'cpu': '2', 'memory': '2Gi'}}}]}},
                    {'metadata': {'namespace': 'services', 'labels': {'app': 'mec-placement-face-detection'}},
                     'status': {'phase': 'Running'},
                     'spec': {'containers': [{'resources': {'requests': {'cpu': '500m', 'memory': '256Mi'}}}]}},
                ]}))
            raise AssertionError(f'unexpected kube call: {args}')
        return fake_kube

    monkeypatch.setattr(campaign, 'kube', mock_node_and_pods('16', '32Gi'))
    campaign.check_node_capacity('gnb-3', manifest, 24, 'mec-placement-face-detection')

    monkeypatch.setattr(campaign, 'kube', mock_node_and_pods('8', '32Gi'))
    with pytest.raises(RuntimeError, match='node gnb-3 cannot fit 24 replicas'):
        campaign.check_node_capacity('gnb-3', manifest, 24, 'mec-placement-face-detection')


@pytest.mark.parametrize('flags', [
    ['--comparison', 'scaling', '--scenario', 'mobility9'],
    ['--comparison', 'full', '--scenario', 'scaler-balanced'],
    ['--comparison', 'placement', '--scenario', 'scaler-balanced'],
    ['--comparison', 'scaling', '--scenario', 'scaler-balanced', '--fixed-replicas', '6'],
    ['--comparison', 'scaling', '--scenario', 'scaler-balanced', '--debug'],
    ['--comparison', 'scaling', '--scenario', 'scaler-balanced', '--autoscaler', 'controller'],
    ['--comparison', 'scaling', '--scenario', 'scaler-balanced', '--placement-engine', 'python'],
    ['--comparison', 'scaling', '--scenario', 'scaler-balanced', '--deschedule-interval', '30'],
    ['--comparison', 'scaling', '--scenario', 'scaler-balanced', '--locality-weight', '0.4'],
    ['--scenario', 'mobility9', '--final-hold'],
    ['--final-hold'],
])
def test_scaler_balanced_rejects_incompatible_options_before_cluster_access(flags, monkeypatch):
    monkeypatch.setattr(campaign, 'prerequisites', lambda _: pytest.fail('cluster accessed'))
    with pytest.raises(SystemExit) as exc:
        campaign.main(['scaler_invalid', *flags])
    assert exc.value.code == 2
