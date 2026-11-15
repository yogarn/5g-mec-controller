import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

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
