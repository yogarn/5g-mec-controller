import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import types
from urllib.error import HTTPError

import pytest
import yaml

from experiments import campaign
from benchmark.client import parse_schedule


@pytest.mark.parametrize(('status_codes', 'expected', 'elapsed'), [
    ([503, 200], 200, 0.5),
    ([401], 401, 0.0),
    ([503] * 41, 503, 20.0),
])
def test_envoy_preflight_retries_only_transient_unavailable(monkeypatch, status_codes, expected, elapsed):
    now = [0.0]
    replies = iter(status_codes)
    monkeypatch.setattr(campaign.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(campaign.time, 'sleep', lambda seconds: now.__setitem__(0, now[0] + seconds))

    def probe(request, timeout):
        assert request.get_header('X-mec-preflight') == 'core'
        code = next(replies)
        headers = {'X-MEC-Route': 'core'}
        body = io.BytesIO(b'readiness response')
        if code != 200:
            raise HTTPError(request.full_url, code, '', headers, body)
        body.status = code
        body.headers = headers
        return body

    monkeypatch.setattr(campaign, 'urlopen', probe)
    status, headers, _ = campaign._default_envoy_preflight_probe(
        'http://localhost/ready', headers={'X-MEC-Preflight': 'core'},
    )
    assert status == expected
    assert headers['x-mec-route'] == 'core'
    assert now[0] == elapsed


def test_workload_service_accepts_cluster_wide_routing():
    documents = yaml.safe_load_all((campaign.ROOT / 'apps/face-detection/manifest.yaml').read_text())
    service = next(document for document in documents if document['kind'] == 'Service')
    campaign.check_service_routing(service)


def test_fixed_vertical_mobility_plan_counts_all_nine_pods_without_hpa(tmp_path):
    output = tmp_path / 'mobility'
    campaign.main([
        '--config', 'experiments/campaign-vertical.yaml',
        '--scenario', 'mobility9', '--comparison', 'placement', '--vertical-mode', 'four-arm',
        '--site-anchors', '--fixed-replicas', '9', '--no-hpa',
        '--phase-schedule', '18:6:6:6,18:14:2:2,18:2:14:2,18:2:2:14',
        '--plan-only', str(output),
    ])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['hpa_managed_bounds_by_arm'] is None
    for arm, method, mode in campaign.FOUR_ARM_SPECS:
        spec = plan['arm_budgets'][arm]
        assert spec['initial_total_replicas'] == spec['min_replicas'] == spec['max_replicas'] == 9
        deployment = next(yaml.safe_load_all((output / arm / f'{method}-workload.yaml').read_text()))
        assert deployment['spec']['replicas'] == (6 if mode == 'local-only' else 3)
        assert not list((output / arm).glob('*-hpa.yaml'))


def test_joint_plan_limits_only_response_time_hpa_scale_down(tmp_path):
    output = tmp_path / 'joint'
    campaign.main([
        '--config', 'experiments/campaign-vertical.yaml',
        '--scenario', 'mobility9', '--comparison', 'full', '--vertical-mode', 'four-arm',
        '--site-anchors', '--latency-floor-ms', '125', '--asymmetric-latency',
        '--proposed-scale-down-period-seconds', '15', '--scale-down-stabilization-seconds', '60',
        '--plan-only', str(output),
    ])
    adapter = yaml.safe_load((output / 'floor-adapter-configmap.yaml').read_text())
    rules = yaml.safe_load(adapter['data']['config.yaml'])['externalRules']
    assert not any('site_scale_down' in r.get('name', {}).get('as', '') for r in rules)
    for arm, method, _ in campaign.FOUR_ARM_SPECS:
        spec = yaml.safe_load((output / arm / f'{method}-hpa.yaml').read_text())['spec']
        if method == 'proposed':
            assert spec['behavior']['scaleDown'] == {
                'stabilizationWindowSeconds': 60,
                'policies': [{'type': 'Pods', 'value': 1, 'periodSeconds': 15}],
            }
            assert len(spec['metrics']) == 1
            assert spec['metrics'][0]['external']['metric']['name'] == 'ue_p90_asymmetric_125ms_target_150ms_seconds'
            assert spec['metrics'][0]['external']['target'] == {'type': 'Value', 'value': '150m'}
        else:
            assert 'policies' not in spec['behavior']['scaleDown']
            assert len(spec['metrics']) == 1


@pytest.mark.parametrize('field,value', [
    ('trafficDistribution', 'PreferSameNode'),
    ('trafficDistribution', 'PreferSameZone'),
    ('externalTrafficPolicy', 'Local'),
    ('internalTrafficPolicy', 'Local'),
    ('sessionAffinity', 'ClientIP'),
])
def test_campaign_rejects_restricted_or_sticky_routing(field, value):
    with pytest.raises(RuntimeError, match='routing does not match'):
        campaign.check_service_routing({'spec': {field: value}})


@pytest.mark.parametrize('key', ['service.kubernetes.io/topology-mode',
                                 'service.kubernetes.io/topology-aware-hints'])
def test_campaign_rejects_legacy_topology_hints(key):
    with pytest.raises(RuntimeError, match='routing does not match'):
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
    assert not progress.traffic_started
    progress.poll()
    assert progress.traffic_started
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


def test_joint_campaign_has_symmetric_load_and_left_to_right_hotspot():
    settings = yaml.safe_load((campaign.ROOT / 'experiments/campaign.yaml').read_text())
    phases = parse_schedule(settings['schedule'])
    counts = [tuple(p[f'gnb{i}_ues'] for i in (1, 2, 3)) for p in phases]
    assert counts == [
        (1, 1, 1), (4, 1, 1), (6, 2, 1), (6, 4, 2), (6, 6, 3),
        (5, 8, 5), (4, 12, 8), (2, 8, 8), (1, 5, 9),
        (1, 3, 8), (1, 2, 6), (1, 1, 4), (1, 1, 1),
    ]
    totals = [p['workload_ue'] for p in phases]
    assert totals == [3, 6, 9, 12, 15, 18, 24, 18, 15, 12, 9, 6, 3]
    assert totals == list(reversed(totals))
    assert totals == [p['workload_ue'] for p in parse_schedule(campaign.SCALER_STAIRCASE_SCHEDULE)]
    assert all(sum(c) == n for c, n in zip(counts, totals))
    # The dominant site moves right, with ties at the transition phases.
    dominant_sites = [c.index(max(c)) for c in counts[1:-1]]
    assert dominant_sites == sorted(dominant_sites)
    assert set(dominant_sites) == {0, 1, 2}
    assert settings['phase_duration'] == 120


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
        'type': 'Utilization', 'averageUtilization': 80}


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
        assert config['mobility_query'] == campaign.UPF_PLACEMENT_QUERY
        assert config['latency_query'] == 'ue_request_attempt_duration_p90_seconds{job="ue-telemetry"}'
    assert hpa['spec']['behavior']['scaleUp']['stabilizationWindowSeconds'] == 0
    assert hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 30
    assert hpa['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 80


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


@pytest.mark.parametrize('method_order', ['baseline-first', 'proposed-first'])
def test_steep_plan_schedule_and_site_anchor_budget(tmp_path, method_order):
    steep = campaign.ROOT / 'experiments/campaign-steep.yaml'
    standard = campaign.load(campaign.ROOT / 'experiments/campaign.yaml')
    settings = campaign.load(steep)
    assert settings == standard | {'schedule': settings['schedule'], 'phase_duration': 120}
    phases = parse_schedule(settings['schedule'])
    assert [(p['workload_ue'], *(p[f'gnb{i}_ues'] for i in (1, 2, 3))) for p in phases] == [
        (3, 1, 1, 1), (8, 2, 3, 3), (12, 2, 8, 2), (18, 2, 14, 2), (24, 2, 20, 2),
        (24, 2, 2, 20), (18, 2, 2, 14), (12, 2, 2, 8), (8, 2, 3, 3), (3, 1, 1, 1),
    ]
    plan = tmp_path / 'plan'
    campaign.main([
        '--plan-only', str(plan), '--config', str(steep), '--comparison', 'full', '--site-anchors',
        '--prefer-close', '--min-replicas', '3', '--max-replicas', '16', '--cpu-hpa-target', '80',
        '--slo-ms', '150', '--latency-floor-ms', '125', '--tc-ms', '10', '--locality-weight', '0.4',
        '--method-order', method_order,
    ])
    metadata = yaml.safe_load((plan / 'plan.yaml').read_text())
    assert metadata['method_order'] == [method_order.split('-')[0],
                                        'proposed' if method_order == 'baseline-first' else 'baseline']
    assert metadata['phase_durations_s'] == [120] * 10
    assert metadata['pacing'] == {'rps_per_ue': 5.0, 'max_in_flight_per_ue': 8}
    assert metadata['prefer_close'] is True
    assert metadata['provenance']['site_anchors'] is True
    assert metadata['site_anchor_nodes'] == {'gnb-1': 'core-5g', 'gnb-2': 'gnb-2', 'gnb-3': 'gnb-3'}
    assert metadata['replica_bounds'] == {'min': 3, 'max': 16}
    assert metadata['initial_replicas'] == 3
    assert metadata['managed_replica_bounds'] == {'min': 1, 'max': 13}
    assert metadata['active_total_replica_bounds'] == {'min': 4, 'max': 16}
    anchors = list(yaml.safe_load_all((plan / 'site-anchors.yaml').read_text()))
    assert len(anchors) == 3
    for anchor, (site, node) in zip(anchors, metadata['site_anchor_nodes'].items()):
        spec = anchor['spec']
        assert spec['replicas'] == 1
        assert spec['template']['spec']['schedulerName'] == 'default-scheduler'
        assert spec['template']['spec']['nodeSelector']['kubernetes.io/hostname'] == node
        assert spec['selector']['matchLabels']['mec-replica'] == f'anchor-{site}'
        assert json.loads(spec['template']['metadata']['annotations']['cni.projectcalico.org/ipv4pools']) == [campaign.POOL_NAME]
    for method, scheduler in [('baseline', 'default-scheduler'), ('proposed', 'mec-placement-scheduler')]:
        workload, service = yaml.safe_load_all((plan / f'{method}-workload.yaml').read_text())
        assert workload['spec']['replicas'] == 0
        assert workload['spec']['selector']['matchLabels']['mec-replica'] == 'managed'
        assert workload['spec']['template']['spec']['schedulerName'] == scheduler
        assert service['spec']['trafficDistribution'] == 'PreferClose'
        assert service['spec']['selector'] == {'app': workload['metadata']['name']}
        hpa = yaml.safe_load((plan / f'{method}-hpa.yaml').read_text())
        assert hpa['spec']['scaleTargetRef']['name'] == workload['metadata']['name']
        assert (hpa['spec']['minReplicas'], hpa['spec']['maxReplicas']) == (1, 13)
        config = yaml.safe_load((plan / f'{method}-controller.yaml').read_text())
        assert config['prefer_close'] is True
        assert config['locality_weight'] == 0.4
    baseline = yaml.safe_load((plan / 'baseline-hpa.yaml').read_text())
    assert baseline['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 80
    proposed = yaml.safe_load((plan / 'proposed-hpa.yaml').read_text())
    assert proposed['spec']['metrics'][0]['external']['target']['value'] == '25m'


@pytest.mark.parametrize('method_order', ['baseline-first', 'proposed-first'])
@pytest.mark.parametrize(('spill_enabled', 'edge_only'), [(False, False), (True, False), (False, True)])
def test_steep_placement_spill_plan(tmp_path, method_order, spill_enabled, edge_only):
    plan = tmp_path / 'plan'
    spill_flags = ['--spill-high', '5', '--spill-low', '3'] if spill_enabled else []
    campaign.main([
        '--plan-only', str(plan), '--config', str(campaign.ROOT / 'experiments/campaign-steep.yaml'),
        '--comparison', 'placement', '--autoscaler', 'hpa', '--prefer-close', '--site-anchors',
        '--min-replicas', '3', '--max-replicas', '16', '--slo-ms', '150', '--latency-floor-ms', '125',
        '--method-order', method_order, *spill_flags, *(['--edge-only'] if edge_only else []),
    ])
    meta = yaml.safe_load((plan / 'plan.yaml').read_text())
    assert meta['method_order'][0] == method_order.split('-')[0]
    assert meta['comparison'] == 'placement'
    assert meta['phase_durations_s'] == [120] * 10
    assert meta['prefer_close'] is True
    assert meta['hpa_targets']['baseline'] == meta['hpa_targets']['proposed']
    assert meta['workload_image_digest'] == meta['provenance']['workload_image'].split('@')[1]
    assert meta['provenance']['workload_image_digest'] == meta['workload_image_digest']
    expected_nodes = ['gnb-2', 'gnb-3'] if edge_only else ['core-5g', 'gnb-2', 'gnb-3']
    for item in (meta, meta['provenance'], meta['campaign_settings']):
        assert item.get('edge_only', False) is edge_only
    if edge_only:
        assert meta['eligible_nodes'] == meta['provenance']['eligible_nodes'] == expected_nodes
    readiness = {'periodSeconds': 2, 'failureThreshold': 1, 'successThreshold': 2, 'timeoutSeconds': 1}
    spill_meta = {'spill_high': 5, 'spill_low': 3, 'spill_readiness_probe': readiness}
    for item in (meta, meta['provenance']):
        if spill_enabled:
            assert {key: item[key] for key in spill_meta} == spill_meta
        else:
            assert spill_meta.keys().isdisjoint(item)

    baseline, proposed = [yaml.safe_load((plan / f'{method}-hpa.yaml').read_text())['spec']
                          for method in ('baseline', 'proposed')]
    assert baseline == proposed
    assert (baseline['minReplicas'], baseline['maxReplicas']) == (1, 14 if edge_only else 13)
    metric = baseline['metrics'][0]['external']
    assert metric['metric']['name'] == 'ue_p90_excess_latency_125ms_seconds'
    assert metric['target']['value'] == '25m'

    original = next(yaml.safe_load_all(campaign.workload_manifest('pedestrian-detection').read_text()))
    original_container = original['spec']['template']['spec']['containers'][0]
    anchors = list(yaml.safe_load_all((plan / 'site-anchors.yaml').read_text()))
    assert {dep['spec']['template']['spec']['nodeSelector']['kubernetes.io/hostname']
            for dep in anchors} == set(expected_nodes)
    workloads = []
    for method in ('baseline', 'proposed'):
        deployment, service = yaml.safe_load_all((plan / f'{method}-workload.yaml').read_text())
        assert service['spec']['trafficDistribution'] == 'PreferClose'
        controller = yaml.safe_load((plan / f'{method}-controller.yaml').read_text())
        assert controller['eligible_nodes'] == expected_nodes
        assert controller['zero_locality_nodes'] == []
        if edge_only:
            assert controller['cpu_utilization_query'].endswith(
                'and on(node) node_cpu_seconds_total{job="node-exporter",mode="idle",node=~"gnb-2|gnb-3"}')
        workloads.append(deployment)
    for deployment in workloads + anchors:
        spec = deployment['spec']['template']['spec']
        terms = spec['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms']
        assert terms == [
            {'matchFields': [{'key': 'metadata.name', 'operator': 'In', 'values': [node]}]}
            for node in expected_nodes
        ]
        container = spec['containers'][0]
        assert container['image'].endswith('@' + meta['workload_image_digest'])
        assert container['livenessProbe'] == original_container['livenessProbe']
        if spill_enabled:
            assert container['readinessProbe'] == original_container['readinessProbe'] | readiness
            assert {env['name']: env['value'] for env in container['env'] if env['name'].startswith('SPILL_')} == {
                'SPILL_HIGH': '5', 'SPILL_LOW': '3',
            }
            assert container['startupProbe']['httpGet']['path'] == '/health'
        else:
            assert container == original_container


@pytest.mark.parametrize('routing_flags', [[], ['--prefer-close-routing-only']])
def test_radio_only_gnb1_routes_to_core_and_excludes_radio_from_placement(tmp_path, routing_flags):
    plan = tmp_path / 'plan'
    campaign.main([
        'radio_only_test', '--config', str(campaign.ROOT / 'experiments/campaign-mobility-core.yaml'),
        '--scenario', 'mobility9', '--fixed-replicas', '3', '--deschedule-interval', '5',
        '--plan-only', str(plan), *routing_flags,
    ])
    metadata = yaml.safe_load((plan / 'plan.yaml').read_text())
    assert metadata['radio_only_nodes'] == ['gnb1']
    assert metadata['application_ingress_nodes']['gnb-1'] == 'core-5g'
    assert metadata['application_ingress_ips']['gnb-1'] == '10.34.211.140'
    for method in ('baseline', 'proposed'):
        deployment, service = yaml.safe_load_all((plan / f'{method}-workload.yaml').read_text())
        pod_spec = deployment['spec']['template']['spec']
        terms = pod_spec['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms']
        assert {term['matchFields'][0]['values'][0] for term in terms} == {'core-5g', 'gnb-2', 'gnb-3'}
        assert deployment['spec']['replicas'] == 3
        assert service['spec'].get('trafficDistribution') == ('PreferClose' if routing_flags else None)
        controller = yaml.safe_load((plan / f'{method}-controller.yaml').read_text())
        assert set(controller['eligible_nodes']) == {'core-5g', 'gnb-2', 'gnb-3'}
        assert controller['zero_locality_nodes'] == []
        assert controller['mobility_query'] == campaign.UPF_PLACEMENT_QUERY
        assert controller['prefer_close'] is (method == 'proposed')


@pytest.mark.parametrize(('flags', 'error'), [
    (['--spill-high', '5'], 'must be supplied together'),
    (['--spill-low', '3'], 'must be supplied together'),
    (['--spill-high', '5', '--spill-low', '5'], '0 <= --spill-low < --spill-high'),
    (['--spill-high', '5', '--spill-low', '-1'], '0 <= --spill-low < --spill-high'),
    (['--spill-high', '5', '--spill-low', '3', '--prefer-close'], 'requires --prefer-close and --site-anchors'),
    (['--spill-high', '5', '--spill-low', '3', '--site-anchors'], 'requires --prefer-close and --site-anchors'),
    (['--cpu-hpa-target', '80'], 'placement uses latency HPA for both methods'),
])
def test_steep_placement_rejects_invalid_flags(tmp_path, capsys, flags, error):
    plan = tmp_path / 'plan'
    with pytest.raises(SystemExit) as exc:
        campaign.main(['--comparison', 'placement', '--plan-only', str(plan), *flags])
    assert exc.value.code == 2
    assert error in capsys.readouterr().err
    assert not plan.exists()


def test_edge_only_rejects_scaling_comparison(tmp_path, capsys):
    plan = tmp_path / 'plan'
    with pytest.raises(SystemExit) as exc:
        campaign.main(['--comparison', 'scaling', '--edge-only', '--plan-only', str(plan)])
    assert exc.value.code == 2
    assert 'scaling runs on core-5g' in capsys.readouterr().err
    assert not plan.exists()


def test_edge_only_connectivity_probe_renders_one_node_per_match_term(monkeypatch):
    settings = campaign.load(campaign.ROOT / 'experiments/campaign-steep.yaml')
    documents = []

    def fake_kube(*args, **kwargs):
        if args == ('create', '-f', '-'):
            documents.extend(yaml.safe_load_all(kwargs['input']))

    monkeypatch.setattr(campaign, 'kube', fake_kube)
    monkeypatch.setattr(campaign, '_pod_nodes', lambda _: [])
    monkeypatch.setattr(campaign, 'get', lambda *_: {'spec': {'ports': [{'nodePort': 30000}]}})
    monkeypatch.setattr(campaign, 'command', lambda *args, **kwargs: None)
    campaign.connectivity_probe(settings | {'edge_only': True})

    deployment = next(doc for doc in documents if doc['kind'] == 'Deployment')
    affinity = deployment['spec']['template']['spec']['affinity']['nodeAffinity']
    assert affinity['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'] == [
        {'matchFields': [{'key': 'metadata.name', 'operator': 'In', 'values': [node]}]}
        for node in ('gnb-2', 'gnb-3')
    ]


def test_site_anchor_readiness_requires_an_anchor_on_each_site(monkeypatch):
    app = 'mec-placement-face-detection'
    endpoints = [
        {'nodeName': node, 'targetRef': {'kind': 'Pod', 'name': f'{app}-anchor-{site}-pod'},
         'conditions': {'ready': True}}
        for site, node in campaign.SITE_NODES.items()
    ]
    monkeypatch.setattr(campaign, 'kube', lambda *a, **kw: types.SimpleNamespace(
        stdout=json.dumps({'items': [{'endpoints': endpoints}]})))
    campaign._wait_site_anchor_endpoints(app, timeout=0)
    endpoints[0]['targetRef']['name'] = f'{app}-managed-pod'
    with pytest.raises(RuntimeError, match='missing ready site-anchor endpoints on core-5g'):
        campaign._wait_site_anchor_endpoints(app, timeout=0)


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


@pytest.mark.parametrize(('floor_ms', 'site_anchors', 'edge_only'),
                         [(0, False, False), (130, False, False), (0, True, False), (125, True, True)])
def test_incluster_go_with_hpa_applies_manifests_without_host_controller_processes(monkeypatch, tmp_path,
                                                                                floor_ms, site_anchors, edge_only):
    spawned = []
    kube_calls = []
    local_commands = []
    capacity_checks = []
    metric_name = campaign._latency_metric_name(floor_ms)

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
        if 'experiments.campaign_ue' in ' '.join(cmd):
            assert not any('scale' in args for args in kube_calls)
            kwargs['stdout'].write('[RUN] phase_01: traffic started\n')
            kwargs['stdout'].flush()
        return proc

    def fake_kube(*args, **kwargs):
        kube_calls.append(args)
        if 'logs' in args and 'stdout' in kwargs:
            kwargs['stdout'].write('scheduler bound pod=test node=gnb-2\n')
        if args[:2] == ('get', '--raw') and metric_name in args[2]:
            return subprocess.CompletedProcess(
                args, 0,
                stdout=json.dumps({'items': [{'metricName': metric_name, 'value': '100m'}]}),
                stderr='',
            )
        return subprocess.CompletedProcess(args, 0, stdout='deployment.apps/mec-placement-pedestrian-detection\n', stderr='')

    def fake_get(kind, name, namespace=campaign.NAMESPACE):
        if kind == 'deployment' and name == 'placement-controller' and namespace == 'placement':
            return {'spec': {'replicas': 1}, 'status': {'readyReplicas': 1}}
        if kind == 'deployment' and namespace == campaign.NAMESPACE:
            replicas = 1 if '-anchor-' in name else 3
            return {'spec': {'replicas': replicas}, 'status': {'readyReplicas': replicas}}
        if kind == 'service':
            return {'spec': {'ports': [{'nodePort': 30080}]}}
        raise AssertionError(f'unexpected get {kind} {name} {namespace}')

    monkeypatch.setattr(campaign.subprocess, 'Popen', fake_popen)
    monkeypatch.setattr(campaign, 'kube', fake_kube)
    monkeypatch.setattr(campaign, 'get', fake_get)
    monkeypatch.setattr(campaign, 'command', lambda args, **kw: local_commands.append(args))
    monkeypatch.setattr(campaign, 'active_telemetry', lambda url: True)
    monkeypatch.setattr(campaign, 'stop_process', lambda p: None)
    monkeypatch.setattr(campaign, 'check_service_routing', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'transfer', lambda *a: None)
    monkeypatch.setattr(campaign, 'time', type('T', (), {'monotonic': lambda: 0.0, 'sleep': lambda _: None}))

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
        'latency_floor_ms': floor_ms,
        'site_anchors': site_anchors,
        'prefer_close': site_anchors,
        'edge_only': edge_only,
    }
    run_root = tmp_path / 'run'
    run_root.mkdir()
    if site_anchors:
        monkeypatch.setattr(campaign, 'check_node_capacity', lambda node, *a: capacity_checks.append(node))
        monkeypatch.setattr(campaign, '_wait_site_anchor_endpoints', lambda *a, **kw: None)
        campaign._campaign_manifest(run_root, True, settings['workload'], prefer_close=True, site_anchors=True,
                                    edge_only=edge_only)
    campaign.run_method(
        settings, 'camp', run_root, 'proposed', delay=None, slo_ms=150, max_replicas=10,
        comparison='placement' if edge_only else 'full', autoscaler='hpa', placement_engine='go',
        min_replicas=3 if site_anchors else None,
    )

    assert not any('controller' in ' '.join(map(str, c)) for c in spawned)
    cm_file = run_root / 'proposed' / 'placement-configmap.yaml'
    assert any(args[:2] == ('apply', '-f') and str(cm_file) in args for args in kube_calls)
    assert any(args[:4] == ('-n', 'placement', 'rollout', 'restart') for args in kube_calls)
    assert any(args[:4] == ('-n', 'placement', 'rollout', 'status') for args in kube_calls)
    assert any(args[:2] == ('apply', '-f') and str(run_root / 'proposed-workload.yaml') in map(str, args) for args in kube_calls)
    assert any(args[:2] == ('apply', '-f') and str(run_root / 'proposed-hpa.yaml') in map(str, args) for args in kube_calls)
    assert any(args[:2] == ('get', '--raw') and metric_name in args[2] for args in kube_calls)
    hpa = yaml.safe_load((run_root / 'proposed-hpa.yaml').read_text())
    assert hpa['spec']['metrics'][0]['external']['metric']['name'] == metric_name
    assert hpa['spec']['metrics'][0]['external']['target']['value'] == f'{150 - floor_ms}m'
    cm = yaml.safe_load(cm_file.read_text())
    cfg = yaml.safe_load(cm['data']['config.yaml'])
    assert cfg['manifest'] == '/etc/placement/manifest.yaml'
    assert cfg['prometheus_url'] == 'http://placement-prometheus.placement.svc:9090'
    assert cfg['mutate'] is True
    assert ('core-5g' in cfg['eligible_nodes']) is not edge_only
    anchor_sites = campaign._site_anchor_nodes(edge_only) if site_anchors else {}
    assert capacity_checks == list(anchor_sites.values())
    assert 'scheduler bound pod=test' in (run_root / 'proposed' / 'controller.log').read_text()
    app = campaign.workload_identity(settings['workload'])
    scale_calls = [args for args in kube_calls if 'scale' in args]
    if site_anchors:
        assert scale_calls == [('-n', campaign.NAMESPACE, 'scale', f'deployment/{app}',
                                '--current-replicas=0', '--replicas=1')]
        assert ('-n', campaign.NAMESPACE, 'rollout', 'status', f'deployment/{app}',
                '--timeout=180s') not in kube_calls
        for site in anchor_sites:
            assert ('-n', campaign.NAMESPACE, 'rollout', 'status', f'deployment/{app}-anchor-{site}',
                    '--timeout=180s') in kube_calls
    else:
        assert scale_calls == []
    preflight = next(cmd for cmd in local_commands if 'utils.preflight' in cmd)
    assert ('--allow-zero-replicas' in preflight) == site_anchors


@pytest.mark.parametrize('transition', ['continuous', 'restart'])
def test_phase_transition_mode_is_recorded_and_forwarded(tmp_path, transition):
    output = tmp_path / transition
    campaign.main(['--scenario', 'mobility9', '--fixed-replicas', '6',
                   '--phase-transition', transition, '--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['phase_transition'] == transition
    assert plan['campaign_settings']['phase_transition'] == transition


def test_mobility9_plan_rotates_fixed_demand_with_matched_hpa(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    output = tmp_path / 'mobility-plan'
    campaign.main(['--scenario', 'mobility9', '--comparison', 'placement', '--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['comparison'] == 'placement'
    assert plan['slo_ms'] == 150
    assert plan['mobility_direction'] == 'forward'
    expected_schedule = '9:7:1:1,9:5:3:1,9:3:5:1,9:1:7:1,9:1:5:3,9:1:3:5,9:1:1:7'
    assert plan['schedule'] == expected_schedule
    assert plan['phase_duration'] == 120
    assert plan['phase_durations_s'] == [120.0] * 7
    parsed = campaign.parse_schedule(plan['schedule'])
    assert len(parsed) == 7
    assert all(p['workload_ue'] == 9 and (p['gnb1_ues'] + p['gnb2_ues'] + p['gnb3_ues']) == 9 for p in parsed)
    settings = plan['campaign_settings']
    assert settings['phase_duration'] == 120
    assert settings['schedule'] == expected_schedule
    assert settings['mobility_direction'] == 'forward'
    baseline = yaml.safe_load((output / 'baseline-hpa.yaml').read_text())
    proposed = yaml.safe_load((output / 'proposed-hpa.yaml').read_text())
    assert baseline['spec'] == proposed['spec']
    assert proposed['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 300
    assert proposed['spec']['metrics'][0]['external']['target']['value'] == '150m'
    cfg = yaml.safe_load((output / 'proposed-controller.yaml').read_text())
    assert cfg['deschedule_interval_seconds'] == 30
    original = yaml.safe_load((campaign.ROOT / 'config.yaml').read_text())
    assert cfg['cpu_utilization_query'] == original['cpu_utilization_query'].replace('gnb1|', 'core-5g|')
    cm = campaign._render_incluster_placement_configmap(cfg, cfg['manifest'])
    cm_cfg = yaml.safe_load(cm['data']['config.yaml'])
    assert cm_cfg['deschedule_interval_seconds'] == 30
    for dead_key in campaign._GO_PLACEMENT_OMIT_KEYS:
        assert dead_key not in cm_cfg
    assert 'gpu_nodes' in cm_cfg
    assert cm_cfg['resource_weights'] == {'cpu': 0.5, 'memory': 0.5, 'gpu': 0.0, 'vram': 0.0}


def test_two_dim_resource_weights_match_legacy_four_dim_with_zero_gpu_vram():
    """Verify 2D (cpu, memory) placement score equals legacy 4D score when gpu=vram=0."""
    nodes = [
        {'name': 'gnb1', 'cap': {'cpu': 8.0, 'memory': 16.0}, 'alloc': {'cpu': 3.0, 'memory': 6.0},
         'arrival': 10.0, 'gpu_free': 0.0, 'vram_free': 0.0},
        {'name': 'gnb-2', 'cap': {'cpu': 20.0, 'memory': 64.0}, 'alloc': {'cpu': 4.0, 'memory': 12.0},
         'arrival': 25.0, 'gpu_free': 0.75, 'vram_free': 0.6},
        {'name': 'gnb-3', 'cap': {'cpu': 20.0, 'memory': 64.0}, 'alloc': {'cpu': 2.0, 'memory': 8.0},
         'arrival': 5.0, 'gpu_free': 0.9, 'vram_free': 0.85},
    ]
    locality_weight = 0.4
    total_arrival = sum(n['arrival'] for n in nodes)
    w_old = {'cpu': 0.5, 'memory': 0.5, 'gpu': 0.0, 'vram': 0.0}
    w_new = {'cpu': 0.5, 'memory': 0.5}
    for n in nodes:
        h_cpu = max(0.0, (n['cap']['cpu'] - n['alloc']['cpu']) / n['cap']['cpu'])
        h_mem = max(0.0, (n['cap']['memory'] - n['alloc']['memory']) / n['cap']['memory'])
        loc = n['arrival'] / total_arrival
        score_old = (1.0 - locality_weight) * (
            w_old['cpu'] * h_cpu + w_old['memory'] * h_mem
            + w_old['gpu'] * n['gpu_free'] + w_old['vram'] * n['vram_free']
        ) + locality_weight * loc
        score_new = (1.0 - locality_weight) * (
            w_new['cpu'] * h_cpu + w_new['memory'] * h_mem
        ) + locality_weight * loc
        assert score_new == score_old


def test_mobility9_reverse_direction_reverses_schedule_and_records_metadata(tmp_path):
    output = tmp_path / 'reverse-plan'
    campaign.main([
        '--scenario', 'mobility9', '--mobility-direction', 'reverse',
        '--fixed-replicas', '6', '--phase-duration', '90',
        '--rps-per-ue', '4', '--max-in-flight-per-ue', '6',
        '--plan-only', str(output),
    ])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    expected_reverse = '9:1:1:7,9:1:3:5,9:1:5:3,9:1:7:1,9:3:5:1,9:5:3:1,9:7:1:1'
    assert plan['mobility_direction'] == 'reverse'
    assert plan['schedule'] == expected_reverse
    assert plan['schedule'].split(',') == list(reversed(campaign.MOBILITY9_SCHEDULE.split(',')))
    assert plan['phase_duration'] == 90
    assert plan['phase_durations_s'] == [90.0] * 7
    assert plan['fixed_replicas'] == 6
    assert plan['replica_bounds'] == {'min': 6, 'max': 6}
    assert plan['pacing'] == {'rps_per_ue': 4.0, 'max_in_flight_per_ue': 6}
    parsed = campaign.parse_schedule(plan['schedule'])
    assert len(parsed) == 7
    assert all(p['workload_ue'] == 9 and (p['gnb1_ues'] + p['gnb2_ues'] + p['gnb3_ues']) == 9 for p in parsed)
    assert plan['campaign_settings']['mobility_direction'] == 'reverse'
    assert plan['campaign_settings']['schedule'] == expected_reverse
    assert plan['campaign_settings']['phase_duration'] == 90


def test_mobility9_long_run_extends_phases_with_fixed_budget_and_normal_timers(tmp_path):
    output = tmp_path / 'long-plan'
    campaign.main(['--scenario', 'mobility9', '--long-run', '--fixed-replicas', '9',
                   '--tc-ms', '10', '--slo-ms', '150', '--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['long_run'] is True
    assert plan['mobility_direction'] == 'forward'
    assert plan['phase_durations_s'] == [120.0] * 7
    assert plan['campaign_settings']['phase_duration'] == 120
    assert plan['campaign_settings']['schedule'] == campaign.MOBILITY9_SCHEDULE
    assert plan['slo_ms'] == 150
    assert plan['tc_ms'] == 10
    assert plan['replica_bounds'] == {'min': 9, 'max': 9}
    assert plan['pacing'] == {'rps_per_ue': 5.0, 'max_in_flight_per_ue': 8}
    cfg = yaml.safe_load((output / 'proposed-controller.yaml').read_text())
    assert cfg['deschedule_interval_seconds'] == 30
    for method in ('baseline', 'proposed'):
        hpa = yaml.safe_load((output / f'{method}-hpa.yaml').read_text())
        assert hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 300


def test_mobility9_preserves_explicit_timer_and_slo_overrides(tmp_path):
    output = tmp_path / 'plan'
    campaign.main(['--scenario', 'mobility9', '--deschedule-interval', '45',
                   '--phase-duration', '90', '--slo-ms', '150', '--plan-only', str(output)])
    plan = yaml.safe_load((output / 'plan.yaml').read_text())
    assert plan['campaign_settings']['phase_duration'] == 90
    assert plan['phase_durations_s'] == [90.0] * 7
    assert plan['slo_ms'] == 150
    cfg = yaml.safe_load((output / 'proposed-controller.yaml').read_text())
    assert cfg['deschedule_interval_seconds'] == 45


@pytest.mark.parametrize('flags', [
    ['--deschedule-interval', '0'], ['--deschedule-interval', 'nan'],
    ['--deschedule-interval', '-1'], ['--deschedule-interval', 'inf'],
    ['--scenario', 'mobility9', '--debug'],
    ['--scenario', 'mobility9', '--placement-engine', 'python'],
    ['--long-run'],
    ['--scenario', 'scaler-balanced', '--long-run'],
    ['--scenario', 'mobility9', '--long-run', '--phase-duration', '90'],
    ['--mobility-direction', 'reverse'],
    ['--scenario', 'scaler-balanced', '--mobility-direction', 'reverse'],
    ['--scenario', 'mobility9', '--mobility-direction', 'sideways'],
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
    assert plan['replica_bounds'] == {'min': 6, 'max': 6}
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
    assert plan['campaign_settings']['phase_duration'] == 120
    assert plan['phase_durations_s'] == [120.0] * 7


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


def test_staircase_plan_is_balanced_with_matched_bounded_hpas(tmp_path):
    out = tmp_path / 'staircase'
    campaign.main(['--scenario', 'scaler-staircase', '--final-hold', '--slo-ms', '150', '--plan-only', str(out)])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    assert plan['comparison'] == 'scaling'
    steps = [list(map(int, s.split(':'))) for s in plan['schedule'].split(',')]
    assert [s[0] for s in steps] == [3, 6, 9, 12, 15, 18, 24, 18, 15, 12, 9, 6, 3]
    assert all(s[1:] == [s[0] // 3] * 3 for s in steps)
    assert plan['phase_durations_s'] == [120.0] * 12 + [600.0]
    assert plan['hpa_behavior_profile'] == 'bounded-staircase'
    hpas = [yaml.safe_load((out / f'{m}-hpa.yaml').read_text()) for m in ('baseline', 'proposed')]
    assert hpas[0]['spec']['behavior'] == hpas[1]['spec']['behavior']
    assert 'tolerance' not in hpas[0]['spec']['behavior']['scaleDown']
    assert 'policies' not in hpas[0]['spec']['behavior']['scaleDown']
    assert 'policies' not in hpas[0]['spec']['behavior']['scaleUp']
    assert hpas[1]['spec']['metrics'][0]['external']['metric']['name'] == 'ue_p90_excess_latency_120ms_seconds'
    assert hpas[1]['spec']['metrics'][0]['external']['target']['value'] == '30m'
    for m in ('baseline', 'proposed'):
        dep = next(d for d in yaml.safe_load_all((out / f'{m}-workload.yaml').read_text()) if d['kind'] == 'Deployment')
        spec = dep['spec']['template']['spec']
        assert spec['schedulerName'] == 'default-scheduler'
        assert spec['containers'][0]['resources']['limits'] == {'cpu': '1', 'memory': '512Mi'}


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
                        'values': ['core-5g'],
                    }]
                }]
            }
        }
    }
    for dep in (base_dep, prop_dep):
        assert dep['spec']['replicas'] == 3
        assert dep['spec']['template']['spec']['schedulerName'] == 'default-scheduler'
        assert dep['spec']['template']['spec']['affinity'] == expected_affinity
        env = {e['name']: e.get('value') for e in dep['spec']['template']['spec']['containers'][0]['env']}
        assert env['HTTP_THREADS'] == '5'

    base_hpa = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())
    prop_hpa = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    assert base_hpa['spec']['minReplicas'] == 3
    assert base_hpa['spec']['maxReplicas'] == 24
    assert base_hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 300
    assert base_hpa['spec']['metrics'][0]['resource']['name'] == 'cpu'
    assert base_hpa['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 80

    assert prop_hpa['spec']['minReplicas'] == 3
    assert prop_hpa['spec']['maxReplicas'] == 24
    assert prop_hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 300
    assert prop_hpa['spec']['metrics'][0]['external']['metric']['name'] == 'ue_p90_latency_seconds'
    assert prop_hpa['spec']['metrics'][0]['external']['target']['value'] == '150m'

    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    assert plan['comparison'] == 'scaling'
    assert plan['scenario'] == 'scaler-balanced'
    assert plan['http_threads'] == 5
    assert plan['schedule'] == '3:1:1:1,9:3:3:3,15:5:5:5,24:8:8:8,15:5:5:5,9:3:3:3,3:1:1:1'
    assert plan['phase_durations_s'] == [120.0] * 7
    assert plan['final_hold_seconds'] == 0
    assert plan['node_restriction'] == 'core-5g'
    assert plan['replica_bounds'] == {'min': 3, 'max': 24}
    assert plan['pacing'] == {'rps_per_ue': 5.0, 'max_in_flight_per_ue': 8}
    assert plan['hpa_targets'] == {
        'baseline': {'type': 'cpu_utilization_percent', 'target': 80},
        'proposed': {'type': 'latency_p90_ms', 'target': 150},
    }
    assert plan['tc_ms'] == 10
    assert plan['resource_measurement'] == {
        'source': 'prometheus-cadvisor',
        'scrape_interval_seconds': 5,
        'rate_window_seconds': 45,
        'freshness_seconds': 25,
        'metrics_server_hpa_unchanged': True,
    }


def test_scaler_balanced_final_hold_and_overrides(tmp_path):
    out = tmp_path / 'scaler_hold'
    campaign.main([
        '--comparison', 'scaling', '--scenario', 'scaler-balanced', '--final-hold',
        '--max-replicas', '12', '--tc-ms', '15', '--rps-per-ue', '4', '--max-in-flight-per-ue', '6',
        '--plan-only', str(out),
    ])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    assert plan['phase_durations_s'] == [120.0] * 6 + [480.0]
    assert plan['final_hold_seconds'] == 360
    assert plan['replica_bounds'] == {'min': 3, 'max': 12}
    assert plan['tc_ms'] == 15.0
    assert plan['pacing'] == {'rps_per_ue': 4.0, 'max_in_flight_per_ue': 6}
    assert yaml.safe_load((out / 'baseline-hpa.yaml').read_text())['spec']['maxReplicas'] == 12
    assert yaml.safe_load((out / 'proposed-hpa.yaml').read_text())['spec']['maxReplicas'] == 12


@pytest.mark.parametrize('floor_ms', [0, 130])
def test_scaling_mode_skips_placement_controller_preflight_and_mutations(tmp_path, monkeypatch, floor_ms):
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
        if args[:4] == ('-n', 'services', 'get', 'events'):
            raise subprocess.CalledProcessError(1, args, 'events unavailable')
        raise AssertionError(f'unexpected kube call: {args}')

    def fake_get(kind, name, namespace='services'):
        if namespace == 'placement' and name == 'placement-controller':
            raise AssertionError('placement-controller must not be queried in scaling mode')
        if namespace == 'placement' and name == 'prometheus-adapter':
            return {'spec': {'replicas': 1}, 'status': {'readyReplicas': 1}}
        if kind == 'deployment' and namespace == campaign.NAMESPACE:
            return {'spec': {'replicas': 3}, 'status': {'readyReplicas': 3}}
        if kind == 'service':
            return {'spec': {'ports': [{'nodePort': 30000}], 'externalTrafficPolicy': 'Local'}}
        raise AssertionError(f'unexpected get({kind}, {name}, {namespace})')

    monkeypatch.setattr(campaign, 'kube', fake_kube)
    monkeypatch.setattr(campaign, 'get', fake_get)
    campaign.verify_incluster_components('hpa', 'go', comparison='scaling')

    monkeypatch.setattr(campaign, 'command', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'check_service_routing', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'active_telemetry', lambda *a, **kw: True)
    queried = []
    def query_metric(allow_not_found=True, metric_name='ue_p90_latency_seconds'):
        queried.append(metric_name)
        return True
    monkeypatch.setattr(campaign, '_query_external_latency_metric', query_metric)
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
        'phase_durations': [60.0] * 6 + [420.0], 'workload': 'pedestrian-detection',
        'ue_host': 'ue', 'ue_project': '/ue',
        'staircase': bool(floor_ms), 'latency_floor_ms': floor_ms,
    }
    kube_calls.clear()
    root = tmp_path / 'run'
    root.mkdir()
    campaign.run_method(
        settings, 'scaler_01', root, 'proposed', None, 150, 24,
        comparison='scaling', autoscaler='hpa', placement_engine='go',
    )
    assert not (root / 'proposed' / 'placement-configmap.yaml').exists()
    assert not any('placement-controller' in ' '.join(map(str, c)) for c in kube_calls)
    assert not any('-m controller' in ' '.join(map(str, cmd)) for cmd in popen_cmds)
    assert any('utils.monitor_resources' in ' '.join(map(str, cmd)) and '--pods-only' in cmd for cmd in popen_cmds)
    assert any('utils.monitor_resources' in ' '.join(map(str, cmd)) and 'hpa-sample' in cmd for cmd in popen_cmds)
    assert (root / 'proposed' / 'workload-events.json').exists()
    assert written_payloads[0]['phase_durations'] == [60.0] * 6 + [420.0]
    assert queried == [campaign._latency_metric_name(floor_ms)]


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


def test_check_node_capacity_counts_init_container_effective_request(tmp_path, monkeypatch):
    manifest = tmp_path / 'workload.yaml'
    manifest.write_text(yaml.safe_dump({
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'spec': {'template': {'spec': {'containers': [{
            'resources': {'requests': {'cpu': '500m', 'memory': '256Mi'}},
        }]}}},
    }))
    node = {'status': {'allocatable': {'cpu': '2', 'memory': '4Gi'}}}
    pods = {'items': [{
        'metadata': {'namespace': 'system'},
        'status': {'phase': 'Running'},
        'spec': {
            'containers': [{'resources': {'requests': {'cpu': '100m', 'memory': '128Mi'}}}],
            'initContainers': [{'resources': {'requests': {'cpu': '2', 'memory': '128Mi'}}}],
        },
    }]}
    monkeypatch.setattr(
        campaign, 'kube',
        lambda *args, **kw: types.SimpleNamespace(stdout=json.dumps(node if args[:2] == ('get', 'node') else pods)),
    )
    with pytest.raises(RuntimeError, match='node gnb-3 cannot fit 1 replicas'):
        campaign.check_node_capacity('gnb-3', manifest, 1)


def test_check_node_capacity_handles_restartable_inits_overhead_pod_resources_and_terminating_pods(tmp_path, monkeypatch):
    # Template: 200m container + 300m restartable init + 100m overhead = 600m/pod.
    manifest = tmp_path / 'workload.yaml'
    manifest.write_text(yaml.safe_dump({
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'spec': {'template': {'spec': {
            'overhead': {'cpu': '100m', 'memory': '64Mi'},
            'initContainers': [
                {'restartPolicy': 'Always', 'resources': {'requests': {'cpu': '300m', 'memory': '128Mi'}}},
                {'resources': {'requests': {'cpu': '150m', 'memory': '64Mi'}}},
            ],
            'containers': [{'resources': {'requests': {'cpu': '200m', 'memory': '128Mi'}}}],
        }}},
    }))
    node = {'status': {'allocatable': {'cpu': '2', 'memory': '4Gi'}}}
    # Active pods:
    # - terminating workload pod with pod-level 1000m CPU + 100m overhead = 1100m (kept reserved)
    # - succeeded pod with 4000m CPU (ignored as terminal)
    pods = {'items': [
        {
            'metadata': {
                'namespace': 'services',
                'deletionTimestamp': '2026-11-16T00:00:00Z',
                'labels': {'app': 'mec-placement-face-detection'},
            },
            'status': {'phase': 'Running'},
            'spec': {
                'resources': {'requests': {'cpu': '1000m', 'memory': '256Mi'}},
                'overhead': {'cpu': '100m', 'memory': '32Mi'},
                'containers': [{'resources': {'requests': {'cpu': '200m', 'memory': '128Mi'}}}],
            },
        },
        {
            'metadata': {'namespace': 'batch'},
            'status': {'phase': 'Succeeded'},
            'spec': {'containers': [{'resources': {'requests': {'cpu': '4', 'memory': '4Gi'}}}]},
        },
    ]}
    monkeypatch.setattr(
        campaign, 'kube',
        lambda *args, **kw: types.SimpleNamespace(stdout=json.dumps(node if args[:2] == ('get', 'node') else pods)),
    )
    # Remaining CPU: 2000m - 1100m = 900m -> fits 1 replica (600m), fails 2 replicas (1200m).
    campaign.check_node_capacity('gnb-3', manifest, 1, 'mec-placement-face-detection')
    with pytest.raises(RuntimeError, match='node gnb-3 cannot fit 2 replicas'):
        campaign.check_node_capacity('gnb-3', manifest, 2, 'mec-placement-face-detection')


@pytest.mark.parametrize('spec', [
    {'resourceClaims': [{'name': 'gpu'}], 'containers': [{'resources': {'requests': {'cpu': '100m'}}}]},
    {'containers': [{'name': 'app', 'resources': {'claims': [{'name': 'gpu'}], 'requests': {'cpu': '100m'}}}]},
    {'initContainers': [{'restartPolicy': 'OnFailure', 'resources': {'requests': {'cpu': '100m'}}}], 'containers': []},
    {'overhead': {'ephemeral-storage': '1Gi'}, 'containers': [{'resources': {'requests': {'cpu': '100m'}}}]},
    {'resources': {'claims': [{'name': 'gpu'}]}, 'containers': [{'resources': {'requests': {'cpu': '100m'}}}]},
])
def test_pod_effective_requests_rejects_unsupported_configurations(spec):
    with pytest.raises(ValueError, match='unsupported'):
        campaign._pod_effective_requests(spec)


def test_run_method_collects_filtered_events_even_on_failure(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    # Keep event filtering independent of test execution speed.
    monkeypatch.setattr(campaign.time, 'time', lambda: now.timestamp())
    old_iso = (now - timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%SZ')
    new_iso = (now + timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

    def fake_kube(*args, **kwargs):
        if args[0] == 'apply' or args[:3] == ('-n', 'services', 'rollout'):
            return types.SimpleNamespace(stdout='')
        if args[:4] == ('-n', 'services', 'get', 'deployments,pods,hpa'):
            return types.SimpleNamespace(stdout=json.dumps({'items': [
                {'metadata': {'name': 'mec-placement-pedestrian-detection-abc', 'uid': 'uid-cur-pod'}},
            ]}))
        if args[:4] == ('-n', 'services', 'get', 'events'):
            return types.SimpleNamespace(stdout=json.dumps({'items': [
                {'involvedObject': {'name': 'mec-placement-pedestrian-detection-old', 'uid': 'uid-old'}, 'lastTimestamp': old_iso},
                {'involvedObject': {'name': 'mec-placement-pedestrian-detection-baseline', 'uid': 'uid-base-hpa'}, 'lastTimestamp': new_iso},
                {'involvedObject': {'name': 'mec-placement-pedestrian-detection-abc', 'uid': 'uid-cur-pod'}, 'lastTimestamp': new_iso},
                {'involvedObject': {'name': 'unrelated-pod', 'uid': 'uid-other'}, 'lastTimestamp': new_iso},
            ]}))
        raise AssertionError(f'unexpected kube call: {args}')

    monkeypatch.setattr(campaign, 'kube', fake_kube)
    monkeypatch.setattr(campaign, 'get', lambda *a, **kw: {'spec': {'ports': [{'nodePort': 30000}], 'externalTrafficPolicy': 'Local'}, 'status': {'readyReplicas': 3}})
    monkeypatch.setattr(campaign, 'command', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'check_service_routing', lambda *a, **kw: None)
    monkeypatch.setattr(campaign, 'active_telemetry', lambda *a, **kw: True)
    monkeypatch.setattr(campaign, '_query_external_latency_metric', lambda allow_not_found=True: True)
    monkeypatch.setattr(campaign, 'RunnerProgress', lambda *a, **kw: types.SimpleNamespace(poll=lambda final=False: None))
    monkeypatch.setattr(campaign.time, 'sleep', lambda _: None)
    monkeypatch.setattr(campaign, 'stop_process', lambda p: None)

    class FailingUEPopen:
        def __init__(self, cmd, **kwargs):
            self.is_ue = 'campaign_ue' in ' '.join(map(str, cmd))
            self.ue_polls = 0
            self.returncode = 1
            self.stdin = types.SimpleNamespace(write=lambda data: None, close=lambda: None)

        def poll(self):
            if not self.is_ue:
                return None
            self.ue_polls += 1
            return None if self.ue_polls == 1 else 1

        def wait(self, timeout=None):
            return 1

    monkeypatch.setattr(campaign.subprocess, 'Popen', FailingUEPopen)
    settings = {
        'inventory': 'inv.yaml', 'nr_ue': '/bin/nr-ue', 'token': 'tok', 'image': 'img',
        'telemetry_url': 'http://127.0.0.1/obs', 'site_node_ips': {'gnb-1': '10.0.0.1', 'gnb-2': '10.0.0.2', 'gnb-3': '10.0.0.3'},
        'schedule': campaign.SCALER_BALANCED_SCHEDULE, 'phase_duration': 60,
        'workload': 'pedestrian-detection', 'ue_host': 'ue', 'ue_project': '/ue',
    }
    root = tmp_path / 'run'
    root.mkdir()
    with pytest.raises(RuntimeError, match='UE workload failed'):
        campaign.run_method(
            settings, 'fail_01', root, 'proposed', None, 150, 24,
            comparison='scaling', autoscaler='hpa', placement_engine='go',
        )
    events = json.loads((root / 'proposed' / 'workload-events.json').read_text())['items']
    assert [ev['involvedObject']['uid'] for ev in events] == ['uid-cur-pod']


@pytest.mark.parametrize('routing,prefer_close', [('--cluster-wide', False), ('--prefer-close', True)])
def test_fixed_mobility_plan_uses_pod_share_without_hpa(tmp_path, routing, prefer_close):
    out = tmp_path / 'plan'
    campaign.main(['--scenario', 'mobility9', '--comparison', 'placement', '--site-anchors',
                   '--fixed-replicas', '6', '--no-hpa', '--pod-share-scoring', routing,
                   '--deschedule-interval', '5', '--plan-only', str(out)])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    controller = yaml.safe_load((out / 'proposed-controller.yaml').read_text())
    assert plan['autoscaler'] == 'none'
    assert plan['hpa_targets'] is None
    assert plan['active_total_replica_bounds'] == {'min': 6, 'max': 6}
    assert plan['prefer_close'] is prefer_close
    assert controller['prefer_close'] is True
    assert controller['deschedule_interval_seconds'] == 5
    assert not list(out.glob('*-hpa.yaml'))
    for method in ('baseline', 'proposed'):
        resources = list(yaml.safe_load_all((out / f'{method}-workload.yaml').read_text()))
        deployment = next(r for r in resources if r['kind'] == 'Deployment')
        service = next(r for r in resources if r['kind'] == 'Service')
        assert deployment['spec']['replicas'] == 3
        assert (service['spec'].get('trafficDistribution') == 'PreferClose') is prefer_close


def test_vertical_arm_manifest_preserves_shared_service_routing(tmp_path):
    campaign._campaign_manifest(tmp_path, True, 'pedestrian-detection',
                                prefer_close=True, site_anchors=True)
    for arm_name in ('baseline-local', 'baseline-vertical'):
        arm = tmp_path / arm_name
        arm.mkdir()
        config, _, hpa = campaign._method_configs(
            arm, 'baseline', 150, 18, workload='pedestrian-detection',
            site_anchors=True, no_hpa=True, base_manifest=tmp_path / 'workload.yaml',
        )
        documents = list(yaml.safe_load_all(Path(config['manifest']).read_text()))
        service = next(doc for doc in documents if doc['kind'] == 'Service')
        deployment = next(doc for doc in documents if doc['kind'] == 'Deployment')
        campaign.check_service_routing(service, prefer_close=True)
        assert deployment['spec']['replicas'] == 0
        assert hpa is None
