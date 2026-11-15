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
    assert len(phases) == 19
    assert counts[:9] == list(reversed(counts[10:]))
    assert settings['phase_duration'] == 300


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
        'type': 'AverageValue', 'averageValue': '1000m'}


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
    assert hpa['spec']['metrics'][0]['resource']['target']['averageValue'] == '1000m'


def test_normal_hpa_and_proposed_share_15s_interval_and_300s_stabilization(tmp_path):
    config, _, path = campaign._method_configs(tmp_path, 'baseline', 500, 30, debug=False)
    hpa = yaml.safe_load(path.read_text())
    assert 'scale_up_cooldown_seconds' not in config
    assert hpa['spec']['behavior']['scaleUp']['stabilizationWindowSeconds'] == 0
    assert hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == config['scale_down_seconds'] == 300
    proposed, proposed_path, _ = campaign._method_configs(tmp_path, 'proposed', 500, 30)
    from controller.config import load_settings
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
