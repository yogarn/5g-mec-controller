import subprocess
from unittest.mock import Mock

import pytest

from experiments import campaign
from utils import connectivity


@pytest.mark.parametrize('failure', [None, 'registration', 'request'])
def test_probe_checks_all_sites_twice_and_always_closes_sessions(tmp_path, monkeypatch, failure):
    images = tmp_path / 'images'
    images.mkdir()
    (images / 'image.png').write_bytes(b'image data')
    output = tmp_path / 'output'
    output.mkdir()
    lifecycle = Mock()
    lifecycle.switch_counts.return_value = ([{'ip': '10.1.0.1'}], [{'ip': '10.2.0.1'}], [{'ip': '10.3.0.1'}])
    if failure == 'registration':
        lifecycle.switch_counts.side_effect = RuntimeError('PDU setup failed')
    monkeypatch.setattr(connectivity, 'OwnedUELifecycle', Mock(return_value=lifecycle))
    attempt = Mock(return_value={'success': failure != 'request', 'http_status': 200 if failure != 'request' else 503,
                                 'error': None, 'duration_ms': 12.5})
    monkeypatch.setattr(connectivity, 'attempt', attempt)
    settings = {'image': str(images), 'inventory': 'inventory.yaml', 'nr_ue': '/bin/nr-ue',
                'site_urls': {f'gnb-{i}': f'http://192.0.2.{i}:30000/detect' for i in (1, 2, 3)}}
    if failure:
        with pytest.raises(RuntimeError):
            connectivity.probe(settings, output)
    else:
        connectivity.probe(settings, output)
        assert lifecycle.switch_counts.call_count == 2
        assert attempt.call_count == 6
        assert [c.args[4] for c in attempt.call_args_list] == ['10.1.0.1', '10.2.0.1', '10.3.0.1'] * 2
        assert all(c.args[1] == 'POST' and c.args[5] == 10 for c in attempt.call_args_list)
    lifecycle.close.assert_called_once()


@pytest.mark.parametrize('failure', [None, 'rollout', 'remote'])
def test_preflight_removes_temporary_namespace_even_on_failure(monkeypatch, failure):
    settings = campaign.load(campaign.ROOT / 'experiments/campaign.yaml')
    calls = []
    def kube(*args, **kwargs):
        calls.append(args)
        if failure == 'rollout' and 'rollout' in args:
            raise RuntimeError('rollout failed')
        return subprocess.CompletedProcess(args, 0, stdout='{"items": []}', stderr='')
    monkeypatch.setattr(campaign, 'kube', kube)
    monkeypatch.setattr(campaign, 'get', lambda *args: {'spec': {'ports': [{'nodePort': 30000}]}})
    def command(args, **kwargs):
        calls.append(args)
        if failure == 'remote' and args[-1].endswith(' probe'):
            raise RuntimeError('UE session failed')
    monkeypatch.setattr(campaign, 'command', command)
    if failure:
        with pytest.raises(RuntimeError):
            campaign.connectivity_probe(settings)
    else:
        campaign.connectivity_probe(settings)
    assert calls[-1][:2] == ('delete', 'namespace')
    if failure != 'rollout':
        assert ' stop ' in calls[-2][-1]


def test_preflight_does_not_delete_namespace_it_could_not_create(monkeypatch):
    kube = Mock(side_effect=RuntimeError('already exists'))
    monkeypatch.setattr(campaign, 'kube', kube)
    with pytest.raises(RuntimeError):
        campaign.connectivity_probe({})
    kube.assert_called_once()
