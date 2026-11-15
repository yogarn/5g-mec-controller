import copy
import json
import subprocess

import pytest

from utils import redeploy_placement as deploy

DIGEST = 'sha256:' + 'a' * 64


def test_redeploy_pins_published_digest_and_preserves_config(monkeypatch, tmp_path):
    monkeypatch.setattr(deploy, 'LOCK', str(tmp_path / 'lock'))
    dep = {'metadata': {'generation': 3}, 'spec': {'replicas': 1, 'template': {'spec': {
        'containers': [{'name': 'placement-controller', 'image': 'old:image'}]}}},
        'status': {'readyReplicas': 1, 'updatedReplicas': 1, 'observedGeneration': 3}}
    calls = []

    def command(args, **kwargs):
        args = list(map(str, args))
        calls.append(args)
        if 'inspect' in args:
            output = json.dumps(['localhost:30500/placement-controller@' + DIGEST])
        elif args[:4] == ['kubectl', '-n', 'placement', 'get']:
            output = json.dumps(copy.deepcopy(dep))
        else:
            output = ''
        if 'set' in args:
            dep['spec']['template']['spec']['containers'][0]['image'] = args[-1].split('=', 1)[1]
        return subprocess.CompletedProcess(args, 0, stdout=output)

    monkeypatch.setattr(deploy, 'command', command)
    deploy.main([])
    assert any('placement-controller=10.34.211.140:30500/placement-controller@' + DIGEST in cmd for cmd in calls)
    assert not any('apply' in cmd or 'restart' in cmd for cmd in calls)
    assert [cmd[1] for cmd in calls if cmd[0] == 'docker'] == ['build', 'push', 'image']


def test_failed_push_never_changes_cluster_image(monkeypatch, tmp_path):
    monkeypatch.setattr(deploy, 'LOCK', str(tmp_path / 'lock'))
    monkeypatch.setattr(deploy, 'deployment', lambda *_: {'spec': {'template': {'spec': {
        'containers': [{'name': 'placement-controller', 'image': 'old:image'}]}}}})
    calls = []

    def command(args, **kwargs):
        calls.append(args)
        if args[:2] == ['docker', 'push']:
            raise subprocess.CalledProcessError(1, args)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(deploy, 'command', command)
    with pytest.raises(subprocess.CalledProcessError):
        deploy.main([])
    assert not any('kubectl' in cmd for cmd in calls)


@pytest.mark.parametrize('values', [[], ['localhost:30500/placement-controller@bad'], ['other/repo@' + DIGEST]])
def test_invalid_digest_rejected(values):
    with pytest.raises(RuntimeError):
        deploy.published_digest('localhost:30500/placement-controller', values)


def test_plan_never_runs_commands(monkeypatch, capsys):
    monkeypatch.setattr(deploy, 'command', lambda *a, **kw: pytest.fail('executed command'))
    deploy.main(['--plan-only'])
    output = capsys.readouterr().out
    assert 'docker build --network=host' in output
    assert '@<published-sha256-digest>' in output


def test_campaign_lock_prevents_redeployment(monkeypatch, tmp_path):
    import fcntl
    lockpath = tmp_path / 'lock'
    monkeypatch.setattr(deploy, 'LOCK', str(lockpath))
    monkeypatch.setattr(deploy, 'command', lambda *a, **kw: pytest.fail('executed command'))
    with lockpath.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match='active'):
            deploy.main([])


def test_failed_readiness_is_not_reported_as_success(monkeypatch, tmp_path):
    monkeypatch.setattr(deploy, 'LOCK', str(tmp_path / 'lock'))
    dep = {'metadata': {'generation': 2}, 'spec': {'replicas': 1, 'template': {'spec': {
        'containers': [{'name': 'placement-controller', 'image': 'old:image'}]}}},
        'status': {'readyReplicas': 0, 'updatedReplicas': 1, 'observedGeneration': 2}}
    monkeypatch.setattr(deploy, 'deployment', lambda *_: dep)

    def command(args, **kwargs):
        if 'set' in args:
            dep['spec']['template']['spec']['containers'][0]['image'] = args[-1].split('=', 1)[1]
        return subprocess.CompletedProcess(args, 0, stdout=json.dumps(['localhost:30500/placement-controller@' + DIGEST]))

    monkeypatch.setattr(deploy, 'command', command)
    with pytest.raises(RuntimeError, match='verification failed'):
        deploy.main([])
