import fcntl
import json
import subprocess
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from utils import rebuild_pedestrian as rebuild


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    monkeypatch.setattr(rebuild, 'ROOT', tmp_path)
    monkeypatch.setattr(rebuild, 'LOCK', str(tmp_path / 'campaign.lock'))
    for relative in rebuild.MANIFESTS:
        path = tmp_path / relative
        path.parent.mkdir(parents=True)
        path.write_text(yaml.safe_dump({'kind': 'Deployment', 'spec': {'template': {'spec': {
            'containers': [{'name': 'pedestrian-detection', 'image': 'old-image',
                            'resources': {'requests': {'cpu': '500m'}}}],
        }}}}))
    return tmp_path


def test_build_smoke_push_and_pin_both_manifests(workspace, monkeypatch):
    calls = []
    digest = 'sha256:' + 'a' * 64

    def command(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(stdout=json.dumps([rebuild.PUSH_REPOSITORY + '@' + digest]))

    monkeypatch.setattr(rebuild, 'command', command)
    rebuild.main([])
    assert [c[1] for c in calls] == ['build', 'run', 'push', 'image']
    assert '/ready' in calls[1][-1] and '/health' in calls[1][-1]
    for relative in rebuild.MANIFESTS:
        container = yaml.safe_load((workspace / relative).read_text())['spec']['template']['spec']['containers'][0]
        assert container['image'] == rebuild.PULL_REPOSITORY + '@' + digest
        assert container['resources']['requests']['cpu'] == '500m'


@pytest.mark.parametrize('failure', ['smoke', 'digest'])
def test_failure_keeps_original_manifest_bytes(workspace, monkeypatch, failure):
    paths = [workspace / p for p in rebuild.MANIFESTS]
    before = [p.read_bytes() for p in paths]
    calls = []

    def command(args, **kwargs):
        calls.append(args)
        if failure == 'smoke' and args[1] == 'run':
            raise subprocess.CalledProcessError(1, args)
        return SimpleNamespace(stdout='[]')

    monkeypatch.setattr(rebuild, 'command', command)
    with pytest.raises((RuntimeError, subprocess.CalledProcessError)):
        rebuild.main([])
    assert [p.read_bytes() for p in paths] == before
    if failure == 'smoke':
        assert not any(c[1] == 'push' for c in calls)


def test_active_campaign_blocks_rebuild(workspace, monkeypatch):
    monkeypatch.setattr(rebuild, 'command', lambda *a, **kw: pytest.fail('Docker accessed'))
    with open(rebuild.LOCK, 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match='active'):
            rebuild.main([])


def test_preview_does_not_mutate_files_or_call_docker(workspace, monkeypatch):
    before = {p: p.read_bytes() for p in workspace.rglob('*.yaml')}
    monkeypatch.setattr(rebuild, 'command', lambda *a, **kw: pytest.fail('Docker accessed'))
    rebuild.main(['--plan-only'])
    assert {p: p.read_bytes() for p in before} == before


@pytest.mark.parametrize('action,args,module', [
    ('rebuild', ['--plan-only'], 'utils.rebuild_pedestrian'),
    ('ramp', ['scaler_02'], 'experiments.campaign'),
])
def test_short_shell_commands_forward_to_the_right_module(tmp_path, action, args, module):
    script = tmp_path / 'experiments/run.sh'
    script.parent.mkdir()
    shutil.copyfile(Path(__file__).resolve().parents[1] / 'experiments/run.sh', script)
    python = tmp_path / '.venv/bin/python'
    python.parent.mkdir(parents=True)
    python.write_text('#!/usr/bin/python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n')
    python.chmod(0o755)
    cmd = json.loads(subprocess.check_output(['bash', str(script), action, *args], text=True))
    assert cmd[:3] == ['-u', '-m', module]
    assert cmd[-len(args):] == args
    if action == 'ramp':
        assert cmd[cmd.index('--phase-duration') + 1] == '180'
        assert cmd[cmd.index('--slo-ms') + 1] == '150'
        assert cmd[cmd.index('--tc-ms') + 1] == '10'
        assert '--final-hold' in cmd
        assert cmd[cmd.index('--scenario') + 1] == 'scaler-staircase'
        assert cmd[cmd.index('--latency-floor-ms') + 1] == '130'
