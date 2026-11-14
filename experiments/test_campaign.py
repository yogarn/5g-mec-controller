import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

import pytest
import yaml

from experiments import campaign


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
