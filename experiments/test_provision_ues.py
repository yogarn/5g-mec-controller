import copy
import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml

from experiments.provision_ues import build_documents, main, provision


class Collection:
    def __init__(self, records):
        self.records = records
        self.inserted = []

    def find(self, query, projection):
        return self.records

    def insert_many(self, docs, ordered):
        assert ordered
        self.inserted.extend(copy.deepcopy(docs))


def inventory(tmp_path):
    base = {'supi': 'imsi-001015000000001', 'key': 'a' * 32, 'op': 'b' * 32,
            'opType': 'OPC', 'amf': '8000',
            'sessions': [{'type': 'IPv4', 'apn': 'site2.testbed', 'slice': {'sst': 1}}]}
    pair = {}
    for site, dnn, ip in [('gnb2', 'site2.testbed', '10.63.1.101'),
                          ('gnb3', 'site3.testbed', '10.64.1.101')]:
        config = copy.deepcopy(base)
        config['sessions'][0]['apn'] = dnn
        path = tmp_path / f'{site}.yaml'
        path.write_text(yaml.safe_dump(config))
        pair[site] = {'config': path.name, 'ip': ip}
    path = tmp_path / 'inventory.yaml'
    path.write_text(yaml.safe_dump({'subscribers': [pair]}))
    return path


def test_check_and_insert_static_sessions_without_secret_output(tmp_path):
    docs = build_documents(inventory(tmp_path))
    collection = Collection([])
    checked = provision(collection, docs)
    assert not collection.inserted
    assert checked[0]['sessions'] == [{'dnn': 'site2.testbed', 'ipv4': '10.63.1.101'},
                                      {'dnn': 'site3.testbed', 'ipv4': '10.64.1.101'}]
    assert 'security' not in checked[0]
    assert provision(collection, docs, apply=True) == checked
    assert collection.inserted[0]['security']['k'] == 'a' * 32
    assert collection.inserted[0]['slice'][0]['session'][1]['ue']['ipv4'] == '10.64.1.101'


@pytest.mark.parametrize('existing', [
    {'imsi': '001015000000001'},
    {'imsi': 'different', 'slice': [{'session': [{'ue': {'ipv4': '10.64.1.101'}}]}]},
])
def test_existing_imsi_or_ip_never_overwritten(tmp_path, existing):
    collection = Collection([existing])
    with pytest.raises(ValueError, match='conflict'):
        provision(collection, build_documents(inventory(tmp_path)), apply=True)
    assert not collection.inserted


def test_mismatched_authentication_is_rejected(tmp_path):
    path = inventory(tmp_path)
    other = tmp_path / 'gnb3.yaml'
    config = yaml.safe_load(other.read_text())
    config['key'] = 'c' * 32
    other.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match='authentication'):
        build_documents(path)


@pytest.mark.parametrize('apply', [False, True])
@pytest.mark.parametrize('mode', ['default', 'verbose', 'json'])
def test_cli_output_is_readable_or_json_without_secrets(tmp_path, monkeypatch, capsys, apply, mode):
    collection = Collection([])
    client = MagicMock()
    client.__enter__.return_value = client
    client.__getitem__.return_value.__getitem__.return_value = collection
    monkeypatch.setitem(sys.modules, 'pymongo', SimpleNamespace(MongoClient=lambda *a, **kw: client))
    args = ['--inventory', str(inventory(tmp_path))]
    if apply:
        args.append('--apply')
    if mode != 'default':
        args.append(f'--{mode}')
    main(args)
    output = capsys.readouterr().out
    action = 'inserted' if apply else 'check-only'
    if mode == 'default':
        assert '001015000000001' not in output
        assert 'site2.testbed' not in output and '10.63.1.101' not in output
    else:
        assert ('001015000000001' in output and 'site2.testbed' in output
                and '10.63.1.101' in output and 'site3.testbed' in output
                and '10.64.1.101' in output)
    assert 'a' * 32 not in output and 'b' * 32 not in output and '8000' not in output
    assert bool(collection.inserted) == apply
    if mode == 'json':
        result = json.loads(output)
        assert result['action'] == action
        assert result['subscribers'][0]['sst'] == 1
    else:
        assert f'Provisioning {action}: 1 subscriber(s)' in output
        if mode == 'verbose':
            assert 'IMSI 001015000000001' in output
