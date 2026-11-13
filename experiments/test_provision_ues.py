import copy

import pytest
import yaml

from experiments.provision_ues import build_documents, provision


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
