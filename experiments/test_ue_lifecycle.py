import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from experiments import ue_lifecycle as ue


def make_config(tmp_path, pairs):
    subscribers = []
    for index, (supi2, supi3, ip2, ip3) in enumerate(pairs):
        entry = {}
        for site, supi, ip in [('gnb1', supi2, f'10.2.0.{index + 2}'),
                               ('gnb2', supi2, ip2), ('gnb3', supi3, ip3)]:
            name = f'{index}-{site}.yaml'
            (tmp_path / name).write_text(yaml.safe_dump({'supi': supi}))
            entry[site] = {'config': name, 'ip': ip}
        subscribers.append(entry)
    path = tmp_path / 'pool.yaml'
    path.write_text(yaml.safe_dump({'subscribers': subscribers}))
    return path


def test_load_config_resolves_relative_paths(tmp_path):
    path = make_config(tmp_path, [('imsi-123', 'imsi-123', '10.0.0.2', '10.0.1.2')])
    subscribers = ue._load_config(path)
    assert subscribers[0][0] == {'config': str(tmp_path / '0-gnb1.yaml'), 'ip': '10.2.0.2'}
    assert subscribers[0][1]['ip'] == '10.0.0.2'
    assert subscribers[0][2]['ip'] == '10.0.1.2'


@pytest.mark.parametrize('pairs', [
    [('a', 'b', '10.0.0.2', '10.0.1.2')],
    [('a', 'a', '10.0.0.2', '10.0.0.2')],
    [('a', 'a', '10.0.0.2', '10.0.1.2'), ('a', 'a', '10.0.0.3', '10.0.1.3')],
    [('a', 'a', 'not-an-ip', '10.0.1.2')],
])
def test_invalid_pool_rejected(tmp_path, pairs):
    with pytest.raises(ValueError):
        ue._load_config(make_config(tmp_path, pairs))


def test_third_site_identity_and_address_must_match_inventory_contract(tmp_path):
    path = make_config(tmp_path, [('a', 'a', '10.0.0.2', '10.0.1.2')])
    site = tmp_path / '0-gnb1.yaml'
    site.write_text(yaml.safe_dump({'supi': 'different'}))
    with pytest.raises(ValueError, match='SUPIs'):
        ue._load_config(path)
    site.write_text(yaml.safe_dump({'supi': 'a'}))
    data = yaml.safe_load(path.read_text())
    data['subscribers'][0]['gnb1']['ip'] = '10.0.1.2'
    path.write_text(yaml.safe_dump(data))
    with pytest.raises(ValueError, match='distinct'):
        ue._load_config(path)


def test_two_site_inventory_rejected_for_new_runs(tmp_path):
    path = make_config(tmp_path, [('a', 'a', '10.0.0.2', '10.0.1.2')])
    data = yaml.safe_load(path.read_text())
    del data['subscribers'][0]['gnb1']
    path.write_text(yaml.safe_dump(data))
    with pytest.raises(ValueError, match='gnb1'):
        ue._load_config(path)


def test_missing_root_fails_before_state_creation(tmp_path):
    with patch.object(ue.os, 'geteuid', return_value=1000):
        with pytest.raises(RuntimeError, match='root'):
            ue.OwnedUELifecycle(tmp_path / 'state', tmp_path / 'missing', Path('/missing'))
    assert not (tmp_path / 'state').exists()


def test_recovery_rejects_incomplete_journal(tmp_path):
    journal = tmp_path / 'lifecycle.jsonl'
    journal.write_text('{"event":"created"}')
    with pytest.raises(RuntimeError, match='incomplete'):
        ue._records(journal)


def test_recovery_rejects_unowned_route(tmp_path):
    journal = tmp_path / 'lifecycle.jsonl'
    rows = [{'event': 'created', 'launch_protocol': ue._PROTOCOL},
            {'event': 'route_intent', 'ip': '10.0.0.2', 'iface': 'uesimtun0', 'table': 20000}]
    journal.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    with pytest.raises(RuntimeError, match='ambiguous'):
        ue._records(journal)


def test_route_cleanup_refuses_foreign_entries():
    with patch.object(ue, '_ip', side_effect=[[], [{'table': 20000, 'dst': 'default', 'dev': 'foreign0'}]]), patch.object(ue, '_run') as run:
        with pytest.raises(RuntimeError, match='foreign route'):
            ue._clean_routes({20000: ('10.0.0.2', 'uesimtun0')})
        run.assert_not_called()


def test_pid_reuse_is_not_signalled():
    with patch.object(ue.os, 'pidfd_open', return_value=123), patch.object(ue.os, 'close'), patch.object(ue, '_identity', return_value=(11, 'boot')), patch.object(ue.signal, 'pidfd_send_signal') as send:
        ue._stop_process(42, 10, 'boot')
        send.assert_not_called()


def test_foreign_source_rule_is_not_removed():
    rule = {'src': '10.0.0.2', 'table': 'foreign', 'priority': 10}
    with patch.object(ue, '_named_rules', return_value=[rule]), patch.object(ue, '_run') as run:
        with pytest.raises(RuntimeError, match='ambiguous'):
            ue._clean_ue_rules({'10.0.0.2'})
        run.assert_not_called()
