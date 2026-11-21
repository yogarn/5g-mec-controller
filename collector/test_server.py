from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
import json
import threading
from urllib.request import urlopen

import pytest
import yaml

import collector.server as collector_server
from collector.server import (
    Collector,
    join_sessions,
    load_settings,
    make_handler,
    map_upf_sessions,
    parse_upf_host,
    validate_settings,
)


def test_window_overflow_and_expiry():
    settings = load_settings('collector/config.yaml')
    now = [0.0]
    collector = Collector(settings, lambda: now[0])
    # Empty window emits no p90 and never emits ue_request_window_complete.
    empty_metrics = collector.metrics()
    assert 'ue_request_window_complete' not in empty_metrics
    assert 'ue_request_duration_p90_seconds' not in empty_metrics
    assert 'ue_request_attempt_duration_p90_seconds' not in empty_metrics
    assert 'ue_request_window_attempts 0\n' in empty_metrics

    for duration, success, status in [(100, True, 200), (800, True, 200), (200, False, 503)]:
        assert collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': duration, 'http_status': status, 'success': success})
    metrics = collector.metrics()
    assert 'ue_request_window_complete' not in metrics
    assert 'ue_request_duration_p90_seconds 0.8\n' in metrics
    assert 'ue_request_attempt_duration_p90_seconds 0.8\n' in metrics
    assert 'ue_request_window_attempts 3\n' in metrics
    assert 'ue_request_window_successes 2\n' in metrics
    assert 'ue_request_window_success_within_slo 1\n' in metrics
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert all('ue_request_window_attempts 3' in value for value in pool.map(lambda _: collector.metrics(), range(50)))
    now[0] = 15
    assert 'ue_request_duration_p90_seconds' not in collector.metrics()
    assert 'ue_request_attempt_duration_p90_seconds' not in collector.metrics()
    assert 'ue_request_window_attempts 0\n' in collector.metrics()

    # Overflow evicts oldest sample, increments overflow counter, and still emits p90 for nonempty window.
    settings['max_observations'] = 1
    collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 1000, 'http_status': 200, 'success': True})
    now[0] = 16
    collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 2000, 'http_status': 200, 'success': True})
    overflow_metrics = collector.metrics()
    assert 'ue_request_window_complete' not in overflow_metrics
    assert 'ue_request_window_attempts 1\n' in overflow_metrics
    assert 'ue_request_duration_p90_seconds 2.0\n' in overflow_metrics
    assert 'ue_request_attempt_duration_p90_seconds 2.0\n' in overflow_metrics
    assert 'ue_telemetry_overflow_evictions_total 1\n' in overflow_metrics

    now[0] = 17
    collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 3000, 'http_status': 200, 'success': True})
    assert 'ue_telemetry_overflow_evictions_total 2\n' in collector.metrics()
    assert 'ue_request_attempt_duration_p90_seconds 3.0\n' in collector.metrics()

    now[0] = 32
    expired_metrics = collector.metrics()
    assert 'ue_request_window_attempts 0\n' in expired_metrics
    assert 'ue_request_duration_p90_seconds' not in expired_metrics
    assert 'ue_request_attempt_duration_p90_seconds' not in expired_metrics
    assert 'ue_telemetry_overflow_evictions_total 2\n' in expired_metrics


def test_join_ambiguity_and_staleness():
    smf = {'items': [{'supi': 'a', 'pdu': [{'ipv4': '10.0.0.1', 'pdu_state': 'active'}]}]}
    amf = {'items': [{'supi': 'a', 'gnb': {'gnb_id': 2}}]}
    assert join_sessions(smf, amf, {2: 'gnb-2'}) == {'10.0.0.1': 'gnb-2'}
    amf['items'].append(amf['items'][0])
    assert join_sessions(smf, amf, {2: 'gnb-2'}) == {}
    settings = load_settings('collector/config.yaml')
    now = [11]
    collector = Collector(settings, lambda: now[0])
    collector.mapping = {'10.0.0.1': 'gnb-2'}
    collector.mapping_time = 0
    collector.ingest({'ue_ip': '10.0.0.1', 'duration_ms': 1000, 'http_status': None, 'success': False})
    assert 'ue_request_window_unassociated 1\n' in collector.metrics()


def test_status_validation_and_failed_response_body():
    collector = Collector(load_settings('collector/config.yaml'))
    sample = {'ue_ip': '127.0.0.1', 'duration_ms': 200, 'http_status': 200, 'success': False}
    assert collector.ingest(sample)  # A response body can fail after a 200 header.
    assert collector.ingest({**sample, 'http_status': None})
    for invalid in ({'http_status': None, 'success': True}, {'http_status': True},
                    {'http_status': 99}, {'http_status': 503, 'success': True},
                    {'duration_ms': float('nan')}):
        assert not collector.ingest({**sample, **invalid})
    assert 'ue_request_window_attempts 2\n' in collector.metrics()
    assert 'ue_request_window_successes 0\n' in collector.metrics()


def test_all_attempt_p90_includes_fast_and_slow_failures():
    collector = Collector(load_settings('collector/config.yaml'))
    assert collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 100,
                             'http_status': 200, 'success': True})
    assert collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 5000,
                             'http_status': 503, 'success': False})
    metrics = collector.metrics()
    assert 'ue_request_duration_p90_seconds 0.1\n' in metrics
    assert 'ue_request_attempt_duration_p90_seconds 5.0\n' in metrics

    collector = Collector(load_settings('collector/config.yaml'))
    for duration in range(1, 10):
        assert collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': duration * 100,
                                 'http_status': 200, 'success': True})
    assert collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 5000,
                             'http_status': 503, 'success': False})
    metrics = collector.metrics()
    assert 'ue_request_duration_p90_seconds 0.9\n' in metrics
    assert 'ue_request_attempt_duration_p90_seconds 0.9\n' in metrics
    assert 'ue_request_window_attempts 10\n' in metrics
    assert 'ue_request_window_successes 9\n' in metrics
    assert 'ue_request_window_success_within_slo 1\n' in metrics

    collector = Collector(load_settings('collector/config.yaml'))
    assert collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 900,
                             'http_status': 200, 'success': True})
    assert collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 10,
                             'http_status': None, 'success': False})
    metrics = collector.metrics()
    assert 'ue_request_attempt_duration_p90_seconds 0.9\n' in metrics
    assert 'ue_request_duration_p90_seconds 0.9\n' in metrics
    for _ in range(9):
        assert collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 10,
                                 'http_status': None, 'success': False})
    metrics = collector.metrics()
    assert 'ue_request_attempt_duration_p90_seconds 0.01\n' in metrics
    assert 'ue_request_duration_p90_seconds 0.9\n' in metrics
    assert 'ue_request_window_attempts 11\n' in metrics
    assert 'ue_request_window_successes 1\n' in metrics
    assert 'ue_request_window_success_within_slo 0\n' in metrics


def test_failure_only_window_reports_actual_attempt_p90():
    now = [0.0]
    collector = Collector(load_settings('collector/config.yaml'), lambda: now[0])
    for duration, status in [(100, None), (800, 503), (200, 200)]:
        assert collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': duration,
                                 'http_status': status, 'success': False})
    metrics = collector.metrics()
    assert '# TYPE ue_request_attempt_duration_p90_seconds gauge\n' in metrics
    assert 'ue_request_attempt_duration_p90_seconds 0.8\n' in metrics
    assert 'ue_request_duration_p90_seconds' not in metrics
    assert 'ue_request_window_attempts 3\n' in metrics
    assert 'ue_request_window_successes 0\n' in metrics
    assert 'ue_request_window_success_within_slo 0\n' in metrics
    now[0] = 15
    assert 'ue_request_attempt_duration_p90_seconds' not in collector.metrics()


def test_sessions_snapshot_and_metrics_before_fresh_and_stale():
    settings = load_settings('collector/config.yaml')
    now = [100.0]
    collector = Collector(settings, lambda: now[0])

    before = collector.sessions_snapshot()
    assert before == {
        'active_sessions': {'gnb1': 0, 'gnb-2': 0, 'gnb-3': 0},
        'total_active_sessions': 0,
        'refresh_age_seconds': None,
        'mapping_max_age_seconds': 5,
        'stale': True,
    }
    metrics_before = collector.metrics()
    assert 'ue_active_sessions{node="gnb-2"} 0\n' in metrics_before
    assert 'ue_session_mapping_age_seconds' not in metrics_before

    collector.mapping = {
        '10.63.1.101': 'gnb-2',
        '10.63.1.102': 'gnb-2',
        '10.64.1.103': 'gnb-3',
    }
    collector.mapping_time = 100.0
    now[0] = 102.5

    fresh = collector.sessions_snapshot()
    assert fresh == {
        'active_sessions': {'gnb1': 0, 'gnb-2': 2, 'gnb-3': 1},
        'total_active_sessions': 3,
        'refresh_age_seconds': 2.5,
        'mapping_max_age_seconds': 5,
        'stale': False,
    }
    metrics_fresh = collector.metrics()
    assert 'ue_active_sessions{node="gnb1"} 0\n' in metrics_fresh
    assert 'ue_active_sessions{node="gnb-2"} 2\n' in metrics_fresh
    assert 'ue_active_sessions{node="gnb-3"} 1\n' in metrics_fresh
    assert 'ue_session_mapping_age_seconds 2.5\n' in metrics_fresh

    now[0] = 111.0
    stale = collector.sessions_snapshot()
    assert stale['active_sessions'] == {'gnb1': 0, 'gnb-2': 2, 'gnb-3': 1}
    assert stale['refresh_age_seconds'] == 11.0
    assert stale['stale'] is True


def test_sessions_http_endpoint_returns_aggregate_counts_without_ue_identifiers():
    settings = load_settings('collector/config.yaml')
    now = [50.0]
    collector = Collector(settings, lambda: now[0])
    collector.mapping = {'10.63.1.101': 'gnb-2', '10.64.1.102': 'gnb-3'}
    collector.mapping_time = 48.25

    server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(collector, 'secret-token'))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urlopen(f'http://127.0.0.1:{port}/sessions', timeout=2) as resp:
            assert resp.status == 200
            assert resp.headers.get_content_type() == 'application/json'
            raw = resp.read().decode('utf-8')
        assert '10.63.1.101' not in raw and '10.64.1.102' not in raw
        payload = json.loads(raw)
        assert payload == {
            'active_sessions': {'gnb1': 0, 'gnb-2': 1, 'gnb-3': 1},
            'total_active_sessions': 2,
            'refresh_age_seconds': 1.75,
            'mapping_max_age_seconds': 5,
            'stale': False,
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_placement_window_5s_cutoff_and_endpoint():
    settings = load_settings('collector/config.yaml')
    assert settings['window_seconds'] == 15
    assert settings['placement_window_seconds'] == 5
    now = [0.0]
    collector = Collector(settings, lambda: now[0])
    collector.mapping = {'10.62.0.1': 'gnb-2', '10.63.0.1': 'gnb-3'}
    collector.mapping_time = 0.0

    # Ingest at t=0.0 (gnb-2), t=2.0 (gnb-3), and t=3.0 (unassociated IP).
    assert collector.ingest({'ue_ip': '10.62.0.1', 'duration_ms': 50, 'http_status': 200, 'success': True})
    now[0] = 2.0
    assert collector.ingest({'ue_ip': '10.63.0.1', 'duration_ms': 60, 'http_status': 200, 'success': True})
    now[0] = 3.0
    assert collector.ingest({'ue_ip': '192.0.2.9', 'duration_ms': 70, 'http_status': 200, 'success': True})

    # At t=4.999, t=0.0 sample is still inside the 5s window; gnb1 is exported as 0.
    now[0] = 4.999
    pm = collector.placement_metrics()
    assert '# TYPE ue_request_node_placement_window_attempts gauge\n' in pm
    assert 'ue_request_node_placement_window_attempts{node="gnb1"} 0\n' in pm
    assert 'ue_request_node_placement_window_attempts{node="gnb-2"} 1\n' in pm
    assert 'ue_request_node_placement_window_attempts{node="gnb-3"} 1\n' in pm
    assert 'ue_request_attempt_duration_p90_seconds' not in pm

    # At t=5.0, t=0.0 sample hits the exact 5s cutoff and drops out of placement_metrics,
    # while remaining in the 15s HPA/telemetry metrics().
    now[0] = 5.0
    pm_cutoff = collector.placement_metrics()
    assert 'ue_request_node_placement_window_attempts{node="gnb1"} 0\n' in pm_cutoff
    assert 'ue_request_node_placement_window_attempts{node="gnb-2"} 0\n' in pm_cutoff
    assert 'ue_request_node_placement_window_attempts{node="gnb-3"} 1\n' in pm_cutoff
    full_metrics = collector.metrics()
    assert 'ue_request_node_window_attempts{node="gnb-2"} 1\n' in full_metrics
    assert 'ue_request_node_window_attempts{node="gnb-3"} 1\n' in full_metrics

    # Verify lightweight /metrics/placement HTTP endpoint.
    server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(collector, 'secret-token'))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urlopen(f'http://127.0.0.1:{port}/metrics/placement', timeout=2) as resp:
            assert resp.status == 200
            body = resp.read().decode('utf-8')
        assert body == pm_cutoff
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_parse_upf_host():
    assert parse_upf_host('[172.16.20.70]:2152') == '172.16.20.70'
    assert parse_upf_host('[172.16.30.11]') == '172.16.30.11'
    assert parse_upf_host('172.16.20.200:2152') == '172.16.20.200'
    assert parse_upf_host('172.16.20.200') == '172.16.20.200'
    assert parse_upf_host('[2001:0db8::1]:2152') == '2001:db8::1'
    for bad in (
        None,
        123,
        '',
        ' [172.16.20.70]:2152',
        '[172.16.20.70]:2152 ',
        '[172.16.20.70',
        '[172.16.20.70]2152',
        '[172.16.20.70]:',
        '[172.16.20.70]:0',
        '[172.16.20.70]:65536',
        '[172.16.20.70]:abc',
        '[172.16.20.70]:' + '9' * 5000,
        '[999.16.20.70]:2152',
        'not-an-ip:2152',
    ):
        assert parse_upf_host(bad) is None


def test_map_upf_sessions_unknown_inactive_and_duplicate_ue_ips():
    upf_nodes = {
        '172.16.30.11': 'gnb1',
        '172.16.20.70': 'gnb-2',
        '172.16.20.200': 'gnb-3',
    }
    smf = {
        'items': [
            {
                'supi': 'imsi-001',
                'pdu': [
                    {
                        'ipv4': '10.61.0.1',
                        'pdu_state': 'active',
                        'n3': {'upf': {'addr': '[172.16.30.11]:2152'}},
                    },
                    {
                        'ipv4': '10.61.0.2',
                        'pdu_state': 'inactive',
                        'n3': {'upf': {'addr': '[172.16.20.70]:2152'}},
                    },
                    {
                        'ipv4': '10.61.0.3',
                        'pdu_state': 'active',
                        'n3': {'upf': {'addr': '[172.16.99.99]:2152'}},
                    },
                    {
                        'ipv4': '10.61.0.4',
                        'pdu_state': 'active',
                        'n3': {'upf': {'addr': '[bad-ip]:2152'}},
                    },
                ],
            },
            {
                'supi': 'imsi-002',
                'pdu': [
                    {
                        'ipv4': '10.61.0.5',
                        'pdu_state': 'active',
                        'n3': {'upf': {'addr': '[172.16.20.70]:2152'}},
                    },
                    {
                        'ipv4': '10.61.0.6',
                        'pdu_state': 'inactive',
                        'n3': {'upf': {'addr': '[172.16.30.11]:2152'}},
                    },
                ],
            },
            {
                'supi': 'imsi-003',
                'pdu': [
                    {
                        'ipv4': '10.61.0.5',
                        'pdu_state': 'active',
                        'n3': {'upf': {'addr': '[172.16.20.200]:2152'}},
                    },
                    {
                        'ipv4': '10.61.0.6',
                        'pdu_state': 'active',
                        'n3': {'upf': {'addr': '[172.16.20.200]:2152'}},
                    },
                ],
            },
        ]
    }
    assert map_upf_sessions(smf, upf_nodes) == {
        '10.61.0.1': 'gnb1',
        '10.61.0.6': 'gnb-3',
    }


def test_upf_placement_metrics_5s_cutoff_zeros_unknown_and_stale():
    settings = load_settings('collector/config.yaml')
    now = [0.0]
    collector = Collector(settings, lambda: now[0])
    collector.mapping = {'10.62.0.1': 'gnb-2', '10.63.0.1': 'gnb-3'}
    collector.upf_mapping = {'10.62.0.1': 'gnb-2'}
    collector.mapping_time = 0.0
    collector.upf_mapping_time = 0.0

    # t=0.0: mapped to gnb-2 on both gNB and UPF.
    assert collector.ingest({'ue_ip': '10.62.0.1', 'duration_ms': 40, 'http_status': 200, 'success': True})
    # t=2.0: mapped to gnb-3 on gNB, but unknown UPF -> UPF-unassociated.
    now[0] = 2.0
    assert collector.ingest({'ue_ip': '10.63.0.1', 'duration_ms': 50, 'http_status': 503, 'success': False})
    # t=3.0: completely unknown UE IP -> UPF-unassociated.
    now[0] = 3.0
    assert collector.ingest({'ue_ip': '192.0.2.9', 'duration_ms': 60, 'http_status': 200, 'success': True})

    now[0] = 4.999
    pm = collector.placement_metrics()
    assert '# TYPE ue_request_upf_placement_window_attempts gauge\n' in pm
    assert 'ue_request_upf_placement_window_attempts{node="gnb1"} 0\n' in pm
    assert 'ue_request_upf_placement_window_attempts{node="gnb-2"} 1\n' in pm
    assert 'ue_request_upf_placement_window_attempts{node="gnb-3"} 0\n' in pm
    assert '# TYPE ue_request_upf_placement_window_unassociated gauge\n' in pm
    assert 'ue_request_upf_placement_window_unassociated 2\n' in pm

    # At t=5.0, the t=0.0 attempt drops out of the 5s placement window.
    now[0] = 5.0
    pm_cutoff = collector.placement_metrics()
    assert 'ue_request_upf_placement_window_attempts{node="gnb1"} 0\n' in pm_cutoff
    assert 'ue_request_upf_placement_window_attempts{node="gnb-2"} 0\n' in pm_cutoff
    assert 'ue_request_upf_placement_window_attempts{node="gnb-3"} 0\n' in pm_cutoff
    assert 'ue_request_upf_placement_window_unassociated 2\n' in pm_cutoff

    # At t=11.0, mapping_time=0.0 is stale (> 10s), so a known UE IP is UPF-unassociated.
    now[0] = 11.0
    assert collector.ingest({'ue_ip': '10.62.0.1', 'duration_ms': 45, 'http_status': 200, 'success': True})
    pm_stale = collector.placement_metrics()
    assert 'ue_request_upf_placement_window_attempts{node="gnb-2"} 0\n' in pm_stale
    assert 'ue_request_upf_placement_window_unassociated 1\n' in pm_stale


def test_ue_stays_on_gnb1_but_switches_to_core_upf(monkeypatch):
    settings = load_settings('collector/config.yaml')
    now = [0.0]
    collector = Collector(settings, lambda: now[0])

    current_smf = {
        'items': [
            {
                'supi': 'imsi-001010000000001',
                'pdu': [
                    {
                        'ipv4': '10.61.0.10',
                        'pdu_state': 'active',
                        'n3': {'upf': {'addr': '[172.16.30.11]:2152'}},
                    }
                ],
            }
        ]
    }
    current_amf = {
        'items': [
            {
                'supi': 'imsi-001010000000001',
                'gnb': {'gnb_id': 0},
            }
        ]
    }

    def fake_fetch(url):
        if url == settings['smf_url']:
            return current_smf
        if url == settings['amf_url']:
            return current_amf
        raise AssertionError(f'unexpected url {url}')

    monkeypatch.setattr(collector_server, 'fetch', fake_fetch)

    collector.refresh()
    assert collector.ingest({'ue_ip': '10.61.0.10', 'duration_ms': 30, 'http_status': 200, 'success': True})
    now[0] = 1.0
    assert collector.ingest({'ue_ip': '10.61.0.10', 'duration_ms': 35, 'http_status': 200, 'success': True})

    # UE stays attached to gnb1, but its active PDU session switches to the core UPF.
    now[0] = 2.0
    current_smf['items'][0]['pdu'][0]['n3']['upf']['addr'] = '[10.34.211.140]:2152'
    collector.refresh()
    for ts in (2.5, 3.0, 3.5):
        now[0] = ts
        assert collector.ingest({'ue_ip': '10.61.0.10', 'duration_ms': 40, 'http_status': 200, 'success': True})

    now[0] = 4.0
    pm = collector.placement_metrics()
    assert 'ue_request_upf_placement_window_attempts{node="gnb1"} 2\n' in pm
    assert 'ue_request_upf_placement_window_attempts{node="core-5g"} 3\n' in pm
    assert 'ue_request_upf_placement_window_attempts{node="gnb-2"} 0\n' in pm
    assert 'ue_request_upf_placement_window_attempts{node="gnb-3"} 0\n' in pm
    assert 'ue_request_upf_placement_window_unassociated 0\n' in pm
    assert 'ue_request_node_placement_window_attempts{node="gnb1"} 5\n' in pm
    assert 'ue_request_node_placement_window_attempts{node="gnb-2"} 0\n' in pm
    assert 'ue_request_node_placement_window_attempts{node="gnb-3"} 0\n' in pm


def test_upf_refresh_survives_amf_failure(monkeypatch):
    settings = load_settings('collector/config.yaml')
    collector = Collector(settings, lambda: 12.0)
    collector.mapping = {'10.62.0.1': 'gnb1'}
    collector.mapping_time = 0.0
    smf = {'items': [{'pdu': [{
        'ipv4': '10.62.0.1',
        'pdu_state': 'active',
        'n3': {'upf': {'addr': '[172.16.20.70]:2152'}},
    }]}]}

    def fake_fetch(url):
        if url == settings['amf_url']:
            raise TimeoutError('AMF unavailable')
        return smf

    monkeypatch.setattr(collector_server, 'fetch', fake_fetch)
    with pytest.raises(TimeoutError):
        collector.refresh()

    assert collector.ingest({'ue_ip': '10.62.0.1', 'duration_ms': 40, 'http_status': 200, 'success': True})
    metrics = collector.placement_metrics()
    assert 'ue_request_upf_placement_window_attempts{node="gnb-2"} 1\n' in metrics
    assert 'ue_request_upf_placement_window_unassociated 0\n' in metrics
    assert 'ue_request_node_placement_window_attempts{node="gnb1"} 0\n' in metrics
    assert collector.sessions_snapshot()['stale'] is True


def test_config_and_manifest_upf_nodes_validation():
    expected_upf_nodes = {
        '10.34.211.140': 'core-5g',
        '172.16.30.11': 'gnb1',
        '172.16.20.70': 'gnb-2',
        '172.16.20.200': 'gnb-3',
    }
    config_settings = load_settings('collector/config.yaml')
    assert config_settings['upf_nodes'] == expected_upf_nodes

    with open('collector/manifest.yaml', encoding='utf-8') as stream:
        docs = list(yaml.safe_load_all(stream))
    configmap = next(doc for doc in docs if doc['kind'] == 'ConfigMap' and doc['metadata']['name'] == 'ue-telemetry-config')
    manifest_settings = validate_settings(yaml.safe_load(configmap['data']['config.yaml']))
    assert manifest_settings['upf_nodes'] == expected_upf_nodes

    legacy = dict(config_settings)
    del legacy['upf_nodes']
    assert validate_settings(legacy)['upf_nodes'] == {}

    for bad_upf_nodes in (None, {'not-an-ip': 'gnb1'}, {'172.16.30.11': 'bad node!'}, {123: 'gnb1'}):
        bad = dict(config_settings)
        bad['upf_nodes'] = bad_upf_nodes
        with pytest.raises(ValueError):
            validate_settings(bad)
