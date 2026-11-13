from concurrent.futures import ThreadPoolExecutor

from collector.server import Collector, join_sessions, load_settings


def test_window_overflow_and_expiry():
    settings = load_settings('collector/config.yaml')
    now = [0.0]
    collector = Collector(settings, lambda: now[0])
    for duration, success, status in [(100, True, 200), (800, True, 200), (200, False, 503)]:
        assert collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': duration, 'http_status': status, 'success': success})
    metrics = collector.metrics()
    assert 'ue_request_duration_p90_seconds 0.8\n' in metrics
    assert 'ue_request_window_attempts 3\n' in metrics
    assert 'ue_request_window_successes 2\n' in metrics
    assert 'ue_request_window_success_within_slo 1\n' in metrics
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert all('ue_request_window_attempts 3' in value for value in pool.map(lambda _: collector.metrics(), range(50)))
    now[0] = 15
    assert 'ue_request_duration_p90_seconds' not in collector.metrics()
    assert 'ue_request_window_attempts 0\n' in collector.metrics()
    settings['max_observations'] = 1
    collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 1000, 'http_status': 200, 'success': True})
    now[0] = 16
    collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 2000, 'http_status': 200, 'success': True})
    assert 'ue_request_window_complete 0\n' in collector.metrics()
    assert 'ue_request_duration_p90_seconds' not in collector.metrics()
    assert 'ue_telemetry_overflow_evictions_total 1\n' in collector.metrics()
    now[0] = 30
    assert 'ue_request_window_complete 1\n' in collector.metrics()


    now[0] = 31
    collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 3000, 'http_status': 200, 'success': True})
    now[0] = 32
    collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 4000, 'http_status': 200, 'success': True})
    now[0] = 33
    collector.ingest({'ue_ip': '127.0.0.1', 'duration_ms': 5000, 'http_status': 200, 'success': True})
    now[0] = 46
    assert 'ue_request_window_complete 0\n' in collector.metrics()
    now[0] = 47
    assert 'ue_request_window_complete 1\n' in collector.metrics()


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
