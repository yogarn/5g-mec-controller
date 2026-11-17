"""Response evidence and application failures must survive the workload pipeline."""
import http.client
import json
from urllib.parse import urlsplit

import pytest

from benchmark import client
from benchmark.telemetry import request_with_telemetry
from collector.server import Collector, load_settings


def result(monkeypatch, raw, status=200, **options):
    class Response:
        def getheader(self, name):
            return 'application/json'

        def read(self):
            if isinstance(raw, Exception):
                raise raw
            return raw

    Response.status = status

    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, *args, **kwargs):
            pass

        def getresponse(self):
            return Response()

        def close(self):
            pass

    monkeypatch.setattr(client.http.client, 'HTTPConnection', Connection)
    return client.attempt(urlsplit('http://detector/detect'), 'POST', b'image', {},
                          '127.0.0.1', 2, response_contract='detection', **options)


@pytest.mark.parametrize('payload', [
    {'person_count': 0, 'persons': []},
    {'person_count': 1, 'persons': [{'x': 0, 'y': 2, 'w': 30, 'h': 40, 'confidence': .91}],
     'model_sha256': 'model', 'input_size': 640, 'server_node': 'gnb-1', 'inference_ms': 12},
    {'face_count': 1, 'faces': [{'x': 1, 'y': 2, 'w': 3, 'h': 4}]},
])
def test_detection_payload_is_logged(monkeypatch, payload):
    raw = json.dumps(payload).encode()
    record = result(monkeypatch, raw)
    assert record['success'] and record['http_success'] and record['response_valid']
    assert record['response_json'] == payload
    assert record['response_body'] == raw.decode()
    assert record['response_size_bytes'] == len(raw)
    assert record['response_content_type'] == 'application/json'
    assert record['response_validation_error'] is None
    json.dumps(record, allow_nan=False)


@pytest.mark.parametrize('raw', [
    b'', b'not json', b'\xff', b'null', b'[]', b'{}',
    b'{"error":"inference failed"}',
    b'{"person_count":true,"persons":[]}',
    b'{"person_count":1,"persons":[]}',
    b'{"person_count":0,"persons":null}',
    b'{"person_count":1,"persons":[null]}',
    b'{"person_count":1,"persons":[{"x":0,"y":0,"w":0,"h":2,"confidence":0.5}]}',
    b'{"person_count":1,"persons":[{"x":0,"y":0,"w":2,"h":2,"confidence":2}]}',
    b'{"person_count":0,"persons":[],"processing_ms":NaN}',
    b'{"person_count":0,"persons":[],"processing_ms":1e999}',
])
def test_http_200_invalid_output_is_failure(monkeypatch, raw):
    record = result(monkeypatch, raw)
    assert record['http_success'] and not record['success'] and not record['response_valid']
    assert record['transport_error'] is None
    assert record['error'].startswith('ResponseValidationError:')
    assert record['response_body'] == raw.decode(errors='replace')
    json.dumps(record, allow_nan=False)


@pytest.mark.parametrize('raw', [b'{"error":"bad image"}', b'<html>proxy error</html>'])
def test_http_error_body_is_preserved(monkeypatch, raw):
    record = result(monkeypatch, raw, status=503)
    assert not record['http_success'] and not record['success']
    assert record['response_body'] == raw.decode()
    assert record['http_status'] == 503


def test_oversized_and_partial_responses_are_explicit(monkeypatch):
    raw = b'x' * (client.RESPONSE_LOG_LIMIT + 1)
    record = result(monkeypatch, raw)
    assert not record['success'] and record['response_body_truncated']
    assert len(record['response_body']) == client.RESPONSE_LOG_LIMIT
    assert record['response_size_bytes'] == len(raw)
    partial = result(monkeypatch, http.client.IncompleteRead(b'{"person_count":', 100))
    assert not partial['http_success'] and not partial['success']
    assert partial['response_body'] == '{"person_count":'
    assert partial['transport_error'].startswith('IncompleteRead:')


def test_validation_precedes_telemetry_and_counts_as_collector_failure(monkeypatch):
    record = result(monkeypatch, b'{}')
    collector = Collector(load_settings('collector/config.yaml'))

    class Publisher:
        def submit(self, source, observation):
            assert not observation['success']
            assert collector.ingest({'ue_ip': source, **{
                k: observation[k] for k in ('duration_ms', 'http_status', 'success')}})

    assert request_with_telemetry(lambda *args, **kwargs: record, Publisher(),
                                  None, 'POST', b'', {}, '127.0.0.1', 1) is record
    assert 'ue_request_window_successes 0\n' in collector.metrics()


def test_parsing_does_not_enter_network_duration(monkeypatch):
    clock = iter([20.0, 20.025])
    monkeypatch.setattr(client.time, 'monotonic', lambda: next(clock))
    original = client.json.loads

    def parse(*args, **kwargs):
        # Both timer reads must have happened before parsing starts.
        with pytest.raises(StopIteration):
            next(clock)
        return original(*args, **kwargs)

    monkeypatch.setattr(client.json, 'loads', parse)
    record = result(monkeypatch, b'{"person_count":0,"persons":[]}')
    assert record['duration_ms'] == pytest.approx(25)
