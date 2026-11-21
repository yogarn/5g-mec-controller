import argparse
import http.server
import json
import threading

import pytest

from benchmark import client


def options(tmp_path, **overrides):
    values = dict(url='http://127.0.0.1:12345/path?q=1', method='POST', body='hello',
                  header=['Content-Type: text/plain'], phase_schedule='1:1:0:0,1:0:1:0,1:0:0:1',
                  gnb1_addresses='127.0.0.3', gnb2_addresses='127.0.0.1', gnb3_addresses='127.0.0.2',
                  phase_duration=0.03, timeout=1.0, output=str(tmp_path / 'results.jsonl'))
    values.update(overrides)
    return argparse.Namespace(**values)


def test_schedule_order_and_validation():
    phases = client.parse_schedule('3:1:1:1,3:0:0:3')
    assert [p['phase'] for p in phases] == ['phase_01', 'phase_02']
    assert [p['gnb3_ues'] for p in phases] == [1, 3]
    for value in ('', '1:1', '2:1:0', '2:1:0:0', '0:0:0:0', '1:-1:1:1', '1:1:0:0,'):
        with pytest.raises(ValueError):
            client.parse_schedule(value)


@pytest.mark.parametrize('changes', [
    {'gnb1_addresses': ''}, {'gnb2_addresses': ''}, {'gnb3_addresses': '127.0.0.1'},
    {'gnb1_addresses': '127.0.0.2'},
    {'gnb2_addresses': '0.0.0.0'}, {'phase_duration': float('nan')},
    {'timeout': 0}, {'url': 'ftp://example.com'}, {'url': 'http://example.com:bad'},
    {'method': 'GET\n'}, {'header': ['Bad Header: x']},
    {'header': ['X: a', 'x: b']}, {'header': ['X: a\r\nb']},
])
def test_invalid_configuration(tmp_path, changes):
    with pytest.raises(ValueError):
        client.validate(options(tmp_path, **changes))
    assert not (tmp_path / 'results.jsonl').exists()


def test_closed_loop_records_and_binding(tmp_path):
    received = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers['Content-Length']))
            received.append((self.client_address[0], self.path, body, self.headers['Content-Type']))
            self.send_response(503 if self.client_address[0] == '127.0.0.2' else 201)
            self.end_headers()
            self.wfile.write(b'ok')

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        args = options(tmp_path, url=f'http://127.0.0.1:{server.server_port}/path?q=1')
        client.run(args)
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
    records = [json.loads(line) for line in (tmp_path / 'results.jsonl').read_text().splitlines()]
    starts = [r for r in records if r['event'] == 'phase_start']
    ends = [r for r in records if r['event'] == 'phase_end']
    requests = [r for r in records if r['event'] == 'request']
    assert [r['phase'] for r in starts] == ['phase_01', 'phase_02', 'phase_03']
    assert [r['phase'] for r in ends] == ['phase_01', 'phase_02', 'phase_03']
    assert records.index(ends[0]) < records.index(starts[1])
    assert {(r['group'], r['source_address']) for r in requests} == {
        ('gnb-1', '127.0.0.3'), ('gnb-2', '127.0.0.1'), ('gnb-3', '127.0.0.2')}
    assert all(r['duration_ms'] >= 0 and r['error'] is None for r in requests)
    assert all(r['success'] == (r['http_status'] == 201) for r in requests)
    assert all(path == '/path?q=1' and body == b'hello' and header == 'text/plain'
               for _, path, body, header in received)
    assert sum(r['attempts'] for r in ends) == len(requests) == len(received)
    assert all(r['per_ue'][0]['attempts'] == r['attempts'] for r in ends)


def test_failed_attempt_and_exclusive_output(tmp_path):
    args = options(tmp_path, phase_schedule='1:0:1:0', gnb3_addresses='',
                   url='http://127.0.0.1:1/', phase_duration=0.01)
    client.run(args)
    path = tmp_path / 'results.jsonl'
    original = path.read_text()
    records = [json.loads(line) for line in original.splitlines()]
    requests = [r for r in records if r['event'] == 'request']
    assert requests and all(not r['success'] and r['error'] for r in requests)
    assert all(r['http_status'] is None for r in requests)
    with pytest.raises(FileExistsError):
        client.run(args)
    assert path.read_text() == original


def test_attempt_records_server_timings_and_nulls_on_error_or_malformed_json():
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.parse import urlsplit

    seen_ids = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            seen_ids.append(self.headers.get('X-Request-ID'))
            if self.path == '/ok':
                body = json.dumps({
                    'request_id': self.headers.get('X-Request-ID'),
                    'server_pod': 'pod-a',
                    'server_node': 'gnb-3',
                    'processing_ms': 12.5,
                    'body_read_ms': 1.0,
                    'decode_ms': 2.0,
                    'lock_wait_ms': 0.5,
                    'inference_ms': 9.0,
                }).encode('utf-8')
                self.send_response(200)
            elif self.path == '/bad-json':
                body = b'not-json{'
                self.send_response(200)
            else:
                body = json.dumps({
                    'server_pod': 'pod-a',
                    'processing_ms': 99.0,
                }).encode('utf-8')
                self.send_response(500)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f'http://127.0.0.1:{server.server_port}'
        ok = client.attempt(urlsplit(base + '/ok'), 'POST', b'x', {}, '127.0.0.1', 2)
        assert ok['success'] is True
        assert ok['request_id'] == seen_ids[0] and ok['request_id']
        assert ok['server_pod'] == 'pod-a'
        assert ok['server_node'] == 'gnb-3'
        assert ok['server_processing_ms'] == 12.5
        assert ok['server_body_read_ms'] == 1.0
        assert ok['server_decode_ms'] == 2.0
        assert ok['server_lock_wait_ms'] == 0.5
        assert ok['server_inference_ms'] == 9.0

        bad = client.attempt(urlsplit(base + '/bad-json'), 'POST', b'x', {}, '127.0.0.1', 2)
        assert bad['success'] is True
        assert bad['server_pod'] is None
        assert bad['server_processing_ms'] is None
        assert bad['server_lock_wait_ms'] is None

        err = client.attempt(urlsplit(base + '/err'), 'POST', b'x', {}, '127.0.0.1', 2)
        assert err['success'] is False
        assert err['http_status'] == 500
        assert err['server_pod'] is None
        assert err['server_processing_ms'] is None
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.mark.parametrize('route', ['local', 'core', 'spread'])
def test_attempt_records_every_known_route_label(route):
    from urllib.parse import urlsplit

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = b'{}'
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('X-MEC-Route', route)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = client.attempt(urlsplit(f'http://127.0.0.1:{server.server_port}/x'), 'GET', None, {},
                                '127.0.0.1', 2.0)
        assert result['selected_route'] == route
    finally:
        server.shutdown()
