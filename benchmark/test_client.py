import argparse
import http.server
import json
import threading

import pytest

from benchmark import client


def options(tmp_path, **overrides):
    values = dict(url='http://127.0.0.1:12345/path?q=1', method='POST', body='hello',
                  header=['Content-Type: text/plain'], phase_schedule='1:1:0,1:0:1',
                  gnb2_addresses='127.0.0.1', gnb3_addresses='127.0.0.2',
                  phase_duration=0.03, timeout=1.0, output=str(tmp_path / 'results.jsonl'))
    values.update(overrides)
    return argparse.Namespace(**values)


def test_schedule_order_and_validation():
    phases = client.parse_schedule('2:1:1,3:0:3')
    assert [p['phase'] for p in phases] == ['phase_01', 'phase_02']
    assert [p['gnb3_ues'] for p in phases] == [1, 3]
    for value in ('', '1:1', '2:1:0', '0:0:0', '1:-1:2', '1:1:0,'):
        with pytest.raises(ValueError):
            client.parse_schedule(value)


@pytest.mark.parametrize('changes', [
    {'gnb2_addresses': ''}, {'gnb3_addresses': '127.0.0.1'},
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
    assert [r['phase'] for r in starts] == ['phase_01', 'phase_02']
    assert [r['phase'] for r in ends] == ['phase_01', 'phase_02']
    assert records.index(ends[0]) < records.index(starts[1])
    assert {r['source_address'] for r in requests} == {'127.0.0.1', '127.0.0.2'}
    assert all(r['duration_ms'] >= 0 and r['error'] is None for r in requests)
    assert all(r['success'] == (r['http_status'] == 201) for r in requests)
    assert all(path == '/path?q=1' and body == b'hello' and header == 'text/plain'
               for _, path, body, header in received)
    assert sum(r['attempts'] for r in ends) == len(requests) == len(received)
    assert all(r['per_ue'][0]['attempts'] == r['attempts'] for r in ends)


def test_failed_attempt_and_exclusive_output(tmp_path):
    args = options(tmp_path, phase_schedule='1:1:0', gnb3_addresses='',
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
