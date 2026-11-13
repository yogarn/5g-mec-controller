import argparse
import http.server
import json
import threading

import pytest

from experiments import experiment, summarize


class FakeUEs:
    instances = []

    def __init__(self, state_dir, config_file, binary):
        self.closed = False
        self.calls = []
        self.instances.append(self)

    def switch_counts(self, total, g2, g3):
        self.calls.append((total, g2, g3))
        return ([{'ip': '127.0.0.1', 'iface': 'ue2'}] * g2,
                [{'ip': '127.0.0.2', 'iface': 'ue3'}] * g3)

    def close(self):
        self.closed = True


def test_ramp_switches_and_sends_face_payload(tmp_path, monkeypatch):
    bodies = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            bodies.append((self.client_address[0], self.rfile.read(int(self.headers['Content-Length']))))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{}')

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    image = tmp_path / 'input.jpg'
    image.write_bytes(b'fixture-image')
    args = argparse.Namespace(url=f'http://127.0.0.1:{server.server_port}/detect', image=str(image),
                              ue_config='ue.yaml', nr_ue='/bin/nr-ue', phase_schedule='1:1:0,1:0:1',
                              phase_duration=0.03, timeout=1, output_dir=str(tmp_path / 'run'), armed=True)
    monkeypatch.setattr(experiment, 'OwnedUELifecycle', FakeUEs)
    try:
        experiment.run(args)
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
    phases = summarize.summarize(tmp_path / 'run' / 'requests.jsonl', 500)
    assert [phase['phase'] for phase in phases] == ['phase_01', 'phase_02']
    assert all(phase['attempts'] > 0 for phase in phases)
    records = [json.loads(line) for line in (tmp_path / 'run' / 'requests.jsonl').read_text().splitlines()]
    assert FakeUEs.instances[-1].calls == [(1, 1, 0), (1, 0, 1)]
    assert FakeUEs.instances[-1].closed
    assert [r['phase'] for r in records if r['event'] == 'phase_end'] == ['phase_01', 'phase_02']
    assert {source for source, _ in bodies} == {'127.0.0.1', '127.0.0.2'}
    assert all(b'fixture-image' in body and b'name="image"' in body for _, body in bodies)
    with pytest.raises(FileExistsError):
        experiment.run(args)


def test_unarmed_never_creates_results(tmp_path):
    args = argparse.Namespace(phase_schedule='1:1:0', armed=False, output_dir=str(tmp_path / 'run'))
    with pytest.raises(ValueError, match='--armed'):
        experiment.run(args)
    assert not (tmp_path / 'run').exists()


def test_default_cli_refuses_noninteractive_execution(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(experiment.sys.stdin, 'isatty', lambda: False)
    with pytest.raises(SystemExit) as error:
        experiment.main([])
    assert error.value.code == 2
    assert not (tmp_path / 'results').exists()
