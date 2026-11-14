import argparse
import hashlib
import http.server
import json
import threading
import time

import pytest

from experiments import experiment, summarize


class FakeUEs:
    instances = []

    def __init__(self, state_dir, config_file, binary):
        self.closed = False
        self.calls = []
        self.instances.append(self)

    def switch_counts(self, total, g1, g2, g3):
        self.calls.append((total, g1, g2, g3))
        return ([{'ip': '127.0.0.3', 'iface': 'ue1'}] * g1,
                [{'ip': '127.0.0.1', 'iface': 'ue2'}] * g2,
                [{'ip': '127.0.0.2', 'iface': 'ue3'}] * g3)

    def close(self):
        self.closed = True


def test_ramp_switches_and_sends_face_payload(tmp_path, monkeypatch, capsys):
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
                              ue_config='ue.yaml', nr_ue='/bin/nr-ue', phase_schedule='1:1:0:0,1:0:1:0,1:0:0:1',
                              phase_duration=0.03, timeout=1, output_dir=str(tmp_path / 'run'), armed=True)
    monkeypatch.setattr(experiment, 'OwnedUELifecycle', FakeUEs)
    try:
        experiment.run(args)
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
    phases = summarize.summarize(tmp_path / 'run' / 'requests.jsonl', 500)
    output = capsys.readouterr().out
    assert '[..] phase_01:' in output
    assert 'successful, 0 failed' in output
    assert f"Results: {tmp_path / 'run' / 'requests.jsonl'}" in output
    assert [phase['phase'] for phase in phases] == ['phase_01', 'phase_02', 'phase_03']
    assert all(phase['attempts'] > 0 for phase in phases)
    records = [json.loads(line) for line in (tmp_path / 'run' / 'requests.jsonl').read_text().splitlines()]
    assert FakeUEs.instances[-1].calls == [(1, 1, 0, 0), (1, 0, 1, 0), (1, 0, 0, 1)]
    assert FakeUEs.instances[-1].closed
    assert [r['phase'] for r in records if r['event'] == 'phase_end'] == ['phase_01', 'phase_02', 'phase_03']
    assert {(r['group'], r['source_address']) for r in records if r['event'] == 'request'} == {
        ('gnb-1', '127.0.0.3'), ('gnb-2', '127.0.0.1'), ('gnb-3', '127.0.0.2')}
    assert {source for source, _ in bodies} == {'127.0.0.1', '127.0.0.2', '127.0.0.3'}
    assert all(b'fixture-image' in body and b'name="image"' in body for _, body in bodies)
    with pytest.raises(FileExistsError):
        experiment.run(args)


def test_unarmed_never_creates_results(tmp_path):
    args = argparse.Namespace(phase_schedule='1:1:0:0', armed=False, output_dir=str(tmp_path / 'run'))
    with pytest.raises(ValueError, match='--armed'):
        experiment.run(args)
    assert not (tmp_path / 'run').exists()


def test_default_cli_refuses_noninteractive_execution(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(experiment.sys.stdin, 'isatty', lambda: False)
    with pytest.raises(SystemExit) as error:
        experiment.main(['--url', 'http://127.0.0.1:30000/detect'])
    assert error.value.code == 2
    assert not (tmp_path / 'results').exists()


def test_cli_requires_explicit_nodeport_url(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as error:
        experiment.main(['--armed'])
    assert error.value.code == 2
    assert not (tmp_path / 'results').exists()


def test_directory_images_assigned_by_subscriber_order_across_phases(tmp_path, monkeypatch):
    images = tmp_path / 'images'
    images.mkdir()
    (images / 'b.png').write_bytes(b'second-image')
    (images / 'a.jpg').write_bytes(b'first-image')
    (images / 'notes.txt').write_text('not an image')
    calls = []
    call_lock = threading.Lock()

    class OrderedUEs(FakeUEs):
        def switch_counts(self, total, g1, g2, g3):
            # Subscriber ordering is stable across site changes.
            return tuple([
                {'ip': f'127.0.{site}.{index + 1}', 'iface': f'ue{index}'}
                for index in indices
            ] for site, indices in ((1, range(g1)), (2, range(g1, g1 + g2)),
                                    (3, range(g1 + g2, total))))

    def fake_attempt(url, method, body, headers, source, timeout):
        with call_lock:
            calls.append((source, body))
        time.sleep(0.001)
        return {'success': True}

    args = argparse.Namespace(url='http://127.0.0.1/detect', image=str(images),
                              ue_config='ue.yaml', nr_ue='/bin/nr-ue',
                              phase_schedule='3:1:1:1,3:0:1:2',
                              phase_duration=0.01, timeout=1, output_dir=str(tmp_path / 'run'), armed=True)
    monkeypatch.setattr(experiment, 'OwnedUELifecycle', OrderedUEs)
    monkeypatch.setattr(experiment, 'attempt', fake_attempt)
    experiment.run(args)

    metadata = json.loads((tmp_path / 'run' / 'run.json').read_text())
    assert [image['filename'] for image in metadata['images']] == ['a.jpg', 'b.png']
    assert [image['sha256'] for image in metadata['images']] == [
        hashlib.sha256(b'first-image').hexdigest(),
        hashlib.sha256(b'second-image').hexdigest()]
    assert [entry['image_index'] for entry in metadata['image_assignments']] == [0, 1, 0]
    records = [json.loads(line) for line in (tmp_path / 'run' / 'requests.jsonl').read_text().splitlines()]
    requests = [record for record in records if record['event'] == 'request']
    assert {(r['phase'], r['ue_index']) for r in requests} == {
        (phase, index) for phase in ('phase_01', 'phase_02') for index in range(3)}
    assert all(r['image_index'] == r['ue_index'] % 2 for r in requests)
    assert all(r['image_filename'] == ('b.png' if r['ue_index'] == 1 else 'a.jpg') for r in requests)
    assert calls
    assert all((b'second-image' if source.endswith('.2') else b'first-image') in body
               for source, body in calls)


@pytest.mark.parametrize('contents', [None, b''])
def test_empty_images_rejected_before_creating_results_or_ues(tmp_path, monkeypatch, contents):
    images = tmp_path / 'images'
    images.mkdir()
    if contents is not None:
        (images / 'empty.jpg').write_bytes(contents)
    args = argparse.Namespace(url='http://127.0.0.1/detect', image=str(images),
                              ue_config='ue.yaml', nr_ue='/bin/nr-ue', phase_schedule='1:1:0:0',
                              phase_duration=1, timeout=1, output_dir=str(tmp_path / 'run'), armed=True)
    monkeypatch.setattr(experiment, 'OwnedUELifecycle',
                        lambda *args: pytest.fail('UE lifecycle must not be created'))
    with pytest.raises(ValueError, match='image'):
        experiment.run(args)
    assert not (tmp_path / 'run').exists()
