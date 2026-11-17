import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.server
import json
import threading
import time
from types import SimpleNamespace

import pytest

from utils import experiment, summarize


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


@pytest.mark.parametrize('valid_response', [True, False])
def test_ramp_switches_and_sends_face_payload(tmp_path, monkeypatch, capsys, valid_response):
    bodies = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            bodies.append((self.client_address[0], self.rfile.read(int(self.headers['Content-Length']))))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"face_count": 0, "faces": []}' if valid_response else b'{"error":"broken inference"}')

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
    assert ('successful, 0 failed' if valid_response else '0 successful,') in output
    assert f"Results: {tmp_path / 'run' / 'requests.jsonl'}" in output
    assert [phase['phase'] for phase in phases] == ['phase_01', 'phase_02', 'phase_03']
    assert all(phase['attempts'] > 0 for phase in phases)
    records = [json.loads(line) for line in (tmp_path / 'run' / 'requests.jsonl').read_text().splitlines()]
    requests = [r for r in records if r['event'] == 'request']
    assert all(r['http_success'] and r['response_valid'] == valid_response for r in requests)
    assert all(r['response_json'] == ({'face_count': 0, 'faces': []} if valid_response
                                     else {'error': 'broken inference'}) for r in requests)
    assert all(p['successes'] == (p['attempts'] if valid_response else 0) for p in phases)
    if not valid_response:
        assert all(p['slo_attainment_all_attempts'] == 0 for p in phases)
    assert FakeUEs.instances[-1].calls == [(1, 1, 0, 0), (1, 0, 1, 0), (1, 0, 0, 1)]
    assert FakeUEs.instances[-1].closed
    assert [r['phase'] for r in records if r['event'] == 'phase_end'] == ['phase_01', 'phase_02', 'phase_03']
    assert {(r['group'], r['source_address']) for r in records if r['event'] == 'request'} == {
        ('gnb-1', '127.0.0.3'), ('gnb-2', '127.0.0.1'), ('gnb-3', '127.0.0.2')}
    assert {source for source, _ in bodies} == {'127.0.0.1', '127.0.0.2', '127.0.0.3'}
    assert all(b'fixture-image' in body and b'name="image"' in body for _, body in bodies)
    with pytest.raises(FileExistsError):
        experiment.run(args)


@pytest.mark.parametrize('error_type', [KeyboardInterrupt, RuntimeError])
def test_interrupted_wait_stops_workers_and_closes_ues(tmp_path, monkeypatch, error_type):
    request_started = threading.Event()
    release_request = threading.Event()
    calls = []
    workers = []
    failure = error_type('interrupted result wait')

    def fake_attempt(url, method, body, headers, source, timeout, *, response_contract='http'):
        calls.append(source)
        # Fail promptly instead of looping forever if cancellation regresses.
        assert len(calls) == 1, 'worker sent traffic after cancellation'
        request_started.set()
        assert release_request.wait(timeout=5), 'executor did not release the in-flight request'
        return {'success': True}

    class InterruptedFuture:
        def result(self):
            assert request_started.wait(timeout=5), 'worker did not start its request'
            raise failure

    class InterruptedExecutor(ThreadPoolExecutor):
        def submit(self, *args, **kwargs):
            workers.append(super().submit(*args, **kwargs))
            return InterruptedFuture()

        def __exit__(self, *args):
            # The request completes only after the result wait has failed and
            # run() has had its chance to cancel, before executor shutdown joins.
            release_request.set()
            return super().__exit__(*args)

    image = tmp_path / 'input.jpg'
    image.write_bytes(b'fixture-image')
    args = argparse.Namespace(url='http://127.0.0.1/detect', image=str(image),
                              ue_config='ue.yaml', nr_ue='/bin/nr-ue',
                              phase_schedule='1:1:0:0,1:0:1:0',
                              phase_duration=180, timeout=1, output_dir=str(tmp_path / 'run'), armed=True)
    monkeypatch.setattr(experiment, 'OwnedUELifecycle', FakeUEs)
    monkeypatch.setattr(experiment, 'attempt', fake_attempt)
    monkeypatch.setattr(experiment, 'ThreadPoolExecutor', InterruptedExecutor)
    # The phase deadline can never expire: only cancellation can stop traffic.
    monkeypatch.setattr(experiment, 'time',
                        SimpleNamespace(time=lambda: 1000, monotonic=lambda: 0))

    with pytest.raises(error_type, match='interrupted result wait') as caught:
        experiment.run(args)

    assert caught.value is failure
    assert calls == ['127.0.0.3']
    assert len(workers) == 1
    assert workers[0].done()
    result = workers[0].result()
    assert {k: result[k] for k in ('group', 'source_address', 'attempts', 'successes', 'scheduled', 'started', 'completed', 'failures', 'missed')} == {
        'group': 'gnb-1', 'source_address': '127.0.0.3', 'attempts': 1, 'successes': 1,
        'scheduled': 1, 'started': 1, 'completed': 1, 'failures': 0, 'missed': 0}
    lifecycle = FakeUEs.instances[-1]
    assert lifecycle.closed
    assert lifecycle.calls == [(1, 1, 0, 0)]
    records = [json.loads(line) for line in (tmp_path / 'run' / 'requests.jsonl').read_text().splitlines()]
    assert [record['event'] for record in records] == ['phase_start', 'request']


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

    def fake_attempt(url, method, body, headers, source, timeout, *, response_contract='http'):
        with call_lock:
            calls.append((source, body))
        time.sleep(0.001)
        return {'success': True}

    args = argparse.Namespace(url='http://127.0.0.1/detect', image=str(images),
                              ue_config='ue.yaml', nr_ue='/bin/nr-ue',
                              phase_schedule='3:1:1:1,3:0:1:2',
                              phase_duration=0.02, rps_per_ue=100.0, timeout=1,
                              output_dir=str(tmp_path / 'run'), armed=True)
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


def test_site_urls_route_each_group_and_record_destination(tmp_path, monkeypatch):
    image = tmp_path / 'input.jpg'
    image.write_bytes(b'image')
    site_urls = {f'gnb-{site}': f'http://127.0.0.{site}:30000/detect' for site in (1, 2, 3)}
    calls = []

    def fake_attempt(url, method, body, headers, source, timeout, *, response_contract='http'):
        calls.append((source, url.geturl()))
        time.sleep(0.001)
        return {'success': True}

    args = argparse.Namespace(url=None, site_urls=site_urls, image=str(image),
                              ue_config='ue.yaml', nr_ue='/bin/nr-ue',
                              phase_schedule='3:1:1:1', phase_duration=0.25, timeout=1,
                              output_dir=str(tmp_path / 'run'), armed=True)
    monkeypatch.setattr(experiment, 'OwnedUELifecycle', FakeUEs)
    monkeypatch.setattr(experiment, 'attempt', fake_attempt)
    experiment.run(args)
    assert json.loads((tmp_path / 'run' / 'run.json').read_text())['site_urls'] == site_urls
    records = [json.loads(line) for line in (tmp_path / 'run' / 'requests.jsonl').read_text().splitlines()]
    requests = [record for record in records if record['event'] == 'request']
    assert {record['group'] for record in requests} == set(site_urls)
    assert all(record['url'] == site_urls[record['group']] for record in requests)
    assert set(calls) == {('127.0.0.3', site_urls['gnb-1']),
                          ('127.0.0.1', site_urls['gnb-2']),
                          ('127.0.0.2', site_urls['gnb-3'])}


@pytest.mark.parametrize('site_urls', [
    {}, {'gnb-1': 'http://host/detect'},
    {'gnb-1': 'http://host/detect', 'gnb-2': 'http://host/detect', 'gnb-3': 'http://host:0/detect'},
    {'gnb-1': 'http://host/detect', 'gnb-2': 'http://host/detect', 'gnb-3': None},
])
def test_invalid_site_urls_rejected_before_results(tmp_path, site_urls):
    args = argparse.Namespace(url=None, site_urls=site_urls, phase_schedule='1:1:0:0',
                              phase_duration=1, timeout=1, armed=True,
                              output_dir=str(tmp_path / 'run'))
    with pytest.raises(ValueError):
        experiment.run(args)
    assert not (tmp_path / 'run').exists()


def test_campaign_ue_forwards_site_urls(tmp_path, monkeypatch):
    from experiments import campaign_ue
    site_urls = {f'gnb-{site}': f'http://node{site}:30000/detect' for site in (1, 2, 3)}
    settings = {'campaign': 'campaign', 'method': 'hpa', 'site_urls': site_urls,
                'image': 'image.jpg', 'inventory': 'ue.yaml', 'nr_ue': '/bin/nr-ue',
                'schedule': '1:1:0:0', 'duration': 1,
                'rps_per_ue': 7.5, 'max_in_flight_per_ue': 6,
                'telemetry_url': 'http://collector/observations', 'token': 'token'}
    captured = []
    def capture(args, **kwargs):
        captured.extend(args)
        raise RuntimeError('captured')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(campaign_ue.subprocess, 'Popen', capture)
    with pytest.raises(RuntimeError, match='captured'):
        campaign_ue.run(settings)
    assert json.loads(captured[captured.index('--site-urls') + 1]) == site_urls
    assert '--url' not in captured
    assert captured[captured.index('--rps-per-ue') + 1] == '7.5'
    assert captured[captured.index('--max-in-flight-per-ue') + 1] == '6'
    assert captured[captured.index('--telemetry-url') + 1] == settings['telemetry_url']
    assert captured[captured.index('--telemetry-token-file') + 1] == settings['token']


def _fake_images(count=1):
    return [{'filename': f'img{i}.jpg', 'sha256': '0' * 64, 'bytes': 1, 'body': b'x', 'headers': {}} for i in range(count)]


def test_fixed_rate_pacing_independent_of_response_duration():
    clock = [0.0]
    clock_lock = threading.Lock()
    release = threading.Event()
    dispatched = []

    def monotonic_fn():
        with clock_lock:
            return clock[0]

    def wait_fn(dt):
        with clock_lock:
            clock[0] += dt
        return False

    def dispatch_fn(group, source, ue_index, image):
        with clock_lock:
            dispatched.append(clock[0])
        release.wait(timeout=2)
        return {'success': True, 'http_status': 200, 'error': None, 'duration_ms': 350.0,
                'start_time': 1.0, 'end_time': 1.35}

    emitted = []
    def emit(rec):
        emitted.append(rec)

    # Release in-flight requests right after the pacer finishes scheduling at t=0.6
    def releaser():
        while len(dispatched) < 3:
            time.sleep(0.002)
        release.set()

    t = threading.Thread(target=releaser, daemon=True)
    t.start()
    per_ue = experiment.run_fixed_rate_phase(
        'phase_01', [('gnb-1', '10.0.0.1', 0)], 0.6, 5.0, 4,
        dispatch_fn, emit, lambda _g: 'http://x/detect', _fake_images(), [0],
        monotonic_fn=monotonic_fn, wait_fn=wait_fn,
    )
    t.join(timeout=2)
    assert len(dispatched) == 3
    assert [round(x, 6) for x in dispatched] == [0.0, 0.2, 0.4]
    assert per_ue[0]['scheduled'] == 3
    assert per_ue[0]['started'] == 3
    assert per_ue[0]['completed'] == 3
    assert per_ue[0]['missed'] == 0
    assert len([r for r in emitted if r['event'] == 'request']) == 3


def test_fixed_rate_bounded_concurrency_and_missed_slots_without_catchup():
    clock = [0.0]
    clock_lock = threading.Lock()
    gate = threading.Event()
    reached_08 = threading.Event()
    drained_before_08 = threading.Event()
    active = 0
    peak_active = 0
    dispatched_offsets = []

    def monotonic_fn():
        with clock_lock:
            return clock[0]

    def wait_fn(dt):
        with clock_lock:
            clock[0] += dt
            cur = clock[0]
        if cur >= 0.79:
            reached_08.set()
            drained_before_08.wait(timeout=2)
        return False

    def dispatch_fn(group, source, ue_index, image):
        nonlocal active, peak_active
        with clock_lock:
            active += 1
            peak_active = max(peak_active, active)
            dispatched_offsets.append(round(clock[0], 6))
        gate.wait(timeout=2)
        with clock_lock:
            active -= 1
        return {'success': True, 'http_status': 200, 'error': None, 'duration_ms': 100.0,
                'start_time': 1.0, 'end_time': 1.1}

    def release_later():
        reached_08.wait(timeout=2)
        while True:
            with clock_lock:
                if active == 2:
                    break
            time.sleep(0.001)
        gate.set()
        while True:
            with clock_lock:
                if active == 0:
                    break
            time.sleep(0.001)
        drained_before_08.set()

    t = threading.Thread(target=release_later, daemon=True)
    t.start()
    per_ue = experiment.run_fixed_rate_phase(
        'phase_01', [('gnb-1', '10.0.0.1', 0)], 1.0, 5.0, 2,
        dispatch_fn, lambda _r: None, lambda _g: 'http://x/detect', _fake_images(), [0],
        monotonic_fn=monotonic_fn, wait_fn=wait_fn,
    )
    t.join(timeout=2)
    assert peak_active == 2
    # Slots: 0.0 (started), 0.2 (started), 0.4 (missed), 0.6 (missed), 0.8 (started after unblock, no catch-up burst)
    assert per_ue[0]['scheduled'] == 5
    assert per_ue[0]['started'] == 3
    assert per_ue[0]['completed'] == 3
    assert per_ue[0]['missed'] == 2
    assert dispatched_offsets[0] == 0.0 and dispatched_offsets[1] == 0.2
    assert dispatched_offsets[2] == 0.8


def test_fixed_rate_scheduling_lag_skips_without_catchup_burst():
    # Jump clock forward from 0.0 -> 0.62 after slot 0 starts so slots 0.2 and 0.4 are missed, 0.6 dispatches once.
    clock = [0.0]
    jumps = iter([0.62, 0.18, 0.2])
    dispatched = []
    slot_started = threading.Event()

    def monotonic_fn():
        return clock[0]

    def wait_fn(dt):
        slot_started.wait(timeout=2)
        slot_started.clear()
        clock[0] += next(jumps, dt)
        return False

    def dispatch_fn(group, source, ue_index, image):
        dispatched.append(round(clock[0], 6))
        slot_started.set()
        return {'success': True, 'http_status': 200, 'error': None, 'duration_ms': 1.0,
                'start_time': 1.0, 'end_time': 1.001}

    per_ue = experiment.run_fixed_rate_phase(
        'phase_01', [('gnb-1', '10.0.0.1', 0)], 1.0, 5.0, 4,
        dispatch_fn, lambda _r: None, lambda _g: 'http://x/detect', _fake_images(), [0],
        monotonic_fn=monotonic_fn, wait_fn=wait_fn,
    )
    assert per_ue[0]['scheduled'] == 5
    assert per_ue[0]['missed'] == 2
    assert per_ue[0]['started'] == 3
    assert len(dispatched) == 3


def test_fixed_rate_staggered_starts_across_ues():
    emitted = []
    emit_lock = threading.Lock()
    tls = threading.local()

    def monotonic_fn():
        return getattr(tls, 't', 0.0)

    def wait_fn(dt):
        tls.t = getattr(tls, 't', 0.0) + dt
        return False

    def dispatch_fn(group, source, ue_index, image):
        return {'success': True, 'http_status': 200, 'error': None, 'duration_ms': 1.0,
                'start_time': 1.0, 'end_time': 1.001}

    def emit(rec):
        with emit_lock:
            emitted.append(rec)

    selected = [('gnb-1', '10.0.0.1', 0), ('gnb-2', '10.0.0.2', 1), ('gnb-3', '10.0.0.3', 2)]
    per_ue = experiment.run_fixed_rate_phase(
        'phase_01', selected, 0.4, 5.0, 4,
        dispatch_fn, emit, lambda _g: 'http://x/detect', _fake_images(), [0, 0, 0],
        monotonic_fn=monotonic_fn, wait_fn=wait_fn,
    )
    by_ue = {idx: sorted(round(r['scheduled_offset_s'], 6) for r in emitted if r['ue_index'] == idx) for idx in (0, 1, 2)}
    assert by_ue[0] == [0.0, 0.2]
    assert by_ue[1] == [round(1 / 15, 6), round(1 / 15 + 0.2, 6)]
    assert by_ue[2] == [round(2 / 15, 6), round(2 / 15 + 0.2, 6)]
    assert sum(u['scheduled'] for u in per_ue) == 6


def test_fixed_rate_phase_end_drains_in_flight_before_returning():
    clock = [0.0]
    dispatched = threading.Event()
    in_flight_done = threading.Event()
    emitted = []

    def monotonic_fn():
        return clock[0]

    def wait_fn(dt):
        clock[0] += dt
        return False

    def dispatch_fn(group, source, ue_index, image):
        dispatched.set()
        in_flight_done.wait(timeout=2)
        return {'success': True, 'http_status': 200, 'error': None, 'duration_ms': 50.0,
                'start_time': 1.0, 'end_time': 1.05}

    def release_after_phase_end():
        dispatched.wait(timeout=2)
        in_flight_done.set()

    t = threading.Thread(target=release_after_phase_end, daemon=True)
    t.start()
    per_ue = experiment.run_fixed_rate_phase(
        'phase_01', [('gnb-1', '10.0.0.1', 0)], 0.2, 5.0, 2,
        dispatch_fn, emitted.append, lambda _g: 'http://x/detect', _fake_images(), [0],
        monotonic_fn=monotonic_fn, wait_fn=wait_fn,
    )
    t.join(timeout=2)
    assert in_flight_done.is_set()
    assert per_ue[0]['completed'] == 1
    assert len(emitted) == 1 and emitted[0]['phase'] == 'phase_01'


def test_fixed_rate_cancellation_stops_without_waiting_for_phase_end():
    cancelled = threading.Event()
    dispatched = 0

    def dispatch_fn(group, source, ue_index, image):
        nonlocal dispatched
        dispatched += 1
        cancelled.set()
        return {'success': True, 'http_status': 200, 'error': None, 'duration_ms': 1.0,
                'start_time': 1.0, 'end_time': 1.001}

    start = time.monotonic()
    per_ue = experiment.run_fixed_rate_phase(
        'phase_01', [('gnb-1', '10.0.0.1', 0)], 30.0, 5.0, 2,
        dispatch_fn, lambda _r: None, lambda _g: 'http://x/detect', _fake_images(), [0],
        cancelled=cancelled,
    )
    elapsed = time.monotonic() - start
    assert elapsed < 1.0
    assert dispatched == 1
    assert per_ue[0]['completed'] == 1


@pytest.mark.parametrize('kwargs', [
    {'rps_per_ue': 0},
    {'rps_per_ue': -5},
    {'rps_per_ue': float('nan')},
    {'max_in_flight_per_ue': 0},
    {'max_in_flight_per_ue': True},
])
def test_invalid_rps_or_concurrency_rejected_before_results(tmp_path, kwargs):
    args = argparse.Namespace(url='http://127.0.0.1:30000/detect', site_urls=None,
                              image=str(tmp_path), ue_config='ue.yaml', nr_ue='/bin/nr-ue',
                              phase_schedule='1:1:0:0', phase_duration=1, timeout=1,
                              output_dir=str(tmp_path / 'run'), armed=True, **kwargs)
    with pytest.raises(ValueError):
        experiment.run(args)
    assert not (tmp_path / 'run').exists()


def test_fixed_rate_expired_queued_tasks_count_as_missed(monkeypatch):
    # Ensure pacer submits all 5 tasks to the executor, then workers execute at t=2.0s (> 1.0s phase).
    import concurrent.futures

    clock = [0.0]
    dispatched = []
    emitted = []
    submitted_count = 0
    all_submitted = threading.Event()

    class DelayedWorkerExecutor(concurrent.futures.ThreadPoolExecutor):
        def submit(self, fn, *args, **kwargs):
            nonlocal submitted_count
            submitted_count += 1
            if submitted_count == 5:
                all_submitted.set()

            def delayed():
                all_submitted.wait(timeout=2)
                clock[0] = 2.0
                return fn(*args, **kwargs)

            return super().submit(delayed)

    monkeypatch.setattr(concurrent.futures, 'ThreadPoolExecutor', DelayedWorkerExecutor)

    def monotonic_fn():
        return clock[0]

    def wait_fn(dt):
        clock[0] += dt
        return False

    def dispatch_fn(group, source, ue_index, image):
        dispatched.append(ue_index)
        return {'success': True, 'http_status': 200, 'error': None, 'duration_ms': 1.0,
                'start_time': 1.0, 'end_time': 1.001}

    per_ue = experiment.run_fixed_rate_phase(
        'phase_01', [('gnb-1', '10.0.0.1', 0)], 1.0, 5.0, 8,
        dispatch_fn, emitted.append, lambda _g: 'http://x/detect', _fake_images(), [0],
        monotonic_fn=monotonic_fn, wait_fn=wait_fn,
    )
    assert submitted_count == 5
    assert per_ue[0]['scheduled'] == 5
    assert per_ue[0]['started'] == 0
    assert per_ue[0]['completed'] == 0
    assert per_ue[0]['missed'] == 5
    assert per_ue[0]['request_coverage'] == 0.0
    assert dispatched == []
    assert emitted == []


def test_fixed_rate_slow_emit_does_not_bypass_worker_deadline():
    # Slot 0 completes and blocks in emit() holding ue_lock until t=0.45s (> 0.30s phase_duration)
    # while slot 1 (0.20s) is waiting to start; slot 1 must recheck deadline after acquiring ue_lock.
    clock = [0.0]
    clock_lock = threading.Lock()
    slot0_in_emit = threading.Event()
    slot1_submitted = threading.Event()
    release_emit = threading.Event()
    dispatched_at = []
    emitted = []

    def monotonic_fn():
        with clock_lock:
            return clock[0]

    def wait_fn(dt):
        if round(monotonic_fn(), 6) == 0.0:
            # Let slot 0 finish and enter emit() (holding ue_lock) before advancing to slot 1 (0.20s).
            slot0_in_emit.wait(timeout=2)
        with clock_lock:
            clock[0] += dt
            cur = clock[0]
        if round(cur, 6) >= 0.20:
            slot1_submitted.set()
        return False

    def dispatch_fn(group, source, ue_index, image):
        dispatched_at.append(round(monotonic_fn(), 6))
        return {'success': True, 'http_status': 200, 'error': None, 'duration_ms': 1.0,
                'start_time': 1.0, 'end_time': 1.001}

    def slow_emit(record):
        slot0_in_emit.set()
        # Wait until slot 1 has been submitted at t=0.20s, then advance clock past phase_end (0.30s) to 0.45s.
        slot1_submitted.wait(timeout=2)
        release_emit.wait(timeout=2)
        emitted.append(record)

    def advance_and_release():
        slot1_submitted.wait(timeout=2)
        # Give slot 1 worker a moment to block on ue_lock before advancing clock to 0.45s.
        time.sleep(0.02)
        with clock_lock:
            clock[0] = 0.45
        release_emit.set()

    t = threading.Thread(target=advance_and_release, daemon=True)
    t.start()
    per_ue = experiment.run_fixed_rate_phase(
        'phase_01', [('gnb-1', '10.0.0.1', 0)], 0.30, 5.0, 4,
        dispatch_fn, slow_emit, lambda _g: 'http://x/detect', _fake_images(), [0],
        monotonic_fn=monotonic_fn, wait_fn=wait_fn,
    )
    t.join(timeout=2)
    assert dispatched_at == [0.0]
    assert per_ue[0]['scheduled'] == 2
    assert per_ue[0]['started'] == 1
    assert per_ue[0]['completed'] == 1
    assert per_ue[0]['missed'] == 1
    assert per_ue[0]['request_coverage'] == 0.5
    assert len(emitted) == 1


def test_fixed_rate_delayed_slot_039_skipped_without_compressed_follow_up():
    # Reproduce Codex's delay scenario: 0.00, then wakeup delayed to 0.39, then 0.40, 0.60, 0.80.
    clock = [0.0]
    starts = []
    emitted = []
    wake_times = iter([0.39, 0.40, 0.60, 0.80, 1.00])
    dispatch_ack = threading.Event()
    expecting_dispatch = {0.0, 0.40, 0.60, 0.80}

    def monotonic_fn():
        return clock[0]

    def wait_fn(dt):
        if round(clock[0], 6) in expecting_dispatch:
            dispatch_ack.wait(timeout=2)
            dispatch_ack.clear()
        clock[0] = next(wake_times, clock[0] + dt)
        return False

    def dispatch_fn(group, source, ue_index, image):
        starts.append(round(clock[0], 6))
        dispatch_ack.set()
        return {'success': True, 'http_status': 200, 'error': None, 'duration_ms': 1.0,
                'start_time': 1.0, 'end_time': 1.001}

    per_ue = experiment.run_fixed_rate_phase(
        'phase_01', [('gnb-1', '10.0.0.1', 0)], 1.0, 5.0, 8,
        dispatch_fn, emitted.append, lambda _g: 'http://x/detect', _fake_images(), [0],
        monotonic_fn=monotonic_fn, wait_fn=wait_fn,
    )
    assert per_ue[0]['scheduled'] == 5
    assert per_ue[0]['started'] == 4
    assert per_ue[0]['missed'] == 1
    assert starts == [0.0, 0.4, 0.6, 0.8]
    offsets = sorted(round(r['dispatch_offset_s'], 6) for r in emitted)
    assert offsets == [0.0, 0.4, 0.6, 0.8]
    assert all(b - a >= 0.19 for a, b in zip(offsets, offsets[1:]))


def test_phase_durations_records_actual_final_hold_without_extra_phases(tmp_path, monkeypatch):
    image = tmp_path / 'input.jpg'
    image.write_bytes(b'image')
    calls = []

    def fake_attempt(url, method, body, headers, source, timeout, *, response_contract='http'):
        return {'success': True}

    def fake_run_phase(phase_name, selected, phase_duration, rps_per_ue, max_in_flight_per_ue,
                       dispatch_fn, emit, url_for_group, images, assignments, cancelled=None):
        calls.append((phase_name, phase_duration))
        scheduled = int(round(phase_duration * rps_per_ue))
        return [{
            'group': group,
            'source_address': source,
            'ue_index': ue_index,
            'scheduled': scheduled,
            'started': scheduled,
            'completed': scheduled,
            'attempts': scheduled,
            'successes': scheduled,
            'failures': 0,
            'missed': 0,
            'actual_start_rate': scheduled / phase_duration,
            'request_coverage': 1.0,
        } for group, source, ue_index in selected]

    args = argparse.Namespace(
        url='http://127.0.0.1/detect', image=str(image),
        ue_config='ue.yaml', nr_ue='/bin/nr-ue',
        phase_schedule='3:1:1:1,3:1:1:1', phase_duration=60.0,
        phase_durations='60,420', rps_per_ue=5.0, max_in_flight_per_ue=8,
        timeout=1, output_dir=str(tmp_path / 'run'), armed=True,
    )
    monkeypatch.setattr(experiment, 'OwnedUELifecycle', FakeUEs)
    monkeypatch.setattr(experiment, 'attempt', fake_attempt)
    monkeypatch.setattr(experiment, 'run_fixed_rate_phase', fake_run_phase)
    experiment.run(args)

    assert calls == [('phase_01', 60.0), ('phase_02', 420.0)]
    metadata = json.loads((tmp_path / 'run' / 'run.json').read_text())
    assert metadata['phase_durations_s'] == [60.0, 420.0]
    records = [json.loads(line) for line in (tmp_path / 'run' / 'requests.jsonl').read_text().splitlines()]
    starts = [r for r in records if r['event'] == 'phase_start']
    ends = [r for r in records if r['event'] == 'phase_end']
    assert [s['duration_s'] for s in starts] == [60.0, 420.0]
    assert [e['generation_duration_s'] for e in ends] == [60.0, 420.0]
    assert [e['scheduled'] for e in ends] == [900, 6300]
    assert [e['actual_start_rate'] for e in ends] == [15.0, 15.0]


@pytest.mark.parametrize('bad_durations', ['60', '60,0', '60,-5', '60,abc'])
def test_invalid_phase_durations_rejected_before_output_or_ues(tmp_path, monkeypatch, bad_durations):
    image = tmp_path / 'input.jpg'
    image.write_bytes(b'image')
    args = argparse.Namespace(
        url='http://127.0.0.1/detect', image=str(image),
        ue_config='ue.yaml', nr_ue='/bin/nr-ue',
        phase_schedule='3:1:1:1,3:1:1:1', phase_duration=60.0,
        phase_durations=bad_durations, timeout=1,
        output_dir=str(tmp_path / 'run'), armed=True,
    )
    monkeypatch.setattr(experiment, 'OwnedUELifecycle',
                        lambda *a: pytest.fail('UE lifecycle must not be created'))
    with pytest.raises(ValueError):
        experiment.run(args)
    assert not (tmp_path / 'run').exists()
