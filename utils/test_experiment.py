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


@pytest.mark.parametrize('error_type', [KeyboardInterrupt, RuntimeError])
def test_interrupted_wait_stops_workers_and_closes_ues(tmp_path, monkeypatch, error_type):
    request_started = threading.Event()
    release_request = threading.Event()
    calls = []
    workers = []
    failure = error_type('interrupted result wait')

    def fake_attempt(url, method, body, headers, source, timeout):
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

    def fake_attempt(url, method, body, headers, source, timeout):
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

    def fake_attempt(url, method, body, headers, source, timeout):
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


def test_fixed_rate_expired_queued_tasks_count_as_missed():
    # Simulate pacer submitting 5 slots in a 1.0s phase while worker execution is delayed until t=2.0s.
    pacer_clock = [0.0]
    pacer_tid = None
    dispatched = []
    emitted = []

    def monotonic_fn():
        if threading.get_ident() == pacer_tid:
            return pacer_clock[0]
        return 2.0

    def wait_fn(dt):
        pacer_clock[0] += dt
        return False

    def dispatch_fn(group, source, ue_index, image):
        dispatched.append(ue_index)
        return {'success': True, 'http_status': 200, 'error': None, 'duration_ms': 1.0,
                'start_time': 1.0, 'end_time': 1.001}

    original = experiment.run_fixed_rate_phase
    # Capture pacer thread id on first wait_fn/monotonic_fn call from inside worker thread.
    def smart_monotonic():
        nonlocal pacer_tid
        # The first two calls in worker() are phase_start_mono (on outer thread? No, phase_start_mono is on caller thread!)
        # Caller thread reads phase_start_mono once; worker thread calls wait_fn; req_pool threads never call wait_fn.
        tid = threading.get_ident()
        if tid == pacer_tid:
            return pacer_clock[0]
        return 2.0 if pacer_tid is not None else 0.0

    def smart_wait(dt):
        nonlocal pacer_tid
        pacer_tid = threading.get_ident()
        pacer_clock[0] += dt
        return False

    # Use a 0.2s wait before slot 0? Slot 0 is at 0.0 (pacer_tid not set yet -> returns 0.0).
    # For slot 0, pacer_tid is set on first wait_fn (for slot 1 at 0.2), so instead set pacer_tid when worker starts:
    # Actually let's use a Gate so worker threads don't run until pacer finishes (pacer_clock[0] == 1.0),
    # and once pacer_clock[0] >= 1.0, monotonic_fn returns 2.0 for ALL threads!
    pacer_done = threading.Event()
    pacer_clock = [0.0]

    def gated_monotonic():
        return 2.0 if pacer_done.is_set() else pacer_clock[0]

    def gated_wait(dt):
        pacer_clock[0] += dt
        if pacer_clock[0] >= 0.99:
            pacer_done.set()
        return False

    # Note: req_pool workers for slots 0..3 might run before slot 4 finishes; block dispatch check until pacer_done?
    # Even simpler: run with a single-threaded executor or make worker threads read 2.0 whenever not in wait_fn?
    # Better: use threading.local: pacer thread has tls.is_pacer = True (set in wait_fn; and slot 0 has offset > 0 so wait_fn runs BEFORE slot 0!).
    # With 2 UEs, UE 1 has offset=0.1s so wait_fn runs before slot 0! Let's test UE 1 (or single UE with offset wait).
    # Actually: if we wrap ThreadPoolExecutor? No, even simpler: in gated_monotonic, if caller thread sets a thread id for main,
    # and wait_fn sets pacer_tid, and for slot 0 we let 2 UEs run and check UE 1 (offset 0.1s -> wait_fn runs first!).
    pacer_tids = set()
    main_tid = threading.get_ident()

    def exact_monotonic():
        tid = threading.get_ident()
        if tid == main_tid or tid in pacer_tids:
            return pacer_clock[0]
        return 2.0

    def exact_wait(dt):
        pacer_tids.add(threading.get_ident())
        pacer_clock[0] += dt
        return False

    per_ue = original(
        'phase_01', [('gnb-1', '10.0.0.1', 0), ('gnb-2', '10.0.0.2', 1)], 1.0, 5.0, 8,
        dispatch_fn, emitted.append, lambda _g: 'http://x/detect', _fake_images(), [0, 0],
        monotonic_fn=exact_monotonic, wait_fn=exact_wait,
    )
    # UE 1 (index 1, offset=0.1s) entered exact_wait before slot 0, so all 5 of its queued tasks saw 2.0s >= 1.0s!
    ue1 = per_ue[1]
    assert ue1['scheduled'] == 5
    assert ue1['started'] == 0
    assert ue1['completed'] == 0
    assert ue1['missed'] == 5
    assert ue1['request_coverage'] == 0.0
    assert all(r['ue_index'] != 1 for r in emitted)


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
