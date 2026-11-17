import json
from pathlib import Path
import threading
import time

import pytest

from benchmark.client import parse_schedule
from utils.continuous_traffic import run_continuous_phases
from utils.summarize import summarize
from utils.ue_lifecycle import ContinuousUELifecycle, plan_assignments


class FakeLifecycle:
    def __init__(self, delay=0.2):
        self.subscribers = [[{'ip': f'10.{s + 1}.0.{i + 1}'} for s in range(3)] for i in range(6)]
        self.assignments = {}
        self.sessions = {}
        self.delay = delay
        self.moves = []
        self.intervals = []

    def reconcile(self, target, before, after):
        changed = [i for i in set(target) | set(self.assignments)
                   if target.get(i) != self.assignments.get(i)]
        for i in changed:
            before(i)
        for i in changed:
            if self.assignments:
                start = time.monotonic()
                time.sleep(self.delay)
                self.intervals.append((start, time.monotonic()))
            self.moves.append(i)
            if i in target:
                self.sessions[i] = {**self.subscribers[i][target[i]], 'site': target[i], 'ue_index': i}
                after(i, self.sessions[i])
            else:
                self.sessions.pop(i, None)
                after(i, None)
        self.assignments = dict(target)


def test_assignment_preserves_existing_ues_on_scaling_and_moves_only_two_for_hotspot():
    initial = plan_assignments({}, [1, 1, 1])
    bigger = plan_assignments(initial, [2, 2, 2])
    assert all(bigger[i] == initial[i] for i in initial)
    hotspot = plan_assignments({}, [7, 1, 1])
    shifted = plan_assignments(hotspot, [5, 3, 1])
    assert sum(hotspot[i] != shifted[i] for i in hotspot) == 2
    assert [sum(s == site for s in shifted.values()) for site in range(3)] == [5, 3, 1]
    with pytest.raises(ValueError):
        plan_assignments({}, [0, 0, 0])


def test_unaffected_ue_sends_through_move_and_windows_reconcile(tmp_path):
    lifecycle = FakeLifecycle(delay=0.25)
    phases = parse_schedule('2:2:0:0,2:1:1:0')
    records, calls = [], []
    lock = threading.Lock()

    def emit(record):
        with lock:
            records.append(record)

    def dispatch(url, image, source, publisher):
        started = time.time()
        with lock:
            calls.append((source, time.monotonic()))
        time.sleep(0.005)
        return dict(success=True, duration_ms=5, http_status=200, error=None,
                    start_time=started, end_time=time.time())

    run_continuous_phases(phases, [0.6, 0.8], lifecycle,
                          [{'filename': 'image.jpg'}], [0, 0],
                          {f'gnb-{s}': f'http://site{s}/detect' for s in range(1, 4)},
                          10, 2, dispatch, emit)
    assert lifecycle.moves == [0, 1, 1]
    move_start, move_end = lifecycle.intervals[0]
    assert any(source == '10.1.0.1' and move_start < when < move_end for source, when in calls)
    assert not any(source == '10.1.0.2' and move_start < when < move_end for source, when in calls)
    starts = [r for r in records if r['event'] == 'phase_start']
    assert starts[1]['start_time'] - starts[0]['start_time'] == pytest.approx(0.6)
    ends = [r for r in records if r['event'] == 'phase_end']
    assert next(r for r in ends if r['phase'] == 'phase_02')['transition_missed'] > 0
    path = tmp_path / 'requests.jsonl'
    path.write_text(''.join(json.dumps(r) + '\n' for r in records
                            if not r['event'].startswith('ue_transition_')))
    result = summarize(path, 150)
    assert len(result) == 2
    assert all(r['scheduled'] == r['completed'] + r['missed'] for r in result)


def test_dispatch_failure_stops_all_pacers_and_closes_publishers():
    lifecycle = FakeLifecycle(delay=0)
    publishers = []

    class Publisher:
        closed = False
        def close(self):
            self.closed = True

    def publisher_factory(ip):
        publisher = Publisher()
        publishers.append(publisher)
        return publisher

    def fail(*args):
        raise ValueError('dispatch broke')

    with pytest.raises(RuntimeError, match='worker failed'):
        run_continuous_phases(parse_schedule('1:1:0:0'), [0.3], lifecycle,
                              [{'filename': 'image.jpg'}], [0], {'gnb-1': 'http://site/detect'},
                              10, 1, fail, lambda record: None, publisher_factory)
    assert publishers and all(p.closed for p in publishers)


def test_inflight_requests_can_finish_after_next_phase_starts(tmp_path):
    lifecycle = FakeLifecycle(delay=0.05)
    records = []
    calls = {}
    lock = threading.Lock()

    def dispatch(url, image, source, publisher):
        started = time.time()
        with lock:
            calls[source] = calls.get(source, 0) + 1
            number = calls[source]
        # The stable UE's final phase-1 request spans the phase boundary.
        time.sleep(0.25 if source == '10.1.0.1' and number == 3 else 0.005)
        return dict(success=True, duration_ms=(time.time() - started) * 1000,
                    http_status=200, error=None, start_time=started, end_time=time.time())

    run_continuous_phases(parse_schedule('2:2:0:0,2:1:1:0'), [0.5, 0.5], lifecycle,
                          [{'filename': 'image.jpg'}], [0, 0],
                          {'gnb-1': 'http://one/detect', 'gnb-2': 'http://two/detect'},
                          5, 2, dispatch, records.append)
    next_start = next(i for i, r in enumerate(records) if r['event'] == 'phase_start' and r['phase'] == 'phase_02')
    assert any(i > next_start and r['event'] == 'request' and r['phase'] == 'phase_01'
               for i, r in enumerate(records))
    path = tmp_path / 'requests.jsonl'
    path.write_text(''.join(json.dumps(r) + '\n' for r in records
                            if not r['event'].startswith('ue_transition_')))
    result = summarize(path, 150)
    assert [r['phase'] for r in result] == ['phase_01', 'phase_02']
    assert result[0]['end_time'] == result[1]['start_time']
    assert result[0]['throughput_denominator_s'] == pytest.approx(0.5)


def test_removed_ue_stops_without_restarting_remaining_ues():
    lifecycle = FakeLifecycle(delay=0.01)
    records = []
    def dispatch(url, image, source, publisher):
        now = time.time()
        return dict(success=True, duration_ms=1, http_status=200, error=None, start_time=now, end_time=now + .001)
    run_continuous_phases(parse_schedule('3:1:1:1,2:1:1:0'), [0.2, 0.3], lifecycle,
                          [{'filename': 'image.jpg'}], [0, 0, 0],
                          {f'gnb-{i}': f'http://site{i}/detect' for i in (1, 2, 3)},
                          10, 1, dispatch, records.append)
    assert lifecycle.moves == [0, 1, 2, 2]
    assert not any(r['event'] == 'request' and r['phase'] == 'phase_02' and r['ue_index'] == 2 for r in records)


def test_moving_ue_drains_its_requests_while_stable_ue_continues():
    active = {}
    lock = threading.Lock()
    calls = []
    numbers = {}

    class DrainedLifecycle(FakeLifecycle):
        def reconcile(self, target, before, after):
            def checked_before(index):
                before(index)
                if index in self.sessions:
                    with lock:
                        assert active.get(self.sessions[index]['ip'], 0) == 0
            return super().reconcile(target, checked_before, after)

    def dispatch(url, image, source, publisher):
        start = time.time()
        with lock:
            active[source] = active.get(source, 0) + 1
            numbers[source] = numbers.get(source, 0) + 1
            number = numbers[source]
            calls.append((source, time.monotonic()))
        time.sleep(0.2 if source == '10.1.0.2' and number == 3 else 0.005)
        with lock:
            active[source] -= 1
        return dict(success=True, duration_ms=(time.time() - start) * 1000,
                    start_time=start, end_time=time.time(), http_status=200, error=None)

    lifecycle = DrainedLifecycle(delay=0.2)
    run_continuous_phases(parse_schedule('2:2:0:0,2:1:1:0'), [0.6, 0.8], lifecycle,
                          [{'filename': 'image.jpg'}], [0, 0],
                          {'gnb-1': 'http://one/detect', 'gnb-2': 'http://two/detect'},
                          5, 2, dispatch, lambda record: None)
    start, end = lifecycle.intervals[0]
    assert any(source == '10.1.0.1' and start < when < end for source, when in calls)


def test_pool_does_not_touch_unchanged_sessions(tmp_path, monkeypatch):
    from utils import ue_lifecycle as ue
    changes = []

    class Child:
        def __init__(self, state_dir, config_file, binary, **kwargs):
            self.subscriber = kwargs['_subscribers'][0]
            self.processes = []
            self.journal = Path(state_dir) / 'lifecycle.jsonl'
        def switch_counts(self, total, *counts):
            site = counts.index(1)
            changes.append(self.subscriber[site]['ip'])
            result = ([], [], [])
            result[site].append({'ip': self.subscriber[site]['ip'], 'iface': 'uesimtun0'})
            return result

    monkeypatch.setattr(ue, 'OwnedUELifecycle', Child)
    monkeypatch.setattr(ue, '_record', lambda *a, **kw: None)
    monkeypatch.setattr(ue, '_cleanup', lambda *a: None)
    pool = ContinuousUELifecycle.__new__(ContinuousUELifecycle)
    pool.closed = False
    pool.subscribers = FakeLifecycle().subscribers
    pool.binary = '/bin/true'
    pool.config_file = 'unused'
    pool.state_dir = tmp_path
    pool.journal = tmp_path / 'lifecycle.jsonl'
    pool.lock = object()
    pool.children, pool.sessions, pool.assignments = {}, {}, {}
    target = plan_assignments({}, [1, 1, 1])
    pool.reconcile(target)
    unchanged = dict(pool.sessions)
    pool.reconcile(plan_assignments(target, [2, 2, 2]))
    assert len(changes) == 6  # Three additions, no restarts of the original UEs.
    assert all(pool.sessions[i] is unchanged[i] for i in unchanged)


def test_experiment_entrypoint_records_continuous_mode_and_separate_transitions(tmp_path, monkeypatch):
    from argparse import Namespace
    from utils import experiment

    instances = []
    class Lifecycle(FakeLifecycle):
        def __init__(self, *args):
            super().__init__(delay=0.05)
            self.closed = False
            instances.append(self)
        def close(self):
            self.closed = True
    monkeypatch.setattr(experiment, 'ContinuousUELifecycle', Lifecycle)
    def attempt(url, method, body, headers, source, timeout):
        now = time.time()
        return dict(start_time=now, end_time=now + .001, duration_ms=1,
                    success=True, http_status=200, error=None)
    monkeypatch.setattr(experiment, 'attempt', attempt)
    image = tmp_path / 'image.jpg'
    image.write_bytes(b'fixture-image')
    output = tmp_path / 'run'
    experiment.run(Namespace(
        phase_transition='continuous', phase_schedule='2:2:0:0,2:1:1:0',
        phase_duration=.3, phase_durations=None, armed=True, timeout=1,
        rps_per_ue=10, max_in_flight_per_ue=1, url='http://site/detect',
        image=str(image), output_dir=str(output), ue_config='unused', nr_ue='/bin/true'))
    assert json.loads((output / 'run.json').read_text())['phase_transition'] == 'continuous'
    assert (output / 'ue-transitions.jsonl').exists()
    assert len(summarize(output / 'requests.jsonl', 150)) == 2
    assert instances[0].closed
