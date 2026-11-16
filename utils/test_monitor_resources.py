import csv
import json
from datetime import datetime, timezone

import pytest

from utils import monitor_resources

from utils.monitor_resources import summarize


def test_delayed_metric_uses_historical_pods_and_missing_is_not_zero(tmp_path):
    start = 1700000000
    events = []
    rows = []
    for index in (1, 2):
        phase = f'phase_{index:02d}'
        phase_start = start + (index - 1) * 60
        events.extend([{'event': 'phase_start', 'phase': phase, 'start_time': phase_start},
                       {'event': 'phase_end', 'phase': phase, 'end_time': phase_start + 30}])
        rows.append({'phase': phase, 'workload_ue': 1, 'gnb1_ues': 1, 'gnb2_ues': 0, 'gnb3_ues': 0,
                     'latency_p50_ms': 10, 'latency_p90_ms': 20, 'attempts': 2, 'successes': 2,
                     'failures': 0, 'start_time': phase_start, 'end_time': phase_start + 30})
    requests = tmp_path / 'requests.jsonl'
    requests.write_text(''.join(json.dumps(event) + '\n' for event in events))
    summary = tmp_path / 'summary.csv'
    with summary.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0]); writer.writeheader(); writer.writerows(rows)
    pod = {'name': 'app-a', 'node': 'gnb1', 'phase': 'Running', 'ready': True}
    old_time = datetime.fromtimestamp(start + 15, timezone.utc).isoformat()
    old_metric = {'timestamp': old_time, 'window': '15s',
                  'containers': [{'name': 'app', 'usage': {'cpu': '750m', 'memory': '1024Ki'}}]}
    samples = [{'sample_time': start + 10, 'pods': [pod], 'metrics': {}},
               {'sample_time': start + 13, 'pods': [pod], 'metrics': {}},
               {'sample_time': start + 29, 'pods': [pod], 'metrics': {}},
               {'sample_time': start + 40, 'pods': [pod], 'metrics': {'app-a': old_metric}},
               {'sample_time': start + 89, 'pods': [pod], 'metrics': {'app-a': old_metric}}]
    data = tmp_path / 'metrics.jsonl'
    data.write_text(''.join(json.dumps(sample) + '\n' for sample in samples))
    summarize(data, requests, summary, tmp_path)
    with (tmp_path / 'resource-by-ramp.csv').open() as stream:
        values = list(csv.DictReader(stream))
    assert float(values[0]['cpu_millicores']) == 750
    assert float(values[0]['memory_mib']) == 1
    assert values[0]['complete_samples'] == '1'
    assert values[1]['complete_samples'] == '0'
    assert values[1]['cpu_millicores'] == values[1]['memory_mib'] == ''


@pytest.mark.parametrize('namespace', [None, 'custom-workload'])
def test_sample_cli_collects_pods_and_metrics_from_selected_namespace(tmp_path, monkeypatch, namespace):
    output = tmp_path / 'metrics.jsonl'
    selected = namespace or 'services'
    argv = ['monitor_resources.py', 'sample', str(output)]
    if namespace is not None:
        argv.extend(['--namespace', namespace])
    monkeypatch.setattr('sys.argv', argv)
    handlers = {}
    monkeypatch.setattr(monitor_resources.signal, 'signal', lambda signum, handler: handlers.update({signum: handler}))
    monkeypatch.setattr(monitor_resources.time, 'sleep',
                        lambda seconds: handlers[monitor_resources.signal.SIGTERM](None, None))
    pod_name = monitor_resources.PREFIX + 'abc'
    metric = {'metadata': {'name': pod_name}, 'containers': [
        {'name': 'app', 'usage': {'cpu': '125m', 'memory': '32Mi'}}]}
    calls = []

    def check_output(command, *, text, timeout):
        calls.append(command)
        assert command[:3] == ['kubectl', '-n', selected]
        if '--raw' in command:
            assert command[3:] == [
                'get', '--raw', f'/apis/metrics.k8s.io/v1beta1/namespaces/{selected}/pods']
            return json.dumps({'items': [metric]})
        assert command[3:] == ['get', 'pods', '-l', monitor_resources.LABEL, '-o', 'json']
        return json.dumps({'items': [{
            'metadata': {'name': pod_name}, 'spec': {'nodeName': 'gnb1'},
            'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]},
        }]})

    monkeypatch.setattr(monitor_resources.subprocess, 'check_output', check_output)
    monitor_resources.main()

    assert len(calls) == 2
    observation, = [json.loads(line) for line in output.read_text().splitlines()]
    assert observation['pods'] == [{'name': pod_name, 'node': 'gnb1', 'phase': 'Running', 'ready': True}]
    assert observation['metrics'] == {pod_name: metric}


def test_async_pod_metrics_across_observations_form_complete_sample(tmp_path):
    start = 1700000000
    events = [{'event': 'phase_start', 'phase': 'phase_01', 'start_time': start},
              {'event': 'phase_end', 'phase': 'phase_01', 'end_time': start + 60}]
    rows = [{'phase': 'phase_01', 'workload_ue': 2, 'gnb1_ues': 1, 'gnb2_ues': 1, 'gnb3_ues': 0,
             'latency_p50_ms': 10, 'latency_p90_ms': 20, 'attempts': 2, 'successes': 2,
             'failures': 0, 'start_time': start, 'end_time': start + 60}]
    requests = tmp_path / 'requests.jsonl'
    requests.write_text(''.join(json.dumps(e) + '\n' for e in events))
    summary = tmp_path / 'summary.csv'
    with summary.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0]); writer.writeheader(); writer.writerows(rows)

    pod_a = {'name': 'app-a', 'node': 'gnb1', 'phase': 'Running', 'ready': True}
    pod_b = {'name': 'app-b', 'node': 'gnb-2', 'phase': 'Running', 'ready': True}
    pod_c = {'name': 'app-c', 'node': 'gnb-3', 'phase': 'Running', 'ready': True}
    ts = lambda offset: datetime.fromtimestamp(start + offset, timezone.utc).isoformat()
    m = lambda offset, cpu: {'timestamp': ts(offset), 'window': '15s',
                             'containers': [{'name': 'app', 'usage': {'cpu': cpu, 'memory': '1024Ki'}}]}
    samples = [
        {'sample_time': start + 35, 'pods': [pod_a, pod_b], 'metrics': {'app-a': m(38, '100m'), 'app-b': m(20, '100m')}},
        {'sample_time': start + 50, 'pods': [pod_a, pod_b, pod_c], 'metrics': {'app-a': m(52, '300m'), 'app-b': m(42, '200m')}},
    ]
    data = tmp_path / 'metrics.jsonl'
    data.write_text(''.join(json.dumps(s) + '\n' for s in samples))
    summarize(data, requests, summary, tmp_path)
    with (tmp_path / 'resource-by-ramp.csv').open() as stream:
        row, = list(csv.DictReader(stream))
    assert row['complete_samples'] == '1'
    assert float(row['cpu_millicores']) == 300.0
    assert float(row['memory_mib']) == 2.0


def _write_single_phase(tmp_path, start=1700000000, duration=60):
    events = [{'event': 'phase_start', 'phase': 'phase_01', 'start_time': start},
              {'event': 'phase_end', 'phase': 'phase_01', 'end_time': start + duration}]
    rows = [{'phase': 'phase_01', 'workload_ue': 2, 'gnb1_ues': 1, 'gnb2_ues': 1, 'gnb3_ues': 0,
             'latency_p50_ms': 10, 'latency_p90_ms': 20, 'attempts': 2, 'successes': 2,
             'failures': 0, 'start_time': start, 'end_time': start + duration}]
    requests = tmp_path / 'requests.jsonl'
    requests.write_text(''.join(json.dumps(e) + '\n' for e in events))
    summary = tmp_path / 'summary.csv'
    with summary.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0]); writer.writeheader(); writer.writerows(rows)
    return requests, summary


def test_sample_preserves_metrics_for_recently_terminated_workload_pods(tmp_path, monkeypatch):
    output = tmp_path / 'metrics.jsonl'
    monkeypatch.setattr('sys.argv', ['monitor_resources.py', 'sample', str(output)])
    handlers = {}
    monkeypatch.setattr(monitor_resources.signal, 'signal', lambda signum, handler: handlers.update({signum: handler}))
    monkeypatch.setattr(monitor_resources.time, 'sleep',
                        lambda seconds: handlers[monitor_resources.signal.SIGTERM](None, None))
    pod_active = monitor_resources.PREFIX + 'active'
    pod_term = monitor_resources.PREFIX + 'terminating'
    m_active = {'metadata': {'name': pod_active}, 'containers': [{'name': 'app', 'usage': {'cpu': '100m', 'memory': '32Mi'}}]}
    m_term = {'metadata': {'name': pod_term}, 'containers': [{'name': 'app', 'usage': {'cpu': '200m', 'memory': '32Mi'}}]}
    m_other = {'metadata': {'name': 'unrelated-pod'}, 'containers': [{'name': 'app', 'usage': {'cpu': '50m', 'memory': '16Mi'}}]}

    def check_output(command, *, text, timeout):
        if '--raw' in command:
            return json.dumps({'items': [m_active, m_term, m_other]})
        return json.dumps({'items': [
            {'metadata': {'name': pod_active}, 'spec': {'nodeName': 'gnb1'},
             'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
            {'metadata': {'name': pod_term, 'deletionTimestamp': '2026-11-16T00:00:00Z'}, 'spec': {'nodeName': 'gnb-2'},
             'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
        ]})

    monkeypatch.setattr(monitor_resources.subprocess, 'check_output', check_output)
    monitor_resources.main()

    observation, = [json.loads(line) for line in output.read_text().splitlines()]
    assert [p['name'] for p in observation['pods']] == [pod_active]
    assert set(observation['metrics']) == {pod_active, pod_term}


def test_replacement_churn_recovers_complete_round_and_flags_partial_rounds(tmp_path):
    start = 1700000000
    requests, summary = _write_single_phase(tmp_path, start=start, duration=60)
    pod_a = {'name': 'app-a', 'node': 'gnb1', 'phase': 'Running', 'ready': True}
    pod_b = {'name': 'app-b', 'node': 'gnb-2', 'phase': 'Running', 'ready': True}
    pod_c = {'name': 'app-c', 'node': 'gnb-3', 'phase': 'Running', 'ready': True}
    ts = lambda offset: datetime.fromtimestamp(start + offset, timezone.utc).isoformat()
    m = lambda offset, cpu: {'timestamp': ts(offset), 'window': '15s',
                             'containers': [{'name': 'app', 'usage': {'cpu': cpu, 'memory': '1024Ki'}}]}
    samples = [
        {'sample_time': start + 15, 'pods': [pod_a, pod_b], 'metrics': {'app-a': m(15, '100m')}},
        {'sample_time': start + 22, 'pods': [pod_a, pod_c], 'metrics': {'app-a': m(15, '100m'), 'app-b': m(18, '250m')}},
        {'sample_time': start + 35, 'pods': [pod_a, pod_c], 'metrics': {'app-a': m(35, '400m')}},
    ]
    data = tmp_path / 'metrics.jsonl'
    data.write_text(''.join(json.dumps(s) + '\n' for s in samples))
    summarize(data, requests, summary, tmp_path)
    with (tmp_path / 'resource-by-ramp.csv').open() as stream:
        row, = list(csv.DictReader(stream))
    assert row['complete_samples'] == '1'
    assert float(row['cpu_millicores']) == 350.0
    assert float(row['memory_mib']) == 2.0
    assert row['partial_samples'] == '1'
    assert float(row['partial_cpu_millicores']) == 400.0
    assert row['completeness_status'] == 'partial'
    assert float(row['coverage_ratio']) == 0.5
    assert row['missing_reasons'] == 'missing_pod_metrics'


def test_skewed_pod_timestamps_never_combined_into_simultaneous_total(tmp_path):
    start = 1700000000
    requests, summary = _write_single_phase(tmp_path, start=start, duration=60)
    pod_a = {'name': 'app-a', 'node': 'gnb1', 'phase': 'Running', 'ready': True}
    pod_b = {'name': 'app-b', 'node': 'gnb-2', 'phase': 'Running', 'ready': True}
    ts = lambda offset: datetime.fromtimestamp(start + offset, timezone.utc).isoformat()
    m = lambda offset, cpu: {'timestamp': ts(offset), 'window': '15s',
                             'containers': [{'name': 'app', 'usage': {'cpu': cpu, 'memory': '1024Ki'}}]}
    samples = [
        {'sample_time': start + 10, 'pods': [pod_a, pod_b], 'metrics': {'app-a': m(10, '100m')}},
        {'sample_time': start + 35, 'pods': [pod_a, pod_b], 'metrics': {'app-b': m(35, '200m')}},
    ]
    data = tmp_path / 'metrics.jsonl'
    data.write_text(''.join(json.dumps(s) + '\n' for s in samples))
    summarize(data, requests, summary, tmp_path)
    with (tmp_path / 'resource-by-ramp.csv').open() as stream:
        row, = list(csv.DictReader(stream))
    assert row['complete_samples'] == '0'
    assert row['cpu_millicores'] == ''
    assert row['memory_mib'] == ''
    assert row['partial_samples'] == '2'
    assert row['completeness_status'] == 'incomplete_only'
    assert float(row['coverage_ratio']) == 0.0
    assert row['missing_reasons'] == 'missing_pod_metrics'


def test_mismatched_cpu_windows_rejected_and_zero_metrics_phase_is_missing(tmp_path):
    start = 1700000000
    events = [
        {'event': 'phase_start', 'phase': 'phase_01', 'start_time': start},
        {'event': 'phase_end', 'phase': 'phase_01', 'end_time': start + 60},
        {'event': 'phase_start', 'phase': 'phase_02', 'start_time': start + 60},
        {'event': 'phase_end', 'phase': 'phase_02', 'end_time': start + 120},
    ]
    rows = [
        {'phase': 'phase_01', 'workload_ue': 2, 'gnb1_ues': 1, 'gnb2_ues': 1, 'gnb3_ues': 0,
         'latency_p50_ms': 10, 'latency_p90_ms': 20, 'attempts': 2, 'successes': 2,
         'failures': 0, 'start_time': start, 'end_time': start + 60},
        {'phase': 'phase_02', 'workload_ue': 2, 'gnb1_ues': 1, 'gnb2_ues': 1, 'gnb3_ues': 0,
         'latency_p50_ms': 10, 'latency_p90_ms': 20, 'attempts': 2, 'successes': 2,
         'failures': 0, 'start_time': start + 60, 'end_time': start + 120},
    ]
    requests = tmp_path / 'requests.jsonl'
    requests.write_text(''.join(json.dumps(e) + '\n' for e in events))
    summary = tmp_path / 'summary.csv'
    with summary.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0]); writer.writeheader(); writer.writerows(rows)

    pod_a = {'name': 'app-a', 'node': 'gnb1', 'phase': 'Running', 'ready': True}
    pod_b = {'name': 'app-b', 'node': 'gnb-2', 'phase': 'Running', 'ready': True}
    ts = lambda offset: datetime.fromtimestamp(start + offset, timezone.utc).isoformat()
    samples = [
        # Phase 1: overlapping differing CPU windows (5.58s vs 32.59s) -> memory exact, CPU approximate.
        {'sample_time': start + 16, 'pods': [pod_a, pod_b], 'metrics': {
            'app-a': {'timestamp': ts(15), 'window': '5.58s', 'containers': [{'name': 'app', 'usage': {'cpu': '100m', 'memory': '1024Ki'}}]},
            'app-b': {'timestamp': ts(16), 'window': '32.59s', 'containers': [{'name': 'app', 'usage': {'cpu': '200m', 'memory': '1024Ki'}}]},
        }},
        # Phase 2: empty observations -> missing.
        {'sample_time': start + 80, 'pods': [pod_a, pod_b], 'metrics': {}},
    ]
    data = tmp_path / 'metrics.jsonl'
    data.write_text(''.join(json.dumps(s) + '\n' for s in samples))
    summarize(data, requests, summary, tmp_path)
    with (tmp_path / 'resource-by-ramp.csv').open() as stream:
        p1, p2 = list(csv.DictReader(stream))
    assert p1['memory_complete_samples'] == '1'
    assert float(p1['memory_mib']) == 2.0
    assert p1['cpu_complete_samples'] == '1'
    assert p1['cpu_approximate_samples'] == '1'
    assert p1['cpu_quality'] == 'approximate'
    assert float(p1['cpu_millicores']) == 300.0
    assert 'cpu_window_skew_approximate' in p1['missing_reasons'].split(';')

    assert p2['complete_samples'] == '0'
    assert p2['missing_samples'] == '1'
    assert p2['cpu_millicores'] == ''
    assert p2['memory_mib'] == ''
    assert p2['completeness_status'] == 'missing'
    assert p2['missing_reasons'] == 'empty_observations;no_metrics_in_phase'


def test_non_overlapping_cpu_windows_reject_cpu_while_preserving_memory(tmp_path):
    start = 1700000000
    requests, summary = _write_single_phase(tmp_path, start=start, duration=60)
    pod_a = {'name': 'app-a', 'node': 'gnb1', 'phase': 'Running', 'ready': True}
    pod_b = {'name': 'app-b', 'node': 'gnb-2', 'phase': 'Running', 'ready': True}
    ts = lambda offset: datetime.fromtimestamp(start + offset, timezone.utc).isoformat()
    samples = [
        # Windows [5, 10] and [13, 28] do not overlap in time, though timestamps are 8s apart.
        {'sample_time': start + 18, 'pods': [pod_a, pod_b], 'metrics': {
            'app-a': {'timestamp': ts(10), 'window': '5s', 'containers': [{'name': 'app', 'usage': {'cpu': '100m', 'memory': '1024Ki'}}]},
            'app-b': {'timestamp': ts(18), 'window': '5s', 'containers': [{'name': 'app', 'usage': {'cpu': '200m', 'memory': '1024Ki'}}]},
        }},
    ]
    data = tmp_path / 'metrics.jsonl'
    data.write_text(''.join(json.dumps(s) + '\n' for s in samples))
    summarize(data, requests, summary, tmp_path)
    with (tmp_path / 'resource-by-ramp.csv').open() as stream:
        row, = list(csv.DictReader(stream))
    assert row['memory_complete_samples'] == '1'
    assert float(row['memory_mib']) == 2.0
    assert row['memory_completeness_status'] == 'complete'
    assert row['cpu_complete_samples'] == '0'
    assert row['cpu_rejected_samples'] == '1'
    assert row['cpu_millicores'] == ''
    assert row['cpu_quality'] == 'missing'
    assert row['cpu_completeness_status'] == 'incomplete_only'
    assert 'non_overlapping_cpu_windows' in row['missing_reasons'].split(';')


def test_missing_intervals_and_errors_counted_in_availability_and_coverage(tmp_path):
    start = 1700000000
    requests, summary = _write_single_phase(tmp_path, start=start, duration=60)
    pod = {'name': 'a', 'node': 'gnb-3', 'phase': 'Running', 'ready': True}
    ts = lambda offset: datetime.fromtimestamp(start + offset, timezone.utc).isoformat()
    metric = {'timestamp': ts(5), 'window': '15s',
              'containers': [{'name': 'app', 'usage': {'cpu': '100m', 'memory': '1024Ki'}}]}
    samples = [{'sample_time': start + 5, 'pods': [pod], 'metrics': {'a': metric}}]
    samples += [{'sample_time': start + t, 'pods': [pod], 'metrics': {}} for t in (10, 20, 30, 40, 50, 59)]
    samples += [{'sample_time': start + 25, 'error': 'metrics API unavailable'}]
    data = tmp_path / 'metrics.jsonl'
    data.write_text(''.join(json.dumps(s) + '\n' for s in samples))
    summarize(data, requests, summary, tmp_path)
    with (tmp_path / 'resource-by-ramp.csv').open() as stream:
        row, = list(csv.DictReader(stream))
    assert row['complete_samples'] == '1'
    assert row['missing_samples'] == '7'
    assert row['empty_observations'] == '6'
    assert row['error_observations'] == '1'
    assert row['total_observations'] == '8'
    assert float(row['round_completeness_ratio']) == 1.0
    assert float(row['availability_ratio']) == 0.125
    assert float(row['coverage_ratio']) == 0.125
    assert row['completeness_status'] == 'partial'
    assert row['missing_reasons'] == 'empty_observations;sampler_errors'


def test_pod_membership_change_within_round_is_rejected(tmp_path):
    start = 1700000000
    requests, summary = _write_single_phase(tmp_path, start=start, duration=60)
    pod_a = {'name': 'a', 'node': 'gnb-3', 'phase': 'Running', 'ready': True}
    pod_b = {'name': 'b', 'node': 'gnb-3', 'phase': 'Running', 'ready': True}
    ts = lambda offset: datetime.fromtimestamp(start + offset, timezone.utc).isoformat()
    m = lambda offset: {'timestamp': ts(offset), 'window': '15s',
                        'containers': [{'name': 'app', 'usage': {'cpu': '100m', 'memory': '1024Ki'}}]}
    samples = [
        {'sample_time': start + 10, 'pods': [pod_a], 'metrics': {'a': m(10)}},
        {'sample_time': start + 15, 'pods': [pod_a, pod_b], 'metrics': {}},
        {'sample_time': start + 18, 'pods': [pod_b], 'metrics': {'b': m(18)}},
    ]
    data = tmp_path / 'metrics.jsonl'
    data.write_text(''.join(json.dumps(s) + '\n' for s in samples))
    summarize(data, requests, summary, tmp_path)
    with (tmp_path / 'resource-by-ramp.csv').open() as stream:
        row, = list(csv.DictReader(stream))
    assert row['complete_samples'] == '0'
    assert row['rejected_samples'] == '1'
    assert row['cpu_millicores'] == ''
    assert row['memory_mib'] == ''
    assert row['completeness_status'] == 'incomplete_only'
    assert float(row['coverage_ratio']) == 0.0
    assert 'pod_membership_changed_within_round' in row['missing_reasons'].split(';')


def test_sample_records_pod_inventory_even_when_metrics_server_fails(tmp_path, monkeypatch):
    import subprocess
    from utils.cadvisor_resources import _load_running_lookup

    output = tmp_path / 'metrics.jsonl'
    monkeypatch.setattr('sys.argv', ['monitor_resources.py', 'sample', str(output)])
    handlers = {}
    monkeypatch.setattr(monitor_resources.signal, 'signal', lambda signum, handler: handlers.update({signum: handler}))
    monkeypatch.setattr(monitor_resources.time, 'sleep',
                        lambda seconds: handlers[monitor_resources.signal.SIGTERM](None, None))
    pod_name = monitor_resources.PREFIX + 'healthy'

    def check_output(command, *, text, timeout):
        if '--raw' in command:
            raise subprocess.CalledProcessError(1, command, 'metrics server unavailable')
        return json.dumps({'items': [{
            'metadata': {'name': pod_name}, 'spec': {'nodeName': 'gnb-3'},
            'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]},
        }]})

    monkeypatch.setattr(monitor_resources.subprocess, 'check_output', check_output)
    monitor_resources.main()

    observation, = [json.loads(line) for line in output.read_text().splitlines()]
    assert observation['pods'] == [{'name': pod_name, 'node': 'gnb-3', 'phase': 'Running', 'ready': True}]
    assert observation['metrics'] == {}
    assert 'metrics_error' in observation
    lookup = _load_running_lookup(output)
    assert lookup(observation['sample_time'] + 1.0) == frozenset({pod_name})
