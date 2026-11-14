import csv
import json
from datetime import datetime, timezone

from experiments.monitor_resources import summarize


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
