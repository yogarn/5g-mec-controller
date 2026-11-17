import json
import math

import pytest

from utils.resource_windows import build_resource_windows


def window(lower, upper):
    return dict(phase='phase_01', window_index=int(lower // 15), start_offset_s=lower,
                end_offset_s=upper, actual_start_time=1000 + lower, actual_end_time=1000 + upper)


def fixture(tmp_path):
    times = list(range(960, 1031, 5))
    def series(values):
        return {'metric': {'namespace': 'services', 'pod': 'app-a', 'container': 'app', 'id': 'old'},
                'values': values}
    payload = dict(pod_prefix='app-', step_seconds=5, rate_window_seconds=45, freshness_seconds=25,
                   results={
                       'cpu_seconds_total': [series([[t, (t - 960) * .1 if t <= 1015 else 5.5 + (t - 1015) * .5] for t in times])],
                       'memory_working_set_bytes': [series([[t, (128 if t <= 1015 else 256) * 1024**2] for t in times])],
                   })
    (tmp_path / 'resources-prometheus.json').write_text(json.dumps(payload))
    (tmp_path / 'metrics.jsonl').write_text(''.join(json.dumps({
        'sample_time': t, 'pods': [{'name': 'app-a', 'phase': 'Running', 'node': 'gnb-3', 'ready': True}],
    }) + '\n' for t in times))
    return payload


def test_resource_windows_use_raw_samples_not_phase_means(tmp_path):
    fixture(tmp_path)
    rows = build_resource_windows(tmp_path, [window(0, 15), window(15, 30)])
    assert rows[0]['cpu_millicores'] == pytest.approx(100)
    assert rows[1]['cpu_millicores'] == pytest.approx(188.8888889)
    assert [r['memory_mib'] for r in rows] == [128, 256]
    assert all(r['rate_window_seconds'] == 45 and r['cpu_coverage_percent'] == 100 for r in rows)


def test_missing_raw_resources_stay_missing(tmp_path):
    row = build_resource_windows(tmp_path, [window(0, 15)])[0]
    assert math.isnan(row['cpu_millicores']) and math.isnan(row['memory_mib'])
    assert row['cpu_coverage_percent'] == 0
    assert 'raw_cadvisor_data_unavailable' in row['missing_reasons']


def test_query_failure_does_not_hide_other_resource(tmp_path):
    payload = fixture(tmp_path)
    payload['query_errors'] = {'cpu_seconds_total': 'query failed'}
    (tmp_path / 'resources-prometheus.json').write_text(json.dumps(payload))
    row = build_resource_windows(tmp_path, [window(0, 15)])[0]
    assert math.isnan(row['cpu_millicores'])
    assert row['memory_mib'] == 128
    assert row['cpu_coverage_percent'] == 0 and row['memory_coverage_percent'] == 100


def test_stale_lifecycle_does_not_fabricate_resources(tmp_path):
    fixture(tmp_path)
    (tmp_path / 'metrics.jsonl').write_text(json.dumps({
        'sample_time': 980, 'pods': [{'name': 'app-a', 'phase': 'Running'}],
    }) + '\n')
    row = build_resource_windows(tmp_path, [window(0, 15)])[0]
    assert math.isnan(row['cpu_millicores']) and math.isnan(row['memory_mib'])
    assert row['cpu_complete_samples'] == 0


def test_restarted_container_does_not_reuse_old_cpu_rate(tmp_path):
    payload = fixture(tmp_path)
    new = {'metric': {'namespace': 'services', 'pod': 'app-a', 'container': 'app', 'id': 'new'},
           'values': [[1014, 0.01]]}
    payload['results']['cpu_seconds_total'][0]['values'] = [
        p for p in payload['results']['cpu_seconds_total'][0]['values'] if p[0] < 1014]
    payload['results']['cpu_seconds_total'].append(new)
    (tmp_path / 'resources-prometheus.json').write_text(json.dumps(payload))
    row = build_resource_windows(tmp_path, [window(10, 15)])[0]
    assert math.isnan(row['cpu_millicores'])
    assert row['cpu_coverage_percent'] == 0


def test_per_node_resources_use_saved_pod_placement_and_observed_empty_nodes(tmp_path):
    fixture(tmp_path)
    windows = [window(0, 15), window(15, 30)]
    fleet = build_resource_windows(tmp_path, windows)
    busy = build_resource_windows(tmp_path, windows, node='gnb-3')
    idle = build_resource_windows(tmp_path, windows, node='gnb-2')
    assert [r['cpu_millicores'] for r in busy] == [r['cpu_millicores'] for r in fleet]
    assert [r['memory_mib'] for r in busy] == [128, 256]
    assert [r['memory_mib'] for r in idle] == [0, 0]
    assert all(r['node'] == 'gnb-3' for r in busy)
    (tmp_path/'metrics.jsonl').write_text(json.dumps(dict(sample_time=1004, pods=[dict(name='app-a', phase='Running')]))+'\n')
    row = build_resource_windows(tmp_path, [window(0, 15)], node='gnb-3')[0]
    assert math.isnan(row['cpu_millicores']) and math.isnan(row['memory_mib'])
