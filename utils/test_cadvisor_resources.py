import csv
import json
from pathlib import Path

import pytest

from utils.cadvisor_resources import (
    RESOURCE_SOURCE,
    export_prometheus_resources,
    main as cadvisor_main,
    summarize_cadvisor,
)


def _setup_phases(tmp_path, phases, pod_timeline):
    """Write requests.jsonl, summary.csv, and metrics.jsonl for synthetic phase tests."""
    events = []
    rows = []
    for name, start, end in phases:
        events.append({'event': 'phase_start', 'phase': name, 'start_time': start})
        events.append({'event': 'phase_end', 'phase': name, 'end_time': end})
        rows.append({
            'phase': name, 'workload_ue': 3, 'gnb1_ues': 1, 'gnb2_ues': 1, 'gnb3_ues': 1,
            'latency_p50_ms': 10, 'latency_p90_ms': 20, 'attempts': 10, 'successes': 10,
            'failures': 0, 'start_time': start, 'end_time': end,
        })
    requests = tmp_path / 'requests.jsonl'
    requests.write_text(''.join(json.dumps(e) + '\n' for e in events))
    summary = tmp_path / 'summary.csv'
    with summary.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0])
        writer.writeheader()
        writer.writerows(rows)
    metrics_jsonl = tmp_path / 'metrics.jsonl'
    metrics_jsonl.write_text(''.join(json.dumps({
        'sample_time': ts,
        'pods': [{'name': p, 'node': 'gnb-3', 'phase': 'Running', 'ready': True} for p in pods],
        'metrics': {},
    }) + '\n' for ts, pods in pod_timeline))
    return requests, summary, metrics_jsonl


def _write_prom_json(path, cpu_series, mem_series, up_series=None, rate_window=45.0, freshness=25.0, step=5.0, query_errors=None):
    payload = {
        'source': RESOURCE_SOURCE,
        'pod_prefix': 'app-',
        'step_seconds': step,
        'rate_window_seconds': rate_window,
        'freshness_seconds': freshness,
        'scrape_interval_seconds': step,
        'results': {
            'cpu_seconds_total': cpu_series,
            'memory_working_set_bytes': mem_series,
            'scrape_up': up_series or [],
        },
        'query_errors': query_errors or {},
    }
    path.write_text(json.dumps(payload))
    return path


def _series(pod, container, values, instance_id='cid-1', extra=None):
    metric = {'namespace': 'services', 'pod': pod, 'container': container, 'id': instance_id, 'node': 'gnb-3'}
    if extra:
        metric.update(extra)
    return {'metric': metric, 'values': [[ts, str(val)] for ts, val in values]}


def test_constant_cpu_growth_and_memory_mib_conversion(tmp_path):
    start = 1700000000.0
    requests, summary, metrics_jsonl = _setup_phases(
        tmp_path,
        [('phase_01', start, start + 60)],
        [(start + t, ['app-a']) for t in range(-30, 65, 5)],
    )
    # 0.25 cores = 250m constant growth over [-30, 60].
    cpu_vals = [(start + t, (t + 30) * 0.25) for t in range(-30, 65, 5)]
    mem_vals = [(start + t, 128 * 1024 * 1024) for t in range(-30, 65, 5)]
    prom = _write_prom_json(
        tmp_path / 'resources-prometheus.json',
        [_series('app-a', 'app', cpu_vals)],
        [_series('app-a', 'app', mem_vals)],
    )
    out = tmp_path / 'out'
    summarize_cadvisor(prom, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    row, = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert row['resource_source'] == 'prometheus-cadvisor'
    assert row['rate_window_seconds'] == '45'
    assert row['freshness_seconds'] == '25'
    assert float(row['cpu_millicores']) == pytest.approx(250.0)
    assert float(row['memory_mib']) == pytest.approx(128.0)
    assert row['complete_samples'] == '12'
    assert row['expected_samples'] == '12'
    assert row['completeness_status'] == 'complete'


def test_counter_reset_and_container_restart_do_not_corrupt_or_double_count(tmp_path):
    start = 1700000000.0
    requests, summary, metrics_jsonl = _setup_phases(
        tmp_path,
        [('phase_01', start, start + 30)],
        [(start + t, ['app-a']) for t in range(0, 35, 5)],
    )
    # Counter reset at t=15 under same cgroup id:
    # t=0:0, t=5:1, t=10:2, t=15:0.5 (reset: +0.5), t=20:1.5, t=25:2.5, t=30:3.5.
    # Over 30s window [0, 30]: delta = 2 + 0.5 + 3 = 5.5 -> 5.5/30*1000 = 183.333m.
    cpu_vals = [
        (start + 0, 0.0), (start + 5, 1.0), (start + 10, 2.0),
        (start + 15, 0.5), (start + 20, 1.5), (start + 25, 2.5), (start + 30, 3.5),
    ]
    # Also simulate a restarted container instance for memory (old id at t=10: 50MiB, new id at t=15..30: 100MiB).
    # Must pick freshest container instance (100MiB), not 150MiB!
    mem_old = _series('app-a', 'app', [(start + 5, 50 * 1024**2), (start + 10, 50 * 1024**2)], instance_id='old')
    mem_new = _series('app-a', 'app', [(start + t, 100 * 1024**2) for t in range(15, 35, 5)], instance_id='new')
    prom = _write_prom_json(
        tmp_path / 'resources-prometheus.json',
        [_series('app-a', 'app', cpu_vals)],
        [mem_old, mem_new],
    )
    out = tmp_path / 'out'
    summarize_cadvisor(prom, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    row, = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert float(row['memory_mib']) == pytest.approx((50 * 2 + 100 * 4) / 6)
    assert row['cpu_complete_samples'] == '6'  # t=5,10,15,20,25,30
    # At t=30, rate over [0, 30] is 5.5/30*1000 = 183.333m; average over t=5..30 is positive and finite.
    assert float(row['cpu_millicores']) == pytest.approx((200.0 * 2 + (2.5 / 15) * 1000 + (3.5 / 20) * 1000 + (4.5 / 25) * 1000 + (5.5 / 30) * 1000) / 6, rel=1e-3)


def test_pod_replacement_and_duplicate_scrape_targets_handled_cleanly(tmp_path):
    start = 1700000000.0
    # app-old runs [0..15], replaced by app-new [15..30].
    timeline = [(start + t, ['app-old'] if t < 15 else ['app-new']) for t in range(0, 35, 5)]
    requests, summary, metrics_jsonl = _setup_phases(tmp_path, [('phase_01', start, start + 30)], timeline)
    # Duplicate series for app-old (two scrape targets) -> must dedupe, not double!
    cpu_old_1 = _series('app-old', 'app', [(start + t, t * 0.2) for t in (0, 5, 10)], extra={'instance': 'a'})
    cpu_old_2 = _series('app-old', 'app', [(start + t, t * 0.2) for t in (0, 5, 10)], extra={'instance': 'b'})
    cpu_new = _series('app-new', 'app', [(start + t, (t - 15) * 0.4) for t in (15, 20, 25, 30)])
    mem_old_1 = _series('app-old', 'app', [(start + t, 64 * 1024**2) for t in (0, 5, 10)], extra={'instance': 'a'})
    mem_old_2 = _series('app-old', 'app', [(start + t, 64 * 1024**2) for t in (0, 5, 10)], extra={'instance': 'b'})
    mem_new = _series('app-new', 'app', [(start + t, 128 * 1024**2) for t in (15, 20, 25, 30)])
    # Also include empty container & POD pause series that must be ignored if present in raw JSON.
    pause = {'metric': {'namespace': 'services', 'pod': 'app-old', 'container': 'POD'}, 'values': [[start + 5, '999']]}
    prom = _write_prom_json(
        tmp_path / 'resources-prometheus.json',
        [cpu_old_1, cpu_old_2, cpu_new, pause],
        [mem_old_1, mem_old_2, mem_new, pause],
    )
    out = tmp_path / 'out'
    summarize_cadvisor(prom, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    row, = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert int(row['duplicate_series_deduped']) > 0
    # At t=5,10 -> 200m; t=15 -> insufficient samples (app-new has 1 sample at t=15); t=20,25,30 -> 400m.
    assert row['cpu_complete_samples'] == '5'
    assert row['insufficient_rate_samples'] == '1'
    assert float(row['cpu_millicores']) == pytest.approx((200 * 2 + 400 * 3) / 5)
    # Memory is available at all 6 steps (t=5,10: 64MiB; t=15..30: 128MiB).
    assert row['memory_complete_samples'] == '6'
    assert float(row['memory_mib']) == pytest.approx((64 * 2 + 128 * 4) / 6)


def test_stale_data_insufficient_samples_and_memory_without_cpu(tmp_path):
    start = 1700000000.0
    requests, summary, metrics_jsonl = _setup_phases(
        tmp_path,
        [('phase_01', start, start + 40)],
        [(start + t, ['app-a']) for t in range(0, 45, 5)],
    )
    # CPU has only 1 sample at t=5 -> insufficient_samples_for_rate; later steps are stale/missing.
    # Memory is fresh at t=5..35 (within 25s of t=10) and stale at t=40 (30s > 25s).
    prom = _write_prom_json(
        tmp_path / 'resources-prometheus.json',
        [_series('app-a', 'app', [(start + 5, 1.0)])],
        [_series('app-a', 'app', [(start + 5, 64 * 1024**2), (start + 10, 64 * 1024**2)])],
    )
    out = tmp_path / 'out'
    summarize_cadvisor(prom, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    row, = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert row['cpu_millicores'] == ''
    assert row['cpu_complete_samples'] == '0'
    assert row['cpu_quality'] == 'missing'
    assert int(row['insufficient_rate_samples']) >= 1
    assert 'insufficient_samples_for_rate' in row['missing_reasons'].split(';')
    assert float(row['memory_mib']) == pytest.approx(64.0)
    assert row['memory_complete_samples'] == '7'  # t=5..35 within 25s of t=10; t=40 is stale
    assert row['memory_completeness_status'] == 'partial'


def test_partial_workload_coverage_never_zero_fills_or_claims_total(tmp_path):
    start = 1700000000.0
    requests, summary, metrics_jsonl = _setup_phases(
        tmp_path,
        [('phase_01', start, start + 20)],
        [(start + t, ['app-a', 'app-b']) for t in range(0, 25, 5)],
    )
    # Only app-a has metrics; app-b is missing completely.
    prom = _write_prom_json(
        tmp_path / 'resources-prometheus.json',
        [_series('app-a', 'app', [(start + t, t * 0.1) for t in range(0, 25, 5)])],
        [_series('app-a', 'app', [(start + t, 100 * 1024**2) for t in range(0, 25, 5)])],
    )
    out = tmp_path / 'out'
    summarize_cadvisor(prom, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    row, = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert row['cpu_millicores'] == ''
    assert row['memory_mib'] == ''
    assert row['complete_samples'] == '0'
    assert row['completeness_status'] == 'incomplete_only'
    assert float(row['partial_cpu_millicores']) == pytest.approx(100.0)
    assert float(row['partial_memory_mib']) == pytest.approx(100.0)


def test_phase_boundaries_extended_final_hold_and_missing_lifecycle(tmp_path):
    start = 1700000000.0
    # Phase 1: 60s, Phase 2 (final hold): 420s.
    timeline = [(start + t, ['app-a']) for t in range(0, 485, 5) if not (60 < t <= 90)]
    requests, summary, metrics_jsonl = _setup_phases(
        tmp_path,
        [('phase_01', start, start + 60), ('phase_02', start + 60, start + 480)],
        timeline,
    )
    cpu_vals = [(start + t, t * 0.2) for t in range(-30, 485, 5)]
    mem_vals = [(start + t, 50 * 1024**2) for t in range(-30, 485, 5)]
    prom = _write_prom_json(
        tmp_path / 'resources-prometheus.json',
        [_series('app-a', 'app', cpu_vals)],
        [_series('app-a', 'app', mem_vals)],
    )
    out = tmp_path / 'out'
    summarize_cadvisor(prom, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    p1, p2 = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert p1['expected_samples'] == '12'
    assert p1['complete_samples'] == '12'
    assert p2['expected_samples'] == '84'
    assert int(p2['missing_lifecycle_samples']) > 0
    assert 'missing_lifecycle_info' in p2['missing_reasons'].split(';')
    assert p2['completeness_status'] == 'partial'


def test_offline_export_saves_metadata_and_records_errors_without_fallback(tmp_path):
    start = 1700000000.0
    requests, summary, metrics_jsonl = _setup_phases(
        tmp_path, [('phase_01', start, start + 60)], [(start, ['app-a'])]
    )

    def failing_fetch(_url):
        raise OSError('connection refused')

    out = tmp_path / 'out'
    prom_json = export_prometheus_resources(
        requests, out, prometheus_url='http://127.0.0.1:30990',
        fetch_fn=failing_fetch, strict=False,
    )
    saved = json.loads(prom_json.read_text())
    assert saved['source'] == 'prometheus-cadvisor'
    assert saved['step_seconds'] == 5.0
    assert saved['rate_window_seconds'] == 45.0
    assert saved['freshness_seconds'] == 25.0
    assert saved['source_metadata']['fallback_used'] is False
    assert set(saved['query_errors']) == {
        'cpu_seconds_total', 'memory_working_set_bytes',
        'cpu_cfs_throttled_periods_total', 'cpu_cfs_periods_total',
        'cpu_cfs_throttled_seconds_total', 'node_cpu_idle_seconds_total',
        'scrape_up',
    }

    summarize_cadvisor(prom_json, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    row, = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert row['resource_source'] == 'prometheus-cadvisor'
    assert row['cpu_millicores'] == ''
    assert row['memory_mib'] == ''
    assert row['completeness_status'] == 'missing'
    assert 'prometheus_query_error' in row['missing_reasons'].split(';')


def test_export_uses_raw_range_vector_query_for_exact_scrape_timestamps(tmp_path):
    start = 1700000000.0
    requests, _, _ = _setup_phases(tmp_path, [('phase_01', start, start + 60)], [(start, ['app-a'])])
    seen_urls = []

    def fake_fetch(url):
        seen_urls.append(url)
        return {'status': 'success', 'data': {'resultType': 'matrix', 'result': []}}

    out = tmp_path / 'out'
    prom_json = export_prometheus_resources(requests, out, fetch_fn=fake_fetch)
    saved = json.loads(prom_json.read_text())
    assert saved['query_type'] == 'raw_range_vector_at_end'
    assert len(seen_urls) == 7
    assert all('/api/v1/query?' in u and '/api/v1/query_range' not in u for u in seen_urls)
    assert saved['queries']['cpu_seconds_total'].endswith('[115s]')


def test_restarted_container_never_inherits_old_instance_cpu_rate(tmp_path):
    start = 1700000000.0
    requests, summary, metrics_jsonl = _setup_phases(
        tmp_path,
        [('phase_01', start, start + 30)],
        [(start + t, ['app-a']) for t in range(0, 35, 5)],
    )
    # Old instance runs t=0..15 at 250m; new instance starts at t=20 (only 1 sample at t=20 -> insufficient!)
    # and runs at 500m for t=25, 30. At t=20, summarizer MUST NOT fall back to old instance's 250m!
    cpu_old = _series('app-a', 'app', [(start + t, t * 0.25) for t in (0, 5, 10, 15)], instance_id='old')
    cpu_new = _series('app-a', 'app', [(start + 20, 0.0), (start + 25, 2.5), (start + 30, 5.0)], instance_id='new')
    mem_old = _series('app-a', 'app', [(start + t, 64 * 1024**2) for t in (0, 5, 10, 15)], instance_id='old')
    mem_new = _series('app-a', 'app', [(start + t, 128 * 1024**2) for t in (20, 25, 30)], instance_id='new')
    prom = _write_prom_json(
        tmp_path / 'resources-prometheus.json',
        [cpu_old, cpu_new],
        [mem_old, mem_new],
    )
    out = tmp_path / 'out'
    summarize_cadvisor(prom, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    row, = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert row['cpu_complete_samples'] == '5'
    assert row['insufficient_rate_samples'] == '1'
    assert float(row['cpu_millicores']) == pytest.approx((250 * 3 + 500 * 2) / 5)
    assert row['memory_complete_samples'] == '6'
    assert float(row['memory_mib']) == pytest.approx((64 * 3 + 128 * 3) / 6)


def test_staggered_10_to_20s_source_samples_with_stable_nine_pod_inventory(tmp_path):
    start = 1700000000.0
    pods = [f'app-{i}' for i in range(1, 10)]
    requests, summary, metrics_jsonl = _setup_phases(
        tmp_path,
        [('phase_01', start, start + 60)],
        [(start + t, pods) for t in range(-45, 65, 5)],
    )
    # Staggered 10-20s cAdvisor timestamps across 9 pods (e.g. 16s spacing with per-pod offsets).
    # With freshness=25s and rate_window=45s, all 12 phase steps have complete CPU & RAM.
    cpu_series = []
    mem_series = []
    for idx, pod in enumerate(pods):
        offset = (idx * 3) % 16
        ts_list = [start + t for t in range(-48 + offset, 65, 16)]
        cpu_series.append(_series(pod, 'app', [(ts, (ts - (start - 48)) * 0.1) for ts in ts_list]))
        mem_series.append(_series(pod, 'app', [(ts, 64 * 1024**2) for ts in ts_list]))
    prom = _write_prom_json(tmp_path / 'resources-prometheus.json', cpu_series, mem_series)
    out = tmp_path / 'out'
    summarize_cadvisor(prom, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    row, = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert row['rate_window_seconds'] == '45'
    assert row['freshness_seconds'] == '25'
    assert row['memory_complete_samples'] == '12'
    assert row['cpu_complete_samples'] == '12'
    assert row['completeness_status'] == 'complete'
    assert float(row['cpu_millicores']) == pytest.approx(900.0)
    assert float(row['memory_mib']) == pytest.approx(9 * 64.0)


def test_staggered_pod_replacement_preserves_incomplete_cpu_after_replacement(tmp_path):
    start = 1700000000.0
    stable_pods = [f'app-{i}' for i in range(1, 9)]
    # app-9 replaced by app-10 at t=25.
    timeline = [
        (start + t, stable_pods + (['app-9'] if t < 25 else ['app-10']))
        for t in range(-45, 65, 5)
    ]
    requests, summary, metrics_jsonl = _setup_phases(tmp_path, [('phase_01', start, start + 60)], timeline)
    cpu_series = []
    mem_series = []
    for idx, pod in enumerate(stable_pods):
        ts_list = [start + t for t in range(-45 + (idx % 3) * 4, 65, 15)]
        cpu_series.append(_series(pod, 'app', [(ts, (ts - (start - 45)) * 0.1) for ts in ts_list]))
        mem_series.append(_series(pod, 'app', [(ts, 50 * 1024**2) for ts in ts_list]))
    # app-9 stops at t=20; app-10 gets first sample at t=30 (1 sample -> insufficient at t=30, 35, 40)
    # and second sample at t=45 (rate valid from t=45..60). Memory is valid at t=25? Wait: app-10 mem starts at t=25!
    cpu_series.append(_series('app-9', 'app', [(start + t, (t + 45) * 0.1) for t in range(-45, 25, 15)]))
    cpu_series.append(_series('app-10', 'app', [(start + 30, 0.0), (start + 45, 3.0), (start + 60, 6.0)]))
    mem_series.append(_series('app-9', 'app', [(start + t, 50 * 1024**2) for t in range(-45, 25, 15)]))
    mem_series.append(_series('app-10', 'app', [(start + t, 50 * 1024**2) for t in (25, 40, 55)]))
    prom = _write_prom_json(tmp_path / 'resources-prometheus.json', cpu_series, mem_series)
    out = tmp_path / 'out'
    summarize_cadvisor(prom, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    row, = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert row['memory_complete_samples'] == '12'
    assert int(row['cpu_complete_samples']) < 12
    assert int(row['insufficient_rate_samples']) > 0
    assert row['cpu_completeness_status'] == 'partial'


def test_offline_reprocessing_with_cpu_window_override_and_historical_file_compat(tmp_path):
    start = 1700000000.0
    requests, summary, metrics_jsonl = _setup_phases(
        tmp_path, [('phase_01', start, start + 60)], [(start + t, ['app-a']) for t in range(-45, 65, 5)]
    )
    # Historical file written with 30s rate_window and NO freshness_seconds field.
    hist_path = tmp_path / 'resources-prometheus.json'
    hist_path.write_text(json.dumps({
        'source': RESOURCE_SOURCE,
        'pod_prefix': 'app-',
        'step_seconds': 5.0,
        'rate_window_seconds': 30.0,
        'scrape_interval_seconds': 5.0,
        'results': {
            'cpu_seconds_total': [_series('app-a', 'app', [(start + t, (t + 45) * 0.2) for t in range(-45, 65, 20)])],
            'memory_working_set_bytes': [_series('app-a', 'app', [(start + t, 100 * 1024**2) for t in range(-45, 65, 20)])],
            'scrape_up': [],
        },
        'query_errors': {},
    }))
    reprocessed_dir = tmp_path / 'reprocessed'
    cadvisor_main([
        'summarize', str(hist_path), str(requests), str(summary), str(reprocessed_dir),
        '--metrics-jsonl', str(metrics_jsonl), '--rate-window', '45', '--freshness', '25',
    ])
    row, = list(csv.DictReader((reprocessed_dir / 'resource-by-ramp.csv').open()))
    assert row['rate_window_seconds'] == '45'
    assert row['freshness_seconds'] == '25'
    assert row['cpu_complete_samples'] == '12'
    assert row['memory_complete_samples'] == '12'
    assert float(row['cpu_millicores']) == pytest.approx(200.0)


def test_throttling_aggregate_ratio_with_reset_and_missing_data(tmp_path):
    requests = tmp_path / 'requests.jsonl'
    requests.write_text(
        json.dumps({'event': 'phase_start', 'phase': 'p1', 'start_time': 100.0}) + '\n'
        + json.dumps({'event': 'phase_end', 'phase': 'p1', 'end_time': 160.0}) + '\n'
    )
    summary = tmp_path / 'summary.csv'
    summary.write_text('phase\np1\n')
    metrics_jsonl = tmp_path / 'metrics.jsonl'
    metrics_jsonl.write_text(
        json.dumps({'sample_time': 130.0, 'pods': [{'name': 'mec-placement-face-detection-a', 'phase': 'Running'}]}) + '\n'
    )

    # Container 1: 100->120 throttled (+20), 200->400 periods (+200).
    # Container 2: reset 50->10 throttled (+10), 300->500 periods (+200).
    # Aggregate ratio = (20 + 10) / (200 + 200) = 30 / 400 = 0.075 (NOT avg of 0.1 and 0.05).
    prom_json = tmp_path / 'resources-prometheus.json'
    prom_json.write_text(json.dumps({
        'pod_prefix': 'mec-placement-face-detection-',
        'step_seconds': 5.0,
        'rate_window_seconds': 45.0,
        'freshness_seconds': 25.0,
        'scrape_interval_seconds': 5.0,
        'results': {
            'cpu_seconds_total': [],
            'memory_working_set_bytes': [],
            'cpu_cfs_throttled_periods_total': [
                {'metric': {'namespace': 'services', 'pod': 'mec-placement-face-detection-a', 'container': 'app', 'id': 'c1'},
                 'values': [[105.0, '100'], [155.0, '120']]},
                {'metric': {'namespace': 'services', 'pod': 'mec-placement-face-detection-b', 'container': 'app', 'id': 'c2'},
                 'values': [[105.0, '50'], [155.0, '10']]},
            ],
            'cpu_cfs_periods_total': [
                {'metric': {'namespace': 'services', 'pod': 'mec-placement-face-detection-a', 'container': 'app', 'id': 'c1'},
                 'values': [[105.0, '200'], [155.0, '400']]},
                {'metric': {'namespace': 'services', 'pod': 'mec-placement-face-detection-b', 'container': 'app', 'id': 'c2'},
                 'values': [[105.0, '300'], [155.0, '500']]},
            ],
            'cpu_cfs_throttled_seconds_total': [
                {'metric': {'namespace': 'services', 'pod': 'mec-placement-face-detection-a', 'container': 'app', 'id': 'c1'},
                 'values': [[105.0, '1.0'], [155.0, '2.5']]},
            ],
            'node_cpu_idle_seconds_total': [
                {'metric': {'node': 'gnb-3', 'cpu': '0'}, 'values': [[105.0, '1000.0'], [155.0, '1030.0']]},
            ],
            'scrape_up': [],
        },
    }))
    out = tmp_path / 'out_with_thr'
    summarize_cadvisor(prom_json, requests, summary, out, metrics_jsonl_path=metrics_jsonl)
    row, = list(csv.DictReader((out / 'resource-by-ramp.csv').open()))
    assert float(row['throttled_period_ratio']) == pytest.approx(0.075)
    assert float(row['cfs_throttled_periods']) == pytest.approx(30.0)
    assert float(row['cfs_periods']) == pytest.approx(400.0)
    assert float(row['cfs_throttled_seconds']) == pytest.approx(1.5)
    # 50s elapsed, 30s idle -> 0.40 CPU utilization.
    assert float(row['node_cpu_max_utilization']) == pytest.approx(0.4)
    assert json.loads(row['node_cpu_utilization'])['gnb-3'] == pytest.approx(0.4)

    # Missing throttling series must be blank (''), never '0'.
    prom_missing = tmp_path / 'resources-missing.json'
    prom_missing.write_text(json.dumps({
        'pod_prefix': 'mec-placement-face-detection-',
        'step_seconds': 5.0,
        'results': {'cpu_seconds_total': [], 'memory_working_set_bytes': [], 'scrape_up': []},
    }))
    out_missing = tmp_path / 'out_missing'
    summarize_cadvisor(prom_missing, requests, summary, out_missing, metrics_jsonl_path=metrics_jsonl)
    row_m, = list(csv.DictReader((out_missing / 'resource-by-ramp.csv').open()))
    assert row_m['throttled_period_ratio'] == ''
    assert row_m['cfs_throttled_seconds'] == ''
    assert row_m['node_cpu_max_utilization'] == ''
    assert 'missing_throttling_series' in row_m['missing_reasons']
