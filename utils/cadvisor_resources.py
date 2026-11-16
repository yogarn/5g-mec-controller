#!/usr/bin/env python3
"""Export and summarize Prometheus/cAdvisor CPU and memory usage per UE phase.

Uses kubelet /metrics/cadvisor series scraped by Prometheus:
- container_cpu_usage_seconds_total (per-container-instance rate with reset handling)
- container_memory_working_set_bytes (point-in-time working set)

Boundary note: a 45-second CPU rate window can span 60-second phase transitions,
and new containers require at least two samples in the window before a rate
exists. Overlapping rate averages are averages of valid observations, not exact
phase CPU energy or consumption.
"""
import argparse
import csv
import json
import math
from bisect import bisect_right
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

RESOURCE_SOURCE = 'prometheus-cadvisor'
DEFAULT_PROMETHEUS_URL = 'http://10.34.211.140:30990'
DEFAULT_SCRAPE_INTERVAL_S = 5.0
DEFAULT_RATE_WINDOW_S = 45.0
DEFAULT_FRESHNESS_S = 25.0
POD_LOOKUP_MAX_AGE_S = 8.0


def build_queries(namespace='services', pod_prefix='mec-placement-face-detection-', job='kubelet-cadvisor'):
    selector = f'job="{job}",namespace="{namespace}",pod=~"{pod_prefix}.*",container!="",container!="POD"'
    return {
        'cpu_seconds_total': f'container_cpu_usage_seconds_total{{{selector}}}',
        'memory_working_set_bytes': f'container_memory_working_set_bytes{{{selector}}}',
        'cpu_cfs_throttled_periods_total': f'container_cpu_cfs_throttled_periods_total{{{selector}}}',
        'cpu_cfs_periods_total': f'container_cpu_cfs_periods_total{{{selector}}}',
        'cpu_cfs_throttled_seconds_total': f'container_cpu_cfs_throttled_seconds_total{{{selector}}}',
        'node_cpu_idle_seconds_total': 'node_cpu_seconds_total{mode="idle"}',
        'scrape_up': f'up{{job="{job}"}}',
    }


def _fetch_json(url, timeout=15):
    with urlopen(url, timeout=timeout) as response:
        return json.load(response)


def _load_phases(requests_path):
    records = [json.loads(line) for line in Path(requests_path).read_text().splitlines() if line.strip()]
    starts = {r['phase']: r for r in records if r.get('event') == 'phase_start'}
    ends = {r['phase']: r for r in records if r.get('event') == 'phase_end'}
    if not starts or set(starts) != set(ends):
        raise ValueError('incomplete or mismatched phase records in requests.jsonl')
    return starts, ends


def export_prometheus_resources(
    requests_path,
    output_dir,
    prometheus_url=DEFAULT_PROMETHEUS_URL,
    namespace='services',
    pod_prefix='mec-placement-face-detection-',
    step_s=DEFAULT_SCRAPE_INTERVAL_S,
    rate_window_s=DEFAULT_RATE_WINDOW_S,
    scrape_interval_s=DEFAULT_SCRAPE_INTERVAL_S,
    freshness_s=DEFAULT_FRESHNESS_S,
    fetch_fn=None,
    strict=False,
):
    """Query Prometheus range history for the full method and save resources-prometheus.json."""
    starts, ends = _load_phases(requests_path)
    lower = min(float(x['start_time']) for x in starts.values()) - float(rate_window_s) - float(step_s)
    upper = max(float(x['end_time']) for x in ends.values()) + float(step_s)
    queries = build_queries(namespace=namespace, pod_prefix=pod_prefix)
    span_s = max(1, int(math.ceil(upper - lower)))
    range_queries = {key: f'{query}[{span_s}s]' for key, query in queries.items()}
    fetcher = fetch_fn or _fetch_json
    results = {}
    query_errors = {}
    base = prometheus_url.rstrip('/')
    for key, query in range_queries.items():
        # Querying /api/v1/query with a range selector returns exact raw TSDB
        # scrape timestamps rather than query_range step-repeated lookback values.
        url = base + '/api/v1/query?' + urlencode({
            'query': query,
            'time': upper,
        })
        try:
            data = fetcher(url)
            if data.get('status') != 'success' or data.get('data', {}).get('resultType') != 'matrix':
                raise ValueError(f'invalid Prometheus matrix response for {key}')
            results[key] = data['data']['result']
        except (OSError, URLError, ValueError, KeyError, TypeError) as exc:
            if strict:
                raise
            results[key] = []
            query_errors[key] = str(exc)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    payload = {
        'source': RESOURCE_SOURCE,
        'query_type': 'raw_range_vector_at_end',
        'prometheus_url': prometheus_url,
        'namespace': namespace,
        'pod_prefix': pod_prefix,
        'step_seconds': float(step_s),
        'rate_window_seconds': float(rate_window_s),
        'freshness_seconds': float(freshness_s),
        'scrape_interval_seconds': float(scrape_interval_s),
        'start': lower,
        'end': upper,
        'units': {
            'cpu': 'millicores (rate from cumulative CPU seconds)',
            'memory': 'MiB (from container_memory_working_set_bytes)',
        },
        'queries': range_queries,
        'results': results,
        'query_errors': query_errors,
        'source_metadata': {
            'primary_source': RESOURCE_SOURCE,
            'fallback_used': False,
        },
    }
    target = out / 'resources-prometheus.json'
    target.write_text(json.dumps(payload, indent=2) + '\n', encoding='utf-8')
    return target


def _dedupe_series(matrix, pod_prefix):
    """Group series by container instance key, deduplicating duplicate scrape targets."""
    by_instance = {}
    pod_of = {}
    container_of = {}
    duplicates = 0
    for series in matrix:
        metric = series.get('metric') or {}
        pod = metric.get('pod', '')
        container = metric.get('container', '')
        if not pod.startswith(pod_prefix) or container in ('', 'POD'):
            continue
        instance_id = metric.get('id') or metric.get('container_id') or ''
        ckey = (metric.get('namespace', ''), pod, container, instance_id)
        pod_of[ckey] = pod
        container_of[ckey] = (pod, container)
        ts_map = by_instance.setdefault(ckey, {})
        for raw_ts, raw_val in series.get('values', []):
            ts = float(raw_ts)
            val = float(raw_val)
            if not math.isfinite(val):
                continue
            if ts in ts_map:
                duplicates += 1
            else:
                ts_map[ts] = val
    cleaned = {ckey: sorted(ts_map.items()) for ckey, ts_map in by_instance.items()}
    return cleaned, pod_of, container_of, duplicates


def _counter_delta_in_phase(samples, start, end):
    """Return counter delta within [start, end] with reset handling, or None if <2 samples."""
    in_phase = [(ts, val) for ts, val in samples if start <= ts <= end]
    if len(in_phase) < 2 or in_phase[-1][0] <= in_phase[0][0]:
        return None
    delta = 0.0
    for (_, prev_v), (_, curr_v) in zip(in_phase, in_phase[1:]):
        if curr_v >= prev_v:
            delta += curr_v - prev_v
        else:
            delta += curr_v
    return delta


def _node_cpu_utilization_in_phase(node_idle_series, start, end):
    """Compute per-node CPU utilization (0..1) from node_cpu_seconds_total{mode='idle'}."""
    by_node_cpu = {}
    for series in node_idle_series or []:
        metric = series.get('metric') or {}
        node = metric.get('node') or metric.get('instance') or ''
        cpu = metric.get('cpu', '0')
        if not node:
            continue
        samples = []
        for raw_ts, raw_val in series.get('values', []):
            ts = float(raw_ts)
            val = float(raw_val)
            if math.isfinite(val):
                samples.append((ts, val))
        samples.sort()
        in_phase = [(ts, val) for ts, val in samples if start <= ts <= end]
        if len(in_phase) < 2 or in_phase[-1][0] <= in_phase[0][0]:
            continue
        dt = in_phase[-1][0] - in_phase[0][0]
        delta = _counter_delta_in_phase(samples, start, end)
        if delta is None or dt <= 0:
            continue
        idle_frac = max(0.0, min(1.0, delta / dt))
        by_node_cpu.setdefault(node, []).append(1.0 - idle_frac)
    return {node: sum(vals) / len(vals) for node, vals in sorted(by_node_cpu.items()) if vals}


def _rate_millicores_at(samples, t_eval, rate_window_s, freshness_s):
    """Compute per-container CPU rate in millicores over [t_eval - rate_window_s, t_eval]."""
    in_window = [(ts, val) for ts, val in samples if t_eval - rate_window_s <= ts <= t_eval]
    if not in_window or (t_eval - in_window[-1][0]) > freshness_s:
        return None, None, 'stale_or_missing'
    if len(in_window) < 2 or in_window[-1][0] <= in_window[0][0]:
        return None, None, 'insufficient_samples'
    dt = in_window[-1][0] - in_window[0][0]
    delta = 0.0
    for (_, prev_v), (_, curr_v) in zip(in_window, in_window[1:]):
        if curr_v >= prev_v:
            delta += curr_v - prev_v
        else:
            # Counter reset (e.g. container restart under same cgroup id).
            delta += curr_v
    return (delta / dt) * 1000.0, in_window[-1][0], 'ok'


def _memory_mib_at(samples, t_eval, freshness_s):
    """Return latest working-set memory in MiB if fresh at t_eval."""
    prior = [(ts, val) for ts, val in samples if ts <= t_eval]
    if not prior or (t_eval - prior[-1][0]) > freshness_s:
        return None, None, 'stale_or_missing'
    return prior[-1][1] / (1024.0 * 1024.0), prior[-1][0], 'ok'


def _load_running_lookup(metrics_jsonl_path):
    if metrics_jsonl_path is None or not Path(metrics_jsonl_path).exists():
        return lambda _when: None
    samples = [json.loads(line) for line in Path(metrics_jsonl_path).read_text().splitlines() if line.strip()]
    valid = sorted((s for s in samples if 'pods' in s), key=lambda s: s['sample_time'])
    sample_times = [s['sample_time'] for s in valid]

    def running_at(when):
        index = bisect_right(sample_times, when) - 1
        if index < 0 or when - sample_times[index] > POD_LOOKUP_MAX_AGE_S:
            return None
        return frozenset(p['name'] for p in valid[index]['pods'] if p['phase'] == 'Running')

    return running_at


def summarize_cadvisor(
    prometheus_json_path,
    requests_path,
    summary_csv_path,
    output_dir,
    metrics_jsonl_path=None,
    rate_window_s=None,
    freshness_s=None,
):
    """Summarize exported Prometheus/cAdvisor series aligned to UE phases."""
    prom = json.loads(Path(prometheus_json_path).read_text())
    starts, ends = _load_phases(requests_path)
    with Path(summary_csv_path).open(newline='') as stream:
        summary = list(csv.DictReader(stream))
    if {r['phase'] for r in summary} != set(starts):
        raise ValueError('summary.csv phases do not match requests.jsonl')

    pod_prefix = prom.get('pod_prefix', 'mec-placement-face-detection-')
    step_s = float(prom.get('step_seconds', DEFAULT_SCRAPE_INTERVAL_S))
    rate_window_s = float(rate_window_s if rate_window_s is not None else prom.get('rate_window_seconds', DEFAULT_RATE_WINDOW_S))
    freshness_s = float(freshness_s if freshness_s is not None else prom.get('freshness_seconds', DEFAULT_FRESHNESS_S))
    scrape_interval_s = float(prom.get('scrape_interval_seconds', DEFAULT_SCRAPE_INTERVAL_S))
    results = prom.get('results') or {}
    query_errors = prom.get('query_errors') or {}

    cpu_series, _, cpu_container_of, cpu_dups = _dedupe_series(results.get('cpu_seconds_total', []), pod_prefix)
    mem_series, _, mem_container_of, mem_dups = _dedupe_series(results.get('memory_working_set_bytes', []), pod_prefix)
    thr_periods_series, _, thr_container_of, _ = _dedupe_series(results.get('cpu_cfs_throttled_periods_total', []), pod_prefix)
    periods_series, _, periods_container_of, _ = _dedupe_series(results.get('cpu_cfs_periods_total', []), pod_prefix)
    thr_seconds_series, _, _, _ = _dedupe_series(results.get('cpu_cfs_throttled_seconds_total', []), pod_prefix)
    node_idle_series = results.get('node_cpu_idle_seconds_total', [])
    scrape_up_series = results.get('scrape_up', [])
    running_at = _load_running_lookup(metrics_jsonl_path)

    fields = [
        'phase', 'resource_source', 'rate_window_seconds', 'freshness_seconds', 'scrape_interval_seconds',
        'cpu_millicores', 'memory_mib',
        'throttled_period_ratio', 'cfs_throttled_periods', 'cfs_periods', 'cfs_throttled_seconds',
        'throttling_completeness_status', 'throttling_coverage_ratio',
        'node_cpu_max_utilization', 'node_cpu_utilization',
        'complete_samples',
        'memory_complete_samples', 'cpu_complete_samples',
        'partial_samples', 'memory_partial_samples', 'cpu_partial_samples',
        'missing_samples', 'expected_samples',
        'insufficient_rate_samples', 'missing_lifecycle_samples',
        'scrape_down_samples', 'duplicate_series_deduped', 'query_error_count',
        'coverage_ratio', 'memory_coverage_ratio', 'cpu_coverage_ratio',
        'partial_cpu_millicores', 'partial_memory_mib',
        'completeness_status', 'memory_completeness_status', 'cpu_completeness_status',
        'cpu_quality', 'missing_reasons',
        'first_metric_time', 'last_metric_time', 'description',
    ]

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    target = out / 'resource-by-ramp.csv'
    with target.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in summary:
            phase = row['phase']
            start = float(starts[phase]['start_time'])
            end = float(ends[phase]['end_time'])

            eval_times = []
            t_cur = start + step_s
            while t_cur <= end + 1e-9:
                eval_times.append(t_cur)
                t_cur += step_s
            if not eval_times:
                eval_times = [end]

            mem_full = []
            cpu_full = []
            mem_partial = []
            cpu_partial = []
            mem_missing = 0
            cpu_missing = 0
            insufficient_rate = 0
            missing_lifecycle = 0
            expected_thr_pods = set()
            reasons = set()
            if query_errors:
                reasons.add('prometheus_query_error')
            if not cpu_series:
                reasons.add('no_cadvisor_cpu_series')
            if not mem_series:
                reasons.add('no_cadvisor_memory_series')

            scrape_down = sum(
                1
                for series in scrape_up_series
                for ts, val in series.get('values', [])
                if start <= float(ts) <= end and float(val) != 1.0
            )
            if scrape_down:
                reasons.add('scrape_target_down')

            for t_eval in eval_times:
                running_pods = running_at(t_eval)
                if running_pods is None:
                    missing_lifecycle += 1
                    reasons.add('missing_lifecycle_info')
                    continue
                if not running_pods:
                    continue
                expected_thr_pods.update(running_pods)

                # Select the current container instance per (pod, container) before validating
                # samples so a restarted container never falls back to the old instance's rate.
                def _current_instances(series_dict, container_of):
                    selected = {}
                    for ckey, samples in series_dict.items():
                        pod, cname = container_of[ckey]
                        if pod not in running_pods:
                            continue
                        latest_ts = None
                        for ts, _ in samples:
                            if ts <= t_eval:
                                latest_ts = ts
                            else:
                                break
                        if latest_ts is None:
                            continue
                        rank = (latest_ts, samples[0][0])
                        prev = selected.get((pod, cname))
                        if prev is None or rank > prev[0]:
                            selected[(pod, cname)] = (rank, samples)
                    return {key: item[1] for key, item in selected.items()}

                best_mem = {}
                for (pod, cname), samples in _current_instances(mem_series, mem_container_of).items():
                    val, sample_ts, state = _memory_mib_at(samples, t_eval, freshness_s)
                    if state == 'ok':
                        best_mem[(pod, cname)] = (val, sample_ts)
                fresh_mem_pods = {pod for pod, _ in best_mem}
                if running_pods <= fresh_mem_pods:
                    mem_full.append((sum(v[0] for v in best_mem.values()), t_eval))
                elif fresh_mem_pods & running_pods:
                    mem_partial.append(sum(v[0] for v in best_mem.values()))
                    reasons.add('partial_memory_coverage')
                else:
                    mem_missing += 1
                    reasons.add('missing_memory_samples')

                best_cpu = {}
                saw_insufficient = False
                for (pod, cname), samples in _current_instances(cpu_series, cpu_container_of).items():
                    val, sample_ts, state = _rate_millicores_at(samples, t_eval, rate_window_s, freshness_s)
                    if state == 'ok':
                        best_cpu[(pod, cname)] = (val, sample_ts)
                    elif state == 'insufficient_samples':
                        saw_insufficient = True
                fresh_cpu_pods = {pod for pod, _ in best_cpu}
                if running_pods <= fresh_cpu_pods:
                    cpu_full.append((sum(v[0] for v in best_cpu.values()), t_eval))
                elif fresh_cpu_pods & running_pods:
                    cpu_partial.append(sum(v[0] for v in best_cpu.values()))
                    reasons.add('partial_cpu_coverage')
                    if saw_insufficient:
                        insufficient_rate += 1
                        reasons.add('insufficient_samples_for_rate')
                else:
                    cpu_missing += 1
                    if saw_insufficient:
                        insufficient_rate += 1
                        reasons.add('insufficient_samples_for_rate')
                    else:
                        reasons.add('missing_cpu_samples')

            expected = len(eval_times)
            mem_cov = len(mem_full) / expected if expected else 0.0
            cpu_cov = len(cpu_full) / expected if expected else 0.0

            def _status(full_n, part_n, miss_n):
                if full_n == expected and expected > 0:
                    return 'complete'
                if full_n > 0:
                    return 'partial'
                if part_n > 0:
                    return 'incomplete_only'
                if miss_n > 0 or missing_lifecycle > 0:
                    return 'missing'
                return 'missing'

            mem_status = _status(len(mem_full), len(mem_partial), mem_missing)
            cpu_status = _status(len(cpu_full), len(cpu_partial), cpu_missing)
            overall_complete = min(len(mem_full), len(cpu_full)) if (mem_full and cpu_full) else len(mem_full)
            overall_cov = min(mem_cov, cpu_cov) if (mem_full or cpu_full) else 0.0
            overall_status = (
                'complete' if mem_status == 'complete' and cpu_status == 'complete'
                else 'partial' if (mem_full or cpu_full)
                else 'incomplete_only' if (mem_partial or cpu_partial)
                else 'missing'
            )

            all_times = [v[1] for v in mem_full] + [v[1] for v in cpu_full]

            # Aggregate CFS throttling ratio across container instances in [start, end].
            thr_p_sum = 0.0
            per_sum = 0.0
            matched_thr_instances = 0
            observed_thr_pods = set()
            for ckey in sorted(set(thr_periods_series) & set(periods_series)):
                tp_delta = _counter_delta_in_phase(thr_periods_series[ckey], start, end)
                p_delta = _counter_delta_in_phase(periods_series[ckey], start, end)
                if tp_delta is None or p_delta is None:
                    continue
                thr_p_sum += tp_delta
                per_sum += p_delta
                matched_thr_instances += 1
                observed_thr_pods.add(thr_container_of[ckey][0])
            if not expected_thr_pods:
                for ckey, samples in cpu_series.items():
                    if any(start <= ts <= end for ts, _ in samples):
                        expected_thr_pods.add(cpu_container_of[ckey][0])
                for ckey, samples in mem_series.items():
                    if any(start <= ts <= end for ts, _ in samples):
                        expected_thr_pods.add(mem_container_of[ckey][0])
            if matched_thr_instances > 0 and per_sum > 0:
                thr_ratio_str = f'{thr_p_sum / per_sum:.6f}'
                thr_p_str = f'{thr_p_sum:g}'
                per_str = f'{per_sum:g}'
                if expected_thr_pods:
                    covered = len(observed_thr_pods & expected_thr_pods)
                    thr_cov_str = f'{covered / len(expected_thr_pods):.4f}'
                    if expected_thr_pods <= observed_thr_pods:
                        thr_status = 'complete'
                    else:
                        thr_status = 'partial'
                        reasons.add('partial_throttling_coverage')
                else:
                    thr_status = 'partial'
                    thr_cov_str = ''
                    reasons.add('unverified_throttling_coverage')
            else:
                thr_ratio_str = ''
                thr_p_str = ''
                per_str = ''
                thr_status = 'missing'
                thr_cov_str = '0.0000' if expected_thr_pods else ''
                if not thr_periods_series or not periods_series:
                    reasons.add('missing_throttling_series')
                else:
                    reasons.add('missing_throttling_samples')

            thr_sec_deltas = [
                d for samples in thr_seconds_series.values()
                if (d := _counter_delta_in_phase(samples, start, end)) is not None
            ]
            thr_sec_str = f'{sum(thr_sec_deltas):.6f}' if thr_sec_deltas else ''

            node_cpu = _node_cpu_utilization_in_phase(node_idle_series, start, end)
            node_cpu_max_str = f'{max(node_cpu.values()):.6f}' if node_cpu else ''
            node_cpu_json_str = json.dumps(node_cpu, sort_keys=True) if node_cpu else ''

            writer.writerow({
                'phase': phase,
                'resource_source': RESOURCE_SOURCE,
                'rate_window_seconds': f'{rate_window_s:g}',
                'freshness_seconds': f'{freshness_s:g}',
                'scrape_interval_seconds': f'{scrape_interval_s:g}',
                'cpu_millicores': sum(v[0] for v in cpu_full) / len(cpu_full) if cpu_full else '',
                'memory_mib': sum(v[0] for v in mem_full) / len(mem_full) if mem_full else '',
                'throttled_period_ratio': thr_ratio_str,
                'cfs_throttled_periods': thr_p_str,
                'cfs_periods': per_str,
                'cfs_throttled_seconds': thr_sec_str,
                'throttling_completeness_status': thr_status,
                'throttling_coverage_ratio': thr_cov_str,
                'node_cpu_max_utilization': node_cpu_max_str,
                'node_cpu_utilization': node_cpu_json_str,
                'complete_samples': overall_complete,
                'memory_complete_samples': len(mem_full),
                'cpu_complete_samples': len(cpu_full),
                'partial_samples': max(len(mem_partial), len(cpu_partial)),
                'memory_partial_samples': len(mem_partial),
                'cpu_partial_samples': len(cpu_partial),
                'missing_samples': max(mem_missing, cpu_missing) + missing_lifecycle,
                'expected_samples': expected,
                'insufficient_rate_samples': insufficient_rate,
                'missing_lifecycle_samples': missing_lifecycle,
                'scrape_down_samples': scrape_down,
                'duplicate_series_deduped': cpu_dups + mem_dups,
                'query_error_count': len(query_errors),
                'coverage_ratio': f'{overall_cov:.4f}',
                'memory_coverage_ratio': f'{mem_cov:.4f}',
                'cpu_coverage_ratio': f'{cpu_cov:.4f}',
                'partial_cpu_millicores': sum(cpu_partial) / len(cpu_partial) if cpu_partial else '',
                'partial_memory_mib': sum(mem_partial) / len(mem_partial) if mem_partial else '',
                'completeness_status': overall_status,
                'memory_completeness_status': mem_status,
                'cpu_completeness_status': cpu_status,
                'cpu_quality': f'rate_{int(round(rate_window_s))}s' if cpu_full else 'missing',
                'missing_reasons': ';'.join(sorted(reasons)),
                'first_metric_time': min(all_times) if all_times else '',
                'last_metric_time': max(all_times) if all_times else '',
                'description': (
                    f'Prometheus/cAdvisor ({rate_window_s:g}s CPU rate window, {freshness_s:g}s freshness, '
                    f'{scrape_interval_s:g}s scrape); CPU rate windows can cross phase boundaries; '
                    'averages of valid observations, not exact phase CPU consumption'
                ),
            })
            print(
                f'{phase} [cAdvisor]: mem={len(mem_full)}/{expected} cpu={len(cpu_full)}/{expected} '
                f'partial=({len(mem_partial)},{len(cpu_partial)}) insufficient_rate={insufficient_rate}'
            )
    return target


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)

    exp = sub.add_parser('export', help='Query Prometheus cAdvisor history and save resources-prometheus.json')
    exp.add_argument('requests', type=Path)
    exp.add_argument('output', type=Path)
    exp.add_argument('--prometheus-url', default=DEFAULT_PROMETHEUS_URL)
    exp.add_argument('--namespace', default='services')
    exp.add_argument('--pod-prefix', default='mec-placement-face-detection-')
    exp.add_argument('--step', type=float, default=DEFAULT_SCRAPE_INTERVAL_S)
    exp.add_argument('--rate-window', type=float, default=DEFAULT_RATE_WINDOW_S)
    exp.add_argument('--freshness', type=float, default=DEFAULT_FRESHNESS_S)
    exp.add_argument('--scrape-interval', type=float, default=DEFAULT_SCRAPE_INTERVAL_S)
    exp.add_argument('--strict', action='store_true')

    sum_p = sub.add_parser('summarize', help='Summarize resources-prometheus.json into resource-by-ramp.csv')
    sum_p.add_argument('prometheus_json', type=Path)
    sum_p.add_argument('requests', type=Path)
    sum_p.add_argument('summary', type=Path)
    sum_p.add_argument('output', type=Path)
    sum_p.add_argument('--metrics-jsonl', type=Path, default=None)
    sum_p.add_argument('--rate-window', type=float, default=None, help='override CPU rate window in seconds (default: file metadata or 45s)')
    sum_p.add_argument('--freshness', type=float, default=None, help='override freshness window in seconds (default: file metadata or 25s)')

    run_p = sub.add_parser('run', help='Export from Prometheus and summarize into resource-by-ramp.csv')
    run_p.add_argument('requests', type=Path)
    run_p.add_argument('summary', type=Path)
    run_p.add_argument('output', type=Path)
    run_p.add_argument('--metrics-jsonl', type=Path, default=None)
    run_p.add_argument('--prometheus-url', default=DEFAULT_PROMETHEUS_URL)
    run_p.add_argument('--namespace', default='services')
    run_p.add_argument('--pod-prefix', default='mec-placement-face-detection-')
    run_p.add_argument('--step', type=float, default=DEFAULT_SCRAPE_INTERVAL_S)
    run_p.add_argument('--rate-window', type=float, default=DEFAULT_RATE_WINDOW_S)
    run_p.add_argument('--scrape-interval', type=float, default=DEFAULT_SCRAPE_INTERVAL_S)
    run_p.add_argument('--freshness', type=float, default=DEFAULT_FRESHNESS_S)

    args = parser.parse_args(argv)
    if args.action == 'export':
        export_prometheus_resources(
            args.requests, args.output, prometheus_url=args.prometheus_url,
            namespace=args.namespace, pod_prefix=args.pod_prefix,
            step_s=args.step, rate_window_s=args.rate_window,
            scrape_interval_s=args.scrape_interval, freshness_s=args.freshness, strict=args.strict,
        )
    elif args.action == 'summarize':
        summarize_cadvisor(
            args.prometheus_json, args.requests, args.summary, args.output,
            metrics_jsonl_path=args.metrics_jsonl, rate_window_s=args.rate_window,
            freshness_s=args.freshness,
        )
    else:
        prom_json = export_prometheus_resources(
            args.requests, args.output, prometheus_url=args.prometheus_url,
            namespace=args.namespace, pod_prefix=args.pod_prefix,
            step_s=args.step, rate_window_s=args.rate_window,
            scrape_interval_s=args.scrape_interval, freshness_s=args.freshness,
        )
        summarize_cadvisor(
            prom_json, args.requests, args.summary, args.output,
            metrics_jsonl_path=args.metrics_jsonl, rate_window_s=args.rate_window,
            freshness_s=args.freshness,
        )


if __name__ == '__main__':
    main()
