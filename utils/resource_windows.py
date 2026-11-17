"""Local 15-second resource summaries from saved raw cAdvisor series."""
import json
import math
from pathlib import Path

from utils.cadvisor_resources import (
    DEFAULT_FRESHNESS_S, DEFAULT_RATE_WINDOW_S, DEFAULT_SCRAPE_INTERVAL_S,
    _dedupe_series, _load_running_lookup, _memory_mib_at, _rate_millicores_at,
)


def _instances_at(series, containers, running, when):
    selected = {}
    for key, samples in series.items():
        pod, container = containers[key]
        if pod not in running:
            continue
        prior = [t for t, _ in samples if t <= when]
        if not prior:
            continue
        rank = (prior[-1], samples[0][0])
        if (pod, container) not in selected or rank > selected[(pod, container)][0]:
            selected[(pod, container)] = (rank, samples)
    return {key: val[1] for key, val in selected.items()}


def build_resource_windows(method_dir, windows, node=None):
    """Average complete 5s evaluations within each reporting window.

    CPU retains the archived rate-window setting (normally 45s); these are
    15s reporting intervals, not newly invented 15s CPU counter deltas.
    """
    directory = Path(method_dir)
    path = directory / 'resources-prometheus.json'
    prom = json.loads(path.read_text()) if path.exists() else {}
    result = prom.get('results') or {}
    prefix = prom.get('pod_prefix', 'mec-placement-face-detection-')
    cpu, _, cpu_containers, _ = _dedupe_series(result.get('cpu_seconds_total', []), prefix)
    memory, _, mem_containers, _ = _dedupe_series(result.get('memory_working_set_bytes', []), prefix)
    running_at = (_load_running_lookup(directory / 'metrics.jsonl', node=node) if prom else lambda when: None)
    step = float(prom.get('step_seconds', DEFAULT_SCRAPE_INTERVAL_S))
    rate_window = float(prom.get('rate_window_seconds', DEFAULT_RATE_WINDOW_S))
    freshness = float(prom.get('freshness_seconds', DEFAULT_FRESHNESS_S))
    if any(not math.isfinite(v) or v <= 0 for v in (step, rate_window, freshness)):
        raise ValueError('resource sampling intervals must be finite and positive')
    query_errors = prom.get('query_errors') or {}
    output = []
    for win in windows:
        lower, upper = win['start_offset_s'], win['end_offset_s']
        phase_start = win['actual_start_time'] - lower
        times = []
        offset = (math.floor(lower / step) + 1) * step
        while offset <= upper + 1e-9:
            times.append(phase_start + offset)
            offset += step
        if not times:
            times = [win['actual_end_time']]
        values = {'cpu_millicores': [], 'memory_mib': []}
        reasons = set()
        if not prom:
            reasons.add('raw_cadvisor_data_unavailable')
        for when in times:
            running = running_at(when)
            if running is None:
                reasons.add('missing_or_stale_pod_observation')
                continue
            if not running:
                # An observed empty Running set means measured zero workload.
                for series_values in values.values():
                    series_values.append(0.0)
                continue
            for name, series, containers, metric in [
                ('cpu_millicores', cpu, cpu_containers, 'cpu_seconds_total'),
                ('memory_mib', memory, mem_containers, 'memory_working_set_bytes'),
            ]:
                if metric in query_errors:
                    reasons.add(f'{metric}_query_error')
                    continue
                selected = _instances_at(series, containers, running, when)
                observed, valid = set(), []
                complete = True
                for (pod, _container), samples in selected.items():
                    if name == 'cpu_millicores':
                        value, _, status = _rate_millicores_at(samples, when, rate_window, freshness)
                    else:
                        value, _, status = _memory_mib_at(samples, when, freshness)
                    if status != 'ok':
                        complete = False
                    else:
                        observed.add(pod)
                        valid.append(value)
                if complete and observed == running:
                    values[name].append(sum(valid))
                else:
                    reasons.add(f'incomplete_{metric}')
        row = dict(win)
        if node is not None:
            row['node'] = node
        row.update(resource_source='prometheus-cadvisor', rate_window_seconds=rate_window,
                   freshness_seconds=freshness, evaluation_step_seconds=step,
                   expected_samples=len(times), missing_reasons=';'.join(sorted(reasons)))
        for name, samples in values.items():
            short = 'cpu' if name == 'cpu_millicores' else 'memory'
            row[name] = sum(samples) / len(samples) if samples else math.nan
            row[f'{short}_complete_samples'] = len(samples)
            row[f'{short}_coverage_percent'] = 100 * len(samples) / len(times)
        output.append(row)
    return output
