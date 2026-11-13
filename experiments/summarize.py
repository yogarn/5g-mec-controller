#!/usr/bin/env python3
"""Summarize benchmark/client.py JSONL without modifying experiment data."""

import argparse
import csv
import json
from contextlib import ExitStack
import math
from pathlib import Path
import sys

from experiments import defaults


def number(value, name, *, minimum=None):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f'{name} must be a finite number')
    if minimum is not None and value < minimum:
        raise ValueError(f'{name} must be at least {minimum}')
    return value


def integer(value, name, *, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f'{name} must be an integer of at least {minimum}')
    return value


def text(value, name):
    if not isinstance(value, str) or not value:
        raise ValueError(f'{name} must be a nonempty string')
    return value


def counts(record):
    phase = text(record['phase'], 'phase')
    total = integer(record['workload_ue'], 'workload_ue', minimum=1)
    gnb2 = integer(record['gnb2_ues'], 'gnb2_ues')
    gnb3 = integer(record['gnb3_ues'], 'gnb3_ues')
    if total != gnb2 + gnb3:
        raise ValueError('UE counts do not add up')
    return phase, total, gnb2, gnb3


def ue_key(record):
    group = text(record['group'], 'group')
    if group not in ('gnb-2', 'gnb-3'):
        raise ValueError('unknown UE group')
    return group, text(record['source_address'], 'source_address')


def percentile(values, fraction):
    if not values:
        return None
    position = (len(values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON key: {key}')
        result[key] = value
    return result


def summarize(path, slo_ms, ue_results=None):
    number(slo_ms, 'SLO', minimum=0)
    phases = []
    ue_rows = []
    seen = set()
    current = None
    with Path(path).open(encoding='utf-8') as source:
        for line_number, line in enumerate(source, 1):
            try:
                record = json.loads(line, object_pairs_hook=unique_object,
                                    parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f'invalid number: {value}')))
                if not isinstance(record, dict):
                    raise ValueError('event must be an object')
                event = record['event']
                if event == 'phase_start':
                    if current is not None:
                        raise ValueError('phase started before previous phase ended')
                    info = counts(record)
                    if info[0] in seen:
                        raise ValueError('duplicate phase')
                    seen.add(info[0])
                    start = number(record['start_time'], 'start_time')
                    duration = number(record['duration_s'], 'duration_s', minimum=0)
                    if duration == 0:
                        raise ValueError('duration_s must be positive')
                    ues = record['ues']
                    if not isinstance(ues, list):
                        raise ValueError('ues must be a list')
                    keys = [ue_key(ue) for ue in ues]
                    if (len(keys) != info[1] or len(set(keys)) != len(keys)
                            or sum(group == 'gnb-2' for group, _ in keys) != info[2]
                            or sum(group == 'gnb-3' for group, _ in keys) != info[3]
                            or len({address for _, address in keys}) != len(keys)):
                        raise ValueError('UE list does not match phase counts')
                    current = {'info': info, 'start': start, 'duration': duration,
                               'ues': {key: [0, 0, [], 0] for key in keys}, 'latencies': [], 'slo_met': 0}
                elif event == 'request':
                    if current is None or record['phase'] != current['info'][0]:
                        raise ValueError('request outside its phase')
                    key = ue_key(record)
                    if key not in current['ues']:
                        raise ValueError('request from unknown UE')
                    stats = current['ues'][key]
                    if integer(record['attempt'], 'attempt', minimum=1) != stats[0] + 1:
                        raise ValueError('missing or out-of-order UE attempt')
                    number(record['start_time'], 'request start_time')
                    number(record['end_time'], 'request end_time')
                    latency = number(record['duration_ms'], 'duration_ms', minimum=0)
                    text(record['url'], 'url')
                    text(record['method'], 'method')
                    success = record['success']
                    if not isinstance(success, bool):
                        raise ValueError('success must be boolean')
                    status = record['http_status']
                    if status is not None:
                        integer(status, 'http_status', minimum=100)
                        if status > 999:
                            raise ValueError('invalid HTTP status')
                    error = record['error']
                    if error is not None and not isinstance(error, str):
                        raise ValueError('error must be a string or null')
                    if success != (error is None and status is not None and 200 <= status < 300):
                        raise ValueError('success is inconsistent with status and error')
                    stats[0] += 1
                    if success:
                        stats[1] += 1
                        current['latencies'].append(latency)
                        stats[2].append(latency)
                        stats[3] += latency <= slo_ms
                        current['slo_met'] += latency <= slo_ms
                elif event == 'phase_end':
                    if current is None or counts(record) != current['info']:
                        raise ValueError('phase_end does not match phase_start')
                    start = number(record['start_time'], 'start_time')
                    end = number(record['end_time'], 'end_time')
                    generation = number(record['generation_duration_s'], 'generation_duration_s', minimum=0)
                    if start != current['start'] or generation != current['duration'] or end <= start:
                        raise ValueError('invalid or inconsistent phase boundaries')
                    per_ue = record['per_ue']
                    if not isinstance(per_ue, list):
                        raise ValueError('per_ue must be a list')
                    reported = {}
                    for ue in per_ue:
                        key = ue_key(ue)
                        if key in reported:
                            raise ValueError('duplicate UE in phase_end')
                        reported[key] = [integer(ue['attempts'], 'attempts'), integer(ue['successes'], 'successes')]
                    if reported != {key: stats[:2] for key, stats in current['ues'].items()}:
                        raise ValueError('per-UE totals do not match requests')
                    attempts = sum(value[0] for value in reported.values())
                    successes = sum(value[1] for value in reported.values())
                    if integer(record['attempts'], 'attempts') != attempts or integer(record['successes'], 'successes') != successes:
                        raise ValueError('phase totals do not match requests')
                    latencies = sorted(current['latencies'])
                    phase, total, gnb2, gnb3 = current['info']
                    elapsed = end - start
                    phases.append({'phase': phase, 'workload_ue': total, 'gnb2_ues': gnb2, 'gnb3_ues': gnb3,
                                   'start_time': start, 'end_time': end, 'generation_duration_s': generation,
                                   'throughput_denominator_s': elapsed, 'attempts': attempts,
                                   'successes': successes, 'failures': attempts - successes,
                                   'attempt_throughput_per_s': attempts / elapsed,
                                   'success_throughput_per_s': successes / elapsed,
                                   'latency_p50_ms': percentile(latencies, .50),
                                   'latency_p95_ms': percentile(latencies, .95),
                                   'latency_p99_ms': percentile(latencies, .99),
                                   'slo_ms': slo_ms, 'slo_met_attempts': current['slo_met'],
                                   'slo_attainment_all_attempts': current['slo_met'] / attempts if attempts else None})
                    for (group, address), (ue_attempts, ue_successes, ue_latencies, ue_slo_met) in current['ues'].items():
                        ue_latencies.sort()
                        ue_rows.append({'phase': phase, 'group': group, 'source_address': address,
                                        'attempts': ue_attempts, 'successes': ue_successes,
                                        'failures': ue_attempts - ue_successes,
                                        'latency_p50_ms': percentile(ue_latencies, .50),
                                        'latency_p95_ms': percentile(ue_latencies, .95),
                                        'latency_p99_ms': percentile(ue_latencies, .99),
                                        'slo_ms': slo_ms, 'slo_met_attempts': ue_slo_met,
                                        'slo_attainment_all_attempts': ue_slo_met / ue_attempts if ue_attempts else None})
                    current = None
                else:
                    raise ValueError(f'unknown event: {event!r}')
            except (ValueError, KeyError, TypeError, OverflowError) as exc:
                raise ValueError(f'line {line_number}: {exc}') from exc
    if current is not None:
        raise ValueError('incomplete input: missing phase_end')
    if not phases:
        raise ValueError('input contains no complete phases')
    if ue_results is not None:
        ue_results.extend(ue_rows)
    return phases


def write_results(phases, prefix, ue_results=None):
    outputs = [(Path(f'{prefix}.json'), phases, 'json'),
               (Path(f'{prefix}.csv'), phases, 'csv')]
    if ue_results is not None:
        outputs.extend([(Path(f'{prefix}-ues.json'), ue_results, 'json'),
                        (Path(f'{prefix}-ues.csv'), ue_results, 'csv')])
    created = []
    try:
        with ExitStack() as stack:
            opened = []
            for path, rows, kind in outputs:
                file = stack.enter_context(path.open('x', encoding='utf-8', newline='' if kind == 'csv' else None))
                created.append(path)
                opened.append((file, rows, kind))
            for file, rows, kind in opened:
                if kind == 'json':
                    json.dump(rows, file, indent=2, allow_nan=False)
                    file.write('\n')
                else:
                    writer = csv.DictWriter(file, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
    except BaseException:
        for path in created:
            path.unlink(missing_ok=True)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description='Convert benchmark JSONL into per-phase and per-UE JSON and CSV. Latency percentiles use linear interpolation over successful requests; SLO attainment counts successful requests at or below the threshold over all attempts. Throughput uses phase end minus phase start (including drain time). Empty percentiles and zero-attempt attainment are null/blank.')
    parser.add_argument('input', nargs='?', help='request JSONL; defaults to the sole run in results/')
    parser.add_argument('--output-prefix', help='output prefix; defaults to summary beside the input')
    parser.add_argument('--slo-ms', type=float, default=defaults.SLO_MS, help='inclusive latency SLO in milliseconds')
    args = parser.parse_args(argv)
    try:
        if args.input is None:
            runs = sorted(defaults.RESULTS.glob('*/requests.jsonl'))
            if len(runs) != 1:
                raise ValueError(f'expected exactly one results/*/requests.jsonl, found {len(runs)}; specify input explicitly')
            args.input = str(runs[0])
        if args.output_prefix is None:
            args.output_prefix = str(Path(args.input).parent / 'summary')
        ue_results = []
        phases = summarize(args.input, args.slo_ms, ue_results)
        write_results(phases, args.output_prefix, ue_results)
        print(f'Summarized {len(phases)} phase(s) and {len(ue_results)} per-UE result(s)')
        print(f'  Phase JSON: {args.output_prefix}.json')
        print(f'  Phase CSV:  {args.output_prefix}.csv')
        print(f'  Per-UE JSON: {args.output_prefix}-ues.json')
        print(f'  Per-UE CSV:  {args.output_prefix}-ues.csv')
    except (OSError, UnicodeError, ValueError) as exc:
        parser.exit(2, f'error: {exc}\n')


if __name__ == '__main__':
    main()
