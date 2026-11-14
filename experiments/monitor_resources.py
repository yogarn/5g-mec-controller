#!/usr/bin/env python3
"""Sample workload pods and Metrics Server, then align observations to UE phases.

The metrics-server CPU value is an average over its reported window; memory is
working set. Windows can overlap phase boundaries. Samples missing any running
pod are excluded, rather than interpreted as zero resource usage.
"""
import argparse
import csv
import json
import signal
import subprocess
import time
from bisect import bisect_right
from datetime import datetime
from pathlib import Path

LABEL = 'app=mec-placement-face-detection'
PREFIX = 'mec-placement-face-detection-'
NODES = ('core-5g', 'gnb1', 'gnb-2', 'gnb-3', '<pending>')


def kubectl(*args):
    return json.loads(subprocess.check_output(['kubectl', '-n', 'services', *args], text=True, timeout=10))


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def quantity(value, kind):
    suffixes = ({'n': 1e-6, 'u': 1e-3, 'm': 1, '': 1000} if kind == 'cpu'
                else {'Ki': 1 / 1024, 'Mi': 1, 'Gi': 1024, '': 1 / (1024 * 1024)})
    for suffix in sorted(suffixes, key=len, reverse=True):
        if not suffix or value.endswith(suffix):
            return float(value[:-len(suffix)] if suffix else value) * suffixes[suffix]
    raise ValueError(f'unsupported {kind} quantity: {value}')


def sample(output):
    stop = False
    def request_stop(signum, frame):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    with Path(output).open('x', encoding='utf-8') as stream:
        while not stop:
            try:
                pods = kubectl('get', 'pods', '-l', LABEL, '-o', 'json')['items']
                measurements = kubectl('get', '--raw', '/apis/metrics.k8s.io/v1beta1/namespaces/services/pods')['items']
                metrics = {m['metadata']['name']: m for m in measurements if m['metadata']['name'].startswith(PREFIX)}
                rows = []
                for pod in pods:
                    metadata, status = pod['metadata'], pod.get('status', {})
                    if metadata.get('deletionTimestamp') or status.get('phase') in ('Succeeded', 'Failed'):
                        continue
                    rows.append({'name': metadata['name'], 'node': pod.get('spec', {}).get('nodeName') or '<pending>',
                                 'phase': status.get('phase'),
                                 'ready': any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in status.get('conditions', []))})
                observation = {'sample_time': time.time(), 'pods': rows,
                               'metrics': {p['name']: metrics[p['name']] for p in rows if p['name'] in metrics}}
                stream.write(json.dumps(observation, separators=(',', ':')) + '\n')
                stream.flush()
            except (subprocess.SubprocessError, OSError, ValueError, KeyError) as exc:
                stream.write(json.dumps({'sample_time': time.time(), 'error': str(exc)}) + '\n')
                stream.flush()
            time.sleep(2)


def summarize(raw, requests, summary_csv, output):
    samples = [json.loads(line) for line in Path(raw).read_text().splitlines()]
    events = [json.loads(line) for line in Path(requests).read_text().splitlines()]
    starts = {r['phase']: r for r in events if r.get('event') == 'phase_start'}
    ends = {r['phase']: r for r in events if r.get('event') == 'phase_end'}
    with Path(summary_csv).open(newline='') as stream:
        summary = list(csv.DictReader(stream))
    if not starts or set(starts) != set(ends) or {r['phase'] for r in summary} != set(starts):
        raise ValueError('incomplete or mismatched phase records')
    valid = [s for s in samples if 'pods' in s and 'metrics' in s]
    valid.sort(key=lambda s: s['sample_time'])
    sample_times = [s['sample_time'] for s in valid]

    def running_at(when):
        index = bisect_right(sample_times, when) - 1
        if index < 0 or when - sample_times[index] > 8:
            return None
        return frozenset(p['name'] for p in valid[index]['pods'] if p['phase'] == 'Running')

    nodes = sorted(set(NODES) | {p['node'] for s in valid for p in s['pods']})
    out = Path(output)
    with (out / 'ramp-summary.csv').open('x', newline='') as stream:
        fields = ['phase', 'workload_ue', 'gnb1_ues', 'gnb2_ues', 'gnb3_ues', 'p50_ms', 'p90_ms', 'attempts', 'successes', 'failures', 'start_time', 'end_time']
        writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader()
        for row in summary:
            writer.writerow({**{k: row[k] for k in fields if k not in ('p50_ms', 'p90_ms')},
                             'p50_ms': row['latency_p50_ms'], 'p90_ms': row['latency_p90_ms']})
    with (out / 'pods-by-ramp.csv').open('x', newline='') as stream:
        fields = ['phase', 'node', 'pod_count', 'ready_count', 'pod_names', 'sample_time', 'phase_end', 'sample_age_seconds']
        writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader()
        for row in summary:
            phase = row['phase']; end = ends[phase]['end_time']; start = starts[phase]['start_time']
            candidates = [s for s in valid if start <= s['sample_time'] <= end]
            if not candidates:
                raise ValueError(f'no placement sample during {phase}')
            last = max(candidates, key=lambda s: s['sample_time'])
            for node in nodes:
                pods = [p for p in last['pods'] if p['node'] == node]
                writer.writerow({'phase': phase, 'node': node, 'pod_count': len(pods),
                                 'ready_count': sum(p['ready'] for p in pods),
                                 'pod_names': ';'.join(p['name'] for p in pods), 'sample_time': last['sample_time'],
                                 'phase_end': end, 'sample_age_seconds': end - last['sample_time']})
    with (out / 'resource-by-ramp.csv').open('x', newline='') as stream:
        fields = ['phase', 'cpu_millicores', 'memory_mib', 'complete_samples', 'first_metric_time', 'last_metric_time', 'description']
        writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader()
        for row in summary:
            phase = row['phase']; start = starts[phase]['start_time']; end = ends[phase]['end_time']
            unique = {}
            for observation in valid:
                if not start <= observation['sample_time'] <= end + 60:
                    continue
                metrics = observation['metrics']
                candidates = set()
                for entry in metrics.values():
                    t = timestamp(entry['timestamp'])
                    if start <= t <= end:
                        running = running_at(t)
                        if running:
                            candidates.add(running)
                for running in candidates:
                    if any(name not in metrics for name in running):
                        continue
                    entries = [metrics[name] for name in running]
                    times = [timestamp(e['timestamp']) for e in entries]
                    if any(not start <= t <= end or running_at(t) != running for t in times):
                        continue
                    key = tuple(sorted((name, metrics[name]['timestamp']) for name in running))
                    cpu = sum(quantity(c['usage']['cpu'], 'cpu') for e in entries for c in e['containers'])
                    memory = sum(quantity(c['usage']['memory'], 'memory') for e in entries for c in e['containers'])
                    unique[key] = (cpu, memory, min(times), max(times))
            vals = list(unique.values())
            writer.writerow({'phase': phase, 'cpu_millicores': sum(v[0] for v in vals) / len(vals) if vals else '',
                             'memory_mib': sum(v[1] for v in vals) / len(vals) if vals else '',
                             'complete_samples': len(vals), 'first_metric_time': min((v[2] for v in vals), default=''),
                             'last_metric_time': max((v[3] for v in vals), default=''),
                             'description': 'mean of complete Metrics Server pod samples; CPU windows may overlap phases'})
            print(f'{phase}: resource samples={len(vals)}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    p = sub.add_parser('sample'); p.add_argument('output')
    p = sub.add_parser('summarize'); p.add_argument('samples'); p.add_argument('requests'); p.add_argument('summary'); p.add_argument('output')
    args = parser.parse_args()
    if args.action == 'sample':
        sample(args.output)
    else:
        summarize(args.samples, args.requests, args.summary, args.output)


if __name__ == '__main__':
    main()
