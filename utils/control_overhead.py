"""Archive control-plane timers and report lifecycle delay separately from execution.

Native histogram counters have cluster scope, not per-workload attribution.
No historical decision durations are inferred from log spacing or polling.
"""
import argparse
import json
import math
from pathlib import Path
import re
import signal
import subprocess
import time
from datetime import datetime

FAMILIES = (
    'scheduler_scheduling_algorithm_duration_seconds',
    'scheduler_scheduling_attempt_duration_seconds',
    'scheduler_pod_scheduling_sli_duration_seconds',
    'horizontal_pod_autoscaler_controller_reconciliation_duration_seconds',
    'horizontal_pod_autoscaler_controller_metric_computation_duration_seconds',
)
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')
SAMPLE = re.compile(r'^(\w+)(?:\{(.*)\})?\s+([^\s]+)')


def parse_component_metrics(text):
    output = []
    for line in text.splitlines():
        if not line.startswith(FAMILIES):
            continue
        match = SAMPLE.match(line)
        if not match:
            continue
        name, raw_labels, raw_value = match.groups()
        if not any(name == f + suffix for f in FAMILIES for suffix in ('_sum', '_count', '_bucket')):
            continue
        value = float(raw_value)
        if math.isfinite(value) and value >= 0:
            labels = {key: json.loads('"' + val + '"') for key, val in LABEL.findall(raw_labels or '')}
            output.append(dict(metric=name, labels=labels, value=value))
    return output


def component_snapshot():
    start = time.time()
    try:
        raw = subprocess.check_output(['kubectl', 'get', '--raw', '/metrics'], text=True, timeout=10)
        process_start = re.search(r'^process_start_time_seconds\s+([0-9.eE+-]+)', raw, re.MULTILINE)
        return dict(process_start_time=float(process_start.group(1)) if process_start else None,
                    sample_time=time.time(), collection_started_at=start, source='kubernetes-/metrics',
                    scope='cluster-wide', series=parse_component_metrics(raw))
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        return dict(sample_time=time.time(), error=str(exc), scope='cluster-wide')


def sample_components(path, once=False, interval=5):
    stop = False
    def request_stop(*_args):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    with Path(path).open('x' if once else 'a') as stream:
        next_sample = time.monotonic()
        while not stop:
            stream.write(json.dumps(component_snapshot(), separators=(',', ':')) + '\n')
            stream.flush()
            if once:
                return
            next_sample += interval
            now = time.monotonic()
            if next_sample <= now:
                next_sample += (int((now - next_sample) // interval) + 1) * interval
            if not stop:
                time.sleep(next_sample - now)


def _records(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()] if Path(path).exists() else []


def _timestamp(value):
    if value in (None, ''):
        return None
    return float(value) if isinstance(value, (int, float)) else datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def pod_timing_fields(pod):
    meta, status = pod.get('metadata', {}), pod.get('status', {})
    conditions = {c['type']: c for c in status.get('conditions', []) if c.get('status') == 'True'}
    return dict(uid=meta.get('uid'), scheduler_name=pod.get('spec', {}).get('schedulerName'),
                created_at=meta.get('creationTimestamp'),
                scheduled_at=conditions.get('PodScheduled', {}).get('lastTransitionTime'),
                ready_at=conditions.get('Ready', {}).get('lastTransitionTime'))


def scheduling_delays(directory):
    """One row per observed pod; fallback snapshots contain survivors only."""
    directory = Path(directory)
    pods = {}
    for rec in _records(directory / 'metrics.jsonl'):
        for pod in rec.get('pods', []):
            key = pod.get('uid') or pod['name']
            saved = pods.setdefault(key, dict(pod, source='sampled-pod-lifecycle'))
            for key, value in pod.items():
                if value is None:
                    continue
                if key in ('created_at', 'scheduled_at', 'ready_at') and saved.get(key) is not None:
                    # Startup timing uses the first True condition, not a later
                    # readiness recovery under load.
                    if _timestamp(value) < _timestamp(saved[key]):
                        saved[key] = value
                else:
                    saved[key] = value
    after = directory / 'deployments-after.json'
    if after.exists():
        for pod in json.loads(after.read_text()).get('items', []):
            if pod.get('kind') != 'Pod':
                continue
            fields = pod_timing_fields(pod)
            name = pod['metadata']['name']
            # Filter by names already seen to exclude unrelated workloads.
            candidates = [p for p in pods.values() if p.get('name') == name and
                          (not p.get('uid') or p['uid'] == fields.get('uid'))]
            if not candidates:
                continue
            existing = candidates[0]
            missing = {key: value for key, value in fields.items() if value is not None and existing.get(key) is None}
            if missing:
                sampled_creation = bool(existing.get('created_at'))
                existing.update(missing)
                existing['source'] = 'sampled-and-final-snapshot' if sampled_creation else 'final-snapshot-survivor'
    output = []
    for pod in pods.values():
        created, scheduled = _timestamp(pod.get('created_at')), _timestamp(pod.get('scheduled_at'))
        ready = _timestamp(pod.get('ready_at'))
        delay = (scheduled - created) * 1000 if created is not None and scheduled is not None and scheduled >= created else math.nan
        output.append(dict(pod=pod['name'], uid=pod.get('uid'), node=pod.get('node'), scheduler=pod.get('scheduler_name'),
                           created_at=created, scheduled_at=scheduled, ready_at=ready,
                           created_to_scheduled_ms=delay,
                           scheduled_to_ready_ms=(ready - scheduled) * 1000 if scheduled is not None and ready is not None and ready >= scheduled else math.nan,
                           status='measured' if math.isfinite(delay) else 'timestamps_not_recorded', source=pod['source'],
                           scope='PodScheduled condition timestamps; includes queueing/status propagation; API timestamp precision'))
    return output


def hpa_action_delays(directory):
    """Last scale request to first sampled settled target, with polling bounds.

    Superseded/unfinished targets remain missing, never treated as completed.
    """
    directory = Path(directory)
    pods = sorted(_records(directory / 'metrics.jsonl'), key=lambda r: r['sample_time'])
    actions = []
    seen = set()
    for rec in sorted(_records(directory / 'hpa-status.jsonl'), key=lambda r: r['sample_time']):
        when = _timestamp(rec.get('last_scale_time'))
        target = rec.get('desired_replicas')
        if when is None or target is None or when in seen:
            continue
        seen.add(when)
        actions.append(dict(action_time=when, decision_observed_at=rec['sample_time'], target_replicas=int(target), hpa=rec.get('name')))
    output = []
    for index, action in enumerate(actions):
        stop = actions[index + 1]['action_time'] if index + 1 < len(actions) else math.inf
        last_unsettled = action['action_time']
        settled = None
        predates_recording = bool(pods and action['action_time'] < pods[0]['sample_time'] - 10)
        for rec in ([] if predates_recording else pods):
            if not action['action_time'] <= rec['sample_time'] < stop or 'pods' not in rec or 'error' in rec:
                continue
            active = rec['pods']
            if len(active) == action['target_replicas'] and sum(bool(p.get('ready')) for p in active) == action['target_replicas']:
                settled = rec['sample_time']
                break
            last_unsettled = rec['sample_time']
        output.append(dict(action, settled_observed_at=settled,
                           scale_to_ready_ms_lower=(last_unsettled - action['action_time']) * 1000 if settled is not None else math.nan,
                           scale_to_ready_ms_upper=(settled - action['action_time']) * 1000 if settled is not None else math.nan,
                           status='action_predates_recording' if predates_recording else 'observed_settled' if settled is not None else 'superseded' if math.isfinite(stop) else 'not_observed_settled'))
    return output


def component_execution_windows(directory):
    """Histogram sum/count deltas: mean wall execution ms per sampled interval.

    A process restart/reset, invalid collection, or long scrape gap stays missing.
    Counters shared by other workloads cannot be attributed to this workload.
    """
    observations = sorted(_records(Path(directory) / 'control-overhead.jsonl'), key=lambda r: r['sample_time'])
    output = []
    for previous, current in zip(observations, observations[1:]):
        if (previous.get('process_start_time') != current.get('process_start_time') or
                'error' in previous or 'error' in current or
                not 0 < current['sample_time'] - previous['sample_time'] <= 20):
            continue
        def pairs(rec):
            values = {}
            for item in rec.get('series', []):
                name = item['metric']
                if not name.endswith(('_count', '_sum')):
                    continue
                family, stat = name.rsplit('_', 1)
                key = family, json.dumps(item.get('labels') or {}, sort_keys=True)
                values.setdefault(key, {})[stat] = item['value']
            return values
        before, after = pairs(previous), pairs(current)
        for key in before.keys() & after.keys():
            if not all(stat in before[key] and stat in after[key] for stat in ('sum', 'count')):
                continue
            count = after[key]['count'] - before[key]['count']
            total = after[key]['sum'] - before[key]['sum']
            status = 'counter_reset' if count < 0 or total < 0 else 'measured' if count > 0 else 'no_observations'
            output.append(dict(start_time=previous['sample_time'], end_time=current['sample_time'], operation=key[0], labels=key[1],
                               observations=count if status != 'counter_reset' else 0,
                               mean_execution_ms=1000 * total / count if status == 'measured' else math.nan,
                               duration_sum_ms=1000 * total if status != 'counter_reset' else math.nan,
                               status=status, scope='cluster-wide; workload attribution unavailable'))
    return output


def proposed_execution_records(directory):
    path = Path(directory) / 'controller.log'
    output = []
    if path.exists():
        for line in path.read_text().splitlines():
            match = re.search(r'decision timing operation=(\w+) duration_ms=([\d.eE+-]+)', line)
            if match:
                operation, duration = match.groups()
                value = float(duration)
                if math.isfinite(value) and value >= 0:
                    pod = re.search(r'pod=(\S+)', line)
                    output.append(dict(operation=operation, pod=pod.group(1) if pod else '',
                                       execution_ms=value, source='proposed-controller-monotonic-timer'))
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('snapshot', 'sample'))
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    sample_components(args.output, once=args.action == 'snapshot')


if __name__ == '__main__':
    main()
