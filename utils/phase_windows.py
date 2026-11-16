"""Describe whole-phase and final-60s request cohorts without assuming convergence."""
import csv
import json
import math
from pathlib import Path

from utils.transient_analysis import _percentile


def summarize_windows(method_dir, slo_ms, window_seconds=60):
    method_dir = Path(method_dir)
    records = [json.loads(line) for line in (method_dir / 'requests.jsonl').read_text().splitlines() if line.strip()]
    starts = {r['phase']: r for r in records if r.get('event') == 'phase_start'}
    ends = {r['phase']: r for r in records if r.get('event') == 'phase_end'}
    rows = []
    for phase, start in starts.items():
        duration = float(ends[phase].get('generation_duration_s', start['duration_s']))
        requests = [r for r in records if r.get('event') == 'request' and r.get('phase') == phase]
        for name, lower in [('whole_phase', 0), ('late_phase', max(0, duration - window_seconds))]:
            selected = [r for r in requests if lower <= float(r.get('dispatch_offset_s', r['start_time'] - start['start_time'])) < duration]
            latencies = sorted(float(r['duration_ms']) for r in selected if r.get('success'))
            met = sum(r.get('success', False) and float(r['duration_ms']) <= slo_ms for r in selected)
            scheduled = cohort_started = cohort_met = ''
            # Slot accounting is independent of dispatch-time latency cohorts. Older
            # files without exact slot metadata retain unknown scheduled coverage.
            rate = start.get('rps_per_ue')
            ues = start.get('ues', [])
            if rate and ues and all('scheduled_offset_s' in r for r in requests):
                period = 1 / float(rate)
                scheduled = 0
                for pos in range(len(ues)):
                    offset = pos / len(ues) * period
                    first = max(0, math.ceil((lower - offset) / period - 1e-9))
                    stop = max(0, math.ceil((duration - offset) / period - 1e-9))
                    scheduled += stop - first
                cohort = [r for r in requests if lower <= float(r['scheduled_offset_s']) < duration]
                cohort_started = len(cohort)
                cohort_met = sum(r.get('success', False) and float(r['duration_ms']) <= slo_ms for r in cohort)
                if cohort_started > scheduled:
                    raise ValueError(f'{phase}: started slots exceed scheduled slots')
            rows.append({
                'phase': phase, 'window': name, 'start_offset_s': lower, 'end_offset_s': duration,
                'attempts': len(selected), 'successes': len(latencies), 'failures': len(selected) - len(latencies),
                'p50_ms': _percentile(latencies, .5), 'p90_ms': _percentile(latencies, .9),
                'violation_percent': 100 * (len(selected) - met) / len(selected) if selected else '',
                'scheduled': scheduled, 'started_slots': cohort_started,
                'missed_slots': scheduled - cohort_started if scheduled != '' else '',
                'request_coverage': cohort_started / scheduled if scheduled else '',
                'scheduled_slo_attainment': cohort_met / scheduled if scheduled else '',
                'steady_state_assumed': False,
            })
    return rows


def write_windows(rows, path):
    with Path(path).open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
