#!/usr/bin/env python3
"""Offline 10-second binned transient analysis for phase transitions.

Writes derived CSVs into a new output directory without modifying the original
campaign results directory.
"""
import argparse
import csv
import json
import math
from pathlib import Path


def _percentile(sorted_values, fraction):
    if not sorted_values:
        return ''
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * fraction
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)


def _scheduled_per_bin(num_ues, rps_per_ue, duration_s, bin_seconds, num_bins):
    counts = [0] * num_bins
    if num_ues <= 0 or rps_per_ue <= 0 or duration_s <= 0:
        return counts
    period = 1.0 / float(rps_per_ue)
    for pos in range(num_ues):
        offset = (pos / num_ues) * period
        slot_index = 0
        slot = offset
        while slot < duration_s:
            idx = min(num_bins - 1, max(0, int((slot + 1e-9) // bin_seconds)))
            counts[idx] += 1
            slot_index += 1
            slot = offset + slot_index * period
    return counts


def _infer_slo_ms(method_dir, explicit_slo_ms):
    if explicit_slo_ms is not None:
        return float(explicit_slo_ms)
    summary = method_dir / 'summary.csv'
    if summary.exists():
        with summary.open(newline='', encoding='utf-8') as stream:
            for row in csv.DictReader(stream):
                if row.get('slo_ms'):
                    return float(row['slo_ms'])
    return 150.0


def analyze_method_transient(method_dir, output_csv, phase='phase_02', bin_seconds=10.0, slo_ms=None):
    """Bin one method's phase requests, pod readiness, restarts, and HPA status."""
    method_dir = Path(method_dir)
    slo_ms = _infer_slo_ms(method_dir, slo_ms)
    records = [json.loads(line) for line in (method_dir / 'requests.jsonl').read_text().splitlines() if line.strip()]
    start_rec = next((r for r in records if r.get('event') == 'phase_start' and r.get('phase') == phase), None)
    end_rec = next((r for r in records if r.get('event') == 'phase_end' and r.get('phase') == phase), None)
    if start_rec is None or end_rec is None:
        raise ValueError(f'{method_dir}: missing phase_start/phase_end for {phase}')

    phase_start = float(start_rec['start_time'])
    duration_s = float(end_rec.get('generation_duration_s', start_rec['duration_s']))
    num_bins = max(1, int(math.ceil(duration_s / bin_seconds)))
    rps_per_ue = start_rec.get('rps_per_ue')
    ues = start_rec.get('ues') or []
    num_ues = len(ues)
    ue_pos_by_source = {ue.get('source_address'): idx for idx, ue in enumerate(ues) if ue.get('source_address')}
    if rps_per_ue is not None and num_ues > 0:
        period = 1.0 / float(rps_per_ue)
        scheduled_bins = _scheduled_per_bin(num_ues, float(rps_per_ue), duration_s, bin_seconds, num_bins)
    else:
        period = None
        scheduled_bins = None

    req_bins = [{'started': 0, 'success_latencies': [], 'failures': 0, 'slo_met': 0} for _ in range(num_bins)]
    started_in_sched_bin = [0] * num_bins
    slo_met_in_sched_bin = [0] * num_bins
    for rec in records:
        if rec.get('event') != 'request' or rec.get('phase') != phase:
            continue
        offset_s = rec.get('dispatch_offset_s')
        if offset_s is None:
            offset_s = float(rec['start_time']) - phase_start
        offset_s = float(offset_s)
        idx = min(num_bins - 1, max(0, int((offset_s + 1e-9) // bin_seconds)))
        bucket = req_bins[idx]
        bucket['started'] += 1
        met_slo = False
        if rec.get('success'):
            lat = float(rec['duration_ms'])
            bucket['success_latencies'].append(lat)
            if lat <= slo_ms:
                bucket['slo_met'] += 1
                met_slo = True
        else:
            bucket['failures'] += 1

        if scheduled_bins is not None:
            sched_offset_s = rec.get('scheduled_offset_s')
            if sched_offset_s is not None:
                sched_idx = min(num_bins - 1, max(0, int((float(sched_offset_s) + 1e-9) // bin_seconds)))
            else:
                pos = ue_pos_by_source.get(rec.get('source_address'))
                if pos is None and rec.get('ue_index') is not None and 0 <= int(rec['ue_index']) < num_ues:
                    pos = int(rec['ue_index'])
                if pos is not None:
                    ue_offset = (pos / num_ues) * period
                    slot_idx = max(0, int(math.floor((offset_s - ue_offset + 1e-9) / period)))
                    sched_slot = ue_offset + slot_idx * period
                    sched_idx = min(num_bins - 1, max(0, int((sched_slot + 1e-9) // bin_seconds)))
                else:
                    sched_idx = idx
            started_in_sched_bin[sched_idx] += 1
            if met_slo:
                slo_met_in_sched_bin[sched_idx] += 1
        elif met_slo:
            slo_met_in_sched_bin[idx] += 1

    if scheduled_bins is not None:
        missed_bins = [max(0, s - st) for s, st in zip(scheduled_bins, started_in_sched_bin)]
    else:
        scheduled_bins = [b['started'] for b in req_bins]
        missed_bins = [0] * num_bins

    total_scheduled = sum(scheduled_bins)
    total_started = sum(b['started'] for b in req_bins)
    total_missed = sum(missed_bins)
    if end_rec.get('scheduled') is not None and total_scheduled != int(end_rec['scheduled']):
        raise ValueError(f'{method_dir} {phase}: binned scheduled {total_scheduled} != recorded {end_rec["scheduled"]}')
    if end_rec.get('started') is not None and total_started != int(end_rec['started']):
        raise ValueError(f'{method_dir} {phase}: binned started {total_started} != recorded {end_rec["started"]}')
    if end_rec.get('missed') is not None and total_missed != int(end_rec['missed']):
        raise ValueError(f'{method_dir} {phase}: binned missed {total_missed} != recorded {end_rec["missed"]}')

    pod_bins = [[] for _ in range(num_bins)]
    metrics_path = method_dir / 'metrics.jsonl'
    if metrics_path.exists():
        for line in metrics_path.read_text().splitlines():
            if not line.strip():
                continue
            obs = json.loads(line)
            if 'pods' not in obs:
                continue
            ts = float(obs['sample_time'])
            if not phase_start <= ts <= phase_start + duration_s:
                continue
            idx = min(num_bins - 1, max(0, int((ts - phase_start) // bin_seconds)))
            pods = obs['pods']
            restarts = sum(
                cs.get('restarts', 0) for cs in (obs.get('container_statuses') or {}).values()
            )
            pod_bins[idx].append((len(pods), sum(1 for p in pods if p.get('ready')), restarts))

    hpa_bins = [[] for _ in range(num_bins)]
    hpa_path = method_dir / 'hpa-status.jsonl'
    if hpa_path.exists():
        for line in hpa_path.read_text().splitlines():
            if not line.strip():
                continue
            obs = json.loads(line)
            if 'desired_replicas' not in obs:
                continue
            ts = float(obs['sample_time'])
            if not phase_start <= ts <= phase_start + duration_s:
                continue
            idx = min(num_bins - 1, max(0, int((ts - phase_start) // bin_seconds)))
            hpa_bins[idx].append((obs.get('current_replicas'), obs.get('desired_replicas')))

    fields = [
        'phase', 'bin_index', 'bin_start_s', 'bin_end_s',
        'scheduled', 'started', 'completed', 'missed', 'successes', 'failures',
        'success_p50_ms', 'success_p90_ms',
        'all_attempt_violation_rate', 'slo_attainment_all_attempts', 'slo_attainment_scheduled',
        'pods_min', 'pods_max', 'ready_min', 'ready_max', 'pods_end', 'ready_end', 'restarts_max',
        'hpa_current_end', 'hpa_desired_max',
    ]
    with Path(output_csv).open('x', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for idx in range(num_bins):
            bucket = req_bins[idx]
            started = bucket['started']
            scheduled = scheduled_bins[idx]
            missed = missed_bins[idx]
            successes = len(bucket['success_latencies'])
            failures = bucket['failures']
            completed = successes + failures
            lats = sorted(bucket['success_latencies'])
            slo_met = bucket['slo_met']
            p_list = pod_bins[idx]
            h_list = [h for h in hpa_bins[idx] if h[0] is not None and h[1] is not None]
            writer.writerow({
                'phase': phase,
                'bin_index': idx + 1,
                'bin_start_s': idx * bin_seconds,
                'bin_end_s': min(duration_s, (idx + 1) * bin_seconds),
                'scheduled': scheduled,
                'started': started,
                'completed': completed,
                'missed': missed,
                'successes': successes,
                'failures': failures,
                'success_p50_ms': _percentile(lats, 0.50),
                'success_p90_ms': _percentile(lats, 0.90),
                'all_attempt_violation_rate': (completed - slo_met) / completed if completed else '',
                'slo_attainment_all_attempts': slo_met / completed if completed else '',
                'slo_attainment_scheduled': slo_met_in_sched_bin[idx] / scheduled if scheduled else '',
                'pods_min': min(p[0] for p in p_list) if p_list else '',
                'pods_max': max(p[0] for p in p_list) if p_list else '',
                'ready_min': min(p[1] for p in p_list) if p_list else '',
                'ready_max': max(p[1] for p in p_list) if p_list else '',
                'pods_end': p_list[-1][0] if p_list else '',
                'ready_end': p_list[-1][1] if p_list else '',
                'restarts_max': max(p[2] for p in p_list) if p_list else '',
                'hpa_current_end': h_list[-1][0] if h_list else '',
                'hpa_desired_max': max(h[1] for h in h_list) if h_list else '',
            })


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results_dir', type=Path, help='original campaign results directory (read-only)')
    parser.add_argument('output_dir', type=Path, help='new output directory for derived CSVs')
    parser.add_argument('--phase', default='phase_02', help='phase name to bin (default: phase_02)')
    parser.add_argument('--bin-seconds', type=float, default=10.0, help='bin width in seconds (default: 10)')
    parser.add_argument('--slo-ms', type=float, default=None, help='SLO threshold in ms (default: from summary.csv or 150)')
    args = parser.parse_args(argv)

    src = args.results_dir.resolve()
    dst = args.output_dir.resolve()
    if dst == src or src in dst.parents:
        parser.error('output_dir must be a new directory outside results_dir')
    if args.output_dir.exists():
        parser.error(f'output_dir already exists; provide a new directory: {args.output_dir}')
    if args.bin_seconds <= 0 or not math.isfinite(args.bin_seconds):
        parser.error('--bin-seconds must be finite and positive')

    args.output_dir.mkdir(parents=True, exist_ok=False)
    written = []
    for method in ('baseline', 'proposed'):
        method_dir = args.results_dir / method
        if (method_dir / 'requests.jsonl').exists():
            out_csv = args.output_dir / f'{method}-{args.phase}-10s-bins.csv'
            analyze_method_transient(
                method_dir, out_csv, phase=args.phase,
                bin_seconds=args.bin_seconds, slo_ms=args.slo_ms,
            )
            written.append(out_csv)
    if not written:
        raise ValueError(f'no baseline/proposed requests.jsonl found under {args.results_dir}')
    for path in written:
        print(f'Wrote {path}')


if __name__ == '__main__':
    main()
