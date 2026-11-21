#!/usr/bin/env python3
"""Summarize four-arm vertical offloading campaigns on a shared phase axis.

Reuses phase validation conventions from utils.summarize, preserves phase_end
offered-load counters (scheduled, started, completed, missed, request_coverage),
reports both per-attempt SLO violation rate and scheduled-slot SLO attainment,
and tracks Core share with explicit denominators plus unidentified-backend counts.
"""
import argparse
import csv
import json
import math
from pathlib import Path

from utils.summarize import counts as parse_phase_counts


ARMS = ('baseline-local', 'proposed-local', 'baseline-vertical', 'proposed-vertical')
SITES = ('gnb-1', 'gnb-2', 'gnb-3')


def _p90(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(0.9 * len(ordered)) - 1]


def _phase_signature(phase_row: dict) -> tuple:
    return (
        phase_row['phase'],
        phase_row.get('workload_ue'),
        phase_row.get('gnb1_ues'),
        phase_row.get('gnb2_ues'),
        phase_row.get('gnb3_ues'),
        phase_row.get('duration_s'),
        phase_row.get('rps_per_ue'),
    )


def validate_arm_alignment(arms_summary: dict) -> None:
    """Ensure all present arms share identical phase order and offered load before plotting."""
    arm_names = [a for a in ARMS if a in arms_summary]
    if len(arm_names) < 2:
        return
    ref_arm = arm_names[0]
    ref_sigs = [_phase_signature(p) for p in arms_summary[ref_arm]['phases']]
    for arm in arm_names[1:]:
        sigs = [_phase_signature(p) for p in arms_summary[arm]['phases']]
        if sigs != ref_sigs:
            raise ValueError(
                f'phase/offered-load mismatch between {ref_arm} and {arm}: '
                f'{ref_sigs!r} != {sigs!r}'
            )


def summarize_arm(arm_dir: Path, slo_ms: float, *, core_node: str = 'core-5g') -> dict:
    req_path = arm_dir / 'requests.jsonl'
    if not req_path.is_file():
        raise FileNotFoundError(f'missing requests.jsonl in {arm_dir}')

    phases_order: list[str] = []
    phase_meta: dict[str, dict] = {}
    phase_end_meta: dict[str, dict] = {}
    phase_records: dict[str, list[dict]] = {}

    with req_path.open(encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            ev = rec.get('event')
            if ev == 'phase_start':
                phase, total, g1, g2, g3 = parse_phase_counts(rec)
                if phase not in phase_meta:
                    phases_order.append(phase)
                    phase_meta[phase] = {
                        'start_time': rec.get('start_time'),
                        'duration_s': rec.get('duration_s'),
                        'rps_per_ue': rec.get('rps_per_ue'),
                        'workload_ue': total,
                        'gnb1_ues': g1,
                        'gnb2_ues': g2,
                        'gnb3_ues': g3,
                    }
                    phase_records[phase] = []
            elif ev == 'phase_end':
                phase = rec['phase']
                phase_end_meta[phase] = {
                    'scheduled': rec.get('scheduled'),
                    'started': rec.get('started'),
                    'completed': rec.get('completed', rec.get('attempts')),
                    'missed': rec.get('missed'),
                    'transition_missed': rec.get('transition_missed'),
                    'request_coverage': rec.get('request_coverage'),
                    'per_ue': rec.get('per_ue') or [],
                }
            elif ev == 'request':
                phase = rec['phase']
                if phase not in phase_records:
                    phases_order.append(phase)
                    phase_meta[phase] = {}
                    phase_records[phase] = []
                phase_records[phase].append(rec)

    phase_rows = []
    site_core_rows = []
    total_scheduled = 0
    total_started = 0
    total_completed = 0
    total_missed = 0
    total_attempts = 0
    total_successes = 0
    total_slo_met = 0
    total_core_served = 0
    total_local_served = 0
    total_unidentified_backend = 0
    all_success_latencies: list[float] = []

    for idx, phase in enumerate(phases_order, start=1):
        recs = phase_records[phase]
        p_meta = phase_meta.get(phase) or {}
        end_meta = phase_end_meta.get(phase) or {}

        attempts = len(recs)
        completed = int(end_meta['completed']) if end_meta.get('completed') is not None else attempts
        scheduled = int(end_meta['scheduled']) if end_meta.get('scheduled') is not None else attempts
        started = int(end_meta['started']) if end_meta.get('started') is not None else attempts
        missed = int(end_meta['missed']) if end_meta.get('missed') is not None else max(0, scheduled - completed)
        request_coverage = (
            float(end_meta['request_coverage'])
            if end_meta.get('request_coverage') is not None
            else (completed / scheduled if scheduled > 0 else None)
        )

        success_recs = [r for r in recs if r.get('success')]
        success_latencies = [float(r['duration_ms']) for r in success_recs]
        successes = len(success_latencies)
        slo_met = sum(1 for ms in success_latencies if ms <= slo_ms)
        errors = attempts - successes
        timeouts = sum(1 for r in recs if r.get('timed_out') or r.get('http_status') == 504)
        p90_ms = _p90(success_latencies)

        violation_rate_attempt = (1.0 - (slo_met / attempts)) if attempts > 0 else None
        slo_attainment_scheduled = (slo_met / scheduled) if scheduled > 0 else None
        violation_rate_scheduled = (1.0 - slo_attainment_scheduled) if slo_attainment_scheduled is not None else None
        error_rate = (errors / attempts) if attempts > 0 else None

        core_served = sum(1 for r in success_recs if r.get('server_node') == core_node)
        local_served = sum(
            1 for r in success_recs
            if isinstance(r.get('server_node'), str) and r['server_node'].strip() and r['server_node'] != core_node
        )
        unidentified_backend = successes - core_served - local_served
        core_pct = (100.0 * core_served / successes) if successes > 0 else 0.0

        total_scheduled += scheduled
        total_started += started
        total_completed += completed
        total_missed += missed
        total_attempts += attempts
        total_successes += successes
        total_slo_met += slo_met
        total_core_served += core_served
        total_local_served += local_served
        total_unidentified_backend += unidentified_backend
        all_success_latencies.extend(success_latencies)

        phase_rows.append({
            'arm': arm_dir.name,
            'phase_index': idx,
            'phase': phase,
            'workload_ue': p_meta.get('workload_ue'),
            'gnb1_ues': p_meta.get('gnb1_ues'),
            'gnb2_ues': p_meta.get('gnb2_ues'),
            'gnb3_ues': p_meta.get('gnb3_ues'),
            'duration_s': p_meta.get('duration_s'),
            'rps_per_ue': p_meta.get('rps_per_ue'),
            'scheduled': scheduled,
            'started': started,
            'completed': completed,
            'missed': missed,
            'request_coverage': None if request_coverage is None else round(request_coverage, 6),
            'attempts': attempts,
            'successes': successes,
            'errors': errors,
            'timeouts': timeouts,
            'slo_met': slo_met,
            'p90_ms': None if p90_ms is None else round(p90_ms, 3),
            'slo_violation_rate': None if violation_rate_attempt is None else round(violation_rate_attempt, 6),
            'slo_violation_denominator': 'attempts',
            'slo_attainment_scheduled': None if slo_attainment_scheduled is None else round(slo_attainment_scheduled, 6),
            'slo_violation_rate_scheduled': None if violation_rate_scheduled is None else round(violation_rate_scheduled, 6),
            'slo_attainment_scheduled_denominator': 'scheduled',
            'error_rate': None if error_rate is None else round(error_rate, 6),
            'local_served_requests': local_served,
            'core_served_requests': core_served,
            'unidentified_backend_successes': unidentified_backend,
            'core_served_pct': round(core_pct, 3),
            'core_share_denominator': 'successes',
        })

        per_ue = end_meta.get('per_ue') or []
        for site in SITES:
            s_recs = [r for r in recs if r.get('group') == site]
            s_attempts = len(s_recs)
            s_succ = [r for r in s_recs if r.get('success')]
            s_core = sum(1 for r in s_succ if r.get('server_node') == core_node)
            s_local = sum(
                1 for r in s_succ
                if isinstance(r.get('server_node'), str) and r['server_node'].strip() and r['server_node'] != core_node
            )
            s_unid = len(s_succ) - s_core - s_local
            s_p90 = _p90([float(r['duration_ms']) for r in s_succ])
            s_slo_met = sum(1 for r in s_succ if float(r['duration_ms']) <= slo_ms)

            ue_site = [u for u in per_ue if u.get('group') == site]
            s_sched = sum(int(u.get('scheduled', 0)) for u in ue_site) if ue_site and any('scheduled' in u for u in ue_site) else s_attempts
            s_missed = sum(int(u.get('missed', 0)) for u in ue_site) if ue_site and any('missed' in u for u in ue_site) else max(0, s_sched - s_attempts)
            s_cov = (s_attempts / s_sched) if s_sched > 0 else None

            site_core_rows.append({
                'arm': arm_dir.name,
                'phase_index': idx,
                'phase': phase,
                'site': site,
                'scheduled': s_sched,
                'attempts': s_attempts,
                'missed': s_missed,
                'request_coverage': None if s_cov is None else round(s_cov, 6),
                'successes': len(s_succ),
                'slo_met': s_slo_met,
                'p90_ms': None if s_p90 is None else round(s_p90, 3),
                'slo_violation_rate': round(1.0 - (s_slo_met / s_attempts), 6) if s_attempts > 0 else None,
                'slo_attainment_scheduled': round(s_slo_met / s_sched, 6) if s_sched > 0 else None,
                'local_served_requests': s_local,
                'core_served_requests': s_core,
                'unidentified_backend_successes': s_unid,
                'core_served_pct': round(100.0 * s_core / len(s_succ), 3) if s_succ else 0.0,
                'core_share_denominator': 'successes',
            })

    route_changes = []
    events_path = arm_dir / 'offload-events.jsonl'
    if events_path.is_file():
        with events_path.open(encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                ev = json.loads(line)
                if ev.get('event') in ('route_decision', 'envoy_ack', 'envoy_update_failed'):
                    route_changes.append(ev)

    return {
        'arm': arm_dir.name,
        'scheduled': total_scheduled,
        'started': total_started,
        'completed': total_completed,
        'missed': total_missed,
        'request_coverage': round(total_completed / total_scheduled, 6) if total_scheduled > 0 else None,
        'attempts': total_attempts,
        'successes': total_successes,
        'slo_met': total_slo_met,
        'p90_ms': None if not all_success_latencies else round(_p90(all_success_latencies), 3),
        'slo_violation_rate': round(1.0 - (total_slo_met / total_attempts), 6) if total_attempts > 0 else None,
        'slo_violation_denominator': 'attempts',
        'slo_attainment_scheduled': round(total_slo_met / total_scheduled, 6) if total_scheduled > 0 else None,
        'slo_attainment_scheduled_denominator': 'scheduled',
        'local_served_requests': total_local_served,
        'core_served_requests': total_core_served,
        'unidentified_backend_successes': total_unidentified_backend,
        'core_served_pct': round(100.0 * total_core_served / total_successes, 3) if total_successes > 0 else 0.0,
        'core_share_denominator': 'successes',
        'phases': phase_rows,
        'site_phases': site_core_rows,
        'route_changes': route_changes,
    }


def summarize_campaign(campaign_dir: Path, slo_ms: float, *, core_node: str = 'core-5g') -> dict:
    arms_summary = {}
    all_phase_rows = []
    all_site_rows = []
    for arm in ARMS:
        arm_dir = campaign_dir / arm
        if arm_dir.is_dir():
            res = summarize_arm(arm_dir, slo_ms, core_node=core_node)
            arms_summary[arm] = res
            all_phase_rows.extend(res['phases'])
            all_site_rows.extend(res['site_phases'])

    validate_arm_alignment(arms_summary)

    if all_phase_rows:
        with (campaign_dir / 'vertical-4arm-phases.csv').open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(all_phase_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_phase_rows)

    if all_site_rows:
        with (campaign_dir / 'vertical-4arm-core-share.csv').open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(all_site_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_site_rows)

    summary = {'slo_ms': slo_ms, 'core_node': core_node, 'arms': arms_summary}
    (campaign_dir / 'vertical-4arm-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign_dir', type=Path)
    parser.add_argument('--slo-ms', type=float, default=150.0)
    parser.add_argument('--core-node', default='core-5g')
    args = parser.parse_args(argv)
    summarize_campaign(args.campaign_dir, args.slo_ms, core_node=args.core_node)


if __name__ == '__main__':
    main()
