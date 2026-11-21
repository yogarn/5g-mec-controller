"""Per-site bucket aggregation for /offload/snapshot and /sessions responses."""
from collections import Counter
import math

from collector.config import VALID_ROUTES, VALID_SITES


def summarize_bucket(records: list[tuple], now: float, stale_after: float) -> dict:
    """Compute p90, attempt/error/timeout counts, and freshness for one sample bucket."""
    if not records:
        return {
            'status': 'empty', 'p90_ms': None, 'attempts': 0, 'successes': 0,
            'errors': 0, 'timeouts': 0, 'error_rate': None, 'first_sample_ts': None,
            'last_sample_age_seconds': None, 'last_sample_ts': None,
            'last_success_age_seconds': None, 'newest_sample_seq': 0,
        }
    attempts = len(records)
    success_ms = sorted(dur * 1000.0 for _, _, dur, ok, _, _ in records if ok)
    successes = len(success_ms)
    errors = attempts - successes
    timeouts = sum(1 for _, _, _, _, timed_out, _ in records if timed_out)
    last_age = max(0.0, now - max(ts for ts, _, _, _, _, _ in records))
    success_monos = [ts for ts, _, _, ok, _, _ in records if ok]
    last_success_age = max(0.0, now - max(success_monos)) if success_monos else None
    p90_ms = round(success_ms[math.ceil(0.9 * successes) - 1], 3) if successes else None
    status = 'stale' if last_age > stale_after else ('no_success' if successes == 0 else 'ok')
    return {
        'status': status,
        'p90_ms': p90_ms,
        'attempts': attempts,
        'successes': successes,
        'errors': errors,
        'timeouts': timeouts,
        'error_rate': round(errors / attempts, 6),
        'first_sample_ts': round(min(w for _, w, _, _, _, _ in records), 6),
        'last_sample_age_seconds': round(last_age, 6),
        'last_sample_ts': round(max(w for _, w, _, _, _, _ in records), 6),
        'last_success_age_seconds': None if last_success_age is None else round(last_success_age, 6),
        'newest_sample_seq': max(seq for _, _, _, _, _, seq in records),
    }


def build_offload_snapshot(user_samples: list[tuple], probe_samples: list[tuple],
                           now: float, wall_now: float, settings: dict) -> dict:
    """Build the JSON payload for GET /offload/snapshot across all sites and route buckets."""
    stale_after = settings['offload_stale_seconds']
    sites = {}
    for site in VALID_SITES:
        user_buckets = {
            route: summarize_bucket(
                [(ts, w, dur, ok, to, seq) for ts, w, dur, ok, s, r, to, seq in user_samples if s == site and r == route],
                now, stale_after,
            )
            for route in VALID_ROUTES
        }
        overall_recs = [(ts, w, dur, ok, to, seq) for ts, w, dur, ok, s, _r, to, seq in user_samples if s == site]
        user_buckets['overall'] = summarize_bucket(overall_recs, now, stale_after)
        probe_recs = [
            (ts, w, dur, ok, to, seq)
            for ts, w, dur, ok, s, r, to, seq in probe_samples
            if s == site and r == 'local'
        ]
        sites[site] = {
            'node': settings['site_nodes'][site],
            'user': user_buckets,
            'probe': {'local': summarize_bucket(probe_recs, now, stale_after)},
        }
    return {
        'generated_at': round(wall_now, 6),
        'window_seconds': float(settings['offload_window_seconds']),
        'stale_after_seconds': float(stale_after),
        'p90_basis': 'success_only',
        'sites': sites,
    }


def build_sessions_snapshot(counts: Counter, mapping_time: float | None, now: float, settings: dict) -> dict:
    """Build the JSON payload for GET /sessions."""
    active_sessions = {node: counts[node] for node in settings['gnb_nodes'].values()}
    age = None if mapping_time is None else max(0.0, now - mapping_time)
    return {
        'active_sessions': active_sessions,
        'total_active_sessions': sum(active_sessions.values()),
        'refresh_age_seconds': None if age is None else round(age, 6),
        'mapping_max_age_seconds': settings['mapping_max_age_seconds'],
        'stale': age is None or age > settings['mapping_max_age_seconds'],
    }
