"""SPREAD traffic-share weight math and collector snapshot window helpers.

Computes per-site Core traffic shares and Envoy EDS local pod weights for gradual
SPREAD retreat, and extracts freshness-checked telemetry buckets from snapshots.
"""
import math


def spread_core_ratio(core_ready: int, local_ready: int, k: int, retreat_level: int = 0, *, retreat_factor: int = 2) -> float:
    """Return target Core traffic share s_n = s_0 / 2^n where s_0 = c / (c + max(1, k) * x_i)."""
    c = max(0, int(core_ready))
    x_i = max(0, int(local_ready))
    k_eff = max(1, int(k))
    n = max(0, int(retreat_level))
    denom = c + k_eff * x_i
    if denom <= 0 or c == 0:
        return 0.0
    s0 = c / denom
    return s0 / (retreat_factor ** n)


def spread_local_weight(core_ready: int, local_ready: int, k: int, retreat_level: int = 0, *, retreat_factor: int = 2) -> int:
    """Return integer EDS load_balancing_weight (>= 1) per local pod for target Core share s_n.

    Core pods always receive weight 1. Local pods receive:
        w_l = max(1, round(c * (1 - s_n) / (s_n * x_i)))
    Edge cases:
    - c == 0 or x_i == 0: returns 1 (valid >= 1 integer without division by zero).
    """
    c = max(0, int(core_ready))
    x_i = max(0, int(local_ready))
    if c == 0 or x_i == 0:
        return 1
    s_n = spread_core_ratio(c, x_i, k, retreat_level, retreat_factor=retreat_factor)
    if s_n <= 0.0:
        return 1
    if s_n >= 1.0:
        return 1
    return max(1, int(round(c * (1.0 - s_n) / (s_n * x_i))))


def _bucket_stale(bucket, stale_after):
    if not isinstance(bucket, dict):
        return True
    status = bucket.get('status')
    if status in ('empty', 'stale', None):
        return True
    age = bucket.get('last_sample_age_seconds')
    if age is None or age > stale_after:
        return True
    return False


def _post_ack_window(snapshot, confirmed_since, wait=None):
    """Return the snapshot window once enough time has passed since the ACK.

    By default a full collector window must pass so no sample predates the change. A retreat step that
    followed a healthy level may be judged after `wait` seconds instead; the window then still holds the
    previous level's (already healthy) samples, and any slow samples of the new level lift the p90.
    """
    if not isinstance(snapshot, dict):
        return None
    window = snapshot.get('window_seconds')
    generated = snapshot.get('generated_at')
    if (
        confirmed_since is None
        or not isinstance(window, (int, float)) or not math.isfinite(window) or window <= 0
        or not isinstance(generated, (int, float)) or not math.isfinite(generated)
    ):
        return None
    required = window if wait is None else min(window, wait)
    if generated - confirmed_since < required:
        return None
    return window


def _overall_user_bucket(user_buckets: dict, stale_after: float) -> dict:
    """Return the site's overall user bucket from the snapshot.

    When the collector provides user['overall'], it includes all user samples for
    the site regardless of route (local, core, and unknown failures/timeouts).
    If an older snapshot omits 'overall', this falls back to combining 'local' and
    'core' only (which does NOT include 'unknown' samples).
    """
    overall = user_buckets.get('overall')
    if isinstance(overall, dict):
        return overall
    local_b = user_buckets.get('local') or {}
    core_b = user_buckets.get('core') or {}
    attempts = int(local_b.get('attempts') or 0) + int(core_b.get('attempts') or 0)
    successes = int(local_b.get('successes') or 0) + int(core_b.get('successes') or 0)
    errors = int(local_b.get('errors') or 0) + int(core_b.get('errors') or 0)
    timeouts = int(local_b.get('timeouts') or 0) + int(core_b.get('timeouts') or 0)
    seq = max(int(local_b.get('newest_sample_seq') or 0), int(core_b.get('newest_sample_seq') or 0))
    ages = [
        a for a in (local_b.get('last_sample_age_seconds'), core_b.get('last_sample_age_seconds'))
        if a is not None
    ]
    age = min(ages) if ages else None
    p90s = [
        b.get('p90_ms')
        for b in (local_b, core_b)
        if not _bucket_stale(b, stale_after) and b.get('p90_ms') is not None
    ]
    p90_ms = max(p90s) if p90s else None
    if attempts == 0:
        status = 'empty'
    elif age is None or age > stale_after:
        status = 'stale'
    elif successes == 0:
        status = 'no_success'
    else:
        status = 'ok'
    return {
        'status': status,
        'p90_ms': p90_ms,
        'attempts': attempts,
        'successes': successes,
        'errors': errors,
        'timeouts': timeouts,
        'error_rate': None if attempts == 0 else round(errors / attempts, 6),
        'last_sample_age_seconds': age,
        'newest_sample_seq': seq,
    }
