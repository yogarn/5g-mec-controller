"""SPREAD traffic-share weight math and collector snapshot window helpers."""
import math

# Halve the Core traffic share at each gradual retreat step to prevent oscillation.
SPREAD_RETREAT_FACTOR = 2
# Below 1% remaining Core share, Core contribution is negligible so the site returns to LOCAL.
SPREAD_RETREAT_FLOOR = 0.01


def spread_core_ratio(
    core_ready: int,
    local_ready: int,
    k: int,
    retreat_level: int = 0,
    *,
    retreat_factor: int = SPREAD_RETREAT_FACTOR,
) -> float:
    """Return target Core traffic share s_n = s_0 / factor^n where s_0 = c / (c + max(1, k) * x_i)."""
    c = max(0, int(core_ready))
    x_i = max(0, int(local_ready))
    denom = c + max(1, int(k)) * x_i
    if denom <= 0 or c == 0:
        return 0.0
    return (c / denom) / (retreat_factor ** max(0, int(retreat_level)))


def spread_local_weight(
    core_ready: int,
    local_ready: int,
    k: int,
    retreat_level: int = 0,
    *,
    retreat_factor: int = SPREAD_RETREAT_FACTOR,
) -> int:
    """Return integer EDS load_balancing_weight (>= 1) per local pod (Core pods always get 1)."""
    c = max(0, int(core_ready))
    x_i = max(0, int(local_ready))
    if c == 0 or x_i == 0:
        return 1
    s_n = spread_core_ratio(c, x_i, k, retreat_level, retreat_factor=retreat_factor)
    if s_n <= 0.0 or s_n >= 1.0:
        return 1
    return max(1, int(round(c * (1.0 - s_n) / (s_n * x_i))))


def bucket_stale(bucket, stale_after: float) -> bool:
    """Return True when a collector telemetry bucket is missing, empty, or older than stale_after."""
    if not isinstance(bucket, dict) or bucket.get('status') in ('empty', 'stale', None):
        return True
    age = bucket.get('last_sample_age_seconds')
    return age is None or age > stale_after


def post_ack_window(snapshot, confirmed_since, wait=None):
    """Return snapshot window_seconds once enough time has elapsed after Envoy ACK."""
    if not isinstance(snapshot, dict) or confirmed_since is None:
        return None
    window = snapshot.get('window_seconds')
    generated = snapshot.get('generated_at')
    if (
        not isinstance(window, (int, float)) or not math.isfinite(window) or window <= 0
        or not isinstance(generated, (int, float)) or not math.isfinite(generated)
    ):
        return None
    required = window if wait is None else min(window, wait)
    return window if generated - confirmed_since >= required else None


def overall_user_bucket(user_buckets: dict, stale_after: float) -> dict:
    """Return user['overall'], falling back to combining local + core when absent."""
    overall = user_buckets.get('overall')
    if isinstance(overall, dict):
        return overall
    local_b = user_buckets.get('local') or {}
    core_b = user_buckets.get('core') or {}
    attempts = int(local_b.get('attempts') or 0) + int(core_b.get('attempts') or 0)
    successes = int(local_b.get('successes') or 0) + int(core_b.get('successes') or 0)
    errors = int(local_b.get('errors') or 0) + int(core_b.get('errors') or 0)
    timeouts = int(local_b.get('timeouts') or 0) + int(core_b.get('timeouts') or 0)
    ages = [a for a in (local_b.get('last_sample_age_seconds'), core_b.get('last_sample_age_seconds')) if a is not None]
    age = min(ages) if ages else None
    p90s = [
        b.get('p90_ms') for b in (local_b, core_b)
        if not bucket_stale(b, stale_after) and b.get('p90_ms') is not None
    ]
    if attempts == 0:
        status = 'empty'
    elif age is None or age > stale_after:
        status = 'stale'
    else:
        status = 'no_success' if successes == 0 else 'ok'
    return {
        'status': status,
        'p90_ms': max(p90s) if p90s else None,
        'attempts': attempts,
        'successes': successes,
        'errors': errors,
        'timeouts': timeouts,
        'error_rate': None if attempts == 0 else round(errors / attempts, 6),
        'last_sample_age_seconds': age,
        'newest_sample_seq': max(int(local_b.get('newest_sample_seq') or 0), int(core_b.get('newest_sample_seq') or 0)),
    }


# Aliases kept for internal callers and tests.
_bucket_stale = bucket_stale
_post_ack_window = post_ack_window
_overall_user_bucket = overall_user_bucket
