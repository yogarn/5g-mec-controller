"""SPREAD traffic-share weight math and collector snapshot window helpers."""

# Halve the Core traffic share at each gradual retreat step to prevent oscillation.
SPREAD_RETREAT_FACTOR = 2
# Below 1% remaining Core share, Core contribution is negligible so the site returns to LOCAL.
SPREAD_RETREAT_FLOOR = 0.01
# After a retreat step is rolled back (SLO breach), wait min_dwell * 2^failures before the next try, so a
# site whose local pods cannot take more traffic stops probing every few seconds.
SPREAD_RETRY_BACKOFF_FACTOR = 2
# Upper bound of that wait (one 120 s phase), so a drop in load is still noticed.
SPREAD_RETRY_BACKOFF_MAX_SECONDS = 120.0


def retry_wait_seconds(min_dwell: float, failures: int) -> float:
    """Wait before the next retreat attempt after `failures` consecutive rolled-back attempts."""
    return max(min_dwell, min(min_dwell * SPREAD_RETRY_BACKOFF_FACTOR ** failures, SPREAD_RETRY_BACKOFF_MAX_SECONDS))


def spread_core_ratio(
    core_ready: int,
    local_ready: int,
    k: int,
    retreat_level: int = 0,
    *,
    retreat_factor: int = SPREAD_RETREAT_FACTOR,
) -> float:
    """Return target Core traffic share s_n = s_0 / factor^n where s_0 = c / (c + max(1, k) * x_i)."""
    denom = core_ready + max(1, k) * local_ready
    if denom <= 0 or core_ready <= 0:
        return 0.0
    return (core_ready / denom) / (retreat_factor ** retreat_level)


def spread_local_weight(
    core_ready: int,
    local_ready: int,
    k: int,
    retreat_level: int = 0,
    *,
    retreat_factor: int = SPREAD_RETREAT_FACTOR,
) -> int:
    """Return integer EDS load_balancing_weight (>= 1) per local pod (Core pods always get 1)."""
    if core_ready <= 0 or local_ready <= 0:
        return 1
    s_n = spread_core_ratio(core_ready, local_ready, k, retreat_level, retreat_factor=retreat_factor)
    return max(1, int(round(core_ready * (1.0 - s_n) / (s_n * local_ready))))


def bucket_stale(bucket: dict, stale_after: float) -> bool:
    """Return True when a collector telemetry bucket is empty, stale, or older than stale_after."""
    if not bucket or bucket.get('status') in ('empty', 'stale', None):
        return True
    age = bucket.get('last_sample_age_seconds')
    return age is None or age > stale_after


def post_ack_window(snapshot: dict | None, confirmed_since: float | None, wait: float | None = None):
    """Return snapshot window_seconds once enough time has elapsed after Envoy ACK."""
    if snapshot is None or confirmed_since is None:
        return None
    window = snapshot['window_seconds']
    required = window if wait is None else min(window, wait)
    return window if snapshot['generated_at'] - confirmed_since >= required else None


def overall_user_bucket(user_buckets: dict) -> dict:
    """Return the site's overall user bucket produced by the collector."""
    return user_buckets.get('overall') or {}


# Aliases kept for internal callers and tests.
_bucket_stale = bucket_stale
_post_ack_window = post_ack_window
_overall_user_bucket = overall_user_bucket
