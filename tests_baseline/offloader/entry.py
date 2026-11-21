"""LOCAL-state overload detection and SPREAD entry candidate evaluation.

Evaluates a single site in the LOCAL route against p90 latency and error-rate
thresholds (including post-ACK unlabeled timeouts) and returns a SPREAD entry
candidate once breach duration, fresh updates, and dwell are satisfied.
"""
from offloader.spread import _bucket_stale
from offloader.state import DecisionEvent, SiteState


def evaluate_local_entry(
    policy,
    *,
    site: str,
    st: SiteState,
    now: float,
    user_buckets: dict,
    local_ready: int,
    core_ready: int,
    events: list[DecisionEvent],
) -> dict | None:
    """Evaluate a LOCAL site and return an enter-SPREAD candidate dict if triggered."""
    cfg = policy.config
    policy._clear_recovery(st)
    bucket = user_buckets.get('local') or {}
    unknown_bucket = user_buckets.get('unknown') or {}
    p90_ms = bucket.get('p90_ms')
    successes = int(bucket.get('successes') or 0)
    attempts = int(bucket.get('attempts') or 0)
    errors = int(bucket.get('errors') or 0)
    error_rate = bucket.get('error_rate')
    seq = int(bucket.get('newest_sample_seq') or 0)

    unlabeled_timeouts = 0
    unknown_seq = 0
    local_confirmed = (
        st.active_route == 'LOCAL'
        and st.pending_update_since is None
        and st.local_confirmed_since is not None
    )
    if (
        local_confirmed
        and not _bucket_stale(unknown_bucket, cfg.stale_after_seconds)
        and int(unknown_bucket.get('timeouts') or 0) > 0
    ):
        first_ts = unknown_bucket.get('first_sample_ts')
        if first_ts is not None and first_ts >= st.local_confirmed_since:
            unlabeled_timeouts = int(unknown_bucket.get('timeouts') or 0)
            unknown_seq = int(unknown_bucket.get('newest_sample_seq') or 0)

    effective_attempts = attempts + unlabeled_timeouts
    effective_errors = errors + unlabeled_timeouts
    effective_error_rate = (
        round(effective_errors / effective_attempts, 6) if effective_attempts > 0 else error_rate
    )
    effective_seq = max(seq, unknown_seq)

    st.last_p90_ms = p90_ms
    st.last_samples = successes
    st.last_attempts = effective_attempts
    st.last_error_rate = effective_error_rate
    st.last_unlabeled_timeouts = unlabeled_timeouts

    local_stale = _bucket_stale(bucket, cfg.stale_after_seconds)
    if local_stale and unlabeled_timeouts == 0:
        policy._clear_breach(st)
        note = 'telemetry_empty' if bucket.get('status') == 'empty' else 'telemetry_stale'
        if st.last_status_note != note:
            st.last_status_note = note
            events.append(
                DecisionEvent(
                    timestamp=now,
                    event='hold_route',
                    site=site,
                    old_route='LOCAL',
                    new_route='LOCAL',
                    desired_route='LOCAL',
                    active_route=st.active_route,
                    reason=note,
                    p90_ms=p90_ms,
                    samples=successes,
                    attempts=attempts,
                    error_rate=error_rate,
                    local_ready_endpoints=local_ready,
                    core_ready_endpoints=core_ready,
                )
            )
        return None

    p90_breach = (
        not local_stale
        and successes >= cfg.min_samples
        and p90_ms is not None
        and p90_ms > cfg.offload_p90_threshold_ms
    )
    labeled_error_breach = (
        not local_stale
        and attempts >= cfg.min_error_samples
        and (
            (error_rate is not None and error_rate >= cfg.offload_error_rate_threshold)
            or (bucket.get('status') == 'no_success' and errors >= cfg.min_error_samples)
        )
    )
    unlabeled_timeout_breach = (
        unlabeled_timeouts > 0
        and effective_attempts >= cfg.min_error_samples
        and (
            (effective_error_rate is not None and effective_error_rate >= cfg.offload_error_rate_threshold)
            or (successes == 0 and effective_errors >= cfg.min_error_samples)
        )
    )
    error_breach = labeled_error_breach or unlabeled_timeout_breach

    if not (p90_breach or error_breach):
        policy._clear_breach(st)
        st.last_status_note = 'local_healthy'
        return None

    if core_ready == 0:
        policy._clear_breach(st)
        if st.last_status_note != 'core_not_ready':
            st.last_status_note = 'core_not_ready'
            events.append(
                DecisionEvent(
                    timestamp=now,
                    event='hold_route',
                    site=site,
                    old_route='LOCAL',
                    new_route='LOCAL',
                    desired_route='LOCAL',
                    active_route=st.active_route,
                    reason='core_not_ready',
                    p90_ms=p90_ms,
                    samples=successes,
                    attempts=effective_attempts,
                    error_rate=effective_error_rate,
                    local_ready_endpoints=local_ready,
                    core_ready_endpoints=core_ready,
                    extra={'unlabeled_timeouts': unlabeled_timeouts} if unlabeled_timeouts else {},
                )
            )
        return None

    if not policy._advance_breach(st, now, effective_seq):
        st.last_status_note = 'breach_waiting_fresh_samples'
        return None
    st.last_status_note = 'breach_pending'

    if (
        (now - st.breach_started_at) >= cfg.breach_duration_seconds
        and st.breach_fresh_updates >= cfg.min_fresh_updates
        and policy._dwell_satisfied(st, now)
    ):
        if unlabeled_timeout_breach and not p90_breach and not labeled_error_breach:
            reason = 'local_unlabeled_timeout_breach'
        elif error_breach and not p90_breach:
            reason = 'local_error_rate_breach'
        elif p90_breach and error_breach:
            reason = 'local_p90_and_error_breach'
        else:
            reason = 'local_p90_breach'
        return {
            'site': site,
            'st': st,
            'reason': reason,
            'p90_ms': p90_ms,
            'samples': successes,
            'attempts': effective_attempts,
            'error_rate': effective_error_rate,
            'local_ready': local_ready,
            'core_ready': core_ready,
            'unlabeled_timeouts': unlabeled_timeouts,
        }
    return None
