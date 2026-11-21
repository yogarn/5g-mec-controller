"""LOCAL-state overload detection and SPREAD entry candidate evaluation."""
from offloader.spread import bucket_stale
from offloader.state import DecisionEvent, SiteState


def _post_ack_unlabeled_timeouts(st: SiteState, unknown_bucket: dict, stale_after: float) -> tuple[int, int]:
    """Count timeouts in the unknown bucket only when attributed to confirmed LOCAL traffic."""
    local_confirmed = (
        st.active_route == 'LOCAL'
        and st.pending_update_since is None
        and st.local_confirmed_since is not None
    )
    if not local_confirmed or bucket_stale(unknown_bucket, stale_after):
        return 0, 0
    timeouts = int(unknown_bucket.get('timeouts') or 0)
    first_ts = unknown_bucket.get('first_sample_ts')
    if timeouts > 0 and first_ts is not None and first_ts >= st.local_confirmed_since:
        return timeouts, int(unknown_bucket.get('newest_sample_seq') or 0)
    return 0, 0


def _detect_local_breach(bucket: dict, cfg, local_stale: bool, unlabeled_timeouts: int,
                         eff_attempts: int, eff_errors: int, eff_error_rate) -> str | None:
    """Return the breach reason string if LOCAL telemetry violates SLO or error rate, else None."""
    p90_ms = bucket.get('p90_ms')
    successes = int(bucket.get('successes') or 0)
    attempts = int(bucket.get('attempts') or 0)
    errors = int(bucket.get('errors') or 0)
    error_rate = bucket.get('error_rate')

    p90_breach = (
        not local_stale
        and successes >= cfg.min_samples
        and p90_ms is not None
        and p90_ms > cfg.offload_p90_threshold_ms
    )
    labeled_err = (
        not local_stale
        and attempts >= cfg.min_error_samples
        and (
            (error_rate is not None and error_rate >= cfg.offload_error_rate_threshold)
            or (bucket.get('status') == 'no_success' and errors >= cfg.min_error_samples)
        )
    )
    unlabeled_err = (
        unlabeled_timeouts > 0
        and eff_attempts >= cfg.min_error_samples
        and (
            (eff_error_rate is not None and eff_error_rate >= cfg.offload_error_rate_threshold)
            or (successes == 0 and eff_errors >= cfg.min_error_samples)
        )
    )
    if not (p90_breach or labeled_err or unlabeled_err):
        return None
    if unlabeled_err and not p90_breach and not labeled_err:
        return 'local_unlabeled_timeout_breach'
    if (labeled_err or unlabeled_err) and not p90_breach:
        return 'local_error_rate_breach'
    return 'local_p90_and_error_breach' if (p90_breach and (labeled_err or unlabeled_err)) else 'local_p90_breach'


def _emit_local_hold(st: SiteState, now: float, reason: str, events: list[DecisionEvent], *,
                     p90_ms, samples: int, attempts: int, error_rate,
                     local_ready: int, core_ready: int, extra: dict | None = None):
    if st.last_status_note == reason:
        return
    st.last_status_note = reason
    events.append(st.make_event(
        timestamp=now, event='hold_route', old_route='LOCAL', new_route='LOCAL',
        desired_route='LOCAL', reason=reason, p90_ms=p90_ms, samples=samples,
        attempts=attempts, error_rate=error_rate,
        local_ready=local_ready, core_ready=core_ready, extra=extra,
    ))


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
    """Evaluate a LOCAL site and return an enter-SPREAD candidate once sustained breach & dwell pass."""
    cfg = policy.config
    policy._clear_recovery(st)
    bucket = user_buckets.get('local') or {}
    unlabeled_timeouts, unknown_seq = _post_ack_unlabeled_timeouts(
        st, user_buckets.get('unknown') or {}, cfg.stale_after_seconds,
    )
    p90_ms = bucket.get('p90_ms')
    successes = int(bucket.get('successes') or 0)
    attempts = int(bucket.get('attempts') or 0)
    errors = int(bucket.get('errors') or 0)
    eff_attempts = attempts + unlabeled_timeouts
    eff_errors = errors + unlabeled_timeouts
    eff_error_rate = round(eff_errors / eff_attempts, 6) if eff_attempts > 0 else bucket.get('error_rate')

    st.last_p90_ms, st.last_samples = p90_ms, successes
    st.last_attempts, st.last_error_rate = eff_attempts, eff_error_rate
    st.last_unlabeled_timeouts = unlabeled_timeouts

    local_stale = bucket_stale(bucket, cfg.stale_after_seconds)
    if local_stale and unlabeled_timeouts == 0:
        policy._clear_breach(st)
        note = 'telemetry_empty' if bucket.get('status') == 'empty' else 'telemetry_stale'
        _emit_local_hold(
            st, now, note, events, p90_ms=p90_ms, samples=successes,
            attempts=attempts, error_rate=bucket.get('error_rate'),
            local_ready=local_ready, core_ready=core_ready,
        )
        return None

    reason = _detect_local_breach(bucket, cfg, local_stale, unlabeled_timeouts, eff_attempts, eff_errors, eff_error_rate)
    if reason is None:
        policy._clear_breach(st)
        st.last_status_note = 'local_healthy'
        return None

    if core_ready == 0:
        policy._clear_breach(st)
        _emit_local_hold(
            st, now, 'core_not_ready', events, p90_ms=p90_ms, samples=successes,
            attempts=eff_attempts, error_rate=eff_error_rate, local_ready=local_ready,
            core_ready=core_ready, extra={'unlabeled_timeouts': unlabeled_timeouts} if unlabeled_timeouts else None,
        )
        return None

    effective_seq = max(int(bucket.get('newest_sample_seq') or 0), unknown_seq)
    if not policy._advance_breach(st, now, effective_seq):
        st.last_status_note = 'breach_waiting_fresh_samples'
        return None
    st.last_status_note = 'breach_pending'

    if (
        (now - st.breach_started_at) >= cfg.breach_duration_seconds
        and st.breach_fresh_updates >= cfg.min_fresh_updates
        and policy._dwell_satisfied(st, now)
    ):
        return {
            'site': site, 'st': st, 'reason': reason, 'p90_ms': p90_ms,
            'samples': successes, 'attempts': eff_attempts, 'error_rate': eff_error_rate,
            'local_ready': local_ready, 'core_ready': core_ready,
            'unlabeled_timeouts': unlabeled_timeouts,
        }
    return None
