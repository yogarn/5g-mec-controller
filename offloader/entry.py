"""LOCAL-state overload detection and SPREAD entry candidate evaluation."""
from offloader.spread import bucket_stale
from offloader.state import DecisionEvent, SiteState


def _unlabeled_timeouts(st: SiteState, unknown_bucket: dict, stale_after: float) -> tuple[int, int]:
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


def _update_local_metrics(st: SiteState, bucket: dict, unknown_bucket: dict, stale_after: float) -> dict:
    """Update st.last_* metrics from LOCAL + post-ACK unknown buckets and return effective stats."""
    unlabeled, unknown_seq = _unlabeled_timeouts(st, unknown_bucket, stale_after)
    p90_ms = bucket.get('p90_ms')
    successes = int(bucket.get('successes') or 0)
    attempts = int(bucket.get('attempts') or 0)
    errors = int(bucket.get('errors') or 0)
    eff_attempts = attempts + unlabeled
    eff_errors = errors + unlabeled
    eff_error_rate = round(eff_errors / eff_attempts, 6) if eff_attempts > 0 else bucket.get('error_rate')

    st.last_p90_ms, st.last_samples = p90_ms, successes
    st.last_attempts, st.last_error_rate = eff_attempts, eff_error_rate
    st.last_unlabeled_timeouts = unlabeled
    return {
        'p90_ms': p90_ms, 'successes': successes, 'attempts': attempts, 'errors': errors,
        'error_rate': bucket.get('error_rate'), 'unlabeled': unlabeled,
        'eff_attempts': eff_attempts, 'eff_errors': eff_errors, 'eff_error_rate': eff_error_rate,
        'seq': max(int(bucket.get('newest_sample_seq') or 0), unknown_seq),
    }


def _is_breaching(bucket: dict, cfg, local_stale: bool, m: dict) -> str | None:
    """Return the overload breach reason if LOCAL violates SLO or error rate, else None."""
    p90_breach = (
        not local_stale and m['successes'] >= cfg.min_samples
        and m['p90_ms'] is not None and m['p90_ms'] > cfg.offload_p90_threshold_ms
    )
    labeled_err = (
        not local_stale and m['attempts'] >= cfg.min_error_samples and (
            (m['error_rate'] is not None and m['error_rate'] >= cfg.offload_error_rate_threshold)
            or (bucket.get('status') == 'no_success' and m['errors'] >= cfg.min_error_samples)
        )
    )
    unlabeled_err = (
        m['unlabeled'] > 0 and m['eff_attempts'] >= cfg.min_error_samples and (
            (m['eff_error_rate'] is not None and m['eff_error_rate'] >= cfg.offload_error_rate_threshold)
            or (m['successes'] == 0 and m['eff_errors'] >= cfg.min_error_samples)
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


def _is_ready_to_enter(policy, st: SiteState, now: float, seq: int) -> bool:
    """Advance breach counters on fresh arrivals and check duration, fresh updates, and dwell."""
    if not policy._advance_breach(st, now, seq):
        st.last_status_note = 'breach_waiting_fresh_samples'
        return False
    st.last_status_note = 'breach_pending'
    cfg = policy.config
    return (
        (now - st.breach_started_at) >= cfg.breach_duration_seconds
        and st.breach_fresh_updates >= cfg.min_fresh_updates
        and policy._dwell_satisfied(st, now)
    )


def evaluate_local_entry(policy, *, site: str, st: SiteState, now: float, user_buckets: dict,
                         local_ready: int, core_ready: int, events: list[DecisionEvent]) -> dict | None:
    """Evaluate a LOCAL site and return an enter-SPREAD candidate once sustained breach & dwell pass."""
    cfg = policy.config
    policy._clear_recovery(st)
    bucket = user_buckets.get('local') or {}
    m = _update_local_metrics(st, bucket, user_buckets.get('unknown') or {}, cfg.stale_after_seconds)
    local_stale = bucket_stale(bucket, cfg.stale_after_seconds)

    if local_stale and m['unlabeled'] == 0:
        policy._clear_breach(st)
        note = 'telemetry_empty' if bucket.get('status') == 'empty' else 'telemetry_stale'
        _emit_local_hold(st, now, note, events, p90_ms=m['p90_ms'], samples=m['successes'],
                         attempts=m['attempts'], error_rate=m['error_rate'],
                         local_ready=local_ready, core_ready=core_ready)
        return None

    reason = _is_breaching(bucket, cfg, local_stale, m)
    if reason is None:
        policy._clear_breach(st)
        st.last_status_note = 'local_healthy'
        return None

    if core_ready == 0:
        policy._clear_breach(st)
        extra = {'unlabeled_timeouts': m['unlabeled']} if m['unlabeled'] else None
        _emit_local_hold(st, now, 'core_not_ready', events, p90_ms=m['p90_ms'], samples=m['successes'],
                         attempts=m['eff_attempts'], error_rate=m['eff_error_rate'],
                         local_ready=local_ready, core_ready=core_ready, extra=extra)
        return None

    if not _is_ready_to_enter(policy, st, now, m['seq']):
        return None
    return {
        'site': site, 'st': st, 'reason': reason, 'p90_ms': m['p90_ms'],
        'samples': m['successes'], 'attempts': m['eff_attempts'], 'error_rate': m['eff_error_rate'],
        'local_ready': local_ready, 'core_ready': core_ready, 'unlabeled_timeouts': m['unlabeled'],
    }
