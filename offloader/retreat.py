"""SPREAD-state gradual retreat and breach-restore evaluation."""
from offloader.spread import bucket_stale, overall_user_bucket, post_ack_window, spread_core_ratio
from offloader.state import DecisionEvent, SiteState


def _precondition_hold_reason(st: SiteState, snapshot: dict, overall: dict,
                              local_ready: int, stale_after: float) -> str | None:
    """Return a hold reason if SPREAD telemetry cannot be evaluated yet, else None."""
    if st.pending_weight_since is not None:
        return 'spread_waiting_eds_ack'
    if st.active_route != 'SPREAD' or post_ack_window(snapshot, st.spread_confirmed_since, st.spread_wait_seconds) is None:
        return 'spread_waiting_post_ack'
    if bucket_stale(overall, stale_after):
        return 'spread_telemetry_empty' if overall.get('status') == 'empty' else 'spread_telemetry_stale'
    if local_ready == 0:
        return 'spread_local_not_ready'
    return None


def _detect_spread_breach(overall: dict, cfg) -> tuple[bool, bool]:
    """Return (p90_breach, error_breach) for a SPREAD site's overall user bucket."""
    p90_ms = overall.get('p90_ms')
    successes = int(overall.get('successes') or 0)
    attempts = int(overall.get('attempts') or 0)
    errors = int(overall.get('errors') or 0)
    error_rate = overall.get('error_rate')

    error_breach = attempts >= cfg.min_error_samples and (
        (error_rate is not None and error_rate >= cfg.offload_error_rate_threshold)
        or (overall.get('status') == 'no_success' and errors >= cfg.min_error_samples)
    )
    p90_breach = successes >= cfg.min_samples and p90_ms is not None and p90_ms > cfg.offload_p90_threshold_ms
    return p90_breach, error_breach


def _advance_recovery(st: SiteState, now: float, seq: int) -> bool:
    """Advance the fresh-sample recovery counter; return False if no new sample arrived."""
    if st.recovery_started_at is None:
        st.recovery_started_at = now
        st.recovery_last_seq = seq
        st.recovery_fresh_updates = 1
        st.last_status_note = 'recovery_pending'
        return True
    if seq > st.recovery_last_seq:
        st.recovery_last_seq = seq
        st.recovery_fresh_updates += 1
        st.last_status_note = 'recovery_pending'
        return True
    st.last_status_note = 'recovery_waiting_fresh_samples'
    return False


def _evaluate_spread_breach(policy, *, site: str, st: SiteState, now: float, overall: dict,
                            p90_breach: bool, error_breach: bool, local_ready: int, core_ready: int,
                            k_current: int, retreat_factor: int, events: list[DecisionEvent]) -> dict | None:
    """Handle an SLO/error breach in SPREAD: build a restore candidate or hold at n=0."""
    policy._clear_recovery(st)
    seq = int(overall.get('newest_sample_seq') or 0)
    if st.retreat_level > st.good_level or st.good_level > 0:
        if policy._advance_breach(st, now, seq) and st.breach_fresh_updates >= policy.config.min_fresh_updates:
            reason = 'spread_retreat_error_breach' if error_breach and not p90_breach else 'spread_retreat_p90_breach'
            return {
                'site': site, 'st': st, 'reason': reason,
                # A level already judged healthy that now breaches goes one level further back.
                'target_level': st.good_level if st.retreat_level > st.good_level else st.good_level - 1,
                'core_ratio': spread_core_ratio(core_ready, local_ready, k_current, st.retreat_level, retreat_factor=retreat_factor),
                'p90_ms': overall.get('p90_ms'), 'samples': int(overall.get('successes') or 0),
                'attempts': int(overall.get('attempts') or 0), 'error_rate': overall.get('error_rate'),
                'local_ready': local_ready, 'core_ready': core_ready,
            }
        st.last_status_note = 'spread_restore_pending'
        return None

    policy._clear_breach(st)
    note = 'spread_error_rate_high' if error_breach and not p90_breach else 'spread_p90_above_slo'
    policy._note_hold(events, st, site, note, local_ready=local_ready, core_ready=core_ready)
    return None


def evaluate_spread_retreat(
    policy,
    *,
    site: str,
    st: SiteState,
    now: float,
    snapshot: dict,
    user_buckets: dict,
    local_ready: int,
    core_ready: int,
    k_current: int,
    retreat_factor: int,
    retreat_floor: float,
    events: list[DecisionEvent],
) -> tuple[dict | None, dict | None]:
    """Evaluate a SPREAD site and return (restore_candidate, retreat_candidate)."""
    cfg = policy.config
    overall = overall_user_bucket(user_buckets, cfg.stale_after_seconds)
    st.last_p90_ms = overall.get('p90_ms')
    st.last_samples = int(overall.get('successes') or 0)
    st.last_attempts = int(overall.get('attempts') or 0)
    st.last_error_rate = overall.get('error_rate')

    hold = _precondition_hold_reason(st, snapshot, overall, local_ready, cfg.stale_after_seconds)
    if hold is not None:
        policy._clear_breach(st)
        policy._clear_recovery(st)
        policy._note_hold(events, st, site, hold, local_ready=local_ready, core_ready=core_ready)
        return None, None

    p90_breach, error_breach = _detect_spread_breach(overall, cfg)
    if p90_breach or error_breach:
        return _evaluate_spread_breach(
            policy, site=site, st=st, now=now, overall=overall,
            p90_breach=p90_breach, error_breach=error_breach,
            local_ready=local_ready, core_ready=core_ready,
            k_current=k_current, retreat_factor=retreat_factor, events=events,
        ), None

    policy._clear_breach(st)
    if st.last_samples < cfg.min_samples or st.last_p90_ms is None:
        policy._clear_recovery(st)
        policy._note_hold(events, st, site, 'spread_insufficient_samples', local_ready=local_ready, core_ready=core_ready)
        return None, None

    if not _advance_recovery(st, now, int(overall.get('newest_sample_seq') or 0)):
        return None, None
    if st.recovery_fresh_updates < cfg.min_fresh_updates:
        return None, None

    if st.retreat_level > st.good_level:
        st.good_level = st.retreat_level
    if st.hold_until is not None and now < st.hold_until:
        policy._note_hold(events, st, site, 'spread_retreat_hold', local_ready=local_ready, core_ready=core_ready)
        return None, None
    if not policy._dwell_satisfied(st, now):
        return None, None

    next_level = st.retreat_level + 1
    next_ratio = spread_core_ratio(core_ready, local_ready, k_current, next_level, retreat_factor=retreat_factor)
    return None, {
        'site': site, 'st': st, 'action': 'complete' if next_ratio < retreat_floor else 'step',
        'next_level': next_level,
        'core_ratio': spread_core_ratio(core_ready, local_ready, k_current, st.retreat_level, retreat_factor=retreat_factor),
        'p90_ms': st.last_p90_ms, 'samples': st.last_samples,
        'attempts': st.last_attempts, 'error_rate': st.last_error_rate,
        'local_ready': local_ready, 'core_ready': core_ready,
    }
