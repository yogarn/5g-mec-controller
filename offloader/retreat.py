"""SPREAD-state gradual retreat and breach-restore evaluation.

Evaluates a single site in the SPREAD route after Envoy EDS/RDS ACK settlement:
restores a deeper Core share on breach, or steps down the Core share by half
(completing to LOCAL once below retreat_floor) when overall p90 stays within SLO.
"""
from offloader.spread import _bucket_stale, _overall_user_bucket, _post_ack_window, spread_core_ratio
from offloader.state import DecisionEvent, SiteState


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
    overall = _overall_user_bucket(user_buckets, cfg.stale_after_seconds)
    p90_ms = overall.get('p90_ms')
    successes = int(overall.get('successes') or 0)
    attempts = int(overall.get('attempts') or 0)
    errors = int(overall.get('errors') or 0)
    error_rate = overall.get('error_rate')
    seq = int(overall.get('newest_sample_seq') or 0)

    st.last_p90_ms = p90_ms
    st.last_samples = successes
    st.last_attempts = attempts
    st.last_error_rate = error_rate

    if st.pending_weight_since is not None:
        policy._clear_breach(st)
        policy._clear_recovery(st)
        policy._note_hold(
            events, st, site, 'spread_waiting_eds_ack',
            local_ready=local_ready, core_ready=core_ready,
            p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
        )
        return None, None

    if st.active_route != 'SPREAD' or _post_ack_window(snapshot, st.spread_confirmed_since, st.spread_wait_seconds) is None:
        policy._clear_breach(st)
        policy._clear_recovery(st)
        policy._note_hold(
            events, st, site, 'spread_waiting_post_ack',
            local_ready=local_ready, core_ready=core_ready,
            p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
        )
        return None, None

    if _bucket_stale(overall, cfg.stale_after_seconds):
        policy._clear_breach(st)
        policy._clear_recovery(st)
        note = 'spread_telemetry_empty' if overall.get('status') == 'empty' else 'spread_telemetry_stale'
        policy._note_hold(
            events, st, site, note,
            local_ready=local_ready, core_ready=core_ready,
            p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
        )
        return None, None

    if local_ready == 0:
        policy._clear_breach(st)
        policy._clear_recovery(st)
        policy._note_hold(
            events, st, site, 'spread_local_not_ready',
            local_ready=local_ready, core_ready=core_ready,
            p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
        )
        return None, None

    spread_error_breach = (
        attempts >= cfg.min_error_samples
        and (
            (error_rate is not None and error_rate >= cfg.offload_error_rate_threshold)
            or (overall.get('status') == 'no_success' and errors >= cfg.min_error_samples)
        )
    )
    spread_p90_breach = (
        successes >= cfg.min_samples
        and p90_ms is not None
        and p90_ms > cfg.offload_p90_threshold_ms
    )

    if spread_p90_breach or spread_error_breach:
        policy._clear_recovery(st)
        if st.retreat_level > st.good_level or st.good_level > 0:
            if policy._advance_breach(st, now, seq) and st.breach_fresh_updates >= cfg.min_fresh_updates:
                reason = 'spread_retreat_error_breach' if spread_error_breach and not spread_p90_breach else 'spread_retreat_p90_breach'
                restore_cand = {
                    'site': site,
                    'st': st,
                    'reason': reason,
                    # A level already judged healthy that now breaches goes one level further back.
                    'target_level': st.good_level if st.retreat_level > st.good_level else st.good_level - 1,
                    'core_ratio': spread_core_ratio(core_ready, local_ready, k_current, st.retreat_level, retreat_factor=retreat_factor),
                    'p90_ms': p90_ms,
                    'samples': successes,
                    'attempts': attempts,
                    'error_rate': error_rate,
                    'local_ready': local_ready,
                    'core_ready': core_ready,
                }
                return restore_cand, None
            st.last_status_note = 'spread_restore_pending'
        else:
            policy._clear_breach(st)
            note = 'spread_error_rate_high' if spread_error_breach and not spread_p90_breach else 'spread_p90_above_slo'
            policy._note_hold(
                events, st, site, note,
                local_ready=local_ready, core_ready=core_ready,
                p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
            )
        return None, None

    policy._clear_breach(st)

    if successes < cfg.min_samples or p90_ms is None:
        policy._clear_recovery(st)
        policy._note_hold(
            events, st, site, 'spread_insufficient_samples',
            local_ready=local_ready, core_ready=core_ready,
            p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
        )
        return None, None

    # Overall p90 <= SLO: advance recovery counter only on fresh sample arrivals.
    if st.recovery_started_at is None:
        st.recovery_started_at = now
        st.recovery_last_seq = seq
        st.recovery_fresh_updates = 1
        st.last_status_note = 'recovery_pending'
    elif seq > st.recovery_last_seq:
        st.recovery_last_seq = seq
        st.recovery_fresh_updates += 1
        st.last_status_note = 'recovery_pending'
    else:
        st.last_status_note = 'recovery_waiting_fresh_samples'
        return None, None

    if st.recovery_fresh_updates < cfg.min_fresh_updates:
        return None, None

    # Step proven healthy: promote good_level to current retreat_level.
    if st.retreat_level > st.good_level:
        st.good_level = st.retreat_level

    # Respect post-restore hold window before attempting another retreat step.
    if st.hold_until is not None and now < st.hold_until:
        policy._note_hold(
            events, st, site, 'spread_retreat_hold',
            local_ready=local_ready, core_ready=core_ready,
            p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
        )
        return None, None

    if policy._dwell_satisfied(st, now):
        next_level = st.retreat_level + 1
        next_ratio = spread_core_ratio(core_ready, local_ready, k_current, next_level, retreat_factor=retreat_factor)
        action = 'complete' if next_ratio < retreat_floor else 'step'
        retreat_cand = {
            'site': site,
            'st': st,
            'action': action,
            'next_level': next_level,
            'core_ratio': spread_core_ratio(core_ready, local_ready, k_current, st.retreat_level, retreat_factor=retreat_factor),
            'p90_ms': p90_ms,
            'samples': successes,
            'attempts': attempts,
            'error_rate': error_rate,
            'local_ready': local_ready,
            'core_ready': core_ready,
        }
        return None, retreat_cand
    return None, None
