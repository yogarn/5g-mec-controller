"""Envoy RDS/EDS ACK, NACK, and timeout state transitions for VerticalOffloadPolicy."""
from offloader.state import DecisionEvent, SiteState


def record_rds_ack(policy, site: str, route: str, version: str, now: float, *,
                   monotonic_now: float | None = None) -> DecisionEvent | None:
    """Record Envoy RDS ACK for a site route version and anchor its post-ACK window."""
    st = policy.states[site]
    route, old_active = route.upper(), st.active_route
    st.active_route, st.envoy_update_failed, st.envoy_last_nack = route, False, None
    if st.active_route == st.desired_route:
        if st.pending_update_since is not None and monotonic_now is not None:
            policy.last_route_switch_at = monotonic_now
        st.pending_update_since = None
        if route == 'LOCAL':
            if st.local_confirmed_since is None or old_active != 'LOCAL':
                st.local_confirmed_since = now
            st.spread_confirmed_since = None
        else:
            st.local_confirmed_since = None
            if st.spread_confirmed_since is None or old_active != 'SPREAD':
                st.spread_confirmed_since, st.spread_wait_seconds = now, None
    if old_active == route and st.last_status_note != 'envoy_update_failed':
        return None
    st.last_status_note = 'envoy_acked'
    extra = {'envoy_ack_version': version}
    if 'SPREAD' in (route, old_active):
        extra.update(policy._spread_extra(st, st.last_local_ready, st.last_core_ready))
    return st.make_event(
        timestamp=now, event='envoy_ack', old_route=old_active, new_route=route,
        reason='envoy_rds_ack', extra=extra,
    )


def record_eds_ack(policy, site: str, level: int, version: str, now: float, *,
                   monotonic_now: float | None = None) -> DecisionEvent | None:
    """Record Envoy EDS ACK for a SPREAD weight change and start its post-ACK window."""
    st = policy.states[site]
    if st.desired_route != 'SPREAD' or st.pending_weight_since is None or st.pending_weight_level != int(level):
        return None
    st.pending_weight_since = st.pending_weight_level = st.envoy_last_nack = None
    st.envoy_update_failed = False
    if monotonic_now is not None:
        policy.last_route_switch_at = monotonic_now
    st.spread_confirmed_since = now
    st.spread_wait_seconds = policy.config.route_switch_interval_seconds if st.pending_weight_fast else None
    st.pending_weight_fast = False
    policy._clear_breach(st)
    policy._clear_recovery(st)
    st.last_status_note = 'envoy_eds_acked'
    extra = {'envoy_ack_version': version, **policy._spread_extra(st, st.last_local_ready, st.last_core_ready)}
    return st.make_event(
        timestamp=now, event='envoy_ack', old_route='SPREAD', new_route='SPREAD',
        reason='envoy_eds_ack', extra=extra,
    )


def record_nack(policy, site: str, error_message: str, now: float, *,
                reason: str = 'envoy_rds_nack') -> DecisionEvent:
    """Record an Envoy RDS or EDS NACK failure for a site."""
    st = policy.states[site]
    st.envoy_update_failed = True
    st.envoy_last_nack = error_message
    st.last_status_note = 'envoy_update_failed'
    return st.make_event(
        timestamp=now, event='envoy_update_failed', old_route=st.active_route,
        new_route=st.desired_route, reason=reason, extra={'envoy_nack_error': error_message},
    )


def check_ack_timeouts(policy, events: list[DecisionEvent], st: SiteState, now: float, k_current: int):
    """Emit failure events when an outstanding Envoy RDS or EDS ACK exceeds timeout."""
    timeout = policy.config.envoy_ack_timeout_seconds
    if (st.desired_route != st.active_route and st.pending_update_since is not None
            and not st.envoy_update_failed and (now - st.pending_update_since) >= timeout):
        st.envoy_update_failed = True
        st.last_status_note = 'envoy_ack_timeout'
        events.append(st.make_event(
            timestamp=now, event='envoy_update_failed', old_route=st.active_route,
            new_route=st.desired_route, reason='envoy_ack_timeout',
        ))
    if (st.desired_route == 'SPREAD' and st.pending_weight_since is not None
            and not st.envoy_update_failed and (now - st.pending_weight_since) >= timeout):
        st.envoy_update_failed = True
        st.last_status_note = 'envoy_eds_ack_timeout'
        events.append(st.make_event(
            timestamp=now, event='envoy_update_failed', old_route='SPREAD', new_route='SPREAD',
            reason='envoy_eds_ack_timeout',
            extra=policy._spread_extra(st, st.last_local_ready, st.last_core_ready, k_current),
        ))
