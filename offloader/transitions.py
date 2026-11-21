"""Serialized execution of route and retreat-weight transitions for VerticalOffloadPolicy."""
from offloader.state import DecisionEvent, SiteState


def apply_local_only(policy, st: SiteState, now: float, events: list[DecisionEvent]):
    """Lock a site to LOCAL while the offloader is in local-only mode."""
    if st.desired_route != 'LOCAL':
        old = st.desired_route
        st.desired_route = 'LOCAL'
        st.last_transition_time = now
        st.pending_update_since = now
        st.envoy_update_failed = False
        policy._reset_retreat_state(st)
        st.last_decision_reason = 'local_only_locked'
        events.append(st.make_event(
            timestamp=now, event='route_decision', old_route=old,
            new_route='LOCAL', desired_route='LOCAL', reason='local_only_locked',
        ))
    policy._clear_breach(st)
    policy._clear_recovery(st)
    st.last_status_note = 'local_only_locked'


def apply_core_failback(policy, st: SiteState, now: float, snapshot: dict | None,
                        wall_now, k_current: int, events: list[DecisionEvent]):
    """Fail back a SPREAD site to LOCAL if Core has 0 ready endpoints and Local is ready."""
    policy._clear_breach(st)
    policy._clear_recovery(st)
    if st.last_local_ready <= 0:
        policy._note_hold(events, st, st.site, 'both_backends_unready',
                          local_ready=st.last_local_ready, core_ready=st.last_core_ready)
        return
    if not policy._try_start_route_switch(st, 'LOCAL', now, snapshot=snapshot, require_prev_post_ack=False):
        st.last_status_note = 'route_switch_waiting'
        return
    extra = policy._spread_extra(st, st.last_local_ready, st.last_core_ready, k_current)
    policy._reset_other_spread_post_ack(st.site, wall_now)
    policy._reset_retreat_state(st)
    st.spread_confirmed_since = None
    st.last_decision_reason = 'core_backend_unready_failback'
    st.last_status_note = 'core_backend_unready_failback'
    events.append(st.make_event(
        timestamp=now, event='route_decision', old_route='SPREAD',
        new_route='LOCAL', desired_route='LOCAL', reason='core_backend_unready_failback', extra=extra,
    ))


def _mark_waiting(candidates: list[dict], chosen: dict | None):
    for cand in candidates:
        if cand is not chosen:
            cand['st'].last_status_note = 'route_switch_waiting'


def _apply_enter(policy, chosen: dict, now: float, snapshot, wall_now, events: list[DecisionEvent]) -> bool:
    st = chosen['st']
    if not policy._try_start_route_switch(st, 'SPREAD', now, snapshot=snapshot):
        return False
    policy._reset_retreat_state(st)
    policy._reset_other_spread_post_ack(st.site, wall_now)
    st.local_confirmed_since = None
    st.spread_confirmed_since = None
    st.last_decision_reason = chosen['reason']
    st.last_status_note = chosen['reason']
    policy._clear_breach(st)
    extra = policy._spread_extra(st, chosen['local_ready'], chosen['core_ready'], policy.spread_count(), level=0)
    if chosen['unlabeled_timeouts']:
        extra['unlabeled_timeouts'] = chosen['unlabeled_timeouts']
    events.append(st.make_event(
        timestamp=now, event='route_decision', old_route='LOCAL', new_route='SPREAD',
        desired_route='SPREAD', reason=chosen['reason'], p90_ms=chosen['p90_ms'],
        samples=chosen['samples'], attempts=chosen['attempts'], error_rate=chosen['error_rate'],
        local_ready=chosen['local_ready'], core_ready=chosen['core_ready'], extra=extra,
    ))
    return True


def _apply_restore(policy, chosen: dict, now: float, snapshot, k_current: int, events: list[DecisionEvent]) -> bool:
    st = chosen['st']
    target_level = chosen['target_level']
    if not policy._try_start_weight_change(st, target_level, now, snapshot=snapshot):
        return False
    st.good_level = min(st.good_level, target_level)
    st.hold_until = now + policy.config.min_dwell_seconds
    st.last_decision_reason = chosen['reason']
    st.last_status_note = 'spread_retreat_restore'
    policy._clear_breach(st)
    policy._clear_recovery(st)
    extra = policy._spread_extra(st, chosen['local_ready'], chosen['core_ready'], k_current, level=target_level)
    events.append(st.make_event(
        timestamp=now, event='spread_retreat_restore', old_route='SPREAD', new_route='SPREAD',
        desired_route='SPREAD', reason=chosen['reason'], p90_ms=chosen['p90_ms'],
        samples=chosen['samples'], attempts=chosen['attempts'], error_rate=chosen['error_rate'],
        local_ready=chosen['local_ready'], core_ready=chosen['core_ready'], extra=extra,
    ))
    return True


def _apply_retreat(policy, chosen: dict, now: float, snapshot, wall_now,
                   k_current: int, events: list[DecisionEvent]) -> bool:
    st = chosen['st']
    next_level = chosen['next_level']
    if chosen['action'] == 'complete':
        if not policy._try_start_route_switch(st, 'LOCAL', now, snapshot=snapshot):
            return False
        extra = policy._spread_extra(st, chosen['local_ready'], chosen['core_ready'], k_current, level=next_level)
        policy._reset_other_spread_post_ack(st.site, wall_now)
        policy._reset_retreat_state(st)
        st.spread_confirmed_since = None
        st.last_decision_reason = 'spread_retreat_complete'
        st.last_status_note = 'spread_retreat_complete'
        policy._clear_breach(st)
        policy._clear_recovery(st)
        events.append(st.make_event(
            timestamp=now, event='route_decision', old_route='SPREAD', new_route='LOCAL',
            desired_route='LOCAL', reason='spread_retreat_complete', p90_ms=chosen['p90_ms'],
            samples=chosen['samples'], attempts=chosen['attempts'], error_rate=chosen['error_rate'],
            local_ready=chosen['local_ready'], core_ready=chosen['core_ready'], extra=extra,
        ))
        return True

    if not policy._try_start_weight_change(st, next_level, now, snapshot=snapshot):
        return False
    st.last_decision_reason = 'spread_retreat_step'
    st.last_status_note = 'spread_retreat_step'
    policy._clear_breach(st)
    policy._clear_recovery(st)
    extra = policy._spread_extra(st, chosen['local_ready'], chosen['core_ready'], k_current, level=next_level)
    events.append(st.make_event(
        timestamp=now, event='spread_retreat_step', old_route='SPREAD', new_route='SPREAD',
        desired_route='SPREAD', reason='spread_retreat_step', p90_ms=chosen['p90_ms'],
        samples=chosen['samples'], attempts=chosen['attempts'], error_rate=chosen['error_rate'],
        local_ready=chosen['local_ready'], core_ready=chosen['core_ready'], extra=extra,
    ))
    return True


def execute_transitions(policy, events: list[DecisionEvent], now: float, snapshot: dict | None,
                        wall_now, k_current: int, enter_cands: list[dict],
                        restore_cands: list[dict], retreat_cands: list[dict]):
    """Execute at most one transition per global switch interval (priority: enter > restore > retreat)."""
    switched = False
    if enter_cands:
        enter_cands.sort(key=lambda c: c['site'])
        switched = _apply_enter(policy, enter_cands[0], now, snapshot, wall_now, events)
        _mark_waiting(enter_cands, enter_cands[0] if switched else None)

    if not switched and restore_cands:
        restore_cands.sort(key=lambda c: (-c['core_ratio'], c['site']))
        switched = _apply_restore(policy, restore_cands[0], now, snapshot, k_current, events)
        _mark_waiting(restore_cands, restore_cands[0] if switched else None)
    elif switched:
        _mark_waiting(restore_cands, None)

    if not switched and retreat_cands:
        retreat_cands.sort(key=lambda c: (-c['core_ratio'], c['site']))
        switched = _apply_retreat(policy, retreat_cands[0], now, snapshot, wall_now, k_current, events)
        _mark_waiting(retreat_cands, retreat_cands[0] if switched else None)
    elif switched:
        _mark_waiting(retreat_cands, None)
