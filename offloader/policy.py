"""Per-site vertical offloading state machine (LOCAL / weighted SPREAD with gradual retreat).

Coordinates per-site state transitions, Envoy RDS/EDS ACK/NACK tracking, global
switch serialization, and delegates LOCAL-entry and SPREAD-retreat evaluation to
offloader.entry and offloader.retreat.
"""
from dataclasses import replace
import math

from offloader.entry import evaluate_local_entry
from offloader.retreat import evaluate_spread_retreat
from offloader.spread import (
    _bucket_stale,
    _overall_user_bucket,
    _post_ack_window,
    spread_core_ratio as _spread_core_ratio,
    spread_local_weight as _spread_local_weight,
)
from offloader.state import (
    DEFAULT_SITE_NODES,
    MODES,
    ROUTES,
    SITES,
    DecisionEvent,
    OffloadConfig,
    RouteSwitchBusy,
    SiteState,
)

# Gradual SPREAD retreat: halve the Core traffic share at each step to prevent full-jump oscillation.
SPREAD_RETREAT_FACTOR = 2
# Below 1% remaining Core share, Core contribution is negligible so the site returns to full LOCAL.
SPREAD_RETREAT_FLOOR = 0.01


def spread_core_ratio(core_ready: int, local_ready: int, k: int, retreat_level: int = 0) -> float:
    """Return target Core traffic share s_n = s_0 / 2^n where s_0 = c / (c + max(1, k) * x_i)."""
    return _spread_core_ratio(core_ready, local_ready, k, retreat_level, retreat_factor=SPREAD_RETREAT_FACTOR)


def spread_local_weight(core_ready: int, local_ready: int, k: int, retreat_level: int = 0) -> int:
    """Return integer EDS load_balancing_weight (>= 1) per local pod for target Core share s_n."""
    return _spread_local_weight(core_ready, local_ready, k, retreat_level, retreat_factor=SPREAD_RETREAT_FACTOR)


class VerticalOffloadPolicy:
    """Per-site LOCAL/SPREAD offloading with gradual retreat and global change serialization."""

    def __init__(self, config: OffloadConfig | None = None):
        self.config = config or OffloadConfig()
        self.states = {site: SiteState(site=site) for site in SITES}
        self.last_route_switch_at: float | None = None
        self.last_switched_site: str | None = None
        self._now: float = 0.0

    def spread_count(self) -> int:
        return sum(1 for st in self.states.values() if st.desired_route == 'SPREAD')

    def _spread_extra(
        self,
        st: SiteState,
        local_ready: int,
        core_ready: int,
        k: int | None = None,
        *,
        level: int | None = None,
    ) -> dict:
        k_val = self.spread_count() if k is None else int(k)
        n_val = st.retreat_level if level is None else int(level)
        ratio = spread_core_ratio(core_ready, local_ready, k_val, n_val)
        w_l = spread_local_weight(core_ready, local_ready, k_val, n_val)
        return {
            'c': core_ready,
            'x_i': local_ready,
            'k': k_val,
            'n': n_val,
            'good_level': st.good_level,
            'core_ratio': round(ratio, 6),
            'local_weight': w_l,
            'hold_until': st.hold_until,
        }

    def _reset_other_spread_post_ack(self, changed_site: str, wall_now: float | None):
        """Restart the post-ACK observation window for other SPREAD sites when k changes."""
        for other in self.states.values():
            if other.site != changed_site and other.desired_route == 'SPREAD':
                other.spread_confirmed_since = wall_now
                other.spread_wait_seconds = None
                self._clear_breach(other)
                self._clear_recovery(other)

    def _reset_retreat_state(self, st: SiteState):
        st.retreat_level = 0
        st.good_level = 0
        st.hold_until = None
        st.pending_weight_since = None
        st.pending_weight_level = None

    def set_mode(self, mode: str):
        self.config = replace(self.config, mode=mode)
        if mode == 'local-only':
            for st in self.states.values():
                st.manual_override = None
                self._clear_breach(st)
                self._clear_recovery(st)
                self._reset_retreat_state(st)
                st.desired_route = 'LOCAL'
                st.pending_update_since = None
                st.last_decision_reason = 'mode_local_only'

    def reset(self, *, active_route: str = 'LOCAL'):
        self.last_route_switch_at = None
        self.last_switched_site = None
        for st in self.states.values():
            st.reset(active_route=active_route)

    def _has_pending_ack(self, now: float) -> bool:
        timeout = self.config.envoy_ack_timeout_seconds
        for other in self.states.values():
            if other.envoy_update_failed:
                continue
            if other.pending_update_since is not None and now - other.pending_update_since < timeout:
                return True
            if other.pending_weight_since is not None and now - other.pending_weight_since < timeout:
                return True
        return False

    def _can_start_route_switch(
        self,
        st: SiteState,
        now: float,
        *,
        snapshot: dict | None = None,
        require_prev_post_ack: bool = True,
    ) -> bool:
        last_switch = self.last_route_switch_at
        if last_switch is not None and now - last_switch < self.config.route_switch_interval_seconds:
            return False
        if self._has_pending_ack(now):
            return False
        if require_prev_post_ack and snapshot is not None and self.last_switched_site is not None:
            prev = self.states[self.last_switched_site]
            if prev is not st and not prev.envoy_update_failed:
                if prev.active_route != prev.desired_route or prev.pending_weight_since is not None:
                    return False
                confirmed_since = (
                    prev.spread_confirmed_since if prev.active_route == 'SPREAD' else prev.local_confirmed_since
                )
                if _post_ack_window(snapshot, confirmed_since) is None:
                    return False
        return True

    def _try_start_route_switch(
        self,
        st: SiteState,
        route: str,
        now: float,
        *,
        snapshot: dict | None = None,
        require_prev_post_ack: bool = True,
    ) -> bool:
        if not self._can_start_route_switch(
            st, now, snapshot=snapshot, require_prev_post_ack=require_prev_post_ack
        ):
            return False
        self.last_route_switch_at = now
        self.last_switched_site = st.site
        st.desired_route = route
        st.last_transition_time = now
        st.pending_update_since = now
        st.pending_weight_since = None
        st.pending_weight_level = None
        st.envoy_update_failed = False
        return True

    def _try_start_weight_change(
        self,
        st: SiteState,
        new_level: int,
        now: float,
        *,
        snapshot: dict | None = None,
    ) -> bool:
        if not self._can_start_route_switch(st, now, snapshot=snapshot, require_prev_post_ack=True):
            return False
        self.last_route_switch_at = now
        self.last_switched_site = st.site
        st.pending_weight_fast = int(new_level) > st.retreat_level
        st.spread_wait_seconds = None
        st.retreat_level = int(new_level)
        # last_transition_time stays the time of the last route change: the dwell applies to route changes,
        # while retreat steps are spaced by the global switch interval and post-ACK evaluation.
        st.pending_weight_since = now
        st.pending_weight_level = int(new_level)
        st.spread_confirmed_since = None
        st.envoy_update_failed = False
        return True

    def set_manual_override(self, site: str, route: str | None, now: float) -> DecisionEvent | None:
        if site not in self.states:
            raise ValueError(f'unknown site: {site}')
        st = self.states[site]
        if route is None:
            st.manual_override = None
            st.last_status_note = 'manual_override_cleared'
            return None
        route = route.upper()
        if route not in ROUTES:
            raise ValueError(f'route must be one of {ROUTES}')
        if self.config.mode == 'local-only' and route == 'SPREAD':
            raise PermissionError('SPREAD override is forbidden while mode is local-only')
        old = st.desired_route
        k_before = self.spread_count()
        if route != old and not self._try_start_route_switch(
            st, route, now, require_prev_post_ack=False
        ):
            raise RouteSwitchBusy('Another route change is pending or inside the global switch interval')
        st.manual_override = route
        self._clear_breach(st)
        self._clear_recovery(st)
        # Manual override to SPREAD always starts at n=0 and does not use gradual retreat.
        self._reset_retreat_state(st)
        if route != old:
            st.local_confirmed_since = None
            st.spread_confirmed_since = None
            self._reset_other_spread_post_ack(site, None)
            st.last_decision_reason = f'manual_override_{route.lower()}'
            k_after = self.spread_count()
            extra = (
                self._spread_extra(st, st.last_local_ready, st.last_core_ready, k_after if route == 'SPREAD' else k_before)
                if ('SPREAD' in (old, route))
                else {}
            )
            return DecisionEvent(
                timestamp=now,
                event='route_decision',
                site=site,
                old_route=old,
                new_route=route,
                desired_route=st.desired_route,
                active_route=st.active_route,
                reason=st.last_decision_reason,
                p90_ms=st.last_p90_ms,
                samples=st.last_samples,
                attempts=st.last_attempts,
                error_rate=st.last_error_rate,
                local_ready_endpoints=st.last_local_ready,
                core_ready_endpoints=st.last_core_ready,
                extra=extra,
            )
        return None

    def record_envoy_ack(self, site: str, route: str, version: str, now: float, *,
                         monotonic_now: float | None = None) -> DecisionEvent | None:
        st = self.states[site]
        route = route.upper()
        old_active = st.active_route
        st.active_route = route
        st.envoy_update_failed = False
        st.envoy_last_nack = None
        if st.active_route == st.desired_route:
            if st.pending_update_since is not None and monotonic_now is not None:
                self.last_route_switch_at = monotonic_now
            st.pending_update_since = None
            if route == 'LOCAL':
                if st.local_confirmed_since is None or old_active != 'LOCAL':
                    st.local_confirmed_since = now
                st.spread_confirmed_since = None
            else:
                st.local_confirmed_since = None
                if st.spread_confirmed_since is None or old_active != 'SPREAD':
                    st.spread_confirmed_since = now
                    st.spread_wait_seconds = None
        if old_active != route or st.last_status_note == 'envoy_update_failed':
            st.last_status_note = 'envoy_acked'
            extra = {'envoy_ack_version': version}
            if route == 'SPREAD' or old_active == 'SPREAD':
                extra.update(self._spread_extra(st, st.last_local_ready, st.last_core_ready))
            return DecisionEvent(
                timestamp=now,
                event='envoy_ack',
                site=site,
                old_route=old_active,
                new_route=route,
                desired_route=st.desired_route,
                active_route=st.active_route,
                reason='envoy_rds_ack',
                p90_ms=st.last_p90_ms,
                samples=st.last_samples,
                attempts=st.last_attempts,
                error_rate=st.last_error_rate,
                local_ready_endpoints=st.last_local_ready,
                core_ready_endpoints=st.last_core_ready,
                extra=extra,
            )
        return None

    def record_envoy_eds_ack(self, site: str, level: int, version: str, now: float, *,
                             monotonic_now: float | None = None) -> DecisionEvent | None:
        """Record Envoy EDS ACK for a spread-gnb-N weight change and start its post-ACK window."""
        st = self.states[site]
        if st.desired_route != 'SPREAD':
            return None
        if st.pending_weight_since is None or st.pending_weight_level != int(level):
            return None
        st.pending_weight_since = None
        st.pending_weight_level = None
        st.envoy_update_failed = False
        st.envoy_last_nack = None
        if monotonic_now is not None:
            self.last_route_switch_at = monotonic_now
        st.spread_confirmed_since = now
        # A retreat step is judged after the global switch interval; a restore waits a full window so the
        # slow samples that triggered it have left the window.
        st.spread_wait_seconds = self.config.route_switch_interval_seconds if st.pending_weight_fast else None
        st.pending_weight_fast = False
        self._clear_breach(st)
        self._clear_recovery(st)
        st.last_status_note = 'envoy_eds_acked'
        extra = {'envoy_ack_version': version}
        extra.update(self._spread_extra(st, st.last_local_ready, st.last_core_ready))
        return DecisionEvent(
            timestamp=now,
            event='envoy_ack',
            site=site,
            old_route='SPREAD',
            new_route='SPREAD',
            desired_route=st.desired_route,
            active_route=st.active_route,
            reason='envoy_eds_ack',
            p90_ms=st.last_p90_ms,
            samples=st.last_samples,
            attempts=st.last_attempts,
            error_rate=st.last_error_rate,
            local_ready_endpoints=st.last_local_ready,
            core_ready_endpoints=st.last_core_ready,
            extra=extra,
        )

    def record_envoy_nack(self, site: str, error_message: str, now: float, *,
                          reason: str = 'envoy_rds_nack') -> DecisionEvent:
        st = self.states[site]
        st.envoy_update_failed = True
        st.envoy_last_nack = error_message
        st.last_status_note = 'envoy_update_failed'
        return DecisionEvent(
            timestamp=now,
            event='envoy_update_failed',
            site=site,
            old_route=st.active_route,
            new_route=st.desired_route,
            desired_route=st.desired_route,
            active_route=st.active_route,
            reason=reason,
            p90_ms=st.last_p90_ms,
            samples=st.last_samples,
            attempts=st.last_attempts,
            error_rate=st.last_error_rate,
            local_ready_endpoints=st.last_local_ready,
            core_ready_endpoints=st.last_core_ready,
            extra={'envoy_nack_error': error_message},
        )

    def _clear_breach(self, st: SiteState):
        st.breach_started_at = None
        st.breach_fresh_updates = 0

    def _note_hold(self, events, st: SiteState, site: str, note: str, *, local_ready: int, core_ready: int,
                   p90_ms, samples: int, attempts: int, error_rate, extra: dict | None = None):
        """Record a SPREAD-state hold reason once per change so it is visible in the event log."""
        if st.last_status_note == note:
            return
        st.last_status_note = note
        merged = self._spread_extra(st, local_ready, core_ready)
        if extra:
            merged.update(extra)
        events.append(
            DecisionEvent(
                timestamp=self._now,
                event='hold_route',
                site=site,
                old_route='SPREAD',
                new_route='SPREAD',
                desired_route='SPREAD',
                active_route=st.active_route,
                reason=note,
                p90_ms=p90_ms,
                samples=samples,
                attempts=attempts,
                error_rate=error_rate,
                local_ready_endpoints=local_ready,
                core_ready_endpoints=core_ready,
                extra=merged,
            )
        )

    def _clear_recovery(self, st: SiteState):
        st.recovery_started_at = None
        st.recovery_fresh_updates = 0

    def _advance_breach(self, st, now, seq):
        if st.breach_started_at is None:
            st.breach_started_at = now
            st.breach_fresh_updates = 1
        elif seq > st.breach_last_seq:
            st.breach_fresh_updates += 1
        else:
            return False
        st.breach_last_seq = seq
        return True

    def _dwell_satisfied(self, st: SiteState, now: float) -> bool:
        if st.last_transition_time is None:
            return True
        return (now - st.last_transition_time) >= self.config.min_dwell_seconds

    def _check_ack_timeouts(self, events, st: SiteState, site: str, now: float,
                            local_ready: int, core_ready: int, k_current: int):
        """Emit failure events when an outstanding Envoy RDS or EDS ACK exceeds timeout."""
        cfg = self.config
        if (
            st.desired_route != st.active_route
            and st.pending_update_since is not None
            and not st.envoy_update_failed
            and (now - st.pending_update_since) >= cfg.envoy_ack_timeout_seconds
        ):
            st.envoy_update_failed = True
            st.last_status_note = 'envoy_ack_timeout'
            events.append(
                DecisionEvent(
                    timestamp=now,
                    event='envoy_update_failed',
                    site=site,
                    old_route=st.active_route,
                    new_route=st.desired_route,
                    desired_route=st.desired_route,
                    active_route=st.active_route,
                    reason='envoy_ack_timeout',
                    p90_ms=st.last_p90_ms,
                    samples=st.last_samples,
                    attempts=st.last_attempts,
                    error_rate=st.last_error_rate,
                    local_ready_endpoints=local_ready,
                    core_ready_endpoints=core_ready,
                )
            )
        if (
            st.desired_route == 'SPREAD'
            and st.pending_weight_since is not None
            and not st.envoy_update_failed
            and (now - st.pending_weight_since) >= cfg.envoy_ack_timeout_seconds
        ):
            st.envoy_update_failed = True
            st.last_status_note = 'envoy_eds_ack_timeout'
            events.append(
                DecisionEvent(
                    timestamp=now,
                    event='envoy_update_failed',
                    site=site,
                    old_route='SPREAD',
                    new_route='SPREAD',
                    desired_route=st.desired_route,
                    active_route=st.active_route,
                    reason='envoy_eds_ack_timeout',
                    p90_ms=st.last_p90_ms,
                    samples=st.last_samples,
                    attempts=st.last_attempts,
                    error_rate=st.last_error_rate,
                    local_ready_endpoints=local_ready,
                    core_ready_endpoints=core_ready,
                    extra=self._spread_extra(st, local_ready, core_ready, k_current),
                )
            )

    def _execute_transitions(self, events, now: float, snapshot: dict | None, wall_now,
                             k_current: int, enter_candidates: list[dict],
                             restore_candidates: list[dict], retreat_candidates: list[dict]):
        """Execute at most one route or weight change per global switch interval.

        Priority: 1) entering SPREAD (SLO protection), 2) restoring a breaching
        retreat step, 3) advancing gradual retreat toward LOCAL.
        """
        cfg = self.config
        switched = False
        if enter_candidates:
            enter_candidates.sort(key=lambda c: c['site'])
            chosen = enter_candidates[0]
            st = chosen['st']
            site = chosen['site']
            if self._try_start_route_switch(st, 'SPREAD', now, snapshot=snapshot):
                switched = True
                self._reset_retreat_state(st)
                self._reset_other_spread_post_ack(site, wall_now)
                k_after = self.spread_count()
                st.local_confirmed_since = None
                st.spread_confirmed_since = None
                st.last_decision_reason = chosen['reason']
                st.last_status_note = chosen['reason']
                self._clear_breach(st)
                extra = self._spread_extra(st, chosen['local_ready'], chosen['core_ready'], k_after, level=0)
                if chosen['unlabeled_timeouts']:
                    extra['unlabeled_timeouts'] = chosen['unlabeled_timeouts']
                events.append(
                    DecisionEvent(
                        timestamp=now,
                        event='route_decision',
                        site=site,
                        old_route='LOCAL',
                        new_route='SPREAD',
                        desired_route='SPREAD',
                        active_route=st.active_route,
                        reason=chosen['reason'],
                        p90_ms=chosen['p90_ms'],
                        samples=chosen['samples'],
                        attempts=chosen['attempts'],
                        error_rate=chosen['error_rate'],
                        local_ready_endpoints=chosen['local_ready'],
                        core_ready_endpoints=chosen['core_ready'],
                        extra=extra,
                    )
                )
            for cand in enter_candidates:
                if not switched or cand is not chosen:
                    cand['st'].last_status_note = 'route_switch_waiting'

        if not switched and restore_candidates:
            restore_candidates.sort(key=lambda c: (-c['core_ratio'], c['site']))
            chosen = restore_candidates[0]
            st = chosen['st']
            site = chosen['site']
            target_level = chosen['target_level']
            if self._try_start_weight_change(st, target_level, now, snapshot=snapshot):
                switched = True
                st.good_level = min(st.good_level, target_level)
                st.hold_until = now + cfg.min_dwell_seconds
                st.last_decision_reason = chosen['reason']
                st.last_status_note = 'spread_retreat_restore'
                self._clear_breach(st)
                self._clear_recovery(st)
                events.append(
                    DecisionEvent(
                        timestamp=now,
                        event='spread_retreat_restore',
                        site=site,
                        old_route='SPREAD',
                        new_route='SPREAD',
                        desired_route='SPREAD',
                        active_route=st.active_route,
                        reason=chosen['reason'],
                        p90_ms=chosen['p90_ms'],
                        samples=chosen['samples'],
                        attempts=chosen['attempts'],
                        error_rate=chosen['error_rate'],
                        local_ready_endpoints=chosen['local_ready'],
                        core_ready_endpoints=chosen['core_ready'],
                        extra=self._spread_extra(st, chosen['local_ready'], chosen['core_ready'], k_current, level=target_level),
                    )
                )
            for cand in restore_candidates:
                if not switched or cand is not chosen:
                    cand['st'].last_status_note = 'route_switch_waiting'
        elif switched:
            for cand in restore_candidates:
                cand['st'].last_status_note = 'route_switch_waiting'

        if not switched and retreat_candidates:
            # Largest Core ratio first; tie-break by site name.
            retreat_candidates.sort(key=lambda c: (-c['core_ratio'], c['site']))
            chosen = retreat_candidates[0]
            st = chosen['st']
            site = chosen['site']
            if chosen['action'] == 'complete':
                if self._try_start_route_switch(st, 'LOCAL', now, snapshot=snapshot):
                    switched = True
                    extra = self._spread_extra(st, chosen['local_ready'], chosen['core_ready'], k_current, level=chosen['next_level'])
                    self._reset_other_spread_post_ack(site, wall_now)
                    self._reset_retreat_state(st)
                    st.spread_confirmed_since = None
                    st.last_decision_reason = 'spread_retreat_complete'
                    st.last_status_note = 'spread_retreat_complete'
                    self._clear_breach(st)
                    self._clear_recovery(st)
                    events.append(
                        DecisionEvent(
                            timestamp=now,
                            event='route_decision',
                            site=site,
                            old_route='SPREAD',
                            new_route='LOCAL',
                            desired_route='LOCAL',
                            active_route=st.active_route,
                            reason='spread_retreat_complete',
                            p90_ms=chosen['p90_ms'],
                            samples=chosen['samples'],
                            attempts=chosen['attempts'],
                            error_rate=chosen['error_rate'],
                            local_ready_endpoints=chosen['local_ready'],
                            core_ready_endpoints=chosen['core_ready'],
                            extra=extra,
                        )
                    )
            else:
                next_level = chosen['next_level']
                if self._try_start_weight_change(st, next_level, now, snapshot=snapshot):
                    switched = True
                    st.last_decision_reason = 'spread_retreat_step'
                    st.last_status_note = 'spread_retreat_step'
                    self._clear_breach(st)
                    self._clear_recovery(st)
                    events.append(
                        DecisionEvent(
                            timestamp=now,
                            event='spread_retreat_step',
                            site=site,
                            old_route='SPREAD',
                            new_route='SPREAD',
                            desired_route='SPREAD',
                            active_route=st.active_route,
                            reason='spread_retreat_step',
                            p90_ms=chosen['p90_ms'],
                            samples=chosen['samples'],
                            attempts=chosen['attempts'],
                            error_rate=chosen['error_rate'],
                            local_ready_endpoints=chosen['local_ready'],
                            core_ready_endpoints=chosen['core_ready'],
                            extra=self._spread_extra(st, chosen['local_ready'], chosen['core_ready'], k_current, level=next_level),
                        )
                    )
            for cand in retreat_candidates:
                if not switched or cand is not chosen:
                    cand['st'].last_status_note = 'route_switch_waiting'
        elif switched:
            for cand in retreat_candidates:
                cand['st'].last_status_note = 'route_switch_waiting'

    def evaluate(
        self,
        now: float,
        snapshot: dict | None,
        local_ready_counts: dict[str, int],
        core_ready_count: int,
        *,
        collector_error: str | None = None,
    ) -> list[DecisionEvent]:
        """Evaluate all sites from one snapshot and serialize runtime route/weight changes."""
        events: list[DecisionEvent] = []
        cfg = self.config
        self._now = now
        wall_now = snapshot.get('generated_at') if isinstance(snapshot, dict) else None
        k_current = self.spread_count()

        enter_candidates: list[dict] = []
        restore_candidates: list[dict] = []
        retreat_candidates: list[dict] = []

        for site in SITES:
            st = self.states[site]
            local_ready = max(0, int(local_ready_counts.get(site, 0)))
            core_ready = max(0, int(core_ready_count))
            st.last_local_ready = local_ready
            st.last_core_ready = core_ready
            st.last_spread_k = k_current
            if st.desired_route == 'SPREAD':
                st.last_core_ratio = spread_core_ratio(core_ready, local_ready, k_current, st.retreat_level)
                st.last_local_weight = spread_local_weight(core_ready, local_ready, k_current, st.retreat_level)
            else:
                st.last_core_ratio = 0.0
                st.last_local_weight = 1

            # Anchor restarted post-ACK window if k changed outside evaluate() (e.g. manual override).
            if (
                st.desired_route == 'SPREAD'
                and st.active_route == 'SPREAD'
                and st.pending_weight_since is None
                and st.spread_confirmed_since is None
                and isinstance(wall_now, (int, float))
                and math.isfinite(wall_now)
            ):
                st.spread_confirmed_since = wall_now
                st.spread_wait_seconds = None

            self._check_ack_timeouts(events, st, site, now, local_ready, core_ready, k_current)

            # Manual override bypasses automatic state transitions (and never uses gradual retreat).
            if st.manual_override is not None:
                st.desired_route = st.manual_override
                continue

            # Local-only mode locks every site to LOCAL regardless of SLO breach.
            if cfg.mode == 'local-only':
                if st.desired_route != 'LOCAL':
                    old = st.desired_route
                    st.desired_route = 'LOCAL'
                    st.last_transition_time = now
                    st.pending_update_since = now
                    st.envoy_update_failed = False
                    self._reset_retreat_state(st)
                    st.last_decision_reason = 'local_only_locked'
                    events.append(
                        DecisionEvent(
                            timestamp=now,
                            event='route_decision',
                            site=site,
                            old_route=old,
                            new_route='LOCAL',
                            desired_route='LOCAL',
                            active_route=st.active_route,
                            reason='local_only_locked',
                            p90_ms=st.last_p90_ms,
                            samples=st.last_samples,
                            attempts=st.last_attempts,
                            error_rate=st.last_error_rate,
                            local_ready_endpoints=local_ready,
                            core_ready_endpoints=core_ready,
                        )
                    )
                self._clear_breach(st)
                self._clear_recovery(st)
                st.last_status_note = 'local_only_locked'
                continue

            # Safety failback: if SPREAD is active/desired and Core loses all ready endpoints.
            if st.desired_route == 'SPREAD' and core_ready == 0:
                self._clear_breach(st)
                self._clear_recovery(st)
                if local_ready > 0:
                    if not self._try_start_route_switch(
                        st, 'LOCAL', now, snapshot=snapshot, require_prev_post_ack=False
                    ):
                        st.last_status_note = 'route_switch_waiting'
                        continue
                    extra = self._spread_extra(st, local_ready, core_ready, k_current)
                    self._reset_other_spread_post_ack(site, wall_now)
                    self._reset_retreat_state(st)
                    st.spread_confirmed_since = None
                    st.last_decision_reason = 'core_backend_unready_failback'
                    st.last_status_note = 'core_backend_unready_failback'
                    events.append(
                        DecisionEvent(
                            timestamp=now,
                            event='route_decision',
                            site=site,
                            old_route='SPREAD',
                            new_route='LOCAL',
                            desired_route='LOCAL',
                            active_route=st.active_route,
                            reason='core_backend_unready_failback',
                            p90_ms=st.last_p90_ms,
                            samples=st.last_samples,
                            attempts=st.last_attempts,
                            error_rate=st.last_error_rate,
                            local_ready_endpoints=local_ready,
                            core_ready_endpoints=core_ready,
                            extra=extra,
                        )
                    )
                else:
                    self._note_hold(
                        events, st, site, 'both_backends_unready',
                        local_ready=local_ready, core_ready=core_ready,
                        p90_ms=st.last_p90_ms, samples=st.last_samples,
                        attempts=st.last_attempts, error_rate=st.last_error_rate,
                    )
                continue

            # Collector fetch failure: hold last applied route and log status transition.
            if collector_error is not None or snapshot is None:
                self._clear_breach(st)
                self._clear_recovery(st)
                if st.last_status_note != 'collector_error':
                    st.last_status_note = 'collector_error'
                    extra = {'collector_error': collector_error or 'missing_snapshot'}
                    if st.desired_route == 'SPREAD':
                        extra.update(self._spread_extra(st, local_ready, core_ready, k_current))
                    events.append(
                        DecisionEvent(
                            timestamp=now,
                            event='hold_route',
                            site=site,
                            old_route=st.desired_route,
                            new_route=st.desired_route,
                            desired_route=st.desired_route,
                            active_route=st.active_route,
                            reason='collector_error',
                            p90_ms=None,
                            samples=0,
                            attempts=0,
                            error_rate=None,
                            local_ready_endpoints=local_ready,
                            core_ready_endpoints=core_ready,
                            extra=extra,
                        )
                    )
                continue

            site_snap = (snapshot.get('sites') or {}).get(site) or {}
            user_buckets = site_snap.get('user') or {}

            if st.desired_route == 'LOCAL':
                cand = evaluate_local_entry(
                    self,
                    site=site,
                    st=st,
                    now=now,
                    user_buckets=user_buckets,
                    local_ready=local_ready,
                    core_ready=core_ready,
                    events=events,
                )
                if cand is not None:
                    enter_candidates.append(cand)
            else:
                restore_cand, retreat_cand = evaluate_spread_retreat(
                    self,
                    site=site,
                    st=st,
                    now=now,
                    snapshot=snapshot,
                    user_buckets=user_buckets,
                    local_ready=local_ready,
                    core_ready=core_ready,
                    k_current=k_current,
                    retreat_factor=SPREAD_RETREAT_FACTOR,
                    retreat_floor=SPREAD_RETREAT_FLOOR,
                    events=events,
                )
                if restore_cand is not None:
                    restore_candidates.append(restore_cand)
                if retreat_cand is not None:
                    retreat_candidates.append(retreat_cand)

        self._execute_transitions(
            events, now, snapshot, wall_now, k_current,
            enter_candidates, restore_candidates, retreat_candidates,
        )

        # Refresh k, core_ratio, and local_weight across all sites after any transition in this tick.
        k_final = self.spread_count()
        for st in self.states.values():
            st.last_spread_k = k_final
            if st.desired_route == 'SPREAD':
                st.last_core_ratio = spread_core_ratio(st.last_core_ready, st.last_local_ready, k_final, st.retreat_level)
                st.last_local_weight = spread_local_weight(st.last_core_ready, st.last_local_ready, k_final, st.retreat_level)
            else:
                st.last_core_ratio = 0.0
                st.last_local_weight = 1

        return events
