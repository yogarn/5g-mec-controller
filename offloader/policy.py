"""Per-site vertical offloading state machine (LOCAL / weighted SPREAD with gradual retreat)."""
from dataclasses import replace
import math

from offloader.entry import evaluate_local_entry
from offloader.envoy_ack import check_ack_timeouts, record_eds_ack, record_nack, record_rds_ack
from offloader.retreat import evaluate_spread_retreat
from offloader.spread import (
    SPREAD_RETREAT_FACTOR,
    SPREAD_RETREAT_FLOOR,
    _bucket_stale,
    _overall_user_bucket,
    _post_ack_window,
    post_ack_window,
    spread_core_ratio,
    spread_local_weight,
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
from offloader.transitions import apply_core_failback, apply_local_only, execute_transitions


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

    def _spread_extra(self, st: SiteState, local_ready: int, core_ready: int,
                      k: int | None = None, *, level: int | None = None) -> dict:
        k_val = self.spread_count() if k is None else int(k)
        n_val = st.retreat_level if level is None else int(level)
        return {
            'c': core_ready, 'x_i': local_ready, 'k': k_val, 'n': n_val,
            'good_level': st.good_level,
            'core_ratio': round(spread_core_ratio(core_ready, local_ready, k_val, n_val), 6),
            'local_weight': spread_local_weight(core_ready, local_ready, k_val, n_val),
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
        st.retreat_level = st.good_level = 0
        st.hold_until = st.pending_weight_since = st.pending_weight_level = None

    def _clear_breach(self, st: SiteState):
        st.breach_started_at = None
        st.breach_fresh_updates = 0

    def _clear_recovery(self, st: SiteState):
        st.recovery_started_at = None
        st.recovery_fresh_updates = 0

    def _advance_breach(self, st: SiteState, now: float, seq: int) -> bool:
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
        return st.last_transition_time is None or (now - st.last_transition_time) >= self.config.min_dwell_seconds

    def _note_hold(self, events: list[DecisionEvent], st: SiteState, site: str, note: str, *,
                   local_ready: int, core_ready: int, extra: dict | None = None):
        if st.last_status_note == note:
            return
        st.last_status_note = note
        merged = self._spread_extra(st, local_ready, core_ready)
        if extra:
            merged.update(extra)
        events.append(st.make_event(
            timestamp=self._now, event='hold_route', old_route='SPREAD', new_route='SPREAD',
            desired_route='SPREAD', reason=note, local_ready=local_ready, core_ready=core_ready, extra=merged,
        ))

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
        return any(
            not s.envoy_update_failed and (
                (s.pending_update_since is not None and now - s.pending_update_since < timeout)
                or (s.pending_weight_since is not None and now - s.pending_weight_since < timeout)
            )
            for s in self.states.values()
        )

    def _can_start_route_switch(self, st: SiteState, now: float, *,
                                snapshot: dict | None = None, require_prev_post_ack: bool = True) -> bool:
        if self.last_route_switch_at is not None and now - self.last_route_switch_at < self.config.route_switch_interval_seconds:
            return False
        if self._has_pending_ack(now):
            return False
        if require_prev_post_ack and snapshot is not None and self.last_switched_site is not None:
            prev = self.states[self.last_switched_site]
            if prev is not st and not prev.envoy_update_failed:
                if prev.active_route != prev.desired_route or prev.pending_weight_since is not None:
                    return False
                confirmed = prev.spread_confirmed_since if prev.active_route == 'SPREAD' else prev.local_confirmed_since
                if post_ack_window(snapshot, confirmed) is None:
                    return False
        return True

    def _try_start_route_switch(self, st: SiteState, route: str, now: float, *,
                                snapshot: dict | None = None, require_prev_post_ack: bool = True) -> bool:
        if not self._can_start_route_switch(st, now, snapshot=snapshot, require_prev_post_ack=require_prev_post_ack):
            return False
        self.last_route_switch_at, self.last_switched_site = now, st.site
        st.desired_route = route
        st.last_transition_time = st.pending_update_since = now
        st.pending_weight_since = st.pending_weight_level = None
        st.envoy_update_failed = False
        return True

    def _try_start_weight_change(self, st: SiteState, new_level: int, now: float, *,
                                 snapshot: dict | None = None) -> bool:
        if not self._can_start_route_switch(st, now, snapshot=snapshot, require_prev_post_ack=True):
            return False
        self.last_route_switch_at, self.last_switched_site = now, st.site
        st.pending_weight_fast = int(new_level) > st.retreat_level
        st.spread_wait_seconds = None
        st.retreat_level = st.pending_weight_level = int(new_level)
        st.pending_weight_since = now
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
        old, k_before = st.desired_route, self.spread_count()
        if route != old and not self._try_start_route_switch(st, route, now, require_prev_post_ack=False):
            raise RouteSwitchBusy('Another route change is pending or inside the global switch interval')
        st.manual_override = route
        self._clear_breach(st)
        self._clear_recovery(st)
        self._reset_retreat_state(st)
        if route == old:
            return None
        st.local_confirmed_since = st.spread_confirmed_since = None
        self._reset_other_spread_post_ack(site, None)
        st.last_decision_reason = f'manual_override_{route.lower()}'
        k_val = self.spread_count() if route == 'SPREAD' else k_before
        extra = self._spread_extra(st, st.last_local_ready, st.last_core_ready, k_val) if 'SPREAD' in (old, route) else {}
        return st.make_event(
            timestamp=now, event='route_decision', old_route=old, new_route=route,
            reason=st.last_decision_reason, extra=extra,
        )

    def record_envoy_ack(self, site: str, route: str, version: str, now: float, *,
                         monotonic_now: float | None = None) -> DecisionEvent | None:
        return record_rds_ack(self, site, route, version, now, monotonic_now=monotonic_now)

    def record_envoy_eds_ack(self, site: str, level: int, version: str, now: float, *,
                             monotonic_now: float | None = None) -> DecisionEvent | None:
        return record_eds_ack(self, site, level, version, now, monotonic_now=monotonic_now)

    def record_envoy_nack(self, site: str, error_message: str, now: float, *,
                          reason: str = 'envoy_rds_nack') -> DecisionEvent:
        return record_nack(self, site, error_message, now, reason=reason)

    def _refresh_spread_weights(self):
        k = self.spread_count()
        for st in self.states.values():
            st.last_spread_k = k
            if st.desired_route == 'SPREAD':
                st.last_core_ratio = spread_core_ratio(st.last_core_ready, st.last_local_ready, k, st.retreat_level)
                st.last_local_weight = spread_local_weight(st.last_core_ready, st.last_local_ready, k, st.retreat_level)
            else:
                st.last_core_ratio, st.last_local_weight = 0.0, 1

    def _handle_collector_error(self, st: SiteState, now: float, collector_error: str | None,
                                k_current: int, events: list[DecisionEvent]):
        self._clear_breach(st)
        self._clear_recovery(st)
        if st.last_status_note == 'collector_error':
            return
        st.last_status_note = 'collector_error'
        extra = {'collector_error': collector_error or 'missing_snapshot'}
        if st.desired_route == 'SPREAD':
            extra.update(self._spread_extra(st, st.last_local_ready, st.last_core_ready, k_current))
        events.append(st.make_event(
            timestamp=now, event='hold_route', old_route=st.desired_route, new_route=st.desired_route,
            reason='collector_error', p90_ms=None, samples=0, attempts=0, error_rate=None, extra=extra,
        ))

    def _update_site_endpoints(self, st: SiteState, local_ready: int, core_ready: int, k_current: int, wall_now):
        st.last_local_ready, st.last_core_ready, st.last_spread_k = local_ready, core_ready, k_current
        if st.desired_route == 'SPREAD':
            st.last_core_ratio = spread_core_ratio(core_ready, local_ready, k_current, st.retreat_level)
            st.last_local_weight = spread_local_weight(core_ready, local_ready, k_current, st.retreat_level)
        else:
            st.last_core_ratio, st.last_local_weight = 0.0, 1
        if (st.desired_route == 'SPREAD' and st.active_route == 'SPREAD'
                and st.pending_weight_since is None and st.spread_confirmed_since is None
                and isinstance(wall_now, (int, float)) and math.isfinite(wall_now)):
            st.spread_confirmed_since, st.spread_wait_seconds = wall_now, None

    def _evaluate_site(self, site: str, now: float, snapshot: dict | None, wall_now,
                       local_ready: int, core_ready: int, k_current: int,
                       collector_error: str | None, events: list[DecisionEvent],
                       enter_cands: list[dict], restore_cands: list[dict], retreat_cands: list[dict]):
        st = self.states[site]
        self._update_site_endpoints(st, local_ready, core_ready, k_current, wall_now)
        check_ack_timeouts(self, events, st, now, k_current)
        if st.manual_override is not None:
            st.desired_route = st.manual_override
            return
        if self.config.mode == 'local-only':
            apply_local_only(self, st, now, events)
            return
        if st.desired_route == 'SPREAD' and core_ready == 0:
            apply_core_failback(self, st, now, snapshot, wall_now, k_current, events)
            return
        if collector_error is not None or snapshot is None:
            self._handle_collector_error(st, now, collector_error, k_current, events)
            return

        user_buckets = ((snapshot.get('sites') or {}).get(site) or {}).get('user') or {}
        if st.desired_route == 'LOCAL':
            cand = evaluate_local_entry(
                self, site=site, st=st, now=now, user_buckets=user_buckets,
                local_ready=local_ready, core_ready=core_ready, events=events,
            )
            if cand is not None:
                enter_cands.append(cand)
        else:
            restore_cand, retreat_cand = evaluate_spread_retreat(
                self, site=site, st=st, now=now, snapshot=snapshot, user_buckets=user_buckets,
                local_ready=local_ready, core_ready=core_ready, k_current=k_current,
                retreat_factor=SPREAD_RETREAT_FACTOR, retreat_floor=SPREAD_RETREAT_FLOOR, events=events,
            )
            if restore_cand is not None:
                restore_cands.append(restore_cand)
            if retreat_cand is not None:
                retreat_cands.append(retreat_cand)

    def evaluate(
        self,
        now: float,
        snapshot: dict | None,
        local_ready_counts: dict[str, int],
        core_ready_count: int,
        *,
        collector_error: str | None = None,
    ) -> list[DecisionEvent]:
        """Run one 1 Hz tick: (1) update site metrics, (2) check ACK timeouts, (3) collect
        enter/restore/retreat candidates, (4) execute <=1 change, (5) refresh spread weights."""
        events: list[DecisionEvent] = []
        self._now = now
        wall_now = snapshot.get('generated_at') if isinstance(snapshot, dict) else None
        k_current = self.spread_count()
        core_ready = max(0, int(core_ready_count))
        enter_cands, restore_cands, retreat_cands = [], [], []

        for site in SITES:
            local_ready = max(0, int(local_ready_counts.get(site, 0)))
            self._evaluate_site(
                site, now, snapshot, wall_now, local_ready, core_ready, k_current,
                collector_error, events, enter_cands, restore_cands, retreat_cands,
            )

        execute_transitions(self, events, now, snapshot, wall_now, k_current, enter_cands, restore_cands, retreat_cands)
        self._refresh_spread_weights()
        return events
