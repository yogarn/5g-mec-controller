"""Per-site vertical offloading state machine (LOCAL / weighted SPREAD with gradual retreat)."""
from dataclasses import dataclass, field, replace
import math


SITES = ('gnb-1', 'gnb-2', 'gnb-3')
ROUTES = ('LOCAL', 'SPREAD')
MODES = ('local-only', 'vertical')

# Gradual SPREAD retreat: halve the Core traffic share at each step to prevent full-jump oscillation.
SPREAD_RETREAT_FACTOR = 2
# Below 1% remaining Core share, Core contribution is negligible so the site returns to full LOCAL.
SPREAD_RETREAT_FLOOR = 0.01


class RouteSwitchBusy(RuntimeError):
    """The offloader cannot start another route or weight change yet."""


DEFAULT_SITE_NODES = {
    'gnb-1': 'gnb1',
    'gnb-2': 'gnb-2',
    'gnb-3': 'gnb-3',
}


def spread_core_ratio(core_ready: int, local_ready: int, k: int, retreat_level: int = 0) -> float:
    """Return target Core traffic share s_n = s_0 / 2^n where s_0 = c / (c + max(1, k) * x_i)."""
    c = max(0, int(core_ready))
    x_i = max(0, int(local_ready))
    k_eff = max(1, int(k))
    n = max(0, int(retreat_level))
    denom = c + k_eff * x_i
    if denom <= 0 or c == 0:
        return 0.0
    s0 = c / denom
    return s0 / (SPREAD_RETREAT_FACTOR ** n)


def spread_local_weight(core_ready: int, local_ready: int, k: int, retreat_level: int = 0) -> int:
    """Return integer EDS load_balancing_weight (>= 1) per local pod for target Core share s_n.

    Core pods always receive weight 1. Local pods receive:
        w_l = max(1, round(c * (1 - s_n) / (s_n * x_i)))
    Edge cases:
    - c == 0 or x_i == 0: returns 1 (valid >= 1 integer without division by zero).
    """
    c = max(0, int(core_ready))
    x_i = max(0, int(local_ready))
    if c == 0 or x_i == 0:
        return 1
    s_n = spread_core_ratio(c, x_i, k, retreat_level)
    if s_n <= 0.0:
        return 1
    if s_n >= 1.0:
        return 1
    return max(1, int(round(c * (1.0 - s_n) / (s_n * x_i))))


@dataclass(frozen=True)
class OffloadConfig:
    """Validated vertical offloading parameters.

    Default rationale (for 5 RPS/UE campaign schedule with 15s collector window):
    - poll_interval_seconds=1.0: 1 Hz telemetry read cadence (never on data path).
    - offload_p90_threshold_ms=150.0: matches campaign SLO target (150 ms) for both
      entering SPREAD (> SLO) and evaluating gradual retreat steps (overall p90 <= SLO).
    - min_samples=15: 1 UE at 5 RPS produces ~75 req in 15s; 15 successes (~3s of 1 UE)
      gives a statistically meaningful p90 without delaying overload detection.
    - min_error_samples=5 & offload_error_rate_threshold=0.5: repeated failures/timeouts
      trigger SPREAD or retreat restore even when successes=0 (p90_ms=None).
    - breach_duration_seconds=5.0 with min_fresh_updates=2: requires sustained overload
      across multiple fresh sample arrivals, not repeated reads of the same rolling window.
    - min_dwell_seconds=15.0: holds a newly applied route or restored retreat level at least
      one collector window to avoid oscillation while the rolling window flushes.
    - route_switch_interval_seconds=5.0: spaces route and weight changes globally across sites.
    - stale_after_seconds=5.0: holds the last applied route when telemetry stops advancing.
    - envoy_ack_timeout_seconds=5.0: flags failed/unacknowledged Envoy xDS updates.
    """

    mode: str = 'vertical'
    poll_interval_seconds: float = 1.0
    collector_timeout_seconds: float = 1.0
    offload_p90_threshold_ms: float = 150.0
    min_samples: int = 15
    min_error_samples: int = 5
    offload_error_rate_threshold: float = 0.5
    breach_duration_seconds: float = 5.0
    min_fresh_updates: int = 2
    min_dwell_seconds: float = 15.0
    route_switch_interval_seconds: float = 5.0
    stale_after_seconds: float = 5.0
    envoy_ack_timeout_seconds: float = 5.0

    def __post_init__(self):
        if self.mode not in MODES:
            raise ValueError(f'mode must be one of {MODES}')
        for name in (
            'poll_interval_seconds',
            'collector_timeout_seconds',
            'offload_p90_threshold_ms',
            'breach_duration_seconds',
            'min_dwell_seconds',
            'route_switch_interval_seconds',
            'stale_after_seconds',
            'envoy_ack_timeout_seconds',
        ):
            val = getattr(self, name)
            if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(val) or val <= 0:
                raise ValueError(f'{name} must be finite and positive')
        for name in ('min_samples', 'min_error_samples', 'min_fresh_updates'):
            val = getattr(self, name)
            if isinstance(val, bool) or not isinstance(val, int) or val < 1:
                raise ValueError(f'{name} must be a positive integer')
        val = self.offload_error_rate_threshold
        if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(val) or not 0.0 <= val <= 1.0:
            raise ValueError('offload_error_rate_threshold must be in [0, 1]')


@dataclass
class SiteState:
    site: str
    desired_route: str = 'LOCAL'
    active_route: str = 'LOCAL'
    manual_override: str | None = None
    last_transition_time: float | None = None
    local_confirmed_since: float | None = None
    spread_confirmed_since: float | None = None
    retreat_level: int = 0
    good_level: int = 0
    hold_until: float | None = None
    pending_weight_since: float | None = None
    pending_weight_level: int | None = None
    pending_weight_fast: bool = False
    spread_wait_seconds: float | None = None
    breach_started_at: float | None = None
    breach_last_seq: int = 0
    breach_fresh_updates: int = 0
    recovery_started_at: float | None = None
    recovery_last_seq: int = 0
    recovery_fresh_updates: int = 0
    last_status_note: str = 'init'
    last_decision_reason: str = 'init'
    last_p90_ms: float | None = None
    last_samples: int = 0
    last_attempts: int = 0
    last_error_rate: float | None = None
    last_unlabeled_timeouts: int = 0
    last_local_ready: int = 0
    last_core_ready: int = 0
    last_spread_k: int = 0
    last_core_ratio: float = 0.0
    last_local_weight: int = 1
    pending_update_since: float | None = None
    envoy_update_failed: bool = False
    envoy_last_nack: str | None = None

    def reset(self, *, active_route: str = 'LOCAL'):
        self.desired_route = 'LOCAL'
        self.active_route = active_route
        self.manual_override = None
        self.last_transition_time = None
        self.local_confirmed_since = None
        self.spread_confirmed_since = None
        self.retreat_level = 0
        self.good_level = 0
        self.hold_until = None
        self.pending_weight_since = None
        self.pending_weight_level = None
        self.pending_weight_fast = False
        self.spread_wait_seconds = None
        self.breach_started_at = None
        self.breach_last_seq = 0
        self.breach_fresh_updates = 0
        self.recovery_started_at = None
        self.recovery_last_seq = 0
        self.recovery_fresh_updates = 0
        self.last_status_note = 'reset'
        self.last_decision_reason = 'reset'
        self.last_p90_ms = None
        self.last_samples = 0
        self.last_attempts = 0
        self.last_error_rate = None
        self.last_unlabeled_timeouts = 0
        self.last_spread_k = 0
        self.last_core_ratio = 0.0
        self.last_local_weight = 1
        self.pending_update_since = None
        self.envoy_update_failed = False
        self.envoy_last_nack = None

    def to_dict(self):
        return {
            'site': self.site,
            'desired_route': self.desired_route,
            'active_route': self.active_route,
            'manual_override': self.manual_override,
            'last_transition_time': self.last_transition_time,
            'local_confirmed_since': self.local_confirmed_since,
            'spread_confirmed_since': self.spread_confirmed_since,
            'retreat_level': self.retreat_level,
            'good_level': self.good_level,
            'hold_until': self.hold_until,
            'pending_weight_since': self.pending_weight_since,
            'last_status_note': self.last_status_note,
            'last_decision_reason': self.last_decision_reason,
            'last_p90_ms': self.last_p90_ms,
            'last_samples': self.last_samples,
            'last_attempts': self.last_attempts,
            'last_error_rate': self.last_error_rate,
            'last_unlabeled_timeouts': self.last_unlabeled_timeouts,
            'local_ready_endpoints': self.last_local_ready,
            'core_ready_endpoints': self.last_core_ready,
            'spread_sites': self.last_spread_k,
            'core_ratio': round(self.last_core_ratio, 6),
            'local_weight': self.last_local_weight,
            'envoy_update_failed': self.envoy_update_failed,
            'envoy_last_nack': self.envoy_last_nack,
        }


@dataclass
class DecisionEvent:
    timestamp: float
    event: str
    site: str
    old_route: str
    new_route: str
    desired_route: str
    active_route: str
    reason: str
    p90_ms: float | None
    samples: int
    attempts: int
    error_rate: float | None
    local_ready_endpoints: int
    core_ready_endpoints: int
    extra: dict = field(default_factory=dict)

    def to_dict(self):
        record = {
            'timestamp': self.timestamp,
            'event': self.event,
            'site': self.site,
            'old_route': self.old_route,
            'new_route': self.new_route,
            'desired_route': self.desired_route,
            'active_route': self.active_route,
            'reason': self.reason,
            'p90_ms': self.p90_ms,
            'samples': self.samples,
            'attempts': self.attempts,
            'error_rate': self.error_rate,
            'local_ready_endpoints': self.local_ready_endpoints,
            'core_ready_endpoints': self.core_ready_endpoints,
        }
        if self.extra:
            record.update(self.extra)
        return record


def _bucket_stale(bucket, stale_after):
    if not isinstance(bucket, dict):
        return True
    status = bucket.get('status')
    if status in ('empty', 'stale', None):
        return True
    age = bucket.get('last_sample_age_seconds')
    if age is None or age > stale_after:
        return True
    return False


def _post_ack_window(snapshot, confirmed_since, wait=None):
    """Return the snapshot window once enough time has passed since the ACK.

    By default a full collector window must pass so no sample predates the change. A retreat step that
    followed a healthy level may be judged after `wait` seconds instead; the window then still holds the
    previous level's (already healthy) samples, and any slow samples of the new level lift the p90.
    """
    if not isinstance(snapshot, dict):
        return None
    window = snapshot.get('window_seconds')
    generated = snapshot.get('generated_at')
    if (
        confirmed_since is None
        or not isinstance(window, (int, float)) or not math.isfinite(window) or window <= 0
        or not isinstance(generated, (int, float)) or not math.isfinite(generated)
    ):
        return None
    required = window if wait is None else min(window, wait)
    if generated - confirmed_since < required:
        return None
    return window


def _overall_user_bucket(user_buckets: dict, stale_after: float) -> dict:
    """Return the site's overall user bucket from the snapshot.

    When the collector provides user['overall'], it includes all user samples for
    the site regardless of route (local, core, and unknown failures/timeouts).
    If an older snapshot omits 'overall', this falls back to combining 'local' and
    'core' only (which does NOT include 'unknown' samples).
    """
    overall = user_buckets.get('overall')
    if isinstance(overall, dict):
        return overall
    local_b = user_buckets.get('local') or {}
    core_b = user_buckets.get('core') or {}
    attempts = int(local_b.get('attempts') or 0) + int(core_b.get('attempts') or 0)
    successes = int(local_b.get('successes') or 0) + int(core_b.get('successes') or 0)
    errors = int(local_b.get('errors') or 0) + int(core_b.get('errors') or 0)
    timeouts = int(local_b.get('timeouts') or 0) + int(core_b.get('timeouts') or 0)
    seq = max(int(local_b.get('newest_sample_seq') or 0), int(core_b.get('newest_sample_seq') or 0))
    ages = [
        a for a in (local_b.get('last_sample_age_seconds'), core_b.get('last_sample_age_seconds'))
        if a is not None
    ]
    age = min(ages) if ages else None
    p90s = [
        b.get('p90_ms')
        for b in (local_b, core_b)
        if not _bucket_stale(b, stale_after) and b.get('p90_ms') is not None
    ]
    p90_ms = max(p90s) if p90s else None
    if attempts == 0:
        status = 'empty'
    elif age is None or age > stale_after:
        status = 'stale'
    elif successes == 0:
        status = 'no_success'
    else:
        status = 'ok'
    return {
        'status': status,
        'p90_ms': p90_ms,
        'attempts': attempts,
        'successes': successes,
        'errors': errors,
        'timeouts': timeouts,
        'error_rate': None if attempts == 0 else round(errors / attempts, 6),
        'last_sample_age_seconds': age,
        'newest_sample_seq': seq,
    }


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

            # Detect Envoy RDS ACK timeout for an outstanding route change.
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

            # Detect Envoy EDS ACK timeout for an outstanding weight change.
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
                self._clear_recovery(st)
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
                    self._clear_breach(st)
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
                    continue

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
                    self._clear_breach(st)
                    st.last_status_note = 'local_healthy'
                    continue

                if core_ready == 0:
                    self._clear_breach(st)
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
                    continue

                if not self._advance_breach(st, now, effective_seq):
                    st.last_status_note = 'breach_waiting_fresh_samples'
                    continue
                st.last_status_note = 'breach_pending'

                if (
                    (now - st.breach_started_at) >= cfg.breach_duration_seconds
                    and st.breach_fresh_updates >= cfg.min_fresh_updates
                    and self._dwell_satisfied(st, now)
                ):
                    if unlabeled_timeout_breach and not p90_breach and not labeled_error_breach:
                        reason = 'local_unlabeled_timeout_breach'
                    elif error_breach and not p90_breach:
                        reason = 'local_error_rate_breach'
                    elif p90_breach and error_breach:
                        reason = 'local_p90_and_error_breach'
                    else:
                        reason = 'local_p90_breach'
                    enter_candidates.append({
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
                    })
            else:
                # SPREAD state: gradual retreat by halving Core share per step, with restore on breach.
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
                    self._clear_breach(st)
                    self._clear_recovery(st)
                    self._note_hold(
                        events, st, site, 'spread_waiting_eds_ack',
                        local_ready=local_ready, core_ready=core_ready,
                        p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
                    )
                    continue

                if st.active_route != 'SPREAD' or _post_ack_window(snapshot, st.spread_confirmed_since, st.spread_wait_seconds) is None:
                    self._clear_breach(st)
                    self._clear_recovery(st)
                    self._note_hold(
                        events, st, site, 'spread_waiting_post_ack',
                        local_ready=local_ready, core_ready=core_ready,
                        p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
                    )
                    continue

                if _bucket_stale(overall, cfg.stale_after_seconds):
                    self._clear_breach(st)
                    self._clear_recovery(st)
                    note = 'spread_telemetry_empty' if overall.get('status') == 'empty' else 'spread_telemetry_stale'
                    self._note_hold(
                        events, st, site, note,
                        local_ready=local_ready, core_ready=core_ready,
                        p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
                    )
                    continue

                if local_ready == 0:
                    self._clear_breach(st)
                    self._clear_recovery(st)
                    self._note_hold(
                        events, st, site, 'spread_local_not_ready',
                        local_ready=local_ready, core_ready=core_ready,
                        p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
                    )
                    continue

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
                    self._clear_recovery(st)
                    if st.retreat_level > st.good_level or st.good_level > 0:
                        if self._advance_breach(st, now, seq) and st.breach_fresh_updates >= cfg.min_fresh_updates:
                            reason = 'spread_retreat_error_breach' if spread_error_breach and not spread_p90_breach else 'spread_retreat_p90_breach'
                            restore_candidates.append({
                                'site': site,
                                'st': st,
                                'reason': reason,
                                # A level already judged healthy that now breaches goes one level further back.
                                'target_level': st.good_level if st.retreat_level > st.good_level else st.good_level - 1,
                                'core_ratio': spread_core_ratio(core_ready, local_ready, k_current, st.retreat_level),
                                'p90_ms': p90_ms,
                                'samples': successes,
                                'attempts': attempts,
                                'error_rate': error_rate,
                                'local_ready': local_ready,
                                'core_ready': core_ready,
                            })
                        else:
                            st.last_status_note = 'spread_restore_pending'
                    else:
                        self._clear_breach(st)
                        note = 'spread_error_rate_high' if spread_error_breach and not spread_p90_breach else 'spread_p90_above_slo'
                        self._note_hold(
                            events, st, site, note,
                            local_ready=local_ready, core_ready=core_ready,
                            p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
                        )
                    continue

                self._clear_breach(st)

                if successes < cfg.min_samples or p90_ms is None:
                    self._clear_recovery(st)
                    self._note_hold(
                        events, st, site, 'spread_insufficient_samples',
                        local_ready=local_ready, core_ready=core_ready,
                        p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
                    )
                    continue

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
                    continue

                if st.recovery_fresh_updates < cfg.min_fresh_updates:
                    continue

                # Step proven healthy: promote good_level to current retreat_level.
                if st.retreat_level > st.good_level:
                    st.good_level = st.retreat_level

                # Respect post-restore hold window before attempting another retreat step.
                if st.hold_until is not None and now < st.hold_until:
                    self._note_hold(
                        events, st, site, 'spread_retreat_hold',
                        local_ready=local_ready, core_ready=core_ready,
                        p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
                    )
                    continue

                if self._dwell_satisfied(st, now):
                    next_level = st.retreat_level + 1
                    next_ratio = spread_core_ratio(core_ready, local_ready, k_current, next_level)
                    action = 'complete' if next_ratio < SPREAD_RETREAT_FLOOR else 'step'
                    retreat_candidates.append({
                        'site': site,
                        'st': st,
                        'action': action,
                        'next_level': next_level,
                        'core_ratio': spread_core_ratio(core_ready, local_ready, k_current, st.retreat_level),
                        'p90_ms': p90_ms,
                        'samples': successes,
                        'attempts': attempts,
                        'error_rate': error_rate,
                        'local_ready': local_ready,
                        'core_ready': core_ready,
                    })

        # Phase 2: execute at most one mode or weight change per global interval.
        # Priority: 1) entering SPREAD (SLO protection), 2) restoring a breaching retreat step, 3) retreating.
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
