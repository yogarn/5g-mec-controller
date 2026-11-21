"""Per-site vertical offloading state machine and parameter validation."""
from dataclasses import dataclass, field, replace
import math


SITES = ('gnb-1', 'gnb-2', 'gnb-3')
ROUTES = ('LOCAL', 'CORE')
MODES = ('local-only', 'vertical')


class RouteSwitchBusy(RuntimeError):
    """The offloader cannot start another route change yet."""


DEFAULT_SITE_NODES = {
    'gnb-1': 'gnb1',
    'gnb-2': 'gnb-2',
    'gnb-3': 'gnb-3',
}


@dataclass(frozen=True)
class OffloadConfig:
    """Validated vertical offloading parameters.

    Default rationale (for 5 RPS/UE campaign schedule with 15s collector window):
    - poll_interval_seconds=1.0: 1 Hz telemetry read cadence (never on data path).
    - offload_p90_threshold_ms=150.0: matches campaign SLO target (150 ms).
    - recovery_p90_threshold_ms=125.0: uses the campaign's latency reference,
      providing 25 ms of hysteresis below the 150 ms offload threshold.
    - min_samples=15: 1 UE at 5 RPS produces ~75 req in 15s; 15 successes (~3s of 1 UE)
      gives a statistically meaningful p90 without delaying overload detection.
    - min_error_samples=5 & offload_error_rate_threshold=0.5: repeated failures/timeouts
      trigger fallback even when successes=0 (p90_ms=None).
    - breach_duration_seconds=5.0 with min_fresh_updates=2: requires sustained overload
      across multiple fresh sample arrivals, not repeated reads of the same rolling window.
    - probe_min_samples=5 & recovery_stable_seconds=10.0: requires healthy local probes.
      During Core overload, failback also requires more Ready local endpoints than at offload.
    - min_dwell_seconds=15.0: holds a newly applied route at least one collector window
      to avoid oscillation while the rolling window flushes.
    - route_switch_interval_seconds=5.0: spaces route changes globally across sites and directions.
    - stale_after_seconds=5.0: holds the last applied route when telemetry stops advancing.
    - envoy_ack_timeout_seconds=5.0: flags failed/unacknowledged Envoy xDS updates.
    """

    mode: str = 'vertical'
    poll_interval_seconds: float = 1.0
    collector_timeout_seconds: float = 1.0
    offload_p90_threshold_ms: float = 150.0
    recovery_p90_threshold_ms: float = 125.0
    min_samples: int = 15
    min_error_samples: int = 5
    offload_error_rate_threshold: float = 0.5
    breach_duration_seconds: float = 5.0
    min_fresh_updates: int = 2
    probe_min_samples: int = 5
    probe_max_error_rate: float = 0.1
    recovery_stable_seconds: float = 10.0
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
            'recovery_p90_threshold_ms',
            'breach_duration_seconds',
            'recovery_stable_seconds',
            'min_dwell_seconds',
            'route_switch_interval_seconds',
            'stale_after_seconds',
            'envoy_ack_timeout_seconds',
        ):
            val = getattr(self, name)
            if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(val) or val <= 0:
                raise ValueError(f'{name} must be finite and positive')
        if self.recovery_p90_threshold_ms >= self.offload_p90_threshold_ms:
            raise ValueError('recovery_p90_threshold_ms must be strictly less than offload_p90_threshold_ms')
        for name in ('min_samples', 'min_error_samples', 'min_fresh_updates', 'probe_min_samples'):
            val = getattr(self, name)
            if isinstance(val, bool) or not isinstance(val, int) or val < 1:
                raise ValueError(f'{name} must be a positive integer')
        for name in ('offload_error_rate_threshold', 'probe_max_error_rate'):
            val = getattr(self, name)
            if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(val) or not 0.0 <= val <= 1.0:
                raise ValueError(f'{name} must be in [0, 1]')


@dataclass
class SiteState:
    site: str
    desired_route: str = 'LOCAL'
    active_route: str = 'LOCAL'
    manual_override: str | None = None
    last_transition_time: float | None = None
    local_confirmed_since: float | None = None
    core_confirmed_since: float | None = None
    local_ready_since: float | None = None
    breach_started_at: float | None = None
    breach_last_seq: int = 0
    breach_fresh_updates: int = 0
    core_overload_confirmed: bool = False
    core_gate_event_sent: bool = False
    recovery_started_at: float | None = None
    recovery_last_seq: int = 0
    recovery_fresh_updates: int = 0
    recovery_reason: str | None = None
    offload_ready_count: int | None = None
    healthy_local_rps: dict[int, float] = field(default_factory=dict)
    last_status_note: str = 'init'
    last_decision_reason: str = 'init'
    last_p90_ms: float | None = None
    last_samples: int = 0
    last_attempts: int = 0
    last_error_rate: float | None = None
    last_unlabeled_timeouts: int = 0
    last_local_ready: int = 0
    last_core_ready: int = 0
    pending_update_since: float | None = None
    envoy_update_failed: bool = False
    envoy_last_nack: str | None = None

    def reset(self, *, active_route: str = 'LOCAL'):
        self.desired_route = 'LOCAL'
        self.active_route = active_route
        self.manual_override = None
        self.last_transition_time = None
        self.local_confirmed_since = None
        self.core_confirmed_since = None
        self.local_ready_since = None
        self.breach_started_at = None
        self.breach_last_seq = 0
        self.breach_fresh_updates = 0
        self.core_overload_confirmed = False
        self.core_gate_event_sent = False
        self.recovery_started_at = None
        self.recovery_last_seq = 0
        self.recovery_fresh_updates = 0
        self.recovery_reason = None
        self.offload_ready_count = None
        self.healthy_local_rps.clear()
        self.last_status_note = 'reset'
        self.last_decision_reason = 'reset'
        self.last_p90_ms = None
        self.last_samples = 0
        self.last_attempts = 0
        self.last_error_rate = None
        self.last_unlabeled_timeouts = 0
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
            'last_status_note': self.last_status_note,
            'last_decision_reason': self.last_decision_reason,
            'last_p90_ms': self.last_p90_ms,
            'last_samples': self.last_samples,
            'last_attempts': self.last_attempts,
            'last_error_rate': self.last_error_rate,
            'last_unlabeled_timeouts': self.last_unlabeled_timeouts,
            'local_ready_endpoints': self.last_local_ready,
            'core_ready_endpoints': self.last_core_ready,
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


def _post_ack_window(snapshot, confirmed_since):
    window = snapshot.get('window_seconds')
    generated = snapshot.get('generated_at')
    if (
        confirmed_since is None
        or not isinstance(window, (int, float)) or not math.isfinite(window) or window <= 0
        or not isinstance(generated, (int, float)) or not math.isfinite(generated)
        or generated - confirmed_since < window
    ):
        return None
    return window


def _healthy_user_rps(snapshot, user_buckets, route, confirmed_since, cfg):
    window = _post_ack_window(snapshot, confirmed_since)
    if window is None or any(bucket.get('attempts', 0) for name, bucket in user_buckets.items() if name != route):
        return None
    bucket = user_buckets.get(route) or {}
    p90 = bucket.get('p90_ms')
    if (
        _bucket_stale(bucket, cfg.stale_after_seconds)
        or bucket.get('successes', 0) < cfg.min_samples
        or bucket.get('errors') != 0
        or p90 is None or p90 > cfg.offload_p90_threshold_ms
    ):
        return None
    return bucket['attempts'] / window


class VerticalOffloadPolicy:
    """Per-site offloading decisions with a shared route-switch interval."""

    def __init__(self, config: OffloadConfig | None = None):
        self.config = config or OffloadConfig()
        self.states = {site: SiteState(site=site) for site in SITES}
        self.last_route_switch_at: float | None = None
        self._now: float = 0.0

    def set_mode(self, mode: str):
        self.config = replace(self.config, mode=mode)
        if mode == 'local-only':
            for st in self.states.values():
                st.manual_override = None
                self._clear_breach(st)
                self._clear_recovery(st)
                st.offload_ready_count = None
                st.healthy_local_rps.clear()
                st.desired_route = 'LOCAL'
                st.pending_update_since = None
                st.last_decision_reason = 'mode_local_only'

    def reset(self, *, active_route: str = 'LOCAL'):
        self.last_route_switch_at = None
        for st in self.states.values():
            st.reset(active_route=active_route)

    def _try_start_route_switch(self, st: SiteState, route: str, now: float) -> bool:
        last_switch = self.last_route_switch_at
        if last_switch is not None and now - last_switch < self.config.route_switch_interval_seconds:
            return False
        if any(
            other.pending_update_since is not None
            and not other.envoy_update_failed
            and now - other.pending_update_since < self.config.envoy_ack_timeout_seconds
            for other in self.states.values()
        ):
            return False
        self.last_route_switch_at = now
        st.desired_route = route
        st.last_transition_time = now
        st.pending_update_since = now
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
        if self.config.mode == 'local-only' and route == 'CORE':
            raise PermissionError('CORE override is forbidden while mode is local-only')
        old = st.desired_route
        if route != old and not self._try_start_route_switch(st, route, now):
            raise RouteSwitchBusy('Another route change is pending or inside the global switch interval')
        st.manual_override = route
        self._clear_breach(st)
        self._clear_recovery(st)
        st.offload_ready_count = None
        if route != old:
            st.local_confirmed_since = None
            st.last_decision_reason = f'manual_override_{route.lower()}'
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
                st.core_confirmed_since = None
            else:
                st.local_confirmed_since = None
                if st.core_confirmed_since is None or old_active != 'CORE':
                    st.core_confirmed_since = now
        if old_active != route or st.last_status_note == 'envoy_update_failed':
            st.last_status_note = 'envoy_acked'
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
                extra={'envoy_ack_version': version},
            )
        return None

    def record_envoy_nack(self, site: str, error_message: str, now: float) -> DecisionEvent:
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
            reason='envoy_rds_nack',
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
        st.core_overload_confirmed = False
        st.core_gate_event_sent = False

    def _core_overloaded_elsewhere(self, st: SiteState) -> bool:
        """True while another site on CORE has a confirmed CORE overload."""
        return any(
            other is not st and other.active_route == 'CORE' and other.core_overload_confirmed
            for other in self.states.values()
        )

    def _note_hold(self, events, st: SiteState, site: str, note: str, *, local_ready: int, core_ready: int,
                   p90_ms, samples: int, attempts: int, error_rate, extra: dict | None = None):
        """Record a CORE-state hold reason once per change so it is visible in the event log."""
        if st.last_status_note == note:
            return
        st.last_status_note = note
        events.append(
            DecisionEvent(
                timestamp=self._now,
                event='hold_route',
                site=site,
                old_route='CORE',
                new_route='CORE',
                desired_route='CORE',
                active_route=st.active_route,
                reason=note,
                p90_ms=p90_ms,
                samples=samples,
                attempts=attempts,
                error_rate=error_rate,
                local_ready_endpoints=local_ready,
                core_ready_endpoints=core_ready,
                extra=extra or {},
            )
        )

    def _clear_recovery(self, st: SiteState):
        st.recovery_started_at = None
        st.recovery_fresh_updates = 0
        st.recovery_reason = None

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

    def _update_core_overload(self, st, now, snapshot, core_bucket):
        cfg = self.config
        if _post_ack_window(snapshot, st.core_confirmed_since) is None or _bucket_stale(core_bucket, cfg.stale_after_seconds):
            self._clear_breach(st)
            return
        p90 = core_bucket.get('p90_ms')
        error_rate = core_bucket.get('error_rate')
        overloaded = (
            (core_bucket.get('successes', 0) >= cfg.min_samples
             and p90 is not None and p90 > cfg.offload_p90_threshold_ms)
            or (core_bucket.get('attempts', 0) >= cfg.min_error_samples
                and error_rate is not None and error_rate >= cfg.offload_error_rate_threshold)
        )
        if not overloaded:
            self._clear_breach(st)
        elif self._advance_breach(st, now, int(core_bucket.get('newest_sample_seq') or 0)):
            st.core_overload_confirmed = (
                now - st.breach_started_at >= cfg.breach_duration_seconds
                and st.breach_fresh_updates >= cfg.min_fresh_updates
            )

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
        """Evaluate all sites from one snapshot and serialize runtime route changes."""
        events: list[DecisionEvent] = []
        cfg = self.config
        self._now = now

        for site in SITES:
            st = self.states[site]
            local_ready = max(0, int(local_ready_counts.get(site, 0)))
            core_ready = max(0, int(core_ready_count))
            if st.local_ready_since is None or local_ready != st.last_local_ready:
                st.local_ready_since = now
                self._clear_recovery(st)
            st.last_local_ready = local_ready
            st.last_core_ready = core_ready

            # Detect Envoy ACK timeout for an outstanding route change.
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

            # Manual override bypasses automatic state transitions (except logging).
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

            # Safety failback: if CORE is active/desired and Core loses all ready endpoints.
            if st.desired_route == 'CORE' and core_ready == 0:
                self._clear_breach(st)
                self._clear_recovery(st)
                if local_ready > 0:
                    if not self._try_start_route_switch(st, 'LOCAL', now):
                        st.last_status_note = 'route_switch_waiting'
                        continue
                    st.offload_ready_count = None
                    st.last_decision_reason = 'core_backend_unready_failback'
                    st.last_status_note = 'core_backend_unready_failback'
                    events.append(
                        DecisionEvent(
                            timestamp=now,
                            event='route_decision',
                            site=site,
                            old_route='CORE',
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
                        )
                    )
                else:
                    if st.last_status_note != 'both_backends_unready':
                        st.last_status_note = 'both_backends_unready'
                        events.append(
                            DecisionEvent(
                                timestamp=now,
                                event='hold_route',
                                site=site,
                                old_route=st.desired_route,
                                new_route=st.desired_route,
                                desired_route=st.desired_route,
                                active_route=st.active_route,
                                reason='both_backends_unready',
                                p90_ms=st.last_p90_ms,
                                samples=st.last_samples,
                                attempts=st.last_attempts,
                                error_rate=st.last_error_rate,
                                local_ready_endpoints=local_ready,
                                core_ready_endpoints=core_ready,
                            )
                        )
                continue

            # Collector fetch failure: hold last applied route and log status transition.
            if collector_error is not None or snapshot is None:
                self._clear_breach(st)
                self._clear_recovery(st)
                if st.last_status_note != 'collector_error':
                    st.last_status_note = 'collector_error'
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
                            extra={'collector_error': collector_error or 'missing_snapshot'},
                        )
                    )
                continue

            site_snap = (snapshot.get('sites') or {}).get(site) or {}
            user_buckets = site_snap.get('user') or {}

            if st.desired_route == 'LOCAL':
                self._clear_recovery(st)
                # Use ONLY this site's local user traffic bucket; never mix other sites or core RTT.
                # Unlabeled pre-header timeouts from user.unknown are attributed as a separate
                # failure signal ONLY when LOCAL is confirmed active (no pending route update)
                # and every timeout in the window occurred after local_confirmed_since.
                if local_ready > 0 and st.active_route == 'LOCAL':
                    healthy_rps = _healthy_user_rps(snapshot, user_buckets, 'local', st.local_confirmed_since, cfg)
                    if healthy_rps is not None and now - st.local_ready_since >= snapshot['window_seconds']:
                        st.healthy_local_rps[local_ready] = max(st.healthy_local_rps.get(local_ready, 0), healthy_rps)
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

                # Track breach duration only when fresh samples continue arriving.
                if not self._advance_breach(st, now, effective_seq):
                    # Same rolling window snapshot with no new samples; do NOT treat as new evidence.
                    st.last_status_note = 'breach_waiting_fresh_samples'
                    continue
                st.last_status_note = 'breach_pending'

                if (
                    (now - st.breach_started_at) >= cfg.breach_duration_seconds
                    and st.breach_fresh_updates >= cfg.min_fresh_updates
                    and self._dwell_satisfied(st, now)
                ):
                    if self._core_overloaded_elsewhere(st):
                        # Do not add load to a CORE pool that is already confirmed overloaded.
                        st.last_status_note = 'core_overloaded_hold_local'
                        if not st.core_gate_event_sent:
                            st.core_gate_event_sent = True
                            events.append(
                                DecisionEvent(
                                    timestamp=now,
                                    event='hold_route',
                                    site=site,
                                    old_route='LOCAL',
                                    new_route='LOCAL',
                                    desired_route='LOCAL',
                                    active_route=st.active_route,
                                    reason='core_overloaded_hold_local',
                                    p90_ms=p90_ms,
                                    samples=successes,
                                    attempts=effective_attempts,
                                    error_rate=effective_error_rate,
                                    local_ready_endpoints=local_ready,
                                    core_ready_endpoints=core_ready,
                                )
                            )
                        continue
                    if not self._try_start_route_switch(st, 'CORE', now):
                        st.last_status_note = 'route_switch_waiting'
                        continue
                    if unlabeled_timeout_breach and not p90_breach and not labeled_error_breach:
                        reason = 'local_unlabeled_timeout_breach'
                    elif error_breach and not p90_breach:
                        reason = 'local_error_rate_breach'
                    elif p90_breach and error_breach:
                        reason = 'local_p90_and_error_breach'
                    else:
                        reason = 'local_p90_breach'
                    st.offload_ready_count = local_ready
                    st.local_confirmed_since = None
                    st.last_decision_reason = reason
                    st.last_status_note = reason
                    self._clear_breach(st)
                    events.append(
                        DecisionEvent(
                            timestamp=now,
                            event='route_decision',
                            site=site,
                            old_route='LOCAL',
                            new_route='CORE',
                            desired_route='CORE',
                            active_route=st.active_route,
                            reason=reason,
                            p90_ms=p90_ms,
                            samples=successes,
                            attempts=effective_attempts,
                            error_rate=effective_error_rate,
                            local_ready_endpoints=local_ready,
                            core_ready_endpoints=core_ready,
                            extra={'unlabeled_timeouts': unlabeled_timeouts} if unlabeled_timeouts else {},
                        )
                    )
            else:
                # Local probes establish health; Core user traffic identifies overload or recovery.
                core_bucket = user_buckets.get('core') or {}
                self._update_core_overload(st, now, snapshot, core_bucket)
                bucket = (site_snap.get('probe') or {}).get('local') or {}
                p90_ms = bucket.get('p90_ms')
                successes = int(bucket.get('successes') or 0)
                attempts = int(bucket.get('attempts') or 0)
                error_rate = bucket.get('error_rate')
                seq = int(bucket.get('newest_sample_seq') or 0)
                st.last_p90_ms = p90_ms
                st.last_samples = successes
                st.last_attempts = attempts
                st.last_error_rate = error_rate

                if _bucket_stale(bucket, cfg.stale_after_seconds):
                    self._clear_recovery(st)
                    note = 'probe_empty' if bucket.get('status') == 'empty' else 'probe_stale'
                    if st.last_status_note != note:
                        st.last_status_note = note
                        events.append(
                            DecisionEvent(
                                timestamp=now,
                                event='hold_route',
                                site=site,
                                old_route='CORE',
                                new_route='CORE',
                                desired_route='CORE',
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

                probe_healthy = (
                    local_ready > 0
                    and successes >= cfg.probe_min_samples
                    and p90_ms is not None
                    and p90_ms <= cfg.recovery_p90_threshold_ms
                    and (error_rate is None or error_rate <= cfg.probe_max_error_rate)
                )

                if not probe_healthy:
                    self._clear_recovery(st)
                    self._note_hold(
                        events, st, site, 'core_probe_not_recovered',
                        local_ready=local_ready, core_ready=core_ready,
                        p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
                        extra={
                            'probe_p90_ms': p90_ms,
                            'probe_successes': successes,
                            'probe_error_rate': error_rate,
                            'core_user_p90_ms': core_bucket.get('p90_ms'),
                            'core_user_error_rate': core_bucket.get('error_rate'),
                            'core_overload_confirmed': st.core_overload_confirmed,
                            'offload_local_ready_endpoints': st.offload_ready_count,
                        },
                    )
                    continue

                reason = 'local_probe_recovered'
                evidence = {}
                if (
                    st.core_overload_confirmed
                    and st.offload_ready_count is not None
                    and local_ready > st.offload_ready_count
                ):
                    reason = 'core_overloaded_local_capacity'
                    evidence = {
                        'core_user_p90_ms': core_bucket.get('p90_ms'),
                        'core_user_error_rate': core_bucket.get('error_rate'),
                        'offload_local_ready_endpoints': st.offload_ready_count,
                    }
                elif st.offload_ready_count is not None:
                    core_rps = _healthy_user_rps(snapshot, user_buckets, 'core', st.core_confirmed_since, cfg)
                    local_limit = st.healthy_local_rps.get(local_ready)
                    # With newly added pods, allow a trial at the previously observed rate per pod.
                    if local_limit is None and local_ready > st.offload_ready_count and st.healthy_local_rps:
                        known_count = max(st.healthy_local_rps)
                        local_limit = st.healthy_local_rps[known_count] * local_ready / known_count
                    if core_rps is None or local_limit is None or core_rps > local_limit:
                        self._clear_recovery(st)
                        self._note_hold(
                            events, st, site, 'recovery_waiting_for_supported_traffic_rate',
                            local_ready=local_ready, core_ready=core_ready,
                            p90_ms=p90_ms, samples=successes, attempts=attempts, error_rate=error_rate,
                            extra={
                                'core_user_rps': core_rps,
                                'local_supported_rps': local_limit,
                                'offload_local_ready_endpoints': st.offload_ready_count,
                            },
                        )
                        continue

                if st.recovery_started_at is None or st.recovery_reason != reason:
                    st.recovery_started_at = now
                    st.recovery_reason = reason
                    st.recovery_last_seq = seq
                    st.recovery_fresh_updates = 1
                    st.last_status_note = 'recovery_pending'
                elif seq > st.recovery_last_seq:
                    st.recovery_last_seq = seq
                    st.recovery_fresh_updates += 1
                    st.last_status_note = 'recovery_pending'
                else:
                    st.last_status_note = 'recovery_waiting_fresh_probes'
                    continue

                if (
                    (now - st.recovery_started_at) >= cfg.recovery_stable_seconds
                    and st.recovery_fresh_updates >= cfg.min_fresh_updates
                    and self._dwell_satisfied(st, now)
                ):
                    if not self._try_start_route_switch(st, 'LOCAL', now):
                        st.last_status_note = 'route_switch_waiting'
                        continue
                    st.offload_ready_count = None
                    st.last_decision_reason = reason
                    st.last_status_note = reason
                    self._clear_breach(st)
                    self._clear_recovery(st)
                    events.append(
                        DecisionEvent(
                            timestamp=now,
                            event='route_decision',
                            site=site,
                            old_route='CORE',
                            new_route='LOCAL',
                            desired_route='LOCAL',
                            active_route=st.active_route,
                            reason=reason,
                            p90_ms=p90_ms,
                            samples=successes,
                            attempts=attempts,
                            error_rate=error_rate,
                            local_ready_endpoints=local_ready,
                            core_ready_endpoints=core_ready,
                            extra=evidence,
                        )
                    )

        return events
