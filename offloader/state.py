"""State, configuration, and event dataclasses for the vertical offloading controller."""
from dataclasses import dataclass, field

SITES = ('gnb-1', 'gnb-2', 'gnb-3')
ROUTES = ('LOCAL', 'SPREAD')
MODES = ('local-only', 'vertical')
DEFAULT_SITE_NODES = {'gnb-1': 'gnb1', 'gnb-2': 'gnb-2', 'gnb-3': 'gnb-3'}


class RouteSwitchBusy(RuntimeError):
    """Raised when a route or weight change cannot start yet due to global serialization."""


@dataclass(frozen=True)
class OffloadConfig:
    """Vertical offloading parameters loaded from offloader/config.yaml."""

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


@dataclass
class DecisionEvent:
    """Structured audit record emitted on route, weight, hold, ACK, or failure transitions."""

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

    def to_dict(self) -> dict:
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


@dataclass
class SiteState:
    """Mutable per-site offloading state tracked across 1 Hz evaluation ticks."""

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
        keep_local, keep_core = self.last_local_ready, self.last_core_ready
        self.__dict__.update(SiteState(site=self.site, active_route=active_route).__dict__)
        self.last_local_ready, self.last_core_ready = keep_local, keep_core
        self.last_status_note = self.last_decision_reason = 'reset'

    def make_event(
        self,
        *,
        timestamp: float,
        event: str,
        old_route: str,
        new_route: str,
        reason: str,
        desired_route: str | None = None,
        p90_ms=...,
        samples: int | None = None,
        attempts: int | None = None,
        error_rate=...,
        local_ready: int | None = None,
        core_ready: int | None = None,
        extra: dict | None = None,
    ) -> DecisionEvent:
        """Build a DecisionEvent defaulting telemetry fields from this site's latest state."""
        return DecisionEvent(
            timestamp=timestamp,
            event=event,
            site=self.site,
            old_route=old_route,
            new_route=new_route,
            desired_route=self.desired_route if desired_route is None else desired_route,
            active_route=self.active_route,
            reason=reason,
            p90_ms=self.last_p90_ms if p90_ms is ... else p90_ms,
            samples=self.last_samples if samples is None else samples,
            attempts=self.last_attempts if attempts is None else attempts,
            error_rate=self.last_error_rate if error_rate is ... else error_rate,
            local_ready_endpoints=self.last_local_ready if local_ready is None else local_ready,
            core_ready_endpoints=self.last_core_ready if core_ready is None else core_ready,
            extra=extra or {},
        )

    def to_dict(self) -> dict:
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
