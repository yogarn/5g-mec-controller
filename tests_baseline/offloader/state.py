"""State, configuration, and event dataclasses for the vertical offloading controller.

Defines the validated OffloadConfig, mutable per-site SiteState, RouteSwitchBusy
exception, and structured DecisionEvent records emitted by VerticalOffloadPolicy.
"""
from dataclasses import dataclass, field
import math

SITES = ('gnb-1', 'gnb-2', 'gnb-3')
ROUTES = ('LOCAL', 'SPREAD')
MODES = ('local-only', 'vertical')

DEFAULT_SITE_NODES = {
    'gnb-1': 'gnb1',
    'gnb-2': 'gnb-2',
    'gnb-3': 'gnb-3',
}


class RouteSwitchBusy(RuntimeError):
    """The offloader cannot start another route or weight change yet."""


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
