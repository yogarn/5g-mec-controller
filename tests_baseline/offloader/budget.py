"""Single source of truth for vertical offloading replica budget (fixed + dynamic)."""
from dataclasses import dataclass


DEFAULT_MIN_REPLICAS = 6
DEFAULT_MAX_REPLICAS = 18
DEFAULT_EDGE_ANCHORS = 3
DEFAULT_CORE_FALLBACK_REPLICAS = 3


@dataclass(frozen=True)
class VerticalBudget:
    """Per-arm inference budget covering edge anchors, Core fallback and managed replicas."""
 
    min_replicas: int = DEFAULT_MIN_REPLICAS
    max_replicas: int = DEFAULT_MAX_REPLICAS
    edge_anchors: int = DEFAULT_EDGE_ANCHORS
    core_fallback_replicas: int = DEFAULT_CORE_FALLBACK_REPLICAS
    core_counts_toward_budget: bool = True

    def __post_init__(self):
        for field_name in ('min_replicas', 'max_replicas', 'edge_anchors', 'core_fallback_replicas'):
            val = getattr(self, field_name)
            if isinstance(val, bool) or not isinstance(val, int) or val < 0:
                raise ValueError(f'{field_name} must be a nonnegative integer')
        if not self.core_counts_toward_budget:
            raise ValueError('core_counts_toward_budget must be True under the unified vertical budget')
        if self.edge_anchors != 3:
            raise ValueError('vertical mode requires exactly 3 edge anchors (gnb1, gnb-2, gnb-3)')
        if self.min_replicas < self.fixed_replicas:
            raise ValueError(
                f'min_replicas ({self.min_replicas}) must be at least fixed_replicas ({self.fixed_replicas}: '
                f'{self.edge_anchors} edge anchors + {self.core_fallback_replicas} Core fallback)'
            )
        if self.max_replicas < self.min_replicas:
            raise ValueError(f'max_replicas ({self.max_replicas}) must be >= min_replicas ({self.min_replicas})')

    @property
    def fixed_replicas(self) -> int:
        return self.edge_anchors + self.core_fallback_replicas

    @property
    def dynamic_min(self) -> int:
        return self.min_replicas - self.fixed_replicas

    @property
    def dynamic_max(self) -> int:
        return self.max_replicas - self.fixed_replicas

    @property
    def edge_min_replicas(self) -> int:
        return self.edge_anchors + self.dynamic_min

    @property
    def edge_max_replicas(self) -> int:
        return self.edge_anchors + self.dynamic_max

    def for_arm_mode(self, arm_mode: str, *, fixed_total: bool = False) -> 'VerticalBudget':
        """Derive the actual arm budget preserving the same total replica ceiling."""
        if arm_mode == 'local-only':
            return VerticalBudget(
                min_replicas=self.min_replicas if fixed_total else self.edge_min_replicas,
                max_replicas=self.max_replicas,
                edge_anchors=self.edge_anchors,
                core_fallback_replicas=0,
                core_counts_toward_budget=self.core_counts_toward_budget,
            )
        if arm_mode == 'vertical':
            if self.core_fallback_replicas < 1:
                raise ValueError('vertical mode requires at least 1 Core fallback replica')
            return self
        raise ValueError(f'unsupported vertical arm mode: {arm_mode!r}')

    def dynamic_target_for_edge(self, edge_target: int) -> int:
        """Convert an edge replica target (anchors + dynamic, e.g. 3..15) to dynamic managed replicas (0..12)."""
        raw = int(edge_target) - self.edge_anchors
        return max(self.dynamic_min, min(self.dynamic_max, raw))

    def dynamic_target_for(self, total_target: int) -> int:
        """Convert a total replica target (e.g. 3..18 local-only or 6..18 vertical) to dynamic replicas (0..12)."""
        raw = int(total_target) - self.fixed_replicas
        return max(self.dynamic_min, min(self.dynamic_max, raw))

    def edge_for_dynamic(self, dynamic_replicas: int) -> int:
        return self.edge_anchors + max(0, int(dynamic_replicas))

    def total_for_dynamic(self, dynamic_replicas: int) -> int:
        return self.fixed_replicas + max(0, int(dynamic_replicas))

    def to_metadata(self) -> dict:
        return {
            'min_replicas': self.min_replicas,
            'max_replicas': self.max_replicas,
            'fixed_replicas': self.fixed_replicas,
            'edge_anchor_replicas': self.edge_anchors,
            'core_backend_replicas': self.core_fallback_replicas,
            'core_counts_toward_budget': self.core_counts_toward_budget,
            'edge_min_replicas': self.edge_min_replicas,
            'edge_max_replicas': self.edge_max_replicas,
            'dynamic_min_replicas': self.dynamic_min,
            'dynamic_max_replicas': self.dynamic_max,
            'initial_total_replicas': self.fixed_replicas + self.dynamic_min,
            'initial_edge_replicas': self.edge_anchors + self.dynamic_min,
            'initial_dynamic_replicas': self.dynamic_min,
        }
