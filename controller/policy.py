"""Pure scaling and placement decisions, independent of Kubernetes I/O."""
import math
from dataclasses import dataclass


@dataclass(frozen=True)
class Node:
    name: str
    capacity: dict[str, float]
    allocated: dict[str, float]
    cpu_utilization: float
    arrival_rate: float
    ready: bool = True
    gpu_free: float = 0.0
    vram_free: float = 0.0


class LatencyRatioPolicy:
    """Scale by the latency/SLO ratio, stabilizing sustained scale-down."""

    def __init__(self, down_seconds):
        self.down_seconds = down_seconds
        self.down_since = None

    def target(self, replicas, ready, latency, slo, minimum, maximum, now):
        if (not all(math.isfinite(v) for v in (latency, slo, now)) or
                latency <= 0 or slo <= 0 or replicas < 1 or
                ready < 0 or ready > replicas):
            self.down_since = None
            raise ValueError('invalid scaling input')
        if ready != replicas:
            self.down_since = None
            return replicas
        ratio = latency / slo
        desired = maximum if ratio >= maximum / replicas else max(
            minimum, math.ceil(ratio * replicas))
        if desired >= replicas:
            self.down_since = None
            return desired
        if self.down_since is None:
            self.down_since = now
        if now - self.down_since < self.down_seconds:
            return replicas
        return desired

    def action_applied(self, now):
        self.down_since = None


DEFAULT_RESOURCE_WEIGHTS = {'cpu': .4, 'memory': .4, 'gpu': .1, 'vram': .1}


def validate_resource_weights(weights):
    if (not isinstance(weights, dict) or set(weights) != set(DEFAULT_RESOURCE_WEIGHTS) or
            any(isinstance(v, bool) or not isinstance(v, (int, float)) or
                not math.isfinite(v) or v < 0 for v in weights.values()) or
            not math.isclose(sum(weights.values()), 1.0, rel_tol=0, abs_tol=1e-9)):
        raise ValueError('resource_weights must contain nonnegative finite cpu, memory, gpu and vram weights summing to 1')
    return {key: float(value) for key, value in weights.items()}


def rank_nodes(nodes: list[Node], requests: dict[str, float], saturation: float,
               locality_weight: float, resource_weights: dict[str, float]) -> dict[str, float]:
    """Filter absolute requests, then score relative headroom and traffic share."""
    weights = validate_resource_weights(resource_weights)
    if (not nodes or not requests or any(not math.isfinite(v) or v < 0 for v in requests.values()) or
            not math.isfinite(saturation) or not 0 < saturation <= 1 or
            not math.isfinite(locality_weight) or not 0 <= locality_weight <= 1):
        raise ValueError('missing or invalid node/resource data')
    for node in nodes:
        if (not math.isfinite(node.cpu_utilization) or not 0 <= node.cpu_utilization <= 1 or
                not math.isfinite(node.arrival_rate) or node.arrival_rate < 0 or
                any(not math.isfinite(v) or not 0 <= v <= 1 for v in (node.gpu_free, node.vram_free)) or
                any(not math.isfinite(v) or v < 0 for v in (*node.capacity.values(), *node.allocated.values()))):
            raise ValueError('invalid node telemetry or resources')
    total = sum(n.arrival_rate for n in nodes)
    if not math.isfinite(total):
        raise ValueError('invalid total arrival rate')
    scores = {}
    for node in nodes:
        if not node.ready or node.cpu_utilization >= saturation:
            continue
        if any(node.capacity.get(k, 0) - node.allocated.get(k, 0) < req for k, req in requests.items()):
            continue
        headroom = {}
        for key in ('cpu', 'memory'):
            capacity = node.capacity.get(key, 0)
            headroom[key] = ((capacity - node.allocated.get(key, 0) - requests.get(key, 0)) / capacity
                             if capacity else 0)
            headroom[key] = max(0, headroom[key])
        resource = (weights['cpu'] * headroom['cpu'] + weights['memory'] * headroom['memory'] +
                    weights['gpu'] * node.gpu_free + weights['vram'] * node.vram_free)
        locality = node.arrival_rate / total if total else 0
        scores[node.name] = (1 - locality_weight) * resource + locality_weight * locality
    return scores
