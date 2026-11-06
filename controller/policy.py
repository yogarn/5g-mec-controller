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


def rank_nodes(nodes: list[Node], requests: dict[str, float], saturation: float, locality_weight: float) -> dict[str, float]:
    """Capacity filter then normalized post-placement headroom and traffic share."""
    if not nodes or not requests or any(not math.isfinite(v) or v < 0 for v in requests.values()):
        raise ValueError('missing or invalid node/resource data')
    if any(not math.isfinite(n.cpu_utilization) or n.cpu_utilization < 0 or not math.isfinite(n.arrival_rate) or n.arrival_rate < 0 for n in nodes):
        raise ValueError('invalid node telemetry')
    total = sum(n.arrival_rate for n in nodes)
    max_capacity = {k: max(n.capacity.get(k, 0) for n in nodes) for k in requests}
    scores = {}
    for node in nodes:
        if not node.ready or node.cpu_utilization >= saturation:
            continue
        if any(node.capacity.get(k, 0) - node.allocated.get(k, 0) < req for k, req in requests.items()):
            continue
        headroom = sum((node.capacity.get(k, 0) - node.allocated.get(k, 0) - requests[k]) / max_capacity[k]
                       for k in requests if max_capacity[k] > 0) / len(requests)
        locality = node.arrival_rate / total if total else 0
        scores[node.name] = (1 - locality_weight) * headroom + locality_weight * locality
    return scores
