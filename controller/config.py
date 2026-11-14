"""Strict configuration and read-only workload identification."""
from dataclasses import dataclass
from pathlib import Path
import math
import yaml
from controller.policy import DEFAULT_RESOURCE_WEIGHTS, validate_resource_weights


@dataclass(frozen=True)
class Settings:
    manifest: Path
    namespace: str | None
    prometheus_url: str
    latency_query: str
    slo_seconds: float
    min_replicas: int
    max_replicas: int
    interval_seconds: float
    mode: str
    mutate: bool
    eligible_nodes: tuple[str, ...]
    cpu_utilization_query: str | None
    mobility_query: str | None
    cpu_saturation: float
    locality_weight: float
    cooldown_seconds: float
    max_metric_age_seconds: float
    scale_down_seconds: float
    resource_weights: dict[str, float]
    gpu_nodes: tuple[str, ...]
    gpu_utilization_query: str | None
    vram_utilization_query: str | None
    zero_locality_nodes: tuple[str, ...]


def load_settings(path: str) -> Settings:
    config_path = Path(path).resolve()
    with config_path.open() as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError('configuration must be a mapping')
    allowed = {'manifest', 'namespace', 'prometheus_url', 'latency_query', 'slo_seconds',
               'min_replicas', 'max_replicas', 'interval_seconds', 'mode', 'mutate',
               'eligible_nodes', 'cpu_utilization_query', 'mobility_query', 'cpu_saturation',
               'locality_weight', 'cooldown_seconds', 'max_metric_age_seconds',
               'scale_down_seconds', 'resource_weights', 'gpu_nodes',
               'gpu_utilization_query', 'vram_utilization_query', 'zero_locality_nodes'}
    if set(data) - allowed:
        raise ValueError(f'unknown configuration keys: {sorted(set(data) - allowed)}')
    for key in ('manifest', 'prometheus_url', 'latency_query', 'slo_seconds', 'min_replicas', 'max_replicas', 'mode'):
        if key not in data:
            raise ValueError(f'missing configuration: {key}')
    for key in ('manifest', 'prometheus_url', 'latency_query'):
        if not isinstance(data[key], str) or not data[key].strip():
            raise ValueError(f'{key} must be a nonempty string')
    mode = data['mode']
    if mode not in ('scaling-only', 'mobility-aware'):
        raise ValueError('mode must be scaling-only or mobility-aware')
    if type(data.get('mutate', False)) is not bool:
        raise ValueError('mutate must be a boolean')
    for key in ('min_replicas', 'max_replicas'):
        if type(data[key]) is not int or data[key] < 1:
            raise ValueError(f'{key} must be a positive integer')
    if data['min_replicas'] > data['max_replicas']:
        raise ValueError('min_replicas exceeds max_replicas')
    nodes = data.get('eligible_nodes', [])
    if not isinstance(nodes, list) or any(not isinstance(n, str) or not n for n in nodes) or len(set(nodes)) != len(nodes):
        raise ValueError('eligible_nodes must be a list of unique node names')
    zero_locality_nodes = data.get('zero_locality_nodes', [])
    if (not isinstance(zero_locality_nodes, list) or
            any(not isinstance(n, str) or not n.strip() for n in zero_locality_nodes) or
            len(set(zero_locality_nodes)) != len(zero_locality_nodes) or
            not set(zero_locality_nodes).issubset(nodes)):
        raise ValueError('zero_locality_nodes must be a unique subset of eligible_nodes')
    if nodes and len(zero_locality_nodes) == len(nodes):
        raise ValueError('at least one eligible node must require locality telemetry')
    if mode == 'mobility-aware' and 'gpu_nodes' not in data:
        raise ValueError('mobility-aware mode requires an explicit gpu_nodes inventory')
    gpu_nodes = data.get('gpu_nodes', [])
    if (not isinstance(gpu_nodes, list) or
            any(not isinstance(n, str) or not n.strip() for n in gpu_nodes) or
            len(set(gpu_nodes)) != len(gpu_nodes) or not set(gpu_nodes).issubset(nodes)):
        raise ValueError('gpu_nodes must be a unique subset of eligible_nodes')
    for key in ('gpu_utilization_query', 'vram_utilization_query'):
        query = data.get(key)
        if (query is not None and (not isinstance(query, str) or not query.strip())) or (
                mode == 'mobility-aware' and gpu_nodes and query is None):
            raise ValueError(f'{key} must be a nonempty query when GPU nodes are configured')
    resource_weights = validate_resource_weights(data.get('resource_weights', DEFAULT_RESOURCE_WEIGHTS))
    if mode == 'mobility-aware':
        if not nodes or any(not isinstance(data.get(k), str) or not data[k].strip() for k in ('cpu_utilization_query', 'mobility_query')):
            raise ValueError('mobility-aware mode requires eligible_nodes, cpu_utilization_query and mobility_query')
    def number(key, default, low, high=None):
        value = data.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= low or (high is not None and value > high):
            raise ValueError(f'invalid {key}')
        return float(value)
    namespace = data.get('namespace')
    if namespace is not None and (not isinstance(namespace, str) or not namespace):
        raise ValueError('namespace must be a nonempty string')
    manifest = Path(data['manifest']).expanduser()
    if not manifest.is_absolute():
        manifest = config_path.parent / manifest
    locality_weight = data.get('locality_weight', .4)
    if isinstance(locality_weight, bool) or not isinstance(locality_weight, (int, float)) or not math.isfinite(locality_weight) or not 0 <= locality_weight <= 1:
        raise ValueError('invalid locality_weight')
    return Settings(manifest, namespace, data['prometheus_url'].rstrip('/'), data['latency_query'],
                    number('slo_seconds', None, 0), data['min_replicas'], data['max_replicas'],
                    number('interval_seconds', 30, 0), mode, data.get('mutate', False), tuple(nodes),
                    data.get('cpu_utilization_query'), data.get('mobility_query'),
                    number('cpu_saturation', .8, 0, 1), float(locality_weight),
                    number('cooldown_seconds', 120, 0),
                    number('max_metric_age_seconds', 120, 0),
                    number('scale_down_seconds', 180, 0), resource_weights, tuple(gpu_nodes),
                    data.get('gpu_utilization_query'), data.get('vram_utilization_query'),
                    tuple(zero_locality_nodes))


def workload_identity(path: Path, namespace: str | None) -> tuple[str, str]:
    with path.open() as f:
        documents = list(yaml.safe_load_all(f))
    deployments = [d for d in documents if isinstance(d, dict) and d.get('apiVersion') == 'apps/v1' and d.get('kind') == 'Deployment']
    if len(deployments) != 1:
        raise ValueError('manifest must contain exactly one apps/v1 Deployment')
    metadata = deployments[0].get('metadata') or {}
    name, declared = metadata.get('name'), metadata.get('namespace')
    if not isinstance(name, str) or not name or (declared is not None and (not isinstance(declared, str) or not declared)):
        raise ValueError('Deployment requires metadata.name and a valid namespace')
    if namespace and declared and namespace != declared:
        raise ValueError('configured namespace conflicts with manifest')
    if not (namespace or declared):
        raise ValueError('namespace must be provided in the Deployment or configuration')
    return namespace or declared, name
