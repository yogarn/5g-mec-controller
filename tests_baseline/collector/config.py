"""Configuration schema validation and YAML loading for the UE observation collector."""
import ipaddress
import math
from urllib.parse import urlsplit

import yaml

_NODE_NAME_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-'
VALID_SITES = ('gnb-1', 'gnb-2', 'gnb-3')
VALID_ROUTES = ('local', 'core', 'unknown')
VALID_TRAFFIC_CLASSES = ('user', 'probe')
DEFAULT_SITE_NODES = {'gnb-1': 'gnb1', 'gnb-2': 'gnb-2', 'gnb-3': 'gnb-3'}

DEFAULT_SETTINGS = {
    'placement_window_seconds': 5,
    'offload_stale_seconds': 5.0,
    'core_node': 'core-5g',
    'site_nodes': DEFAULT_SITE_NODES,
}

_POSITIVE_FLOAT_KEYS = (
    'window_seconds',
    'placement_window_seconds',
    'offload_window_seconds',
    'offload_stale_seconds',
    'mapping_refresh_seconds',
    'mapping_max_age_seconds',
    'slo_seconds',
)


def is_valid_node_name(value) -> bool:
    return isinstance(value, str) and bool(value) and all(c in _NODE_NAME_CHARS for c in value)


def _validate_upf_nodes(upf_nodes) -> dict[str, str]:
    if not isinstance(upf_nodes, dict):
        raise ValueError('upf_nodes must map UPF IP addresses to valid node names')
    normalized: dict[str, str] = {}
    for addr, node in upf_nodes.items():
        if not isinstance(addr, str):
            raise ValueError('upf_nodes keys must be IP address strings')
        try:
            ip = str(ipaddress.ip_address(addr))
        except ValueError as exc:
            raise ValueError(f'invalid UPF IP address in upf_nodes: {addr!r}') from exc
        if not is_valid_node_name(node):
            raise ValueError(f'invalid node name in upf_nodes: {node!r}')
        if ip in normalized:
            raise ValueError(f'duplicate UPF IP address in upf_nodes: {ip}')
        normalized[ip] = node
    return normalized


def _validate_urls(settings: dict):
    for key in ('smf_url', 'amf_url'):
        url = urlsplit(settings[key])
        if url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password or url.fragment:
            raise ValueError(f'invalid {key}')
        _ = url.port


def validate_settings(settings: dict) -> dict:
    """Apply defaults and validate all collector runtime settings."""
    if not isinstance(settings, dict):
        raise ValueError('settings must be a mapping')
    settings.setdefault('placement_window_seconds', DEFAULT_SETTINGS['placement_window_seconds'])
    settings.setdefault('offload_window_seconds', settings['window_seconds'])
    settings.setdefault('offload_stale_seconds', DEFAULT_SETTINGS['offload_stale_seconds'])
    settings.setdefault('core_node', DEFAULT_SETTINGS['core_node'])
    settings.setdefault('site_nodes', dict(DEFAULT_SETTINGS['site_nodes']))

    for key in _POSITIVE_FLOAT_KEYS:
        val = settings[key]
        if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(val) or val <= 0:
            raise ValueError(f'{key} must be finite and positive')
    if settings['placement_window_seconds'] > settings['window_seconds']:
        raise ValueError('placement_window_seconds must not exceed window_seconds')
    if settings['offload_window_seconds'] > settings['window_seconds']:
        raise ValueError('offload_window_seconds must not exceed window_seconds')
    if not is_valid_node_name(settings['core_node']):
        raise ValueError('core_node must be a valid node name')

    site_nodes = settings['site_nodes']
    if (not isinstance(site_nodes, dict) or set(site_nodes) != set(VALID_SITES)
            or any(not is_valid_node_name(v) for v in site_nodes.values())):
        raise ValueError('site_nodes must map gnb-1, gnb-2, gnb-3 to valid node names')
    max_obs = settings['max_observations']
    if isinstance(max_obs, bool) or not isinstance(max_obs, int) or max_obs <= 0:
        raise ValueError('max_observations must be a positive integer')
    nodes = settings['gnb_nodes']
    if (not isinstance(nodes, dict) or not nodes
            or any(isinstance(k, bool) or not isinstance(k, int) or k < 0 or not is_valid_node_name(v) for k, v in nodes.items())
            or len(set(nodes.values())) != len(nodes)):
        raise ValueError('gnb_nodes must contain unique valid node names and numeric IDs')

    settings['upf_nodes'] = _validate_upf_nodes(settings.get('upf_nodes', {}))
    _validate_urls(settings)
    return settings


def load_settings(path: str) -> dict:
    with open(path, encoding='utf-8') as stream:
        return validate_settings(yaml.safe_load(stream))
