"""Configuration loading and defaults for the UE observation collector."""
import ipaddress

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


def is_valid_node_name(value) -> bool:
    return isinstance(value, str) and bool(value) and all(c in _NODE_NAME_CHARS for c in value)


def _normalize_upf_nodes(upf_nodes) -> dict[str, str]:
    if not isinstance(upf_nodes, dict):
        raise ValueError('upf_nodes must map UPF IP addresses to valid node names')
    normalized: dict[str, str] = {}
    for addr, node in upf_nodes.items():
        if not isinstance(addr, str):
            raise ValueError('upf_nodes keys must be IP address strings')
        ip = str(ipaddress.ip_address(addr))
        if not is_valid_node_name(node):
            raise ValueError(f'invalid node name in upf_nodes: {node!r}')
        if ip in normalized:
            raise ValueError(f'duplicate UPF IP address in upf_nodes: {ip}')
        normalized[ip] = node
    return normalized


def validate_settings(settings: dict) -> dict:
    """Apply defaults and normalize UPF IP keys for repo-authored collector config."""
    settings.setdefault('placement_window_seconds', DEFAULT_SETTINGS['placement_window_seconds'])
    settings.setdefault('offload_window_seconds', settings['window_seconds'])
    settings.setdefault('offload_stale_seconds', DEFAULT_SETTINGS['offload_stale_seconds'])
    settings.setdefault('core_node', DEFAULT_SETTINGS['core_node'])
    settings.setdefault('site_nodes', dict(DEFAULT_SETTINGS['site_nodes']))
    if settings['placement_window_seconds'] > settings['window_seconds']:
        raise ValueError('placement_window_seconds must not exceed window_seconds')
    if settings['offload_window_seconds'] > settings['window_seconds']:
        raise ValueError('offload_window_seconds must not exceed window_seconds')
    settings['upf_nodes'] = _normalize_upf_nodes(settings.get('upf_nodes', {}))
    return settings


def load_settings(path: str) -> dict:
    with open(path, encoding='utf-8') as stream:
        return validate_settings(yaml.safe_load(stream))
