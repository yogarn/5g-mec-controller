"""Envoy v3 RDS RouteConfiguration and EDS ClusterLoadAssignment resource builders."""

RDS_TYPE_URL = 'type.googleapis.com/envoy.config.route.v3.RouteConfiguration'
EDS_TYPE_URL = 'type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment'
CORE_EDS_SERVICE_NAME = 'core-5g'


def route_config_name(site: str) -> str:
    return f'route-{site}'


def local_eds_service_name(site: str) -> str:
    return f'local-{site}'


def spread_eds_service_name(site: str) -> str:
    return f'spread-{site}'


def _response_headers(site: str, route_label: str, version: str, traffic_class: str) -> list[dict]:
    pairs = (
        ('x-mec-site', site),
        ('x-mec-route', route_label),
        ('x-mec-route-version', version),
        ('x-mec-traffic-class', traffic_class),
    )
    return [{'header': {'key': k, 'value': v}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'} for k, v in pairs]


def build_route_configuration(site: str, route: str, version: str, *, route_timeout: str = '10s') -> dict:
    """Build an Envoy v3 RouteConfiguration for one site with explicit headers and zero retries."""
    if route.upper() == 'SPREAD':
        target_cluster, route_label = spread_eds_service_name(site), 'spread'
    else:
        target_cluster, route_label = 'local_backend', 'local'
    routes = [
        {
            'name': f'{site}-core-preflight',
            'match': {
                'path': '/ready',
                'headers': [{'name': 'x-mec-preflight', 'string_match': {'exact': 'core'}}],
            },
            'route': {'cluster': 'core_backend', 'timeout': route_timeout},
            'response_headers_to_add': _response_headers(site, 'core', version, 'probe'),
        },
        {
            'name': f'{site}-local-probe',
            'match': {
                'prefix': '/',
                'headers': [{'name': 'x-mec-probe', 'string_match': {'exact': 'local'}}],
            },
            'route': {'cluster': 'local_backend', 'timeout': route_timeout},
            'response_headers_to_add': _response_headers(site, 'local', version, 'probe'),
        },
        {
            'name': f'{site}-user-traffic',
            'match': {'prefix': '/'},
            'route': {'cluster': target_cluster, 'timeout': route_timeout},
            'response_headers_to_add': _response_headers(site, route_label, version, 'user'),
        },
    ]
    return {
        '@type': RDS_TYPE_URL,
        'name': route_config_name(site),
        'virtual_hosts': [{'name': f'{site}-inference', 'domains': ['*'], 'routes': routes}],
    }


def build_invalid_route_configuration(site: str) -> dict:
    """Deliberately invalid RouteConfiguration (duplicate virtual host) for NACK testing."""
    return {
        '@type': RDS_TYPE_URL,
        'name': route_config_name(site),
        'virtual_hosts': [
            {'name': 'dup', 'domains': ['*'], 'routes': [{'match': {'prefix': '/'}, 'route': {'cluster': 'local_backend'}}]},
            {'name': 'dup', 'domains': ['*'], 'routes': [{'match': {'prefix': '/'}, 'route': {'cluster': 'core_backend'}}]},
        ],
    }


def build_cluster_load_assignment(service_name: str, endpoints: list[tuple]) -> dict:
    """Build an Envoy v3 ClusterLoadAssignment pinned to explicit Pod IPs (optionally weighted)."""
    seen: set[tuple[str, int]] = set()
    lb_endpoints = []
    for item in sorted(endpoints):
        if len(item) == 3:
            ip, port, weight = item
            weight_int = int(weight)
            if weight_int < 1:
                raise ValueError(f'EDS load_balancing_weight must be >= 1, got {weight_int}')
        else:
            ip, port = item
            weight_int = None
        key = (str(ip), int(port))
        if key in seen:
            continue
        seen.add(key)
        entry = {'endpoint': {'address': {'socket_address': {'address': key[0], 'port_value': key[1]}}}}
        if weight_int is not None:
            entry['load_balancing_weight'] = weight_int
        lb_endpoints.append(entry)
    return {
        '@type': EDS_TYPE_URL,
        'cluster_name': service_name,
        'endpoints': [{'lb_endpoints': lb_endpoints}] if lb_endpoints else [],
    }
