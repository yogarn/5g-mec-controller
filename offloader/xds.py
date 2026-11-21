"""Envoy REST-JSON xDS v3 control plane (RDS + EDS) and authenticated admin HTTP API."""
import hmac
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from offloader.policy import RouteSwitchBusy, SITES, spread_local_weight


RDS_TYPE_URL = 'type.googleapis.com/envoy.config.route.v3.RouteConfiguration'
EDS_TYPE_URL = 'type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment'


def route_config_name(site: str) -> str:
    return f'route-{site}'


def local_eds_service_name(site: str) -> str:
    return f'local-{site}'


def spread_eds_service_name(site: str) -> str:
    return f'spread-{site}'


CORE_EDS_SERVICE_NAME = 'core-5g'


def build_route_configuration(site: str, route: str, version: str, *, route_timeout: str = '10s') -> dict:
    """Build an Envoy v3 RouteConfiguration for one site with explicit headers and no cross-backend retries."""
    route_upper = route.upper()
    if route_upper == 'SPREAD':
        target_cluster = spread_eds_service_name(site)
        route_label = 'spread'
    else:
        target_cluster = 'local_backend'
        route_label = 'local'
    return {
        '@type': RDS_TYPE_URL,
        'name': route_config_name(site),
        'virtual_hosts': [
            {
                'name': f'{site}-inference',
                'domains': ['*'],
                'routes': [
                    # Read-only Core preflight readiness check on /ready only (cannot steer /detect).
                    {
                        'name': f'{site}-core-preflight',
                        'match': {
                            'path': '/ready',
                            'headers': [
                                {'name': 'x-mec-preflight', 'string_match': {'exact': 'core'}},
                            ],
                        },
                        'route': {
                            'cluster': 'core_backend',
                            'timeout': route_timeout,
                        },
                        'response_headers_to_add': [
                            {'header': {'key': 'x-mec-site', 'value': site}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                            {'header': {'key': 'x-mec-route', 'value': 'core'}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                            {'header': {'key': 'x-mec-route-version', 'value': version}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                            {'header': {'key': 'x-mec-traffic-class', 'value': 'probe'}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                        ],
                    },
                    # Recovery probes are always forced to local_backend regardless of active user route.
                    {
                        'name': f'{site}-local-probe',
                        'match': {
                            'prefix': '/',
                            'headers': [
                                {'name': 'x-mec-probe', 'string_match': {'exact': 'local'}},
                            ],
                        },
                        'route': {
                            'cluster': 'local_backend',
                            'timeout': route_timeout,
                        },
                        'response_headers_to_add': [
                            {'header': {'key': 'x-mec-site', 'value': site}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                            {'header': {'key': 'x-mec-route', 'value': 'local'}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                            {'header': {'key': 'x-mec-route-version', 'value': version}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                            {'header': {'key': 'x-mec-traffic-class', 'value': 'probe'}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                        ],
                    },
                    # User inference traffic follows the site's active route with zero cross-backend retries.
                    {
                        'name': f'{site}-user-traffic',
                        'match': {'prefix': '/'},
                        'route': {
                            'cluster': target_cluster,
                            'timeout': route_timeout,
                        },
                        'response_headers_to_add': [
                            {'header': {'key': 'x-mec-site', 'value': site}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                            {'header': {'key': 'x-mec-route', 'value': route_label}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                            {'header': {'key': 'x-mec-route-version', 'value': version}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                            {'header': {'key': 'x-mec-traffic-class', 'value': 'user'}, 'append_action': 'OVERWRITE_IF_EXISTS_OR_ADD'},
                        ],
                    },
                ],
            }
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
        entry = {
            'endpoint': {
                'address': {
                    'socket_address': {
                        'address': key[0],
                        'port_value': key[1],
                    }
                }
            }
        }
        if weight_int is not None:
            entry['load_balancing_weight'] = weight_int
        lb_endpoints.append(entry)
    return {
        '@type': EDS_TYPE_URL,
        'cluster_name': service_name,
        'endpoints': [{'lb_endpoints': lb_endpoints}] if lb_endpoints else [],
    }


class XdsState:
    """Thread-safe xDS snapshot store with epoch-scoped RDS versions and ACK/NACK tracking."""

    def __init__(self, route_timeout: str = '10s', clock=time.time, epoch: str | None = None):
        self.lock = threading.Lock()
        self.route_timeout = route_timeout
        self.clock = clock
        # Unique generation epoch prevents stale versions from a pre-restart offloader
        # from ever being accepted as ACKs for the new instance's desired routes.
        self.epoch = epoch or uuid.uuid4().hex[:8]
        self._site_seq: dict[str, int] = {site: 1 for site in SITES}
        self._site_routes: dict[str, str] = {site: 'LOCAL' for site in SITES}
        self._site_acked_version: dict[str, str | None] = {site: None for site in SITES}
        self._site_acked_route: dict[str, str | None] = {site: None for site in SITES}
        self._site_acked_at: dict[str, float | None] = {site: None for site in SITES}
        self._site_version_routes: dict[str, dict[str, str]] = {
            site: {self._ver(1): 'LOCAL'} for site in SITES
        }
        self._site_last_nack: dict[str, str | None] = {site: None for site in SITES}
        self._invalid_rds_version: dict[str, str | None] = {site: None for site in SITES}
        self._eds_seq: int = 1
        self._local_endpoints: dict[str, list[tuple[str, int]]] = {site: [] for site in SITES}
        self._core_endpoints: list[tuple[str, int]] = []
        self._eds_endpoints: dict[str, list[tuple]] = self._build_eds_map_locked()

    def _ver(self, seq: int) -> str:
        return f'{self.epoch}-{seq}'

    def _spread_count_locked(self) -> int:
        return sum(1 for r in self._site_routes.values() if r == 'SPREAD')

    def _build_eds_map_locked(self) -> dict[str, list[tuple]]:
        local_weight = spread_local_weight(self._spread_count_locked())
        eds_map: dict[str, list[tuple]] = {}
        core_sorted = sorted(set(self._core_endpoints))
        eds_map[CORE_EDS_SERVICE_NAME] = core_sorted
        for site in SITES:
            local_sorted = sorted(set(self._local_endpoints.get(site, [])))
            eds_map[local_eds_service_name(site)] = local_sorted
            local_set = set(local_sorted)
            spread_eps: list[tuple[str, int, int]] = [
                (ip, port, local_weight) for ip, port in local_sorted
            ]
            for ip, port in core_sorted:
                if (ip, port) not in local_set:
                    spread_eps.append((ip, port, 1))
            eds_map[spread_eds_service_name(site)] = sorted(spread_eps)
        return eds_map

    def _refresh_eds_locked(self):
        new_map = self._build_eds_map_locked()
        if new_map != self._eds_endpoints:
            self._eds_endpoints = new_map
            self._eds_seq += 1

    def set_site_route(self, site: str, route: str) -> str:
        route = route.upper()
        if site not in self._site_routes:
            raise ValueError(f'unknown site: {site}')
        with self.lock:
            if self._site_routes[site] != route:
                self._site_seq[site] += 1
                self._site_routes[site] = route
                version = self._ver(self._site_seq[site])
                self._site_version_routes[site][version] = route
                # Recompute weighted spread-gnb-N endpoints when k changes.
                self._refresh_eds_locked()
                return version
            return self._ver(self._site_seq[site])

    def inject_invalid_rds_for_test(self, site: str, desired_route: str = 'SPREAD') -> str:
        """Test-only helper: bump site version and serve an invalid RouteConfiguration so Envoy NACKs."""
        if site not in self._site_routes:
            raise ValueError(f'unknown site: {site}')
        with self.lock:
            self._site_seq[site] += 1
            self._site_routes[site] = desired_route.upper()
            version = self._ver(self._site_seq[site])
            self._site_version_routes[site][version] = desired_route.upper()
            self._invalid_rds_version[site] = version
            self._refresh_eds_locked()
            return version

    def reset_routes(self) -> dict[str, str]:
        versions = {}
        with self.lock:
            for site in SITES:
                self._site_seq[site] += 1
                self._site_routes[site] = 'LOCAL'
                version = self._ver(self._site_seq[site])
                self._site_version_routes[site][version] = 'LOCAL'
                self._site_last_nack[site] = None
                self._invalid_rds_version[site] = None
                versions[site] = version
            self._refresh_eds_locked()
        return versions

    def update_endpoints(self, local_by_site: dict[str, list[tuple[str, int]]], core_endpoints: list[tuple[str, int]]):
        with self.lock:
            self._local_endpoints = {
                site: sorted(set(local_by_site.get(site, [])))
                for site in SITES
            }
            self._core_endpoints = sorted(set(core_endpoints))
            self._refresh_eds_locked()

    def get_endpoints_snapshot(self) -> dict[str, list[tuple[str, int]]]:
        with self.lock:
            return {k: [(e[0], e[1]) for e in v] for k, v in self._eds_endpoints.items()}

    def _site_from_rds_request(self, req: dict) -> str | None:
        names = req.get('resource_names') or []
        for site in SITES:
            if route_config_name(site) in names:
                return site
        node_id = ((req.get('node') or {}).get('id') or '').strip()
        for site in SITES:
            if node_id.endswith(site):
                return site
        return None

    def handle_rds(self, req: dict, *, on_ack=None, on_nack=None) -> tuple[int, dict | None]:
        site = self._site_from_rds_request(req)
        if site is None:
            return 404, {'error': 'unknown site in RDS DiscoveryRequest'}
        client_version = str(req.get('version_info') or '')
        error_detail = req.get('error_detail')
        now = self.clock()
        if error_detail:
            msg = error_detail.get('message') if isinstance(error_detail, dict) else str(error_detail)
            with self.lock:
                self._site_last_nack[site] = msg or 'envoy_nack'
            if on_nack is not None:
                on_nack(site, msg or 'envoy_nack', now)
            return 304, None

        acked_route = None
        with self.lock:
            current_version = self._ver(self._site_seq[site])
            current_route = self._site_routes[site]
            # Only versions issued by THIS offloader epoch are valid ACKs.
            if client_version and client_version in self._site_version_routes[site]:
                prev_ack = self._site_acked_version[site]
                self._site_acked_version[site] = client_version
                self._site_acked_route[site] = self._site_version_routes[site][client_version]
                self._site_acked_at[site] = now
                self._site_last_nack[site] = None
                if prev_ack != client_version:
                    acked_route = self._site_acked_route[site]
            if client_version == current_version:
                body = None
                status = 304
            elif self._invalid_rds_version[site] == current_version:
                # Deliberately invalid RouteConfiguration (duplicate virtual host name) for NACK testing.
                body = {
                    'version_info': current_version,
                    'type_url': RDS_TYPE_URL,
                    'nonce': f'{site}-{current_version}',
                    'resources': [
                        {
                            '@type': RDS_TYPE_URL,
                            'name': route_config_name(site),
                            'virtual_hosts': [
                                {'name': 'dup', 'domains': ['*'], 'routes': [{'match': {'prefix': '/'}, 'route': {'cluster': 'local_backend'}}]},
                                {'name': 'dup', 'domains': ['*'], 'routes': [{'match': {'prefix': '/'}, 'route': {'cluster': 'core_backend'}}]},
                            ],
                        }
                    ],
                }
                status = 200
            else:
                body = {
                    'version_info': current_version,
                    'type_url': RDS_TYPE_URL,
                    'nonce': f'{site}-{current_version}',
                    'resources': [
                        build_route_configuration(
                            site,
                            current_route,
                            current_version,
                            route_timeout=self.route_timeout,
                        )
                    ],
                }
                status = 200
        if acked_route is not None and on_ack is not None:
            on_ack(site, acked_route, client_version, now)
        return status, body

    def handle_eds(self, req: dict) -> tuple[int, dict | None]:
        client_version = str(req.get('version_info') or '')
        names = req.get('resource_names') or []
        with self.lock:
            current_version = self._ver(self._eds_seq)
            if client_version == current_version:
                return 304, None
            requested = names if names else list(self._eds_endpoints.keys())
            resources = [
                build_cluster_load_assignment(name, self._eds_endpoints.get(name, []))
                for name in requested
            ]
        return 200, {
            'version_info': current_version,
            'type_url': EDS_TYPE_URL,
            'nonce': f'eds-{current_version}',
            'resources': resources,
        }

    def status_snapshot(self) -> dict:
        with self.lock:
            k = self._spread_count_locked()
            return {
                'epoch': self.epoch,
                'sites': {
                    site: {
                        'desired_route': self._site_routes[site],
                        'desired_version': self._ver(self._site_seq[site]),
                        'acked_version': self._site_acked_version[site],
                        'acked_route': self._site_acked_route[site],
                        'acked_at': self._site_acked_at[site],
                        'last_nack': self._site_last_nack[site],
                    }
                    for site in SITES
                },
                'eds_version': self._ver(self._eds_seq),
                'spread_sites': k,
                'spread_local_weight': spread_local_weight(k),
                'endpoints': {k_name: [f'{e[0]}:{e[1]}' for e in v] for k_name, v in self._eds_endpoints.items()},
            }


def _check_bearer_auth(headers, expected_token: str) -> bool:
    if not expected_token:
        return False
    auth = headers.get('Authorization', '')
    return hmac.compare_digest(auth.encode('utf-8'), f'Bearer {expected_token}'.encode('utf-8'))


def make_xds_discovery_handler(controller):
    """HTTP handler exposing ONLY Envoy xDS discovery endpoints (no admin/mutation surface)."""

    class Handler(BaseHTTPRequestHandler):
        def _reply_json(self, status: int, payload: dict | None):
            if status == 304 or payload is None:
                self.send_response(status)
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            body = (json.dumps(payload, sort_keys=True) + '\n').encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            length = int(self.headers.get('Content-Length', '0') or '0')
            if length == 0:
                return {}
            if length < 0 or length > 1024 * 1024:
                raise ValueError('invalid Content-Length')
            raw = self.rfile.read(length)
            return json.loads(raw.decode('utf-8'))

        def do_GET(self):
            if self.path == '/health':
                self._reply_json(200, {'status': 'ok'})
            else:
                self._reply_json(404, {'error': 'not found'})

        def do_POST(self):
            try:
                body = self._read_json()
            except (ValueError, UnicodeError) as exc:
                self._reply_json(400, {'error': str(exc)})
                return
            if self.path == '/v3/discovery:routes':
                status, payload = controller.handle_rds(body)
                self._reply_json(status, payload)
            elif self.path == '/v3/discovery:endpoints':
                status, payload = controller.handle_eds(body)
                self._reply_json(status, payload)
            else:
                self._reply_json(404, {'error': 'not found'})

        def log_message(self, format, *args):
            return

    return Handler


def make_admin_handler(controller, admin_token: str):
    """HTTP handler exposing authenticated admin/control endpoints separated from xDS discovery."""
    if not isinstance(admin_token, str) or not admin_token.strip() or '\n' in admin_token or '\r' in admin_token:
        raise ValueError('admin_token must be a nonempty single-line string')

    class Handler(BaseHTTPRequestHandler):
        def _reply_json(self, status: int, payload: dict | None):
            if payload is None:
                self.send_response(status)
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            body = (json.dumps(payload, sort_keys=True) + '\n').encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            length = int(self.headers.get('Content-Length', '0') or '0')
            if length == 0:
                return {}
            if length < 0 or length > 1024 * 1024:
                raise ValueError('invalid Content-Length')
            raw = self.rfile.read(length)
            return json.loads(raw.decode('utf-8'))

        def _require_auth(self) -> bool:
            if not _check_bearer_auth(self.headers, admin_token):
                self._reply_json(401, {'error': 'unauthorized'})
                return False
            return True

        def do_GET(self):
            if self.path == '/health':
                self._reply_json(200, {'status': 'ok'})
                return
            if not self._require_auth():
                return
            if self.path == '/status':
                self._reply_json(200, controller.get_status())
            elif self.path == '/events':
                self._reply_json(200, {'events': controller.get_recent_events()})
            else:
                self._reply_json(404, {'error': 'not found'})

        def do_POST(self):
            if not self._require_auth():
                return
            try:
                body = self._read_json()
            except (ValueError, UnicodeError) as exc:
                self._reply_json(400, {'error': str(exc)})
                return

            if self.path == '/override':
                try:
                    site = body['site']
                    route = body.get('route')
                    result = controller.manual_override(site, route)
                except PermissionError as exc:
                    self._reply_json(403, {'error': str(exc)})
                    return
                except RouteSwitchBusy as exc:
                    self._reply_json(429, {'error': str(exc)})
                    return
                except (KeyError, ValueError) as exc:
                    self._reply_json(400, {'error': str(exc)})
                    return
                self._reply_json(200, result)
            elif self.path == '/reset':
                mode = body.get('mode')
                try:
                    result = controller.reset(mode=mode)
                except ValueError as exc:
                    self._reply_json(400, {'error': str(exc)})
                    return
                self._reply_json(200, result)
            elif self.path == '/mode':
                try:
                    result = controller.set_mode(body['mode'])
                except (KeyError, ValueError) as exc:
                    self._reply_json(400, {'error': str(exc)})
                    return
                self._reply_json(200, result)
            else:
                self._reply_json(404, {'error': 'not found'})

        def log_message(self, format, *args):
            return

    return Handler


def start_xds_server(controller, host: str = '0.0.0.0', port: int = 8085) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, int(port)), make_xds_discovery_handler(controller))
    thread = threading.Thread(target=server.serve_forever, daemon=True, name='offloader-xds')
    thread.start()
    return server


def start_admin_server(controller, admin_token: str, host: str = '0.0.0.0', port: int = 8086) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, int(port)), make_admin_handler(controller, admin_token))
    thread = threading.Thread(target=server.serve_forever, daemon=True, name='offloader-admin')
    thread.start()
    return server
