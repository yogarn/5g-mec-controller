"""Thread-safe Envoy xDS snapshot store with epoch-scoped RDS/EDS versions and ACK/NACK tracking."""
import threading
import time
import uuid

from offloader.spread import spread_local_weight
from offloader.state import SITES
from offloader.xds_http import (
    make_admin_handler,
    make_xds_discovery_handler,
    start_admin_server,
    start_xds_server,
)
from offloader.xds_resources import (
    CORE_EDS_SERVICE_NAME,
    EDS_TYPE_URL,
    RDS_TYPE_URL,
    build_cluster_load_assignment,
    build_invalid_route_configuration,
    build_route_configuration,
    local_eds_service_name,
    route_config_name,
    spread_eds_service_name,
)


class XdsState:
    """Epoch-scoped RDS/EDS version store and ACK/NACK tracker for Envoy discovery."""

    def __init__(self, route_timeout: str = '10s', clock=time.time, epoch: str | None = None):
        self.lock = threading.Lock()
        self.route_timeout = route_timeout
        self.clock = clock
        # Unique epoch prevents stale versions from a pre-restart offloader from ACKing new routes.
        self.epoch = epoch or uuid.uuid4().hex[:8]
        self._site_seq = {site: 1 for site in SITES}
        self._site_routes = {site: 'LOCAL' for site in SITES}
        self._site_acked_version: dict[str, str | None] = {site: None for site in SITES}
        self._site_acked_route: dict[str, str | None] = {site: None for site in SITES}
        self._site_acked_at: dict[str, float | None] = {site: None for site in SITES}
        self._site_version_routes = {site: {self._ver(1): 'LOCAL'} for site in SITES}
        self._site_last_nack: dict[str, str | None] = {site: None for site in SITES}
        self._invalid_rds_version: dict[str, str | None] = {site: None for site in SITES}
        self._eds_seq = 1
        self._site_retreat_level = {site: 0 for site in SITES}
        self._spread_eds_version_levels = {site: {self._ver(1): 0} for site in SITES}
        self._spread_eds_acked_version: dict[str, str | None] = {site: None for site in SITES}
        self._spread_eds_last_nack: dict[str, str | None] = {site: None for site in SITES}
        self._local_endpoints: dict[str, list[tuple[str, int]]] = {site: [] for site in SITES}
        self._core_endpoints: list[tuple[str, int]] = []
        self._eds_endpoints = self._build_eds_map_locked()

    def _ver(self, seq: int) -> str:
        return f'{self.epoch}-{seq}'

    def _spread_count_locked(self) -> int:
        return sum(1 for r in self._site_routes.values() if r == 'SPREAD')

    def _build_eds_map_locked(self) -> dict[str, list[tuple]]:
        k = self._spread_count_locked()
        core_sorted = sorted(set(self._core_endpoints))
        eds_map: dict[str, list[tuple]] = {CORE_EDS_SERVICE_NAME: core_sorted}
        for site in SITES:
            local_sorted = sorted(set(self._local_endpoints.get(site, [])))
            eds_map[local_eds_service_name(site)] = local_sorted
            weight = spread_local_weight(len(core_sorted), len(local_sorted), k, self._site_retreat_level[site])
            local_set = set(local_sorted)
            spread_eps = [(ip, port, weight) for ip, port in local_sorted]
            spread_eps.extend((ip, port, 1) for ip, port in core_sorted if (ip, port) not in local_set)
            eds_map[spread_eds_service_name(site)] = sorted(spread_eps)
        return eds_map

    def _refresh_eds_locked(self):
        new_map = self._build_eds_map_locked()
        if new_map != self._eds_endpoints:
            self._eds_endpoints = new_map
            self._eds_seq += 1
            ver = self._ver(self._eds_seq)
            for site in SITES:
                self._spread_eds_version_levels[site][ver] = self._site_retreat_level[site]

    def set_site_route(self, site: str, route: str) -> str:
        route = route.upper()
        if site not in self._site_routes:
            raise ValueError(f'unknown site: {site}')
        with self.lock:
            if self._site_routes[site] != route:
                self._site_seq[site] += 1
                self._site_routes[site] = route
                if route == 'LOCAL':
                    self._site_retreat_level[site] = 0
                version = self._ver(self._site_seq[site])
                self._site_version_routes[site][version] = route
                self._refresh_eds_locked()
                return version
            return self._ver(self._site_seq[site])

    def set_site_retreat_level(self, site: str, level: int) -> str:
        if site not in self._site_retreat_level:
            raise ValueError(f'unknown site: {site}')
        level_int = max(0, int(level))
        with self.lock:
            if self._site_retreat_level[site] != level_int:
                self._site_retreat_level[site] = level_int
                self._refresh_eds_locked()
            return self._ver(self._eds_seq)

    def inject_invalid_rds_for_test(self, site: str, desired_route: str = 'SPREAD') -> str:
        """Test helper: bump version and serve an invalid RouteConfiguration so Envoy NACKs."""
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
                self._site_retreat_level[site] = 0
                version = self._ver(self._site_seq[site])
                self._site_version_routes[site][version] = 'LOCAL'
                self._site_last_nack[site] = self._spread_eds_last_nack[site] = self._invalid_rds_version[site] = None
                versions[site] = version
            self._refresh_eds_locked()
        return versions

    def update_endpoints(self, local_by_site: dict[str, list[tuple[str, int]]], core_endpoints: list[tuple[str, int]]):
        with self.lock:
            self._local_endpoints = {site: sorted(set(local_by_site.get(site, []))) for site in SITES}
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
        return next((site for site in SITES if node_id.endswith(site)), None)

    def _sites_from_eds_request(self, req: dict, requested: list[str]) -> list[str]:
        matched = [site for site in SITES if spread_eds_service_name(site) in requested]
        if matched:
            return matched
        node_id = ((req.get('node') or {}).get('id') or '').strip()
        return [site for site in SITES if node_id.endswith(site)][:1]

    def _rds_response_locked(self, site: str, client_version: str) -> tuple[int, dict | None]:
        current_version = self._ver(self._site_seq[site])
        if client_version == current_version:
            return 304, None
        resource = (
            build_invalid_route_configuration(site)
            if self._invalid_rds_version[site] == current_version
            else build_route_configuration(site, self._site_routes[site], current_version, route_timeout=self.route_timeout)
        )
        return 200, {
            'version_info': current_version, 'type_url': RDS_TYPE_URL,
            'nonce': f'{site}-{current_version}', 'resources': [resource],
        }

    def handle_rds(self, req: dict, *, on_ack=None, on_nack=None) -> tuple[int, dict | None]:
        site = self._site_from_rds_request(req)
        if site is None:
            return 404, {'error': 'unknown site in RDS DiscoveryRequest'}
        client_version = str(req.get('version_info') or '')
        error_detail = req.get('error_detail')
        now = self.clock()
        if error_detail:
            msg = (error_detail.get('message') if isinstance(error_detail, dict) else str(error_detail)) or 'envoy_nack'
            with self.lock:
                self._site_last_nack[site] = msg
            if on_nack is not None:
                on_nack(site, msg, now)
            return 304, None

        acked_route = None
        with self.lock:
            if client_version and client_version in self._site_version_routes[site]:
                prev_ack = self._site_acked_version[site]
                self._site_acked_version[site] = client_version
                self._site_acked_route[site] = self._site_version_routes[site][client_version]
                self._site_acked_at[site] = now
                self._site_last_nack[site] = None
                if prev_ack != client_version:
                    acked_route = self._site_acked_route[site]
            status, body = self._rds_response_locked(site, client_version)
        if acked_route is not None and on_ack is not None:
            on_ack(site, acked_route, client_version, now)
        return status, body

    def handle_eds(self, req: dict, *, on_ack=None, on_nack=None) -> tuple[int, dict | None]:
        client_version = str(req.get('version_info') or '')
        names = req.get('resource_names') or []
        error_detail = req.get('error_detail')
        now = self.clock()
        if error_detail:
            msg = (error_detail.get('message') if isinstance(error_detail, dict) else str(error_detail)) or 'envoy_eds_nack'
            nack_sites = self._sites_from_eds_request(req, names)
            with self.lock:
                for site in nack_sites:
                    self._spread_eds_last_nack[site] = msg
            if on_nack is not None:
                for site in nack_sites:
                    on_nack(site, msg, now)
            return 304, None

        acked: list[tuple[str, int, str]] = []
        with self.lock:
            current_version = self._ver(self._eds_seq)
            requested = names if names else list(self._eds_endpoints.keys())
            for site in SITES:
                if (spread_eds_service_name(site) in requested
                        and client_version and client_version in self._spread_eds_version_levels[site]
                        and self._spread_eds_acked_version[site] != client_version):
                    self._spread_eds_acked_version[site] = client_version
                    self._spread_eds_last_nack[site] = None
                    acked.append((site, self._spread_eds_version_levels[site][client_version], client_version))
            if client_version == current_version:
                status, body = 304, None
            else:
                resources = [build_cluster_load_assignment(n, self._eds_endpoints.get(n, [])) for n in requested]
                status, body = 200, {
                    'version_info': current_version, 'type_url': EDS_TYPE_URL,
                    'nonce': f'eds-{current_version}', 'resources': resources,
                }
        if acked and on_ack is not None:
            for site, level, ver in acked:
                on_ack(site, level, ver, now)
        return status, body

    def status_snapshot(self) -> dict:
        with self.lock:
            k, c = self._spread_count_locked(), len(self._core_endpoints)
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
                        'retreat_level': self._site_retreat_level[site],
                        'spread_local_weight': spread_local_weight(c, len(self._local_endpoints[site]), k, self._site_retreat_level[site]),
                        'spread_eds_acked_version': self._spread_eds_acked_version[site],
                        'spread_eds_last_nack': self._spread_eds_last_nack[site],
                    }
                    for site in SITES
                },
                'eds_version': self._ver(self._eds_seq),
                'spread_sites': k,
                'endpoints': {name: [f'{e[0]}:{e[1]}' for e in eps] for name, eps in self._eds_endpoints.items()},
            }
