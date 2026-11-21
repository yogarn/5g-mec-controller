"""Core-side vertical offloading controller with 1-second collector polling and Envoy xDS."""
import argparse
from collections import deque
import json
import os
from pathlib import Path
import signal
import threading
import time
from urllib.request import urlopen

import yaml

from offloader.k8s import KubernetesEndpointDiscoverer
from offloader.policy import DEFAULT_SITE_NODES, OffloadConfig, SITES, VerticalOffloadPolicy
from offloader.probe import LocalProbeWorker
from offloader.xds import (
    CORE_EDS_SERVICE_NAME,
    XdsState,
    local_eds_service_name,
    start_admin_server,
    start_xds_server,
)


def fetch_collector_snapshot(url: str, timeout: float = 1.0) -> dict:
    with urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


class VerticalOffloadController:
    """Coordinates 1 Hz collector polling, per-site policy, Envoy xDS, and audit logging."""

    def __init__(
        self,
        config: OffloadConfig,
        *,
        collector_snapshot_url: str,
        discoverer=None,
        snapshot_fetcher=fetch_collector_snapshot,
        events_log_path: str | None = None,
        clock=time.monotonic,
        wall_clock=time.time,
        route_timeout: str = '10s',
        epoch: str | None = None,
    ):
        self.policy = VerticalOffloadPolicy(config)
        # Start active_route as UNKNOWN until Envoy ACKs a version from this offloader epoch.
        self.policy.reset(active_route='UNKNOWN')
        self.xds = XdsState(route_timeout=route_timeout, clock=wall_clock, epoch=epoch)
        self.collector_snapshot_url = collector_snapshot_url
        self.discoverer = discoverer
        self.snapshot_fetcher = snapshot_fetcher
        self.events_log_path = Path(events_log_path) if events_log_path else None
        self.clock = clock
        self.wall_clock = wall_clock
        self.lock = threading.Lock()
        self.recent_events: deque[dict] = deque(maxlen=1000)
        self._last_discovery_error: str | None = None

    def _emit_events(self, events):
        if not events:
            return
        for ev in events:
            record = ev.to_dict()
            with self.lock:
                self.recent_events.append(record)
            line = json.dumps(record, sort_keys=True)
            print(line, flush=True)
            if self.events_log_path is not None:
                self.events_log_path.parent.mkdir(parents=True, exist_ok=True)
                with self.events_log_path.open('a', encoding='utf-8') as f:
                    f.write(line + '\n')

    def handle_rds(self, req: dict) -> tuple[int, dict | None]:
        def on_ack(site, route, version, now_wall):
            with self.lock:
                ev = self.policy.record_envoy_ack(
                    site, route, version, now_wall, monotonic_now=self.clock(),
                )
            if ev is not None:
                self._emit_events([ev])

        def on_nack(site, msg, now_wall):
            with self.lock:
                ev = self.policy.record_envoy_nack(site, msg, now_wall)
            self._emit_events([ev])

        return self.xds.handle_rds(req, on_ack=on_ack, on_nack=on_nack)

    def handle_eds(self, req: dict) -> tuple[int, dict | None]:
        return self.xds.handle_eds(req)

    def manual_override(self, site: str, route: str | None) -> dict:
        with self.lock:
            ev = self.policy.set_manual_override(site, route, self.clock())
            desired = self.policy.states[site].desired_route
            version = self.xds.set_site_route(site, desired)
        if ev is not None:
            self._emit_events([ev])
        return {'site': site, 'desired_route': desired, 'desired_version': version}

    def set_mode(self, mode: str) -> dict:
        with self.lock:
            self.policy.set_mode(mode)
            routes = {site: self.policy.states[site].desired_route for site in SITES}
            for site, route in routes.items():
                self.xds.set_site_route(site, route)
        return {'mode': mode, 'desired_routes': routes}

    def reset(self, *, mode: str | None = None) -> dict:
        with self.lock:
            # Mark active_route UNKNOWN until Envoy ACKs the newly issued LOCAL version.
            self.policy.reset(active_route='UNKNOWN')
            if mode is not None:
                self.policy.set_mode(mode)
            self.recent_events.clear()
            active_mode = self.policy.config.mode
            versions = self.xds.reset_routes()
        return {'reset': True, 'mode': active_mode, 'epoch': self.xds.epoch, 'desired_versions': versions}

    def sites_requiring_probe(self) -> list[str]:
        with self.lock:
            if self.policy.config.mode != 'vertical':
                return []
            return [
                site for site in SITES
                if self.policy.states[site].desired_route == 'SPREAD'
                or self.policy.states[site].active_route == 'SPREAD'
            ]

    def poll_once(self) -> list[dict]:
        """Run one 1-second control tick: discover endpoints, fetch collector snapshot, evaluate."""
        local_counts = {site: 0 for site in SITES}
        core_count = 0
        if self.discoverer is not None:
            try:
                local_by_site, core_eps = self.discoverer.discover()
                self.xds.update_endpoints(local_by_site, core_eps)
                local_counts = {site: len(local_by_site.get(site, [])) for site in SITES}
                core_count = len(core_eps)
                self._last_discovery_error = None
            except Exception as exc:
                self._last_discovery_error = f'{type(exc).__name__}: {exc}'
                eps = self.xds.get_endpoints_snapshot()
                local_counts = {site: len(eps.get(local_eds_service_name(site), [])) for site in SITES}
                core_count = len(eps.get(CORE_EDS_SERVICE_NAME, []))
        else:
            eps = self.xds.get_endpoints_snapshot()
            local_counts = {site: len(eps.get(local_eds_service_name(site), [])) for site in SITES}
            core_count = len(eps.get(CORE_EDS_SERVICE_NAME, []))

        snapshot = None
        collector_error = None
        try:
            snapshot = self.snapshot_fetcher(
                self.collector_snapshot_url,
                timeout=self.policy.config.collector_timeout_seconds,
            )
        except Exception as exc:
            collector_error = f'{type(exc).__name__}: {exc}'

        with self.lock:
            events = self.policy.evaluate(
                self.clock(),
                snapshot,
                local_counts,
                core_count,
                collector_error=collector_error,
            )
            for site, state in self.policy.states.items():
                self.xds.set_site_route(site, state.desired_route)

        self._emit_events(events)
        return [ev.to_dict() for ev in events]

    def get_status(self) -> dict:
        with self.lock:
            policy_states = {site: st.to_dict() for site, st in self.policy.states.items()}
            cfg = dict(self.policy.config.__dict__)
        return {
            'mode': cfg['mode'],
            'config': cfg,
            'sites': policy_states,
            'xds': self.xds.status_snapshot(),
            'last_discovery_error': self._last_discovery_error,
        }

    def get_recent_events(self) -> list[dict]:
        with self.lock:
            return list(self.recent_events)


def load_offloader_settings(path: str) -> dict:
    with open(path, encoding='utf-8') as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError('offloader config must be a YAML mapping')
    return data


def main(argv=None):
    parser = argparse.ArgumentParser(description='Core-side vertical offloading controller and Envoy xDS server')
    parser.add_argument('--config', default='offloader/config.yaml', help='Path to offloader YAML config')
    args = parser.parse_args(argv)

    raw = load_offloader_settings(args.config)
    policy_keys = set(OffloadConfig.__dataclass_fields__.keys())
    policy_cfg = OffloadConfig(**{k: v for k, v in raw.items() if k in policy_keys})

    site_nodes = raw.get('site_nodes') or dict(DEFAULT_SITE_NODES)
    discoverer = KubernetesEndpointDiscoverer(
        namespace=raw.get('namespace', 'services'),
        site_nodes=site_nodes,
        core_node=raw.get('core_node', 'core-5g'),
        edge_app_label=raw.get('edge_app_label', 'mec-placement-pedestrian-detection'),
        core_app_label=raw.get('core_app_label', 'mec-placement-pedestrian-detection-core'),
        container_port=int(raw.get('container_port', 5000)),
    )
    controller = VerticalOffloadController(
        policy_cfg,
        collector_snapshot_url=raw['collector_snapshot_url'],
        discoverer=discoverer,
        events_log_path=raw.get('events_log_path'),
        route_timeout=raw.get('envoy_route_timeout', '10s'),
    )

    listen_host = raw.get('listen_host', '0.0.0.0')
    listen_port = int(raw.get('listen_port', 8085))
    admin_host = raw.get('admin_host', '0.0.0.0')
    admin_port = int(raw.get('admin_port', 8086))
    xds_server = start_xds_server(controller, host=listen_host, port=listen_port)

    token = (
        os.environ.get('OFFLOADER_ADMIN_TOKEN', '').strip()
        or os.environ.get('UE_TELEMETRY_TOKEN', '').strip()
    )
    if not token and raw.get('admin_token_file') and Path(raw['admin_token_file']).is_file():
        token = Path(raw['admin_token_file']).read_text(encoding='utf-8').strip()
    if not token and raw.get('collector_token_file') and Path(raw['collector_token_file']).is_file():
        token = Path(raw['collector_token_file']).read_text(encoding='utf-8').strip()
    if not token:
        raise ValueError('OFFLOADER_ADMIN_TOKEN or UE_TELEMETRY_TOKEN must be set for the admin control listener')
    admin_server = start_admin_server(controller, token, host=admin_host, port=admin_port)

    probe_worker = None
    probe_urls = raw.get('site_probe_urls') or {}
    collector_obs_url = raw.get('collector_observations_url')
    collector_token = os.environ.get('UE_TELEMETRY_TOKEN', '').strip() or token
    if probe_urls and collector_obs_url and collector_token:
        probe_worker = LocalProbeWorker(
            probe_urls,
            collector_obs_url,
            collector_token,
            controller.sites_requiring_probe,
            probe_image_path=raw.get('probe_image_path'),
            interval_seconds=float(raw.get('probe_interval_seconds', 1.0)),
            timeout_seconds=float(raw.get('probe_timeout_seconds', 2.0)),
        )
        probe_worker.start()

    stop_event = threading.Event()

    def _stop(_sig, _frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    print(
        f'vertical-offloader xds={listen_host}:{listen_port} admin={admin_host}:{admin_port} '
        f'mode={policy_cfg.mode} epoch={controller.xds.epoch}',
        flush=True,
    )
    try:
        while not stop_event.is_set():
            tick = time.monotonic()
            controller.poll_once()
            elapsed = time.monotonic() - tick
            stop_event.wait(max(0.05, policy_cfg.poll_interval_seconds - elapsed))
    finally:
        if probe_worker is not None:
            probe_worker.stop()
        admin_server.shutdown()
        admin_server.server_close()
        xds_server.shutdown()
        xds_server.server_close()


if __name__ == '__main__':
    main()
