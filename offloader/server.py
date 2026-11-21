"""Core-side vertical offloading controller with 1 Hz collector polling and Envoy xDS."""
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
        for ev in events or ():
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
                ev = self.policy.record_envoy_ack(site, route, version, now_wall, monotonic_now=self.clock())
            if ev is not None:
                self._emit_events([ev])

        def on_nack(site, msg, now_wall):
            with self.lock:
                ev = self.policy.record_envoy_nack(site, msg, now_wall)
            self._emit_events([ev])

        return self.xds.handle_rds(req, on_ack=on_ack, on_nack=on_nack)

    def handle_eds(self, req: dict) -> tuple[int, dict | None]:
        def on_ack(site, level, version, now_wall):
            with self.lock:
                ev = self.policy.record_envoy_eds_ack(site, level, version, now_wall, monotonic_now=self.clock())
            if ev is not None:
                self._emit_events([ev])

        def on_nack(site, msg, now_wall):
            with self.lock:
                ev = self.policy.record_envoy_nack(site, msg, now_wall, reason='envoy_eds_nack')
            self._emit_events([ev])

        return self.xds.handle_eds(req, on_ack=on_ack, on_nack=on_nack)

    def manual_override(self, site: str, route: str | None) -> dict:
        with self.lock:
            ev = self.policy.set_manual_override(site, route, self.clock())
            state = self.policy.states[site]
            version = self.xds.set_site_route(site, state.desired_route)
            self.xds.set_site_retreat_level(site, state.retreat_level)
        if ev is not None:
            self._emit_events([ev])
        return {'site': site, 'desired_route': state.desired_route, 'desired_version': version}

    def set_mode(self, mode: str) -> dict:
        with self.lock:
            self.policy.set_mode(mode)
            routes = {site: self.policy.states[site].desired_route for site in SITES}
            for site, route in routes.items():
                self.xds.set_site_route(site, route)
                self.xds.set_site_retreat_level(site, self.policy.states[site].retreat_level)
        return {'mode': mode, 'desired_routes': routes}

    def reset(self, *, mode: str | None = None) -> dict:
        with self.lock:
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

    def _discover_ready_counts(self) -> tuple[dict[str, int], int]:
        if self.discoverer is not None:
            try:
                local_by_site, core_eps = self.discoverer.discover()
                self.xds.update_endpoints(local_by_site, core_eps)
                self._last_discovery_error = None
                return {site: len(local_by_site.get(site, [])) for site in SITES}, len(core_eps)
            except Exception as exc:
                self._last_discovery_error = f'{type(exc).__name__}: {exc}'
        eps = self.xds.get_endpoints_snapshot()
        return {site: len(eps.get(local_eds_service_name(site), [])) for site in SITES}, len(eps.get(CORE_EDS_SERVICE_NAME, []))

    def poll_once(self) -> list[dict]:
        """Run one 1 Hz tick: discover endpoints, fetch collector snapshot, evaluate, push xDS."""
        local_counts, core_count = self._discover_ready_counts()
        snapshot, collector_error = None, None
        try:
            snapshot = self.snapshot_fetcher(self.collector_snapshot_url, timeout=self.policy.config.collector_timeout_seconds)
        except Exception as exc:
            collector_error = f'{type(exc).__name__}: {exc}'

        with self.lock:
            events = self.policy.evaluate(self.clock(), snapshot, local_counts, core_count, collector_error=collector_error)
            for site, state in self.policy.states.items():
                self.xds.set_site_route(site, state.desired_route)
                self.xds.set_site_retreat_level(site, state.retreat_level)

        self._emit_events(events)
        return [ev.to_dict() for ev in events]

    def get_status(self) -> dict:
        with self.lock:
            policy_states = {site: st.to_dict() for site, st in self.policy.states.items()}
            cfg = dict(self.policy.config.__dict__)
        return {
            'mode': cfg['mode'], 'config': cfg, 'sites': policy_states,
            'xds': self.xds.status_snapshot(), 'last_discovery_error': self._last_discovery_error,
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


def _resolve_admin_token(raw: dict) -> str:
    token = os.environ.get('OFFLOADER_ADMIN_TOKEN', '').strip() or os.environ.get('UE_TELEMETRY_TOKEN', '').strip()
    for key in ('admin_token_file', 'collector_token_file'):
        if not token and raw.get(key) and Path(raw[key]).is_file():
            token = Path(raw[key]).read_text(encoding='utf-8').strip()
    if not token:
        raise ValueError('OFFLOADER_ADMIN_TOKEN or UE_TELEMETRY_TOKEN must be set for the admin control listener')
    return token


def _maybe_start_probe_worker(raw: dict, controller: VerticalOffloadController, admin_token: str) -> LocalProbeWorker | None:
    probe_urls = raw.get('site_probe_urls') or {}
    collector_obs_url = raw.get('collector_observations_url')
    collector_token = os.environ.get('UE_TELEMETRY_TOKEN', '').strip() or admin_token
    if not (probe_urls and collector_obs_url and collector_token):
        return None
    worker = LocalProbeWorker(
        probe_urls, collector_obs_url, collector_token, controller.sites_requiring_probe,
        probe_image_path=raw.get('probe_image_path'),
        interval_seconds=float(raw.get('probe_interval_seconds', 1.0)),
        timeout_seconds=float(raw.get('probe_timeout_seconds', 2.0)),
    )
    worker.start()
    return worker


def main(argv=None):
    parser = argparse.ArgumentParser(description='Core-side vertical offloading controller and Envoy xDS server')
    parser.add_argument('--config', default='offloader/config.yaml', help='Path to offloader YAML config')
    raw = load_offloader_settings(parser.parse_args(argv).config)
    policy_cfg = OffloadConfig(**{k: v for k, v in raw.items() if k in OffloadConfig.__dataclass_fields__})
    discoverer = KubernetesEndpointDiscoverer(
        namespace=raw.get('namespace', 'services'),
        site_nodes=raw.get('site_nodes') or dict(DEFAULT_SITE_NODES),
        core_node=raw.get('core_node', 'core-5g'),
        edge_app_label=raw.get('edge_app_label', 'mec-placement-pedestrian-detection'),
        core_app_label=raw.get('core_app_label', 'mec-placement-pedestrian-detection-core'),
        container_port=int(raw.get('container_port', 5000)),
    )
    controller = VerticalOffloadController(
        policy_cfg, collector_snapshot_url=raw['collector_snapshot_url'],
        discoverer=discoverer, events_log_path=raw.get('events_log_path'),
        route_timeout=raw.get('envoy_route_timeout', '10s'),
    )
    xds_server = start_xds_server(controller, host=raw.get('listen_host', '0.0.0.0'), port=int(raw.get('listen_port', 8085)))
    token = _resolve_admin_token(raw)
    admin_server = start_admin_server(controller, token, host=raw.get('admin_host', '0.0.0.0'), port=int(raw.get('admin_port', 8086)))
    probe_worker = _maybe_start_probe_worker(raw, controller, token)

    stop_event = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    print(f'vertical-offloader mode={policy_cfg.mode} epoch={controller.xds.epoch}', flush=True)
    try:
        while not stop_event.is_set():
            tick = time.monotonic()
            controller.poll_once()
            stop_event.wait(max(0.05, policy_cfg.poll_interval_seconds - (time.monotonic() - tick)))
    finally:
        if probe_worker is not None:
            probe_worker.stop()
        for srv in (admin_server, xds_server):
            srv.shutdown()
            srv.server_close()


if __name__ == '__main__':
    main()
