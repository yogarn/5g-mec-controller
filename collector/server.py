"""Single-replica in-memory UE observation collector and HTTP server entrypoint."""
import argparse
from collections import Counter, deque
import hmac
import ipaddress
import json
import math
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from collector.buckets import build_offload_snapshot, build_sessions_snapshot
from collector.config import (
    DEFAULT_SETTINGS,
    VALID_ROUTES,
    VALID_SITES,
    VALID_TRAFFIC_CLASSES,
    is_valid_node_name,
    load_settings,
    validate_settings,
)
from collector.metrics import render_metrics, render_placement_metrics
from collector.sessions import fetch, join_sessions, map_upf_sessions, parse_upf_host


def _parse_observation(obs) -> tuple:
    """Validate and normalize one incoming observation JSON payload."""
    if not isinstance(obs, dict):
        raise ValueError()
    traffic_class = obs.get('traffic_class', 'user')
    if traffic_class not in VALID_TRAFFIC_CLASSES:
        raise ValueError()
    ip = obs.get('ue_ip')
    if ip is not None:
        if not isinstance(ip, str):
            raise ValueError()
        ip = str(ipaddress.IPv4Address(ip))
    elif traffic_class == 'user':
        raise ValueError()
    duration, status, success = obs['duration_ms'], obs['http_status'], obs['success']
    if (isinstance(duration, bool) or not isinstance(duration, (int, float))
            or not math.isfinite(duration) or duration < 0 or not isinstance(success, bool)
            or (status is not None and (type(status) is not int or not 100 <= status <= 999))
            or (success and (status is None or not 200 <= status < 300))):
        raise ValueError()
    raw_site = obs.get('site')
    if raw_site is not None and raw_site not in VALID_SITES:
        raise ValueError()
    raw_route = obs.get('route')
    if raw_route is not None and raw_route not in VALID_ROUTES:
        raise ValueError()
    server_node = obs.get('server_node')
    if server_node is not None and not is_valid_node_name(server_node):
        raise ValueError()
    timed_out = obs.get('timed_out', False)
    if not isinstance(timed_out, bool):
        raise ValueError()
    if status == 504:
        timed_out = True
    if traffic_class == 'probe' and raw_site is None:
        raise ValueError()
    return traffic_class, ip, duration / 1000, success, raw_site, raw_route, server_node, timed_out


class Collector:
    """Thread-safe sliding-window store for UE observations and SMF/AMF session mappings."""

    def __init__(self, settings: dict, clock=time.monotonic, wall_clock=time.time):
        self.settings = settings
        self.clock = clock
        self.wall_clock = wall_clock
        self.lock = threading.Lock()
        self.samples: deque = deque()
        self.probe_samples: deque = deque()
        self.mapping: dict[str, str] = {}
        self.upf_mapping: dict[str, str] = {}
        self.mapping_time: float | None = None
        self.upf_mapping_time: float | None = None
        self.ingested = self.rejected = self.overflow = self.probe_ingested = self.sample_seq = 0
        self._node_to_site = {node: site for site, node in self.settings['site_nodes'].items()}

    def refresh(self):
        smf = fetch(self.settings['smf_url'])
        upf_mapping = map_upf_sessions(smf, self.settings['upf_nodes'])
        with self.lock:
            self.upf_mapping, self.upf_mapping_time = upf_mapping, self.clock()
        amf = fetch(self.settings['amf_url'])
        mapping = join_sessions(smf, amf, self.settings['gnb_nodes'])
        with self.lock:
            self.mapping, self.mapping_time = mapping, self.clock()

    def evict_expired(self, now: float):
        cutoff = now - self.settings['window_seconds']
        while self.samples and self.samples[0][0] <= cutoff:
            self.samples.popleft()
        probe_cutoff = now - self.settings['offload_window_seconds']
        while self.probe_samples and self.probe_samples[0][0] <= probe_cutoff:
            self.probe_samples.popleft()

    def reject(self):
        with self.lock:
            self.rejected += 1

    def _resolve_route(self, raw_route, site, server_node) -> str:
        if raw_route in ('local', 'core'):
            return raw_route
        if server_node is not None:
            if server_node == self.settings['core_node']:
                return 'core'
            if site is not None and server_node == self.settings['site_nodes'].get(site):
                return 'local'
        return 'unknown'

    def ingest(self, observation) -> bool:
        try:
            traffic_class, ip, duration, success, raw_site, raw_route, server_node, timed_out = _parse_observation(observation)
        except (KeyError, ValueError, TypeError, OverflowError):
            self.reject()
            return False
        with self.lock:
            now, wall_now = self.clock(), self.wall_clock()
            self.evict_expired(now)
            self.sample_seq += 1
            seq = self.sample_seq
            if traffic_class == 'probe':
                route = self._resolve_route(raw_route, raw_site, server_node)
                if len(self.probe_samples) >= self.settings['max_observations']:
                    self.probe_samples.popleft()
                self.probe_samples.append((now, wall_now, duration, success, raw_site, route, server_node, timed_out, seq))
                self.probe_ingested += 1
                return True
            if len(self.samples) >= self.settings['max_observations']:
                self.samples.popleft()
                self.overflow += 1
            max_age = self.settings['mapping_max_age_seconds']
            gnb_node = self.mapping.get(ip) if self.mapping_time is not None and now - self.mapping_time <= max_age else None
            upf_node = self.upf_mapping.get(ip) if self.upf_mapping_time is not None and now - self.upf_mapping_time <= max_age else None
            site = raw_site or self._node_to_site.get(gnb_node) or self._node_to_site.get(upf_node)
            route = self._resolve_route(raw_route, site, server_node)
            self.samples.append((now, duration, success, gnb_node, upf_node, site, route, server_node, timed_out, wall_now, seq))
            self.ingested += 1
        return True

    def offload_snapshot(self) -> dict:
        with self.lock:
            now, wall_now = self.clock(), self.wall_clock()
            self.evict_expired(now)
            cutoff = now - self.settings['offload_window_seconds']
            user_samples = [
                (ts, w, dur, ok, site, route, to, seq)
                for ts, dur, ok, _, _, site, route, _, to, w, seq in self.samples if ts > cutoff
            ]
            probe_samples = [
                (ts, w, dur, ok, site, route, to, seq)
                for ts, w, dur, ok, site, route, _, to, seq in self.probe_samples if ts > cutoff
            ]
        return build_offload_snapshot(user_samples, probe_samples, now, wall_now, self.settings)

    def sessions_snapshot(self) -> dict:
        with self.lock:
            now, mapping_time, counts = self.clock(), self.mapping_time, Counter(self.mapping.values())
        return build_sessions_snapshot(counts, mapping_time, now, self.settings)

    def placement_metrics(self) -> str:
        with self.lock:
            now = self.clock()
            self.evict_expired(now)
            cutoff = now - self.settings['placement_window_seconds']
            recent = [(row[3], row[4]) for row in self.samples if row[0] > cutoff]
        return render_placement_metrics(recent, self.settings)

    def metrics(self) -> str:
        with self.lock:
            now = self.clock()
            self.evict_expired(now)
            samples = list(self.samples)
            ingested, rejected, overflow, probe_ingested = self.ingested, self.rejected, self.overflow, self.probe_ingested
            mapping_time, session_counts = self.mapping_time, Counter(self.mapping.values())
        return render_metrics(
            samples, ingested=ingested, rejected=rejected, overflow=overflow,
            probe_ingested=probe_ingested, mapping_time=mapping_time,
            session_counts=session_counts, now=now, settings=self.settings,
            offload_snap=self.offload_snapshot(),
        )


def make_handler(collector: Collector, token: str):
    """Build HTTP request handler exposing collector read endpoints and authenticated /observations."""

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status: int, body: bytes = b'', content_type: str = 'text/plain; charset=utf-8'):
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == '/health':
                self.reply(200, b'ok\n')
            elif self.path == '/metrics':
                self.reply(200, collector.metrics().encode(), 'text/plain; version=0.0.4; charset=utf-8')
            elif self.path == '/metrics/placement':
                self.reply(200, collector.placement_metrics().encode(), 'text/plain; version=0.0.4; charset=utf-8')
            elif self.path == '/sessions':
                self.reply(200, (json.dumps(collector.sessions_snapshot(), sort_keys=True) + '\n').encode('utf-8'), 'application/json; charset=utf-8')
            elif self.path == '/offload/snapshot':
                self.reply(200, (json.dumps(collector.offload_snapshot(), sort_keys=True) + '\n').encode('utf-8'), 'application/json; charset=utf-8')
            else:
                self.reply(404)

        def do_POST(self):
            if self.path != '/observations':
                self.reply(404)
                return
            if not hmac.compare_digest(self.headers.get('Authorization', '').encode('utf-8'), ('Bearer ' + token).encode('utf-8')):
                collector.reject()
                self.reply(401)
                return
            try:
                length = int(self.headers.get('Content-Length', ''))
                if length < 0 or length > 4096 or self.headers.get('Transfer-Encoding'):
                    raise ValueError()
                data = self.rfile.read(length)
                if len(data) != length:
                    raise ValueError()
                obs = json.loads(data, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('non-finite JSON')))
            except (ValueError, UnicodeError, OverflowError):
                collector.reject()
                self.reply(400)
                return
            self.reply(202 if collector.ingest(obs) else 400)

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    settings = load_settings(parser.parse_args().config)
    token = os.environ.get('UE_TELEMETRY_TOKEN', '').strip()
    if not token or '\n' in token or '\r' in token:
        raise ValueError('UE_TELEMETRY_TOKEN must be nonempty and single-line')
    collector = Collector(settings)

    def refresh_loop():
        while True:
            try:
                collector.refresh()
            except (OSError, ValueError, TypeError) as exc:
                print(f'mapping refresh failed: {type(exc).__name__}', flush=True)
            time.sleep(settings['mapping_refresh_seconds'])

    threading.Thread(target=refresh_loop, daemon=True).start()
    ThreadingHTTPServer(('', 8080), make_handler(collector, token)).serve_forever()


if __name__ == '__main__':
    main()
