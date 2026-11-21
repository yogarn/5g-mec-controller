"""Single-replica in-memory UE observation collector."""
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
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import urlopen

import yaml


# Allowed characters in Kubernetes node names reported by telemetry.
_NODE_NAME_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-'
# Fixed observation schema vocabularies (not runtime tuning knobs).
_VALID_SITES = ('gnb-1', 'gnb-2', 'gnb-3')
_VALID_ROUTES = ('local', 'core', 'unknown')
_VALID_TRAFFIC_CLASSES = ('user', 'probe')
_DEFAULT_SITE_NODES = {'gnb-1': 'gnb1', 'gnb-2': 'gnb-2', 'gnb-3': 'gnb-3'}

# Fallback defaults applied when optional keys are omitted from collector/config.yaml.
DEFAULT_SETTINGS = {
    'placement_window_seconds': 5,
    'offload_stale_seconds': 5.0,
    'core_node': 'core-5g',
    'site_nodes': _DEFAULT_SITE_NODES,
}


def _is_valid_node_name(value):
    return isinstance(value, str) and bool(value) and all(c in _NODE_NAME_CHARS for c in value)


def validate_settings(settings):
    if not isinstance(settings, dict):
        raise ValueError('settings must be a mapping')
    settings.setdefault('placement_window_seconds', DEFAULT_SETTINGS['placement_window_seconds'])
    settings.setdefault('offload_window_seconds', settings['window_seconds'])
    settings.setdefault('offload_stale_seconds', DEFAULT_SETTINGS['offload_stale_seconds'])
    settings.setdefault('core_node', DEFAULT_SETTINGS['core_node'])
    settings.setdefault('site_nodes', dict(DEFAULT_SETTINGS['site_nodes']))
    for key in ('window_seconds', 'placement_window_seconds', 'offload_window_seconds',
                'offload_stale_seconds', 'mapping_refresh_seconds', 'mapping_max_age_seconds', 'slo_seconds'):
        value = settings[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f'{key} must be finite and positive')
    if settings['placement_window_seconds'] > settings['window_seconds']:
        raise ValueError('placement_window_seconds must not exceed window_seconds')
    if settings['offload_window_seconds'] > settings['window_seconds']:
        raise ValueError('offload_window_seconds must not exceed window_seconds')
    if not _is_valid_node_name(settings['core_node']):
        raise ValueError('core_node must be a valid node name')
    site_nodes = settings['site_nodes']
    if (not isinstance(site_nodes, dict) or set(site_nodes) != set(_VALID_SITES)
            or any(not _is_valid_node_name(v) for v in site_nodes.values())):
        raise ValueError('site_nodes must map gnb-1, gnb-2, gnb-3 to valid node names')
    value = settings['max_observations']
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError('max_observations must be a positive integer')
    nodes = settings['gnb_nodes']
    if (not isinstance(nodes, dict) or not nodes or
            any(isinstance(k, bool) or not isinstance(k, int) or k < 0 or not _is_valid_node_name(v)
                for k, v in nodes.items()) or len(set(nodes.values())) != len(nodes)):
        raise ValueError('gnb_nodes must contain unique valid node names and numeric IDs')
    upf_nodes = settings.get('upf_nodes', {})
    if not isinstance(upf_nodes, dict):
        raise ValueError('upf_nodes must map UPF IP addresses to valid node names')
    normalized_upf_nodes = {}
    for addr, node in upf_nodes.items():
        if not isinstance(addr, str):
            raise ValueError('upf_nodes keys must be IP address strings')
        try:
            ip = str(ipaddress.ip_address(addr))
        except ValueError as exc:
            raise ValueError(f'invalid UPF IP address in upf_nodes: {addr!r}') from exc
        if not _is_valid_node_name(node):
            raise ValueError(f'invalid node name in upf_nodes: {node!r}')
        if ip in normalized_upf_nodes:
            raise ValueError(f'duplicate UPF IP address in upf_nodes: {ip}')
        normalized_upf_nodes[ip] = node
    settings['upf_nodes'] = normalized_upf_nodes
    for key in ('smf_url', 'amf_url'):
        url = urlsplit(settings[key])
        if url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password or url.fragment:
            raise ValueError(f'invalid {key}')
        _ = url.port
    return settings


def load_settings(path):
    with open(path, encoding='utf-8') as stream:
        settings = yaml.safe_load(stream)
    return validate_settings(settings)


def parse_upf_host(addr):
    if not isinstance(addr, str) or not addr or addr != addr.strip():
        return None
    port = None
    if addr.startswith('['):
        end = addr.find(']')
        if end == -1:
            return None
        host = addr[1:end]
        rest = addr[end + 1:]
        if rest:
            if not rest.startswith(':'):
                return None
            port = rest[1:]
    elif addr.count(':') == 1:
        host, port = addr.split(':', 1)
    else:
        host = addr
    if port is not None and (len(port) > 5 or not port.isascii() or
                             not port.isdigit() or not 1 <= int(port) <= 65535):
        return None
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        return None


def map_upf_sessions(smf, upf_nodes):
    if not isinstance(smf, dict) or not isinstance(smf.get('items'), list):
        raise ValueError('invalid session response')
    candidates = {}
    for item in smf['items']:
        if not isinstance(item, dict):
            continue
        sessions = item.get('pdu')
        if not isinstance(sessions, list):
            continue
        for session in sessions:
            if not isinstance(session, dict) or session.get('pdu_state') != 'active':
                continue
            raw_ip = session.get('ipv4')
            if not isinstance(raw_ip, str):
                continue
            try:
                ip = str(ipaddress.IPv4Address(raw_ip))
            except (ValueError, TypeError, ipaddress.AddressValueError):
                continue
            n3 = session.get('n3')
            upf = n3.get('upf') if isinstance(n3, dict) else None
            addr = upf.get('addr') if isinstance(upf, dict) else None
            host = parse_upf_host(addr)
            node = upf_nodes.get(host) if host is not None else None
            candidates.setdefault(ip, []).append(node)
    return {ip: values[0] for ip, values in candidates.items() if len(values) == 1 and values[0] is not None}


def join_sessions(smf, amf, nodes):
    if not isinstance(smf, dict) or not isinstance(smf.get('items'), list) or not isinstance(amf, dict) or not isinstance(amf.get('items'), list):
        raise ValueError('invalid session response')
    amf_by_supi = {}
    for item in amf['items']:
        if not isinstance(item, dict) or not isinstance(item.get('supi'), str):
            continue
        amf_by_supi.setdefault(item['supi'], []).append(item.get('gnb'))
    smf_counts = Counter(item.get('supi') for item in smf['items'] if isinstance(item, dict) and isinstance(item.get('supi'), str))
    candidates = {}
    for item in smf['items']:
        if not isinstance(item, dict) or not isinstance(item.get('supi'), str):
            continue
        supi = item['supi']
        sessions = item.get('pdu')
        if not isinstance(sessions, list):
            continue
        matches = amf_by_supi.get(supi, [])
        node = None
        if smf_counts[supi] == 1 and len(matches) == 1 and isinstance(matches[0], dict):
            node = nodes.get(matches[0].get('gnb_id'))
        for session in sessions:
            if not isinstance(session, dict) or session.get('pdu_state') != 'active':
                continue
            try:
                ip = str(ipaddress.IPv4Address(session.get('ipv4')))
            except (ValueError, TypeError, ipaddress.AddressValueError):
                continue
            candidates.setdefault(ip, []).append(node)
    return {ip: values[0] for ip, values in candidates.items() if len(values) == 1 and values[0] is not None}


def fetch(url):
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query) if k != 'page'] + [('page', '-1')]
    with urlopen(urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), '')), timeout=2) as response:
        return json.load(response)


class Collector:
    def __init__(self, settings, clock=time.monotonic, wall_clock=time.time):
        self.settings = settings
        self.clock = clock
        self.wall_clock = wall_clock
        self.lock = threading.Lock()
        self.samples = deque()
        self.probe_samples = deque()
        self.mapping = {}
        self.upf_mapping = {}
        self.mapping_time = None
        self.upf_mapping_time = None
        self.ingested = self.rejected = self.overflow = 0
        self.probe_ingested = 0
        self.sample_seq = 0
        self._node_to_site = {node: site for site, node in self.settings['site_nodes'].items()}

    def refresh(self):
        smf = fetch(self.settings['smf_url'])
        upf_mapping = map_upf_sessions(smf, self.settings['upf_nodes'])
        with self.lock:
            self.upf_mapping = upf_mapping
            self.upf_mapping_time = self.clock()
        amf = fetch(self.settings['amf_url'])
        mapping = join_sessions(smf, amf, self.settings['gnb_nodes'])
        with self.lock:
            self.mapping = mapping
            self.mapping_time = self.clock()

    def evict_expired(self, now):
        cutoff = now - self.settings['window_seconds']
        while self.samples and self.samples[0][0] <= cutoff:
            self.samples.popleft()
        probe_cutoff = now - self.settings['offload_window_seconds']
        while self.probe_samples and self.probe_samples[0][0] <= probe_cutoff:
            self.probe_samples.popleft()

    def reject(self):
        with self.lock:
            self.rejected += 1

    def _resolve_route(self, raw_route, site, server_node):
        if raw_route in ('local', 'core'):
            return raw_route
        if server_node is not None:
            if server_node == self.settings['core_node']:
                return 'core'
            if site is not None and server_node == self.settings['site_nodes'].get(site):
                return 'local'
        return 'unknown'

    def ingest(self, observation):
        try:
            if not isinstance(observation, dict):
                raise ValueError()
            traffic_class = observation.get('traffic_class', 'user')
            if traffic_class not in _VALID_TRAFFIC_CLASSES:
                raise ValueError()
            ip = observation.get('ue_ip')
            if ip is not None:
                if not isinstance(ip, str):
                    raise ValueError()
                ip = str(ipaddress.IPv4Address(ip))
            elif traffic_class == 'user':
                raise ValueError()
            duration = observation['duration_ms']
            status = observation['http_status']
            success = observation['success']
            if (isinstance(duration, bool) or not isinstance(duration, (int, float))
                    or not math.isfinite(duration) or duration < 0 or not isinstance(success, bool)
                    or (status is not None and (type(status) is not int or not 100 <= status <= 999))
                    or (success and (status is None or not 200 <= status < 300))):
                raise ValueError()
            raw_site = observation.get('site')
            if raw_site is not None and raw_site not in _VALID_SITES:
                raise ValueError()
            raw_route = observation.get('route')
            if raw_route is not None and raw_route not in _VALID_ROUTES:
                raise ValueError()
            server_node = observation.get('server_node')
            if server_node is not None and not _is_valid_node_name(server_node):
                raise ValueError()
            timed_out = observation.get('timed_out', False)
            if not isinstance(timed_out, bool):
                raise ValueError()
            if status == 504:
                timed_out = True
            if traffic_class == 'probe' and raw_site is None:
                raise ValueError()
            duration /= 1000
        except (KeyError, ValueError, TypeError, OverflowError):
            self.reject()
            return False
        with self.lock:
            now = self.clock()
            wall_now = self.wall_clock()
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
            gnb_node = None
            upf_node = None
            if self.mapping_time is not None and now - self.mapping_time <= self.settings['mapping_max_age_seconds']:
                gnb_node = self.mapping.get(ip)
            if self.upf_mapping_time is not None and now - self.upf_mapping_time <= self.settings['mapping_max_age_seconds']:
                upf_node = self.upf_mapping.get(ip)
            site = raw_site or self._node_to_site.get(gnb_node) or self._node_to_site.get(upf_node)
            route = self._resolve_route(raw_route, site, server_node)
            self.samples.append((now, duration, success, gnb_node, upf_node, site, route, server_node, timed_out, wall_now, seq))
            self.ingested += 1
        return True

    def _summarize_bucket(self, records, now):
        stale_after = self.settings['offload_stale_seconds']
        if not records:
            return {
                'status': 'empty',
                'p90_ms': None,
                'attempts': 0,
                'successes': 0,
                'errors': 0,
                'timeouts': 0,
                'error_rate': None,
                'first_sample_ts': None,
                'last_sample_age_seconds': None,
                'last_sample_ts': None,
                'last_success_age_seconds': None,
                'newest_sample_seq': 0,
            }
        attempts = len(records)
        success_ms = sorted(duration * 1000.0 for _, _, duration, success, _, _ in records if success)
        successes = len(success_ms)
        errors = attempts - successes
        timeouts = sum(1 for _, _, _, _, timed_out, _ in records if timed_out)
        last_mono = max(ts for ts, _, _, _, _, _ in records)
        first_wall = min(wall for _, wall, _, _, _, _ in records)
        last_wall = max(wall for _, wall, _, _, _, _ in records)
        newest_seq = max(seq for _, _, _, _, _, seq in records)
        last_age = max(0.0, now - last_mono)
        success_monos = [ts for ts, _, _, success, _, _ in records if success]
        last_success_age = max(0.0, now - max(success_monos)) if success_monos else None
        p90_ms = round(success_ms[math.ceil(0.9 * successes) - 1], 3) if successes else None
        if last_age > stale_after:
            status = 'stale'
        elif successes == 0:
            status = 'no_success'
        else:
            status = 'ok'
        return {
            'status': status,
            'p90_ms': p90_ms,
            'attempts': attempts,
            'successes': successes,
            'errors': errors,
            'timeouts': timeouts,
            'error_rate': round(errors / attempts, 6),
            'first_sample_ts': round(first_wall, 6),
            'last_sample_age_seconds': round(last_age, 6),
            'last_sample_ts': round(last_wall, 6),
            'last_success_age_seconds': None if last_success_age is None else round(last_success_age, 6),
            'newest_sample_seq': newest_seq,
        }

    def offload_snapshot(self):
        with self.lock:
            now = self.clock()
            wall_now = self.wall_clock()
            self.evict_expired(now)
            cutoff = now - self.settings['offload_window_seconds']
            user_samples = [
                (ts, wall_ts, duration, success, site, route, timed_out, seq)
                for ts, duration, success, _, _, site, route, _, timed_out, wall_ts, seq in self.samples
                if ts > cutoff
            ]
            probe_samples = [
                (ts, wall_ts, duration, success, site, route, timed_out, seq)
                for ts, wall_ts, duration, success, site, route, _, timed_out, seq in self.probe_samples
                if ts > cutoff
            ]
        sites = {}
        for site in _VALID_SITES:
            user_buckets = {}
            for route in _VALID_ROUTES:
                recs = [
                    (ts, wall_ts, duration, success, timed_out, seq)
                    for ts, wall_ts, duration, success, s, r, timed_out, seq in user_samples
                    if s == site and r == route
                ]
                user_buckets[route] = self._summarize_bucket(recs, now)
            overall_recs = [
                (ts, wall_ts, duration, success, timed_out, seq)
                for ts, wall_ts, duration, success, s, _r, timed_out, seq in user_samples
                if s == site
            ]
            user_buckets['overall'] = self._summarize_bucket(overall_recs, now)
            probe_recs = [
                (ts, wall_ts, duration, success, timed_out, seq)
                for ts, wall_ts, duration, success, s, r, timed_out, seq in probe_samples
                if s == site and r == 'local'
            ]
            sites[site] = {
                'node': self.settings['site_nodes'][site],
                'user': user_buckets,
                'probe': {
                    'local': self._summarize_bucket(probe_recs, now),
                },
            }
        return {
            'generated_at': round(wall_now, 6),
            'window_seconds': float(self.settings['offload_window_seconds']),
            'stale_after_seconds': float(self.settings['offload_stale_seconds']),
            'p90_basis': 'success_only',
            'sites': sites,
        }

    def sessions_snapshot(self):
        with self.lock:
            now = self.clock()
            mapping_time = self.mapping_time
            counts = Counter(self.mapping.values())
        active_sessions = {node: counts[node] for node in self.settings['gnb_nodes'].values()}
        age = None if mapping_time is None else max(0.0, now - mapping_time)
        stale = age is None or age > self.settings['mapping_max_age_seconds']
        return {
            'active_sessions': active_sessions,
            'total_active_sessions': sum(active_sessions.values()),
            'refresh_age_seconds': None if age is None else round(age, 6),
            'mapping_max_age_seconds': self.settings['mapping_max_age_seconds'],
            'stale': stale,
        }

    def placement_metrics(self):
        with self.lock:
            now = self.clock()
            self.evict_expired(now)
            cutoff = now - self.settings['placement_window_seconds']
            recent = [(row[3], row[4]) for row in self.samples if row[0] > cutoff]
        gnb_counts = Counter(gnb_node for gnb_node, _ in recent)
        upf_counts = Counter(upf_node for _, upf_node in recent)
        lines = ['# TYPE ue_request_node_placement_window_attempts gauge']
        for node in self.settings['gnb_nodes'].values():
            lines.append(f'ue_request_node_placement_window_attempts{{node="{node}"}} {gnb_counts[node]}')
        lines.append('# TYPE ue_request_upf_placement_window_attempts gauge')
        for node in dict.fromkeys(self.settings['upf_nodes'].values()):
            lines.append(f'ue_request_upf_placement_window_attempts{{node="{node}"}} {upf_counts[node]}')
        lines.append('# TYPE ue_request_upf_placement_window_unassociated gauge')
        lines.append(f'ue_request_upf_placement_window_unassociated {upf_counts[None]}')
        return '\n'.join(lines) + '\n'

    def metrics(self):
        with self.lock:
            now = self.clock()
            self.evict_expired(now)
            samples = list(self.samples)
            ingested, rejected, overflow = self.ingested, self.rejected, self.overflow
            probe_ingested = self.probe_ingested
            mapping_time = self.mapping_time
            session_counts = Counter(self.mapping.values())
        successes = sorted(row[1] for row in samples if row[2])
        lines = [f'ue_request_window_attempts {len(samples)}',
                 f'ue_request_window_successes {len(successes)}',
                 f'ue_request_window_success_within_slo {sum(x <= self.settings["slo_seconds"] for x in successes)}',
                 f'ue_request_window_unassociated {sum(row[3] is None for row in samples)}',
                 f'ue_telemetry_ingested_total {ingested}', f'ue_telemetry_rejected_total {rejected}',
                 f'ue_telemetry_overflow_evictions_total {overflow}',
                 f'ue_telemetry_probe_ingested_total {probe_ingested}']
        if successes:
            lines.append(f'ue_request_duration_p90_seconds {successes[math.ceil(0.9 * len(successes)) - 1]}')
        if samples:
            durations = sorted(row[1] for row in samples)
            lines.append(f'ue_request_attempt_duration_p90_seconds {durations[math.ceil(0.9 * len(durations)) - 1]}')
        counts = Counter(row[3] for row in samples)
        for node in self.settings['gnb_nodes'].values():
            lines.append(f'ue_request_node_window_attempts{{node="{node}"}} {counts[node]}')
        for node in self.settings['gnb_nodes'].values():
            lines.append(f'ue_active_sessions{{node="{node}"}} {session_counts[node]}')
        if mapping_time is not None:
            lines.append(f'ue_session_mapping_age_seconds {max(0.0, now - mapping_time)}')
        snap = self.offload_snapshot()
        for site in _VALID_SITES:
            site_data = snap['sites'][site]
            for route in ('local', 'core'):
                bucket = site_data['user'][route]
                lines.append(f'ue_site_route_window_attempts{{site="{site}",route="{route}",traffic_class="user"}} {bucket["attempts"]}')
                lines.append(f'ue_site_route_window_errors{{site="{site}",route="{route}",traffic_class="user"}} {bucket["errors"]}')
                lines.append(f'ue_site_route_window_timeouts{{site="{site}",route="{route}",traffic_class="user"}} {bucket["timeouts"]}')
                if bucket['p90_ms'] is not None:
                    lines.append(f'ue_site_route_request_duration_p90_seconds{{site="{site}",route="{route}",traffic_class="user"}} {bucket["p90_ms"] / 1000.0}')
            probe_bucket = site_data['probe']['local']
            lines.append(f'ue_site_route_window_attempts{{site="{site}",route="local",traffic_class="probe"}} {probe_bucket["attempts"]}')
            lines.append(f'ue_site_route_window_errors{{site="{site}",route="local",traffic_class="probe"}} {probe_bucket["errors"]}')
            if probe_bucket['p90_ms'] is not None:
                lines.append(f'ue_site_route_request_duration_p90_seconds{{site="{site}",route="local",traffic_class="probe"}} {probe_bucket["p90_ms"] / 1000.0}')
        typed = []
        seen = set()
        for line in lines:
            name = line.split(' ', 1)[0].split('{', 1)[0]
            if name not in seen:
                typed.append(f'# TYPE {name} {"counter" if name.endswith("_total") else "gauge"}')
                seen.add(name)
            typed.append(line)
        return '\n'.join(typed) + '\n'


def make_handler(collector, token):
    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, body=b'', content_type='text/plain; charset=utf-8'):
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
                body = (json.dumps(collector.sessions_snapshot(), sort_keys=True) + '\n').encode('utf-8')
                self.reply(200, body, 'application/json; charset=utf-8')
            elif self.path == '/offload/snapshot':
                body = (json.dumps(collector.offload_snapshot(), sort_keys=True) + '\n').encode('utf-8')
                self.reply(200, body, 'application/json; charset=utf-8')
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
                observation = json.loads(data, parse_constant=lambda value: (_ for _ in ()).throw(ValueError('non-finite JSON')))
            except (ValueError, UnicodeError, OverflowError):
                collector.reject()
                self.reply(400)
                return
            self.reply(202 if collector.ingest(observation) else 400)

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    settings = load_settings(args.config)
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
