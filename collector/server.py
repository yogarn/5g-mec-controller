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


def load_settings(path):
    with open(path, encoding='utf-8') as stream:
        settings = yaml.safe_load(stream)
    for key in ('window_seconds', 'mapping_refresh_seconds', 'mapping_max_age_seconds', 'slo_seconds'):
        value = settings[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f'{key} must be finite and positive')
    value = settings['max_observations']
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError('max_observations must be a positive integer')
    nodes = settings['gnb_nodes']
    if (not isinstance(nodes, dict) or not nodes or
            any(isinstance(k, bool) or not isinstance(k, int) or k < 0 or
                not isinstance(v, str) or not v or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in v)
                for k, v in nodes.items()) or len(set(nodes.values())) != len(nodes)):
        raise ValueError('gnb_nodes must contain unique valid node names and numeric IDs')
    for key in ('smf_url', 'amf_url'):
        url = urlsplit(settings[key])
        if url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password or url.fragment:
            raise ValueError(f'invalid {key}')
        _ = url.port
    return settings


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
    def __init__(self, settings, clock=time.monotonic):
        self.settings = settings
        self.clock = clock
        self.lock = threading.Lock()
        self.samples = deque()
        self.mapping = {}
        self.mapping_time = None
        self.incomplete_until = 0
        self.ingested = self.rejected = self.overflow = 0

    def refresh(self):
        mapping = join_sessions(fetch(self.settings['smf_url']), fetch(self.settings['amf_url']), self.settings['gnb_nodes'])
        with self.lock:
            self.mapping = mapping
            self.mapping_time = self.clock()

    def evict_expired(self, now):
        while self.samples and self.samples[0][0] <= now - self.settings['window_seconds']:
            self.samples.popleft()

    def reject(self):
        with self.lock:
            self.rejected += 1

    def ingest(self, observation):
        try:
            if not isinstance(observation, dict):
                raise ValueError()
            ip = observation['ue_ip']
            if not isinstance(ip, str):
                raise ValueError()
            ip = str(ipaddress.IPv4Address(ip))
            duration = observation['duration_ms']
            status = observation['http_status']
            success = observation['success']
            if (isinstance(duration, bool) or not isinstance(duration, (int, float))
                    or not math.isfinite(duration) or duration < 0 or not isinstance(success, bool)
                    or (status is not None and (type(status) is not int or not 100 <= status <= 999))
                    or (success and (status is None or not 200 <= status < 300))):
                raise ValueError()
            duration /= 1000
        except (KeyError, ValueError, TypeError, OverflowError):
            self.reject()
            return False
        with self.lock:
            now = self.clock()
            self.evict_expired(now)
            if len(self.samples) >= self.settings['max_observations']:
                oldest = self.samples.popleft()
                self.overflow += 1
                self.incomplete_until = oldest[0] + self.settings['window_seconds']
            node = None
            if self.mapping_time is not None and now - self.mapping_time <= self.settings['mapping_max_age_seconds']:
                node = self.mapping.get(ip)
            self.samples.append((now, duration, success, node))
            self.ingested += 1
        return True

    def metrics(self):
        with self.lock:
            now = self.clock()
            self.evict_expired(now)
            samples = list(self.samples)
            complete = now >= self.incomplete_until
            ingested, rejected, overflow = self.ingested, self.rejected, self.overflow
        successes = sorted(duration for _, duration, success, _ in samples if success)
        lines = [f'ue_request_window_complete {int(complete)}',
                 f'ue_request_window_attempts {len(samples)}',
                 f'ue_request_window_successes {len(successes)}',
                 f'ue_request_window_success_within_slo {sum(x <= self.settings["slo_seconds"] for x in successes)}',
                 f'ue_request_window_unassociated {sum(node is None for _, _, _, node in samples)}',
                 f'ue_telemetry_ingested_total {ingested}', f'ue_telemetry_rejected_total {rejected}',
                 f'ue_telemetry_overflow_evictions_total {overflow}']
        if complete and successes:
            lines.append(f'ue_request_duration_p90_seconds {successes[math.ceil(0.9 * len(successes)) - 1]}')
        if complete and samples:
            durations = sorted(duration for _, duration, _, _ in samples)
            lines.append(f'ue_request_attempt_duration_p90_seconds {durations[math.ceil(0.9 * len(durations)) - 1]}')
        counts = Counter(node for _, _, _, node in samples)
        for node in self.settings['gnb_nodes'].values():
            lines.append(f'ue_request_node_window_attempts{{node="{node}"}} {counts[node]}')
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
