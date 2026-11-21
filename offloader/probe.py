"""Diagnostic local probe worker active while a site is in SPREAD (not a decision gate)."""
import hashlib
import http.client
import json
import socket
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit


def build_multipart_image(image_bytes: bytes) -> tuple[bytes, str]:
    boundary = 'mec-probe-' + hashlib.sha256(image_bytes).hexdigest()[:24]
    body = (
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="image"; filename="probe.jpg"\r\n'
        f'Content-Type: application/octet-stream\r\n\r\n'
    ).encode('utf-8') + image_bytes + f'\r\n--{boundary}--\r\n'.encode('utf-8')
    return body, f'multipart/form-data; boundary={boundary}'


def _parse_server_node(raw: bytes, status: int) -> str | None:
    if not (200 <= status < 300):
        return None
    try:
        payload = json.loads(raw.decode('utf-8'))
        return payload['server_node'] if isinstance(payload, dict) and isinstance(payload.get('server_node'), str) else None
    except (UnicodeDecodeError, ValueError):
        return None


def send_local_probe(url: str, body: bytes | None, content_type: str | None, *,
                     timeout: float = 2.0, method: str = 'POST') -> dict:
    """Send one forced-local probe request through the site's Envoy ingress."""
    parsed = urlsplit(url)
    target = (parsed.path or '/') + (f'?{parsed.query}' if parsed.query else '')
    headers = {'X-MEC-Probe': 'local', **({'Content-Type': content_type} if content_type else {})}
    start = time.monotonic()
    status, timed_out, server_node, selected_route, error, conn = None, False, None, 'local', None, None
    try:
        cls = http.client.HTTPSConnection if parsed.scheme == 'https' else http.client.HTTPConnection
        conn = cls(parsed.hostname, parsed.port, timeout=timeout)
        conn.request(method, target, body=body, headers=headers)
        resp = conn.getresponse()
        status = resp.status
        hdr_route = (resp.getheader('X-MEC-Route') or '').lower()
        if hdr_route in ('local', 'core'):
            selected_route = hdr_route
        timed_out = status == 504
        server_node = _parse_server_node(resp.read(), status)
    except (OSError, http.client.HTTPException, ValueError) as exc:
        error = f'{type(exc).__name__}: {exc}'
        timed_out = isinstance(exc, (socket.timeout, TimeoutError))
    finally:
        if conn is not None:
            conn.close()
    return {
        'duration_ms': (time.monotonic() - start) * 1000.0,
        'http_status': status,
        'success': error is None and status is not None and 200 <= status < 300,
        'timed_out': timed_out, 'route': selected_route, 'server_node': server_node, 'error': error,
    }


def post_probe_observation(collector_observations_url: str, token: str, site: str,
                           result: dict, *, timeout: float = 1.0) -> bool:
    """Report a probe measurement to the collector with traffic_class='probe'."""
    parsed = urlsplit(collector_observations_url)
    payload = {
        'traffic_class': 'probe', 'site': site, 'route': result.get('route') or 'local',
        'duration_ms': result['duration_ms'], 'http_status': result['http_status'],
        'success': result['success'], 'timed_out': bool(result.get('timed_out', False)),
    }
    if result.get('server_node'):
        payload['server_node'] = result['server_node']
    conn = None
    try:
        cls = http.client.HTTPSConnection if parsed.scheme == 'https' else http.client.HTTPConnection
        conn = cls(parsed.hostname, parsed.port, timeout=timeout)
        conn.request(
            'POST', parsed.path or '/observations',
            body=json.dumps(payload, allow_nan=False).encode('utf-8'),
            headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'},
        )
        resp = conn.getresponse()
        resp.read()
        return 200 <= resp.status < 300
    except (OSError, http.client.HTTPException, ValueError):
        return False
    finally:
        if conn is not None:
            conn.close()


class LocalProbeWorker:
    """Background worker that sends forced-local diagnostic probes for sites in SPREAD."""

    def __init__(self, site_probe_urls: dict[str, str], collector_observations_url: str,
                 collector_token: str, active_sites_fn, *, probe_image_path: str | None = None,
                 interval_seconds: float = 1.0, timeout_seconds: float = 2.0):
        self.site_probe_urls = dict(site_probe_urls)
        self.collector_observations_url = collector_observations_url
        self.collector_token = collector_token
        self.active_sites_fn = active_sites_fn
        self.interval_seconds = float(interval_seconds)
        self.timeout_seconds = float(timeout_seconds)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        if probe_image_path and Path(probe_image_path).is_file():
            self._body, self._content_type = build_multipart_image(Path(probe_image_path).read_bytes())
            self._method = 'POST'
        else:
            self._body, self._content_type, self._method = None, None, 'GET'

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(target=self._loop, daemon=True, name='local-recovery-probe')
            self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def probe_once(self, site: str) -> dict | None:
        url = self.site_probe_urls.get(site)
        if not url:
            return None
        result = send_local_probe(url, self._body, self._content_type, timeout=self.timeout_seconds, method=self._method)
        post_probe_observation(self.collector_observations_url, self.collector_token, site, result, timeout=min(1.0, self.timeout_seconds))
        return result

    def _loop(self):
        while not self._stop.is_set():
            tick = time.monotonic()
            for site in self.active_sites_fn():
                if self._stop.is_set():
                    break
                self.probe_once(site)
            self._stop.wait(max(0.05, self.interval_seconds - (time.monotonic() - tick)))
