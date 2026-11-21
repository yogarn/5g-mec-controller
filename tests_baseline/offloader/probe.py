"""Lightweight diagnostic local probe worker (active while a site is in SPREAD; not a decision gate).

Measured path documentation:
- Probe traffic originates from the Core offloader process, traverses the Core-to-gNB
  network to the site's Envoy ingress (with header `X-MEC-Probe: local`), and is
  forced by Envoy RDS to `local_backend` (ready inference pods pinned to that gNB node).
- It intentionally excludes the UE radio / UERANSIM / UPF segment because the offloader
  runs in Core, while accurately measuring local Envoy + local inference pod queueing,
  processing latency, and readiness at the gNB site.
- Probe observations are reported to the collector with `traffic_class="probe"` so they
  are kept strictly separate from UE `user` traffic and never pollute HPA or placement
  metrics.
"""
import hashlib
import http.client
import json
import socket
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit


def build_multipart_image(image_bytes: bytes) -> tuple[bytes, str]:
    digest = hashlib.sha256(image_bytes).hexdigest()
    boundary = 'mec-probe-' + digest[:24]
    body = (
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="image"; filename="probe.jpg"\r\n'
        f'Content-Type: application/octet-stream\r\n\r\n'
    ).encode('utf-8') + image_bytes + f'\r\n--{boundary}--\r\n'.encode('utf-8')
    return body, f'multipart/form-data; boundary={boundary}'


def send_local_probe(
    url: str,
    body: bytes | None,
    content_type: str | None,
    *,
    timeout: float = 2.0,
    method: str = 'POST',
) -> dict:
    """Send one forced-local probe request through the site's Envoy ingress."""
    parsed = urlsplit(url)
    target = parsed.path or '/'
    if parsed.query:
        target += '?' + parsed.query
    headers = {'X-MEC-Probe': 'local'}
    if content_type:
        headers['Content-Type'] = content_type
    start = time.monotonic()
    status = None
    timed_out = False
    server_node = None
    selected_route = 'local'
    error = None
    conn = None
    try:
        cls = http.client.HTTPSConnection if parsed.scheme == 'https' else http.client.HTTPConnection
        conn = cls(parsed.hostname, parsed.port, timeout=timeout)
        conn.request(method, target, body=body, headers=headers)
        resp = conn.getresponse()
        status = resp.status
        hdr_route = resp.getheader('X-MEC-Route')
        if hdr_route and hdr_route.lower() in ('local', 'core'):
            selected_route = hdr_route.lower()
        if status == 504:
            timed_out = True
        raw = resp.read()
        if 200 <= status < 300:
            try:
                payload = json.loads(raw.decode('utf-8'))
                if isinstance(payload, dict) and isinstance(payload.get('server_node'), str):
                    server_node = payload['server_node']
            except (UnicodeDecodeError, ValueError):
                pass
    except (OSError, http.client.HTTPException, ValueError) as exc:
        error = f'{type(exc).__name__}: {exc}'
        if isinstance(exc, (socket.timeout, TimeoutError)):
            timed_out = True
    finally:
        if conn is not None:
            conn.close()
    duration_ms = (time.monotonic() - start) * 1000.0
    success = error is None and status is not None and 200 <= status < 300
    return {
        'duration_ms': duration_ms,
        'http_status': status,
        'success': success,
        'timed_out': timed_out,
        'route': selected_route,
        'server_node': server_node,
        'error': error,
    }


def post_probe_observation(
    collector_observations_url: str,
    token: str,
    site: str,
    result: dict,
    *,
    timeout: float = 1.0,
) -> bool:
    """Report a probe measurement to the collector with traffic_class='probe'."""
    parsed = urlsplit(collector_observations_url)
    payload = {
        'traffic_class': 'probe',
        'site': site,
        'route': result.get('route') or 'local',
        'duration_ms': result['duration_ms'],
        'http_status': result['http_status'],
        'success': result['success'],
        'timed_out': bool(result.get('timed_out', False)),
    }
    if result.get('server_node'):
        payload['server_node'] = result['server_node']
    conn = None
    try:
        cls = http.client.HTTPSConnection if parsed.scheme == 'https' else http.client.HTTPConnection
        conn = cls(parsed.hostname, parsed.port, timeout=timeout)
        conn.request(
            'POST',
            parsed.path or '/observations',
            body=json.dumps(payload, allow_nan=False).encode('utf-8'),
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {token}',
            },
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
    """Background worker that probes local_backend only for sites currently in CORE."""

    def __init__(
        self,
        site_probe_urls: dict[str, str],
        collector_observations_url: str,
        collector_token: str,
        active_sites_fn,
        *,
        probe_image_path: str | None = None,
        interval_seconds: float = 1.0,
        timeout_seconds: float = 2.0,
    ):
        self.site_probe_urls = dict(site_probe_urls)
        self.collector_observations_url = collector_observations_url
        self.collector_token = collector_token
        self.active_sites_fn = active_sites_fn
        self.interval_seconds = float(interval_seconds)
        self.timeout_seconds = float(timeout_seconds)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        if probe_image_path and Path(probe_image_path).is_file():
            raw = Path(probe_image_path).read_bytes()
            self._body, self._content_type = build_multipart_image(raw)
            self._method = 'POST'
        else:
            self._body, self._content_type = None, None
            self._method = 'GET'

    def start(self):
        if self._thread is not None:
            return
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
        result = send_local_probe(
            url,
            self._body,
            self._content_type,
            timeout=self.timeout_seconds,
            method=self._method,
        )
        post_probe_observation(
            self.collector_observations_url,
            self.collector_token,
            site,
            result,
            timeout=min(1.0, self.timeout_seconds),
        )
        return result

    def _loop(self):
        while not self._stop.is_set():
            tick = time.monotonic()
            sites = self.active_sites_fn()
            for site in sites:
                if self._stop.is_set():
                    break
                self.probe_once(site)
            elapsed = time.monotonic() - tick
            self._stop.wait(max(0.05, self.interval_seconds - elapsed))
