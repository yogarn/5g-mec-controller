"""Source-bound, best-effort per-attempt telemetry delivery."""
import http.client
import json
import queue
import socket
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit


def telemetry_config(url, token_file):
    if not url and not token_file:
        return None
    if not url or not token_file:
        raise ValueError('telemetry URL and token file must be supplied together')
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError('invalid telemetry URL port') from exc
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname or port == 0
            or parsed.username is not None or parsed.password is not None
            or parsed.path != '/observations' or parsed.query or parsed.fragment
            or any(c.isspace() for c in url)):
        raise ValueError('telemetry URL must be an absolute HTTP(S) /observations URL')
    token = Path(token_file).read_text(encoding='utf-8').strip()
    if not token or any(c in token for c in '\r\n'):
        raise ValueError('telemetry token must be nonempty and single-line')
    return parsed, token


class TelemetryPublisher:
    def __init__(self, config, sources, capacity=1024):
        self.url, self.token = config
        self.queues = {source: queue.Queue(capacity) for source in sources}
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.connections = {}
        self.delivered = self.dropped = 0
        self.threads = [threading.Thread(target=self._send, args=(source,), daemon=True)
                        for source in self.queues]
        for thread in self.threads:
            thread.start()

    def _drop(self, reason):
        with self.lock:
            self.dropped += 1
        print(f'warning: telemetry dropped: {reason}', file=sys.stderr)

    def submit(self, source, result):
        try:
            self.queues[source].put_nowait({'ue_ip': source, 'duration_ms': result['duration_ms'],
                                           'http_status': result['http_status'], 'success': result['success']})
        except queue.Full:
            self._drop('queue full')

    def _send(self, source):
        pending = self.queues[source]
        while not self.stopping.is_set():
            try:
                observation = pending.get(timeout=0.05)
            except queue.Empty:
                continue
            conn = None
            try:
                cls = http.client.HTTPSConnection if self.url.scheme == 'https' else http.client.HTTPConnection
                conn = cls(self.url.hostname, self.url.port, timeout=2, source_address=(source, 0))
                with self.lock:
                    stopping = self.stopping.is_set()
                    if not stopping:
                        self.connections[source] = conn
                if stopping:
                    self._drop('drain deadline')
                    continue
                conn.request('POST', '/observations', body=json.dumps(observation, allow_nan=False),
                             headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.token})
                response = conn.getresponse()
                response.read()
                if not 200 <= response.status < 300:
                    self._drop(f'HTTP {response.status}')
                else:
                    with self.lock:
                        self.delivered += 1
            except (OSError, http.client.HTTPException, ValueError) as exc:
                self._drop(type(exc).__name__)
            finally:
                if conn is not None:
                    conn.close()
                with self.lock:
                    self.connections.pop(source, None)
                pending.task_done()

    def close(self, timeout=5):
        deadline = time.monotonic() + max(0, timeout - 2)
        while any(q.unfinished_tasks for q in self.queues.values()) and time.monotonic() < deadline:
            time.sleep(0.01)
        self.stopping.set()
        with self.lock:
            connections = list(self.connections.values())
        for conn in connections:
            if conn.sock is not None:
                try:
                    conn.sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                conn.close()
        for pending in self.queues.values():
            while True:
                try:
                    pending.get_nowait()
                except queue.Empty:
                    break
                self._drop('drain deadline')
                pending.task_done()
        # An in-flight socket has a two-second timeout. Wait for it before UE routes disappear.
        for thread in self.threads:
            thread.join()
        print(f'telemetry: delivered={self.delivered} dropped={self.dropped}', file=sys.stderr)


def request_with_telemetry(attempt, publisher, url, method, body, headers, source, timeout):
    result = attempt(url, method, body, headers, source, timeout)
    if publisher is not None:
        publisher.submit(source, result)
    return result
