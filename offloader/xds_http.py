"""HTTP listeners for Envoy xDS discovery (/v3/discovery:*) and authenticated admin API."""
import hmac
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from offloader.state import RouteSwitchBusy


def _check_bearer_auth(headers, expected_token: str) -> bool:
    if not expected_token:
        return False
    auth = headers.get('Authorization', '')
    return hmac.compare_digest(auth.encode('utf-8'), f'Bearer {expected_token}'.encode('utf-8'))


class _JsonHandler(BaseHTTPRequestHandler):
    """Shared JSON request/response helpers for xDS and admin HTTP handlers."""

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
        return json.loads(self.rfile.read(length).decode('utf-8'))

    def log_message(self, format, *args):
        return


def make_xds_discovery_handler(controller):
    """HTTP handler exposing ONLY Envoy xDS discovery endpoints (no admin mutation surface)."""

    class Handler(_JsonHandler):
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

    return Handler


def _dispatch_admin_post(controller, path: str, body: dict) -> tuple[int, dict]:
    if path == '/override':
        try:
            return 200, controller.manual_override(body['site'], body.get('route'))
        except PermissionError as exc:
            return 403, {'error': str(exc)}
        except RouteSwitchBusy as exc:
            return 429, {'error': str(exc)}
        except (KeyError, ValueError) as exc:
            return 400, {'error': str(exc)}
    if path == '/reset':
        try:
            return 200, controller.reset(mode=body.get('mode'))
        except ValueError as exc:
            return 400, {'error': str(exc)}
    if path == '/mode':
        try:
            return 200, controller.set_mode(body['mode'])
        except (KeyError, ValueError) as exc:
            return 400, {'error': str(exc)}
    return 404, {'error': 'not found'}


def make_admin_handler(controller, admin_token: str):
    """HTTP handler exposing authenticated admin/control endpoints separated from xDS discovery."""
    if not isinstance(admin_token, str) or not admin_token.strip() or '\n' in admin_token or '\r' in admin_token:
        raise ValueError('admin_token must be a nonempty single-line string')

    class Handler(_JsonHandler):
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
            status, payload = _dispatch_admin_post(controller, self.path, body)
            self._reply_json(status, payload)

    return Handler


def start_xds_server(controller, host: str = '0.0.0.0', port: int = 8085) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, int(port)), make_xds_discovery_handler(controller))
    threading.Thread(target=server.serve_forever, daemon=True, name='offloader-xds').start()
    return server


def start_admin_server(controller, admin_token: str, host: str = '0.0.0.0', port: int = 8086) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, int(port)), make_admin_handler(controller, admin_token))
    threading.Thread(target=server.serve_forever, daemon=True, name='offloader-admin').start()
    return server
