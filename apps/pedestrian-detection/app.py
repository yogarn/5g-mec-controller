"""HTTP image detection compatible with the existing UE multipart requests."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
import threading
import time
import uuid
import warnings

from flask import Flask, jsonify, request
from PIL import Image, ImageOps, UnidentifiedImageError


class HealthState:
    """Thread-safe model and inference-worker health state."""

    def __init__(self, hang_timeout_s=30.0, spill_high=None, spill_low=None):
        self._lock = threading.Lock()
        self.ready = False
        self.failed = False
        self.error = None
        self.model_sha256 = None
        self.input_size = None
        self.inference_started_at = None
        self.hang_timeout_s = float(hang_timeout_s)
        self.spill_high = spill_high
        self.spill_low = spill_low
        self.pending = 0
        self.saturated = False

    def mark_ready(self, model_sha256, input_size):
        with self._lock:
            self.ready = True
            self.failed = False
            self.error = None
            self.model_sha256 = model_sha256
            self.input_size = input_size

    def mark_failed(self, error):
        with self._lock:
            self.ready = False
            self.failed = True
            self.error = str(error)

    def begin_inference(self):
        with self._lock:
            self.inference_started_at = time.monotonic()

    def end_inference(self):
        with self._lock:
            self.inference_started_at = None

    def update_pending(self, delta):
        with self._lock:
            self.pending += delta
            if self.pending >= self.spill_high:
                self.saturated = True
            elif self.pending <= self.spill_low:
                self.saturated = False

    def status(self, check_hang=True, check_saturation=False):
        with self._lock:
            if self.failed:
                return 503, {'status': 'error', 'error': self.error or 'worker failed'}
            if not self.ready:
                return 503, {'status': 'starting'}
            if check_hang and self.inference_started_at is not None:
                elapsed = time.monotonic() - self.inference_started_at
                if elapsed > self.hang_timeout_s:
                    return 503, {'status': 'hung', 'elapsed_seconds': round(elapsed, 3)}
            if check_saturation and self.saturated:
                return 503, {'status': 'saturated', 'pending': self.pending}
            return 200, {'status': 'ok', 'model_sha256': self.model_sha256, 'input_size': self.input_size}


def start_health_server(state, host='0.0.0.0', port=5001):
    """Run a dedicated health HTTP listener independent of the Gunicorn inference queue."""
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ('/ready', '/health'):
                code, payload = state.status(check_saturation=self.path == '/ready')
            else:
                self.send_response(404)
                self.end_headers()
                return
            body = json.dumps(payload).encode('utf-8')
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer((host, int(port)), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name='health-listener')
    thread.start()
    return server


def create_app(detector=None, health_port=None):
    spill_high = spill_low = None
    if os.getenv('SPILL_HIGH'):
        try:
            spill_high = int(os.environ['SPILL_HIGH'])
            spill_low = int(os.environ.get('SPILL_LOW', ''))
            if not 0 <= spill_low < spill_high:
                raise ValueError
        except ValueError as exc:
            raise ValueError('SPILL_HIGH and SPILL_LOW must be integers with 0 <= SPILL_LOW < SPILL_HIGH') from exc
    state = HealthState(hang_timeout_s=float(os.getenv('HEALTH_INFERENCE_HANG_SECONDS', '30')),
                        spill_high=spill_high, spill_low=spill_low)
    if health_port is None:
        env_port = os.getenv('HEALTH_PORT', '').strip()
        if env_port:
            health_port = int(env_port)
    health_server = start_health_server(state, port=health_port) if health_port is not None else None

    if detector is None:
        from detector import Detector
        try:
            detector = Detector(os.getenv('MODEL_PATH', '/app/models/yolo11n-320.onnx'),
                                threads=int(os.getenv('ORT_THREADS', '1')),
                                confidence=float(os.getenv('CONFIDENCE', '.25')),
                                iou=float(os.getenv('NMS_IOU', '.45')))
        except Exception as exc:
            state.mark_failed(exc)
            raise
    state.mark_ready(detector.model_sha256, detector.size)

    inference_lock = threading.Lock()
    app = Flask(__name__)
    app.config['MAX_CONTENT_LENGTH'] = 4 * 1024 * 1024
    app.extensions['health_state'] = state
    app.extensions['health_server'] = health_server

    @app.get('/health')
    def health():
        code, payload = state.status(check_hang=True)
        return jsonify(payload), code

    @app.get('/ready')
    def ready():
        code, payload = state.status(check_saturation=True)
        return jsonify(payload), code

    @app.post('/detect')
    def detect():
        if spill_high is None:
            return detect_image()
        state.update_pending(1)
        try:
            return detect_image()
        finally:
            state.update_pending(-1)

    def detect_image():
        started = time.perf_counter()
        request_id = request.headers.get('X-Request-ID') or uuid.uuid4().hex
        if 'image' not in request.files:
            return jsonify(error="missing 'image' file field", request_id=request_id), 400
        raw_bytes = request.files['image'].read()
        after_read = time.perf_counter()
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('error', Image.DecompressionBombWarning)
                with Image.open(io.BytesIO(raw_bytes)) as source:
                    if source.width * source.height > 16_000_000:
                        return jsonify(error='image exceeds 16 megapixels', request_id=request_id), 413
                    image = ImageOps.exif_transpose(source).convert('RGB')
        except (UnidentifiedImageError, OSError, ValueError,
                Image.DecompressionBombError, Image.DecompressionBombWarning):
            return jsonify(error='could not decode image', request_id=request_id), 400
        after_decode = time.perf_counter()
        with inference_lock:
            after_lock = time.perf_counter()
            state.begin_inference()
            try:
                boxes = detector.predict(image)
            except Exception as exc:
                state.mark_failed(exc)
                raise
            finally:
                state.end_inference()
            after_infer = time.perf_counter()
        return jsonify(
            person_count=len(boxes),
            persons=boxes,
            request_id=request_id,
            server_pod=os.getenv('POD_NAME') or None,
            server_node=os.getenv('NODE_NAME') or None,
            processing_ms=(after_infer - started) * 1000,
            body_read_ms=(after_read - started) * 1000,
            decode_ms=(after_decode - after_read) * 1000,
            lock_wait_ms=(after_lock - after_decode) * 1000,
            inference_ms=(after_infer - after_lock) * 1000,
            model_sha256=detector.model_sha256,
            input_size=detector.size,
        )

    return app
