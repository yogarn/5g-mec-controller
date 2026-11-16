import importlib.util
import io
import os
from pathlib import Path
import sys

import numpy as np
from PIL import Image
import pytest

APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_DIR))
from app import create_app
from detector import Detector, letterbox, person_boxes


def payload(size=(640, 480)):
    stream = io.BytesIO()
    Image.new('RGB', size, 'white').save(stream, format='JPEG')
    stream.seek(0)
    return {'image': (stream, 'road.jpg')}


def test_letterbox_and_person_nms_restore_original_coordinates():
    tensor, transform = letterbox(Image.new('RGB', (640, 320)), 320)
    assert tensor.shape == (1, 3, 320, 320)
    assert tensor.dtype == np.float32
    output = np.zeros((1, 84, 4), dtype=np.float32)
    output[0, :4, :] = np.array([[160, 160, 80, 10], [160, 160, 160, 10],
                                 [80, 80, 20, 0], [80, 80, 20, 0]])
    output[0, 4, :] = [.9, .8, .7, .9]
    output[0, 6, 2] = .95  # A different winning class must not become a person.
    boxes = person_boxes(output, (640, 320), transform)
    assert len(boxes) == 1
    assert boxes[0]['x'] == 240
    assert boxes[0]['y'] == 80
    assert boxes[0]['w'] == boxes[0]['h'] == 160


def test_runtime_and_http_contract_with_real_onnx_session(tmp_path):
    # Small constant graph tests the actual ORT path without downloading weights.
    onnx = pytest.importorskip('onnx')
    from onnx import helper, TensorProto, numpy_helper
    prediction = np.zeros((1, 84, 1), dtype=np.float32)
    prediction[0, :5, 0] = [160, 160, 80, 100, .9]
    graph = helper.make_graph(
        [helper.make_node('Constant', [], ['output'], value=numpy_helper.from_array(prediction))],
        'test-person', [helper.make_tensor_value_info('images', TensorProto.FLOAT, [1, 3, 320, 320])],
        [helper.make_tensor_value_info('output', TensorProto.FLOAT, [1, 84, 1])])
    model = helper.make_model(graph, opset_imports=[helper.make_opsetid('', 17)])
    model.ir_version = 10
    path = tmp_path / 'test.onnx'
    onnx.save(model, path)
    detector = Detector(path)
    client = create_app(detector).test_client()
    assert client.get('/health').json['model_sha256'] == detector.model_sha256
    result = client.post('/detect', data=payload())
    assert result.status_code == 200
    assert result.json['person_count'] == 1
    assert result.json['processing_ms'] >= 0
    assert result.json['persons'][0]['w'] == 160
    assert result.json['server_pod'] == os.getenv('POD_NAME', None)
    assert result.json['server_node'] == os.getenv('NODE_NAME', None)
    assert isinstance(result.json['request_id'], str) and result.json['request_id']
    summed = (result.json['body_read_ms'] + result.json['decode_ms']
              + result.json['lock_wait_ms'] + result.json['inference_ms'])
    assert abs(summed - result.json['processing_ms']) < 0.05
    assert client.post('/detect').status_code == 400
    assert client.post('/detect', data={'image': (io.BytesIO(b'bad'), 'bad.jpg')}).status_code == 400
    assert client.post('/detect', data=b'x' * (4 * 1024 * 1024 + 1), content_type='application/octet-stream').status_code == 413


def test_bad_export_shape_fails_instead_of_silent_detections():
    with pytest.raises(ValueError, match='COCO output'):
        person_boxes(np.zeros((1, 300, 6)), (640, 480), (1, 1, 0, 0))


def test_health_listener_stays_responsive_while_detect_is_blocked():
    import json
    import threading
    from urllib.request import urlopen
    from werkzeug.serving import make_server
    from app import HealthState

    entered = threading.Event()
    release = threading.Event()

    class BlockingDetector:
        model_sha256 = 'deadbeef'
        size = 320

        def predict(self, _image):
            entered.set()
            assert release.wait(timeout=5)
            return []

    flask_app = create_app(BlockingDetector(), health_port=0)
    health_server = flask_app.extensions['health_server']
    health_port = health_server.server_address[1]
    api_server = make_server('127.0.0.1', 0, flask_app, threaded=False)
    api_thread = threading.Thread(target=api_server.serve_forever, daemon=True)
    api_thread.start()
    try:
        # Send /detect on the single-threaded API server; it blocks inside predict().
        boundary = 'testboundary'
        img_bytes = payload()['image'][0].read()
        body = (
            f'--{boundary}\r\nContent-Disposition: form-data; name="image"; filename="road.jpg"\r\n'
            f'Content-Type: image/jpeg\r\n\r\n'
        ).encode() + img_bytes + f'\r\n--{boundary}--\r\n'.encode()
        detect_result = {}

        def call_detect():
            from urllib.request import Request
            req = Request(
                f'http://127.0.0.1:{api_server.server_port}/detect',
                data=body,
                headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
                method='POST',
            )
            with urlopen(req, timeout=5) as resp:
                detect_result['status'] = resp.status

        t = threading.Thread(target=call_detect, daemon=True)
        t.start()
        assert entered.wait(timeout=3)

        # Health listener on dedicated port responds immediately while /detect is blocked.
        with urlopen(f'http://127.0.0.1:{health_port}/health', timeout=1) as resp:
            assert resp.status == 200
            assert json.loads(resp.read())['status'] == 'ok'
        with urlopen(f'http://127.0.0.1:{health_port}/ready', timeout=1) as resp:
            assert resp.status == 200

        release.set()
        t.join(timeout=3)
        assert detect_result['status'] == 200
    finally:
        release.set()
        api_server.shutdown()
        api_server.server_close()
        health_server.shutdown()
        health_server.server_close()


def test_health_reflects_startup_hang_and_worker_failure(monkeypatch, tmp_path):
    from app import HealthState

    state = HealthState(hang_timeout_s=0.05)
    code, body = state.status(check_hang=True)
    assert code == 503 and body['status'] == 'starting'

    state.mark_ready('abc', 320)
    assert state.status(check_hang=True)[0] == 200

    state.begin_inference()
    import time
    time.sleep(0.08)
    code, body = state.status(check_hang=True)
    assert code == 503 and body['status'] == 'hung'
    state.end_inference()

    # Verify HTTP /health and /ready both return 503 when watchdog expires on live listener.
    from urllib.error import HTTPError
    from urllib.request import urlopen
    hung_app = create_app(type('D', (), {'model_sha256': 'abc', 'size': 320, 'predict': lambda self, img: []})(), health_port=0)
    hung_server = hung_app.extensions['health_server']
    hung_port = hung_server.server_address[1]
    try:
        hung_state = hung_app.extensions['health_state']
        hung_state.hang_timeout_s = 0.02
        hung_state.begin_inference()
        time.sleep(0.05)
        for path in ('/health', '/ready'):
            with pytest.raises(HTTPError) as exc:
                urlopen(f'http://127.0.0.1:{hung_port}{path}', timeout=1)
            assert exc.value.code == 503
        hung_state.end_inference()
    finally:
        hung_server.shutdown()
        hung_server.server_close()

    class FailingDetector:
        model_sha256 = 'abc'
        size = 320

        def predict(self, _image):
            raise RuntimeError('ort failure')

    app = create_app(FailingDetector())
    app.config['TESTING'] = True
    client = app.test_client()
    assert client.get('/health').status_code == 200
    with pytest.raises(RuntimeError, match='ort failure'):
        client.post('/detect', data=payload())
    assert client.get('/health').status_code == 503
    assert client.get('/ready').status_code == 503

    monkeypatch.setenv('MODEL_PATH', str(tmp_path / 'missing.onnx'))
    with pytest.raises(Exception):
        create_app()
