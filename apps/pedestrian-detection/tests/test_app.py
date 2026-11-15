import importlib.util
import io
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
    assert client.post('/detect').status_code == 400
    assert client.post('/detect', data={'image': (io.BytesIO(b'bad'), 'bad.jpg')}).status_code == 400
    assert client.post('/detect', data=b'x' * (4 * 1024 * 1024 + 1), content_type='application/octet-stream').status_code == 413


def test_bad_export_shape_fails_instead_of_silent_detections():
    with pytest.raises(ValueError, match='COCO output'):
        person_boxes(np.zeros((1, 300, 6)), (640, 480), (1, 1, 0, 0))
