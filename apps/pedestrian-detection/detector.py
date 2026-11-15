"""CPU-only YOLO11 COCO person detection; no runtime weight downloads."""
import hashlib
from pathlib import Path
import threading

import numpy as np
from PIL import Image


def letterbox(image, size):
    width, height = image.size
    scale = min(size / width, size / height)
    resized = (max(1, round(width * scale)), max(1, round(height * scale)))
    left, top = (size - resized[0]) // 2, (size - resized[1]) // 2
    canvas = Image.new('RGB', (size, size), (114, 114, 114))
    canvas.paste(image.resize(resized, Image.Resampling.BILINEAR), (left, top))
    tensor = np.asarray(canvas, dtype=np.float32).transpose(2, 0, 1)[None] / 255.0
    return np.ascontiguousarray(tensor), (resized[0] / width, resized[1] / height, left, top)


def person_boxes(output, original_size, transform, confidence=.25, iou=.45):
    """Decode raw [1,84,N] COCO output, then suppress overlapping person boxes."""
    predictions = np.asarray(output)
    if predictions.ndim != 3 or predictions.shape[:2] != (1, 84):
        raise ValueError('expected raw YOLO11 COCO output [1,84,N]; export with nms=False')
    rows = predictions[0].T
    # COCO person is class 0; retain only candidates whose winning class is person.
    keep = (rows[:, 4] >= confidence) & (rows[:, 4:].argmax(axis=1) == 0)
    rows = rows[keep & np.isfinite(rows).all(axis=1)]
    if not len(rows):
        return []
    boxes = np.column_stack((rows[:, :2] - rows[:, 2:4] / 2,
                             rows[:, :2] + rows[:, 2:4] / 2))
    sx, sy, left, top = transform
    boxes[:, [0, 2]] = (boxes[:, [0, 2]] - left) / sx
    boxes[:, [1, 3]] = (boxes[:, [1, 3]] - top) / sy
    width, height = original_size
    boxes[:, [0, 2]] = boxes[:, [0, 2]].clip(0, width)
    boxes[:, [1, 3]] = boxes[:, [1, 3]].clip(0, height)
    scores = rows[:, 4]
    valid = (boxes[:, 2] > boxes[:, 0]) & (boxes[:, 3] > boxes[:, 1])
    boxes, scores = boxes[valid], scores[valid]
    order = scores.argsort()[::-1]
    selected = []
    while len(order) and len(selected) < 100:
        current = order[0]
        selected.append(int(current))
        rest = order[1:]
        intersection = np.maximum(0, np.minimum(boxes[current, 2:], boxes[rest, 2:]) -
                                  np.maximum(boxes[current, :2], boxes[rest, :2])).prod(axis=1)
        areas = (boxes[:, 2:] - boxes[:, :2]).prod(axis=1)
        overlap = intersection / np.maximum(areas[current] + areas[rest] - intersection, 1e-9)
        order = rest[overlap <= iou]
    return [{'x': float(boxes[j, 0]), 'y': float(boxes[j, 1]),
             'w': float(boxes[j, 2] - boxes[j, 0]), 'h': float(boxes[j, 3] - boxes[j, 1]),
             'confidence': float(scores[j])} for j in selected]


class Detector:
    def __init__(self, model_path, threads=1, confidence=.25, iou=.45):
        import onnxruntime as ort
        if threads < 1 or not 0 < confidence < 1 or not 0 < iou < 1:
            raise ValueError('invalid detector settings')
        path = Path(model_path)
        self.model_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        options = ort.SessionOptions()
        options.intra_op_num_threads = threads
        options.inter_op_num_threads = 1
        options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        options.add_session_config_entry('session.intra_op.allow_spinning', '0')
        self.session = ort.InferenceSession(str(path), options, providers=['CPUExecutionProvider'])
        inputs = self.session.get_inputs()
        shape = inputs[0].shape
        if len(inputs) != 1 or len(shape) != 4 or shape[:2] != [1, 3] or shape[2] != shape[3] or not isinstance(shape[2], int):
            raise ValueError('model must have fixed input [1,3,S,S]')
        if inputs[0].type != 'tensor(float)':
            raise ValueError('model input must be float32')
        self.size, self.input_name = shape[2], inputs[0].name
        self.confidence, self.iou = confidence, iou
        self.lock = threading.Lock()
        # Validate output and warm up before HTTP readiness is available.
        self.predict(Image.new('RGB', (self.size, self.size)))

    def predict(self, image):
        tensor, transform = letterbox(image, self.size)
        with self.lock:
            output = self.session.run(None, {self.input_name: tensor})[0]
        return person_boxes(output, image.size, transform, self.confidence, self.iou)
