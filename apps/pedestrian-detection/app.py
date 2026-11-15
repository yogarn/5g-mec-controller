"""HTTP image detection compatible with the existing UE multipart requests."""
import io
import os
import time
import warnings

from flask import Flask, jsonify, request
from PIL import Image, ImageOps, UnidentifiedImageError


def create_app(detector=None):
    if detector is None:
        from detector import Detector
        detector = Detector(os.getenv('MODEL_PATH', '/app/models/yolo11n-320.onnx'),
                            threads=int(os.getenv('ORT_THREADS', '1')),
                            confidence=float(os.getenv('CONFIDENCE', '.25')),
                            iou=float(os.getenv('NMS_IOU', '.45')))
    app = Flask(__name__)
    app.config['MAX_CONTENT_LENGTH'] = 4 * 1024 * 1024

    @app.get('/health')
    def health():
        return jsonify(status='ok', model_sha256=detector.model_sha256,
                       input_size=detector.size)

    @app.post('/detect')
    def detect():
        started = time.perf_counter()
        if 'image' not in request.files:
            return jsonify(error="missing 'image' file field"), 400
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('error', Image.DecompressionBombWarning)
                with Image.open(io.BytesIO(request.files['image'].read())) as source:
                    if source.width * source.height > 16_000_000:
                        return jsonify(error='image exceeds 16 megapixels'), 413
                    image = ImageOps.exif_transpose(source).convert('RGB')
        except (UnidentifiedImageError, OSError, ValueError,
                Image.DecompressionBombError, Image.DecompressionBombWarning):
            return jsonify(error='could not decode image'), 400
        boxes = detector.predict(image)
        return jsonify(person_count=len(boxes), persons=boxes,
                       processing_ms=(time.perf_counter() - started) * 1000,
                       model_sha256=detector.model_sha256, input_size=detector.size)

    return app
