"""Explicit local export step, separate from the lean runtime environment."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import shutil


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--weights', default='yolo11n.pt', help='local checkpoint or official Ultralytics model name')
    parser.add_argument('--size', type=int, default=320, choices=(320, 640))
    args = parser.parse_args()
    from ultralytics import YOLO
    import torch
    torch.set_num_threads(1)
    model = YOLO(args.weights)
    if model.names.get(0) != 'person' or len(model.names) != 80:
        raise ValueError('requires standard 80-class COCO checkpoint with class 0 person')
    exported = Path(model.export(format='onnx', imgsz=args.size, batch=1, device='cpu',
                                 dynamic=False, simplify=False, nms=False, opset=17))
    output = Path(__file__).resolve().parent / 'models' / f'yolo11n-{args.size}.onnx'
    output.parent.mkdir(exist_ok=True)
    shutil.copyfile(exported, output)
    checkpoint = Path(model.ckpt_path)
    metadata = {'architecture': 'YOLO11n', 'task': 'COCO person detection',
                'input_size': args.size, 'weights': str(checkpoint),
                'weights_sha256': hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
                'onnx_sha256': hashlib.sha256(output.read_bytes()).hexdigest(),
                'ultralytics_version': importlib.metadata.version('ultralytics'),
                'torch_version': torch.__version__, 'opset': 17, 'nms': False,
                'dynamic': False}
    output.with_suffix('.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print(output)


if __name__ == '__main__':
    main()
