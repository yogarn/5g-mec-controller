"""Offline HTTP-handler smoke benchmark; NOT UE/5G latency or calibrated capacity."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
from pathlib import Path
import platform
import time

import numpy as np
from app import create_app
from detector import Detector


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model', type=Path, required=True)
    parser.add_argument('--image', type=Path, required=True)
    parser.add_argument('--requests-per-client', type=int, default=10)
    parser.add_argument('--threads', type=int, default=1)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.requests_per_client < 1:
        parser.error('--requests-per-client must be positive')
    data = args.image.read_bytes()
    app = create_app(Detector(args.model, threads=args.threads))
    def client_run(_):
        records = []
        with app.test_client() as client:
            for _ in range(args.requests_per_client):
                start = time.perf_counter()
                response = client.post('/detect', data={'image': (io.BytesIO(data), args.image.name)})
                record = {'response_ms': (time.perf_counter() - start) * 1000,
                          'status': response.status_code, 'result': response.json}
                records.append(record)
        return records
    tiers = []
    for concurrency in (1, 3):
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            records = [r for group in pool.map(client_run, range(concurrency)) for r in group]
        tiers.append({'concurrency': concurrency, 'requests': records,
                      'p90_ms': float(np.percentile([r['response_ms'] for r in records], 90)),
                      'acceptance_100ms': sum(r['status'] == 200 and r['response_ms'] <= 100
                                             for r in records) / len(records)})
    result = {'scope': 'local in-process Flask test client; no network, UE, CPU quota, or Kubernetes',
              'python': platform.python_version(), 'image_sha256': hashlib.sha256(data).hexdigest(),
              'image_bytes': len(data), 'ort_threads': args.threads, 'tiers': tiers}
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps([{k: v for k, v in tier.items() if k != 'requests'} for tier in tiers]))


if __name__ == '__main__':
    main()
