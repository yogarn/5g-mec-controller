"""Short, journalled UE-to-application checks; no observations enter telemetry."""
import hashlib
import json
from pathlib import Path

from benchmark.client import attempt
from utils.experiment import validate_site_urls
from utils.ue_lifecycle import OwnedUELifecycle


def probe(settings, output):
    urls = validate_site_urls(settings['site_urls'])
    images = sorted(p for p in Path(settings['image']).iterdir()
                    if p.suffix.lower() in {'.png', '.jpg', '.jpeg', '.bmp', '.webp', '.gif', '.tiff'})
    if not images:
        raise ValueError('connectivity probe requires an image')
    image = images[0].read_bytes()
    boundary = 'mec-probe-' + hashlib.sha256(image).hexdigest()[:16]
    body = (f'--{boundary}\r\nContent-Disposition: form-data; name="image"; filename="image"\r\n'
            'Content-Type: application/octet-stream\r\n\r\n').encode() + image + f'\r\n--{boundary}--\r\n'.encode()
    headers = {'Content-Type': f'multipart/form-data; boundary={boundary}'}
    lifecycle = OwnedUELifecycle(output / 'owned_ues', Path(settings['inventory']), Path(settings['nr_ue']))
    try:
        # Reattach once: a successful initial attachment alone missed the previous failure.
        durations = []
        with (output / 'requests.jsonl').open('x') as stream:
            for cycle in (1, 2):
                print(f'[CHECK] UE connectivity cycle {cycle}/2: registering one UE per gNB', flush=True)
                groups = lifecycle.switch_counts(3, 1, 1, 1)
                for site, group in zip(('gnb-1', 'gnb-2', 'gnb-3'), groups):
                    source = group[0]['ip']
                    result = attempt(urls[site], 'POST', body, headers, source, 10)
                    stream.write(json.dumps({'cycle': cycle, 'site': site, 'source': source,
                                             'rt_ms': result['duration_ms'], **result}) + '\n')
                    stream.flush()
                    if not result['success']:
                        raise RuntimeError(f'{site} inference probe failed: HTTP {result["http_status"]}; {result["error"]}')
                    durations.append(result['duration_ms'])
                    proc = f', processing={result["processing_ms"]:.1f}ms' if 'processing_ms' in result else ''
                    print(f'[OK] {site}: cycle={cycle}, UE={source}, inference HTTP={result["http_status"]}, '
                          f'rt={result["duration_ms"]:.1f}ms{proc}', flush=True)
        if durations:
            mean_rt = sum(durations) / len(durations)
            print(f'[OK] UE probe RT summary: count={len(durations)}, min={min(durations):.1f}ms, '
                  f'mean={mean_rt:.1f}ms, max={max(durations):.1f}ms', flush=True)
    finally:
        lifecycle.close()
    print('[OK] UE probe sessions and routes cleaned up', flush=True)
