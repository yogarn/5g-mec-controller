#!/usr/bin/env python3
"""Run a closed-loop face-detection ramp using owned UERANSIM sessions."""
import argparse
import hashlib
import json
import math
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

from benchmark.client import attempt, parse_schedule
from benchmark.telemetry import TelemetryPublisher, request_with_telemetry, telemetry_config
from experiments import defaults
from experiments.ue_lifecycle import OwnedUELifecycle


def run(args):
    phases = parse_schedule(args.phase_schedule)
    if any(phase['workload_ue'] > 50 for phase in phases):
        raise ValueError('at most 50 UEs are supported')
    if not args.armed:
        raise ValueError('pass --armed to create and replace UE sessions')
    if not all(math.isfinite(x) and x > 0 for x in (args.phase_duration, args.timeout)):
        raise ValueError('phase duration and timeout must be finite and positive')
    telemetry = telemetry_config(getattr(args, 'telemetry_url', None), getattr(args, 'telemetry_token_file', None))
    url = urlsplit(args.url)
    if (url.scheme not in ('http', 'https') or not url.hostname or url.username is not None
            or url.password is not None or url.fragment or any(c.isspace() for c in args.url)):
        raise ValueError('URL must be an absolute HTTP(S) URL without credentials or fragment')
    try:
        if url.port == 0:
            raise ValueError('invalid URL port')
    except ValueError as exc:
        raise ValueError(f'invalid URL port: {exc}') from exc
    image_path = Path(args.image)
    if image_path.is_dir():
        paths = sorted(path for path in image_path.iterdir()
                       if path.is_file() and path.suffix.lower() in
                       {'.jpg', '.jpeg', '.png', '.bmp', '.webp', '.gif', '.tif', '.tiff'})
    else:
        paths = [image_path]
    if not paths:
        raise ValueError('image directory contains no image files')
    images = []
    for path in paths:
        image = path.read_bytes()
        if not image:
            raise ValueError(f'image is empty: {path}')
        digest = hashlib.sha256(image).hexdigest()
        boundary = 'mec-face-' + digest[:32]
        body = (f'--{boundary}\r\nContent-Disposition: form-data; name="image"; filename="image"\r\n'
                f'Content-Type: application/octet-stream\r\n\r\n').encode() + image + f'\r\n--{boundary}--\r\n'.encode()
        images.append({'filename': path.name, 'sha256': digest, 'bytes': len(image),
                       'body': body, 'headers': {'Content-Type': f'multipart/form-data; boundary={boundary}'}})
    assignments = [i % len(images) for i in range(max(phase['workload_ue'] for phase in phases))]
    out = Path(args.output_dir)
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    metadata = {'url': args.url, 'schedule': phases, 'phase_duration_s': args.phase_duration,
                'timeout_s': args.timeout, 'ue_config': str(Path(args.ue_config).resolve()),
                'nr_ue': str(Path(args.nr_ue).resolve()), 'start_time': time.time(),
                'workload': 'closed-loop', 'switching': 'stop-and-reattach; not handover'}
    metadata['images'] = [{key: image[key] for key in ('filename', 'sha256', 'bytes')} for image in images]
    metadata['image_assignments'] = [
        {'ue_index': index, 'image_index': image_index, 'filename': images[image_index]['filename']}
        for index, image_index in enumerate(assignments)]
    if telemetry:
        metadata['telemetry_url'] = args.telemetry_url
    (out / 'run.json').write_text(json.dumps(metadata, indent=2) + '\n')
    lock = threading.Lock()
    with (out / 'requests.jsonl').open('x', encoding='utf-8') as output:
        def emit(record):
            with lock:
                output.write(json.dumps(record, allow_nan=False) + '\n')
                output.flush()

        lifecycle = OwnedUELifecycle(out / 'owned_ues', Path(args.ue_config), Path(args.nr_ue))
        try:
            for phase in phases:
                print(f"[..] {phase['phase']}: switching to {phase['gnb2_ues']} gNB-2, {phase['gnb3_ues']} gNB-3", flush=True)
                g2, g3 = lifecycle.switch_counts(phase['workload_ue'], phase['gnb2_ues'], phase['gnb3_ues'])
                if len(g2) != phase['gnb2_ues'] or len(g3) != phase['gnb3_ues']:
                    raise RuntimeError('registered UE count differs from requested split')
                selected = [(group, ue['ip'], index)
                            for index, (group, ue) in enumerate(
                                (group, ue) for group, ues in (('gnb-2', g2), ('gnb-3', g3)) for ue in ues)]
                publisher = TelemetryPublisher(telemetry, [source for _, source, _ in selected]) if telemetry else None
                start = time.time()
                deadline = time.monotonic() + args.phase_duration
                emit({'event': 'phase_start', **phase, 'start_time': start,
                      'duration_s': args.phase_duration, 'ues': [
                          {'group': group, 'source_address': ue['ip'], 'interface': ue['iface']}
                          for group, ues in (('gnb-2', g2), ('gnb-3', g3)) for ue in ues]})

                def worker(group, source, ue_index):
                    image = images[assignments[ue_index]]
                    count = successes = 0
                    while time.monotonic() < deadline:
                        record = request_with_telemetry(attempt, publisher, url, 'POST', image['body'], image['headers'], source, args.timeout)
                        count += 1
                        successes += record['success']
                        emit({'event': 'request', 'phase': phase['phase'], 'group': group,
                              'source_address': source, 'url': args.url, 'method': 'POST',
                              'ue_index': ue_index, 'image_index': assignments[ue_index],
                              'image_filename': image['filename'],
                              'attempt': count, **record})
                    return {'group': group, 'source_address': source,
                            'attempts': count, 'successes': successes}

                try:
                    with ThreadPoolExecutor(max_workers=len(selected)) as executor:
                        futures = [executor.submit(worker, group, source, index) for group, source, index in selected]
                        per_ue = [future.result() for future in futures]
                finally:
                    if publisher:
                        publisher.close()
                emit({'event': 'phase_end', **phase, 'start_time': start,
                      'generation_duration_s': args.phase_duration, 'end_time': time.time(),
                      'attempts': sum(ue['attempts'] for ue in per_ue),
                      'successes': sum(ue['successes'] for ue in per_ue), 'per_ue': per_ue})
                attempts = sum(ue['attempts'] for ue in per_ue)
                successes = sum(ue['successes'] for ue in per_ue)
                print(f"[--] {phase['phase']}: {attempts} requests, {successes} successful, {attempts - successes} failed", flush=True)
        finally:
            lifecycle.close()
    print(f"Results: {out / 'requests.jsonl'}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True, help='Face-detection URL using a reachable Kubernetes node IP and Service NodePort, e.g. http://NODE_IP:NODEPORT/detect')
    parser.add_argument('--image', default=str(defaults.IMAGE), help='Image file or directory of JPG, PNG, BMP, WebP, GIF or TIFF files; assigned in sorted order')
    parser.add_argument('--ue-config', default=str(defaults.INVENTORY), help='Private UE inventory YAML')
    parser.add_argument('--nr-ue', default=str(defaults.NR_UE), help='Absolute path to the UERANSIM nr-ue executable')
    parser.add_argument('--phase-schedule', default=defaults.SCHEDULE, help='Ordered total:gnb2:gnb3 counts, comma separated')
    parser.add_argument('--phase-duration', type=float, default=defaults.PHASE_DURATION, help='Request-generation seconds per phase')
    parser.add_argument('--timeout', type=float, default=10.0, help='Socket timeout in seconds')
    parser.add_argument('--output-dir', default=None, help='New output directory; defaults to a unique results directory')
    parser.add_argument('--telemetry-url', help='Collector HTTP(S) /observations URL')
    parser.add_argument('--telemetry-token-file', help='File containing the collector bearer token')
    parser.add_argument('--armed', action='store_true', help='Authorize creation and replacement of UE sessions')
    args = parser.parse_args(argv)
    if args.output_dir is None:
        args.output_dir = str(defaults.RESULTS / f'run-{time.time_ns()}')
    if not args.armed:
        print(f'UE sessions will be created and replaced; traffic will be sent to {args.url}.')
        print(f'Schedule: {args.phase_schedule}; {args.phase_duration}s per phase; output: {args.output_dir}')
        print(f'Inventory: {args.ue_config}; image: {args.image}; nr-ue: {args.nr_ue}')
        if not sys.stdin.isatty() or input('Start this experiment? [y/N] ').strip().lower() != 'y':
            parser.exit(2, 'Experiment not started; pass --armed for non-interactive execution.\n')
        args.armed = True
    def stop(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    try:
        run(args)
    except (ValueError, OSError, RuntimeError) as exc:
        parser.exit(2, f'error: {exc}\n')


if __name__ == '__main__':
    main()
