#!/usr/bin/env python3
"""Run a closed-loop face-detection ramp using owned UERANSIM sessions."""
import argparse
import hashlib
import json
import math
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

from benchmark.client import attempt, parse_schedule
from experiments.ue_lifecycle import OwnedUELifecycle


def run(args):
    phases = parse_schedule(args.phase_schedule)
    if any(phase['workload_ue'] > 50 for phase in phases):
        raise ValueError('at most 50 UEs are supported')
    if not args.armed:
        raise ValueError('pass --armed to create and replace UE sessions')
    if not all(math.isfinite(x) and x > 0 for x in (args.phase_duration, args.timeout)):
        raise ValueError('phase duration and timeout must be finite and positive')
    url = urlsplit(args.url)
    if (url.scheme not in ('http', 'https') or not url.hostname or url.username is not None
            or url.password is not None or url.fragment or any(c.isspace() for c in args.url)):
        raise ValueError('URL must be an absolute HTTP(S) URL without credentials or fragment')
    try:
        if url.port == 0:
            raise ValueError('invalid URL port')
    except ValueError as exc:
        raise ValueError(f'invalid URL port: {exc}') from exc
    image = Path(args.image).read_bytes()
    if not image:
        raise ValueError('image is empty')
    boundary = 'mec-face-' + hashlib.sha256(image).hexdigest()[:32]
    body = (f'--{boundary}\r\nContent-Disposition: form-data; name="image"; filename="image"\r\n'
            f'Content-Type: application/octet-stream\r\n\r\n').encode() + image + f'\r\n--{boundary}--\r\n'.encode()
    headers = {'Content-Type': f'multipart/form-data; boundary={boundary}'}
    out = Path(args.output_dir)
    out.mkdir(mode=0o700, parents=False, exist_ok=False)
    metadata = {'url': args.url, 'schedule': phases, 'phase_duration_s': args.phase_duration,
                'timeout_s': args.timeout, 'image_sha256': hashlib.sha256(image).hexdigest(),
                'image_bytes': len(image), 'ue_config': str(Path(args.ue_config).resolve()),
                'nr_ue': str(Path(args.nr_ue).resolve()), 'start_time': time.time(),
                'workload': 'closed-loop', 'switching': 'stop-and-reattach; not handover'}
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
                print(f"Switching {phase['phase']}: {phase['gnb2_ues']} gnb-2, {phase['gnb3_ues']} gnb-3", flush=True)
                g2, g3 = lifecycle.switch_counts(phase['workload_ue'], phase['gnb2_ues'], phase['gnb3_ues'])
                if len(g2) != phase['gnb2_ues'] or len(g3) != phase['gnb3_ues']:
                    raise RuntimeError('registered UE count differs from requested split')
                selected = [(group, ue['ip']) for group, ues in (('gnb-2', g2), ('gnb-3', g3)) for ue in ues]
                start = time.time()
                deadline = time.monotonic() + args.phase_duration
                emit({'event': 'phase_start', **phase, 'start_time': start,
                      'duration_s': args.phase_duration, 'ues': [
                          {'group': group, 'source_address': ue['ip'], 'interface': ue['iface']}
                          for group, ues in (('gnb-2', g2), ('gnb-3', g3)) for ue in ues]})

                def worker(group, source):
                    count = successes = 0
                    while time.monotonic() < deadline:
                        record = attempt(url, 'POST', body, headers, source, args.timeout)
                        count += 1
                        successes += record['success']
                        emit({'event': 'request', 'phase': phase['phase'], 'group': group,
                              'source_address': source, 'url': args.url, 'method': 'POST',
                              'attempt': count, **record})
                    return {'group': group, 'source_address': source,
                            'attempts': count, 'successes': successes}

                with ThreadPoolExecutor(max_workers=len(selected)) as executor:
                    futures = [executor.submit(worker, group, source) for group, source in selected]
                    per_ue = [future.result() for future in futures]
                emit({'event': 'phase_end', **phase, 'start_time': start,
                      'generation_duration_s': args.phase_duration, 'end_time': time.time(),
                      'attempts': sum(ue['attempts'] for ue in per_ue),
                      'successes': sum(ue['successes'] for ue in per_ue), 'per_ue': per_ue})
                print(f"Completed {phase['phase']}: {sum(ue['attempts'] for ue in per_ue)} attempts", flush=True)
        finally:
            lifecycle.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True, help='Face-detection /detect URL through the intended UE path')
    parser.add_argument('--image', required=True, help='Fixed image file sent as multipart field image')
    parser.add_argument('--ue-config', required=True, help='UE inventory YAML; contains paths to private subscriber configs')
    parser.add_argument('--nr-ue', required=True, help='Absolute path to the UERANSIM nr-ue executable')
    parser.add_argument('--phase-schedule', required=True, help='Ordered total:gnb2:gnb3 counts, comma separated')
    parser.add_argument('--phase-duration', required=True, type=float, help='Request-generation seconds per phase')
    parser.add_argument('--timeout', type=float, default=10.0, help='Socket timeout in seconds')
    parser.add_argument('--output-dir', required=True, help='New private output directory; must not exist')
    parser.add_argument('--armed', action='store_true', help='Authorize creation and replacement of UE sessions')
    args = parser.parse_args(argv)
    def stop(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    try:
        run(args)
    except (ValueError, OSError, RuntimeError) as exc:
        parser.exit(2, f'error: {exc}\n')


if __name__ == '__main__':
    main()
