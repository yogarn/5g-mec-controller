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
import concurrent.futures
from pathlib import Path
from urllib.parse import urlsplit

from benchmark.client import attempt, parse_schedule
from benchmark.telemetry import TelemetryPublisher, request_with_telemetry, telemetry_config
from utils import defaults
from utils.ue_lifecycle import OwnedUELifecycle

DEFAULT_RPS_PER_UE = 5.0
DEFAULT_MAX_IN_FLIGHT_PER_UE = 8


def validate_url(value):
    if not isinstance(value, str):
        raise ValueError('URL must be a string')
    url = urlsplit(value)
    if (url.scheme not in ('http', 'https') or not url.hostname or url.username is not None
            or url.password is not None or url.fragment or any(c.isspace() for c in value)):
        raise ValueError('URL must be an absolute HTTP(S) URL without credentials or fragment')
    try:
        if url.port == 0:
            raise ValueError('invalid URL port')
    except ValueError as exc:
        raise ValueError(f'invalid URL port: {exc}') from exc
    return url


def validate_site_urls(site_urls):
    if not isinstance(site_urls, dict) or set(site_urls) != {'gnb-1', 'gnb-2', 'gnb-3'}:
        raise ValueError('site URLs must map exactly gnb-1, gnb-2 and gnb-3 to URLs')
    return {group: validate_url(url) for group, url in site_urls.items()}


def run_fixed_rate_phase(
    phase_name,
    selected,
    phase_duration,
    rps_per_ue,
    max_in_flight_per_ue,
    dispatch_fn,
    emit,
    target_url_for,
    images,
    assignments,
    *,
    cancelled=None,
    monotonic_fn=time.monotonic,
    wait_fn=None,
):
    """Run one phase with deterministic staggered fixed-rate arrivals per UE."""
    if cancelled is None:
        cancelled = threading.Event()
    if wait_fn is None:
        wait_fn = cancelled.wait
    period = 1.0 / rps_per_ue
    lateness_tolerance = 0.25 * period
    num_ues = len(selected)
    phase_start_mono = monotonic_fn()
    phase_end_mono = phase_start_mono + phase_duration

    def worker(pos, group, source, ue_index):
        image_index = assignments[ue_index]
        image = images[image_index]
        target_url = target_url_for(group)
        offset = (pos / num_ues) * period
        inflight_lock = threading.Lock()
        ue_lock = threading.Lock()
        in_flight = 0
        scheduled = started = completed = successes = missed = 0
        futures = []

        def run_one(slot_index, scheduled_mono):
            nonlocal started, completed, successes, missed, in_flight
            try:
                with ue_lock:
                    actual_start = monotonic_fn()
                    if (cancelled.is_set() or actual_start >= phase_end_mono
                            or actual_start > scheduled_mono + lateness_tolerance):
                        missed += 1
                        return
                    started += 1
                record = dispatch_fn(group, source, ue_index, image)
                with ue_lock:
                    completed += 1
                    successes += int(record['success'])
                    emit({
                        'event': 'request',
                        'phase': phase_name,
                        'group': group,
                        'source_address': source,
                        'url': target_url,
                        'method': 'POST',
                        'ue_index': ue_index,
                        'image_index': image_index,
                        'image_filename': image['filename'],
                        'slot': slot_index,
                        'scheduled_offset_s': scheduled_mono - phase_start_mono,
                        'dispatch_offset_s': actual_start - phase_start_mono,
                        'attempt': completed,
                        **record,
                    })
            finally:
                with inflight_lock:
                    in_flight -= 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_in_flight_per_ue) as req_pool:
            slot_index = 0
            slot_time = phase_start_mono + offset
            while slot_time < phase_end_mono and not cancelled.is_set():
                now = monotonic_fn()
                if now < slot_time:
                    if wait_fn(slot_time - now):
                        break
                    now = monotonic_fn()
                    if cancelled.is_set():
                        break
                if now >= phase_end_mono:
                    while slot_time < phase_end_mono:
                        scheduled += 1
                        with ue_lock:
                            missed += 1
                        slot_index += 1
                        slot_time = phase_start_mono + offset + slot_index * period
                    break
                if now > slot_time + lateness_tolerance:
                    while slot_time < phase_end_mono and now > slot_time + lateness_tolerance:
                        scheduled += 1
                        with ue_lock:
                            missed += 1
                        slot_index += 1
                        slot_time = phase_start_mono + offset + slot_index * period
                    continue
                scheduled += 1
                with inflight_lock:
                    can_dispatch = in_flight < max_in_flight_per_ue
                    if can_dispatch:
                        in_flight += 1
                if not can_dispatch:
                    with ue_lock:
                        missed += 1
                else:
                    futures.append(req_pool.submit(run_one, slot_index, slot_time))
                slot_index += 1
                slot_time = phase_start_mono + offset + slot_index * period

            for fut in futures:
                fut.result()

        return {
            'group': group,
            'source_address': source,
            'scheduled': scheduled,
            'started': started,
            'completed': completed,
            'attempts': completed,
            'successes': successes,
            'failures': completed - successes,
            'missed': missed,
            'actual_start_rate': started / phase_duration,
            'request_coverage': completed / scheduled if scheduled else None,
        }

    with ThreadPoolExecutor(max_workers=num_ues) as executor:
        try:
            futures = [
                executor.submit(worker, pos, group, source, ue_index)
                for pos, (group, source, ue_index) in enumerate(selected)
            ]
            return [future.result() for future in futures]
        except BaseException:
            cancelled.set()
            raise


def probe_registered_service_paths(site_groups, urls, timeout, count=5):
    """Probe the actual NodePort /health service path from registered lifecycle-owned UEs."""
    results = {}
    for group, ues in site_groups:
        if not ues:
            continue
        ue = ues[0]
        raw_url = urls[group]
        parsed = (urlsplit(raw_url) if isinstance(raw_url, str) else raw_url)._replace(
            path='/health', query='', fragment='',
        )
        health_url = parsed.geturl()
        samples = [attempt(parsed, 'GET', None, {}, ue['ip'], timeout) for _ in range(count)]
        durs = sorted(s['duration_ms'] for s in samples if s['success'])
        results[group] = {
            'source_address': ue['ip'],
            'interface': ue.get('iface'),
            'url': health_url,
            'samples': samples,
            'successes': len(durs),
            'min_ms': durs[0] if durs else None,
            'median_ms': durs[len(durs) // 2] if durs else None,
            'max_ms': durs[-1] if durs else None,
        }
    return results


def parse_phase_durations(text, count, default):
    if text is None:
        if not math.isfinite(default) or default <= 0:
            raise ValueError('phase duration and timeout must be finite and positive')
        return [float(default)] * count
    parts = [part.strip() for part in str(text).split(',')]
    if len(parts) != count:
        raise ValueError(f'phase_durations count ({len(parts)}) must match phase schedule count ({count})')
    durations = []
    for part in parts:
        try:
            value = float(part)
        except ValueError as exc:
            raise ValueError(f'invalid phase duration: {part!r}') from exc
        if not math.isfinite(value) or value <= 0:
            raise ValueError('phase durations must be finite and positive')
        durations.append(value)
    return durations


def run(args):
    phases = parse_schedule(args.phase_schedule)
    if any(phase['workload_ue'] > 50 for phase in phases):
        raise ValueError('at most 50 UEs are supported')
    if not args.armed:
        raise ValueError('pass --armed to create and replace UE sessions')
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        raise ValueError('phase duration and timeout must be finite and positive')
    phase_durations = parse_phase_durations(getattr(args, 'phase_durations', None), len(phases), args.phase_duration)
    rps_per_ue = getattr(args, 'rps_per_ue', DEFAULT_RPS_PER_UE)
    if isinstance(rps_per_ue, bool) or not isinstance(rps_per_ue, (int, float)) or not math.isfinite(rps_per_ue) or rps_per_ue <= 0:
        raise ValueError('rps_per_ue must be a finite positive number')
    max_in_flight_per_ue = getattr(args, 'max_in_flight_per_ue', DEFAULT_MAX_IN_FLIGHT_PER_UE)
    if type(max_in_flight_per_ue) is not int or max_in_flight_per_ue < 1:
        raise ValueError('max_in_flight_per_ue must be a positive integer')
    telemetry = telemetry_config(getattr(args, 'telemetry_url', None), getattr(args, 'telemetry_token_file', None))
    site_urls = getattr(args, 'site_urls', None)
    if site_urls is not None:
        urls = validate_site_urls(site_urls)
    else:
        urls = {group: validate_url(args.url) for group in ('gnb-1', 'gnb-2', 'gnb-3')}
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
    metadata = {'url': args.url, 'site_urls': site_urls, 'schedule': phases,
                'phase_duration_s': args.phase_duration, 'phase_durations_s': phase_durations,
                'timeout_s': args.timeout, 'rps_per_ue': float(rps_per_ue),
                'max_in_flight_per_ue': max_in_flight_per_ue,
                'ue_config': str(Path(args.ue_config).resolve()),
                'nr_ue': str(Path(args.nr_ue).resolve()), 'start_time': time.time(),
                'workload': 'open-loop-fixed-rate', 'switching': 'stop-and-reattach; not handover'}
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
            previous_total = None
            for index, (phase, phase_duration) in enumerate(zip(phases, phase_durations), 1):
                counts = tuple(phase[f'gnb{site}_ues'] for site in (1, 2, 3))
                total = phase['workload_ue']
                direction = ('initial' if previous_total is None else
                             'ramp-up' if total > previous_total else
                             'ramp-down' if total < previous_total else 'redistribution')
                previous_total = total
                print(f"[..] {phase['phase']}: {index}/{len(phases)} {direction}; switching to {counts[0]} gNB-1, {counts[1]} gNB-2, {counts[2]} gNB-3", flush=True)
                sites = lifecycle.switch_counts(phase['workload_ue'], *counts)
                if len(sites) != 3 or tuple(map(len, sites)) != counts:
                    raise RuntimeError('registered UE count differs from requested split')
                site_groups = tuple((f'gnb-{site}', ues) for site, ues in enumerate(sites, 1))
                if getattr(args, 'probe_service_path', False):
                    probes_path = out / 'service-path-probes.json'
                    existing_probes = json.loads(probes_path.read_text()) if probes_path.exists() else {}
                    existing_probes[phase['phase']] = probe_registered_service_paths(site_groups, urls, args.timeout)
                    probes_path.write_text(json.dumps(existing_probes, indent=2) + '\n')
                selected = [(group, ue['ip'], index)
                            for index, (group, ue) in enumerate(
                                (group, ue) for group, ues in site_groups for ue in ues)]
                publisher = TelemetryPublisher(telemetry, [source for _, source, _ in selected]) if telemetry else None
                start = time.time()
                print(f"[RUN] {phase['phase']}: {index}/{len(phases)} {direction}; {total} UEs, split {counts}; traffic for {phase_duration:g}s at {float(rps_per_ue):g} RPS/UE (max_in_flight={max_in_flight_per_ue})", flush=True)
                emit({'event': 'phase_start', **phase, 'start_time': start,
                      'duration_s': phase_duration, 'rps_per_ue': float(rps_per_ue),
                      'max_in_flight_per_ue': max_in_flight_per_ue, 'ues': [
                          {'group': group, 'source_address': ue['ip'], 'interface': ue['iface']}
                          for group, ues in site_groups for ue in ues]})

                cancelled = threading.Event()
                def dispatch(group, source, ue_index, image):
                    return request_with_telemetry(attempt, publisher, urls[group], 'POST', image['body'], image['headers'], source, args.timeout)

                try:
                    per_ue = run_fixed_rate_phase(
                        phase['phase'],
                        selected,
                        phase_duration,
                        float(rps_per_ue),
                        max_in_flight_per_ue,
                        dispatch,
                        emit,
                        lambda group: site_urls[group] if site_urls is not None else args.url,
                        images,
                        assignments,
                        cancelled=cancelled,
                    )
                finally:
                    if publisher:
                        publisher.close()
                scheduled = sum(ue['scheduled'] for ue in per_ue)
                started = sum(ue['started'] for ue in per_ue)
                completed = sum(ue['completed'] for ue in per_ue)
                successes = sum(ue['successes'] for ue in per_ue)
                missed = sum(ue['missed'] for ue in per_ue)
                emit({'event': 'phase_end', **phase, 'start_time': start,
                      'generation_duration_s': phase_duration, 'end_time': time.time(),
                      'rps_per_ue': float(rps_per_ue), 'max_in_flight_per_ue': max_in_flight_per_ue,
                      'scheduled': scheduled, 'started': started, 'completed': completed,
                      'attempts': completed, 'successes': successes, 'failures': completed - successes,
                      'missed': missed, 'actual_start_rate': started / phase_duration,
                      'request_coverage': completed / scheduled if scheduled else None,
                      'per_ue': per_ue})
                print(f"[--] {phase['phase']}: scheduled={scheduled}, started={started}, completed={completed}, {successes} successful, {completed - successes} failed, missed={missed}", flush=True)
        finally:
            lifecycle.close()
    print(f"Results: {out / 'requests.jsonl'}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument('--url', help='Face-detection URL using a reachable Kubernetes node IP and Service NodePort, e.g. http://NODE_IP:NODEPORT/detect')
    destination.add_argument('--site-urls', type=json.loads, help='JSON mapping gnb-1, gnb-2 and gnb-3 to face-detection URLs')
    parser.add_argument('--image', default=str(defaults.IMAGE), help='Image file or directory of JPG, PNG, BMP, WebP, GIF or TIFF files; assigned in sorted order')
    parser.add_argument('--ue-config', default=str(defaults.INVENTORY), help='Private UE inventory YAML')
    parser.add_argument('--nr-ue', default=str(defaults.NR_UE), help='Absolute path to the UERANSIM nr-ue executable')
    parser.add_argument('--phase-schedule', default=defaults.SCHEDULE, help='Ordered total:gnb1:gnb2:gnb3 counts, comma separated')
    parser.add_argument('--phase-duration', type=float, default=defaults.PHASE_DURATION, help='Request-generation seconds per phase')
    parser.add_argument('--phase-durations', default=None, help='Comma-separated request-generation seconds per phase; overrides --phase-duration')
    parser.add_argument('--rps-per-ue', type=float, default=DEFAULT_RPS_PER_UE, help='Target arrival rate per active UE in requests/second')
    parser.add_argument('--max-in-flight-per-ue', type=int, default=DEFAULT_MAX_IN_FLIGHT_PER_UE, help='Maximum concurrent in-flight requests per UE')
    parser.add_argument('--timeout', type=float, default=10.0, help='Socket timeout in seconds')
    parser.add_argument('--output-dir', default=None, help='New output directory; defaults to a unique results directory')
    parser.add_argument('--telemetry-url', help='Collector HTTP(S) /observations URL')
    parser.add_argument('--telemetry-token-file', help='File containing the collector bearer token')
    parser.add_argument('--probe-service-path', action='store_true',
                        help='probe NodePort /health from one registered UE per active site before phase traffic')
    parser.add_argument('--armed', action='store_true', help='Authorize creation and replacement of UE sessions')
    args = parser.parse_args(argv)
    if args.output_dir is None:
        args.output_dir = str(defaults.RESULTS / f'run-{time.time_ns()}')
    if not args.armed:
        print(f'UE sessions will be created and replaced; traffic will be sent to {args.site_urls or args.url}.')
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
