"""Closed-loop HTTP workload over existing UE source addresses; no UE lifecycle management.

Run `python -m benchmark.client --help` for the CLI. Output is newline-delimited
JSON, exclusively created: phase_start, request, and phase_end records in order.
"""

import argparse
import http.client
import ipaddress
import json
import math
import re
import socket
import ssl
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit


TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


def parse_schedule(value):
    phases = []
    for index, entry in enumerate(value.split(','), 1):
        fields = entry.split(':')
        if len(fields) != 3 or any(not f.strip().isdecimal() for f in fields):
            raise ValueError('phase schedule entries must be total:gnb2:gnb3')
        total, gnb2, gnb3 = map(int, fields)
        if total < 1 or gnb2 + gnb3 != total:
            raise ValueError('phase counts must be nonnegative and sum to a positive total')
        phases.append({'phase': f'phase_{index:02d}', 'workload_ue': total,
                       'gnb2_ues': gnb2, 'gnb3_ues': gnb3})
    return phases


def parse_addresses(value):
    addresses = []
    for item in value.split(',') if value else []:
        try:
            address = str(ipaddress.ip_address(item.strip()))
        except ValueError as exc:
            raise ValueError(f'invalid UE address: {item!r}') from exc
        if ipaddress.ip_address(address).is_unspecified:
            raise ValueError('UE source addresses must not be unspecified')
        addresses.append(address)
    return addresses


def parse_headers(values):
    headers = {}
    for value in values:
        name, sep, content = value.partition(':')
        if not sep or not TOKEN.fullmatch(name) or '\r' in content or '\n' in content:
            raise ValueError(f'invalid HTTP header: {value!r}')
        if name.lower() in (key.lower() for key in headers):
            raise ValueError(f'duplicate HTTP header: {name}')
        headers[name] = content.lstrip()
    return headers


def validate(args):
    phases = parse_schedule(args.phase_schedule)
    groups = {'gnb-2': parse_addresses(args.gnb2_addresses),
              'gnb-3': parse_addresses(args.gnb3_addresses)}
    all_addresses = groups['gnb-2'] + groups['gnb-3']
    if len(set(all_addresses)) != len(all_addresses):
        raise ValueError('each UE must have a unique source address')
    for phase in phases:
        for group, key in (('gnb-2', 'gnb2_ues'), ('gnb-3', 'gnb3_ues')):
            if phase[key] > len(groups[group]):
                raise ValueError(f"{phase['phase']} requires {phase[key]} {group} addresses, got {len(groups[group])}")
    if not math.isfinite(args.phase_duration) or args.phase_duration <= 0:
        raise ValueError('phase duration must be finite and positive')
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        raise ValueError('timeout must be finite and positive')
    if not TOKEN.fullmatch(args.method):
        raise ValueError('invalid HTTP method')
    url = urlsplit(args.url)
    try:
        port = url.port
    except ValueError as exc:
        raise ValueError(f'invalid URL port: {exc}') from exc
    if (url.scheme not in ('http', 'https') or not url.hostname or url.username is not None
            or url.password is not None or url.fragment or any(c.isspace() for c in args.url)):
        raise ValueError('URL must be an absolute HTTP(S) URL without credentials or fragment')
    if port == 0:
        raise ValueError('URL port must be positive')
    headers = parse_headers(args.header)
    return phases, groups, url, headers


def attempt(url, method, body, headers, source, timeout):
    start = time.time()
    clock = time.monotonic()
    status = None
    error = None
    conn = None
    try:
        cls = http.client.HTTPSConnection if url.scheme == 'https' else http.client.HTTPConnection
        conn = cls(url.hostname, url.port, timeout=timeout, source_address=(source, 0))
        target = url.path or '/'
        if url.query:
            target += '?' + url.query
        conn.request(method, target, body=body, headers=headers)
        response = conn.getresponse()
        status = response.status
        response.read()
    except (OSError, http.client.HTTPException, ssl.SSLError, ValueError) as exc:
        error = f'{type(exc).__name__}: {exc}'
    finally:
        if conn is not None:
            conn.close()
    return {'start_time': start, 'end_time': time.time(),
            'duration_ms': (time.monotonic() - clock) * 1000,
            'http_status': status, 'error': error,
            'success': error is None and status is not None and 200 <= status < 300}


def run(args):
    phases, groups, url, headers = validate(args)
    body = args.body.encode('utf-8') if args.body is not None else None
    lock = threading.Lock()
    # Exclusive creation prevents accidentally destroying a previous experiment.
    with Path(args.output).open('x', encoding='utf-8') as output:
        def emit(record):
            with lock:
                output.write(json.dumps(record, allow_nan=False) + '\n')
                output.flush()

        for phase in phases:
            selected = [(group, address) for group, key in
                        (('gnb-2', 'gnb2_ues'), ('gnb-3', 'gnb3_ues'))
                        for address in groups[group][:phase[key]]]
            start = time.time()
            deadline = time.monotonic() + args.phase_duration
            emit({'event': 'phase_start', **phase, 'start_time': start,
                  'duration_s': args.phase_duration, 'ues': [
                      {'group': group, 'source_address': address} for group, address in selected]})

            def worker(group, source):
                count = successes = 0
                while time.monotonic() < deadline:
                    record = attempt(url, args.method, body, headers, source, args.timeout)
                    count += 1
                    successes += record['success']
                    emit({'event': 'request', 'phase': phase['phase'], 'group': group,
                          'source_address': source, 'url': args.url, 'method': args.method,
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


def main(argv=None):
    parser = argparse.ArgumentParser(description='Closed-loop HTTP benchmark using existing UERANSIM UE source addresses; does not manage UE processes.')
    parser.add_argument('--url', required=True, help='Absolute HTTP(S) target URL')
    parser.add_argument('--method', default='GET', help='HTTP method (default GET)')
    parser.add_argument('--body', help='UTF-8 request body')
    parser.add_argument('--header', action='append', default=[], help='HTTP header NAME: VALUE; repeatable')
    parser.add_argument('--phase-schedule', required=True, help='Ordered total:gnb2:gnb3 counts, comma separated')
    parser.add_argument('--gnb2-addresses', default='', help='Comma-separated existing gNB-2 UE source IPs')
    parser.add_argument('--gnb3-addresses', default='', help='Comma-separated existing gNB-3 UE source IPs')
    parser.add_argument('--phase-duration', type=float, required=True, help='Request generation seconds per phase; in-flight attempts drain')
    parser.add_argument('--timeout', type=float, default=30.0, help='Socket timeout in seconds (default 30)')
    parser.add_argument('--output', required=True, help='New JSONL output file; must not already exist')
    args = parser.parse_args(argv)
    try:
        run(args)
        print(f'Results: {args.output}')
    except (ValueError, OSError) as exc:
        parser.exit(2, f'error: {exc}\n')


if __name__ == '__main__':
    main()
