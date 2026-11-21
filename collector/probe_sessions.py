"""Poll the collector /sessions endpoint and log JSONL snapshots for transition-timing verification."""
import argparse
import json
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen


def poll_once(url, timeout):
    with urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode('utf-8'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://10.34.211.140:31669/sessions')
    parser.add_argument('--interval', type=float, default=0.5)
    parser.add_argument('--duration', type=float, default=30.0)
    parser.add_argument('--timeout', type=float, default=2.0)
    parser.add_argument('--output', default='-')
    args = parser.parse_args()

    out = sys.stdout if args.output == '-' else open(args.output, 'a', encoding='utf-8')
    prev_counts = None
    deadline = time.monotonic() + args.duration
    try:
        while True:
            now_mono = time.monotonic()
            record = {'request_started_at': time.time()}
            try:
                snap = poll_once(args.url, args.timeout)
                record['timestamp'] = time.time()
                record.update(snap)
                counts = snap.get('active_sessions') or {}
                if snap.get('stale', True):
                    prev_counts = None
                elif prev_counts is not None:
                    delta = {
                        node: counts.get(node, 0) - prev_counts.get(node, 0)
                        for node in sorted(set(counts) | set(prev_counts))
                        if counts.get(node, 0) != prev_counts.get(node, 0)
                    }
                    if delta:
                        record['session_delta'] = delta
                if not snap.get('stale', True):
                    prev_counts = counts
            except (URLError, OSError, ValueError) as exc:
                record['timestamp'] = time.time()
                record['error'] = f'{type(exc).__name__}: {exc}'
            out.write(json.dumps(record, sort_keys=True) + '\n')
            out.flush()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(args.interval, max(0.0, args.interval - (time.monotonic() - now_mono)), remaining))
    finally:
        if out is not sys.stdout:
            out.close()


if __name__ == '__main__':
    main()
