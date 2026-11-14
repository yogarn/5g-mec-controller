#!/usr/bin/env python3
"""Save Prometheus collector series and window samples aligned to UE phases."""
import argparse
import csv
import json
import math
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

QUERIES = {
    'attempts': 'ue_request_window_attempts{job="ue-telemetry"}',
    'unassociated': 'ue_request_window_unassociated{job="ue-telemetry"}',
    'complete': 'ue_request_window_complete{job="ue-telemetry"}',
    'node_attempts': 'ue_request_node_window_attempts{job="ue-telemetry"}',
    'p90_seconds': 'ue_request_attempt_duration_p90_seconds{job="ue-telemetry"}',
    'ingested_total': 'ue_telemetry_ingested_total{job="ue-telemetry"}',
    'rejected_total': 'ue_telemetry_rejected_total{job="ue-telemetry"}',
    'overflow_total': 'ue_telemetry_overflow_evictions_total{job="ue-telemetry"}',
    'scrape_up': 'up{job="ue-telemetry"}',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('requests', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--prometheus-url', default='http://10.34.211.140:30990')
    args = parser.parse_args()
    records = [json.loads(line) for line in args.requests.read_text().splitlines()]
    starts = {r['phase']: r for r in records if r.get('event') == 'phase_start'}
    ends = {r['phase']: r for r in records if r.get('event') == 'phase_end'}
    if not starts or set(starts) != set(ends):
        raise ValueError('incomplete phase boundaries')
    lower = min(x['start_time'] for x in starts.values()) - 10
    upper = max(x['end_time'] for x in ends.values())
    history = {}
    for key, query in QUERIES.items():
        url = args.prometheus_url.rstrip('/') + '/api/v1/query_range?' + urlencode(
            {'query': query, 'start': lower, 'end': upper, 'step': 5})
        with urlopen(url, timeout=10) as response:
            data = json.load(response)
        if data.get('status') != 'success' or data.get('data', {}).get('resultType') != 'matrix':
            raise ValueError(f'invalid Prometheus response for {key}')
        history[key] = data['data']['result']
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / 'telemetry-prometheus.json').open('x', encoding='utf-8') as out:
        json.dump({'queries': QUERIES, 'step_seconds': 5, 'start': lower, 'end': upper,
                   'results': history}, out, indent=2)
        out.write('\n')

    def values(key, start, end, node=None):
        return [float(value) for series in history[key]
                if node is None or series.get('metric', {}).get('node') == node
                for ts, value in series.get('values', [])
                if start <= float(ts) <= end and math.isfinite(float(value))]

    fields = ['phase', 'prometheus_samples', 'window_attempts_mean', 'window_unassociated_max',
              'incomplete_samples', 'gnb1_window_attempts_mean', 'gnb2_window_attempts_mean',
              'gnb3_window_attempts_mean', 'p90_seconds_last', 'scrape_down_samples']
    with (args.output / 'telemetry-by-ramp.csv').open('x', newline='', encoding='utf-8') as out:
        writer = csv.DictWriter(out, fieldnames=fields); writer.writeheader()
        for phase, first in starts.items():
            start, end = first['start_time'], ends[phase]['end_time']
            attempts = values('attempts', start, end)
            unknown = values('unassociated', start, end)
            complete = values('complete', start, end)
            p90 = values('p90_seconds', start, end)
            up = values('scrape_up', start, end)
            row = {'phase': phase, 'prometheus_samples': len(attempts),
                   'window_attempts_mean': sum(attempts) / len(attempts) if attempts else '',
                   'window_unassociated_max': max(unknown) if unknown else '',
                   'incomplete_samples': sum(v != 1 for v in complete) if complete else '',
                   'p90_seconds_last': p90[-1] if p90 else '',
                   'scrape_down_samples': sum(v != 1 for v in up) if up else ''}
            for site, node in (('gnb1', 'gnb1'), ('gnb2', 'gnb-2'), ('gnb3', 'gnb-3')):
                v = values('node_attempts', start, end, node)
                row[site + '_window_attempts_mean'] = sum(v) / len(v) if v else ''
            writer.writerow(row)
    print(f'Saved Prometheus history for {len(starts)} phases')


if __name__ == '__main__':
    main()
