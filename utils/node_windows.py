"""Serving-node request attribution in the same dispatch-time reporting windows."""
import json
import math
from pathlib import Path

from utils.transient_analysis import _percentile


def build_node_request_windows(method_dir, windows, nodes, slo_ms):
    records = [json.loads(line) for line in (Path(method_dir) / 'requests.jsonl').read_text().splitlines() if line.strip()]
    by_phase = {}
    for r in records:
        if r.get('event') == 'request':
            by_phase.setdefault(r['phase'], []).append(r)
    output = []
    for win in windows:
        start = win['actual_start_time'] - win['start_offset_s']
        selected = [r for r in by_phase.get(win['phase'], [])
                    if win['start_offset_s'] - 1e-9 <= float(r.get('dispatch_offset_s', r['start_time'] - start)) < win['end_offset_s'] - 1e-9]
        unknown = sum(r.get('server_node') not in nodes for r in selected)
        for node in nodes:
            reqs = [r for r in selected if r.get('server_node') == node]
            latencies = sorted(float(r['duration_ms']) for r in reqs if r.get('success'))
            met = sum(bool(r.get('success')) and float(r['duration_ms']) <= slo_ms for r in reqs)
            output.append(dict(win, node=node, attempts=len(reqs), successes=len(latencies), failures=len(reqs) - len(latencies),
                               p50_ms=_percentile(latencies, .5) if latencies else math.nan,
                               p90_ms=_percentile(latencies, .9) if latencies else math.nan,
                               violation_percent=100 * (len(reqs) - met) / len(reqs) if reqs else math.nan,
                               all_window_attempts=len(selected), unattributed_attempts=unknown,
                               attribution_percent=100 * (len(selected) - unknown) / len(selected) if selected else math.nan))
    return output
