"""Prometheus text-format metric renderers for /metrics and /metrics/placement."""
from collections import Counter
import math

from collector.config import VALID_SITES


def render_placement_metrics(recent: list[tuple], settings: dict) -> str:
    """Render lightweight per-gNB and per-UPF attempt counts for /metrics/placement."""
    gnb_counts = Counter(gnb_node for gnb_node, _ in recent)
    upf_counts = Counter(upf_node for _, upf_node in recent)
    lines = ['# TYPE ue_request_node_placement_window_attempts gauge']
    for node in settings['gnb_nodes'].values():
        lines.append(f'ue_request_node_placement_window_attempts{{node="{node}"}} {gnb_counts[node]}')
    lines.append('# TYPE ue_request_upf_placement_window_attempts gauge')
    for node in dict.fromkeys(settings['upf_nodes'].values()):
        lines.append(f'ue_request_upf_placement_window_attempts{{node="{node}"}} {upf_counts[node]}')
    lines.append('# TYPE ue_request_upf_placement_window_unassociated gauge')
    lines.append(f'ue_request_upf_placement_window_unassociated {upf_counts[None]}')
    return '\n'.join(lines) + '\n'


def _append_site_route_lines(lines: list[str], snap: dict):
    for site in VALID_SITES:
        site_data = snap['sites'][site]
        for route in ('local', 'core'):
            b = site_data['user'][route]
            lines.append(f'ue_site_route_window_attempts{{site="{site}",route="{route}",traffic_class="user"}} {b["attempts"]}')
            lines.append(f'ue_site_route_window_errors{{site="{site}",route="{route}",traffic_class="user"}} {b["errors"]}')
            lines.append(f'ue_site_route_window_timeouts{{site="{site}",route="{route}",traffic_class="user"}} {b["timeouts"]}')
            if b['p90_ms'] is not None:
                lines.append(f'ue_site_route_request_duration_p90_seconds{{site="{site}",route="{route}",traffic_class="user"}} {b["p90_ms"] / 1000.0}')
        pb = site_data['probe']['local']
        lines.append(f'ue_site_route_window_attempts{{site="{site}",route="local",traffic_class="probe"}} {pb["attempts"]}')
        lines.append(f'ue_site_route_window_errors{{site="{site}",route="local",traffic_class="probe"}} {pb["errors"]}')
        if pb['p90_ms'] is not None:
            lines.append(f'ue_site_route_request_duration_p90_seconds{{site="{site}",route="local",traffic_class="probe"}} {pb["p90_ms"] / 1000.0}')


def _add_prometheus_types(lines: list[str]) -> str:
    typed: list[str] = []
    seen: set[str] = set()
    for line in lines:
        name = line.split(' ', 1)[0].split('{', 1)[0]
        if name not in seen:
            typed.append(f'# TYPE {name} {"counter" if name.endswith("_total") else "gauge"}')
            seen.add(name)
        typed.append(line)
    return '\n'.join(typed) + '\n'


def render_metrics(samples: list[tuple], *, ingested: int, rejected: int, overflow: int,
                   probe_ingested: int, mapping_time: float | None, session_counts: Counter,
                   now: float, settings: dict, offload_snap: dict) -> str:
    """Render full Prometheus metrics for GET /metrics."""
    successes = sorted(row[1] for row in samples if row[2])
    lines = [
        f'ue_request_window_attempts {len(samples)}',
        f'ue_request_window_successes {len(successes)}',
        f'ue_request_window_success_within_slo {sum(x <= settings["slo_seconds"] for x in successes)}',
        f'ue_request_window_unassociated {sum(row[3] is None for row in samples)}',
        f'ue_telemetry_ingested_total {ingested}',
        f'ue_telemetry_rejected_total {rejected}',
        f'ue_telemetry_overflow_evictions_total {overflow}',
        f'ue_telemetry_probe_ingested_total {probe_ingested}',
    ]
    if successes:
        lines.append(f'ue_request_duration_p90_seconds {successes[math.ceil(0.9 * len(successes)) - 1]}')
    if samples:
        durations = sorted(row[1] for row in samples)
        lines.append(f'ue_request_attempt_duration_p90_seconds {durations[math.ceil(0.9 * len(durations)) - 1]}')
    counts = Counter(row[3] for row in samples)
    for node in settings['gnb_nodes'].values():
        lines.append(f'ue_request_node_window_attempts{{node="{node}"}} {counts[node]}')
    for node in settings['gnb_nodes'].values():
        lines.append(f'ue_active_sessions{{node="{node}"}} {session_counts[node]}')
    if mapping_time is not None:
        lines.append(f'ue_session_mapping_age_seconds {max(0.0, now - mapping_time)}')
    _append_site_route_lines(lines, offload_snap)
    return _add_prometheus_types(lines)
