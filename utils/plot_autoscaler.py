#!/usr/bin/env python3
"""Plot measured autoscaler comparisons.

Usage: python -m utils.plot_autoscaler RESULTS_DIR OUTPUT_DIR

RESULTS_DIR/{baseline,proposed} must each contain summary.csv, ramp-summary.csv,
pods-by-ramp.csv, and resource-by-ramp.csv. Placement files must explicitly
record every phase/node pair, including measured zero counts. SLO violations
are the fraction of all attempts that failed or exceeded the threshold.
"""
import argparse
from bisect import bisect_right
import csv
import json
import math
from pathlib import Path
import sys

import matplotlib.pyplot as plt
from matplotlib.transforms import ScaledTranslation
from matplotlib.ticker import FixedLocator, MaxNLocator
from utils.cadvisor_resources import POD_LOOKUP_MAX_AGE_S
from utils.phase_windows import summarize_windows, write_windows
from utils.transient_analysis import _percentile
from utils.resource_windows import build_resource_windows

METHODS = ("baseline", "proposed")
STYLES = {"baseline": "--", "proposed": "-"}
METHOD_COLORS = {"proposed": "#D55E00", "baseline": "#0072B2"}
METHOD_LABELS = {"baseline": "Baseline", "proposed": "Proposed"}
ORIGIN_NOTE = "(0,0) is a display origin; dots thereafter summarize measured windows"
WINDOW_SECONDS = 15.0
THESIS_FIG_WIDTH = 7.5
ALIGNED_TIME_NOTE = "Cumulative traffic elapsed time (s); setup, draining, and reattachment gaps excluded"


def read_rows(path, required):
    try:
        with path.open(newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            missing = set(required) - set(reader.fieldnames or ())
            if missing:
                raise ValueError(f"{path}: missing columns: {', '.join(sorted(missing))}")
            rows = list(reader)
    except OSError as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    if not rows:
        raise ValueError(f"{path}: no measurements")
    return rows, set(reader.fieldnames)


def number(value, description, integer=False):
    try:
        result = int(value) if integer else float(value)
    except (ValueError, TypeError):
        raise ValueError(f"{description}: invalid number {value!r}") from None
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{description}: expected a finite nonnegative number")
    return result

def optional_number(value, description):
    if value is None or not str(value).strip():
        return math.nan
    return number(value, description)



def phase_rows(rows, path):
    result = {}
    for row in rows:
        phase = (row.get("phase") or "").strip()
        if not phase or phase in result:
            raise ValueError(f"{path}: missing or duplicate phase {phase!r}")
        result[phase] = row
    return result


def load(root):
    summaries, placements, resources, schemas = {}, {}, {}, {}
    for method in METHODS:
        directory = root / method
        path = directory / "ramp-summary.csv"
        if path.exists():
            rows, fields = read_rows(path, ("phase", "workload_ue", "gnb2_ues", "gnb3_ues", "p50_ms", "p90_ms"))
        else:
            path = directory / "summary.csv"
            rows, fields = read_rows(path, ("phase", "workload_ue", "gnb2_ues", "gnb3_ues", "latency_p50_ms", "latency_p90_ms"))
            for row in rows:
                row["p50_ms"] = row["latency_p50_ms"]
                row["p90_ms"] = row["latency_p90_ms"]
        schemas[method] = "gnb1_ues" in fields
        summaries[method] = phase_rows(rows, path)
        path = directory / "pods-by-ramp.csv"
        rows, _ = read_rows(path, ("phase", "node", "pod_count"))
        placements[method] = {}
        for row in rows:
            phase, node = (row.get("phase") or "").strip(), (row.get("node") or "").strip()
            key = (phase, node)
            if not phase or not node or key in placements[method]:
                raise ValueError(f"{path}: missing or duplicate phase/node {key!r}")
            placements[method][key] = number(row["pod_count"], f"{path}: {key}", integer=True)
        path = directory / "resource-by-ramp.csv"
        rows, _ = read_rows(path, ("phase", "cpu_millicores", "memory_mib"))
        resources[method] = phase_rows(rows, path)

    if schemas["baseline"] != schemas["proposed"]:
        raise ValueError("methods must use the same gNB distribution schema")
    site_keys = (("gnb1_ues",) if schemas["baseline"] else ()) + ("gnb2_ues", "gnb3_ues")
    phases = list(summaries["baseline"])
    nodes = sorted({node for counts in placements.values() for _, node in counts})
    if len(nodes) > 20:
        raise ValueError("more than 20 placement nodes cannot be shown with distinct colors")
    for method in METHODS:
        if set(summaries[method]) != set(phases) or set(resources[method]) != set(phases):
            raise ValueError(f"{method}: summary and resource phases must match baseline phases")
        if set(placements[method]) != {(phase, node) for phase in phases for node in nodes}:
            raise ValueError(f"{method}: placement must explicitly include every phase/node pair in both methods")
        for phase in phases:
            row = summaries[method][phase]
            workload = number(row["workload_ue"], f"{method} {phase} workload_ue", integer=True)
            counts = [number(row[key], f"{method} {phase} {key}", integer=True) for key in site_keys]
            if sum(counts) != workload:
                raise ValueError(f"{method} {phase}: site counts do not sum to workload_ue")
            for key in ("p50_ms", "p90_ms"):
                optional_number(row[key], f"{method} {phase} {key}")
            for key in ("cpu_millicores", "memory_mib"):
                optional_number(resources[method][phase][key], f"{method} {phase} {key}")
            for key in ("workload_ue", *site_keys):
                if int(row[key]) != int(summaries["baseline"][phase][key]):
                    raise ValueError(f"{method} {phase}: {key} differs from baseline")
    return summaries, placements, resources, site_keys, phases, nodes


def load_slo(root, phases, slo_ms):
    """Return all-attempt violation percentages; missing attempts stay missing."""
    if not math.isfinite(slo_ms) or slo_ms <= 0:
        raise ValueError('SLO target must be a positive finite number')
    violations = {}
    for method in METHODS:
        path = root / method / 'summary.csv'
        rows, _ = read_rows(path, ('phase', 'attempts', 'slo_ms', 'slo_met_attempts'))
        summaries = phase_rows(rows, path)
        if set(summaries) != set(phases):
            raise ValueError(f'{path}: phases must match ramp-summary.csv')
        violations[method] = []
        for phase in phases:
            row = summaries[phase]
            attempts = number(row['attempts'], f'{path}: {phase} attempts', integer=True)
            met = number(row['slo_met_attempts'], f'{path}: {phase} slo_met_attempts', integer=True)
            threshold = number(row['slo_ms'], f'{path}: {phase} slo_ms')
            if not math.isclose(threshold, slo_ms, rel_tol=0, abs_tol=1e-9):
                raise ValueError(f'{path}: {phase} SLO threshold does not match --slo-ms')
            if met > attempts:
                raise ValueError(f'{path}: {phase} SLO-met count exceeds attempts')
            violations[method].append(100 * (attempts - met) / attempts if attempts else math.nan)
    return violations


def load_coverage(root, phases):
    """Return request coverage percentages (completed / scheduled); 100% if legacy summary lacks scheduled."""
    coverage = {}
    for method in METHODS:
        path = root / method / 'summary.csv'
        rows, fields = read_rows(path, ('phase', 'attempts'))
        summaries = phase_rows(rows, path)
        if set(summaries) != set(phases):
            raise ValueError(f'{path}: phases must match ramp-summary.csv')
        coverage[method] = []
        for phase in phases:
            row = summaries[phase]
            if 'scheduled' not in fields:
                attempts = number(row['attempts'], f'{path}: {phase} attempts', integer=True)
                coverage[method].append(100.0 if attempts else math.nan)
                continue
            scheduled = number(row['scheduled'], f'{path}: {phase} scheduled', integer=True)
            completed = number(row['completed'], f'{path}: {phase} completed', integer=True) if 'completed' in fields else number(row['attempts'], f'{path}: {phase} attempts', integer=True)
            if completed > scheduled:
                raise ValueError(f'{path}: {phase} completed exceeds scheduled')
            coverage[method].append(100 * completed / scheduled if scheduled else math.nan)
    return coverage


def load_resource_coverage(root, phases):
    """Return per-phase resource sample completeness metadata from resource-by-ramp.csv."""
    coverage = {}
    for method in METHODS:
        path = root / method / 'resource-by-ramp.csv'
        rows, fields = read_rows(path, ('phase', 'cpu_millicores', 'memory_mib'))
        summaries = phase_rows(rows, path)
        if set(summaries) != set(phases):
            raise ValueError(f'{path}: phases must match ramp-summary.csv')
        coverage[method] = []
        for phase in phases:
            row = summaries[phase]
            complete = number(row['complete_samples'], f'{path}: {phase} complete_samples', integer=True) if 'complete_samples' in fields else (0 if optional_number(row['cpu_millicores'], 'cpu_millicores') != optional_number(row['cpu_millicores'], 'cpu_millicores') else 1)
            partial = number(row['partial_samples'], f'{path}: {phase} partial_samples', integer=True) if 'partial_samples' in fields else 0
            rejected = number(row['rejected_samples'], f'{path}: {phase} rejected_samples', integer=True) if 'rejected_samples' in fields else 0
            missing = number(row['missing_samples'], f'{path}: {phase} missing_samples', integer=True) if 'missing_samples' in fields else 0
            rounds = complete + partial + rejected
            denom = number(row['expected_samples'], f'{path}: {phase} expected_samples', integer=True) if 'expected_samples' in fields and str(row['expected_samples']).strip() else (rounds + missing)
            if 'coverage_ratio' in fields and str(row['coverage_ratio']).strip():
                cov_pct = 100.0 * number(row['coverage_ratio'], f'{path}: {phase} coverage_ratio')
            else:
                cov_pct = 100.0 * complete / denom if denom > 0 else 0.0
            status = (row.get('completeness_status') or '').strip() or (
                'missing' if complete == 0 and denom == 0 else
                'incomplete_only' if complete == 0 else
                'partial' if denom > complete else
                'complete'
            )
            source = (row.get('resource_source') or '').strip() or 'metrics-server'
            rate_win = (row.get('rate_window_seconds') or '').strip() or ''
            freshness = (row.get('freshness_seconds') or '').strip() or ''
            coverage[method].append({
                'complete': complete,
                'rounds': rounds,
                'missing': missing,
                'expected': denom,
                'coverage_pct': cov_pct,
                'status': status,
                'source': source,
                'rate_window_seconds': rate_win,
                'freshness_seconds': freshness,
                'cpu_quality': (row.get('cpu_quality') or '').strip() or ('exact' if complete > 0 else 'missing'),
            })
    return coverage


def load_durations(root, phases):
    """Return per-phase generation durations in seconds when recorded in summary.csv."""
    durations = {}
    for method in METHODS:
        path = root / method / 'summary.csv'
        rows, fields = read_rows(path, ('phase',))
        if 'generation_duration_s' not in fields:
            return None
        summaries = phase_rows(rows, path)
        if set(summaries) != set(phases):
            raise ValueError(f'{path}: phases must match ramp-summary.csv')
        durations[method] = [
            number(summaries[phase]['generation_duration_s'], f'{path}: {phase} generation_duration_s')
            for phase in phases
        ]
    if durations['baseline'] != durations['proposed']:
        raise ValueError('baseline and proposed phase durations differ')
    return durations['baseline']


def _format_csv_num(value):
    if value is None or value == "":
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return value


def write_csv_rows(rows, fieldnames, path):
    with Path(path).open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _format_csv_num(row.get(k, "")) for k in fieldnames})


def iter_window_bounds(duration_s, window_seconds=WINDOW_SECONDS):
    """Yield (window_index, start_offset_s, end_offset_s) covering [0, duration_s)."""
    if duration_s <= 0:
        return
    idx = 0
    lower = 0.0
    while lower < duration_s - 1e-9:
        upper = min(float(duration_s), lower + float(window_seconds))
        yield idx, lower, upper
        idx += 1
        lower = upper


def build_phase_windows_15s(method_dir, slo_ms, window_seconds=WINDOW_SECONDS):
    """Compute 15s (or partial final) request windows from raw requests.jsonl."""
    method_dir = Path(method_dir)
    records = [
        json.loads(line)
        for line in (method_dir / "requests.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    starts = {}
    phase_order = []
    for r in records:
        if r.get("event") == "phase_start":
            phase = r["phase"]
            starts[phase] = r
            phase_order.append(phase)
    ends = {r["phase"]: r for r in records if r.get("event") == "phase_end"}
    requests_by_phase = {phase: [] for phase in phase_order}
    for r in records:
        if r.get("event") == "request" and r.get("phase") in requests_by_phase:
            requests_by_phase[r["phase"]].append(r)

    rows = []
    for phase in phase_order:
        start = starts[phase]
        end_rec = ends.get(phase, {})
        phase_start_t = float(start["start_time"])
        duration = float(end_rec.get("generation_duration_s", start["duration_s"]))
        reqs = requests_by_phase[phase]
        rate = start.get("rps_per_ue")
        ues = start.get("ues", [])
        has_slots = bool(rate and ues and all("scheduled_offset_s" in r for r in reqs))
        period = (1.0 / float(rate)) if has_slots else None

        for win_idx, lower, upper in iter_window_bounds(duration, window_seconds):
            selected = []
            for r in reqs:
                offset = float(r.get("dispatch_offset_s", float(r["start_time"]) - phase_start_t))
                if lower - 1e-9 <= offset < upper - 1e-9:
                    selected.append(r)
            latencies = sorted(float(r["duration_ms"]) for r in selected if r.get("success"))
            attempts = len(selected)
            successes = len(latencies)
            failures = attempts - successes
            met = sum(1 for r in selected if r.get("success") and float(r["duration_ms"]) <= slo_ms)
            p50 = _percentile(latencies, 0.5)
            p90 = _percentile(latencies, 0.9)
            viol_pct = (100.0 * (attempts - met) / attempts) if attempts > 0 else math.nan

            scheduled = started_slots = missed_slots = cov_pct = ""
            if has_slots:
                scheduled = 0
                for pos in range(len(ues)):
                    ue_off = pos / len(ues) * period
                    first = max(0, math.ceil((lower - ue_off) / period - 1e-9))
                    stop = max(0, math.ceil((upper - ue_off) / period - 1e-9))
                    scheduled += max(0, stop - first)
                cohort = [r for r in reqs if lower - 1e-9 <= float(r["scheduled_offset_s"]) < upper - 1e-9]
                started_slots = len(cohort)
                if started_slots > scheduled:
                    raise ValueError(f"{phase} [{lower}, {upper}): started slots exceed scheduled slots")
                missed_slots = scheduled - started_slots
                cov_pct = (100.0 * started_slots / scheduled) if scheduled > 0 else math.nan

            rows.append({
                "phase": phase,
                "phase_transition": start.get("phase_transition", "restart"),
                "window_index": win_idx,
                "start_offset_s": lower,
                "end_offset_s": upper,
                "phase_duration_s": duration,
                "actual_start_time": phase_start_t + lower,
                "actual_end_time": phase_start_t + upper,
                "attempts": attempts,
                "successes": successes,
                "failures": failures,
                "p50_ms": p50 if p50 != "" else math.nan,
                "p90_ms": p90 if p90 != "" else math.nan,
                "slo_met_attempts": met,
                "violation_percent": viol_pct,
                "scheduled": scheduled,
                "started_slots": started_slots,
                "missed_slots": missed_slots,
                "request_coverage_percent": cov_pct,
            })
    return rows


def build_pod_placement_15s(method_dir, window_rows, nodes, freshness_s=POD_LOOKUP_MAX_AGE_S):
    """Evaluate per-node ready replica counts at each window end from metrics.jsonl."""
    metrics_path = Path(method_dir) / "metrics.jsonl"
    observations = []
    if metrics_path.exists():
        for line in metrics_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            if "error" in rec or "pods" not in rec or "sample_time" not in rec:
                continue
            observations.append((float(rec["sample_time"]), rec["pods"]))
    observations.sort(key=lambda item: item[0])
    sample_times = [item[0] for item in observations]

    placement_rows = []
    for win in window_rows:
        t_eval = win["actual_end_time"]
        idx = bisect_right(sample_times, t_eval) - 1
        if idx < 0:
            obs_time = ""
            age_s = ""
            status = "missing"
            pods_by_node = None
        else:
            obs_time, pods = observations[idx]
            age_s = t_eval - obs_time
            if age_s > freshness_s:
                status = "stale"
                pods_by_node = None
            else:
                status = "fresh"
                pods_by_node = {node: [p for p in pods if p.get("node") == node] for node in nodes}

        for node in nodes:
            if pods_by_node is None:
                ready_count = math.nan
                pod_count = math.nan
            else:
                node_pods = pods_by_node[node]
                ready_count = sum(1 for p in node_pods if p.get("ready"))
                pod_count = len(node_pods)
            placement_rows.append({
                "phase": win["phase"],
                "window_index": win["window_index"],
                "start_offset_s": win["start_offset_s"],
                "end_offset_s": win["end_offset_s"],
                "window_end_time": t_eval,
                "node": node,
                "ready_count": ready_count,
                "pod_count": pod_count,
                "sample_time": obs_time,
                "sample_age_seconds": age_s,
                "freshness_seconds": freshness_s,
                "status": status,
            })
    return placement_rows


def build_pod_observations(method_dir, window_rows, nodes):
    """Keep every saved Pod snapshot at its actual observation time.

    Phase intervals are half open, so a boundary observation belongs to the
    incoming phase. No phase-end count is backdated to a phase midpoint.
    """
    path = Path(method_dir) / "metrics.jsonl"
    if not path.exists():
        return []
    bounds = {}
    for win in window_rows:
        bounds.setdefault(win['phase'], [win['actual_start_time'], win['actual_end_time']])[1] = win['actual_end_time']
    cursor = 0.0
    intervals = []
    for phase, (start, end) in bounds.items():
        intervals.append((phase, start, end, cursor))
        cursor += end - start
    output = []
    observations = {}
    for line in path.read_text().splitlines():
        if line.strip():
            rec = json.loads(line)
            if 'error' not in rec and 'pods' in rec and 'sample_time' in rec:
                observations[float(rec['sample_time'])] = rec['pods']
    for when, pods in sorted(observations.items()):
        for index, (phase, start, end, aligned_start) in enumerate(intervals):
            if start <= when < end or (index == len(intervals) - 1 and when == end):
                for node in nodes:
                    selected = [p for p in pods if p.get('node') == node]
                    output.append(dict(phase=phase, sample_time=when,
                                       aligned_sample_s=aligned_start + when - start,
                                       node=node, pod_count=len(selected),
                                       ready_count=sum(bool(p.get('ready')) for p in selected)))
                break
    return output


def _window_points(points, key):
    """Add the requested display origin once; never zero-fill missing data."""
    xs = [r['aligned_end_s'] for r in points]
    ys = [r[key] for r in points]
    if points and abs(xs[0] - points[0]['end_offset_s']) < 1e-9:
        xs.insert(0, 0.0)
        ys.insert(0, 0.0)
    return xs, ys


def load_ready_placements(root, phases, nodes):
    """Return {(phase, node): ready_count} when ready_count is recorded in both methods, else None."""
    result = {}
    for method in METHODS:
        path = Path(root) / method / "pods-by-ramp.csv"
        rows, fields = read_rows(path, ("phase", "node", "pod_count"))
        if "ready_count" not in fields:
            return None
        result[method] = {}
        for row in rows:
            key = (row["phase"].strip(), row["node"].strip())
            result[method][key] = number(row["ready_count"], f"{path}: {key} ready_count", integer=True)
        if set(result[method]) != {(p, n) for p in phases for n in nodes}:
            return None
    return result


def compute_aligned_timeline(phases, durations):
    """Return cumulative [start_s, end_s] per phase in chronological order."""
    bounds = {}
    cursor = 0.0
    for phase, dur in zip(phases, durations):
        d = float(dur)
        bounds[phase] = (cursor, cursor + d)
        cursor += d
    return bounds, cursor


def _export_png(fig, output_dir, name):
    fig.savefig(Path(output_dir) / f"{name}.png", dpi=300)
    plt.close(fig)


def _decorate_time_axes(axes, phases, phase_bounds, total_traffic_s, compact_labels, window_ends, xlabel):
    boundary_ticks = [0.0] + [phase_bounds[p][1] for p in phases]
    phase_mids = [0.5 * (phase_bounds[p][0] + phase_bounds[p][1]) for p in phases]
    if len(boundary_ticks) > 9:
        step = max(2, len(boundary_ticks) // 7)
        sparse_ticks = boundary_ticks[::step]
        if sparse_ticks[-1] != boundary_ticks[-1]:
            sparse_ticks.append(boundary_ticks[-1])
    else:
        sparse_ticks = boundary_ticks

    if total_traffic_s <= 240 and window_ends:
        sparse_ticks = sorted(set([0.0, *window_ends]))

    for ax in axes:
        for b in boundary_ticks[1:-1]:
            ax.axvline(b, color="#888888", linestyle=":", linewidth=0.8)
        ax.set_xlim(0, total_traffic_s)
    bottom = axes[-1]
    bottom.xaxis.set_major_locator(FixedLocator(sparse_ticks))
    bottom.set_xticklabels([f"{t:g}" for t in sparse_ticks], fontsize=8)
    if window_ends:
        bottom.xaxis.set_minor_locator(FixedLocator(sorted(set(window_ends))))
        bottom.tick_params(axis="x", which="minor", length=3, color="#666666")
    # Seconds first, then phase brackets and labels underneath. Physical offsets
    # keep the spacing consistent across figures with different panel heights.
    bracket_transform = bottom.get_xaxis_transform() + ScaledTranslation(
        0, -29 / 72, bottom.figure.dpi_scale_trans)
    text_transform = bottom.get_xaxis_transform() + ScaledTranslation(
        0, -36 / 72, bottom.figure.dpi_scale_trans)
    for phase, mid, label in zip(phases, phase_mids, compact_labels):
        start, end = phase_bounds[phase]
        bottom.plot([start, start, end, end], [0.015, 0, 0, 0.015],
                    transform=bracket_transform, clip_on=False, color="#555555",
                    linewidth=0.7, gid="phase-bracket")
        bottom.text(mid, 0, label, transform=text_transform,
                    ha="center", va="top", fontsize=7 if len(phases) > 8 else 8,
                    clip_on=False, gid="phase-label")
    bottom.set_xlabel(xlabel, labelpad=80)


def _decorate_index_axes(axes, phases, compact_labels, xlabel):
    x = list(range(len(phases)))
    for ax in axes:
        ax.set_xlim(-0.5, len(phases) - 0.5)
    axes[-1].set_xticks(x)
    axes[-1].set_xticklabels(compact_labels, fontsize=7 if len(phases) > 8 else 8)
    axes[-1].set_xlabel(xlabel)


def _render_resource_summary(output_dir, phases, resources, resource_coverage, compact_labels,
                             phase_bounds=None, total_traffic_s=None, window_ends=None, durations=None,
                             name='resource-usage', xlabel=ALIGNED_TIME_NOTE):
    sources = sorted({entry["source"] for m in METHODS for entry in resource_coverage[m]})
    rate_wins = sorted({entry["rate_window_seconds"] for m in METHODS for entry in resource_coverage[m] if entry["rate_window_seconds"]})
    freshnesses = sorted({entry["freshness_seconds"] for m in METHODS for entry in resource_coverage[m] if entry.get("freshness_seconds")})
    details = []
    if rate_wins:
        details.append(f"{', '.join(rate_wins)}s CPU rate window")
    if freshnesses:
        details.append(f"{', '.join(freshnesses)}s freshness")
    source_note = ", ".join(sources) + (f"; {'; '.join(details)}" if details else "")

    xs = ([0.5 * (phase_bounds[p][0] + phase_bounds[p][1]) for p in phases]
          if phase_bounds is not None else list(range(len(phases))))

    fig, axes = plt.subplots(2, 1, figsize=(THESIS_FIG_WIDTH, 6.5), sharex=True, layout="constrained")
    fig.suptitle(f"Phase summary: mean workload resources per phase ({source_note}; gaps/annotations show missingness)")
    for ax, key, title, ylabel in zip(axes[:2], ("cpu_millicores", "memory_mib"),
                                      ("CPU usage (~ = approx. overlapping windows)", "RAM working set (point-in-time)"),
                                      ("Total pod CPU (millicores)", "Total pod memory (MiB)")):
        for method in ("proposed", "baseline"):
            color = METHOD_COLORS[method]
            ys = [optional_number(resources[method][phase][key], key) for phase in phases]
            ax.plot(xs, ys, linestyle=STYLES[method], color=color, marker="o", markersize=4,
                    label=METHOD_LABELS[method])
            for pos, val, cov_info in zip(xs, ys, resource_coverage[method]):
                is_approx = key == "cpu_millicores" and cov_info.get("cpu_quality", "exact") in ("approximate", "mixed_exact_and_approximate")
                denom = cov_info.get("expected") or (cov_info["rounds"] + cov_info["missing"])
                if math.isnan(val):
                    tag = f"{METHOD_LABELS[method][0]}:0/{denom}" if denom else f"{METHOD_LABELS[method][0]}:N/A"
                    ax.annotate(tag, (pos, 0), ha="center", va="bottom", fontsize=7,
                                xytext=(0, 2 if method == "baseline" else 10), textcoords="offset points")
                elif cov_info["coverage_pct"] < 100.0 or is_approx:
                    prefix = "~" if is_approx else ""
                    ax.annotate(f"{prefix}{cov_info['complete']}/{denom}", (pos, val),
                                ha="center", va="bottom", fontsize=7,
                                xytext=(0, 4), textcoords="offset points")
        ax.set(ylabel=ylabel, title=title, ylim=(0, None))
        ax.grid(axis="y", linestyle=":")
        ax.legend(frameon=False, ncols=2)
    if phase_bounds is not None:
        _decorate_time_axes(axes, phases, phase_bounds, total_traffic_s, compact_labels, window_ends, xlabel)
    else:
        _decorate_index_axes(axes, phases, compact_labels, "Phase; total UE; gNB UE distribution")
    _export_png(fig, output_dir, name)


def _render_resource_windows(output_dir, phases, rows_by_method, decorate, continuous=False):
    def groups(method):
        return ([rows_by_method[method]] if continuous else
                [[r for r in rows_by_method[method] if r['phase'] == phase] for phase in phases])
    fig, axes = plt.subplots(2, 1, figsize=(THESIS_FIG_WIDTH, 6.5), sharex=True, layout='constrained')
    rates = sorted({r['rate_window_seconds'] for rows in rows_by_method.values() for r in rows})
    fig.suptitle('Workload resources: 15-second reporting windows\n' + ORIGIN_NOTE)
    for ax, key, title, ylabel in zip(
        axes[:2], ('cpu_millicores', 'memory_mib'),
        (f"CPU: mean of rolling rates ({', '.join(f'{v:g}s' for v in rates)} lookback)",
         'RAM: mean sampled working set in preceding 15-second window'),
        ('Total Pod CPU (millicores)', 'Total Pod memory (MiB)'),
    ):
        finite = False
        for method in METHODS:
            for pi, points in enumerate(groups(method)):
                finite |= any(math.isfinite(r[key]) for r in points)
                ax.plot(*_window_points(points, key),
                        linestyle=STYLES[method], color=METHOD_COLORS[method], marker='o', markersize=3,
                        label=METHOD_LABELS[method] if pi == 0 else None)
        if not finite:
            ax.text(0.5, 0.5, 'Saved raw resource measurements unavailable',
                    ha='center', transform=ax.transAxes)
        ax.set(title=title, ylabel=ylabel, ylim=(0, None))
        ax.grid(axis='y', linestyle=':')
        ax.legend(frameon=False, ncols=2)
    decorate(axes)
    _export_png(fig, output_dir, 'resource-usage')


def _placement_bars(ax, lefts, widths, counts, nodes, colors, labels=True):
    """Stack node counts; omit a whole observation if any node is unknown."""
    valid = [i for i in range(len(lefts))
             if all(math.isfinite(counts[node][i]) for node in nodes)]
    bottom = [0.0] * len(valid)
    for node in nodes:
        heights = [counts[node][i] for i in valid]
        ax.bar([lefts[i] for i in valid], heights,
               width=[widths[i] for i in valid], bottom=bottom, align="edge",
               color=colors[node], label=node if labels else None)
        bottom = [b + h for b, h in zip(bottom, heights)]
    return bottom


def _render_phase_summary_fallbacks(output_dir, phases, summaries, violations, coverage,
                                    placements, ready_placements, nodes, slo_ms, compact_labels,
                                    phase_bounds, total_traffic_s):
    xs = ([0.5 * (phase_bounds[p][0] + phase_bounds[p][1]) for p in phases]
          if phase_bounds is not None else list(range(len(phases))))
    xlabel = ALIGNED_TIME_NOTE if phase_bounds is not None else "Phase; total UE; gNB UE distribution"

    def finish(axes):
        if phase_bounds is not None:
            _decorate_time_axes(axes, phases, phase_bounds, total_traffic_s, compact_labels, [], xlabel)
        else:
            _decorate_index_axes(axes, phases, compact_labels, xlabel)

    fig, axes = plt.subplots(2, 1, figsize=(THESIS_FIG_WIDTH, 6.5), sharex=True, layout="constrained")
    fig.suptitle("Phase summary: successful request response time (whole-phase)")
    for ax, key, title in zip(axes, ("p90_ms", "p50_ms"),
                              ("Phase-summary P90 response time", "Phase-summary P50 response time")):
        for method in ("proposed", "baseline"):
            ax.plot(xs, [optional_number(summaries[method][p][key], key) for p in phases],
                    linestyle=STYLES[method], color=METHOD_COLORS[method], marker="o", markersize=4,
                    label=METHOD_LABELS[method])
        if key == "p90_ms":
            ax.axhline(slo_ms, color="#d62728", linestyle="--", linewidth=1.2, label=f"SLO ({slo_ms:g} ms)")
        ax.set(ylabel="Response time (ms)", title=title, ylim=(0, None))
        ax.grid(axis="y", linestyle=":")
        ax.legend(frameon=False, ncols=3)
    finish(axes)
    _export_png(fig, output_dir, "response-time-phase-summary")

    fig, axes = plt.subplots(2, 1, figsize=(THESIS_FIG_WIDTH, 6.5), sharex=True, layout="constrained")
    fig.suptitle("Phase summary: SLO violations and request coverage (whole-phase)")
    ax, cov_ax = axes
    for method in ("baseline", "proposed"):
        ax.plot(xs, violations[method], linestyle=STYLES[method], color=METHOD_COLORS[method],
                marker="o", markersize=4, label=METHOD_LABELS[method])
        cov_ax.plot(xs, coverage[method], linestyle=STYLES[method], color=METHOD_COLORS[method],
                    marker="o", markersize=4, label=METHOD_LABELS[method])
    ax.set(ylim=(0, 100), ylabel="SLO violations (%)",
           title=f"Phase-summary failed or slower than {slo_ms:g} ms (denominator: all attempts)")
    ax.grid(axis="y", linestyle=":")
    ax.legend(frameon=False, ncols=2)
    cov_ax.set(ylim=(0, 105), ylabel="Request coverage (%)",
               title="Phase-summary completed / scheduled requests")
    cov_ax.grid(axis="y", linestyle=":")
    finish(axes)
    _export_png(fig, output_dir, "slo-violations-phase-summary")

    source_counts = ready_placements if ready_placements is not None else placements
    count_label = "Ready replicas" if ready_placements is not None else "Total pods (ready counts unavailable)"
    title_suffix = "ready replica counts near phase end" if ready_placements is not None else "total pod counts near phase end (ready counts unavailable)"
    fig, axes = plt.subplots(2, 1, figsize=(THESIS_FIG_WIDTH, 6.0), sharex=True, sharey=True, layout="constrained")
    fig.suptitle(f"Phase summary: per-node {title_suffix}")
    palette = plt.get_cmap("tab10").colors
    colors = dict(zip(nodes, palette))
    all_counts = []
    widths = ([0.7 * (phase_bounds[p][1] - phase_bounds[p][0]) for p in phases]
              if phase_bounds is not None else [0.7] * len(phases))
    for ax, method in zip(axes, METHODS):
        counts = {node: [source_counts[method][(p, node)] for p in phases] for node in nodes}
        all_counts.extend(_placement_bars(
            ax, [x - w / 2 for x, w in zip(xs, widths)], widths, counts, nodes, colors))
        ax.set(ylabel=count_label, title=METHOD_LABELS[method])
        ax.grid(axis="y", linestyle=":")
    max_cnt = max(all_counts) if all_counts else 1
    axes[0].set_ylim(0, max(1, math.ceil(max_cnt)) + max(1.5, 0.2 * max_cnt))
    axes[0].yaxis.set_major_locator(MaxNLocator(integer=True))
    axes[0].legend(frameon=False, ncols=min(len(nodes), 5), loc="upper right")
    finish(axes)
    _export_png(fig, output_dir, "pod-placement-phase-summary")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", type=Path, help="directory containing baseline/ and proposed/ CSV inputs")
    parser.add_argument("output_dir", type=Path, help="directory for 300 DPI PNG figures and window CSVs")
    parser.add_argument("--slo-ms", type=float, default=500,
                        help="controller p90 target and recorded request SLO in milliseconds (default: 500)")
    args = parser.parse_args()
    try:
        summaries, placements, resources, site_keys, phases, nodes = load(args.results_dir)
        violations = load_slo(args.results_dir, phases, args.slo_ms)
        coverage = load_coverage(args.results_dir, phases)
        resource_coverage = load_resource_coverage(args.results_dir, phases)
        durations = load_durations(args.results_dir, phases)
        ready_placements = load_ready_placements(args.results_dir, phases, nodes)
    except ValueError as exc:
        parser.error(str(exc))

    has_requests = all((args.results_dir / m / "requests.jsonl").exists() for m in METHODS)
    compact_labels = [
        f"{p.replace('phase_', 'P')}\n{summaries['baseline'][p]['workload_ue']} UE\n"
        + ":".join(summaries["baseline"][p][key] for key in site_keys)
        for p in phases
    ]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # Remove the retired generated view when replotting an existing directory.
    (args.output_dir / 'late-phase.png').unlink(missing_ok=True)

    if not has_requests:
        print(
            "Skipping 15s detailed plots: raw requests.jsonl missing for at least one method; "
            "writing phase-summary plots only.",
            file=sys.stderr,
        )
        phase_bounds, total_traffic_s = (
            compute_aligned_timeline(phases, durations) if durations is not None else (None, None)
        )
        with plt.rc_context({"font.size": 9, "axes.titlesize": 10, "legend.fontsize": 8}):
            _render_phase_summary_fallbacks(
                args.output_dir, phases, summaries, violations, coverage,
                placements, ready_placements, nodes, args.slo_ms, compact_labels,
                phase_bounds, total_traffic_s,
            )
            _render_resource_summary(
                args.output_dir, phases, resources, resource_coverage, compact_labels,
                phase_bounds=phase_bounds, total_traffic_s=total_traffic_s,
                window_ends=[], durations=durations,
            )
        return

    windows_15s = {
        method: build_phase_windows_15s(args.results_dir / method, args.slo_ms)
        for method in METHODS
    }
    continuous = all(r.get('phase_transition') == 'continuous' for rows in windows_15s.values() for r in rows)
    traffic_label = ('Traffic elapsed time (s); moving-UE reattachment is included'
                     if continuous else ALIGNED_TIME_NOTE)
    if durations is None:
        dur_by_phase = {r["phase"]: r["phase_duration_s"] for r in windows_15s["baseline"]}
        durations = [dur_by_phase[p] for p in phases]

    phase_bounds, total_traffic_s = compute_aligned_timeline(phases, durations)
    phase_idx_map = {p: i for i, p in enumerate(phases, 1)}

    win_by_phase = {m: {p: [] for p in phases} for m in METHODS}
    export_win_rows = []
    window_ends = []
    for method in METHODS:
        for r in windows_15s[method]:
            p = r["phase"]
            if p not in win_by_phase[method]:
                continue
            p_start, _ = phase_bounds[p]
            row = {
                "method": method,
                "phase": p,
                "phase_index": phase_idx_map[p],
                "workload_ue": summaries["baseline"][p]["workload_ue"],
                **{k: summaries["baseline"][p][k] for k in site_keys},
                **r,
                "aligned_start_s": p_start + r["start_offset_s"],
                "aligned_end_s": p_start + r["end_offset_s"],
            }
            win_by_phase[method][p].append(row)
            export_win_rows.append(row)
            window_ends.append(row["aligned_end_s"])
    win_fields = [
        "method", "phase", "phase_index", "workload_ue", *site_keys,
        "window_index", "start_offset_s", "end_offset_s", "phase_duration_s",
        "aligned_start_s", "aligned_end_s", "actual_start_time", "actual_end_time",
        "attempts", "successes", "failures", "p50_ms", "p90_ms",
        "slo_met_attempts", "violation_percent",
        "scheduled", "started_slots", "missed_slots", "request_coverage_percent",
    ]
    write_csv_rows(export_win_rows, win_fields, args.output_dir / "window-metrics-15s.csv")

    pod_by_phase = {m: {p: [] for p in phases} for m in METHODS}
    export_pod_rows = []
    for method in METHODS:
        pod_rows = build_pod_placement_15s(
            args.results_dir / method, windows_15s[method], nodes, freshness_s=POD_LOOKUP_MAX_AGE_S
        )
        for r in pod_rows:
            p = r["phase"]
            if p not in pod_by_phase[method]:
                continue
            p_start, _ = phase_bounds[p]
            row = {
                "method": method,
                "phase_index": phase_idx_map[p],
                **r,
                "aligned_end_s": p_start + r["end_offset_s"],
            }
            pod_by_phase[method][p].append(row)
            export_pod_rows.append(row)
    pod_fields = [
        "method", "phase", "phase_index", "window_index",
        "start_offset_s", "end_offset_s", "aligned_end_s", "window_end_time",
        "node", "ready_count", "pod_count", "sample_time",
        "sample_age_seconds", "freshness_seconds", "status",
    ]
    write_csv_rows(export_pod_rows, pod_fields, args.output_dir / "pod-placement-15s.csv")

    def decorate(axes):
        _decorate_time_axes(
            axes, phases, phase_bounds, total_traffic_s, compact_labels, window_ends, traffic_label
        )

    def window_groups(method):
        groups = [win_by_phase[method][phase] for phase in phases]
        return [[row for group in groups for row in group]] if continuous else groups

    with plt.rc_context({"font.size": 9, "axes.titlesize": 10, "legend.fontsize": 8}):
        fig, axes = plt.subplots(2, 1, figsize=(THESIS_FIG_WIDTH, 6.5), sharex=True, layout="constrained")
        fig.suptitle("Successful-request response time over preceding 15-second window\n" + ORIGIN_NOTE)
        for ax, key, title in zip(
            axes,
            ("p90_ms", "p50_ms"),
            ("Successful-request p90 over preceding 15-second window",
             "Successful-request p50 over preceding 15-second window"),
        ):
            for method in ("proposed", "baseline"):
                color = METHOD_COLORS[method]
                labeled = False
                for pts in window_groups(method):
                    xs, ys = _window_points(pts, key)
                    ax.plot(xs, ys, linestyle=STYLES[method], color=color, marker="o", markersize=3,
                            label=METHOD_LABELS[method] if not labeled else None)
                    labeled = True
            if key == "p90_ms":
                ax.axhline(args.slo_ms, color="#d62728", linestyle="--", linewidth=1.2,
                           label=f"SLO ({args.slo_ms:g} ms)")
            ax.set(ylabel="Response time (ms)", title=title, ylim=(0, None))
            ax.grid(axis="y", linestyle=":")
            ax.legend(frameon=False, ncols=3)
        decorate(axes)
        _export_png(fig, args.output_dir, "response-time")

        fig, axes = plt.subplots(2, 1, figsize=(THESIS_FIG_WIDTH, 6.5), sharex=True, layout="constrained")
        # Preserve phase diagnostic exports for existing analysis consumers.
        for method in METHODS:
            write_windows(summarize_windows(args.results_dir / method, args.slo_ms),
                          args.output_dir / f'{method}-phase-windows.csv')
        fig.suptitle('Response time: 15-second windows, zoomed\n' + ORIGIN_NOTE)
        for ax, key in zip(axes, ('p90_ms', 'p50_ms')):
            for method in METHODS:
                for index, pts in enumerate(window_groups(method)):
                    xs, values = _window_points(pts, key)
                    clipped = [min(v, 300) if math.isfinite(v) else math.nan for v in values]
                    ax.plot(xs, clipped, linestyle=STYLES[method], color=METHOD_COLORS[method],
                            marker='o', markersize=3, label=METHOD_LABELS[method] if index == 0 else None)
                    for x, value in zip(xs, values):
                        if math.isfinite(value) and value > 300:
                            ax.plot(x, 295, marker='^', color=METHOD_COLORS[method], markersize=4)
            ax.axhline(args.slo_ms, color='#d62728', linestyle='--', label=f'SLO ({args.slo_ms:g} ms)')
            ax.set(ylim=(0, 300), ylabel='Response time (ms)',
                   title=f"Successful-request {key.split('_')[0]} over preceding 15-second window (zoom ≤ 300 ms)")
            ax.grid(axis='y', linestyle=':')
            ax.legend(frameon=False, ncols=3)
        decorate(axes)
        _export_png(fig, args.output_dir, 'response-time-zoom')

        fig, axes = plt.subplots(2, 1, figsize=(THESIS_FIG_WIDTH, 6.5), sharex=True, layout="constrained")
        fig.suptitle("SLO violations and request coverage\n" + ORIGIN_NOTE)
        ax, cov_ax = axes
        for method in ("baseline", "proposed"):
            color = METHOD_COLORS[method]
            labeled = False
            for pts in window_groups(method):
                xs, viol_vals = _window_points(pts, "violation_percent")
                cov_xs = [r["aligned_end_s"] for r in pts]
                cov_vals = [
                    r["request_coverage_percent"] if r["request_coverage_percent"] != "" else math.nan
                    for r in pts
                ]
                ax.plot(xs, viol_vals, linestyle=STYLES[method], color=color, marker="o", markersize=3,
                        label=METHOD_LABELS[method] if not labeled else None)
                cov_ax.plot(cov_xs, cov_vals, linestyle=STYLES[method], color=color, marker="o", markersize=3,
                            label=METHOD_LABELS[method] if not labeled else None)
                labeled = True
        ax.set(ylim=(0, 100), ylabel="SLO violations (%)",
               title=f"Failed or slower than {args.slo_ms:g} ms over preceding 15-second window (denominator: all attempts)")
        ax.grid(axis="y", linestyle=":")
        ax.legend(frameon=False, ncols=2)
        cov_ax.set(ylim=(0, 105), ylabel="Request coverage (%)",
                   title="Request coverage over preceding 15-second window (completed / scheduled)")
        cov_ax.grid(axis="y", linestyle=":")
        decorate(axes)
        _export_png(fig, args.output_dir, "slo-violations")

        fig, axes = plt.subplots(2, 1, figsize=(THESIS_FIG_WIDTH, 6.0), sharex=True, sharey=True, layout="constrained")
        fig.suptitle("Per-node pod counts at recorded observation times (includes unready pods)")
        palette = plt.get_cmap("tab10").colors
        colors = dict(zip(nodes, palette))
        all_ready = []
        snapshot_rows = []
        for ax, method in zip(axes, METHODS):
            rows = build_pod_observations(args.results_dir / method, windows_15s[method], nodes)
            snapshot_rows.extend(dict(method=method, **r) for r in rows)
            for phase_index, phase in enumerate(phases):
                by_node = {node: [r for r in rows if r['phase'] == phase and r['node'] == node] for node in nodes}
                observations = by_node[nodes[0]]
                times = [r['aligned_sample_s'] for r in observations]
                # Thin, centered bars show snapshots rather than interval totals.
                widths = [1.5] * len(times)
                lefts = [t - w / 2 for t, w in zip(times, widths)]
                counts = {node: [r['pod_count'] for r in by_node[node]] for node in nodes}
                all_ready.extend(_placement_bars(ax, lefts, widths, counts, nodes, colors, labels=phase_index == 0))
            ax.set(ylabel="Pods", title=METHOD_LABELS[method])
            ax.grid(axis="y", linestyle=":")
        write_csv_rows(snapshot_rows, ['method', 'phase', 'sample_time', 'aligned_sample_s',
                                     'node', 'pod_count', 'ready_count'],
                       args.output_dir / 'pod-placement-observations.csv')
        max_ready = max(all_ready) if all_ready else 1
        axes[0].set_ylim(0, max(1, math.ceil(max_ready)) + max(1.5, 0.2 * max_ready))
        axes[0].yaxis.set_major_locator(MaxNLocator(integer=True))
        axes[0].legend(frameon=False, ncols=min(len(nodes), 5), loc="upper right")
        decorate(axes)
        _export_png(fig, args.output_dir, "pod-placement")

        resource_rows = {}
        for method in METHODS:
            resource_rows[method] = build_resource_windows(args.results_dir / method, windows_15s[method])
            for row in resource_rows[method]:
                row['method'] = method
                row['aligned_end_s'] = phase_bounds[row['phase']][0] + row['end_offset_s']
        write_csv_rows([r for rows in resource_rows.values() for r in rows], [
            'method', 'phase', 'window_index', 'start_offset_s', 'end_offset_s',
            'actual_start_time', 'actual_end_time', 'aligned_end_s', 'resource_source',
            'rate_window_seconds', 'freshness_seconds', 'evaluation_step_seconds',
            'cpu_millicores', 'memory_mib', 'expected_samples', 'cpu_complete_samples',
            'memory_complete_samples', 'cpu_coverage_percent', 'memory_coverage_percent', 'missing_reasons',
        ], args.output_dir / 'resource-windows-15s.csv')
        _render_resource_windows(args.output_dir, phases, resource_rows, decorate, continuous=continuous)
        _render_resource_summary(
            args.output_dir, phases, resources, resource_coverage, compact_labels,
            phase_bounds=phase_bounds, total_traffic_s=total_traffic_s,
            window_ends=window_ends, durations=durations, name='resource-usage-phase-summary', xlabel=traffic_label,
        )


if __name__ == "__main__":
    main()
