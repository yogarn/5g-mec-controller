#!/usr/bin/env python3
"""Generate final per-phase thesis plots (300-dpi PNG and plotted-value CSVs)."""
import argparse
import collections
import csv
import json
import math
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import LogLocator, MaxNLocator, NullFormatter, StrMethodFormatter

FINAL_DIR = Path("/home/member/skripsi_yoga/5g-mec-controller/results/final")
OUTPUT_DIR = FINAL_DIR / "plots"
SLO_MS = 150.0
TIMEOUT_MS = 10000.0
TIMEOUT_COLOR = "#C00000"
FIG_WIDTH = 7.2

NODES = ("gnb1", "gnb-2", "gnb-3", "core-5g")
NODE_COLORS = {
    "gnb1": "#0072B2",
    "gnb-2": "#E69F00",
    "gnb-3": "#009E73",
    "core-5g": "#CC79A7",
}

FOUR_ARMS = ("baseline-local", "proposed-local", "baseline-vertical", "proposed-vertical")
TWO_ARMS = ("baseline", "proposed")

SCENARIOS = {
    "joint": {
        "dir": "vertical_joint_full_20261122T160552Z",
        "arms": FOUR_ARMS,
        "source": "vertical",
        "labels": {
            "baseline-local": "CPU-based HPA + Default scheduler + RAN only",
            "baseline-vertical": "CPU-based HPA + Default scheduler + Vertical offloading",
            "proposed-local": "SLO-based HPA + Mobility-aware scheduler + RAN only",
            "proposed-vertical": "SLO-based HPA + Mobility-aware scheduler + Vertical offloading (Proposed)",
        },
    },
    "mobility": {
        "dir": "vertical_mobility_full_20261122T151516Z",
        "arms": FOUR_ARMS,
        "source": "vertical",
        "labels": {
            "baseline-local": "Default scheduler + RAN only",
            "baseline-vertical": "Default scheduler + Vertical offloading",
            "proposed-local": "Mobility-aware scheduler + RAN only",
            "proposed-vertical": "Mobility-aware scheduler + Vertical offloading (Proposed)",
        },
    },
    "scaling": {
        "dir": "scaling_20261122T165359Z",
        "arms": TWO_ARMS,
        "source": "scaling",
        "labels": {
            "baseline": "CPU-based HPA (Baseline)",
            "proposed": "SLO-based HPA (Proposed)",
        },
    },
}

# Consistent colorblind-safe palette across all scenarios:
# Baseline = warm orange/vermilion tones, Proposed = blue tones;
# RAN only (local) = dashed, Vertical offloading / full proposed = solid.
ARM_STYLES = {
    "baseline-local": {"color": "#E69F00", "linestyle": "--", "dashes": (4, 2), "marker": "o"},
    "baseline-vertical": {"color": "#D55E00", "linestyle": "-", "marker": "s"},
    "proposed-local": {"color": "#56B4E9", "linestyle": "--", "dashes": (4, 2), "marker": "^"},
    "proposed-vertical": {"color": "#0072B2", "linestyle": "-", "marker": "D"},
    "baseline": {"color": "#D55E00", "linestyle": "--", "dashes": (4, 2), "marker": "s"},
    "proposed": {"color": "#0072B2", "linestyle": "-", "marker": "D"},
}

RESOURCE_NOTE = (
    "CPU rate window is 45 s and can cross phase boundaries "
    "(averages of valid observations, not exact phase CPU consumption)."
)
POD_NOTE = (
    "End-of-phase snapshot (~2 s before phase end); "
    "hatched = static site anchors & core fallback pods, solid = dynamic pods; pending excluded."
)

RC_STYLE = {
    "font.size": 9,
    "axes.titlesize": 10,
    "axes.labelsize": 9,
    "xtick.labelsize": 7.5,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
}


def parse_float(value):
    """Parse a float or return NaN if blank/missing."""
    if value is None or str(value).strip() == "":
        return math.nan
    try:
        parsed = float(value)
    except ValueError:
        return math.nan
    return parsed if math.isfinite(parsed) else math.nan


def parse_int(value):
    """Parse an integer or return None if blank/missing."""
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def fmt_csv(value):
    """Format a number for CSV output, leaving missing values blank."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    if isinstance(value, int):
        return str(value)
    return f"{value:.6f}"


def phase_tick_label(phase, info):
    """Format multi-line x-axis tick with phase, total UEs, and gnb1:gnb2:gnb3 split."""
    short = phase.replace("phase_", "P")
    if short.startswith("P") and short[1:].isdigit():
        short = f"P{int(short[1:])}"
    ue_str = "?" if info["workload_ue"] is None else str(info["workload_ue"])
    dist = ":".join("?" if info[k] is None else str(info[k]) for k in ("gnb1_ues", "gnb2_ues", "gnb3_ues"))
    return f"{short}\n{ue_str} UE\n{dist}"


def phase_csv_label(phase, info):
    """Format single-line phase label with UE count and distribution for CSV tables."""
    short = phase.replace("phase_", "P")
    if short.startswith("P") and short[1:].isdigit():
        short = f"P{int(short[1:])}"
    ue_str = "?" if info["workload_ue"] is None else str(info["workload_ue"])
    dist = ":".join("?" if info[k] is None else str(info[k]) for k in ("gnb1_ues", "gnb2_ues", "gnb3_ues"))
    return f"{short} ({ue_str} UE, {dist})"


def read_csv_rows(path):
    """Read all rows from a CSV file as dicts."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def nearest_rank_p90_by_phase(requests_path):
    """p90 (ms) of successful requests per phase by nearest rank: value at ceil(0.9 * n).

    Same definition as utils.vertical_summary, so every figure uses one method.
    """
    latencies = {}
    with requests_path.open(encoding="utf-8") as handle:
        for line in handle:
            if '"request"' not in line:
                continue
            record = json.loads(line)
            if record.get("event") == "request" and record.get("success"):
                latencies.setdefault(record["phase"], []).append(float(record["duration_ms"]))
    return {phase: sorted(values)[math.ceil(0.9 * len(values)) - 1]
            for phase, values in latencies.items()}


def load_phase_metrics(run_dir, scenario_cfg):
    """Load per-phase p90 response time, SLO violation (%), and UE distribution."""
    arms = scenario_cfg["arms"]
    rows = []
    missing = []

    if scenario_cfg["source"] == "vertical":
        raw_rows, _ = read_csv_rows(run_dir / "vertical-4arm-phases.csv")
        for row in raw_rows:
            arm = row["arm"].strip()
            if arm not in arms:
                continue
            phase = row["phase"].strip()
            p90 = parse_float(row.get("p90_ms"))
            viol_rate = parse_float(row.get("slo_violation_rate"))
            viol_pct = viol_rate * 100.0 if not math.isnan(viol_rate) else math.nan
            if math.isnan(p90):
                missing.append((arm, phase, "p90_ms"))
            if math.isnan(viol_pct):
                missing.append((arm, phase, "slo_violation_rate"))
            rows.append({
                "arm": arm,
                "phase": phase,
                "workload_ue": parse_int(row.get("workload_ue")),
                "gnb1_ues": parse_int(row.get("gnb1_ues")),
                "gnb2_ues": parse_int(row.get("gnb2_ues")),
                "gnb3_ues": parse_int(row.get("gnb3_ues")),
                "p90_ms": p90,
                "slo_violation_pct": viol_pct,
            })
    else:
        for arm in arms:
            raw_rows, _ = read_csv_rows(run_dir / arm / "summary.csv")
            p90_by_phase = nearest_rank_p90_by_phase(run_dir / arm / "requests.jsonl")
            for row in raw_rows:
                phase = row["phase"].strip()
                p90 = p90_by_phase.get(phase, math.nan)
                attainment = parse_float(row.get("slo_attainment_all_attempts"))
                viol_pct = (1.0 - attainment) * 100.0 if not math.isnan(attainment) else math.nan
                if math.isnan(p90):
                    missing.append((arm, phase, "p90_ms (requests.jsonl)"))
                if math.isnan(viol_pct):
                    missing.append((arm, phase, "slo_attainment_all_attempts"))
                rows.append({
                    "arm": arm,
                    "phase": phase,
                    "workload_ue": parse_int(row.get("workload_ue")),
                    "gnb1_ues": parse_int(row.get("gnb1_ues")),
                    "gnb2_ues": parse_int(row.get("gnb2_ues")),
                    "gnb3_ues": parse_int(row.get("gnb3_ues")),
                    "p90_ms": p90,
                    "slo_violation_pct": viol_pct,
                })

    phases = []
    phase_info = {}
    for row in rows:
        if row["phase"] not in phase_info:
            phases.append(row["phase"])
            phase_info[row["phase"]] = {
                "workload_ue": row["workload_ue"],
                "gnb1_ues": row["gnb1_ues"],
                "gnb2_ues": row["gnb2_ues"],
                "gnb3_ues": row["gnb3_ues"],
            }
    return rows, phases, phase_info, missing


def load_resources(run_dir, arms, phases):
    """Load per-phase CPU (millicores) and memory (MiB) for all arms."""
    rows = []
    missing = []
    partial = []

    for arm in arms:
        raw_rows, _ = read_csv_rows(run_dir / arm / "resource-by-ramp.csv")
        by_phase = {r["phase"].strip(): r for r in raw_rows}
        for phase in phases:
            row = by_phase.get(phase)
            if row is None:
                missing.append((arm, phase, "resource_row"))
                rows.append({
                    "arm": arm, "phase": phase,
                    "cpu_millicores": math.nan, "memory_mib": math.nan,
                    "cpu_completeness_status": "missing",
                    "memory_completeness_status": "missing",
                    "rate_window_seconds": "",
                })
                continue
            cpu = parse_float(row.get("cpu_millicores"))
            mem = parse_float(row.get("memory_mib"))
            cpu_status = (row.get("cpu_completeness_status") or "").strip()
            mem_status = (row.get("memory_completeness_status") or "").strip()
            if math.isnan(cpu):
                missing.append((arm, phase, "cpu_millicores"))
            if math.isnan(mem):
                missing.append((arm, phase, "memory_mib"))
            if cpu_status and cpu_status != "complete":
                partial.append((arm, phase, "cpu", cpu_status))
            if mem_status and mem_status != "complete":
                partial.append((arm, phase, "memory", mem_status))
            rows.append({
                "arm": arm,
                "phase": phase,
                "cpu_millicores": cpu,
                "memory_mib": mem,
                "cpu_completeness_status": cpu_status,
                "memory_completeness_status": mem_status,
                "rate_window_seconds": (row.get("rate_window_seconds") or "").strip(),
            })
    return rows, missing, partial


def _read_core_backend_replicas(run_dir):
    """Read top-level static core_backend_replicas from campaign.yaml if present."""
    cy = Path(run_dir) / "campaign.yaml"
    if not cy.exists():
        return 0
    for line in cy.read_text(encoding="utf-8").splitlines():
        if line.startswith("core_backend_replicas:"):
            val = line.split(":", 1)[1].strip()
            parsed = parse_int(val)
            return parsed if parsed is not None and parsed > 0 else 0
    return 0


def _count_anchor_pods(pod_names_str):
    """Count static site-anchor pods from semicolon-separated pod_names."""
    if not pod_names_str:
        return 0
    return sum(1 for name in pod_names_str.split(";") if "-anchor-" in name.strip())


def load_pods(run_dir, arms, phases):
    """Load end-of-phase pod placement snapshots by node, splitting static anchors vs dynamic pods."""
    rows = []
    missing = []
    pending_nonzero = []
    core_backend_replicas = _read_core_backend_replicas(run_dir)

    for arm in arms:
        raw_rows, _ = read_csv_rows(run_dir / arm / "pods-by-ramp.csv")
        by_phase_node = {}
        anchors_by_phase_node = {}
        age_by_phase = {}
        for r in raw_rows:
            ph = r["phase"].strip()
            nd = r["node"].strip()
            by_phase_node[(ph, nd)] = parse_int(r.get("pod_count"))
            anchors_by_phase_node[(ph, nd)] = _count_anchor_pods(r.get("pod_names", ""))
            if ph not in age_by_phase:
                age_by_phase[ph] = parse_float(r.get("sample_age_seconds"))

        static_core = core_backend_replicas if arm.endswith("-vertical") else 0

        for phase in phases:
            entry = {
                "arm": arm,
                "phase": phase,
                "sample_age_seconds": age_by_phase.get(phase, math.nan),
            }
            for node in NODES:
                count = by_phase_node.get((phase, node))
                if count is None:
                    missing.append((arm, phase, f"pods:{node}"))
                    entry[node] = None
                    entry[f"{node}_anchor"] = 0
                    entry[f"{node}_dynamic"] = 0
                else:
                    anchor = anchors_by_phase_node.get((phase, node), 0)
                    dyn = max(0, count - anchor)
                    if node == "core-5g" and static_core > 0:
                        anchor += static_core
                        count += static_core
                    entry[node] = count
                    entry[f"{node}_anchor"] = anchor
                    entry[f"{node}_dynamic"] = dyn
            pending = by_phase_node.get((phase, "<pending>"), 0)
            entry["pending_pods"] = 0 if pending is None else pending
            if entry["pending_pods"] > 0:
                pending_nonzero.append((arm, phase, entry["pending_pods"]))
            rows.append(entry)
    return rows, missing, pending_nonzero


def save_outputs(fig, out_dir, basename, fieldnames, csv_rows, note=None):
    """Write 300-dpi PNG and exact plotted-values CSV."""
    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / f"{basename}.png"
    csv_path = out_dir / f"{basename}.csv"

    png_meta = {"Description": note} if note else None
    fig.savefig(png_path, dpi=300, metadata=png_meta)
    plt.close(fig)

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)

    return [png_path, csv_path]


def _make_figure(panel_count, legend_rows, panel_height, sharey=False):
    """Create a figure with a dedicated non-overlapping legend band below suptitle."""
    legend_h = 0.24 * legend_rows + 0.12
    total_h = legend_h + panel_count * panel_height + 0.8
    fig = plt.figure(figsize=(FIG_WIDTH, total_h), layout="constrained")
    grid = fig.add_gridspec(panel_count + 1, 1, height_ratios=[legend_h] + [panel_height] * panel_count)
    legend_ax = fig.add_subplot(grid[0])
    legend_ax.set_axis_off()
    axes = []
    for i in range(panel_count):
        kwargs = {}
        if i > 0:
            kwargs["sharex"] = axes[0]
            if sharey:
                kwargs["sharey"] = axes[0]
        ax = fig.add_subplot(grid[i + 1], **kwargs)
        if i < panel_count - 1:
            ax.tick_params(labelbottom=False)
        axes.append(ax)
    return fig, legend_ax, axes


def _draw_legend(legend_ax, source_ax, ncol=1):
    """Render legend inside its dedicated band with long handles so dashes show clearly."""
    handles, labels = source_ax.get_legend_handles_labels()
    legend_ax.legend(handles, labels, loc="center", ncol=ncol, frameon=False, handlelength=3.8)


def _style_phase_axis(ax, labels):
    """Apply multi-line phase + UE distribution tick labels along the x-axis."""
    positions = list(range(len(labels)))
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    ax.set_xlabel("Phase (Active UEs, gnb1:gnb-2:gnb-3)")
    ax.grid(axis="y", linestyle=":", alpha=0.6)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _format_log_y(ax):
    """Format log y-axis with plain numeric tick labels."""
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:g}"))
    ax.yaxis.set_minor_formatter(NullFormatter())


def plot_p90(scenario, cfg, metrics, phases, phase_info, out_dir):
    """Plot per-phase p90 response time (log y-axis) with 150 ms SLO line."""
    labels = [phase_tick_label(p, phase_info[p]) for p in phases]
    lookup = {(r["arm"], r["phase"]): r["p90_ms"] for r in metrics}
    xs = list(range(len(phases)))
    ncol = 1 if len(cfg["arms"]) > 2 else 3
    legend_rows = 5 if ncol == 1 else 1

    with plt.rc_context(RC_STYLE):
        fig, legend_ax, (ax,) = _make_figure(1, legend_rows, panel_height=3.2)
        for arm in cfg["arms"]:
            ys = [lookup.get((arm, p), math.nan) for p in phases]
            ys_plot = [y if (not math.isnan(y) and y > 0) else math.nan for y in ys]
            ax.plot(xs, ys_plot, linewidth=1.7, markersize=4.5, label=cfg["labels"][arm], **ARM_STYLES[arm])

        ax.axhline(SLO_MS, color="#444444", linestyle=":", linewidth=1.4, label=f"SLO ({SLO_MS:g} ms)")
        _format_log_y(ax)
        _style_phase_axis(ax, labels)
        ax.set_ylabel("p90 Response Time (ms, log scale)")
        fig.suptitle("p90 Response Time per Phase", fontsize=11)
        _draw_legend(legend_ax, ax, ncol=ncol)

        csv_rows = [
            {
                "phase": p,
                "phase_label": phase_csv_label(p, phase_info[p]),
                "workload_ue": phase_info[p]["workload_ue"],
                "arm": arm,
                "p90_ms": fmt_csv(lookup.get((arm, p), math.nan)),
            }
            for arm in cfg["arms"]
            for p in phases
        ]
        return save_outputs(
            fig, out_dir, f"{scenario}_p90_response_time",
            ["phase", "phase_label", "workload_ue", "arm", "p90_ms"], csv_rows,
        )


def plot_slo_violation(scenario, cfg, metrics, phases, phase_info, out_dir):
    """Plot per-phase SLO violation rate (%) on a linear y-axis."""
    labels = [phase_tick_label(p, phase_info[p]) for p in phases]
    lookup = {(r["arm"], r["phase"]): r["slo_violation_pct"] for r in metrics}
    xs = list(range(len(phases)))
    ncol = 1 if len(cfg["arms"]) > 2 else 2
    legend_rows = 4 if ncol == 1 else 1

    with plt.rc_context(RC_STYLE):
        fig, legend_ax, (ax,) = _make_figure(1, legend_rows, panel_height=3.2)
        for arm in cfg["arms"]:
            ys = [lookup.get((arm, p), math.nan) for p in phases]
            ax.plot(xs, ys, linewidth=1.7, markersize=4.5, label=cfg["labels"][arm], **ARM_STYLES[arm])

        ax.set_ylim(-2, 105)
        _style_phase_axis(ax, labels)
        ax.set_ylabel("SLO Violation Rate (%)")
        fig.suptitle("SLO Violation Rate per Phase", fontsize=11)
        _draw_legend(legend_ax, ax, ncol=ncol)

        csv_rows = [
            {
                "phase": p,
                "phase_label": phase_csv_label(p, phase_info[p]),
                "workload_ue": phase_info[p]["workload_ue"],
                "arm": arm,
                "slo_violation_pct": fmt_csv(lookup.get((arm, p), math.nan)),
            }
            for arm in cfg["arms"]
            for p in phases
        ]
        return save_outputs(
            fig, out_dir, f"{scenario}_slo_violation_rate",
            ["phase", "phase_label", "workload_ue", "arm", "slo_violation_pct"], csv_rows,
        )


def plot_resources(scenario, cfg, resources, phases, phase_info, out_dir):
    """Plot per-phase CPU (millicores) and memory (MiB) in two stacked panels."""
    labels = [phase_tick_label(p, phase_info[p]) for p in phases]
    lookup = {(r["arm"], r["phase"]): r for r in resources}
    xs = list(range(len(phases)))
    ncol = 1 if len(cfg["arms"]) > 2 else 2
    legend_rows = 4 if ncol == 1 else 1

    with plt.rc_context(RC_STYLE):
        fig, legend_ax, (ax_cpu, ax_mem) = _make_figure(2, legend_rows, panel_height=2.4)
        for arm in cfg["arms"]:
            cpus = [lookup[(arm, p)]["cpu_millicores"] for p in phases]
            mems = [lookup[(arm, p)]["memory_mib"] for p in phases]
            ax_cpu.plot(xs, cpus, linewidth=1.7, markersize=4.2, label=cfg["labels"][arm], **ARM_STYLES[arm])
            ax_mem.plot(xs, mems, linewidth=1.7, markersize=4.2, **ARM_STYLES[arm])

        ax_cpu.set_ylabel("CPU Usage (millicores)")
        ax_cpu.set_ylim(bottom=0)
        ax_cpu.grid(axis="y", linestyle=":", alpha=0.6)
        ax_cpu.spines["top"].set_visible(False)
        ax_cpu.spines["right"].set_visible(False)

        ax_mem.set_ylabel("Memory Usage (MiB)")
        ax_mem.set_ylim(bottom=0)
        _style_phase_axis(ax_mem, labels)

        fig.suptitle("Resource Usage per Phase", fontsize=11)
        _draw_legend(legend_ax, ax_cpu, ncol=ncol)
        fig.supxlabel(RESOURCE_NOTE, fontsize=7.2, color="#444444")

        csv_rows = [
            {
                "phase": p,
                "phase_label": phase_csv_label(p, phase_info[p]),
                "workload_ue": phase_info[p]["workload_ue"],
                "arm": arm,
                "cpu_millicores": fmt_csv(lookup[(arm, p)]["cpu_millicores"]),
                "memory_mib": fmt_csv(lookup[(arm, p)]["memory_mib"]),
                "cpu_completeness_status": lookup[(arm, p)]["cpu_completeness_status"],
                "memory_completeness_status": lookup[(arm, p)]["memory_completeness_status"],
                "rate_window_seconds": lookup[(arm, p)]["rate_window_seconds"],
            }
            for arm in cfg["arms"]
            for p in phases
        ]
        return save_outputs(
            fig, out_dir, f"{scenario}_resource_usage",
            [
                "phase", "phase_label", "workload_ue", "arm",
                "cpu_millicores", "memory_mib",
                "cpu_completeness_status", "memory_completeness_status", "rate_window_seconds",
            ],
            csv_rows,
            note=RESOURCE_NOTE,
        )


def plot_pod_placement(scenario, cfg, pods, phases, phase_info, out_dir):
    """Plot end-of-phase stacked pod counts by node as small multiples per arm.

    Within each node slice, static site anchors and vertical-offload core fallback
    pods are drawn hatched (///) and dynamically scheduled pods are drawn solid.
    """
    labels = [phase_tick_label(p, phase_info[p]) for p in phases]
    lookup = {(r["arm"], r["phase"]): r for r in pods}
    arms = cfg["arms"]
    xs = list(range(len(phases)))
    has_anchors = any(
        lookup[(arm, p)].get(f"{node}_anchor", 0) > 0
        for arm in arms
        for p in phases
        for node in NODES
    )
    legend_rows = 2 if has_anchors else 1

    with plt.rc_context(RC_STYLE):
        fig, legend_ax, axes = _make_figure(len(arms), legend_rows=legend_rows, panel_height=1.9, sharey=True)

        peak = 0
        for idx, (ax, arm) in enumerate(zip(axes, arms)):
            bottoms = [0] * len(phases)
            for node in NODES:
                anchors = [lookup[(arm, p)].get(f"{node}_anchor", 0) for p in phases]
                dynamics = [lookup[(arm, p)].get(f"{node}_dynamic", 0) for p in phases]
                if any(a > 0 for a in anchors):
                    ax.bar(
                        xs, anchors, bottom=bottoms, width=0.66,
                        color=NODE_COLORS[node], edgecolor="#333333", linewidth=0.5,
                        hatch="///", alpha=0.82,
                    )
                    bottoms = [b + v for b, v in zip(bottoms, anchors)]
                if any(d > 0 for d in dynamics):
                    ax.bar(
                        xs, dynamics, bottom=bottoms, width=0.66,
                        color=NODE_COLORS[node],
                    )
                    bottoms = [b + v for b, v in zip(bottoms, dynamics)]
            peak = max(peak, max(bottoms, default=0))
            for x, total, p in zip(xs, bottoms, phases):
                pend = lookup[(arm, p)]["pending_pods"]
                if pend > 0:
                    ax.text(x, total + 0.3, f"+{pend}p", ha="center", va="bottom", fontsize=7, color="#D55E00")
            ax.set_title(cfg["labels"][arm], loc="left", fontsize=8.5)
            ax.set_ylabel("Pods")
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax.grid(axis="y", linestyle=":", alpha=0.6)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)

        axes[0].set_ylim(0, peak + max(2, int(0.2 * peak)))
        _style_phase_axis(axes[-1], labels)

        fig.suptitle("Pod Placement at End of Phase", fontsize=11)
        node_handles = [Patch(facecolor=NODE_COLORS[n], label=n) for n in NODES]
        if has_anchors:
            style_handles = [
                Patch(facecolor="#BBBBBB", edgecolor="#333333", hatch="///", alpha=0.82,
                      label="Static anchor / core fallback"),
                Patch(facecolor="#BBBBBB", edgecolor="none", label="Dynamic (scheduled / HPA)"),
            ]
            legend_ax.legend(
                handles=node_handles + style_handles,
                loc="center", ncol=4, frameon=False, handlelength=2.4, columnspacing=1.2,
            )
        else:
            legend_ax.legend(handles=node_handles, loc="center", ncol=len(NODES), frameon=False, handlelength=2.4)
        fig.supxlabel(POD_NOTE, fontsize=7.2, color="#444444")

        csv_rows = [
            {
                "phase": p,
                "phase_label": phase_csv_label(p, phase_info[p]),
                "workload_ue": phase_info[p]["workload_ue"],
                "arm": arm,
                "gnb1": fmt_csv(lookup[(arm, p)]["gnb1"]),
                "gnb-2": fmt_csv(lookup[(arm, p)]["gnb-2"]),
                "gnb-3": fmt_csv(lookup[(arm, p)]["gnb-3"]),
                "core-5g": fmt_csv(lookup[(arm, p)]["core-5g"]),
                "gnb1_anchor": fmt_csv(lookup[(arm, p)].get("gnb1_anchor", 0)),
                "gnb-2_anchor": fmt_csv(lookup[(arm, p)].get("gnb-2_anchor", 0)),
                "gnb-3_anchor": fmt_csv(lookup[(arm, p)].get("gnb-3_anchor", 0)),
                "core-5g_anchor": fmt_csv(lookup[(arm, p)].get("core-5g_anchor", 0)),
                "pending_pods": fmt_csv(lookup[(arm, p)]["pending_pods"]),
                "sample_age_seconds": fmt_csv(lookup[(arm, p)]["sample_age_seconds"]),
            }
            for arm in arms
            for p in phases
        ]
        return save_outputs(
            fig, out_dir, f"{scenario}_pod_placement",
            [
                "phase", "phase_label", "workload_ue", "arm",
                *NODES, "gnb1_anchor", "gnb-2_anchor", "gnb-3_anchor", "core-5g_anchor",
                "pending_pods", "sample_age_seconds",
            ],
            csv_rows,
            note=POD_NOTE,
        )


BAR_STYLES = {
    "baseline-local":    {"color": "#E69F00", "edgecolor": "#8C5E00", "hatch": "//"},
    "proposed-local":    {"color": "#56B4E9", "edgecolor": "#1D6F9E", "hatch": "//"},
    "baseline-vertical": {"color": "#D55E00", "edgecolor": "#7A3500", "hatch": ""},
    "proposed-vertical": {"color": "#0072B2", "edgecolor": "#00446B", "hatch": ""},
    "baseline":          {"color": "#D55E00", "edgecolor": "#7A3500", "hatch": "//"},
    "proposed":          {"color": "#0072B2", "edgecolor": "#00446B", "hatch": ""},
}

SLIDE_SHORT_LABELS = {
    "baseline-local": "Baseline (RAN Only)",
    "proposed-local": "Proposed (RAN Only)",
    "baseline-vertical": "Baseline (Vertical Offload)",
    "proposed-vertical": "Proposed (Vertical Offload)",
    "baseline": "Baseline (CPU HPA)",
    "proposed": "Proposed (SLO HPA)",
}

SLIDE_RC_STYLE = {
    "font.size": 11,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "xtick.labelsize": 9.5,
    "ytick.labelsize": 10,
    "legend.fontsize": 9.5,
}


def _bar_offsets(n_arms):
    if n_arms == 4:
        w = 0.18
        return w, [-1.62 * w, -0.62 * w, 0.62 * w, 1.62 * w]
    w = 0.30
    return w, [-0.55 * w, 0.55 * w]


def _load_request_distributions(run_dir, arms, phases):
    """Load per-(arm, phase) all-attempt durations, p90, and per-arm phase_01 start times."""
    durs_map = {}
    all_p90 = {}
    start_times = {}
    for arm in arms:
        req_path = Path(run_dir) / arm / "requests.jsonl"
        sum_path = Path(run_dir) / arm / "summary.csv"
        if not req_path.is_file() or not sum_path.is_file():
            return None, None, None
        with sum_path.open(newline="", encoding="utf-8") as handle:
            first = next(csv.DictReader(handle), None)
            if first and first.get("start_time"):
                start_times[arm] = float(first["start_time"])
        by_phase_obs = collections.defaultdict(list)
        by_phase_pen = collections.defaultdict(list)
        with req_path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                r = json.loads(line)
                if r.get("event") == "request" and r.get("duration_ms") is not None:
                    obs = float(r["duration_ms"])
                    pen = obs if r.get("success") else TIMEOUT_MS
                    by_phase_obs[r["phase"]].append(obs)
                    by_phase_pen[r["phase"]].append(pen)
        for p in phases:
            arr_obs = sorted(by_phase_obs[p])
            arr_pen = sorted(by_phase_pen[p])
            durs_map[(arm, p)] = np.array(arr_obs, dtype=float)
            all_p90[(arm, p)] = arr_pen[max(0, math.ceil(0.9 * len(arr_pen)) - 1)] if arr_pen else math.nan
    return durs_map, all_p90, start_times


def _load_goodput_and_failures(run_dir, arms, phases):
    """Load per-(arm, phase) goodput (req/s) and failure rates from summary.csv."""
    goodput, fail_pct, fail_cnt, attempts_map = {}, {}, {}, {}
    for arm in arms:
        sum_path = Path(run_dir) / arm / "summary.csv"
        if not sum_path.is_file():
            return None, None, None, None
        with sum_path.open(newline="", encoding="utf-8") as handle:
            for r in csv.DictReader(handle):
                p = r["phase"].strip()
                att = parse_int(r.get("attempts")) or 0
                fail = parse_int(r.get("failures")) or 0
                gp = parse_float(r.get("success_throughput_per_s"))
                goodput[(arm, p)] = gp
                fail_cnt[(arm, p)] = fail
                attempts_map[(arm, p)] = att
                fail_pct[(arm, p)] = (100.0 * fail / att) if att > 0 else math.nan
    return goodput, fail_pct, fail_cnt, attempts_map


def plot_p90_bar(scenario, cfg, all_p90, phases, phase_info, out_dir, *, slides_dir=None):
    """Routing-grouped per-phase all-attempt p90 bar chart with SLO & Timeout lines."""
    arms = cfg["arms"]
    labels = [phase_tick_label(p, phase_info[p]) for p in phases]
    xs = np.arange(len(phases))
    width, offsets = _bar_offsets(len(arms))

    def _fmt(v):
        if math.isnan(v):
            return ""
        if v >= 9950:
            return "10k"
        if v >= 1000:
            return f"{v / 1000:.1f}k"
        return f"{v:.0f}"

    def _draw(arm_labels, figsize, rc, legend_ncol, title_fs, val_fs):
        with plt.rc_context(rc):
            fig, ax = plt.subplots(1, 1, figsize=figsize, layout="constrained")
            for off, arm in zip(offsets, arms):
                ys = [all_p90.get((arm, p), math.nan) for p in phases]
                st = BAR_STYLES[arm]
                bars = ax.bar(
                    xs + off, ys, width=width * 0.92,
                    color=st["color"], edgecolor=st["edgecolor"], linewidth=0.7,
                    hatch=st["hatch"], label=arm_labels[arm], zorder=3,
                )
                for bar, y in zip(bars, ys):
                    if not math.isnan(y) and y > 0:
                        ax.text(bar.get_x() + bar.get_width() / 2.0, y * 1.12, _fmt(y),
                                ha="center", va="bottom", fontsize=val_fs, color="#222222",
                                rotation=90 if val_fs >= 7.5 else 0)
            valid_vals = [v for v in all_p90.values() if not math.isnan(v) and v > 0]
            max_val = max(valid_vals, default=SLO_MS)
            show_timeout = max_val > 500.0
            ax.axhline(SLO_MS, color="#444444", linestyle=":", linewidth=1.7, label=f"SLO ({SLO_MS:g} ms)", zorder=4)
            if show_timeout:
                ax.axhline(TIMEOUT_MS, color=TIMEOUT_COLOR, linestyle=":", linewidth=1.9, label="Timeout (10,000 ms)", zorder=4)
                ax.set_ylim(30, 26000)
            else:
                ax.set_ylim(30, max(300, max_val * 1.8))
            ax.set_yscale("log")
            ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
            ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
            ax.yaxis.set_minor_formatter(NullFormatter())
            ax.set_xticks(xs)
            ax.set_xticklabels(labels)
            ax.set_xlabel("Phase (Active UEs, gnb1:gnb-2:gnb-3)\nAll attempts; failed requests penalized at 10,000 ms timeout")
            ax.set_ylabel("p90 Response Time (ms, log scale)")
            ax.grid(axis="y", which="major", linestyle=":", alpha=0.55, zorder=0)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.02), ncol=legend_ncol, frameon=False, handlelength=2.4)
            fig.suptitle("p90 Response Time per Phase", fontsize=title_fs)
            return fig

    csv_rows = [
        {"phase": p, "phase_label": phase_csv_label(p, phase_info[p]),
         "workload_ue": phase_info[p]["workload_ue"], "arm": arm,
         "all_attempt_p90_ms": fmt_csv(all_p90.get((arm, p), math.nan))}
        for arm in arms for p in phases
    ]
    fig = _draw(cfg["labels"], (8.6, 5.8), RC_STYLE, 1, 11.5, 7.0)
    written = save_outputs(
        fig, out_dir, f"{scenario}_p90_bar",
        ["phase", "phase_label", "workload_ue", "arm", "all_attempt_p90_ms"], csv_rows,
    )
    if slides_dir is not None:
        Path(slides_dir).mkdir(parents=True, exist_ok=True)
        sfig = _draw(SLIDE_SHORT_LABELS, (10.5, 5.6), SLIDE_RC_STYLE, 3, 14, 8.5)
        sp = Path(slides_dir) / "response-time-p90-bar.png"
        sfig.savefig(sp, dpi=300)
        plt.close(sfig)
        written.append(sp)
    return written


def plot_latency_boxplot(scenario, cfg, durs_map, phases, phase_info, out_dir, *, slides_dir=None):
    """Routing-grouped per-phase latency boxplot (Q25-Q75 box, median, p5-p95 whiskers)."""
    arms = cfg["arms"]
    labels = [phase_tick_label(p, phase_info[p]) for p in phases]
    xs = np.arange(len(phases))
    width, offsets = _bar_offsets(len(arms))

    def _draw(arm_labels, figsize, rc, legend_ncol, title_fs):
        with plt.rc_context(rc):
            fig, ax = plt.subplots(1, 1, figsize=figsize, layout="constrained")
            for off, arm in zip(offsets, arms):
                st = BAR_STYLES[arm]
                data_list = [durs_map[(arm, p)] for p in phases]
                bp = ax.boxplot(
                    data_list, positions=xs + off, widths=width * 0.88,
                    whis=(5, 95), showfliers=False, patch_artist=True,
                    medianprops=dict(color="#111111", linewidth=1.4),
                    whiskerprops=dict(color=st["edgecolor"], linewidth=1.1),
                    capprops=dict(color=st["edgecolor"], linewidth=1.1),
                    boxprops=dict(facecolor=st["color"], edgecolor=st["edgecolor"], linewidth=0.9, alpha=0.88),
                    zorder=3,
                )
                if st["hatch"]:
                    for patch in bp["boxes"]:
                        patch.set_hatch(st["hatch"])
            p95_vals = [float(np.percentile(arr, 95)) for arr in durs_map.values() if len(arr) > 0]
            max_p95 = max(p95_vals, default=SLO_MS)
            show_timeout = max_p95 > 500.0
            ax.axhline(SLO_MS, color="#444444", linestyle=":", linewidth=1.7, zorder=4)
            if show_timeout:
                ax.axhline(TIMEOUT_MS, color=TIMEOUT_COLOR, linestyle=":", linewidth=1.9, zorder=4)
                ax.set_ylim(20, 18000)
            else:
                ax.set_ylim(20, max(300, max_p95 * 1.6))
            ax.set_yscale("log")
            ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
            ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
            ax.yaxis.set_minor_formatter(NullFormatter())
            ax.set_xlim(-0.55, len(phases) - 0.45)
            ax.set_xticks(xs)
            ax.set_xticklabels(labels)
            ax.set_xlabel("Phase (Active UEs, gnb1:gnb-2:gnb-3)\nBoxes: Q25–Q75 with median; whiskers: p5–p95 · All attempts (actual duration, incl. failures)")
            ax.set_ylabel("Response Time (ms, log scale)")
            ax.grid(axis="y", which="major", linestyle=":", alpha=0.55, zorder=0)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            handles = [
                Line2D([0], [0], color="#444444", linestyle=":", linewidth=1.7, label=f"SLO ({SLO_MS:g} ms)"),
            ]
            if show_timeout:
                handles.append(
                    Line2D([0], [0], color=TIMEOUT_COLOR, linestyle=":", linewidth=1.9, label="Timeout (10,000 ms)")
                )
            handles += [
                Patch(facecolor=BAR_STYLES[a]["color"], edgecolor=BAR_STYLES[a]["edgecolor"],
                      hatch=BAR_STYLES[a]["hatch"], alpha=0.88, label=arm_labels[a])
                for a in arms
            ]
            ax.legend(handles=handles, loc="lower center", bbox_to_anchor=(0.5, 1.02),
                      ncol=legend_ncol, frameon=False, handlelength=2.4)
            fig.suptitle("Response Time Distribution per Phase", fontsize=title_fs)
            return fig

    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / f"{scenario}_latency_boxplot.png"
    fig = _draw(cfg["labels"], (8.6, 5.9), RC_STYLE, 1, 11.5)
    fig.savefig(png_path, dpi=300)
    plt.close(fig)
    written = [png_path]
    if slides_dir is not None:
        Path(slides_dir).mkdir(parents=True, exist_ok=True)
        sfig = _draw(SLIDE_SHORT_LABELS, (10.5, 5.8), SLIDE_RC_STYLE, 3, 14)
        sp = Path(slides_dir) / "response-time-boxplot.png"
        sfig.savefig(sp, dpi=300)
        plt.close(sfig)
        written.append(sp)
    return written


def plot_slo_violation_bar(scenario, cfg, metrics, phases, phase_info, out_dir, *, slides_dir=None):
    """Routing-grouped per-phase SLO violation rate (%) bar chart with exact value labels."""
    arms = cfg["arms"]
    labels = [phase_tick_label(p, phase_info[p]) for p in phases]
    lookup = {(r["arm"], r["phase"]): r["slo_violation_pct"] for r in metrics}
    xs = np.arange(len(phases))
    width, offsets = _bar_offsets(len(arms))

    def _draw(arm_labels, figsize, rc, legend_ncol, title_fs, val_fs):
        with plt.rc_context(rc):
            fig, ax = plt.subplots(1, 1, figsize=figsize, layout="constrained")
            for off, arm in zip(offsets, arms):
                ys = [lookup.get((arm, p), math.nan) for p in phases]
                st = BAR_STYLES[arm]
                bars = ax.bar(
                    xs + off, ys, width=width * 0.92,
                    color=st["color"], edgecolor=st["edgecolor"], linewidth=0.7,
                    hatch=st["hatch"], label=arm_labels[arm], zorder=3,
                )
                for bar, y in zip(bars, ys):
                    if not math.isnan(y):
                        txt = "0%" if y < 0.05 else f"{y:.1f}%"
                        ax.text(bar.get_x() + bar.get_width() / 2.0, y + 1.4, txt,
                                ha="center", va="bottom", fontsize=val_fs, color="#222222", rotation=90)
            ax.set_ylim(0, 110)
            ax.set_yticks([0, 20, 40, 60, 80, 100])
            ax.set_xticks(xs)
            ax.set_xticklabels(labels)
            ax.set_xlabel("Phase (Active UEs, gnb1:gnb-2:gnb-3)")
            ax.set_ylabel("SLO Violation Rate (%)")
            ax.grid(axis="y", linestyle=":", alpha=0.55, zorder=0)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.02), ncol=legend_ncol, frameon=False, handlelength=2.4)
            fig.suptitle(f"SLO Violation Rate per Phase (SLO = {SLO_MS:g} ms)", fontsize=title_fs)
            return fig

    out_dir.mkdir(parents=True, exist_ok=True)
    png_path = out_dir / f"{scenario}_slo_violation_bar.png"
    fig = _draw(cfg["labels"], (8.6, 5.7), RC_STYLE, 1, 11.5, 7.5)
    fig.savefig(png_path, dpi=300)
    plt.close(fig)
    written = [png_path]
    if slides_dir is not None:
        Path(slides_dir).mkdir(parents=True, exist_ok=True)
        sfig = _draw(SLIDE_SHORT_LABELS, (10.5, 5.5), SLIDE_RC_STYLE, 2, 14, 8.5)
        sp = Path(slides_dir) / "slo-violations-bar.png"
        sfig.savefig(sp, dpi=300)
        plt.close(sfig)
        written.append(sp)
    return written


def plot_goodput_and_failures(scenario, cfg, goodput, fail_pct, fail_cnt, attempts_map,
                              phases, phase_info, out_dir, *, slides_dir=None):
    """Plot per-phase Goodput (req/s vs offered load) and Failed Request Rate (%)."""
    arms = cfg["arms"]
    labels = [phase_tick_label(p, phase_info[p]) for p in phases]
    xs = list(range(len(phases)))
    offered_rps = [phase_info[p]["workload_ue"] * 5.0 for p in phases]
    csv_rows = [
        {
            "phase": p, "phase_label": phase_csv_label(p, phase_info[p]),
            "workload_ue": phase_info[p]["workload_ue"], "offered_rps": f"{phase_info[p]['workload_ue'] * 5.0:.1f}",
            "arm": arm, "goodput_rps": fmt_csv(goodput.get((arm, p), math.nan)),
            "failures": fail_cnt.get((arm, p), 0), "attempts": attempts_map.get((arm, p), 0),
            "failure_rate_pct": fmt_csv(fail_pct.get((arm, p), math.nan)),
        }
        for arm in arms for p in phases
    ]
    fields = ["phase", "phase_label", "workload_ue", "offered_rps", "arm",
              "goodput_rps", "failures", "attempts", "failure_rate_pct"]

    with plt.rc_context(RC_STYLE):
        fig, legend_ax, (ax_gp, ax_fr) = _make_figure(2, legend_rows=len(arms) + 1, panel_height=2.55)
        for arm in arms:
            ax_gp.plot(xs, [goodput.get((arm, p), math.nan) for p in phases], linewidth=1.7, markersize=4.5,
                       label=cfg["labels"][arm], **ARM_STYLES[arm])
            ax_fr.plot(xs, [fail_pct.get((arm, p), math.nan) for p in phases], linewidth=1.7, markersize=4.5,
                       label=cfg["labels"][arm], **ARM_STYLES[arm])
        ax_gp.plot(xs, offered_rps, color="#444444", linestyle=":", linewidth=1.4, label="Offered load (5 req/s per UE)")
        ax_gp.set_ylabel("Goodput (successful req/s)")
        ax_gp.set_ylim(0, max(offered_rps) * 1.12)
        ax_gp.grid(axis="y", linestyle=":", alpha=0.6)
        ax_gp.spines["top"].set_visible(False)
        ax_gp.spines["right"].set_visible(False)
        ax_fr.set_ylabel("Failed Requests (%)")
        ax_fr.set_ylim(-2, 102)
        _style_phase_axis(ax_fr, labels)
        fig.suptitle("Goodput and Request Failure Rate per Phase", fontsize=11)
        _draw_legend(legend_ax, ax_gp, ncol=1)
        written = save_outputs(fig, out_dir, f"{scenario}_goodput_and_failures", fields, csv_rows)

    if slides_dir is not None:
        Path(slides_dir).mkdir(parents=True, exist_ok=True)
        with plt.rc_context(SLIDE_RC_STYLE):
            fig, (ax_gp, ax_fr) = plt.subplots(2, 1, figsize=(9.5, 6.2), sharex=True, layout="constrained")
            for arm in arms:
                ax_gp.plot(xs, [goodput.get((arm, p), math.nan) for p in phases], linewidth=2.2, markersize=6,
                           label=SLIDE_SHORT_LABELS[arm], **ARM_STYLES[arm])
                ax_fr.plot(xs, [fail_pct.get((arm, p), math.nan) for p in phases], linewidth=2.2, markersize=6,
                           label=SLIDE_SHORT_LABELS[arm], **ARM_STYLES[arm])
            ax_gp.plot(xs, offered_rps, color="#444444", linestyle=":", linewidth=1.8, label="Offered load")
            ax_gp.set_title("Goodput (successful req/s)")
            ax_gp.set_ylabel("req/s")
            ax_gp.set_ylim(0, max(offered_rps) * 1.15)
            ax_gp.grid(axis="y", linestyle=":", alpha=0.6)
            ax_gp.spines["top"].set_visible(False)
            ax_gp.spines["right"].set_visible(False)
            ax_gp.legend(loc="upper left", ncol=2, frameon=False, handlelength=3.2)
            ax_fr.set_title("Failed Requests (%)")
            ax_fr.set_ylabel("% of attempts")
            ax_fr.set_ylim(-2, 102)
            _style_phase_axis(ax_fr, labels)
            fig.suptitle("Goodput & Failed Request Rate", fontsize=14)
            sp = Path(slides_dir) / "goodput-and-failures.png"
            fig.savefig(sp, dpi=300)
            plt.close(fig)
            written.append(sp)
    return written


def generate_rich_plots(run_dir, scenario, out_dir, slides_dir=None):
    """Generate the full suite (standard 4 + p90 bar, boxplot, SLO bar, goodput/failures) for one run."""
    run_dir = Path(run_dir)
    out_dir = Path(out_dir)
    cfg = dict(SCENARIOS[scenario])
    cfg["dir"] = run_dir.name
    metrics, phases, phase_info, m_missing = load_phase_metrics(run_dir, cfg)
    resources, r_missing, _ = load_resources(run_dir, cfg["arms"], phases)
    pods, p_missing, _ = load_pods(run_dir, cfg["arms"], phases)

    files = []
    files.extend(plot_p90(scenario, cfg, metrics, phases, phase_info, out_dir))
    files.extend(plot_slo_violation(scenario, cfg, metrics, phases, phase_info, out_dir))
    files.extend(plot_resources(scenario, cfg, resources, phases, phase_info, out_dir))
    files.extend(plot_pod_placement(scenario, cfg, pods, phases, phase_info, out_dir))
    files.extend(plot_slo_violation_bar(scenario, cfg, metrics, phases, phase_info, out_dir, slides_dir=slides_dir))

    durs_map, all_p90, _ = _load_request_distributions(run_dir, cfg["arms"], phases)
    if durs_map is not None:
        files.extend(plot_p90_bar(scenario, cfg, all_p90, phases, phase_info, out_dir, slides_dir=slides_dir))
        files.extend(plot_latency_boxplot(scenario, cfg, durs_map, phases, phase_info, out_dir, slides_dir=slides_dir))

    gp, fp, fc, am = _load_goodput_and_failures(run_dir, cfg["arms"], phases)
    if gp is not None:
        files.extend(plot_goodput_and_failures(
            scenario, cfg, gp, fp, fc, am, phases, phase_info, out_dir, slides_dir=slides_dir,
        ))
    return files, m_missing + r_missing + p_missing


def generate_all_plots(final_dir=FINAL_DIR, output_dir=None):
    """Load all three scenarios, render 12 PNG figures + CSVs, and return a report."""
    out_dir = Path(output_dir) if output_dir is not None else Path(final_dir) / "plots"
    report = {"files": [], "missing": {}, "partial_resources": {}, "pending_nonzero": {}}

    for scenario, cfg in SCENARIOS.items():
        run_dir = Path(final_dir) / cfg["dir"]
        metrics, phases, phase_info, m_missing = load_phase_metrics(run_dir, cfg)
        resources, r_missing, r_partial = load_resources(run_dir, cfg["arms"], phases)
        pods, p_missing, pending = load_pods(run_dir, cfg["arms"], phases)

        report["missing"][scenario] = m_missing + r_missing + p_missing
        report["partial_resources"][scenario] = r_partial
        report["pending_nonzero"][scenario] = pending

        report["files"].extend(plot_p90(scenario, cfg, metrics, phases, phase_info, out_dir))
        report["files"].extend(plot_slo_violation(scenario, cfg, metrics, phases, phase_info, out_dir))
        report["files"].extend(plot_resources(scenario, cfg, resources, phases, phase_info, out_dir))
        report["files"].extend(plot_pod_placement(scenario, cfg, pods, phases, phase_info, out_dir))

    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, default=None,
                        help="Generate the full rich plot suite (including bar, boxplot, goodput) for a single run dir.")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="joint",
                        help="Scenario key when using --run-dir (default: joint).")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="Output directory for thesis PNG/CSV files.")
    parser.add_argument("--slides-dir", type=Path, default=None,
                        help="Optional output directory for slide-ready PNGs (defaults to RUN_DIR/slides with --run-dir).")
    args = parser.parse_args(argv)

    if args.run_dir is not None:
        out_dir = args.output_dir or (args.run_dir.parent / "plots")
        slides_dir = args.slides_dir or (args.run_dir / "slides")
        files, missing = generate_rich_plots(args.run_dir, args.scenario, out_dir, slides_dir=slides_dir)
        print(f"Wrote {len(files)} rich plot files to {out_dir} and {slides_dir}:")
        for path in files:
            print(f"  {path}")
        if missing:
            print(f"Missing: {missing}")
        return

    report = generate_all_plots(FINAL_DIR, args.output_dir or OUTPUT_DIR)
    print(f"Wrote {len(report['files'])} files to {args.output_dir or OUTPUT_DIR}:")
    for path in report["files"]:
        print(f"  {path.name}")
    print("\nMissing values:")
    for scenario, items in report["missing"].items():
        print(f"  {scenario}: {items if items else 'none'}")
    print("\nPartial resource coverage phases (valid observations averaged):")
    for scenario, items in report["partial_resources"].items():
        print(f"  {scenario}: {len(items)} entries")
    print("\nPhases with pending pods > 0:")
    for scenario, items in report["pending_nonzero"].items():
        print(f"  {scenario}: {items if items else 'none'}")


if __name__ == "__main__":
    main()
