#!/usr/bin/env python3
"""Generate final per-phase thesis plots (300-dpi PNG and plotted-value CSVs)."""
import csv
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import LogLocator, MaxNLocator, NullFormatter, StrMethodFormatter

FINAL_DIR = Path("/home/member/skripsi_yoga/5g-mec-controller/results/final")
OUTPUT_DIR = FINAL_DIR / "plots"
SLO_MS = 150.0
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
        "dir": "vertical_joint_full_20261121T191538Z",
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
        "dir": "vertical_mobility_20261121T205052Z",
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
        "dir": "scaling_20261121T214725Z",
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
POD_NOTE = "End-of-phase snapshot (~2 s before phase end); pending pods excluded from stack."

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


def load_pods(run_dir, arms, phases):
    """Load end-of-phase pod placement snapshots by node and pending counts."""
    rows = []
    missing = []
    pending_nonzero = []

    for arm in arms:
        raw_rows, _ = read_csv_rows(run_dir / arm / "pods-by-ramp.csv")
        by_phase_node = {}
        age_by_phase = {}
        for r in raw_rows:
            ph = r["phase"].strip()
            nd = r["node"].strip()
            by_phase_node[(ph, nd)] = parse_int(r.get("pod_count"))
            if ph not in age_by_phase:
                age_by_phase[ph] = parse_float(r.get("sample_age_seconds"))

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
                entry[node] = count
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
    """Plot end-of-phase stacked pod counts by node as small multiples per arm."""
    labels = [phase_tick_label(p, phase_info[p]) for p in phases]
    lookup = {(r["arm"], r["phase"]): r for r in pods}
    arms = cfg["arms"]
    xs = list(range(len(phases)))

    with plt.rc_context(RC_STYLE):
        fig, legend_ax, axes = _make_figure(len(arms), legend_rows=1, panel_height=1.9, sharey=True)

        peak = 0
        for idx, (ax, arm) in enumerate(zip(axes, arms)):
            bottoms = [0] * len(phases)
            for node in NODES:
                counts = [lookup[(arm, p)][node] for p in phases]
                vals = [0 if c is None else c for c in counts]
                ax.bar(
                    xs, vals, bottom=bottoms, width=0.66,
                    color=NODE_COLORS[node], label=node if idx == 0 else None,
                )
                bottoms = [b + v for b, v in zip(bottoms, vals)]
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
        _draw_legend(legend_ax, axes[0], ncol=len(NODES))
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
                *NODES, "pending_pods", "sample_age_seconds",
            ],
            csv_rows,
            note=POD_NOTE,
        )


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


def main():
    report = generate_all_plots(FINAL_DIR, OUTPUT_DIR)
    print(f"Wrote {len(report['files'])} files to {OUTPUT_DIR}:")
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
