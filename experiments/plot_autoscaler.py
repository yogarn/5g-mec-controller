#!/usr/bin/env python3
"""Plot measured autoscaler comparisons.

Usage: python -m experiments.plot_autoscaler RESULTS_DIR OUTPUT_DIR

RESULTS_DIR/{baseline,proposed} must each contain ramp-summary.csv,
pods-by-ramp.csv, and resource-by-ramp.csv. Placement files must explicitly
record every phase/node pair, including measured zero counts.
"""
import argparse
import csv
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Patch

METHODS = ("baseline", "proposed")
STYLES = {"baseline": "--", "proposed": "-"}
METHOD_LABELS = {"baseline": "Baseline", "proposed": "Proposed"}


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
        rows, fields = read_rows(path, ("phase", "workload_ue", "gnb2_ues", "gnb3_ues", "p50_ms", "p90_ms"))
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", type=Path, help="directory containing baseline/ and proposed/ CSV inputs")
    parser.add_argument("output_dir", type=Path, help="directory for the three PNG/PDF figure pairs")
    args = parser.parse_args()
    try:
        summaries, placements, resources, site_keys, phases, nodes = load(args.results_dir)
    except ValueError as exc:
        parser.error(str(exc))

    site_names = ":".join(key.replace("_ues", "").upper() for key in site_keys)
    labels = [f"{phase}\n{summaries['baseline'][phase]['workload_ue']} UE\n" +
              ":".join(summaries["baseline"][phase][key] for key in site_keys) for phase in phases]
    x = list(range(len(phases)))
    xlabel = f"Phase; total UE; {site_names} UE distribution"
    width = max(9, 1.45 * len(phases))
    args.output_dir.mkdir(parents=True, exist_ok=True)

    def export(fig, name):
        for extension in ("png", "pdf"):
            fig.savefig(args.output_dir / f"{name}.{extension}", dpi=220)
        plt.close(fig)

    with plt.rc_context({"font.size": 9, "axes.titlesize": 10, "legend.fontsize": 8}):
        fig, axes = plt.subplots(2, 1, figsize=(width, 7), sharex=True, layout="constrained")
        fig.suptitle("Successful request response time by phase")
        for ax, key, title in zip(axes, ("p90_ms", "p50_ms"), ("P90 response time", "P50 response time")):
            for method, color in (("proposed", "#D55E00"), ("baseline", "#0072B2")):
                ax.plot(x, [optional_number(summaries[method][phase][key], key) for phase in phases],
                        linestyle=STYLES[method], color=color, marker="o",
                        label=METHOD_LABELS[method])
            ax.set(ylabel="Response time (ms)", title=title, ylim=(0, None))
            ax.grid(axis="y", linestyle=":")
            ax.legend(frameon=False, ncols=2)
        axes[-1].set(xticks=x, xticklabels=labels, xlabel=xlabel)
        export(fig, "response-time")

        fig, ax = plt.subplots(figsize=(width, 4.5), layout="constrained")
        palette = plt.get_cmap("tab20").colors
        colors = dict(zip(nodes, palette))
        for method, offset in (("baseline", -0.2), ("proposed", 0.2)):
            bottoms = [0] * len(phases)
            for node in nodes:
                heights = [placements[method][(phase, node)] for phase in phases]
                ax.bar([v + offset for v in x], heights, bottom=bottoms, width=0.4,
                       color=colors[node], edgecolor="black", linewidth=0.5,
                       hatch="///" if method == "baseline" else None)
                bottoms = [a + b for a, b in zip(bottoms, heights)]
        handles = [Patch(facecolor=colors[node], edgecolor="black", label=node) for node in nodes]
        handles += [Patch(facecolor="white", edgecolor="black", hatch="///", label="Baseline"),
                    Patch(facecolor="white", edgecolor="black", label="Proposed")]
        ax.set(xticks=x, xticklabels=labels, xlabel=xlabel, ylabel="Pod count", title="Pod placement near phase end (including pending pods)", ylim=(0, None))
        ax.grid(axis="y", linestyle=":")
        ax.legend(handles=handles, frameon=False, loc="upper left", bbox_to_anchor=(1, 1))
        export(fig, "pod-placement")

        fig, axes = plt.subplots(2, 1, figsize=(width, 7), sharex=True, layout="constrained")
        fig.suptitle("Mean sampled workload resources per phase; CPU windows can overlap phases")
        for ax, key, title, ylabel in zip(axes, ("cpu_millicores", "memory_mib"),
                                          ("CPU usage", "RAM working set"), ("Total pod CPU (millicores)", "Total pod memory (MiB)")):
            for method in reversed(METHODS):
                ax.plot(x, [optional_number(resources[method][phase][key], key) for phase in phases],
                        linestyle=STYLES[method], marker="o", label=METHOD_LABELS[method])
            ax.set(ylabel=ylabel, title=title, ylim=(0, None))
            ax.grid(axis="y", linestyle=":")
            ax.legend(frameon=False)
        axes[-1].set(xticks=x, xticklabels=labels, xlabel=xlabel)
        export(fig, "resource-usage")


if __name__ == "__main__":
    main()
