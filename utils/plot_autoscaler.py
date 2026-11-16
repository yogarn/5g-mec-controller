#!/usr/bin/env python3
"""Plot measured autoscaler comparisons.

Usage: python -m utils.plot_autoscaler RESULTS_DIR OUTPUT_DIR

RESULTS_DIR/{baseline,proposed} must each contain summary.csv, ramp-summary.csv,
pods-by-ramp.csv, and resource-by-ramp.csv. Placement files must explicitly
record every phase/node pair, including measured zero counts. SLO violations
are the fraction of all attempts that failed or exceeded the threshold.
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
            denom = rounds + missing
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
            coverage[method].append({
                'complete': complete,
                'rounds': rounds,
                'missing': missing,
                'coverage_pct': cov_pct,
                'status': status,
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", type=Path, help="directory containing baseline/ and proposed/ CSV inputs")
    parser.add_argument("output_dir", type=Path, help="directory for PNG/PDF figure pairs")
    parser.add_argument("--slo-ms", type=float, default=500,
                        help="controller p90 target and recorded request SLO in milliseconds (default: 500)")
    args = parser.parse_args()
    try:
        summaries, placements, resources, site_keys, phases, nodes = load(args.results_dir)
        violations = load_slo(args.results_dir, phases, args.slo_ms)
        coverage = load_coverage(args.results_dir, phases)
        resource_coverage = load_resource_coverage(args.results_dir, phases)
        durations = load_durations(args.results_dir, phases)
    except ValueError as exc:
        parser.error(str(exc))

    site_names = ":".join(key.replace("_ues", "").upper() for key in site_keys)
    varying_durations = durations is not None and len(set(durations)) > 1
    labels = [
        f"{phase}\n{summaries['baseline'][phase]['workload_ue']} UE\n"
        + ":".join(summaries["baseline"][phase][key] for key in site_keys)
        + (f"\n{dur:g}s" if varying_durations else "")
        for phase, dur in zip(phases, durations or [None] * len(phases))
    ]
    x = list(range(len(phases)))
    xlabel = f"Phase; total UE; {site_names} UE distribution" + ("; duration" if varying_durations else "")
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
            if key == "p90_ms":
                ax.axhline(args.slo_ms, color="#d62728", linestyle="--", linewidth=1.5,
                           label=f"Controller p90 target ({args.slo_ms:g} ms)")
            ax.set(ylabel="Response time (ms)", title=title, ylim=(0, None))
            ax.grid(axis="y", linestyle=":")
            ax.legend(frameon=False, ncols=2)
        axes[-1].set(xticks=x, xticklabels=labels, xlabel=xlabel)
        export(fig, "response-time")

        fig, axes = plt.subplots(2, 1, figsize=(width, 7), sharex=True, layout="constrained")
        ax, cov_ax = axes
        for method, offset, color in (("baseline", -0.2, "#0072B2"), ("proposed", 0.2, "#D55E00")):
            values = violations[method]
            ax.bar([v + offset for v in x], values, width=0.4, color=color,
                   edgecolor="black", linewidth=0.5,
                   hatch="xx" if method == "baseline" else None, label=METHOD_LABELS[method])
            for pos, value in zip(x, values):
                if math.isnan(value):
                    ax.annotate("N/A", (pos + offset, 0), ha="center", va="bottom", fontsize=8)
            cov_values = coverage[method]
            cov_ax.bar([v + offset for v in x], cov_values, width=0.4, color=color,
                       edgecolor="black", linewidth=0.5,
                       hatch="xx" if method == "baseline" else None, label=METHOD_LABELS[method])
            for pos, value in zip(x, cov_values):
                if math.isnan(value):
                    cov_ax.annotate("N/A", (pos + offset, 0), ha="center", va="bottom", fontsize=8)
        ax.set(ylim=(0, 100), ylabel="Attempts violating SLO (%)",
               title=f"Failed or slower than {args.slo_ms:g} ms; denominator: all attempts")
        ax.grid(axis="y", linestyle=":")
        ax.legend(frameon=False, ncols=2)
        cov_ax.set(xticks=x, xticklabels=labels, xlabel=xlabel, ylim=(0, 105),
                   ylabel="Request coverage (%)",
                   title="Completed / scheduled requests (100% = no omitted or dropped slots)")
        cov_ax.grid(axis="y", linestyle=":")
        export(fig, "slo-violations")

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

        fig, axes = plt.subplots(3, 1, figsize=(width, 9), sharex=True, layout="constrained")
        fig.suptitle("Mean of complete simultaneous resource samples per phase; gaps and coverage show churn/missingness")
        for ax, key, title, ylabel in zip(axes[:2], ("cpu_millicores", "memory_mib"),
                                          ("CPU usage", "RAM working set"), ("Total pod CPU (millicores)", "Total pod memory (MiB)")):
            for method, offset in (("proposed", 0.0), ("baseline", 0.0)):
                ys = [optional_number(resources[method][phase][key], key) for phase in phases]
                ax.plot(x, ys, linestyle=STYLES[method], marker="o", label=METHOD_LABELS[method])
                for pos, val, cov_info in zip(x, ys, resource_coverage[method]):
                    if math.isnan(val):
                        tag = f"{METHOD_LABELS[method][0]}:0/{cov_info['rounds']}" if cov_info['rounds'] else f"{METHOD_LABELS[method][0]}:N/A"
                        ax.annotate(tag, (pos, 0), ha="center", va="bottom", fontsize=7,
                                    xytext=(0, 2 if method == "baseline" else 10), textcoords="offset points")
                    elif cov_info['coverage_pct'] < 100.0:
                        denom = cov_info['rounds'] + cov_info['missing']
                        ax.annotate(f"{cov_info['complete']}/{denom}", (pos, val),
                                    ha="center", va="bottom", fontsize=7,
                                    xytext=(0, 4), textcoords="offset points")
            ax.set(ylabel=ylabel, title=title, ylim=(0, None))
            ax.grid(axis="y", linestyle=":")
            ax.legend(frameon=False)
        res_cov_ax = axes[2]
        for method, offset, color in (("baseline", -0.2, "#0072B2"), ("proposed", 0.2, "#D55E00")):
            cov_vals = [entry['coverage_pct'] for entry in resource_coverage[method]]
            res_cov_ax.bar([v + offset for v in x], cov_vals, width=0.4, color=color,
                           edgecolor="black", linewidth=0.5,
                           hatch="xx" if method == "baseline" else None, label=METHOD_LABELS[method])
        res_cov_ax.set(xticks=x, xticklabels=labels, xlabel=xlabel, ylim=(0, 105),
                       ylabel="Resource coverage (%)",
                       title="Complete simultaneous rounds / total rounds (100% = no churned or partial rounds)")
        res_cov_ax.grid(axis="y", linestyle=":")
        res_cov_ax.legend(frameon=False, ncols=2)
        export(fig, "resource-usage")


if __name__ == "__main__":
    main()
