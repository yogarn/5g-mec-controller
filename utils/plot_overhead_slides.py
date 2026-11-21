"""Export HPA scale-up and pod lifecycle charts for a saved comparison run."""

import argparse
from collections import defaultdict
from pathlib import Path
from statistics import fmean

import matplotlib.pyplot as plt

from utils.control_overhead import _records, hpa_action_delays, scheduling_delays
from utils.plot_autoscaler import METHODS, METHOD_COLORS, METHOD_LABELS


def scale_up_actions(directory):
    samples = defaultdict(list)
    for row in sorted(_records(directory / "hpa-status.jsonl"), key=lambda row: row["sample_time"]):
        desired = row.get("desired_replicas") or row.get("current_replicas")
        if desired is not None:
            samples[row.get("name")].append((row["sample_time"], int(desired)))

    increases = []
    unknown = 0
    for action in hpa_action_delays(directory):
        if action["status"] == "action_predates_recording":
            continue
        previous = next((target for when, target in reversed(samples[action["hpa"]])
                         if when < action["action_time"]), None)
        if previous is None:
            unknown += 1
        elif action["target_replicas"] > previous:
            increases.append(action)
    return increases, unknown


def draw_averages(ax, samples, totals, unit, empty_label):
    for pos, method in enumerate(METHODS):
        values = samples[method]
        total = totals[method]
        if values:
            average = fmean(values)
            ax.barh(pos, average, height=.48, color=METHOD_COLORS[method])
            label = f"{average:.2f} s   ({len(values)} of {total} {unit})"
        else:
            average = 0
            label = f"No completed timings   (0 of {total} {unit})" if total else empty_label
        ax.annotate(label, (average, pos), xytext=(7, 0), textcoords="offset points", va="center")
    ax.set(ylim=(1.65, -.65), yticks=range(len(METHODS)),
           yticklabels=[METHOD_LABELS[method] for method in METHODS])
    ax.grid(axis="x", linestyle=":", alpha=.5)
    ax.set_axisbelow(True)


def render_hpa(results_dir, output_dir):
    groups = {method: scale_up_actions(results_dir / method) for method in METHODS}
    samples = {
        method: [action["scale_to_ready_ms_upper"] / 1000 for action in actions
                 if action["status"] == "observed_settled"]
        for method, (actions, _) in groups.items()
    }
    totals = {method: len(actions) for method, (actions, _) in groups.items()}
    largest = max((fmean(values) for values in samples.values() if values), default=0)

    fig, ax = plt.subplots(figsize=(9.5, 3.3), layout="constrained")
    fig.suptitle("Average HPA scale-up time", fontsize=17)
    draw_averages(ax, samples, totals, "scale-ups", "No scale-ups recorded")
    ax.set(xlim=(0, max(1, largest * 1.43)), xlabel="Seconds from scale request to first poll with all target replicas ready")
    caption = "All phases pooled per method. Averages include scale-ups with a recorded ready target."
    if not any(samples.values()):
        ax.xaxis.set_visible(False)
        caption = "No average is available without a completed scale-up."
    unknown = sum(count for _, count in groups.values())
    if unknown:
        caption += f"\n{unknown} requests had unknown direction."
    fig.supxlabel(caption, fontsize=10)
    fig.savefig(output_dir / "hpa-scale-up-to-ready.png", dpi=300)
    plt.close(fig)


def render_scheduler(results_dir, output_dir):
    cohorts = {method: scheduling_delays(results_dir / method) for method in METHODS}
    stages = (
        ("Pod created → scheduled", "created_at", "scheduled_at"),
        ("Pod scheduled → ready", "scheduled_at", "ready_at"),
        ("Pod created → ready", "created_at", "ready_at"),
    )
    totals = {method: len(cohort) for method, cohort in cohorts.items()}
    fig, axes = plt.subplots(3, 1, sharex=True, figsize=(9.5, 6.8), layout="constrained")
    fig.suptitle("Average pod scheduling and startup time", fontsize=17)
    largest = 0
    for ax, (title, start, end) in zip(axes, stages):
        samples = {
            method: [row[end] - row[start] for row in cohort
                     if row[start] is not None and row[end] is not None and row[end] >= row[start]]
            for method, cohort in cohorts.items()
        }
        largest = max(largest, *(fmean(values) if values else 0 for values in samples.values()))
        draw_averages(ax, samples, totals, "pods", "No pods recorded")
        ax.set_title(title, loc="left")
    axes[-1].set(xlim=(0, max(1, largest * 1.4)), xlabel="Average elapsed time (seconds)")
    caption = ("All phases and initial startup pooled per method. Each average uses pods with both timestamps.\n"
               "Kubernetes records the underlying timestamps in whole seconds.")
    if any(row["source"] == "final-snapshot-survivor" for rows in cohorts.values() for row in rows):
        caption += "\nSome timings were recovered from surviving pods in the final snapshot."
    fig.supxlabel(caption, fontsize=10)
    fig.savefig(output_dir / "pod-scheduling-and-startup.png", dpi=300)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", type=Path, help="saved run with baseline/ and proposed/ data")
    args = parser.parse_args()
    for method in METHODS:
        if not (args.results_dir / method / "metrics.jsonl").is_file():
            parser.error(f"missing {method}/metrics.jsonl in {args.results_dir}")
    output_dir = args.results_dir / "slides"
    output_dir.mkdir(exist_ok=True)
    render_hpa(args.results_dir, output_dir)
    render_scheduler(args.results_dir, output_dir)
    print(f"Overhead figures written to {output_dir}")


if __name__ == "__main__":
    main()
