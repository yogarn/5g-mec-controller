#!/usr/bin/env python3
"""Plot the four vertical-offloading arms on a single shared phase axis."""
import argparse
import json
import math
from pathlib import Path

import yaml

from utils.vertical_summary import ARMS, SITES, summarize_campaign, validate_arm_alignment


ARM_STYLES = {
    'baseline-local': {'color': '#0072B2', 'linestyle': '--'},
    'proposed-local': {'color': '#D55E00', 'linestyle': '--'},
    'baseline-vertical': {'color': '#0072B2', 'linestyle': '-'},
    'proposed-vertical': {'color': '#D55E00', 'linestyle': '-'},
}


def _arm_labels(campaign_dir: Path) -> dict[str, str]:
    metadata_path = campaign_dir / 'campaign.yaml'
    metadata = (yaml.safe_load(metadata_path.read_text(encoding='utf-8')) or {}
                if metadata_path.is_file() else {})
    methods = {'baseline': 'Default Scheduler', 'proposed': 'Mobility-Aware Scheduler'}
    if metadata.get('comparison') == 'full' and metadata.get('autoscaler') == 'hpa':
        methods['baseline'] += ' + CPU HPA'
        methods['proposed'] += ' + SLO-Based Autoscaler'
    modes = {'local': 'RAN Only', 'vertical': 'Vertical Offloading'}
    labels = {}
    for arm in ARMS:
        method, mode = arm.split('-')
        labels[arm] = f'{methods[method]} ({modes[mode]})'
    return labels


def _format_log_rtt(ax):
    from matplotlib.ticker import LogLocator, NullFormatter, StrMethodFormatter

    ax.set_yscale('log')
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
    ax.yaxis.set_major_formatter(StrMethodFormatter('{x:g}'))
    ax.yaxis.set_minor_formatter(NullFormatter())


def plot_slides(campaign_dir: Path, output_dir: Path, slo_ms: float) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator
    from utils import plot_autoscaler as common
    from utils.plot_slides import export_slide

    summary = json.loads((campaign_dir / 'vertical-4arm-summary.json').read_text())
    arm_labels = _arm_labels(campaign_dir)
    phases = summary['arms'][ARMS[0]]['phases']
    names = [row['phase'] for row in phases]
    bounds, duration = common.compute_aligned_timeline(names, [row['duration_s'] for row in phases])
    labels = [
        f"{row['phase'].replace('phase_', 'P')}\n{row['workload_ue']} UE\n"
        + ':'.join(str(row[f'gnb{i}_ues']) for i in (1, 2, 3))
        for row in phases
    ]
    windows = {arm: common.build_phase_windows_15s(campaign_dir / arm, slo_ms) for arm in ARMS}
    continuous = all(row.get('phase_transition') == 'continuous'
                     for rows in windows.values() for row in rows)
    xlabel = ('Traffic elapsed time (s); moving-UE reattachment is included'
              if continuous else common.ALIGNED_TIME_NOTE)
    window_ends = [bounds[row['phase']][0] + row['end_offset_s'] for row in windows[ARMS[0]]]
    output_dir.mkdir(parents=True, exist_ok=True)

    def draw(axes, panels, rows_by_arm, *, log_y=False):
        for ax, (key, title, ylabel, upper) in zip(axes, panels):
            for arm, rows in rows_by_arm.items():
                groups = [rows] if continuous else [[row for row in rows if row['phase'] == phase] for phase in names]
                for i, group in enumerate(groups):
                    points = [dict(row, aligned_end_s=bounds[row['phase']][0] + row['end_offset_s'])
                              for row in group]
                    style = ARM_STYLES[arm].copy()
                    if i == 0:
                        style['label'] = arm_labels[arm]
                    if log_y:
                        xs = [row['aligned_end_s'] for row in points]
                        ys = [row[key] if row[key] > 0 else math.nan for row in points]
                    else:
                        xs, ys = common._window_points(points, key)
                    ax.plot(xs, ys, gid='window-series', linewidth=1.6, **style)
            ax.set(title=title, ylabel=ylabel)
            if log_y:
                _format_log_rtt(ax)
                ax.margins(y=0.08)
            else:
                ax.set_ylim(0, upper)
            ax.grid(axis='y', linestyle=':', which='both' if log_y else 'major')
            ax.legend(frameon=False)
        common._decorate_time_axes(axes, names, bounds, duration, labels, window_ends, xlabel)

    with plt.rc_context({'font.size': 9, 'axes.titlesize': 10, 'legend.fontsize': 8}):
        fig = plt.figure(figsize=(common.THESIS_FIG_WIDTH, 7.1), layout='constrained')
        grid = fig.add_gridspec(3, 1, height_ratios=(0.4, 1, 1))
        legend_ax = fig.add_subplot(grid[0])
        p90_ax = fig.add_subplot(grid[1])
        axes = (p90_ax, fig.add_subplot(grid[2], sharex=p90_ax))
        p90_ax.tick_params(labelbottom=False)
        draw(axes, [('p90_ms', 'Response time p90', 'Response time (ms, log scale)', None),
                    ('p50_ms', 'Response time p50', 'Response time (ms, log scale)', None)],
             windows, log_y=True)
        for ax in axes:
            ax.axhline(slo_ms, color='#666666', linestyle=':', label=f'SLO ({slo_ms:g} ms)')
            ax.get_legend().remove()
        handles, labels_for_legend = p90_ax.get_legend_handles_labels()
        legend_ax.set_axis_off()
        legend_ax.legend(handles, labels_for_legend, loc='center', frameon=False)
        fig.suptitle('Response time over 15-second windows', fontsize=15)
        fig.savefig(output_dir / 'response-time.png', dpi=300)
        plt.close(fig)
        for old_plot in ('response-time_zoom.png', 'response-time_slo.png'):
            (output_dir / old_plot).unlink(missing_ok=True)

        fig, ax = plt.subplots(figsize=(common.THESIS_FIG_WIDTH, 4.5), layout='constrained')
        draw([ax], [('violation_percent', f'Failed or slower than {slo_ms:g} ms', 'SLO violations (%)', 100)], windows)
        export_slide(fig, output_dir, 'slo-violations', legend_columns=1)

        resources = {arm: common.build_resource_windows(campaign_dir / arm, rows) for arm, rows in windows.items()}
        fig, axes = plt.subplots(2, 1, figsize=(common.THESIS_FIG_WIDTH, 6.5), sharex=True, layout='constrained')
        draw(axes, [('cpu_millicores', 'CPU (anchors and dynamic pods)', 'Pod CPU (millicores)', None),
                    ('memory_mib', 'RAM (anchors and dynamic pods)', 'Pod memory (MiB)', None)], resources)
        export_slide(fig, output_dir, 'resource-usage', legend_columns=1)

        nodes = ('gnb1', 'gnb-2', 'gnb-3', 'core-5g')
        colors = common._node_colors(nodes)
        fig, axes = plt.subplots(len(ARMS), 1, figsize=(common.THESIS_FIG_WIDTH, 9),
                                 sharex=True, sharey=True, layout='constrained')
        peak = 0
        uses_readiness = False
        for ax, arm in zip(axes, ARMS):
            arm_dir = campaign_dir / arm
            readiness_path = arm_dir / 'vertical-readiness.json'
            readiness = json.loads(readiness_path.read_text()) if readiness_path.is_file() else {}
            rows = common.build_pod_observations(
                arm_dir, windows[arm], nodes, archived_core_count=readiness.get('core_backend_actual_ready'),
            )
            uses_readiness |= any(row.get('from_readiness') for row in rows)
            times = [row['aligned_sample_s'] for row in rows if row['node'] == nodes[0]]
            counts = {node: [row['pod_count'] for row in rows if row['node'] == node] for node in nodes}
            if times:
                widths = [1.5] * len(times)
                heights = common._placement_bars(ax, [t - 0.75 for t in times], widths, counts, nodes, colors)
                peak = max(peak, max(heights, default=0))
            else:
                ax.text(0.5, 0.5, 'No pod observations', ha='center', va='center', transform=ax.transAxes)
            ax.set(title=arm_labels[arm], ylabel='Pods')
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax.grid(axis='y', linestyle=':')
        axes[0].set_ylim(0, peak + max(1.5, 0.2 * peak))
        if axes[0].patches:
            axes[0].legend(frameon=False, ncols=len(nodes), loc='upper right')
        common._decorate_time_axes(axes, names, bounds, duration, labels, window_ends, xlabel)
        title = 'Inference pod placement'
        if uses_readiness:
            title += '\nArchived Core count from arm-start readiness'
        fig.suptitle(title, fontsize=15)
        common._export_png(fig, output_dir, 'pod-placement')


def plot_four_arm(campaign_dir: Path, output_dir: Path, slo_ms: float = 150.0) -> Path:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    summary_path = campaign_dir / 'vertical-4arm-summary.json'
    if summary_path.is_file():
        summary = json.loads(summary_path.read_text(encoding='utf-8'))
    else:
        summary = summarize_campaign(campaign_dir, slo_ms)

    arms = summary.get('arms') or {}
    if not arms:
        raise RuntimeError(f'no arm summaries found in {campaign_dir}')
    validate_arm_alignment(arms)
    arm_labels = _arm_labels(campaign_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / 'vertical-4arm-comparison.png'

    fig, axes = plt.subplots(4, 1, figsize=(11, 13), sharex=True)
    ax_p90, ax_viol, ax_sched, ax_core = axes

    phase_labels = []
    for arm_name in ARMS:
        if arm_name not in arms:
            continue
        phases = arms[arm_name]['phases']
        if not phase_labels:
            phase_labels = [p['phase'] for p in phases]
        xs = [p['phase_index'] for p in phases]
        p90s = [p['p90_ms'] if p['p90_ms'] is not None else float('nan') for p in phases]
        viols = [
            p['slo_violation_rate'] * 100.0 if p['slo_violation_rate'] is not None else float('nan')
            for p in phases
        ]
        sched_att = [
            p['slo_attainment_scheduled'] * 100.0 if p.get('slo_attainment_scheduled') is not None else float('nan')
            for p in phases
        ]
        cov = [
            p['request_coverage'] * 100.0 if p.get('request_coverage') is not None else float('nan')
            for p in phases
        ]
        style = dict(ARM_STYLES[arm_name], label=arm_labels[arm_name])
        ax_p90.plot(xs, p90s, **style, linewidth=2)
        ax_viol.plot(xs, viols, **style, linewidth=2)
        ax_sched.plot(xs, sched_att, **style, linewidth=2)
        ax_sched.plot(xs, cov, color=style['color'], linestyle=':', alpha=0.55, linewidth=1.2)

    ax_p90.axhline(slo_ms, color='#d62728', linestyle=':', linewidth=1.5, label=f'SLO ({slo_ms:g} ms)')
    _format_log_rtt(ax_p90)
    ax_p90.set_ylabel('p90 RTT (ms, log scale)')
    ax_p90.set_title('Vertical Offloading 4-Arm Comparison (Shared Phase Axis)')
    ax_p90.grid(True, alpha=0.3)
    ax_p90.legend(loc='upper right', ncol=2, fontsize=8)

    ax_viol.set_ylabel('SLO Violation / Attempt (%)')
    ax_viol.set_ylim(-2, 105)
    ax_viol.grid(True, alpha=0.3)
    ax_viol.legend(loc='upper right', ncol=2, fontsize=8)

    ax_sched.set_ylabel('Scheduled SLO Attainment (%)\n(dotted = coverage %)')
    ax_sched.set_ylim(-2, 105)
    ax_sched.grid(True, alpha=0.3)
    ax_sched.legend(loc='lower right', ncol=2, fontsize=8)

    site_colors = {'gnb-1': '#1f77b4', 'gnb-2': '#ff7f0e', 'gnb-3': '#2ca02c'}
    for arm_name, ls in (('baseline-vertical', '--'), ('proposed-vertical', '-')):
        if arm_name not in arms:
            continue
        site_phases = arms[arm_name]['site_phases']
        for site in SITES:
            rows = [r for r in site_phases if r['site'] == site]
            xs = [r['phase_index'] for r in rows]
            ys = [r['core_served_pct'] for r in rows]
            ax_core.plot(
                xs,
                ys,
                color=site_colors[site],
                linestyle=ls,
                marker='o' if ls == '-' else 'x',
                linewidth=1.8,
                label=f'{arm_labels[arm_name]} · {site}',
            )

    ax_core.set_ylabel('Core-Served (% of successes)')
    ax_core.set_xlabel('Campaign Phase')
    ax_core.set_ylim(-2, 105)
    ax_core.grid(True, alpha=0.3)
    ax_core.legend(loc='upper right', ncol=2, fontsize=8)

    if phase_labels:
        ticks = list(range(1, len(phase_labels) + 1))
        ax_core.set_xticks(ticks)
        ax_core.set_xticklabels(phase_labels, rotation=20, ha='right')

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign_dir', type=Path)
    parser.add_argument('output_dir', type=Path)
    parser.add_argument('--slo-ms', type=float, default=150.0)
    args = parser.parse_args(argv)
    out = plot_four_arm(args.campaign_dir, args.output_dir, slo_ms=args.slo_ms)
    print(f'Plot written: {out}')
    slides_dir = args.campaign_dir / 'slides'
    plot_slides(args.campaign_dir, slides_dir, args.slo_ms)
    print(f'Slide figures written to {slides_dir}')


if __name__ == '__main__':
    main()
