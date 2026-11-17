import csv
import math

import pytest

from utils.plot_autoscaler import load_coverage, load_resource_coverage, load_slo


def write_summary(root, method, rows):
    directory = root / method
    directory.mkdir()
    with (directory / 'summary.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=['phase', 'attempts', 'slo_ms', 'slo_met_attempts'])
        writer.writeheader()
        writer.writerows(rows)


def test_slo_violations_include_all_unsuccessful_attempts_and_keep_empty_phase_missing(tmp_path):
    rows = [{'phase': 'one', 'attempts': 10, 'slo_ms': 500, 'slo_met_attempts': 7},
            {'phase': 'empty', 'attempts': 0, 'slo_ms': 500, 'slo_met_attempts': 0}]
    for method in ('baseline', 'proposed'):
        write_summary(tmp_path, method, rows)
    result = load_slo(tmp_path, ['one', 'empty'], 500)
    assert result['baseline'][0] == 30
    assert math.isnan(result['baseline'][1])
    assert result['proposed'][0] == 30


def test_slo_violations_reject_mismatched_threshold(tmp_path):
    rows = [{'phase': 'one', 'attempts': 10, 'slo_ms': 250, 'slo_met_attempts': 8}]
    for method in ('baseline', 'proposed'):
        write_summary(tmp_path, method, rows)
    with pytest.raises(ValueError, match='threshold'):
        load_slo(tmp_path, ['one'], 500)


def test_slo_violations_reject_more_met_than_attempts(tmp_path):
    rows = [{'phase': 'one', 'attempts': 2, 'slo_ms': 500, 'slo_met_attempts': 3}]
    for method in ('baseline', 'proposed'):
        write_summary(tmp_path, method, rows)
    with pytest.raises(ValueError, match='exceeds attempts'):
        load_slo(tmp_path, ['one'], 500)


def test_load_coverage_computes_completed_over_scheduled(tmp_path):
    for method, rows in [
        ('baseline', [{'phase': 'one', 'attempts': 8, 'scheduled': 10, 'completed': 8}]),
        ('proposed', [{'phase': 'one', 'attempts': 10, 'scheduled': 10, 'completed': 10}]),
    ]:
        directory = tmp_path / method
        directory.mkdir()
        with (directory / 'summary.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=['phase', 'attempts', 'scheduled', 'completed'])
            writer.writeheader()
            writer.writerows(rows)
    cov = load_coverage(tmp_path, ['one'])
    assert cov['baseline'] == [80.0]
    assert cov['proposed'] == [100.0]


def test_load_resource_coverage_preserves_missing_and_partial_rounds(tmp_path):
    fieldnames = [
        'phase', 'cpu_millicores', 'memory_mib', 'complete_samples',
        'partial_samples', 'rejected_samples', 'coverage_ratio', 'completeness_status',
    ]
    for method, rows in [
        ('baseline', [
            {'phase': 'p1', 'cpu_millicores': '200', 'memory_mib': '64', 'complete_samples': 3,
             'partial_samples': 0, 'rejected_samples': 0, 'coverage_ratio': '1.0', 'completeness_status': 'complete'},
            {'phase': 'p2', 'cpu_millicores': '', 'memory_mib': '', 'complete_samples': 0,
             'partial_samples': 2, 'rejected_samples': 1, 'coverage_ratio': '0.0', 'completeness_status': 'incomplete_only'},
        ]),
        ('proposed', [
            {'phase': 'p1', 'cpu_millicores': '250', 'memory_mib': '64', 'complete_samples': 1,
             'partial_samples': 1, 'rejected_samples': 0, 'coverage_ratio': '0.5', 'completeness_status': 'partial'},
            {'phase': 'p2', 'cpu_millicores': '', 'memory_mib': '', 'complete_samples': 0,
             'partial_samples': 0, 'rejected_samples': 0, 'coverage_ratio': '0.0', 'completeness_status': 'missing'},
        ]),
    ]:
        directory = tmp_path / method
        directory.mkdir()
        with (directory / 'resource-by-ramp.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    cov = load_resource_coverage(tmp_path, ['p1', 'p2'])
    assert cov['baseline'][0] == {'complete': 3, 'rounds': 3, 'missing': 0, 'expected': 3, 'coverage_pct': 100.0, 'status': 'complete', 'source': 'metrics-server', 'rate_window_seconds': '', 'freshness_seconds': '', 'cpu_quality': 'exact'}
    assert cov['baseline'][1] == {'complete': 0, 'rounds': 3, 'missing': 0, 'expected': 3, 'coverage_pct': 0.0, 'status': 'incomplete_only', 'source': 'metrics-server', 'rate_window_seconds': '', 'freshness_seconds': '', 'cpu_quality': 'missing'}
    assert cov['proposed'][0] == {'complete': 1, 'rounds': 2, 'missing': 0, 'expected': 2, 'coverage_pct': 50.0, 'status': 'partial', 'source': 'metrics-server', 'rate_window_seconds': '', 'freshness_seconds': '', 'cpu_quality': 'exact'}
    assert cov['proposed'][1] == {'complete': 0, 'rounds': 0, 'missing': 0, 'expected': 0, 'coverage_pct': 0.0, 'status': 'missing', 'source': 'metrics-server', 'rate_window_seconds': '', 'freshness_seconds': '', 'cpu_quality': 'missing'}


def test_load_resource_coverage_supports_cadvisor_source_and_rate_window(tmp_path):
    fieldnames = [
        'phase', 'resource_source', 'rate_window_seconds', 'freshness_seconds', 'cpu_millicores', 'memory_mib',
        'complete_samples', 'expected_samples', 'coverage_ratio', 'completeness_status', 'cpu_quality',
    ]
    for method, val in (('baseline', '12'), ('proposed', '6')):
        directory = tmp_path / method
        directory.mkdir()
        with (directory / 'resource-by-ramp.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({
                'phase': 'p1', 'resource_source': 'prometheus-cadvisor', 'rate_window_seconds': '45',
                'freshness_seconds': '25',
                'cpu_millicores': '250', 'memory_mib': '128', 'complete_samples': val,
                'expected_samples': '12', 'coverage_ratio': f'{int(val) / 12:.4f}',
                'completeness_status': 'complete' if val == '12' else 'partial', 'cpu_quality': 'rate_45s',
            })
    cov = load_resource_coverage(tmp_path, ['p1'])
    assert cov['baseline'][0]['source'] == 'prometheus-cadvisor'
    assert cov['baseline'][0]['rate_window_seconds'] == '45'
    assert cov['baseline'][0]['freshness_seconds'] == '25'
    assert cov['baseline'][0]['expected'] == 12
    assert cov['proposed'][0]['coverage_pct'] == 50.0


def test_15s_windows_cover_120s_and_partial_and_empty_windows(tmp_path):
    import json
    from utils.plot_autoscaler import build_phase_windows_15s

    method_dir = tmp_path / 'proposed'
    method_dir.mkdir()
    records = [
        {'event': 'phase_start', 'phase': 'phase_01', 'start_time': 1000.0, 'duration_s': 120.0,
         'rps_per_ue': 1.0, 'ues': [{'source_address': '10.0.0.1'}]},
        # Window 0 [0,15): 2 successes (10ms, 30ms) + 1 failure -> p90 = 28ms, violation = 33.33%
        {'event': 'request', 'phase': 'phase_01', 'start_time': 1001.0, 'dispatch_offset_s': 1.0,
         'scheduled_offset_s': 1.0, 'success': True, 'duration_ms': 10.0},
        {'event': 'request', 'phase': 'phase_01', 'start_time': 1005.0, 'dispatch_offset_s': 5.0,
         'scheduled_offset_s': 5.0, 'success': True, 'duration_ms': 30.0},
        {'event': 'request', 'phase': 'phase_01', 'start_time': 1010.0, 'dispatch_offset_s': 10.0,
         'scheduled_offset_s': 10.0, 'success': False, 'duration_ms': 50.0},
        # Window 1 [15,30): intentionally empty -> p90 NaN, violation NaN
        {'event': 'phase_end', 'phase': 'phase_01', 'generation_duration_s': 120.0, 'end_time': 1120.0},
        # Partial 50s phase -> 4 windows, last [45, 50)
        {'event': 'phase_start', 'phase': 'phase_02', 'start_time': 1200.0, 'duration_s': 50.0,
         'rps_per_ue': 1.0, 'ues': [{'source_address': '10.0.0.1'}]},
        {'event': 'request', 'phase': 'phase_02', 'start_time': 1246.0, 'dispatch_offset_s': 46.0,
         'scheduled_offset_s': 46.0, 'success': True, 'duration_ms': 200.0},
        {'event': 'phase_end', 'phase': 'phase_02', 'generation_duration_s': 50.0, 'end_time': 1250.0},
    ]
    (method_dir / 'requests.jsonl').write_text('\n'.join(json.dumps(r) for r in records) + '\n')

    rows = build_phase_windows_15s(method_dir, slo_ms=100.0)
    p1 = [r for r in rows if r['phase'] == 'phase_01']
    p2 = [r for r in rows if r['phase'] == 'phase_02']
    assert len(p1) == 8
    assert [(r['start_offset_s'], r['end_offset_s']) for r in p1] == [
        (0.0, 15.0), (15.0, 30.0), (30.0, 45.0), (45.0, 60.0),
        (60.0, 75.0), (75.0, 90.0), (90.0, 105.0), (105.0, 120.0),
    ]
    assert p1[0]['attempts'] == 3
    assert p1[0]['failures'] == 1
    assert math.isclose(p1[0]['p90_ms'], 28.0)
    assert math.isclose(p1[0]['violation_percent'], 100.0 / 3.0)
    assert p1[0]['actual_start_time'] == 1000.0
    assert p1[0]['actual_end_time'] == 1015.0
    assert p1[1]['attempts'] == 0
    assert math.isnan(p1[1]['p90_ms'])
    assert math.isnan(p1[1]['violation_percent'])

    assert len(p2) == 4
    assert (p2[-1]['start_offset_s'], p2[-1]['end_offset_s']) == (45.0, 50.0)
    assert math.isclose(p2[-1]['p90_ms'], 200.0)
    assert math.isclose(p2[-1]['violation_percent'], 100.0)


def test_pod_placement_15s_enforces_freshness_and_reports_missing(tmp_path):
    import json
    from utils.plot_autoscaler import POD_LOOKUP_MAX_AGE_S, build_pod_placement_15s

    assert POD_LOOKUP_MAX_AGE_S == 8.0
    method_dir = tmp_path / 'proposed'
    method_dir.mkdir()
    metrics = [
        # Fresh for window 0 end (1015.0, age 5s <= 8s) and window 1 end (1018.0, age 8s == 8s)
        {'sample_time': 1010.0, 'pods': [
            {'name': 'p1', 'node': 'gnb-2', 'ready': True},
            {'name': 'p2', 'node': 'gnb-2', 'ready': False},
            {'name': 'p3', 'node': 'gnb-3', 'ready': True},
        ]},
        # Future relative to window 0/1/2 ends (1035.0 > 1030.0) -> must not be used for window 2!
        {'sample_time': 1035.0, 'pods': [
            {'name': 'p4', 'node': 'gnb-3', 'ready': True},
        ]},
    ]
    (method_dir / 'metrics.jsonl').write_text('\n'.join(json.dumps(m) for m in metrics) + '\n')
    windows = [
        {'phase': 'phase_01', 'window_index': 0, 'start_offset_s': 0.0, 'end_offset_s': 15.0, 'actual_end_time': 1015.0},
        {'phase': 'phase_01', 'window_index': 1, 'start_offset_s': 15.0, 'end_offset_s': 18.0, 'actual_end_time': 1018.0},
        {'phase': 'phase_01', 'window_index': 2, 'start_offset_s': 18.0, 'end_offset_s': 30.0, 'actual_end_time': 1030.0},
        {'phase': 'phase_01', 'window_index': 3, 'start_offset_s': 30.0, 'end_offset_s': 45.0, 'actual_end_time': 1045.0},
    ]
    rows = build_pod_placement_15s(method_dir, windows, ['gnb-2', 'gnb-3'])
    by_key = {(r['window_index'], r['node']): r for r in rows}
    assert by_key[(0, 'gnb-2')]['ready_count'] == 1
    assert by_key[(0, 'gnb-2')]['pod_count'] == 2
    assert by_key[(0, 'gnb-3')]['ready_count'] == 1
    assert by_key[(0, 'gnb-2')]['status'] == 'fresh'
    assert by_key[(0, 'gnb-2')]['freshness_seconds'] == 8.0
    assert by_key[(1, 'gnb-2')]['status'] == 'fresh'  # exact 8.0s boundary
    assert by_key[(2, 'gnb-2')]['status'] == 'stale'  # 1030 - 1010 = 20s > 8s; 1035 ignored
    assert math.isnan(by_key[(2, 'gnb-2')]['ready_count'])
    assert by_key[(3, 'gnb-3')]['status'] == 'stale'  # 1045 - 1035 = 10s > 8s
    assert math.isnan(by_key[(3, 'gnb-3')]['ready_count'])


def _make_run_dir(res_dir, phases, baseline_pods=1, proposed_pods=1, include_requests_for=('baseline', 'proposed'), include_ready_count=True):
    import json

    for m_idx, method in enumerate(('baseline', 'proposed')):
        mdir = res_dir / method
        mdir.mkdir(parents=True)
        offset = m_idx * 500.0
        req_lines, met_lines = [], []
        ramp_rows, sum_rows, pod_rows, res_rows = [], [], [], []
        n_pods = baseline_pods if method == 'baseline' else proposed_pods
        for p, ue, g1, g2, g3, t0, dur in phases:
            t_start = t0 + offset
            req_lines.append(json.dumps({
                'event': 'phase_start', 'phase': p, 'start_time': t_start, 'duration_s': dur,
                'rps_per_ue': 1.0, 'ues': [{'source_address': f'10.0.0.{i}'} for i in range(int(ue))],
            }))
            req_lines.append(json.dumps({
                'event': 'request', 'phase': p, 'start_time': t_start + 5.0, 'dispatch_offset_s': 5.0,
                'scheduled_offset_s': 5.0, 'success': True, 'duration_ms': 40.0 + m_idx * 10.0,
            }))
            req_lines.append(json.dumps({
                'event': 'phase_end', 'phase': p, 'generation_duration_s': dur, 'end_time': t_start + dur,
            }))
            pods_list = [{'name': f'pod-{i}', 'node': 'gnb-3', 'ready': True} for i in range(n_pods)]
            for t_off in (10.0, 25.0):
                met_lines.append(json.dumps({'sample_time': t_start + t_off, 'pods': pods_list}))
            ramp_rows.append({'phase': p, 'workload_ue': ue, 'gnb1_ues': g1, 'gnb2_ues': g2, 'gnb3_ues': g3, 'p50_ms': 40, 'p90_ms': 50})
            sum_rows.append({'phase': p, 'attempts': 1, 'slo_ms': 100, 'slo_met_attempts': 1, 'scheduled': 30, 'completed': 1, 'generation_duration_s': dur})
            for node in ('gnb-2', 'gnb-3'):
                row = {'phase': p, 'node': node, 'pod_count': n_pods if node == 'gnb-3' else 0}
                if include_ready_count:
                    row['ready_count'] = n_pods if node == 'gnb-3' else 0
                pod_rows.append(row)
            res_rows.append({'phase': p, 'cpu_millicores': 200, 'memory_mib': 128, 'complete_samples': 2, 'expected_samples': 2, 'coverage_ratio': 1.0, 'completeness_status': 'complete'})
        if method in include_requests_for:
            (mdir / 'requests.jsonl').write_text('\n'.join(req_lines) + '\n')
        (mdir / 'metrics.jsonl').write_text('\n'.join(met_lines) + '\n')
        for fname, rows in [('ramp-summary.csv', ramp_rows), ('summary.csv', sum_rows), ('pods-by-ramp.csv', pod_rows), ('resource-by-ramp.csv', res_rows)]:
            with (mdir / fname).open('w', newline='') as s:
                w = csv.DictWriter(s, fieldnames=list(rows[0]))
                w.writeheader()
                w.writerows(rows)


def test_plot_autoscaler_exports_png_only_at_300_dpi_and_window_csvs(tmp_path, monkeypatch):
    import sys
    from PIL import Image
    from utils import plot_autoscaler

    res_dir = tmp_path / 'results'
    out_dir = tmp_path / 'plots'
    phases = [
        ('phase_01', '3', '1', '1', '1', 1000.0, 30.0),
        ('phase_02', '9', '3', '3', '3', 1100.0, 30.0),
        ('phase_03', '3', '1', '1', '1', 1200.0, 30.0),  # repeated UE level
    ]
    _make_run_dir(res_dir, phases)

    captured_figs = {}
    orig_export = plot_autoscaler._export_png

    def capture_export(fig, output_dir, name):
        captured_figs[name] = {
            'axes_xdata': [[list(line.get_xdata()) for line in ax.get_lines()] for ax in fig.axes],
            'axes_ylim': [ax.get_ylim() for ax in fig.axes],
            'series': [[(list(line.get_xdata()), list(line.get_ydata())) for line in ax.lines
                        if line.get_label() in ('Baseline', 'Proposed')] for ax in fig.axes],
        }
        orig_export(fig, output_dir, name)

    monkeypatch.setattr(plot_autoscaler, '_export_png', capture_export)
    monkeypatch.setattr(sys, 'argv', ['plot_autoscaler', str(res_dir), str(out_dir), '--slo-ms', '100'])
    plot_autoscaler.main()

    for name in ('response-time', 'resource-usage', 'slo-violations'):
        xs, ys = captured_figs[name]['series'][0][0]
        assert xs[:2] == [0, 15] and ys[0] == 0
    assert len(captured_figs['resource-usage']['series']) == 2
    assert len(captured_figs['resource-usage-phase-summary']['series']) == 2
    assert list(out_dir.glob('*.pdf')) == []
    pngs = sorted(p.name for p in out_dir.glob('*.png'))
    expected = ['pod-placement', 'resource-usage-phase-summary', 'resource-usage', 'response-time', 'slo-violations',
                'cpu-by-node', 'memory-by-node', 'response-time-p90-by-node', 'response-time-p50-by-node',
                'slo-violations-by-node', 'scheduling-delay']
    assert pngs == sorted(f'{name}{suffix}.png' for name in expected for suffix in ('', '_zoom'))
    with Image.open(out_dir / 'response-time.png') as img:
        dpi = img.info.get('dpi')
        assert dpi is not None and round(dpi[0]) == 300

    # Window-end x-coordinates: [15, 30], [45, 60], [75, 90] (never midpoints, never connected across phases)
    rt_lines = captured_figs['response-time']['axes_xdata'][0]
    assert [0.0, 15.0, 30.0] in rt_lines
    assert [45.0, 60.0] in rt_lines
    assert [75.0, 90.0] in rt_lines
    assert not any(len(xs) > 2 and xs == [15.0, 30.0, 45.0, 60.0, 75.0, 90.0] for xs in rt_lines)

    with (out_dir / 'window-metrics-15s.csv').open() as s:
        win_rows = list(csv.DictReader(s))
    base_p = [r['phase'] for r in win_rows if r['method'] == 'baseline']
    assert base_p == ['phase_01', 'phase_01', 'phase_02', 'phase_02', 'phase_03', 'phase_03']
    assert [float(r['aligned_end_s']) for r in win_rows if r['method'] == 'baseline'] == [15.0, 30.0, 45.0, 60.0, 75.0, 90.0]


def test_pod_placement_y_axis_not_clipped_by_baseline(tmp_path, monkeypatch):
    import sys
    from utils import plot_autoscaler

    res_dir = tmp_path / 'results'
    out_dir = tmp_path / 'plots'
    phases = [('phase_01', '9', '3', '3', '3', 1000.0, 30.0)]
    _make_run_dir(res_dir, phases, baseline_pods=3, proposed_pods=8)

    placement_limits = {}
    orig_export = plot_autoscaler._export_png

    def capture(fig, output_dir, name):
        if name == 'pod-placement':
            placement_limits['ylims'] = [ax.get_ylim() for ax in fig.axes]
            placement_limits['yticks'] = [list(ax.get_yticks()) for ax in fig.axes]
            placement_limits['bars'] = [[
                (p.get_x() + p.get_width() / 2, p.get_height()) for p in ax.patches
            ] for ax in fig.axes]
        orig_export(fig, output_dir, name)

    monkeypatch.setattr(plot_autoscaler, '_export_png', capture)
    monkeypatch.setattr(sys, 'argv', ['plot_autoscaler', str(res_dir), str(out_dir), '--slo-ms', '100'])
    plot_autoscaler.main()

    for lo, hi in placement_limits['ylims']:
        assert lo <= 0
        assert hi >= 8.0
    for ticks in placement_limits['yticks']:
        assert all(float(t).is_integer() for t in ticks)
    assert sorted({end for end, height in placement_limits['bars'][0]}) == [10.0, 25.0]
    assert max(height for end, height in placement_limits['bars'][0]) == 3
    assert max(height for end, height in placement_limits['bars'][1]) == 8


def test_continuous_mode_uses_one_timeline_and_preserves_missing_windows(tmp_path, monkeypatch):
    import json
    import sys
    import matplotlib.pyplot as plt
    from utils import plot_autoscaler

    root = tmp_path / 'results'
    _make_run_dir(root, [('phase_01', '3', '1', '1', '1', 1000, 30),
                         ('phase_02', '3', '1', '1', '1', 1030, 30)])
    for method in ('baseline', 'proposed'):
        path = root / method / 'requests.jsonl'
        records = [json.loads(line) for line in path.read_text().splitlines()]
        for record in records:
            if record['event'] == 'phase_start':
                record['phase_transition'] = 'continuous'
        path.write_text(''.join(json.dumps(record) + '\n' for record in records))
    captured = {}
    def capture(fig, directory, name):
        if name == 'response-time':
            captured['lines'] = [(list(line.get_xdata()), list(line.get_ydata())) for line in fig.axes[0].lines
                                 if line.get_label() in ('Baseline', 'Proposed')]
            captured['label'] = fig.axes[-1].get_xlabel()
        plt.close(fig)
    monkeypatch.setattr(plot_autoscaler, '_export_png', capture)
    monkeypatch.setattr(sys, 'argv', ['plot', str(root), str(tmp_path / 'plots'), '--slo-ms', '100'])
    plot_autoscaler.main()
    assert len(captured['lines']) == 2
    for xs, ys in captured['lines']:
        assert xs == [0, 15, 30, 45, 60]
        assert ys[0] == 0
        assert math.isnan(ys[2]) and math.isnan(ys[4])
    assert 'reattachment is included' in captured['label']


def test_placement_bars_stack_nodes_and_omit_unknown_observations():
    import matplotlib.pyplot as plt
    from utils.plot_autoscaler import _placement_bars

    fig, ax = plt.subplots()
    try:
        totals = _placement_bars(
            ax, [3, 18, 33], [12, 12, 4],
            {'a': [3, math.nan, 2], 'b': [8, 4, 1]},
            ['a', 'b'], {'a': 'blue', 'b': 'orange'})
        assert totals == [11, 3]
        assert len(ax.patches) == 4
        assert [p.get_y() for p in ax.patches] == [0, 0, 3, 2]
        assert [p.get_x() + p.get_width() for p in ax.patches] == [15, 37, 15, 37]
    finally:
        plt.close(fig)


def test_phase_brackets_group_seconds_below_the_time_ticks():
    import matplotlib.pyplot as plt
    from utils.plot_autoscaler import _decorate_time_axes

    fig, axes = plt.subplots(2, 1, figsize=(7.5, 6.5), layout='constrained')
    try:
        _decorate_time_axes(axes, ['p1', 'p2'], {'p1': (0, 120), 'p2': (120, 240)},
                            240, ['P1\n9 UE\n7:1:1', 'P2\n9 UE\n1:7:1'],
                            list(range(15, 241, 15)), 'Traffic elapsed time (s)')
        fig.canvas.draw()
        brackets = [line for line in axes[-1].lines if line.get_gid() == 'phase-bracket']
        labels = [text for text in axes[-1].texts if text.get_gid() == 'phase-label']
        assert [list(line.get_xdata()) for line in brackets] == [[0, 0, 120, 120], [120, 120, 240, 240]]
        assert [text.get_position()[0] for text in labels] == [60, 180]
        renderer = fig.canvas.get_renderer()
        ticks = [text.get_window_extent(renderer) for text in axes[-1].get_xticklabels()]
        assert all(text.get_window_extent(renderer).y1 < min(t.y0 for t in ticks) for text in labels)
        assert all(text.get_window_extent(renderer).y0 >= 0 for text in labels)
        assert not axes[0].child_axes  # No phase labels above the first panel.
    finally:
        plt.close(fig)


@pytest.mark.parametrize('include_requests_for', [('baseline',), ()])
def test_missing_requests_writes_phase_summary_outputs_without_15s_claims(tmp_path, monkeypatch, capsys, include_requests_for):
    import sys
    from utils import plot_autoscaler

    res_dir = tmp_path / 'results'
    out_dir = tmp_path / 'plots'
    phases = [('phase_01', '3', '1', '1', '1', 1000.0, 30.0)]
    _make_run_dir(res_dir, phases, include_requests_for=include_requests_for, include_ready_count=False)

    titles = {}
    orig_export = plot_autoscaler._export_png

    def capture(fig, output_dir, name):
        titles[name] = (fig._suptitle.get_text() if fig._suptitle else '') + ' ' + ' '.join(ax.get_ylabel() for ax in fig.axes)
        orig_export(fig, output_dir, name)

    monkeypatch.setattr(plot_autoscaler, '_export_png', capture)
    monkeypatch.setattr(sys, 'argv', ['plot_autoscaler', str(res_dir), str(out_dir), '--slo-ms', '100'])
    plot_autoscaler.main()

    err = capsys.readouterr().err
    assert 'Skipping 15s detailed plots' in err
    assert not (out_dir / 'window-metrics-15s.csv').exists()
    assert not (out_dir / 'pod-placement-15s.csv').exists()
    assert not (out_dir / 'response-time.png').exists()
    assert (out_dir / 'response-time-phase-summary.png').exists()
    assert (out_dir / 'slo-violations-phase-summary.png').exists()
    assert (out_dir / 'pod-placement-phase-summary.png').exists()
    assert 'ready counts unavailable' in titles['pod-placement-phase-summary']
    assert 'Phase summary' in titles['resource-usage']


def test_observation_bars_preserve_intermediate_counts_and_phase_boundaries(tmp_path):
    import json
    from utils.plot_autoscaler import build_pod_observations

    windows = [dict(phase='p1', actual_start_time=1000, actual_end_time=1015),
               dict(phase='p1', actual_start_time=1015, actual_end_time=1030),
               dict(phase='p2', actual_start_time=1030, actual_end_time=1045)]
    records = []
    for when, count in [(999, 2), (1028, 3), (1030, 4), (1032, 5), (1046, 6)]:
        records.append(dict(sample_time=when, pods=[dict(node='n', ready=i < 3) for i in range(count)]))
    records.append(dict(sample_time=1031, error='API unavailable'))
    (tmp_path / 'metrics.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in records))
    rows = build_pod_observations(tmp_path, windows, ['n', 'empty'])
    observed = [r for r in rows if r['node'] == 'n']
    assert [(r['aligned_sample_s'], r['pod_count'], r['phase']) for r in observed] == [
        (28, 3, 'p1'), (30, 4, 'p2'), (32, 5, 'p2')]
    assert [r['ready_count'] for r in observed] == [3, 3, 3]
    assert all(r['pod_count'] == 0 for r in rows if r['node'] == 'empty')


def test_display_origin_never_fills_missing_windows_or_restarts_at_phase_boundary():
    from utils.plot_autoscaler import _window_points
    points = [dict(aligned_end_s=15, end_offset_s=15, value=12),
              dict(aligned_end_s=30, end_offset_s=30, value=math.nan)]
    xs, ys = _window_points(points, 'value')
    assert xs == [0, 15, 30]
    assert ys[:2] == [0, 12] and math.isnan(ys[2])
    xs, ys = _window_points([dict(aligned_end_s=75, end_offset_s=15, value=12)], 'value')
    assert xs == [75] and ys == [12]


def test_zoom_fits_all_measured_values_shared_axes_and_omits_only_synthetic_origin():
    import matplotlib.pyplot as plt
    from utils.plot_autoscaler import _zoom_axes
    fig, axes = plt.subplots(2, sharey=True)
    axes[0].plot([0, 15, 30], [0, 140, 150], marker='o', gid='window-series')
    axes[1].plot([15, 30], [400, 410], marker='o')
    axes[0].axhline(1000, label='SLO')
    _zoom_axes(fig)
    assert math.isnan(axes[0].lines[0].get_ydata()[0])
    assert axes[0].get_ylim() == axes[1].get_ylim()
    low, high = axes[0].get_ylim()
    assert 0 < low < 140 and 410 < high < 1000
    assert list(axes[1].lines[0].get_ydata()) == [400, 410]
    plt.close(fig)


def test_zoom_bars_preserve_zero_and_total_stacked_height():
    import matplotlib.pyplot as plt
    from utils.plot_autoscaler import _zoom_axes
    fig, ax = plt.subplots()
    ax.bar([1], [3])
    ax.bar([1], [5], bottom=[3])
    _zoom_axes(fig)
    low, high = ax.get_ylim()
    assert low == 0 and high > 8
    plt.close(fig)
