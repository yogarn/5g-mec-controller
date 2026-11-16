import csv
import json

import pytest

from utils.transient_analysis import main as transient_main


def test_transient_10s_bins_distinguish_latency_violations_attainment_and_readiness(tmp_path):
    results_dir = tmp_path / 'results'
    out_dir = tmp_path / 'derived'
    method_dir = results_dir / 'proposed'
    method_dir.mkdir(parents=True)

    # 1 UE at 1 RPS/UE over 20s => 10 scheduled per 10s bin (total 20 scheduled).
    start = 1700000000.0
    events = [
        {'event': 'phase_start', 'phase': 'phase_02', 'start_time': start, 'duration_s': 20.0,
         'rps_per_ue': 1.0, 'ues': [{'group': 'gnb-1', 'source_address': '10.0.0.1', 'interface': 'u1'}]},
        # Bin 1 (0..10s): 8 started (2 missed), 6 succeed <= 150ms (100ms), 1 succeeds > 150ms (200ms), 1 fails.
        *[{'event': 'request', 'phase': 'phase_02', 'start_time': start + 1.0, 'dispatch_offset_s': 1.0 + i * 0.5, 'success': True, 'duration_ms': 100.0} for i in range(6)],
        {'event': 'request', 'phase': 'phase_02', 'start_time': start + 5.0, 'dispatch_offset_s': 5.0, 'success': True, 'duration_ms': 200.0},
        {'event': 'request', 'phase': 'phase_02', 'start_time': start + 6.0, 'dispatch_offset_s': 6.0, 'success': False, 'duration_ms': 5000.0},
        # Bin 2 (10..20s): 10 started (0 missed), all 10 succeed at 120ms.
        *[{'event': 'request', 'phase': 'phase_02', 'start_time': start + 11.0, 'dispatch_offset_s': 11.0 + i * 0.5, 'success': True, 'duration_ms': 120.0} for i in range(10)],
        {'event': 'phase_end', 'phase': 'phase_02', 'start_time': start, 'end_time': start + 20.0, 'generation_duration_s': 20.0},
    ]
    (method_dir / 'requests.jsonl').write_text(''.join(json.dumps(e) + '\n' for e in events))
    (method_dir / 'summary.csv').write_text('phase,slo_ms\nphase_02,150.0\n')
    (method_dir / 'metrics.jsonl').write_text(''.join(json.dumps(m) + '\n' for m in [
        {'sample_time': start + 2.0, 'pods': [{'name': 'p1', 'ready': True}, {'name': 'p2', 'ready': False}],
         'container_statuses': {'p1': {'restarts': 1}}},
        {'sample_time': start + 15.0, 'pods': [{'name': 'p1', 'ready': True}, {'name': 'p2', 'ready': True}],
         'container_statuses': {'p1': {'restarts': 1}}},
    ]))
    (method_dir / 'hpa-status.jsonl').write_text(json.dumps({
        'sample_time': start + 4.0, 'current_replicas': 1, 'desired_replicas': 5,
    }) + '\n')

    transient_main([str(results_dir), str(out_dir)])
    rows = list(csv.DictReader((out_dir / 'proposed-phase_02-10s-bins.csv').open()))
    assert len(rows) == 2

    b1, b2 = rows
    assert b1['scheduled'] == '10'
    assert b1['started'] == '8'
    assert b1['missed'] == '2'
    assert b1['completed'] == '8'
    assert b1['successes'] == '7'
    assert b1['failures'] == '1'
    # Successful-request p50/p90 ignores the 5000ms failure.
    assert float(b1['success_p50_ms']) == pytest.approx(100.0)
    assert float(b1['success_p90_ms']) == pytest.approx(140.0)
    # All-attempt violation rate: 2/8 (1 slow + 1 failure); attainment: 6/8; scheduled attainment: 6/10.
    assert float(b1['all_attempt_violation_rate']) == pytest.approx(0.25)
    assert float(b1['slo_attainment_all_attempts']) == pytest.approx(0.75)
    assert float(b1['slo_attainment_scheduled']) == pytest.approx(0.60)
    assert b1['pods_end'] == '2'
    assert b1['ready_end'] == '1'
    assert b1['restarts_max'] == '1'
    assert b1['hpa_desired_max'] == '5'

    assert b2['scheduled'] == '10'
    assert b2['started'] == '10'
    assert b2['missed'] == '0'
    assert b2['ready_end'] == '2'


def test_transient_refuses_to_write_inside_results_dir(tmp_path):
    results_dir = tmp_path / 'results'
    results_dir.mkdir()
    with pytest.raises(SystemExit) as exc:
        transient_main([str(results_dir), str(results_dir / 'derived')])
    assert exc.value.code == 2


def test_transient_refuses_existing_output_dir_and_reconciles_boundary_slots(tmp_path):
    results_dir = tmp_path / 'results'
    existing_out = tmp_path / 'already_exists'
    existing_out.mkdir()
    method_dir = results_dir / 'proposed'
    method_dir.mkdir(parents=True)
    # 9 UEs at 5 RPS/UE over 120s => 450 scheduled per 10s bin (total 5400).
    # Simulate a slot scheduled at 9.995s whose dispatch_offset_s is 10.015s (within 0.05s tolerance).
    start = 1700000000.0
    ues = [{'group': f'gnb-{(i % 3) + 1}', 'source_address': f'10.0.0.{i + 1}', 'interface': f'u{i}'} for i in range(9)]
    events = [
        {'event': 'phase_start', 'phase': 'phase_02', 'start_time': start, 'duration_s': 120.0, 'rps_per_ue': 5.0, 'ues': ues},
        # Dispatch 1 request for UE 8 (offset = 8/9*0.2 = 0.17778) at slot_idx=49 (sched=9.97778s)
        # delayed slightly to dispatch_offset_s=10.01s (inside bin 2!).
        {'event': 'request', 'phase': 'phase_02', 'ue_index': 8, 'source_address': '10.0.0.9',
         'start_time': start + 10.01, 'dispatch_offset_s': 10.01, 'success': True, 'duration_ms': 120.0},
        {'event': 'phase_end', 'phase': 'phase_02', 'start_time': start, 'end_time': start + 120.0,
         'generation_duration_s': 120.0, 'scheduled': 5400, 'started': 1, 'missed': 5399},
    ]
    (method_dir / 'requests.jsonl').write_text(''.join(json.dumps(e) + '\n' for e in events))

    with pytest.raises(SystemExit) as exc:
        transient_main([str(results_dir), str(existing_out)])
    assert exc.value.code == 2

    out_dir = tmp_path / 'fresh_out'
    transient_main([str(results_dir), str(out_dir)])
    rows = list(csv.DictReader((out_dir / 'proposed-phase_02-10s-bins.csv').open()))
    assert len(rows) == 12
    assert [int(r['scheduled']) for r in rows] == [450] * 12
    assert sum(int(r['missed']) for r in rows) == 5399
    assert int(rows[0]['missed']) == 449
    assert int(rows[1]['missed']) == 450
    # Request was scheduled in bin 0 (9.97778s) and dispatched in bin 1 (10.01s):
    # scheduled attainment credits bin 0 (1/450), not bin 1 (0/450), while started counts bin 1.
    assert int(rows[0]['started']) == 0
    assert int(rows[1]['started']) == 1
    assert float(rows[0]['slo_attainment_scheduled']) == pytest.approx(1 / 450)
    assert float(rows[1]['slo_attainment_scheduled']) == pytest.approx(0.0)
    assert float(rows[1]['slo_attainment_all_attempts']) == pytest.approx(1.0)
