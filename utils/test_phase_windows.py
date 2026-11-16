import json

import pytest

from utils.phase_windows import summarize_windows


def test_dispatch_and_scheduled_cohorts_stay_separate(tmp_path):
    records = [
        {'event': 'phase_start', 'phase': 'p1', 'start_time': 1000, 'duration_s': 120,
         'rps_per_ue': 1, 'ues': [{'source_address': 'one'}]},
        # This request was scheduled before the late window but dispatched inside it.
        {'event': 'request', 'phase': 'p1', 'start_time': 1060.1, 'dispatch_offset_s': 60.1,
         'scheduled_offset_s': 59, 'success': True, 'duration_ms': 5000},
        {'event': 'request', 'phase': 'p1', 'start_time': 1061, 'dispatch_offset_s': 61,
         'scheduled_offset_s': 61, 'success': True, 'duration_ms': 100},
        {'event': 'request', 'phase': 'p1', 'start_time': 1062, 'dispatch_offset_s': 62,
         'scheduled_offset_s': 62, 'success': False, 'duration_ms': 1000},
        {'event': 'phase_end', 'phase': 'p1', 'generation_duration_s': 120},
    ]
    (tmp_path / 'requests.jsonl').write_text('\n'.join(map(json.dumps, records)))
    whole, late = summarize_windows(tmp_path, 150)
    assert whole['scheduled'] == 120
    assert late['scheduled'] == 60
    assert late['attempts'] == 3
    assert late['started_slots'] == 2
    assert late['missed_slots'] == 58
    assert late['request_coverage'] == 2 / 60
    assert late['scheduled_slo_attainment'] == 1 / 60
    assert late['violation_percent'] == pytest.approx(200 / 3)
    assert late['p90_ms'] == pytest.approx(4510)
    assert late['steady_state_assumed'] is False


def test_old_records_do_not_invent_coverage_and_empty_latency_is_blank(tmp_path):
    records = [
        {'event': 'phase_start', 'phase': 'p1', 'start_time': 1000, 'duration_s': 180,
         'rps_per_ue': 1, 'ues': [{}]},
        {'event': 'request', 'phase': 'p1', 'start_time': 1001, 'success': True, 'duration_ms': 100},
        {'event': 'phase_end', 'phase': 'p1'},
    ]
    (tmp_path / 'requests.jsonl').write_text('\n'.join(map(json.dumps, records)))
    whole, late = summarize_windows(tmp_path, 150)
    assert whole['scheduled'] == ''
    assert late['attempts'] == 0
    assert late['p90_ms'] == ''
    assert late['violation_percent'] == ''
