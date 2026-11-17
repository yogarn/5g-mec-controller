import json
import math

import pytest

from utils import control_overhead as overhead


def save(path, records):
    path.write_text(''.join(json.dumps(r) + '\n' for r in records))


def test_native_metrics_parser_filters_and_retains_scope_labels():
    name = overhead.FAMILIES[0]
    rows = overhead.parse_component_metrics(f'''# HELP x description
{name}_sum{{profile="default-scheduler"}} 0.012
{name}_count{{profile="default-scheduler"}} 3
{name}_bucket{{profile="default-scheduler",le="+Inf"}} 3
unrelated_total 999
''')
    assert [r['value'] for r in rows] == [.012, 3, 3]
    assert rows[0]['labels'] == {'profile': 'default-scheduler'}
    assert rows[2]['labels']['le'] == '+Inf'


def test_snapshot_failure_is_saved_not_zero_filled(monkeypatch):
    def fail(*a, **kw):
        raise OSError('metrics endpoint unavailable')
    monkeypatch.setattr(overhead.subprocess, 'check_output', fail)
    row = overhead.component_snapshot()
    assert 'error' in row and 'series' not in row


def test_execution_counter_deltas_do_not_treat_cumulative_time_as_cycle_time(tmp_path):
    family = overhead.FAMILIES[0]
    def rec(t, total, count):
        return dict(sample_time=t, series=[dict(metric=family+'_sum', labels={}, value=total),
                                          dict(metric=family+'_count', labels={}, value=count)])
    save(tmp_path/'control-overhead.jsonl', [rec(100, 2, 100), rec(105, 2.012, 103), rec(110, 2.012, 103), rec(115, .002, 1)])
    rows = overhead.component_execution_windows(tmp_path)
    assert rows[0]['observations'] == 3
    assert rows[0]['mean_execution_ms'] == pytest.approx(4)
    assert rows[0]['scope'].startswith('cluster-wide')
    assert rows[1]['status'] == 'no_observations' and math.isnan(rows[1]['mean_execution_ms'])
    assert rows[2]['status'] == 'counter_reset' and math.isnan(rows[2]['mean_execution_ms'])


def test_failed_or_long_native_collection_gaps_are_not_interpolated(tmp_path):
    save(tmp_path/'control-overhead.jsonl', [dict(sample_time=100, series=[]), dict(sample_time=105, error='failed'),
                                           dict(sample_time=110, series=[]), dict(sample_time=200, series=[])])
    assert overhead.component_execution_windows(tmp_path) == []


def test_pod_lifecycle_fields_and_survivor_fallback_are_explicit(tmp_path):
    pod = dict(kind='Pod', metadata=dict(name='app-a', uid='u', creationTimestamp='2026-01-01T00:00:00Z'),
               spec=dict(nodeName='gnb-2', schedulerName='default-scheduler'),
               status=dict(conditions=[dict(type='PodScheduled', status='True', lastTransitionTime='2026-01-01T00:00:01Z'),
                                       dict(type='Ready', status='True', lastTransitionTime='2026-01-01T00:00:10Z')]))
    fields = overhead.pod_timing_fields(pod)
    save(tmp_path/'metrics.jsonl', [dict(sample_time=1, pods=[dict(name='app-a', node='gnb-2'), dict(name='app-evicted', node='gnb-3')])])
    (tmp_path/'deployments-after.json').write_text(json.dumps(dict(items=[pod, dict(pod, metadata=dict(name='unrelated', uid='other'))])))
    rows = overhead.scheduling_delays(tmp_path)
    assert len(rows) == 2
    assert rows[0]['source'] == 'final-snapshot-survivor'
    assert rows[0]['created_to_scheduled_ms'] == 1000
    assert rows[0]['scheduled_to_ready_ms'] == 9000
    assert rows[1]['status'] == 'timestamps_not_recorded' and math.isnan(rows[1]['created_to_scheduled_ms'])
    save(tmp_path/'metrics.jsonl', [dict(sample_time=t, pods=[dict(name='app-a', node='gnb-2', **fields)]) for t in [1, 2]])
    rows = overhead.scheduling_delays(tmp_path)
    assert len(rows) == 1 and rows[0]['source'] == 'sampled-pod-lifecycle'


def test_hpa_action_delay_bounds_and_superseded_targets(tmp_path):
    save(tmp_path/'hpa-status.jsonl', [dict(sample_time=101, last_scale_time=100, desired_replicas=5, name='hpa'),
                                     dict(sample_time=111, last_scale_time=110, desired_replicas=3, name='hpa'),
                                     dict(sample_time=116, last_scale_time=110, desired_replicas=3, name='hpa')])
    save(tmp_path/'metrics.jsonl', [dict(sample_time=t, pods=[dict(ready=True)] * count) for t, count in [(101, 2), (106, 4), (111, 4), (116, 3)]])
    rows = overhead.hpa_action_delays(tmp_path)
    assert rows[0]['status'] == 'superseded' and math.isnan(rows[0]['scale_to_ready_ms_upper'])
    assert rows[1]['scale_to_ready_ms_lower'] == 1000
    assert rows[1]['scale_to_ready_ms_upper'] == 6000
    assert rows[1]['status'] == 'observed_settled'


def test_old_run_has_no_fabricated_execution_durations(tmp_path):
    (tmp_path/'controller.log').write_text('2026/11/17 01:00:00 scheduler bound pod=a\n2026/11/17 01:00:02 descheduler noop\n')
    assert overhead.proposed_execution_records(tmp_path) == []
    assert overhead.component_execution_windows(tmp_path) == []
    (tmp_path/'controller.log').write_text('decision timing operation=scheduler_score duration_ms=0.123000 pod=a\n')
    assert overhead.proposed_execution_records(tmp_path)[0]['execution_ms'] == .123


def test_native_process_restart_with_higher_counters_is_not_a_valid_delta(tmp_path):
    family = overhead.FAMILIES[0]
    def sample(t, start, value):
        return dict(sample_time=t, process_start_time=start, series=[
            dict(metric=family+'_sum', labels={}, value=value),
            dict(metric=family+'_count', labels={}, value=value*1000)])
    save(tmp_path/'control-overhead.jsonl', [sample(100, 1, .01), sample(105, 102, .02)])
    assert overhead.component_execution_windows(tmp_path) == []


def test_hpa_preexisting_scale_time_is_not_reported_as_new_action(tmp_path):
    save(tmp_path/'hpa-status.jsonl', [dict(sample_time=200, last_scale_time=10, desired_replicas=3)])
    save(tmp_path/'metrics.jsonl', [dict(sample_time=201, pods=[dict(ready=True)]*3)])
    row = overhead.hpa_action_delays(tmp_path)[0]
    assert row['status'] == 'action_predates_recording' and math.isnan(row['scale_to_ready_ms_upper'])


def test_native_snapshot_records_process_start_and_filters_raw_metrics(monkeypatch):
    raw = 'process_start_time_seconds 123.5\n' + overhead.FAMILIES[0] + '_count 2\nunrelated 999\n'
    monkeypatch.setattr(overhead.subprocess, 'check_output', lambda *a, **kw: raw)
    row = overhead.component_snapshot()
    assert row['process_start_time'] == 123.5
    assert len(row['series']) == 1 and row['series'][0]['value'] == 2


def test_startup_delay_keeps_first_ready_timestamp_after_readiness_flaps(tmp_path):
    base = dict(name='app-a', uid='u', node='n', created_at=100, scheduled_at=101)
    save(tmp_path/'metrics.jsonl', [dict(sample_time=t, pods=[dict(base, ready_at=ready)])
                                  for t, ready in [(110, 110), (115, None), (130, 130)]])
    row = overhead.scheduling_delays(tmp_path)[0]
    assert row['ready_at'] == 110 and row['scheduled_to_ready_ms'] == 9000


def test_final_snapshot_can_fill_missing_fields_without_replacing_sampled_start(tmp_path):
    save(tmp_path/'metrics.jsonl', [dict(sample_time=101, pods=[dict(name='app-a', uid='u', node='n', created_at=100)])])
    pod = dict(kind='Pod', metadata=dict(name='app-a', uid='u', creationTimestamp=100), spec=dict(schedulerName='default-scheduler'),
               status=dict(conditions=[dict(type='PodScheduled', status='True', lastTransitionTime=101),
                                       dict(type='Ready', status='True', lastTransitionTime=110)]))
    (tmp_path/'deployments-after.json').write_text(json.dumps(dict(items=[pod])))
    row = overhead.scheduling_delays(tmp_path)[0]
    assert row['source'] == 'sampled-and-final-snapshot'
    assert row['created_at'] == 100 and row['created_to_scheduled_ms'] == 1000
    assert row['scheduled_to_ready_ms'] == 9000
