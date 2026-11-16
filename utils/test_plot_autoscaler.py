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
    assert cov['baseline'][0] == {'complete': 3, 'rounds': 3, 'coverage_pct': 100.0, 'status': 'complete'}
    assert cov['baseline'][1] == {'complete': 0, 'rounds': 3, 'coverage_pct': 0.0, 'status': 'incomplete_only'}
    assert cov['proposed'][0] == {'complete': 1, 'rounds': 2, 'coverage_pct': 50.0, 'status': 'partial'}
    assert cov['proposed'][1] == {'complete': 0, 'rounds': 0, 'coverage_pct': 0.0, 'status': 'missing'}
