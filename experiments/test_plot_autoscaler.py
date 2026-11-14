import csv
import math

import pytest

from experiments.plot_autoscaler import load_slo


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
