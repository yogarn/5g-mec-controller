from types import SimpleNamespace
from unittest.mock import patch

import pytest

from experiments.preflight import PreflightError, collector_status


def test_collector_idle_requires_healthy_complete_window():
    settings = SimpleNamespace(prometheus_url='http://prom', max_metric_age_seconds=30)
    with patch('experiments.preflight.Prometheus.scalar', side_effect=[1, 1, 0]):
        assert collector_status(settings)['attempts'] == 0
    with patch('experiments.preflight.Prometheus.scalar', side_effect=[1, 1, 1]):
        assert collector_status(settings)['attempts'] == 1
    for values in ([0], [1, 0]):
        with patch('experiments.preflight.Prometheus.scalar', side_effect=values):
            with pytest.raises(PreflightError):
                collector_status(settings)
    with patch('experiments.preflight.Prometheus.scalar', side_effect=ValueError('malformed')):
        with pytest.raises(ValueError):
            collector_status(settings)
