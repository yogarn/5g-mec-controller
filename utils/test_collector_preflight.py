from types import SimpleNamespace
from unittest.mock import patch

import pytest

from utils.preflight import PreflightError, collector_status


def test_collector_idle_requires_healthy_complete_window():
    settings = SimpleNamespace(prometheus_url='http://prom', max_metric_age_seconds=30)
    # Healthy collector with 0 overflow evictions (idle and active).
    with patch('utils.preflight.Prometheus.scalar', side_effect=[1, 0, 0]):
        status = collector_status(settings)
        assert status['attempts'] == 0
        assert status['overflow_evictions'] == 0
    with patch('utils.preflight.Prometheus.scalar', side_effect=[1, 0, 1]):
        assert collector_status(settings)['attempts'] == 1
    # Overflow evictions (> 0) are informational and non-fatal.
    with patch('utils.preflight.Prometheus.scalar', side_effect=[1, 3, 2]):
        status = collector_status(settings)
        assert status['overflow_evictions'] == 3
        assert status['attempts'] == 2
    # Scrape down, negative/non-integer overflow, or negative attempts remain fatal.
    for values in ([0], [1, -1], [1, 1.5], [1, 0, -1]):
        with patch('utils.preflight.Prometheus.scalar', side_effect=values):
            with pytest.raises(PreflightError):
                collector_status(settings)
    with patch('utils.preflight.Prometheus.scalar', side_effect=ValueError('malformed')):
        with pytest.raises(ValueError):
            collector_status(settings)
