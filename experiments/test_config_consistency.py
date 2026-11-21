"""Cross-component YAML/manifest consistency tests.

Verifies that shared runtime parameters (SLO, p90 windows, staleness, descheduler
intervals, site-to-node topology, and collector ConfigMap parity) stay synchronized
across collector, offloader, scheduler/descheduler, autoscaler, and defaults.
"""
from decimal import Decimal
from pathlib import Path
import re

import pytest
import yaml

from autoscaler.config import load_autoscaler_config, validate_autoscaler_config
from collector.server import load_settings as load_collector_settings
from experiments import campaign
from utils import defaults

ROOT = Path(__file__).resolve().parent.parent


def _load_configmap_yaml(manifest_path: Path, configmap_name: str, data_key: str = 'config.yaml') -> dict:
    for doc in yaml.safe_load_all(manifest_path.read_text(encoding='utf-8')):
        if doc and doc.get('kind') == 'ConfigMap' and doc.get('metadata', {}).get('name') == configmap_name:
            return yaml.safe_load(doc['data'][data_key])
    raise AssertionError(f'ConfigMap {configmap_name!r} not found in {manifest_path}')


def _parse_k8s_millis(value: str) -> float:
    """Parse Kubernetes quantity like '150m' -> 0.15 (seconds)."""
    text = str(value).strip()
    if text.endswith('m'):
        return float(Decimal(text[:-1]) / Decimal(1000))
    return float(Decimal(text))


def test_slo_consistent_across_all_components():
    collector_cfg = load_collector_settings(ROOT / 'collector/config.yaml')
    collector_cm = _load_configmap_yaml(ROOT / 'collector/manifest.yaml', 'ue-telemetry-config')
    offloader_cfg = yaml.safe_load((ROOT / 'offloader/config.yaml').read_text(encoding='utf-8'))
    root_cfg = yaml.safe_load((ROOT / 'config.yaml').read_text(encoding='utf-8'))
    autoscaler_cfg = load_autoscaler_config()
    hpa_proposed = yaml.safe_load((ROOT / 'autoscaler/hpa-proposed.yaml').read_text(encoding='utf-8'))

    expected_slo_ms = 150.0
    expected_slo_s = 0.15

    assert float(collector_cfg['slo_seconds']) == pytest.approx(expected_slo_s)
    assert float(collector_cm['slo_seconds']) == pytest.approx(expected_slo_s)
    assert float(offloader_cfg['offload_p90_threshold_ms']) == pytest.approx(expected_slo_ms)
    assert float(root_cfg['slo_seconds']) == pytest.approx(expected_slo_s)
    assert float(autoscaler_cfg['slo_ms']) == pytest.approx(expected_slo_ms)
    assert float(defaults.SLO_MS) == pytest.approx(expected_slo_ms)
    assert float(campaign.DEFAULT_SLO_MS) == pytest.approx(expected_slo_ms)

    hpa_target_s = _parse_k8s_millis(hpa_proposed['spec']['metrics'][0]['external']['target']['value'])
    assert hpa_target_s == pytest.approx(expected_slo_s)


def test_p90_windows_and_shared_intervals_consistent():
    collector_cfg = load_collector_settings(ROOT / 'collector/config.yaml')
    offloader_cfg = yaml.safe_load((ROOT / 'offloader/config.yaml').read_text(encoding='utf-8'))
    root_cfg = yaml.safe_load((ROOT / 'config.yaml').read_text(encoding='utf-8'))
    placement_cm = _load_configmap_yaml(ROOT / 'placement/deploy.yaml', 'placement-controller-config')
    autoscaler_cfg = load_autoscaler_config()
    hpa_baseline = yaml.safe_load((ROOT / 'autoscaler/hpa-baseline.yaml').read_text(encoding='utf-8'))

    # p90 sliding window and staleness shared between collector and offloader.
    assert collector_cfg['window_seconds'] == 15
    assert collector_cfg['offload_window_seconds'] == collector_cfg['window_seconds']
    assert float(collector_cfg['offload_stale_seconds']) == pytest.approx(float(offloader_cfg['stale_after_seconds']))

    # Placement traffic-share window (5 s) must match the '/ 5' divisor in mobility_query.
    placement_window = collector_cfg['placement_window_seconds']
    for label, query in (
        ('config.yaml', root_cfg['mobility_query']),
        ('placement/deploy.yaml', placement_cm['mobility_query']),
    ):
        match = re.search(r'/\s*(\d+)\s*$', query.strip())
        assert match is not None, f'missing window divisor in {label} mobility_query: {query!r}'
        assert int(match.group(1)) == placement_window

    # Descheduler interval shared across scheduler configs and campaign defaults.
    assert int(root_cfg['deschedule_interval_seconds']) == int(placement_cm['deschedule_interval_seconds'])
    assert int(root_cfg['deschedule_interval_seconds']) == int(campaign.DEFAULT_DESCHEDULE_INTERVAL_S)

    # Baseline CPU HPA target matches autoscaler.yaml.
    baseline_cpu_target = hpa_baseline['spec']['metrics'][0]['resource']['target']['averageUtilization']
    assert int(baseline_cpu_target) == int(autoscaler_cfg['cpu_hpa_target'])


def test_collector_manifest_matches_collector_config_yaml():
    cfg_file = yaml.safe_load((ROOT / 'collector/config.yaml').read_text(encoding='utf-8'))
    cfg_cm = _load_configmap_yaml(ROOT / 'collector/manifest.yaml', 'ue-telemetry-config')
    assert cfg_cm == cfg_file


def test_site_topology_consistent_between_collector_and_offloader():
    collector_cfg = load_collector_settings(ROOT / 'collector/config.yaml')
    offloader_cfg = yaml.safe_load((ROOT / 'offloader/config.yaml').read_text(encoding='utf-8'))
    assert offloader_cfg['core_node'] == collector_cfg['core_node']
    assert offloader_cfg['site_nodes'] == collector_cfg['site_nodes']


def test_autoscaler_config_loader_validates_inputs():
    cfg = load_autoscaler_config()
    assert cfg['slo_ms'] == 150
    assert cfg['cpu_hpa_target'] == 80
    assert cfg['latency_floor_fraction'] == pytest.approx(0.8)
    assert cfg['proposed_scale_down_period_seconds'] == 15

    with pytest.raises(ValueError, match='unknown autoscaler'):
        validate_autoscaler_config({'bogus_key': 1})
    with pytest.raises(ValueError, match='cpu_hpa_target'):
        validate_autoscaler_config({'cpu_hpa_target': 150})
    with pytest.raises(ValueError, match='latency_floor_fraction'):
        validate_autoscaler_config({'latency_floor_fraction': 1.0})
