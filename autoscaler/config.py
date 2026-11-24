"""Single-source loader and validator for autoscaler/autoscaler.yaml.

Reads HPA policy defaults (SLO, CPU target, latency floor fraction, replica bounds,
and scale-up/scale-down damping parameters) and validates every field on load.
"""
import math
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / 'autoscaler.yaml'

# Fallback defaults used only when a key is omitted from autoscaler.yaml.
DEFAULT_AUTOSCALER_SETTINGS = {
    'slo_ms': 150,
    'cpu_hpa_target': 80,
    'latency_floor_fraction': 0.8,
    'rt_target_factor': 1.0,
    'min_replicas': 3,
    'max_replicas': 24,
    'staircase_max_replicas': 18,
    'scale_up_stabilization_seconds': 0,
    'scale_down_stabilization_seconds': 60,
    'scale_down_pods': 1,
    'scale_down_period_seconds': 15,
    'proposed_scale_down_period_seconds': 15,
}


def _check_positive_int(value, name, *, minimum=1):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f'{name} must be an integer >= {minimum}')
    return value


def _check_finite_number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f'{name} must be a finite number')
    return value


def validate_autoscaler_config(raw):
    """Validate and normalize autoscaler policy settings."""
    if not isinstance(raw, dict):
        raise ValueError('autoscaler configuration must be a mapping')
    unknown = set(raw) - set(DEFAULT_AUTOSCALER_SETTINGS)
    if unknown:
        raise ValueError(f'unknown autoscaler configuration keys: {sorted(unknown)}')
    cfg = {**DEFAULT_AUTOSCALER_SETTINGS, **raw}

    slo_ms = _check_finite_number(cfg['slo_ms'], 'slo_ms')
    if slo_ms <= 0:
        raise ValueError('slo_ms must be positive')
    cpu_target = _check_positive_int(cfg['cpu_hpa_target'], 'cpu_hpa_target', minimum=1)
    if cpu_target > 100:
        raise ValueError('cpu_hpa_target must be <= 100')
    floor_frac = _check_finite_number(cfg['latency_floor_fraction'], 'latency_floor_fraction')
    if not 0 <= floor_frac < 1:
        raise ValueError('latency_floor_fraction must be in [0, 1)')
    rt_factor = _check_finite_number(cfg['rt_target_factor'], 'rt_target_factor')
    if not 0 < rt_factor <= 1:
        raise ValueError('rt_target_factor must be in (0, 1]')

    min_rep = _check_positive_int(cfg['min_replicas'], 'min_replicas', minimum=1)
    max_rep = _check_positive_int(cfg['max_replicas'], 'max_replicas', minimum=min_rep)
    staircase_max = _check_positive_int(cfg['staircase_max_replicas'], 'staircase_max_replicas', minimum=min_rep)

    _check_positive_int(cfg['scale_up_stabilization_seconds'], 'scale_up_stabilization_seconds', minimum=0)
    _check_positive_int(cfg['scale_down_stabilization_seconds'], 'scale_down_stabilization_seconds', minimum=0)
    _check_positive_int(cfg['scale_down_pods'], 'scale_down_pods', minimum=1)
    _check_positive_int(cfg['scale_down_period_seconds'], 'scale_down_period_seconds', minimum=1)
    _check_positive_int(cfg['proposed_scale_down_period_seconds'], 'proposed_scale_down_period_seconds', minimum=1)

    return cfg


def load_autoscaler_config(path=DEFAULT_CONFIG_PATH):
    """Load and validate autoscaler policy defaults from YAML."""
    raw = yaml.safe_load(Path(path).read_text(encoding='utf-8'))
    return validate_autoscaler_config(raw)
