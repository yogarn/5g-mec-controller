"""Tests for the joint baseline external-metric CPU HPA."""
from decimal import Decimal
import json
import subprocess
import yaml
import pytest

from experiments import campaign, manifests


@pytest.mark.parametrize(
    ('cpu_target', 'pod_req_str', 'pod_req_expr', 'n_fixed', 'window_s'),
    [
        (80, '500m', '0.5', 3, 45),
        (80, '500m', '0.5', 6, 45),
        (65, '250m', '0.25', 4, 30),
        (70, '750m', '0.75', 5, 45),
        (90, '1', '1', 2, 60),
    ],
)
def test_baseline_cpu_rule_derives_constants_from_config(
    cpu_target, pod_req_str, pod_req_expr, n_fixed, window_s
):
    pod_req_cores = Decimal(str(manifests._parse_cpu_millicores(pod_req_str))) / Decimal('1000')
    rule = manifests._baseline_cpu_adapter_rule(
        n_fixed,
        cpu_hpa_target=cpu_target,
        pod_request_cores=pod_req_cores,
        window_seconds=window_s,
    )
    expected_name = f'mec_baseline_cpu_dynamic_replicas_fixed{n_fixed}'
    expected_target = manifests._format_decimal(Decimal(cpu_target) / Decimal(100))
    assert rule['name'] == {'matches': '^up$', 'as': expected_name}
    assert rule['resources'] == {'namespaced': False}
    assert rule['metricsQuery'] == (
        f'clamp_min((sum(rate(container_cpu_usage_seconds_total{{'
        f'job="kubelet-cadvisor",namespace="services",container="pedestrian-detection",'
        f'pod=~"mec-placement-pedestrian-detection.*"}}[{window_s}s])) or vector(0)) '
        f'/ ({expected_target} * {pod_req_expr}) - {n_fixed}, 0)'
    )


def test_plan_only_changes_cpu_target_pod_request_and_n_fixed(tmp_path, monkeypatch):
    # Copy apps/pedestrian-detection/manifest.yaml with a custom 250m CPU request.
    custom_manifest = tmp_path / 'manifest.yaml'
    docs = list(yaml.safe_load_all((campaign.ROOT / 'apps/pedestrian-detection/manifest.yaml').read_text()))
    for doc in docs:
        if doc and doc.get('kind') == 'Deployment':
            doc['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'] = '250m'
    custom_manifest.write_text(yaml.safe_dump_all(docs, sort_keys=False))
    monkeypatch.setattr(manifests, 'workload_manifest', lambda workload='pedestrian-detection': custom_manifest)
    monkeypatch.setattr(campaign, 'workload_manifest', lambda workload='pedestrian-detection': custom_manifest)
    custom_core = tmp_path / 'core-backend.yaml'
    core_doc = yaml.safe_load((campaign.ROOT / 'offloader/core-backend.yaml').read_text())
    core_doc['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'] = '250m'
    custom_core.write_text(yaml.safe_dump(core_doc, sort_keys=False))
    monkeypatch.setattr(campaign, '_core_backend_manifest', lambda: custom_core)

    # Custom vertical campaign config with core_backend_replicas=4 -> N_fixed=7 (vertical), 3 (local).
    cfg = yaml.safe_load((campaign.ROOT / 'experiments/campaign-vertical.yaml').read_text())
    cfg['core_backend_replicas'] = 4
    cfg['min_replicas'] = 7
    custom_cfg = tmp_path / 'campaign-vertical-custom.yaml'
    custom_cfg.write_text(yaml.safe_dump(cfg, sort_keys=False))

    out = tmp_path / 'plan'
    campaign.main([
        '--profile', 'vertical-joint',
        '--config', str(custom_cfg),
        '--cpu-hpa-target', '65',
        '--min-replicas', '7',
        '--plan-only', str(out),
    ])

    cm = yaml.safe_load((out / 'floor-adapter-configmap.yaml').read_text())
    rules = yaml.safe_load(cm['data']['config.yaml'])['externalRules']
    by_name = {r['name']['as']: r for r in rules}
    # Proposed rules preserved
    assert 'ue_p90_latency_seconds' in by_name
    assert 'ue_p90_excess_latency_120ms_seconds' in by_name
    # Baseline rules reflect cpu_target=0.65, pod_request=0.25, N_fixed=3 and 7
    assert '/ (0.65 * 0.25) - 3, 0)' in by_name['mec_baseline_cpu_dynamic_replicas_fixed3']['metricsQuery']
    assert '/ (0.65 * 0.25) - 7, 0)' in by_name['mec_baseline_cpu_dynamic_replicas_fixed7']['metricsQuery']

    local_hpa = yaml.safe_load((out / 'baseline-local/baseline-hpa.yaml').read_text())
    assert local_hpa['spec']['minReplicas'] == 1
    assert local_hpa['spec']['maxReplicas'] == 15
    assert local_hpa['spec']['metrics'] == [{
        'type': 'External',
        'external': {
            'metric': {'name': 'mec_baseline_cpu_dynamic_replicas_fixed3'},
            'target': {'type': 'AverageValue', 'averageValue': '1'},
        },
    }]
    assert local_hpa['spec']['behavior']['scaleUp']['stabilizationWindowSeconds'] == 0
    assert local_hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] == 60

    vert_hpa = yaml.safe_load((out / 'baseline-vertical/baseline-hpa.yaml').read_text())
    assert vert_hpa['spec']['minReplicas'] == 1
    assert vert_hpa['spec']['maxReplicas'] == 11
    assert vert_hpa['spec']['metrics'] == [{
        'type': 'External',
        'external': {
            'metric': {'name': 'mec_baseline_cpu_dynamic_replicas_fixed7'},
            'target': {'type': 'AverageValue', 'averageValue': '1'},
        },
    }]


def test_wait_external_metric_ready_aborts_when_unavailable(monkeypatch):
    clock = [0.0]

    def fake_monotonic():
        return clock[0]

    def fake_sleep(seconds):
        clock[0] += seconds

    def fake_kube(*args, **kwargs):
        raise subprocess.CalledProcessError(1, args, stderr='Error from server (NotFound): metric not found')

    monkeypatch.setattr(campaign, 'kube', fake_kube)
    monkeypatch.setattr(campaign.time, 'monotonic', fake_monotonic)
    monkeypatch.setattr(campaign.time, 'sleep', fake_sleep)

    with pytest.raises(RuntimeError, match='did not return a value from prometheus-adapter'):
        campaign._wait_external_metric_ready(
            'mec_baseline_cpu_dynamic_replicas_fixed3', arm_name='baseline-local', timeout=15, poll_interval=3
        )


def _native_cpu_hpa_step(total_cpu, current_total, *, min_total, max_total,
                         cpu_target=0.8, pod_request=0.5, tolerance=0.1):
    """Simulate Kubernetes autoscaling/v2 Resource CPU HPA over all serving Pods."""
    per_pod_target = cpu_target * pod_request
    usage_ratio = total_cpu / (current_total * per_pod_target)
    outside = abs(1.0 - usage_ratio) > tolerance
    if not outside:
        return current_total, outside
    desired = int(-(-total_cpu // per_pod_target)) if total_cpu > 0 else 0
    # Use exact math.ceil for floating point consistency with k8s HPA
    import math
    desired = math.ceil(total_cpu / per_pod_target) if total_cpu > 0 else 0
    return max(min_total, min(max_total, desired)), outside


def _external_cpu_hpa_step(total_cpu, current_dynamic, *, n_fixed, min_dynamic, max_dynamic,
                           cpu_target=0.8, pod_request=0.5, tolerance=0.1):
    """Simulate autoscaling/v2 External AverageValue=1 HPA using the prometheus-adapter formula."""
    import math
    per_pod_target = cpu_target * pod_request
    metric = max(total_cpu / per_pod_target - n_fixed, 0.0)
    usage_ratio = metric / current_dynamic
    outside = abs(1.0 - usage_ratio) > tolerance
    if not outside:
        return n_fixed + current_dynamic, current_dynamic, outside
    desired_dynamic = max(min_dynamic, min(max_dynamic, math.ceil(metric / 1.0)))
    return n_fixed + desired_dynamic, desired_dynamic, outside


@pytest.mark.parametrize(
    ('arm_type', 'n_fixed', 'max_dynamic'),
    [
        ('local', 3, 15),
        ('vertical', 6, 12),
    ],
)
def test_offline_equivalence_with_native_80pct_cpu_hpa(arm_type, n_fixed, max_dynamic):
    min_dynamic = 1
    min_total = n_fixed + min_dynamic
    max_total = n_fixed + max_dynamic
    assert max_total == 18

    # Sequence of total CPU (cores) covering idle, ramp-up, ceiling saturation, and ramp-down.
    cpu_sequence = [
        0.2, 0.8, 1.5, 2.2, 3.0, 4.2, 5.5, 6.8, 7.6, 8.5,
        6.0, 4.5, 3.2, 2.0, 1.0, 0.3,
    ]
    native_total = min_total
    ext_dynamic = min_dynamic
    compared_outside_band = 0

    for total_cpu in cpu_sequence:
        # Compare from the same starting total Pod count so we test the controller decision directly.
        native_next, native_out = _native_cpu_hpa_step(
            total_cpu, native_total, min_total=min_total, max_total=max_total
        )
        ext_next_total, ext_next_dyn, ext_out = _external_cpu_hpa_step(
            total_cpu, native_total - n_fixed,
            n_fixed=n_fixed, min_dynamic=min_dynamic, max_dynamic=max_dynamic,
        )
        if native_out and ext_out:
            assert ext_next_total == native_next, (
                f'{arm_type}: total_cpu={total_cpu} from {native_total} pods -> '
                f'native={native_next}, external={ext_next_total} (dyn={ext_next_dyn})'
            )
            compared_outside_band += 1
        native_total = native_next
        ext_dynamic = ext_next_dyn

    assert compared_outside_band >= 10
    # Verify final ramp-down returns to minimum total Pods with no +N_fixed offset.
    assert native_total == min_total
    assert n_fixed + ext_dynamic == min_total


def test_repo_anchor_dynamic_and_core_pod_cpu_requests_are_identical(tmp_path):
    manifests._campaign_manifest(
        tmp_path, use_tc=True, workload='pedestrian-detection',
        prefer_close=True, site_anchors=True, site_nodes=campaign.LOCAL_SITE_NODES,
    )
    cpu_cores = manifests.verify_matching_pod_cpu_requests(
        tmp_path / 'workload.yaml',
        anchor_manifest=tmp_path / 'site-anchors.yaml',
        core_manifest=campaign.ROOT / 'offloader/core-backend.yaml',
    )
    assert cpu_cores == Decimal('0.5')


def test_mismatched_core_or_anchor_pod_cpu_request_raises(tmp_path):
    manifests._campaign_manifest(
        tmp_path, use_tc=True, workload='pedestrian-detection',
        prefer_close=True, site_anchors=True, site_nodes=campaign.LOCAL_SITE_NODES,
    )
    bad_core = tmp_path / 'bad-core.yaml'
    core_doc = yaml.safe_load((campaign.ROOT / 'offloader/core-backend.yaml').read_text())
    core_doc['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'] = '750m'
    bad_core.write_text(yaml.safe_dump(core_doc, sort_keys=False))

    with pytest.raises(ValueError, match='Pod CPU request mismatch'):
        manifests.verify_matching_pod_cpu_requests(
            tmp_path / 'workload.yaml',
            anchor_manifest=tmp_path / 'site-anchors.yaml',
            core_manifest=bad_core,
        )
