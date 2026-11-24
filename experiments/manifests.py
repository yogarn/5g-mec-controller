"""Pure manifest, HPA, ConfigMap, and resource-request builders for campaigns.

Renders workload Deployments, Services, site-anchor Deployments, native HPA
manifests, prometheus-adapter floor configs, and in-cluster Go placement
ConfigMaps from profile settings without performing cluster mutations.
"""
from copy import deepcopy
from decimal import Decimal
import json
import math
from pathlib import Path
import sys

import yaml
from utils.config import validate_resource_weights
from utils.traffic_control import POOL_NAME

from experiments.profiles import (
    AUTOSCALER_CONFIG,
    AUTOSCALERS,
    COMPARISONS,
    DEFAULT_CPU_HPA_TARGET,
    LOCAL_SITE_NODES,
    NAMESPACE,
    PLACEMENT_NAMESPACE,
    PLACEMENT_PROMETHEUS_URL,
    ROOT,
    SCALER_MIN_REPLICAS,
    SCALER_NODE,
    SITE_NODES,
    SPILL_READINESS_PROBE,
    UPF_PLACEMENT_QUERY,
    WORKLOADS,
    _GO_PLACEMENT_OMIT_KEYS,
)


DEFAULT_BASELINE_CPU_RATE_WINDOW_SECONDS = 45


def _get_root():
    """Return campaign.ROOT when monkeypatched by tests, else repository ROOT."""
    mod = sys.modules.get('experiments.campaign')
    return getattr(mod, 'ROOT', ROOT) if mod is not None else ROOT


def workload_identity(workload='pedestrian-detection'):
    if workload not in WORKLOADS:
        raise ValueError('unknown workload')
    return 'mec-placement-' + workload


def workload_manifest(workload='pedestrian-detection'):
    workload_identity(workload)
    return _get_root() / 'apps' / workload / 'manifest.yaml'


def _workload_http_threads(workload='pedestrian-detection'):
    """Return configured HTTP_THREADS from the workload Deployment env, if set."""
    for doc in yaml.safe_load_all(workload_manifest(workload).read_text()):
        if doc and doc.get('kind') == 'Deployment':
            for container in doc.get('spec', {}).get('template', {}).get('spec', {}).get('containers', []):
                for entry in container.get('env') or []:
                    if entry.get('name') == 'HTTP_THREADS' and entry.get('value') is not None:
                        return int(entry['value'])
    return None


def selected_workload(settings):
    return settings.get('workload', 'pedestrian-detection')


def _site_nodes(settings):
    return LOCAL_SITE_NODES if settings.get('radio_only_gnb1') is False else SITE_NODES


def _site_anchor_nodes(edge_only, site_nodes=SITE_NODES):
    return {site: node for site, node in site_nodes.items() if not edge_only or node != 'core-5g'}


def _placement_query(nodes):
    return UPF_PLACEMENT_QUERY.replace('core-5g|gnb-2|gnb-3', '|'.join(nodes))


def check_service_routing(service, prefer_close=False):
    """Require cluster-wide endpoints without locality hints or client affinity."""
    spec = service['spec']
    annotations = service.get('metadata', {}).get('annotations') or {}
    if (spec.get('trafficDistribution') != ('PreferClose' if prefer_close else None) or
            spec.get('externalTrafficPolicy', 'Cluster') != 'Cluster' or
            spec.get('internalTrafficPolicy', 'Cluster') != 'Cluster' or
            spec.get('sessionAffinity', 'None') != 'None' or
            any(annotations.get(key, 'Disabled').lower() not in ('disabled', '')
                for key in ('service.kubernetes.io/topology-mode',
                            'service.kubernetes.io/topology-aware-hints'))):
        raise RuntimeError('Workload Service routing does not match requested mode or has session affinity')


def _restrict_workload_nodes(deployment, nodes):
    pod_spec = deployment['spec']['template']['spec']
    node_affinity = pod_spec.setdefault('affinity', {}).setdefault('nodeAffinity', {})
    node_affinity['requiredDuringSchedulingIgnoredDuringExecution'] = {
        # Kubernetes combines terms with OR and accepts one value per node field match.
        'nodeSelectorTerms': [{
            'matchFields': [{'key': 'metadata.name', 'operator': 'In', 'values': [node]}],
        } for node in nodes],
    }


def _campaign_manifest(root, use_tc, workload='pedestrian-detection', scheduler_name=None, filename='workload.yaml',
                       prefer_close=False, site_anchors=False, spill_high=None, spill_low=None, edge_only=False, site_nodes=SITE_NODES):
    """Write one shared workload manifest for both methods."""
    documents = list(yaml.safe_load_all(workload_manifest(workload).read_text()))
    deployments = [item for item in documents if item and item.get('kind') == 'Deployment']
    if len(deployments) != 1:
        raise ValueError('application manifest requires exactly one Deployment')
    _restrict_workload_nodes(deployments[0], tuple(_site_anchor_nodes(edge_only, site_nodes).values()))
    if scheduler_name is not None:
        deployments[0]['spec']['template']['spec']['schedulerName'] = scheduler_name
    if use_tc:
        metadata = deployments[0]['spec']['template'].setdefault('metadata', {})
        metadata.setdefault('annotations', {})['cni.projectcalico.org/ipv4pools'] = json.dumps([POOL_NAME])
    if prefer_close:
        service = next((doc for doc in documents if doc and doc.get('kind') == 'Service'), None)
        if service is None:
            raise ValueError('PreferClose requires a workload Service')
        service['spec']['trafficDistribution'] = 'PreferClose'
    if spill_high is not None:
        container = deployments[0]['spec']['template']['spec']['containers'][0]
        container.setdefault('env', []).extend([
            {'name': 'SPILL_HIGH', 'value': str(spill_high)},
            {'name': 'SPILL_LOW', 'value': str(spill_low)},
        ])
        container['readinessProbe'].update(SPILL_READINESS_PROBE)
        # Startup probe failures can restart the container, just like liveness.
        container['startupProbe']['httpGet']['path'] = '/health'
    if site_anchors:
        deployment = deployments[0]
        app = deployment['metadata']['name']
        anchors = []
        for site, node in _site_anchor_nodes(edge_only, site_nodes).items():
            anchor = deepcopy(deployment)
            anchor['metadata']['name'] = f'{app}-anchor-{site}'
            anchor['spec']['replicas'] = 1
            template = anchor['spec']['template']
            selector = {'mec-replica': f'anchor-{site}'}
            anchor['spec']['selector']['matchLabels'].update(selector)
            template['metadata']['labels'].update(selector)
            template['spec']['schedulerName'] = 'default-scheduler'
            template['spec'].setdefault('nodeSelector', {})['kubernetes.io/hostname'] = node
            anchors.append(anchor)
        managed = {'mec-replica': 'managed'}
        deployment['spec']['replicas'] = 0
        deployment['spec']['selector']['matchLabels'].update(managed)
        deployment['spec']['template']['metadata']['labels'].update(managed)
        with (root / 'site-anchors.yaml').open('x') as stream:
            yaml.safe_dump_all(anchors, stream, sort_keys=False)
    path = root / filename
    with path.open('x') as stream:
        yaml.safe_dump_all(documents, stream, sort_keys=False)
    return path



def _latency_metric_name(floor_ms=0, unit='seconds', asymmetric_target_ms=None):
    if unit not in ('seconds', 'milliseconds'):
        raise ValueError(f'unsupported latency metric unit: {unit}')
    if asymmetric_target_ms is not None:
        target = _format_hpa_target(asymmetric_target_ms, unit='milliseconds').replace('.', '_')
        return f'ue_p90_asymmetric_{floor_ms}ms_target_{target}ms_{unit}'
    if floor_ms:
        return f'ue_p90_excess_latency_{floor_ms}ms_{unit}'
    return f'ue_p90_latency_{unit}'


def _format_decimal(value):
    dec = Decimal(str(value)).normalize()
    text = format(dec, 'f')
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    return text


def _pod_cpu_request_cores(deployment):
    """Return total container CPU request in cores (Decimal) for a Deployment."""
    containers = deployment['spec']['template']['spec'].get('containers') or []
    if not containers:
        raise ValueError('Deployment has no containers')
    total_milli = sum(_container_requests(c)[0] for c in containers)
    if total_milli <= 0:
        raise ValueError('Deployment CPU request must be positive')
    return (Decimal(str(total_milli)) / Decimal('1000')).normalize()


def _workload_deployment(workload='pedestrian-detection', manifest_path=None):
    path = Path(manifest_path) if manifest_path is not None else workload_manifest(workload)
    docs = [d for d in yaml.safe_load_all(path.read_text()) if d and d.get('kind') == 'Deployment']
    if len(docs) != 1:
        raise ValueError('application manifest requires exactly one Deployment')
    return docs[0]


def _workload_cpu_request_cores(workload='pedestrian-detection', manifest_path=None):
    return _pod_cpu_request_cores(_workload_deployment(workload, manifest_path=manifest_path))


def _workload_container_name(workload='pedestrian-detection', manifest_path=None):
    containers = _workload_deployment(workload, manifest_path=manifest_path)['spec']['template']['spec'].get('containers') or []
    if not containers or not containers[0].get('name'):
        raise ValueError('Deployment requires a named container')
    return containers[0]['name']


def _core_backend_manifest():
    return _get_root() / 'offloader' / 'core-backend.yaml'


def verify_matching_pod_cpu_requests(dynamic_manifest, *, anchor_manifest=None, core_manifest=None):
    """Verify dynamic, anchor, and core Deployments declare identical Pod CPU requests."""
    dyn_dep = _workload_deployment(manifest_path=dynamic_manifest)
    expected = _pod_cpu_request_cores(dyn_dep)
    checks = []
    if anchor_manifest is not None:
        anchor_docs = [
            d for d in yaml.safe_load_all(Path(anchor_manifest).read_text())
            if d and d.get('kind') == 'Deployment'
        ]
        if not anchor_docs:
            raise ValueError(f'no anchor Deployments found in {anchor_manifest}')
        for doc in anchor_docs:
            checks.append((doc['metadata']['name'], _pod_cpu_request_cores(doc)))
    if core_manifest is not None:
        core_dep = _workload_deployment(manifest_path=core_manifest)
        checks.append((core_dep['metadata']['name'], _pod_cpu_request_cores(core_dep)))
    mismatches = [
        f'{name} ({req} cores != {expected} cores)'
        for name, req in checks
        if req != expected
    ]
    if mismatches:
        raise ValueError(
            f'Pod CPU request mismatch against {dyn_dep["metadata"]["name"]} ({expected} cores): '
            + ', '.join(mismatches)
        )
    return expected


def _baseline_cpu_metric_name(n_fixed):
    if type(n_fixed) is not int or n_fixed < 0:
        raise ValueError('n_fixed must be a non-negative integer')
    return f'mec_baseline_cpu_dynamic_replicas_fixed{n_fixed}'


def _baseline_cpu_adapter_rule(
    n_fixed,
    *,
    cpu_hpa_target=DEFAULT_CPU_HPA_TARGET,
    pod_request_cores=Decimal('0.5'),
    namespace=NAMESPACE,
    container='pedestrian-detection',
    pod_prefix='mec-placement-pedestrian-detection',
    window_seconds=DEFAULT_BASELINE_CPU_RATE_WINDOW_SECONDS,
):
    if type(n_fixed) is not int or n_fixed < 0:
        raise ValueError('n_fixed must be a non-negative integer')
    if type(cpu_hpa_target) is not int or cpu_hpa_target < 1:
        raise ValueError('cpu_hpa_target must be a positive integer percent of CPU requests')
    pod_req = Decimal(str(pod_request_cores))
    if pod_req <= 0:
        raise ValueError('pod_request_cores must be positive')
    if type(window_seconds) is not int or window_seconds < 1:
        raise ValueError('window_seconds must be a positive integer')
    cpu_target = Decimal(str(cpu_hpa_target)) / Decimal('100')
    metric_name = _baseline_cpu_metric_name(n_fixed)
    expr = (
        f'clamp_min((sum(rate(container_cpu_usage_seconds_total{{'
        f'job="kubelet-cadvisor",namespace="{namespace}",container="{container}",'
        f'pod=~"{pod_prefix}.*"}}[{window_seconds}s])) or vector(0)) '
        f'/ ({_format_decimal(cpu_target)} * {_format_decimal(pod_req)}) - {n_fixed}, 0)'
    )
    return {
        'seriesQuery': 'up{job="kubelet-cadvisor"}',
        'resources': {'namespaced': False},
        'name': {'matches': '^up$', 'as': metric_name},
        'metricsQuery': expr,
    }


def _floor_adapter_config(floor_ms, configmap=None, unit='seconds', asymmetric_target_ms=None,
                          baseline_cpu_rules=None):
    """Append a latency-scaling rule, preserving the raw metric and unrelated rules."""
    if unit not in ('seconds', 'milliseconds'):
        raise ValueError(f'unsupported latency metric unit: {unit}')
    has_latency_rule = bool(floor_ms) or unit == 'milliseconds'
    if type(floor_ms) is not int or floor_ms < 0 or (not has_latency_rule and not baseline_cpu_rules):
        raise ValueError('adapter floor must be a positive whole number of milliseconds')
    if configmap is None:
        docs = yaml.safe_load_all((_get_root() / 'telemetry/05-prometheus-adapter.yaml').read_text())
        configmap = next(d for d in docs if d and d.get('kind') == 'ConfigMap'
                         and d['metadata']['name'] == 'prometheus-adapter-config')
    # Persist only declarative fields, not live resource versions or managed fields.
    result = {'apiVersion': 'v1', 'kind': 'ConfigMap',
              'metadata': {'name': 'prometheus-adapter-config', 'namespace': PLACEMENT_NAMESPACE},
              'data': dict(configmap['data'])}
    config = yaml.safe_load(result['data']['config.yaml'])
    rules = config.setdefault('externalRules', [])
    if has_latency_rule:
        name = _latency_metric_name(floor_ms, unit=unit, asymmetric_target_ms=asymmetric_target_ms)
        rules[:] = [
            r for r in rules
            if r.get('name', {}).get('as') != name
            and (asymmetric_target_ms is not None or not str(r.get('name', {}).get('as') or '').startswith('ue_p90_asymmetric_'))
        ]
        base_expr = 'ue_request_attempt_duration_p90_seconds{job="ue-telemetry"}'
        expr = f'clamp_min({base_expr} - {floor_ms / 1000:g}, 0)' if floor_ms else base_expr
        if asymmetric_target_ms is not None:
            target = Decimal(str(asymmetric_target_ms))
            if not 0 < floor_ms < target:
                raise ValueError('asymmetric latency scaling requires 0 < floor < target')
            # Keep the expression continuous at the target and preserve the above-target ratio.
            expr = f'{base_expr} + ({floor_ms} / {target - floor_ms:f}) * clamp_min({base_expr} - {target / 1000:f}, 0)'
        if unit == 'milliseconds':
            expr = f'({expr}) * 1000'
        rules.append({
            'seriesQuery': base_expr,
            'resources': {'namespaced': False},
            'name': {'matches': '^ue_request_attempt_duration_p90_seconds$', 'as': name},
            'metricsQuery': expr,
        })
    if baseline_cpu_rules:
        rules[:] = [
            r for r in rules
            if not str(r.get('name', {}).get('as') or '').startswith('mec_baseline_cpu_dynamic_replicas_fixed')
        ]
        rules.extend(deepcopy(baseline_cpu_rules))
    result['data']['config.yaml'] = yaml.safe_dump(config, sort_keys=False)
    return result


def _decimal_to_number(value):
    return int(value) if value == value.to_integral_value() else float(value)


def _metric_unit_for_target(target_dec):
    # Kubernetes HPA reads external metric targets via Quantity.MilliValue(), which
    # rounds fractional milli-quantities (e.g. 142.5m -> 143m). Use a millisecond-unit
    # metric whenever the target has sub-millisecond precision so Quantity.MilliValue()
    # preserves 0.001 ms resolution.
    return 'seconds' if target_dec == target_dec.to_integral_value() else 'milliseconds'


def _format_hpa_target(value_ms, unit='seconds'):
    dec = Decimal(str(value_ms)).normalize()
    text = format(dec, 'f')
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    return f'{text}m' if unit == 'seconds' else text


def _render_hpa(template_name, output_path, hpa_name, app_name, min_replicas, max_replicas, scale_down_seconds,
                slo_ms=None, staircase=False, latency_floor_ms=0, rt_target_factor=AUTOSCALER_CONFIG['rt_target_factor'],
                scale_down_stabilization_seconds=None, scale_down_pods=None, scale_down_period_seconds=None, cpu_hpa_target=DEFAULT_CPU_HPA_TARGET, asymmetric_latency=False,
                external_metric_name=None, external_average_value=None):
    hpa = yaml.safe_load((_get_root() / template_name).read_text())
    hpa['metadata']['name'] = hpa_name
    hpa['spec']['scaleTargetRef']['name'] = app_name
    hpa['spec'].update(minReplicas=min_replicas, maxReplicas=max_replicas)
    for metric in hpa['spec']['metrics']:
        if metric.get('resource', {}).get('name') == 'cpu':
            metric['resource']['target']['averageUtilization'] = cpu_hpa_target
    default_down_pods = AUTOSCALER_CONFIG['scale_down_pods']
    default_down_period = AUTOSCALER_CONFIG['scale_down_period_seconds']
    if staircase:
        # Matched damping for both metrics, independent of latency signal choice. Only the scale-down
        # stabilization window is set; scale-up and the scale-down rate keep the Kubernetes defaults
        # unless a rate limit is requested explicitly.
        stab = (AUTOSCALER_CONFIG['scale_down_stabilization_seconds']
                if scale_down_stabilization_seconds is None else int(scale_down_stabilization_seconds))
        scale_down = {'stabilizationWindowSeconds': stab}
        if scale_down_pods is not None or scale_down_period_seconds is not None:
            pods = default_down_pods if scale_down_pods is None else int(scale_down_pods)
            period = default_down_period if scale_down_period_seconds is None else int(scale_down_period_seconds)
            scale_down['policies'] = [{'type': 'Pods', 'value': pods, 'periodSeconds': period}]
        hpa['spec']['behavior'] = {
            'scaleUp': {'stabilizationWindowSeconds': AUTOSCALER_CONFIG['scale_up_stabilization_seconds']},
            'scaleDown': scale_down,
        }
    else:
        stab = int(scale_down_seconds) if scale_down_stabilization_seconds is None else int(scale_down_stabilization_seconds)
        hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] = stab
        if scale_down_pods is not None or scale_down_period_seconds is not None:
            pods = default_down_pods if scale_down_pods is None else int(scale_down_pods)
            period = default_down_period if scale_down_period_seconds is None else int(scale_down_period_seconds)
            hpa['spec']['behavior']['scaleDown']['policies'] = [
                {'type': 'Pods', 'value': pods, 'periodSeconds': period}
            ]
    if external_metric_name is not None:
        hpa['spec']['metrics'] = [{
            'type': 'External',
            'external': {
                'metric': {'name': external_metric_name},
                'target': {
                    'type': 'AverageValue',
                    'averageValue': str(external_average_value if external_average_value is not None else '1'),
                },
            },
        }]
    elif slo_ms is not None:
        metric = hpa['spec']['metrics'][0]['external']
        internal_target_dec = Decimal(str(slo_ms)) * Decimal(str(rt_target_factor))
        target_dec = internal_target_dec - latency_floor_ms if latency_floor_ms and not asymmetric_latency else internal_target_dec
        unit = _metric_unit_for_target(target_dec)
        metric['metric']['name'] = _latency_metric_name(
            latency_floor_ms, unit=unit, asymmetric_target_ms=internal_target_dec if asymmetric_latency else None)
        metric['target']['value'] = _format_hpa_target(target_dec, unit=unit)
    with output_path.open('x') as stream:
        yaml.safe_dump(hpa, stream, sort_keys=False)
    return output_path


def _render_incluster_placement_configmap(config, manifest_path):
    """Build the placement-controller-config ConfigMap for the in-cluster Go scheduler/descheduler."""
    # Omit dead Python-controller keys; the Go controller reads the rest as-is.
    incluster_cfg = {k: v for k, v in config.items() if k not in _GO_PLACEMENT_OMIT_KEYS}
    incluster_cfg['manifest'] = '/etc/placement/manifest.yaml'
    incluster_cfg['namespace'] = NAMESPACE
    incluster_cfg['prometheus_url'] = PLACEMENT_PROMETHEUS_URL
    incluster_cfg['mode'] = 'mobility-aware'
    incluster_cfg['mutate'] = True
    return {
        'apiVersion': 'v1',
        'kind': 'ConfigMap',
        'metadata': {
            'name': 'placement-controller-config',
            'namespace': PLACEMENT_NAMESPACE,
        },
        'data': {
            'config.yaml': yaml.safe_dump(incluster_cfg, sort_keys=False),
            'manifest.yaml': Path(manifest_path).read_text(),
        },
    }


def _parse_cpu_millicores(raw):
    text = str(raw).strip()
    if not text:
        raise ValueError('empty CPU quantity')
    if text.endswith('n'):
        val = float(text[:-1]) / 1e6
    elif text.endswith('u'):
        val = float(text[:-1]) / 1e3
    elif text.endswith('m'):
        val = float(text[:-1])
    else:
        val = float(text) * 1000.0
    if not math.isfinite(val) or val < 0:
        raise ValueError(f'invalid CPU quantity: {raw!r}')
    return val


def _parse_memory_bytes(raw):
    text = str(raw).strip()
    if not text:
        raise ValueError('empty memory quantity')
    units = {
        'Ki': 1024, 'Mi': 1024 ** 2, 'Gi': 1024 ** 3, 'Ti': 1024 ** 4,
        'Pi': 1024 ** 5, 'Ei': 1024 ** 6,
        'K': 1000, 'M': 1000 ** 2, 'G': 1000 ** 3, 'T': 1000 ** 4,
        'P': 1000 ** 5, 'E': 1000 ** 6,
    }
    val = None
    for suffix in sorted(units, key=len, reverse=True):
        if text.endswith(suffix):
            val = float(text[:-len(suffix)]) * units[suffix]
            break
    if val is None:
        val = float(text)
    if not math.isfinite(val) or val < 0:
        raise ValueError(f'invalid memory quantity: {raw!r}')
    return val


def _container_requests(container):
    resources = container.get('resources') or {}
    if resources.get('claims'):
        raise ValueError(f"unsupported container resource claims in {container.get('name', '<container>')}")
    req = resources.get('requests') or {}
    lim = resources.get('limits') or {}
    cpu_raw = req.get('cpu', lim.get('cpu', 0))
    mem_raw = req.get('memory', lim.get('memory', 0))
    return _parse_cpu_millicores(cpu_raw), _parse_memory_bytes(mem_raw)


def _pod_effective_requests(spec):
    """Compute Kubernetes-effective Pod CPU (millicores) and memory (bytes) requests."""
    if spec.get('resourceClaims'):
        raise ValueError('unsupported Pod resourceClaims in capacity check')
    regular_cpu = 0.0
    regular_mem = 0.0
    for container in spec.get('containers') or []:
        c_cpu, c_mem = _container_requests(container)
        regular_cpu += c_cpu
        regular_mem += c_mem

    restartable_cpu = 0.0
    restartable_mem = 0.0
    max_init_cpu = 0.0
    max_init_mem = 0.0
    for init_c in spec.get('initContainers') or []:
        i_cpu, i_mem = _container_requests(init_c)
        restart_policy = init_c.get('restartPolicy')
        if restart_policy == 'Always':
            restartable_cpu += i_cpu
            restartable_mem += i_mem
            max_init_cpu = max(max_init_cpu, restartable_cpu)
            max_init_mem = max(max_init_mem, restartable_mem)
        elif restart_policy is None:
            max_init_cpu = max(max_init_cpu, restartable_cpu + i_cpu)
            max_init_mem = max(max_init_mem, restartable_mem + i_mem)
        else:
            raise ValueError(f'unsupported initContainer restartPolicy: {restart_policy!r}')

    effective_cpu = max(regular_cpu + restartable_cpu, max_init_cpu)
    effective_mem = max(regular_mem + restartable_mem, max_init_mem)

    pod_resources = spec.get('resources') or {}
    if pod_resources:
        if pod_resources.get('claims'):
            raise ValueError('unsupported Pod-level resource claims in capacity check')
        unsupported_keys = set(pod_resources) - {'requests', 'limits'}
        if unsupported_keys:
            raise ValueError(f'unsupported Pod-level resources keys: {sorted(unsupported_keys)}')
        pod_req = pod_resources.get('requests') or {}
        pod_lim = pod_resources.get('limits') or {}
        if 'cpu' in pod_req or 'cpu' in pod_lim:
            effective_cpu = max(effective_cpu, _parse_cpu_millicores(pod_req.get('cpu', pod_lim.get('cpu'))))
        if 'memory' in pod_req or 'memory' in pod_lim:
            effective_mem = max(effective_mem, _parse_memory_bytes(pod_req.get('memory', pod_lim.get('memory'))))

    overhead = spec.get('overhead') or {}
    unsupported_overhead = set(overhead) - {'cpu', 'memory'}
    if unsupported_overhead:
        raise ValueError(f'unsupported Pod overhead resources: {sorted(unsupported_overhead)}')
    if 'cpu' in overhead:
        effective_cpu += _parse_cpu_millicores(overhead['cpu'])
    if 'memory' in overhead:
        effective_mem += _parse_memory_bytes(overhead['memory'])

    return effective_cpu, effective_mem


def _managed_replica_bounds(min_replicas, max_replicas, *, edge_only=False, site_nodes=SITE_NODES):
    anchors = len(_site_anchor_nodes(edge_only, site_nodes))
    minimum = max(1, min_replicas - anchors)
    maximum = max_replicas - anchors
    if maximum < minimum:
        raise ValueError('--max-replicas must fit all site anchors and at least one managed replica')
    return minimum, maximum


def _supports_latency_floor(comparison, staircase, site_anchors):
    return (comparison == 'full' or (comparison == 'scaling' and staircase)
            or (comparison == 'placement' and site_anchors))


def _method_configs(root, method, slo_ms, max_replicas, debug=False, resource_weights=None, locality_weight=None, workload='pedestrian-detection', comparison='full', autoscaler='hpa', deschedule_interval=None, fixed_replicas=None, staircase=False, latency_floor_ms=0, rt_target_factor=1.0, min_replicas=None, scale_down_stabilization_seconds=None, scale_down_pods=None, scale_down_period_seconds=None, cpu_hpa_target=DEFAULT_CPU_HPA_TARGET, pod_share_scoring=False, site_anchors=False, edge_only=False, no_hpa=False, asymmetric_latency=False, site_nodes=SITE_NODES, *, base_manifest=None, vertical_budget=None, proposed_scale_down_period_seconds=None):
    """Persist the exact matched replica budget and manifests used by a method."""
    if type(cpu_hpa_target) is not int or cpu_hpa_target < 1:
        raise ValueError('cpu_hpa_target must be a positive integer percent of CPU requests')
    if comparison not in COMPARISONS:
        raise ValueError(f'comparison must be one of {COMPARISONS}')
    if autoscaler not in AUTOSCALERS:
        raise ValueError(f'autoscaler must be one of {AUTOSCALERS}')
    if (isinstance(rt_target_factor, bool) or not isinstance(rt_target_factor, (int, float))
            or not math.isfinite(rt_target_factor) or not 0 < rt_target_factor <= 1):
        raise ValueError('rt_target_factor must be finite and in (0, 1]')
    if rt_target_factor != 1.0 and autoscaler != 'hpa':
        raise ValueError('rt_target_factor != 1.0 requires native HPA (--autoscaler hpa)')
    if rt_target_factor != 1.0 and comparison == 'placement':
        raise ValueError('rt_target_factor != 1.0 is not supported with --comparison placement (baseline and proposed must share the same latency autoscaler)')
    internal_target_dec = Decimal(str(slo_ms)) * Decimal(str(rt_target_factor))
    if latency_floor_ms and (not _supports_latency_floor(comparison, staircase, site_anchors)
                            or autoscaler != 'hpa'
                            or type(latency_floor_ms) is not int or latency_floor_ms <= 0
                            or internal_target_dec <= Decimal(str(latency_floor_ms))):
        raise ValueError('latency floor requires full, placement with site anchors, or staircase scaling with native HPA and 0 < floor < internal RT target')
    config = yaml.safe_load((_get_root() / 'config.yaml').read_text())
    eligible = tuple(_site_anchor_nodes(edge_only, site_nodes).values())
    if vertical_budget is not None and vertical_budget.core_fallback_replicas:
        eligible += ('core-5g',)
    config['eligible_nodes'] = list(eligible)
    config['zero_locality_nodes'] = [node for node in config.get('zero_locality_nodes', []) if node in eligible]
    # Keep queries scoped to the same topology as the workload and Go config.
    node_filter = (
        f'node_cpu_seconds_total{{job="node-exporter",mode="idle",node=~"{"|".join(eligible)}"}}'
    )
    if site_nodes == LOCAL_SITE_NODES:
        config['cpu_utilization_query'] = f'1 - avg by (node) (rate({node_filter}[1m]))'
    else:
        cpu_query = config['cpu_utilization_query'].replace(
            '|'.join(LOCAL_SITE_NODES.values()), '|'.join(SITE_NODES.values())
        )
        if edge_only:
            cpu_query = f'({cpu_query}) and on(node) {node_filter}'
        config['cpu_utilization_query'] = cpu_query
    config['mobility_query'] = _placement_query(eligible)
    if vertical_budget is not None:
        effective_min_replicas = vertical_budget.dynamic_min if no_hpa else max(1, vertical_budget.dynamic_min)
        max_replicas = vertical_budget.dynamic_max
        if max_replicas < effective_min_replicas:
            raise ValueError('vertical HPA requires room for at least one managed replica')
    else:
        if min_replicas is not None:
            if type(min_replicas) is not int or min_replicas < 1:
                raise ValueError('min replicas must be a positive integer')
            if fixed_replicas is not None:
                raise ValueError('use min_replicas without fixed_replicas')
        if fixed_replicas is not None:
            if type(fixed_replicas) is not int or fixed_replicas < 1:
                raise ValueError('fixed replicas must be a positive integer')
            effective_min_replicas = max_replicas = fixed_replicas
        elif min_replicas is not None:
            effective_min_replicas = min_replicas
        elif comparison == 'scaling':
            effective_min_replicas = SCALER_MIN_REPLICAS
        else:
            effective_min_replicas = config['min_replicas']
        if type(max_replicas) is not int or max_replicas < effective_min_replicas:
            raise ValueError('max replicas must be an integer at least min_replicas')
    config['pod_share_scoring'] = bool(pod_share_scoring and (method == 'proposed' or site_anchors))
    if site_anchors and vertical_budget is None:
        effective_min_replicas, max_replicas = _managed_replica_bounds(
            effective_min_replicas, max_replicas, edge_only=edge_only, site_nodes=site_nodes)
    config['min_replicas'] = effective_min_replicas
    if resource_weights is not None:
        config['resource_weights'] = validate_resource_weights(resource_weights)
    if locality_weight is not None:
        if not math.isfinite(locality_weight) or not 0 <= locality_weight <= 1:
            raise ValueError('locality weight must be between 0 and 1')
        config['locality_weight'] = locality_weight
    if base_manifest is None:
        base_manifest = root / 'workload.yaml'
    source_text = base_manifest.read_text() if base_manifest.exists() else workload_manifest(workload).read_text()
    documents = list(yaml.safe_load_all(source_text))
    deployments = [item for item in documents if item and item.get('kind') == 'Deployment']
    if len(deployments) != 1:
        raise ValueError('application manifest requires exactly one Deployment')
    scheduler_name = ('default-scheduler' if (method == 'baseline' or comparison == 'scaling')
                      else config.get('scheduler_name', 'mec-placement-scheduler'))
    deployments[0]['spec']['template']['spec']['schedulerName'] = scheduler_name
    if comparison == 'scaling':
        deployments[0]['spec']['replicas'] = effective_min_replicas
        _restrict_workload_nodes(deployments[0], [SCALER_NODE])
    else:
        _restrict_workload_nodes(deployments[0], eligible)
    if vertical_budget is not None:
        deployments[0]['spec']['replicas'] = vertical_budget.dynamic_min
    elif site_anchors and fixed_replicas is None:
        deployments[0]['spec']['replicas'] = 0
    elif fixed_replicas is not None or min_replicas is not None:
        deployments[0]['spec']['replicas'] = effective_min_replicas
    method_manifest = root / f'{method}-workload.yaml'
    with method_manifest.open('x') as stream:
        yaml.safe_dump_all(documents, stream, sort_keys=False)
    if method == 'proposed':
        mode = 'mobility-aware'
        mutate = True
    else:
        mode = 'mobility-aware'
        mutate = False
    config.update(manifest=str(method_manifest), mode=mode, mutate=mutate,
                  slo_seconds=slo_ms / 1000, max_replicas=max_replicas)
    if debug:
        config.update(interval_seconds=15, cooldown_seconds=15,
                      scale_down_seconds=30, deschedule_interval_seconds=30)
        config['cpu_utilization_query'] = config['cpu_utilization_query'].replace('[2m]', '[1m]')
    if deschedule_interval is not None:
        if not math.isfinite(deschedule_interval) or deschedule_interval <= 0:
            raise ValueError('descheduler interval must be finite and positive')
        config['deschedule_interval_seconds'] = deschedule_interval
    config_path = root / f'{method}-controller.yaml'
    with config_path.open('x') as stream:
        yaml.safe_dump(config, stream, sort_keys=False)
    app_name = workload_identity(workload)
    hpa_path = None
    if no_hpa:
        return config, config_path, hpa_path
    if method == 'baseline' and comparison in ('full', 'scaling'):
        external_metric_name = None
        external_average_value = None
        if comparison == 'full' and site_anchors:
            n_fixed = (vertical_budget.fixed_replicas if vertical_budget is not None
                       else len(_site_anchor_nodes(edge_only, site_nodes)))
            external_metric_name = _baseline_cpu_metric_name(n_fixed)
            external_average_value = '1'
        hpa_path = _render_hpa(
            'autoscaler/hpa-baseline.yaml', root / 'baseline-hpa.yaml',
            app_name + '-baseline', app_name,
            config['min_replicas'], max_replicas, config['scale_down_seconds'], staircase=staircase,
            scale_down_stabilization_seconds=scale_down_stabilization_seconds,
            scale_down_pods=scale_down_pods,
            scale_down_period_seconds=scale_down_period_seconds,
            cpu_hpa_target=cpu_hpa_target,
            external_metric_name=external_metric_name,
            external_average_value=external_average_value,
        )
    elif autoscaler == 'hpa' and (method == 'proposed' or comparison == 'placement'):
        effective_factor = rt_target_factor if method == 'proposed' else 1.0
        # Joint suite design: rate-limit proposed only. Shared scale-down flags
        # can set identical limits when the proposed-only override is omitted.
        proposed_period = proposed_scale_down_period_seconds if method == 'proposed' else None
        hpa_path = _render_hpa(
            'autoscaler/hpa-proposed.yaml', root / f'{method}-hpa.yaml',
            f'{app_name}-{method}', app_name,
            config['min_replicas'], max_replicas, config['scale_down_seconds'], slo_ms=slo_ms, staircase=staircase,
            latency_floor_ms=latency_floor_ms, rt_target_factor=effective_factor, asymmetric_latency=asymmetric_latency,
            scale_down_stabilization_seconds=scale_down_stabilization_seconds,
            scale_down_pods=1 if proposed_period is not None else scale_down_pods,
            scale_down_period_seconds=proposed_period if proposed_period is not None else scale_down_period_seconds,
        )
    return config, config_path, hpa_path
