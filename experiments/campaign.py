"""Run the matched gNB3-hotspot campaign from core, including remote UEs and plots.

Orchestrates campaign execution (preflight, workload/HPA/placement setup, remote
UE traffic runner, artifact collection, and plotting) while re-exporting profile,
manifest, CLI, and vertical helpers from their dedicated submodules.
"""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import fcntl
import hashlib
import ipaddress
import math
import json
import os
from pathlib import Path
from dataclasses import replace
import random
import re
import shlex
import signal
import subprocess
import sys
import tarfile
import time
import uuid
from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import yaml
from autoscaler.config import load_autoscaler_config
from benchmark.client import parse_schedule
from offloader.budget import VerticalBudget
from utils.traffic_control import POOL_NAME, TrafficDelay
from utils.config import validate_resource_weights
from utils.experiment import DEFAULT_RPS_PER_UE

from experiments.profiles import (
    APP,
    ARM_SELECTIONS,
    AUTOSCALER_CONFIG,
    AUTOSCALERS,
    COMPARISONS,
    COMPARISON_NOTES,
    CORE_GNB1_SESSION,
    DEFAULT_CPU_HPA_TARGET,
    DEFAULT_DESCHEDULE_INTERVAL_S,
    DEFAULT_LATENCY_FLOOR_FRACTION,
    DEFAULT_LATENCY_FLOOR_MS,
    DEFAULT_MIN_REPLICAS,
    DEFAULT_PHASE_SECONDS,
    DEFAULT_SLO_MS,
    DEFAULT_TC_MS,
    FINAL_HOLD_SECONDS,
    FOUR_ARM_SPECS,
    LOCAL_GNB1_SESSION,
    LOCAL_GNB2_SESSION,
    LOCAL_GNB3_SESSION,
    LOCAL_SITE_NODES,
    MOBILITY9_SCHEDULE,
    MOBILITY_PHASE_SECONDS,
    NAMESPACE,
    PLACEMENT_ENGINES,
    PLACEMENT_NAMESPACE,
    PLACEMENT_PROMETHEUS_URL,
    PROFILES,
    ROOT,
    SCALER_BALANCED_SCHEDULE,
    SCALER_DEFAULT_MAX_REPLICAS,
    SCALER_MIN_REPLICAS,
    SCALER_NODE,
    SCALER_PHASE_SECONDS,
    SCALER_SCENARIOS,
    SCALER_STAIRCASE_MAX_REPLICAS,
    SCALER_STAIRCASE_SCHEDULE,
    SITE_NODES,
    SPILL_READINESS_PROBE,
    UPF_PLACEMENT_QUERY,
    VERTICAL_JOINT_SCHEDULE,
    VERTICAL_MOBILITY_SCHEDULE,
    VERTICAL_MODES,
    VERTICAL_SMOKE_SCHEDULE,
    WORKLOADS,
    _GO_PLACEMENT_OMIT_KEYS,
    _VERTICAL_COMMON,
    _VERTICAL_FULL,
)
from experiments.manifests import (
    DEFAULT_BASELINE_CPU_RATE_WINDOW_SECONDS,
    _baseline_cpu_adapter_rule,
    _baseline_cpu_metric_name,
    _campaign_manifest,
    _container_requests,
    _decimal_to_number,
    _floor_adapter_config,
    _format_hpa_target,
    _latency_metric_name,
    _managed_replica_bounds,
    _method_configs,
    _metric_unit_for_target,
    _parse_cpu_millicores,
    _parse_memory_bytes,
    _placement_query,
    _pod_effective_requests,
    _render_hpa,
    _render_incluster_placement_configmap,
    _restrict_workload_nodes,
    _site_anchor_nodes,
    _site_nodes,
    _supports_latency_floor,
    _workload_http_threads,
    check_service_routing,
    selected_workload,
    _core_backend_manifest,
    _workload_container_name,
    _workload_cpu_request_cores,
    verify_matching_pod_cpu_requests,
    workload_identity,
    workload_manifest,
)
from experiments.cli import _build_parser, _parse_args
from experiments.vertical import (
    _default_envoy_preflight_probe,
    _load_offloader_token,
    _offloader_request,
    cleanup_vertical_state,
    fetch_offloader_artifacts,
    get_core_pool_replicas,
    prepare_vertical_arm,
    reset_offloader,
    scale_core_pool,
    set_offloader_mode,
    verify_vertical_arm_readiness,
    vertical_budget_from_settings,
    wait_core_pool_scaled,
    wait_offloader_core_endpoints,
)


class RunnerProgress:
    """Relay complete remote log lines while retaining the current phase."""

    def __init__(self, path, method):
        self.path, self.method = path, method
        self.offset = 0
        self.phase = 'starting UE runner'
        self.traffic_started = False
        self.last_status = None

    def poll(self, final=False):
        with self.path.open() as stream:
            stream.seek(self.offset)
            while True:
                before = stream.tell()
                line = stream.readline()
                if not line or (not final and not line.endswith('\n')):
                    self.offset = before
                    break
                self.offset = stream.tell()
                text = line.rstrip()
                if text.startswith(('[..]', '[RUN]', '[--]')):
                    self.phase = text
                if text.startswith('[RUN]'):
                    self.traffic_started = True
                print(f'{self.method}: {text}', flush=True)
        now = time.monotonic()
        if not final and (self.last_status is None or now - self.last_status >= 30):
            print(f'{self.method}: current progress: {self.phase}', flush=True)
            self.last_status = now


def command(args, **kwargs):
    print('+', shlex.join(map(str, args)), flush=True)
    return subprocess.run(list(map(str, args)), check=True, **kwargs)


def kube(*args, **kwargs):
    return command(['kubectl', *args], **kwargs)


def get(kind, name, namespace=NAMESPACE):
    return json.loads(kube('-n', namespace, 'get', kind, name, '-o', 'json', capture_output=True, text=True).stdout)


def remote(settings, args):
    return ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', settings['ue_host'],
            'cd ' + shlex.quote(settings['ue_project']) + ' && ' + shlex.join(args)]


def remote_settings(settings):
    local_gnb1 = _site_nodes(settings) == LOCAL_SITE_NODES
    dnn, subnet = LOCAL_GNB1_SESSION if local_gnb1 else CORE_GNB1_SESSION
    result = {k: settings[k] for k in ('inventory', 'nr_ue', 'token', 'image', 'telemetry_url')} | {
        'max_ues': max(p['workload_ue'] for p in parse_schedule(settings['schedule'])),
        'gnb1_dnn': dnn, 'gnb1_subnet': subnet,
        'rps_per_ue': float(settings.get('rps_per_ue', DEFAULT_RPS_PER_UE))}
    if local_gnb1:
        result['gnb2_dnn'], result['gnb2_subnet'] = LOCAL_GNB2_SESSION
        result['gnb3_dnn'], result['gnb3_subnet'] = LOCAL_GNB3_SESSION
    return result


def check_result_name(settings, name):
    """Reject collisions before active preflight, without creating either directory."""
    root = ROOT / 'results' / name
    try:
        root.lstat()
    except FileNotFoundError:
        pass
    else:
        raise FileExistsError(f'Campaign name already exists on core: {root}. '
                              'Archive it or use a new campaign name; UE results must also be absent.')
    script = ('import os,sys\n'
              'try: os.lstat(sys.argv[1])\n'
              'except FileNotFoundError: sys.exit(0)\n'
              'else: sys.exit(17)\n')
    args = remote(settings, ['.venv/bin/python', '-c', script, f'results/{name}'])
    # Keep SSH errors distinct from a positive remote existence check.
    result = subprocess.run(args, text=True, capture_output=True, check=False)
    if result.returncode == 17:
        raise FileExistsError(f'Campaign name already exists on UE host {settings["ue_host"]}: '
                              f'{settings["ue_project"]}/results/{name}. '
                              'Archive that directory (it may be root-owned) or use a new campaign name. '
                              'No connectivity probes or campaign traffic were started by this check.')
    if result.returncode:
        raise RuntimeError(f'Could not check result name on UE host {settings["ue_host"]} '
                           f'(exit {result.returncode}): {result.stderr.strip()}. '
                           'No result directory was created.')


def load(path):
    data = yaml.safe_load(Path(path).read_text())
    expected = {'ue_host', 'ue_project', 'inventory', 'nr_ue', 'token', 'image', 'site_node_ips',
                'telemetry_url', 'schedule', 'phase_duration'}
    optional = {
        'workload', 'radio_only_gnb1', 'envoy_site_ports', 'offloader_url',
        'offloader_token_file', 'core_backend_replicas', 'core_counts_toward_budget',
        'min_replicas', 'max_replicas',
    }
    if not isinstance(data, dict) or not expected.issubset(data) or (set(data) - expected - optional):
        raise ValueError('campaign configuration keys must be: ' + ', '.join(sorted(expected))
                         + ' (optional: ' + ', '.join(sorted(optional)) + ')')
    if 'workload' in data:
        workload_identity(data['workload'])
    if 'radio_only_gnb1' in data and not isinstance(data['radio_only_gnb1'], bool):
        raise ValueError('radio_only_gnb1 must be a boolean')
    if 'envoy_site_ports' in data:
        ports = data['envoy_site_ports']
        if (not isinstance(ports, dict) or set(ports) != {'gnb-1', 'gnb-2', 'gnb-3'}
                or any(isinstance(p, bool) or not isinstance(p, int) or not 1 <= p <= 65535 for p in ports.values())):
            raise ValueError('envoy_site_ports must map gnb-1, gnb-2, gnb-3 to TCP port numbers')
    if 'offloader_url' in data and (not isinstance(data['offloader_url'], str) or not data['offloader_url'].strip()):
        raise ValueError('offloader_url must be a nonempty string')
    if 'offloader_token_file' in data and (not isinstance(data['offloader_token_file'], str) or not data['offloader_token_file'].strip()):
        raise ValueError('offloader_token_file must be a nonempty string')
    for int_key in ('core_backend_replicas', 'min_replicas', 'max_replicas'):
        if int_key in data:
            val = data[int_key]
            if isinstance(val, bool) or not isinstance(val, int) or val < 0:
                raise ValueError(f'{int_key} must be a nonnegative integer')
    if 'core_counts_toward_budget' in data and not isinstance(data['core_counts_toward_budget'], bool):
        raise ValueError('core_counts_toward_budget must be a boolean')
    if not isinstance(data['phase_duration'], (int, float)) or data['phase_duration'] < 30:
        raise ValueError('phase_duration must be at least 30 seconds for telemetry checks')
    parse_schedule(data['schedule'])
    for key in expected - {'phase_duration', 'site_node_ips'}:
        if not isinstance(data[key], str) or not data[key].strip():
            raise ValueError(f'{key} must be a nonempty string')
    sites = data['site_node_ips']
    if not isinstance(sites, dict) or set(sites) != {'gnb-1', 'gnb-2', 'gnb-3'}:
        raise ValueError('site_node_ips must map gnb-1, gnb-2 and gnb-3 to node IPv4 addresses')
    for address in sites.values():
        try:
            if not isinstance(address, str) or ipaddress.ip_address(address).version != 4:
                raise ValueError
        except ValueError:
            raise ValueError('site_node_ips must contain valid IPv4 addresses') from None
    return data


def _pod_nodes(namespace):
    """Return formatted '<pod>@<node> (<phase>)' entries for pods in namespace."""
    pods = json.loads(kube('-n', namespace, 'get', 'pods', '-o', 'json', capture_output=True, text=True).stdout)
    entries = []
    for pod in pods.get('items', []):
        name = pod.get('metadata', {}).get('name', '<unknown>')
        node = pod.get('spec', {}).get('nodeName') or '<unscheduled>'
        phase = pod.get('status', {}).get('phase', 'Unknown')
        entries.append(f'{name}@{node} ({phase})')
    return entries


def prerequisites(settings):
    import matplotlib
    from utils.config import load_settings
    config = load_settings(ROOT / 'config.yaml')
    workload = selected_workload(settings)
    config = replace(config, manifest=workload_manifest(workload))
    kube('get', '--raw=/readyz')
    nodes = json.loads(kube('get', 'nodes', '-o', 'json', capture_output=True, text=True).stdout)
    by_name = {node['metadata']['name']: node for node in nodes['items']}
    for site, node_name in _site_nodes(settings).items():
        addresses = {item['address'] for item in by_name.get(node_name, {}).get('status', {}).get('addresses', [])
                     if item.get('type') == 'InternalIP'}
        if settings['site_node_ips'][site] not in addresses:
            raise RuntimeError(f'{site} NodePort IP does not match node {node_name} InternalIP')
    collector = get('deployment', 'ue-telemetry', 'placement')
    if collector['spec']['replicas'] != 1 or collector.get('status', {}).get('readyReplicas') != 1:
        raise RuntimeError('placement/ue-telemetry must have exactly one ready replica')
    with urlopen(settings['telemetry_url'].rsplit('/', 1)[0] + '/health', timeout=10) as response:
        if response.status != 200:
            raise RuntimeError('collector health check failed')
    compute_nodes = tuple(_site_nodes(settings).values())
    values = query(config.prometheus_url, _placement_query(compute_nodes))
    if sorted(row['metric'].get('node', '') for row in values) != sorted(compute_nodes):
        raise RuntimeError('UPF placement telemetry must return exactly one series per application node')
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    other = command(remote(settings, ['git', 'rev-parse', 'HEAD']), capture_output=True, text=True).stdout.strip()
    if revision != other:
        raise RuntimeError('core and UE project revisions differ; git pull --ff-only on both first')
    command(remote(settings, ['sudo', '-n', '.venv/bin/python', '-m', 'experiments.campaign_ue', 'check']),
            input=json.dumps(remote_settings(settings)), text=True)
    from utils.preflight import collector_status
    collector_status(config)
    verify_incluster_components('none' if settings.get('no_hpa', False) else settings.get('_autoscaler', 'hpa'),
                                settings.get('_placement_engine', 'go'),
                                settings.get('_comparison', 'full'))
    for manifest in (workload_manifest(workload), ROOT / 'autoscaler/hpa-baseline.yaml'):
        docs = [d for d in yaml.safe_load_all(Path(manifest).read_text()) if d]
        if any(d['metadata'].get('namespace') != NAMESPACE for d in docs):
            raise ValueError(f'{manifest} must use the {NAMESPACE} namespace')
    connectivity_probe(settings)
    print('Prerequisites passed, including UE inference paths and reattachment on all three gNBs.', flush=True)


def connectivity_probe(settings):
    """Check a temporary app/NodePort without touching campaign or shared workloads."""
    probe_id = 'mec-preflight-' + uuid.uuid4().hex[:12]
    documents = [d for d in yaml.safe_load_all(workload_manifest(selected_workload(settings)).read_text()) if d]
    app = workload_identity(selected_workload(settings))
    for document in documents:
        document['metadata']['namespace'] = probe_id
        if document.get('kind') == 'Deployment':
            _restrict_workload_nodes(document, tuple(_site_anchor_nodes(settings.get('edge_only', False), _site_nodes(settings)).values()))
    created = False
    remote_started = False
    try:
        kube('create', 'namespace', probe_id)
        created = True
        print(f'[CHECK] Temporary connectivity workload: {probe_id}', flush=True)
        kube('create', '-f', '-', input=yaml.safe_dump_all(documents), text=True)
        kube('-n', probe_id, 'rollout', 'status', 'deployment/' + app, '--timeout=180s')
        print('[CHECK] Probe pod(s): ' + ', '.join(_pod_nodes(probe_id)), flush=True)
        service = get('service', app, probe_id)
        check_service_routing(service)
        port = service['spec']['ports'][0]['nodePort']
        payload = remote_settings(settings) | {'probe_id': probe_id, 'site_urls': {
            site: f'http://{ip}:{port}/detect' for site, ip in settings['site_node_ips'].items()}}
        remote_started = True
        command(remote(settings, ['sudo', '-n', '.venv/bin/python', '-u', '-m', 'experiments.campaign_ue', 'probe']),
                input=json.dumps(payload), text=True)
    finally:
        try:
            if remote_started:
                command(remote(settings, ['sudo', '-n', '.venv/bin/python', '-m', 'experiments.campaign_ue',
                                          'stop', '--campaign', probe_id, '--method', 'baseline']))
        finally:
            if created:
                kube('delete', 'namespace', probe_id, '--wait=true', '--timeout=180s')
    print(f'[OK] Connectivity probe complete; UE diagnostics: results/{probe_id}/ on UE host', flush=True)


def reset(workload='pedestrian-detection'):
    """Delete only the selected experiment app and its HPAs."""
    app = workload_identity(workload)
    kube('-n', NAMESPACE, 'delete', 'hpa/' + app + '-baseline',
         'hpa/' + app + '-proposed',
         'deployment/' + app, 'service/' + app,
         *(f'deployment/{app}-anchor-{site}' for site in SITE_NODES), '--ignore-not-found=true',
         '--cascade=foreground', '--wait=true', '--timeout=180s')
    pods = json.loads(kube('-n', NAMESPACE, 'get', 'pods', '-l', 'app=' + app,
                          '-o', 'json', capture_output=True, text=True).stdout)['items']
    if pods:
        kube('-n', NAMESPACE, 'wait', '--for=delete', 'pod', '-l', 'app=' + app,
             '--timeout=180s')


def set_prefer_close_zones(nodes):
    """Save node zones and label each node as its own zone for PreferClose."""
    original = {}
    try:
        for name in nodes:
            node = json.loads(kube('get', 'node', name, '-o', 'json', capture_output=True, text=True).stdout)
            original[name] = (node.get('metadata', {}).get('labels') or {}).get('topology.kubernetes.io/zone')
            kube('label', 'node', name, 'topology.kubernetes.io/zone=' + name, '--overwrite')
    except Exception:
        restore_prefer_close_zones(original)
        raise
    return original


def restore_prefer_close_zones(original):
    for name, zone in original.items():
        label = 'topology.kubernetes.io/zone-' if zone is None else 'topology.kubernetes.io/zone=' + zone
        kube('label', 'node', name, label, '--overwrite')


def query(url, expression):
    with urlopen(url.rstrip('/') + '/api/v1/query?' + urlencode({'query': expression}), timeout=10) as response:
        result = json.load(response)
    if result.get('status') != 'success':
        raise RuntimeError('Prometheus query failed')
    return result['data']['result']


def active_telemetry(url):
    values = query(url, 'up{job="ue-telemetry"}')
    if len(values) != 1 or float(values[0]['value'][1]) != 1:
        return False
    values = query(url, 'ue_request_window_attempts{job="ue-telemetry"}')
    if len(values) != 1 or float(values[0]['value'][1]) <= 0:
        return False
    values = query(url, 'ue_request_attempt_duration_p90_seconds{job="ue-telemetry"}')
    return len(values) == 1 and 0 < float(values[0]['value'][1]) < float('inf')


def stop_process(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=90)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def transfer(settings, campaign, method, output):
    source = f'results/{campaign}'
    archive = output / 'ue-results.tar.gz'
    with archive.open('xb') as stream:
        command(remote(settings, ['sudo', '-n', 'tar', '-C', source, '-czf', '-',
                                  method, f'{method}-runner.log']), stdout=stream)
    # This archive is produced by our runner, but never trust path traversal or links.
    with tarfile.open(archive) as stream:
        for member in stream.getmembers():
            if member.name.startswith('/') or '..' in Path(member.name).parts or member.issym() or member.islnk():
                raise ValueError('unsafe result archive member')
            if not ((member.name == method and member.isdir()) or
                    member.name == f'{method}-runner.log' or member.name.startswith(method + '/')):
                raise ValueError('unexpected result archive member')
            if member.isfile() and (output.parent / member.name).exists():
                raise FileExistsError(output.parent / member.name)
        stream.extractall(output.parent, filter='data')
    raw = output / 'requests.jsonl'
    local_hash = hashlib.sha256(raw.read_bytes()).hexdigest()
    source_hash = command(remote(settings, ['sudo', '-n', 'sha256sum', f'{source}/{method}/requests.jsonl']),
                          capture_output=True, text=True).stdout.split()[0]
    if local_hash != source_hash:
        raise RuntimeError('request checksum mismatch after transfer')
    print(f'{method}: raw request checksum verified {local_hash}', flush=True)


def _prepare_floor_adapter(root, floor_ms, unit='seconds', asymmetric_target_ms=None,
                           baseline_cpu_rules=None):
    """Install the selected rule before either method; no workload or traffic here."""
    current = get('configmap', 'prometheus-adapter-config', PLACEMENT_NAMESPACE)
    desired = _floor_adapter_config(
        floor_ms, current, unit=unit, asymmetric_target_ms=asymmetric_target_ms,
        baseline_cpu_rules=baseline_cpu_rules,
    )
    filename = 'floor-adapter-configmap.yaml' if floor_ms else 'latency-adapter-configmap.yaml'
    path = root / filename
    path.write_text(yaml.safe_dump(desired, sort_keys=False))
    (root / 'adapter-before.json').write_text(json.dumps(current, indent=2))
    current_cfg = yaml.safe_load(current['data']['config.yaml'])
    desired_cfg = yaml.safe_load(desired['data']['config.yaml'])
    changed = current_cfg != desired_cfg
    if changed:
        # Merge just this data key: preserve live metadata, binaryData and other keys.
        patch = json.dumps({'metadata': {'resourceVersion': current['metadata']['resourceVersion']},
                            'data': {'config.yaml': desired['data']['config.yaml']}})
        kube('-n', PLACEMENT_NAMESPACE, 'patch', 'configmap', 'prometheus-adapter-config',
             '--type=merge', '-p', patch)
    try:
        # Also reload when the rule already exists: a prior ConfigMap edit may
        # not yet have reached a running adapter process.
        kube('-n', PLACEMENT_NAMESPACE, 'rollout', 'restart', 'deployment/prometheus-adapter')
        kube('-n', PLACEMENT_NAMESPACE, 'rollout', 'status', 'deployment/prometheus-adapter', '--timeout=120s')
    except (OSError, subprocess.SubprocessError):
        if changed:
            # Restore the prior query config if the adapter cannot start. Abort
            # the campaign even if the rollback succeeds; no traffic has begun.
            kube('-n', PLACEMENT_NAMESPACE, 'patch', 'configmap', 'prometheus-adapter-config',
                 '--type=merge', '-p', json.dumps({'data': {'config.yaml': current['data']['config.yaml']}}))
            kube('-n', PLACEMENT_NAMESPACE, 'rollout', 'restart', 'deployment/prometheus-adapter')
            kube('-n', PLACEMENT_NAMESPACE, 'rollout', 'status', 'deployment/prometheus-adapter', '--timeout=120s')
        raise


def _local_git_provenance():
    try:
        rev = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        dirty = bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip())
        return {'revision': rev, 'dirty': dirty}
    except (OSError, subprocess.SubprocessError):
        return None


def _workload_image_ref(workload):
    docs = [d for d in yaml.safe_load_all(workload_manifest(workload).read_text()) if d]
    dep = next((d for d in docs if d.get('kind') == 'Deployment'), None)
    if not dep:
        return None
    containers = dep.get('spec', {}).get('template', {}).get('spec', {}).get('containers', [])
    return containers[0].get('image') if containers else None


def _collect_live_provenance(settings):
    ue_git = None
    try:
        ue_rev_res = command(remote(settings, ['git', 'rev-parse', 'HEAD']), capture_output=True, text=True)
        ue_status_res = command(remote(settings, ['git', 'status', '--porcelain']), capture_output=True, text=True)
        ue_rev = (getattr(ue_rev_res, 'stdout', None) or '').strip()
        if ue_rev:
            ue_status = (getattr(ue_status_res, 'stdout', None) or '').strip()
            ue_git = {'revision': ue_rev, 'dirty': bool(ue_status)}
    except (OSError, subprocess.SubprocessError, AttributeError):
        ue_git = None

    k8s_version = None
    try:
        ver_res = kube('version', '-o', 'json', capture_output=True, text=True)
        ver_raw = getattr(ver_res, 'stdout', None) or ''
        if ver_raw.strip():
            k8s_version = (json.loads(ver_raw).get('serverVersion') or {}).get('gitVersion')
    except (OSError, subprocess.SubprocessError, ValueError, AttributeError):
        k8s_version = None

    kube_proxy_mode = None
    try:
        cm_res = kube('-n', 'kube-system', 'get', 'configmap', 'kube-proxy',
                      '--ignore-not-found', '-o', 'json', capture_output=True, text=True)
        cm_raw = (getattr(cm_res, 'stdout', None) or '').strip()
        if cm_raw:
            cm_data = (json.loads(cm_raw).get('data') or {}).get('config.conf', '')
            parsed = yaml.safe_load(cm_data) if cm_data else None
            if isinstance(parsed, dict):
                kube_proxy_mode = parsed.get('mode') or 'iptables'
    except (OSError, subprocess.SubprocessError, ValueError, yaml.YAMLError, AttributeError):
        kube_proxy_mode = None
    if kube_proxy_mode is None:
        try:
            metrics_res = kube('get', '--raw', '/metrics', capture_output=True, text=True)
            metrics_raw = getattr(metrics_res, 'stdout', None) or ''
            for mode in ('iptables', 'nftables', 'ipvs'):
                if f'kubeproxy_{mode}_' in metrics_raw:
                    kube_proxy_mode = mode
                    break
        except (OSError, subprocess.SubprocessError, AttributeError):
            kube_proxy_mode = None

    return {
        'core_git': _local_git_provenance(),
        'ue_git': ue_git,
        'kubernetes_server_version': k8s_version,
        'kube_proxy_mode': kube_proxy_mode,
    }


def _query_external_latency_metric(allow_not_found=False, metric_name='ue_p90_latency_seconds'):
    """Query the selected raw or floor-adjusted latency metric via prometheus-adapter."""
    path = f'/apis/external.metrics.k8s.io/v1beta1/namespaces/{NAMESPACE}/{metric_name}?labelSelector=job%3Due-telemetry'
    try:
        raw = kube('get', '--raw', path, capture_output=True, text=True).stdout
    except subprocess.CalledProcessError as exc:
        detail = ((exc.stderr or '') + '\n' + (exc.stdout or '')).lower()
        if allow_not_found and ('notfound' in detail or 'not found' in detail):
            return None
        raise
    payload = json.loads(raw)
    items = payload.get('items') or []
    if not items:
        return [] if allow_not_found else None
    return items


def _query_external_metric(metric_name, allow_not_found=False):
    """Query a namespace-scoped external metric without a label selector."""
    path = f'/apis/external.metrics.k8s.io/v1beta1/namespaces/{NAMESPACE}/{metric_name}'
    try:
        raw = kube('get', '--raw', path, capture_output=True, text=True).stdout
    except subprocess.CalledProcessError as exc:
        detail = ((exc.stderr or '') + '\n' + (exc.stdout or '')).lower()
        if allow_not_found and ('notfound' in detail or 'not found' in detail):
            return None
        raise
    payload = json.loads(raw)
    items = payload.get('items') or []
    if not items:
        return [] if allow_not_found else None
    return items


def _wait_external_metric_ready(metric_name, *, arm_name='baseline', timeout=45, poll_interval=3):
    """Wait for prometheus-adapter to expose metric_name with a value before HPA creation."""
    deadline = time.monotonic() + timeout
    while True:
        items = _query_external_metric(metric_name, allow_not_found=True)
        if items and any(item.get('value') is not None for item in items):
            return items
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f'{arm_name}: external metric {metric_name} did not return a value from '
                f'prometheus-adapter within {timeout}s; aborting arm'
            )
        time.sleep(poll_interval)


def check_node_capacity(node_name, manifest_path, max_replicas, workload_app=None):
    """Fail clearly before traffic if node_name cannot fit max_replicas of manifest_path."""
    docs = [d for d in yaml.safe_load_all(Path(manifest_path).read_text()) if d]
    deployments = [d for d in docs if d.get('kind') == 'Deployment']
    if len(deployments) != 1:
        raise ValueError('application manifest requires exactly one Deployment')
    per_pod_cpu, per_pod_mem = _pod_effective_requests(deployments[0]['spec']['template']['spec'])
    node = json.loads(kube('get', 'node', node_name, '-o', 'json', capture_output=True, text=True).stdout)
    alloc = node.get('status', {}).get('allocatable') or {}
    alloc_cpu = _parse_cpu_millicores(alloc.get('cpu', 0))
    alloc_mem = _parse_memory_bytes(alloc.get('memory', 0))
    pods = json.loads(kube('get', 'pods', '-A', '--field-selector', f'spec.nodeName={node_name}',
                           '-o', 'json', capture_output=True, text=True).stdout)
    used_cpu = 0.0
    used_mem = 0.0
    for pod in pods.get('items', []):
        phase = pod.get('status', {}).get('phase')
        if phase in ('Succeeded', 'Failed'):
            continue
        meta = pod.get('metadata') or {}
        if (
            workload_app
            and not meta.get('deletionTimestamp')
            and meta.get('namespace') == NAMESPACE
            and (meta.get('labels') or {}).get('app') == workload_app
        ):
            continue
        c_cpu, c_mem = _pod_effective_requests(pod.get('spec') or {})
        used_cpu += c_cpu
        used_mem += c_mem
    need_cpu = max_replicas * per_pod_cpu
    need_mem = max_replicas * per_pod_mem
    avail_cpu = alloc_cpu - used_cpu
    avail_mem = alloc_mem - used_mem
    if need_cpu > avail_cpu or need_mem > avail_mem:
        mib = 1024 ** 2
        raise RuntimeError(
            f'node {node_name} cannot fit {max_replicas} replicas: '
            f'needs {need_cpu:.0f}m CPU / {need_mem / mib:.0f}Mi memory, '
            f'available {avail_cpu:.0f}m CPU / {avail_mem / mib:.0f}Mi memory '
            f'(allocatable {alloc_cpu:.0f}m / {alloc_mem / mib:.0f}Mi, '
            f'used {used_cpu:.0f}m / {used_mem / mib:.0f}Mi)'
        )


def verify_incluster_components(autoscaler, placement_engine, comparison='full'):
    """Verify required in-cluster HPA external metrics adapter and Go placement Deployment are ready."""
    if autoscaler == 'hpa':
        adapter = get('deployment', 'prometheus-adapter', PLACEMENT_NAMESPACE)
        if adapter['spec'].get('replicas') != 1 or adapter.get('status', {}).get('readyReplicas') != 1:
            raise RuntimeError('placement/prometheus-adapter must have exactly one ready replica')
        apiservice = json.loads(kube('get', 'apiservice', 'v1beta1.external.metrics.k8s.io', '-o', 'json',
                                     capture_output=True, text=True).stdout)
        svc = apiservice.get('spec', {}).get('service') or {}
        if svc.get('namespace') != PLACEMENT_NAMESPACE or svc.get('name') != 'prometheus-adapter':
            raise RuntimeError(
                'APIService v1beta1.external.metrics.k8s.io must point to '
                f'{PLACEMENT_NAMESPACE}/prometheus-adapter (got {svc.get("namespace")}/{svc.get("name")})'
            )
        conditions = apiservice.get('status', {}).get('conditions', [])
        if not any(c.get('type') == 'Available' and c.get('status') == 'True' for c in conditions):
            raise RuntimeError('APIService v1beta1.external.metrics.k8s.io must be Available=True')
        discovery = json.loads(kube('get', '--raw', '/apis/external.metrics.k8s.io/v1beta1',
                                    capture_output=True, text=True).stdout)
        if discovery.get('kind') != 'APIResourceList':
            raise RuntimeError('external.metrics.k8s.io discovery did not return APIResourceList')
        # Probe ue_p90_latency_seconds directly; NotFound is tolerated only when telemetry is idle,
        # whereas 5xx/adapter configuration errors (e.g. missing namespaced: false) fail preflight.
        _query_external_latency_metric(allow_not_found=True)
    if comparison != 'scaling' and placement_engine == 'go':
        placement_dep = get('deployment', 'placement-controller', PLACEMENT_NAMESPACE)
        if placement_dep['spec'].get('replicas') != 1 or placement_dep.get('status', {}).get('readyReplicas') != 1:
            raise RuntimeError('placement/placement-controller must have exactly one ready replica')


def _wait_initial_replicas_ready(app, expected_replicas, method=None, timeout=180):
    """Wait for deployment rollout and verify at least expected_replicas are ready."""
    prefix = f'{method}: ' if method else ''
    try:
        kube('-n', NAMESPACE, 'rollout', 'status', 'deployment/' + app, f'--timeout={timeout}s')
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f'{prefix}deployment/{app} did not reach {expected_replicas} ready replicas within {timeout}s'
        ) from exc
    deadline = time.monotonic() + timeout
    while True:
        dep = get('deployment', app)
        ready = (dep.get('status') or {}).get('readyReplicas') or 0
        if ready >= expected_replicas:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f'{prefix}deployment/{app} has {ready}/{expected_replicas} ready replicas after {timeout}s'
            )
        time.sleep(1)


def _wait_site_anchor_endpoints(app, timeout=60, *, edge_only=False, site_nodes=SITE_NODES):
    """Verify the common Service has a ready anchor at each application ingress."""
    expected = {node: f'{app}-anchor-{site}-' for site, node in _site_anchor_nodes(edge_only, site_nodes).items()}
    deadline = time.monotonic() + timeout
    while True:
        slices = json.loads(kube('-n', NAMESPACE, 'get', 'endpointslices', '-l',
                                 f'kubernetes.io/service-name={app}', '-o', 'json',
                                 capture_output=True, text=True).stdout)['items']
        missing = set(expected)
        for item in slices:
            for endpoint in item.get('endpoints', []):
                node = endpoint.get('nodeName')
                pod = endpoint.get('targetRef') or {}
                if (node in expected and pod.get('kind') == 'Pod'
                        and pod.get('name', '').startswith(expected[node])
                        and endpoint.get('conditions', {}).get('ready') is True):
                    missing.discard(node)
        if not missing:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f'{app} missing ready site-anchor endpoints on {", ".join(sorted(missing))}')
        time.sleep(2)


def run_method(settings, campaign, root, method, delay, slo_ms, max_replicas, debug=False,
               resource_weights=None, locality_weight=None, comparison='full',
               autoscaler='hpa', placement_engine='go', deschedule_interval=None, fixed_replicas=None, min_replicas=None,
               *, output_name=None, vertical_mode='off', initial_core_replicas=None, restore_core_pool=True):
    if placement_engine not in PLACEMENT_ENGINES:
        raise ValueError(f'placement_engine must be one of {PLACEMENT_ENGINES}')
    arm_name = output_name or method
    output = root / arm_name
    output.mkdir()
    vertical_active = vertical_mode in ('local-only', 'vertical')
    if vertical_active and fixed_replicas is not None:
        min_replicas = max_replicas = fixed_replicas
    vertical_budget = (
        vertical_budget_from_settings(
            settings, min_replicas=min_replicas, max_replicas=max_replicas, arm_mode=vertical_mode,
            fixed_total=fixed_replicas is not None,
        )
        if vertical_active else None
    )
    config, config_path, hpa_path = _method_configs(output if vertical_active else root, method, slo_ms, max_replicas, debug,
                                                  resource_weights, locality_weight, selected_workload(settings),
                                                  comparison, autoscaler, deschedule_interval, fixed_replicas,
                                                  staircase=settings.get('bounded_hpa_behavior', settings.get('staircase', False)),
                                                  latency_floor_ms=settings.get('latency_floor_ms', 0),
                                                  rt_target_factor=settings.get('rt_target_factor', 1.0),
                                                  min_replicas=min_replicas if min_replicas is not None else settings.get('min_replicas'),
                                                  scale_down_stabilization_seconds=settings.get('scale_down_stabilization_seconds'),
                                                  scale_down_pods=settings.get('scale_down_pods'),
                                                  scale_down_period_seconds=settings.get('scale_down_period_seconds'),
                                                  cpu_hpa_target=settings.get('cpu_hpa_target', DEFAULT_CPU_HPA_TARGET),
                                                  pod_share_scoring=settings.get(
                                                      'pod_share_scoring', comparison != 'scaling' and placement_engine == 'go'),
                                                  no_hpa=settings.get('no_hpa', False),
                                                  asymmetric_latency=settings.get('asymmetric_latency', False),
                                                  site_anchors=settings.get('site_anchors', False),
                                                  edge_only=settings.get('edge_only', False), site_nodes=_site_nodes(settings),
                                                  base_manifest=root / 'workload.yaml',
                                                  vertical_budget=vertical_budget,
                                                  proposed_scale_down_period_seconds=settings.get('proposed_scale_down_period_seconds'))
    app = workload_identity(selected_workload(settings))
    if vertical_active:
        (output / 'arm-budget.json').write_text(json.dumps({
            'arm': arm_name,
            'method': method,
            'vertical_mode': vertical_mode,
            'autoscaler': 'hpa' if hpa_path is not None else 'none',
            'hpa_min_managed_replicas': config['min_replicas'] if hpa_path is not None else None,
            'budget': vertical_budget.to_metadata(),
            'eligible_nodes': config.get('eligible_nodes'),
        }, indent=2) + '\n')
    processes = []
    remote_started = False
    completed = False
    offloader_token = None
    incluster_go = comparison != 'scaling' and placement_engine == 'go' and method == 'proposed'
    method_start_dt = datetime.now(timezone.utc)
    method_start_iso = method_start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        with (output / 'controller.log').open('x') as control_log, (output / 'sampler.log').open('x') as sampler_log, (output / 'runner-ssh.log').open('x') as runner_log:
            try:
                # Native timers are cluster-wide; archive before deployment so
                # initial scheduler observations have a counter baseline.
                command([sys.executable, '-m', 'utils.control_overhead', 'snapshot', output / 'control-overhead.jsonl'])
                processes.append(subprocess.Popen(
                    [sys.executable, '-u', '-m', 'utils.control_overhead', 'sample', str(output / 'control-overhead.jsonl')],
                    stdout=sampler_log, stderr=subprocess.STDOUT, start_new_session=True))
                if incluster_go:
                    cm = _render_incluster_placement_configmap(config, config['manifest'])
                    cm_path = output / 'placement-configmap.yaml'
                    with cm_path.open('x') as stream:
                        yaml.safe_dump(cm, stream, sort_keys=False)
                    kube('apply', '-f', str(cm_path))
                    kube('-n', PLACEMENT_NAMESPACE, 'rollout', 'restart', 'deployment/placement-controller')
                    kube('-n', PLACEMENT_NAMESPACE, 'rollout', 'status', 'deployment/placement-controller', '--timeout=120s')
                    placement_dep = get('deployment', 'placement-controller', PLACEMENT_NAMESPACE)
                    if placement_dep.get('status', {}).get('readyReplicas') != 1:
                        raise RuntimeError('placement/placement-controller is not ready before workload deployment')
                if settings.get('site_anchors'):
                    anchor_sites = _site_anchor_nodes(settings.get('edge_only', False), _site_nodes(settings))
                    for node in anchor_sites.values():
                        check_node_capacity(node, config['manifest'], 1)
                    kube('apply', '-f', str(root / 'site-anchors.yaml'))
                    for site in anchor_sites:
                        _wait_initial_replicas_ready(f'{app}-anchor-{site}', 1, method=method)
                if vertical_active or hpa_path is not None or settings.get('no_hpa', False):
                    kube('apply', '-f', config['manifest'])
                if hpa_path is not None:
                    if method == 'baseline' and comparison == 'full' and settings.get('site_anchors', False):
                        n_fixed = (vertical_budget.fixed_replicas if vertical_budget is not None
                                   else len(_site_anchor_nodes(settings.get('edge_only', False), _site_nodes(settings))))
                        _wait_external_metric_ready(_baseline_cpu_metric_name(n_fixed), arm_name=arm_name)
                    kube('apply', '-f', hpa_path)
                seed_managed = (
                    settings.get('site_anchors', False)
                    and fixed_replicas is None
                    and hpa_path is not None
                    and (not vertical_active or vertical_budget.dynamic_min == 0)
                )
                if not seed_managed:
                    _wait_initial_replicas_ready(app, config['min_replicas'], method=method)
                service = get('service', app)
                check_service_routing(service, settings.get('prefer_close', False))
                if settings.get('site_anchors'):
                    _wait_site_anchor_endpoints(app, edge_only=settings.get('edge_only', False), site_nodes=_site_nodes(settings))
                port = service['spec']['ports'][0]['nodePort']
                offloader_token = None
                if vertical_active:
                    offloader_url = settings.get('offloader_url')
                    envoy_ports = settings.get('envoy_site_ports')
                    if not offloader_url or not envoy_ports:
                        raise RuntimeError(f'{arm_name}: vertical_mode requires offloader_url and envoy_site_ports in campaign config')
                    offloader_token = _load_offloader_token(settings)
                    if restore_core_pool and initial_core_replicas is None:
                        initial_core_replicas = get_core_pool_replicas(app, arm_name=arm_name)
                    readiness = prepare_vertical_arm(
                        settings, app, vertical_mode, vertical_budget,
                        token=offloader_token, arm_name=arm_name,
                    )
                    (output / 'vertical-readiness.json').write_text(json.dumps(readiness, indent=2) + '\n')
                    print(
                        f'{arm_name}: readiness complete (mode={vertical_mode}, '
                        f'edge_anchors={vertical_budget.edge_anchors}, '
                        f'dynamic={vertical_budget.dynamic_min}..{vertical_budget.dynamic_max}, '
                        f'core_fallback={vertical_budget.core_fallback_replicas}, '
                        f'total_budget={vertical_budget.min_replicas}..{vertical_budget.max_replicas}, '
                        f'eligible_nodes={config.get("eligible_nodes")})',
                        flush=True,
                    )
                    site_urls = {site: f'http://{address}:{envoy_ports[site]}/detect'
                                 for site, address in settings['site_node_ips'].items()}
                else:
                    site_urls = {site: f'http://{address}:{port}/detect'
                                 for site, address in settings['site_node_ips'].items()}
                if delay is not None:
                    delay.check()
                preflight = [sys.executable, '-m', 'utils.preflight', '--config', config_path]
                if seed_managed:
                    preflight.append('--allow-zero-replicas')
                command(preflight)
                sampler_cmd = [sys.executable, '-u', '-m', 'utils.monitor_resources', 'sample',
                               str(output / 'metrics.jsonl'), '--namespace', NAMESPACE, '--app', app, '--pods-only']
                if vertical_active:
                    sampler_cmd.extend(['--core-app', f'{app}-core'])
                sampler = subprocess.Popen(sampler_cmd, stdout=sampler_log, stderr=subprocess.STDOUT, start_new_session=True)
                processes.append(sampler)
                if hpa_path is not None:
                    hpa_sampler = subprocess.Popen([sys.executable, '-u', '-m', 'utils.monitor_resources', 'hpa-sample', str(output / 'hpa-status.jsonl'), '--namespace', NAMESPACE, '--hpa', f'{app}-{method}'], stdout=sampler_log, stderr=subprocess.STDOUT, start_new_session=True)
                    processes.append(hpa_sampler)
                payload = remote_settings(settings) | {'campaign': campaign, 'method': method, 'output_name': arm_name, 'site_urls': site_urls,
                           'schedule': settings['schedule'], 'duration': settings['phase_duration'],
                           'phase_transition': settings.get('phase_transition', 'continuous')}
                if settings.get('phase_durations'):
                    payload['phase_durations'] = settings['phase_durations']
                ue = subprocess.Popen(remote(settings, ['sudo', '-n', '.venv/bin/python', '-u', '-m', 'experiments.campaign_ue', 'run']), stdin=subprocess.PIPE, stdout=runner_log, stderr=subprocess.STDOUT, text=True, start_new_session=True)
                remote_started = True
                ue.stdin.write(json.dumps(payload)); ue.stdin.close()
                progress = RunnerProgress(output / 'runner-ssh.log', method)
                verified = False
                deadline = time.monotonic() + 90
                while ue.poll() is None:
                    progress.poll()
                    if seed_managed and progress.traffic_started:
                        # A native HPA with a positive minimum stays inactive at zero.
                        kube('-n', NAMESPACE, 'scale', f'deployment/{app}', '--current-replicas=0',
                             f'--replicas={config["min_replicas"]}')
                        seed_managed = False
                    if delay is not None:
                        delay.check()
                    if any(p.poll() is not None for p in processes):
                        raise RuntimeError('controller or sampler exited during traffic; see logs')
                    if not verified:
                        prom_ok = active_telemetry(config['prometheus_url'])
                        if hpa_path is None or method == 'baseline' and comparison == 'full':
                            adapter_ok = True
                        elif ((method == 'proposed' or comparison == 'placement')
                              and (settings.get('latency_floor_ms') or settings.get('rt_metric_unit') == 'milliseconds')):
                            metric_unit = settings.get('rt_metric_unit', 'seconds')
                            adapter_ok = bool(_query_external_latency_metric(
                                allow_not_found=True,
                                metric_name=settings.get('rt_metric_name') or _latency_metric_name(
                                    settings.get('latency_floor_ms', 0), unit=metric_unit)))
                        else:
                            adapter_ok = bool(_query_external_latency_metric(allow_not_found=True))
                        verified = prom_ok and adapter_ok
                        if verified:
                            print(f'{method}: active UE latency telemetry verified', flush=True)
                        elif time.monotonic() > deadline:
                            raise RuntimeError('no valid active telemetry within 90 seconds')
                    time.sleep(10)
                progress.poll(final=True)
                if ue.returncode != 0:
                    raise RuntimeError(f'UE workload failed ({ue.returncode}); see runner-ssh.log')
                if not verified:
                    raise RuntimeError('UE workload ended without valid active telemetry')
                if incluster_go:
                    placement_dep = get('deployment', 'placement-controller', PLACEMENT_NAMESPACE)
                    if placement_dep.get('status', {}).get('readyReplicas') != 1:
                        raise RuntimeError('placement/placement-controller became unready during traffic')
                completed = True
            finally:
                if incluster_go:
                    try:
                        kube('-n', PLACEMENT_NAMESPACE, 'logs', 'deployment/placement-controller',
                             f'--since-time={method_start_iso}', stdout=control_log)
                    except (OSError, subprocess.SubprocessError) as exc:
                        control_log.write(f'failed to fetch in-cluster placement-controller logs: {exc}\n')
                primary_error = sys.exc_info()[1]
                errors = []
                if remote_started and not completed:
                    try:
                        command(remote(settings, ['sudo', '-n', '.venv/bin/python', '-m', 'experiments.campaign_ue', 'stop', '--campaign', campaign, '--method', arm_name]))
                        ue.wait(timeout=100)
                    except (OSError, subprocess.SubprocessError) as exc:
                        errors.append(f'UE cleanup failed: {exc}')
                for process in reversed(processes):
                    try:
                        stop_process(process)
                    except (OSError, subprocess.SubprocessError) as exc:
                        errors.append(f'Local process cleanup failed: {exc}')
                if errors:
                    message = '; '.join(errors)
                    if primary_error is None:
                        raise RuntimeError(message)
                    print(f'ERROR: {message}', file=sys.stderr, flush=True)
    finally:
        resource_uids = set()
        try:
            dep_raw = kube('-n', NAMESPACE, 'get', 'deployments,pods,hpa', '-o', 'json', capture_output=True, text=True).stdout
            (output / 'deployments-after.json').write_text(dep_raw)
            for item in json.loads(dep_raw).get('items', []):
                meta = item.get('metadata') or {}
                if (meta.get('name') or '').startswith(app) and meta.get('uid'):
                    resource_uids.add(meta['uid'])
        except (OSError, subprocess.SubprocessError, ValueError):
            pass
        try:
            raw_events = kube('-n', NAMESPACE, 'get', 'events', '-o', 'json', capture_output=True, text=True).stdout
            all_events = json.loads(raw_events).get('items', [])
            hpa_name = f'{app}-{method}'
            other_hpa = f'{app}-{"proposed" if method == "baseline" else "baseline"}'
            filtered = []
            for ev in all_events:
                inv = ev.get('involvedObject') or {}
                name = inv.get('name') or ''
                uid = inv.get('uid') or ''
                ts_raw = ev.get('lastTimestamp') or ev.get('eventTime') or ev.get('firstTimestamp') or (ev.get('metadata') or {}).get('creationTimestamp')
                if ts_raw:
                    try:
                        ev_dt = datetime.fromisoformat(ts_raw.replace('Z', '+00:00'))
                        if ev_dt < method_start_dt:
                            continue
                    except ValueError:
                        pass
                if uid and uid in resource_uids:
                    filtered.append(ev)
                elif name == app or name == hpa_name or (name.startswith(app + '-') and name != other_hpa):
                    filtered.append(ev)
            (output / 'workload-events.json').write_text(json.dumps({'items': filtered}, indent=2) + '\n')
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            (output / 'workload-events.json').write_text(json.dumps({'error': str(exc)}) + '\n')
        finally:
            if vertical_active and settings.get('offloader_url'):
                cleanup_token = offloader_token
                if cleanup_token is None:
                    try:
                        cleanup_token = _load_offloader_token(settings)
                    except RuntimeError:
                        cleanup_token = None
                if cleanup_token is not None:
                    try:
                        fetch_offloader_artifacts(settings['offloader_url'], output, token=cleanup_token)
                    except (OSError, ValueError) as exc:
                        (output / 'offload-artifacts-error.txt').write_text(f'{type(exc).__name__}: {exc}\n')
                cleanup_vertical_state(
                    settings, app, token=cleanup_token,
                    initial_core_replicas=initial_core_replicas,
                    output_dir=output, arm_name=arm_name,
                    restore_core_pool=restore_core_pool,
                    raise_on_error=completed,
                )
    transfer(settings, campaign, arm_name, output)
    from utils.cadvisor_resources import (
        DEFAULT_FRESHNESS_S,
        DEFAULT_RATE_WINDOW_S,
        DEFAULT_SCRAPE_INTERVAL_S,
    )
    command([sys.executable, '-m', 'utils.summarize', output / 'requests.jsonl', '--slo-ms', str(slo_ms),
             '--gnb1-ingress-node', _site_nodes(settings)['gnb-1']])
    command([sys.executable, '-m', 'utils.monitor_resources', 'summarize', output / 'metrics.jsonl', output / 'requests.jsonl', output / 'summary.csv', output, '--skip-resource-csv'])
    command([
        sys.executable, '-m', 'utils.cadvisor_resources', 'run',
        output / 'requests.jsonl', output / 'summary.csv', output,
        '--metrics-jsonl', output / 'metrics.jsonl',
        '--prometheus-url', config['prometheus_url'],
        '--namespace', NAMESPACE, '--pod-prefix', app + '-',
        '--step', str(DEFAULT_SCRAPE_INTERVAL_S),
        '--rate-window', str(DEFAULT_RATE_WINDOW_S),
        '--scrape-interval', str(DEFAULT_SCRAPE_INTERVAL_S),
        '--freshness', str(DEFAULT_FRESHNESS_S),
    ])
    command([sys.executable, '-m', 'utils.telemetry_history', output / 'requests.jsonl', output, '--prometheus-url', config['prometheus_url']])


def main(argv=None):
    parser, args = _parse_args(argv)
    if not args.check and args.plan_only is None and (not args.campaign or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', args.campaign)):
        parser.error('provide a new campaign name using letters, digits, hyphens or underscores')
    if args.prefer_close_routing_only:
        args.prefer_close = True
    if args.long_run:
        if args.scenario not in ('mobility9', 'joint'):
            parser.error('--long-run requires --scenario mobility9')
        args.phase_duration = MOBILITY_PHASE_SECONDS
    if args.mobility_direction is not None and args.scenario not in ('mobility9', 'joint'):
        parser.error('--mobility-direction requires --scenario mobility9')
    mobility_direction = (args.mobility_direction or 'forward') if args.scenario in ('mobility9', 'joint') else None
    if args.fixed_replicas is not None:
        if args.fixed_replicas < 1:
            parser.error('--fixed-replicas must be a positive integer')
        if args.scenario not in ('mobility9', 'joint'):
            parser.error('--fixed-replicas requires --scenario mobility9')
        if args.max_replicas is not None:
            parser.error('use --fixed-replicas without --max-replicas')
        if args.min_replicas is not None:
            parser.error('use --min-replicas without --fixed-replicas')
    if args.min_replicas is not None and args.min_replicas < 1:
        parser.error('--min-replicas must be a positive integer')
    if args.scale_down_stabilization_seconds is not None and args.scale_down_stabilization_seconds < 0:
        parser.error('--scale-down-stabilization-seconds must be nonnegative')
    if args.scale_down_pods is not None and args.scale_down_pods < 1:
        parser.error('--scale-down-pods must be a positive integer')
    if args.scale_down_period_seconds is not None and args.scale_down_period_seconds < 1:
        parser.error('--scale-down-period-seconds must be a positive integer')
    if args.proposed_scale_down_period_seconds is not None and not 1 <= args.proposed_scale_down_period_seconds <= 1800:
        parser.error('--proposed-scale-down-period-seconds must be in 1..1800')
    if args.scenario in SCALER_SCENARIOS and args.comparison not in (None, 'scaling'):
        parser.error('scaling scenarios require --comparison scaling')
    if args.comparison == 'scaling' and args.scenario not in (None, *SCALER_SCENARIOS):
        parser.error('--comparison scaling requires scaler-balanced or scaler-staircase')
    if args.comparison == 'scaling' and args.scenario is None:
        args.scenario = 'scaler-balanced'
    args.comparison = args.comparison or (
        'scaling' if args.scenario in SCALER_SCENARIOS else
        'full' if args.scenario else
        'full'
    )
    if args.no_hpa and (args.comparison != 'placement' or args.fixed_replicas is None
                        or args.placement_engine != 'go' or args.autoscaler != 'hpa'):
        parser.error('--no-hpa requires a fixed-replica Go placement comparison')
    if args.bounded_hpa_behavior and (args.comparison != 'full' or args.autoscaler != 'hpa'):
        parser.error('--bounded-hpa-behavior requires a full comparison with native HPA')
    if args.pod_share_scoring and (args.comparison == 'scaling' or args.placement_engine != 'go'):
        parser.error('--pod-share-scoring requires Go placement')
    if args.edge_only and args.comparison == 'scaling':
        parser.error('--edge-only requires a placement or full comparison; scaling runs on core-5g')
    spill_enabled = args.spill_high is not None or args.spill_low is not None
    if spill_enabled:
        if args.spill_high is None or args.spill_low is None:
            parser.error('--spill-high and --spill-low must be supplied together')
        if not 0 <= args.spill_low < args.spill_high:
            parser.error('spill thresholds require 0 <= --spill-low < --spill-high')
        if not (args.prefer_close and args.site_anchors):
            parser.error('spillover requires --prefer-close and --site-anchors')
    if args.site_anchors:
        if args.comparison == 'scaling':
            parser.error('--site-anchors requires a placement or full comparison')
        if args.autoscaler != 'hpa':
            parser.error('--site-anchors requires native HPA (--autoscaler hpa)')
        args.prefer_close = not args.cluster_wide
    if args.final_hold and args.scenario not in SCALER_SCENARIOS:
        parser.error('--final-hold requires a scaling scenario')
    if args.scenario in ('mobility9', 'joint'):
        if args.debug:
            parser.error('mobility9 uses standard HPA timers; omit --debug')
        if args.autoscaler != 'hpa' or args.placement_engine != 'go':
            parser.error('mobility9 requires native HPA and Go placement')
        if args.slo_ms is None:
            args.slo_ms = DEFAULT_SLO_MS
        if args.deschedule_interval is None:
            args.deschedule_interval = DEFAULT_DESCHEDULE_INTERVAL_S
    elif args.scenario in SCALER_SCENARIOS:
        if args.debug:
            parser.error('scaling scenarios have their own HPA profile; omit --debug')
        if args.autoscaler != 'hpa':
            parser.error('scaling scenarios require native HPA (--autoscaler hpa)')
        if args.placement_engine != 'go':
            parser.error('scaling scenarios do not use a custom placement engine; omit --placement-engine python')
        if args.deschedule_interval is not None:
            parser.error('scaling scenarios do not use the descheduler; omit --deschedule-interval')
        if args.resource_weights is not None or args.locality_weight is not None:
            parser.error('scaling scenarios do not use custom placement scoring')
        if args.slo_ms is None:
            args.slo_ms = DEFAULT_SLO_MS
    if args.cpu_hpa_target is not None:
        if args.cpu_hpa_target < 1:
            parser.error('--cpu-hpa-target must be a positive integer percent of CPU requests')
        if args.comparison == 'placement':
            parser.error('--cpu-hpa-target requires --comparison full or scaling; placement uses latency HPA for both methods')
    else:
        args.cpu_hpa_target = DEFAULT_CPU_HPA_TARGET
    if args.comparison != 'scaling' and args.deschedule_interval is None:
        args.deschedule_interval = DEFAULT_DESCHEDULE_INTERVAL_S
    if args.deschedule_interval is not None and (not math.isfinite(args.deschedule_interval) or args.deschedule_interval <= 0):
        parser.error('--deschedule-interval must be finite and positive')
    if args.tc_ms is not None and (not math.isfinite(args.tc_ms) or args.tc_ms < 0.001):
        parser.error('--tc-ms must be finite and at least 0.001 ms')
    if args.slo_ms is not None and (not math.isfinite(args.slo_ms) or args.slo_ms <= 0):
        parser.error('--slo-ms must be finite and positive')
    if not math.isfinite(args.rt_target_factor) or not 0 < args.rt_target_factor <= 1:
        parser.error('--rt-target-factor must be finite and in (0, 1]')
    if args.rt_target_factor != 1.0 and args.autoscaler != 'hpa':
        parser.error('--rt-target-factor != 1.0 requires --autoscaler hpa')
    if args.rt_target_factor != 1.0 and args.comparison == 'placement':
        parser.error('--rt-target-factor != 1.0 is not supported with --comparison placement (baseline and proposed must share the same latency autoscaler)')
    if args.latency_floor_ms is None:
        args.latency_floor_ms = (
            round(DEFAULT_LATENCY_FLOOR_FRACTION * (args.slo_ms if args.slo_ms is not None else DEFAULT_SLO_MS))
            if args.autoscaler == 'hpa' and (args.comparison == 'full' or args.scenario == 'scaler-staircase')
            else 0
        )
    if args.latency_floor_ms < 0:
        parser.error('--latency-floor-ms must be nonnegative')
    if args.asymmetric_latency and (
        not args.latency_floor_ms or (args.comparison != 'full' and args.scenario != 'scaler-staircase')
    ):
        parser.error('--asymmetric-latency requires a full or staircase comparison with a positive latency floor')
    if args.proposed_scale_down_period_seconds is not None and (args.autoscaler != 'hpa' or args.no_hpa):
        parser.error('--proposed-scale-down-period-seconds requires an active HPA')
    if args.latency_floor_ms and args.no_hpa:
        parser.error('--latency-floor-ms requires an active HPA')
    if args.latency_floor_ms:
        if args.autoscaler != 'hpa':
            parser.error('--latency-floor-ms requires native HPA (--autoscaler hpa)')
        if not _supports_latency_floor(args.comparison, args.scenario == 'scaler-staircase', args.site_anchors):
            parser.error('--latency-floor-ms requires --comparison full, --comparison placement with --site-anchors, or --scenario scaler-staircase')
    if args.max_replicas is not None and args.max_replicas < 1:
        parser.error('--max-replicas must be a positive integer')
    if args.phase_duration is not None and (not math.isfinite(args.phase_duration) or args.phase_duration < 60):
        parser.error('--phase-duration must be finite and at least 60 seconds')
    if not math.isfinite(args.rps_per_ue) or args.rps_per_ue <= 0:
        parser.error('--rps-per-ue must be finite and positive')
    os.chdir(ROOT)
    os.environ.setdefault('KUBECONFIG', '/etc/rancher/k3s/k3s.yaml')
    settings = load(args.config)
    site_nodes = _site_nodes(settings)
    settings['rps_per_ue'] = float(args.rps_per_ue)
    settings['phase_transition'] = args.phase_transition
    if args.scenario in ('mobility9', 'joint'):
        mobility_phases = (args.phase_schedule or MOBILITY9_SCHEDULE).split(',')
        if mobility_direction == 'reverse':
            mobility_phases = list(reversed(mobility_phases))
        settings.update(
            schedule=','.join(mobility_phases),
            phase_duration=MOBILITY_PHASE_SECONDS,
            mobility_direction=mobility_direction,
        )
    elif args.scenario in SCALER_SCENARIOS:
        settings.update(schedule=SCALER_STAIRCASE_SCHEDULE if args.scenario == 'scaler-staircase' else SCALER_BALANCED_SCHEDULE,
                        phase_duration=SCALER_PHASE_SECONDS)
    if args.phase_schedule is not None:
        parse_schedule(args.phase_schedule)
        if args.scenario not in ('mobility9', 'joint'):
            settings['schedule'] = args.phase_schedule
    workload = args.workload if args.workload is not None else selected_workload(settings)
    if spill_enabled and workload != 'pedestrian-detection':
        parser.error('spillover requires the pedestrian-detection workload')
    settings['workload'] = workload
    settings['staircase'] = args.scenario == 'scaler-staircase'
    settings['bounded_hpa_behavior'] = settings['staircase'] or args.bounded_hpa_behavior
    settings['no_hpa'] = args.no_hpa
    settings['pod_share_scoring'] = args.comparison != 'scaling' and args.placement_engine == 'go'
    settings['latency_floor_ms'] = args.latency_floor_ms
    settings['asymmetric_latency'] = args.asymmetric_latency
    settings['rt_target_factor'] = float(args.rt_target_factor)
    settings['cpu_hpa_target'] = args.cpu_hpa_target
    settings['prefer_close'] = args.prefer_close
    settings['prefer_close_routing_only'] = args.prefer_close_routing_only
    settings['site_anchors'] = args.site_anchors
    if args.edge_only:
        settings['edge_only'] = True
    if args.scale_down_stabilization_seconds is not None:
        settings['scale_down_stabilization_seconds'] = args.scale_down_stabilization_seconds
    if args.scale_down_pods is not None:
        settings['scale_down_pods'] = args.scale_down_pods
    if args.scale_down_period_seconds is not None:
        settings['scale_down_period_seconds'] = args.scale_down_period_seconds
    if args.proposed_scale_down_period_seconds is not None:
        settings['proposed_scale_down_period_seconds'] = args.proposed_scale_down_period_seconds
    if args.workload is not None:
        # Separate config and an explicit SLO prevent accidental reuse of legacy defaults.
        if args.config == str(ROOT / 'experiments/campaign.yaml') or args.slo_ms is None:
            parser.error('pedestrian workload requires a separate --config and explicit --slo-ms after calibration')
        if '/face-detection/' in settings['image']:
            parser.error('pedestrian workload must not use face-detection fixtures')
    if args.debug:
        settings['phase_duration'] = 60
    if args.phase_duration is not None:
        settings['phase_duration'] = args.phase_duration
    phases = parse_schedule(settings['schedule']) if 'schedule' in settings else []
    hold_seconds = (max(0, 600 - float(settings['phase_duration']))
                    if args.scenario == 'scaler-staircase' else FINAL_HOLD_SECONDS) if args.final_hold else 0
    if args.scenario in SCALER_SCENARIOS:
        base_duration = float(settings['phase_duration'])
        settings['phase_durations'] = [base_duration] * (len(phases) - 1) + [base_duration + hold_seconds]
    phase_durations = settings.get('phase_durations') or (
        [float(settings['phase_duration'])] * len(phases) if 'phase_duration' in settings and phases else []
    )
    controller_config = yaml.safe_load((ROOT / 'config.yaml').read_text())
    try:
        resource_weights = validate_resource_weights(
            dict(zip(('cpu', 'memory'), args.resource_weights))
            if args.resource_weights is not None else controller_config['resource_weights'])
    except ValueError as exc:
        parser.error(str(exc))
    locality_weight = (args.locality_weight if args.locality_weight is not None
                       else controller_config.get('locality_weight', .4))
    if not math.isfinite(locality_weight) or not 0 <= locality_weight <= 1:
        parser.error('--locality-weight must be finite and between 0 and 1')
    slo_ms = args.slo_ms if args.slo_ms is not None else DEFAULT_SLO_MS
    internal_target_dec = Decimal(str(slo_ms)) * Decimal(str(args.rt_target_factor))
    internal_target_ms = _decimal_to_number(internal_target_dec)
    effective_target_dec = (internal_target_dec - Decimal(str(args.latency_floor_ms))
                            if args.latency_floor_ms and not args.asymmetric_latency else internal_target_dec)
    effective_metric_target_ms = _decimal_to_number(effective_target_dec)
    metric_unit = _metric_unit_for_target(effective_target_dec) if args.autoscaler == 'hpa' else 'seconds'
    asymmetric_target_ms = internal_target_dec if args.asymmetric_latency else None
    metric_name = _latency_metric_name(args.latency_floor_ms, unit=metric_unit, asymmetric_target_ms=asymmetric_target_ms)
    vertical_active = args.vertical_mode != 'off'
    settings['rt_metric_name'] = metric_name
    settings['rt_metric_unit'] = metric_unit
    hpa_target_value = _format_hpa_target(effective_target_dec, unit=metric_unit)
    if args.latency_floor_ms and internal_target_dec <= Decimal(str(args.latency_floor_ms)):
        if args.rt_target_factor == 1.0:
            parser.error('--latency-floor-ms must be smaller than --slo-ms')
        parser.error('internal RT target (--slo-ms * --rt-target-factor) must be greater than --latency-floor-ms')
    cli_min_replicas = args.min_replicas
    cli_max_replicas = args.max_replicas
    default_max = controller_config['max_replicas']
    if args.comparison == 'scaling':
        default_max = SCALER_STAIRCASE_MAX_REPLICAS if settings['staircase'] else SCALER_DEFAULT_MAX_REPLICAS
    max_replicas = args.max_replicas if args.max_replicas is not None else default_max
    if args.fixed_replicas is not None:
        max_replicas = args.fixed_replicas
    min_replicas = (args.fixed_replicas if args.fixed_replicas is not None
                    else args.min_replicas if args.min_replicas is not None
                    else DEFAULT_MIN_REPLICAS)
    if not vertical_active and args.fixed_replicas is None and max_replicas < min_replicas:
        parser.error('--max-replicas must be at least min_replicas')
    anchor_sites = _site_anchor_nodes(args.edge_only, site_nodes) if args.site_anchors else {}
    if anchor_sites and not vertical_active:
        try:
            managed_min, managed_max = _managed_replica_bounds(min_replicas, max_replicas, edge_only=args.edge_only, site_nodes=site_nodes)
        except ValueError as exc:
            parser.error(str(exc))
    if not vertical_active and args.fixed_replicas is None:
        # Feed the resolved default into manifests as well as metadata.
        args.min_replicas = min_replicas
    proposed_latency_target = {
        'type': 'latency_p90_ms' if args.autoscaler == 'hpa' else 'controller_latency_p90_ms',
        'target': effective_metric_target_ms,
    }
    if args.rt_target_factor != 1.0:
        proposed_latency_target.update(
            slo_ms=slo_ms,
            internal_target_ms=internal_target_ms,
            rt_target_factor=args.rt_target_factor,
            metric=metric_name,
            metric_unit=metric_unit,
            hpa_target_value=hpa_target_value,
        )
    hpa_targets = (
        {'baseline': {'type': 'cpu_utilization_percent', 'target': args.cpu_hpa_target},
         'proposed': proposed_latency_target}
        if args.comparison == 'scaling' else
        {'baseline': {'type': 'latency_p90_ms', 'target': slo_ms},
         'proposed': proposed_latency_target}
        if args.comparison == 'placement' else
        {'baseline': {'type': 'cpu_utilization_percent', 'target': args.cpu_hpa_target},
         'proposed': proposed_latency_target}
    )
    if args.latency_floor_ms:
        hpa_targets['proposed'] = {
            'type': 'asymmetric_latency_p90_ms' if args.asymmetric_latency else 'excess_latency_p90_ms',
            'target': effective_metric_target_ms,
            'floor_ms': args.latency_floor_ms, 'slo_ms': slo_ms,
            'metric': metric_name,
        }
        if args.rt_target_factor != 1.0:
            hpa_targets['proposed'].update(
                internal_target_ms=internal_target_ms,
                rt_target_factor=args.rt_target_factor,
                metric_unit=metric_unit,
                hpa_target_value=hpa_target_value,
            )
        if args.comparison == 'placement':
            hpa_targets['baseline'] = hpa_targets['proposed'].copy()
    if args.proposed_scale_down_period_seconds is not None:
        hpa_targets['proposed']['scale_down_limit'] = {
            'pods': 1,
            'period_seconds': args.proposed_scale_down_period_seconds,
        }
    if args.rt_target_factor == 1.0:
        scaling_formula = ('max(RT-floor, 0)/(SLO-floor)' if args.latency_floor_ms
                           else 'RT/SLO (unchanged; rounding can retain excess replicas)')
    else:
        scaling_formula = ('max(RT-floor, 0)/(internal_target-floor)' if args.latency_floor_ms
                           else 'RT/internal_target')
    if args.asymmetric_latency:
        scaling_formula = 'RT/internal_target below target; (RT-floor)/(internal_target-floor) above target'
    if args.method_order == 'proposed-first':
        method_order = ['proposed', 'baseline']
    elif args.method_order == 'random':
        method_order = ['baseline', 'proposed'] if random.choice((True, False)) else ['proposed', 'baseline']
    else:
        method_order = ['baseline', 'proposed']
    effective_scale_down = {
        'stabilization_window_seconds': (
            args.scale_down_stabilization_seconds
            if args.scale_down_stabilization_seconds is not None
            else (60 if settings['bounded_hpa_behavior'] else int(controller_config['scale_down_seconds']))
        ),
        'pods': (
            args.scale_down_pods
            if args.scale_down_pods is not None
            else None
        ),
        'period_seconds': (
            args.scale_down_period_seconds
            if args.scale_down_period_seconds is not None
            else None
        ),
    }
    workload_image = _workload_image_ref(workload)
    image_digest = (workload_image or '').partition('@')[2] or None
    provenance = {
        'core_git': _local_git_provenance(),
        'ue_git': None,
        'kubernetes_server_version': None,
        'kube_proxy_mode': None,
        'workload_image': workload_image,
        'workload_image_digest': image_digest,
        'method_order': method_order,
    }
    vertical_budget = None
    arm_budgets_obj = {}
    if vertical_active:
        fixed_vertical = args.comparison == 'placement' and args.no_hpa and args.fixed_replicas is not None
        dynamic_vertical = args.comparison == 'full' and not args.no_hpa and args.fixed_replicas is None
        if args.autoscaler != 'hpa' or not (fixed_vertical or dynamic_vertical):
            parser.error('--vertical-mode requires a full comparison with native HPA or a fixed Go placement comparison with --no-hpa')
        if fixed_vertical:
            cli_min_replicas = cli_max_replicas = args.fixed_replicas
        if settings.get('radio_only_gnb1') is not False:
            parser.error('--vertical-mode requires radio_only_gnb1: false (local PDU sessions for all three gNBs)')
        if not args.site_anchors:
            parser.error('--vertical-mode requires --site-anchors so both baseline and proposed guarantee a ready local pod on every gNB site')
        if not settings.get('offloader_url') or not settings.get('envoy_site_ports'):
            parser.error('--vertical-mode requires offloader_url and envoy_site_ports in campaign config')
        try:
            if args.vertical_mode == 'four-arm':
                vertical_budget = vertical_budget_from_settings(
                    settings,
                    min_replicas=cli_min_replicas,
                    max_replicas=cli_max_replicas,
                    arm_mode='vertical',
                )
                local_arm_budget = vertical_budget.for_arm_mode('local-only', fixed_total=fixed_vertical)
                arm_budgets_obj = {
                    arm_name: (local_arm_budget if arm_vmode == 'local-only' else vertical_budget)
                    for arm_name, _method, arm_vmode in ARM_SELECTIONS[args.arms]
                }
            else:
                vertical_budget = vertical_budget_from_settings(
                    settings,
                    min_replicas=cli_min_replicas,
                    max_replicas=cli_max_replicas,
                    arm_mode=args.vertical_mode,
                    fixed_total=fixed_vertical,
                )
                arm_budgets_obj = {method: vertical_budget for method in method_order}
        except ValueError as exc:
            parser.error(str(exc))
        min_replicas = vertical_budget.min_replicas
        max_replicas = vertical_budget.max_replicas
        managed_min = vertical_budget.dynamic_min
        managed_max = vertical_budget.dynamic_max
    use_hpa_metadata = not args.no_hpa
    baseline_cpu_rules = None
    if args.comparison == 'full' and args.site_anchors and not args.no_hpa:
        core_manifest_path = _core_backend_manifest()
        pod_request_cores = verify_matching_pod_cpu_requests(
            workload_manifest(workload),
            core_manifest=core_manifest_path if (vertical_active and core_manifest_path.exists()) else None,
        )
        container_name = _workload_container_name(workload)
        pod_prefix = workload_identity(workload)
        if args.vertical_mode == 'four-arm':
            n_fixed_values = sorted({
                arm_budgets_obj[arm].fixed_replicas
                for arm, method_name, _ in ARM_SELECTIONS[args.arms]
                if method_name == 'baseline'
            })
            baseline_metrics = {
                arm: _baseline_cpu_metric_name(arm_budgets_obj[arm].fixed_replicas)
                for arm, method_name, _ in ARM_SELECTIONS[args.arms]
                if method_name == 'baseline'
            }
            hpa_targets['baseline'] = {
                'type': 'external_cpu_dynamic_replicas',
                'target': args.cpu_hpa_target,
                'average_value': '1',
                'pod_request_cores': float(pod_request_cores),
                'rate_window_seconds': DEFAULT_BASELINE_CPU_RATE_WINDOW_SECONDS,
                'metrics_by_arm': baseline_metrics,
            }
        elif vertical_active:
            n_fixed_values = [vertical_budget.fixed_replicas]
            hpa_targets['baseline'] = {
                'type': 'external_cpu_dynamic_replicas',
                'target': args.cpu_hpa_target,
                'average_value': '1',
                'pod_request_cores': float(pod_request_cores),
                'rate_window_seconds': DEFAULT_BASELINE_CPU_RATE_WINDOW_SECONDS,
                'metric': _baseline_cpu_metric_name(vertical_budget.fixed_replicas),
            }
        else:
            n_fixed_values = [len(anchor_sites)]
            hpa_targets['baseline'] = {
                'type': 'external_cpu_dynamic_replicas',
                'target': args.cpu_hpa_target,
                'average_value': '1',
                'pod_request_cores': float(pod_request_cores),
                'rate_window_seconds': DEFAULT_BASELINE_CPU_RATE_WINDOW_SECONDS,
                'metric': _baseline_cpu_metric_name(len(anchor_sites)),
            }
        baseline_cpu_rules = [
            _baseline_cpu_adapter_rule(
                n_fixed,
                cpu_hpa_target=args.cpu_hpa_target,
                pod_request_cores=pod_request_cores,
                container=container_name,
                pod_prefix=pod_prefix,
            )
            for n_fixed in n_fixed_values
        ]
    shared_metadata = {
        'workload': workload,
        'vertical_mode': args.vertical_mode,
        'arms': args.arms if args.vertical_mode == 'four-arm' else None,
        'vertical_budget': vertical_budget.to_metadata() if vertical_budget is not None else None,
        'arm_budgets': {k: v.to_metadata() for k, v in arm_budgets_obj.items()} if vertical_active else None,
        'core_backend_replicas': vertical_budget.core_fallback_replicas if vertical_budget is not None else None,
        'core_backend_replicas_by_arm': (
            {k: v.core_fallback_replicas for k, v in arm_budgets_obj.items()} if vertical_active else None
        ),
        'core_counts_toward_budget': vertical_budget.core_counts_toward_budget if vertical_budget is not None else None,
        'budget_flag_semantics': (
            '--min-replicas/--max-replicas represent the vertical-arm total budget (including core_backend_replicas); '
            'local-only removes Core fallback from the starting minimum and retains the same total ceiling.'
            if vertical_active else None
        ),
        'response_contract': 'detection',
        'response_log_schema_version': 1,
        'http_threads': _workload_http_threads(workload),
        'workload_image_digest': image_digest,
        'phase_transition': args.phase_transition,
        'comparison': args.comparison,
        'scenario': args.scenario,
        'mobility_direction': mobility_direction,
        'fixed_replicas': args.fixed_replicas,
        'deschedule_interval_seconds': args.deschedule_interval,
        'deschedule_min_score_improvement': 0.05 if settings['pod_share_scoring'] else None,
        'autoscaler': 'none' if args.no_hpa else args.autoscaler,
        'hpa_managed_bounds_by_arm': {
            arm: {'min': max(1, budget.dynamic_min), 'max': budget.dynamic_max}
            for arm, budget in arm_budgets_obj.items()
        } if vertical_active and use_hpa_metadata else None,
        'placement_engine': None if args.comparison == 'scaling' else args.placement_engine,
        'attribution_note': (
            'Fixed-replica placement comparison with HPA disabled in both arms.'
            if args.no_hpa else (
                'Native CPU HPA and default scheduling versus native latency HPA and Go placement. '
                'All arms share the same total replica ceiling. Vertical arms reserve Core fallback slots.'
                if vertical_active else COMPARISON_NOTES[args.comparison]
            )
        ),
        'slo_ms': slo_ms,
        'reporting_slo_ms': slo_ms,
        'rt_target_factor': float(args.rt_target_factor),
        'internal_rt_target_ms': internal_target_ms,
        'effective_rt_metric_target_ms': effective_metric_target_ms if use_hpa_metadata else None,
        'rt_metric_name': metric_name if use_hpa_metadata else None,
        'rt_metric_unit': metric_unit if use_hpa_metadata else None,
        'rt_hpa_target_value': hpa_target_value if use_hpa_metadata else None,
        'schedule': settings.get('schedule'),
        'phase_duration': settings.get('phase_duration'),
        'long_run': args.long_run,
        'phase_durations_s': phase_durations,
        'hpa_behavior_profile': ('disabled' if not use_hpa_metadata else
                                 'bounded-staircase' if settings['bounded_hpa_behavior'] else 'original'),
        'hpa_scale_down': effective_scale_down if use_hpa_metadata else None,
        'method_order_requested': args.method_order,
        'method_order': method_order,
        'provenance': provenance,
        'latency_floor_ms': args.latency_floor_ms,
        'latency_floor_source': ('reference latency set to 0.8 x SLO; replicas double when p90 reaches 1.2 x SLO'
                             if args.latency_floor_ms == round(DEFAULT_LATENCY_FLOOR_FRACTION * slo_ms)
                             else 'explicit --latency-floor-ms reference; not an automatically measured physical minimum') if args.latency_floor_ms else None,
        'latency_scaling_formula': None if args.no_hpa else scaling_formula,
        'analysis_windows': {'whole_phase': True, 'late_phase_seconds': 60, 'steady_state_assumed': False},
        'final_hold_seconds': hold_seconds,
        'node_restriction': SCALER_NODE if args.comparison == 'scaling' else None,
        'hpa_targets': hpa_targets if use_hpa_metadata else None,
        'cpu_hpa_target': args.cpu_hpa_target if (use_hpa_metadata and args.comparison != 'placement') else None,
        'prefer_close': args.prefer_close,
        'prefer_close_routing_only': args.prefer_close_routing_only,
        'pod_share_scoring': settings['pod_share_scoring'],
        'replica_bounds': {'min': min_replicas, 'max': max_replicas},
        'pacing': {'rps_per_ue': float(args.rps_per_ue)},
        'tc_ms': args.tc_ms,
        'resource_measurement': {
            'source': 'prometheus-cadvisor',
            'scrape_interval_seconds': 5,
            'rate_window_seconds': 45,
            'freshness_seconds': 25,
            'metrics_server_hpa_unchanged': True,
        },
    }
    if spill_enabled:
        spill_metadata = {'spill_high': args.spill_high, 'spill_low': args.spill_low,
                          'spill_readiness_probe': SPILL_READINESS_PROBE.copy()}
        shared_metadata.update(spill_metadata)
        provenance.update(spill_metadata)
    eligible_nodes = (SCALER_NODE,) if args.comparison == 'scaling' else tuple(_site_anchor_nodes(args.edge_only, site_nodes).values())
    topology = {
        'radio_only_nodes': ['gnb1'] if site_nodes == SITE_NODES else [],
        'eligible_nodes': list(eligible_nodes),
        'application_ingress_nodes': site_nodes,
        'application_ingress_ips': settings['site_node_ips'],
        'placement_demand_source': 'upf',
    }
    if vertical_active:
        topology['eligible_nodes_by_arm'] = {
            arm: [*eligible_nodes, 'core-5g'] if budget.core_fallback_replicas else list(eligible_nodes)
            for arm, budget in arm_budgets_obj.items()
        }
    if args.edge_only:
        topology['edge_only'] = True
    shared_metadata.update(topology)
    provenance.update(topology)
    if anchor_sites:
        shared_metadata['site_anchors'] = True
        provenance['site_anchors'] = True
        shared_metadata['site_anchor_nodes'] = anchor_sites
        shared_metadata['managed_replica_bounds'] = {'min': managed_min, 'max': managed_max}
        if vertical_active:
            if args.vertical_mode == 'four-arm':
                shared_metadata['initial_replicas'] = None
                shared_metadata['initial_replicas_by_arm'] = {
                    k: v.fixed_replicas + v.dynamic_min for k, v in arm_budgets_obj.items()
                }
                shared_metadata['active_total_replica_bounds'] = None
                shared_metadata['active_total_replica_bounds_by_arm'] = {
                    k: {'min': v.fixed_replicas + max(1, v.dynamic_min), 'max': v.max_replicas}
                    for k, v in arm_budgets_obj.items()
                }
            else:
                shared_metadata['initial_replicas'] = vertical_budget.fixed_replicas + vertical_budget.dynamic_min
                shared_metadata['active_total_replica_bounds'] = {
                    'min': vertical_budget.fixed_replicas + max(1, vertical_budget.dynamic_min),
                    'max': vertical_budget.max_replicas,
                }
        else:
            shared_metadata['initial_replicas'] = args.fixed_replicas or len(anchor_sites)
            shared_metadata['active_total_replica_bounds'] = {
                'min': managed_min + len(anchor_sites), 'max': max_replicas,
            }
    if args.plan_only is not None:
        output = args.plan_only.resolve()
        output.mkdir(parents=True, exist_ok=False)
        _campaign_manifest(output, args.tc_ms is not None, workload, prefer_close=args.prefer_close,
                           site_anchors=args.site_anchors, spill_high=args.spill_high, spill_low=args.spill_low,
                           edge_only=args.edge_only, site_nodes=site_nodes)
        plan_arms = (ARM_SELECTIONS[args.arms] if args.vertical_mode == 'four-arm'
                     else [(method, method, args.vertical_mode) for method in method_order])
        for arm, method, _ in plan_arms:
            arm_dir = output / arm if args.vertical_mode == 'four-arm' else output
            arm_dir.mkdir(exist_ok=True)
            _method_configs(arm_dir, method, slo_ms, max_replicas, args.debug,
                            resource_weights, locality_weight, workload, args.comparison, args.autoscaler, args.deschedule_interval, args.fixed_replicas,
                            staircase=settings['bounded_hpa_behavior'], latency_floor_ms=args.latency_floor_ms,
                            rt_target_factor=args.rt_target_factor, min_replicas=args.min_replicas,
                            scale_down_stabilization_seconds=args.scale_down_stabilization_seconds,
                            scale_down_pods=args.scale_down_pods,
                            scale_down_period_seconds=args.scale_down_period_seconds,
                            cpu_hpa_target=args.cpu_hpa_target,
                            pod_share_scoring=settings['pod_share_scoring'], no_hpa=args.no_hpa,
                            asymmetric_latency=args.asymmetric_latency,
                            site_anchors=args.site_anchors, edge_only=args.edge_only, site_nodes=site_nodes,
                            base_manifest=output / 'workload.yaml', vertical_budget=arm_budgets_obj.get(arm),
                            proposed_scale_down_period_seconds=args.proposed_scale_down_period_seconds)
        if args.latency_floor_ms or metric_unit == 'milliseconds' or baseline_cpu_rules:
            adapter_filename = 'floor-adapter-configmap.yaml' if args.latency_floor_ms else 'latency-adapter-configmap.yaml'
            (output / adapter_filename).write_text(
                yaml.safe_dump(_floor_adapter_config(
                    args.latency_floor_ms, unit=metric_unit, asymmetric_target_ms=asymmetric_target_ms,
                    baseline_cpu_rules=baseline_cpu_rules,
                ), sort_keys=False))
        (output / 'plan.yaml').write_text(yaml.safe_dump(shared_metadata | {
            'calibrated': False,
            'warning': 'offline preview only; image digest, dataset, resource sizes, and HPA target require validation',
            'campaign_settings': settings}, sort_keys=False))
        print(f'Offline plan written: {output}; no cluster access or traffic started.')
        return
    if args.placement_engine == 'go' and args.autoscaler != 'hpa':
        parser.error('--placement-engine go requires --autoscaler hpa (no Python controller process)')
    with open('/tmp/mec-experiment-campaign.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        def interrupted(signum, frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, interrupted)
        if not args.check:
            check_result_name(settings, args.campaign)
        prerequisites(settings | {
            '_autoscaler': args.autoscaler,
            '_placement_engine': args.placement_engine,
            '_comparison': args.comparison,
            'no_hpa': args.no_hpa,
        })
        if args.check:
            return
        root = ROOT / 'results' / args.campaign
        # Recheck after preflight in case another process reserved the name.
        check_result_name(settings, args.campaign)
        root.mkdir(parents=True, exist_ok=False)
        shared_metadata['provenance'] = shared_metadata['provenance'] | _collect_live_provenance(settings)
        with (root / 'campaign.yaml').open('x') as stream:
            yaml.safe_dump(settings | shared_metadata | {
                'min_replicas': min_replicas,
                'max_replicas': max_replicas, 'debug': args.debug,
                'resource_weights': resource_weights,
                'locality_weight': locality_weight}, stream)
        minutes = sum(phase_durations) / 60
        duration_desc = (f'{settings["phase_duration"]:g}s each' if not hold_seconds
                         else f'{settings["phase_duration"]:g}s each + {hold_seconds}s final hold')
        arm_count = len(ARM_SELECTIONS[args.arms]) if args.vertical_mode == 'four-arm' else len(method_order)
        print(f'Campaign: {len(phases)} phases per method, {duration_desc}; '
              f'{minutes:g} minutes traffic per arm ({arm_count * minutes:g} minutes total), '
              'plus UE switching, setup and transfer. Objective: all-attempt p90 '
              f'<= {slo_ms:g} ms. Shared replica ceiling: {max_replicas}.', flush=True)
        if vertical_active:
            for arm, budget in arm_budgets_obj.items():
                print(f'{arm}: starts at {budget.min_replicas} total; '
                      f'HPA managed bounds {max(1, budget.dynamic_min)}–{budget.dynamic_max}; '
                      f'total ceiling {budget.max_replicas}; Core fallback {budget.core_fallback_replicas}.', flush=True)
        elif args.site_anchors and args.fixed_replicas is not None:
            print(f'Each arm waits for {len(anchor_sites)} site anchors and {managed_min} managed replicas '
                  'to be Ready before phase 1.', flush=True)
        elif args.site_anchors:
            print(f'Each arm starts with {len(anchor_sites)} site anchors, then enables HPA '
                  f'for {managed_min}–{managed_max} managed replicas at phase 1.', flush=True)
        if args.comparison != 'scaling':
            locality_signal = ('UPF demand share minus managed pod share'
                               if settings['pod_share_scoring'] else 'UPF demand share')
            print(f'Proposed scoring: {locality_signal}; resource weights={resource_weights}; '
                  f'locality weight={locality_weight:g}; resource contribution={1-locality_weight:g}.', flush=True)
        if args.comparison == 'full':
            target_label = 'Baseline CPU HPA target'
            print(f'{target_label}: {args.cpu_hpa_target}% of CPU requests.', flush=True)
        if args.scenario in ('mobility9', 'joint'):
            trajectory = ' -> '.join(':'.join(str(p[f'gnb{s}_ues']) for s in (1, 2, 3)) for p in phases)
            print(f'Mobility scenario ({mobility_direction}): UE splits {trajectory}. '
                  f'Descheduler checks every {args.deschedule_interval:g}s; evictions require settled Pods and a score gain of at least 0.05 in Go placement. '
                  'UE moves use stop-and-reattach, not seamless handover.', flush=True)
        elif args.scenario in SCALER_SCENARIOS:
            target_desc = (f'internal scaling target {internal_target_ms:g} ms p90 = {slo_ms:g} ms * {args.rt_target_factor:g}; '
                           f'reporting SLO {slo_ms:g} ms'
                           if args.rt_target_factor != 1.0
                           else f'scaling target {internal_target_ms:g} ms p90; reporting SLO {slo_ms:g} ms')
            ue_counts = ' -> '.join(str(p['workload_ue']) for p in phases)
            print(f'Scaling scenario: UE counts {ue_counts}; workload on {SCALER_NODE} with default-scheduler; '
                  f'CPU HPA ({args.cpu_hpa_target}%) vs latency HPA ({target_desc}), replicas {min_replicas}..{max_replicas}.', flush=True)
        if args.fixed_replicas is not None:
            hpa_mode = 'HPA disabled' if args.no_hpa else 'HPA min/max clamped'
            print(f'Fixed placement experiment: {args.fixed_replicas} replicas per method; '
                  f'{hpa_mode}. Eviction may temporarily reduce readiness.', flush=True)
        if args.debug:
            print('DEBUG profile: 15s collector window retained; controller interval 15s, '
                  'placement-only cooldown 15s, scale-down stabilization 30s, descheduler 30s, '
                  'node CPU window 60s; HPA scale-down stabilization 30s. '
                  'Cluster scrape and metrics-server/HPA sync intervals are unchanged. '
                  'Short phases do not guarantee convergence.', flush=True)
        ns = kube('get', 'namespace', NAMESPACE, '--ignore-not-found', '-o', 'name', capture_output=True, text=True)
        if not ns.stdout.strip():
            kube('create', 'namespace', NAMESPACE)
        delay = TrafficDelay(args.tc_ms) if args.tc_ms is not None else None
        original_zones = None
        initial_core_replicas = None
        campaign_completed = False
        app_id = workload_identity(workload)
        try:
            if vertical_active:
                initial_core_replicas = get_core_pool_replicas(app_id, arm_name=args.campaign)
                (root / 'core-pool-before.json').write_text(json.dumps({
                    'deployment': f'{app_id}-core',
                    'namespace': NAMESPACE,
                    'initial_replicas': initial_core_replicas,
                }, indent=2) + '\n')
            if args.prefer_close:
                original_zones = set_prefer_close_zones(eligible_nodes)
                (root / 'node-zones-before.yaml').write_text(yaml.safe_dump(original_zones))
            if delay is not None:
                delay.start()
            _campaign_manifest(root, delay is not None, workload, prefer_close=args.prefer_close,
                               site_anchors=args.site_anchors, spill_high=args.spill_high, spill_low=args.spill_low,
                               edge_only=args.edge_only, site_nodes=site_nodes)
            if args.comparison == 'scaling':
                reset(workload)
                check_node_capacity(SCALER_NODE, root / 'workload.yaml', max_replicas, app_id)
            if args.asymmetric_latency:
                print(f'Asymmetric latency HPA: {scaling_formula}; metric {metric_name}, target {hpa_target_value}.', flush=True)
                adapter_kw = {'baseline_cpu_rules': baseline_cpu_rules} if baseline_cpu_rules else {}
                _prepare_floor_adapter(root, args.latency_floor_ms, unit=metric_unit, asymmetric_target_ms=asymmetric_target_ms, **adapter_kw)
            elif args.latency_floor_ms:
                print(f'Exploratory floor-adjusted HPA: fixed reference {args.latency_floor_ms}ms, '
                      f'internal RT scaling target {internal_target_ms:g}ms (factor {args.rt_target_factor:g}), '
                      f'excess target {effective_metric_target_ms:g}ms (metric {metric_name}, target {hpa_target_value}). '
                      f'Reporting SLO remains {slo_ms:g}ms.', flush=True)
                adapter_kw = {'baseline_cpu_rules': baseline_cpu_rules} if baseline_cpu_rules else {}
                _prepare_floor_adapter(root, args.latency_floor_ms, unit=metric_unit, **adapter_kw)
            elif args.rt_target_factor != 1.0 or metric_unit == 'milliseconds':
                print(f'Proposed RT HPA: internal scaling target {internal_target_ms:g}ms '
                      f'(reporting SLO {slo_ms:g}ms * factor {args.rt_target_factor:g}; '
                      f'metric {metric_name}, target {hpa_target_value}). '
                      f'Reporting SLO remains {slo_ms:g}ms.', flush=True)
                if metric_unit == 'milliseconds' or baseline_cpu_rules:
                    adapter_kw = {'baseline_cpu_rules': baseline_cpu_rules} if baseline_cpu_rules else {}
                    _prepare_floor_adapter(root, 0, unit=metric_unit, **adapter_kw)
            elif baseline_cpu_rules:
                _prepare_floor_adapter(root, 0, unit=metric_unit, baseline_cpu_rules=baseline_cpu_rules)
            if args.vertical_mode == 'four-arm':
                for arm_name, method, arm_vertical_mode in ARM_SELECTIONS[args.arms]:
                    print(f'=== {arm_name.upper()} ({method}, {arm_vertical_mode}) ===', flush=True)
                    reset(workload)
                    run_method(settings, args.campaign, root, method, delay, slo_ms, cli_max_replicas, args.debug,
                               resource_weights, locality_weight, args.comparison,
                               args.autoscaler, args.placement_engine, args.deschedule_interval, args.fixed_replicas,
                               min_replicas=cli_min_replicas, output_name=arm_name, vertical_mode=arm_vertical_mode,
                               initial_core_replicas=initial_core_replicas, restore_core_pool=False)
                command([sys.executable, '-m', 'utils.vertical_summary', root, '--slo-ms', str(slo_ms)])
                if args.arms == 'all':
                    command([sys.executable, '-m', 'utils.plot_vertical', root, root / 'plots', '--slo-ms', str(slo_ms)])
                    final_scenario = 'mobility' if args.comparison == 'placement' else 'joint'
                    command([sys.executable, '-m', 'utils.plot_final_phases', '--run-dir', root,
                             '--scenario', final_scenario, '--output-dir', root / 'plots'])
                else:
                    print(f'Plots skipped: arms={args.arms} is a partial run.', flush=True)
            else:
                for method in method_order:
                    print(f'=== {method.upper()} ===', flush=True)
                    reset(workload)
                    run_method(settings, args.campaign, root, method, delay, slo_ms,
                               cli_max_replicas if vertical_active else max_replicas, args.debug,
                               resource_weights, locality_weight, args.comparison,
                               args.autoscaler, args.placement_engine, args.deschedule_interval, args.fixed_replicas,
                               min_replicas=cli_min_replicas if vertical_active else args.min_replicas,
                               vertical_mode=args.vertical_mode,
                               initial_core_replicas=initial_core_replicas,
                               restore_core_pool=False)
                command([sys.executable, '-m', 'utils.plot_autoscaler', root, root / 'plots', '--slo-ms', str(slo_ms)])
                command([sys.executable, '-m', 'utils.plot_slides', root, '--output-dir', root / 'slides', '--slo-ms', str(slo_ms)])
                command([sys.executable, '-m', 'utils.plot_overhead_slides', root])
                if args.comparison == 'scaling':
                    command([sys.executable, '-m', 'utils.plot_final_phases', '--run-dir', root,
                             '--scenario', 'scaling', '--output-dir', root / 'plots'])
            campaign_completed = True
        finally:
            try:
                reset(workload)
            finally:
                try:
                    if vertical_active and initial_core_replicas is not None:
                        cleanup_vertical_state(
                            settings, app_id, token=None,
                            initial_core_replicas=initial_core_replicas,
                            output_dir=root, arm_name=args.campaign,
                            restore_core_pool=True,
                            raise_on_error=campaign_completed,
                        )
                finally:
                    try:
                        if original_zones is not None:
                            restore_prefer_close_zones(original_zones)
                    finally:
                        if delay is not None:
                            delay.close()
        print(f'Campaign complete. Raw data, logs, plots and slides: {root}', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        raise SystemExit(f'Campaign stopped: {exc}') from exc
