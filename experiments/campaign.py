"""Run the matched gNB3-hotspot campaign from core, including remote UEs and plots."""
import argparse
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
from urllib.request import urlopen

import yaml
from benchmark.client import parse_schedule
from utils.traffic_control import POOL_NAME, TrafficDelay
from utils.config import validate_resource_weights
from utils.experiment import DEFAULT_MAX_IN_FLIGHT_PER_UE, DEFAULT_RPS_PER_UE

ROOT = Path(__file__).resolve().parent.parent
NAMESPACE = 'services'
APP = 'mec-placement-face-detection'
WORKLOADS = ('face-detection', 'pedestrian-detection')
MOBILITY9_SCHEDULE = '9:7:1:1,9:5:3:1,9:3:5:1,9:1:7:1,9:1:5:3,9:1:3:5,9:1:1:7'
SCALER_BALANCED_SCHEDULE = '3:1:1:1,9:3:3:3,15:5:5:5,24:8:8:8,15:5:5:5,9:3:3:3,3:1:1:1'
SCALER_STAIRCASE_SCHEDULE = ','.join(f'{n}:{n//3}:{n//3}:{n//3}' for n in (3, 6, 9, 12, 15, 18, 24, 18, 15, 12, 9, 6, 3))
SCALER_SCENARIOS = ('scaler-balanced', 'scaler-staircase')
SCALER_NODE = 'gnb-3'
SCALER_MIN_REPLICAS = 3
SCALER_DEFAULT_MAX_REPLICAS = 24
DEFAULT_SLO_MS = 150
DEFAULT_TC_MS = 10
DEFAULT_MIN_REPLICAS = 3
DEFAULT_DESCHEDULE_INTERVAL_S = 30
DEFAULT_PHASE_SECONDS = 120
MOBILITY_PHASE_SECONDS = DEFAULT_PHASE_SECONDS
SCALER_PHASE_SECONDS = DEFAULT_PHASE_SECONDS
FINAL_HOLD_SECONDS = 360
COMPARISONS = ('full', 'placement', 'scaling')
AUTOSCALERS = ('controller', 'hpa')
PLACEMENT_ENGINES = ('python', 'go')
PLACEMENT_NAMESPACE = 'placement'
PLACEMENT_PROMETHEUS_URL = 'http://placement-prometheus.placement.svc:9090'
COMPARISON_NOTES = {
    'placement': 'Comparison A (placement): same latency-ratio autoscaler for both methods; isolates scheduler/descheduler placement.',
    'full': 'Comparison B (full system): default-scheduler + CPU HPA vs full proposed system (autoscaler + scheduler + descheduler); do not attribute results solely to placement.',
    'scaling': 'Comparison C (scaling): default-scheduler + gnb-3-only required affinity for both methods; native CPU HPA (60% target) vs native latency HPA (target recorded in hpa_targets).',
}


def workload_identity(workload='face-detection'):
    if workload not in WORKLOADS:
        raise ValueError('unknown workload')
    return 'mec-placement-' + workload


def workload_manifest(workload='face-detection'):
    workload_identity(workload)
    return ROOT / 'apps' / workload / 'manifest.yaml'


def _workload_http_threads(workload='face-detection'):
    """Return configured HTTP_THREADS from the workload Deployment env, if set."""
    for doc in yaml.safe_load_all(workload_manifest(workload).read_text()):
        if doc and doc.get('kind') == 'Deployment':
            for container in doc.get('spec', {}).get('template', {}).get('spec', {}).get('containers', []):
                for entry in container.get('env') or []:
                    if entry.get('name') == 'HTTP_THREADS' and entry.get('value') is not None:
                        return int(entry['value'])
    return None


def selected_workload(settings):
    return settings.get('workload', 'face-detection')


def check_service_routing(service):
    """Require cluster-wide endpoints without locality hints or client affinity."""
    spec = service['spec']
    annotations = service.get('metadata', {}).get('annotations') or {}
    if (spec.get('trafficDistribution') is not None or
            spec.get('externalTrafficPolicy', 'Cluster') != 'Cluster' or
            spec.get('internalTrafficPolicy', 'Cluster') != 'Cluster' or
            spec.get('sessionAffinity', 'None') != 'None' or
            any(annotations.get(key, 'Disabled').lower() not in ('disabled', '')
                for key in ('service.kubernetes.io/topology-mode',
                            'service.kubernetes.io/topology-aware-hints'))):
        raise RuntimeError('Workload Service requires cluster-wide routing without locality preference or session affinity')


class RunnerProgress:
    """Relay complete remote log lines while retaining the current phase."""

    def __init__(self, path, method):
        self.path, self.method = path, method
        self.offset = 0
        self.phase = 'starting UE runner'
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
    return {k: settings[k] for k in ('inventory', 'nr_ue', 'token', 'image', 'telemetry_url')} | {
        'max_ues': max(p['workload_ue'] for p in parse_schedule(settings['schedule'])),
        'rps_per_ue': float(settings.get('rps_per_ue', DEFAULT_RPS_PER_UE)),
        'max_in_flight_per_ue': int(settings.get('max_in_flight_per_ue', DEFAULT_MAX_IN_FLIGHT_PER_UE))}


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
    optional = {'workload'}
    if not isinstance(data, dict) or not expected.issubset(data) or (set(data) - expected - optional):
        raise ValueError('campaign configuration keys must be: ' + ', '.join(sorted(expected))
                         + ' (optional: ' + ', '.join(sorted(optional)) + ')')
    if 'workload' in data:
        workload_identity(data['workload'])
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
    if workload != 'face-detection':
        config = replace(config, manifest=workload_manifest(workload))
    kube('get', '--raw=/readyz')
    nodes = json.loads(kube('get', 'nodes', '-o', 'json', capture_output=True, text=True).stdout)
    by_name = {node['metadata']['name']: node for node in nodes['items']}
    for site, node_name in (('gnb-1', 'gnb1'), ('gnb-2', 'gnb-2'), ('gnb-3', 'gnb-3')):
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
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    other = command(remote(settings, ['git', 'rev-parse', 'HEAD']), capture_output=True, text=True).stdout.strip()
    if revision != other:
        raise RuntimeError('core and UE project revisions differ; git pull --ff-only on both first')
    command(remote(settings, ['sudo', '-n', '.venv/bin/python', '-m', 'experiments.campaign_ue', 'check']),
            input=json.dumps(remote_settings(settings)), text=True)
    from utils.preflight import collector_status
    collector_status(config)
    verify_incluster_components(settings.get('_autoscaler', 'hpa'),
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


def reset(workload='face-detection'):
    """Delete only the selected experiment app and its HPAs."""
    app = workload_identity(workload)
    kube('-n', NAMESPACE, 'delete', 'hpa/' + app + '-baseline',
         'hpa/' + app + '-proposed',
         'deployment/' + app, 'service/' + app, '--ignore-not-found=true',
         '--cascade=foreground', '--wait=true', '--timeout=180s')
    pods = json.loads(kube('-n', NAMESPACE, 'get', 'pods', '-l', 'app=' + app,
                          '-o', 'json', capture_output=True, text=True).stdout)['items']
    if pods:
        kube('-n', NAMESPACE, 'wait', '--for=delete', 'pod', '-l', 'app=' + app,
             '--timeout=180s')


def query(url, expression):
    with urlopen(url.rstrip('/') + '/api/v1/query?' + urlencode({'query': expression}), timeout=10) as response:
        result = json.load(response)
    if result.get('status') != 'success':
        raise RuntimeError('Prometheus query failed')
    return result['data']['result']


def active_telemetry(url):
    for metric, expected in [('up{job="ue-telemetry"}', 1),
                             ('ue_request_window_complete{job="ue-telemetry"}', 1)]:
        values = query(url, metric)
        if len(values) != 1 or float(values[0]['value'][1]) != expected:
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

def _campaign_manifest(root, use_tc, workload='face-detection', scheduler_name=None, filename='workload.yaml'):
    """Write one shared workload manifest for both methods."""
    documents = list(yaml.safe_load_all(workload_manifest(workload).read_text()))
    deployments = [item for item in documents if item and item.get('kind') == 'Deployment']
    if len(deployments) != 1:
        raise ValueError('application manifest requires exactly one Deployment')
    if scheduler_name is not None:
        deployments[0]['spec']['template']['spec']['schedulerName'] = scheduler_name
    if use_tc:
        metadata = deployments[0]['spec']['template'].setdefault('metadata', {})
        metadata.setdefault('annotations', {})['cni.projectcalico.org/ipv4pools'] = json.dumps([POOL_NAME])
    path = root / filename
    with path.open('x') as stream:
        yaml.safe_dump_all(documents, stream, sort_keys=False)
    return path



def _latency_metric_name(floor_ms=0, unit='seconds'):
    if unit not in ('seconds', 'milliseconds'):
        raise ValueError(f'unsupported latency metric unit: {unit}')
    if floor_ms:
        return f'ue_p90_excess_latency_{floor_ms}ms_{unit}'
    return f'ue_p90_latency_{unit}'


def _floor_adapter_config(floor_ms, configmap=None, unit='seconds'):
    """Append a latency-scaling rule, preserving the raw metric and unrelated rules."""
    if unit not in ('seconds', 'milliseconds'):
        raise ValueError(f'unsupported latency metric unit: {unit}')
    if type(floor_ms) is not int or floor_ms < 0 or (floor_ms == 0 and unit != 'milliseconds'):
        raise ValueError('adapter floor must be a positive whole number of milliseconds')
    if configmap is None:
        docs = yaml.safe_load_all((ROOT / 'telemetry/05-prometheus-adapter.yaml').read_text())
        configmap = next(d for d in docs if d and d.get('kind') == 'ConfigMap'
                         and d['metadata']['name'] == 'prometheus-adapter-config')
    # Persist only declarative fields, not live resource versions or managed fields.
    result = {'apiVersion': 'v1', 'kind': 'ConfigMap',
              'metadata': {'name': 'prometheus-adapter-config', 'namespace': PLACEMENT_NAMESPACE},
              'data': dict(configmap['data'])}
    config = yaml.safe_load(result['data']['config.yaml'])
    name = _latency_metric_name(floor_ms, unit=unit)
    rules = config.setdefault('externalRules', [])
    rules[:] = [r for r in rules if r.get('name', {}).get('as') != name]
    base_expr = 'ue_request_attempt_duration_p90_seconds{job="ue-telemetry"}'
    expr = f'clamp_min({base_expr} - {floor_ms / 1000:g}, 0)' if floor_ms else base_expr
    if unit == 'milliseconds':
        expr = f'({expr}) * 1000'
    rules.append({
        'seriesQuery': base_expr,
        'resources': {'namespaced': False},
        'name': {'matches': '^ue_request_attempt_duration_p90_seconds$', 'as': name},
        'metricsQuery': f'{expr} and on() (ue_request_window_complete{{job="ue-telemetry"}} == 1)',
    })
    result['data']['config.yaml'] = yaml.safe_dump(config, sort_keys=False)
    return result


def _prepare_floor_adapter(root, floor_ms, unit='seconds'):
    """Install the selected rule before either method; no workload or traffic here."""
    current = get('configmap', 'prometheus-adapter-config', PLACEMENT_NAMESPACE)
    desired = _floor_adapter_config(floor_ms, current, unit=unit)
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
                slo_ms=None, staircase=False, latency_floor_ms=0, rt_target_factor=1.0,
                scale_down_stabilization_seconds=None, scale_down_pods=None, scale_down_period_seconds=None):
    hpa = yaml.safe_load((ROOT / template_name).read_text())
    hpa['metadata']['name'] = hpa_name
    hpa['spec']['scaleTargetRef']['name'] = app_name
    hpa['spec'].update(minReplicas=min_replicas, maxReplicas=max_replicas)
    if staircase:
        # Matched damping for both metrics, independent of latency signal choice.
        stab = 60 if scale_down_stabilization_seconds is None else int(scale_down_stabilization_seconds)
        pods = 1 if scale_down_pods is None else int(scale_down_pods)
        period = 30 if scale_down_period_seconds is None else int(scale_down_period_seconds)
        hpa['spec']['behavior'] = {
            'scaleUp': {'stabilizationWindowSeconds': 0,
                        'policies': [{'type': 'Pods', 'value': 2, 'periodSeconds': 30}]},
            'scaleDown': {'stabilizationWindowSeconds': stab, 'tolerance': 0.01,
                          'policies': [{'type': 'Pods', 'value': pods, 'periodSeconds': period}]},
        }
    else:
        stab = int(scale_down_seconds) if scale_down_stabilization_seconds is None else int(scale_down_stabilization_seconds)
        hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] = stab
        if scale_down_pods is not None or scale_down_period_seconds is not None:
            pods = 1 if scale_down_pods is None else int(scale_down_pods)
            period = 30 if scale_down_period_seconds is None else int(scale_down_period_seconds)
            hpa['spec']['behavior']['scaleDown']['policies'] = [
                {'type': 'Pods', 'value': pods, 'periodSeconds': period}
            ]
    if slo_ms is not None:
        metric = hpa['spec']['metrics'][0]['external']
        internal_target_dec = Decimal(str(slo_ms)) * Decimal(str(rt_target_factor))
        target_dec = internal_target_dec - Decimal(str(latency_floor_ms)) if latency_floor_ms else internal_target_dec
        unit = _metric_unit_for_target(target_dec)
        metric['metric']['name'] = _latency_metric_name(latency_floor_ms, unit=unit)
        metric['target']['value'] = _format_hpa_target(target_dec, unit=unit)
    with output_path.open('x') as stream:
        yaml.safe_dump(hpa, stream, sort_keys=False)
    return output_path


def _render_incluster_placement_configmap(config, manifest_path):
    """Build the placement-controller-config ConfigMap for the in-cluster Go scheduler/descheduler."""
    incluster_cfg = dict(config)
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


def _method_configs(root, method, slo_ms, max_replicas, debug=False, resource_weights=None, locality_weight=None, workload='face-detection', comparison='full', autoscaler='controller', deschedule_interval=None, fixed_replicas=None, staircase=False, latency_floor_ms=0, rt_target_factor=1.0, min_replicas=None, scale_down_stabilization_seconds=None, scale_down_pods=None, scale_down_period_seconds=None):
    """Persist the exact matched replica budget and manifests used by a method."""
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
    if latency_floor_ms and (not staircase or comparison != 'scaling' or autoscaler != 'hpa'
                            or type(latency_floor_ms) is not int or latency_floor_ms <= 0
                            or internal_target_dec <= Decimal(str(latency_floor_ms))):
        raise ValueError('latency floor requires staircase scaling with native HPA and 0 < floor < internal RT target')
    config = yaml.safe_load((ROOT / 'config.yaml').read_text())
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
    config['min_replicas'] = effective_min_replicas
    if type(max_replicas) is not int or max_replicas < config['min_replicas']:
        raise ValueError('max replicas must be an integer at least min_replicas')
    if resource_weights is not None:
        config['resource_weights'] = validate_resource_weights(resource_weights)
    if locality_weight is not None:
        if not math.isfinite(locality_weight) or not 0 <= locality_weight <= 1:
            raise ValueError('locality weight must be between 0 and 1')
        config['locality_weight'] = locality_weight
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
        deployments[0]['spec']['template']['spec']['affinity'] = {
            'nodeAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchFields': [{
                            'key': 'metadata.name',
                            'operator': 'In',
                            'values': [SCALER_NODE],
                        }]
                    }]
                }
            }
        }
    if fixed_replicas is not None or min_replicas is not None:
        deployments[0]['spec']['replicas'] = effective_min_replicas
    method_manifest = root / f'{method}-workload.yaml'
    with method_manifest.open('x') as stream:
        yaml.safe_dump_all(documents, stream, sort_keys=False)
    if method == 'proposed':
        mode = 'mobility-aware'
        mutate = True
    elif comparison == 'placement' and autoscaler == 'controller':
        mode = 'scaling-only'
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
    if method == 'baseline' and comparison in ('full', 'scaling'):
        hpa_path = _render_hpa(
            'autoscaler/hpa-baseline.yaml', root / 'baseline-hpa.yaml',
            app_name + '-baseline', app_name,
            config['min_replicas'], max_replicas, config['scale_down_seconds'], staircase=staircase,
            scale_down_stabilization_seconds=scale_down_stabilization_seconds,
            scale_down_pods=scale_down_pods,
            scale_down_period_seconds=scale_down_period_seconds,
        )
    elif autoscaler == 'hpa' and (method == 'proposed' or comparison == 'placement'):
        effective_factor = rt_target_factor if method == 'proposed' else 1.0
        hpa_path = _render_hpa(
            'autoscaler/hpa-proposed.yaml', root / f'{method}-hpa.yaml',
            f'{app_name}-{method}', app_name,
            config['min_replicas'], max_replicas, config['scale_down_seconds'], slo_ms=slo_ms, staircase=staircase,
            latency_floor_ms=latency_floor_ms, rt_target_factor=effective_factor,
            scale_down_stabilization_seconds=scale_down_stabilization_seconds,
            scale_down_pods=scale_down_pods,
            scale_down_period_seconds=scale_down_period_seconds,
        )
    return config, config_path, hpa_path


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


def run_method(settings, campaign, root, method, delay, slo_ms, max_replicas, debug=False,
               resource_weights=None, locality_weight=None, comparison='full',
               autoscaler='controller', placement_engine='python', deschedule_interval=None, fixed_replicas=None, min_replicas=None):
    if placement_engine not in PLACEMENT_ENGINES:
        raise ValueError(f'placement_engine must be one of {PLACEMENT_ENGINES}')
    output = root / method
    output.mkdir()
    config, config_path, hpa_path = _method_configs(root, method, slo_ms, max_replicas, debug,
                                                  resource_weights, locality_weight, selected_workload(settings),
                                                  comparison, autoscaler, deschedule_interval, fixed_replicas,
                                                  staircase=settings.get('staircase', False),
                                                  latency_floor_ms=settings.get('latency_floor_ms', 0),
                                                  rt_target_factor=settings.get('rt_target_factor', 1.0),
                                                  min_replicas=min_replicas if min_replicas is not None else settings.get('min_replicas'),
                                                  scale_down_stabilization_seconds=settings.get('scale_down_stabilization_seconds'),
                                                  scale_down_pods=settings.get('scale_down_pods'),
                                                  scale_down_period_seconds=settings.get('scale_down_period_seconds'))
    app = workload_identity(selected_workload(settings))
    processes = []
    remote_started = False
    completed = False
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
                if hpa_path is not None:
                    kube('apply', '-f', config['manifest'])
                    kube('apply', '-f', hpa_path)
                if placement_engine == 'python' and (method == 'proposed' or (comparison == 'placement' and autoscaler == 'controller')):
                    cmd = [sys.executable, '-u', '-m', 'controller', '--config', str(config_path)]
                    if hpa_path is None:
                        cmd.append('--deploy')
                    else:
                        cmd.append('--placement-only')
                    controller_process = subprocess.Popen(cmd, stdout=control_log, stderr=subprocess.STDOUT, start_new_session=True)
                    processes.append(controller_process)
                    if hpa_path is None:
                        deadline = time.monotonic() + 30
                        while time.monotonic() < deadline:
                            if controller_process.poll() is not None:
                                raise RuntimeError('controller startup failed; see controller.log')
                            check = kube('-n', NAMESPACE, 'get', 'deployment', app, '--ignore-not-found', '-o', 'name', capture_output=True, text=True)
                            if check.stdout.strip():
                                break
                            time.sleep(1)
                        else:
                            raise RuntimeError('controller did not deploy the workload')
                _wait_initial_replicas_ready(app, config['min_replicas'], method=method)
                service = get('service', app)
                check_service_routing(service)
                port = service['spec']['ports'][0]['nodePort']
                site_urls = {site: f'http://{address}:{port}/detect'
                             for site, address in settings['site_node_ips'].items()}
                if delay is not None:
                    delay.check()
                command([sys.executable, '-m', 'utils.preflight', '--config', config_path])
                sampler = subprocess.Popen([sys.executable, '-u', '-m', 'utils.monitor_resources', 'sample', str(output / 'metrics.jsonl'), '--namespace', NAMESPACE, '--app', app, '--pods-only'], stdout=sampler_log, stderr=subprocess.STDOUT, start_new_session=True)
                processes.append(sampler)
                if hpa_path is not None:
                    hpa_sampler = subprocess.Popen([sys.executable, '-u', '-m', 'utils.monitor_resources', 'hpa-sample', str(output / 'hpa-status.jsonl'), '--namespace', NAMESPACE, '--hpa', f'{app}-{method}'], stdout=sampler_log, stderr=subprocess.STDOUT, start_new_session=True)
                    processes.append(hpa_sampler)
                payload = remote_settings(settings) | {'campaign': campaign, 'method': method, 'site_urls': site_urls,
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
                    if delay is not None:
                        delay.check()
                    if any(p.poll() is not None for p in processes):
                        raise RuntimeError('controller or sampler exited during traffic; see logs')
                    if not verified:
                        prom_ok = active_telemetry(config['prometheus_url'])
                        if autoscaler != 'hpa':
                            adapter_ok = True
                        elif method == 'proposed' and (settings.get('latency_floor_ms') or settings.get('rt_metric_unit') == 'milliseconds'):
                            metric_unit = settings.get('rt_metric_unit', 'seconds')
                            adapter_ok = bool(_query_external_latency_metric(
                                allow_not_found=True,
                                metric_name=_latency_metric_name(settings.get('latency_floor_ms', 0), unit=metric_unit)))
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
                try:
                    if remote_started and not completed:
                        command(remote(settings, ['sudo', '-n', '.venv/bin/python', '-m', 'experiments.campaign_ue', 'stop', '--campaign', campaign, '--method', method]))
                        ue.wait(timeout=100)
                finally:
                    errors = []
                    for process in reversed(processes):
                        try:
                            stop_process(process)
                        except (OSError, subprocess.SubprocessError) as exc:
                            errors.append(str(exc))
                    if errors:
                        raise RuntimeError('Local process cleanup failed: ' + '; '.join(errors))
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
    transfer(settings, campaign, method, output)
    from utils.cadvisor_resources import (
        DEFAULT_FRESHNESS_S,
        DEFAULT_RATE_WINDOW_S,
        DEFAULT_SCRAPE_INTERVAL_S,
    )
    command([sys.executable, '-m', 'utils.summarize', output / 'requests.jsonl', '--slo-ms', str(slo_ms)])
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign', nargs='?', help='new result name, for example run_2')
    parser.add_argument('--workload', choices=WORKLOADS, default=None,
                        help='workload to deploy and monitor (default: campaign config or face-detection)')
    parser.add_argument('--scenario', choices=['mobility9', *SCALER_SCENARIOS],
                        help='mobility9: 9 UEs shifting across gNBs (7 phases); scaler-balanced: legacy steps; scaler-staircase: gradual balanced steps on gnb-3')
    parser.add_argument('--mobility-direction', choices=('forward', 'reverse'), default=None,
                        help='mobility9 only: UE hotspot trajectory direction (forward or reverse; default: forward)')
    parser.add_argument('--phase-transition', choices=('continuous', 'restart'), default='continuous',
                        help='continuous preserves unaffected UE traffic at phase boundaries; restart reproduces legacy runs')
    parser.add_argument('--phase-schedule',
                        help='override the scenario with explicit total:gnb1:gnb2:gnb3 phases (recorded in metadata)')
    parser.add_argument('--final-hold', action='store_true',
                        help='extend final scaling phase: +360s for legacy balanced, or to at least 600s for staircase')
    parser.add_argument('--deschedule-interval', type=float,
                        help='seconds between proposed descheduler checks (default: 30; disabled for scaling)')
    parser.add_argument('--comparison', choices=COMPARISONS, default=None,
                        help='placement: same latency autoscaler; full: CPU HPA vs full proposed; scaling: CPU HPA vs latency HPA on gnb-3')
    parser.add_argument('--autoscaler', choices=AUTOSCALERS, default='hpa',
                        help='proposed latency autoscaler engine: hpa (Kubernetes HPA + prometheus-adapter) or controller (Python fallback)')
    parser.add_argument('--placement-engine', choices=PLACEMENT_ENGINES, default='go',
                        help='scheduler/descheduler runtime for proposed method: go (in-cluster Deployment) or python (host fallback)')
    parser.add_argument('--plan-only', type=Path, metavar='DIRECTORY',
                        help='write workload/controller/HPA manifests locally without any probes or cluster access')
    parser.add_argument('--config', default=str(ROOT / 'experiments/campaign.yaml'))
    parser.add_argument('--check', action='store_true', help='dependency checks plus temporary workload and UE connectivity probes')
    parser.add_argument('--tc-ms', type=float, default=DEFAULT_TC_MS,
                        help='workload link shaping one-way delay in milliseconds (default: 10)')
    parser.add_argument('--slo-ms', type=float,
                        help='reporting p90 SLO threshold in milliseconds (also base RT target before --rt-target-factor; default: 150)')
    parser.add_argument('--rt-target-factor', type=float, default=1.0,
                        help='experimental headroom multiplier in (0, 1] applied only to proposed RT autoscaler internal target (default: 1.0)')
    parser.add_argument('--latency-floor-ms', type=int, default=0,
                        help='staircase only: fixed reference latency in whole milliseconds; 0 keeps raw RT/SLO')
    parser.add_argument('--fixed-replicas', type=int,
                        help='fixed replica count for both mobility9 methods; clamps HPA min/max and initial Deployment replicas')
    parser.add_argument('--min-replicas', type=int,
                        help='shared baseline HPA and proposed minimum and initial replica count (default: 3)')
    parser.add_argument('--max-replicas', type=int,
                        help='shared baseline HPA and proposed replica ceiling (default: 24 for scaling, config.yaml otherwise)')
    parser.add_argument('--scale-down-stabilization-seconds', type=int,
                        help='shared HPA scale-down stabilization window in seconds (default: 60 for staircase, config.yaml otherwise)')
    parser.add_argument('--scale-down-pods', type=int,
                        help='shared HPA scale-down pods per period (default: 1 for staircase)')
    parser.add_argument('--scale-down-period-seconds', type=int,
                        help='shared HPA scale-down period in seconds (default: 30 for staircase)')
    parser.add_argument('--method-order', choices=('baseline-first', 'proposed-first', 'random'),
                        default='baseline-first',
                        help='order in which baseline and proposed run (default: baseline-first)')
    parser.add_argument('--debug', action='store_true',
                        help='60s phases and shorter controller/HPA timers for debugging; separate from evaluation runs')
    duration_options = parser.add_mutually_exclusive_group()
    duration_options.add_argument('--long-run', action='store_true',
                                  help='mobility9 only: use 120s phases (default for mobility9; kept for compatibility)')
    duration_options.add_argument('--phase-duration', type=float,
                                  help='traffic seconds per phase (minimum 60; default: 120 for all full experiments; joint reads campaign config; 60 with --debug)')
    parser.add_argument('--rps-per-ue', type=float, default=DEFAULT_RPS_PER_UE,
                        help='target request arrival rate per active UE in requests/second (default: 5)')
    parser.add_argument('--max-in-flight-per-ue', type=int, default=DEFAULT_MAX_IN_FLIGHT_PER_UE,
                        help='maximum concurrent in-flight requests per UE (default: 8)')
    parser.add_argument('--resource-weights', nargs=4, type=float,
                        metavar=('CPU', 'MEMORY', 'GPU', 'VRAM'),
                        help='proposed resource-score weights, nonnegative and summing to 1')
    parser.add_argument('--locality-weight', type=float,
                        help='proposed traffic-share weight between 0 and 1 (default: config.yaml)')
    args = parser.parse_args(argv)
    if args.long_run:
        if args.scenario != 'mobility9':
            parser.error('--long-run requires --scenario mobility9')
        args.phase_duration = MOBILITY_PHASE_SECONDS
    if args.mobility_direction is not None and args.scenario != 'mobility9':
        parser.error('--mobility-direction requires --scenario mobility9')
    mobility_direction = (args.mobility_direction or 'forward') if args.scenario == 'mobility9' else None
    if args.fixed_replicas is not None:
        if args.fixed_replicas < 1:
            parser.error('--fixed-replicas must be a positive integer')
        if args.scenario != 'mobility9':
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
    if args.scenario in SCALER_SCENARIOS and args.comparison not in (None, 'scaling'):
        parser.error('scaling scenarios require --comparison scaling')
    if args.comparison == 'scaling' and args.scenario not in (None, *SCALER_SCENARIOS):
        parser.error('--comparison scaling requires scaler-balanced or scaler-staircase')
    if args.comparison == 'scaling' and args.scenario is None:
        args.scenario = 'scaler-balanced'
    args.comparison = args.comparison or (
        'scaling' if args.scenario in SCALER_SCENARIOS else
        'placement' if args.scenario else
        'full'
    )
    if args.final_hold and args.scenario not in SCALER_SCENARIOS:
        parser.error('--final-hold requires a scaling scenario')
    if args.scenario == 'mobility9':
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
    if args.latency_floor_ms < 0:
        parser.error('--latency-floor-ms must be nonnegative')
    if args.latency_floor_ms and args.scenario != 'scaler-staircase':
        parser.error('--latency-floor-ms requires --scenario scaler-staircase')
    if args.max_replicas is not None and args.max_replicas < 1:
        parser.error('--max-replicas must be a positive integer')
    if args.phase_duration is not None and (not math.isfinite(args.phase_duration) or args.phase_duration < 60):
        parser.error('--phase-duration must be finite and at least 60 seconds')
    if not math.isfinite(args.rps_per_ue) or args.rps_per_ue <= 0:
        parser.error('--rps-per-ue must be finite and positive')
    if args.max_in_flight_per_ue < 1:
        parser.error('--max-in-flight-per-ue must be a positive integer')
    os.chdir(ROOT)
    os.environ.setdefault('KUBECONFIG', '/etc/rancher/k3s/k3s.yaml')
    settings = load(args.config)
    settings['rps_per_ue'] = float(args.rps_per_ue)
    settings['max_in_flight_per_ue'] = args.max_in_flight_per_ue
    settings['phase_transition'] = args.phase_transition
    if args.scenario == 'mobility9':
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
        if args.scenario != 'mobility9':
            settings['schedule'] = args.phase_schedule
    workload = args.workload if args.workload is not None else selected_workload(settings)
    settings['workload'] = workload
    settings['staircase'] = args.scenario == 'scaler-staircase'
    settings['latency_floor_ms'] = args.latency_floor_ms
    settings['rt_target_factor'] = float(args.rt_target_factor)
    if args.scale_down_stabilization_seconds is not None:
        settings['scale_down_stabilization_seconds'] = args.scale_down_stabilization_seconds
    if args.scale_down_pods is not None:
        settings['scale_down_pods'] = args.scale_down_pods
    if args.scale_down_period_seconds is not None:
        settings['scale_down_period_seconds'] = args.scale_down_period_seconds
    if args.workload is not None and args.workload != 'face-detection':
        # Separate config and an explicit SLO prevent accidental reuse of face calibration.
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
            dict(zip(('cpu', 'memory', 'gpu', 'vram'), args.resource_weights))
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
                            if args.latency_floor_ms else internal_target_dec)
    effective_metric_target_ms = _decimal_to_number(effective_target_dec)
    metric_unit = _metric_unit_for_target(effective_target_dec) if args.autoscaler == 'hpa' else 'seconds'
    metric_name = _latency_metric_name(args.latency_floor_ms, unit=metric_unit)
    hpa_target_value = _format_hpa_target(effective_target_dec, unit=metric_unit)
    settings['rt_metric_unit'] = metric_unit
    if args.latency_floor_ms and internal_target_dec <= Decimal(str(args.latency_floor_ms)):
        if args.rt_target_factor == 1.0:
            parser.error('--latency-floor-ms must be smaller than --slo-ms')
        parser.error('internal RT target (--slo-ms * --rt-target-factor) must be greater than --latency-floor-ms')
    default_max = SCALER_DEFAULT_MAX_REPLICAS if args.comparison == 'scaling' else controller_config['max_replicas']
    max_replicas = args.max_replicas if args.max_replicas is not None else default_max
    if args.fixed_replicas is not None:
        max_replicas = args.fixed_replicas
    min_replicas = (args.fixed_replicas if args.fixed_replicas is not None
                    else args.min_replicas if args.min_replicas is not None
                    else DEFAULT_MIN_REPLICAS)
    if args.fixed_replicas is None and max_replicas < min_replicas:
        parser.error('--max-replicas must be at least min_replicas')
    if args.fixed_replicas is None:
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
        {'baseline': {'type': 'cpu_utilization_percent', 'target': 60},
         'proposed': proposed_latency_target}
        if args.comparison == 'scaling' else
        {'baseline': {'type': 'latency_p90_ms', 'target': slo_ms},
         'proposed': proposed_latency_target}
        if args.comparison == 'placement' else
        {'baseline': {'type': 'cpu_utilization_percent', 'target': 60},
         'proposed': proposed_latency_target}
    )
    if args.latency_floor_ms:
        hpa_targets['proposed'] = {
            'type': 'excess_latency_p90_ms', 'target': effective_metric_target_ms,
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
    if args.rt_target_factor == 1.0:
        scaling_formula = ('max(RT-floor, 0)/(SLO-floor)' if args.latency_floor_ms
                           else 'RT/SLO (unchanged; rounding can retain excess replicas)')
    else:
        scaling_formula = ('max(RT-floor, 0)/(internal_target-floor)' if args.latency_floor_ms
                           else 'RT/internal_target')
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
            else (60 if settings['staircase'] else int(controller_config['scale_down_seconds']))
        ),
        'pods': (
            args.scale_down_pods
            if args.scale_down_pods is not None
            else (1 if settings['staircase'] else None)
        ),
        'period_seconds': (
            args.scale_down_period_seconds
            if args.scale_down_period_seconds is not None
            else (30 if settings['staircase'] else None)
        ),
    }
    provenance = {
        'core_git': _local_git_provenance(),
        'ue_git': None,
        'kubernetes_server_version': None,
        'kube_proxy_mode': None,
        'workload_image': _workload_image_ref(workload),
        'method_order': method_order,
    }
    shared_metadata = {
        'workload': workload,
        'response_contract': 'detection',
        'response_log_schema_version': 1,
        'http_threads': _workload_http_threads(workload),
        'phase_transition': args.phase_transition,
        'comparison': args.comparison,
        'scenario': args.scenario,
        'mobility_direction': mobility_direction,
        'fixed_replicas': args.fixed_replicas,
        'deschedule_interval_seconds': args.deschedule_interval,
        'autoscaler': args.autoscaler,
        'placement_engine': None if args.comparison == 'scaling' else args.placement_engine,
        'attribution_note': COMPARISON_NOTES[args.comparison],
        'slo_ms': slo_ms,
        'reporting_slo_ms': slo_ms,
        'rt_target_factor': float(args.rt_target_factor),
        'internal_rt_target_ms': internal_target_ms,
        'effective_rt_metric_target_ms': effective_metric_target_ms,
        'rt_metric_name': metric_name,
        'rt_metric_unit': metric_unit,
        'rt_hpa_target_value': hpa_target_value,
        'schedule': settings.get('schedule'),
        'phase_duration': settings.get('phase_duration'),
        'long_run': args.long_run,
        'phase_durations_s': phase_durations,
        'hpa_behavior_profile': 'bounded-staircase' if settings['staircase'] else 'original',
        'hpa_scale_down': effective_scale_down,
        'method_order_requested': args.method_order,
        'method_order': method_order,
        'provenance': provenance,
        'latency_floor_ms': args.latency_floor_ms,
        'latency_floor_source': 'fixed exploratory reference; not an automatically measured physical minimum' if args.latency_floor_ms else None,
        'latency_scaling_formula': scaling_formula,
        'analysis_windows': {'whole_phase': True, 'late_phase_seconds': 60, 'steady_state_assumed': False},
        'final_hold_seconds': hold_seconds,
        'node_restriction': SCALER_NODE if args.comparison == 'scaling' else None,
        'hpa_targets': hpa_targets,
        'replica_bounds': {'min': min_replicas, 'max': max_replicas},
        'pacing': {'rps_per_ue': float(args.rps_per_ue), 'max_in_flight_per_ue': args.max_in_flight_per_ue},
        'tc_ms': args.tc_ms,
        'resource_measurement': {
            'source': 'prometheus-cadvisor',
            'scrape_interval_seconds': 5,
            'rate_window_seconds': 45,
            'freshness_seconds': 25,
            'metrics_server_hpa_unchanged': True,
        },
    }
    if not args.check and args.plan_only is None and (not args.campaign or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', args.campaign)):
        parser.error('provide a new campaign name using letters, digits, hyphens or underscores')
    if args.plan_only is not None:
        output = args.plan_only.resolve()
        output.mkdir(parents=True, exist_ok=False)
        _campaign_manifest(output, args.tc_ms is not None, workload)
        for method in method_order:
            _method_configs(output, method, slo_ms, max_replicas, args.debug,
                            resource_weights, locality_weight, workload, args.comparison, args.autoscaler, args.deschedule_interval, args.fixed_replicas,
                            staircase=settings['staircase'], latency_floor_ms=args.latency_floor_ms,
                            rt_target_factor=args.rt_target_factor, min_replicas=args.min_replicas,
                            scale_down_stabilization_seconds=args.scale_down_stabilization_seconds,
                            scale_down_pods=args.scale_down_pods,
                            scale_down_period_seconds=args.scale_down_period_seconds)
        if args.latency_floor_ms or metric_unit == 'milliseconds':
            adapter_filename = 'floor-adapter-configmap.yaml' if args.latency_floor_ms else 'latency-adapter-configmap.yaml'
            (output / adapter_filename).write_text(
                yaml.safe_dump(_floor_adapter_config(args.latency_floor_ms, unit=metric_unit), sort_keys=False))
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
        prerequisites(settings | {'_autoscaler': args.autoscaler, '_placement_engine': args.placement_engine, '_comparison': args.comparison})
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
        print(f'Campaign: {len(phases)} phases per method, {duration_desc}; '
              f'{minutes:g} minutes traffic per method ({2 * minutes:g} minutes total), '
              'plus UE switching, setup and transfer. Objective: all-attempt p90 '
              f'<= {slo_ms:g} ms. Shared replica ceiling: {max_replicas}.', flush=True)
        if args.comparison != 'scaling':
            print(f'Proposed scoring: resource weights={resource_weights}; '
                  f'locality weight={locality_weight:g}; resource contribution={1-locality_weight:g}.', flush=True)
        if args.scenario == 'mobility9':
            trajectory = ' -> '.join(':'.join(str(p[f'gnb{s}_ues']) for s in (1, 2, 3)) for p in phases)
            print(f'Mobility scenario ({mobility_direction}): UE splits {trajectory}. '
                  f'Descheduler checks every {args.deschedule_interval:g}s; evictions require settled Pods and positive score improvement. '
                  'UE moves use stop-and-reattach, not seamless handover.', flush=True)
        elif args.scenario in SCALER_SCENARIOS:
            target_desc = (f'internal scaling target {internal_target_ms:g} ms p90 = {slo_ms:g} ms * {args.rt_target_factor:g}; '
                           f'reporting SLO {slo_ms:g} ms'
                           if args.rt_target_factor != 1.0
                           else f'scaling target {internal_target_ms:g} ms p90; reporting SLO {slo_ms:g} ms')
            ue_counts = ' -> '.join(str(p['workload_ue']) for p in phases)
            print(f'Scaling scenario: UE counts {ue_counts}; workload on {SCALER_NODE} with default-scheduler; '
                  f'CPU HPA (60%) vs latency HPA ({target_desc}), replicas {min_replicas}..{max_replicas}.', flush=True)
        if args.fixed_replicas is not None:
            print(f'Fixed placement experiment: {args.fixed_replicas} replicas per method; '
                  'initial Deployment and HPA min/max are equal. Eviction may temporarily reduce readiness.', flush=True)
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
        try:
            if delay is not None:
                delay.start()
            _campaign_manifest(root, delay is not None, workload)
            if args.comparison == 'scaling':
                reset(workload)
                check_node_capacity(SCALER_NODE, root / 'workload.yaml', max_replicas, workload_identity(workload))
            if args.latency_floor_ms:
                print(f'Exploratory floor-adjusted HPA: fixed reference {args.latency_floor_ms}ms, '
                      f'internal RT scaling target {internal_target_ms:g}ms (factor {args.rt_target_factor:g}), '
                      f'excess target {effective_metric_target_ms:g}ms (metric {metric_name}, target {hpa_target_value}). '
                      f'Reporting SLO remains {slo_ms:g}ms.', flush=True)
                _prepare_floor_adapter(root, args.latency_floor_ms, unit=metric_unit)
            elif args.rt_target_factor != 1.0 or metric_unit == 'milliseconds':
                print(f'Proposed RT HPA: internal scaling target {internal_target_ms:g}ms '
                      f'(reporting SLO {slo_ms:g}ms * factor {args.rt_target_factor:g}; '
                      f'metric {metric_name}, target {hpa_target_value}). '
                      f'Reporting SLO remains {slo_ms:g}ms.', flush=True)
                if metric_unit == 'milliseconds':
                    _prepare_floor_adapter(root, 0, unit=metric_unit)
            for method in method_order:
                print(f'=== {method.upper()} ===', flush=True)
                reset(workload)
                run_method(settings, args.campaign, root, method, delay, slo_ms, max_replicas, args.debug,
                           resource_weights, locality_weight, args.comparison,
                           args.autoscaler, args.placement_engine, args.deschedule_interval, args.fixed_replicas,
                           min_replicas=args.min_replicas)
            command([sys.executable, '-m', 'utils.plot_autoscaler', root, root / 'plots', '--slo-ms', str(slo_ms)])
        finally:
            try:
                reset(workload)
            finally:
                if delay is not None:
                    delay.close()
        print(f'Campaign complete. Raw data, logs and plots: {root}', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        raise SystemExit(f'Campaign stopped: {exc}') from exc
