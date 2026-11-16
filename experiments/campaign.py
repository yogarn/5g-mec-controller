"""Run the matched gNB3-hotspot campaign from core, including remote UEs and plots."""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import ipaddress
import math
import json
import os
from pathlib import Path
from dataclasses import replace
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
MOBILITY9_SCHEDULE = '9:3:3:3,9:7:1:1,9:1:7:1,9:1:1:7,9:1:7:1,9:7:1:1,9:3:3:3'
SCALER_BALANCED_SCHEDULE = '3:1:1:1,9:3:3:3,15:5:5:5,24:8:8:8,15:5:5:5,9:3:3:3,3:1:1:1'
SCALER_NODE = 'gnb-3'
SCALER_MIN_REPLICAS = 1
SCALER_DEFAULT_MAX_REPLICAS = 24
FINAL_HOLD_SECONDS = 360
COMPARISONS = ('full', 'placement', 'scaling')
AUTOSCALERS = ('controller', 'hpa')
PLACEMENT_ENGINES = ('python', 'go')
PLACEMENT_NAMESPACE = 'placement'
PLACEMENT_PROMETHEUS_URL = 'http://placement-prometheus.placement.svc:9090'
COMPARISON_NOTES = {
    'placement': 'Comparison A (placement): same latency-ratio autoscaler for both methods; isolates scheduler/descheduler placement.',
    'full': 'Comparison B (full system): default-scheduler + CPU HPA vs full proposed system (autoscaler + scheduler + descheduler); do not attribute results solely to placement.',
    'scaling': 'Comparison C (scaling): default-scheduler + gnb-3-only required affinity for both methods; native CPU HPA (60% target) vs native latency HPA (100 ms p90 target).',
}


def workload_identity(workload='face-detection'):
    if workload not in WORKLOADS:
        raise ValueError('unknown workload')
    return 'mec-placement-' + workload


def workload_manifest(workload='face-detection'):
    workload_identity(workload)
    return ROOT / 'apps' / workload / 'manifest.yaml'


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



def _render_hpa(template_name, output_path, hpa_name, app_name, min_replicas, max_replicas, scale_down_seconds, slo_ms=None):
    hpa = yaml.safe_load((ROOT / template_name).read_text())
    hpa['metadata']['name'] = hpa_name
    hpa['spec']['scaleTargetRef']['name'] = app_name
    hpa['spec'].update(minReplicas=min_replicas, maxReplicas=max_replicas)
    hpa['spec']['behavior']['scaleDown']['stabilizationWindowSeconds'] = int(scale_down_seconds)
    if slo_ms is not None:
        hpa['spec']['metrics'][0]['external']['target']['value'] = f'{int(round(slo_ms))}m'
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


def _query_external_latency_metric(allow_not_found=False):
    """Query Kubernetes external metrics API for ue_p90_latency_seconds via prometheus-adapter."""
    path = f'/apis/external.metrics.k8s.io/v1beta1/namespaces/{NAMESPACE}/ue_p90_latency_seconds?labelSelector=job%3Due-telemetry'
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


def _method_configs(root, method, slo_ms, max_replicas, debug=False, resource_weights=None, locality_weight=None, workload='face-detection', comparison='full', autoscaler='controller', deschedule_interval=None, fixed_replicas=None):
    """Persist the exact matched replica budget and manifests used by a method."""
    if comparison not in COMPARISONS:
        raise ValueError(f'comparison must be one of {COMPARISONS}')
    if autoscaler not in AUTOSCALERS:
        raise ValueError(f'autoscaler must be one of {AUTOSCALERS}')
    config = yaml.safe_load((ROOT / 'config.yaml').read_text())
    if fixed_replicas is not None:
        if type(fixed_replicas) is not int or fixed_replicas < 1:
            raise ValueError('fixed replicas must be a positive integer')
        config['min_replicas'] = max_replicas = fixed_replicas
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
        deployments[0]['spec']['replicas'] = SCALER_MIN_REPLICAS
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
        config['min_replicas'] = SCALER_MIN_REPLICAS
    if fixed_replicas is not None:
        deployments[0]['spec']['replicas'] = fixed_replicas
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
            config['min_replicas'], max_replicas, config['scale_down_seconds'],
        )
    elif autoscaler == 'hpa' and (method == 'proposed' or comparison == 'placement'):
        hpa_path = _render_hpa(
            'autoscaler/hpa-proposed.yaml', root / f'{method}-hpa.yaml',
            f'{app_name}-{method}', app_name,
            config['min_replicas'], max_replicas, config['scale_down_seconds'], slo_ms=slo_ms,
        )
    return config, config_path, hpa_path


def run_method(settings, campaign, root, method, delay, slo_ms, max_replicas, debug=False,
               resource_weights=None, locality_weight=None, comparison='full',
               autoscaler='controller', placement_engine='python', deschedule_interval=None, fixed_replicas=None):
    if placement_engine not in PLACEMENT_ENGINES:
        raise ValueError(f'placement_engine must be one of {PLACEMENT_ENGINES}')
    output = root / method
    output.mkdir()
    config, config_path, hpa_path = _method_configs(root, method, slo_ms, max_replicas, debug,
                                                  resource_weights, locality_weight, selected_workload(settings),
                                                  comparison, autoscaler, deschedule_interval, fixed_replicas)
    app = workload_identity(selected_workload(settings))
    processes = []
    remote_started = False
    completed = False
    incluster_go = comparison != 'scaling' and placement_engine == 'go' and method == 'proposed'
    method_start_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    with (output / 'controller.log').open('x') as control_log, (output / 'sampler.log').open('x') as sampler_log, (output / 'runner-ssh.log').open('x') as runner_log:
        try:
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
                processes.append(subprocess.Popen(cmd, stdout=control_log, stderr=subprocess.STDOUT, start_new_session=True))
                if hpa_path is None:
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        if processes[0].poll() is not None:
                            raise RuntimeError('controller startup failed; see controller.log')
                        check = kube('-n', NAMESPACE, 'get', 'deployment', app, '--ignore-not-found', '-o', 'name', capture_output=True, text=True)
                        if check.stdout.strip():
                            break
                        time.sleep(1)
                    else:
                        raise RuntimeError('controller did not deploy the workload')
            kube('-n', NAMESPACE, 'rollout', 'status', 'deployment/' + app, '--timeout=180s')
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
            payload = remote_settings(settings) | {'campaign': campaign, 'method': method, 'site_urls': site_urls,
                       'schedule': settings['schedule'], 'duration': settings['phase_duration']}
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
                    adapter_ok = True if autoscaler != 'hpa' else bool(_query_external_latency_metric(allow_not_found=True))
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
    with (output / 'deployments-after.json').open('x') as stream:
        kube('-n', NAMESPACE, 'get', 'deployments,pods,hpa', '-o', 'json', stdout=stream)
    transfer(settings, campaign, method, output)
    command([sys.executable, '-m', 'utils.summarize', output / 'requests.jsonl', '--slo-ms', str(slo_ms)])
    command([sys.executable, '-m', 'utils.monitor_resources', 'summarize', output / 'metrics.jsonl', output / 'requests.jsonl', output / 'summary.csv', output, '--skip-resource-csv'])
    command([sys.executable, '-m', 'utils.cadvisor_resources', 'run', output / 'requests.jsonl', output / 'summary.csv', output, '--metrics-jsonl', output / 'metrics.jsonl', '--prometheus-url', config['prometheus_url'], '--namespace', NAMESPACE, '--pod-prefix', app + '-'])
    command([sys.executable, '-m', 'utils.telemetry_history', output / 'requests.jsonl', output, '--prometheus-url', config['prometheus_url']])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign', nargs='?', help='new result name, for example run_2')
    parser.add_argument('--workload', choices=WORKLOADS, default=None,
                        help='workload to deploy and monitor (default: campaign config or face-detection)')
    parser.add_argument('--scenario', choices=['mobility9', 'scaler-balanced'],
                        help='mobility9: 9 UEs rotating hotspots; scaler-balanced: balanced 3..24..3 UEs on gnb-3 only')
    parser.add_argument('--final-hold', action='store_true',
                        help='extend the final scaler-balanced phase by 360s (420s total) to observe HPA scale-down')
    parser.add_argument('--deschedule-interval', type=float,
                        help='seconds between proposed descheduler checks (mobility9 default: 30; otherwise config.yaml)')
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
    parser.add_argument('--tc-ms', type=float, help='enable workload link shaping at this one-way delay in milliseconds')
    parser.add_argument('--slo-ms', type=float, help='controller p90 target and reporting threshold in milliseconds (default: config.yaml)')
    parser.add_argument('--fixed-replicas', type=int,
                        help='fixed replica count for both mobility9 methods; clamps HPA min/max and initial Deployment replicas')
    parser.add_argument('--max-replicas', type=int,
                        help='shared baseline HPA and proposed replica ceiling (default: config.yaml)')
    parser.add_argument('--debug', action='store_true',
                        help='60s phases and shorter controller/HPA timers for debugging; separate from evaluation runs')
    parser.add_argument('--phase-duration', type=float,
                        help='traffic seconds per phase (minimum 60; default: campaign config, or 60 with --debug)')
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
    if args.fixed_replicas is not None:
        if args.fixed_replicas < 1:
            parser.error('--fixed-replicas must be a positive integer')
        if args.scenario != 'mobility9':
            parser.error('--fixed-replicas requires --scenario mobility9')
        if args.max_replicas is not None:
            parser.error('use --fixed-replicas without --max-replicas')
    if args.scenario == 'scaler-balanced' and args.comparison not in (None, 'scaling'):
        parser.error('--scenario scaler-balanced requires --comparison scaling')
    if args.comparison == 'scaling' and args.scenario not in (None, 'scaler-balanced'):
        parser.error('--comparison scaling requires --scenario scaler-balanced')
    if args.comparison == 'scaling' and args.scenario is None:
        args.scenario = 'scaler-balanced'
    args.comparison = args.comparison or (
        'scaling' if args.scenario == 'scaler-balanced' else
        'placement' if args.scenario else
        'full'
    )
    if args.final_hold and args.scenario != 'scaler-balanced':
        parser.error('--final-hold requires --scenario scaler-balanced')
    if args.scenario == 'mobility9':
        if args.debug:
            parser.error('mobility9 uses standard HPA timers; omit --debug')
        if args.autoscaler != 'hpa' or args.placement_engine != 'go':
            parser.error('mobility9 requires native HPA and Go placement')
        if args.slo_ms is None:
            args.slo_ms = 100
        if args.deschedule_interval is None:
            args.deschedule_interval = 30
    elif args.scenario == 'scaler-balanced':
        if args.debug:
            parser.error('scaler-balanced uses standard HPA timers; omit --debug')
        if args.autoscaler != 'hpa':
            parser.error('scaler-balanced requires native HPA (--autoscaler hpa)')
        if args.placement_engine != 'go':
            parser.error('scaler-balanced does not use a custom placement engine; omit --placement-engine python')
        if args.deschedule_interval is not None:
            parser.error('scaler-balanced does not use the descheduler; omit --deschedule-interval')
        if args.resource_weights is not None or args.locality_weight is not None:
            parser.error('scaler-balanced does not use custom placement scoring')
        if args.slo_ms is None:
            args.slo_ms = 100
    if args.deschedule_interval is not None and (not math.isfinite(args.deschedule_interval) or args.deschedule_interval <= 0):
        parser.error('--deschedule-interval must be finite and positive')
    if args.tc_ms is not None and (not math.isfinite(args.tc_ms) or args.tc_ms < 0.001):
        parser.error('--tc-ms must be finite and at least 0.001 ms')
    if args.slo_ms is not None and (not math.isfinite(args.slo_ms) or args.slo_ms <= 0):
        parser.error('--slo-ms must be finite and positive')
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
    if args.scenario == 'mobility9':
        settings.update(schedule=MOBILITY9_SCHEDULE, phase_duration=60)
    elif args.scenario == 'scaler-balanced':
        settings.update(schedule=SCALER_BALANCED_SCHEDULE, phase_duration=60)
    workload = args.workload if args.workload is not None else selected_workload(settings)
    settings['workload'] = workload
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
    hold_seconds = FINAL_HOLD_SECONDS if args.final_hold else 0
    if args.scenario == 'scaler-balanced':
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
    slo_ms = args.slo_ms if args.slo_ms is not None else controller_config['slo_seconds'] * 1000
    default_max = SCALER_DEFAULT_MAX_REPLICAS if args.comparison == 'scaling' else controller_config['max_replicas']
    max_replicas = args.max_replicas if args.max_replicas is not None else default_max
    if args.fixed_replicas is not None:
        max_replicas = args.fixed_replicas
    min_replicas = (args.fixed_replicas if args.fixed_replicas is not None
                    else SCALER_MIN_REPLICAS if args.comparison == 'scaling'
                    else controller_config['min_replicas'])
    if args.fixed_replicas is None and max_replicas < min_replicas:
        parser.error('--max-replicas must be at least min_replicas')
    hpa_targets = (
        {'baseline': {'type': 'cpu_utilization_percent', 'target': 60},
         'proposed': {'type': 'latency_p90_ms', 'target': slo_ms}}
        if args.comparison == 'scaling' else
        {'baseline': {'type': 'latency_p90_ms', 'target': slo_ms},
         'proposed': {'type': 'latency_p90_ms', 'target': slo_ms}}
        if args.comparison == 'placement' else
        {'baseline': {'type': 'cpu_utilization_percent', 'target': 60},
         'proposed': {'type': 'latency_p90_ms' if args.autoscaler == 'hpa' else 'controller_latency_p90_ms', 'target': slo_ms}}
    )
    shared_metadata = {
        'workload': workload,
        'comparison': args.comparison,
        'scenario': args.scenario,
        'fixed_replicas': args.fixed_replicas,
        'deschedule_interval_seconds': args.deschedule_interval,
        'autoscaler': args.autoscaler,
        'placement_engine': None if args.comparison == 'scaling' else args.placement_engine,
        'attribution_note': COMPARISON_NOTES[args.comparison],
        'slo_ms': slo_ms,
        'schedule': settings.get('schedule'),
        'phase_duration': settings.get('phase_duration'),
        'phase_durations_s': phase_durations,
        'final_hold_seconds': hold_seconds,
        'node_restriction': SCALER_NODE if args.comparison == 'scaling' else None,
        'hpa_targets': hpa_targets,
        'replica_bounds': {'min': min_replicas, 'max': max_replicas},
        'pacing': {'rps_per_ue': float(args.rps_per_ue), 'max_in_flight_per_ue': args.max_in_flight_per_ue},
        'tc_ms': args.tc_ms,
        'resource_measurement': {
            'source': 'prometheus-cadvisor',
            'scrape_interval_seconds': 5,
            'rate_window_seconds': 30,
            'metrics_server_hpa_unchanged': True,
        },
    }
    if not args.check and args.plan_only is None and (not args.campaign or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', args.campaign)):
        parser.error('provide a new campaign name using letters, digits, hyphens or underscores')
    if args.plan_only is not None:
        output = args.plan_only.resolve()
        output.mkdir(parents=True, exist_ok=False)
        _campaign_manifest(output, args.tc_ms is not None, workload)
        for method in ('baseline', 'proposed'):
            _method_configs(output, method, slo_ms, max_replicas, args.debug,
                            resource_weights, locality_weight, workload, args.comparison, args.autoscaler, args.deschedule_interval, args.fixed_replicas)
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
        with (root / 'campaign.yaml').open('x') as stream:
            yaml.safe_dump(settings | shared_metadata | {
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
            print('Mobility scenario: fixed nine UEs; hotspot order gNB-1 -> gNB-2 -> gNB-3 -> gNB-2 -> gNB-1. '
                  f'Descheduler checks every {args.deschedule_interval:g}s; evictions require settled Pods and positive score improvement. '
                  'UE moves use stop-and-reattach, not handover.', flush=True)
        elif args.scenario == 'scaler-balanced':
            print(f'Scaling scenario: balanced UEs (3..24..3) on {SCALER_NODE} with default-scheduler; '
                  f'CPU HPA (60%) vs latency HPA ({slo_ms:g} ms p90), replicas {min_replicas}..{max_replicas}.', flush=True)
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
            for method in ('baseline', 'proposed'):
                print(f'=== {method.upper()} ===', flush=True)
                reset(workload)
                run_method(settings, args.campaign, root, method, delay, slo_ms, max_replicas, args.debug,
                           resource_weights, locality_weight, args.comparison,
                           args.autoscaler, args.placement_engine, args.deschedule_interval, args.fixed_replicas)
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
