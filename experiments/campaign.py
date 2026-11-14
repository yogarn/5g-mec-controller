"""Run the matched gNB3-hotspot campaign from core, including remote UEs and plots."""
import argparse
import fcntl
import hashlib
import ipaddress
import math
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tarfile
import time
from urllib.parse import urlencode
from urllib.request import urlopen

import yaml
from benchmark.client import parse_schedule
from experiments.traffic_control import POOL_NAME, TrafficDelay

ROOT = Path(__file__).resolve().parent.parent
NAMESPACE = 'services'
APP = 'mec-placement-face-detection'


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
        raise RuntimeError('face-detection Service requires cluster-wide routing without locality preference or session affinity')


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
        'max_ues': max(p['workload_ue'] for p in parse_schedule(settings['schedule']))}


def load(path):
    data = yaml.safe_load(Path(path).read_text())
    expected = {'ue_host', 'ue_project', 'inventory', 'nr_ue', 'token', 'image', 'site_node_ips',
                'telemetry_url', 'schedule', 'phase_duration'}
    if not isinstance(data, dict) or set(data) != expected:
        raise ValueError('campaign configuration keys must be: ' + ', '.join(sorted(expected)))
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


def prerequisites(settings):
    import matplotlib
    from controller.config import load_settings
    config = load_settings(ROOT / 'config.yaml')
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
    from experiments.preflight import collector_status
    collector_status(config)
    for manifest in ('apps/face-detection/manifest.yaml', 'experiments/hpa-baseline.yaml'):
        docs = [d for d in yaml.safe_load_all((ROOT / manifest).read_text()) if d]
        if any(d['metadata'].get('namespace') != NAMESPACE for d in docs):
            raise ValueError(f'{manifest} must use the {NAMESPACE} namespace')
    print('Prerequisites passed. App stays in services; collector stays in placement.', flush=True)


def reset():
    """Delete only the experiment app, its HPA and dependent managed groups."""
    kube('-n', NAMESPACE, 'delete', 'hpa/mec-placement-face-detection-baseline',
         'deployment/' + APP, 'service/' + APP, '--ignore-not-found=true',
         '--cascade=foreground', '--wait=true', '--timeout=180s')
    pods = json.loads(kube('-n', NAMESPACE, 'get', 'pods', '-l', 'app=' + APP,
                          '-o', 'json', capture_output=True, text=True).stdout)['items']
    if pods:
        kube('-n', NAMESPACE, 'wait', '--for=delete', 'pod', '-l', 'app=' + APP,
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

def _campaign_manifest(root, use_tc):
    """Write one shared workload manifest for both methods."""
    documents = list(yaml.safe_load_all((ROOT / 'apps/face-detection/manifest.yaml').read_text()))
    deployments = [item for item in documents if item and item.get('kind') == 'Deployment']
    if len(deployments) != 1:
        raise ValueError('application manifest requires exactly one Deployment')
    if use_tc:
        metadata = deployments[0]['spec']['template'].setdefault('metadata', {})
        metadata.setdefault('annotations', {})['cni.projectcalico.org/ipv4pools'] = json.dumps([POOL_NAME])
    path = root / 'workload.yaml'
    with path.open('x') as stream:
        yaml.safe_dump_all(documents, stream, sort_keys=False)
    return path



def _method_configs(root, method, slo_ms, max_replicas):
    """Persist the exact matched replica budget and manifests used by a method."""
    config = yaml.safe_load((ROOT / 'config.yaml').read_text())
    if type(max_replicas) is not int or max_replicas < config['min_replicas']:
        raise ValueError('max replicas must be an integer at least min_replicas')
    config.update(manifest=str(root / 'workload.yaml'), mutate=method == 'proposed',
                  slo_seconds=slo_ms / 1000, max_replicas=max_replicas)
    config_path = root / f'{method}-controller.yaml'
    with config_path.open('x') as stream:
        yaml.safe_dump(config, stream, sort_keys=False)
    hpa_path = None
    if method == 'baseline':
        hpa = yaml.safe_load((ROOT / 'experiments/hpa-baseline.yaml').read_text())
        hpa['spec'].update(minReplicas=config['min_replicas'], maxReplicas=max_replicas)
        hpa_path = root / 'baseline-hpa.yaml'
        with hpa_path.open('x') as stream:
            yaml.safe_dump(hpa, stream, sort_keys=False)
    return config, config_path, hpa_path


def run_method(settings, campaign, root, method, delay, slo_ms, max_replicas):
    output = root / method
    output.mkdir()
    config, config_path, hpa_path = _method_configs(root, method, slo_ms, max_replicas)
    processes = []
    remote_started = False
    completed = False
    with (output / 'controller.log').open('x') as control_log, (output / 'sampler.log').open('x') as sampler_log, (output / 'runner-ssh.log').open('x') as runner_log:
        try:
            if method == 'baseline':
                kube('apply', '-f', root / 'workload.yaml')
                kube('apply', '-f', hpa_path)
            else:
                processes.append(subprocess.Popen([sys.executable, '-u', '-m', 'controller', '--config', str(config_path), '--deploy'], stdout=control_log, stderr=subprocess.STDOUT, start_new_session=True))
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if processes[0].poll() is not None:
                        raise RuntimeError('controller startup failed; see controller.log')
                    check = kube('-n', NAMESPACE, 'get', 'deployment', APP, '--ignore-not-found', '-o', 'name', capture_output=True, text=True)
                    if check.stdout.strip():
                        break
                    time.sleep(1)
                else:
                    raise RuntimeError('controller did not deploy the workload')
            kube('-n', NAMESPACE, 'rollout', 'status', 'deployment/' + APP, '--timeout=180s')
            service = get('service', APP)
            check_service_routing(service)
            port = service['spec']['ports'][0]['nodePort']
            site_urls = {site: f'http://{address}:{port}/detect'
                         for site, address in settings['site_node_ips'].items()}
            if delay is not None:
                delay.check()
            command([sys.executable, '-m', 'experiments.preflight', '--config', config_path])
            sampler = subprocess.Popen([sys.executable, '-u', '-m', 'experiments.monitor_resources', 'sample', str(output / 'metrics.jsonl'), '--namespace', NAMESPACE], stdout=sampler_log, stderr=subprocess.STDOUT, start_new_session=True)
            processes.append(sampler)
            payload = remote_settings(settings) | {'campaign': campaign, 'method': method, 'site_urls': site_urls,
                       'schedule': settings['schedule'], 'duration': settings['phase_duration']}
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
                    verified = active_telemetry(config['prometheus_url'])
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
            completed = True
        finally:
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
    command([sys.executable, '-m', 'experiments.summarize', output / 'requests.jsonl', '--slo-ms', str(slo_ms)])
    command([sys.executable, '-m', 'experiments.monitor_resources', 'summarize', output / 'metrics.jsonl', output / 'requests.jsonl', output / 'summary.csv', output])
    command([sys.executable, '-m', 'experiments.telemetry_history', output / 'requests.jsonl', output, '--prometheus-url', config['prometheus_url']])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign', nargs='?', help='new result name, for example run_2')
    parser.add_argument('--config', default=str(ROOT / 'experiments/campaign.yaml'))
    parser.add_argument('--check', action='store_true', help='read-only dependency and connectivity checks')
    parser.add_argument('--tc-ms', type=float, help='enable workload link shaping at this one-way delay in milliseconds')
    parser.add_argument('--slo-ms', type=float, help='controller p90 target and reporting threshold in milliseconds (default: config.yaml)')
    parser.add_argument('--max-replicas', type=int,
                        help='shared baseline HPA and proposed replica ceiling (default: config.yaml)')
    args = parser.parse_args(argv)
    if args.tc_ms is not None and (not math.isfinite(args.tc_ms) or args.tc_ms < 0.001):
        parser.error('--tc-ms must be finite and at least 0.001 ms')
    if args.slo_ms is not None and (not math.isfinite(args.slo_ms) or args.slo_ms <= 0):
        parser.error('--slo-ms must be finite and positive')
    if args.max_replicas is not None and args.max_replicas < 1:
        parser.error('--max-replicas must be a positive integer')
    os.chdir(ROOT)
    os.environ.setdefault('KUBECONFIG', '/etc/rancher/k3s/k3s.yaml')
    settings = load(args.config)
    controller_config = yaml.safe_load((ROOT / 'config.yaml').read_text())
    slo_ms = args.slo_ms if args.slo_ms is not None else controller_config['slo_seconds'] * 1000
    max_replicas = args.max_replicas if args.max_replicas is not None else controller_config['max_replicas']
    if max_replicas < controller_config['min_replicas']:
        parser.error('--max-replicas must be at least min_replicas from config.yaml')
    if not args.check and (not args.campaign or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', args.campaign)):
        parser.error('provide a new campaign name using letters, digits, hyphens or underscores')
    with open('/tmp/mec-experiment-campaign.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        prerequisites(settings)
        if args.check:
            return
        root = ROOT / 'results' / args.campaign
        root.mkdir(parents=True, exist_ok=False)
        command(remote(settings, ['test', '!', '-e', f'results/{args.campaign}']))
        with (root / 'campaign.yaml').open('x') as stream:
            yaml.safe_dump(settings | {'tc_ms': args.tc_ms, 'slo_ms': slo_ms,
                                       'max_replicas': max_replicas}, stream)
        phases = parse_schedule(settings['schedule'])
        minutes = len(phases) * settings['phase_duration'] / 60
        print(f'Campaign: {len(phases)} phases per method, {settings["phase_duration"]:g}s each; '
              f'{minutes:g} minutes traffic per method ({2 * minutes:g} minutes total), '
              'plus UE switching, setup and transfer. Objective: all-attempt p90 '
              f'<= {slo_ms:g} ms. Shared replica ceiling: {max_replicas}.', flush=True)
        ns = kube('get', 'namespace', NAMESPACE, '--ignore-not-found', '-o', 'name', capture_output=True, text=True)
        if not ns.stdout.strip():
            kube('create', 'namespace', NAMESPACE)
        def interrupted(signum, frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, interrupted)
        delay = TrafficDelay(args.tc_ms) if args.tc_ms is not None else None
        try:
            if delay is not None:
                delay.start()
            _campaign_manifest(root, delay is not None)
            for method in ('baseline', 'proposed'):
                print(f'=== {method.upper()} ===', flush=True)
                reset()
                run_method(settings, args.campaign, root, method, delay, slo_ms, max_replicas)
            command([sys.executable, '-m', 'experiments.plot_autoscaler', root, root / 'plots', '--slo-ms', str(slo_ms)])
        finally:
            try:
                reset()
            finally:
                if delay is not None:
                    delay.close()
        print(f'Campaign complete. Raw data, logs and plots: {root}', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        raise SystemExit(f'Campaign stopped: {exc}') from exc
