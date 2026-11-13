#!/usr/bin/env python3
"""Read-only experiment preflight; proxy changes require explicit operator consent."""
import argparse
import copy
from contextlib import contextmanager
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import time
from urllib.parse import urlsplit

import requests
import yaml

from controller.config import load_settings, workload_identity
from controller.telemetry import Prometheus



def mutation_blockers(checks):
    return [name for name, result in checks.items()
            if name != 'envoy_config_match' and not result['ok'] and not result.get('unverified', False)]
class PreflightError(Exception):
    pass

def is_workload_latency_query(query):
    expected = ('(histogram_quantile(0.95,sumby(le)(rate('
                'envoy_http_downstream_rq_time_bucket{job="upf-envoy",'
                'envoy_http_conn_manager_prefix="mec_placement_face",'
                'node=~"gnb-2|gnb-3"}[2m])))/1000)andon()(count(countby(node)('
                'envoy_http_downstream_rq_time_count{job="upf-envoy",'
                'envoy_http_conn_manager_prefix="mec_placement_face",'
                'node=~"gnb-2|gnb-3"}))==2)')
    return ''.join(query.split()) == expected


def proxy_is_idle(settings):
    """Confirm both edge scrapes and workload counters before reporting idle traffic."""
    if not is_workload_latency_query(settings.latency_query):
        return False
    prometheus = Prometheus(settings.prometheus_url, settings.max_metric_age_seconds)
    for node in ('gnb-2', 'gnb-3'):
        labels = f'job="upf-envoy",node="{node}"'
        if prometheus.scalar(f'up{{{labels}}}') != 1:
            return False
        count = prometheus.scalar(
            'sum(increase(envoy_http_downstream_rq_time_count'
            f'{{{labels},envoy_http_conn_manager_prefix="mec_placement_face"}}[2m]))')
        if count != 0:
            return False
    return True

@contextmanager
def port_forward(namespace, resource, port):
    process = subprocess.Popen(
        ['kubectl', '-n', namespace, 'port-forward', '--address', '127.0.0.1',
         resource, f':{port}'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        ready, _, _ = select.select([process.stdout], [], [], 10)
        line = process.stdout.readline().strip() if ready else ''
        prefix = 'Forwarding from 127.0.0.1:'
        if not line.startswith(prefix):
            raise PreflightError(f'kubectl port-forward failed or timed out for {resource}: {line}')
        local_port = int(line[len(prefix):].split(' ', 1)[0])
        yield f'http://127.0.0.1:{local_port}'
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        process.stdout.close()


def kubectl(*args, input_text=None):
    try:
        result = subprocess.run(['kubectl', '--request-timeout=10s', *args], input=input_text,
                                text=True, capture_output=True, timeout=120, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PreflightError(f'kubectl failed: {exc}') from exc
    if result.returncode:
        raise PreflightError(f"kubectl {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def get(kind, name, namespace):
    return json.loads(kubectl('-n', namespace, 'get', kind, name, '-o', 'json'))


def check_deployment(deployment):
    spec = deployment.get('spec', {})
    desired = spec.get('replicas', 1)
    template = spec.get('template', {})
    if (not isinstance(desired, int) or isinstance(desired, bool) or desired < 1
            or not template.get('metadata', {}).get('labels')
            or not template.get('spec', {}).get('containers')):
        raise PreflightError('Deployment must have at least one desired replica and a labeled pod template with containers')
    return {'desired': desired, 'readiness': 'not checked (cold start permitted)'}


def check_service(service, deployment):
    spec = service.get('spec', {})
    if spec.get('type') == 'ExternalName':
        raise PreflightError('ExternalName Service cannot select Deployment pods')
    selector = spec.get('selector')
    labels = deployment.get('spec', {}).get('template', {}).get('metadata', {}).get('labels', {})
    if not isinstance(selector, dict) or not selector or any(labels.get(k) != v for k, v in selector.items()):
        raise PreflightError('Service selector does not match Deployment pod template labels')
    ports = spec.get('ports', [])
    if not ports:
        raise PreflightError('Service has no ports')
    containers = deployment.get('spec', {}).get('template', {}).get('spec', {}).get('containers', [])
    for port in ports:
        number = port.get('port')
        target = port.get('targetPort', number)
        protocol = port.get('protocol', 'TCP')
        if (not isinstance(number, int) or isinstance(number, bool) or not 1 <= number <= 65535
                or protocol not in ('TCP', 'UDP', 'SCTP')):
            raise PreflightError('Service has an invalid port or protocol')
        if isinstance(target, str):
            if not any(p.get('name') == target and p.get('protocol', 'TCP') == protocol
                       for container in containers for p in container.get('ports', [])):
                raise PreflightError(f'Service targetPort {target!r} does not match a Deployment container port')
        elif not isinstance(target, int) or isinstance(target, bool) or not 1 <= target <= 65535:
            raise PreflightError('Service has an invalid targetPort')
    return {'selector': 'matches Deployment pod template', 'ports': len(ports),
            'availability': 'not checked (cold start permitted)'}


def find_pod(namespace, statefulset, node):
    pods = json.loads(kubectl('-n', namespace, 'get', 'pods', '-o', 'json'))['items']
    matches = [p for p in pods if p.get('spec', {}).get('nodeName') == node
               and any(o.get('kind') == 'StatefulSet' and o.get('name') == statefulset['metadata']['name']
                       and o.get('uid') == statefulset['metadata']['uid'] and o.get('controller')
                       for o in p['metadata'].get('ownerReferences', []))
               and not p['metadata'].get('deletionTimestamp')]
    if len(matches) != 1:
        raise PreflightError(f'expected exactly one {statefulset["metadata"]["name"]} pod on {node}, found {len(matches)}')
    return matches[0]


def validate_proxy_mount(pod, container_name, configmap, key, config_path):
    containers = [c for c in pod.get('spec', {}).get('containers', []) if c['name'] == container_name]
    if len(containers) != 1:
        raise PreflightError(f'proxy container {container_name!r} not found')
    statuses = [s for s in pod.get('status', {}).get('containerStatuses', []) if s['name'] == container_name]
    if len(statuses) != 1 or not statuses[0].get('ready') or 'running' not in statuses[0].get('state', {}):
        raise PreflightError('proxy container is not running and ready')
    volumes = {v['name']: v for v in pod['spec'].get('volumes', [])}
    for mount in containers[0].get('volumeMounts', []):
        volume = volumes.get(mount['name'], {}).get('configMap', {})
        if volume.get('name') != configmap or mount.get('subPathExpr'):
            continue
        items = volume.get('items')
        paths = [item['path'] for item in items if item.get('key') == key] if items is not None else [key]
        for path in paths:
            if mount.get('subPath'):
                mounted = mount['mountPath'] if mount['subPath'] == path else None
            else:
                mounted = os.path.join(mount['mountPath'], path)
            if mounted == config_path:
                return
    raise PreflightError('specified ConfigMap key is not mounted at the specified path in the running proxy container')


def runtime_matches(dump, desired):
    def contains(actual, expected):
        if isinstance(expected, dict):
            return isinstance(actual, dict) and all(key in actual and contains(actual[key], value)
                                                    for key, value in expected.items())
        if isinstance(expected, list):
            return isinstance(actual, list) and len(actual) == len(expected) and all(
                contains(a, e) for a, e in zip(actual, expected))
        return actual == expected

    listeners, clusters = [], []
    for section in dump.get('configs', []):
        listeners.extend(item.get('listener', {}) for item in section.get('static_listeners', []))
        listeners.extend(item.get('active_state', {}).get('listener', {})
                         for item in section.get('dynamic_listeners', []))
        clusters.extend(item.get('cluster', {}) for item in section.get('static_clusters', []))
        clusters.extend(item.get('cluster', {}) for item in section.get('dynamic_active_clusters', []))
    for kind, active in (('listeners', listeners), ('clusters', clusters)):
        expected = desired['static_resources'].get(kind, [])
        if not expected:
            raise PreflightError(f'configured Envoy has no {kind}')
        for resource in expected:
            # Envoy's protobuf JSON omits ROUND_ROBIN, the default cluster policy.
            if kind == 'clusters' and resource.get('lb_policy') == 'ROUND_ROBIN':
                active = [dict(candidate, lb_policy=candidate.get('lb_policy', 'ROUND_ROBIN'))
                          for candidate in active]
            if not any(contains(candidate, resource) for candidate in active):
                raise PreflightError(f"active Envoy {kind} does not match configured resource {resource.get('name')!r}")
    return 'configured listener and cluster fields found in active Envoy resources'


def backup_configmap(directory, configmap):
    directory = Path(directory).expanduser()
    # Require a new directory to avoid overwriting or mixing recovery artifacts.
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    path = directory / 'configmap.json'
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(configmap, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    return str(path.resolve())


def apply_config(args, pod, configmap, text):
    namespace, name = args.proxy_namespace, args.envoy_configmap
    pod_name = pod['metadata']['name']
    if not configmap.get('metadata', {}).get('resourceVersion'):
        raise PreflightError('ConfigMap has no resourceVersion; refusing unsafe replacement')
    # Validation runs the same Envoy executable/image without modifying the pod.
    kubectl('-n', namespace, 'exec', pod_name, '-c', args.envoy_container,
            '--', args.envoy_binary, '--mode', 'validate', '--config-yaml', text)
    backup = backup_configmap(args.recovery_dir, configmap)
    if configmap.get('data', {}).get(args.envoy_config_key) == text:
        try:
            actual = get('configmap', name, namespace)
            if (actual.get('metadata', {}).get('resourceVersion') != configmap['metadata']['resourceVersion']
                    or actual.get('data', {}).get(args.envoy_config_key) != text):
                raise PreflightError('ConfigMap changed after inspection')
        except Exception as exc:
            raise PreflightError(f'ConfigMap could not be verified: {exc}; recovery backup: {backup}') from exc
        return backup
    updated = copy.deepcopy(configmap)
    updated.setdefault('data', {})[args.envoy_config_key] = text
    # Replace carries resourceVersion, so a concurrent ConfigMap edit fails closed.
    try:
        kubectl('-n', namespace, 'replace', '-f', '-', input_text=json.dumps(updated))
        actual = get('configmap', name, namespace)
        if actual.get('data', {}).get(args.envoy_config_key) != text:
            raise PreflightError('ConfigMap update not verified')
    except Exception as exc:
        raise PreflightError(f'ConfigMap replacement failed or could not be verified: {exc}; recovery backup: {backup}') from exc
    return backup


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='config.yaml', help='Controller configuration (default: config.yaml)')
    parser.add_argument('--service', help='Service name (default: the Deployment name)')
    parser.add_argument('--proxy-namespace', default='open5gs')
    parser.add_argument('--node', default='gnb-2')
    parser.add_argument('--upf-statefulset', help='UPF StatefulSet (default: upf-<node>)')
    parser.add_argument('--envoy-container', default='envoy')
    parser.add_argument('--envoy-configmap', help='Envoy ConfigMap (default: upf-<node>-envoy-config)')
    parser.add_argument('--envoy-config-key', default='envoy.yaml')
    parser.add_argument('--envoy-config-path', default='/etc/envoy-config/envoy.yaml', help='Mounted Envoy configuration path')
    parser.add_argument('--envoy-binary', default='envoy')
    parser.add_argument('--envoy-admin-url', default='http://127.0.0.1:9901')
    parser.add_argument('--envoy-config', default='apps/face-detection/envoy-gnb2.yaml')
    parser.add_argument('--apply-envoy-config', action='store_true', help='Explicitly replace the ConfigMap key after validation and backup; removes previous proxy routes')
    parser.add_argument('--recovery-dir', help='New, nonexisting directory for the live ConfigMap backup (required for apply)')
    parser.add_argument('--restart-upf', action='store_true', help='Separately opt in to deleting the target UPF pod after apply; interrupts traffic')
    parser.add_argument('--restart-timeout', type=int, default=180)
    parser.add_argument('--json', action='store_true', help='Print the structured JSON report')
    args = parser.parse_args(argv)
    if args.upf_statefulset is None:
        args.upf_statefulset = f'upf-{args.node}'
    if args.envoy_configmap is None:
        args.envoy_configmap = f'upf-{args.node}-envoy-config'
    if args.apply_envoy_config and not args.recovery_dir:
        parser.error('--apply-envoy-config requires --recovery-dir')
    if args.restart_upf and not args.apply_envoy_config:
        parser.error('--restart-upf requires --apply-envoy-config')
    if args.restart_timeout <= 0:
        parser.error('--restart-timeout must be positive')
    return args


def render_report(report):
    """Render check results without dumping Kubernetes resources."""
    lines = []
    for name, result in report['checks'].items():
        status = 'WARN' if result.get('unverified') else ('OK' if result['ok'] else 'FAIL')
        if result.get('unverified'):
            detail = result.get('warning', 'workload latency not yet verified')
        elif not result['ok']:
            detail = result.get('error', '')
        elif name == 'service':
            resource = result.get('detail', {})
            metadata = resource.get('metadata', {})
            spec = resource.get('spec', {})
            detail = ', '.join(f'{key}={value}' for key, value in (
                ('name', metadata.get('name')), ('namespace', metadata.get('namespace')),
                ('cluster_ip', spec.get('clusterIP'))) if value is not None)
        else:
            detail = result.get('detail', '')
            if isinstance(detail, dict):
                detail = ', '.join(f'{key}={value}' for key, value in detail.items()
                                   if isinstance(value, (str, int, float, bool)))
            elif isinstance(detail, (tuple, list)):
                detail = '/'.join(str(value) for value in detail)
        detail = ' '.join(str(detail).split())
        if len(detail) > 200:
            detail = detail[:197] + '...'
        lines.append(f'[{status}] {name}' + (f': {detail}' if detail else ''))
        if name == 'prometheus_query' and result.get('unverified'):
            lines.append('  Telemetry is not established; verify it before running the controller. The controller fails closed without valid latency.')
        elif name == 'prometheus_query' and not result['ok'] and 'invalid or stale Prometheus sample' in result.get('error', ''):
            lines.append('  Hint: Inspect the query, metric values and sample timestamps.')
    for warning in report['warnings']:
        lines.append(f'[WARN] {warning}')
    for action in report['actions']:
        details = ', '.join(f'{key}={value}' for key, value in action.items() if key != 'action')
        lines.append(f"[ACTION] {action['action']}" + (f': {details}' if details else ''))
    status = 'OK (unverified telemetry warning)' if report['ok'] and any(item.get('unverified') for item in report['checks'].values()) else ('OK' if report['ok'] else 'FAIL')
    lines.append(f'Preflight: {status}')
    return '\n'.join(lines)


def main(argv=None):
    args = parse_args(argv)
    report = {'ok': True, 'checks': {}, 'actions': [], 'warnings': []}

    def check(name, function):
        try:
            detail = function()
            report['checks'][name] = {'ok': True, 'detail': detail}
            return detail
        except Exception as exc:
            report['checks'][name] = {'ok': False, 'error': str(exc)}
            report['ok'] = False
            return None

    settings = check('configuration', lambda: load_settings(args.config))
    if settings:
        identity = check('workload_identity', lambda: workload_identity(settings.manifest, settings.namespace))
    else:
        identity = None
    connected = check('kubernetes', lambda: kubectl('get', '--raw=/readyz').strip() or 'ready')
    service = None
    if connected and identity:
        namespace, deployment = identity
        resource = None
        def inspect_deployment():
            nonlocal resource
            resource = get('deployment', deployment, namespace)
            return check_deployment(resource)
        check('deployment', inspect_deployment)
        service_name = args.service or deployment
        service = check('service', lambda: get('service', service_name, namespace))
        if service and resource:
            check('service_selection', lambda: check_service(service, resource))
    if settings:
        def http_health():
            response = requests.get(settings.prometheus_url + '/-/healthy', timeout=10)
            response.raise_for_status()
            return {'status_code': response.status_code, 'body': response.text.strip()}
        check('prometheus_health', http_health)
        check('prometheus_query', lambda: {'latency_seconds': Prometheus(settings.prometheus_url, settings.max_metric_age_seconds).scalar(settings.latency_query), 'query': settings.latency_query})
        result = report['checks']['prometheus_query']
        error = result.get('error')
        if is_workload_latency_query(settings.latency_query) and error == 'Prometheus query returned no samples':
            result.update(unverified=True, warning='workload latency not yet verified: no samples')
        elif is_workload_latency_query(settings.latency_query) and error == 'Prometheus sample is NaN':
            try:
                if proxy_is_idle(settings):
                    result.update(unverified=True, warning='workload latency not yet verified: both edge proxies have no requests in the last 2 minutes')
            except ValueError as exc:
                if str(exc) == 'Prometheus query returned no samples':
                    result.update(unverified=True, warning='workload latency not yet verified: edge proxy metrics missing')
                else:
                    result['idle_diagnostic_error'] = str(exc)
            except Exception as exc:
                result['idle_diagnostic_error'] = str(exc)

    def read_desired():
        text = Path(args.envoy_config).read_text()
        parsed = yaml.safe_load(text)
        if not isinstance(parsed, dict) or not isinstance(parsed.get('static_resources'), dict):
            raise PreflightError('Envoy config must contain static_resources')
        return text, parsed
    desired = check('envoy_source', read_desired)
    if connected and desired:
        ns = args.proxy_namespace
        sts = check('upf_statefulset', lambda: get('statefulset', args.upf_statefulset, ns))
        if sts:
            strategy = sts.get('spec', {}).get('updateStrategy', {}).get('type', 'RollingUpdate')
            report['checks']['upf_statefulset']['detail'] = {'name': args.upf_statefulset, 'update_strategy': strategy}
            if strategy == 'OnDelete':
                report['warnings'].append('OnDelete: updating the ConfigMap does not restart or reload the UPF; pod replacement is a separate interruption.')
            pod = check('upf_pod', lambda: find_pod(ns, sts, args.node))
            cm = check('envoy_configmap', lambda: get('configmap', args.envoy_configmap, ns))
            if pod and cm:
                mount_ok = check('envoy_mount', lambda: (validate_proxy_mount(pod, args.envoy_container, args.envoy_configmap, args.envoy_config_key, args.envoy_config_path), 'verified')[1])
                current = cm.get('data', {}).get(args.envoy_config_key)
                if current is None:
                    check('envoy_config_match', lambda: (_ for _ in ()).throw(PreflightError('ConfigMap key not found')))
                elif current == desired[0]:
                    report['checks']['envoy_config_match'] = {'ok': True, 'detail': 'ConfigMap key matches repository configuration'}
                else:
                    report['checks']['envoy_config_match'] = {'ok': False, 'error': 'ConfigMap key differs from repository configuration'}
                    report['ok'] = False
                def active():
                    admin = urlsplit(args.envoy_admin_url)
                    if admin.scheme != 'http' or admin.hostname not in ('127.0.0.1', 'localhost') or admin.path not in ('', '/'):
                        raise PreflightError('Envoy admin URL must be a local HTTP URL without a path')
                    with port_forward(ns, 'pod/' + pod['metadata']['name'], admin.port or 80) as base:
                        response = requests.get(base + '/config_dump', timeout=10)
                        response.raise_for_status()
                        return runtime_matches(response.json(), desired[1])

                if mount_ok and current is not None and not args.apply_envoy_config and not args.json and sys.stdin.isatty():
                    mismatch = current != desired[0]
                    if not mismatch:
                        try:
                            active()
                        except PreflightError as exc:
                            mismatch = str(exc).startswith('active Envoy ')
                        except (requests.RequestException, OSError, ValueError):
                            pass  # The normal check below reports the access failure.
                    if mismatch:
                        blockers = mutation_blockers(report['checks'])
                        if blockers:
                            print('[WARN] Envoy differs, but rollout is blocked by: ' + ', '.join(blockers))
                        else:
                            print(f'[WARN] Envoy on {args.node} differs from the desired configuration.')
                            print('Redeployment replaces proxy routes and restarts the shared UPF, interrupting 5G traffic.')
                            if input('Back up the current ConfigMap and redeploy? [y/N] ').strip().lower() == 'y':
                                backup_root = Path('results')
                                backup_root.mkdir(mode=0o700, parents=True, exist_ok=True)
                                args.recovery_dir = str(backup_root / f'envoy-backup-{args.node}-{time.time_ns()}')
                                args.apply_envoy_config = True
                                args.restart_upf = True
                blockers = mutation_blockers(report['checks'])
                if args.apply_envoy_config and blockers:
                    report['checks']['envoy_apply'] = {'ok': False, 'error': 'Mutation blocked by failed preflight checks: ' + ', '.join(blockers)}
                elif args.apply_envoy_config and mount_ok and current is not None and (current != desired[0] or args.restart_upf):
                    report['warnings'].append('This static Envoy configuration replaces existing proxy routes; restarting the UPF interrupts traffic.')
                    backup = check('envoy_apply', lambda: apply_config(args, pod, cm, desired[0]))
                    if backup:
                        action = 'configmap_replaced' if current != desired[0] else 'configmap_backed_up'
                        report['actions'].append({'action': action, 'backup': backup})
                        report['checks']['envoy_config_match'] = {'ok': True, 'detail': 'ConfigMap verified'}
                        if args.restart_upf:
                            def restart():
                                latest = get('pod', pod['metadata']['name'], ns)
                                if latest['metadata']['uid'] != pod['metadata']['uid']:
                                    raise PreflightError('UPF pod changed since inspection; refusing restart')
                                report['actions'].append({'action': 'upf_pod_deletion_requested', 'pod': pod['metadata']['name']})
                                kubectl('-n', ns, 'delete', 'pod', pod['metadata']['name'], '--wait=true', f'--timeout={args.restart_timeout}s')
                                deadline = time.monotonic() + args.restart_timeout
                                last_error = 'replacement pod not yet observed'
                                while time.monotonic() < deadline:
                                    try:
                                        replacement = find_pod(ns, sts, args.node)
                                        if (replacement['metadata']['name'] != pod['metadata']['name']
                                                or replacement['metadata']['uid'] == pod['metadata']['uid']):
                                            raise PreflightError('replacement UPF pod identity not verified')
                                        validate_proxy_mount(replacement, args.envoy_container, args.envoy_configmap, args.envoy_config_key, args.envoy_config_path)
                                        return replacement
                                    except PreflightError as exc:
                                        last_error = str(exc)
                                        time.sleep(min(2, max(0, deadline - time.monotonic())))
                                raise PreflightError(f'UPF replacement did not become ready: {last_error}')
                            replacement = check('upf_restart', restart)
                            if replacement:
                                pod = replacement
                                report['actions'].append({'action': 'upf_pod_restarted', 'pod': pod['metadata']['name']})
                        else:
                            report['warnings'].append('UPF was not restarted; running Envoy may still use its previous configuration.')
                elif args.apply_envoy_config and mount_ok and current == desired[0]:
                    report['checks']['envoy_apply'] = {'ok': True, 'detail': 'ConfigMap already matches; no changes performed'}
                if mount_ok:
                    check('envoy_active', active)
    report['ok'] = all(item['ok'] or item.get('unverified', False) for item in report['checks'].values())
    # Configuration objects are not JSON serializable and may include secrets.
    if settings:
        report['checks']['configuration']['detail'] = {'path': args.config}
    if desired:
        report['checks']['envoy_source']['detail'] = {'path': args.envoy_config}
    if isinstance(report['checks'].get('upf_pod', {}).get('detail'), dict):
        report['checks']['upf_pod']['detail'] = {'name': report['checks']['upf_pod']['detail']['metadata']['name'], 'node': args.node}
    if isinstance(report['checks'].get('envoy_configmap', {}).get('detail'), dict):
        report['checks']['envoy_configmap']['detail'] = {'name': args.envoy_configmap, 'namespace': args.proxy_namespace}
    if isinstance(report['checks'].get('upf_restart', {}).get('detail'), dict):
        report['checks']['upf_restart']['detail'] = {'name': report['checks']['upf_restart']['detail']['metadata']['name'], 'node': args.node}
    if args.json:
        print(json.dumps(report, indent=2, allow_nan=False))
    else:
        print(render_report(report))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    sys.exit(main())
