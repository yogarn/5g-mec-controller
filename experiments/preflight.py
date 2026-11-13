#!/usr/bin/env python3
"""Read-only checks for the workload and collector telemetry."""
import argparse
import json
import subprocess
import sys

import requests

from controller.config import load_settings, workload_identity
from controller.telemetry import Prometheus


class PreflightError(Exception):
    pass


def collector_status(settings):
    """Check scrape health and window completeness independently of latency."""
    prometheus = Prometheus(settings.prometheus_url, settings.max_metric_age_seconds)
    labels = 'job="ue-telemetry"'
    up = prometheus.scalar(f'up{{{labels}}}')
    if up != 1:
        raise PreflightError('collector scrape is down or invalid')
    complete = prometheus.scalar(f'ue_request_window_complete{{{labels}}}')
    if complete == 0:
        raise PreflightError('collector window incomplete due to overflow')
    if complete != 1:
        raise PreflightError('collector window completeness metric is invalid')
    attempts = prometheus.scalar(f'ue_request_window_attempts{{{labels}}}')
    if attempts < 0 or not float(attempts).is_integer():
        raise PreflightError('collector window attempts metric is invalid')
    return {'attempts': int(attempts), 'window': 'complete', 'scrape': 'up'}


def kubectl(*args):
    try:
        result = subprocess.run(['kubectl', '--request-timeout=10s', *args],
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
    if spec.get('type') != 'NodePort':
        raise PreflightError('Service must have type NodePort')
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
        node_port = port.get('nodePort')
        if (not isinstance(node_port, int) or isinstance(node_port, bool)
                or not 1 <= node_port <= 65535):
            raise PreflightError('Service has no valid allocated nodePort')
        if (not isinstance(number, int) or isinstance(number, bool) or not 1 <= number <= 65535
                or protocol not in ('TCP', 'UDP', 'SCTP')):
            raise PreflightError('Service has an invalid port or protocol')
        if isinstance(target, str):
            if not any(p.get('name') == target and p.get('protocol', 'TCP') == protocol
                       for container in containers for p in container.get('ports', [])):
                raise PreflightError(f'Service targetPort {target!r} does not match a Deployment container port')
        elif not isinstance(target, int) or isinstance(target, bool) or not 1 <= target <= 65535:
            raise PreflightError('Service has an invalid targetPort')
    tcp_ports = [p['nodePort'] for p in ports if p.get('protocol', 'TCP') == 'TCP']
    if not tcp_ports:
        raise PreflightError('Service has no TCP NodePort for HTTP')
    return {'selector': 'matches Deployment pod template', 'node_ports': tcp_ports,
            'availability': 'not checked (cold start permitted)', 'reachability': 'not checked'}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='config.yaml', help='Controller configuration')
    parser.add_argument('--service', help='Service name (default: the Deployment name)')
    parser.add_argument('--json', action='store_true', help='Print the structured JSON report')
    return parser.parse_args(argv)


def render_report(report):
    lines = []
    for name, result in report['checks'].items():
        status = 'WARN' if result.get('unverified') else ('OK' if result['ok'] else 'FAIL')
        detail = (result.get('warning', '') if result.get('unverified') else
                  result.get('detail', '') if result['ok'] else result.get('error', ''))
        if isinstance(detail, dict):
            detail = ', '.join(f'{key}={value}' for key, value in detail.items())
        elif isinstance(detail, (tuple, list)):
            detail = '/'.join(str(value) for value in detail)
        detail = ' '.join(str(detail).split())
        lines.append(f'[{status}] {name}' + (f': {detail}' if detail else ''))
        if name == 'prometheus_query' and result.get('unverified'):
            lines.append('  Telemetry is not established; verify it before running the controller. '
                         'The controller fails closed without valid latency.')
    status = ('OK (unverified telemetry warning)' if report['ok'] and
              any(item.get('unverified') for item in report['checks'].values()) else
              'OK' if report['ok'] else 'FAIL')
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
            return None

    settings = check('configuration', lambda: load_settings(args.config))
    identity = (check('workload_identity', lambda: workload_identity(settings.manifest, settings.namespace))
                if settings else None)
    connected = check('kubernetes', lambda: kubectl('get', '--raw=/readyz').strip() or 'ready')
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
        if service is not None:
            report['checks']['service']['detail'] = {'name': service_name, 'namespace': namespace}
        if service is not None and resource is not None:
            check('service_selection', lambda: check_service(service, resource))

    if settings:
        def http_health():
            response = requests.get(settings.prometheus_url + '/-/healthy', timeout=10)
            response.raise_for_status()
            return {'status_code': response.status_code}

        check('prometheus_health', http_health)
        collector = check('collector', lambda: collector_status(settings))
        check('prometheus_query', lambda: {
            'latency_seconds': Prometheus(settings.prometheus_url, settings.max_metric_age_seconds).scalar(settings.latency_query),
            'query': settings.latency_query})
        result = report['checks']['prometheus_query']
        if (collector is not None and collector['attempts'] == 0
                and ''.join(settings.latency_query.split()) == 'ue_request_duration_p90_seconds{job="ue-telemetry"}'
                and result.get('error') == 'Prometheus query returned no samples'):
            result.update(unverified=True, warning='collector latency not yet verified: no attempts in the window')
        report['checks']['configuration']['detail'] = {'path': args.config}

    report['ok'] = all(item['ok'] or item.get('unverified', False) for item in report['checks'].values())
    print(json.dumps(report, indent=2, allow_nan=False) if args.json else render_report(report))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    sys.exit(main())
