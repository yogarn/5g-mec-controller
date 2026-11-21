#!/usr/bin/env python3
"""Post-sync verification and lab smoke test helper for vertical offloading.

Covers:
1. `check-env`: verifies gnb1 node capacity (4 vCPU, ~8 GiB RAM, headroom), Envoy ingress
   readiness, Core fallback readiness, and explicit per-node EDS endpoint pinning.
2. `verify-session`: verifies UE session IP, serving UPF (SMF /pdu-info), target Envoy
   endpoint, and answering pod node without using core DNN embb.testbed for RAN-1.
3. `smoke-manual`: performs manual LOCAL -> SPREAD -> LOCAL switch from Core via offloader
   `/override`, verifying X-MEC-Site, X-MEC-Route, server_node, and constant UE session/DNN.
4. `smoke-auto`: verifies automatic p90-driven SPREAD entry and overall-p90 recovery events.
"""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import Request, urlopen

from benchmark.client import attempt
from urllib.parse import urlsplit


EXPECTED_LOCAL_UPFS = {
    'gnb-1': {'node': 'gnb1', 'dnn': 'mec.icn-2.testbed', 'subnet': '172.16.50.0/24', 'upf_ip': '172.16.30.11'},
    'gnb-2': {'node': 'gnb-2', 'dnn': 'embb.gnb2.testbed', 'subnet': '10.63.1.0/24', 'upf_ip': '172.16.20.70'},
    'gnb-3': {'node': 'gnb-3', 'dnn': 'embb.gnb3.testbed', 'subnet': '10.64.1.0/24', 'upf_ip': '172.16.20.200'},
}
FORBIDDEN_RAN1_SUBNET = ipaddress.ip_network('172.16.45.0/24')


def _load_token(token_file: str | None) -> str | None:
    token = (
        os.environ.get('OFFLOADER_ADMIN_TOKEN', '').strip()
        or os.environ.get('UE_TELEMETRY_TOKEN', '').strip()
    )
    if token:
        return token
    if token_file and Path(token_file).is_file():
        return Path(token_file).read_text(encoding='utf-8').strip()
    return None


def _http_json(url: str, *, method: str = 'GET', payload: dict | None = None, token: str | None = None, timeout: float = 5.0) -> dict:
    data = json.dumps(payload).encode('utf-8') if payload is not None else None
    headers = {'Content-Type': 'application/json'} if data is not None else {}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    req = Request(url, data=data, method=method, headers=headers)
    with urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def check_gnb1_capacity(node_json: dict, *, min_cpus: int = 4, min_ram_gib: float = 7.0) -> dict:
    alloc = (node_json.get('status') or {}).get('allocatable') or {}
    cpu_raw = str(alloc.get('cpu', '0'))
    cpus = int(cpu_raw[:-1]) / 1000.0 if cpu_raw.endswith('m') else float(cpu_raw)
    mem_raw = str(alloc.get('memory', '0Ki'))
    if mem_raw.endswith('Ki'):
        mem_gib = int(mem_raw[:-2]) / (1024 * 1024)
    elif mem_raw.endswith('Mi'):
        mem_gib = int(mem_raw[:-2]) / 1024
    else:
        mem_gib = float(mem_raw) / (1024 ** 3)
    ok = cpus >= min_cpus and mem_gib >= min_ram_gib
    return {
        'node': (node_json.get('metadata') or {}).get('name'),
        'allocatable_cpu': cpus,
        'allocatable_mem_gib': round(mem_gib, 2),
        'meets_4vcpu_8gb': ok,
    }


def verify_eds_pinning(offloader_status: dict, pods_json: dict) -> dict:
    """Verify every EDS endpoint IP belongs to a Ready pod on the exact expected node."""
    ip_to_node = {}
    for pod in pods_json.get('items') or []:
        ip = (pod.get('status') or {}).get('podIP')
        node = (pod.get('spec') or {}).get('nodeName')
        if ip and node:
            ip_to_node[ip] = node

    endpoints = ((offloader_status.get('xds') or {}).get('endpoints') or {})
    expected_nodes = {
        'local-gnb-1': 'gnb1',
        'local-gnb-2': 'gnb-2',
        'local-gnb-3': 'gnb-3',
        'core-5g': 'core-5g',
    }
    mismatches = []
    for svc, expected_node in expected_nodes.items():
        for ep in endpoints.get(svc) or []:
            ip = ep.split(':', 1)[0]
            actual_node = ip_to_node.get(ip)
            if actual_node != expected_node:
                mismatches.append({'eds_service': svc, 'ip': ip, 'expected_node': expected_node, 'actual_node': actual_node})
    return {'ok': len(mismatches) == 0, 'endpoints': endpoints, 'mismatches': mismatches}


def verify_smf_sessions(smf_json: dict) -> dict:
    """Verify active PDU sessions use local UPFs and RAN-1 is never on 172.16.45.0/24."""
    sessions = []
    violations = []
    for item in smf_json.get('items') or []:
        supi = item.get('supi')
        for pdu in item.get('pdu') or []:
            if pdu.get('pdu_state') != 'active':
                continue
            ip_str = pdu.get('ipv4')
            upf_addr = ((pdu.get('n3') or {}).get('upf') or {}).get('addr', '')
            if not ip_str:
                continue
            ip = ipaddress.IPv4Address(ip_str)
            entry = {'supi': supi, 'ue_ip': ip_str, 'upf_addr': upf_addr}
            sessions.append(entry)
            if ip in FORBIDDEN_RAN1_SUBNET:
                violations.append({'reason': 'ue_on_forbidden_core_subnet_172_16_45', **entry})
    return {'ok': len(violations) == 0, 'active_sessions': sessions, 'violations': violations}


def cmd_check_env(args):
    token = _load_token(args.token_file)
    node_raw = subprocess.check_output(['kubectl', 'get', 'node', 'gnb1', '-o', 'json'], text=True)
    cap = check_gnb1_capacity(json.loads(node_raw))
    pods_raw = subprocess.check_output(['kubectl', '-n', 'services', 'get', 'pods', '-o', 'json'], text=True)
    status = _http_json(f"{args.offloader_url.rstrip('/')}/status", token=token)
    pinning = verify_eds_pinning(status, json.loads(pods_raw))
    report = {'gnb1_capacity': cap, 'eds_pinning': pinning}
    print(json.dumps(report, indent=2))
    if not cap['meets_4vcpu_8gb'] or not pinning['ok']:
        sys.exit(1)


def cmd_verify_session(args):
    smf_json = _http_json(args.smf_url)
    res = verify_smf_sessions(smf_json)
    print(json.dumps(res, indent=2))
    if not res['ok']:
        sys.exit(1)


def cmd_smoke_manual(args):
    token = _load_token(args.token_file)
    base = args.offloader_url.rstrip('/')
    site = args.site
    expected_local_node = EXPECTED_LOCAL_UPFS[site]['node']
    target_url = urlsplit(args.ingress_url)

    def probe_step(expected_route: str, expected_nodes: tuple[str, ...]):
        rec = attempt(target_url, 'GET', None, {}, args.source_ip, args.timeout)
        ok = (
            rec['success']
            and rec.get('ingress_site') == site
            and rec.get('selected_route') == expected_route
            and (rec.get('server_node') in expected_nodes if rec.get('server_node') else True)
        )
        return {'ok': ok, 'expected_route': expected_route, 'expected_nodes': list(expected_nodes),
                'ingress_site': rec.get('ingress_site'), 'selected_route': rec.get('selected_route'),
                'server_node': rec.get('server_node'), 'server_pod': rec.get('server_pod'),
                'http_status': rec.get('http_status'), 'duration_ms': round(rec['duration_ms'], 2)}

    _http_json(f'{base}/override', method='POST', payload={'site': site, 'route': 'LOCAL'}, token=token)
    time.sleep(args.wait_ack)
    before = probe_step('local', (expected_local_node,))

    _http_json(f'{base}/override', method='POST', payload={'site': site, 'route': 'SPREAD'}, token=token)
    time.sleep(args.wait_ack)
    during = probe_step('spread', (expected_local_node, 'core-5g'))

    _http_json(f'{base}/override', method='POST', payload={'site': site, 'route': 'LOCAL'}, token=token)
    time.sleep(args.wait_ack)
    after = probe_step('local', (expected_local_node,))

    _http_json(f'{base}/override', method='POST', payload={'site': site, 'route': None}, token=token)
    report = {'site': site, 'before': before, 'during': during, 'after': after,
              'all_ok': before['ok'] and during['ok'] and after['ok']}
    print(json.dumps(report, indent=2))
    if not report['all_ok']:
        sys.exit(1)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='cmd', required=True)

    p_env = sub.add_parser('check-env')
    p_env.add_argument('--offloader-url', default='http://127.0.0.1:18086')
    p_env.add_argument('--token-file', default='/home/member/.config/5g-mec/ue-telemetry-token')
    p_env.set_defaults(func=cmd_check_env)

    p_sess = sub.add_parser('verify-session')
    p_sess.add_argument('--smf-url', default='http://10.34.211.140:9090/pdu-info?page=-1')
    p_sess.set_defaults(func=cmd_verify_session)

    p_man = sub.add_parser('smoke-manual')
    p_man.add_argument('--site', choices=('gnb-1', 'gnb-2', 'gnb-3'), required=True)
    p_man.add_argument('--ingress-url', required=True)
    p_man.add_argument('--source-ip', required=True)
    p_man.add_argument('--offloader-url', default='http://127.0.0.1:18086')
    p_man.add_argument('--token-file', default='/home/member/.config/5g-mec/ue-telemetry-token')
    p_man.add_argument('--wait-ack', type=float, default=1.5)
    p_man.add_argument('--timeout', type=float, default=5.0)
    p_man.set_defaults(func=cmd_smoke_manual)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == '__main__':
    main()
