"""Campaign-owned configurable inter-node delay for the reserved face-detection pool.

The Calico IP pool is deliberately retained after use: ordinary pods cannot
allocate from it, so stale rules after a crash cannot select ordinary workloads.
Empty clsact hooks are also retained to avoid deleting other owners' filters.
"""
import hashlib
import ipaddress
import json
import logging
import math
import os
import shlex
import subprocess
import time
import uuid

logger = logging.getLogger(__name__)

POOL_NAME = 'mec-campaign-reserved'
POOL_CIDR = '172.16.40.0/23'
NODES = ('gnb1', 'gnb-2', 'gnb-3', 'core-5g')
PREFERENCES = (49210, 49211)


def command(*args, **kwargs):
    return subprocess.run(list(map(str, args)), check=True, text=True, **kwargs)


class TrafficDelay:
    """Install and remove only this campaign's isolated tc filters and IFBs."""

    def __init__(self, delay_ms):
        if not math.isfinite(delay_ms) or delay_ms < 0.001:
            raise ValueError('tc delay must be at least 0.001 ms')
        self.delay_ms = delay_ms
        self.agents = {}
        self.devices = []
        self.token = uuid.uuid4().hex[:6]
        self.started = False

    def _json(self, *args):
        return json.loads(command(*args, capture_output=True).stdout)

    def _exec(self, node, *args, **kwargs):
        return command('kubectl', '-n', 'kube-system', 'exec', self.agents[node],
                       '-c', 'calico-node', '--', *args, **kwargs)

    def _pool(self):
        pools = self._json('kubectl', 'get', 'ippools.crd.projectcalico.org', '-o', 'json')['items']
        wanted = ipaddress.ip_network(POOL_CIDR)
        expected = {'cidr': POOL_CIDR, 'blockSize': 26, 'disabled': False,
                    'ipipMode': 'Never', 'vxlanMode': 'Always', 'natOutgoing': True,
                    'nodeSelector': '!all()'}
        existing = None
        for pool in pools:
            if pool['metadata']['name'] == POOL_NAME:
                existing = pool
            elif ipaddress.ip_network(pool['spec']['cidr']).overlaps(wanted):
                raise RuntimeError(f'{POOL_CIDR} overlaps Calico pool {pool["metadata"]["name"]}')
        if existing:
            spec = existing['spec']
            if any(spec.get(k, False if k == 'disabled' else None) != v for k, v in expected.items()):
                raise RuntimeError(f'existing {POOL_NAME} has unexpected settings')
            if spec.get('allowedUses', ['Workload', 'Tunnel']) != ['Workload']:
                raise RuntimeError(f'existing {POOL_NAME} must allow only Workload allocations')
            return
        pool = {'apiVersion': 'crd.projectcalico.org/v1', 'kind': 'IPPool',
                'metadata': {'name': POOL_NAME}, 'spec': {**expected, 'allowedUses': ['Workload']}}
        command('kubectl', 'create', '-f', '-', input=json.dumps(pool), capture_output=True)

    def _agents_and_devices(self):
        nodes = self._json('kubectl', 'get', 'nodes', '-o', 'json')['items']
        by_name = {node['metadata']['name']: node for node in nodes}
        addresses = {}
        for node in NODES:
            values = [a['address'] for a in by_name.get(node, {}).get('status', {}).get('addresses', [])
                      if a.get('type') == 'InternalIP' and ':' not in a.get('address', '')]
            if len(values) != 1:
                raise RuntimeError(f'{node} needs exactly one IPv4 InternalIP')
            addresses[node] = values[0]
        pods = self._json('kubectl', '-n', 'kube-system', 'get', 'pods', '-l', 'k8s-app=calico-node', '-o', 'json')['items']
        for node in NODES:
            matches = [p for p in pods if p.get('spec', {}).get('nodeName') == node
                       and not p['metadata'].get('deletionTimestamp')
                       and p.get('status', {}).get('phase') == 'Running'
                       and any(c.get('type') == 'Ready' and c.get('status') == 'True'
                               for c in p.get('status', {}).get('conditions', []))]
            if len(matches) != 1:
                raise RuntimeError(f'{node} needs exactly one ready Calico agent')
            self.agents[node] = matches[0]['metadata']['name']
        result = []
        for node in NODES:
            interfaces = {'vxlan.calico'}
            for peer in NODES:
                if peer == node:
                    continue
                route = json.loads(self._exec(node, 'ip', '-j', '-4', 'route', 'get', addresses[peer],
                                              capture_output=True).stdout)
                if len(route) != 1 or not route[0].get('dev'):
                    raise RuntimeError(f'cannot resolve interface from {node} to {peer}')
                interfaces.add(route[0]['dev'])
            for interface in sorted(interfaces):
                links = json.loads(self._exec(node, 'ip', '-j', 'link', 'show', 'dev', interface,
                                              capture_output=True).stdout)
                if len(links) != 1:
                    raise RuntimeError(f'{node} interface {interface} is unavailable')
                suffix = hashlib.sha256(f'{node}/{interface}/{self.token}'.encode()).hexdigest()[:8]
                result.append((node, interface, 'mec' + suffix))
        return result

    def _read_exec(self, node, description, *args):
        env = None
        for attempt in range(1, 4):
            try:
                return self._exec(node, *args, capture_output=True, timeout=10, env=env).stdout
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                detail = (exc.stderr or b'').strip()
                if isinstance(detail, bytes):
                    detail = detail.decode(errors='replace')
                message = f'{node}: {description} failed on attempt {attempt}/3: {detail or exc}'
                if attempt == 3:
                    raise RuntimeError(message) from exc
                logger.warning('%s; retrying in 1s with SPDY exec', message)
                env = dict(os.environ, KUBECTL_REMOTE_COMMAND_WEBSOCKETS='false')
                time.sleep(1)

    def _read_tc(self, node, *args):
        return self._read_exec(node, 'tc ' + ' '.join(args), 'tc', *args)

    def _tc_snapshot(self, node, queries):
        # A separator after each successful command also frames empty filter output.
        script = '\n'.join(shlex.join(('tc', *query)) + "\nprintf '\\036'" for query in queries)
        output = self._read_exec(node, 'tc health snapshot', 'sh', '-ec', script)
        *results, trailing = output.split('\x1e')
        if len(results) != len(queries) or trailing:
            raise RuntimeError(f'{node}: incomplete tc health snapshot')
        return iter(results)

    def _filter_query(self, interface, preference):
        return ('filter', 'show', 'dev', interface, 'egress', 'pref', str(preference))

    def _filter(self, node, interface, preference):
        return self._read_tc(node, *self._filter_query(interface, preference))

    def start(self):
        if self.started or self.devices:
            raise RuntimeError('traffic delay already started')
        self._pool()
        interfaces = self._agents_and_devices()
        try:
            for node, interface, ifb in interfaces:
                qdiscs = json.loads(self._read_tc(node, '-j', 'qdisc', 'show', 'dev', interface))
                if not any(q.get('kind') == 'clsact' for q in qdiscs):
                    self._exec(node, 'tc', 'qdisc', 'add', 'dev', interface, 'clsact')
                for preference in PREFERENCES:
                    if self._filter(node, interface, preference).strip():
                        raise RuntimeError(f'{node}/{interface} tc preference {preference} is occupied')
                self._exec(node, 'ip', 'link', 'add', ifb, 'type', 'ifb')
                record = {'node': node, 'interface': interface, 'ifb': ifb, 'filters': []}
                self.devices.append(record)
                self._exec(node, 'ip', 'link', 'set', ifb, 'up')
                self._exec(node, 'tc', 'qdisc', 'add', 'dev', ifb, 'root', 'handle', '1:', 'netem', 'delay', f'{self.delay_ms}ms')
                for preference, direction in zip(PREFERENCES, ('dst', 'src')):
                    self._exec(node, 'tc', 'filter', 'add', 'dev', interface, 'egress', 'protocol', 'ip',
                               'pref', str(preference), 'handle', '1', 'flower', 'ip_proto', 'tcp',
                               direction + '_ip', POOL_CIDR, direction + '_port', '5000',
                               'action', 'mirred', 'egress', 'redirect', 'dev', ifb)
                    record['filters'].append((preference, direction))
            self.started = True
            self.check()
        except BaseException:
            self.close()
            raise

    def check(self):
        if not self.started:
            raise RuntimeError('traffic delay is not running')
        by_node = {}
        for record in self.devices:
            by_node.setdefault(record['node'], []).append(record)
        for node, records in by_node.items():
            queries = []
            for record in records:
                queries.append(('-j', 'qdisc', 'show', 'dev', record['ifb']))
                queries.extend(self._filter_query(record['interface'], preference)
                               for preference, _ in record['filters'])
            results = self._tc_snapshot(node, queries)
            for record in records:
                interface, ifb = record['interface'], record['ifb']
                qdiscs = json.loads(next(results))
                delays = [q.get('options', {}).get('delay', {}).get('delay') for q in qdiscs if q.get('kind') == 'netem']
                # iproute2/kernel time conversion can truncate reported microseconds.
                if len(delays) != 1 or not isinstance(delays[0], (int, float)) or delays[0] <= 0 or not math.isclose(delays[0], self.delay_ms / 1000, rel_tol=1e-9, abs_tol=0.000002):
                    raise RuntimeError(f'{node}/{ifb} {self.delay_ms:g} ms delay is missing or changed')
                for preference, direction in record['filters']:
                    text = next(results)
                    if (f'{direction}_ip {POOL_CIDR}' not in text or f'{direction}_port 5000' not in text
                            or f'redirect to device {ifb}' not in text.lower()):
                        raise RuntimeError(f'{node}/{interface} workload tc filter {preference} is missing or changed')

    def close(self):
        errors = []
        for record in reversed(self.devices):
            node, interface, ifb = record['node'], record['interface'], record['ifb']
            errors_before = len(errors)
            for preference, direction in reversed(record['filters']):
                try:
                    text = self._filter(node, interface, preference)
                    if not text.strip():
                        continue
                    if (f'{direction}_ip {POOL_CIDR}' not in text or f'{direction}_port 5000' not in text
                            or f'redirect to device {ifb}' not in text.lower()):
                        raise RuntimeError(f'refusing to remove changed filter {node}/{interface}/{preference}')
                    self._exec(node, 'tc', 'filter', 'del', 'dev', interface, 'egress',
                               'pref', str(preference), 'handle', '1', 'flower')
                except (OSError, subprocess.SubprocessError, RuntimeError) as exc:
                    errors.append(str(exc))
            if len(errors) == errors_before:
                try:
                    self._exec(node, 'ip', 'link', 'del', ifb)
                except (OSError, subprocess.SubprocessError) as exc:
                    errors.append(str(exc))
        self.started = False
        if errors:
            raise RuntimeError('traffic delay cleanup failed: ' + '; '.join(errors))
        self.devices.clear()
