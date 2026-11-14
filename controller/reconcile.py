"""Observe and reconcile scaling and controller-owned pinned Deployment groups."""
import copy
import json
import logging
import math
import hashlib
import time
from kubernetes.utils.quantity import parse_quantity
from .policy import LatencyRatioPolicy, Node, plan_placement

LOG = logging.getLogger(__name__)
OWNER_LABEL = 'mec-placement-controller.io/owner'
GROUP_LABEL = 'mec-placement-controller.io/group'
NODE_ANNOTATION = 'mec-placement-controller.io/node'
MIGRATION_ANNOTATION = 'mec-placement-controller.io/migration'


def quantities(resources):
    return {k: float(parse_quantity(str(v))) for k, v in (resources or {}).items()}


def pod_requests(spec):
    """Kubernetes scheduler request accounting, including init maxima and overhead."""
    total = {}
    for container in spec.get('containers', []):
        for key, value in quantities(container.get('resources', {}).get('requests')).items():
            total[key] = total.get(key, 0) + value
    for container in spec.get('initContainers', []):
        if container.get('restartPolicy') == 'Always':
            raise ValueError('restartable init containers are not supported for request accounting')
        for key, value in quantities(container.get('resources', {}).get('requests')).items():
            total[key] = max(total.get(key, 0), value)
    for key, value in quantities(spec.get('overhead')).items():
        total[key] = total.get(key, 0) + value
    if spec.get('resources'):
        raise ValueError('pod-level resources are not supported for request accounting')
    return total



def selector_matches(selector, labels):
    if not selector or any(labels.get(k) != v for k, v in selector.get('matchLabels', {}).items()):
        return False
    for term in selector.get('matchExpressions', []):
        key, op = term['key'], term['operator']
        values = term.get('values', [])
        if op == 'In':
            matched = key in labels and labels[key] in values
        elif op == 'NotIn':
            matched = key not in labels or labels[key] not in values
        elif op == 'Exists':
            matched = key in labels
        elif op == 'DoesNotExist':
            matched = key not in labels
        else:
            raise ValueError('unknown Deployment selector operator')
        if not matched:
            return False
    return True


class Reconciler:
    def __init__(self, settings, apps, core, autoscaling, api_client, prometheus, namespace, name):
        self.settings, self.apps, self.core, self.autoscaling = settings, apps, core, autoscaling
        self.api_client, self.prometheus = api_client, prometheus
        self.namespace, self.name = namespace, name
        self.last_action = None
        self.policy = LatencyRatioPolicy(settings.scale_down_seconds)

    def _dict(self, obj):
        return self.api_client.sanitize_for_serialization(obj)

    def _nodes(self, request):
        s = self.settings
        zero_locality_nodes = s.zero_locality_nodes
        try:
            cpu = self.prometheus.per_node(s.cpu_utilization_query, s.eligible_nodes)
        except ValueError as exc:
            raise ValueError(
                f'CPU utilization query requires one fresh node-labelled series per eligible node '
                f'(including {list(zero_locality_nodes)}): {exc}'
            ) from exc
        locality_nodes = tuple(name for name in s.eligible_nodes if name not in zero_locality_nodes)
        mobility = self.prometheus.per_node(s.mobility_query, locality_nodes)
        mobility.update({name: 0.0 for name in zero_locality_nodes})
        gpu, vram = {}, {}
        if s.gpu_nodes:
            for query, values in ((s.gpu_utilization_query, gpu),
                                  (s.vram_utilization_query, vram)):
                if not isinstance(query, str) or not query.strip():
                    raise ValueError('GPU-capable nodes require GPU and VRAM queries')
                readings = self.prometheus.per_node(query, s.gpu_nodes)
                if not isinstance(readings, dict) or set(readings) != set(s.gpu_nodes):
                    raise ValueError('GPU query must return one series per GPU-capable node')
                for name, utilization in readings.items():
                    if (isinstance(utilization, bool) or not isinstance(utilization, (int, float)) or
                            not math.isfinite(utilization) or not 0 <= utilization <= 1):
                        raise ValueError(f'invalid GPU or VRAM utilization for node {name}')
                    values[name] = 1 - utilization
        all_nodes = {n['metadata']['name']: n for n in self._dict(self.core.list_node())['items']}
        if not set(s.eligible_nodes) <= set(all_nodes):
            raise ValueError('eligible node missing from Kubernetes')
        allocated = {n: {} for n in s.eligible_nodes}
        pods = self._dict(self.core.list_pod_for_all_namespaces())['items']
        for pod in pods:
            node = pod.get('spec', {}).get('nodeName')
            if node not in allocated or pod.get('status', {}).get('phase') in ('Succeeded', 'Failed'):
                continue
            for key, value in pod_requests(pod['spec']).items():
                allocated[node][key] = allocated[node].get(key, 0) + value
        nodes = []
        for name in s.eligible_nodes:
            item = all_nodes[name]
            status = item.get('status', {})
            ready = any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in status.get('conditions', []))
            ready = ready and not item.get('spec', {}).get('unschedulable', False)
            capacity = quantities(status.get('allocatable'))
            if any(capacity.get(k, 0) <= 0 for k in ('cpu', 'memory')):
                raise ValueError(f'node {name} lacks allocatable CPU or memory')
            nodes.append(Node(name, capacity, allocated[name], cpu[name], mobility[name], ready,
                              gpu.get(name, 0.0), vram.get(name, 0.0)))
        return nodes

    def _group_body(self, original, node):
        uid = original['metadata']['uid']
        token = hashlib.sha256((uid + '\0' + node).encode()).hexdigest()[:16]
        name = self.name[:46].rstrip('-.') + '-mec-' + token
        spec = copy.deepcopy(original['spec'])
        template = spec['template']
        labels = template.setdefault('metadata', {}).setdefault('labels', {})
        if OWNER_LABEL in labels or GROUP_LABEL in labels:
            raise ValueError('original template uses reserved placement labels')
        labels.update({OWNER_LABEL: uid, GROUP_LABEL: token})
        pod = template['spec']
        if pod.get('nodeName'):
            raise ValueError('original template already pins nodeName')
        affinity = pod.setdefault('affinity', {}).setdefault('nodeAffinity', {})
        required = affinity.get('requiredDuringSchedulingIgnoredDuringExecution')
        if required is None:
            required = {'nodeSelectorTerms': [{}]}
            affinity['requiredDuringSchedulingIgnoredDuringExecution'] = required
        elif not required.get('nodeSelectorTerms') or any(
                not term.get('matchExpressions') and not term.get('matchFields')
                for term in required['nodeSelectorTerms']):
            raise ValueError('original node affinity contains an unsatisfiable term')
        terms = required['nodeSelectorTerms']
        for term in terms:
            term.setdefault('matchFields', []).append(
                {'key': 'metadata.name', 'operator': 'In', 'values': [node]})
        spec.update({'replicas': 0, 'selector': {'matchLabels': {GROUP_LABEL: token}},
                     'strategy': {'type': 'Recreate'}})
        return {'apiVersion': 'apps/v1', 'kind': 'Deployment', 'spec': spec,
                'metadata': {'name': name, 'namespace': self.namespace,
                             'labels': {OWNER_LABEL: uid, GROUP_LABEL: token},
                             'annotations': {NODE_ANNOTATION: node},
                             'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment',
                                                  'name': self.name, 'uid': uid}]}}

    @staticmethod
    def _settled(deployment):
        metadata, status = deployment['metadata'], deployment.get('status', {})
        replicas = deployment['spec'].get('replicas', 1)
        return (status.get('observedGeneration', 0) >= metadata['generation'] and
                all(status.get(key, 0) == replicas for key in
                    ('replicas', 'updatedReplicas', 'readyReplicas', 'availableReplicas')))

    def _replicas(self, deployment, count):
        self.apps.patch_namespaced_deployment(
            deployment['metadata']['name'], self.namespace,
            {'metadata': {'resourceVersion': deployment['metadata']['resourceVersion']},
             'spec': {'replicas': count}})

    def _migration(self, original, state):
        self.apps.patch_namespaced_deployment(
            self.name, self.namespace,
            {'metadata': {'resourceVersion': original['metadata']['resourceVersion'],
                          'annotations': {MIGRATION_ANNOTATION:
                                          json.dumps(state, sort_keys=True) if state else None}}})

    def _mobility(self, original, hpas):
        s, meta, spec = self.settings, original['metadata'], original['spec']
        maximum = s.max_replicas
        if s.min_replicas > maximum:
            raise ValueError('minimum replicas exceeds the mobility replica cap')
        uid = meta.get('uid')
        if (not uid or meta.get('deletionTimestamp') or
                any(owner.get('controller') for owner in meta.get('ownerReferences', []))):
            raise ValueError('original Deployment has no exclusive stable identity')
        request = pod_requests(spec['template']['spec'])
        if request.get('cpu', 0) <= 0 or request.get('memory', 0) <= 0:
            raise ValueError('Deployment containers must request CPU and memory')
        request = {k: v for k, v in request.items() if v > 0}
        nodes = self._nodes(request)
        expected = {n: self._group_body(original, n) for n in s.eligible_nodes}
        expected_names = {d['metadata']['name'] for d in expected.values()}
        if len(expected_names) != len(expected):
            raise ValueError('placement Deployment name collision')
        deployments = self._dict(self.apps.list_namespaced_deployment(self.namespace))['items']
        by_name = {d['metadata']['name']: d for d in deployments}
        groups = {}
        for node, body in expected.items():
            name = body['metadata']['name']
            group = by_name.get(name)
            if group is None:
                continue
            gm = group['metadata']
            if (gm.get('deletionTimestamp') or
                    gm.get('labels', {}).get(OWNER_LABEL) != uid or
                    gm.get('labels', {}).get(GROUP_LABEL) != body['metadata']['labels'][GROUP_LABEL] or
                    gm.get('annotations', {}).get(NODE_ANNOTATION) != node or
                    not any(o.get('uid') == uid and o.get('kind') == 'Deployment' and
                            o.get('name') == self.name for o in gm.get('ownerReferences', [])) or
                    group['spec']['selector'] != body['spec']['selector'] or
                    group['spec']['template'] != body['spec']['template'] or
                    group['spec'].get('strategy', {}).get('type') != 'Recreate'):
                raise ValueError('placement Deployment ownership or template conflict: ' + name)
            groups[node] = group
        if any((d['metadata'].get('labels', {}).get(OWNER_LABEL) == uid or
                any(o.get('uid') == uid for o in d['metadata'].get('ownerReferences', []))) and
               d['metadata']['name'] not in expected_names for d in deployments):
            raise ValueError('unexpected owned placement Deployment')
        for item in deployments:
            if item['metadata']['name'] in expected_names | {self.name}:
                continue
            for body in expected.values():
                if selector_matches(item['spec'].get('selector'),
                                    body['spec']['template']['metadata']['labels']):
                    raise ValueError('unrelated Deployment selector overlaps placement pods')
        if any(h.get('spec', {}).get('scaleTargetRef', {}).get('kind') == 'Deployment' and
               h['spec']['scaleTargetRef'].get('name') in expected_names | {self.name} for h in hpas):
            raise ValueError('an HPA targets a managed Deployment')
        labels = spec['template'].get('metadata', {}).get('labels', {})
        services = self._dict(self.core.list_namespaced_service(self.namespace))['items']
        for service in services:
            selector = service.get('spec', {}).get('selector') or {}
            if not selector:
                continue
            matches = all(labels.get(k) == v for k, v in selector.items())
            for body in expected.values():
                child_labels = body['spec']['template']['metadata']['labels']
                if all(child_labels.get(k) == v for k, v in selector.items()) != matches:
                    raise ValueError('placement labels change Service selection')
        managed = {self.name: original}
        managed.update({d['metadata']['name']: d for d in groups.values()})
        deployment_uids = {d['metadata']['uid']: name for name, d in managed.items()}
        replica_sets = self._dict(self.apps.list_namespaced_replica_set(self.namespace))['items']
        rs_owners = {}
        for rs in replica_sets:
            for owner in rs['metadata'].get('ownerReferences', []):
                if owner.get('controller') and owner.get('uid') in deployment_uids:
                    rs_owners[rs['metadata']['uid']] = deployment_uids[owner['uid']]
        pod_counts = {name: 0 for name in managed}
        all_ready = True
        tokens = {body['metadata']['labels'][GROUP_LABEL] for body in expected.values()}
        group_tokens = {d['metadata']['name']: d['metadata']['labels'][GROUP_LABEL]
                        for d in groups.values()}
        for pod in self._dict(self.core.list_namespaced_pod(self.namespace))['items']:
            if pod.get('status', {}).get('phase') in ('Succeeded', 'Failed'):
                continue
            pod_labels = pod['metadata'].get('labels', {})
            token = pod_labels.get(GROUP_LABEL)
            pod_owner = next((o.get('uid') for o in pod['metadata'].get('ownerReferences', [])
                              if o.get('controller')), None)
            owner_name = rs_owners.get(pod_owner)
            if ((token in tokens or pod_labels.get(OWNER_LABEL) == uid) and
                    (owner_name not in group_tokens or
                     token != group_tokens[owner_name] or pod_labels.get(OWNER_LABEL) != uid)):
                raise ValueError('unowned pod conflicts with placement selector')
            if owner_name in group_tokens and (token != group_tokens[owner_name] or
                                                pod_labels.get(OWNER_LABEL) != uid):
                raise ValueError('managed pod placement labels changed')
            for owner in pod['metadata'].get('ownerReferences', []):
                if owner.get('controller') and owner.get('uid') in rs_owners:
                    pod_counts[rs_owners[owner['uid']]] += 1
                    if (pod['metadata'].get('deletionTimestamp') or
                            not any(c.get('type') == 'Ready' and c.get('status') == 'True'
                                    for c in pod.get('status', {}).get('conditions', []))):
                        all_ready = False
                    node = next((n for n, d in groups.items()
                                 if d['metadata']['name'] == rs_owners[owner['uid']]), None)
                    if node is not None and pod.get('spec', {}).get('nodeName') != node:
                        raise ValueError('managed pod is not on its pinned node')
        counts = {node: d['spec'].get('replicas', 1) for node, d in groups.items()}
        original_count = spec.get('replicas', 1)
        replicas = original_count + sum(counts.values())
        state_text = (meta.get('annotations') or {}).get(MIGRATION_ANNOTATION)
        try:
            state = json.loads(state_text) if state_text else None
        except (TypeError, ValueError):
            raise ValueError('invalid durable migration state') from None
        batch_state = isinstance(state, dict) and state.get('kind') == 'batch-scale-up'
        scaling_state = isinstance(state, dict) and state.get('kind') == 'scale'
        if batch_state:
            if (set(state) != {'kind', 'original', 'before', 'targets'} or
                    type(state['original']) is not int or state['original'] != original_count or
                    not isinstance(state['before'], dict) or not isinstance(state['targets'], dict) or
                    set(state['before']) != set(s.eligible_nodes) or
                    set(state['targets']) != set(s.eligible_nodes)):
                raise ValueError('invalid durable batch scaling state')
            for node in s.eligible_nodes:
                before, after = state['before'][node], state['targets'][node]
                current = counts.get(node, 0)
                if (type(before) is not int or type(after) is not int or
                        before < 0 or after < before or current not in (before, after)):
                    raise ValueError('batch scaling replica count changed externally')
            if not replicas <= original_count + sum(state['targets'].values()) <= maximum:
                raise ValueError('batch scaling target outside permitted bounds')
        elif scaling_state:
            if (set(state) != {'kind', 'destination', 'before', 'after'} or
                    state['destination'] not in managed or
                    type(state['before']) is not int or type(state['after']) is not int or
                    min(state['before'], state['after']) < 0 or
                    abs(state['after'] - state['before']) != 1 or
                    (state['destination'] == self.name and state['after'] > state['before'])):
                raise ValueError('invalid durable scaling state')
            dest = managed[state['destination']]
            dst_count = dest['spec'].get('replicas', 1)
            if dst_count not in (state['before'], state['after']):
                raise ValueError('scaling replica count changed externally')
        elif state is not None:
            if (not isinstance(state, dict) or set(state) != {'source', 'destination', 'source_before', 'destination_before'} or
                    state['source'] not in managed or state['destination'] not in managed or
                    state['source'] == state['destination'] or state['destination'] == self.name or
                    type(state['source_before']) is not int or state['source_before'] < 1 or
                    type(state['destination_before']) is not int or state['destination_before'] < 0):
                raise ValueError('invalid durable migration state')
            source, dest = managed[state['source']], managed[state['destination']]
            src_count, dst_count = source['spec'].get('replicas', 1), dest['spec'].get('replicas', 1)
            if (src_count, dst_count) not in ((state['source_before'], state['destination_before']),
                                              (state['source_before'], state['destination_before'] + 1),
                                              (state['source_before'] - 1, state['destination_before'] + 1)):
                raise ValueError('migration replica counts changed externally')
        if replicas < 1 or replicas > maximum + (1 if state and not scaling_state and not batch_state else 0):
            raise ValueError('managed replicas outside permitted bounds')
        if batch_state and s.mutate:
            for node in s.eligible_nodes:
                target = state['targets'][node]
                if counts.get(node, 0) == target:
                    continue
                if node not in groups:
                    self.apps.create_namespaced_deployment(self.namespace, expected[node])
                else:
                    self._replicas(groups[node], target)
                return
            self._migration(original, None)
            self.policy.action_applied(time.monotonic())
            self.last_action = time.monotonic()
            return
        try:
            latency = self.prometheus.scalar(s.latency_query)
            ready = sum(d.get('status', {}).get('readyReplicas', 0) for d in managed.values())
            desired = self.policy.target(replicas, min(ready, replicas), latency, s.slo_seconds,
                                         s.min_replicas, maximum, time.monotonic())
            placement = plan_placement(nodes, request, counts, sum(counts.values()),
                                       s.cpu_saturation, s.locality_weight, s.resource_weights)
        except Exception:
            self.policy.down_since = None
            raise
        if not s.mutate:
            LOG.info('mobility dry run replicas=%s target=%s placement=%s', replicas, desired, placement)
            return
        now = time.monotonic()
        if not state and self.last_action is not None and now - self.last_action < s.cooldown_seconds:
            return
        if not all_ready or any(not self._settled(d) or pod_counts[name] != d['spec'].get('replicas', 1)
                                for name, d in managed.items()):
            return
        if scaling_state:
            if dst_count == state['before']:
                if ((state['after'] > state['before'] and replicas >= maximum) or
                        (state['after'] < state['before'] and replicas <= s.min_replicas)):
                    raise ValueError('pending scaling action exceeds replica bounds')
                if ((state['after'] > state['before'] and desired <= replicas) or
                        (state['after'] < state['before'] and desired >= replicas)):
                    self._migration(original, None)
                    self.last_action = now
                    return
                if state['after'] > state['before']:
                    plan = plan_placement(nodes, request, counts, sum(counts.values()) + 1,
                                          s.cpu_saturation, s.locality_weight, s.resource_weights)
                    destination_node = next(n for n, d in groups.items() if d is dest)
                    if plan['targets'].get(destination_node, 0) <= counts[destination_node]:
                        self._migration(original, None)
                    else:
                        self._replicas(dest, state['after'])
                        self.policy.action_applied(now)
                else:
                    self._replicas(dest, state['after'])
                    self.policy.action_applied(now)
            else:
                self._migration(original, None)
        elif state:
            if (src_count, dst_count) == (state['source_before'], state['destination_before']):
                if sum(pod_counts.values()) >= maximum + 1:
                    raise ValueError('migration surge limit exceeded')
                if state['source'] == self.name:
                    plan = plan_placement(nodes, request, counts, sum(counts.values()) + 1,
                                          s.cpu_saturation, s.locality_weight, s.resource_weights)
                    destination_node = next(n for n, d in groups.items() if d is dest)
                    if plan['targets'].get(destination_node, 0) <= counts[destination_node]:
                        self._migration(original, None)
                        self.last_action = now
                        return
                else:
                    source_node = next(n for n, d in groups.items() if d is source)
                    destination_node = next(n for n, d in groups.items() if d is dest)
                    if placement['migration'] != (source_node, destination_node):
                        self._migration(original, None)
                        self.last_action = now
                        return
                self._replicas(dest, dst_count + 1)
            elif src_count == state['source_before']:
                self._replicas(source, src_count - 1)
            else:
                self._migration(original, None)
        elif desired > replicas:
            plan = plan_placement(nodes, request, counts, desired - original_count,
                                  s.cpu_saturation, s.locality_weight, s.resource_weights)
            self._migration(original, {
                'kind': 'batch-scale-up',
                'original': original_count,
                'before': {node: counts.get(node, 0) for node in s.eligible_nodes},
                'targets': plan['targets'],
            })
        elif original_count and desired < replicas:
            if sum(counts.values()):
                plan = plan_placement(nodes, request, counts, sum(counts.values()) - 1,
                                      s.cpu_saturation, s.locality_weight, s.resource_weights)
                node = next(n for n, value in plan['targets'].items() if value < counts.get(n, 0))
                target_group, before = groups[node], counts[node]
            else:
                target_group, before = original, original_count
            self._migration(original, {'kind': 'scale', 'destination': target_group['metadata']['name'],
                                       'before': before, 'after': before - 1})
        elif original_count:
            plan = plan_placement(nodes, request, counts, sum(counts.values()) + 1,
                                  s.cpu_saturation, s.locality_weight, s.resource_weights)
            node = next((n for n, value in plan['targets'].items() if value > counts.get(n, 0)), None)
            if node is None:
                raise ValueError('no feasible destination for original workload')
            if node not in groups:
                self.apps.create_namespaced_deployment(self.namespace, expected[node])
            else:
                self._migration(original, {'source': self.name, 'destination': groups[node]['metadata']['name'],
                                           'source_before': original_count, 'destination_before': counts[node]})
        elif desired != replicas:
            plan = plan_placement(nodes, request, counts, desired, s.cpu_saturation,
                                  s.locality_weight, s.resource_weights)
            direction = 1 if desired > replicas else -1
            node = next((n for n, value in plan['targets'].items()
                         if (value - counts.get(n, 0)) * direction > 0), None)
            if node is None:
                raise ValueError('placement cannot satisfy desired replicas')
            if node not in groups:
                self.apps.create_namespaced_deployment(self.namespace, expected[node])
            else:
                self._migration(original, {'kind': 'scale', 'destination': groups[node]['metadata']['name'],
                                           'before': counts[node], 'after': counts[node] + direction})
        elif placement['migration']:
            src, dst = placement['migration']
            if dst not in groups:
                self.apps.create_namespaced_deployment(self.namespace, expected[dst])
            else:
                self._migration(original, {'source': groups[src]['metadata']['name'],
                    'destination': groups[dst]['metadata']['name'],
                    'source_before': counts[src], 'destination_before': counts[dst]})
        else:
            return
        self.last_action = now

    def run_once(self):
        s = self.settings
        deployment = self._dict(self.apps.read_namespaced_deployment(self.name, self.namespace))
        spec, status = deployment['spec'], deployment.get('status', {})
        metadata = deployment['metadata']
        replicas = spec.get('replicas', 1)
        ready = status.get('readyReplicas', 0)
        if status.get('observedGeneration', 0) < metadata['generation']:
            raise ValueError('Deployment controller has not observed the latest generation')
        hpas = self._dict(self.autoscaling.list_namespaced_horizontal_pod_autoscaler(self.namespace))['items']
        if s.mode == 'mobility-aware':
            return self._mobility(deployment, hpas)
        if metadata.get('uid'):
            if (metadata.get('annotations') or {}).get(MIGRATION_ANNOTATION):
                raise ValueError('cannot switch to scaling-only during placement migration')
            others = self._dict(self.apps.list_namespaced_deployment(self.namespace))['items']
            if any(d['metadata'].get('labels', {}).get(OWNER_LABEL) == metadata['uid'] for d in others):
                raise ValueError('cannot switch to scaling-only while placement groups exist')
        if any(h.get('spec', {}).get('scaleTargetRef', {}).get('kind') == 'Deployment' and
               h['spec']['scaleTargetRef'].get('name') == self.name for h in hpas):
            raise ValueError('an HPA targets this Deployment; refusing to compete for replicas')
        try:
            latency = self.prometheus.scalar(s.latency_query)
            target = self.policy.target(replicas, ready, latency, s.slo_seconds,
                                        s.min_replicas, s.max_replicas, time.monotonic())
        except Exception:
            self.policy.down_since = None
            raise
        patch = {'metadata': {'resourceVersion': metadata['resourceVersion']}}
        changes = {}
        if target != replicas:
            changes['replicas'] = target
        choice = None
        if changes:
            patch['spec'] = changes
        LOG.info('latency_seconds=%s replicas=%s ready=%s target=%s placement=%s mutation=%s', latency, replicas, ready, target, choice, s.mutate)
        if changes and s.mutate:
            now = time.monotonic()
            if self.last_action is not None and now - self.last_action < s.cooldown_seconds:
                LOG.info('cooldown active; no mutation')
                return
            self.apps.patch_namespaced_deployment(self.name, self.namespace, patch)
            self.policy.action_applied(now)
            self.last_action = now
