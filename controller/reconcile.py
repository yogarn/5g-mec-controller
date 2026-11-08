"""Observe and optionally reconcile one existing Deployment using optimistic locking."""
import copy
import json
import logging
import time
from kubernetes.utils.quantity import parse_quantity
from .policy import LatencyRatioPolicy, Node, rank_nodes

LOG = logging.getLogger(__name__)
ANNOTATION = 'mec-placement-controller.io/preferred-node'


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


def preferred_term(node):
    return {'weight': 100, 'preference': {'matchFields': [
        {'key': 'metadata.name', 'operator': 'In', 'values': [node]}]}}


def preference_patch(deployment, target):
    """Keep all user's affinity terms; replace only the previously recorded term."""
    metadata = deployment['metadata']
    annotations = metadata.get('annotations') or {}
    previous = annotations.get(ANNOTATION)
    affinity = copy.deepcopy(deployment['spec']['template']['spec'].get('affinity') or {})
    node_affinity = affinity.setdefault('nodeAffinity', {})
    terms = node_affinity.setdefault('preferredDuringSchedulingIgnoredDuringExecution', [])
    if previous:
        old_term = preferred_term(previous)
        if terms.count(old_term) != 1:
            raise ValueError('managed node affinity was externally modified or is ambiguous')
        terms.remove(old_term)
    new_term = preferred_term(target)
    if new_term in terms:
        raise ValueError('user affinity contains an indistinguishable preference')
    terms.append(new_term)
    return affinity


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
        cpu = self.prometheus.per_node(s.cpu_utilization_query, s.eligible_nodes)
        mobility = self.prometheus.per_node(s.mobility_query, s.eligible_nodes)
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
            nodes.append(Node(name, capacity, allocated[name], cpu[name], mobility[name], ready))
        return nodes

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
        if s.mode == 'mobility-aware':
            request = pod_requests(spec['template']['spec'])
            if request.get('cpu', 0) <= 0 or request.get('memory', 0) <= 0:
                raise ValueError('Deployment containers must request CPU and memory')
            request = {k: v for k, v in request.items() if v > 0}
            nodes = self._nodes(request)
            scores = rank_nodes(nodes, request, s.cpu_saturation, s.locality_weight)
            if not scores:
                raise ValueError('no eligible node passes resource and saturation filters')
            choice = max(sorted(scores), key=scores.get)
            current = (metadata.get('annotations') or {}).get(ANNOTATION)
            if current in scores and scores[choice] <= scores[current] * s.improvement_factor:
                choice = current
            if choice != current and ready == replicas and status.get('updatedReplicas', 0) == replicas:
                affinity = preference_patch(deployment, choice)
                changes['template'] = {'spec': {'affinity': affinity}}
                patch['metadata']['annotations'] = {ANNOTATION: choice}
            LOG.info('placement scores=%s selected=%s previous=%s', scores, choice, current)
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
