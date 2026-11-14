import copy
import json
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from controller.policy import Node
from controller.reconcile import MIGRATION_ANNOTATION, OWNER_LABEL, Reconciler


@pytest.fixture
def world():
    settings = SimpleNamespace(mode='mobility-aware', scale_down_seconds=0, min_replicas=1,
        max_replicas=2, latency_query='latency', slo_seconds=1, mutate=True,
        cooldown_seconds=0, eligible_nodes=('a', 'b'), zero_locality_nodes=(), cpu_saturation=.9,
        locality_weight=.5, resource_weights={'cpu': .4, 'memory': .4, 'gpu': .1, 'vram': .1})
    original = {'metadata': {'name': 'app', 'uid': 'original-uid', 'generation': 1,
                             'resourceVersion': '1'},
        'spec': {'replicas': 1, 'selector': {'matchLabels': {'app': 'app'}},
                 'template': {'metadata': {'labels': {'app': 'app'}}, 'spec': {
                     'containers': [{'name': 'app', 'resources': {'requests': {'cpu': '1', 'memory': '1'}}}]}}},
        'status': {'observedGeneration': 1, 'replicas': 1, 'updatedReplicas': 1,
                   'readyReplicas': 1, 'availableReplicas': 1}}
    apps, core, autoscaling, api, prom = (Mock() for _ in range(5))
    api.sanitize_for_serialization.side_effect = copy.deepcopy
    apps.read_namespaced_deployment.return_value = original
    apps.list_namespaced_deployment.return_value = {'items': [original]}
    autoscaling.list_namespaced_horizontal_pod_autoscaler.return_value = {'items': []}
    core.list_namespaced_service.return_value = {'items': [{'spec': {'selector': {'app': 'app'}}}]}
    prom.scalar.return_value = 1
    controller = Reconciler(settings, apps, core, autoscaling, api, prom, 'ns', 'app')
    controller._nodes = Mock(return_value=[Node('a', {'cpu': 10, 'memory': 10}, {'cpu': 1, 'memory': 1}, .1, 9),
                                          Node('b', {'cpu': 10, 'memory': 10}, {}, .1, 1)])
    def observe(groups=(), pending=False):
        deployments = [original, *groups]
        apps.list_namespaced_deployment.return_value = {'items': deployments}
        replica_sets, pods = [], []
        for d in deployments:
            name, uid = d['metadata']['name'], d['metadata']['uid']
            rs = 'rs-' + uid
            replica_sets.append({'metadata': {'uid': rs, 'ownerReferences': [{'uid': uid, 'controller': True}]}})
            for i in range(d['spec']['replicas']):
                node = d['metadata'].get('annotations', {}).get('mec-placement-controller.io/node', 'a')
                pods.append({'metadata': {'name': name + str(i), 'labels': d['spec']['template']['metadata']['labels'],
                                          'ownerReferences': [{'uid': rs, 'controller': True}]},
                             'spec': {'nodeName': node}, 'status': {'phase': 'Running', 'conditions': [
                                 {'type': 'Ready', 'status': 'False' if pending and d is not original else 'True'}]}})
        apps.list_namespaced_replica_set.return_value = {'items': replica_sets}
        core.list_namespaced_pod.return_value = {'items': pods}
    observe()
    return controller, original, apps, core, autoscaling, prom, observe


def group(controller, original, node, replicas):
    result = controller._group_body(original, node)
    result['metadata'].update(uid='child-' + node, generation=1, resourceVersion='2')
    result['spec']['replicas'] = replicas
    result['status'] = {'observedGeneration': 1, 'replicas': replicas, 'updatedReplicas': replicas,
                        'readyReplicas': replicas, 'availableReplicas': replicas}
    return result


def test_positive_delta_creates_pinned_group_with_shared_label(world):
    controller, original, apps, *_ = world
    controller._nodes.return_value[0] = Node('a', {'cpu': 10, 'memory': 10}, {'cpu': 1, 'memory': 1}, .1, 0)
    controller.run_once()
    body = apps.create_namespaced_deployment.call_args.args[1]
    assert body['metadata']['labels'][OWNER_LABEL] == original['metadata']['uid']
    assert body['spec']['template']['metadata']['labels']['app'] == 'app'
    assert body['spec']['replicas'] == 0
    assert body['spec']['selector'] != original['spec']['selector']
    assert body['spec']['template']['spec']['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchFields'][0]['values'] == ['b']
    apps.patch_namespaced_deployment.assert_not_called()


def test_dry_run_never_creates_or_scales(world):
    controller, _, apps, *_ = world
    controller.settings.mutate = False
    controller.run_once()
    apps.create_namespaced_deployment.assert_not_called()
    apps.patch_namespaced_deployment.assert_not_called()


def test_original_pod_stays_when_no_alternative_has_positive_delta(world):
    controller, _, apps, *_ = world
    controller.run_once()
    apps.create_namespaced_deployment.assert_not_called()
    apps.patch_namespaced_deployment.assert_not_called()


def settle(deployment, replicas):
    deployment['spec']['replicas'] = replicas
    deployment['status'].update(replicas=replicas, updatedReplicas=replicas,
                                readyReplicas=replicas, availableReplicas=replicas)


def save_state(original, apps):
    state = json.loads(apps.patch_namespaced_deployment.call_args.args[2]['metadata']['annotations'][MIGRATION_ANNOTATION])
    original['metadata']['annotations'] = {MIGRATION_ANNOTATION: json.dumps(state)}
    apps.patch_namespaced_deployment.reset_mock()
    return state


def test_restart_resumes_removal_first_even_when_no_replicas_remain(world):
    controller, original, apps, _, _, prom, observe = world
    controller._nodes.return_value[0] = Node('a', {'cpu': 10, 'memory': 10}, {'cpu': 1, 'memory': 1}, .1, 0)
    child = group(controller, original, 'b', 0)
    observe([child])
    controller.run_once()
    state = save_state(original, apps)
    assert state['kind'] == 'deschedule'
    controller.run_once()
    assert apps.patch_namespaced_deployment.call_args.args[0] == 'app'
    assert apps.patch_namespaced_deployment.call_args.args[2]['spec']['replicas'] == 0
    settle(original, 0)
    observe([child])
    apps.patch_namespaced_deployment.reset_mock()
    restarted = Reconciler(controller.settings, apps, controller.core, controller.autoscaling,
                           controller.api_client, prom, 'ns', 'app')
    restarted._nodes = controller._nodes
    # Replacement does not require fresh latency after the service goes to zero.
    prom.scalar.side_effect = ValueError('no latency samples')
    restarted.run_once()
    assert apps.patch_namespaced_deployment.call_args.args[0] == child['metadata']['name']
    assert apps.patch_namespaced_deployment.call_args.args[2]['spec']['replicas'] == 1
    settle(child, 1)
    observe([child], pending=True)
    apps.patch_namespaced_deployment.reset_mock()
    restarted.run_once()
    apps.patch_namespaced_deployment.assert_not_called()
    observe([child])
    restarted.run_once()
    assert apps.patch_namespaced_deployment.call_args.args[2]['metadata']['annotations'][MIGRATION_ANNOTATION] is None


def test_hpa_and_name_collision_fail_closed(world):
    controller, original, apps, _, autoscaling, _, observe = world
    child = group(controller, original, 'a', 0)
    observe([child])
    autoscaling.list_namespaced_horizontal_pod_autoscaler.return_value = {'items': [
        {'spec': {'scaleTargetRef': {'kind': 'Deployment', 'name': child['metadata']['name']}}}]}
    with pytest.raises(ValueError, match='HPA'):
        controller.run_once()
    autoscaling.list_namespaced_horizontal_pod_autoscaler.return_value = {'items': []}
    child['metadata']['labels'][OWNER_LABEL] = 'other'
    with pytest.raises(ValueError, match='conflict'):
        controller.run_once()
    apps.patch_namespaced_deployment.assert_not_called()


def test_invalid_latency_does_not_mutate(world):
    controller, _, apps, _, _, prom, _ = world
    prom.scalar.return_value = float('nan')
    with pytest.raises(ValueError):
        controller.run_once()
    apps.patch_namespaced_deployment.assert_not_called()
    apps.create_namespaced_deployment.assert_not_called()


def test_at_cap_descheduling_removes_first_and_waits_for_pod_disappearance(world):
    controller, original, apps, core, _, prom, observe = world
    # A p90 breach at the ceiling must still allow score-improving placement.
    prom.scalar.return_value = 5
    settle(original, 0)
    source = group(controller, original, 'a', 2)
    dest = group(controller, original, 'b', 0)
    observe([source, dest])
    controller._nodes.return_value = [
        Node('a', {'cpu': 10, 'memory': 10}, {'cpu': 2, 'memory': 2}, .1, 0),
        Node('b', {'cpu': 10, 'memory': 10}, {}, .1, 10)]
    controller.run_once()
    state = save_state(original, apps)
    assert state['source'] == source['metadata']['name']
    assert state['destination'] == dest['metadata']['name']
    controller.run_once()
    assert apps.patch_namespaced_deployment.call_args.args[0] == source['metadata']['name']
    assert apps.patch_namespaced_deployment.call_args.args[2]['spec']['replicas'] == 1
    settle(source, 1)
    # API still reports two source Pods, even though Deployment status is settled.
    apps.patch_namespaced_deployment.reset_mock()
    controller.run_once()
    apps.patch_namespaced_deployment.assert_not_called()
    observe([source, dest])
    controller.run_once()
    assert apps.patch_namespaced_deployment.call_args.args[0] == dest['metadata']['name']
    assert apps.patch_namespaced_deployment.call_args.args[2]['spec']['replicas'] == 1
    settle(dest, 1)
    observe([source, dest], pending=True)
    apps.patch_namespaced_deployment.reset_mock()
    controller.run_once()
    apps.patch_namespaced_deployment.assert_not_called()
    observe([source, dest])
    controller.run_once()
    assert apps.patch_namespaced_deployment.call_args.args[2]['metadata']['annotations'][MIGRATION_ANNOTATION] is None


def test_batch_scale_up_resumes_without_cooldown_or_readiness(world):
    controller, original, apps, _, _, prom, observe = world
    controller.settings.max_replicas = 10
    controller.settings.cooldown_seconds = 120
    prom.scalar.return_value = 4
    controller.run_once()
    state = json.loads(apps.patch_namespaced_deployment.call_args.args[2]['metadata']['annotations'][MIGRATION_ANNOTATION])
    assert state['kind'] == 'batch-scale-up'
    assert sum(state['targets'].values()) == 3
    assert state['original'] == 1
    original['metadata']['annotations'] = {MIGRATION_ANNOTATION: json.dumps(state)}
    children = []
    for node in controller.settings.eligible_nodes:
        if not state['targets'][node]:
            continue
        observe(children, pending=True)
        apps.create_namespaced_deployment.reset_mock()
        controller.run_once()
        assert apps.create_namespaced_deployment.call_count == 1
        child = group(controller, original, node, 0)
        children.append(child)
        observe(children, pending=True)
        apps.patch_namespaced_deployment.reset_mock()
        controller.run_once()
        assert apps.patch_namespaced_deployment.call_args.args[0] == child['metadata']['name']
        assert apps.patch_namespaced_deployment.call_args.args[2]['spec']['replicas'] == state['targets'][node]
        child['spec']['replicas'] = state['targets'][node]
    observe(children, pending=True)
    apps.patch_namespaced_deployment.reset_mock()
    controller.run_once()
    assert apps.patch_namespaced_deployment.call_args.args[2]['metadata']['annotations'][MIGRATION_ANNOTATION] is None


def test_batch_scale_up_rejects_external_replica_change(world):
    controller, original, apps, _, _, _, observe = world
    controller.settings.max_replicas = 10
    child = group(controller, original, 'a', 2)
    original['metadata']['annotations'] = {MIGRATION_ANNOTATION: json.dumps({
        'kind': 'batch-scale-up', 'original': 1,
        'before': {'a': 0, 'b': 0}, 'targets': {'a': 3, 'b': 0}})}
    observe([child])
    with pytest.raises(ValueError, match='changed externally'):
        controller.run_once()
    apps.patch_namespaced_deployment.assert_not_called()


def test_periodic_descheduling_waits_until_next_check(world):
    controller, original, apps, _, _, _, observe = world
    settle(original, 0)
    source = group(controller, original, 'a', 2)
    dest = group(controller, original, 'b', 0)
    observe([source, dest])
    controller._nodes.return_value = [
        Node('a', {'cpu': 10, 'memory': 10}, {'cpu': 2, 'memory': 2}, .1, 0),
        Node('b', {'cpu': 10, 'memory': 10}, {}, .1, 10)]
    controller.last_deschedule_check = 100
    with patch('controller.reconcile.time.monotonic', return_value=219):
        controller.run_once()
    apps.patch_namespaced_deployment.assert_not_called()
    with patch('controller.reconcile.time.monotonic', return_value=220):
        controller.run_once()
    assert save_state(original, apps)['kind'] == 'deschedule'


def test_descheduling_waits_and_retries_if_destination_loses_capacity(world):
    controller, original, apps, _, _, _, observe = world
    controller._nodes.return_value[0] = Node('a', {'cpu': 10, 'memory': 10}, {'cpu': 1, 'memory': 1}, .1, 0)
    child = group(controller, original, 'b', 0)
    observe([child])
    controller.run_once()
    save_state(original, apps)
    # Source is already removed, but target is no longer feasible.
    settle(original, 0)
    observe([child])
    controller._nodes.return_value = [
        Node('a', {'cpu': 10, 'memory': 10}, {}, .1, 0),
        Node('b', {'cpu': 10, 'memory': 10}, {'cpu': 10, 'memory': 10}, .1, 1)]
    with pytest.raises(ValueError, match='temporarily unavailable'):
        controller.run_once()
    apps.patch_namespaced_deployment.assert_not_called()
    controller._nodes.return_value[1] = Node('b', {'cpu': 10, 'memory': 10}, {}, .1, 1)
    controller.run_once()
    assert apps.patch_namespaced_deployment.call_args.args[0] == child['metadata']['name']
    assert apps.patch_namespaced_deployment.call_args.args[2]['spec']['replicas'] == 1


def test_legacy_surge_state_is_rejected_without_mutation(world):
    controller, original, apps, _, _, _, observe = world
    child = group(controller, original, 'a', 0)
    observe([child])
    original['metadata']['annotations'] = {MIGRATION_ANNOTATION: json.dumps({
        'source': 'app', 'destination': child['metadata']['name'],
        'source_before': 1, 'destination_before': 0})}
    with pytest.raises(ValueError, match='finish old migrations'):
        controller.run_once()
    apps.patch_namespaced_deployment.assert_not_called()


def test_ratio_target_can_reach_configured_hundred_replica_ceiling(world):
    controller, _, apps, _, _, prom, _ = world
    controller.settings.max_replicas = 100
    controller._nodes.return_value = [
        Node('a', {'cpu': 200, 'memory': 200}, {'cpu': 1, 'memory': 1}, .1, 1),
        Node('b', {'cpu': 200, 'memory': 200}, {}, .1, 1)]
    prom.scalar.return_value = 100
    controller.run_once()
    state = json.loads(apps.patch_namespaced_deployment.call_args.args[2]['metadata']['annotations'][MIGRATION_ANNOTATION])
    assert state['original'] + sum(state['targets'].values()) == 100
