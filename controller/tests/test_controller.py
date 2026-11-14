import copy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from controller.config import load_settings, workload_identity
from controller.policy import LatencyRatioPolicy, Node, rank_nodes
from controller.reconcile import ANNOTATION, Reconciler, pod_requests, preference_patch, preferred_term
from controller.telemetry import Prometheus

WEIGHTS = {'cpu': .4, 'memory': .4, 'gpu': .1, 'vram': .1}


class ManifestTests(unittest.TestCase):
    def test_multidocument_identity_and_namespace_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'workload.yaml'
            path.write_text('apiVersion: v1\nkind: Service\nmetadata:\n  name: gateway\n---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: arbitrary\n  namespace: edge\n')
            self.assertEqual(workload_identity(path, None), ('edge', 'arbitrary'))
            with self.assertRaises(ValueError):
                workload_identity(path, 'other')
            path.write_text(path.read_text() + '---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: second\n')
            with self.assertRaises(ValueError):
                workload_identity(path, None)

    def test_mutation_is_opt_in(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / 'config.yaml'
            config.write_text('manifest: app.yaml\nprometheus_url: http://localhost:9090\nlatency_query: latency\nslo_seconds: 1\nmin_replicas: 1\nmax_replicas: 4\nmode: scaling-only\n')
            settings = load_settings(str(config))
            self.assertFalse(settings.mutate)
            self.assertEqual(settings.manifest, Path(directory) / 'app.yaml')
            self.assertEqual(settings.locality_weight, .4)
            self.assertEqual(settings.resource_weights, WEIGHTS)
            self.assertEqual(settings.gpu_nodes, ())

    def test_gpu_configuration_and_weights_are_strict(self):
        base = ('manifest: app.yaml\nprometheus_url: http://localhost:9090\n'
                'latency_query: latency\nslo_seconds: 1\nmin_replicas: 1\n'
                'max_replicas: 4\nmode: mobility-aware\neligible_nodes: [a, b]\n'
                'cpu_utilization_query: cpu\nmobility_query: traffic\n')
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / 'config.yaml'
            valid = 'gpu_nodes: [b]\ngpu_utilization_query: gpu\nvram_utilization_query: vram\n'
            config.write_text(base + valid)
            settings = load_settings(str(config))
            self.assertEqual(settings.gpu_nodes, ('b',))
            config.write_text(base)
            with self.assertRaisesRegex(ValueError, 'explicit gpu_nodes'):
                load_settings(str(config))
            config.write_text(base + 'gpu_nodes: []\n')
            self.assertEqual(load_settings(str(config)).gpu_nodes, ())
            self.assertEqual(settings.gpu_utilization_query, 'gpu')
            for extra in (
                'gpu_nodes: [b, b]\n',
                'gpu_nodes: [c]\n',
                'gpu_nodes: [b]\n',
                'gpu_nodes: [b]\ngpu_utilization_query: gpu\nvram_utilization_query: " "\n',
                'resource_weights: {cpu: 0.4, memory: 0.4, gpu: 0.1}\n',
                'resource_weights: {cpu: 0.4, memory: 0.4, gpu: -0.1, vram: 0.3}\n',
                'resource_weights: {cpu: 0.4, memory: 0.4, gpu: 0.1, vram: 0.2}\n',
                'resource_weights: {cpu: .nan, memory: 0.4, gpu: 0.1, vram: 0.1}\n',
            ):
                with self.subTest(extra=extra):
                    config.write_text(base + extra)
                    with self.assertRaises(ValueError):
                        load_settings(str(config))


class PolicyTests(unittest.TestCase):
    def test_ratio_scaling_and_stabilization(self):
        policy = LatencyRatioPolicy(20)
        self.assertEqual(policy.target(2, 2, 2, 1, 1, 4, 0), 4)
        self.assertEqual(policy.target(2, 2, 100, 1, 1, 4, 1), 4)
        self.assertEqual(policy.target(3, 2, .2, 1, 1, 4, 20), 3)
        self.assertEqual(policy.target(3, 3, .2, 1, 1, 4, 21), 3)
        self.assertEqual(policy.target(3, 3, .2, 1, 1, 4, 40), 3)
        self.assertEqual(policy.target(3, 3, .2, 1, 1, 4, 41), 1)
        policy.action_applied(41)
        self.assertEqual(policy.target(3, 3, .2, 1, 1, 4, 42), 3)
        with self.assertRaises(ValueError):
            policy.target(2, 2, float('nan'), 1, 1, 4, 43)
        self.assertEqual(policy.target(3, 3, .2, 1, 1, 4, 44), 3)

    def test_capacity_saturation_and_locality(self):
        nodes = [Node('a', {'cpu': 4, 'memory': 8}, {'cpu': 1, 'memory': 2}, .2, 8),
                 Node('b', {'cpu': 4, 'memory': 8}, {'cpu': 1, 'memory': 2}, .2, 2),
                 Node('c', {'cpu': 4, 'memory': 8}, {'cpu': 1, 'memory': 2}, .9, 0)]
        scores = rank_nodes(nodes, {'cpu': 1, 'memory': 1}, .8, .3, WEIGHTS)
        self.assertGreater(scores['a'], scores['b'])
        self.assertNotIn('c', scores)
        self.assertEqual(rank_nodes(nodes, {'cpu': 9}, .8, .3, WEIGHTS), {})

    def test_weighted_score_and_relative_headroom(self):
        nodes = [
            Node('small', {'cpu': 4, 'memory': 8}, {'cpu': 1, 'memory': 3}, .2, 3,
                 gpu_free=.8, vram_free=.5),
            Node('large', {'cpu': 8, 'memory': 16}, {'cpu': 3, 'memory': 7}, .2, 1,
                 gpu_free=.8, vram_free=.5),
            Node('no-gpu', {'cpu': 4, 'memory': 8}, {'cpu': 1, 'memory': 3}, .2, 0),
        ]
        scores = rank_nodes(nodes, {'cpu': 1, 'memory': 1}, .8, .4, WEIGHTS)
        resource = .4 * .5 + .4 * .5 + .1 * .8 + .1 * .5
        self.assertAlmostEqual(scores['small'], .6 * resource + .4 * .75)
        self.assertAlmostEqual(scores['large'], .6 * resource + .4 * .25)
        self.assertAlmostEqual(scores['no-gpu'], .6 * (.4 * .5 + .4 * .5))
        idle = [Node('a', {'cpu': 4, 'memory': 8}, {}, .1, 0)]
        self.assertAlmostEqual(rank_nodes(idle, {'cpu': 0, 'memory': 0}, .8, .4, WEIGHTS)['a'], .48)

    def test_gpu_resource_feasibility_and_invalid_inputs(self):
        nodes = [Node('a', {'cpu': 4, 'memory': 8, 'nvidia.com/gpu': 1},
                      {'nvidia.com/gpu': 1}, .2, 0, gpu_free=1, vram_free=1),
                 Node('b', {'cpu': 4, 'memory': 8, 'nvidia.com/gpu': 2},
                      {}, .2, 0, gpu_free=.5, vram_free=.5)]
        scores = rank_nodes(nodes, {'cpu': 1, 'nvidia.com/gpu': 1}, .8, .4, WEIGHTS)
        self.assertEqual(set(scores), {'b'})
        for weights in ({}, {**WEIGHTS, 'gpu': float('nan')},
                        {**WEIGHTS, 'gpu': -1}, {**WEIGHTS, 'gpu': .2}):
            with self.subTest(weights=weights), self.assertRaises(ValueError):
                rank_nodes(nodes, {'cpu': 1}, .8, .4, weights)
        with self.assertRaises(ValueError):
            rank_nodes([Node('bad', {'cpu': 1}, {}, .2, 0, gpu_free=1.1)],
                       {'cpu': 1}, .8, .4, WEIGHTS)

    def test_init_and_overhead_requests(self):
        spec = {'containers': [{'resources': {'requests': {'cpu': '500m', 'memory': '1Gi'}}}],
                'initContainers': [{'resources': {'requests': {'cpu': '2'}}}],
                'overhead': {'cpu': '100m'}}
        self.assertAlmostEqual(pod_requests(spec)['cpu'], 2.1)
        spec['initContainers'][0]['restartPolicy'] = 'Always'
        with self.assertRaises(ValueError):
            pod_requests(spec)

    def test_preference_preserves_user_constraints(self):
        deployment = {'metadata': {'annotations': {ANNOTATION: 'old'}}, 'spec': {'template': {'spec': {
            'affinity': {'nodeAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': {'nodeSelectorTerms': []},
                         'preferredDuringSchedulingIgnoredDuringExecution': [preferred_term('old')]},
                         'podAntiAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': []}}}}}}
        result = preference_patch(deployment, 'new')
        self.assertIn('podAntiAffinity', result)
        self.assertIn('requiredDuringSchedulingIgnoredDuringExecution', result['nodeAffinity'])
        self.assertEqual(result['nodeAffinity']['preferredDuringSchedulingIgnoredDuringExecution'], [preferred_term('new')])
        self.assertEqual(deployment['spec']['template']['spec']['affinity']['nodeAffinity']['preferredDuringSchedulingIgnoredDuringExecution'], [preferred_term('old')])


class ReconciliationTests(unittest.TestCase):
    def setup_controller(self, mutate=False, hpa=False):
        deployment = {'metadata': {'generation': 1, 'resourceVersion': '42'},
                      'spec': {'replicas': 2, 'template': {'spec': {}}},
                      'status': {'observedGeneration': 1, 'readyReplicas': 2}}
        apps = Mock()
        apps.read_namespaced_deployment.return_value = deployment
        autoscaling = Mock()
        autoscaling.list_namespaced_horizontal_pod_autoscaler.return_value = {'items': ([{'spec': {'scaleTargetRef': {'kind': 'Deployment', 'name': 'app'}}}] if hpa else [])}
        api = Mock()
        api.sanitize_for_serialization.side_effect = lambda x: copy.deepcopy(x)
        prom = Mock()
        prom.scalar.return_value = 2
        settings = SimpleNamespace(mode='scaling-only', latency_query='latency', slo_seconds=1,
                                   scale_down_seconds=20, min_replicas=1, max_replicas=4,
                                   mutate=mutate, cooldown_seconds=10)
        return Reconciler(settings, apps, Mock(), autoscaling, api, prom, 'ns', 'app'), apps, prom

    def test_dry_run_and_real_patch(self):
        controller, apps, _ = self.setup_controller()
        controller.run_once()
        apps.patch_namespaced_deployment.assert_not_called()
        controller, apps, _ = self.setup_controller(mutate=True)
        controller.run_once()
        apps.patch_namespaced_deployment.assert_called_once_with('app', 'ns', {'metadata': {'resourceVersion': '42'}, 'spec': {'replicas': 4}})

    def test_missing_metric_and_hpa_fail_closed(self):
        controller, apps, prom = self.setup_controller(mutate=True)
        prom.scalar.side_effect = ValueError('missing metric')
        with self.assertRaises(ValueError):
            controller.run_once()
        apps.patch_namespaced_deployment.assert_not_called()
        controller, apps, _ = self.setup_controller(mutate=True, hpa=True)
        with self.assertRaises(ValueError):
            controller.run_once()
        apps.patch_namespaced_deployment.assert_not_called()

    def test_mobility_preference_is_soft_and_resource_versioned(self):
        controller, apps, _ = self.setup_controller(mutate=True)
        controller.settings.mode = 'mobility-aware'
        controller.settings.cpu_saturation = .8
        controller.settings.locality_weight = .3
        controller.settings.resource_weights = WEIGHTS
        controller.settings.improvement_factor = 1.1
        deployment = apps.read_namespaced_deployment.return_value
        deployment['spec']['template']['spec']['containers'] = [
            {'resources': {'requests': {'cpu': '1', 'memory': '1Gi'}}}]
        deployment['status']['updatedReplicas'] = 2
        controller._nodes = Mock(return_value=[
            Node('edge-a', {'cpu': 4, 'memory': 4 * 2**30}, {}, .2, 5)])
        controller.run_once()
        patch = apps.patch_namespaced_deployment.call_args.args[2]
        self.assertEqual(patch['metadata']['resourceVersion'], '42')
        self.assertEqual(patch['metadata']['annotations'][ANNOTATION], 'edge-a')
        affinity = patch['spec']['template']['spec']['affinity']['nodeAffinity']
        self.assertNotIn('requiredDuringSchedulingIgnoredDuringExecution', affinity)
        self.assertEqual(affinity['preferredDuringSchedulingIgnoredDuringExecution'], [preferred_term('edge-a')])

    def test_missing_node_series_fails_closed(self):
        prom = Prometheus('http://example', 60)
        prom.vector = Mock(return_value=[({'node': 'a'}, 1)])
        with self.assertRaises(ValueError):
            prom.per_node('query', ('a', 'b'))

    def test_nan_and_stale_telemetry_remain_unusable(self):
        prom = Prometheus('http://example', 60)
        response = Mock()
        prom.session.get = Mock(return_value=response)
        with patch('controller.telemetry.time.time', return_value=100):
            response.json.return_value = {'status': 'success', 'data': {
                'resultType': 'vector', 'result': [{'metric': {}, 'value': [100, 'NaN']}]}}
            with self.assertRaisesRegex(ValueError, 'sample is NaN'):
                prom.scalar('latency')
            response.json.return_value['data']['result'][0]['value'][0] = 1
            with self.assertRaisesRegex(ValueError, 'invalid or stale'):
                prom.scalar('latency')
            response.json.return_value['data']['result'][0]['value'] = [100, '+Inf']
            with self.assertRaisesRegex(ValueError, 'invalid or stale'):
                prom.scalar('latency')


if __name__ == '__main__':
    unittest.main()
