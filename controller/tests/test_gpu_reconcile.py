import copy
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from controller.reconcile import Reconciler
from controller.telemetry import Prometheus


class GPUReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.settings = SimpleNamespace(
            scale_down_seconds=20, mode='mobility-aware', latency_query='latency',
            slo_seconds=1, min_replicas=1, max_replicas=4, mutate=False,
            cooldown_seconds=10, eligible_nodes=('gnb1', 'gnb-2', 'gnb-3'),
            gpu_nodes=('gnb-2', 'gnb-3'), cpu_utilization_query='cpu',
            mobility_query='traffic', gpu_utilization_query='gpu',
            vram_utilization_query='vram', cpu_saturation=.8, locality_weight=.4,
            improvement_factor=1.1,
            resource_weights={'cpu': .4, 'memory': .4, 'gpu': .1, 'vram': .1})
        self.apps = Mock()
        self.apps.read_namespaced_deployment.return_value = {
            'metadata': {'generation': 1, 'resourceVersion': '42'},
            'spec': {'replicas': 2, 'template': {'spec': {'containers': [
                {'resources': {'requests': {'cpu': '1', 'memory': '1Gi'}}}]}}},
            'status': {'observedGeneration': 1, 'readyReplicas': 2, 'updatedReplicas': 2}}
        self.core = Mock()
        self.core.list_node.return_value = {'items': [
            {'metadata': {'name': name}, 'status': {
                'allocatable': {'cpu': '4', 'memory': '4Gi'},
                'conditions': [{'type': 'Ready', 'status': 'True'}]}}
            for name in self.settings.eligible_nodes]}
        self.core.list_pod_for_all_namespaces.return_value = {'items': []}
        autoscaling = Mock()
        autoscaling.list_namespaced_horizontal_pod_autoscaler.return_value = {'items': []}
        api = Mock()
        api.sanitize_for_serialization.side_effect = copy.deepcopy
        self.prom = Prometheus('http://example', 60)
        self.series = {
            'cpu': [({'node': n}, .2) for n in self.settings.eligible_nodes],
            'traffic': [({'node': n}, 1.0) for n in self.settings.eligible_nodes],
            'gpu': [({'node': 'gnb-2'}, .25), ({'node': 'gnb-3'}, .8)],
            'vram': [({'node': 'gnb-2'}, .5), ({'node': 'gnb-3'}, 1.0)],
        }
        self.prom.vector = Mock(side_effect=lambda query: self.series[query])
        self.prom.scalar = Mock(return_value=2)
        self.controller = Reconciler(self.settings, self.apps, self.core, autoscaling,
                                     api, self.prom, 'ns', 'app')

    def test_free_fractions_and_weights_reach_ranking_in_dry_run(self):
        with patch.object(self.prom, 'per_node', wraps=self.prom.per_node) as per_node:
            with patch('controller.reconcile.rank_nodes', return_value={'gnb-2': .5}) as rank:
                self.controller.run_once()
        nodes, request, saturation, locality, weights = rank.call_args.args
        self.assertEqual([(n.name, n.gpu_free, n.vram_free) for n in nodes],
                         [('gnb1', 0.0, 0.0), ('gnb-2', .75, .5),
                          ('gnb-3', 1 - .8, 0.0)])
        self.assertEqual(request, {'cpu': 1, 'memory': 2**30})
        self.assertEqual((saturation, locality, weights), (.8, .4, self.settings.resource_weights))
        self.assertEqual(self.prom.vector.call_args_list,
                         [call('cpu'), call('traffic'), call('gpu'), call('vram')])
        self.assertEqual(per_node.call_args_list, [
            call('cpu', self.settings.eligible_nodes),
            call('traffic', self.settings.eligible_nodes),
            call('gpu', self.settings.gpu_nodes),
            call('vram', self.settings.gpu_nodes)])
        self.apps.patch_namespaced_deployment.assert_not_called()

    def test_gpu_less_nodes_need_no_exporter(self):
        self.settings.gpu_nodes = ()
        nodes = self.controller._nodes({'cpu': 1, 'memory': 1})
        self.assertTrue(all(n.gpu_free == n.vram_free == 0 for n in nodes))
        self.assertEqual(self.prom.vector.call_args_list, [call('cpu'), call('traffic')])

    def test_missing_duplicate_or_unexpected_exporter_fails_closed(self):
        for query in ('gpu', 'vram'):
            for series in ([], [({'node': 'gnb-2'}, .2)],
                           [({'node': 'gnb-2'}, .2), ({'node': 'gnb-2'}, .3),
                            ({'node': 'gnb-3'}, .4)],
                           [({'node': 'gnb-2'}, .2), ({'node': 'gnb1'}, .3)],
                           [({}, .2), ({'node': 'gnb-3'}, .3)]):
                with self.subTest(query=query, series=series):
                    original = self.series[query]
                    self.series[query] = series
                    with self.assertRaises(ValueError):
                        self.controller.run_once()
                    self.apps.patch_namespaced_deployment.assert_not_called()
                    self.series[query] = original

    def test_invalid_utilization_fails_closed(self):
        for query in ('gpu', 'vram'):
            for value in (-.1, 1.1, float('nan'), float('inf'), 'bad', None, True):
                with self.subTest(query=query, value=value):
                    original = self.series[query]
                    self.series[query] = [({'node': 'gnb-2'}, value),
                                          ({'node': 'gnb-3'}, .5)]
                    with self.assertRaises((ValueError, TypeError)):
                        self.controller.run_once()
                    self.apps.patch_namespaced_deployment.assert_not_called()
                    self.series[query] = original

    def test_missing_gpu_query_fails_closed(self):
        self.settings.vram_utilization_query = None
        with self.assertRaises(ValueError):
            self.controller.run_once()
        self.apps.patch_namespaced_deployment.assert_not_called()
