import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from controller import __main__ as cli


class ControllerCLITests(unittest.TestCase):
    def test_once_failure_reports_error_without_traceback(self):
        reconciler = Mock()
        reconciler.policy = SimpleNamespace(down_since=1)
        reconciler.run_once.side_effect = ValueError('latency unavailable')
        with patch('sys.argv', ['controller', '--once']), \
             patch.object(cli, 'load_settings', return_value=SimpleNamespace(manifest='m', namespace=None, prometheus_url='http://localhost', max_metric_age_seconds=10)), \
             patch.object(cli, 'workload_identity', return_value=('ns', 'app')), \
             patch.object(cli.config, 'load_incluster_config'), \
             patch.object(cli, 'Reconciler', return_value=reconciler), \
             patch.object(cli, 'deploy_workload') as deploy, \
             self.assertLogs(level='ERROR') as logs, self.assertRaises(SystemExit) as result:
            cli.main()
        self.assertEqual(result.exception.code, 1)
        self.assertIn('latency unavailable', logs.output[0])
        self.assertNotIn('Traceback', logs.output[0])
        self.assertIsNone(reconciler.policy.down_since)
        deploy.assert_not_called()

    def test_deploy_requires_mutate_before_cluster_access(self):
        with patch('sys.argv', ['controller', '--deploy']), \
             patch.object(cli, 'load_settings', return_value=SimpleNamespace(mutate=False)), \
             patch.object(cli.config, 'load_incluster_config') as cluster, \
             patch.object(cli, 'deploy_workload') as deploy, \
             patch('sys.stderr'), self.assertRaises(SystemExit) as result:
            cli.main()
        self.assertEqual(result.exception.code, 2)
        cluster.assert_not_called()
        deploy.assert_not_called()

    def test_deploy_bootstraps_before_first_reconciliation(self):
        settings = SimpleNamespace(manifest='m', namespace='mec-experiment', mutate=True,
                                   prometheus_url='http://localhost', max_metric_age_seconds=10)
        reconciler = Mock()
        apps, core = Mock(), Mock()
        calls = Mock()
        calls.attach_mock(reconciler.run_once, 'run_once')
        with patch('sys.argv', ['controller', '--once', '--deploy']), \
             patch.object(cli, 'load_settings', return_value=settings), \
             patch.object(cli, 'workload_identity', return_value=('mec-experiment', 'app')), \
             patch.object(cli.config, 'load_incluster_config'), \
             patch.object(cli.client, 'AppsV1Api', return_value=apps), \
             patch.object(cli.client, 'CoreV1Api', return_value=core), \
             patch.object(cli, 'deploy_workload') as deploy, \
             patch.object(cli, 'Reconciler', return_value=reconciler):
            calls.attach_mock(deploy, 'deploy')
            cli.main()
        deploy.assert_called_once_with('m', 'mec-experiment', apps, core, mutate=True)
        self.assertEqual([call[0] for call in calls.mock_calls], ['deploy', 'run_once'])

    def test_bootstrap_failure_exits_without_reconciling(self):
        settings = SimpleNamespace(manifest='m', namespace='mec-experiment', mutate=True)
        with patch('sys.argv', ['controller', '--once', '--deploy']), \
             patch.object(cli, 'load_settings', return_value=settings), \
             patch.object(cli, 'workload_identity', return_value=('mec-experiment', 'app')), \
             patch.object(cli.config, 'load_incluster_config'), \
             patch.object(cli, 'deploy_workload', side_effect=ValueError('invalid Service namespace')), \
             patch.object(cli, 'Reconciler') as reconciler, \
             self.assertLogs(level='ERROR') as logs, self.assertRaises(SystemExit) as result:
            cli.main()
        self.assertEqual(result.exception.code, 1)
        self.assertIn('invalid Service namespace', logs.output[0])
        self.assertNotIn('Traceback', logs.output[0])
        reconciler.assert_not_called()
