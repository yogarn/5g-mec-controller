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
             self.assertLogs(level='ERROR') as logs, self.assertRaises(SystemExit) as result:
            cli.main()
        self.assertEqual(result.exception.code, 1)
        self.assertIn('latency unavailable', logs.output[0])
        self.assertNotIn('Traceback', logs.output[0])
        self.assertIsNone(reconciler.policy.down_since)
