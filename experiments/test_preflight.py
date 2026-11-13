import json
from contextlib import nullcontext, redirect_stdout
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from experiments import preflight


class PreflightSafetyTests(unittest.TestCase):
    def setUp(self):
        self.args = SimpleNamespace(proxy_namespace='open5gs', envoy_configmap='upf',
                                    envoy_container='envoy', envoy_config_key='envoy.yaml',
                                    envoy_binary='envoy', recovery_dir=None)
        self.pod = {'metadata': {'name': 'upf-0'}}
        self.cm = {'metadata': {'name': 'upf', 'resourceVersion': '7'},
                   'data': {'envoy.yaml': 'old', 'other': 'preserve'}}

    def test_mutation_requires_explicit_backup_and_restart_opt_in(self):
        required = ['--upf-statefulset', 'upf', '--envoy-container', 'envoy',
                    '--envoy-configmap', 'upf', '--envoy-config-key', 'envoy.yaml',
                    '--envoy-config-path', '/etc/envoy/envoy.yaml']
        args = preflight.parse_args(required)
        self.assertFalse(args.apply_envoy_config)
        self.assertFalse(args.restart_upf)
        with self.assertRaises(SystemExit):
            preflight.parse_args(required + ['--apply-envoy-config'])
        with self.assertRaises(SystemExit):
            preflight.parse_args(required + ['--restart-upf'])

    def test_validation_failure_never_backs_up_or_updates(self):
        with tempfile.TemporaryDirectory() as temp:
            self.args.recovery_dir = str(Path(temp) / 'recovery')
            with patch.object(preflight, 'kubectl', side_effect=preflight.PreflightError('invalid')) as run:
                with self.assertRaises(preflight.PreflightError):
                    preflight.apply_config(self.args, self.pod, self.cm, 'new')
            self.assertEqual(run.call_count, 1)
            self.assertFalse(Path(self.args.recovery_dir).exists())

    def test_backup_is_exclusive_and_before_replace(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / 'recovery'
            self.args.recovery_dir = str(directory)
            def run(*args, **kwargs):
                if 'replace' in args:
                    self.assertEqual(json.loads((directory / 'configmap.json').read_text()), self.cm)
                    payload = json.loads(kwargs['input_text'])
                    self.assertEqual(payload['metadata']['resourceVersion'], '7')
                    self.assertEqual(payload['data'], {'envoy.yaml': 'new', 'other': 'preserve'})
                return ''
            with patch.object(preflight, 'kubectl', side_effect=run), patch.object(preflight, 'get', return_value={'data': {'envoy.yaml': 'new'}}):
                preflight.apply_config(self.args, self.pod, self.cm, 'new')
                with self.assertRaises(FileExistsError):
                    preflight.apply_config(self.args, self.pod, self.cm, 'newer')
            self.assertEqual(json.loads((directory / 'configmap.json').read_text()), self.cm)

    def test_replace_failure_reports_backup(self):
        with tempfile.TemporaryDirectory() as temp:
            self.args.recovery_dir = str(Path(temp) / 'recovery')
            with patch.object(preflight, 'kubectl', side_effect=['', preflight.PreflightError('conflict')]):
                with self.assertRaisesRegex(preflight.PreflightError, 'recovery backup:'):
                    preflight.apply_config(self.args, self.pod, self.cm, 'new')
            self.assertEqual(json.loads((Path(self.args.recovery_dir) / 'configmap.json').read_text()), self.cm)
    def test_restart_matching_config_and_block_failed_preflight(self):
        text = 'static_resources: {}\n'
        for healthy in (True, False):
            with self.subTest(healthy=healthy), tempfile.TemporaryDirectory() as temp:
                config = Path(temp) / 'envoy.yaml'
                config.write_text(text)
                recovery = Path(temp) / 'recovery'
                cm = {'metadata': {'resourceVersion': '7'}, 'data': {'envoy.yaml': text}}
                old = {'metadata': {'name': 'upf-0', 'uid': 'old'}}
                new = {'metadata': {'name': 'upf-0', 'uid': 'new'}}
                sts = {'metadata': {'name': 'upf', 'uid': 'owner'}, 'spec': {}}
                deployment = {'metadata': {'generation': 1}, 'spec': {'replicas': 1},
                              'status': {'observedGeneration': 1, 'updatedReplicas': 1,
                                         'readyReplicas': int(healthy), 'availableReplicas': 1, 'replicas': 1}}
                service = {'spec': {'ports': [{'port': 5000}]}}
                endpoints = {'subsets': [{'addresses': [{'ip': '10.0.0.1'}], 'ports': [{'port': 5000}]}]}
                objects = {'statefulset': sts, 'configmap': cm, 'pod': old, 'deployment': deployment,
                           'service': service, 'endpoints': endpoints}
                settings = SimpleNamespace(manifest=Path('app.yaml'), namespace='open5gs',
                                           prometheus_url='http://prom', max_metric_age_seconds=60,
                                           latency_query='query')
                response = SimpleNamespace(status_code=200, text='ok', raise_for_status=lambda: None,
                                           json=lambda: {})
                cli = ['--upf-statefulset', 'upf', '--envoy-container', 'envoy',
                       '--envoy-configmap', 'upf', '--envoy-config-key', 'envoy.yaml',
                       '--envoy-config-path', '/etc/envoy/envoy.yaml', '--envoy-config', str(config),
                       '--apply-envoy-config', '--recovery-dir', str(recovery), '--restart-upf']
                out = io.StringIO()
                with (patch.object(preflight, 'load_settings', return_value=settings),
                      patch.object(preflight, 'workload_identity', return_value=('open5gs', 'app')),
                      patch.object(preflight, 'get', side_effect=lambda kind, name, namespace: objects[kind]),
                      patch.object(preflight, 'find_pod', side_effect=[old, new]),
                      patch.object(preflight, 'validate_proxy_mount'),
                      patch.object(preflight, 'kubectl', return_value='ok') as run,
                      patch.object(preflight, 'port_forward', return_value=nullcontext('http://localhost:1234')),
                      patch.object(preflight.requests, 'get', return_value=response),
                      patch.object(preflight.Prometheus, 'scalar', return_value=0.1),
                      patch.object(preflight, 'runtime_matches', return_value='verified') as active,
                      redirect_stdout(out)):
                    result = preflight.main(cli)
                report = json.loads(out.getvalue())
                deleted = [call for call in run.call_args_list if 'delete' in call.args]
                replaced = [call for call in run.call_args_list if 'replace' in call.args]
                self.assertFalse(replaced)
                self.assertEqual(bool(deleted), healthy)
                self.assertEqual(recovery.exists(), healthy)
                if healthy:
                    self.assertEqual(result, 0)
                    self.assertTrue(report['checks']['upf_restart']['ok'])
                    self.assertTrue(report['checks']['envoy_active']['ok'])
                    self.assertTrue(active.called)
                    self.assertEqual(json.loads((recovery / 'configmap.json').read_text()), cm)
                else:
                    self.assertEqual(result, 1)
                    self.assertIn('deployment', report['checks']['envoy_apply']['error'])


    def test_running_config_requires_active_matching_resources(self):
        desired = {'static_resources': {'listeners': [{'name': 'listener', 'port': 8000}],
                                       'clusters': [{'name': 'cluster', 'address': 'service'}]}}
        dump = {'configs': [{'static_listeners': [{'listener': {'name': 'listener', 'port': 8000}}],
                             'static_clusters': [{'cluster': {'name': 'cluster', 'address': 'service'}}]}]}
        preflight.runtime_matches(dump, desired)
        dump['configs'][0]['static_clusters'][0]['cluster']['address'] = 'other'
        with self.assertRaises(preflight.PreflightError):
            preflight.runtime_matches(dump, desired)

    def test_unready_endpoints_and_deployment_fail(self):
        with self.assertRaises(preflight.PreflightError):
            preflight.check_endpoints({'spec': {}}, {'subsets': [{'notReadyAddresses': [{'ip': '10.0.0.1'}], 'ports': [{'port': 5000}]}]})
        with self.assertRaises(preflight.PreflightError):
            preflight.check_deployment({'metadata': {'generation': 2}, 'spec': {'replicas': 1},
                                        'status': {'observedGeneration': 1, 'readyReplicas': 1}})


if __name__ == '__main__':
    unittest.main()
