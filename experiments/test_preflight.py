import json
from contextlib import nullcontext, redirect_stdout
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from experiments import preflight

WORKLOAD_QUERY = (
    '(histogram_quantile(0.95, sum by (le) (rate('
    'envoy_http_downstream_rq_time_bucket{job="upf-envoy",'
    'envoy_http_conn_manager_prefix="mec_placement_face",node=~"gnb-2|gnb-3"}[2m]))) / 1000) '
    'and on() (count(count by (node) (envoy_http_downstream_rq_time_count{job="upf-envoy",'
    'envoy_http_conn_manager_prefix="mec_placement_face",node=~"gnb-2|gnb-3"})) == 2)')


class PreflightSafetyTests(unittest.TestCase):
    def setUp(self):
        self.args = SimpleNamespace(proxy_namespace='open5gs', envoy_configmap='upf',
                                    envoy_container='envoy', envoy_config_key='envoy.yaml',
                                    envoy_binary='envoy', recovery_dir=None)
        self.pod = {'metadata': {'name': 'upf-0'}}
        self.cm = {'metadata': {'name': 'upf', 'resourceVersion': '7'},
                   'data': {'envoy.yaml': 'old', 'other': 'preserve'}}

    def test_site_defaults_and_overrides(self):
        default = preflight.parse_args([])
        self.assertEqual(default.upf_statefulset, 'upf-gnb-2')
        self.assertEqual(default.envoy_configmap, 'upf-gnb-2-envoy-config')
        other = preflight.parse_args(['--node', 'gnb-3'])
        self.assertEqual(other.upf_statefulset, 'upf-gnb-3')
        self.assertEqual(other.envoy_configmap, 'upf-gnb-3-envoy-config')

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
                deployment = {'metadata': {'generation': 1},
                              'spec': {'replicas': 1,
                                       'template': {'metadata': {'labels': {'app': 'face'}},
                                                    'spec': {'containers': [{'name': 'face'}]}}},
                              'status': {'readyReplicas': 0}}
                service = {'spec': {'selector': {'app': 'face' if healthy else 'other'}, 'ports': [{'port': 5000}]}}
                objects = {'statefulset': sts, 'configmap': cm, 'pod': old, 'deployment': deployment,
                           'service': service}
                settings = SimpleNamespace(manifest=Path('app.yaml'), namespace='open5gs',
                                           prometheus_url='http://prom', max_metric_age_seconds=60,
                                           latency_query=WORKLOAD_QUERY)
                response = SimpleNamespace(status_code=200, text='ok', raise_for_status=lambda: None,
                                           json=lambda: {})
                cli = ['--upf-statefulset', 'upf', '--envoy-container', 'envoy',
                       '--envoy-configmap', 'upf', '--envoy-config-key', 'envoy.yaml',
                       '--envoy-config-path', '/etc/envoy/envoy.yaml', '--envoy-config', str(config),
                       '--apply-envoy-config', '--recovery-dir', str(recovery), '--restart-upf', '--json']
                out = io.StringIO()
                with (patch.object(preflight, 'load_settings', return_value=settings),
                      patch.object(preflight, 'workload_identity', return_value=('open5gs', 'app')),
                      patch.object(preflight, 'get', side_effect=lambda kind, name, namespace: objects[kind]) as get_resource,
                      patch.object(preflight, 'find_pod', side_effect=[old, new]),
                      patch.object(preflight, 'validate_proxy_mount'),
                      patch.object(preflight, 'kubectl', return_value='ok') as run,
                      patch.object(preflight, 'port_forward', return_value=nullcontext('http://localhost:1234')),
                      patch.object(preflight.requests, 'get', return_value=response) as http,
                      patch.object(preflight.Prometheus, 'scalar',
                                   side_effect=ValueError('Prometheus query returned no samples') if healthy else None,
                                   return_value=0.1),
                      patch.object(preflight, 'runtime_matches', return_value='verified') as active,
                      redirect_stdout(out)):
                    result = preflight.main(cli)
                report = json.loads(out.getvalue())
                self.assertNotIn('service_endpoints', report['checks'])
                self.assertNotIn('service_http_health', report['checks'])
                self.assertNotIn('endpoints', [call.args[0] for call in get_resource.call_args_list])
                self.assertFalse(any('/health' in call.args[0] and '/-/healthy' not in call.args[0]
                                     for call in http.call_args_list))
                deleted = [call for call in run.call_args_list if 'delete' in call.args]
                replaced = [call for call in run.call_args_list if 'replace' in call.args]
                self.assertFalse(replaced)
                self.assertEqual(bool(deleted), healthy)
                self.assertEqual(recovery.exists(), healthy)
                if healthy:
                    self.assertEqual(result, 0)
                    self.assertTrue(report['checks']['upf_restart']['ok'])
                    self.assertTrue(report['checks']['prometheus_query']['unverified'])
                    self.assertTrue(report['checks']['envoy_active']['ok'])
                    self.assertTrue(active.called)
                    self.assertEqual(json.loads((recovery / 'configmap.json').read_text()), cm)
                else:
                    self.assertEqual(result, 1)
                    self.assertIn('service_selection', report['checks']['envoy_apply']['error'])


    def test_human_report_shows_checks_warnings_and_actions_without_resource_dump(self):
        report = {
            'ok': False,
            'checks': {
                'deployment': {'ok': True, 'detail': {'desired': 2, 'ready': 2}},
                'service': {'ok': True, 'detail': {
                    'metadata': {'name': 'app', 'namespace': 'open5gs', 'annotations': {'secret': 'hidden'}},
                    'spec': {'clusterIP': '10.0.0.1', 'ports': [{'port': 80}]}}},
                'envoy_active': {'ok': False, 'error': 'active configuration differs'},
                'prometheus_query': {'ok': False, 'error': 'invalid or stale Prometheus sample'},
            },
            'warnings': ['UPF was not restarted'],
            'actions': [{'action': 'configmap_replaced', 'backup': '/tmp/backup'}],
        }
        output = preflight.render_report(report)
        self.assertIn('[OK] deployment: desired=2, ready=2', output)
        self.assertIn('[OK] service: name=app, namespace=open5gs, cluster_ip=10.0.0.1', output)
        self.assertIn('[FAIL] envoy_active: active configuration differs', output)
        self.assertIn('[WARN] UPF was not restarted', output)
        self.assertIn('[FAIL] prometheus_query: invalid or stale Prometheus sample', output)
        self.assertIn('Inspect the query, metric values and sample timestamps', output)
        self.assertIn('[ACTION] configmap_replaced: backup=/tmp/backup', output)
        self.assertTrue(output.endswith('Preflight: FAIL'))
        self.assertNotIn('annotations', output)
        self.assertNotIn('hidden', output)
        self.assertNotIn('ports', output)

    def test_default_output_reports_actual_failures(self):
        out = io.StringIO()
        with (patch.object(preflight, 'load_settings', side_effect=preflight.PreflightError('bad config')),
              patch.object(preflight, 'kubectl', side_effect=preflight.PreflightError('cluster unavailable')),
              patch.object(preflight.Path, 'read_text', side_effect=OSError('missing envoy config')),
              redirect_stdout(out)):
            result = preflight.main([])
        self.assertEqual(result, 1)
        self.assertIn('[FAIL] configuration: bad config', out.getvalue())
        self.assertIn('[FAIL] kubernetes: cluster unavailable', out.getvalue())
        self.assertIn('[FAIL] envoy_source: missing envoy config', out.getvalue())
        self.assertTrue(out.getvalue().strip().endswith('Preflight: FAIL'))

    def test_json_option_preserves_structured_failure_report(self):
        out = io.StringIO()
        with (patch.object(preflight, 'load_settings', side_effect=preflight.PreflightError('bad config')),
              patch.object(preflight, 'kubectl', side_effect=preflight.PreflightError('cluster unavailable')),
              patch.object(preflight.Path, 'read_text', side_effect=OSError('missing envoy config')),
              redirect_stdout(out)):
            result = preflight.main(['--json'])
        self.assertEqual(result, 1)
        self.assertEqual(json.loads(out.getvalue()), {
            'ok': False,
            'checks': {
                'configuration': {'ok': False, 'error': 'bad config'},
                'kubernetes': {'ok': False, 'error': 'cluster unavailable'},
                'envoy_source': {'ok': False, 'error': 'missing envoy config'},
            },
            'actions': [],
            'warnings': [],
        })

    def test_running_config_requires_active_matching_resources(self):
        desired = {'static_resources': {'listeners': [{'name': 'listener', 'port': 8000}],
                                       'clusters': [{'name': 'cluster', 'address': 'service'}]}}
        dump = {'configs': [{'static_listeners': [{'listener': {'name': 'listener', 'port': 8000}}],
                             'static_clusters': [{'cluster': {'name': 'cluster', 'address': 'service'}}]}]}
        preflight.runtime_matches(dump, desired)
        dump['configs'][0]['static_clusters'][0]['cluster']['address'] = 'other'
        with self.assertRaises(preflight.PreflightError):
            preflight.runtime_matches(dump, desired)

    def test_envoy_default_round_robin_is_equivalent_but_other_values_are_not(self):
        desired = {'static_resources': {'listeners': [{'name': 'listener'}],
                                       'clusters': [{'name': 'face', 'lb_policy': 'ROUND_ROBIN'}]}}
        dump = {'configs': [{'static_listeners': [{'listener': {'name': 'listener'}}],
                             'static_clusters': [{'cluster': {'name': 'face'}}]}]}
        preflight.runtime_matches(dump, desired)
        dump['configs'][0]['static_clusters'][0]['cluster']['lb_policy'] = 'RANDOM'
        with self.assertRaises(preflight.PreflightError):
            preflight.runtime_matches(dump, desired)

    def test_idle_counter_does_not_manufacture_a_latency_value(self):
        settings = SimpleNamespace(latency_query=WORKLOAD_QUERY,
                                   prometheus_url='http://prom', max_metric_age_seconds=60)
        with patch.object(preflight.Prometheus, 'scalar', side_effect=[1, 0, 1, 0]) as scalar:
            self.assertTrue(preflight.proxy_is_idle(settings))
            self.assertIn('node="gnb-2"', scalar.call_args_list[1].args[0])
            self.assertIn('node="gnb-3"', scalar.call_args_list[3].args[0])
        with patch.object(preflight.Prometheus, 'scalar', side_effect=[1, 0, 1, 1]):
            self.assertFalse(preflight.proxy_is_idle(settings))
        with patch.object(preflight.Prometheus, 'scalar', side_effect=[1, 0, 1, ValueError('Prometheus query returned no samples')]):
            with self.assertRaises(ValueError):
                preflight.proxy_is_idle(settings)
        self.assertEqual(preflight.mutation_blockers({
            'prometheus_query': {'ok': False, 'unverified': True},
            'deployment': {'ok': False}}), ['deployment'])

    def test_idle_is_warning_but_stale_samples_fail(self):
        query = WORKLOAD_QUERY
        for error, idle, expected in [('Prometheus sample is NaN', True, 0),
                                      ('Prometheus query returned no samples', True, 0),
                                      ('invalid or stale Prometheus sample', False, 1)]:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as temp:
                config = Path(temp) / 'envoy.yaml'
                text = 'static_resources: {}\n'
                config.write_text(text)
                sts = {'metadata': {'name': 'upf', 'uid': 'owner'}, 'spec': {}}
                pod = {'metadata': {'name': 'upf-0', 'uid': 'old'}}
                cm = {'metadata': {'resourceVersion': '7'}, 'data': {'envoy.yaml': text}}
                deployment = {'spec': {'replicas': 1,
                                       'template': {'metadata': {'labels': {'app': 'face'}},
                                                    'spec': {'containers': [{'name': 'face'}]}}},
                              'status': {'readyReplicas': 0}}
                service = {'spec': {'selector': {'app': 'face'}, 'ports': [{'port': 5000}]}}
                objects = {'statefulset': sts, 'configmap': cm, 'deployment': deployment,
                           'service': service}
                settings = SimpleNamespace(manifest=Path('app.yaml'), namespace='open5gs',
                                           prometheus_url='http://prom', max_metric_age_seconds=60,
                                           latency_query=query)
                response = SimpleNamespace(status_code=200, text='ok', raise_for_status=lambda: None,
                                           json=lambda: {})
                output = io.StringIO()
                with (patch.object(preflight, 'load_settings', return_value=settings),
                      patch.object(preflight, 'workload_identity', return_value=('open5gs', 'app')),
                      patch.object(preflight, 'get', side_effect=lambda kind, name, namespace: objects[kind]),
                      patch.object(preflight, 'find_pod', return_value=pod),
                      patch.object(preflight, 'validate_proxy_mount'),
                      patch.object(preflight, 'kubectl', return_value='ok'),
                      patch.object(preflight, 'port_forward', return_value=nullcontext('http://localhost:1234')),
                      patch.object(preflight.requests, 'get', return_value=response),
                      patch.object(preflight.Prometheus, 'scalar', side_effect=[ValueError(error), 1, 0, 1, 0]),
                      patch.object(preflight, 'runtime_matches', return_value='verified'),
                      redirect_stdout(output)):
                    result = preflight.main(['--upf-statefulset', 'upf', '--envoy-configmap', 'upf',
                                             '--envoy-config', str(config), '--json'])
                report = json.loads(output.getvalue())
                self.assertEqual(result, expected)
                self.assertEqual(report['checks']['prometheus_query'].get('unverified', False), idle)
                self.assertFalse(report['checks']['prometheus_query']['ok'])
                if idle:
                    self.assertIn('[WARN] prometheus_query:', preflight.render_report(report))

    def test_interactive_rollout_requires_confirmation(self):
        text = 'static_resources: {}\n'
        for answer in ('n', 'y'):
            with self.subTest(answer=answer), tempfile.TemporaryDirectory() as temp:
                config = Path(temp) / 'envoy.yaml'
                config.write_text(text)
                cm = {'metadata': {'resourceVersion': '7'}, 'data': {'envoy.yaml': text}}
                old = {'metadata': {'name': 'upf-0', 'uid': 'old'}}
                new = {'metadata': {'name': 'upf-0', 'uid': 'new'}}
                sts = {'metadata': {'name': 'upf', 'uid': 'owner'}, 'spec': {}}
                deployment = {'spec': {'replicas': 1,
                                       'template': {'metadata': {'labels': {'app': 'face'}},
                                                    'spec': {'containers': [{'name': 'face'}]}}},
                              'status': {'readyReplicas': 0}}
                service = {'spec': {'selector': {'app': 'face'}, 'ports': [{'port': 5000}]}}
                objects = {'statefulset': sts, 'configmap': cm, 'pod': old, 'deployment': deployment,
                           'service': service}
                settings = SimpleNamespace(manifest=Path('app.yaml'), namespace='open5gs',
                                           prometheus_url='http://prom', max_metric_age_seconds=60,
                                           latency_query='query')
                response = SimpleNamespace(status_code=200, text='ok', raise_for_status=lambda: None,
                                           json=lambda: {})
                mismatch = preflight.PreflightError('active Envoy clusters does not match configured resource')
                matches = [mismatch, 'verified'] if answer == 'y' else [mismatch, mismatch]
                with (patch.object(preflight, 'load_settings', return_value=settings),
                      patch.object(preflight, 'workload_identity', return_value=('open5gs', 'app')),
                      patch.object(preflight, 'get', side_effect=lambda kind, name, namespace: objects[kind]),
                      patch.object(preflight, 'find_pod', side_effect=[old, new]),
                      patch.object(preflight, 'validate_proxy_mount'),
                      patch.object(preflight, 'kubectl', return_value='ok') as kube,
                      patch.object(preflight, 'port_forward', return_value=nullcontext('http://localhost:1234')),
                      patch.object(preflight.requests, 'get', return_value=response),
                      patch.object(preflight.Prometheus, 'scalar', return_value=0.1),
                      patch.object(preflight, 'runtime_matches', side_effect=matches),
                      patch.object(preflight, 'apply_config', return_value='/tmp/backup') as apply,
                      patch.object(preflight.Path, 'mkdir'),
                      patch('sys.stdin.isatty', return_value=True),
                      patch('builtins.input', return_value=answer),
                      redirect_stdout(io.StringIO())):
                    result = preflight.main(['--upf-statefulset', 'upf', '--envoy-configmap', 'upf',
                                             '--envoy-config-path', '/etc/envoy/envoy.yaml',
                                             '--envoy-config', str(config)])
                self.assertEqual(bool(apply.called), answer == 'y')
                self.assertEqual(any('delete' in call.args for call in kube.call_args_list), answer == 'y')
                self.assertEqual(result, 0 if answer == 'y' else 1)

    def test_cold_deployment_and_service_static_validation(self):
        deployment = {'spec': {'replicas': 1, 'template': {
            'metadata': {'labels': {'app': 'face'}},
            'spec': {'containers': [{'name': 'face', 'ports': [{'name': 'http', 'containerPort': 5000}]}]}}},
            'status': {'readyReplicas': 0}}
        self.assertIn('not checked', preflight.check_deployment(deployment)['readiness'])
        service = {'spec': {'selector': {'app': 'face'}, 'ports': [{'port': 80, 'targetPort': 'http'}]}}
        preflight.check_service(service, deployment)
        service['spec']['selector'] = {'app': 'other'}
        with self.assertRaises(preflight.PreflightError):
            preflight.check_service(service, deployment)
        service['spec']['selector'] = {'app': 'face'}
        service['spec']['ports'][0]['targetPort'] = 'missing'
        with self.assertRaises(preflight.PreflightError):
            preflight.check_service(service, deployment)
        deployment['spec']['replicas'] = 0
        with self.assertRaises(preflight.PreflightError):
            preflight.check_deployment(deployment)

    def test_missing_workload_resources_are_failures(self):
        deployment = {'spec': {'replicas': 1, 'template': {
            'metadata': {'labels': {'app': 'face'}},
            'spec': {'containers': [{'name': 'face'}]}}}}
        service = {'spec': {'selector': {'app': 'face'}, 'ports': [{'port': 5000}]}}
        settings = SimpleNamespace(manifest=Path('app.yaml'), namespace='services',
                                   prometheus_url='http://prom', max_metric_age_seconds=60,
                                   latency_query='query')
        for missing in ('deployment', 'service'):
            objects = {'deployment': deployment, 'service': service}
            def get_resource(kind, name, namespace):
                if kind == missing:
                    raise preflight.PreflightError(f'{kind} not found')
                return objects[kind]
            response = SimpleNamespace(status_code=200, text='ok', raise_for_status=lambda: None)
            output = io.StringIO()
            with (patch.object(preflight, 'load_settings', return_value=settings),
                  patch.object(preflight, 'workload_identity', return_value=('services', 'app')),
                  patch.object(preflight, 'get', side_effect=get_resource),
                  patch.object(preflight, 'kubectl', return_value='ok'),
                  patch.object(preflight.requests, 'get', return_value=response),
                  patch.object(preflight.Prometheus, 'scalar', return_value=0.1),
                  redirect_stdout(output)):
                result = preflight.main(['--envoy-config', '/nonexistent', '--json'])
            report = json.loads(output.getvalue())
            self.assertEqual(result, 1)
            self.assertFalse(report['checks'][missing]['ok'])


if __name__ == '__main__':
    unittest.main()
