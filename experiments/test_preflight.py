import copy
import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch

from experiments import preflight


QUERY = 'ue_request_duration_p90_seconds{job="ue-telemetry"}'
DEPLOYMENT = {'spec': {'replicas': 1, 'template': {
    'metadata': {'labels': {'app': 'face'}},
    'spec': {'containers': [{'name': 'face', 'ports': [{'name': 'http', 'containerPort': 5000}]}]}}}}
SERVICE = {'spec': {'type': 'NodePort', 'selector': {'app': 'face'},
                    'ports': [{'port': 80, 'targetPort': 'http', 'nodePort': 31234}]}}


class PreflightTests(unittest.TestCase):
    def test_static_workload_checks_and_nodeport(self):
        self.assertIn('not checked', preflight.check_deployment(DEPLOYMENT)['readiness'])
        detail = preflight.check_service(SERVICE, DEPLOYMENT)
        self.assertEqual(detail['node_ports'], [31234])
        self.assertEqual(detail['reachability'], 'not checked')
        for mutation in ('type', 'selector', 'nodePort', 'targetPort', 'protocol'):
            service = copy.deepcopy(SERVICE)
            if mutation == 'type':
                service['spec']['type'] = 'ClusterIP'
            elif mutation == 'selector':
                service['spec']['selector'] = {'app': 'wrong'}
            elif mutation == 'nodePort':
                del service['spec']['ports'][0]['nodePort']
            elif mutation == 'targetPort':
                service['spec']['ports'][0]['targetPort'] = 'missing'
            else:
                service['spec']['ports'][0]['protocol'] = 'UDP'
                service['spec']['ports'][0]['targetPort'] = 5000
            with self.subTest(mutation=mutation), self.assertRaises(preflight.PreflightError):
                preflight.check_service(service, DEPLOYMENT)
        deployment = copy.deepcopy(DEPLOYMENT)
        deployment['spec']['replicas'] = 0
        with self.assertRaises(preflight.PreflightError):
            preflight.check_deployment(deployment)

    def run_preflight(self, values, latency=0.2, missing=None, json_output=True, query=QUERY):
        settings = SimpleNamespace(manifest='app.yaml', namespace='services', prometheus_url='http://prom',
                                   max_metric_age_seconds=60, latency_query=query)
        objects = {'deployment': DEPLOYMENT, 'service': SERVICE}

        def get(kind, name, namespace):
            if kind == missing:
                raise preflight.PreflightError(f'{kind} not found')
            return objects[kind]

        def scalar(query):
            if query == settings.latency_query:
                value = latency
            else:
                value = values[query.split('{')[0]]
            if isinstance(value, Exception):
                raise value
            return value

        response = SimpleNamespace(status_code=200, raise_for_status=lambda: None)
        output = io.StringIO()
        with (patch.object(preflight, 'load_settings', return_value=settings),
              patch.object(preflight, 'workload_identity', return_value=('services', 'face')),
              patch.object(preflight, 'get', side_effect=get),
              patch.object(preflight, 'kubectl', return_value='ok') as kube,
              patch.object(preflight.requests, 'get', return_value=response),
              patch.object(preflight.Prometheus, 'scalar', side_effect=scalar),
              redirect_stdout(output)):
            code = preflight.main(['--json'] if json_output else [])
        self.assertEqual([call.args for call in kube.call_args_list], [('get', '--raw=/readyz')])
        return code, json.loads(output.getvalue()) if json_output else output.getvalue()

    def test_success_and_idle_report(self):
        values = {'up': 1, 'ue_request_window_complete': 1, 'ue_request_window_attempts': 3}
        code, report = self.run_preflight(values)
        self.assertEqual(code, 0)
        self.assertEqual(report['checks']['prometheus_query']['detail']['latency_seconds'], 0.2)
        self.assertEqual(report['checks']['service_selection']['detail']['node_ports'], [31234])
        values['ue_request_window_attempts'] = 0
        code, report = self.run_preflight(values, ValueError('Prometheus query returned no samples'))
        self.assertEqual(code, 0)
        self.assertTrue(report['checks']['prometheus_query']['unverified'])
        self.assertNotIn('latency_seconds', report['checks']['prometheus_query'])
        code, text = self.run_preflight(values, ValueError('Prometheus query returned no samples'), json_output=False)
        self.assertIn('[WARN] prometheus_query:', text)
        self.assertIn('fails closed', text)

    def test_missing_latency_only_idle_when_collector_is_healthy(self):
        no_samples = ValueError('Prometheus query returned no samples')
        cases = [
            ({'up': 0, 'ue_request_window_complete': 1, 'ue_request_window_attempts': 0}, no_samples),
            ({'up': 1, 'ue_request_window_complete': 0, 'ue_request_window_attempts': 0}, no_samples),
            ({'up': 1, 'ue_request_window_complete': 1, 'ue_request_window_attempts': 2}, no_samples),
            ({'up': 1, 'ue_request_window_complete': 1, 'ue_request_window_attempts': -1}, no_samples),
            ({'up': 1, 'ue_request_window_complete': 1, 'ue_request_window_attempts': 0},
             ValueError('Prometheus sample is NaN')),
            ({'up': 1, 'ue_request_window_complete': 1, 'ue_request_window_attempts': 0},
             ValueError('invalid or stale Prometheus sample')),
            ({'up': 1, 'ue_request_window_complete': ValueError('bad response'),
              'ue_request_window_attempts': 0}, no_samples),
        ]
        for values, latency in cases:
            with self.subTest(values=values, latency=latency):
                code, report = self.run_preflight(values, latency)
                self.assertEqual(code, 1)
                self.assertFalse(report['checks']['prometheus_query'].get('unverified', False))
        values = {'up': 1, 'ue_request_window_complete': 0, 'ue_request_window_attempts': 0}
        code, report = self.run_preflight(values, 0.2)
        self.assertEqual(code, 1)
        self.assertIn('overflow', report['checks']['collector']['error'])

    def test_missing_resources_fail(self):
        values = {'up': 1, 'ue_request_window_complete': 1, 'ue_request_window_attempts': 1}
        for missing in ('deployment', 'service'):
            with self.subTest(missing=missing):
                code, report = self.run_preflight(values, missing=missing)
                self.assertEqual(code, 1)
                self.assertFalse(report['checks'][missing]['ok'])

    def test_removed_mutation_options_are_rejected(self):
        with redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
            preflight.parse_args(['--apply-envoy-config'])


if __name__ == '__main__':
    unittest.main()
