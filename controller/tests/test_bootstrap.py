import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from kubernetes.client.exceptions import ApiException
import yaml

from controller.bootstrap import deploy_workload


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'manifest.yaml'
        self.namespace = 'mec-experiment'
        self.documents = [
            {'apiVersion': 'apps/v1', 'kind': 'Deployment',
             'metadata': {'name': 'app', 'namespace': self.namespace},
             'spec': {'replicas': 1}},
            {'apiVersion': 'v1', 'kind': 'Service',
             'metadata': {'name': 'app', 'namespace': self.namespace},
             'spec': {'type': 'NodePort', 'ports': [{'port': 5000}]}},
        ]
        self.apps = Mock()
        self.core = Mock()
        self.apps.read_namespaced_deployment.side_effect = ApiException(status=404)
        self.core.read_namespaced_service.side_effect = ApiException(status=404)

    def deploy(self, mutate=True):
        self.path.write_text(yaml.safe_dump_all(self.documents))
        deploy_workload(self.path, self.namespace, self.apps, self.core, mutate=mutate)

    def test_creates_both_missing_objects_from_manifest(self):
        self.deploy()
        self.core.read_namespace.assert_called_once_with(self.namespace)
        self.apps.create_namespaced_deployment.assert_called_once_with(
            namespace=self.namespace, body=self.documents[0])
        self.core.create_namespaced_service.assert_called_once_with(
            namespace=self.namespace, body=self.documents[1])
        self.assertNotIn('nodePort', self.core.create_namespaced_service.call_args.kwargs['body']['spec']['ports'][0])

    def test_existing_objects_are_never_modified(self):
        self.apps.read_namespaced_deployment.side_effect = None
        self.core.read_namespaced_service.side_effect = None
        self.deploy()
        self.apps.create_namespaced_deployment.assert_not_called()
        self.core.create_namespaced_service.assert_not_called()
        self.apps.patch_namespaced_deployment.assert_not_called()
        self.apps.replace_namespaced_deployment.assert_not_called()
        self.core.patch_namespaced_service.assert_not_called()
        self.core.replace_namespaced_service.assert_not_called()

    def test_partial_bootstrap_creates_only_missing_service(self):
        self.apps.read_namespaced_deployment.side_effect = None
        self.deploy()
        self.apps.create_namespaced_deployment.assert_not_called()
        self.core.create_namespaced_service.assert_called_once()

    def test_partial_bootstrap_creates_only_missing_deployment(self):
        self.core.read_namespaced_service.side_effect = None
        self.deploy()
        self.apps.create_namespaced_deployment.assert_called_once()
        self.core.create_namespaced_service.assert_not_called()

    def test_concurrent_creation_keeps_existing_objects(self):
        self.apps.create_namespaced_deployment.side_effect = ApiException(status=409)
        self.core.create_namespaced_service.side_effect = ApiException(status=409)
        self.deploy()
        self.apps.patch_namespaced_deployment.assert_not_called()
        self.core.replace_namespaced_service.assert_not_called()

    def test_requires_mutation_before_any_api_access(self):
        with self.assertRaisesRegex(ValueError, 'mutate: true'):
            self.deploy(mutate=False)
        self.assertEqual(self.apps.mock_calls, [])
        self.assertEqual(self.core.mock_calls, [])

    def test_conflicting_service_namespace_fails_before_any_api_access(self):
        self.documents[1]['metadata']['namespace'] = 'placement'
        with self.assertRaisesRegex(ValueError, 'Service namespace conflicts'):
            self.deploy()
        self.assertEqual(self.apps.mock_calls, [])
        self.assertEqual(self.core.mock_calls, [])

    def test_conflicting_deployment_namespace_fails_before_any_api_access(self):
        self.documents[0]['metadata']['namespace'] = 'placement'
        with self.assertRaisesRegex(ValueError, 'Deployment namespace conflicts'):
            self.deploy()
        self.assertEqual(self.apps.mock_calls, [])
        self.assertEqual(self.core.mock_calls, [])

    def test_namespace_can_be_supplied_by_configuration(self):
        for document in self.documents:
            del document['metadata']['namespace']
        self.deploy()
        for api in (self.apps.create_namespaced_deployment, self.core.create_namespaced_service):
            self.assertEqual(api.call_args.kwargs['body']['metadata']['namespace'], self.namespace)

    def test_missing_namespace_requires_campaign_precreation(self):
        self.core.read_namespace.side_effect = ApiException(status=404)
        with self.assertRaisesRegex(ValueError, 'create it before --deploy'):
            self.deploy()
        self.apps.create_namespaced_deployment.assert_not_called()
        self.core.create_namespaced_service.assert_not_called()
        self.core.create_namespace.assert_not_called()

    def test_read_permission_failure_is_not_treated_as_missing(self):
        self.apps.read_namespaced_deployment.side_effect = ApiException(status=403)
        with self.assertRaises(ApiException) as result:
            self.deploy()
        self.assertEqual(result.exception.status, 403)
        self.apps.create_namespaced_deployment.assert_not_called()
        self.core.create_namespaced_service.assert_not_called()

    def test_create_failure_is_propagated(self):
        self.apps.create_namespaced_deployment.side_effect = ApiException(status=422)
        with self.assertRaises(ApiException) as result:
            self.deploy()
        self.assertEqual(result.exception.status, 422)
        self.core.create_namespaced_service.assert_not_called()

    def test_invalid_manifest_is_fully_validated_before_api_access(self):
        valid = copy.deepcopy(self.documents)
        cases = [[], valid[:1], valid + [valid[1]], valid + [
            {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': self.namespace}}],
            valid + ['not an object']]
        for documents in cases:
            with self.subTest(documents=documents):
                self.documents = documents
                with self.assertRaises(ValueError):
                    self.deploy()
                self.assertEqual(self.apps.mock_calls, [])
                self.assertEqual(self.core.mock_calls, [])

    def test_service_name_is_required_before_creating_deployment(self):
        del self.documents[1]['metadata']['name']
        with self.assertRaisesRegex(ValueError, 'Service requires metadata.name'):
            self.deploy()
        self.apps.create_namespaced_deployment.assert_not_called()
