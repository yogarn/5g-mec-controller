"""Create missing application objects without changing existing resources.

The campaign must create the configured namespace before controller --deploy.
Namespace lifecycle and all resources other than the application Deployment and
Service remain outside the controller's bootstrap responsibility.
"""
import logging
from pathlib import Path

from kubernetes.client.exceptions import ApiException
import yaml

LOG = logging.getLogger(__name__)


def deploy_workload(path: Path, namespace: str, apps, core, *, mutate: bool):
    """Validate the whole manifest, then create missing Deployment and Service.

    Existing objects (including objects created concurrently) are never replaced
    or patched. A failed startup may leave a partially bootstrapped application;
    rerunning --deploy safely creates whichever object is still missing.
    """
    if not mutate:
        raise ValueError('--deploy requires mutate: true')
    if not isinstance(namespace, str) or not namespace.strip():
        raise ValueError('bootstrap requires a nonempty namespace')
    with Path(path).open() as stream:
        documents = [document for document in yaml.safe_load_all(stream) if document is not None]
    expected = {('apps/v1', 'Deployment'), ('v1', 'Service')}
    objects = {}
    for document in documents:
        if not isinstance(document, dict):
            raise ValueError('bootstrap manifest documents must be mappings')
        identity = (document.get('apiVersion'), document.get('kind'))
        if identity not in expected or identity in objects:
            raise ValueError('bootstrap manifest must contain exactly one apps/v1 Deployment and one v1 Service')
        metadata = document.get('metadata')
        if not isinstance(metadata, dict) or not isinstance(metadata.get('name'), str) or not metadata['name'].strip():
            raise ValueError(f'{identity[1]} requires metadata.name')
        declared = metadata.get('namespace')
        if declared is not None and declared != namespace:
            raise ValueError(f'{identity[1]} namespace conflicts with configured namespace {namespace}')
        metadata['namespace'] = namespace
        objects[identity] = document
    if set(objects) != expected:
        raise ValueError('bootstrap manifest must contain exactly one apps/v1 Deployment and one v1 Service')

    try:
        core.read_namespace(namespace)
    except ApiException as exc:
        if exc.status == 404:
            raise ValueError(f'namespace {namespace} does not exist; create it before --deploy') from None
        raise

    resources = (
        (objects['apps/v1', 'Deployment'], apps.read_namespaced_deployment, apps.create_namespaced_deployment),
        (objects['v1', 'Service'], core.read_namespaced_service, core.create_namespaced_service),
    )
    for body, read, create in resources:
        name = body['metadata']['name']
        try:
            read(name=name, namespace=namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise
        else:
            LOG.info('Keeping existing %s %s/%s', body['kind'], namespace, name)
            continue
        try:
            create(namespace=namespace, body=body)
        except ApiException as exc:
            if exc.status != 409:
                raise
            LOG.info('Keeping concurrently created %s %s/%s', body['kind'], namespace, name)
        else:
            LOG.info('Created %s %s/%s', body['kind'], namespace, name)
