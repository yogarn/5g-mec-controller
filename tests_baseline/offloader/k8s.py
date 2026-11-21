"""Kubernetes Pod endpoint discovery pinned strictly by node and readiness."""
import json
import os
import ssl
import subprocess
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from offloader.policy import DEFAULT_SITE_NODES, SITES


def is_pod_ready(pod: dict) -> bool:
    """Return True iff the Pod is Running, Ready, not terminating, and has an IP."""
    meta = pod.get('metadata') or {}
    if meta.get('deletionTimestamp'):
        return False
    status = pod.get('status') or {}
    if status.get('phase') != 'Running' or not status.get('podIP'):
        return False
    for cond in status.get('conditions') or []:
        if cond.get('type') == 'Ready' and cond.get('status') == 'True':
            return True
    return False


def filter_pinned_endpoints(
    pods: list[dict],
    *,
    site_nodes: dict[str, str] = DEFAULT_SITE_NODES,
    core_node: str = 'core-5g',
    edge_app_label: str = 'mec-placement-pedestrian-detection',
    core_app_label: str = 'mec-placement-pedestrian-detection-core',
    container_port: int = 5000,
) -> tuple[dict[str, list[tuple[str, int]]], list[tuple[str, int]]]:
    """Partition ready Pod endpoints strictly by nodeName and workload role label.

    Ready managed edge pods scheduled on core_node are included in core_endpoints
    so they serve both the core-5g cluster and the Core portion of spread-gnb-N
    clusters. Not-ready or terminating pods are excluded and IPs are deduplicated.
    """
    node_to_site = {node: site for site, node in site_nodes.items()}
    local_by_site: dict[str, list[tuple[str, int]]] = {site: [] for site in SITES}
    core_endpoints: list[tuple[str, int]] = []

    for pod in pods:
        if not is_pod_ready(pod):
            continue
        labels = (pod.get('metadata') or {}).get('labels') or {}
        app = labels.get('app')
        node_name = (pod.get('spec') or {}).get('nodeName')
        pod_ip = (pod.get('status') or {}).get('podIP')
        if not node_name or not pod_ip:
            continue
        if app == edge_app_label and node_name in node_to_site:
            site = node_to_site[node_name]
            local_by_site[site].append((pod_ip, container_port))
        elif node_name == core_node and (
            app == core_app_label or (app == edge_app_label and labels.get('mec-replica') == 'managed')
        ):
            core_endpoints.append((pod_ip, container_port))

    for site in SITES:
        local_by_site[site] = sorted(set(local_by_site[site]))
    core_endpoints = sorted(set(core_endpoints))
    return local_by_site, core_endpoints


class KubernetesEndpointDiscoverer:
    """Fetches Pod lists from in-cluster Kubernetes API or kubectl fallback."""

    def __init__(
        self,
        namespace: str = 'services',
        site_nodes: dict[str, str] = DEFAULT_SITE_NODES,
        core_node: str = 'core-5g',
        edge_app_label: str = 'mec-placement-pedestrian-detection',
        core_app_label: str = 'mec-placement-pedestrian-detection-core',
        container_port: int = 5000,
        timeout: float = 2.0,
    ):
        self.namespace = namespace
        self.site_nodes = dict(site_nodes)
        self.core_node = core_node
        self.edge_app_label = edge_app_label
        self.core_app_label = core_app_label
        self.container_port = int(container_port)
        self.timeout = float(timeout)

    def _fetch_pods_incluster(self) -> list[dict]:
        host = os.environ.get('KUBERNETES_SERVICE_HOST')
        port = os.environ.get('KUBERNETES_SERVICE_PORT')
        token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'
        ca_path = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
        if not host or not port or not os.path.exists(token_path):
            raise FileNotFoundError('not running in-cluster')
        with open(token_path, encoding='utf-8') as f:
            token = f.read().strip()
        selector = f'app in ({self.edge_app_label},{self.core_app_label})'
        query = urlencode({'labelSelector': selector})
        url = f'https://{host}:{port}/api/v1/namespaces/{self.namespace}/pods?{query}'
        req = Request(url, headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'})
        ctx = ssl.create_default_context(cafile=ca_path) if os.path.exists(ca_path) else ssl.create_default_context()
        with urlopen(req, timeout=self.timeout, context=ctx) as resp:
            data = json.load(resp)
        return data.get('items') or []

    def _fetch_pods_kubectl(self) -> list[dict]:
        selector = f'app in ({self.edge_app_label},{self.core_app_label})'
        out = subprocess.check_output(
            ['kubectl', '-n', self.namespace, 'get', 'pods', '-l', selector, '-o', 'json'],
            timeout=self.timeout,
            text=True,
        )
        return json.loads(out).get('items') or []

    def discover(self) -> tuple[dict[str, list[tuple[str, int]]], list[tuple[str, int]]]:
        try:
            pods = self._fetch_pods_incluster()
        except FileNotFoundError:
            pods = self._fetch_pods_kubectl()
        return filter_pinned_endpoints(
            pods,
            site_nodes=self.site_nodes,
            core_node=self.core_node,
            edge_app_label=self.edge_app_label,
            core_app_label=self.core_app_label,
            container_port=self.container_port,
        )
