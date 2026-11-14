"""Run with python -m controller --config PATH [--once] [--deploy]."""
import argparse
import logging
import time
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from .bootstrap import deploy_workload
from .config import load_settings, workload_identity
from .reconcile import Reconciler
from .telemetry import Prometheus


def main():
    parser = argparse.ArgumentParser(description='Generic MEC placement and scaling controller')
    parser.add_argument('--config', default='config.yaml', help='configuration file (default: config.yaml)')
    parser.add_argument('--once', action='store_true', help='perform one observation/reconciliation')
    parser.add_argument('--deploy', action='store_true',
                        help='create missing manifest Deployment and Service before reconciling; '
                             'requires mutate: true and an existing namespace')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    settings = load_settings(args.config)
    if args.deploy and not settings.mutate:
        parser.error('--deploy requires mutate: true')
    namespace, name = workload_identity(settings.manifest, settings.namespace)
    try:
        config.load_incluster_config()
    except ConfigException:
        config.load_kube_config()
    api_client = client.ApiClient()
    apps = client.AppsV1Api(api_client)
    core = client.CoreV1Api(api_client)
    if args.deploy:
        try:
            deploy_workload(settings.manifest, namespace, apps, core, mutate=settings.mutate)
        except Exception as exc:
            logging.error('Deployment bootstrap failed: %s', exc)
            raise SystemExit(1) from None
    controller = Reconciler(settings, apps, core, client.AutoscalingV1Api(api_client), api_client,
                            Prometheus(settings.prometheus_url, settings.max_metric_age_seconds), namespace, name)
    while True:
        try:
            controller.run_once()
        except Exception as exc:
            controller.policy.down_since = None
            logging.error('Reconciliation failed: %s', exc)
            if args.once:
                raise SystemExit(1) from None
        if args.once:
            return
        time.sleep(settings.interval_seconds)


if __name__ == '__main__':
    main()
