"""Run with python -m controller --config PATH [--once]."""
import argparse
import logging
import time
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from .config import load_settings, workload_identity
from .reconcile import Reconciler
from .telemetry import Prometheus


def main():
    parser = argparse.ArgumentParser(description='Generic MEC placement and scaling controller')
    parser.add_argument('--config', default='config.yaml', help='configuration file (default: config.yaml)')
    parser.add_argument('--once', action='store_true', help='perform one observation/reconciliation')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    settings = load_settings(args.config)
    namespace, name = workload_identity(settings.manifest, settings.namespace)
    try:
        config.load_incluster_config()
    except ConfigException:
        config.load_kube_config()
    api_client = client.ApiClient()
    controller = Reconciler(settings, client.AppsV1Api(api_client), client.CoreV1Api(api_client),
                            client.AutoscalingV1Api(api_client), api_client,
                            Prometheus(settings.prometheus_url, settings.max_metric_age_seconds), namespace, name)
    while True:
        try:
            controller.run_once()
        except Exception:
            controller.policy.down_since = None
            logging.exception('Reconciliation failed; no further action in this cycle')
            if args.once:
                raise
        if args.once:
            return
        time.sleep(settings.interval_seconds)


if __name__ == '__main__':
    main()
