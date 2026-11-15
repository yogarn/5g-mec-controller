"""Build, push, and roll out the Go scheduler from core-5g."""
import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import uuid

ROOT = Path(__file__).resolve().parent.parent
LOCK = '/tmp/mec-experiment-campaign.lock'
CONTAINER = 'placement-controller'


def command(args, **kwargs):
    print('+', shlex.join(map(str, args)), flush=True)
    return subprocess.run(list(map(str, args)), check=True, **kwargs)


def deployment(namespace, name):
    return json.loads(command(['kubectl', '-n', namespace, 'get', 'deployment', name,
                              '-o', 'json'], capture_output=True, text=True).stdout)


def image_of(dep):
    containers = dep['spec']['template']['spec']['containers']
    return next(c['image'] for c in containers if c['name'] == CONTAINER)


def published_digest(repo, values):
    matches = {value.split('@', 1)[1] for value in values if value.startswith(repo + '@')}
    if len(matches) != 1:
        raise RuntimeError('expected exactly one published digest for the pushed repository')
    digest = matches.pop()
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
        raise RuntimeError('Docker returned an invalid published digest')
    return digest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--push-repository', default='localhost:30500/placement-controller',
                        help='Docker push repository reachable on core (no tag or digest)')
    parser.add_argument('--pull-repository', default='10.34.211.140:30500/placement-controller',
                        help='same repository as seen by cluster nodes (no tag or digest)')
    parser.add_argument('--namespace', default='placement')
    parser.add_argument('--deployment', default='placement-controller')
    parser.add_argument('--timeout', type=int, default=180, help='rollout timeout in seconds')
    parser.add_argument('--plan-only', action='store_true', help='print steps without Docker or cluster access')
    args = parser.parse_args(argv)
    for repo in (args.push_repository, args.pull_repository):
        if not re.fullmatch(r'[A-Za-z0-9._:-]+(?:/[a-z0-9._-]+)+', repo) or ':' in repo.rsplit('/', 1)[-1]:
            parser.error('repositories must not contain a tag, digest, scheme, or whitespace')
    if args.timeout <= 0:
        parser.error('--timeout must be positive')
    tag = datetime.now(timezone.utc).strftime('build-%Y%m%dT%H%M%SZ-') + uuid.uuid4().hex[:8]
    pushed_image = f'{args.push_repository}:{tag}'
    build = ['docker', 'build', '--network=host', '-t', pushed_image, ROOT / 'placement']
    push = ['docker', 'push', pushed_image]
    inspect = ['docker', 'image', 'inspect', pushed_image, '--format', '{{json .RepoDigests}}']
    kube = ['kubectl', '-n', args.namespace]
    if args.plan_only:
        for cmd in [kube + ['get', 'deployment', args.deployment, '-o', 'json'], build, push, inspect,
                    kube + ['set', 'image', 'deployment/' + args.deployment,
                            CONTAINER + '=' + args.pull_repository + '@<published-sha256-digest>'],
                    kube + ['rollout', 'status', 'deployment/' + args.deployment, f'--timeout={args.timeout}s']]:
            print(shlex.join(map(str, cmd)))
        return
    os.environ.setdefault('KUBECONFIG', '/etc/rancher/k3s/k3s.yaml')
    with open(LOCK, 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('a campaign or scheduler redeployment is active; retry after it finishes') from None
        before = deployment(args.namespace, args.deployment)
        try:
            previous = image_of(before)
        except (KeyError, StopIteration):
            raise RuntimeError('existing Deployment must contain the placement-controller container') from None
        print(f'Previous image: {previous}', flush=True)
        command(build)
        command(push)
        values = json.loads(command(inspect, capture_output=True, text=True).stdout)
        if not isinstance(values, list):
            raise RuntimeError('Docker did not report published repository digests')
        image = args.pull_repository + '@' + published_digest(args.push_repository, values)
        command(kube + ['set', 'image', 'deployment/' + args.deployment, CONTAINER + '=' + image])
        command(kube + ['rollout', 'status', 'deployment/' + args.deployment, f'--timeout={args.timeout}s'])
        after = deployment(args.namespace, args.deployment)
        status = after.get('status', {})
        replicas = after['spec'].get('replicas', 1)
        if (image_of(after) != image or replicas != 1 or status.get('readyReplicas', 0) != replicas
                or status.get('updatedReplicas', 0) != replicas
                or status.get('observedGeneration', 0) < after['metadata']['generation']):
            raise RuntimeError('rollout verification failed; inspect Deployment status and scheduler logs')
        print(f'Scheduler ready: {image}', flush=True)
        print('Existing ConfigMap preserved. No campaign or workload was started.', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        raise SystemExit(f'Scheduler redeployment stopped: {exc}') from exc
