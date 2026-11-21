"""Build and validate pedestrian detection, then pin its published image digest."""
import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid

import yaml

from utils.redeploy_placement import LOCK, command, published_digest

ROOT = Path(__file__).resolve().parent.parent
PUSH_REPOSITORY = 'localhost:30500/pedestrian-detection'
PULL_REPOSITORY = '10.34.211.140:30500/pedestrian-detection'
MANIFESTS = ('apps/pedestrian-detection/manifest.yaml',)
HEALTH_CHECK = (
    'from app import create_app; from urllib.request import urlopen; '
    'app = create_app(); '
    'assert urlopen("http://127.0.0.1:5001/health", timeout=3).status == 200; '
    'assert urlopen("http://127.0.0.1:5001/ready", timeout=3).status == 200; '
    'print("Model loaded; dedicated health and readiness endpoints passed")'
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan-only', action='store_true', help='preview without Docker or file changes')
    args = parser.parse_args(argv)
    manifests = []
    for relative in MANIFESTS:
        path = ROOT / relative
        docs = list(yaml.safe_load_all(path.read_text()))
        containers = [c for d in docs if d and d.get('kind') == 'Deployment'
                      for c in d['spec']['template']['spec']['containers']
                      if c['name'] == 'pedestrian-detection']
        if len(containers) != 1:
            raise RuntimeError(f'{relative} must contain exactly one pedestrian-detection container')
        manifests.append((path, docs, containers[0]))

    tag = datetime.now(timezone.utc).strftime('health-%Y%m%dT%H%M%SZ-') + uuid.uuid4().hex[:8]
    image = f'{PUSH_REPOSITORY}:{tag}'
    steps = [
        ['docker', 'build', '--network=host', '-t', image, ROOT / 'apps/pedestrian-detection'],
        ['docker', 'run', '--rm', '--entrypoint', 'python', image, '-c', HEALTH_CHECK],
        ['docker', 'push', image],
    ]
    inspect = ['docker', 'image', 'inspect', image, '--format', '{{json .RepoDigests}}']
    if args.plan_only:
        import shlex
        for step in steps + [inspect]:
            print(shlex.join(map(str, step)))
        print('Pin both manifests to the validated published digest. The campaign deploys the image.')
        return

    with open(LOCK, 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('a campaign or rebuild is active; retry after it finishes') from None
        for step in steps:
            command(step)
        refs = json.loads(command(inspect, capture_output=True, text=True).stdout)
        if not isinstance(refs, list):
            raise RuntimeError('Docker did not report published repository digests')
        pinned = PULL_REPOSITORY + '@' + published_digest(PUSH_REPOSITORY, refs)
        prepared = []
        try:
            for path, docs, container in manifests:
                container['image'] = pinned
                with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, prefix=path.name + '.', delete=False) as stream:
                    prepared.append((Path(stream.name), path))
                    yaml.safe_dump_all(docs, stream, sort_keys=False)
                os.chmod(stream.name, path.stat().st_mode & 0o777)
            for temporary, path in prepared:
                os.replace(temporary, path)
        finally:
            for temporary, _ in prepared:
                temporary.unlink(missing_ok=True)
        print(f'Pedestrian image ready: {pinned}', flush=True)
        print('Both manifests updated. Start the campaign to deploy and test this image.', flush=True)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        raise SystemExit(f'Pedestrian rebuild stopped: {exc}') from exc
