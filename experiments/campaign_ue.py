"""Remote side of the campaign runner; invoked over SSH, not manually."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def identity(pid):
    stat = Path(f'/proc/{pid}/stat').read_text()
    return [stat[stat.rfind(')') + 2:].split()[19],
            Path('/proc/sys/kernel/random/boot_id').read_text().strip()]


def marker(campaign, method):
    return Path('results') / campaign / f'.{method}-process.json'


def stop(campaign, method):
    path = marker(campaign, method)
    if not path.exists():
        return
    record = json.loads(path.read_text())
    try:
        fd = os.pidfd_open(record['pid'])
    except ProcessLookupError:
        path.unlink()
        return
    try:
        if identity(record['pid']) != record['identity']:
            raise RuntimeError('UE process identity changed; refusing to signal it')
        signal.pidfd_send_signal(fd, signal.SIGTERM)
        for _ in range(90):
            if not Path(f'/proc/{record["pid"]}').exists():
                path.unlink(missing_ok=True)
                return
            time.sleep(1)
        raise RuntimeError('UE cleanup exceeded 90 seconds; inspect its lifecycle journal')
    finally:
        os.close(fd)


def check(settings):
    from experiments.ue_lifecycle import _load_config
    subscribers = _load_config(settings['inventory'])
    if len(subscribers) < settings['max_ues']:
        raise ValueError('not enough reserved subscribers for the campaign')
    if not os.access(settings['nr_ue'], os.X_OK):
        raise ValueError('nr-ue executable is missing')
    if not Path(settings['token']).read_bytes().strip():
        raise ValueError('telemetry token file is empty')
    from benchmark.telemetry import telemetry_config
    telemetry_config(settings['telemetry_url'], settings['token'])
    images = Path(settings['image'])
    if not images.is_dir() or not any(images.glob('*.png')):
        raise ValueError('campaign image directory is missing PNG inputs')
    for path in Path('/proc').glob('[0-9]*/cmdline'):
        try:
            argv = path.read_bytes().split(b'\0')
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if argv and (Path(os.fsdecode(argv[0])).name == 'nr-ue' or b'experiments.experiment' in argv):
            raise RuntimeError('a UE workload is already active; finish it before starting a campaign')
    print(f'UE prerequisites ready: {len(subscribers)} reserved subscribers', flush=True)


def run(settings):
    from experiments.experiment import validate_site_urls
    validate_site_urls(settings['site_urls'])
    campaign, method = settings['campaign'], settings['method']
    root = Path('results') / campaign
    root.mkdir(parents=True, exist_ok=True)
    output = root / method
    if output.exists():
        raise FileExistsError(output)
    args = [sys.executable, '-u', '-m', 'experiments.experiment', '--armed',
            '--site-urls', json.dumps(settings['site_urls']), '--image', settings['image'],
            '--ue-config', settings['inventory'], '--nr-ue', settings['nr_ue'],
            '--phase-schedule', settings['schedule'], '--phase-duration', str(settings['duration']),
            '--telemetry-url', settings['telemetry_url'], '--telemetry-token-file', settings['token'],
            '--output-dir', str(output)]
    path = marker(campaign, method)
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    def terminate(signum, frame):
        if process.poll() is None:
            process.terminate()
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    try:
        with path.open('x') as stream:
            json.dump({'pid': process.pid, 'identity': identity(process.pid)}, stream)
        with (root / f'{method}-runner.log').open('x') as log:
            for line in process.stdout:
                log.write(line); log.flush()
                print(line, end='', flush=True)
        return process.wait()
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=90)
        path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['check', 'run', 'stop'])
    parser.add_argument('--campaign')
    parser.add_argument('--method', choices=['baseline', 'proposed'])
    args = parser.parse_args()
    if args.action == 'stop':
        stop(args.campaign, args.method)
        return
    settings = json.load(sys.stdin)
    if args.action == 'check':
        check(settings)
    else:
        raise SystemExit(run(settings))


if __name__ == '__main__':
    main()
