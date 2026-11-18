"""Short real-traffic smoke campaigns; use --plan-only for an offline preview."""
import argparse
import csv
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time

from experiments import campaign
from utils.plot_paths import default_csv_dir

PROFILES = {
    'mobility': {
        'schedule': '9:1:7:1,9:1:5:3',
        'flags': ['--scenario', 'mobility9', '--comparison', 'full'],
        'description': 'Fixed replicas; move two UEs from gNB-2 to gNB-3.',
    },
    'scaling': {
        'schedule': '3:1:1:1,12:4:4:4,3:1:1:1',
        'flags': ['--scenario', 'scaler-staircase', '--comparison', 'scaling'],
        'description': 'Low/high/low load on gNB-3; default scheduler for both methods.',
    },
    'joint': {
        'schedule': '3:1:1:1,9:1:7:1,9:1:1:7,3:1:1:1',
        'flags': ['--comparison', 'full', '--deschedule-interval', '30'],
        'description': 'Scale up, move the hotspot, then reduce load; full-system comparison.',
    },
}
PHASE_SECONDS = 60
WALL_BUDGET_SECONDS = 900
CLEANUP_RESERVE_SECONDS = 120
STOP_AFTER_SECONDS = WALL_BUDGET_SECONDS - CLEANUP_RESERVE_SECONDS


def build_campaign_args(args):
    profile = PROFILES[args.profile]
    flags = [args.name, *profile['flags'],
             '--phase-schedule', profile['schedule'], '--phase-duration', str(PHASE_SECONDS),
             '--phase-transition', 'continuous', '--config', str(args.config),
             '--slo-ms', str(args.slo_ms), '--tc-ms', str(args.tc_ms),
             '--rps-per-ue', str(args.rps_per_ue),
             '--max-in-flight-per-ue', str(args.max_in_flight_per_ue),
             '--method-order', args.method_order]
    if args.profile == 'mobility':
        flags += ['--fixed-replicas', str(args.replicas)]
    else:
        flags += ['--min-replicas', str(args.min_replicas), '--max-replicas', str(args.max_replicas)]
    if args.profile == 'scaling':
        flags += ['--latency-floor-ms', str(args.latency_floor_ms)]
    if args.plan_only is not None:
        flags += ['--plan-only', str(args.plan_only.resolve())]
    return flags


def smoke_metadata(args, flags):
    phases = campaign.parse_schedule(PROFILES[args.profile]['schedule'])
    return {
        'purpose': 'smoke test; not a repeated or steady-state performance evaluation',
        'profile': args.profile,
        'description': PROFILES[args.profile]['description'],
        'campaign': args.name,
        'schedule': PROFILES[args.profile]['schedule'],
        'phase_seconds': PHASE_SECONDS,
        'methods': 2,
        'traffic_seconds_per_method': len(phases) * PHASE_SECONDS,
        'traffic_seconds_total': 2 * len(phases) * PHASE_SECONDS,
        'wall_budget_seconds': WALL_BUDGET_SECONDS,
        'stop_requested_after_seconds': STOP_AFTER_SECONDS,
        'cleanup_reserve_seconds': CLEANUP_RESERVE_SECONDS,
        'command': [sys.executable, '-u', '-m', 'experiments.campaign', *flags],
        'result_directory': str(campaign.ROOT / 'results' / args.name),
        'plots_directory': str(campaign.ROOT / 'results' / args.name / 'plots'),
        'csv_directory': str(campaign.ROOT / 'results' / args.name / 'csv'),
        'note': 'Setup, transfer and cleanup vary. Timeout requests graceful cleanup; no forced kill.',
    }


def _write_status(path, record):
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, prefix=path.name + '.', delete=False) as stream:
        temp = Path(stream.name)
        json.dump(record, stream, indent=2)
        stream.write('\n')
    try:
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _interrupt(process):
    try:
        process.send_signal(signal.SIGINT)
    except ProcessLookupError:
        pass  # It exited between the timeout check and the stop request.


def inspect_outputs(plots, csv_dir=None):
    csv_dir = Path(csv_dir) if csv_dir is not None else default_csv_dir(plots)
    required = {'response-time.png', 'pod-placement.png', 'resource-usage.png', 'slo-violations.png'}
    pngs = sorted(p.name for p in plots.glob('*.png'))
    errors = [f'missing {name}' for name in sorted(required - set(pngs))]
    measurements = {}
    for name, keys in [
        ('window-metrics-15s.csv', ['p90_ms']),
        ('pod-placement-15s.csv', ['ready_count']),
        ('resource-windows-15s.csv', ['cpu_millicores', 'memory_mib']),
    ]:
        try:
            path = csv_dir / name
            if not path.exists():
                path = plots / name  # Read historical smoke output layouts too.
            with path.open(newline='') as stream:
                rows = list(csv.DictReader(stream))
        except (OSError, csv.Error) as exc:
            errors.append(f'{name}: {exc}')
            continue
        for method in ('baseline', 'proposed'):
            for key in keys:
                valid = 0
                for row in rows:
                    if row.get('method') != method:
                        continue
                    try:
                        valid += int(math.isfinite(float(row[key])))
                    except (KeyError, ValueError, TypeError):
                        pass
                measurements[f'{name}:{method}:{key}'] = valid
                if not valid:
                    errors.append(f'{name}: no finite {key} observations for {method}')
    return pngs, measurements, errors


def supervise(command, status_path, metadata, *, stop_after=STOP_AFTER_SECONDS,
              popen=subprocess.Popen, monotonic=time.monotonic):
    """Request one SIGINT at the deadline, allowing campaign-owned cleanup."""
    started = monotonic()
    record = {**metadata, 'status': 'starting',
              'started_at': datetime.now(timezone.utc).isoformat()}
    _write_status(status_path, record)
    process = None
    reason = None
    reserve_warning = False
    def terminated(signum, frame):
        raise KeyboardInterrupt
    previous_terminate = signal.signal(signal.SIGTERM, terminated)
    try:
        process = popen(command, cwd=campaign.ROOT, start_new_session=True)
        record.update(status='running', pid=process.pid)
        _write_status(status_path, record)
        while True:
            try:
                code = process.wait(timeout=0.25)
                break
            except subprocess.TimeoutExpired:
                elapsed = monotonic() - started
                if reason is None and elapsed >= stop_after:
                    reason = 'time_budget'
                    _interrupt(process)
                    record.update(status='stopping', stop_reason=reason)
                    _write_status(status_path, record)
                    print('Smoke time budget reached; requesting campaign cleanup. Partial results are retained.', flush=True)
                elif reason is not None and not reserve_warning and elapsed >= WALL_BUDGET_SECONDS:
                    reserve_warning = True
                    record.update(cleanup_over_budget=True, elapsed_seconds=elapsed)
                    _write_status(status_path, record)
                    print('Cleanup exceeded the 15-minute budget; waiting without force-killing owned UE cleanup.', flush=True)
            except KeyboardInterrupt:
                if reason is None:
                    reason = 'user_interrupt'
                    _interrupt(process)
                    record.update(status='stopping', stop_reason=reason)
                    _write_status(status_path, record)
                    print('Interrupt forwarded; waiting for cleanup. Do not interrupt again.', flush=True)
        plots = Path(metadata['plots_directory'])
        pngs, measurements, errors = inspect_outputs(plots, metadata.get('csv_directory'))
        complete = not errors
        record.update(status=('complete' if code == 0 and complete and reason is None else
                              'timed_out' if reason == 'time_budget' else
                              'interrupted' if reason == 'user_interrupt' else 'failed'),
                      returncode=code, elapsed_seconds=monotonic() - started,
                      plot_files=pngs, plots_complete=complete,
                      finite_measurements=measurements, output_errors=errors,
                      finished_at=datetime.now(timezone.utc).isoformat())
        _write_status(status_path, record)
        print(f"Smoke {record['status']}; status: {status_path}", flush=True)
        if pngs:
            print(f'Plots: {plots}', flush=True)
        if errors and code == 0:
            print('Smoke output checks failed: ' + '; '.join(errors), flush=True)
        return 0 if record['status'] == 'complete' else 124 if reason == 'time_budget' else 130 if reason else code or 1
    except BaseException as exc:
        # Never abandon a running campaign when writing status or polling fails.
        if process is not None and process.poll() is None:
            if reason is None:
                _interrupt(process)
            process.wait()
        record.update(status='interrupted' if isinstance(exc, KeyboardInterrupt) else 'failed',
                      error=str(exc), elapsed_seconds=monotonic() - started)
        _write_status(status_path, record)
        if isinstance(exc, KeyboardInterrupt):
            return 130
        raise
    finally:
        signal.signal(signal.SIGTERM, previous_terminate)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('profile', choices=PROFILES)
    parser.add_argument('name', help='fresh campaign name; existing results are never overwritten')
    parser.add_argument('--config', type=Path, default=campaign.ROOT / 'experiments/campaign.yaml')
    parser.add_argument('--plan-only', type=Path, metavar='DIRECTORY', help='render offline manifests; no traffic or subprocess')
    parser.add_argument('--replicas', type=int, help='mobility only: fixed replicas per method (default: 3)')
    parser.add_argument('--min-replicas', type=int, help='scaling/joint shared initial and minimum replicas (default: 3)')
    parser.add_argument('--max-replicas', type=int, help='scaling/joint shared ceiling (default: 12)')
    parser.add_argument('--slo-ms', type=float, default=150)
    parser.add_argument('--tc-ms', type=float, default=10)
    parser.add_argument('--rps-per-ue', type=float, default=5)
    parser.add_argument('--max-in-flight-per-ue', type=int, default=8)
    parser.add_argument('--latency-floor-ms', type=int, default=None,
                        help='scaling only: exploratory floor (default: 125); joint uses raw RT/SLO')
    parser.add_argument('--method-order', choices=('baseline-first', 'proposed-first', 'random'), default='baseline-first')
    args = parser.parse_args(argv)
    if args.profile == 'mobility' and (args.min_replicas is not None or args.max_replicas is not None):
        parser.error('mobility uses --replicas; min/max options are for scaling and joint')
    if args.profile != 'mobility' and args.replicas is not None:
        parser.error('--replicas is only available for mobility')
    args.replicas = 3 if args.replicas is None else args.replicas
    args.min_replicas = 3 if args.min_replicas is None else args.min_replicas
    args.max_replicas = 12 if args.max_replicas is None else args.max_replicas
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', args.name) or args.name == 'smoke-status':
        parser.error('use a fresh campaign name with letters, digits, hyphens or underscores')
    if any(not math.isfinite(v) or v <= 0 for v in (args.slo_ms, args.tc_ms, args.rps_per_ue)):
        parser.error('SLO, TC delay, and RPS must be finite and positive')
    if min(args.replicas, args.min_replicas, args.max_replicas, args.max_in_flight_per_ue) < 1:
        parser.error('replica counts and maximum in-flight requests must be positive')
    if args.max_replicas < args.min_replicas:
        parser.error('max replicas must be at least min replicas')
    if args.latency_floor_ms is not None and args.profile != 'scaling':
        parser.error('--latency-floor-ms is only available for the scaling profile')
    args.latency_floor_ms = (125 if args.profile == 'scaling' else 0) if args.latency_floor_ms is None else args.latency_floor_ms
    if args.latency_floor_ms < 0 or args.latency_floor_ms >= args.slo_ms:
        parser.error('latency floor must be nonnegative and smaller than the SLO')
    args.config = args.config.resolve(strict=True)
    if args.plan_only is not None:
        args.plan_only = args.plan_only.resolve()
    flags = build_campaign_args(args)
    metadata = smoke_metadata(args, flags)
    print(f"{args.profile}: {metadata['traffic_seconds_total'] // 60} minutes traffic across both methods; "
          '60s phases, continuous UE transitions. Stop guard: 13 minutes, plus cleanup.', flush=True)
    if args.profile == 'joint':
        print('Joint uses the existing full-system raw RT/SLO policy, without the scaling-only floor.', flush=True)
    if args.plan_only is not None:
        campaign.main(flags)
        (args.plan_only / 'smoke-plan.json').write_text(json.dumps(metadata, indent=2) + '\n')
        return 0
    if (campaign.ROOT / 'results' / args.name).exists():
        parser.error('result directory already exists; choose a new campaign name')
    status_dir = campaign.ROOT / 'results/smoke-status'
    status_dir.mkdir(parents=True, exist_ok=True)
    status = status_dir / f'{args.name}.json'
    try:
        with status.open('x') as stream:
            json.dump({**metadata, 'status': 'reserved'}, stream, indent=2)
    except FileExistsError:
        parser.error('smoke status already exists; choose a new campaign name')
    return supervise(metadata['command'], status, metadata)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        raise SystemExit(f'Smoke stopped: {exc}') from exc
