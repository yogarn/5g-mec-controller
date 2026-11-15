#!/usr/bin/env python3
"""Fixed-replica calibration harness for G1/G2 (reuses UE lifecycle, sampler, and TrafficDelay)."""
import argparse
import csv
import fcntl
import hashlib
import json
import math
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

import yaml

from experiments.campaign import APP, NAMESPACE, check_service_routing, command, kube, remote, stop_process
from utils.traffic_control import POOL_NAME, TrafficDelay

ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = Path('/tmp/mec-experiment-campaign.lock')


def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


def build_manifest(replicas, node, use_tc):
    docs = list(yaml.safe_load_all((ROOT / 'apps/face-detection/manifest.yaml').read_text()))
    for doc in docs:
        if not doc:
            continue
        doc['metadata']['namespace'] = NAMESPACE
        if doc.get('kind') == 'Deployment':
            doc['spec']['replicas'] = replicas
            spec = doc['spec']['template']['spec']
            spec['nodeSelector'] = {'kubernetes.io/hostname': node}
            if use_tc:
                meta = doc['spec']['template'].setdefault('metadata', {})
                ann = meta.setdefault('annotations', {})
                ann['cni.projectcalico.org/ipv4pools'] = json.dumps([POOL_NAME])
        elif doc.get('kind') == 'Service':
            check_service_routing(doc)
    return yaml.safe_dump_all(docs, sort_keys=False)


def cleanup_app():
    kube('-n', NAMESPACE, 'delete', 'deployment', APP, '--ignore-not-found', '--wait=true', '--timeout=120s')
    kube('-n', NAMESPACE, 'delete', 'service', APP, '--ignore-not-found', '--wait=true', '--timeout=120s')
    kube('-n', NAMESPACE, 'wait', '--for=delete', 'pod', '-l', f'app={APP}', '--timeout=120s')


def summarize_requests(jsonl_path, warmup_s):
    rows = [json.loads(line) for line in jsonl_path.read_text().splitlines() if line.strip()]
    starts = [r for r in rows if r.get('event') == 'phase_start']
    ends = [r for r in rows if r.get('event') == 'phase_end']
    reqs = [r for r in rows if r.get('event') == 'request']
    if not starts or not ends:
        raise ValueError('missing phase_start or phase_end')
    t0 = starts[0]['start_time']
    t_measure_start = t0 + warmup_s
    t_end = ends[-1]['end_time']

    def stats(subset, dur_s):
        n = len(subset)
        succ = [r for r in subset if r.get('success')]
        fail = n - len(succ)
        succ_durs = sorted(r['duration_ms'] for r in succ)
        all_durs = sorted(r['duration_ms'] for r in subset)
        acc_500 = sum(1 for r in succ if r['duration_ms'] <= 500.0) / n if n else 0.0
        acc_300 = sum(1 for r in succ if r['duration_ms'] <= 300.0) / n if n else 0.0
        return {
            'offered': n,
            'completed': len(succ),
            'failures': fail,
            'error_rate': fail / n if n else 0.0,
            'rps': len(succ) / dur_s if dur_s > 0 else 0.0,
            'acc_500': acc_500,
            'viol_500': 1.0 - acc_500,
            'acc_300': acc_300,
            'viol_300': 1.0 - acc_300,
            'succ_p50_ms': percentile(succ_durs, 0.50),
            'succ_p90_ms': percentile(succ_durs, 0.90),
            'succ_p95_ms': percentile(succ_durs, 0.95),
            'all_p90_ms': percentile(all_durs, 0.90),
        }

    steady = [r for r in reqs if r['start_time'] >= t_measure_start]
    steady_dur = max(1.0, t_end - t_measure_start)
    full_dur = max(1.0, t_end - t0)
    return {
        'start_time': t0,
        'measure_start_time': t_measure_start,
        'end_time': t_end,
        'steady': stats(steady, steady_dur),
        'full': stats(reqs, full_dur),
    }


def summarize_resources(res_jsonl, t_measure_start, t_end):
    rows = [json.loads(line) for line in res_jsonl.read_text().splitlines() if line.strip() and 'metrics' in line]
    steady_cpus, steady_mems = [], []
    for row in rows:
        t = row['sample_time']
        if not (t_measure_start <= t <= t_end):
            continue
        pods = row.get('pods', [])
        metrics = row.get('metrics', {})
        if not pods or any(p['name'] not in metrics for p in pods):
            continue
        total_cpu = 0.0
        total_mem = 0.0
        for p in pods:
            for c in metrics[p['name']].get('containers', []):
                u = c['usage']
                cpu_s = u['cpu']
                if cpu_s.endswith('n'):
                    total_cpu += float(cpu_s[:-1]) / 1e6
                elif cpu_s.endswith('u'):
                    total_cpu += float(cpu_s[:-1]) / 1e3
                elif cpu_s.endswith('m'):
                    total_cpu += float(cpu_s[:-1])
                else:
                    total_cpu += float(cpu_s) * 1000.0
                mem_s = u['memory']
                if mem_s.endswith('Ki'):
                    total_mem += float(mem_s[:-2]) / 1024.0
                elif mem_s.endswith('Mi'):
                    total_mem += float(mem_s[:-2])
                elif mem_s.endswith('Gi'):
                    total_mem += float(mem_s[:-2]) * 1024.0
        steady_cpus.append(total_cpu)
        steady_mems.append(total_mem)
    return {
        'samples': len(steady_cpus),
        'cpu_mean_m': sum(steady_cpus) / len(steady_cpus) if steady_cpus else None,
        'cpu_max_m': max(steady_cpus) if steady_cpus else None,
        'mem_mean_mi': sum(steady_mems) / len(steady_mems) if steady_mems else None,
        'mem_max_mi': max(steady_mems) if steady_mems else None,
    }


def run_one(settings, campaign_dir, stage, block, run_id, replicas, node, schedule, duration, warmup, tc_ms):
    run_dir = campaign_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    pod_logs_dir = run_dir / 'pod-logs'
    pod_logs_dir.mkdir()

    cleanup_app()
    use_tc = tc_ms is not None and tc_ms > 0
    manifest_yaml = build_manifest(replicas, node, use_tc)
    (run_dir / 'manifest.yaml').write_text(manifest_yaml)

    rev = subprocess.check_output(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True).strip()
    eff_cfg = {
        'stage': stage, 'block': block, 'run_id': run_id, 'revision': rev,
        'replicas': replicas, 'node': node, 'schedule': schedule,
        'duration_s': duration, 'warmup_s': warmup, 'measure_s': duration - warmup,
        'tc_ms': tc_ms, 'namespace': NAMESPACE,
    }
    (run_dir / 'effective-config.json').write_text(json.dumps(eff_cfg, indent=2) + '\n')

    tc = TrafficDelay(tc_ms) if use_tc else None
    sampler = None
    try:
        if tc:
            tc.start()
        subprocess.run(['kubectl', 'apply', '-f', str(run_dir / 'manifest.yaml')], check=True)
        kube('-n', NAMESPACE, 'rollout', 'status', f'deployment/{APP}', '--timeout=180s')

        svc = json.loads(kube('-n', NAMESPACE, 'get', 'service', APP, '-o', 'json', capture_output=True, text=True).stdout)
        nodeport = svc['spec']['ports'][0]['nodePort']
        pods_json = json.loads(kube('-n', NAMESPACE, 'get', 'pods', '-l', f'app={APP}', '-o', 'json', capture_output=True, text=True).stdout)
        pods_json['items'] = [p for p in pods_json['items'] if not p['metadata'].get('deletionTimestamp')]
        (run_dir / 'pods-ready.json').write_text(json.dumps(pods_json, indent=2) + '\n')

        site_urls = {site: f'http://{ip}:{nodeport}/detect' for site, ip in settings['site_node_ips'].items()}
        res_file = run_dir / 'resources.jsonl'
        sampler = subprocess.Popen(
            [sys.executable, '-m', 'utils.monitor_resources', 'sample', str(res_file), '--namespace', NAMESPACE],
            cwd=str(ROOT),
        )

        ue_out = f'results/{campaign_dir.name}/{run_id}'
        ue_cmd = [
            'sudo', '-n', '.venv/bin/python', '-u', '-m', 'utils.experiment', '--armed',
            '--site-urls', json.dumps(site_urls),
            '--image', settings['image'],
            '--ue-config', settings['inventory'],
            '--nr-ue', settings['nr_ue'],
            '--phase-schedule', schedule,
            '--phase-duration', str(duration),
            '--telemetry-url', settings['telemetry_url'],
            '--telemetry-token-file', settings['token'],
            '--output-dir', ue_out,
        ]
        ue_log = run_dir / 'ue-runner.log'
        with ue_log.open('w') as lf:
            subprocess.run(remote(settings, ue_cmd), stdout=lf, stderr=subprocess.STDOUT, check=True)

        stop_process(sampler)
        sampler = None

        # Transfer requests.jsonl and verify checksum
        raw_dest = run_dir / 'requests.jsonl'
        with raw_dest.open('wb') as out_f:
            subprocess.run(remote(settings, ['sudo', '-n', 'cat', f'{ue_out}/requests.jsonl']), stdout=out_f, check=True)
        local_hash = hashlib.sha256(raw_dest.read_bytes()).hexdigest()
        remote_hash = subprocess.check_output(
            remote(settings, ['sudo', '-n', 'sha256sum', f'{ue_out}/requests.jsonl']), text=True
        ).split()[0]
        if local_hash != remote_hash:
            raise RuntimeError(f'checksum mismatch for {run_id}: {local_hash} != {remote_hash}')
        (run_dir / 'requests.sha256').write_text(f'{local_hash}  requests.jsonl\n')

        # Collect per-pod request logs and cgroup stats
        backend_counts = {}
        cgroup_stats = {}
        for pod in pods_json['items']:
            pname = pod['metadata']['name']
            plog = subprocess.check_output(['kubectl', '-n', NAMESPACE, 'logs', pname], text=True)
            (pod_logs_dir / f'{pname}.log').write_text(plog)
            backend_counts[pname] = sum(1 for line in plog.splitlines() if 'POST /detect' in line)
            try:
                cstat = subprocess.check_output(['kubectl', '-n', NAMESPACE, 'exec', pname, '--', 'cat', '/sys/fs/cgroup/cpu.stat'], text=True)
                mpeak = subprocess.check_output(['kubectl', '-n', NAMESPACE, 'exec', pname, '--', 'cat', '/sys/fs/cgroup/memory.peak'], text=True).strip()
                cgroup_stats[pname] = {'cpu_stat': dict(l.split() for l in cstat.strip().splitlines()), 'memory_peak_bytes': int(mpeak)}
            except subprocess.SubprocessError:
                pass
        (run_dir / 'backend-counts.json').write_text(json.dumps(backend_counts, indent=2) + '\n')
        (run_dir / 'cgroup-stats.json').write_text(json.dumps(cgroup_stats, indent=2) + '\n')

        req_sum = summarize_requests(raw_dest, warmup)
        res_sum = summarize_resources(res_file, req_sum['measure_start_time'], req_sum['end_time'])
        summary = {'effective': eff_cfg, 'requests': req_sum, 'resources': res_sum,
                   'backend_counts': backend_counts, 'cgroup': cgroup_stats}
        (run_dir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        return summary
    finally:
        stop_process(sampler)
        cleanup_app()
        if tc:
            tc.close()


def append_ledger(csv_path, summary, valid=True, exclusion_reason='', next_decision=''):
    eff = summary['effective']
    st = summary['requests']['steady']
    rs = summary['resources']
    fields = [
        'stage', 'run_id', 'block', 'method', 'revision', 'config_path',
        'start_time', 'end_time', 'offered', 'completed', 'failures',
        'acc_500ms', 'acc_300ms', 'succ_p50_ms', 'succ_p90_ms', 'succ_p95_ms', 'all_p90_ms',
        'rps', 'replicas', 'cpu_mean_m', 'mem_max_mi', 'backend_counts',
        'valid', 'exclusion_reason', 'next_decision',
    ]
    write_header = not csv_path.exists()
    with csv_path.open('a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            w.writeheader()
        w.writerow({
            'stage': eff['stage'],
            'run_id': eff['run_id'],
            'block': eff['block'],
            'method': f"fixed-r{eff['replicas']}-{eff['node']}" + (f"-tc{eff['tc_ms']}" if eff['tc_ms'] else ''),
            'revision': eff['revision'],
            'config_path': f"{eff['run_id']}/effective-config.json",
            'start_time': f"{summary['requests']['measure_start_time']:.3f}",
            'end_time': f"{summary['requests']['end_time']:.3f}",
            'offered': st['offered'],
            'completed': st['completed'],
            'failures': st['failures'],
            'acc_500ms': f"{st['acc_500']:.4f}",
            'acc_300ms': f"{st['acc_300']:.4f}",
            'succ_p50_ms': f"{st['succ_p50_ms']:.2f}" if st['succ_p50_ms'] is not None else '',
            'succ_p90_ms': f"{st['succ_p90_ms']:.2f}" if st['succ_p90_ms'] is not None else '',
            'succ_p95_ms': f"{st['succ_p95_ms']:.2f}" if st['succ_p95_ms'] is not None else '',
            'all_p90_ms': f"{st['all_p90_ms']:.2f}" if st['all_p90_ms'] is not None else '',
            'rps': f"{st['rps']:.2f}",
            'replicas': eff['replicas'],
            'cpu_mean_m': f"{rs['cpu_mean_m']:.1f}" if rs['cpu_mean_m'] is not None else '',
            'mem_max_mi': f"{rs['mem_max_mi']:.1f}" if rs['mem_max_mi'] is not None else '',
            'backend_counts': json.dumps(summary['backend_counts']),
            'valid': str(valid),
            'exclusion_reason': exclusion_reason,
            'next_decision': next_decision,
        })


def main():
    p = argparse.ArgumentParser(description='Fixed-replica calibration harness for G1/G2')
    p.add_argument('--campaign-dir', required=True)
    p.add_argument('--stage', required=True)
    p.add_argument('--block', type=int, required=True)
    p.add_argument('--run-id', required=True)
    p.add_argument('--replicas', type=int, required=True)
    p.add_argument('--node', required=True)
    p.add_argument('--schedule', default='6:0:6:0')
    p.add_argument('--duration', type=float, default=180.0)
    p.add_argument('--warmup', type=float, default=60.0)
    p.add_argument('--tc-ms', type=float, default=None)
    p.add_argument('--cooldown-after', type=float, default=16.0)
    args = p.parse_args()

    settings = yaml.safe_load((ROOT / 'experiments/campaign.yaml').read_text())
    cdir = Path(args.campaign_dir)
    cdir.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open('w') as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        summary = run_one(settings, cdir, args.stage, args.block, args.run_id,
                          args.replicas, args.node, args.schedule, args.duration, args.warmup, args.tc_ms)
        append_ledger(cdir / 'runs.csv', summary)
        if args.cooldown_after > 0:
            time.sleep(args.cooldown_after)


if __name__ == '__main__':
    main()
