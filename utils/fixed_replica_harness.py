#!/usr/bin/env python3
"""Fixed-replica calibration harness for G1/G2 (reuses UE lifecycle, sampler, and TrafficDelay)."""
import argparse
import csv
import fcntl
import hashlib
import json
import math
import subprocess
import sys
import time
from pathlib import Path

import yaml

from experiments.campaign import (
    NAMESPACE,
    _collect_live_provenance,
    _local_git_provenance,
    _workload_image_ref,
    check_service_routing,
    kube,
    remote,
    stop_process,
    workload_identity,
    workload_manifest,
)
from utils.cadvisor_resources import DEFAULT_PROMETHEUS_URL, export_prometheus_resources, summarize_cadvisor
from utils.experiment import DEFAULT_MAX_IN_FLIGHT_PER_UE, DEFAULT_RPS_PER_UE
from utils.summarize import main as summarize_main
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


def build_manifest(replicas, node, use_tc, workload='face-detection'):
    docs = list(yaml.safe_load_all(workload_manifest(workload).read_text()))
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


def cleanup_app(app_name):
    kube('-n', NAMESPACE, 'delete', 'deployment', app_name, '--ignore-not-found', '--wait=true', '--timeout=120s')
    kube('-n', NAMESPACE, 'delete', 'service', app_name, '--ignore-not-found', '--wait=true', '--timeout=120s')
    kube('-n', NAMESPACE, 'wait', '--for=delete', 'pod', '-l', f'app={app_name}', '--timeout=120s')


def summarize_requests(jsonl_path, warmup_s, slo_ms=500.0):
    rows = [json.loads(line) for line in jsonl_path.read_text().splitlines() if line.strip()]
    starts = [r for r in rows if r.get('event') == 'phase_start']
    ends = [r for r in rows if r.get('event') == 'phase_end']
    reqs = [r for r in rows if r.get('event') == 'request']
    if not starts or not ends or len(starts) != len(ends):
        raise ValueError('missing or mismatched phase_start / phase_end')
    t0 = starts[0]['start_time']
    t_measure_start = t0 + warmup_s
    t_end = ends[-1]['end_time']

    def _timing_summary(subset, key):
        vals = sorted(r[key] for r in subset if isinstance(r.get(key), (int, float)) and not isinstance(r.get(key), bool))
        if not vals:
            return {'count': 0, 'mean_ms': None, 'p50_ms': None, 'p90_ms': None}
        return {
            'count': len(vals),
            'mean_ms': sum(vals) / len(vals),
            'p50_ms': percentile(vals, 0.50),
            'p90_ms': percentile(vals, 0.90),
        }

    def stats(subset, gen_dur_s, drain_dur_s=0.0, scheduled=None, started=None, missed=None):
        completed = len(subset)
        succ = [r for r in subset if r.get('success')]
        successful = len(succ)
        fail = completed - successful
        denom = scheduled if scheduled is not None else completed
        succ_durs = sorted(r['duration_ms'] for r in succ)
        all_durs = sorted(r['duration_ms'] for r in subset)
        acc_slo = sum(1 for r in succ if r['duration_ms'] <= slo_ms) / denom if denom else 0.0
        acc_500 = sum(1 for r in succ if r['duration_ms'] <= 500.0) / denom if denom else 0.0
        acc_300 = sum(1 for r in succ if r['duration_ms'] <= 300.0) / denom if denom else 0.0
        return {
            'scheduled': scheduled,
            'started': started,
            'completed': completed,
            'successful': successful,
            'failures': fail,
            'missed': missed,
            'generation_duration_s': gen_dur_s,
            'drain_duration_s': drain_dur_s,
            'error_rate': fail / completed if completed else 0.0,
            'offered_rps': scheduled / gen_dur_s if (scheduled is not None and gen_dur_s > 0) else None,
            'started_rps': started / gen_dur_s if (started is not None and gen_dur_s > 0) else None,
            'completed_rps': completed / gen_dur_s if gen_dur_s > 0 else 0.0,
            'rps': successful / gen_dur_s if gen_dur_s > 0 else 0.0,
            'slo_ms': float(slo_ms),
            'acc_slo': acc_slo,
            'viol_slo': 1.0 - acc_slo,
            'acc_500': acc_500,
            'viol_500': 1.0 - acc_500,
            'acc_300': acc_300,
            'viol_300': 1.0 - acc_300,
            'succ_p50_ms': percentile(succ_durs, 0.50),
            'succ_p90_ms': percentile(succ_durs, 0.90),
            'succ_p95_ms': percentile(succ_durs, 0.95),
            'all_p90_ms': percentile(all_durs, 0.90),
            'server_timings': {
                field: _timing_summary(succ, field)
                for field in (
                    'server_processing_ms',
                    'server_body_read_ms',
                    'server_decode_ms',
                    'server_lock_wait_ms',
                    'server_inference_ms',
                )
            },
        }

    ends_by_phase = {e['phase']: e for e in ends}
    full_gen_dur = 0.0
    full_drain_dur = 0.0
    steady_gen_dur = 0.0
    for st_rec in starts:
        end_rec = ends_by_phase[st_rec['phase']]
        elapsed_s = max(0.0, float(end_rec['end_time']) - float(st_rec['start_time']))
        gen_s = float(end_rec.get('generation_duration_s', st_rec.get('duration_s', elapsed_s)))
        drain_s = max(0.0, elapsed_s - gen_s)
        full_gen_dur += gen_s
        full_drain_dur += drain_s
        lower = max(0.0, t_measure_start - float(st_rec['start_time']))
        steady_gen_dur += max(0.0, gen_s - lower)

    if all(e.get('scheduled') is not None for e in ends):
        full_scheduled = sum(int(e['scheduled']) for e in ends)
        full_started = sum(int(e.get('started', e['scheduled'])) for e in ends)
        full_missed = sum(int(e['missed']) for e in ends)
    else:
        full_scheduled = len(reqs)
        full_started = len(reqs)
        full_missed = 0

    full_stats = stats(
        reqs, max(1.0, full_gen_dur), drain_dur_s=full_drain_dur,
        scheduled=full_scheduled, started=full_started, missed=full_missed,
    )

    if warmup_s <= 0:
        steady = list(reqs)
        steady_scheduled = full_scheduled
        steady_started = full_started
        steady_missed = full_missed
    elif all('scheduled_offset_s' in r for r in reqs) and all(s.get('rps_per_ue') and s.get('ues') for s in starts):
        steady = []
        steady_scheduled = 0
        for st_rec in starts:
            phase = st_rec['phase']
            end_rec = ends_by_phase[phase]
            gen_s = float(end_rec.get('generation_duration_s', st_rec['duration_s']))
            lower = max(0.0, t_measure_start - float(st_rec['start_time']))
            if lower >= gen_s:
                continue
            period = 1.0 / float(st_rec['rps_per_ue'])
            ues = st_rec['ues']
            for pos in range(len(ues)):
                offset = pos / len(ues) * period
                first = max(0, math.ceil((lower - offset) / period - 1e-9))
                stop = max(0, math.ceil((gen_s - offset) / period - 1e-9))
                steady_scheduled += max(0, stop - first)
            steady.extend(
                r for r in reqs
                if r.get('phase') == phase and lower <= float(r['scheduled_offset_s']) < gen_s
            )
        steady_started = len(steady) if full_started == len(reqs) else None
        steady_missed = max(0, steady_scheduled - len(steady)) if full_started == len(reqs) else None
    else:
        steady = [r for r in reqs if r['start_time'] >= t_measure_start]
        steady_scheduled = None
        steady_started = None
        steady_missed = None

    steady_stats = stats(
        steady, max(1.0, steady_gen_dur), drain_dur_s=full_drain_dur,
        scheduled=steady_scheduled, started=steady_started, missed=steady_missed,
    )
    by_site = {
        site: stats([r for r in steady if r.get('group') == site], max(1.0, steady_gen_dur), drain_dur_s=full_drain_dur)
        for site in sorted({r.get('group') for r in steady if r.get('group')})
    }
    return {
        'start_time': t0,
        'measure_start_time': t_measure_start,
        'end_time': t_end,
        'steady': steady_stats,
        'full': full_stats,
        'steady_by_site': by_site,
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


def _default_prometheus_url():
    cfg_path = ROOT / 'config.yaml'
    if cfg_path.exists():
        data = yaml.safe_load(cfg_path.read_text()) or {}
        if data.get('prometheus_url'):
            return data['prometheus_url']
    return DEFAULT_PROMETHEUS_URL


def run_one(settings, campaign_dir, stage, block, run_id, replicas, node, schedule, duration, warmup, tc_ms,
            rps_per_ue=DEFAULT_RPS_PER_UE, max_in_flight_per_ue=DEFAULT_MAX_IN_FLIGHT_PER_UE,
            workload='face-detection', slo_ms=500.0, probe_service_path=False, plan_only=False,
            prometheus_url=None):
    run_dir = campaign_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    app_name = workload_identity(workload)
    use_tc = tc_ms is not None and tc_ms > 0
    manifest_yaml = build_manifest(replicas, node, use_tc, workload=workload)
    (run_dir / 'manifest.yaml').write_text(manifest_yaml)

    provenance = {
        'core_git': _local_git_provenance(),
        'ue_git': None,
        'kubernetes_server_version': None,
        'kube_proxy_mode': None,
        'workload_image': _workload_image_ref(workload),
    }
    rev = (provenance['core_git'] or {}).get('revision')
    eff_cfg = {
        'stage': stage, 'block': block, 'run_id': run_id, 'revision': rev,
        'workload': workload, 'app_name': app_name, 'slo_ms': float(slo_ms),
        'replicas': replicas, 'node': node, 'schedule': schedule,
        'duration_s': duration, 'warmup_s': warmup, 'measure_s': duration - warmup,
        'rps_per_ue': float(rps_per_ue), 'max_in_flight_per_ue': int(max_in_flight_per_ue),
        'tc_ms': tc_ms, 'namespace': NAMESPACE,
        'probe_service_path': bool(probe_service_path),
        'provenance': provenance,
    }
    if plan_only:
        (run_dir / 'effective-config.json').write_text(json.dumps(eff_cfg, indent=2) + '\n')
        return {'effective': eff_cfg, 'plan_only': True}

    pod_logs_dir = run_dir / 'pod-logs'
    pod_logs_dir.mkdir()
    provenance.update(_collect_live_provenance(settings))
    eff_cfg['provenance'] = provenance
    (run_dir / 'effective-config.json').write_text(json.dumps(eff_cfg, indent=2) + '\n')

    cleanup_app(app_name)
    tc = TrafficDelay(tc_ms) if use_tc else None
    sampler = None
    try:
        if tc:
            tc.start()
        subprocess.run(['kubectl', 'apply', '-f', str(run_dir / 'manifest.yaml')], check=True)
        kube('-n', NAMESPACE, 'rollout', 'status', f'deployment/{app_name}', '--timeout=180s')

        svc = json.loads(kube('-n', NAMESPACE, 'get', 'service', app_name, '-o', 'json', capture_output=True, text=True).stdout)
        nodeport = svc['spec']['ports'][0]['nodePort']
        pods_json = json.loads(kube('-n', NAMESPACE, 'get', 'pods', '-l', f'app={app_name}', '-o', 'json', capture_output=True, text=True).stdout)
        pods_json['items'] = [p for p in pods_json['items'] if not p['metadata'].get('deletionTimestamp')]
        (run_dir / 'pods-ready.json').write_text(json.dumps(pods_json, indent=2) + '\n')

        site_urls = {site: f'http://{ip}:{nodeport}/detect' for site, ip in settings['site_node_ips'].items()}
        res_file = run_dir / 'resources.jsonl'
        sampler = subprocess.Popen(
            [sys.executable, '-m', 'utils.monitor_resources', 'sample', str(res_file), '--namespace', NAMESPACE, '--app', app_name],
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
            '--rps-per-ue', str(float(rps_per_ue)),
            '--max-in-flight-per-ue', str(int(max_in_flight_per_ue)),
            '--telemetry-url', settings['telemetry_url'],
            '--telemetry-token-file', settings['token'],
            '--output-dir', ue_out,
        ]
        if probe_service_path:
            ue_cmd.append('--probe-service-path')
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

        service_path_probes = None
        if probe_service_path:
            probes_dest = run_dir / 'service-path-probes.json'
            with probes_dest.open('wb') as out_f:
                subprocess.run(remote(settings, ['sudo', '-n', 'cat', f'{ue_out}/service-path-probes.json']), stdout=out_f, check=True)
            service_path_probes = json.loads(probes_dest.read_text())

        # Collect per-pod logs, cgroup stats, and observed serving counts from requests.jsonl.
        backend_counts = {pod['metadata']['name']: 0 for pod in pods_json['items']}
        for line in raw_dest.read_text().splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            if rec.get('event') == 'request':
                pod_id = rec.get('server_pod') or 'unknown'
                backend_counts[pod_id] = backend_counts.get(pod_id, 0) + 1
        cgroup_stats = {}
        for pod in pods_json['items']:
            pname = pod['metadata']['name']
            plog = subprocess.check_output(['kubectl', '-n', NAMESPACE, 'logs', pname], text=True)
            (pod_logs_dir / f'{pname}.log').write_text(plog)
            try:
                cstat = subprocess.check_output(['kubectl', '-n', NAMESPACE, 'exec', pname, '--', 'cat', '/sys/fs/cgroup/cpu.stat'], text=True)
                mpeak = subprocess.check_output(['kubectl', '-n', NAMESPACE, 'exec', pname, '--', 'cat', '/sys/fs/cgroup/memory.peak'], text=True).strip()
                cgroup_stats[pname] = {'cpu_stat': dict(l.split() for l in cstat.strip().splitlines()), 'memory_peak_bytes': int(mpeak)}
            except subprocess.SubprocessError:
                pass
        (run_dir / 'backend-counts.json').write_text(json.dumps(backend_counts, indent=2) + '\n')
        (run_dir / 'cgroup-stats.json').write_text(json.dumps(cgroup_stats, indent=2) + '\n')

        summarize_main([str(raw_dest), '--slo-ms', str(slo_ms)])
        prom_url = prometheus_url or settings.get('prometheus_url') or _default_prometheus_url()
        prom_json = export_prometheus_resources(
            raw_dest, run_dir, prometheus_url=prom_url,
            namespace=NAMESPACE, pod_prefix=f'{app_name}-',
        )
        cadvisor_summary = str(summarize_cadvisor(
            prom_json, raw_dest, run_dir / 'summary.csv', run_dir,
            metrics_jsonl_path=res_file,
        ))

        req_sum = summarize_requests(raw_dest, warmup, slo_ms=slo_ms)
        res_sum = summarize_resources(res_file, req_sum['measure_start_time'], req_sum['end_time'])
        summary = {'effective': eff_cfg, 'requests': req_sum, 'resources': res_sum,
                   'cadvisor_summary_csv': cadvisor_summary,
                   'service_path_probes': service_path_probes,
                   'backend_counts': backend_counts, 'cgroup': cgroup_stats}
        (run_dir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        return summary
    finally:
        stop_process(sampler)
        cleanup_app(app_name)
        if tc:
            tc.close()


def append_ledger(csv_path, summary, valid=True, exclusion_reason='', next_decision=''):
    eff = summary['effective']
    st = summary['requests']['steady']
    rs = summary['resources']
    fields = [
        'stage', 'run_id', 'block', 'method', 'revision', 'config_path',
        'start_time', 'end_time', 'generation_duration_s', 'drain_duration_s',
        'scheduled', 'started', 'completed', 'successful', 'failures', 'missed',
        'acc_slo', 'acc_500ms', 'acc_300ms', 'succ_p50_ms', 'succ_p90_ms', 'succ_p95_ms', 'all_p90_ms',
        'offered_rps', 'rps', 'replicas', 'cpu_mean_m', 'mem_max_mi', 'backend_counts',
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
            'generation_duration_s': f"{st['generation_duration_s']:.3f}",
            'drain_duration_s': f"{st['drain_duration_s']:.3f}",
            'scheduled': '' if st['scheduled'] is None else st['scheduled'],
            'started': '' if st['started'] is None else st['started'],
            'completed': st['completed'],
            'successful': st['successful'],
            'failures': st['failures'],
            'missed': '' if st['missed'] is None else st['missed'],
            'acc_slo': f"{st['acc_slo']:.4f}",
            'acc_500ms': f"{st['acc_500']:.4f}",
            'acc_300ms': f"{st['acc_300']:.4f}",
            'succ_p50_ms': f"{st['succ_p50_ms']:.2f}" if st['succ_p50_ms'] is not None else '',
            'succ_p90_ms': f"{st['succ_p90_ms']:.2f}" if st['succ_p90_ms'] is not None else '',
            'succ_p95_ms': f"{st['succ_p95_ms']:.2f}" if st['succ_p95_ms'] is not None else '',
            'all_p90_ms': f"{st['all_p90_ms']:.2f}" if st['all_p90_ms'] is not None else '',
            'offered_rps': f"{st['offered_rps']:.2f}" if st['offered_rps'] is not None else '',
            'rps': f"{st['rps']:.2f}",
            'replicas': eff['replicas'],
            'cpu_mean_m': f"{rs['cpu_mean_m']:.1f}" if rs['cpu_mean_m'] is not None else '',
            'mem_max_mi': f"{rs['mem_max_mi']:.1f}" if rs['mem_max_mi'] is not None else '',
            'backend_counts': json.dumps(summary['backend_counts']),
            'valid': str(valid),
            'exclusion_reason': exclusion_reason,
            'next_decision': next_decision,
        })


def main(argv=None):
    p = argparse.ArgumentParser(description='Fixed-replica diagnostic/calibration harness for B1/B2/G1/G2')
    p.add_argument('--campaign-dir', required=True)
    p.add_argument('--stage', required=True)
    p.add_argument('--block', type=int, required=True)
    p.add_argument('--run-id', required=True)
    p.add_argument('--replicas', type=int, required=True)
    p.add_argument('--node', required=True)
    p.add_argument('--workload', choices=('face-detection', 'pedestrian-detection'), default='face-detection')
    p.add_argument('--config', default=str(ROOT / 'experiments/campaign.yaml'))
    p.add_argument('--slo-ms', type=float, default=500.0)
    p.add_argument('--schedule', default='6:0:6:0')
    p.add_argument('--duration', type=float, default=180.0)
    p.add_argument('--warmup', type=float, default=60.0)
    p.add_argument('--tc-ms', type=float, default=None)
    p.add_argument('--rps-per-ue', type=float, default=DEFAULT_RPS_PER_UE)
    p.add_argument('--max-in-flight-per-ue', type=int, default=DEFAULT_MAX_IN_FLIGHT_PER_UE)
    p.add_argument('--cooldown-after', type=float, default=16.0)
    p.add_argument('--probe-service-path', action='store_true',
                   help='run UE-bound HTTP /health probes over the NodePort service path per site')
    p.add_argument('--prometheus-url', default=None,
                   help='Prometheus URL for cAdvisor/throttling export (default: config prometheus_url or config.yaml)')
    p.add_argument('--plan-only', action='store_true',
                   help='write manifest and effective-config.json without cluster access or traffic')
    args = p.parse_args(argv)
    if args.replicas < 1:
        p.error('--replicas must be a positive integer')
    if not math.isfinite(args.slo_ms) or args.slo_ms <= 0:
        p.error('--slo-ms must be finite and positive')
    if not math.isfinite(args.rps_per_ue) or args.rps_per_ue <= 0:
        p.error('--rps-per-ue must be finite and positive')
    if args.max_in_flight_per_ue < 1:
        p.error('--max-in-flight-per-ue must be a positive integer')
    if args.workload == 'pedestrian-detection' and Path(args.config).resolve() == (ROOT / 'experiments/campaign.yaml').resolve():
        p.error('pedestrian-detection requires a separate --config (e.g. experiments/campaign-pedestrian.example.yaml)')

    settings = yaml.safe_load(Path(args.config).read_text())
    cdir = Path(args.campaign_dir)
    cdir.mkdir(parents=True, exist_ok=True)
    if args.plan_only:
        run_one(settings, cdir, args.stage, args.block, args.run_id,
                args.replicas, args.node, args.schedule, args.duration, args.warmup, args.tc_ms,
                args.rps_per_ue, args.max_in_flight_per_ue,
                workload=args.workload, slo_ms=args.slo_ms,
                probe_service_path=args.probe_service_path, plan_only=True)
        return
    with LOCK_PATH.open('w') as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        summary = run_one(settings, cdir, args.stage, args.block, args.run_id,
                          args.replicas, args.node, args.schedule, args.duration, args.warmup, args.tc_ms,
                          args.rps_per_ue, args.max_in_flight_per_ue,
                          workload=args.workload, slo_ms=args.slo_ms,
                          probe_service_path=args.probe_service_path,
                          prometheus_url=args.prometheus_url)
        append_ledger(cdir / 'runs.csv', summary)
        if args.cooldown_after > 0:
            time.sleep(args.cooldown_after)


if __name__ == '__main__':
    main()
