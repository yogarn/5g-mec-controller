import json
from pathlib import Path

import pytest
import yaml

from utils import fixed_replica_harness as harness


def test_pedestrian_requires_separate_config(tmp_path):
    with pytest.raises(SystemExit):
        harness.main([
            '--campaign-dir', str(tmp_path / 'b1'),
            '--stage', 'B1', '--block', '1', '--run-id', 'r1',
            '--replicas', '2', '--node', 'gnb-3',
            '--workload', 'pedestrian-detection', '--plan-only',
        ])


def test_pedestrian_plan_only_manifest_and_provenance(tmp_path):
    cdir = tmp_path / 'b1'
    harness.main([
        '--campaign-dir', str(cdir),
        '--stage', 'B1', '--block', '1', '--run-id', 'b1_tc_on',
        '--replicas', '2', '--node', 'gnb-3',
        '--workload', 'pedestrian-detection',
        '--config', 'experiments/campaign-pedestrian.example.yaml',
        '--slo-ms', '150', '--tc-ms', '10',
        '--schedule', '3:1:1:1', '--duration', '60', '--warmup', '15',
        '--probe-service-path', '--plan-only',
    ])
    run_dir = cdir / 'b1_tc_on'
    docs = list(yaml.safe_load_all((run_dir / 'manifest.yaml').read_text()))
    dep = next(d for d in docs if d and d.get('kind') == 'Deployment')
    svc = next(d for d in docs if d and d.get('kind') == 'Service')
    cfg = json.loads((run_dir / 'effective-config.json').read_text())

    assert dep['metadata']['name'] == 'mec-placement-pedestrian-detection'
    assert dep['spec']['replicas'] == 2
    assert dep['spec']['template']['spec']['nodeSelector'] == {'kubernetes.io/hostname': 'gnb-3'}
    assert svc['metadata']['name'] == 'mec-placement-pedestrian-detection'
    assert cfg['workload'] == 'pedestrian-detection'
    assert cfg['slo_ms'] == 150.0
    assert cfg['probe_service_path'] is True
    assert cfg['provenance']['workload_image']
    assert cfg['provenance']['core_git'] is not None


def test_summarize_requests_includes_server_timings_and_by_site(tmp_path):
    req_path = tmp_path / 'requests.jsonl'
    rows = [
        {'event': 'phase_start', 'phase': 'p1', 'start_time': 100.0},
        {'event': 'request', 'phase': 'p1', 'group': 'gnb-1', 'start_time': 120.0, 'duration_ms': 140.0,
         'success': True, 'server_processing_ms': 15.0, 'server_body_read_ms': 1.0,
         'server_decode_ms': 2.0, 'server_lock_wait_ms': 0.5, 'server_inference_ms': 11.5},
        {'event': 'request', 'phase': 'p1', 'group': 'gnb-3', 'start_time': 125.0, 'duration_ms': 30.0,
         'success': True, 'server_processing_ms': 14.0, 'server_body_read_ms': 1.0,
         'server_decode_ms': 2.0, 'server_lock_wait_ms': 0.0, 'server_inference_ms': 11.0},
        {'event': 'request', 'phase': 'p1', 'group': 'gnb-1', 'start_time': 130.0, 'duration_ms': 200.0,
         'success': False, 'server_processing_ms': None},
        {'event': 'phase_end', 'phase': 'p1', 'end_time': 160.0},
    ]
    req_path.write_text('\n'.join(json.dumps(r) for r in rows) + '\n')

    summary = harness.summarize_requests(req_path, warmup_s=10.0, slo_ms=150.0)
    assert summary['steady']['completed'] == 3
    assert summary['steady']['successful'] == 2
    assert summary['steady']['failures'] == 1
    assert summary['steady']['scheduled'] is None
    assert summary['steady']['acc_slo'] == pytest.approx(2 / 3)
    assert summary['steady']['server_timings']['server_lock_wait_ms']['mean_ms'] == pytest.approx(0.25)
    assert summary['steady_by_site']['gnb-1']['completed'] == 2
    assert summary['steady_by_site']['gnb-3']['successful'] == 1


def test_summarize_requests_accounts_scheduled_started_completed_successful_missed(tmp_path):
    req_path = tmp_path / 'requests.jsonl'
    rows = [
        {'event': 'phase_start', 'phase': 'p1', 'start_time': 100.0},
        {'event': 'request', 'phase': 'p1', 'group': 'gnb-1', 'start_time': 110.0, 'duration_ms': 50.0, 'success': True},
        {'event': 'request', 'phase': 'p1', 'group': 'gnb-1', 'start_time': 120.0, 'duration_ms': 200.0, 'success': False},
        {'event': 'phase_end', 'phase': 'p1', 'end_time': 160.0,
         'scheduled': 3, 'started': 2, 'completed': 2, 'attempts': 2, 'successes': 1, 'failures': 1, 'missed': 1},
    ]
    req_path.write_text('\n'.join(json.dumps(r) for r in rows) + '\n')

    summary = harness.summarize_requests(req_path, warmup_s=0.0, slo_ms=150.0)
    assert summary['full']['scheduled'] == 3
    assert summary['full']['started'] == 2
    assert summary['full']['completed'] == 2
    assert summary['full']['successful'] == 1
    assert summary['full']['failures'] == 1
    assert summary['full']['missed'] == 1
    assert summary['full']['acc_slo'] == pytest.approx(1 / 3)

    ledger = tmp_path / 'runs.csv'
    full_summary = {
        'effective': {'stage': 'B2', 'run_id': 'r1', 'block': 1, 'replicas': 1, 'node': 'gnb-3', 'tc_ms': 10, 'revision': 'abc'},
        'requests': summary,
        'resources': {'cpu_mean_m': 250.0, 'mem_max_mi': 100.0},
        'backend_counts': {'pod-1': 2},
    }
    harness.append_ledger(ledger, full_summary)
    import csv
    row, = list(csv.DictReader(ledger.open()))
    assert row['scheduled'] == '3'
    assert row['started'] == '2'
    assert row['completed'] == '2'
    assert row['successful'] == '1'
    assert row['failures'] == '1'
    assert row['missed'] == '1'


def test_probe_registered_service_paths_uses_registered_ues():
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from utils.experiment import probe_registered_service_paths

    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append((self.path, self.client_address[0]))
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    srv = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        site_groups = (
            ('gnb-1', [{'ip': '127.0.0.1', 'iface': 'uesimtun0'}]),
            ('gnb-2', []),
            ('gnb-3', [{'ip': '127.0.0.1', 'iface': 'uesimtun1'}]),
        )
        urls = {
            'gnb-1': f'http://127.0.0.1:{srv.server_port}/detect',
            'gnb-2': f'http://127.0.0.1:{srv.server_port}/detect',
            'gnb-3': f'http://127.0.0.1:{srv.server_port}/detect',
        }
        probes = probe_registered_service_paths(site_groups, urls, timeout=2.0, count=2)
        assert set(probes) == {'gnb-1', 'gnb-3'}
        assert probes['gnb-1']['successes'] == 2
        assert probes['gnb-1']['interface'] == 'uesimtun0'
        assert probes['gnb-1']['url'].endswith('/health')
        assert probes['gnb-3']['successes'] == 2
        assert all(path == '/health' for path, _ in seen)
    finally:
        srv.shutdown()
        t.join()
        srv.server_close()


def test_run_one_pedestrian_collects_summary_cadvisor_and_service_probes(tmp_path, monkeypatch):
    import hashlib
    import subprocess
    from types import SimpleNamespace

    cdir = tmp_path / 'b1'
    cdir.mkdir()

    req_lines = '\n'.join([
        json.dumps({'event': 'phase_start', 'phase': 'phase_01', 'workload_ue': 3, 'gnb1_ues': 1, 'gnb2_ues': 1, 'gnb3_ues': 1,
                    'start_time': 1000.0, 'duration_s': 60.0, 'ues': [
                        {'group': 'gnb-1', 'source_address': '10.1.0.1', 'interface': 'uesimtun0'},
                        {'group': 'gnb-2', 'source_address': '10.1.0.2', 'interface': 'uesimtun1'},
                        {'group': 'gnb-3', 'source_address': '10.1.0.3', 'interface': 'uesimtun2'},
                    ]}),
        json.dumps({'event': 'request', 'phase': 'phase_01', 'group': 'gnb-1', 'source_address': '10.1.0.1', 'attempt': 1,
                    'url': 'http://10.34.211.157:31000/detect', 'method': 'POST',
                    'start_time': 1010.0, 'end_time': 1010.1, 'duration_ms': 100.0, 'http_status': 200, 'error': None, 'success': True}),
        json.dumps({'event': 'phase_end', 'phase': 'phase_01', 'workload_ue': 3, 'gnb1_ues': 1, 'gnb2_ues': 1, 'gnb3_ues': 1,
                    'start_time': 1000.0, 'generation_duration_s': 60.0, 'end_time': 1060.0,
                    'attempts': 1, 'successes': 1, 'failures': 0,
                    'scheduled': 1, 'started': 1, 'completed': 1, 'missed': 0,
                    'per_ue': [
                        {'group': 'gnb-1', 'source_address': '10.1.0.1', 'attempts': 1, 'successes': 1, 'scheduled': 1, 'started': 1, 'completed': 1, 'missed': 0},
                        {'group': 'gnb-2', 'source_address': '10.1.0.2', 'attempts': 0, 'successes': 0, 'scheduled': 0, 'started': 0, 'completed': 0, 'missed': 0},
                        {'group': 'gnb-3', 'source_address': '10.1.0.3', 'attempts': 0, 'successes': 0, 'scheduled': 0, 'started': 0, 'completed': 0, 'missed': 0},
                    ]}),
    ]) + '\n'
    req_bytes = req_lines.encode('utf-8')
    req_hash = hashlib.sha256(req_bytes).hexdigest()
    probes_bytes = json.dumps({'phase_01': {'gnb-1': {'successes': 5, 'median_ms': 12.0}}}).encode('utf-8')

    class DummyProc:
        def poll(self): return None
        def terminate(self): pass
        def wait(self, timeout=None): return 0

    def fake_kube(*args, **kwargs):
        if args[:3] == ('-n', 'services', 'get') and args[3] == 'service':
            return SimpleNamespace(stdout=json.dumps({'spec': {'ports': [{'nodePort': 31000}]}}))
        if args[:3] == ('-n', 'services', 'get') and args[3] == 'pods':
            return SimpleNamespace(stdout=json.dumps({'items': [{'metadata': {'name': 'mec-placement-pedestrian-detection-p1'}}]}))
        return SimpleNamespace(stdout='')

    seen_ue_cmds = []

    def fake_run(cmd, **kwargs):
        joined = ' '.join(map(str, cmd))
        if 'utils.experiment' in joined:
            seen_ue_cmds.append(cmd)
        if 'cat' in joined and 'requests.jsonl' in joined:
            kwargs['stdout'].write(req_bytes)
        elif 'cat' in joined and 'service-path-probes.json' in joined:
            kwargs['stdout'].write(probes_bytes)
        return SimpleNamespace(returncode=0, stdout='')

    def fake_check_output(cmd, **kwargs):
        joined = ' '.join(map(str, cmd))
        if 'sha256sum' in joined:
            return f'{req_hash}  requests.jsonl\n'
        if 'logs' in joined:
            return 'POST /detect HTTP/1.1 200\n'
        if 'cpu.stat' in joined:
            return 'nr_periods 10\nnr_throttled 0\n'
        if 'memory.peak' in joined:
            return '1024\n'
        return ''

    monkeypatch.setattr(harness, 'kube', fake_kube)
    monkeypatch.setattr(harness, 'stop_process', lambda p: None)
    monkeypatch.setattr(harness, '_collect_live_provenance', lambda s: {'ue_git': {'revision': 'abc', 'dirty': False}})
    def fake_popen(cmd, **kwargs):
        for arg in cmd:
            if str(arg).endswith('resources.jsonl'):
                Path(arg).write_text('')
        return DummyProc()

    monkeypatch.setattr(harness.subprocess, 'Popen', fake_popen)
    monkeypatch.setattr(harness.subprocess, 'run', fake_run)
    monkeypatch.setattr(harness.subprocess, 'check_output', fake_check_output)

    seen_prom_urls = []
    def fake_export(requests_path, output_dir, prometheus_url=None, **kw):
        seen_prom_urls.append(prometheus_url)
        p = output_dir / 'resources-prometheus.json'
        p.write_text(json.dumps({
            'pod_prefix': kw['pod_prefix'],
            'step_seconds': 5.0,
            'results': {'cpu_seconds_total': [], 'memory_working_set_bytes': [], 'scrape_up': []},
        }))
        return p

    monkeypatch.setattr(harness, 'export_prometheus_resources', fake_export)

    # Load pedestrian example config which intentionally has NO prometheus_url.
    settings = yaml.safe_load((harness.ROOT / 'experiments/campaign-pedestrian.example.yaml').read_text())
    assert 'prometheus_url' not in settings

    summary = harness.run_one(
        settings, cdir, 'B1', 1, 'b1_test', 2, 'gnb-3', '3:1:1:1', 60.0, 0.0, None,
        workload='pedestrian-detection', slo_ms=150.0, probe_service_path=True,
    )
    run_dir = cdir / 'b1_test'
    assert (run_dir / 'summary.csv').exists()
    assert (run_dir / 'resource-by-ramp.csv').exists()
    assert (run_dir / 'service-path-probes.json').exists()
    assert summary['cadvisor_summary_csv'] == str(run_dir / 'resource-by-ramp.csv')
    assert summary['service_path_probes']['phase_01']['gnb-1']['median_ms'] == 12.0
    assert seen_prom_urls and seen_prom_urls[0]
    assert any('--probe-service-path' in ' '.join(map(str, c)) for c in seen_ue_cmds)
