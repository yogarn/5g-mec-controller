import json

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
    assert summary['steady']['offered'] == 3
    assert summary['steady']['completed'] == 2
    assert summary['steady']['failures'] == 1
    assert summary['steady']['acc_slo'] == pytest.approx(2 / 3)
    assert summary['steady']['server_timings']['server_lock_wait_ms']['mean_ms'] == pytest.approx(0.25)
    assert summary['steady_by_site']['gnb-1']['offered'] == 2
    assert summary['steady_by_site']['gnb-3']['completed'] == 1
