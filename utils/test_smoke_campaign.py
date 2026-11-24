import json
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest
import yaml

from experiments import campaign
from utils import smoke_campaign as smoke


@pytest.mark.parametrize('profile,seconds,comparison,minimum,maximum', [
    ('mobility', 240, 'full', 3, 3),
    ('scaling', 360, 'scaling', 3, 12),
    ('joint', 480, 'full', 3, 12),
])
def test_offline_profiles_render_real_campaigns_without_subprocess(tmp_path, monkeypatch,
                                                                 profile, seconds, comparison, minimum, maximum):
    monkeypatch.setattr(smoke, 'supervise', lambda *a, **kw: pytest.fail('live process started'))
    monkeypatch.setattr(campaign, 'prerequisites', lambda *a, **kw: pytest.fail('cluster accessed'))
    out = tmp_path / profile
    assert smoke.main([profile, 'smoke_' + profile, '--plan-only', str(out)]) == 0
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    metadata = json.loads((out / 'smoke-plan.json').read_text())
    assert metadata['traffic_seconds_total'] == seconds
    assert seconds < smoke.STOP_AFTER_SECONDS < smoke.WALL_BUDGET_SECONDS <= 900
    assert plan['comparison'] == comparison
    assert plan['schedule'] == smoke.PROFILES[profile]['schedule']
    assert all(d == 60 for d in plan['phase_durations_s'])
    assert plan['phase_transition'] == 'continuous'
    assert plan['replica_bounds'] == {'min': minimum, 'max': maximum}
    assert plan['slo_ms'] == 150 and plan['tc_ms'] == 10
    assert plan['pacing'] == {'rps_per_ue': 5}
    assert plan['latency_floor_ms'] == 120
    for method in ['baseline', 'proposed']:
        hpa = yaml.safe_load((out / f'{method}-hpa.yaml').read_text())
        assert hpa['spec']['minReplicas'] == minimum
        assert hpa['spec']['maxReplicas'] == maximum
        dep = next(d for d in yaml.safe_load_all((out / f'{method}-workload.yaml').read_text())
                   if d and d['kind'] == 'Deployment')
        assert dep['spec']['replicas'] == minimum
        assert dep['spec']['template']['spec']['schedulerName'] == (
            'mec-placement-scheduler' if method == 'proposed' and profile != 'scaling' else 'default-scheduler')


def test_explicit_bounds_and_repeated_plan_does_not_overwrite(tmp_path):
    out = tmp_path / 'plan'
    smoke.main(['scaling', 'short_test', '--min-replicas', '2', '--max-replicas', '6',
                '--latency-floor-ms', '0', '--plan-only', str(out)])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    assert plan['replica_bounds'] == {'min': 2, 'max': 6}
    assert plan['latency_floor_ms'] == 0
    original = (out / 'plan.yaml').read_bytes()
    with pytest.raises(FileExistsError):
        smoke.main(['scaling', 'short_test', '--plan-only', str(out)])
    assert (out / 'plan.yaml').read_bytes() == original


def test_custom_mobility_schedule_respects_reverse_direction(tmp_path):
    out = tmp_path / 'reverse'
    schedule = smoke.PROFILES['mobility']['schedule']
    campaign.main(['--scenario', 'mobility9', '--phase-schedule', schedule,
                   '--mobility-direction', 'reverse', '--fixed-replicas', '3',
                   '--plan-only', str(out)])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    assert plan['schedule'].split(',') == list(reversed(schedule.split(',')))


def test_invalid_custom_schedule_rejected_before_plan_or_cluster(tmp_path, monkeypatch):
    monkeypatch.setattr(campaign, 'prerequisites', lambda *a, **kw: pytest.fail('cluster accessed'))
    out = tmp_path / 'invalid'
    with pytest.raises(ValueError):
        campaign.main(['--scenario', 'scaler-staircase', '--phase-schedule', '3:1:1:2',
                       '--plan-only', str(out)])
    assert not out.exists()


@pytest.mark.parametrize('flags', [
    ['scaling', '../bad'],
    ['scaling', 'test', '--min-replicas', '4', '--max-replicas', '3'],
    ['scaling', 'test', '--replicas', '5'],
    ['mobility', 'test', '--min-replicas', '3'],
    ['joint', 'test', '--latency-floor-ms', '-1'],
    ['scaling', 'test', '--slo-ms', '100'],  # Do not silently change the floor formula.
    ['mobility', 'test', '--rps-per-ue', 'nan'],
])
def test_invalid_smoke_options_fail_before_execution(tmp_path, monkeypatch, flags):
    monkeypatch.setattr(smoke, 'supervise', lambda *a, **kw: pytest.fail('live process started'))
    with pytest.raises(SystemExit):
        smoke.main([*flags, '--plan-only', str(tmp_path / 'invalid')])
    assert not (tmp_path / 'invalid').exists()


class FakeProcess:
    pid = 123
    def __init__(self, responses):
        self.responses = iter(responses)
        self.signals = []
        self.code = None
    def wait(self, timeout=None):
        answer = next(self.responses)
        if isinstance(answer, BaseException):
            raise answer
        self.code = answer
        return answer
    def send_signal(self, sig):
        self.signals.append(sig)
    def poll(self):
        return self.code


def test_watchdog_requests_single_graceful_stop_and_retains_partial_files(tmp_path):
    status = tmp_path / 'status.json'
    plots = tmp_path / 'plots'
    plots.mkdir()
    (plots / 'response-time.png').write_bytes(b'existing partial output')
    process = FakeProcess([subprocess.TimeoutExpired('fake', .25),
                           subprocess.TimeoutExpired('fake', .25), 130])
    times = iter([0, 780, 905, 906])
    calls = []
    def start(command, **kwargs):
        calls.append(kwargs)
        return process
    code = smoke.supervise(['fake'], status, {'plots_directory': str(plots)},
                           popen=start, monotonic=lambda: next(times))
    assert code == 124
    assert process.signals == [signal.SIGINT]
    assert calls[0]['start_new_session'] is True
    result = json.loads(status.read_text())
    assert result['status'] == 'timed_out' and result['plots_complete'] is False
    assert (plots / 'response-time.png').read_bytes() == b'existing partial output'


def test_success_requires_new_detailed_plots(tmp_path):
    plots = tmp_path / 'plots'
    plots.mkdir()
    for name in ['response-time', 'resource-usage', 'pod-placement', 'slo-violations']:
        (plots / f'{name}.png').touch()
    for name, header, values in [
        ('window-metrics-15s.csv', 'method,p90_ms', '150'),
        ('pod-placement-15s.csv', 'method,ready_count', '3'),
        ('resource-windows-15s.csv', 'method,cpu_millicores,memory_mib', '300,256'),
    ]:
        (plots / name).write_text(header + '\n' + '\n'.join(f'{method},{values}' for method in ('baseline', 'proposed')) + '\n')
    process = FakeProcess([0])
    status = tmp_path / 'status.json'
    code = smoke.supervise(['fake'], status, {'plots_directory': str(plots)},
                           popen=lambda *a, **kw: process)
    assert code == 0 and json.loads(status.read_text())['plots_complete'] is True


def test_png_files_without_resource_measurements_do_not_pass(tmp_path):
    plots = tmp_path / 'plots'
    plots.mkdir()
    for name in ['response-time', 'resource-usage', 'pod-placement', 'slo-violations']:
        (plots / f'{name}.png').touch()
    (plots / 'resource-windows-15s.csv').write_text('method,cpu_millicores,memory_mib\nbaseline,,\nproposed,,\n')
    pngs, counts, errors = smoke.inspect_outputs(plots)
    assert len(pngs) == 4
    assert any('no finite cpu_millicores' in error for error in errors)
    assert counts['resource-windows-15s.csv:baseline:cpu_millicores'] == 0


def test_ctrl_c_is_forwarded_once_and_cleanup_is_awaited(tmp_path):
    process = FakeProcess([KeyboardInterrupt(), subprocess.TimeoutExpired('fake', .25), 130])
    status = tmp_path / 'status.json'
    code = smoke.supervise(['fake'], status, {'plots_directory': str(tmp_path / 'plots')},
                           popen=lambda *a, **kw: process)
    assert code == 130 and process.signals == [signal.SIGINT]
    assert json.loads(status.read_text())['status'] == 'interrupted'


def test_real_local_child_receives_stop_and_finishes_cleanup(tmp_path):
    ready, cleaned = tmp_path / 'ready', tmp_path / 'cleaned'
    script = f'''
from pathlib import Path
import signal, time
def stop(signum, frame):
    time.sleep(0.05)
    Path({str(cleaned)!r}).write_text('cleanup complete')
    raise SystemExit(0)
signal.signal(signal.SIGINT, stop)
Path({str(ready)!r}).touch()
while True:
    time.sleep(0.02)
'''
    def start(command, **kwargs):
        process = subprocess.Popen(command, **kwargs)
        deadline = time.monotonic() + 3
        while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(.01)
        return process
    old_handler = signal.getsignal(signal.SIGTERM)
    status = tmp_path / 'status.json'
    assert smoke.supervise([sys.executable, '-c', script], status,
                           {'plots_directory': str(tmp_path / 'plots')},
                           stop_after=.1, popen=start) == 124
    assert cleaned.read_text() == 'cleanup complete'
    assert json.loads(status.read_text())['status'] == 'timed_out'
    assert signal.getsignal(signal.SIGTERM) == old_handler


def test_existing_result_or_status_is_never_reused(tmp_path, monkeypatch):
    monkeypatch.setattr(campaign, 'ROOT', tmp_path)
    config = tmp_path / 'config.yaml'
    config.touch()
    root = tmp_path / 'results'
    (root / 'old').mkdir(parents=True)
    with pytest.raises(SystemExit):
        smoke.main(['scaling', 'old', '--config', str(config)])
    (root / 'smoke-status').mkdir()
    status = root / 'smoke-status' / 'reserved.json'
    status.write_text('keep this')
    with pytest.raises(SystemExit):
        smoke.main(['joint', 'reserved', '--config', str(config)])
    assert status.read_text() == 'keep this'


def test_smoke_inspects_new_csv_directory_without_requiring_tables_in_plots(tmp_path):
    plots, tables = tmp_path/'plots', tmp_path/'csv'
    plots.mkdir()
    tables.mkdir()
    for name in ['response-time', 'resource-usage', 'pod-placement', 'slo-violations']:
        (plots/f'{name}.png').touch()
    for name, header, values in [
        ('window-metrics-15s.csv', 'method,p90_ms', '150'),
        ('pod-placement-15s.csv', 'method,ready_count', '3'),
        ('resource-windows-15s.csv', 'method,cpu_millicores,memory_mib', '300,256'),
    ]:
        (tables/name).write_text(header+'\n'+''.join(f'{m},{values}\n' for m in ('baseline', 'proposed')))
    _, counts, errors = smoke.inspect_outputs(plots)
    assert not errors
    assert counts['resource-windows-15s.csv:proposed:cpu_millicores'] == 1
    assert not list(plots.glob('*.csv'))
