import subprocess
from unittest.mock import Mock

import pytest

from utils.traffic_control import POOL_CIDR, POOL_NAME, TrafficDelay


def test_reserved_pool_rejects_overlapping_existing_pool():
    delay = TrafficDelay(10)
    delay._json = Mock(return_value={'items': [
        {'metadata': {'name': 'other'}, 'spec': {'cidr': '172.16.40.0/24'}}]})
    with pytest.raises(RuntimeError, match='overlaps'):
        delay._pool()


def test_reserved_pool_rejects_unrestricted_allocation():
    delay = TrafficDelay(10)
    delay._json = Mock(return_value={'items': [{
        'metadata': {'name': POOL_NAME},
        'spec': {'cidr': POOL_CIDR, 'blockSize': 26, 'disabled': False,
                 'ipipMode': 'Never', 'vxlanMode': 'Always', 'natOutgoing': True,
                 'nodeSelector': 'all()', 'allowedUses': ['Workload']}}]})
    with pytest.raises(RuntimeError, match='unexpected settings'):
        delay._pool()


def test_cleanup_does_not_remove_replaced_filter_or_its_ifb():
    delay = TrafficDelay(10)
    delay.devices = [{'node': 'a', 'interface': 'eth0', 'ifb': 'mec123',
                      'filters': [(49210, 'dst')]}]
    delay._filter = Mock(return_value='an unrelated filter')
    delay._exec = Mock()
    with pytest.raises(RuntimeError, match='refusing to remove changed filter'):
        delay.close()
    delay._exec.assert_not_called()


@pytest.mark.parametrize(('delay_ms', 'reported_seconds'), [(0.5, 0.0005), (7.5, 0.007499), (10, 0.01), (1000, 1)])
def test_health_check_uses_configured_delay_even_when_tc_changes_units(delay_ms, reported_seconds):
    delay = TrafficDelay(delay_ms)
    delay.started = True
    delay.devices = [{'node': 'a', 'interface': 'eth0', 'ifb': 'mec123', 'filters': []}]
    result = Mock(stdout='[{"kind":"netem","options":{"delay":{"delay":%s}}}]\x1e' % reported_seconds)
    delay._exec = Mock(return_value=result)
    delay.check()
    result.stdout = '[{"kind":"netem","options":{"delay":{"delay":99}}}]\x1e'
    with pytest.raises(RuntimeError, match='delay is missing or changed'):
        delay.check()


def test_health_check_batches_reads_and_retries_over_spdy(monkeypatch, caplog):
    delay = TrafficDelay(10)
    delay.started = True
    delay.devices = [
        {'node': 'gnb1', 'interface': 'ens18', 'ifb': 'mec123', 'filters': [(49211, 'src')]},
        {'node': 'gnb1', 'interface': 'vxlan.calico', 'ifb': 'mec456', 'filters': [(49210, 'dst')]},
    ]
    qdisc = '[{"kind":"netem","options":{"delay":{"delay":0.01}}}]'
    source = f'src_ip {POOL_CIDR}\nsrc_port 5000\nredirect to device mec123'
    destination = f'dst_ip {POOL_CIDR}\ndst_port 5000\nredirect to device mec456'
    delay._exec = Mock(side_effect=[
        subprocess.TimeoutExpired('kubectl', 10, stderr=b'proxy read failed'),
        Mock(stdout='\x1e'.join((qdisc, source, qdisc, destination, ''))),
        Mock(stdout='\x1e'.join((qdisc, source, qdisc, '', ''))),
    ])
    sleep = Mock()
    monkeypatch.setattr('utils.traffic_control.time.sleep', sleep)

    delay.check()

    sleep.assert_called_once_with(1)
    first, retry = delay._exec.call_args_list
    assert first.args == retry.args
    assert 'tc -j qdisc show dev mec123' in first.args[-1]
    assert 'tc filter show dev vxlan.calico egress pref 49210' in first.args[-1]
    assert first.kwargs['env'] is None
    assert retry.kwargs['env']['KUBECTL_REMOTE_COMMAND_WEBSOCKETS'] == 'false'
    assert 'proxy read failed' in caplog.text

    with pytest.raises(RuntimeError, match='vxlan.calico workload tc filter 49210 is missing or changed'):
        delay.check()


def test_health_snapshot_rejects_truncated_output():
    delay = TrafficDelay(10)
    delay._exec = Mock(return_value=Mock(stdout='[]'))
    with pytest.raises(RuntimeError, match='gnb1: incomplete tc health snapshot'):
        delay._tc_snapshot('gnb1', [('-j', 'qdisc', 'show', 'dev', 'ens18')])


def test_filter_read_reports_stderr_after_retries(monkeypatch):
    delay = TrafficDelay(10)
    delay._exec = Mock(side_effect=[
        subprocess.TimeoutExpired('kubectl', 10, stderr=b'exec timed out'),
        subprocess.CalledProcessError(1, 'kubectl', stderr='unable to upgrade connection'),
        subprocess.CalledProcessError(1, 'kubectl', stderr='unable to upgrade connection'),
    ])
    sleep = Mock()
    monkeypatch.setattr('utils.traffic_control.time.sleep', sleep)

    with pytest.raises(RuntimeError, match='gnb1: .*ens18.*49211.*attempt 3/3.*unable to upgrade connection'):
        delay._filter('gnb1', 'ens18', 49211)

    assert delay._exec.call_count == 3
    assert sleep.call_count == 2
