from unittest.mock import Mock

import pytest

from experiments.traffic_control import POOL_CIDR, POOL_NAME, TrafficDelay


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


@pytest.mark.parametrize('delay_ms', [0.5, 10, 1000])
def test_health_check_uses_configured_delay_even_when_tc_changes_units(delay_ms):
    delay = TrafficDelay(delay_ms)
    delay.started = True
    delay.devices = [{'node': 'a', 'interface': 'eth0', 'ifb': 'mec123', 'filters': []}]
    result = Mock(stdout='[{"kind":"netem","options":{"delay":{"delay":%s}}}]' % (delay_ms / 1000))
    delay._exec = Mock(return_value=result)
    delay.check()
    result.stdout = '[{"kind":"netem","options":{"delay":{"delay":99}}}]'
    with pytest.raises(RuntimeError, match='delay is missing or changed'):
        delay.check()
