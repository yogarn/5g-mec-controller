from unittest.mock import Mock

import pytest

from experiments.traffic_control import POOL_CIDR, POOL_NAME, TrafficDelay


def test_reserved_pool_rejects_overlapping_existing_pool():
    delay = TrafficDelay()
    delay._json = Mock(return_value={'items': [
        {'metadata': {'name': 'other'}, 'spec': {'cidr': '172.16.40.0/24'}}]})
    with pytest.raises(RuntimeError, match='overlaps'):
        delay._pool()


def test_reserved_pool_rejects_unrestricted_allocation():
    delay = TrafficDelay()
    delay._json = Mock(return_value={'items': [{
        'metadata': {'name': POOL_NAME},
        'spec': {'cidr': POOL_CIDR, 'blockSize': 26, 'disabled': False,
                 'ipipMode': 'Never', 'vxlanMode': 'Always', 'natOutgoing': True,
                 'nodeSelector': 'all()', 'allowedUses': ['Workload']}}]})
    with pytest.raises(RuntimeError, match='unexpected settings'):
        delay._pool()


def test_cleanup_does_not_remove_replaced_filter_or_its_ifb():
    delay = TrafficDelay()
    delay.devices = [{'node': 'a', 'interface': 'eth0', 'ifb': 'mec123',
                      'filters': [(49210, 'dst')]}]
    delay._filter = Mock(return_value='an unrelated filter')
    delay._exec = Mock()
    with pytest.raises(RuntimeError, match='refusing to remove changed filter'):
        delay.close()
    delay._exec.assert_not_called()
