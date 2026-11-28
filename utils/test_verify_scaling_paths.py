import json

from utils.verify_scaling_paths import check_requests


def _request(site, source, host, node='core-5g', phase='phase_01', ms=140.0):
    return json.dumps({'event': 'request', 'group': site, 'source_address': source, 'phase': phase,
                       'url': f'http://{host}:30124/detect', 'server_node': node,
                       'http_success': True, 'duration_ms': ms})


LOCAL = [_request('gnb-1', '172.16.50.251', '10.34.211.157'),
         _request('gnb-2', '10.63.1.102', '10.34.211.196'),
         _request('gnb-3', '10.64.1.103', '10.34.210.3')]


def test_local_upf_paths_pass():
    problems, medians = check_requests(LOCAL)
    assert problems == []
    assert set(medians) == {'gnb-1', 'gnb-2', 'gnb-3'}


def test_core_dnn_gnb1_path_is_rejected():
    legacy = [_request('gnb-1', '172.16.45.101', '10.34.211.140', ms=33.0), *LOCAL[1:]]
    problems, _ = check_requests(legacy)
    assert any('172.16.45.101 outside 172.16.50.0/24' in p for p in problems)
    assert any('does not target 10.34.211.157' in p for p in problems)


def test_missing_site_and_wrong_serving_node_are_rejected():
    problems, _ = check_requests([_request('gnb-2', '10.63.1.102', '10.34.211.196', node='gnb-2')])
    assert 'gnb-2: served on gnb-2, expected core-5g' in problems
    assert 'gnb-1: no requests' in problems and 'gnb-3: no requests' in problems
