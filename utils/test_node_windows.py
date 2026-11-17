import json
import math

from utils.node_windows import build_node_request_windows


def test_request_attribution_uses_server_not_ue_group_and_retains_unknowns(tmp_path):
    reqs = [dict(event='request', phase='p', start_time=1001, group='gnb-1', server_node='gnb-3', success=True, duration_ms=40),
            dict(event='request', phase='p', start_time=1002, group='gnb-3', server_node='gnb-2', success=True, duration_ms=200),
            dict(event='request', phase='p', start_time=1003, group='gnb-3', success=False, duration_ms=1000),
            dict(event='request', phase='p', start_time=1015, server_node='gnb-3', success=True, duration_ms=80)]
    (tmp_path/'requests.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in reqs))
    windows = [dict(phase='p', actual_start_time=1000+i, actual_end_time=1015+i, start_offset_s=i, end_offset_s=i+15) for i in [0, 15]]
    rows = build_node_request_windows(tmp_path, windows, ['gnb-2', 'gnb-3', 'gnb1'], 150)
    assert rows[0]['p90_ms'] == 200 and rows[0]['violation_percent'] == 100
    assert rows[1]['p90_ms'] == 40 and rows[1]['attempts'] == 1
    assert rows[1]['unattributed_attempts'] == 1 and rows[1]['all_window_attempts'] == 3
    assert rows[2]['attempts'] == 0 and math.isnan(rows[2]['p90_ms']) and math.isnan(rows[2]['violation_percent'])
    assert rows[4]['p90_ms'] == 80
