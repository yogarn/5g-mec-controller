"""Comprehensive local verification tests for vertical offloading, collector snapshot, xDS, and 4-arm tooling."""
import http.client
import http.server
import json
from pathlib import Path
import subprocess
import threading
from urllib.parse import urlsplit

import pytest
import yaml

from benchmark.client import attempt
from collector.server import Collector, load_settings, make_handler
from experiments import campaign
from offloader.budget import VerticalBudget
from offloader.k8s import filter_pinned_endpoints
from offloader.policy import (
    OffloadConfig,
    SITES,
    VerticalOffloadPolicy,
    spread_core_ratio,
    spread_local_weight,
)
from offloader.scaler import PodAccounting, VerticalDynamicScaler, account_edge_pods
from offloader.server import VerticalOffloadController
from offloader.xds import (
    CORE_EDS_SERVICE_NAME,
    XdsState,
    build_cluster_load_assignment,
    local_eds_service_name,
    route_config_name,
    spread_eds_service_name,
    start_admin_server,
    start_xds_server,
)
from utils.plot_vertical import plot_four_arm
from utils.verify_vertical_path import check_gnb1_capacity, verify_eds_pinning, verify_smf_sessions
from utils.vertical_summary import summarize_campaign


def _make_bucket(
    *,
    status='ok',
    p90_ms=80.0,
    attempts=20,
    successes=20,
    errors=0,
    timeouts=0,
    age=0.2,
    seq=1,
):
    return {
        'status': status,
        'p90_ms': p90_ms,
        'attempts': attempts,
        'successes': successes,
        'errors': errors,
        'timeouts': timeouts,
        'error_rate': None if attempts == 0 else round(errors / attempts, 6),
        'last_sample_age_seconds': age,
        'last_sample_ts': 1700000000.0,
        'last_success_age_seconds': age if successes else None,
        'newest_sample_seq': seq,
    }


def _make_snapshot(site_overrides=None):
    site_overrides = site_overrides or {}
    sites = {}
    for site, node in (('gnb-1', 'gnb1'), ('gnb-2', 'gnb-2'), ('gnb-3', 'gnb-3')):
        ov = site_overrides.get(site, {})
        user = {
            'local': ov.get('user_local', _make_bucket(p90_ms=80.0, seq=1)),
            'core': ov.get('user_core', _make_bucket(status='empty', p90_ms=None, attempts=0, successes=0, age=None, seq=0)),
            'unknown': _make_bucket(status='empty', p90_ms=None, attempts=0, successes=0, age=None, seq=0),
        }
        if 'user_overall' in ov:
            user['overall'] = ov['user_overall']
        sites[site] = {
            'node': node,
            'user': user,
            'probe': {
                'local': ov.get('probe_local', _make_bucket(status='empty', p90_ms=None, attempts=0, successes=0, age=None, seq=0)),
            },
        }
    return {
        'generated_at': 1700000000.0,
        'window_seconds': 15.0,
        'stale_after_seconds': 5.0,
        'p90_basis': 'success_only',
        'sites': sites,
    }


@pytest.mark.parametrize('hot_site', SITES)
def test_single_site_enter_spread_and_exit_on_overall_p90_without_probe_gate(hot_site):
    cfg = OffloadConfig(
        mode='vertical',
        offload_p90_threshold_ms=150.0,
        min_samples=15,
        breach_duration_seconds=5.0,
        min_fresh_updates=2,
        min_dwell_seconds=15.0,
    )
    policy = VerticalOffloadPolicy(cfg)
    local_ready = {s: 1 for s in SITES}
    core_ready = 3
    wall_start = 1700000000.0
    policy.record_envoy_ack(hot_site, 'LOCAL', '1', wall_start)

    # Local p90 breaches SLO (>150ms) across breach_duration with fresh updates.
    enter_events = []
    for sec in range(16, 22):
        snap = _make_snapshot({
            hot_site: {'user_local': _make_bucket(p90_ms=210.0, attempts=20, successes=20, seq=10 + sec)},
        })
        snap['generated_at'] = wall_start + sec
        enter_events.extend(policy.evaluate(float(sec), snap, local_ready, core_ready))

    for s in SITES:
        if s == hot_site:
            assert policy.states[s].desired_route == 'SPREAD'
            assert policy.states[s].last_decision_reason == 'local_p90_breach'
        else:
            assert policy.states[s].desired_route == 'LOCAL'

    rd = [e for e in enter_events if e.event == 'route_decision' and e.site == hot_site]
    assert len(rd) == 1
    assert rd[0].old_route == 'LOCAL' and rd[0].new_route == 'SPREAD'
    assert rd[0].extra['c'] == 3 and rd[0].extra['x_i'] == 1 and rd[0].extra['k'] == 1
    assert rd[0].extra['core_ratio'] == pytest.approx(0.75, abs=1e-6)

    # Simulate Envoy ACK for hot_site entering SPREAD.
    ack_ev = policy.record_envoy_ack(hot_site, 'SPREAD', '2', wall_start + 22, monotonic_now=22.0)
    assert ack_ev is not None and ack_ev.event == 'envoy_ack'
    assert ack_ev.extra['k'] == 1 and ack_ev.extra['c'] == 3 and ack_ev.extra['x_i'] == 1
    assert policy.states[hot_site].active_route == 'SPREAD'

    def tick(sec, overall_p90, *, probe_p90=None):
        ov = {
            'user_overall': _make_bucket(p90_ms=overall_p90, attempts=25, successes=25, seq=100 + sec),
        }
        if probe_p90 is not None:
            ov['probe_local'] = _make_bucket(p90_ms=probe_p90, seq=200 + sec)
        snap = _make_snapshot({hot_site: ov})
        snap['generated_at'] = wall_start + sec
        return policy.evaluate(float(sec), snap, local_ready, core_ready)

    # Before post-ACK window completes (22 + 15 = 37): must hold SPREAD even if overall p90 <= SLO.
    evs = tick(35, 110.0)
    assert policy.states[hot_site].desired_route == 'SPREAD'
    assert policy.states[hot_site].last_status_note == 'spread_waiting_post_ack'
    assert any(e.event == 'hold_route' and e.reason == 'spread_waiting_post_ack' and e.extra['k'] == 1 for e in evs)

    # After post-ACK window, overall p90 > SLO (160ms): must NOT exit SPREAD.
    for sec in range(37, 42):
        tick(sec, 160.0)
    assert policy.states[hot_site].desired_route == 'SPREAD'
    assert policy.states[hot_site].last_status_note == 'spread_p90_above_slo'

    # Overall p90 <= SLO (150ms) with NO probe at all (and unhealthy probe on next tick): exits to LOCAL!
    tick(42, 150.0, probe_p90=None)
    assert policy.states[hot_site].desired_route == 'SPREAD'  # 1 fresh update < min_fresh_updates(2)
    exit_evs = tick(43, 145.0, probe_p90=999.0)
    assert policy.states[hot_site].desired_route == 'LOCAL'
    assert policy.states[hot_site].last_decision_reason == 'spread_overall_p90_recovered'
    exit_rd = [e for e in exit_evs if e.event == 'route_decision' and e.site == hot_site]
    assert len(exit_rd) == 1
    assert exit_rd[0].extra['c'] == 3 and exit_rd[0].extra['x_i'] == 1 and exit_rd[0].extra['k'] == 1


def test_spread_weights_and_core_ratios_match_specification_examples():
    # Example 1: c=3, x=3 for all sites -> k=1: 50%, k=2: 33.3%, k=3: 25%.
    assert spread_core_ratio(3, 3, 1) == pytest.approx(0.5, abs=1e-4)
    assert spread_core_ratio(3, 3, 2) == pytest.approx(1 / 3, abs=1e-4)
    assert spread_core_ratio(3, 3, 3) == pytest.approx(0.25, abs=1e-4)

    # Example 2: c=3, x=(gnb-1:1, gnb-2:3, gnb-3:2)
    # k=3 -> 50%, 25%, 33.3%
    assert spread_core_ratio(3, 1, 3) == pytest.approx(0.5, abs=1e-4)
    assert spread_core_ratio(3, 3, 3) == pytest.approx(0.25, abs=1e-4)
    assert spread_core_ratio(3, 2, 3) == pytest.approx(1 / 3, abs=1e-4)
    # Only gnb-2 and gnb-3 SPREAD (k=2) -> 33.3% and 42.9%
    assert spread_core_ratio(3, 3, 2) == pytest.approx(1 / 3, abs=1e-4)
    assert spread_core_ratio(3, 2, 2) == pytest.approx(3 / 7, abs=1e-4)

    # Verify EDS ClusterLoadAssignment weights dynamically recompute as k changes.
    xds = XdsState(epoch='ep1')
    local_eps = {
        'gnb-1': [('10.1.0.1', 5000)],
        'gnb-2': [('10.2.0.1', 5000), ('10.2.0.2', 5000), ('10.2.0.3', 5000)],
        'gnb-3': [('10.3.0.1', 5000), ('10.3.0.2', 5000)],
    }
    core_eps = [('10.9.0.1', 5000), ('10.9.0.2', 5000), ('10.9.0.3', 5000)]
    xds.update_endpoints(local_eps, core_eps)

    def _eds_weights(site):
        status, body = xds.handle_eds({'version_info': '', 'resource_names': [spread_eds_service_name(site)]})
        assert status == 200
        lb_eps = body['resources'][0]['endpoints'][0]['lb_endpoints']
        return {
            e['endpoint']['address']['socket_address']['address']: e['load_balancing_weight']
            for e in lb_eps
        }

    # k=1 (gnb-2 enters SPREAD): local weight=1, core weight=1 -> core share = 3/(3+3)=50%
    xds.set_site_route('gnb-2', 'SPREAD')
    w2 = _eds_weights('gnb-2')
    assert all(v >= 1 for v in w2.values())
    assert w2['10.2.0.1'] == 1 and w2['10.9.0.1'] == 1

    # k=2 (gnb-3 enters SPREAD): local weight=2, core weight=1 -> gnb-2: 3/9=33.3%, gnb-3: 3/7=42.9%
    eds_v_before = xds.status_snapshot()['eds_version']
    xds.set_site_route('gnb-3', 'SPREAD')
    assert xds.status_snapshot()['eds_version'] != eds_v_before
    w2 = _eds_weights('gnb-2')
    w3 = _eds_weights('gnb-3')
    assert w2['10.2.0.1'] == 2 and w2['10.9.0.1'] == 1
    assert w3['10.3.0.1'] == 2 and w3['10.9.0.1'] == 1
    assert sum(w2[ip] for ip, _ in core_eps) / sum(w2.values()) == pytest.approx(1 / 3, abs=1e-4)
    assert sum(w3[ip] for ip, _ in core_eps) / sum(w3.values()) == pytest.approx(3 / 7, abs=1e-4)

    # k=3 (gnb-1 enters SPREAD): local weight=3, core weight=1 -> 50%, 25%, 33.3%
    xds.set_site_route('gnb-1', 'SPREAD')
    for site, expected_ratio in (('gnb-1', 0.5), ('gnb-2', 0.25), ('gnb-3', 1 / 3)):
        ws = _eds_weights(site)
        for ip, _ in local_eps[site]:
            assert ws[ip] == 3
        for ip, _ in core_eps:
            assert ws[ip] == 1
        assert sum(ws[ip] for ip, _ in core_eps) / sum(ws.values()) == pytest.approx(expected_ratio, abs=1e-4)

    # gnb-1 exits SPREAD -> k drops back to 2 -> gnb-2 and gnb-3 local weights recomputed to 2.
    xds.set_site_route('gnb-1', 'LOCAL')
    assert _eds_weights('gnb-2')['10.2.0.1'] == 2
    assert _eds_weights('gnb-3')['10.3.0.1'] == 2


def test_k_change_restarts_post_ack_window_for_other_spread_sites():
    cfg = OffloadConfig(breach_duration_seconds=1.0, min_fresh_updates=2, min_dwell_seconds=5.0, route_switch_interval_seconds=5.0)
    policy = VerticalOffloadPolicy(cfg)
    wall_start = 1700000000.0
    local_ready = {'gnb-1': 1, 'gnb-2': 3, 'gnb-3': 2}

    # gnb-1 is already in SPREAD and its post-ACK window has elapsed (confirmed at t=0, now t=20).
    st1 = policy.states['gnb-1']
    st1.desired_route = st1.active_route = 'SPREAD'
    st1.last_transition_time = 0.0
    st1.spread_confirmed_since = wall_start

    # gnb-2 breaches local SLO and enters SPREAD at t=21 (k: 1 -> 2).
    for sec in (20, 21):
        snap = _make_snapshot({
            'gnb-1': {'user_overall': _make_bucket(p90_ms=160.0, seq=sec)},
            'gnb-2': {'user_local': _make_bucket(p90_ms=220.0, seq=sec)},
        })
        snap['generated_at'] = wall_start + sec
        policy.evaluate(float(sec), snap, local_ready, 3)

    assert policy.states['gnb-2'].desired_route == 'SPREAD'
    # gnb-1's post-ACK window MUST have been restarted to wall_start + 21!
    assert st1.spread_confirmed_since == wall_start + 21

    # ACK gnb-2 at t=22.
    policy.record_envoy_ack('gnb-2', 'SPREAD', '2', wall_start + 22, monotonic_now=22.0)

    # Even when gnb-1 overall p90 drops to 90ms at t=30 (< 21+15=36), gnb-1 MUST wait for its restarted post-ACK window.
    for sec in range(28, 35):
        snap = _make_snapshot({
            'gnb-1': {'user_overall': _make_bucket(p90_ms=90.0, seq=100 + sec)},
            'gnb-2': {'user_overall': _make_bucket(p90_ms=180.0, seq=100 + sec)},
        })
        snap['generated_at'] = wall_start + sec
        policy.evaluate(float(sec), snap, local_ready, 3)
    assert st1.desired_route == 'SPREAD'
    assert st1.last_status_note == 'spread_waiting_post_ack'

    # Once both gnb-1 restarted post-ACK (36) and gnb-2 post-ACK (37) complete, gnb-1 can exit to LOCAL.
    for sec in (37, 38):
        snap = _make_snapshot({
            'gnb-1': {'user_overall': _make_bucket(p90_ms=90.0, seq=200 + sec)},
            'gnb-2': {'user_overall': _make_bucket(p90_ms=180.0, seq=200 + sec)},
        })
        snap['generated_at'] = wall_start + sec
        policy.evaluate(float(sec), snap, local_ready, 3)
    assert st1.desired_route == 'LOCAL'


def test_two_sites_eligible_to_exit_picks_largest_core_ratio_then_site_name_and_serializes():
    cfg = OffloadConfig(breach_duration_seconds=1.0, min_fresh_updates=2, min_dwell_seconds=5.0, route_switch_interval_seconds=5.0)
    policy = VerticalOffloadPolicy(cfg)
    wall_start = 1700000000.0
    # gnb-2 has x=3 (core_ratio=3/9=33.3%), gnb-3 has x=2 (core_ratio=3/7=42.9%) -> gnb-3 MUST exit first!
    local_ready = {'gnb-1': 1, 'gnb-2': 3, 'gnb-3': 2}
    for s in ('gnb-2', 'gnb-3'):
        st = policy.states[s]
        st.desired_route = st.active_route = 'SPREAD'
        st.last_transition_time = 0.0
        st.spread_confirmed_since = wall_start

    def tick(sec):
        snap = _make_snapshot({
            'gnb-2': {'user_overall': _make_bucket(p90_ms=100.0, seq=sec)},
            'gnb-3': {'user_overall': _make_bucket(p90_ms=100.0, seq=sec)},
        })
        snap['generated_at'] = wall_start + sec
        return [e for e in policy.evaluate(float(sec), snap, local_ready, 3) if e.event == 'route_decision']

    assert tick(20) == []  # 1st fresh update
    evs = tick(21)         # 2nd fresh update: both eligible, only gnb-3 (higher core_ratio) exits!
    assert [(e.site, e.new_route) for e in evs] == [('gnb-3', 'LOCAL')]
    assert policy.states['gnb-2'].desired_route == 'SPREAD'

    # gnb-2 cannot exit before gnb-3 ACK + post-ACK window (15s).
    assert tick(22) == []
    policy.record_envoy_ack('gnb-3', 'LOCAL', '2', wall_start + 23, monotonic_now=23.0)
    for sec in range(24, 38):
        assert tick(sec) == []
    # At t=38 (gnb-3 post-ACK 23+15=38 finishes, and gnb-2 already collected >=2 fresh updates since its restarted post-ACK 21+15=36), gnb-2 exits.
    evs2 = tick(38)
    assert [(e.site, e.new_route) for e in evs2] == [('gnb-2', 'LOCAL')]

    # Tie-breaker test: identical core_ratio -> lexicographical site name order (gnb-1 before gnb-2).
    policy2 = VerticalOffloadPolicy(cfg)
    for s in ('gnb-1', 'gnb-2'):
        st = policy2.states[s]
        st.desired_route = st.active_route = 'SPREAD'
        st.last_transition_time = 0.0
        st.spread_confirmed_since = wall_start
    equal_ready = {'gnb-1': 2, 'gnb-2': 2, 'gnb-3': 2}
    for sec in (20, 21):
        snap = _make_snapshot({
            'gnb-1': {'user_overall': _make_bucket(p90_ms=110.0, seq=sec)},
            'gnb-2': {'user_overall': _make_bucket(p90_ms=110.0, seq=sec)},
        })
        snap['generated_at'] = wall_start + sec
        out = [e for e in policy2.evaluate(float(sec), snap, equal_ready, 3) if e.event == 'route_decision']
    assert [(e.site, e.new_route) for e in out] == [('gnb-1', 'LOCAL')]


def test_no_flip_flop_when_p90_oscillates_around_slo():
    cfg = OffloadConfig(
        offload_p90_threshold_ms=150.0,
        breach_duration_seconds=5.0,
        min_fresh_updates=2,
        min_dwell_seconds=15.0,
        route_switch_interval_seconds=5.0,
    )
    policy = VerticalOffloadPolicy(cfg)
    wall_start = 1700000000.0
    local_ready = {s: 2 for s in SITES}
    policy.record_envoy_ack('gnb-1', 'LOCAL', '1', wall_start)

    transitions = []
    # Oscillate p90 around 150ms every second (145, 155, 145, 155, ...) for 60 seconds while LOCAL.
    for sec in range(1, 61):
        p90 = 155.0 if sec % 2 == 1 else 145.0
        snap = _make_snapshot({
            'gnb-1': {'user_local': _make_bucket(p90_ms=p90, seq=sec)},
        })
        snap['generated_at'] = wall_start + sec
        transitions.extend(e for e in policy.evaluate(float(sec), snap, local_ready, 3) if e.event == 'route_decision')
    assert transitions == []
    assert policy.states['gnb-1'].desired_route == 'LOCAL'

    # Now oscillate while in SPREAD (145, 155, 145, 155, ...) after post-ACK: must NOT exit or flip-flop!
    st = policy.states['gnb-1']
    st.desired_route = st.active_route = 'SPREAD'
    st.last_transition_time = 60.0
    st.spread_confirmed_since = wall_start + 60.0
    for sec in range(76, 136):
        p90 = 145.0 if sec % 2 == 0 else 155.0
        snap = _make_snapshot({
            'gnb-1': {'user_overall': _make_bucket(p90_ms=p90, seq=100 + sec)},
        })
        snap['generated_at'] = wall_start + sec
        transitions.extend(e for e in policy.evaluate(float(sec), snap, local_ready, 3) if e.event == 'route_decision')
    assert transitions == []
    assert policy.states['gnb-1'].desired_route == 'SPREAD'


def test_runtime_switches_serialize_across_ack_and_post_ack_window():
    policy = VerticalOffloadPolicy(OffloadConfig(breach_duration_seconds=1.0, min_dwell_seconds=5.0, route_switch_interval_seconds=5.0))
    wall_start = 1700000000.0
    local_ready = dict.fromkeys(SITES, 2)

    def tick(sec):
        snap = _make_snapshot({
            'gnb-1': {'user_local': _make_bucket(p90_ms=220.0 if sec >= 20 else 80.0, seq=sec)},
            'gnb-2': {'user_local': _make_bucket(p90_ms=220.0 if sec >= 20 else 80.0, seq=sec)},
        })
        snap['generated_at'] = wall_start + sec
        return [(e.site, e.new_route) for e in policy.evaluate(float(sec), snap, local_ready, 3) if e.event == 'route_decision']

    assert tick(19) == []
    assert tick(20) == []
    assert tick(21) == [('gnb-1', 'SPREAD')]
    # Pending ACK blocks gnb-2.
    assert tick(22) == []
    assert tick(23) == []
    policy.record_envoy_ack('gnb-1', 'SPREAD', '2', wall_start + 24, monotonic_now=24.0)
    # Post-ACK window (15s after 24 -> 39) blocks gnb-2 even after 5s switch interval.
    for sec in range(24, 39):
        assert tick(sec) == []
    assert tick(39) == [('gnb-2', 'SPREAD')]


def test_min_samples_and_no_duplicate_rolling_window_trigger():
    cfg = OffloadConfig(min_samples=15, breach_duration_seconds=3.0, min_fresh_updates=2)
    policy = VerticalOffloadPolicy(cfg)
    local_ready = {s: 1 for s in SITES}

    # Too few samples (10 < 15): must stay LOCAL even with high p90.
    for sec in range(0, 6):
        snap = _make_snapshot({
            'gnb-2': {'user_local': _make_bucket(p90_ms=250.0, attempts=10, successes=10, seq=1 + sec)},
        })
        policy.evaluate(float(sec), snap, local_ready, 3)
    assert policy.states['gnb-2'].desired_route == 'LOCAL'

    # Enough samples (20 >= 15), but seq never advances (same rolling window read repeatedly): must NOT trigger!
    for sec in range(10, 20):
        snap = _make_snapshot({
            'gnb-2': {'user_local': _make_bucket(p90_ms=250.0, attempts=20, successes=20, seq=99)},
        })
        policy.evaluate(float(sec), snap, local_ready, 3)
    assert policy.states['gnb-2'].desired_route == 'LOCAL'
    assert policy.states['gnb-2'].last_status_note == 'breach_waiting_fresh_samples'


def test_total_failure_error_rate_triggers_fallback():
    cfg = OffloadConfig(min_error_samples=5, offload_error_rate_threshold=0.5, breach_duration_seconds=3.0, min_fresh_updates=2)
    policy = VerticalOffloadPolicy(cfg)
    local_ready = {s: 1 for s in SITES}

    for sec in range(0, 5):
        snap = _make_snapshot({
            'gnb-1': {
                'user_local': _make_bucket(
                    status='no_success', p90_ms=None, attempts=6, successes=0, errors=6, timeouts=6, seq=1 + sec
                )
            }
        })
        policy.evaluate(float(sec), snap, local_ready, 3)
    assert policy.states['gnb-1'].desired_route == 'SPREAD'
    assert policy.states['gnb-1'].last_decision_reason == 'local_error_rate_breach'


def test_stale_telemetry_and_collector_error_hold_last_route():
    cfg = OffloadConfig(stale_after_seconds=5.0, breach_duration_seconds=2.0, min_fresh_updates=2)
    policy = VerticalOffloadPolicy(cfg)
    local_ready = {s: 1 for s in SITES}

    # Stale telemetry must hold LOCAL and log hold_route.
    snap = _make_snapshot({
        'gnb-3': {'user_local': _make_bucket(status='stale', p90_ms=300.0, attempts=20, successes=20, age=8.0, seq=5)},
    })
    events = policy.evaluate(1.0, snap, local_ready, 3)
    assert policy.states['gnb-3'].desired_route == 'LOCAL'
    assert any(ev.site == 'gnb-3' and ev.event == 'hold_route' and ev.reason == 'telemetry_stale' for ev in events)

    # Collector error must hold route and log collector_error.
    events = policy.evaluate(2.0, None, local_ready, 3, collector_error='TimeoutError: timed out')
    assert all(policy.states[s].desired_route == 'LOCAL' for s in SITES)
    assert any(ev.event == 'hold_route' and ev.reason == 'collector_error' for ev in events)


def test_core_unready_blocks_offload_and_fails_back_if_core_lost():
    cfg = OffloadConfig(breach_duration_seconds=2.0, min_fresh_updates=2, min_dwell_seconds=30.0)
    policy = VerticalOffloadPolicy(cfg)
    local_ready = {s: 1 for s in SITES}

    # Core has 0 ready endpoints -> must NOT offload.
    for sec in range(0, 5):
        snap = _make_snapshot({
            'gnb-1': {'user_local': _make_bucket(p90_ms=260.0, attempts=20, successes=20, seq=1 + sec)},
        })
        policy.evaluate(float(sec), snap, local_ready, core_ready_count=0)
    assert policy.states['gnb-1'].desired_route == 'LOCAL'
    assert policy.states['gnb-1'].last_status_note == 'core_not_ready'

    # Core becomes ready -> offloads.
    for sec in range(5, 9):
        snap = _make_snapshot({
            'gnb-1': {'user_local': _make_bucket(p90_ms=260.0, attempts=20, successes=20, seq=10 + sec)},
        })
        policy.evaluate(float(sec), snap, local_ready, core_ready_count=2)
    assert policy.states['gnb-1'].desired_route == 'SPREAD'

    # Core loss still obeys global pacing, including when the previous ACK times out.
    assert not policy.evaluate(10.0, _make_snapshot(), local_ready, core_ready_count=0)
    assert policy.states['gnb-1'].desired_route == 'SPREAD'
    events = policy.evaluate(12.0, _make_snapshot(), local_ready, core_ready_count=0)
    assert policy.states['gnb-1'].desired_route == 'LOCAL'
    assert any(ev.site == 'gnb-1' and ev.reason == 'core_backend_unready_failback' for ev in events)


def test_envoy_update_failure_tracks_desired_and_active_separately():
    cfg = OffloadConfig(breach_duration_seconds=2.0, min_fresh_updates=2, envoy_ack_timeout_seconds=3.0)
    policy = VerticalOffloadPolicy(cfg)
    local_ready = {s: 1 for s in SITES}

    for sec in range(0, 4):
        snap = _make_snapshot({
            'gnb-2': {'user_local': _make_bucket(p90_ms=220.0, attempts=20, successes=20, seq=1 + sec)},
        })
        policy.evaluate(float(sec), snap, local_ready, 3)

    assert policy.states['gnb-2'].desired_route == 'SPREAD'
    assert policy.states['gnb-2'].active_route == 'LOCAL'

    # Envoy NACK keeps active_route at LOCAL while desired_route remains SPREAD.
    nack_ev = policy.record_envoy_nack('gnb-2', 'invalid route config', 4.0)
    assert nack_ev.event == 'envoy_update_failed'
    assert nack_ev.desired_route == 'SPREAD'
    assert nack_ev.active_route == 'LOCAL'
    assert policy.states['gnb-2'].envoy_update_failed is True

    # Also test ACK timeout when no NACK arrives.
    policy.reset()
    for sec in range(0, 4):
        snap = _make_snapshot({
            'gnb-3': {'user_local': _make_bucket(p90_ms=220.0, attempts=20, successes=20, seq=1 + sec)},
        })
        policy.evaluate(float(sec), snap, local_ready, 3)
    events = policy.evaluate(10.0, _make_snapshot(), local_ready, 3)
    assert any(ev.site == 'gnb-3' and ev.event == 'envoy_update_failed' and ev.reason == 'envoy_ack_timeout' for ev in events)
    assert policy.states['gnb-3'].desired_route == 'SPREAD'
    assert policy.states['gnb-3'].active_route == 'LOCAL'


def test_local_only_mode_stays_local_during_slo_breach():
    cfg = OffloadConfig(mode='local-only', breach_duration_seconds=1.0, min_fresh_updates=1)
    policy = VerticalOffloadPolicy(cfg)
    local_ready = {s: 1 for s in SITES}

    for sec in range(0, 10):
        snap = _make_snapshot({
            'gnb-1': {'user_local': _make_bucket(p90_ms=400.0, attempts=30, successes=30, seq=1 + sec)},
            'gnb-2': {'user_local': _make_bucket(p90_ms=400.0, attempts=30, successes=30, seq=1 + sec)},
            'gnb-3': {'user_local': _make_bucket(p90_ms=400.0, attempts=30, successes=30, seq=1 + sec)},
        })
        policy.evaluate(float(sec), snap, local_ready, 3)
    assert all(policy.states[s].desired_route == 'LOCAL' for s in SITES)


def test_collector_offload_snapshot_separates_user_and_probe_and_never_reports_zero_for_empty():
    settings = load_settings(Path(__file__).resolve().parent.parent / 'collector' / 'config.yaml')
    now = [100.0]
    collector = Collector(settings, clock=lambda: now[0], wall_clock=lambda: 1700000000.0 + now[0])

    # Empty window must report status='empty' and p90_ms=None (never 0).
    snap = collector.offload_snapshot()
    assert snap['p90_basis'] == 'success_only'
    assert snap['sites']['gnb-1']['user']['local']['status'] == 'empty'
    assert snap['sites']['gnb-1']['user']['local']['p90_ms'] is None

    # Ingest user samples on gnb-1 local and gnb-1 core, plus probe on gnb-1 local.
    assert collector.ingest({'ue_ip': '172.16.50.251', 'site': 'gnb-1', 'route': 'local', 'duration_ms': 180.0, 'http_status': 200, 'success': True})
    assert collector.ingest({'ue_ip': '172.16.50.251', 'site': 'gnb-1', 'route': 'local', 'duration_ms': 250.0, 'http_status': 504, 'success': False, 'timed_out': True})
    assert collector.ingest({'ue_ip': '172.16.50.251', 'site': 'gnb-1', 'route': 'core', 'duration_ms': 70.0, 'http_status': 200, 'success': True})
    assert collector.ingest({'traffic_class': 'probe', 'site': 'gnb-1', 'route': 'local', 'duration_ms': 60.0, 'http_status': 200, 'success': True})

    # Probe must NOT pollute global HPA window attempts/successes!
    metrics = collector.metrics()
    assert 'ue_request_window_attempts 3\n' in metrics
    assert 'ue_request_window_successes 2\n' in metrics

    snap = collector.offload_snapshot()
    u_local = snap['sites']['gnb-1']['user']['local']
    u_core = snap['sites']['gnb-1']['user']['core']
    u_overall = snap['sites']['gnb-1']['user']['overall']
    p_local = snap['sites']['gnb-1']['probe']['local']
    assert u_local['status'] == 'ok' and u_local['p90_ms'] == 180.0 and u_local['attempts'] == 2 and u_local['timeouts'] == 1
    assert u_core['status'] == 'ok' and u_core['p90_ms'] == 70.0 and u_core['attempts'] == 1
    assert u_overall['status'] == 'ok' and u_overall['attempts'] == 3 and u_overall['successes'] == 2 and u_overall['p90_ms'] == 180.0
    assert p_local['status'] == 'ok' and p_local['p90_ms'] == 60.0 and p_local['attempts'] == 1

    # Advance past stale_after_seconds (5s) -> status becomes 'stale'.
    now[0] += 6.0
    snap = collector.offload_snapshot()
    assert snap['sites']['gnb-1']['user']['local']['status'] == 'stale'


def test_xds_server_rds_eds_ack_nack_and_reset():
    pods = [
        {'metadata': {'labels': {'app': 'mec-placement-pedestrian-detection'}}, 'spec': {'nodeName': 'gnb1'}, 'status': {'phase': 'Running', 'podIP': '172.16.4.10', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
        {'metadata': {'labels': {'app': 'mec-placement-pedestrian-detection'}}, 'spec': {'nodeName': 'gnb1'}, 'status': {'phase': 'Running', 'podIP': '172.16.4.11', 'conditions': [{'type': 'Ready', 'status': 'False'}]}},
        {'metadata': {'labels': {'app': 'mec-placement-pedestrian-detection'}}, 'spec': {'nodeName': 'gnb-2'}, 'status': {'phase': 'Running', 'podIP': '172.16.20.10', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
        {'metadata': {'labels': {'app': 'mec-placement-pedestrian-detection'}}, 'spec': {'nodeName': 'gnb-3'}, 'status': {'phase': 'Running', 'podIP': '172.16.20.20', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
        {'metadata': {'labels': {'app': 'mec-placement-pedestrian-detection-core'}}, 'spec': {'nodeName': 'core-5g'}, 'status': {'phase': 'Running', 'podIP': '172.16.10.50', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
    ]
    local_by_site, core_eps = filter_pinned_endpoints(pods)
    assert local_by_site == {
        'gnb-1': [('172.16.4.10', 5000)],
        'gnb-2': [('172.16.20.10', 5000)],
        'gnb-3': [('172.16.20.20', 5000)],
    }
    assert core_eps == [('172.16.10.50', 5000)]

    now = [100.0]
    ctrl = VerticalOffloadController(
        OffloadConfig(),
        collector_snapshot_url='http://127.0.0.1:9/offload/snapshot',
        clock=lambda: now[0],
    )
    ctrl.xds.update_endpoints(local_by_site, core_eps)
    xds_server = start_xds_server(ctrl, host='127.0.0.1', port=0)
    admin_token = 'test-secret-token'
    admin_server = start_admin_server(ctrl, admin_token, host='127.0.0.1', port=0)
    try:
        xds_port = xds_server.server_port
        admin_port = admin_server.server_port
        xds_conn = http.client.HTTPConnection('127.0.0.1', xds_port, timeout=2)
        admin_conn = http.client.HTTPConnection('127.0.0.1', admin_port, timeout=2)
        auth_headers = {'Authorization': f'Bearer {admin_token}', 'Content-Type': 'application/json'}

        # Discovery listener MUST NOT expose admin mutation endpoints.
        xds_conn.request('POST', '/override', json.dumps({'site': 'gnb-1', 'route': 'SPREAD'}))
        assert xds_conn.getresponse().status == 404
        xds_conn.close()
        xds_conn = http.client.HTTPConnection('127.0.0.1', xds_port, timeout=2)

        # Admin listener MUST reject unauthenticated and wrong-token requests with 401.
        admin_conn.request('POST', '/reset', json.dumps({'mode': 'local-only'}))
        resp = admin_conn.getresponse()
        resp.read()
        assert resp.status == 401
        admin_conn.request('POST', '/override', json.dumps({'site': 'gnb-1', 'route': 'SPREAD'}), {'Authorization': 'Bearer wrong'})
        resp = admin_conn.getresponse()
        resp.read()
        assert resp.status == 401

        # Initial RDS fetch for gnb-1 returns epoch-scoped version 1 pointing to local_backend with no retry_policy.
        xds_conn.request('POST', '/v3/discovery:routes', json.dumps({
            'version_info': '', 'node': {'id': 'envoy-gnb-1'}, 'resource_names': [route_config_name('gnb-1')]
        }))
        resp = xds_conn.getresponse()
        data = json.loads(resp.read())
        v1 = f'{ctrl.xds.epoch}-1'
        assert resp.status == 200 and data['version_info'] == v1
        routes = data['resources'][0]['virtual_hosts'][0]['routes']
        assert routes[1]['route']['cluster'] == 'local_backend'  # probe route
        assert routes[2]['route']['cluster'] == 'local_backend'  # user route
        assert 'retry_policy' not in routes[2]['route']

        # Manual override to SPREAD for gnb-1 only via authenticated admin listener.
        admin_conn.request('POST', '/override', json.dumps({'site': 'gnb-1', 'route': 'SPREAD'}), auth_headers)
        resp = admin_conn.getresponse()
        ov = json.loads(resp.read())
        assert resp.status == 200 and ov['desired_route'] == 'SPREAD'

        admin_conn.request('POST', '/override', json.dumps({'site': 'gnb-2', 'route': 'SPREAD'}), auth_headers)
        resp = admin_conn.getresponse()
        resp.read()
        assert resp.status == 429
        assert ctrl.policy.states['gnb-2'].manual_override is None

        # Envoy polls RDS with old version v1 -> gets v2 (spread-gnb-1 for user, local_backend for probe).
        xds_conn.request('POST', '/v3/discovery:routes', json.dumps({
            'version_info': v1, 'node': {'id': 'envoy-gnb-1'}, 'resource_names': [route_config_name('gnb-1')]
        }))
        resp = xds_conn.getresponse()
        data = json.loads(resp.read())
        v2 = f'{ctrl.xds.epoch}-2'
        assert resp.status == 200 and data['version_info'] == v2
        routes = data['resources'][0]['virtual_hosts'][0]['routes']
        assert routes[1]['route']['cluster'] == 'local_backend'
        assert routes[2]['route']['cluster'] == 'spread-gnb-1'

        # Envoy ACKs version v2 -> receives 304 and active_route updates to SPREAD.
        now[0] = 103.0
        xds_conn.request('POST', '/v3/discovery:routes', json.dumps({
            'version_info': v2, 'node': {'id': 'envoy-gnb-1'}, 'resource_names': [route_config_name('gnb-1')]
        }))
        resp = xds_conn.getresponse()
        resp.read()
        assert resp.status == 304
        assert ctrl.policy.states['gnb-1'].active_route == 'SPREAD'

        now[0] = 105.0
        admin_conn.request('POST', '/override', json.dumps({'site': 'gnb-2', 'route': 'SPREAD'}), auth_headers)
        resp = admin_conn.getresponse()
        resp.read()
        assert resp.status == 429
        now[0] = 108.0
        admin_conn.request('POST', '/override', json.dumps({'site': 'gnb-2', 'route': 'SPREAD'}), auth_headers)
        resp = admin_conn.getresponse()
        resp.read()
        assert resp.status == 200

        # EDS check returns pinned endpoints including weighted spread-gnb-1 (k=2 -> local weight 2, core weight 1).
        xds_conn.request('POST', '/v3/discovery:endpoints', json.dumps({
            'version_info': '', 'resource_names': [local_eds_service_name('gnb-1'), CORE_EDS_SERVICE_NAME, spread_eds_service_name('gnb-1')]
        }))
        resp = xds_conn.getresponse()
        eds = json.loads(resp.read())
        assert resp.status == 200
        by_name = {
            r['cluster_name']: [
                (e['endpoint']['address']['socket_address']['address'], e.get('load_balancing_weight'))
                for e in r['endpoints'][0]['lb_endpoints']
            ]
            for r in eds['resources']
        }
        assert by_name['local-gnb-1'] == [('172.16.4.10', None)]
        assert by_name['core-5g'] == [('172.16.10.50', None)]
        assert by_name['spread-gnb-1'] == [('172.16.10.50', 1), ('172.16.4.10', 2)]

        # Authenticated reset to local-only returns all sites to LOCAL and locks SPREAD overrides (403).
        admin_conn.request('POST', '/reset', json.dumps({'mode': 'local-only'}), auth_headers)
        resp = admin_conn.getresponse()
        res = json.loads(resp.read())
        assert resp.status == 200 and res['reset'] is True and res['mode'] == 'local-only'
        assert ctrl.policy.states['gnb-1'].desired_route == 'LOCAL'
        assert ctrl.policy.states['gnb-1'].active_route == 'UNKNOWN'  # until Envoy ACKs new version

        admin_conn.request('POST', '/override', json.dumps({'site': 'gnb-1', 'route': 'SPREAD'}), auth_headers)
        resp = admin_conn.getresponse()
        resp.read()
        assert resp.status == 403
        xds_conn.close()
        admin_conn.close()
    finally:
        admin_server.shutdown()
        admin_server.server_close()
        xds_server.shutdown()
        xds_server.server_close()


def test_benchmark_client_captures_envoy_route_headers_without_fabricating_pod_on_error():
    class FakeEnvoy(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(503 if self.path == '/fail' else 200)
            self.send_header('X-MEC-Site', 'gnb-2')
            self.send_header('X-MEC-Route', 'core')
            self.send_header('X-MEC-Route-Version', '4')
            if self.path == '/fail':
                self.end_headers()
                return
            body = json.dumps({'person_count': 0, 'persons': [], 'server_node': 'core-5g', 'server_pod': 'core-pod-1'}).encode()
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), FakeEnvoy)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    try:
        ok = attempt(urlsplit(f'http://127.0.0.1:{srv.server_port}/ok'), 'GET', None, {}, '127.0.0.1', 2.0, response_contract='detection')
        assert ok['success'] is True
        assert ok['ingress_site'] == 'gnb-2'
        assert ok['selected_route'] == 'core'
        assert ok['server_node'] == 'core-5g'
        assert ok['server_pod'] == 'core-pod-1'

        fail = attempt(urlsplit(f'http://127.0.0.1:{srv.server_port}/fail'), 'GET', None, {}, '127.0.0.1', 2.0, response_contract='detection')
        assert fail['success'] is False
        assert fail['http_status'] == 503
        assert fail['ingress_site'] == 'gnb-2'
        assert fail['selected_route'] == 'core'
        assert fail['server_node'] is None
        assert fail['server_pod'] is None
    finally:
        srv.shutdown()
        srv.server_close()
        th.join()


def test_campaign_vertical_config_and_four_arm_summary_plot(tmp_path):
    cfg = campaign.load('experiments/campaign-vertical.yaml')
    assert cfg['radio_only_gnb1'] is False
    assert cfg['min_replicas'] == 6
    assert cfg['max_replicas'] == 18
    assert cfg['core_backend_replicas'] == 3
    assert cfg['core_counts_toward_budget'] is True
    assert set(cfg['envoy_site_ports']) == {'gnb-1', 'gnb-2', 'gnb-3'}

    rem = campaign.remote_settings(cfg)
    assert rem['gnb1_dnn'] == 'mec.icn-2.testbed'
    assert rem['gnb1_subnet'] == '172.16.50.0/24'
    assert rem['gnb2_dnn'] == 'embb.gnb2.testbed'
    assert rem['gnb3_dnn'] == 'embb.gnb3.testbed'

    # Create synthetic 4-arm results with phase_end missed slots, timeouts, and unidentified backends.
    for arm, lat, node, missed, timed_out in (
        ('baseline-local', 140.0, 'gnb-2', 4, False),
        ('proposed-local', 140.0, 'gnb-2', 0, False),
        ('baseline-vertical', 120.0, 'core-5g', 0, False),
        ('proposed-vertical', 95.0, 'core-5g', 0, False),
    ):
        arm_dir = tmp_path / arm
        arm_dir.mkdir()
        records = [
            {'event': 'phase_start', 'phase': 'phase_01', 'start_time': 100.0, 'duration_s': 60.0, 'rps_per_ue': 5.0, 'workload_ue': 9, 'gnb1_ues': 1, 'gnb2_ues': 7, 'gnb3_ues': 1},
            {'event': 'request', 'phase': 'phase_01', 'group': 'gnb-2', 'duration_ms': lat, 'success': True, 'server_node': node},
            {'event': 'request', 'phase': 'phase_01', 'group': 'gnb-1', 'duration_ms': 80.0, 'success': True, 'server_node': 'gnb1'},
            {'event': 'request', 'phase': 'phase_01', 'group': 'gnb-3', 'duration_ms': 90.0, 'success': True, 'server_node': None},
            {'event': 'request', 'phase': 'phase_01', 'group': 'gnb-2', 'duration_ms': 10000.0, 'success': False, 'timed_out': True, 'http_status': None, 'server_node': None},
            {
                'event': 'phase_end', 'phase': 'phase_01', 'workload_ue': 9, 'gnb1_ues': 1, 'gnb2_ues': 7, 'gnb3_ues': 1,
                'scheduled': 4 + missed, 'started': 4, 'completed': 4, 'missed': missed,
                'request_coverage': 4 / (4 + missed), 'per_ue': [],
            },
        ]
        (arm_dir / 'requests.jsonl').write_text('\n'.join(json.dumps(r) for r in records) + '\n')
        (arm_dir / 'offload-events.jsonl').write_text(json.dumps({'event': 'route_decision', 'site': 'gnb-2', 'old_route': 'LOCAL', 'new_route': 'CORE', 'timestamp': 110.0}) + '\n')

    summary = summarize_campaign(tmp_path, slo_ms=150.0)
    # baseline-local missed 4 of 8 scheduled slots -> scheduled SLO attainment (3/8=0.375) is much worse than proposed-local (3/4=0.75)
    assert summary['arms']['baseline-local']['missed'] == 4
    assert summary['arms']['baseline-local']['request_coverage'] == 0.5
    assert summary['arms']['baseline-local']['slo_attainment_scheduled'] == 0.375
    assert summary['arms']['proposed-local']['slo_attainment_scheduled'] == 0.75
    assert summary['arms']['baseline-local']['unidentified_backend_successes'] == 1
    assert summary['arms']['baseline-local']['core_served_pct'] == 0.0
    assert summary['arms']['proposed-vertical']['core_served_pct'] == round(100.0 / 3.0, 3)
    assert (tmp_path / 'vertical-4arm-phases.csv').is_file()
    assert (tmp_path / 'vertical-4arm-core-share.csv').is_file()

    plot_path = plot_four_arm(tmp_path, tmp_path / 'plots', slo_ms=150.0)
    assert plot_path.is_file() and plot_path.stat().st_size > 1000

    # Mismatched offered load between arms MUST be rejected before plotting.
    bad_dir = tmp_path / 'mismatch'
    for arm, g2 in (('baseline-local', 7), ('proposed-local', 5)):
        d = bad_dir / arm
        d.mkdir(parents=True)
        recs = [
            {'event': 'phase_start', 'phase': 'phase_01', 'start_time': 100.0, 'duration_s': 60.0, 'rps_per_ue': 5.0, 'workload_ue': 2 + g2, 'gnb1_ues': 1, 'gnb2_ues': g2, 'gnb3_ues': 1},
            {'event': 'request', 'phase': 'phase_01', 'group': 'gnb-2', 'duration_ms': 100.0, 'success': True, 'server_node': 'gnb-2'},
        ]
        (d / 'requests.jsonl').write_text('\n'.join(json.dumps(r) for r in recs) + '\n')
    with pytest.raises(ValueError, match='phase/offered-load mismatch'):
        summarize_campaign(bad_dir, slo_ms=150.0)


def test_unlabeled_pre_header_timeouts_trigger_offload_only_when_local_confirmed():
    cfg = OffloadConfig(min_error_samples=5, offload_error_rate_threshold=0.5, breach_duration_seconds=3.0, min_fresh_updates=2)
    policy = VerticalOffloadPolicy(cfg)
    local_ready = {s: 1 for s in SITES}

    # Confirm LOCAL active on all sites at wall t=1000.0.
    for s in SITES:
        policy.record_envoy_ack(s, 'LOCAL', 'ep-1', 1000.0)

    def _unknown_timeout_snap(site, first_ts, seq, timeouts=6):
        snap = _make_snapshot()
        snap['sites'][site]['user']['local'] = _make_bucket(status='empty', p90_ms=None, attempts=0, successes=0, age=None, seq=0)
        snap['sites'][site]['user']['unknown'] = {
            **_make_bucket(status='no_success', p90_ms=None, attempts=timeouts, successes=0, errors=timeouts, timeouts=timeouts, age=0.1, seq=seq),
            'first_sample_ts': first_ts,
        }
        return snap

    # 1. Timeouts crossing a route change (first_sample_ts < local_confirmed_since=1000.0) MUST NOT trigger.
    for sec in range(0, 6):
        policy.evaluate(float(sec), _unknown_timeout_snap('gnb-2', first_ts=995.0, seq=1 + sec), local_ready, 3)
    assert policy.states['gnb-2'].desired_route == 'LOCAL'

    # 2. Timeouts while Core is unready (core_ready=0) MUST NOT trigger.
    for sec in range(6, 12):
        policy.evaluate(float(sec), _unknown_timeout_snap('gnb-2', first_ts=1005.0, seq=10 + sec), local_ready, 0)
    assert policy.states['gnb-2'].desired_route == 'LOCAL'

    # 3. Consecutive unlabeled timeouts strictly within LOCAL-confirmed interval with Core ready -> ONLY gnb-2 offloads!
    for sec in range(12, 18):
        policy.evaluate(float(sec), _unknown_timeout_snap('gnb-2', first_ts=1005.0, seq=30 + sec), local_ready, 3)
    assert policy.states['gnb-2'].desired_route == 'SPREAD'
    assert policy.states['gnb-2'].last_decision_reason == 'local_unlabeled_timeout_breach'
    assert policy.states['gnb-1'].desired_route == 'LOCAL'
    assert policy.states['gnb-3'].desired_route == 'LOCAL'

    # 4. Timeouts while a route update is pending on gnb-1 MUST NOT trigger.
    policy.states['gnb-1'].pending_update_since = 15.0
    for sec in range(20, 26):
        policy.evaluate(float(sec), _unknown_timeout_snap('gnb-1', first_ts=1005.0, seq=60 + sec), local_ready, 3)
    assert policy.states['gnb-1'].desired_route == 'LOCAL'

    # 5. Timeouts while SPREAD is active on gnb-2 MUST NOT cause erroneous transition.
    policy.record_envoy_ack('gnb-2', 'SPREAD', 'ep-2', 1020.0)
    for sec in range(26, 32):
        policy.evaluate(float(sec), _unknown_timeout_snap('gnb-2', first_ts=1025.0, seq=90 + sec), local_ready, 3)
    assert policy.states['gnb-2'].desired_route == 'SPREAD'


def test_readiness_gate_rejects_unready_local_core_mismatch_and_unacked_reset():
    token = 'gate-secret'
    ctrl = VerticalOffloadController(OffloadConfig(mode='vertical'), collector_snapshot_url='http://127.0.0.1:9/offload/snapshot')
    xds_srv = start_xds_server(ctrl, host='127.0.0.1', port=0)
    adm_srv = start_admin_server(ctrl, token, host='127.0.0.1', port=0)
    try:
        settings = {
            'offloader_url': f'http://127.0.0.1:{adm_srv.server_port}',
            'envoy_site_ports': {'gnb-1': 31801, 'gnb-2': 31802, 'gnb-3': 31803},
            'site_node_ips': {'gnb-1': '10.0.0.1', 'gnb-2': '10.0.0.2', 'gnb-3': '10.0.0.3'},
            'core_backend_replicas': 3,
            'core_counts_toward_budget': True,
            'min_replicas': 6,
            'max_replicas': 18,
        }
        ok_probe = lambda url, headers=None: (200, {'x-mec-site': 'gnb-1' if '31801' in url else ('gnb-2' if '31802' in url else 'gnb-3'), 'x-mec-route': 'core' if (headers or {}).get('X-MEC-Preflight') == 'core' else 'local'}, '{"status":"ok"}')

        # 1. Fails when LOCAL route has not been ACKed yet.
        with pytest.raises(RuntimeError, match='LOCAL route is not confirmed active'):
            campaign.verify_vertical_arm_readiness(settings, 'vertical', token=token, probe_fn=ok_probe)

        # ACK LOCAL for all 3 sites.
        for site in SITES:
            ver = f'{ctrl.xds.epoch}-1'
            ctrl.handle_rds({'version_info': ver, 'node': {'id': f'envoy-{site}'}, 'resource_names': [route_config_name(site)]})

        # 2. Fails when one site has no ready local backend endpoints.
        ctrl.xds.update_endpoints({'gnb-1': [('10.1.0.1', 5000)], 'gnb-2': [('10.1.0.2', 5000)], 'gnb-3': []}, [('10.2.0.1', 5000), ('10.2.0.2', 5000), ('10.2.0.3', 5000)])
        with pytest.raises(RuntimeError, match='gnb-3: no ready local backend endpoints'):
            campaign.verify_vertical_arm_readiness(settings, 'vertical', token=token, probe_fn=ok_probe)

        # 3. Fails when Core ready replicas (2) != configured core_backend_replicas (3).
        ctrl.xds.update_endpoints({'gnb-1': [('10.1.0.1', 5000)], 'gnb-2': [('10.1.0.2', 5000)], 'gnb-3': [('10.1.0.3', 5000)]}, [('10.2.0.1', 5000), ('10.2.0.2', 5000)])
        with pytest.raises(RuntimeError, match='does not match configured core_backend_replicas'):
            campaign.verify_vertical_arm_readiness(settings, 'vertical', token=token, probe_fn=ok_probe)

        # 4. Passes when all 3 local sites and exact 3 Core endpoints are ready + preflight passes.
        ctrl.xds.update_endpoints({'gnb-1': [('10.1.0.1', 5000)], 'gnb-2': [('10.1.0.2', 5000)], 'gnb-3': [('10.1.0.3', 5000)]}, [('10.2.0.1', 5000), ('10.2.0.2', 5000), ('10.2.0.3', 5000)])
        res = campaign.verify_vertical_arm_readiness(settings, 'vertical', token=token, probe_fn=ok_probe)
        assert res['core_backend_actual_ready'] == 3 and res['core_preflight_checked'] is True
    finally:
        adm_srv.shutdown()
        adm_srv.server_close()
        xds_srv.shutdown()
        xds_srv.server_close()


def test_vertical_mode_cli_rejects_missing_site_anchors(tmp_path):
    with pytest.raises(SystemExit) as exc:
        campaign.main(['--config', 'experiments/campaign-vertical.yaml', '--vertical-mode', 'four-arm', '--plan-only', str(tmp_path / 'p')])
    assert exc.value.code == 2


def test_unified_budget_and_dynamic_scaler_6_to_7_to_18_and_back_to_6():
    budget = VerticalBudget(min_replicas=6, max_replicas=18, edge_anchors=3, core_fallback_replicas=3)
    assert budget.fixed_replicas == 6
    assert budget.dynamic_min == 0 and budget.dynamic_max == 12
    assert budget.dynamic_target_for(6) == 0
    assert budget.dynamic_target_for(10) == 4
    assert budget.dynamic_target_for(18) == 12
    assert budget.dynamic_target_for(30) == 12  # clamped at 12 dynamic (18 total)

    with pytest.raises(ValueError):
        VerticalBudget(min_replicas=5, max_replicas=18)
    with pytest.raises(ValueError):
        VerticalBudget(min_replicas=6, max_replicas=18, core_counts_toward_budget=False)

    # Test both baseline (CPU) and proposed (latency) scaling lifecycle: 6 -> 7 -> 18 -> 6.
    for method, hi_one, hi_max, low_sig in (
        ('baseline', 95.0, 600.0, 10.0),
        ('proposed', 180.0, 900.0, 20.0),
    ):
        scaler = VerticalDynamicScaler(
            method,
            budget,
            cpu_target_percent=80.0,
            slo_ms=150.0,
            latency_floor_ms=0.0,
            scale_down_stabilization_seconds=30.0,
            scale_up_cooldown_seconds=5.0,
        )
        # 1. Start at 6 total (3 anchors Ready, 0 dynamic). Signal breaches -> scales up to 7 total (1 dynamic).
        acc0 = PodAccounting(spec_replicas=0, ready_dynamic=0, starting_dynamic=0, terminating_dynamic=0, ready_anchors=3)
        d1 = scaler.evaluate(0.0, acc0, hi_one)
        assert d1['changed'] is True and d1['target_dynamic'] == 1 and d1['target_total'] == 7

        # 2. While the new dynamic pod is starting, rollout guard holds spec_replicas at 1 (no overscaling).
        acc_starting = PodAccounting(spec_replicas=1, ready_dynamic=0, starting_dynamic=1, terminating_dynamic=0, ready_anchors=3)
        d_wait = scaler.evaluate(10.0, acc_starting, hi_max)
        assert d_wait['changed'] is False and d_wait['reason'] == 'wait_starting_pods'

        # 3. Once Ready, extreme load hits max_replicas=18 (clamped at 12 dynamic, never >12).
        acc_ready1 = PodAccounting(spec_replicas=1, ready_dynamic=1, starting_dynamic=0, terminating_dynamic=1, ready_anchors=3)
        d_max = scaler.evaluate(15.0, acc_ready1, hi_max)
        assert d_max['changed'] is True and d_max['target_dynamic'] == 12 and d_max['target_total'] == 18
        assert d_max['accounting']['terminating_dynamic'] == 1

        # 4. Load drops -> waits stabilization window -> scales back down to 0 dynamic (6 total).
        acc_max = PodAccounting(spec_replicas=12, ready_dynamic=12, starting_dynamic=0, terminating_dynamic=0, ready_anchors=3)
        d_hold = scaler.evaluate(20.0, acc_max, low_sig)
        assert d_hold['changed'] is False and d_hold['reason'] == 'scale_down_stabilizing'
        d_down = scaler.evaluate(60.0, acc_max, low_sig)
        assert d_down['changed'] is True and d_down['target_dynamic'] == 0 and d_down['target_total'] == 6

    # Verify pod accounting classifies anchors, ready managed, starting managed, and terminating managed.
    pods = [
        {'metadata': {'labels': {'mec-replica': 'anchor-gnb-1'}}, 'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
        {'metadata': {'labels': {'mec-replica': 'anchor-gnb-2'}}, 'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
        {'metadata': {'labels': {'mec-replica': 'anchor-gnb-3'}}, 'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
        {'metadata': {'labels': {'mec-replica': 'managed'}}, 'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
        {'metadata': {'labels': {'mec-replica': 'managed'}}, 'status': {'phase': 'Pending', 'conditions': []}},
        {'metadata': {'labels': {'mec-replica': 'managed'}, 'deletionTimestamp': '2026-11-20T00:00:00Z'}, 'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
    ]
    acc = account_edge_pods(pods, spec_replicas=2)
    assert (acc.ready_anchors, acc.ready_dynamic, acc.starting_dynamic, acc.terminating_dynamic) == (3, 1, 1, 1)


def test_verify_vertical_path_helpers():
    assert check_gnb1_capacity({'metadata': {'name': 'gnb1'}, 'status': {'allocatable': {'cpu': '2', 'memory': '4009836Ki'}}})['meets_4vcpu_8gb'] is False
    assert check_gnb1_capacity({'metadata': {'name': 'gnb1'}, 'status': {'allocatable': {'cpu': '4', 'memory': '8192000Ki'}}})['meets_4vcpu_8gb'] is True

    pin = verify_eds_pinning(
        {'xds': {'endpoints': {'local-gnb-1': ['172.16.4.10:5000'], 'core-5g': ['172.16.10.50:5000']}}},
        {'items': [
            {'spec': {'nodeName': 'gnb1'}, 'status': {'podIP': '172.16.4.10'}},
            {'spec': {'nodeName': 'core-5g'}, 'status': {'podIP': '172.16.10.50'}},
        ]},
    )
    assert pin['ok'] is True

    smf_ok = verify_smf_sessions({'items': [{'supi': 'imsi-1', 'pdu': [{'pdu_state': 'active', 'ipv4': '172.16.50.251', 'n3': {'upf': {'addr': '172.16.30.11'}}}]}]})
    assert smf_ok['ok'] is True

    smf_bad = verify_smf_sessions({'items': [{'supi': 'imsi-1', 'pdu': [{'pdu_state': 'active', 'ipv4': '172.16.45.10', 'n3': {'upf': {'addr': '172.16.10.7'}}}]}]})
    assert smf_bad['ok'] is False


def test_envoy_and_core_manifests_parse_cleanly():
    envoy_docs = list(yaml.safe_load_all(Path('offloader/envoy-ingress.yaml').read_text()))
    assert len(envoy_docs) == 9  # 3 sites * (ConfigMap + Deployment + Service)
    for doc in envoy_docs:
        if doc['kind'] == 'Service':
            assert doc['spec']['externalTrafficPolicy'] == 'Local'
        elif doc['kind'] == 'ConfigMap':
            envoy_cfg = yaml.safe_load(doc['data']['envoy.yaml'])
            clusters = {c['name']: c for c in envoy_cfg['static_resources']['clusters']}
            assert clusters['local_backend']['type'] == 'EDS'
            assert clusters['core_backend']['type'] == 'EDS'
            site = doc['metadata']['name'].removeprefix('mec-envoy-').removesuffix('-config')
            spread_name = spread_eds_service_name(site)
            assert clusters[spread_name]['type'] == 'EDS'
            assert clusters[spread_name]['eds_cluster_config']['service_name'] == spread_name

    core_doc = yaml.safe_load(Path('offloader/core-backend.yaml').read_text())
    assert core_doc['spec']['replicas'] == 3
    assert core_doc['spec']['template']['spec']['nodeSelector'] == {'kubernetes.io/hostname': 'core-5g'}


def test_four_arm_budget_and_native_hpa_plan(tmp_path):
    # Render four-arm, standalone local-only, and standalone vertical offline plans.
    plan_four = tmp_path / 'plan-four'
    campaign.main([
        '--config', 'experiments/campaign-vertical.yaml',
        '--vertical-mode', 'four-arm',
        '--comparison', 'full',
        '--scenario', 'mobility9',
        '--prefer-close',
        '--site-anchors',
        '--min-replicas', '6',
        '--max-replicas', '18',
        '--plan-only', str(plan_four),
    ])
    p4 = yaml.safe_load((plan_four / 'plan.yaml').read_text())
    assert [p4['arm_budgets'][arm]['core_backend_replicas'] for arm, _, _ in campaign.FOUR_ARM_SPECS] == [0, 0, 3, 3]
    assert all(budget['max_replicas'] == 18 for budget in p4['arm_budgets'].values())
    assert [p4['arm_budgets'][arm]['dynamic_max_replicas']
            for arm, _, _ in campaign.FOUR_ARM_SPECS] == [15, 15, 12, 12]
    assert [p4['arm_budgets'][arm]['initial_total_replicas'] for arm, _, _ in campaign.FOUR_ARM_SPECS] == [3, 3, 6, 6]
    assert p4['autoscaler'] == 'hpa'
    assert p4['hpa_targets']['baseline']['target'] == 80
    assert p4['hpa_targets']['proposed']['type'] == 'asymmetric_latency_p90_ms'
    assert p4['rt_metric_name'] == 'ue_p90_asymmetric_120ms_target_150ms_seconds'
    assert p4['rt_hpa_target_value'] == '150m'
    assert p4['pod_share_scoring'] is True
    assert p4['deschedule_min_score_improvement'] == 0.05
    assert (plan_four / 'floor-adapter-configmap.yaml').is_file()
    assert p4['eligible_nodes'] == ['gnb1', 'gnb-2', 'gnb-3']

    for arm, method, mode in campaign.FOUR_ARM_SPECS:
        arm_dir = plan_four / arm
        expected_nodes = ['gnb1', 'gnb-2', 'gnb-3'] + (['core-5g'] if mode == 'vertical' else [])
        ctrl_cfg = yaml.safe_load((arm_dir / f'{method}-controller.yaml').read_text())
        assert ctrl_cfg['eligible_nodes'] == expected_nodes
        assert p4['eligible_nodes_by_arm'][arm] == expected_nodes
        expected_max = 12 if mode == 'vertical' else 15
        assert (ctrl_cfg['min_replicas'], ctrl_cfg['max_replicas']) == (1, expected_max)
        hpa = yaml.safe_load((arm_dir / f'{method}-hpa.yaml').read_text())['spec']
        assert (hpa['minReplicas'], hpa['maxReplicas']) == (1, expected_max)
        assert p4['hpa_managed_bounds_by_arm'][arm] == {'min': 1, 'max': expected_max}
        docs = [d for d in yaml.safe_load_all((arm_dir / f'{method}-workload.yaml').read_text()) if d]
        dep = next(d for d in docs if d['kind'] == 'Deployment')
        assert dep['spec']['replicas'] == 0
        terms = dep['spec']['template']['spec']['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms']
        allowed_nodes = [t['matchFields'][0]['values'][0] for t in terms]
        assert allowed_nodes == expected_nodes

    anchors = [d for d in yaml.safe_load_all((plan_four / 'site-anchors.yaml').read_text()) if d]
    assert len(anchors) == 3
    assert {d['spec']['template']['spec']['nodeSelector']['kubernetes.io/hostname'] for d in anchors} == {'gnb1', 'gnb-2', 'gnb-3'}

    # Standalone local-only and standalone vertical derive consistent arm budgets from the same flags.
    plan_loc = tmp_path / 'plan-local'
    campaign.main([
        '--config', 'experiments/campaign-vertical.yaml',
        '--vertical-mode', 'local-only',
        '--site-anchors',
        '--plan-only', str(plan_loc),
    ])
    pl = yaml.safe_load((plan_loc / 'plan.yaml').read_text())
    assert pl['vertical_budget']['core_backend_replicas'] == 0
    assert pl['vertical_budget']['min_replicas'] == 3 and pl['vertical_budget']['max_replicas'] == 18
    assert pl['vertical_budget']['dynamic_max_replicas'] == 15
    assert pl['initial_replicas'] == 3 and pl['autoscaler'] == 'hpa'

    plan_ver = tmp_path / 'plan-vertical'
    campaign.main([
        '--config', 'experiments/campaign-vertical.yaml',
        '--vertical-mode', 'vertical',
        '--site-anchors',
        '--plan-only', str(plan_ver),
    ])
    pv = yaml.safe_load((plan_ver / 'plan.yaml').read_text())
    assert pv['vertical_budget']['core_backend_replicas'] == 3
    assert pv['vertical_budget']['min_replicas'] == 6 and pv['vertical_budget']['max_replicas'] == 18
    assert pv['vertical_budget']['dynamic_max_replicas'] == 12
    assert pv['initial_replicas'] == 6 and pv['autoscaler'] == 'hpa'


def test_readiness_local_only_empty_core_eds_and_startup_rejections(monkeypatch):
    token = 'gate-secret'
    ctrl = VerticalOffloadController(OffloadConfig(mode='local-only'), collector_snapshot_url='http://127.0.0.1:9/offload/snapshot')
    xds_srv = start_xds_server(ctrl, host='127.0.0.1', port=0)
    adm_srv = start_admin_server(ctrl, token, host='127.0.0.1', port=0)
    try:
        settings = {
            'offloader_url': f'http://127.0.0.1:{adm_srv.server_port}',
            'envoy_site_ports': {'gnb-1': 31801, 'gnb-2': 31802, 'gnb-3': 31803},
            'site_node_ips': {'gnb-1': '10.0.0.1', 'gnb-2': '10.0.0.2', 'gnb-3': '10.0.0.3'},
            'core_backend_replicas': 3,
            'core_counts_toward_budget': True,
            'min_replicas': 6,
            'max_replicas': 18,
        }
        probed_headers = []

        def ok_probe(url, headers=None):
            probed_headers.append((url, dict(headers or {})))
            site = 'gnb-1' if '31801' in url else ('gnb-2' if '31802' in url else 'gnb-3')
            route = 'core' if (headers or {}).get('X-MEC-Preflight') == 'core' else 'local'
            return 200, {'x-mec-site': site, 'x-mec-route': route}, '{"status":"ok"}'

        for site in SITES:
            ver = f'{ctrl.xds.epoch}-1'
            ctrl.handle_rds({'version_info': ver, 'node': {'id': f'envoy-{site}'}, 'resource_names': [route_config_name(site)]})

        # 1. local-only PASSES when all 3 edge backends are Ready and Core EDS is empty (no Core preflight).
        ctrl.xds.update_endpoints(
            {'gnb-1': [('10.1.0.1', 5000)], 'gnb-2': [('10.1.0.2', 5000)], 'gnb-3': [('10.1.0.3', 5000)]},
            [],
        )
        res = campaign.verify_vertical_arm_readiness(
            settings, 'local-only', token=token, probe_fn=ok_probe, arm_name='baseline-local',
        )
        assert res['core_backend_expected_replicas'] == 0
        assert res['core_backend_actual_ready'] == 0
        assert res['core_preflight_checked'] is False
        assert not any(h.get('X-MEC-Preflight') == 'core' for _, h in probed_headers)

        # 2. local-only REJECTS startup if Core EDS still has any endpoint.
        ctrl.xds.update_endpoints(
            {'gnb-1': [('10.1.0.1', 5000)], 'gnb-2': [('10.1.0.2', 5000)], 'gnb-3': [('10.1.0.3', 5000)]},
            [('10.2.0.1', 5000)],
        )
        with pytest.raises(RuntimeError, match=r'baseline-local:.*local-only arm still has 1 Core endpoint'):
            campaign.verify_vertical_arm_readiness(
                settings, 'local-only', token=token, probe_fn=ok_probe, arm_name='baseline-local',
            )

        # 3. vertical REJECTS startup when Core Ready pods are short of 3 (in EDS and in K8s pod check).
        ctrl.xds.update_endpoints(
            {'gnb-1': [('10.1.0.1', 5000)], 'gnb-2': [('10.1.0.2', 5000)], 'gnb-3': [('10.1.0.3', 5000)]},
            [('10.2.0.1', 5000), ('10.2.0.2', 5000)],
        )
        with pytest.raises(RuntimeError, match=r'baseline-vertical:.*Core backend ready replicas \(2\) does not match configured core_backend_replicas \(3\)'):
            campaign.verify_vertical_arm_readiness(
                settings, 'vertical', token=token, probe_fn=ok_probe,
                arm_name='baseline-vertical', expected_offloader_mode='local-only',
            )

        # 4. wait_core_pool_scaled rejects vertical startup when only 2/3 Core pods are Ready on core-5g.
        def fake_kube(*args, **kwargs):
            if 'rollout' in args:
                return subprocess.CompletedProcess(args, 0, stdout='', stderr='')
            pods = {
                'items': [
                    {'metadata': {'name': 'core-1'}, 'spec': {'nodeName': 'core-5g'}, 'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
                    {'metadata': {'name': 'core-2'}, 'spec': {'nodeName': 'core-5g'}, 'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}},
                    {'metadata': {'name': 'core-3'}, 'spec': {'nodeName': 'core-5g'}, 'status': {'phase': 'Pending', 'conditions': []}},
                ]
            }
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(pods), stderr='')

        monkeypatch.setattr(campaign, 'kube', fake_kube)
        with pytest.raises(RuntimeError, match=r'proposed-vertical:.*reached 2/3 Ready pods on core-5g'):
            campaign.wait_core_pool_scaled(
                'mec-placement-pedestrian-detection', 3,
                arm_name='proposed-vertical', timeout=0.05, poll_interval=0.01,
            )

        # 5. Missing Core fallback Deployment fails startup with arm name and Deployment name.
        def missing_get(kind, name, namespace=campaign.NAMESPACE):
            raise subprocess.CalledProcessError(1, ['kubectl'], stderr='Error from server (NotFound): deployments.apps "mec-placement-pedestrian-detection-core" not found')

        monkeypatch.setattr(campaign, 'get', missing_get)
        with pytest.raises(RuntimeError, match=r'baseline-local:.*mec-placement-pedestrian-detection-core is missing'):
            campaign.prepare_vertical_arm(
                settings, 'mec-placement-pedestrian-detection', 'local-only',
                campaign.vertical_budget_from_settings(settings, arm_mode='local-only'),
                token=token, arm_name='baseline-local', probe_fn=ok_probe,
            )
    finally:
        adm_srv.shutdown()
        adm_srv.server_close()
        xds_srv.shutdown()
        xds_srv.server_close()


def test_four_arm_core_pool_sequence_0_0_3_3_and_strict_mode_order(monkeypatch):
    token = 'seq-secret'
    ctrl = VerticalOffloadController(OffloadConfig(mode='vertical'), collector_snapshot_url='http://127.0.0.1:9/offload/snapshot')
    xds_srv = start_xds_server(ctrl, host='127.0.0.1', port=0)
    adm_srv = start_admin_server(ctrl, token, host='127.0.0.1', port=0)
    try:
        settings = {
            'offloader_url': f'http://127.0.0.1:{adm_srv.server_port}',
            'envoy_site_ports': {'gnb-1': 31801, 'gnb-2': 31802, 'gnb-3': 31803},
            'site_node_ips': {'gnb-1': '10.0.0.1', 'gnb-2': '10.0.0.2', 'gnb-3': '10.0.0.3'},
            'core_backend_replicas': 3,
            'core_counts_toward_budget': True,
            'min_replicas': 6,
            'max_replicas': 18,
        }
        core_scale_sequence = []
        timeline = []
        pool_state = {'replicas': 3}

        def ack_all_local():
            for site in SITES:
                ver = ctrl.xds.status_snapshot()['sites'][site]['desired_version']
                ctrl.handle_rds({'version_info': ver, 'node': {'id': f'envoy-{site}'}, 'resource_names': [route_config_name(site)]})

        real_reset = campaign.reset_offloader

        def tracked_reset(url, mode, *, token, wait_ack_seconds=10.0):
            timeline.append(('reset', mode))
            base = url.rstrip('/')
            campaign._offloader_request(f'{base}/reset', token=token, method='POST', payload={'mode': mode})
            ack_all_local()
            return campaign._offloader_request(f'{base}/status', token=token)

        def fake_get_core(app, *, arm_name=None):
            return pool_state['replicas']

        def fake_scale_core(app, replicas, *, arm_name=None):
            timeline.append(('scale_core', arm_name, replicas, ctrl.get_status()['mode']))
            core_scale_sequence.append(replicas)
            pool_state['replicas'] = replicas
            core_eps = [(f'10.2.0.{i + 1}', 5000) for i in range(replicas)]
            ctrl.xds.update_endpoints(
                {'gnb-1': [('10.1.0.1', 5000)], 'gnb-2': [('10.1.0.2', 5000)], 'gnb-3': [('10.1.0.3', 5000)]},
                core_eps,
            )

        def fake_wait_core(app, expected_replicas, *, arm_name=None, core_node='core-5g', timeout=180, poll_interval=1.0):
            assert pool_state['replicas'] == expected_replicas
            return {'deployment': f'{app}-core', 'expected_replicas': expected_replicas, 'ready_pods_on_core': expected_replicas}

        real_set_mode = campaign.set_offloader_mode

        def tracked_set_mode(url, mode, *, token, arm_name=None, **kwargs):
            timeline.append(('set_mode', arm_name, mode))
            return real_set_mode(url, mode, token=token, arm_name=arm_name, **kwargs)

        def ok_probe(url, headers=None):
            site = 'gnb-1' if '31801' in url else ('gnb-2' if '31802' in url else 'gnb-3')
            route = 'core' if (headers or {}).get('X-MEC-Preflight') == 'core' else 'local'
            timeline.append(('probe', site, route, ctrl.get_status()['mode']))
            return 200, {'x-mec-site': site, 'x-mec-route': route}, '{"status":"ok"}'

        monkeypatch.setattr(campaign, 'reset_offloader', tracked_reset)
        monkeypatch.setattr(campaign, 'get_core_pool_replicas', fake_get_core)
        monkeypatch.setattr(campaign, 'scale_core_pool', fake_scale_core)
        monkeypatch.setattr(campaign, 'wait_core_pool_scaled', fake_wait_core)
        monkeypatch.setattr(campaign, 'set_offloader_mode', tracked_set_mode)

        for arm_name, _method, arm_vmode in campaign.FOUR_ARM_SPECS:
            arm_budget = campaign.vertical_budget_from_settings(settings, arm_mode=arm_vmode)
            assert arm_budget.dynamic_max == (12 if arm_vmode == 'vertical' else 15)
            readiness = campaign.prepare_vertical_arm(
                settings, 'mec-placement-pedestrian-detection', arm_vmode, arm_budget,
                token=token, arm_name=arm_name, probe_fn=ok_probe,
            )
            assert readiness['mode'] == arm_vmode
            assert readiness['core_backend_actual_ready'] == (0 if arm_vmode == 'local-only' else 3)

        assert core_scale_sequence == [0, 0, 3, 3]

        # For vertical arms: scale_core(3) and core preflight probes happen while mode is still 'local-only',
        # and set_mode('vertical') happens strictly AFTER core preflight probes succeed.
        for arm_name in ('baseline-vertical', 'proposed-vertical'):
            scale_idx = next(i for i, ev in enumerate(timeline) if ev[0] == 'scale_core' and ev[1] == arm_name)
            mode_idx = next(i for i, ev in enumerate(timeline) if ev[0] == 'set_mode' and ev[1] == arm_name)
            assert timeline[scale_idx][3] == 'local-only'
            probes_between = [ev for ev in timeline[scale_idx:mode_idx] if ev[0] == 'probe' and ev[2] == 'core']
            assert len(probes_between) == 3
            assert all(ev[3] == 'local-only' for ev in probes_between)
            assert timeline[mode_idx] == ('set_mode', arm_name, 'vertical')

        # Fixed mobility can start with an additional managed Core replica in EDS.
        settings.update(no_hpa=True, schedule='18:14:2:2,18:2:14:2,18:2:2:14', rps_per_ue=5)
        original_wait = campaign.wait_core_pool_scaled

        def fixed_wait(app, expected, **kwargs):
            state = original_wait(app, expected, **kwargs)
            ctrl.xds.update_endpoints(
                {'gnb-1': [('10.1.0.1', 5000)], 'gnb-2': [('10.1.0.2', 5000)], 'gnb-3': [('10.1.0.3', 5000)]},
                [(f'10.2.0.{i}', 5000) for i in range(1, 5)],
            )
            return state

        monkeypatch.setattr(campaign, 'wait_core_pool_scaled', fixed_wait)
        ready = campaign.prepare_vertical_arm(settings, 'test-app', 'vertical',
                                              campaign.vertical_budget_from_settings(settings, min_replicas=9, max_replicas=9),
                                              token=token, arm_name='mobility', probe_fn=ok_probe)
        assert ready['core_backend_actual_ready'] == 4
        real_set_mode(settings['offloader_url'], 'local-only', token=token)
        assert ctrl.get_status()['mode'] == 'local-only'
    finally:
        adm_srv.shutdown()
        adm_srv.server_close()
        xds_srv.shutdown()
        xds_srv.server_close()


def test_cleanup_mid_transition_failure_ctrl_c_and_restore_error(tmp_path, monkeypatch, capsys):
    cfg = campaign.load('experiments/campaign-vertical.yaml')
    cfg['site_anchors'] = True
    cfg['prefer_close'] = True
    cfg['offloader_token_file'] = str(tmp_path / 'token')
    (tmp_path / 'token').write_text('cleanup-secret\n', encoding='utf-8')
    root = tmp_path / 'camp'
    root.mkdir()
    campaign._campaign_manifest(root, False, 'pedestrian-detection', prefer_close=True, site_anchors=True, site_nodes=campaign.LOCAL_SITE_NODES)

    # Common lightweight stubs for run_method.
    monkeypatch.setattr(campaign, 'check_node_capacity', lambda *a, **k: None)
    monkeypatch.setattr(campaign, '_wait_initial_replicas_ready', lambda *a, **k: None)
    monkeypatch.setattr(campaign, '_wait_site_anchor_endpoints', lambda *a, **k: None)
    monkeypatch.setattr(campaign, 'check_service_routing', lambda *a, **k: None)
    monkeypatch.setattr(campaign, 'fetch_offloader_artifacts', lambda *a, **k: None)
    monkeypatch.setattr(campaign, 'get', lambda kind, name, ns=campaign.NAMESPACE: {'spec': {'ports': [{'nodePort': 30500}], 'replicas': 2}, 'status': {'readyReplicas': 1}})
    monkeypatch.setattr(campaign, 'kube', lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout='{"items":[]}', stderr=''))

    # --- Subtest A: Failure mid-mode-transition (after scaling Core to 3, before mode='vertical' finishes) ---
    events_a = []
    monkeypatch.setattr(campaign, 'command', lambda args, **k: events_a.append(('cmd', args)))
    monkeypatch.setattr(campaign, 'get_core_pool_replicas', lambda app, arm_name=None: 2)
    monkeypatch.setattr(campaign, 'scale_core_pool', lambda app, reps, arm_name=None: events_a.append(('scale_core', reps)))
    monkeypatch.setattr(campaign, 'wait_core_pool_scaled', lambda app, reps, arm_name=None, **k: events_a.append(('wait_core', reps)) or {})
    monkeypatch.setattr(campaign, 'wait_offloader_core_endpoints', lambda *a, **k: ['10.2.0.1:5000', '10.2.0.2:5000', '10.2.0.3:5000'])
    monkeypatch.setattr(campaign, 'verify_vertical_arm_readiness', lambda *a, **k: {'mode': 'local-only'})
    monkeypatch.setattr(campaign, 'reset_offloader', lambda url, mode, *, token, **k: events_a.append(('reset_offloader', mode)) or {})

    def fail_set_mode(*a, **k):
        events_a.append(('set_mode_fail',))
        raise RuntimeError('baseline-vertical: mode switch failed mid-transition')

    monkeypatch.setattr(campaign, 'set_offloader_mode', fail_set_mode)

    class DummyProc:
        def __init__(self, cmd):
            self.cmd = cmd
            self.returncode = 0
            self.stdin = type('S', (), {'write': lambda self, s: None, 'close': lambda self: None})()

        def poll(self):
            return None

        def terminate(self):
            events_a.append(('terminate', self.cmd[2] if len(self.cmd) > 2 else self.cmd[0]))

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(campaign.subprocess, 'Popen', lambda cmd, **k: DummyProc(cmd))

    with pytest.raises(RuntimeError, match='mode switch failed mid-transition'):
        campaign.run_method(
            cfg, 'camp-a', root, 'baseline', None, 150, 18,
            comparison='full', autoscaler='hpa', placement_engine='go',
            min_replicas=6, output_name='baseline-vertical-a', vertical_mode='vertical',
            restore_core_pool=True,
        )

    fail_idx = events_a.index(('set_mode_fail',))
    cleanup_events_a = events_a[fail_idx + 1:]
    reset_idx_a = cleanup_events_a.index(('reset_offloader', 'local-only'))
    restore_idx_a = cleanup_events_a.index(('scale_core', 2))
    assert reset_idx_a < restore_idx_a, f'LOCAL reset must precede Core pool reduction: {cleanup_events_a}'

    # --- Subtest B: Ctrl+C (KeyboardInterrupt) during active UE traffic ---
    events_b = []
    monkeypatch.setattr(campaign, 'command', lambda args, **k: events_b.append(('ue_stop' if 'campaign_ue stop' in ' '.join(map(str, args)) else 'cmd', args)))
    monkeypatch.setattr(campaign, 'get_core_pool_replicas', lambda app, arm_name=None: 1)
    monkeypatch.setattr(campaign, 'scale_core_pool', lambda app, reps, arm_name=None: events_b.append(('scale_core', reps)))
    monkeypatch.setattr(campaign, 'wait_core_pool_scaled', lambda app, reps, arm_name=None, **k: events_b.append(('wait_core', reps)) or {})
    monkeypatch.setattr(campaign, 'reset_offloader', lambda url, mode, *, token, **k: events_b.append(('reset_offloader', mode)) or {})
    monkeypatch.setattr(campaign, 'set_offloader_mode', lambda url, mode, *, token, arm_name=None: events_b.append(('set_mode', mode)) or {'mode': mode})

    class CtrlCProc:
        def __init__(self, cmd):
            self.cmd = cmd
            self.returncode = None
            self.stdin = type('S', (), {'write': lambda self, s: None, 'close': lambda self: None})()

        def poll(self):
            if any('campaign_ue' in str(x) for x in self.cmd):
                events_b.append(('ctrl_c',))
                raise KeyboardInterrupt()
            return None

        def terminate(self):
            events_b.append(('terminate', str(self.cmd)))

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(campaign.subprocess, 'Popen', lambda cmd, **k: CtrlCProc(cmd))

    with pytest.raises(KeyboardInterrupt):
        campaign.run_method(
            cfg, 'camp-b', root, 'baseline', None, 150, 18,
            comparison='full', autoscaler='hpa', placement_engine='go',
            min_replicas=6, output_name='baseline-vertical-b', vertical_mode='vertical',
            restore_core_pool=True,
        )

    cc_idx = events_b.index(('ctrl_c',))
    cleanup_events_b = events_b[cc_idx + 1:]
    ue_stop_idx = next(i for i, ev in enumerate(cleanup_events_b) if ev[0] == 'ue_stop')
    reset_idx_b = cleanup_events_b.index(('reset_offloader', 'local-only'))
    restore_idx_b = cleanup_events_b.index(('scale_core', 1))
    assert ue_stop_idx < reset_idx_b < restore_idx_b, f'Expected UE stop -> LOCAL reset -> Core restore: {cleanup_events_b}'

    # --- Subtest C: Restore failure reports clear error to stderr, file, and raises ---
    def broken_scale(app, reps, arm_name=None):
        raise RuntimeError('kubectl scale timed out')

    monkeypatch.setattr(campaign, 'scale_core_pool', broken_scale)
    err_dir = tmp_path / 'restore-err'
    err_dir.mkdir()
    with pytest.raises(RuntimeError, match=r'Failed to restore Core fallback pool services/mec-placement-pedestrian-detection-core to initial replicas \(3\)'):
        campaign.cleanup_vertical_state(
            cfg, 'mec-placement-pedestrian-detection',
            token='cleanup-secret', initial_core_replicas=3,
            output_dir=err_dir, arm_name='test-camp',
            restore_core_pool=True, raise_on_error=True,
        )
    assert (err_dir / 'core-pool-restore-error.txt').is_file()
    assert 'initial replicas (3)' in (err_dir / 'core-pool-restore-error.txt').read_text()
    assert 'Failed to restore Core fallback pool' in capsys.readouterr().err


def test_cleanup_does_not_reduce_core_pool_without_local_ack(monkeypatch):
    from experiments import campaign

    def fail_reset(*args, **kwargs):
        raise RuntimeError('Envoy did not acknowledge LOCAL')

    monkeypatch.setattr(campaign, 'reset_offloader', fail_reset)
    monkeypatch.setattr(campaign, 'get_core_pool_replicas', lambda *args, **kwargs: 3)
    monkeypatch.setattr(campaign, 'scale_core_pool', lambda *args, **kwargs:
                        pytest.fail('Core capacity reduced without LOCAL acknowledgement'))
    with pytest.raises(RuntimeError, match='LOCAL routes must be acknowledged'):
        campaign.cleanup_vertical_state(
            {'offloader_url': 'http://localhost'}, 'pedestrian',
            token='test-token', initial_core_replicas=0, restore_core_pool=True,
        )


def test_ready_dynamic_core_pod_joins_core_and_spread_without_duplicates():
    def pod(app, role, node, ip, ready=True, terminating=False):
        meta = {'labels': {'app': app}}
        if role is not None:
            meta['labels']['mec-replica'] = role
        if terminating:
            meta['deletionTimestamp'] = '2026-11-21T00:00:00Z'
        return {
            'metadata': meta,
            'spec': {'nodeName': node},
            'status': {'phase': 'Running', 'podIP': ip, 'conditions': [
                {'type': 'Ready', 'status': 'True' if ready else 'False'},
            ]},
        }

    local, core = filter_pinned_endpoints([
        pod('mec-placement-pedestrian-detection-core', None, 'core-5g', '172.16.10.10'),
        pod('mec-placement-pedestrian-detection', 'managed', 'core-5g', '172.16.10.11'),
        pod('mec-placement-pedestrian-detection', 'managed', 'core-5g', '172.16.10.11'),  # duplicate IP
        pod('mec-placement-pedestrian-detection', 'managed', 'core-5g', '172.16.10.12', ready=False),
        pod('mec-placement-pedestrian-detection', 'managed', 'core-5g', '172.16.10.14', terminating=True),
        pod('mec-placement-pedestrian-detection', 'anchor-gnb-1', 'core-5g', '172.16.10.13'),
        pod('mec-placement-pedestrian-detection', 'managed', 'gnb1', '172.16.20.11'),
        pod('mec-placement-pedestrian-detection', 'managed', 'gnb1', '172.16.20.12', terminating=True),
    ])
    assert core == [('172.16.10.10', 5000), ('172.16.10.11', 5000)]
    assert local['gnb-1'] == [('172.16.20.11', 5000)]

    # Verify both core-5g and spread-gnb-1 include the managed core-5g pod with valid weights and no duplicate IPs.
    xds = XdsState(epoch='ep-dup')
    xds.update_endpoints(local, core)
    xds.set_site_route('gnb-1', 'SPREAD')
    xds.set_site_route('gnb-2', 'SPREAD')
    status, body = xds.handle_eds({
        'version_info': '',
        'resource_names': [CORE_EDS_SERVICE_NAME, spread_eds_service_name('gnb-1')],
    })
    assert status == 200
    res_map = {r['cluster_name']: r['endpoints'][0]['lb_endpoints'] for r in body['resources']}
    core_ips = [e['endpoint']['address']['socket_address']['address'] for e in res_map[CORE_EDS_SERVICE_NAME]]
    assert core_ips == ['172.16.10.10', '172.16.10.11']
    spread_entries = [
        (e['endpoint']['address']['socket_address']['address'], e['load_balancing_weight'])
        for e in res_map[spread_eds_service_name('gnb-1')]
    ]
    assert len(spread_entries) == len({ip for ip, _ in spread_entries})
    assert spread_entries == [
        ('172.16.10.10', 1),
        ('172.16.10.11', 1),
        ('172.16.20.11', 2),
    ]
    with pytest.raises(ValueError, match='load_balancing_weight must be >= 1'):
        build_cluster_load_assignment('spread-gnb-1', [('172.16.10.10', 5000, 0)])


def test_second_site_enters_spread_without_core_overload_gate():
    policy = VerticalOffloadPolicy(OffloadConfig(breach_duration_seconds=1, min_dwell_seconds=1, route_switch_interval_seconds=1))
    wall_start = 1700000000.0
    gnb1 = policy.states['gnb-1']
    gnb1.desired_route = gnb1.active_route = 'SPREAD'
    gnb1.spread_confirmed_since = wall_start

    # Even while gnb-1 overall p90 is very high (>SLO), gnb-2 is NOT blocked by any core_overloaded gate;
    # weighting by k replaces that gate.
    seen = []
    for sec in range(20, 23):
        snapshot = _make_snapshot({
            'gnb-1': {'user_overall': _make_bucket(p90_ms=1800.0, seq=sec)},
            'gnb-2': {'user_local': _make_bucket(p90_ms=700.0, seq=sec)},
        })
        snapshot['generated_at'] = wall_start + sec
        seen += [e for e in policy.evaluate(float(sec), snapshot, dict.fromkeys(SITES, 2), 3) if e.site == 'gnb-2']
    rds = [e for e in seen if e.event == 'route_decision']
    assert len(rds) == 1 and rds[0].new_route == 'SPREAD'
    assert rds[0].extra['k'] == 2 and rds[0].extra['c'] == 3 and rds[0].extra['x_i'] == 2
    assert rds[0].extra['core_ratio'] == pytest.approx(3 / 7, abs=1e-6)


def test_spread_state_hold_reasons_logged_once_with_spread_metrics():
    policy = VerticalOffloadPolicy(OffloadConfig())
    st = policy.states['gnb-2']
    st.desired_route = st.active_route = 'SPREAD'
    wall_start = 1700000000.0
    st.spread_confirmed_since = wall_start
    events = []
    for sec in range(20, 30):
        snapshot = _make_snapshot({
            'gnb-2': {'user_overall': _make_bucket(p90_ms=220.0, seq=sec)},
        })
        snapshot['generated_at'] = wall_start + sec
        events += [e for e in policy.evaluate(float(sec), snapshot, dict.fromkeys(SITES, 3), 3) if e.site == 'gnb-2']
    holds = [e for e in events if e.reason == 'spread_p90_above_slo']
    assert len(holds) == 1
    record = holds[0].to_dict()
    assert record['c'] == 3 and record['x_i'] == 3 and record['k'] == 1
    assert record['core_ratio'] == pytest.approx(0.5, abs=1e-6)


def test_enter_spread_has_priority_over_exit_to_local_on_same_tick():
    cfg = OffloadConfig(breach_duration_seconds=1.0, min_fresh_updates=2, min_dwell_seconds=5.0, route_switch_interval_seconds=5.0)
    policy = VerticalOffloadPolicy(cfg)
    wall_start = 1700000000.0
    local_ready = {'gnb-1': 2, 'gnb-2': 2, 'gnb-3': 2}

    # gnb-2 is already in SPREAD and ready to exit; gnb-3 breaches LOCAL SLO on the same tick.
    st2 = policy.states['gnb-2']
    st2.desired_route = st2.active_route = 'SPREAD'
    st2.last_transition_time = 0.0
    st2.spread_confirmed_since = wall_start

    for sec in (20, 21):
        snap = _make_snapshot({
            'gnb-2': {'user_overall': _make_bucket(p90_ms=90.0, seq=sec)},
            'gnb-3': {'user_local': _make_bucket(p90_ms=260.0, seq=sec)},
        })
        snap['generated_at'] = wall_start + sec
        evs = [e for e in policy.evaluate(float(sec), snap, local_ready, 3) if e.event == 'route_decision']

    # SLO protection (entering SPREAD) MUST win over exiting SPREAD on the same tick.
    assert [(e.site, e.old_route, e.new_route) for e in evs] == [('gnb-3', 'LOCAL', 'SPREAD')]
    assert policy.states['gnb-3'].desired_route == 'SPREAD'
    assert policy.states['gnb-2'].desired_route == 'SPREAD'
    assert policy.states['gnb-2'].last_status_note == 'route_switch_waiting'


def test_verify_vertical_path_smoke_manual_uses_spread(monkeypatch, capsys):
    from types import SimpleNamespace
    import utils.verify_vertical_path as vvp

    sent_routes = []

    def fake_http(url, *, method='GET', payload=None, token=None, timeout=5.0):
        sent_routes.append(payload['route'])
        return {'ok': True}

    def fake_attempt(*args, **kwargs):
        cur = sent_routes[-1]
        if cur == 'SPREAD':
            return {'success': True, 'ingress_site': 'gnb-1', 'selected_route': 'spread', 'server_node': 'core-5g', 'server_pod': 'p-core', 'http_status': 200, 'duration_ms': 40.0}
        return {'success': True, 'ingress_site': 'gnb-1', 'selected_route': 'local', 'server_node': 'gnb1', 'server_pod': 'p-local', 'http_status': 200, 'duration_ms': 20.0}

    monkeypatch.setattr(vvp, '_http_json', fake_http)
    monkeypatch.setattr(vvp, 'attempt', fake_attempt)
    monkeypatch.setattr(vvp.time, 'sleep', lambda _s: None)
    args = SimpleNamespace(
        site='gnb-1', ingress_url='http://127.0.0.1:31801/detect', source_ip='127.0.0.1',
        offloader_url='http://127.0.0.1:18086', token_file=None, wait_ack=0.0, timeout=1.0,
    )
    vvp.cmd_smoke_manual(args)
    out = json.loads(capsys.readouterr().out)
    assert sent_routes == ['LOCAL', 'SPREAD', 'LOCAL', None]
    assert out['all_ok'] is True
    assert out['during']['expected_route'] == 'spread'
    assert out['during']['selected_route'] == 'spread'
