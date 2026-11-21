"""Differential equivalence tests comparing baseline (pre-simplify-20261121) vs live code.

Covers:
1. VerticalOffloadPolicy.evaluate + ACK/NACK/manual override/mode/reset across 350 deterministic sequences.
2. Envoy xDS RDS/EDS rendering and XdsState protocol handling.
3. Collector ingest, window eviction, /offload/snapshot, /metrics, /metrics/placement, and /sessions.
"""
import importlib
import json
from pathlib import Path
import random
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent
BASELINE_ROOT = REPO_ROOT / 'tests_baseline'


def _load_baseline_packages():
    """Import baseline offloader and collector modules from tests_baseline/ in isolation."""
    if not BASELINE_ROOT.is_dir():
        raise RuntimeError(f'Missing baseline directory: {BASELINE_ROOT}')

    saved_modules = {
        k: v for k, v in sys.modules.items()
        if k == 'offloader' or k.startswith('offloader.')
        or k == 'collector' or k.startswith('collector.')
    }
    for k in list(saved_modules):
        del sys.modules[k]

    saved_path = list(sys.path)
    sys.path.insert(0, str(BASELINE_ROOT))
    try:
        base_policy = importlib.import_module('offloader.policy')
        base_xds = importlib.import_module('offloader.xds')
        base_collector = importlib.import_module('collector.server')
    finally:
        sys.path[:] = saved_path
        for k in list(sys.modules):
            if (k == 'offloader' or k.startswith('offloader.')
                    or k == 'collector' or k.startswith('collector.')):
                del sys.modules[k]
        sys.modules.update(saved_modules)

    return base_policy, base_xds, base_collector


BASE_POLICY, BASE_XDS, BASE_COLLECTOR = _load_baseline_packages()

import offloader.policy as LIVE_POLICY  # noqa: E402
import offloader.xds as LIVE_XDS  # noqa: E402
import collector.server as LIVE_COLLECTOR  # noqa: E402


SITES = ('gnb-1', 'gnb-2', 'gnb-3')


def _make_bucket(rng: random.Random, *, seq: int, wall_now: float, force_mode: str | None = None) -> dict:
    mode = force_mode or rng.choices(
        ['empty', 'stale', 'healthy', 'p90_breach', 'error_breach', 'no_success', 'low_samples', 'boundary'],
        weights=[1, 1, 5, 4, 2, 1, 2, 3],
    )[0]
    if mode == 'empty':
        return {
            'status': 'empty',
            'p90_ms': None,
            'attempts': 0,
            'successes': 0,
            'errors': 0,
            'timeouts': 0,
            'error_rate': None,
            'first_sample_ts': None,
            'last_sample_age_seconds': None,
            'last_sample_ts': None,
            'last_success_age_seconds': None,
            'newest_sample_seq': seq,
        }
    if mode == 'stale':
        age = round(rng.uniform(5.5, 15.0), 3)
        return {
            'status': 'stale',
            'p90_ms': round(rng.uniform(60.0, 260.0), 3),
            'attempts': rng.randint(5, 30),
            'successes': rng.randint(1, 25),
            'errors': 0,
            'timeouts': 0,
            'error_rate': 0.0,
            'first_sample_ts': round(wall_now - 14.0, 6),
            'last_sample_age_seconds': age,
            'last_sample_ts': round(wall_now - age, 6),
            'last_success_age_seconds': age,
            'newest_sample_seq': seq,
        }
    age = round(rng.uniform(0.05, 3.0), 3)
    first_ts = round(wall_now - rng.uniform(1.0, 12.0), 6)
    if mode == 'no_success':
        attempts = rng.randint(5, 15)
        timeouts = rng.randint(0, attempts)
        return {
            'status': 'no_success',
            'p90_ms': None,
            'attempts': attempts,
            'successes': 0,
            'errors': attempts,
            'timeouts': timeouts,
            'error_rate': 1.0,
            'first_sample_ts': first_ts,
            'last_sample_age_seconds': age,
            'last_sample_ts': round(wall_now - age, 6),
            'last_success_age_seconds': None,
            'newest_sample_seq': seq,
        }
    if mode == 'low_samples':
        successes = rng.randint(1, 14)
        errors = rng.randint(0, 3)
        attempts = successes + errors
        return {
            'status': 'ok',
            'p90_ms': round(rng.uniform(50.0, 220.0), 3),
            'attempts': attempts,
            'successes': successes,
            'errors': errors,
            'timeouts': 0,
            'error_rate': round(errors / attempts, 6),
            'first_sample_ts': first_ts,
            'last_sample_age_seconds': age,
            'last_sample_ts': round(wall_now - age, 6),
            'last_success_age_seconds': age,
            'newest_sample_seq': seq,
        }
    if mode == 'error_breach':
        successes = rng.randint(2, 10)
        errors = rng.randint(successes + 2, successes + 15)
        attempts = successes + errors
        timeouts = rng.randint(0, errors)
        return {
            'status': 'ok',
            'p90_ms': round(rng.uniform(80.0, 220.0), 3),
            'attempts': attempts,
            'successes': successes,
            'errors': errors,
            'timeouts': timeouts,
            'error_rate': round(errors / attempts, 6),
            'first_sample_ts': first_ts,
            'last_sample_age_seconds': age,
            'last_sample_ts': round(wall_now - age, 6),
            'last_success_age_seconds': age,
            'newest_sample_seq': seq,
        }
    if mode == 'p90_breach':
        successes = rng.randint(15, 60)
        errors = rng.randint(0, 2)
        attempts = successes + errors
        return {
            'status': 'ok',
            'p90_ms': round(rng.uniform(155.0, 350.0), 3),
            'attempts': attempts,
            'successes': successes,
            'errors': errors,
            'timeouts': 0,
            'error_rate': round(errors / attempts, 6),
            'first_sample_ts': first_ts,
            'last_sample_age_seconds': age,
            'last_sample_ts': round(wall_now - age, 6),
            'last_success_age_seconds': age,
            'newest_sample_seq': seq,
        }
    if mode == 'boundary':
        # Values at and around the 150 ms threshold and the min_samples edge, so off-by-one and
        # '>' versus '>=' changes cannot hide between the healthy and breach ranges.
        successes = rng.choice([14, 15, 16])
        errors = rng.randint(0, 2)
        attempts = successes + errors
        return {
            'status': 'ok',
            'p90_ms': rng.choice([149.0, 149.999, 150.0, 150.001, 151.0]),
            'attempts': attempts,
            'successes': successes,
            'errors': errors,
            'timeouts': 0,
            'error_rate': round(errors / attempts, 6),
            'first_sample_ts': first_ts,
            'last_sample_age_seconds': age,
            'last_sample_ts': round(wall_now - age, 6),
            'last_success_age_seconds': age,
            'newest_sample_seq': seq,
        }
    # healthy
    successes = rng.randint(15, 60)
    errors = rng.randint(0, 2)
    attempts = successes + errors
    return {
        'status': 'ok',
        'p90_ms': round(rng.uniform(40.0, 145.0), 3),
        'attempts': attempts,
        'successes': successes,
        'errors': errors,
        'timeouts': 0,
        'error_rate': round(errors / attempts, 6),
        'first_sample_ts': first_ts,
        'last_sample_age_seconds': age,
        'last_sample_ts': round(wall_now - age, 6),
        'last_success_age_seconds': age,
        'newest_sample_seq': seq,
    }


def _policy_state_signature(policy) -> dict:
    return {
        'mode': policy.config.mode,
        'spread_count': policy.spread_count(),
        'last_route_switch_at': policy.last_route_switch_at,
        'last_switched_site': policy.last_switched_site,
        'sites': {
            site: {
                'to_dict': st.to_dict(),
                'retreat_level': st.retreat_level,
                'good_level': st.good_level,
                'hold_until': st.hold_until,
                'pending_update_since': st.pending_update_since,
                'pending_weight_since': st.pending_weight_since,
                'pending_weight_level': st.pending_weight_level,
                'pending_weight_fast': st.pending_weight_fast,
                'spread_wait_seconds': st.spread_wait_seconds,
                'breach_started_at': st.breach_started_at,
                'breach_last_seq': st.breach_last_seq,
                'breach_fresh_updates': st.breach_fresh_updates,
                'recovery_started_at': st.recovery_started_at,
                'recovery_last_seq': st.recovery_last_seq,
                'recovery_fresh_updates': st.recovery_fresh_updates,
            }
            for site, st in policy.states.items()
        },
    }


class TestPolicyEquivalence(unittest.TestCase):
    SEQUENCES = 350
    STEPS_PER_SEQUENCE = 32

    def test_differential_policy_sequences(self):
        rng = random.Random(20261121)
        total_steps = 0

        for seq_idx in range(self.SEQUENCES):
            mode = 'local-only' if seq_idx % 25 == 0 else 'vertical'
            cfg_kwargs = {
                'mode': mode,
                'offload_p90_threshold_ms': 150.0,
                'breach_duration_seconds': rng.choice([2.0, 5.0]),
                'min_dwell_seconds': rng.choice([5.0, 15.0]),
                'route_switch_interval_seconds': rng.choice([2.0, 5.0]),
                'envoy_ack_timeout_seconds': rng.choice([3.0, 5.0]),
            }
            old_p = BASE_POLICY.VerticalOffloadPolicy(BASE_POLICY.OffloadConfig(**cfg_kwargs))
            new_p = LIVE_POLICY.VerticalOffloadPolicy(LIVE_POLICY.OffloadConfig(**cfg_kwargs))

            if seq_idx % 5 == 0:
                old_p.reset(active_route='UNKNOWN')
                new_p.reset(active_route='UNKNOWN')

            now = 100.0
            wall_base = 1700000000.0
            sample_seq = {site: 0 for site in SITES}
            # Pick a scenario bias so we thoroughly hit entry, retreat, restore, complete, failback
            regime = seq_idx % 7

            for step in range(self.STEPS_PER_SEQUENCE):
                total_steps += 1
                dt = rng.choice([1.0, 2.0, 3.0, 5.0])
                now += dt
                wall_now = wall_base + now

                # Occasional mode toggle, reset, manual override, or ACK/NACK
                action_roll = rng.random()
                if action_roll < 0.04:
                    new_mode = rng.choice(['vertical', 'local-only'])
                    old_p.set_mode(new_mode)
                    new_p.set_mode(new_mode)
                elif action_roll < 0.07:
                    site = rng.choice(SITES)
                    ov_route = rng.choice([None, 'LOCAL', 'SPREAD'])
                    old_res, old_err = None, None
                    new_res, new_err = None, None
                    try:
                        ev = old_p.set_manual_override(site, ov_route, now)
                        old_res = None if ev is None else ev.to_dict()
                    except Exception as exc:
                        old_err = type(exc).__name__
                    try:
                        ev = new_p.set_manual_override(site, ov_route, now)
                        new_res = None if ev is None else ev.to_dict()
                    except Exception as exc:
                        new_err = type(exc).__name__
                    self.assertEqual(old_err, new_err, f'seq={seq_idx} step={step} override err mismatch')
                    self.assertEqual(old_res, new_res, f'seq={seq_idx} step={step} override ev mismatch')

                # Deliver RDS / EDS ACKs or NACKs for sites with pending updates
                for site in SITES:
                    st_old = old_p.states[site]
                    if st_old.pending_update_since is not None or st_old.active_route != st_old.desired_route:
                        r = rng.random()
                        if r < 0.65:
                            ver = f'ep-{step}'
                            e1 = old_p.record_envoy_ack(site, st_old.desired_route, ver, wall_now, monotonic_now=now)
                            e2 = new_p.record_envoy_ack(site, new_p.states[site].desired_route, ver, wall_now, monotonic_now=now)
                            self.assertEqual(
                                None if e1 is None else e1.to_dict(),
                                None if e2 is None else e2.to_dict(),
                                f'seq={seq_idx} step={step} rds ack mismatch',
                            )
                        elif r < 0.75:
                            e1 = old_p.record_envoy_nack(site, 'bad rds', wall_now)
                            e2 = new_p.record_envoy_nack(site, 'bad rds', wall_now)
                            self.assertEqual(e1.to_dict(), e2.to_dict(), f'seq={seq_idx} step={step} rds nack mismatch')
                    if st_old.pending_weight_since is not None:
                        r = rng.random()
                        if r < 0.70:
                            lvl = st_old.pending_weight_level
                            ver = f'eds-{step}'
                            e1 = old_p.record_envoy_eds_ack(site, lvl, ver, wall_now, monotonic_now=now)
                            e2 = new_p.record_envoy_eds_ack(site, lvl, ver, wall_now, monotonic_now=now)
                            self.assertEqual(
                                None if e1 is None else e1.to_dict(),
                                None if e2 is None else e2.to_dict(),
                                f'seq={seq_idx} step={step} eds ack mismatch',
                            )
                        elif r < 0.80:
                            e1 = old_p.record_envoy_nack(site, 'bad eds', wall_now, reason='envoy_eds_nack')
                            e2 = new_p.record_envoy_nack(site, 'bad eds', wall_now, reason='envoy_eds_nack')
                            self.assertEqual(e1.to_dict(), e2.to_dict(), f'seq={seq_idx} step={step} eds nack mismatch')

                local_counts = {site: rng.choices([0, 1, 2, 3, 4], weights=[1, 4, 4, 3, 2])[0] for site in SITES}
                core_count = rng.choices([0, 1, 2, 3], weights=[1, 3, 3, 4])[0]

                if rng.random() < 0.05:
                    snapshot = None
                    collector_error = 'URLError: timeout'
                else:
                    collector_error = None
                    sites_snap = {}
                    for site in SITES:
                        if rng.random() < 0.85:
                            sample_seq[site] += rng.randint(1, 5)
                        seq_val = sample_seq[site]
                        if regime == 1 and step < 10:
                            fmode = 'p90_breach'
                        elif regime == 1 and step >= 10:
                            fmode = 'healthy'
                        elif regime == 2 and step < 8:
                            fmode = 'error_breach'
                        elif regime == 2 and step < 18:
                            fmode = 'healthy'
                        elif regime == 2:
                            fmode = 'p90_breach'
                        elif regime == 3:
                            fmode = 'healthy' if step % 6 >= 3 else 'p90_breach'
                        else:
                            fmode = None
                        local_b = _make_bucket(rng, seq=seq_val, wall_now=wall_now, force_mode=fmode)
                        core_b = _make_bucket(rng, seq=seq_val, wall_now=wall_now, force_mode=fmode)
                        unk_b = _make_bucket(
                            rng,
                            seq=seq_val + (1 if rng.random() < 0.3 else 0),
                            wall_now=wall_now,
                            force_mode=rng.choice(['empty', 'no_success', 'error_breach']),
                        )
                        user_map = {
                            'local': local_b,
                            'core': core_b,
                            'unknown': unk_b,
                            'overall': _make_bucket(rng, seq=seq_val, wall_now=wall_now, force_mode=fmode),
                        }
                        sites_snap[site] = {'user': user_map}
                    snapshot = {
                        'generated_at': round(wall_now, 6),
                        'window_seconds': rng.choice([5.0, 10.0, 15.0]),
                        'stale_after_seconds': 5.0,
                        'sites': sites_snap,
                    }

                ev_old = old_p.evaluate(now, snapshot, local_counts, core_count, collector_error=collector_error)
                ev_new = new_p.evaluate(now, snapshot, local_counts, core_count, collector_error=collector_error)

                self.assertEqual(
                    [e.to_dict() for e in ev_old],
                    [e.to_dict() for e in ev_new],
                    f'seq={seq_idx} step={step} DecisionEvent mismatch',
                )
                self.assertEqual(
                    _policy_state_signature(old_p),
                    _policy_state_signature(new_p),
                    f'seq={seq_idx} step={step} Policy state mismatch',
                )

        self.assertGreaterEqual(total_steps, self.SEQUENCES * self.STEPS_PER_SEQUENCE)


class TestXdsEquivalence(unittest.TestCase):
    def test_differential_rds_eds_and_state(self):
        # Direct builder comparison
        for site in SITES:
            for route in ('LOCAL', 'SPREAD', 'local', 'spread'):
                for timeout in ('5s', '10s'):
                    old_rds = BASE_XDS.build_route_configuration(site, route, 'ep-1', route_timeout=timeout)
                    new_rds = LIVE_XDS.build_route_configuration(site, route, 'ep-1', route_timeout=timeout)
                    self.assertEqual(json.dumps(old_rds, sort_keys=True), json.dumps(new_rds, sort_keys=True))

        endpoint_sets = [
            [],
            [('10.0.0.2', 5000), ('10.0.0.1', 5000), ('10.0.0.1', 5000)],
            [('10.0.0.2', 5000, 3), ('10.0.0.1', 5000, 1), ('10.0.0.3', 5000, 2)],
        ]
        for eps in endpoint_sets:
            old_eds = BASE_XDS.build_cluster_load_assignment('spread-gnb-1', eps)
            new_eds = LIVE_XDS.build_cluster_load_assignment('spread-gnb-1', eps)
            self.assertEqual(json.dumps(old_eds, sort_keys=True), json.dumps(new_eds, sort_keys=True))

        # Stateful XdsState sequence comparison
        t = [1000.0]
        old_xds = BASE_XDS.XdsState(route_timeout='10s', clock=lambda: t[0], epoch='fixedep1')
        new_xds = LIVE_XDS.XdsState(route_timeout='10s', clock=lambda: t[0], epoch='fixedep1')

        rng = random.Random(20261122)
        for step in range(80):
            t[0] += 1.5
            local_by_site = {
                site: [(f'10.1.{i}.{j}', 5000) for j in range(1, rng.randint(1, 4))]
                for i, site in enumerate(SITES, start=1)
            }
            core_eps = [(f'10.9.0.{j}', 5000) for j in range(1, rng.randint(1, 4))]
            old_xds.update_endpoints(local_by_site, core_eps)
            new_xds.update_endpoints(local_by_site, core_eps)

            site = rng.choice(SITES)
            route = rng.choice(['LOCAL', 'SPREAD'])
            self.assertEqual(old_xds.set_site_route(site, route), new_xds.set_site_route(site, route))

            lvl = rng.randint(0, 4)
            self.assertEqual(old_xds.set_site_retreat_level(site, lvl), new_xds.set_site_retreat_level(site, lvl))

            if step % 15 == 14:
                self.assertEqual(
                    old_xds.inject_invalid_rds_for_test(site, 'SPREAD'),
                    new_xds.inject_invalid_rds_for_test(site, 'SPREAD'),
                )
            if step % 23 == 22:
                self.assertEqual(old_xds.reset_routes(), new_xds.reset_routes())

            # RDS request
            rds_ver = old_xds.status_snapshot()['sites'][site]['desired_version'] if rng.random() < 0.5 else ''
            rds_req = {
                'node': {'id': f'envoy-{site}'},
                'resource_names': [f'route-{site}'],
                'version_info': rds_ver,
            }
            if rng.random() < 0.15:
                rds_req['error_detail'] = {'message': 'nack-rds'}
            old_acks, new_acks = [], []
            old_nacks, new_nacks = [], []
            r1 = old_xds.handle_rds(
                rds_req,
                on_ack=lambda *a: old_acks.append(a),
                on_nack=lambda *a: old_nacks.append(a),
            )
            r2 = new_xds.handle_rds(
                rds_req,
                on_ack=lambda *a: new_acks.append(a),
                on_nack=lambda *a: new_nacks.append(a),
            )
            self.assertEqual(json.dumps(r1, sort_keys=True), json.dumps(r2, sort_keys=True))
            self.assertEqual(old_acks, new_acks)
            self.assertEqual(old_nacks, new_nacks)

            # EDS request
            eds_ver = old_xds.status_snapshot()['eds_version'] if rng.random() < 0.5 else ''
            eds_req = {
                'node': {'id': f'envoy-{site}'},
                'resource_names': [f'local-{site}', f'spread-{site}', 'core-5g'] if rng.random() < 0.7 else [],
                'version_info': eds_ver,
            }
            if rng.random() < 0.15:
                eds_req['error_detail'] = {'message': 'nack-eds'}
            old_acks, new_acks = [], []
            old_nacks, new_nacks = [], []
            e1 = old_xds.handle_eds(
                eds_req,
                on_ack=lambda *a: old_acks.append(a),
                on_nack=lambda *a: old_nacks.append(a),
            )
            e2 = new_xds.handle_eds(
                eds_req,
                on_ack=lambda *a: new_acks.append(a),
                on_nack=lambda *a: new_nacks.append(a),
            )
            self.assertEqual(json.dumps(e1, sort_keys=True), json.dumps(e2, sort_keys=True))
            self.assertEqual(old_acks, new_acks)
            self.assertEqual(old_nacks, new_nacks)

            self.assertEqual(
                json.dumps(old_xds.status_snapshot(), sort_keys=True),
                json.dumps(new_xds.status_snapshot(), sort_keys=True),
            )


class TestCollectorEquivalence(unittest.TestCase):
    def test_differential_collector_sequences(self):
        raw_settings = {
            'window_seconds': 15.0,
            'placement_window_seconds': 5.0,
            'offload_window_seconds': 15.0,
            'offload_stale_seconds': 5.0,
            'mapping_refresh_seconds': 5.0,
            'mapping_max_age_seconds': 10.0,
            'slo_seconds': 0.15,
            'max_observations': 200,
            'core_node': 'core-5g',
            'site_nodes': {'gnb-1': 'gnb1', 'gnb-2': 'gnb-2', 'gnb-3': 'gnb-3'},
            'gnb_nodes': {1: 'gnb1', 2: 'gnb-2', 3: 'gnb-3'},
            'upf_nodes': {'10.10.0.1': 'gnb1', '10.10.0.2': 'gnb-2', '10.10.0.3': 'gnb-3'},
            'smf_url': 'http://127.0.0.1:8081/smf',
            'amf_url': 'http://127.0.0.1:8082/amf',
        }
        old_cfg = BASE_COLLECTOR.validate_settings(dict(raw_settings))
        new_cfg = LIVE_COLLECTOR.validate_settings(dict(raw_settings))
        self.assertEqual(old_cfg, new_cfg)

        mono = [100.0]
        wall = [1700000100.0]
        old_c = BASE_COLLECTOR.Collector(old_cfg, clock=lambda: mono[0], wall_clock=lambda: wall[0])
        new_c = LIVE_COLLECTOR.Collector(new_cfg, clock=lambda: mono[0], wall_clock=lambda: wall[0])

        smf_payload = {
            'items': [
                {'supi': 'imsi-1', 'pdu': [{'pdu_state': 'active', 'ipv4': '10.45.0.10', 'n3': {'upf': {'addr': '10.10.0.1:2152'}}}]},
                {'supi': 'imsi-2', 'pdu': [{'pdu_state': 'active', 'ipv4': '10.45.0.11', 'n3': {'upf': {'addr': '10.10.0.2:2152'}}}]},
                {'supi': 'imsi-3', 'pdu': [{'pdu_state': 'active', 'ipv4': '10.45.0.12', 'n3': {'upf': {'addr': '10.10.0.3:2152'}}}]},
            ]
        }
        amf_payload = {
            'items': [
                {'supi': 'imsi-1', 'gnb': {'gnb_id': 1}},
                {'supi': 'imsi-2', 'gnb': {'gnb_id': 2}},
                {'supi': 'imsi-3', 'gnb': {'gnb_id': 3}},
            ]
        }
        old_upf = BASE_COLLECTOR.map_upf_sessions(smf_payload, old_cfg['upf_nodes'])
        new_upf = LIVE_COLLECTOR.map_upf_sessions(smf_payload, new_cfg['upf_nodes'])
        self.assertEqual(old_upf, new_upf)
        old_map = BASE_COLLECTOR.join_sessions(smf_payload, amf_payload, old_cfg['gnb_nodes'])
        new_map = LIVE_COLLECTOR.join_sessions(smf_payload, amf_payload, new_cfg['gnb_nodes'])
        self.assertEqual(old_map, new_map)

        for c in (old_c, new_c):
            c.mapping = dict(old_map)
            c.upf_mapping = dict(old_upf)
            c.mapping_time = mono[0]
            c.upf_mapping_time = mono[0]

        rng = random.Random(20261123)
        ue_ips = ['10.45.0.10', '10.45.0.11', '10.45.0.12', '10.45.0.99']
        nodes = ['gnb1', 'gnb-2', 'gnb-3', 'core-5g', None]

        for step in range(250):
            dt = rng.choice([0.05, 0.2, 0.5, 1.5, 4.0])
            mono[0] += dt
            wall[0] += dt

            tclass = 'probe' if rng.random() < 0.15 else 'user'
            success = rng.random() < 0.72
            status = 200 if success else rng.choice([None, 500, 503, 504])
            timed_out = (not success) and (status == 504 or rng.random() < 0.4)
            site = rng.choice(SITES) if (tclass == 'probe' or rng.random() < 0.7) else None
            route = rng.choice(['local', 'core', 'unknown', None])
            obs = {
                'traffic_class': tclass,
                'duration_ms': round(rng.uniform(15.0, 320.0), 3),
                'http_status': status,
                'success': success,
                'timed_out': timed_out,
                'server_node': rng.choice(nodes),
            }
            if tclass == 'user':
                obs['ue_ip'] = rng.choice(ue_ips)
            if site is not None:
                obs['site'] = site
            if route is not None:
                obs['route'] = route

            self.assertEqual(old_c.ingest(obs), new_c.ingest(obs), f'step={step} ingest mismatch')

            if step % 10 == 0 or step == 249:
                self.assertEqual(
                    json.dumps(old_c.offload_snapshot(), sort_keys=True),
                    json.dumps(new_c.offload_snapshot(), sort_keys=True),
                    f'step={step} offload_snapshot mismatch',
                )
                self.assertEqual(old_c.metrics(), new_c.metrics(), f'step={step} metrics mismatch')
                self.assertEqual(
                    old_c.placement_metrics(),
                    new_c.placement_metrics(),
                    f'step={step} placement_metrics mismatch',
                )
                self.assertEqual(
                    json.dumps(old_c.sessions_snapshot(), sort_keys=True),
                    json.dumps(new_c.sessions_snapshot(), sort_keys=True),
                    f'step={step} sessions_snapshot mismatch',
                )


if __name__ == '__main__':
    unittest.main()
