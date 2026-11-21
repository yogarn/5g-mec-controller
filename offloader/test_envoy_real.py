"""Real Envoy (envoyproxy/envoy:v1.31-latest) end-to-end integration tests for RDS, EDS, restart, and NACK."""
import http.client
import http.server
import json
import math
from pathlib import Path
import socket
import subprocess
import threading
import time
from urllib.request import Request, urlopen

import pytest
import yaml

from offloader.policy import OffloadConfig, SITES
from offloader.server import VerticalOffloadController
from offloader.xds import start_admin_server, start_xds_server


ENVOY_IMAGE = 'envoyproxy/envoy:v1.31-latest'


def _docker_envoy_available() -> bool:
    try:
        subprocess.run(['docker', 'image', 'inspect', ENVOY_IMAGE], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        return True
    except (OSError, subprocess.SubprocessError):
        return False


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _start_dummy_backend(node_name: str, pod_name: str) -> tuple[http.server.ThreadingHTTPServer, int]:
    class Backend(http.server.BaseHTTPRequestHandler):
        def _respond(self):
            body = json.dumps({
                'status': 'ok',
                'person_count': 0,
                'persons': [],
                'server_node': node_name,
                'server_pod': pod_name,
            }).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self._respond()

        def do_POST(self):
            length = int(self.headers.get('Content-Length', '0') or '0')
            if length > 0:
                self.rfile.read(length)
            self._respond()

        def log_message(self, *args):
            return

    srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Backend)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, srv.server_port


def _build_envoy_bootstrap(site: str, listen_port: int, admin_port: int, xds_port: int) -> dict:
    # Load the actual repository manifest ConfigMap template and rebind ports/xDS host for local loopback test.
    docs = list(yaml.safe_load_all(Path(__file__).with_name('envoy-ingress.yaml').read_text(encoding='utf-8')))
    cm_name = f'mec-envoy-{site}-config'
    cm = next(d for d in docs if d['kind'] == 'ConfigMap' and d['metadata']['name'] == cm_name)
    cfg = yaml.safe_load(cm['data']['envoy.yaml'])
    cfg['static_resources']['listeners'][0]['address']['socket_address']['address'] = '127.0.0.1'
    cfg['static_resources']['listeners'][0]['address']['socket_address']['port_value'] = listen_port
    cfg['admin']['address']['socket_address']['address'] = '127.0.0.1'
    cfg['admin']['address']['socket_address']['port_value'] = admin_port
    for cluster in cfg['static_resources']['clusters']:
        if cluster['name'] == 'offloader_xds':
            ep = cluster['load_assignment']['endpoints'][0]['lb_endpoints'][0]['endpoint']['address']['socket_address']
            ep['address'] = '127.0.0.1'
            ep['port_value'] = xds_port
        elif cluster['type'] == 'EDS':
            cluster['eds_cluster_config']['eds_config']['api_config_source']['refresh_delay'] = '0.2s'
    cfg['static_resources']['listeners'][0]['filter_chains'][0]['filters'][0]['typed_config']['rds']['config_source']['api_config_source']['refresh_delay'] = '0.2s'
    return cfg


def _request_envoy(port: int, path: str = '/detect', headers: dict | None = None) -> tuple[int, dict, dict]:
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=3)
    try:
        conn.request('GET', path, headers=headers or {})
        resp = conn.getresponse()
        raw = resp.read()
        hdrs = {k.lower(): v for k, v in resp.getheaders()}
        payload = json.loads(raw.decode('utf-8')) if raw and resp.status == 200 else {}
        return resp.status, hdrs, payload
    finally:
        conn.close()


def _wait_until(predicate, timeout: float = 15.0, interval: float = 0.2):
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:
            last_err = exc
        time.sleep(interval)
    raise AssertionError(f'condition not met within {timeout}s (last_err={last_err})')


@pytest.mark.skipif(not _docker_envoy_available(), reason='Docker with envoyproxy/envoy:v1.31-latest not available')
def test_real_envoy_rds_eds_manual_auto_restart_and_nack(tmp_path):
    # 1. Start 4 dummy backends representing gnb1, gnb-2, gnb-3, and core-5g.
    backends = {}
    ports = {}
    for node in ('gnb1', 'gnb-2', 'gnb-3', 'core-5g'):
        srv, port = _start_dummy_backend(node, f'pod-{node}')
        backends[node] = srv
        ports[node] = port

    local_eps = {
        'gnb-1': [('127.0.0.1', ports['gnb1'])],
        'gnb-2': [('127.0.0.1', ports['gnb-2'])],
        'gnb-3': [('127.0.0.1', ports['gnb-3'])],
    }
    core_eps = [('127.0.0.1', ports['core-5g'])]

    xds_port = _free_port()
    admin_port = _free_port()
    admin_token = 'envoy-real-test-token'

    # Controlled snapshot box for automatic p90 test.
    current_snap = {'value': None}
    sim_now = [0.0]
    wall_start = 1700000000.0

    def make_controller():
        ctrl = VerticalOffloadController(
            OffloadConfig(
                mode='vertical',
                offload_p90_threshold_ms=150.0,
                min_samples=15,
                breach_duration_seconds=2.0,
                min_fresh_updates=2,
                min_dwell_seconds=3.0,
            ),
            collector_snapshot_url='http://127.0.0.1:9/offload/snapshot',
            snapshot_fetcher=lambda _url, timeout=1.0: current_snap['value'],
            clock=lambda: sim_now[0],
            wall_clock=lambda: wall_start + sim_now[0],
        )
        ctrl.xds.update_endpoints(local_eps, core_eps)
        return ctrl

    ctrl = make_controller()
    xds_srv = start_xds_server(ctrl, host='127.0.0.1', port=xds_port)
    admin_srv = start_admin_server(ctrl, admin_token, host='127.0.0.1', port=admin_port)

    containers = []
    envoy_ports = {}
    envoy_admin_ports = {}
    try:
        # 2. Launch 3 real Envoy containers using the repo's envoy-ingress.yaml templates.
        used_ports = {xds_port, admin_port, *ports.values()}
        def alloc_port():
            while True:
                p = _free_port()
                if p not in used_ports:
                    used_ports.add(p)
                    return p

        for idx, site in enumerate(SITES, start=1):
            lport = alloc_port()
            aport = alloc_port()
            envoy_ports[site] = lport
            envoy_admin_ports[site] = aport
            cfg = _build_envoy_bootstrap(site, lport, aport, xds_port)
            cfg_file = tmp_path / f'envoy-{site}.yaml'
            cfg_file.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding='utf-8')
            cfg_file.chmod(0o644)
            cname = f'mec-test-envoy-{site}-{lport}'
            subprocess.run(
                [
                    'docker', 'run', '-d', '--rm', '--network=host', '--name', cname,
                    '-v', f'{cfg_file}:/etc/envoy/envoy.yaml:ro',
                    ENVOY_IMAGE, '-c', '/etc/envoy/envoy.yaml', '--base-id', str(idx),
                    '--service-node', f'envoy-{site}', '--service-cluster', 'mec-inference-ingress',
                ],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            containers.append(cname)

        # Wait for all 3 Envoy instances to ACK LOCAL and serve their local backend.
        expected_local_node = {'gnb-1': 'gnb1', 'gnb-2': 'gnb-2', 'gnb-3': 'gnb-3'}
        for site in SITES:
            _wait_until(lambda s=site: (
                ctrl.policy.states[s].active_route == 'LOCAL'
                and _request_envoy(envoy_ports[s])[2].get('server_node') == expected_local_node[s]
            ))
            code, hdrs, body = _request_envoy(envoy_ports[site])
            assert code == 200
            assert hdrs['x-mec-site'] == site
            assert hdrs['x-mec-route'] == 'local'
            assert body['server_node'] == expected_local_node[site]

        def _envoy_admin_stats(site: str, prefix: str) -> dict[str, int]:
            with urlopen(f'http://127.0.0.1:{envoy_admin_ports[site]}/stats?filter={prefix}', timeout=3) as resp:
                lines = resp.read().decode('utf-8').splitlines()
            out = {}
            for line in lines:
                if ':' in line:
                    k, v = line.split(':', 1)
                    try:
                        out[k.strip()] = int(v.strip())
                    except ValueError:
                        pass
            return out

        def _override(site: str, route: str | None):
            req = Request(
                f'http://127.0.0.1:{admin_port}/override',
                data=json.dumps({'site': site, 'route': route}).encode(),
                method='POST',
                headers=auth,
            )
            with urlopen(req, timeout=3) as resp:
                assert resp.status == 200

        # 3. Manual LOCAL -> SPREAD -> LOCAL on each site one at a time; other two sites stay LOCAL.
        auth = {'Authorization': f'Bearer {admin_token}', 'Content-Type': 'application/json'}
        switch_interval = ctrl.policy.config.route_switch_interval_seconds
        for target_site in SITES:
            sim_now[0] += switch_interval
            _override(target_site, 'SPREAD')
            _wait_until(lambda s=target_site: (
                ctrl.policy.states[s].active_route == 'SPREAD'
                and _request_envoy(envoy_ports[s])[1].get('x-mec-route') == 'spread'
            ))
            # Verify Envoy accepted spread-gnb-N EDS cluster with zero rejections.
            stats = _envoy_admin_stats(target_site, f'cluster.spread-{target_site}.update_')
            assert stats.get(f'cluster.spread-{target_site}.update_success', 0) >= 1
            assert stats.get(f'cluster.spread-{target_site}.update_rejected', 0) == 0
            for other in SITES:
                code, hdrs, body = _request_envoy(envoy_ports[other])
                assert code == 200
                if other == target_site:
                    assert hdrs['x-mec-route'] == 'spread'
                    assert body['server_node'] in (expected_local_node[other], 'core-5g')
                    # Forced local probe MUST still reach local backend while user route is SPREAD.
                    pcode, phdrs, pbody = _request_envoy(envoy_ports[other], headers={'X-MEC-Probe': 'local'})
                    assert pcode == 200 and phdrs['x-mec-route'] == 'local' and pbody['server_node'] == expected_local_node[other]
                else:
                    assert hdrs['x-mec-route'] == 'local' and body['server_node'] == expected_local_node[other]

            # Switch back to LOCAL and clear override.
            sim_now[0] += switch_interval
            _override(target_site, 'LOCAL')
            _override(target_site, None)
            _wait_until(lambda s=target_site: (
                ctrl.policy.states[s].active_route == 'LOCAL'
                and _request_envoy(envoy_ports[s])[2].get('server_node') == expected_local_node[s]
            ))

        # 3b. Real Envoy weighted EDS proportion check for k=1 (p=1/2=0.5) and k=2 (p=1/3=0.3333).
        # With c=1, x_i=1: local weight=k, core weight=1 -> theoretical core ratio p = 1 / (1 + k).
        # Binomial 4-sigma tolerance for N=240 requests: tol = 4 * sqrt(p*(1-p)/N).
        sample_n = 240
        sim_now[0] += switch_interval
        _override('gnb-1', 'SPREAD')
        _wait_until(lambda: (
            ctrl.policy.states['gnb-1'].active_route == 'SPREAD'
            and _request_envoy(envoy_ports['gnb-1'])[1].get('x-mec-route') == 'spread'
        ))
        # Ensure Envoy has polled the current EDS version for k=1 before sampling.
        time.sleep(0.5)
        core_hits_k1 = 0
        for _ in range(sample_n):
            code, hdrs, body = _request_envoy(envoy_ports['gnb-1'])
            assert code == 200 and hdrs['x-mec-route'] == 'spread'
            assert body['server_node'] in ('gnb1', 'core-5g')
            if body['server_node'] == 'core-5g':
                core_hits_k1 += 1
        p_k1 = 1.0 / (1.0 + 1.0 * 1.0)
        tol_k1 = 4.0 * math.sqrt(p_k1 * (1.0 - p_k1) / sample_n)
        ratio_k1 = core_hits_k1 / sample_n
        assert abs(ratio_k1 - p_k1) <= tol_k1, f'k=1 core ratio {ratio_k1:.4f} outside {p_k1:.4f} +/- {tol_k1:.4f}'

        # Enter SPREAD on gnb-2 as well -> k=2 -> gnb-1 EDS weights recomputed (local=2, core=1, p=1/3).
        sim_now[0] += switch_interval
        _override('gnb-2', 'SPREAD')
        _wait_until(lambda: (
            ctrl.policy.states['gnb-2'].active_route == 'SPREAD'
            and _request_envoy(envoy_ports['gnb-2'])[1].get('x-mec-route') == 'spread'
        ))
        # Allow Envoy EDS refresh (0.2s) to apply the new k=2 weights on gnb-1.
        time.sleep(0.6)
        stats_k2 = _envoy_admin_stats('gnb-1', 'cluster.spread-gnb-1.update_')
        assert stats_k2.get('cluster.spread-gnb-1.update_rejected', 0) == 0
        core_hits_k2 = 0
        for _ in range(sample_n):
            code, hdrs, body = _request_envoy(envoy_ports['gnb-1'])
            assert code == 200 and hdrs['x-mec-route'] == 'spread'
            assert body['server_node'] in ('gnb1', 'core-5g')
            if body['server_node'] == 'core-5g':
                core_hits_k2 += 1
        p_k2 = 1.0 / (1.0 + 2.0 * 1.0)
        tol_k2 = 4.0 * math.sqrt(p_k2 * (1.0 - p_k2) / sample_n)
        ratio_k2 = core_hits_k2 / sample_n
        assert abs(ratio_k2 - p_k2) <= tol_k2, f'k=2 core ratio {ratio_k2:.4f} outside {p_k2:.4f} +/- {tol_k2:.4f}'
        print(
            f'REAL_ENVOY_SPREAD_RATIO k=1: {core_hits_k1}/{sample_n}={ratio_k1:.4f} (expected {p_k1:.4f} +/- {tol_k1:.4f}); '
            f'k=2: {core_hits_k2}/{sample_n}={ratio_k2:.4f} (expected {p_k2:.4f} +/- {tol_k2:.4f})'
        )

        # Return both gnb-1 and gnb-2 to LOCAL and clear overrides before automatic test.
        for s in ('gnb-1', 'gnb-2'):
            sim_now[0] += switch_interval
            _override(s, 'LOCAL')
            _override(s, None)
            _wait_until(lambda site=s: (
                ctrl.policy.states[site].active_route == 'LOCAL'
                and _request_envoy(envoy_ports[site])[2].get('server_node') == expected_local_node[site]
            ))

        # 4. Automatic p90-driven SPREAD entry and overall-p90 recovery on gnb-2 (gnb-1 & gnb-3 stay LOCAL).
        def _snap(gnb2_p90, gnb2_seq, overall_p90=None, overall_seq=0):
            return {
                'generated_at': wall_start + sim_now[0],
                'window_seconds': 15.0,
                'stale_after_seconds': 5.0,
                'p90_basis': 'success_only',
                'sites': {
                    s: {
                        'node': expected_local_node[s],
                        'user': {
                            'local': {
                                'status': 'ok',
                                'p90_ms': gnb2_p90 if s == 'gnb-2' else 70.0,
                                'attempts': 20,
                                'successes': 20,
                                'errors': 0,
                                'timeouts': 0,
                                'error_rate': 0.0,
                                'first_sample_ts': 1700000000.0,
                                'last_sample_age_seconds': 0.1,
                                'last_sample_ts': 1700000000.0,
                                'newest_sample_seq': gnb2_seq if s == 'gnb-2' else 1,
                            },
                            'core': {
                                'status': 'empty',
                                'p90_ms': None,
                                'attempts': 0,
                                'successes': 0,
                                'errors': 0,
                                'last_sample_age_seconds': None,
                                'newest_sample_seq': 0,
                            },
                            'overall': {
                                'status': 'ok' if (s == 'gnb-2' and overall_p90 is not None) else 'ok',
                                'p90_ms': overall_p90 if (s == 'gnb-2' and overall_p90 is not None) else (gnb2_p90 if s == 'gnb-2' else 70.0),
                                'attempts': 20,
                                'successes': 20,
                                'errors': 0,
                                'timeouts': 0,
                                'error_rate': 0.0,
                                'last_sample_age_seconds': 0.1,
                                'newest_sample_seq': overall_seq if (s == 'gnb-2' and overall_seq) else (gnb2_seq if s == 'gnb-2' else 1),
                            },
                            'unknown': {'status': 'empty', 'p90_ms': None, 'attempts': 0, 'successes': 0, 'errors': 0, 'timeouts': 0, 'error_rate': None, 'last_sample_age_seconds': None, 'newest_sample_seq': 0},
                        },
                        'probe': {
                            'local': {'status': 'empty', 'p90_ms': None, 'attempts': 0, 'successes': 0, 'errors': 0, 'timeouts': 0, 'error_rate': None, 'last_sample_age_seconds': None, 'newest_sample_seq': 0},
                        },
                    }
                    for s in SITES
                },
            }

        # Advance past gnb-2 post-ACK window from step 3b, then sustain a latency breach.
        automatic_start = sim_now[0] + 20.0
        for t in range(0, 5):
            sim_now[0] = automatic_start + t
            current_snap['value'] = _snap(220.0, 10 + t)
            ctrl.poll_once()
        _wait_until(lambda: (
            ctrl.policy.states['gnb-2'].active_route == 'SPREAD'
            and _request_envoy(envoy_ports['gnb-2'])[1].get('x-mec-route') == 'spread'
        ))
        assert _request_envoy(envoy_ports['gnb-1'])[2].get('server_node') == 'gnb1'
        assert _request_envoy(envoy_ports['gnb-3'])[2].get('server_node') == 'gnb-3'

        # After post-ACK window (15s), overall p90 <= SLO returns gnb-2 to LOCAL without probe gate.
        for t in range(25, 30):
            sim_now[0] = automatic_start + t
            current_snap['value'] = _snap(220.0, 30, overall_p90=110.0, overall_seq=100 + t)
            ctrl.poll_once()
        _wait_until(lambda: (
            ctrl.policy.states['gnb-2'].active_route == 'LOCAL'
            and _request_envoy(envoy_ports['gnb-2'])[2].get('server_node') == 'gnb-2'
        ))

        # Put gnb-2 back on SPREAD via automatic offload before testing offloader restart.
        for t in range(45, 50):
            sim_now[0] = automatic_start + t
            current_snap['value'] = _snap(220.0, 200 + t)
            ctrl.poll_once()
        _wait_until(lambda: (
            ctrl.policy.states['gnb-2'].active_route == 'SPREAD'
            and _request_envoy(envoy_ports['gnb-2'])[1].get('x-mec-route') == 'spread'
        ))

        # 5. Offloader restart while Envoy is still alive (with gnb-2 on SPREAD from old epoch!).
        old_epoch = ctrl.xds.epoch
        admin_srv.shutdown()
        admin_srv.server_close()
        xds_srv.shutdown()
        xds_srv.server_close()

        ctrl = make_controller()
        assert ctrl.xds.epoch != old_epoch
        # Before Envoy polls the new offloader, active_route MUST be UNKNOWN (not falsely LOCAL).
        assert ctrl.policy.states['gnb-2'].active_route == 'UNKNOWN'
        xds_srv = start_xds_server(ctrl, host='127.0.0.1', port=xds_port)
        admin_srv = start_admin_server(ctrl, admin_token, host='127.0.0.1', port=admin_port)

        # Envoy polls with old epoch version -> rejected as ACK -> receives new epoch LOCAL -> ACKs -> traffic back to gnb-2!
        _wait_until(lambda: (
            ctrl.policy.states['gnb-2'].active_route == 'LOCAL'
            and ctrl.xds.status_snapshot()['sites']['gnb-2']['acked_version'] == f'{ctrl.xds.epoch}-1'
            and _request_envoy(envoy_ports['gnb-2'])[2].get('server_node') == 'gnb-2'
        ))

        # 6. Invalid RouteConfiguration rejected by real Envoy:
        # In REST-JSON xDS, Envoy rejects the invalid config, keeps polling with the old
        # version_info (never ACKing the bad version), and continues serving LOCAL traffic;
        # the controller's ACK timeout marks envoy_update_failed while keeping desired != active.
        _wait_until(lambda: ctrl.policy.states['gnb-1'].active_route == 'LOCAL')
        bad_ver = ctrl.xds.inject_invalid_rds_for_test('gnb-1', 'SPREAD')
        ctrl.policy.states['gnb-1'].desired_route = 'SPREAD'
        ctrl.policy.states['gnb-1'].pending_update_since = sim_now[0]
        # Allow Envoy several poll cycles to receive and reject the invalid RouteConfiguration.
        time.sleep(1.0)
        sim_now[0] += ctrl.policy.config.envoy_ack_timeout_seconds + 1.0
        ctrl.poll_once()
        assert ctrl.xds.status_snapshot()['sites']['gnb-1']['acked_version'] != bad_ver
        assert ctrl.policy.states['gnb-1'].envoy_update_failed is True
        assert ctrl.policy.states['gnb-1'].desired_route == 'SPREAD'
        assert ctrl.policy.states['gnb-1'].active_route == 'LOCAL'
        code, hdrs, body = _request_envoy(envoy_ports['gnb-1'])
        assert code == 200 and hdrs['x-mec-route'] == 'local' and body['server_node'] == 'gnb1'
    finally:
        for cname in containers:
            subprocess.run(['docker', 'rm', '-f', cname], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        admin_srv.shutdown()
        admin_srv.server_close()
        xds_srv.shutdown()
        xds_srv.server_close()
        for srv in backends.values():
            srv.shutdown()
            srv.server_close()
