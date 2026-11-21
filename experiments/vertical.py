"""Vertical offloading arm lifecycle helpers (Core pool, Envoy preflight, offloader admin).

Manages Core fallback Deployment scaling, offloader mode switches and resets,
Envoy xDS readiness probes, and artifact collection for vertical experiment arms.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.error import HTTPError
from urllib.request import Request

import yaml
from offloader.budget import VerticalBudget

from experiments.manifests import _site_nodes, workload_identity
from experiments.profiles import (
    APP,
    DEFAULT_SLO_MS,
    NAMESPACE,
    SITE_NODES,
)


def _camp():
    """Return the experiments.campaign module so test monkeypatches are respected."""
    return sys.modules['experiments.campaign']


def _load_offloader_token(settings):
    """Load offloader admin bearer token from file or environment without printing/storing it."""
    token = (
        os.environ.get('OFFLOADER_ADMIN_TOKEN', '').strip()
        or os.environ.get('UE_TELEMETRY_TOKEN', '').strip()
    )
    if token:
        return token
    token_file = settings.get('offloader_token_file') or settings.get('token')
    if token_file and Path(token_file).is_file():
        token = Path(token_file).read_text(encoding='utf-8').strip()
    if not token or '\n' in token or '\r' in token:
        raise RuntimeError(
            'offloader admin token is required (set OFFLOADER_ADMIN_TOKEN or configure offloader_token_file)'
        )
    return token


def _offloader_request(url, *, token, method='GET', payload=None, timeout=5.0):
    headers = {'Authorization': f'Bearer {token}'}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode('utf-8')
        headers['Content-Type'] = 'application/json'
    req = Request(url, data=data, method=method, headers=headers)
    with _camp().urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def reset_offloader(offloader_url, mode, *, token, wait_ack_seconds=10.0):
    """Reset all site routes to LOCAL, clear offloader state, and wait for Envoy epoch-matched ACK."""
    base = offloader_url.rstrip('/')
    result = _camp()._offloader_request(f'{base}/reset', token=token, method='POST', payload={'mode': mode})
    desired_versions = result.get('desired_versions') or {}
    deadline = _camp().time.monotonic() + wait_ack_seconds
    while _camp().time.monotonic() < deadline:
        status = _camp()._offloader_request(f'{base}/status', token=token)
        xds_sites = (status.get('xds') or {}).get('sites') or {}
        policy_sites = status.get('sites') or {}
        if all(
            xds_sites.get(site, {}).get('acked_route') == 'LOCAL'
            and policy_sites.get(site, {}).get('active_route') == 'LOCAL'
            and desired_versions.get(site)
            and xds_sites.get(site, {}).get('acked_version') == desired_versions[site]
            for site in ('gnb-1', 'gnb-2', 'gnb-3')
        ):
            return status
        _camp().time.sleep(0.5)
    raise RuntimeError('Envoy ingress did not ACK LOCAL route reset within timeout')


def _default_envoy_preflight_probe(url, headers=None, timeout=5.0):
    req = Request(url, method='GET', headers=headers or {})
    # The offloader's EDS state can be ready before Envoy polls and applies it.
    deadline = _camp().time.monotonic() + 20.0
    while True:
        try:
            with _camp().urlopen(req, timeout=min(timeout, max(0.1, deadline - _camp().time.monotonic()))) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
                return resp.status, {k.lower(): v for k, v in resp.headers.items()}, raw
        except HTTPError as exc:
            remaining = deadline - _camp().time.monotonic()
            with exc:
                if exc.code != 503 or remaining <= 0:
                    raw = exc.read().decode('utf-8', errors='replace')
                    return exc.code, {k.lower(): v for k, v in exc.headers.items()}, raw
            _camp().time.sleep(min(0.5, remaining))


def vertical_budget_from_settings(settings, *, min_replicas=None, max_replicas=None, arm_mode='vertical',
                                  fixed_total=False):
    """Use the same total ceiling, with three Core fallback slots reserved only for vertical."""
    core_reps = settings.get('core_backend_replicas', 3)
    eff_min = min_replicas if min_replicas is not None else settings.get('min_replicas', 6)
    eff_max = max_replicas if max_replicas is not None else settings.get('max_replicas', 18)
    counts_toward_budget = settings.get('core_counts_toward_budget', True)
    base_budget = VerticalBudget(
        min_replicas=int(eff_min),
        max_replicas=int(eff_max),
        edge_anchors=3,
        core_fallback_replicas=int(core_reps),
        core_counts_toward_budget=bool(counts_toward_budget),
    )
    return base_budget.for_arm_mode(arm_mode, fixed_total=fixed_total)


def get_core_pool_replicas(app, *, arm_name=None):
    """Read current spec.replicas of the experiment Core fallback Deployment."""
    core_dep = f'{app}-core'
    prefix = f'{arm_name}: ' if arm_name else ''
    try:
        dep = _camp().get('deployment', core_dep)
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        detail = getattr(exc, 'stderr', None) or str(exc)
        raise RuntimeError(
            f'{prefix}Core fallback Deployment {NAMESPACE}/{core_dep} is missing or unreadable: {detail}'
        ) from exc
    replicas = (dep.get('spec') or {}).get('replicas')
    return 0 if replicas is None else int(replicas)


def scale_core_pool(app, replicas, *, arm_name=None):
    """Scale only the experiment Core fallback Deployment."""
    core_dep = f'{app}-core'
    prefix = f'{arm_name}: ' if arm_name else ''
    try:
        _camp().kube('-n', NAMESPACE, 'scale', f'deployment/{core_dep}', f'--replicas={int(replicas)}')
    except (OSError, subprocess.SubprocessError) as exc:
        detail = getattr(exc, 'stderr', None) or str(exc)
        raise RuntimeError(
            f'{prefix}failed to scale Core fallback Deployment {NAMESPACE}/{core_dep} to {replicas}: {detail}'
        ) from exc


def wait_core_pool_scaled(app, expected_replicas, *, arm_name=None, core_node='core-5g', timeout=180, poll_interval=1.0):
    """Wait until the Core fallback Deployment reaches exact requested capacity (0 pods or N Ready on core-5g)."""
    core_dep = f'{app}-core'
    prefix = f'{arm_name}: ' if arm_name else ''
    expected = int(expected_replicas)
    deadline = _camp().time.monotonic() + timeout

    if expected == 0:
        while True:
            try:
                dep = _camp().get('deployment', core_dep)
                pods = json.loads(
                    _camp().kube('-n', NAMESPACE, 'get', 'pods', '-l', f'app={core_dep}', '-o', 'json',
                         capture_output=True, text=True).stdout
                ).get('items') or []
            except (OSError, subprocess.SubprocessError, ValueError) as exc:
                raise RuntimeError(
                    f'{prefix}failed inspecting Core fallback pool {NAMESPACE}/{core_dep} while scaling to 0: {exc}'
                ) from exc
            spec_reps = int((dep.get('spec') or {}).get('replicas') or 0)
            status_reps = int((dep.get('status') or {}).get('replicas') or 0)
            if spec_reps == 0 and status_reps == 0 and len(pods) == 0:
                return {'deployment': core_dep, 'expected_replicas': 0, 'remaining_pods': 0, 'ready_pods_on_core': 0}
            if _camp().time.monotonic() >= deadline:
                raise RuntimeError(
                    f'{prefix}Core fallback pool {NAMESPACE}/{core_dep} did not drain to 0 pods within {timeout}s '
                    f'(spec={spec_reps}, status={status_reps}, remaining_pods={len(pods)})'
                )
            _camp().time.sleep(poll_interval)

    try:
        _camp().kube('-n', NAMESPACE, 'rollout', 'status', f'deployment/{core_dep}', f'--timeout={timeout}s')
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(
            f'{prefix}Core fallback pool {NAMESPACE}/{core_dep} rollout did not reach {expected} Ready pods within {timeout}s'
        ) from exc

    while True:
        try:
            pods = json.loads(
                _camp().kube('-n', NAMESPACE, 'get', 'pods', '-l', f'app={core_dep}', '-o', 'json',
                     capture_output=True, text=True).stdout
            ).get('items') or []
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            raise RuntimeError(
                f'{prefix}failed inspecting Core fallback pods for {NAMESPACE}/{core_dep}: {exc}'
            ) from exc
        ready_on_core = 0
        wrong_node = []
        for pod in pods:
            meta = pod.get('metadata') or {}
            if meta.get('deletionTimestamp'):
                continue
            status = pod.get('status') or {}
            phase = status.get('phase')
            is_ready = any(c.get('type') == 'Ready' and c.get('status') == 'True'
                           for c in (status.get('conditions') or []))
            node = (pod.get('spec') or {}).get('nodeName')
            if phase == 'Running' and is_ready:
                if node == core_node:
                    ready_on_core += 1
                else:
                    wrong_node.append(f'{meta.get("name")}@{node}')
        if wrong_node:
            raise RuntimeError(
                f'{prefix}Core fallback pool {NAMESPACE}/{core_dep} has Ready pod(s) off {core_node}: {", ".join(wrong_node)}'
            )
        if ready_on_core == expected:
            return {'deployment': core_dep, 'expected_replicas': expected, 'ready_pods_on_core': ready_on_core}
        if _camp().time.monotonic() >= deadline:
            raise RuntimeError(
                f'{prefix}Core fallback pool {NAMESPACE}/{core_dep} reached {ready_on_core}/{expected} '
                f'Ready pods on {core_node} within {timeout}s'
            )
        _camp().time.sleep(poll_interval)


def set_offloader_mode(offloader_url, mode, *, token, arm_name=None):
    """Switch offloader policy mode (e.g. local-only -> vertical) and verify /status."""
    base = offloader_url.rstrip('/')
    prefix = f'{arm_name}: ' if arm_name else ''
    _camp()._offloader_request(f'{base}/mode', token=token, method='POST', payload={'mode': mode})
    status = _camp()._offloader_request(f'{base}/status', token=token)
    if status.get('mode') != mode:
        raise RuntimeError(
            f'{prefix}failed to switch offloader mode to {mode!r} (status reported {status.get("mode")!r})'
        )
    return status


def wait_offloader_core_endpoints(offloader_url, expected_count, *, token, arm_name=None,
                                  timeout=20.0, poll_interval=0.5, max_count=None):
    """Wait until offloader EDS reflects the expected Core pool and optional managed pods."""
    base = offloader_url.rstrip('/')
    prefix = f'{arm_name}: ' if arm_name else ''
    expected = int(expected_count)
    maximum = expected if max_count is None else max_count
    deadline = _camp().time.monotonic() + timeout
    actual = None
    while True:
        status = _camp()._offloader_request(f'{base}/status', token=token)
        core_eps = ((status.get('xds') or {}).get('endpoints') or {}).get('core-5g') or []
        actual = len(core_eps)
        if expected <= actual <= maximum:
            return core_eps
        if _camp().time.monotonic() >= deadline:
            raise RuntimeError(
                f'{prefix}offloader EDS core-5g endpoint count is {actual} '
                f'(expected {expected}{".." + str(maximum) if maximum != expected else ""}) after {timeout:g}s'
            )
        _camp().time.sleep(poll_interval)


def verify_vertical_arm_readiness(settings, vertical_mode, *, token, probe_fn=None, budget=None,
                                  arm_name=None, expected_offloader_mode=None):
    """Gate every arm before UE traffic: verify LOCAL ACK, 3 edge backends, and arm-specific Core EDS state."""
    prefix = f'{arm_name}: ' if arm_name else ''
    if probe_fn is None:
        probe_fn = _default_envoy_preflight_probe
    if budget is None:
        budget = _camp().vertical_budget_from_settings(settings, arm_mode=vertical_mode)
    base = settings['offloader_url'].rstrip('/')
    status = _camp()._offloader_request(f'{base}/status', token=token)
    want_mode = expected_offloader_mode if expected_offloader_mode is not None else vertical_mode
    if status.get('mode') != want_mode:
        raise RuntimeError(f"{prefix}offloader mode mismatch: expected {want_mode!r}, got {status.get('mode')!r}")

    xds = status.get('xds') or {}
    xds_sites = xds.get('sites') or {}
    policy_sites = status.get('sites') or {}
    endpoints = xds.get('endpoints') or {}
    envoy_ports = settings['envoy_site_ports']
    site_ips = settings['site_node_ips']

    local_summary = {}
    for site in ('gnb-1', 'gnb-2', 'gnb-3'):
        xs = xds_sites.get(site) or {}
        ps = policy_sites.get(site) or {}
        if (
            xs.get('desired_route') != 'LOCAL'
            or xs.get('acked_route') != 'LOCAL'
            or not xs.get('desired_version')
            or xs.get('acked_version') != xs.get('desired_version')
            or ps.get('active_route') != 'LOCAL'
        ):
            raise RuntimeError(
                f'{prefix}readiness gate failed for {site}: LOCAL route is not confirmed active '
                f'(desired={xs.get("desired_route")}, acked={xs.get("acked_route")}, '
                f'desired_ver={xs.get("desired_version")}, acked_ver={xs.get("acked_version")})'
            )
        local_eps = endpoints.get(f'local-{site}') or []
        if not local_eps:
            raise RuntimeError(f'{prefix}readiness gate failed for {site}: no ready local backend endpoints in EDS')

        ready_url = f'http://{site_ips[site]}:{envoy_ports[site]}/ready'
        code, hdrs, _ = probe_fn(ready_url, headers={})
        if code != 200 or hdrs.get('x-mec-site') != site or hdrs.get('x-mec-route') != 'local':
            raise RuntimeError(
                f'{prefix}readiness gate failed for {site}: Envoy local preflight {ready_url} returned '
                f'status={code} x-mec-site={hdrs.get("x-mec-site")!r} x-mec-route={hdrs.get("x-mec-route")!r}'
            )
        local_summary[site] = {
            'acked_version': xs.get('acked_version'),
            'ready_endpoints': len(local_eps),
            'preflight_url': ready_url,
            'preflight_status': code,
        }

    core_eps = endpoints.get('core-5g') or []
    expected_core = budget.core_fallback_replicas
    core_preflight_checked = False
    if vertical_mode == 'local-only':
        if expected_core != 0:
            raise RuntimeError(
                f'{prefix}readiness gate failed: local-only budget must have core_backend_replicas=0 (got {expected_core})'
            )
        if len(core_eps) != 0:
            raise RuntimeError(
                f'{prefix}readiness gate failed: local-only arm still has {len(core_eps)} Core endpoint(s) in EDS (expected 0)'
            )
    else:
        if expected_core < 1:
            raise RuntimeError(
                f'{prefix}readiness gate failed: vertical arm requires core_backend_replicas >= 1 (got {expected_core})'
            )
        max_core = expected_core + budget.dynamic_min
        if not expected_core <= len(core_eps) <= max_core:
            raise RuntimeError(
                f'{prefix}readiness gate failed: Core backend ready replicas ({len(core_eps)}) '
                f'does not match configured core_backend_replicas '
                f'({expected_core}{" plus up to " + str(budget.dynamic_min) + " managed pods" if budget.dynamic_min else ""})'
            )
        for site in ('gnb-1', 'gnb-2', 'gnb-3'):
            ready_url = f'http://{site_ips[site]}:{envoy_ports[site]}/ready'
            code, hdrs, _ = probe_fn(ready_url, headers={'X-MEC-Preflight': 'core'})
            if code != 200 or hdrs.get('x-mec-route') != 'core':
                raise RuntimeError(
                    f'{prefix}readiness gate failed for {site}: Core backend unreachable through Envoy '
                    f'(status={code}, x-mec-route={hdrs.get("x-mec-route")!r})'
                )
        core_preflight_checked = True

    return {
        'mode': status.get('mode'),
        'arm_mode': vertical_mode,
        'epoch': xds.get('epoch'),
        'budget': budget.to_metadata(),
        'local_sites': local_summary,
        'core_backend_expected_replicas': expected_core,
        'core_backend_actual_ready': len(core_eps),
        'core_counts_toward_budget': budget.core_counts_toward_budget,
        'core_preflight_checked': core_preflight_checked,
    }


def prepare_vertical_arm(settings, app, vertical_mode, vertical_budget, *, token, arm_name,
                         probe_fn=None, core_wait_timeout=180, eds_wait_timeout=20.0):
    """Manage Core fallback pool lifecycle and readiness gate before starting arm traffic."""
    offloader_url = settings['offloader_url']
    # Fail clearly if the experiment Core fallback Deployment is missing before mutating routes.
    _camp().get_core_pool_replicas(app, arm_name=arm_name)

    if vertical_mode == 'local-only':
        # 1. Reset offloader to LOCAL and wait for all 3 Envoy ACKs.
        _camp().reset_offloader(offloader_url, 'local-only', token=token)
        # 2. Scale Core fallback Deployment to 0 and verify 0 pods remain.
        _camp().scale_core_pool(app, 0, arm_name=arm_name)
        pool_state = _camp().wait_core_pool_scaled(app, 0, arm_name=arm_name, timeout=core_wait_timeout)
        # 3. Verify Core EDS is empty, then run full local-only readiness gate (checks 3 edge backends).
        _camp().wait_offloader_core_endpoints(offloader_url, 0, token=token, arm_name=arm_name, timeout=eds_wait_timeout)
        readiness = _camp().verify_vertical_arm_readiness(
            settings, 'local-only', token=token, probe_fn=probe_fn,
            budget=vertical_budget, arm_name=arm_name, expected_offloader_mode='local-only',
        )
        readiness['core_pool_state'] = pool_state
        return readiness

    if vertical_mode == 'vertical':
        # 1. Start with all routes LOCAL (local-only mode while bringing up Core pool).
        _camp().reset_offloader(offloader_url, 'local-only', token=token)
        # 2. Scale Core pool to target (3) and wait for Ready pods on core-5g.
        _camp().scale_core_pool(app, vertical_budget.core_fallback_replicas, arm_name=arm_name)
        pool_state = _camp().wait_core_pool_scaled(
            app, vertical_budget.core_fallback_replicas, arm_name=arm_name, timeout=core_wait_timeout,
        )
        # 3. Wait for all Core endpoints in EDS and verify reachability from all 3 Envoys while still LOCAL.
        _camp().wait_offloader_core_endpoints(
            offloader_url, vertical_budget.core_fallback_replicas,
            token=token, arm_name=arm_name, timeout=eds_wait_timeout,
            **({'max_count': vertical_budget.core_fallback_replicas + vertical_budget.dynamic_min}
               if vertical_budget.dynamic_min else {}),
        )
        readiness = _camp().verify_vertical_arm_readiness(
            settings, 'vertical', token=token, probe_fn=probe_fn,
            budget=vertical_budget, arm_name=arm_name, expected_offloader_mode='local-only',
        )
        # 4. Activate vertical mode only after the full readiness gate passes.
        _camp().set_offloader_mode(offloader_url, 'vertical', token=token, arm_name=arm_name)
        readiness['mode'] = 'vertical'
        readiness['core_pool_state'] = pool_state
        return readiness

    raise ValueError(f'{arm_name}: unsupported vertical_mode {vertical_mode!r}')


def cleanup_vertical_state(settings, app, *, token, initial_core_replicas=None, output_dir=None,
                           arm_name=None, restore_core_pool=False, raise_on_error=True, core_wait_timeout=180):
    """Reset offloader routes to LOCAL first, then restore Core fallback pool to initial_core_replicas."""
    errors = []
    if settings.get('offloader_url'):
        if token is None:
            try:
                token = _camp()._load_offloader_token(settings)
            except RuntimeError as exc:
                msg = f'Failed to load offloader token during cleanup: {exc}'
                errors.append(msg)
                if output_dir is not None:
                    (Path(output_dir) / 'offload-reset-error.txt').write_text(msg + '\n')
                print(f'ERROR: {msg}', file=sys.stderr, flush=True)
        if token is not None:
            try:
                _camp().reset_offloader(settings['offloader_url'], 'local-only', token=token)
            except (OSError, ValueError, RuntimeError) as exc:
                msg = f'Failed to reset offloader routes to LOCAL during cleanup: {type(exc).__name__}: {exc}'
                errors.append(msg)
                if output_dir is not None:
                    (Path(output_dir) / 'offload-reset-error.txt').write_text(msg + '\n')
                print(f'ERROR: {msg}', file=sys.stderr, flush=True)

    if restore_core_pool and initial_core_replicas is not None:
        try:
            if errors and initial_core_replicas < _camp().get_core_pool_replicas(app, arm_name=arm_name):
                raise RuntimeError('LOCAL routes must be acknowledged before reducing Core capacity')
            _camp().scale_core_pool(app, initial_core_replicas, arm_name=arm_name)
            _camp().wait_core_pool_scaled(app, initial_core_replicas, arm_name=arm_name, timeout=core_wait_timeout)
        except (OSError, subprocess.SubprocessError, ValueError, RuntimeError) as exc:
            msg = (
                f'Failed to restore Core fallback pool {NAMESPACE}/{app}-core '
                f'to initial replicas ({initial_core_replicas}) during cleanup: {type(exc).__name__}: {exc}'
            )
            errors.append(msg)
            if output_dir is not None:
                (Path(output_dir) / 'core-pool-restore-error.txt').write_text(msg + '\n')
            print(f'ERROR: {msg}', file=sys.stderr, flush=True)

    if errors and raise_on_error:
        raise RuntimeError('; '.join(errors))
    return errors


def fetch_offloader_artifacts(offloader_url, output_dir, *, token):
    """Save offloader decision/ACK events and final status for an experiment arm."""
    base = offloader_url.rstrip('/')
    events = _camp()._offloader_request(f'{base}/events', token=token).get('events') or []
    with (output_dir / 'offload-events.jsonl').open('w', encoding='utf-8') as f:
        for ev in events:
            f.write(json.dumps(ev, sort_keys=True) + '\n')
    status = _camp()._offloader_request(f'{base}/status', token=token)
    (output_dir / 'offload-status-final.json').write_text(json.dumps(status, indent=2) + '\n')
