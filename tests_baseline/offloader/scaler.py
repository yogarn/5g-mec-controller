"""Single-owner dynamic replica scaler supporting 0..12 dynamic edge pods.

Replaces Kubernetes HPA for vertical-mode arms so that:
- Scaling genuinely starts from 0 dynamic pods on both local-only (3 fixed edge anchors, 0 Core)
  and vertical (3 fixed edge anchors + 3 Core fallback = 6 fixed pods) arms.
- Edge capacity is identical across all arms: 3..15 edge pods (0..12 dynamic), so the same edge
  target produces the same dynamic replica count (e.g. edge target 7 -> 4 dynamic -> 7 total in
  local-only and 10 total in vertical).
- Baseline CPU scaling bootstraps from active edge anchor CPU utilization, then seamlessly
  includes dynamic edge pods once active — always with a single writer to Deployment.spec.replicas.
- Proposed latency scaling uses the proposed p90 latency signal against SLO, scaling 0..12 dynamic
  pods and scaling back down to 0 dynamic after stabilization.
- Terminating and starting/rollout pods are accounted explicitly on every tick.
"""
import argparse
from collections import deque
from dataclasses import dataclass
import json
import math
from pathlib import Path
import signal
import subprocess
import time
from urllib.parse import urlencode
from urllib.request import urlopen

from offloader.budget import VerticalBudget


DEFAULT_LATENCY_FLOOR_MS = 120.0  # 0.8 x the 150 ms SLO


@dataclass
class PodAccounting:
    spec_replicas: int
    ready_dynamic: int
    starting_dynamic: int
    terminating_dynamic: int
    ready_anchors: int

    @property
    def non_terminating_dynamic(self) -> int:
        return self.ready_dynamic + self.starting_dynamic

    @property
    def active_edge_pods(self) -> int:
        return self.ready_anchors + self.ready_dynamic

    @property
    def committed_edge_pods(self) -> int:
        # Use max(spec_replicas, non_terminating_dynamic) so rollout/starting pods do not cause overscaling.
        return self.ready_anchors + max(self.spec_replicas, self.non_terminating_dynamic)

    def to_dict(self) -> dict:
        return {
            'spec_replicas': self.spec_replicas,
            'ready_dynamic': self.ready_dynamic,
            'starting_dynamic': self.starting_dynamic,
            'terminating_dynamic': self.terminating_dynamic,
            'ready_anchors': self.ready_anchors,
            'active_edge_pods': self.active_edge_pods,
            'committed_edge_pods': self.committed_edge_pods,
        }


def account_edge_pods(pods: list[dict], *, spec_replicas: int) -> PodAccounting:
    """Classify edge pods into ready anchors, ready dynamic, starting/rollout, and terminating."""
    ready_anchors = 0
    ready_dynamic = 0
    starting_dynamic = 0
    terminating_dynamic = 0

    for pod in pods:
        meta = pod.get('metadata') or {}
        labels = meta.get('labels') or {}
        role = labels.get('mec-replica') or ''
        status = pod.get('status') or {}
        phase = status.get('phase')
        is_ready = any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in (status.get('conditions') or []))
        is_terminating = bool(meta.get('deletionTimestamp'))

        if role.startswith('anchor-'):
            if not is_terminating and phase == 'Running' and is_ready:
                ready_anchors += 1
        elif role == 'managed':
            if is_terminating:
                terminating_dynamic += 1
            elif phase == 'Running' and is_ready:
                ready_dynamic += 1
            elif phase in ('Pending', 'Running'):
                starting_dynamic += 1

    return PodAccounting(
        spec_replicas=max(0, int(spec_replicas)),
        ready_dynamic=ready_dynamic,
        starting_dynamic=starting_dynamic,
        terminating_dynamic=terminating_dynamic,
        ready_anchors=ready_anchors,
    )


class VerticalDynamicScaler:
    """Single-owner scaler for the dynamic edge Deployment (0..dynamic_max)."""

    def __init__(
        self,
        method: str,
        budget: VerticalBudget,
        *,
        cpu_target_percent: float = 80.0,
        slo_ms: float = 150.0,
        latency_floor_ms: float = DEFAULT_LATENCY_FLOOR_MS,
        rt_target_factor: float = 1.0,
        tolerance: float = 0.10,
        scale_down_stabilization_seconds: float = 60.0,
        scale_up_cooldown_seconds: float = 15.0,
    ):
        if method not in ('baseline', 'proposed'):
            raise ValueError('method must be baseline or proposed')
        self.method = method
        self.budget = budget
        self.cpu_target_percent = float(cpu_target_percent)
        self.slo_ms = float(slo_ms)
        self.latency_floor_ms = float(latency_floor_ms)
        self.rt_target_factor = float(rt_target_factor)
        self.tolerance = float(tolerance)
        self.scale_down_stabilization_seconds = float(scale_down_stabilization_seconds)
        self.scale_up_cooldown_seconds = float(scale_up_cooldown_seconds)
        self._recommendations: deque[tuple[float, int]] = deque()
        self._first_eval_time: float | None = None
        self._last_scale_up_time: float | None = None

    def _raw_desired_dynamic(self, accounting: PodAccounting, signal_value: float | None) -> tuple[int, str, float | None]:
        current_dyn = accounting.spec_replicas
        base_edge = max(self.budget.edge_anchors, accounting.committed_edge_pods)

        if signal_value is None or not math.isfinite(signal_value) or signal_value < 0:
            return current_dyn, 'missing_signal', None

        if self.method == 'baseline':
            # signal_value is average CPU utilization percent of request (e.g., 95.0 for 95%).
            ratio = signal_value / self.cpu_target_percent
        else:
            # signal_value is global p90 latency in ms.
            internal_target = self.slo_ms * self.rt_target_factor
            if self.latency_floor_ms > 0 and internal_target > self.latency_floor_ms:
                excess = max(0.0, signal_value - self.latency_floor_ms)
                ratio = excess / (internal_target - self.latency_floor_ms)
            else:
                ratio = signal_value / internal_target

        # Deadband matching Kubernetes HPA default 10% tolerance.
        if abs(1.0 - ratio) <= self.tolerance:
            return current_dyn, 'within_tolerance', round(ratio, 4)

        desired_edge = math.ceil(base_edge * ratio)
        desired_dyn = self.budget.dynamic_target_for_edge(desired_edge)
        reason = 'scale_up' if desired_dyn > current_dyn else ('scale_down' if desired_dyn < current_dyn else 'steady')
        return desired_dyn, reason, round(ratio, 4)

    def evaluate(self, now: float, accounting: PodAccounting, signal_value: float | None) -> dict:
        """Compute next spec.replicas for the dynamic Deployment with stabilization & rollout guards."""
        current_dyn = accounting.spec_replicas
        raw_dyn, reason, ratio = self._raw_desired_dynamic(accounting, signal_value)

        # Record recommendation for scale-down stabilization window.
        if self._first_eval_time is None:
            self._first_eval_time = now
        self._recommendations.append((now, raw_dyn))
        cutoff = now - self.scale_down_stabilization_seconds
        while self._recommendations and self._recommendations[0][0] < cutoff:
            self._recommendations.popleft()

        target_dyn = current_dyn
        applied_reason = reason

        if raw_dyn > current_dyn:
            if accounting.starting_dynamic > 0:
                applied_reason = 'wait_starting_pods'
            elif self._last_scale_up_time is not None and (now - self._last_scale_up_time) < self.scale_up_cooldown_seconds:
                applied_reason = 'scale_up_cooldown'
            else:
                target_dyn = raw_dyn
                self._last_scale_up_time = now
                applied_reason = 'scale_up'
        elif raw_dyn < current_dyn:
            # Stabilized scale-down: choose the highest recommendation seen in the stabilization window.
            if (now - self._first_eval_time) < self.scale_down_stabilization_seconds:
                applied_reason = 'scale_down_stabilizing'
            else:
                stabilized = max(rec for _, rec in self._recommendations)
                if stabilized < current_dyn:
                    target_dyn = max(self.budget.dynamic_min, stabilized)
                    applied_reason = 'scale_down'
                else:
                    applied_reason = 'scale_down_stabilizing'

        return {
            'timestamp': now,
            'method': self.method,
            'signal_value': signal_value,
            'ratio': ratio,
            'current_dynamic': current_dyn,
            'raw_desired_dynamic': raw_dyn,
            'target_dynamic': target_dyn,
            'target_edge': self.budget.edge_for_dynamic(target_dyn),
            'target_total': self.budget.total_for_dynamic(target_dyn),
            'core_replicas': self.budget.core_fallback_replicas,
            'changed': target_dyn != current_dyn,
            'reason': applied_reason,
            'accounting': accounting.to_dict(),
        }


def _parse_cpu_millicores(raw: str) -> float:
    raw = str(raw).strip()
    if raw.endswith('n'):
        return float(raw[:-1]) / 1e6
    if raw.endswith('u'):
        return float(raw[:-1]) / 1e3
    if raw.endswith('m'):
        return float(raw[:-1])
    return float(raw) * 1000.0


def fetch_edge_cpu_percent_kubectl(namespace: str, app: str, *, cpu_request_m: float = 500.0) -> float | None:
    """Query live CPU usage percent of request across active edge pods (anchors + managed) via kubectl top."""
    try:
        out = subprocess.check_output(
            ['kubectl', '-n', namespace, 'top', 'pods', '-l', f'app={app}', '--no-headers'],
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    usages = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            try:
                usages.append(_parse_cpu_millicores(parts[1]) / cpu_request_m * 100.0)
            except ValueError:
                continue
    if not usages:
        return None
    return sum(usages) / len(usages)


def fetch_p90_ms_prometheus(prometheus_url: str, query: str) -> float | None:
    url = prometheus_url.rstrip('/') + '/api/v1/query?' + urlencode({'query': query})
    try:
        with urlopen(url, timeout=3) as resp:
            data = json.load(resp)
        results = (data.get('data') or {}).get('result') or []
        if not results:
            return None
        val = float(results[0]['value'][1])
        if not math.isfinite(val):
            return None
        return val * 1000.0
    except (OSError, ValueError, KeyError, IndexError):
        return None


def main(argv=None):
    parser = argparse.ArgumentParser(description='Single-owner dynamic edge scaler for vertical offloading')
    parser.add_argument('--method', choices=('baseline', 'proposed'), required=True)
    parser.add_argument('--namespace', default='services')
    parser.add_argument('--app', default='mec-placement-pedestrian-detection')
    parser.add_argument('--min-replicas', type=int, default=6)
    parser.add_argument('--max-replicas', type=int, default=18)
    parser.add_argument('--core-replicas', type=int, default=3)
    parser.add_argument('--cpu-target', type=float, default=80.0)
    parser.add_argument('--cpu-request-m', type=float, default=500.0)
    parser.add_argument('--slo-ms', type=float, default=150.0)
    parser.add_argument('--latency-floor-ms', type=float, default=DEFAULT_LATENCY_FLOOR_MS)
    parser.add_argument('--rt-target-factor', type=float, default=1.0)
    parser.add_argument('--prometheus-url', default='http://10.34.211.140:30990')
    parser.add_argument('--latency-query', default='ue_request_attempt_duration_p90_seconds{job="ue-telemetry"}')
    parser.add_argument('--interval', type=float, default=5.0)
    parser.add_argument('--scale-down-stabilization', type=float, default=60.0)
    parser.add_argument('--scale-up-cooldown', type=float, default=15.0)
    parser.add_argument('--log', type=Path, default=None)
    args = parser.parse_args(argv)

    budget = VerticalBudget(
        min_replicas=args.min_replicas,
        max_replicas=args.max_replicas,
        edge_anchors=3,
        core_fallback_replicas=args.core_replicas,
    )
    scaler = VerticalDynamicScaler(
        args.method,
        budget,
        cpu_target_percent=args.cpu_target,
        slo_ms=args.slo_ms,
        latency_floor_ms=args.latency_floor_ms,
        rt_target_factor=args.rt_target_factor,
        scale_down_stabilization_seconds=args.scale_down_stabilization,
        scale_up_cooldown_seconds=args.scale_up_cooldown,
    )

    stop = False

    def _stop(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while not stop:
        tick = time.monotonic()
        try:
            dep_raw = subprocess.check_output(
                ['kubectl', '-n', args.namespace, 'get', 'deployment', args.app, '-o', 'json'],
                text=True,
                timeout=5,
            )
            spec_replicas = int((json.loads(dep_raw).get('spec') or {}).get('replicas') or 0)
            pods_raw = subprocess.check_output(
                ['kubectl', '-n', args.namespace, 'get', 'pods', '-l', f'app={args.app}', '-o', 'json'],
                text=True,
                timeout=5,
            )
            accounting = account_edge_pods(json.loads(pods_raw).get('items') or [], spec_replicas=spec_replicas)
            if args.method == 'baseline':
                sig = fetch_edge_cpu_percent_kubectl(args.namespace, args.app, cpu_request_m=args.cpu_request_m)
            else:
                sig = fetch_p90_ms_prometheus(args.prometheus_url, args.latency_query)

            decision = scaler.evaluate(time.monotonic(), accounting, sig)
            if decision['changed']:
                subprocess.run(
                    ['kubectl', '-n', args.namespace, 'scale', f'deployment/{args.app}', f'--replicas={decision["target_dynamic"]}'],
                    check=True,
                    timeout=10,
                )
            line = json.dumps(decision, sort_keys=True)
            print(line, flush=True)
            if args.log is not None:
                args.log.parent.mkdir(parents=True, exist_ok=True)
                with args.log.open('a', encoding='utf-8') as f:
                    f.write(line + '\n')
        except Exception as exc:
            print(json.dumps({'event': 'scaler_error', 'error': f'{type(exc).__name__}: {exc}'}), flush=True)
        elapsed = time.monotonic() - tick
        if not stop:
            time.sleep(max(0.1, args.interval - elapsed))


if __name__ == '__main__':
    main()
