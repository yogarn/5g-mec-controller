# `gnb1` Traffic-Control Health-Check Failure Diagnosis

**Date:** 2026-11-18  
**Scope:** Read-only diagnosis of recurring `gnb1` (`calico-node-k54wc`) `tc` health-check failures during Phase 6 (75 -> 90 RPS) in the K3s lab (`ssh core`, repo `/home/member/skripsi_yoga/5g-mec-controller`, artifacts `results/joint_floor125_02/proposed/`).  
**Cluster state:** Preserved as-is; no cluster configuration, traffic shaping, or services were modified.

---

## 1. Key Environment & Topology Observations

`kubectl get nodes -o wide` and per-node inspection via SSH show a clear hardware and container-runtime asymmetry on `gnb1`:

| Node | Role | Internal IP | OS Image | Kernel | Container Runtime | vCPUs | RAM | Primary NIC | Calico Pod |
| :--- | :--- | :--- | :--- | :--- | :--- | ---: | ---: | :--- | :--- |
| `core-5g` | control-plane | `10.34.211.140` | Ubuntu 26.04 LTS | `7.0.0-43-generic` | `containerd://2.2.3-k3s1` (embedded) | — | — | — | `calico-node-vqcwr` |
| `gnb-2` | worker | `10.34.211.196` | Ubuntu 26.04 LTS | `7.0.0-43-generic` | `containerd://2.2.3-k3s1` (embedded) | 24 | 30 GiB | `enp129s0` | `calico-node-b2x2k` |
| `gnb-3` | worker | `10.34.210.3` | Ubuntu 26.04 LTS | `7.0.0-43-generic` | `containerd://2.2.3-k3s1` (embedded) | — | — | — | `calico-node-wff8b` |
| `gnb1` (`riset-5g`) | worker | `10.34.211.157` | Ubuntu 24.04.1 LTS | `6.8.0-149-generic` | `containerd://2.2.4` (**external** `containerd.service`) | **2** | **3.8 GiB** | `ens18` | `calico-node-k54wc` |

Additional context from `core`:
- During Window 2 (`results/joint_floor125_02/proposed/controller.log`), `gnb1` had **0** scheduled MEC workload pods (`counts=map[core-5g:0 gnb-2:6 gnb-3:0 gnb1:0]`).
- `repeat-2026-11-18.log` captured the first failure:
  `Campaign stopped: Command '['kubectl', '-n', 'kube-system', 'exec', 'calico-node-k54wc', '-c', 'calico-node', '--', 'tc', 'filter', 'show', 'dev', 'ens18', 'egress', 'pref', '49211']' returned non-zero exit status 1.`
- On `core`, `utils/traffic_control.py` currently has uncommitted working-tree changes that batch `check()` into `_tc_snapshot` (`sh -ec` with `\036` framing) and add `_read_exec` with 3 attempts (`timeout=10`), switching to `KUBECTL_REMOTE_COMMAND_WEBSOCKETS='false'` (SPDY) after attempt 1.

---

## 2. Log Correlation (UTC)

All `journalctl` queries were run with `TZ=UTC` and `--utc` covering `2026-11-18 12:27:40–12:28:25` (Window 1) and `2026-11-18 13:00:40–13:02:05` (Window 2).

### `core-5g` (`k3s` server)
- **Window 1 (`12:27:40`–`12:28:25` UTC):**
  - Bursts of `E1118 ... conn.go:339] Error on socket receive: read tcp 127.0.0.1:6443->127.0.0.1:<port>: use of closed network connection` at `12:27:44–46`, `12:27:56–58`, and `12:28:08`.
  - `12:27:45Z`: `level=warning msg="Proxy error: write failed: write tcp 127.0.0.1:40178->127.0.0.1:10250: write: broken pipe"`
  - `12:28:08Z`: `level=warning msg="Proxy error: read failed: sync from client"` (matches attempt 1 failure timestamp).
- **Window 2 (`13:00:40`–`13:02:05` UTC):**
  - Burst of `E1118 13:00:56–59 conn.go:339] Error on socket receive: read tcp 127.0.0.1:6443->127.0.0.1:<port>: use of closed network connection`.
  - No `Proxy error` line logged in Window 2 (consistent with client-side `timeout=10` killing `kubectl exec` on `TimeoutExpired`).
- **Kernel (`core-5g`):** No non-AppArmor-audit entries in either window.

### `gnb1` (`riset-5g`, `10.34.211.157`)
- **`k3s-agent`:** Only routine 15s `http: TLS handshake error from 10.34.211.6:<port>: remote error: tls: bad certificate` (`12:27:44`, `12:27:59`, `12:28:14`, `13:00:44`, `13:00:59`, `13:01:14` UTC; note klog internal timestamps print in UTC+7 `19:27`/`20:00`). **Zero** agent tunnel disconnects/reconnects and **zero** CRI/streaming errors logged.
- **`containerd.service`:** `-- No entries --` in both windows (verified `journalctl -u containerd` works as `member`).
- **Kernel:** `-- No entries --` in both windows (no soft lockups, RCU stalls, OOMs, or netlink warnings).

### `gnb-2` (healthy worker comparison)
- **`k3s-agent`:** Only normal pod startup/teardown lifecycle messages in both windows.

---

## 3. Timestamped Bounded Probes at Idle (`timeout=10s`)

Executed from `core` targeting `gnb1` (`kube-system/calico-node-k54wc`) and `gnb-2` (`kube-system/calico-node-b2x2k`):

| Node | Probe | Transport | UTC Start | Wall (ms) | Exit | Stdout | Stderr |
| :--- | :--- | :--- | :--- | ---: | ---: | :--- | :--- |
| `gnb1` | `kubectl exec ... -- true` | WebSockets (`true`) | `14:15:13.846` | 99.3 | `0` | No (`0B`) | `''` |
| `gnb1` | `kubectl exec ... -- true` | SPDY (`false`) | `14:15:14.049` | 94.4 | `0` | No (`0B`) | `''` |
| `gnb1` | `kubectl exec ... -- tc -j qdisc show dev ens18` | WebSockets (`true`) | `14:15:13.946` | 102.9 | `0` | Yes (`215B`) | `''` |
| `gnb1` | `kubectl exec ... -- tc -j qdisc show dev ens18` | SPDY (`false`) | `14:15:14.143` | 94.7 | `0` | Yes (`215B`) | `''` |
| `gnb1` | `ssh gnb-1 tc -j qdisc show dev ens18` | SSH | `14:15:14.238` | 1049.0 | `0` | Yes (`215B`) | `''` |
| `gnb1` | `kubectl get --raw /api/v1/nodes/gnb1/proxy/stats/summary` | Non-streaming HTTP | `14:15:15.287` | 71.7 | `0` | Yes (`30142B`) | `''` |
| `gnb-2` | `kubectl exec ... -- true` | WebSockets (`true`) | `14:15:15.359` | 98.1 | `0` | No (`0B`) | `''` |
| `gnb-2` | `kubectl exec ... -- true` | SPDY (`false`) | `14:15:15.577` | 96.1 | `0` | No (`0B`) | `''` |
| `gnb-2` | `kubectl exec ... -- tc -j qdisc show dev enp129s0` | WebSockets (`true`) | `14:16:09.421` | 108.9 | `0` | Yes (`215B`) | `''` |
| `gnb-2` | `kubectl exec ... -- tc -j qdisc show dev enp129s0` | SPDY (`false`) | `14:16:09.530` | 105.5 | `0` | Yes (`215B`) | `''` |
| `gnb-2` | `ssh gnb-2 tc -j qdisc show dev enp129s0` | SSH | `14:16:09.636` | 263.5 | `0` | Yes (`215B`) | `''` |
| `gnb-2` | `kubectl get --raw /api/v1/nodes/gnb-2/proxy/stats/summary` | Non-streaming HTTP | `14:15:16.042` | 85.3 | `0` | Yes (`81083B`) | `''` |

All idle `kubectl exec` probes (both WebSockets and SPDY) and non-streaming kubelet requests pass in ~72–109 ms.

---

## 4. Layer Attribution

1. **Not a K3s agent tunnel drop or non-streaming HTTP proxy failure:**
   `gnb1` `k3s-agent` had zero tunnel reconnects in both failure windows, and both node metrics (during the failures) and `/proxy/stats/summary` (at idle) succeeded.
2. **Not an idle transport issue:**
   Both `KUBECTL_REMOTE_COMMAND_WEBSOCKETS=true` and `false` pass in ~95–103 ms at idle, yet both failed/timed out during Phase 6 in Window 2.
3. **Fault is on the `gnb1` exec/streaming path under Phase 6 load (75 -> 90 RPS):**
   Because idle probes pass, the current evidence cannot yet separate three candidate bottlenecks on `gnb1` under Phase 6 load:
   - **Local `tc`/netlink** on 2-vCPU `gnb1` (e.g., `tc` process or netlink dump starved under Phase 6 packet/CPU pressure).
   - **`k3s-agent` / external `containerd 2.2.4` CRI exec** on `gnb1` (exec spawn or stream I/O stalling under 2-vCPU contention).
   - **`kubectl` / `k3s` API streaming proxy** on `core-5g` (dropping/stalling upgraded SPDY/WebSocket streams, consistent with `Proxy error: read failed: sync from client` in Window 1 and `conn.go:339` errors in both windows).

---

## 5. Recommended Smallest Instrumented Phase-6 Run (for Codex)

Run the existing experiment up to Phase 6 (keeping the 10 ms `TrafficDelay` shaping and offered-load schedule intact), and run a 1–2 Hz sidecar probe script from `core` during Phase 6 with a 5s timeout per probe, logging `(utc_ts, wall_ms, rc, stdout_bytes, stderr)`:

1. **Probe A — Local `tc`/netlink (bypassing K3s & containerd):**
   Over a persistent `ssh -M -S` ControlMaster to `gnb-1` (eliminating per-probe SSH handshake overhead), run:
   - `tc -j qdisc show dev <gnb1-ifb>`
   - `tc filter show dev ens18 egress pref 49211`
   *Interpretation:* If Probe A stalls/times out, the bottleneck is local `tc`/netlink or 2-vCPU scheduling on `gnb1`.
2. **Probe B — Local CRI exec (bypassing `core-5g` K3s API/proxy & `k3s-agent`):**
   Over the same SSH ControlMaster on `gnb-1`, run:
   - `crictl exec <calico-node-cid> true`
   - `crictl exec <calico-node-cid> tc -j qdisc show dev ens18`
   *Interpretation:* If Probe A is fast (<50 ms) while Probe B stalls, the bottleneck is external `containerd 2.2.4` / `runc` CRI exec on `gnb1`.
3. **Probe C — K3s streaming exec vs. non-streaming kubelet proxy:**
   From `core`, run:
   - `KUBECTL_REMOTE_COMMAND_WEBSOCKETS=true kubectl -n kube-system exec calico-node-k54wc -c calico-node -- true`
   - `KUBECTL_REMOTE_COMMAND_WEBSOCKETS=false kubectl -n kube-system exec calico-node-k54wc -c calico-node -- true`
   - `kubectl get --raw /api/v1/nodes/gnb1/proxy/stats/summary`
   *Interpretation:* If Probe B is fast and `/proxy/stats/summary` is fast while Probe C stalls, the bottleneck is the `k3s` API streaming proxy (`core-5g`) or `k3s-agent` streaming server.
4. **CPU pressure correlation:**
   Sample `/proc/pressure/cpu` and `mpstat 1` on `gnb1` during the same Phase 6 window to correlate any probe stall with 2-vCPU saturation.

## 6. Codex follow-up

The preserved UE request log for the second failure places the start of Phase 6 at `13:00:15.874 UTC`. All 7,711 recorded Phase 6 HTTP requests succeeded before the UE runner stopped, including 2,155 through `gnb1`. This is a partial phase with 10,800 scheduled slots. The inference path was serving requests while the health check was failing. Successful node scrapes and the absence of agent reconnects narrow the investigation to the exec path but leave transient streaming and node-local stalls possible.

The proposed-only diagnostic is `results/gnb1_phase6_diagnostic_01/` on core. It reuses the original first six 120-second phases, 5 RPS per UE, and 10 ms shaping with the batched health check deployed. The diagnostic wrapper is saved as `diagnostic.py` in that directory. It schedules one read-only probe per second across the probe types from late Phase 5 through Phase 6. The CPU probe reads `/proc/pressure/cpu` and `/proc/stat` over the same persistent SSH connection used for node-local `tc`. The `member` account on `gnb1` cannot access the root-owned containerd socket, and noninteractive sudo requires a password. The diagnostic therefore leaves the direct CRI exec layer unmeasured.

Phase 6 completed with 10,751 successful requests, zero failures, and 49 slots missed during UE attachment. All 120 sidecar probes tagged Phase 6 exited successfully. Maximum wall times were 15.9 ms for node-local IFB qdisc reads, 14.8 ms for node-local filter reads, 118.9 ms for WebSocket IFB qdisc reads, 120.9 ms for SPDY IFB qdisc reads, and 73.1 ms for the non-streaming kubelet request. The slowest probe overall was a WebSocket `exec -- true` at 295.4 ms. Raw timestamps, byte counts, and stderr are in `proposed/phase6-probes.jsonl`.

Core and the UE host reported matching request-log checksums. The campaign log is saved as `campaign.log` in the diagnostic directory. Cleanup removed the workload and left all four shaping nodes clear of IFB devices. The original failed campaign remains at `results/joint_floor125_02/`.

The sampled aggregate CPU utilization on `gnb1` ranged from 17.9% to 26.5% over approximately nine-second intervals during Phase 6. CPU pressure `some avg10` ranged from 8.10% to 8.56%. The same offered load completed with the batched health checks and this level of CPU utilization. The run did not reproduce the earlier CPU spike or any exec failure, so it does not identify the original failure layer. If it recurs, capture node-local `tc`, direct CRI exec with an authorized operator, and Kubernetes exec concurrently during the stall.
