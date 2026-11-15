# MEC experiment runbook for agy

Prepared: 15 November 2026. Scope: follow-up to `results/calibration-20261115/REPORT.md`. This is an execution plan, not a record of completed experiments or validated parameter values. Do not modify the thesis or historical calibration report while executing it.

## Objective and execution order

Establish that replicas improve this application's performance, that placement changes the measured network path, and that the proposed controller improves SLO performance under changing demand. Avoid a full Cartesian product of parameters. Execute gates G0–G3 in order; run G4 only when its trigger applies, then freeze the configuration for G5. Report a failed gate before spending time on later campaigns.

The immediate next batch is G1: 1, 2, and 3 fixed replicas under 6 local UEs, with three fresh repetitions each. G0 checks must precede traffic generation. The three repetitions are an initial pilot budget, not a power calculation or a guarantee of sufficient precision.

## Instructions to agy

1. Read this file, `EXPERIMENTS.md`, the calibration report, applicable `AGENTS.md` files, and the actual runner implementation before acting. Record Git revision and working-tree changes on core and UE hosts; preserve existing work and results.
2. Create a fresh results directory for each run. Save exact manifests, configuration, image digest, input hashes, commands, timestamps, and logs. Keep credentials and subscriber secrets out of reports.
3. Reuse the existing 5G and monitoring infrastructure. Use isolated experiment resources and the existing campaign lock. Do not delete or reconfigure unrelated applications, UPFs, gNBs, subscribers, or monitoring jobs to make results look better.
4. Implement missing experiment capabilities as separate scripts/configuration copies, with effective settings archived per run. Never pretend an unsupported flag exists. Changes required to shared infrastructure need separate user authorization.
5. Execute one stage, summarize its evidence and decision, and proceed only if the gate is met. Do not search for settings that selectively disadvantage the baseline.
6. After each batch, update a results ledger and a short report: what ran, what failed, what was learned, and the exact next action. Distinguish observations, interpretations, and unresolved questions.

## Fixed settings and definitions

Use the current digest-pinned face-detection image, eight calibration PNGs, application worker/thread settings, 500m/2-core CPU requests/limits, and 256Mi/2Gi RAM requests/limits as provisional fixed conditions. Record actual values and input assignment per UE. Do not call these optimal or fully justified.

Keep the current no-retry behavior and 10-second socket timeout. Document whether the timeout bounds the entire attempt or individual socket operations. Use the actual closed-loop request generator; one UE waits for its request before issuing another. Count request starts and completions separately because fixed UE count does not imply fixed requests per second.

For controller pilots use a primary 500 ms target, reflecting the inspected current configuration. Report 300 ms acceptance from the same raw requests as a secondary diagnostic. This does not validate either threshold as an application requirement. Freeze the primary target and its external/application rationale before G5; do not switch it after seeing which controller wins.

Keep locality weight 0.4 and CPU/RAM resource weights 0.5/0.5 initially, matching the inspected configuration. The historical discussion of 0.3 is not a measured optimum. GPU/VRAM weights remain zero. Use normal controller timing for comparative campaigns; debug timing is for troubleshooting only.

For every run calculate:

- Acceptance at threshold T = successful requests completed within T / all initiated attempts. Errors and timeouts do not count as accepted; reconcile unfinished attempts explicitly.
- Violation rate = 1 minus acceptance. Report error/timeout rate separately so fast failures cannot appear to improve service quality.
- Successful-request p50/p90/p95 and the controller's all-attempt p90 as separately named quantities. Report sample counts and the quantile convention.
- Started and completed requests/s, per-site demand, per-backend request counts, ready/pending/terminating replicas, CPU, RAM, and throttling.
- Readiness and action timestamps, controller CPU/RAM, reconciliation duration, API actions, relocations, and recovery trajectory where applicable.

Measure delay components with explicit boundaries. Client elapsed time minus server processing time is a network/transport residual: it can include transfer, queuing, proxies, and connection setup. Do not label that residual pure propagation delay. Record an independent path-delay measurement and server timing; state any remaining decomposition limitation. Do not add component p90 values and call the sum end-to-end p90.

## G0 — Environment, routing, and measurement checks

No parameter sweep. Capture one current inventory before G1 and recheck relevant changes before subsequent batches.

- Record per-node allocatable resources, existing requests, active use, and explicit resources reserved for network/system workloads. Request headroom is not idle physical capacity. Record `gnb1` as currently infeasible for a 500m Pod if the calibration finding still holds; do not remove its existing application silently.
- Verify actual UE attachment, source address, UPF route, entry NodePort, backend node, Service settings, and proxy mode. The standard connectivity preflight does not prove every path or all 24 subscribers.
- Confirm request-to-backend identification. Reuse existing response/log information if available; otherwise add minimal experiment instrumentation and document its overhead. Aggregate replica counts are insufficient to verify balancing.
- Verify collector delivery coverage, per-site counts, sample timestamps/age, CPU metrics window, scrape intervals, and missing-data behavior. Compare collector-window p90 against raw attempts using matched time boundaries.
- Trace one scale-up lifecycle: decision → create → scheduled → container start → Ready → first served request → complete post-readiness measurement window. A 15-second cooldown does not equal 14-second startup plus a 15-second window. Inspect actual readiness/pending guards before changing timers.
- Establish a network-health reference using existing UPF/gNB counters and low-rate connectivity probes. Define an operational stop condition from existing service expectations before loading the shared nodes; do not invent an unmeasured universal safety threshold.

Existing command, executed on core from its checkout:

```bash
bash experiments/run.sh check
```

This check creates temporary application resources and UE sessions. It is not read-only. Do not invoke it while another UE experiment owns the reserved pool.

Gate: known routes, usable telemetry, archived resource budget, and no competing experiment. If measurements are missing, repair measurement before proceeding.

## G1 — Fixed-replica capacity: run this next

Question: does horizontal replication restore acceptable service under the measured overload?

| Factor | Setting |
|---|---|
| UE schedule | `6:0:6:0`; same six reserved UEs and input assignment |
| Application placement | All replicas pinned to `gnb-2` |
| Variable | Fixed replicas: 1, 2, 3 |
| Automatic actions | No HPA, proposed autoscaler, or descheduler |
| Added network delay | None |
| Per-run timing | All Pods Ready, then 60 seconds traffic warm-up, then 120 seconds measurement |
| Replication | Three fresh runs per replica count: nine runs |

Timing is a provisional pilot choice. Preserve warm-up data, exclude it from the predefined steady-state summary, and inspect time series for persistent drift. If runs do not settle, document that and increase the common duration for the next batch; do not selectively extend only poor results.

Suggested blocked order, one clean restart for every entry:

```text
block 1: 2, 1, 3 replicas
block 2: 3, 2, 1 replicas
block 3: 1, 3, 2 replicas
```

Use names `g1-b01-r02`, etc., under a dated campaign directory. Reset app state, collector-window contamination, and UE sessions between runs. Wait for old telemetry to expire before the next measurement.

The existing campaign runner always compares baseline and proposed; it is not a fixed-replica calibration harness. Add a small dedicated harness that reuses the existing UE lifecycle, resource sampler, isolation, and cleanup. Archive its command/help and manifests. Do not emulate this test by setting only `--max-replicas`: that does not fix the actual replica count or placement.

Gate: show the trajectory of acceptance, throughput, per-backend demand, and CPU as replicas increase. If replicas do not improve performance, investigate balancing, application serialization, shared CPU/network limits, and client behavior before proceeding. If performance remains variable, repeat the uncertain conditions rather than sweeping unrelated parameters. Select the smallest tested replica count meeting the predefined service objective consistently; if none does, extend capacity investigation first.

## G2 — Does placement affect the path and service quality?

Question: does putting the same capacity nearer the demand actually change latency with the current routing policy?

Use the fixed replica count selected in G1. Hold all six UEs at `gnb-3`; compare all replicas on `gnb-3` against all on `gnb-2`. Keep scaling and relocation disabled. Repeat each condition three times, alternating order: six runs. Use G1 warm-up/measurement timing.

Use 10 ms configured inter-node shaping as a declared emulated condition if G0 verifies safe isolation and the actual affected path. Measure its achieved effect; do not assume 10 ms configured means a particular HTTP latency increment. Use the same shaping condition for both placements. If shaping cannot be isolated by the fixed-replica harness, implement that support before running; do not apply broad host-wide netem rules.

The current Service uses cluster-wide endpoints. Concentrating more replicas at a hot site does not guarantee that requests from that site use local replicas. Record the site-to-backend request matrix, including connection reuse. The all-local/all-remote comparison establishes a path effect; a later mixed deployment may behave differently.

Gate: demonstrate a reproducible path/latency difference, or report that locality provides little measurable benefit under this routing/workload. If the effect is negligible, do not spend time optimizing locality weights. Do not silently switch to a locality-aware Service: that would create a different experiment condition requiring separate comparison.

## G3 — One short automatic-controller pilot

Question: do both mechanisms execute correctly under a load increase, redistribution, and decrease?

Use a dedicated campaign configuration copy with these four 300-second phases:

```text
3:1:1:1,9:3:3:3,9:1:1:7,3:1:1:1
```

Compare baseline default scheduler + CPU HPA against proposed placement + latency scaling + relocation. Keep resources, image, UE workload, shaping, eligibility, and starting replicas equal. Use a shared provisional cap of 30 only after checking per-node application budgets; it is an operational ceiling, not proof of safe simultaneous 2-core consumption by 30 Pods. Monitor actual concentration and network health.

The inspected runner supports `--config`, `--slo-ms`, `--max-replicas`, `--tc-ms`, `--locality-weight`, and `--resource-weights`. After creating the configuration copy, an existing-interface example is:

```bash
bash experiments/run.sh g3-pilot-01 \
  --config experiments/campaign-g3.yaml \
  --slo-ms 500 --max-replicas 30 \
  --tc-ms 10 --locality-weight 0.4 \
  --resource-weights 0.5 0.5 0 0
```

Use shaping only if validated in G2. This pilot is approximately 40 minutes of generated traffic across both methods, plus startup and UE switching. Do not use `--debug` for evidence about normal policy timing. The stock runner runs baseline first; this single pair is diagnostic, not a causal conclusion.

Inspect pending/ready guards, failed actions, replica overshoot, metric age, cap hits, removal-first service gaps, readiness delays, recovery after each change, and delayed scale-down. Record the HPA's actual 1000m target and timing; it is not an 80%-of-request baseline. Equal stabilization durations alone do not make the two scaling policies equivalent.

Gate: complete, comparable attempts and telemetry, correct actions, no unresolved infrastructure failure, and interpretable response trajectories. Fix implementation defects before tuning performance. If 300 seconds is too short for settling, extend the common phase duration and archive the reason.

## G4 — Conditional sensitivity only

Run only the rows whose trigger is observed. Change one parameter family at a time and use the same short discriminating workload. Keep calibration outcomes separate from G5.

| Trigger | Small follow-up | Decision |
|---|---|---|
| Locality changes observed paths/performance | Compare weights 0.2 and 0.4 on one fixed-total hotspot shift; add 0.3 only if those outcomes differ materially | Prefer a robust region, not the best isolated run |
| CPU/RAM score components produce different feasible rankings | Compare 0.5/0.5 versus 1/0; GPU/VRAM remain zero | If rankings/actions match, no live weight sweep is needed |
| Controller acts before useful post-readiness evidence or oscillates | Compare current cooldown with one longer value derived from measured lifecycle timestamps, while keeping readiness guards | Verify fewer premature actions without excessive recovery delay |
| Replica cap is reached and leaves unmet SLO | Reassess per-node budget, then compare cap 30 with one feasible alternative | Do not run 20/30/40 automatically |
| HPA 1000m target appears materially responsible for the comparison | Compare one lower target derived from measured CPU/SLO curves on the same workload | Report a baseline sensitivity, not a selectively weakened baseline |
| Network co-tenants degrade or per-Pod memory approaches its limit | Stop comparative runs and profile the relevant resource setting | Resource changes require repeating affected calibration |

Start a triggered comparison with three repetitions per new condition, reusing compatible existing pilot measurements cautiously. Increase replication when variability prevents a decision. If multiple parameter families prove influential, use a small interaction design for those factors; do not claim independent effects from changing several settings together.

## G5 — Frozen comparative evaluation

Freeze source revisions, primary SLO and its rationale, resource budgets, timers, weights, routing, shaping, workload, failure accounting, and analysis boundaries before starting. Use new data, not the calibration runs used to choose settings.

Use one compact workload sequence that separates causes:

```text
low balanced → high balanced → same-total hotspot → same-total balanced → low balanced
```

Start from 3 UE for low load and choose the high tier from G1/G3 evidence (9 or 15 UE first; 24 only if needed and feasible). Keep total UE fixed across the redistribution steps, and report achieved request-start shares: UE shares do not guarantee request shares with closed-loop clients. Choose either the existing `1:1:7` style shift or a two-site approximately 50:50 → 20:80 shift and document it; do not run every ratio.

Use at least 300 seconds per phase initially, extending uniformly if pilot lifecycle evidence requires it. Analyze the full transition as a primary outcome and a predefined late-phase interval separately. Define recovery before running, for example as the first of several consecutive complete windows meeting the frozen target; specify the number/window length from pilot noise and report failure to recover by phase end as censored.

Initial budget: three independent matched pairs, balanced/randomized method order within blocks. The stock runner hardcodes baseline then proposed; agy must add and archive explicit method-order support before claiming counterbalanced runs. Use the same workload/input assignment within each pair and reset between methods. Three pairs are a starting budget: report paired differences and uncertainty and add pairs if conclusions remain unstable.

Primary outcomes: all-attempt SLO violation rate and recovery after the fixed-total distribution shift. Secondary outcomes: response-time components, successful latency percentiles, resource/replica time, readiness, controller overhead, relocations, and failures. Report cases where baseline wins or neither succeeds. This compares whole systems; attributing a gain specifically to placement requires the fixed-capacity evidence in G2 or an additional targeted ablation.

Do not automatically repeat the existing 19-phase schedule: it takes about 190 minutes of traffic per pair and mixes many load/distribution conditions. Reserve it for a later coverage check if the compact campaign leaves a specific thesis question unanswered. Stop-and-reattach UE changes are not handovers.

## Required artifacts and completion criteria

For each stage create `results/<campaign>/PLAN.md`, `runs.csv`, raw attempts, resource samples, action logs, archived effective configurations, and `REPORT.md`. Record any unavailable artifact explicitly rather than inventing a measurement.

Ledger columns: stage, run ID, block, method, revision, settings/config path, start/end, offered/completed counts, failures, acceptance at both thresholds, latency summaries, replica/resource summaries, validity flag, exclusion reason, and next decision. Keep failed runs; replace them under new IDs only after recording why they were invalid.

The final report must classify every original calibration claim as supported, provisional, contradicted, or untested, with links to evidence. Specifically cover replica capacity, per-node budget, gnb1 feasibility, Pod resources, HPA target, SLO rationale, timing/window behavior, locality, CPU/RAM weights, readiness, controller overhead, and network co-tenant impact. A deferred sweep is acceptable when the report explains why it was unnecessary; an unmeasured claim is not a validated result.

Approximate initial traffic budget before conditional work: G1 27 minutes, G2 18 minutes, G3 40 minutes, G5 150 minutes for three pairs of five 300-second phases. Total about four hours of generated traffic, plus potentially substantial readiness, UE switching, reset, and transfer time. G0, troubleshooting, and any duration extensions are additional. Execute stage by stage rather than launching this entire budget unattended.
