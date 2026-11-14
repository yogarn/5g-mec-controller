# 5G MEC placement controller

A three-gNB testbed controller and matched HPA/proposed experiment runner. The primary experiment shifts UE traffic toward **gNB-3**. See [EXPERIMENTS.md](EXPERIMENTS.md) for setup and the complete automated workflow.

On core, after one-time setup:

```bash
bash experiments/run.sh run_2
```

That single command resets the isolated experiment app, runs baseline, resets again, runs proposed, stops its processes, transfers and checksums UE results, summarizes and plots. Each campaign requires a new name. Application resources live in **mec-experiment**; the shared collector and Prometheus remain in **placement**. No manual HPA, sampler, UE, transfer or recovery sequence is required for a normal run.

## Controller

`config.yaml` selects the workload manifest, Prometheus queries, SLO, eligible nodes, bounds and policy. The default is non-mutating. `python -m controller --config PATH --once` observes once. Continuous mutating operation requires `mutate: true`; `--deploy` additionally creates missing Deployment/Service resources from the manifest. The namespace must already exist; the campaign runner creates it. Existing resources are not overwritten by controller bootstrap.

Scaling targets `ceil(replicas × all-attempt p90 / SLO)`, within the configured 1–100 bounds. New decisions wait for ready replicas and respect cooldown; sustained scale-down uses a stabilization window. Mobility-aware scale-up plans each added replica against projected capacity and applies the per-node counts as a batch without cooldown between internal steps. Node-pinned managed Deployments share the original Service. Rescheduling is one surge-first replacement at a time, with at most one temporary replica above the desired maximum.

The default score combines 60% resource headroom and 40% traffic share. The resource component weights CPU/RAM at 40% each and GPU/VRAM at 10% each. Nodes without GPUs contribute zero GPU/VRAM headroom; missing required telemetry fails closed. CPU/RAM headroom uses allocatable capacity minus requested resources across all workloads. Traffic share is a request-attempt proxy, not observed mobility. Service routing does not guarantee each UE reaches its local pod. The cap is not a performance guarantee.

## Telemetry

The single collector receives authenticated UE observations, joins source addresses against SMF/AMF data, and exposes a rolling all-attempt p90, successful-only p90, attempt/success/SLO counts and per-node attempt counts. Prometheus scrapes `ue-telemetry.placement.svc.cluster.local:8080`. NodePort 31669 is used for UE submissions.

Unknown or stale mappings contribute only to global metrics. Client-asserted UE addresses are not independent proof of physical attachment. Fast failures can lower all-attempt p90, so report failures separately. Global p90 cannot be obtained by averaging multiple collector p90s; run one collector. The app URL is the dynamically discovered face-detection NodePort, not a proxy endpoint.

## Provisioning UE subscribers

This is a **first-time setup task**, not part of every campaign. Existing Open5GS, three registered gNBs, UERANSIM and three IPv4 DNNs with nonoverlapping address pools must already work. Provisioning does not configure DNNs, UPFs, gNBs or routing.

For each subscriber, create three private copies of a working UERANSIM UE YAML, one per site. Each copy must share the same unique 15-digit `imsi-...` SUPI, authentication `key`, `op` (OPC), `opType: OPC`, and `amf`. Set the intended site's `gnbSearchList` and one IPv4 session's `apn`; use the same numeric `slice.sst`. The provisioning tool supports one SST without SD and one session per site configuration.

Create a private inventory with this shape, replacing the example paths and addresses with reserved values inside the configured pools:

```yaml
subscribers:
  - gnb1: {config: /secure/ue-01-gnb1.yaml, ip: 10.62.1.101}
    gnb2: {config: /secure/ue-01-gnb2.yaml, ip: 10.63.1.101}
    gnb3: {config: /secure/ue-01-gnb3.yaml, ip: 10.64.1.101}
```

Each subscriber needs all three entries, even if a phase assigns zero UEs to a site. Config paths are relative to the inventory. Subscriber SUPIs and all site-specific IPs must be unique, reserved outside dynamic allocation, and unused by other sessions. Restrict file permissions and keep authentication material out of Git.

On a trusted host with MongoDB access, install requirements and set `OPEN5GS_MONGO_URI` securely if it differs from `mongodb://127.0.0.1:27017/`. Stop other provisioning writers, check the redacted plan, then explicitly insert:

```bash
python3 -m experiments.provision_ues --inventory /secure/ue-inventory.yaml --verbose
python3 -m experiments.provision_ues --inventory /secure/ue-inventory.yaml --apply
```

The tool inserts only new subscribers and rejects existing IMSIs/IPs; it never updates or deletes existing records. Do not reprovision the reserved campaign pool. Database writes are not transactional; after a partial failure inspect the database before retrying. Verify registration, assigned static IP, DNN/UPF anchor and packet paths independently after provisioning.

## Data and measurement

Campaign settings are in `experiments/campaign.yaml`; controller settings in `config.yaml`. The runner uses the same digest-pinned app, eight sorted image inputs, resources, node eligibility and 180-second phase schedule for both methods. UE switching stops and reattaches sessions; it is **not handover**. UE count is closed-loop concurrency, not a fixed offered request rate.

The selected synthetic images under `apps/face-detection/images` come from [Syn-Vis-v0](https://huggingface.co/datasets/retowyss/Syn-Vis-v0), Reto Wyss (2025), revision `100262732989e77f38cd831d70a376a93735006a`. The dataset card dedicates individual images to CC0 and licenses the compilation/documentation under CC-BY-SA 4.0. These CGI portraits are not representative of all real-world faces. Preserve image hashes and assignments recorded in `run.json`.

Raw requests, lifecycle journals, configuration, process logs, sampled resources, Prometheus history, summaries and PNG/PDF plots are retained under `results/CAMPAIGN/` on core; raw UE runs remain on the UE host. Results and `.private/` are ignored by Git. Summaries refuse overwrites. Successful-request latency percentiles differ from the controller's all-attempt p90; SLO attainment includes all attempts. Metrics Server CPU windows may overlap phases. See [RESEARCH.md](RESEARCH.md) for interpretation limits.
