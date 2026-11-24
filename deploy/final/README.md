# Pre-rendered manifests and copy-paste runbooks for the final experiments

Every YAML here is exactly what `experiments.campaign` rendered for the final runs (the `--plan-only`
output of the final profiles, plus the in-cluster placement ConfigMap the campaign builds at run time).
Nothing is generated when you use them.

| scenario | runbook | arms |
|---|---|---|
| joint (placement + autoscaling + offloading) | [`joint/README.md`](joint/README.md) | `baseline-local`, `proposed-local`, `baseline-vertical`, `proposed-vertical` |
| mobility (placement only, fixed replicas) | [`mobility/README.md`](mobility/README.md) | `baseline-local`, `proposed-local`, `baseline-vertical`, `proposed-vertical` |
| scaling (autoscaler only, core-5g) | [`scaling/README.md`](scaling/README.md) | `baseline`, `proposed` |

Each arm folder holds the numbered manifests (`01-...`, `02-...` in apply order) and `ue-payload.json`,
the exact stdin the campaign sends to the UE runner. Each runbook has one self-contained section per
arm with every command written out; nothing says "same for the others".

## Where things run

- **core** (`ssh core`): everything cluster-side. Project at `/home/member/skripsi_yoga/5g-mec-controller`.
- **UE host** `member@10.34.211.6`: the UE processes. The runbooks start them from core over SSH
  (`sudo -n`), exactly as the campaign does.
- In **every** terminal on core, first run `source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh`
  (project directory, `KUBECONFIG`, `NS`, `APP`, `A`, `TOKEN`, `H`). Without it `kubectl` talks to the
  wrong cluster config and `$NS`/`$APP` are empty.
- Four terminals on core: **T1** commands, **T2** the 10 ms path delay (`tools/hold_tc_delay.py`),
  **T3** the offloader port-forward (joint and mobility only), **T4** monitoring.

## Before the first run

1. **UE host revision.** The UE host's checkout is on the old commit `88e7cc8` (clean), core is on the
   frozen commit. The manual commands in the runbooks do not compare revisions and the UE-side code they
   use is identical in both, so they work as is. The automatic `demo.sh` / campaign preflight does compare
   them ("core and UE project revisions differ") and will refuse until the UE host is on the same commit.
2. **What is not YAML** and is therefore in the runbooks as commands: the PreferClose zone labels
   (`gnb1`, `gnb-2`, `gnb-3`; `core-5g` is not labelled), the 10 ms `tc` delay, the offloader admin API
   (reset / Core pool / vertical mode), the one-pod seed of the managed Deployment (done before traffic by default; the runbooks show how to do it
   at `[RUN]` instead, as the recorded runs did), and the UE runner.
3. **Cluster state right now** (read on core): zones on `gnb1`, `gnb-2`, `gnb-3` are already set to
   their own name, the Core pool is at 3 replicas, and the Calico pool `mec-campaign-reserved`
   (172.16.40.0/23) exists.

## Status

The manifests and `ue-payload.json` files are the campaign's own output. The commands around them were
written from `experiments/campaign.py`, `experiments/vertical.py`, `experiments/campaign_ue.py` and
`utils/traffic_control.py`, and every bash block passes `bash -n`, but they have not been executed
end to end. `tools/hold_tc_delay.py` is new and has not been run against the cluster. If a manual arm and
the campaign disagree, the campaign is the reference.
