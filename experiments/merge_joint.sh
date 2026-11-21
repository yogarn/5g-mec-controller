#!/usr/bin/env bash
# Combine the local-only arms of an interrupted joint run with the offloading arms of a later run into one
# four-arm folder, then build the usual four-arm summary and plots.
# Usage: experiments/merge_joint.sh LOCAL_ARMS_RUN OFFLOAD_ARMS_RUN FINAL_RUN   (folders under results/)
set -euo pipefail
cd "$(dirname "$0")/.."
local_run=results/$1; offload_run=results/$2; final=results/$3
[[ -d $final ]] && { echo "refusing to overwrite $final" >&2; exit 1; }
for arm in baseline-local proposed-local; do [[ -f $local_run/$arm/requests.jsonl ]] || { echo "missing $arm in $local_run" >&2; exit 1; }; done
for arm in baseline-vertical proposed-vertical; do [[ -f $offload_run/$arm/offload-events.jsonl ]] || { echo "missing $arm in $offload_run" >&2; exit 1; }; done
mkdir "$final"
for f in campaign.yaml workload.yaml site-anchors.yaml floor-adapter-configmap.yaml adapter-before.json core-pool-before.json node-zones-before.yaml plan.yaml; do
  [[ -f $local_run/$f ]] && cp -a "$local_run/$f" "$final/"
done
cp -a "$local_run/baseline-local" "$local_run/proposed-local" "$final/"
cp -a "$local_run"/baseline-local-runner.log "$local_run"/proposed-local-runner.log "$final/" 2>/dev/null || true
cp -a "$offload_run/baseline-vertical" "$offload_run/proposed-vertical" "$final/"
cp -a "$offload_run"/baseline-vertical-runner.log "$offload_run"/proposed-vertical-runner.log "$final/" 2>/dev/null || true
printf 'local arms from %s\noffload arms from %s\n' "$1" "$2" > "$final/MERGED-FROM.txt"
.venv/bin/python -m utils.vertical_summary "$final" --slo-ms 150
.venv/bin/python -m utils.plot_vertical "$final" "$final/plots" --slo-ms 150
echo "merged: $final"
