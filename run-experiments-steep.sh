#!/usr/bin/env bash
# Phase 1 has three fixed site anchors and one managed pod after the seed.
# Both arms use PreferClose and the same latency HPA (managed 1–13, total 4–16).
set -euo pipefail
cd "$(dirname "$0")"
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

usage() {
  cat <<'USAGE'
Usage: bash run-experiments-steep.sh [--spill] [--plan-only DIRECTORY]

By default, run the two edge-only, spill-off campaigns in sequence.
--spill        Run the two spill-on campaigns with core-5g eligible instead.
--plan-only    Write one offline plan directory per campaign and exit.
USAGE
}

condition=off
condition_flags=(--edge-only)
plan_root=
while (( $# )); do
  case "$1" in
    --spill)
      condition=spill
      condition_flags=(--spill-high 5 --spill-low 3)
      shift
      ;;
    --plan-only)
      if [[ $# -lt 2 || -z ${2:-} || $2 == -* ]]; then
        echo '--plan-only requires an output directory' >&2
        exit 2
      fi
      plan_root=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

common_flags=(
  --config experiments/campaign-steep.yaml --comparison placement
  --autoscaler hpa --prefer-close --site-anchors --min-replicas 3 --max-replicas 16
  --slo-ms 150 --latency-floor-ms 125 --tc-ms 10 --locality-weight 0.4
)

run_campaign() {
  local number=$1 order=$2
  local name="steep_pl_${condition}_${number}"
  local plan_flags=()
  printf '\n%s (%s)\n' "$name" "$order"
  if [[ -n $plan_root ]]; then
    plan_flags=(--plan-only "$plan_root/$name")
  else
    echo 'Node CPU usage before the campaign'
    kubectl top nodes
    echo 'gnb1 allocated resources before the campaign'
    kubectl describe node gnb1 | sed -n '/^Allocated resources:/,/^Events:/p'
  fi
  bash experiments/run.sh "$name" "${common_flags[@]}" "${condition_flags[@]}" \
    --method-order "$order" "${plan_flags[@]}"
}

run_campaign 01 baseline-first
run_campaign 02 proposed-first
