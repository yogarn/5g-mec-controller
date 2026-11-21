#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

campaign="mobility_default_$(date -u +%Y%m%dT%H%M%SZ)"
if (( $# )) && [[ $1 != -* ]]; then
  campaign=$1
  shift
fi

flags=()
while (( $# )); do
  case "$1" in
    --prefer-close)
      flags+=(--prefer-close-routing-only --site-anchors --fixed-replicas=6)
      shift
      ;;
    --check)
      flags+=("$1")
      shift
      ;;
    --plan-only)
      if (( $# < 2 )) || [[ -z $2 || $2 == -* ]]; then
        echo '--plan-only requires an output directory' >&2
        exit 2
      fi
      flags+=("$1" "$2")
      shift 2
      ;;
    *)
      echo 'Usage: bash run-mobility-default.sh [campaign] [--prefer-close] [--check | --plan-only DIR]' >&2
      exit 2
      ;;
  esac
done

exec bash run-mobility-cluster-wide.sh "$campaign" --config experiments/campaign-mobility-core.yaml "${flags[@]}"
