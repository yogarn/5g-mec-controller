#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

campaign=${1:-steep_session_probe_01}
url=http://10.34.211.140:31669/sessions
curl --fail --silent --show-error --max-time 3 "$url"

trace=$(mktemp "/tmp/${campaign}.sessions.XXXXXX.jsonl")
python3 collector/probe_sessions.py --url "$url" --interval 0.5 --duration 1800 --output "$trace" &
probe_pid=$!
stop_probe() {
  kill "$probe_pid" 2>/dev/null || true
  wait "$probe_pid" 2>/dev/null || true
}
trap stop_probe EXIT
printf 'Session probe recording to %s\n' "$trace"

# The Go descheduler first checks after a full interval.
bash run-steep-fixed-move.sh "$campaign" --phase-duration 60 --method-order proposed-first \
  --deschedule-interval 3600
stop_probe
trap - EXIT
mv -- "$trace" "results/$campaign/session-transition-probe.jsonl"
