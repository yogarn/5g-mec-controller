#!/usr/bin/env bash
# Live demo of one experiment arm with real UEs and the real 11-phase hotspot schedule (about 30 minutes).
#   ./demo.sh baseline-local | baseline-vertical | proposed-local | proposed-vertical   run one arm
#   ./demo.sh status                                                                     live view (second terminal)
#   ./demo.sh stop                                                                       stop the run and clean up
# The arms and the schedule are the same as the experiments: PROFILES['demo-*'] in experiments/profiles.py.
set -euo pipefail
cd "$(dirname "$0")"

# Prints the last decision event of each site from the offloader log (JSON lines on stdin).
last_per_site=$(cat <<'PY'
import sys, json
last = {}
for line in sys.stdin:
    try:
        e = json.loads(line)
    except ValueError:
        continue
    if "site" in e and e.get("event") != "envoy_ack":
        last[e["site"]] = e
for site in sorted(last):
    e = last[site]
    p90 = e.get("p90_ms")
    p90 = "n/a" if p90 is None else str(round(p90))
    print("  %s: %s  route=%s  p90=%s ms  local=%s core=%s" % (
        site, e.get("event"), e.get("active_route"), p90,
        e.get("local_ready_endpoints"), e.get("core_ready_endpoints")))
PY
)

case "${1:-}" in
  baseline-local|baseline-vertical|proposed-local|proposed-vertical)
    exec experiments/vertical_campaign.sh "demo-$1" "demo_$1" ;;
  stop)
    # SIGINT lets the campaign run its normal cleanup (offloader back to LOCAL, Core pool restored).
    pkill -INT -f 'experiments.campaign' && echo "stop requested; wait for cleanup to finish" ;;
  status)
    while true; do
      clear
      date +%T
      echo "== pods per node (services) =="
      kubectl -n services get pods -o wide --no-headers 2>/dev/null | grep pedestrian-detection | grep -v anchor \
        | awk '{n[$7]++; if ($2 != "1/1") notready[$7]++} END {for (k in n) printf "  %-10s %d pod(s), %d not ready\n", k, n[k], notready[k]}' | sort
      echo "== HPA =="
      kubectl -n services get hpa --no-headers 2>/dev/null | awk '{print "  " $1, $3, $4, $5, $6, $7}'
      echo "== offloader (last decision per site) =="
      kubectl -n placement logs deploy/vertical-offloader --tail=200 2>/dev/null | python3 -c "$last_per_site"
      sleep 3
    done ;;
  *)
    sed -n 2,6p "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
