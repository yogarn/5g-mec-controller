# The `tc` commands behind `utils/traffic_control.py`

There is no shell script for the inter-node delay. `TrafficDelay` (`utils/traffic_control.py`) builds these
commands in Python and runs each one inside the `calico-node` pod of the node, with
`kubectl -n kube-system exec <calico-node pod> -c calico-node -- <command>` (line 46). This page is the same
sequence written as plain shell, for the day you implement it outside this repo. It was derived by reading the
code, **not executed**; the Python version is the one the final runs used.

The pieces are the standard iproute2 tools `tc` and `ip` (already in the `calico-node` container), nothing else.

## What it does

For every node (`gnb1`, `gnb-2`, `gnb-3`, `core-5g`) and every interface that carries inter-node traffic
(`vxlan.calico` plus the NIC the node uses to reach each other node) it:

1. makes sure a `clsact` qdisc exists on the interface (so `tc filter ... egress` can attach),
2. creates an IFB device and puts a `netem delay` on it,
3. adds two `flower` filters on the interface's egress that redirect to the IFB only TCP traffic on **port 5000**
   to or from the Calico pool `172.16.40.0/23` (the pool the experiment pods are annotated into).

So only experiment pods get the delay, in both directions. `EXPERIMENTS.md` describes it as a one-way `netem`
delay per inter-node egress interface, applied in both directions.

Constants (lines 21 to 23): pool `172.16.40.0/23`, port `5000`, filter preferences `49210` (match destination)
and `49211` (match source).

## 0. One-time: the reserved Calico pool

Pods only get an address from this pool when they carry the annotation
`cni.projectcalico.org/ipv4pools: '["mec-campaign-reserved"]'` (it is already in the rendered workload manifests).
`nodeSelector: '!all()'` keeps ordinary pods from ever allocating from it.

```bash
kubectl create -f - <<'EOF'
apiVersion: crd.projectcalico.org/v1
kind: IPPool
metadata:
  name: mec-campaign-reserved
spec:
  cidr: 172.16.40.0/23
  blockSize: 26
  disabled: false
  ipipMode: Never
  vxlanMode: Always
  natOutgoing: true
  nodeSelector: '!all()'
  allowedUses: ["Workload"]
EOF
```

The code refuses to run if another pool overlaps `172.16.40.0/23` or if an existing `mec-campaign-reserved` has
different settings.

## 1. Install the delay

Run from core. It writes the IFB names it creates to `/tmp/tc-ifbs.txt`, because the removal step needs them
(the Python object keeps them in memory instead).

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
MS=10; POOL=172.16.40.0/23; PORT=5000; RUN=$(date -u +%s)
NODES="gnb1 gnb-2 gnb-3 core-5g"
declare -A IP POD
for n in $NODES; do
  IP[$n]=$(kubectl get node $n -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
  POD[$n]=$(kubectl -n kube-system get pod -l k8s-app=calico-node --field-selector spec.nodeName=$n,status.phase=Running -o jsonpath='{.items[0].metadata.name}')
done
x() { local n=$1; shift; kubectl -n kube-system exec "${POD[$n]}" -c calico-node -- "$@"; }

: > /tmp/tc-ifbs.txt
for n in $NODES; do
  ifaces="vxlan.calico"
  for p in $NODES; do
    [ "$p" = "$n" ] && continue
    ifaces="$ifaces $(x $n ip -j -4 route get ${IP[$p]} | jq -r '.[0].dev')"   # NIC used to reach peer p
  done
  for IF in $(printf '%s\n' $ifaces | sort -u); do
    IFB=mec$(printf '%s' "$n/$IF/$RUN" | sha256sum | cut -c1-8)
    # 1) clsact on the interface if missing (it is deliberately left in place afterwards)
    x $n tc qdisc show dev $IF | grep -q clsact || x $n tc qdisc add dev $IF clsact
    # the two filter slots must be free, otherwise stop
    for pref in 49210 49211; do
      [ -z "$(x $n tc filter show dev $IF egress pref $pref)" ] || { echo "$n/$IF pref $pref is occupied, stopping"; break 3; }
    done
    # 2) IFB device with the delay
    x $n ip link add $IFB type ifb
    echo "$n $IF $IFB" >> /tmp/tc-ifbs.txt
    x $n ip link set $IFB up
    x $n tc qdisc add dev $IFB root handle 1: netem delay ${MS}ms
    # 3) redirect pool traffic on port 5000 into the IFB: destination match, then source match
    x $n tc filter add dev $IF egress protocol ip pref 49210 handle 1 flower ip_proto tcp dst_ip $POOL dst_port $PORT action mirred egress redirect dev $IFB
    x $n tc filter add dev $IF egress protocol ip pref 49211 handle 1 flower ip_proto tcp src_ip $POOL src_port $PORT action mirred egress redirect dev $IFB
  done
done
cat /tmp/tc-ifbs.txt
```

## 2. Check it is in place

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
declare -A POD
for n in gnb1 gnb-2 gnb-3 core-5g; do
  POD[$n]=$(kubectl -n kube-system get pod -l k8s-app=calico-node --field-selector spec.nodeName=$n,status.phase=Running -o jsonpath='{.items[0].metadata.name}')
done
while read n IF IFB; do
  echo "== $n $IF -> $IFB"
  kubectl -n kube-system exec "${POD[$n]}" -c calico-node -- tc qdisc show dev $IFB          # must show: netem ... delay 10ms
  kubectl -n kube-system exec "${POD[$n]}" -c calico-node -- tc filter show dev $IF egress pref 49210   # dst_ip 172.16.40.0/23 dst_port 5000 ... redirect to device $IFB
  kubectl -n kube-system exec "${POD[$n]}" -c calico-node -- tc filter show dev $IF egress pref 49211   # src_ip 172.16.40.0/23 src_port 5000 ...
done < /tmp/tc-ifbs.txt
```

`TrafficDelay.check()` does this every 10 s during traffic and raises if the delay or a filter is missing or
changed. To see the effect itself, time a request between two pods of the pool that sit on different nodes,
with and without the delay.

## 3. Remove the delay

Filters first (in reverse order), then the IFB. The `clsact` qdisc stays, on purpose: other owners may have
filters on it and the code never deletes what it did not create.

```bash
source /home/member/skripsi_yoga/5g-mec-controller/deploy/final/env.sh
declare -A POD
for n in gnb1 gnb-2 gnb-3 core-5g; do
  POD[$n]=$(kubectl -n kube-system get pod -l k8s-app=calico-node --field-selector spec.nodeName=$n,status.phase=Running -o jsonpath='{.items[0].metadata.name}')
done
tac /tmp/tc-ifbs.txt | while read n IF IFB; do
  for pref in 49211 49210; do
    kubectl -n kube-system exec "${POD[$n]}" -c calico-node -- tc filter del dev $IF egress pref $pref handle 1 flower
  done
  kubectl -n kube-system exec "${POD[$n]}" -c calico-node -- ip link del $IFB
done
rm -f /tmp/tc-ifbs.txt
```

The Python version also refuses to delete a filter whose contents changed, and a crash can leave filters and
IFBs behind; the next start then refuses to overwrite them. In that case list what is left with
`tc filter show dev <IF> egress pref 49210` / `49211` and `ip -d link show type ifb` on each node and remove it by hand.

## Where each command lives in the code

| command | `utils/traffic_control.py` |
|---|---|
| `kubectl -n kube-system exec ... -c calico-node -- ...` | line 46 |
| interface discovery (`ip -j -4 route get`, `ip -j link show`) | `_agents_and_devices`, line 72 (the `route get` is on line 98) |
| `tc qdisc add dev IF clsact` | line 155, inside `start` (line 146) |
| `ip link add IFB type ifb`, `ip link set IFB up` | lines 159 and 162 |
| `tc qdisc add dev IFB root handle 1: netem delay` | line 163 |
| `tc filter add ... flower ... mirred egress redirect` | line 165 |
| verification (`tc -j qdisc show`, `tc filter show`) | `check`, line 176 |
| `tc filter del ...`, `ip link del IFB` | `close`, line 202 (the commands are on lines 215 and 221) |
