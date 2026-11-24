"""Print the exact commands utils/traffic_control.TrafficDelay runs, with the line that runs each one.

No cluster is touched. The real TrafficDelay code runs (start() then close()) with its command() helper
replaced by a recorder that returns plausible answers (4 nodes, one NIC `eth0` plus `vxlan.calico`, the Calico
pool already present, no clsact yet). Every command it would send is printed as
`kubectl -n kube-system exec <calico-node pod> -c calico-node -- <command>` minus that wrapper.

    python3 deploy/final/tools/trace_tc_commands.py [node] [delay_ms]      # defaults: gnb1 10
    python3 deploy/final/tools/trace_tc_commands.py gnb1 10 --expand        # also print the health-check script

On a real cluster some steps differ with the answers: e.g. `tc qdisc add ... clsact` is skipped when clsact
already exists, and the IFB names change on every run (they hash a random token).
"""
import hashlib
import json
import shlex
import subprocess
import sys

sys.path.insert(0, '.')
import utils.traffic_control as tc  # noqa: E402

NODES = tc.NODES
INTERFACES = ('vxlan.calico', 'eth0')
trace, added, ifbs = [], set(), []
phase = ['start']


def caller():
    frame = sys._getframe(2)
    while frame:
        name = frame.f_code.co_name
        if frame.f_code.co_filename.endswith('traffic_control.py') and name not in (
                '_exec', '_read_exec', '_read_tc', '_tc_snapshot', '_json', 'fake', 'command'):
            return frame.f_lineno, name
        frame = frame.f_back
    return 0, '?'


def result(argv, out=''):
    return subprocess.CompletedProcess(argv, 0, stdout=out, stderr='')


def filter_text(node, interface, pref):
    side = 'dst' if pref == tc.PREFERENCES[0] else 'src'
    ifb = next(i for n, itf, i in ifbs if n == node and itf == interface)
    return (f'filter protocol ip pref {pref} flower chain 0 handle 0x1 ip_proto tcp {side}_ip {tc.POOL_CIDR} '
            f'{side}_port 5000 action mirred egress redirect to device {ifb}')


def fake(*args, **kwargs):
    argv = [str(a) for a in args]
    if 'ippools.crd.projectcalico.org' in argv:
        return result(argv, json.dumps({'items': [{'metadata': {'name': tc.POOL_NAME}, 'spec': {
            'cidr': tc.POOL_CIDR, 'blockSize': 26, 'disabled': False, 'ipipMode': 'Never', 'vxlanMode': 'Always',
            'natOutgoing': True, 'nodeSelector': '!all()', 'allowedUses': ['Workload']}}]}))
    if argv[:3] == ['kubectl', 'get', 'nodes']:
        return result(argv, json.dumps({'items': [{'metadata': {'name': n}, 'status': {'addresses': [
            {'type': 'InternalIP', 'address': f'10.0.0.{i + 1}'}]}} for i, n in enumerate(NODES)]}))
    if 'k8s-app=calico-node' in argv:
        return result(argv, json.dumps({'items': [{'metadata': {'name': f'calico-node-{n}'}, 'spec': {'nodeName': n},
            'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}} for n in NODES]}))
    pod, cmd = argv[4], argv[8:]
    node = pod.removeprefix('calico-node-')
    line, function = caller()
    out = ''
    if cmd[:3] == ['ip', '-j', '-4']:
        out = json.dumps([{'dst': cmd[-1], 'dev': 'eth0'}])
    elif cmd[:3] == ['ip', '-j', 'link']:
        out = json.dumps([{'ifname': cmd[-1]}])
    elif cmd[:4] == ['tc', '-j', 'qdisc', 'show']:
        out = json.dumps([{'kind': 'netem', 'options': {'delay': {'delay': DELAY_MS / 1000}}}]
                         if cmd[-1].startswith('mec') else [{'kind': 'noqueue'}])
    elif cmd[:3] == ['tc', 'filter', 'show']:
        itf, pref = cmd[4], int(cmd[-1])
        out = filter_text(node, itf, pref) if (pod, itf, pref) in added else ''
    elif cmd[:2] == ['sh', '-ec']:
        parts = []
        for text in cmd[2].split('\n'):
            if text.startswith('tc '):
                q = shlex.split(text)
                parts.append(json.dumps([{'kind': 'netem', 'options': {'delay': {'delay': DELAY_MS / 1000}}}])
                             if q[1:4] == ['-j', 'qdisc', 'show'] else filter_text(node, q[q.index('dev') + 1], int(q[-1])))
        out = ''.join(p + '\x1e' for p in parts)
    elif cmd[:3] == ['tc', 'filter', 'add']:
        added.add((pod, cmd[4], int(cmd[cmd.index('pref') + 1])))
    elif cmd[:3] == ['tc', 'filter', 'del']:
        added.discard((pod, cmd[4], int(cmd[cmd.index('pref') + 1])))
    trace.append((phase[0], line, function, node, cmd))
    return result(argv, out)


def main():
    global DELAY_MS
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    node = args[0] if args else 'gnb1'
    DELAY_MS = float(args[1]) if len(args) > 1 else 10.0
    tc.command = fake
    delay = tc.TrafficDelay(DELAY_MS)
    for n in NODES:       # the IFB names the code derives: 'mec' + sha256(node/interface/token)[:8]
        for itf in INTERFACES:
            ifbs.append((n, itf, 'mec' + hashlib.sha256(f'{n}/{itf}/{delay.token}'.encode()).hexdigest()[:8]))
    delay.start()
    phase[0] = 'close'
    delay.close()
    for ph, line, function, n, cmd in trace:
        if n != node:
            continue
        shown = 'sh -ec <health-check script, see --expand>' if cmd[0] == 'sh' else shlex.join(cmd)
        print(f'[{ph:5}] line {line:3} {function:20} {shown}')
        if cmd[0] == 'sh' and '--expand' in sys.argv:
            print('\n'.join('            ' + t for t in cmd[2].split('\n') if t.startswith('tc ')))
    print(f'--- {len(trace)} exec calls in total, {sum(1 for t in trace if t[3] == node)} on {node}')


if __name__ == '__main__':
    main()
