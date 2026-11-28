"""Check that every UE in a scaling run used the local UPF path of its gNB.

Each site must send from its local DNN subnet to the NodePort on its own node, and every request must be served
on core-5g, so all three sites cross nodes and receive the same tc delay. Prints phase-1 medians per site.
Usage: python -m utils.verify_scaling_paths RUN_DIR
"""
import json
import statistics
import sys
from collections import defaultdict
from ipaddress import ip_address, ip_network
from pathlib import Path

EXPECTED = {
    'gnb-1': ('172.16.50.0/24', '10.34.211.157'),
    'gnb-2': ('10.63.1.0/24', '10.34.211.196'),
    'gnb-3': ('10.64.1.0/24', '10.34.210.3'),
}
SERVING_NODE = 'core-5g'


def check_requests(lines):
    """Return (problems, phase-1 median of successful requests per site) for one requests.jsonl."""
    problems, first_phase, seen = [], defaultdict(list), set()
    for line in lines:
        record = json.loads(line)
        if record.get('event') != 'request':
            continue
        site = record['group']
        subnet, host = EXPECTED[site]
        seen.add(site)
        if ip_address(record['source_address']) not in ip_network(subnet):
            problems.append(f"{site}: source {record['source_address']} outside {subnet}")
        if record['url'].split('/')[2].split(':')[0] != host:
            problems.append(f"{site}: url {record['url']} does not target {host}")
        if record.get('server_node') not in (None, SERVING_NODE):
            problems.append(f"{site}: served on {record['server_node']}, expected {SERVING_NODE}")
        if record['phase'] == 'phase_01' and record.get('http_success'):
            first_phase[site].append(record['duration_ms'])
    problems += [f'{site}: no requests' for site in EXPECTED if site not in seen]
    medians = {site: statistics.median(values) for site, values in first_phase.items()}
    return sorted(set(problems)), medians


def main(argv):
    if len(argv) != 1:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    run_dir, failed = Path(argv[0]), False
    for arm in ('baseline', 'proposed'):
        path = run_dir / arm / 'requests.jsonl'
        if not path.is_file():
            print(f'[FAIL] {arm}: missing {path}')
            failed = True
            continue
        problems, medians = check_requests(path.read_text().splitlines())
        for problem in problems[:20]:
            print(f'[FAIL] {arm}: {problem}')
        failed |= bool(problems)
        summary = ', '.join(f'{site}={medians[site]:.1f} ms' for site in sorted(medians))
        print(f"[{'FAIL' if problems else 'OK'}] {arm}: phase-1 median RT {summary}")
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
