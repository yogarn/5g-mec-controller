"""Exclusive, journalled UERANSIM lifecycle. Run as root on the UE host.

The lifecycle YAML contains a ``subscribers`` list. Each entry has ``gnb2``
and ``gnb3`` mappings, each with ``config`` (an existing UERANSIM YAML path,
relative to the lifecycle YAML) and ``ip`` (the expected IPv4 address). The
paired UERANSIM configs must have the same unique ``supi``. No credentials are
stored in this journal. Recover an interrupted run with ``--recover STATE_DIR``.
"""
from __future__ import annotations

import argparse
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import re
import select
import signal
import stat
import subprocess
import sys
import time

import yaml

_LOCK = '/tmp/benchmark-owned-ue.lock'
_PROTOCOL = 'journal-before-exec-v1'


def _lock():
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        fd = os.open(_LOCK, flags)
    except FileNotFoundError:
        try:
            fd = os.open(_LOCK, flags | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            fd = os.open(_LOCK, flags)
    stream = os.fdopen(fd, 'r')
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise RuntimeError('subscriber lock is not a regular file')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        stream.close()
        raise
    return stream


def _require_root():
    if os.geteuid() != 0:
        raise RuntimeError('owned UE lifecycle requires root')
    if not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        raise RuntimeError('Linux pidfd support is required')


def _identity(pid):
    value = Path(f'/proc/{pid}/stat').read_text()
    return (int(value[value.rfind(')') + 2:].split()[19]),
            Path('/proc/sys/kernel/random/boot_id').read_text().strip())


def _ip(*args):
    return json.loads(subprocess.check_output(['ip', '-j', '-N', *args], text=True, timeout=10))


def _run(*args):
    subprocess.run(['ip', *args], check=True, capture_output=True, text=True, timeout=10)


def _named_rules():
    return json.loads(subprocess.check_output(['ip', '-j', 'rule', 'show'], text=True, timeout=10))


def _record(journal, event, **fields):
    with journal.open('a') as stream:
        stream.write(json.dumps({'event': event, 'timestamp': time.time(), **fields}) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def _load_config(path):
    path = Path(path).resolve(strict=True)
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict) or set(data) != {'subscribers'} or not isinstance(data['subscribers'], list) or not data['subscribers']:
        raise ValueError('configuration must contain a nonempty subscribers list')
    subscribers, identities, addresses = [], set(), set()
    for entry in data['subscribers']:
        if not isinstance(entry, dict) or set(entry) != {'gnb2', 'gnb3'}:
            raise ValueError('each subscriber requires gnb2 and gnb3')
        pair, supis = [], []
        for site in ('gnb2', 'gnb3'):
            item = entry[site]
            if not isinstance(item, dict) or set(item) != {'config', 'ip'} or not isinstance(item['config'], str) or not item['config'] or not isinstance(item['ip'], str):
                raise ValueError('each site requires config and ip strings')
            config = (path.parent / item['config']).resolve(strict=True)
            if not config.is_file():
                raise ValueError('UE config must be a regular file')
            ue = yaml.safe_load(config.read_text())
            supi = ue.get('supi') if isinstance(ue, dict) else None
            if not isinstance(supi, str) or not supi.strip():
                raise ValueError('UE config requires a nonempty supi')
            address = ipaddress.IPv4Address(item['ip'])
            if address.is_unspecified or address.is_multicast or address.is_loopback or address.is_reserved:
                raise ValueError('expected IP must be a usable unicast IPv4 address')
            ip = str(address)
            if ip in addresses:
                raise ValueError('expected IP addresses must be distinct')
            addresses.add(ip)
            supis.append(supi)
            pair.append({'config': str(config), 'ip': ip})
        if supis[0] != supis[1] or supis[0] in identities:
            raise ValueError('paired SUPIs must match and subscribers must be distinct')
        identities.add(supis[0])
        subscribers.append(pair)
    return subscribers


def _other_ues():
    for entry in Path('/proc').iterdir():
        if entry.name.isdigit():
            try:
                if (entry / 'comm').read_text().strip() == 'nr-ue':
                    return True
            except (FileNotFoundError, ProcessLookupError):
                pass
    return False


def _stop_process(pid, ticks, boot):
    try:
        fd = os.pidfd_open(pid)
    except ProcessLookupError:
        return
    try:
        try:
            if _identity(pid) != (ticks, boot):
                return
        except (FileNotFoundError, ProcessLookupError):
            return
        poll = select.poll()
        poll.register(fd, select.POLLIN)
        if poll.poll(0):
            return
        try:
            signal.pidfd_send_signal(fd, signal.SIGTERM)
            if not poll.poll(5000):
                signal.pidfd_send_signal(fd, signal.SIGKILL)
                if not poll.poll(5000):
                    raise RuntimeError(f'owned process {pid} did not exit')
        except ProcessLookupError:
            pass
    finally:
        os.close(fd)


def _clean_routes(routes):
    if not routes:
        return
    rules = _ip('rule', 'show')
    kernel_routes = _ip('route', 'show', 'table', 'all')
    actions = []
    for table, (ip, iface) in routes.items():
        rr = [r for r in rules if str(r.get('table')) == str(table)]
        rt = [r for r in kernel_routes if str(r.get('table')) == str(table)]
        if len(rr) > 1 or len(rt) > 1:
            raise RuntimeError(f'ambiguous entries in table {table}')
        for rule in rr:
            if (set(rule) - {'priority', 'src', 'table', 'protocol', 'action'} or
                    rule.get('src') not in (ip, ip + '/32') or rule.get('priority') != 1000 or
                    rule.get('action', 'to_tbl') != 'to_tbl'):
                raise RuntimeError(f'foreign rule in table {table}')
            actions.append(('rule', 'del', 'from', ip + '/32', 'table', str(table), 'priority', '1000'))
        for route in rt:
            if (set(route) - {'dst', 'dev', 'table', 'protocol', 'scope', 'flags'} or
                    route.get('dst') != 'default' or route.get('dev') != iface or
                    set(route.get('flags', [])) - {'linkdown', 'dead', 'onlink'}):
                raise RuntimeError(f'foreign route in table {table}')
            actions.append(('route', 'del', 'default', 'dev', iface, 'table', str(table)))
    for action in actions:
        _run(*action)


def _clean_ue_rules(ips):
    if not ips:
        return
    rules = _named_rules()
    actions = []
    for ip in ips:
        source = [r for r in rules if r.get('src') in (ip, ip + '/32')]
        if len(source) > 1:
            raise RuntimeError(f'ambiguous source rules for {ip}')
        if source:
            rule = source[0]
            table = str(rule.get('table', ''))
            # Numeric table IDs cannot prove which UERANSIM TUN installed the rule.
            if (not re.fullmatch(r'rt_uesimtun\d+', table) or
                    set(rule) - {'priority', 'src', 'table', 'protocol', 'action'} or
                    type(rule.get('priority')) is not int or rule.get('action', 'to_tbl') != 'to_tbl' or
                    sum(str(r.get('table')) == table for r in rules) != 1):
                raise RuntimeError(f'ambiguous UERANSIM source rule for {ip}')
            actions.append(('rule', 'del', 'from', ip + '/32', 'table', table, 'priority', str(rule['priority'])))
    for action in actions:
        _run(*action)


def _records(journal):
    try:
        text = journal.read_text()
        if not text.endswith('\n'):
            raise ValueError('incomplete journal')
        records = [json.loads(line) for line in text.splitlines()]
        if not records or records[0].get('event') != 'created' or records[0].get('launch_protocol') != _PROTOCOL:
            raise ValueError('unsupported journal')
        processes, routes = [], {}
        for row in records[1:]:
            event = row['event']
            if event == 'stopped':
                if row.get('errors') != []:
                    raise ValueError('previous cleanup failed')
                processes, routes = [], {}
            elif event == 'process_started':
                pid, ticks, boot, ip = (row[k] for k in ('pid', 'start_ticks', 'boot_id', 'ip'))
                if type(pid) is not int or pid <= 0 or type(ticks) is not int or ticks <= 0 or not isinstance(boot, str) or not boot or str(ipaddress.IPv4Address(ip)) != ip:
                    raise ValueError('invalid process identity')
                processes.append((pid, ticks, boot, ip))
            elif event == 'route_intent':
                ip, iface, table = (row[k] for k in ('ip', 'iface', 'table'))
                if (str(ipaddress.IPv4Address(ip)) != ip or not isinstance(iface, str) or
                        not re.fullmatch(r'uesimtun\d+', iface) or len(iface) > 15 or
                        type(table) is not int or not 20000 <= table < 60000 or table in routes):
                    raise ValueError('invalid route intent')
                routes[table] = (ip, iface)
            elif event not in ('route_added', 'ready'):
                raise ValueError('unknown journal event')
        ips = [p[3] for p in processes]
        if len(set(ips)) != len(ips) or len({p[:3] for p in processes}) != len(processes) or any(ip not in ips for ip, _ in routes.values()):
            raise ValueError('ambiguous journal ownership')
        return processes, routes
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        raise RuntimeError(f'cannot recover lifecycle journal {journal}: {exc}') from exc


def _cleanup(journal):
    processes, routes = _records(journal)
    errors = []
    try:
        _clean_routes(routes)
    except Exception as exc:
        errors.append(f'routes: {exc}')
    for pid, ticks, boot, _ in processes:
        try:
            _stop_process(pid, ticks, boot)
        except Exception as exc:
            errors.append(f'process {pid}: {exc}')
    if not errors:
        try:
            _clean_ue_rules({p[3] for p in processes})
            links = _ip('addr', 'show')
            addresses = {a.get('local') for link in links for a in link.get('addr_info', [])}
            if addresses.intersection(p[3] for p in processes):
                raise RuntimeError('owned UE address remains after cleanup')
        except Exception as exc:
            errors.append(str(exc))
    if errors:
        # Leave the journal recoverable; do not mark a partial cleanup complete.
        raise RuntimeError('owned UE cleanup failed: ' + '; '.join(errors))
    _record(journal, 'stopped', errors=[])


def recover_owned_ues(state_dir):
    _require_root()
    with _lock():
        _cleanup(Path(state_dir) / 'lifecycle.jsonl')


class OwnedUELifecycle:
    def __init__(self, state_dir: Path, config_file: Path, binary: Path):
        _require_root()
        self.subscribers = _load_config(config_file)
        self.binary = Path(binary).resolve(strict=True)
        if not self.binary.is_file() or not os.access(self.binary, os.X_OK):
            raise ValueError('binary must be an executable file')
        self.lock = _lock()
        try:
            self.state_dir = Path(state_dir)
            self.state_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
            self.journal = self.state_dir / 'lifecycle.jsonl'
            _record(self.journal, 'created', launch_protocol=_PROTOCOL)
        except BaseException:
            self.lock.close()
            raise
        self.closed = False
        self.processes = []

    def switch_counts(self, total, n2, n3):
        if self.closed:
            raise RuntimeError('lifecycle is closed')
        if any(type(n) is not int for n in (total, n2, n3)) or total < 1 or n2 < 0 or n3 < 0 or n2 + n3 != total or total > len(self.subscribers):
            raise ValueError('invalid UE counts or insufficient subscribers')
        _cleanup(self.journal)
        for process in self.processes:
            process.wait()
        self.processes.clear()
        if _other_ues():
            raise RuntimeError('existing nr-ue process prevents exclusive ownership')
        chosen = [pair[0 if i < n2 else 1] for i, pair in enumerate(self.subscribers[:total])]
        expected = {item['ip'] for item in chosen}
        links = _ip('addr', 'show')
        if expected & {a.get('local') for link in links for a in link.get('addr_info', [])}:
            raise RuntimeError('subscriber address already present')
        rules = _ip('rule', 'show')
        if any(r.get('src') in {ip for addr in expected for ip in (addr, addr + '/32')} for r in rules):
            raise RuntimeError('subscriber source rule already exists')
        existing = {link['ifname'] for link in links}
        phase = self.state_dir / f'phase-{time.time_ns()}'
        phase.mkdir(mode=0o700)
        try:
            found = {}
            deadline = time.monotonic() + 35 + 2 * total
            for i, item in enumerate(chosen):
                with (phase / f'ue-{i + 1}.log').open('ab') as log:
                    proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--exec-owned-ue', str(self.binary), item['config']], stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT)
                self.processes.append(proc)
                ticks, boot = _identity(proc.pid)
                _record(self.journal, 'process_started', pid=proc.pid, start_ticks=ticks, boot_id=boot, ip=item['ip'])
                proc.stdin.write(b'1')
                proc.stdin.close()
                while True:
                    if any(p.poll() is not None for p in self.processes):
                        raise RuntimeError('owned nr-ue exited during registration')
                    observed = {}
                    for link in _ip('addr', 'show'):
                        for addr in link.get('addr_info', []):
                            ip = addr.get('local')
                            if ip in expected:
                                iface = link['ifname']
                                if iface in existing or not re.fullmatch(r'uesimtun\d+', iface) or ip in observed:
                                    raise RuntimeError('ambiguous UE interface ownership')
                                observed[ip] = iface
                    if len(set(observed.values())) != len(observed) or any(observed.get(ip) != iface for ip, iface in found.items()):
                        raise RuntimeError('ambiguous or changed UE interface')
                    if item['ip'] in observed:
                        found = observed
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError('owned UE registration timed out')
                    time.sleep(.2)
                time.sleep(.3)
            if set(found) != expected:
                raise RuntimeError('owned UE registration incomplete')
            rules = _ip('rule', 'show')
            routes = _ip('route', 'show', 'table', 'all')
            used = {str(r.get('table')) for r in rules + routes}
            available = (n for n in range(20000, 60000) if str(n) not in used)
            result = ([], [])
            for i, item in enumerate(chosen):
                ip, iface = item['ip'], found[item['ip']]
                table = next(available)
                _record(self.journal, 'route_intent', ip=ip, iface=iface, table=table)
                _run('route', 'add', 'default', 'dev', iface, 'table', str(table))
                _run('rule', 'add', 'from', ip + '/32', 'table', str(table), 'priority', '1000')
                _record(self.journal, 'route_added', ip=ip, iface=iface, table=table)
                result[0 if i < n2 else 1].append({'ip': ip, 'iface': iface})
            if any(p.poll() is not None for p in self.processes):
                raise RuntimeError('owned nr-ue exited before ready')
            current = {
                (a.get('local'), link['ifname'])
                for link in _ip('addr', 'show')
                for a in link.get('addr_info', [])
            }
            if any((ip, iface) not in current for ip, iface in found.items()):
                raise RuntimeError('owned UE address disappeared before ready')
            _record(self.journal, 'ready', gnb2=result[0], gnb3=result[1])
            return result
        except BaseException:
            for process in self.processes:
                if process.stdin and not process.stdin.closed:
                    process.stdin.close()
            _cleanup(self.journal)
            for process in self.processes:
                process.wait()
            self.processes.clear()
            raise

    def close(self):
        if self.closed:
            return
        try:
            _cleanup(self.journal)
            for process in self.processes:
                process.wait()
            self.processes.clear()
        finally:
            self.closed = True
            self.lock.close()


def _main():
    if len(sys.argv) == 4 and sys.argv[1] == '--exec-owned-ue':
        if sys.stdin.buffer.read(1) != b'1':
            raise SystemExit('UE launch was not committed')
        os.execv(sys.argv[2], [sys.argv[2], '-c', sys.argv[3]])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--recover', required=True, type=Path, metavar='STATE_DIR')
    args = parser.parse_args()
    try:
        recover_owned_ues(args.recover)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        parser.exit(2, f'Recovery failed: {exc}\n')
    print(f'Recovery complete: {args.recover}')


if __name__ == '__main__':
    _main()
