"""Open5GS SMF/AMF session polling and UE-IP-to-node mapping."""
from collections import Counter
import ipaddress
import json
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import urlopen


def parse_upf_host(addr) -> str | None:
    """Extract and normalize an IP literal from an optional host:port or [ipv6]:port string."""
    if not isinstance(addr, str) or not addr or addr != addr.strip():
        return None
    port = None
    if addr.startswith('['):
        end = addr.find(']')
        if end == -1:
            return None
        host, rest = addr[1:end], addr[end + 1:]
        if rest:
            if not rest.startswith(':'):
                return None
            port = rest[1:]
    elif addr.count(':') == 1:
        host, port = addr.split(':', 1)
    else:
        host = addr
    if port is not None and (len(port) > 5 or not port.isascii() or not port.isdigit() or not 1 <= int(port) <= 65535):
        return None
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        return None


def map_upf_sessions(smf: dict, upf_nodes: dict[str, str]) -> dict[str, str]:
    """Map unambiguous active UE IPv4 addresses to their N3 UPF Kubernetes node."""
    if not isinstance(smf, dict) or not isinstance(smf.get('items'), list):
        raise ValueError('invalid session response')
    candidates: dict[str, list[str | None]] = {}
    for item in smf['items']:
        sessions = item.get('pdu') if isinstance(item, dict) else None
        if not isinstance(sessions, list):
            continue
        for session in sessions:
            if not isinstance(session, dict) or session.get('pdu_state') != 'active':
                continue
            raw_ip = session.get('ipv4')
            if not isinstance(raw_ip, str):
                continue
            try:
                ip = str(ipaddress.IPv4Address(raw_ip))
            except (ValueError, TypeError, ipaddress.AddressValueError):
                continue
            n3 = session.get('n3')
            upf = n3.get('upf') if isinstance(n3, dict) else None
            host = parse_upf_host(upf.get('addr') if isinstance(upf, dict) else None)
            candidates.setdefault(ip, []).append(upf_nodes.get(host) if host is not None else None)
    return {ip: vals[0] for ip, vals in candidates.items() if len(vals) == 1 and vals[0] is not None}


def join_sessions(smf: dict, amf: dict, nodes: dict[int, str]) -> dict[str, str]:
    """Join SMF PDU sessions with AMF UE attachments by SUPI to map UE IP -> gNB node."""
    if (not isinstance(smf, dict) or not isinstance(smf.get('items'), list)
            or not isinstance(amf, dict) or not isinstance(amf.get('items'), list)):
        raise ValueError('invalid session response')
    amf_by_supi: dict[str, list] = {}
    for item in amf['items']:
        if isinstance(item, dict) and isinstance(item.get('supi'), str):
            amf_by_supi.setdefault(item['supi'], []).append(item.get('gnb'))
    smf_counts = Counter(
        item.get('supi') for item in smf['items']
        if isinstance(item, dict) and isinstance(item.get('supi'), str)
    )
    candidates: dict[str, list[str | None]] = {}
    for item in smf['items']:
        if not isinstance(item, dict) or not isinstance(item.get('supi'), str):
            continue
        supi, sessions = item['supi'], item.get('pdu')
        if not isinstance(sessions, list):
            continue
        matches = amf_by_supi.get(supi, [])
        node = (
            nodes.get(matches[0].get('gnb_id'))
            if smf_counts[supi] == 1 and len(matches) == 1 and isinstance(matches[0], dict)
            else None
        )
        for session in sessions:
            if not isinstance(session, dict) or session.get('pdu_state') != 'active':
                continue
            try:
                ip = str(ipaddress.IPv4Address(session.get('ipv4')))
            except (ValueError, TypeError, ipaddress.AddressValueError):
                continue
            candidates.setdefault(ip, []).append(node)
    return {ip: vals[0] for ip, vals in candidates.items() if len(vals) == 1 and vals[0] is not None}


def fetch(url: str) -> dict:
    """Fetch an Open5GS SMF/AMF JSON listing endpoint with page=-1."""
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query) if k != 'page'] + [('page', '-1')]
    with urlopen(urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), '')), timeout=2) as resp:
        return json.load(resp)
