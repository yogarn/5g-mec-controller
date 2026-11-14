"""Check or insert Open5GS subscribers from a private UE inventory."""
import argparse
import json
import os
from pathlib import Path
import re
import sys

import yaml

from experiments import defaults
from experiments.ue_lifecycle import _load_config


def _auth_and_session(path):
    config = yaml.safe_load(Path(path).read_text())
    supi = config.get('supi')
    if not isinstance(supi, str) or not re.fullmatch(r'imsi-[0-9]{15}', supi):
        raise ValueError('UE config must contain a 15-digit imsi SUPI')
    key, opc, amf = (config.get(name) for name in ('key', 'op', 'amf'))
    if (str(config.get('opType', '')).upper() != 'OPC' or
            not isinstance(key, str) or not re.fullmatch(r'[a-fA-F0-9]{32}', key) or
            not isinstance(opc, str) or not re.fullmatch(r'[a-fA-F0-9]{32}', opc) or
            not isinstance(amf, str) or not re.fullmatch(r'[a-fA-F0-9]{4}', amf)):
        raise ValueError('UE config requires hexadecimal key, OPC and AMF strings')
    sessions = config.get('sessions')
    if not isinstance(sessions, list) or len(sessions) != 1 or not isinstance(sessions[0], dict):
        raise ValueError('each site config must specify exactly one IPv4 session')
    session = sessions[0]
    dnn = session.get('apn')
    slice_info = session.get('slice')
    if (session.get('type') != 'IPv4' or not isinstance(dnn, str) or not dnn.strip() or
            not isinstance(slice_info, dict) or type(slice_info.get('sst')) is not int or
            not 0 <= slice_info['sst'] <= 255 or 'sd' in slice_info):
        raise ValueError('each session requires IPv4, an APN and an SST without SD')
    return supi[5:], key.lower(), opc.lower(), amf.lower(), dnn, slice_info['sst']


def build_documents(inventory):
    subscribers = _load_config(inventory)
    documents = []
    for sites in subscribers:
        sessions = [_auth_and_session(item['config']) for item in sites]
        if (any(item[:4] != sessions[0][:4] or item[5] != sessions[0][5] for item in sessions[1:])
                or len({item[4] for item in sessions}) != len(sessions)):
            raise ValueError('site UE configs must share IMSI, authentication and SST, with distinct DNNs')
        imsi, key, opc, amf, _, sst = sessions[0]
        def session(dnn, ip):
            return {'name': dnn, 'type': 1,
                    'qos': {'index': 9, 'arp': {'priority_level': 8,
                            'pre_emption_capability': 1, 'pre_emption_vulnerability': 1}},
                    'ambr': {'downlink': {'value': 1, 'unit': 3}, 'uplink': {'value': 1, 'unit': 3}},
                    'pcc_rule': [], 'ue': {'ipv4': ip}}
        documents.append({'imsi': imsi, 'subscribed_rau_tau_timer': 12,
                          'network_access_mode': 0, 'subscriber_status': 0, 'access_restriction_data': 0,
                          'security': {'k': key, 'amf': amf, 'op': '', 'opc': opc, 'sqn': 0},
                          'ambr': {'downlink': {'value': 1, 'unit': 3}, 'uplink': {'value': 1, 'unit': 3}},
                          'slice': [{'sst': sst, 'default_indicator': True,
                                     'session': [session(item[4], site['ip']) for item, site in zip(sessions, sites)]}],
                          '__v': 0})
    return documents


def conflicts(documents, existing):
    identities = {d['imsi'] for d in documents}
    addresses = {session['ue']['ipv4'] for d in documents for sl in d['slice'] for session in sl['session']}
    found_imsi, found_ip = set(), set()
    for record in existing:
        if record.get('imsi') in identities:
            found_imsi.add(record['imsi'])
        for sl in record.get('slice') or []:
            if not isinstance(sl, dict):
                continue
            for session in sl.get('session') or []:
                if isinstance(session, dict) and isinstance(session.get('ue'), dict):
                    ip = session['ue'].get('ipv4')
                    if ip in addresses:
                        found_ip.add(ip)
    return sorted(found_imsi), sorted(found_ip)


def provision(collection, documents, apply=False):
    existing = collection.find({}, {'imsi': 1, 'slice.session.ue.ipv4': 1})
    imsis, ips = conflicts(documents, existing)
    if imsis or ips:
        raise ValueError(f'existing subscriber IMSIs or static addresses conflict: IMSIs={imsis}, IPs={ips}')
    if apply:
        collection.insert_many(documents, ordered=True)
    return [{'imsi': d['imsi'], 'sst': d['slice'][0]['sst'],
             'sessions': [{'dnn': s['name'], 'ipv4': s['ue']['ipv4']} for s in d['slice'][0]['session']]}
            for d in documents]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', default=str(defaults.INVENTORY), help='private UE inventory YAML')
    parser.add_argument('--database', default='open5gs', help='Open5GS MongoDB database')
    parser.add_argument('--apply', action='store_true', help='insert new subscribers after conflict checks')
    parser.add_argument('--json', action='store_true', help='print the machine-readable provisioning plan')
    parser.add_argument('--verbose', action='store_true', help='show each subscriber and session')
    args = parser.parse_args(argv)
    try:
        docs = build_documents(args.inventory)
        from pymongo import MongoClient
        uri = os.environ.get('OPEN5GS_MONGO_URI', 'mongodb://127.0.0.1:27017/')
        with MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
            client.admin.command('ping')
            try:
                plan = provision(client[args.database]['subscribers'], docs, args.apply)
            except Exception as exc:
                if args.apply and not isinstance(exc, ValueError):
                    raise RuntimeError('insert may have partially succeeded; inspect the database before retrying') from exc
                raise
        action = 'inserted' if args.apply else 'check-only'
        if args.json:
            print(json.dumps({'action': action, 'subscribers': plan}, indent=2))
        else:
            print(f"Provisioning {action}: {len(plan)} subscriber(s)")
            if args.verbose:
                for subscriber in plan:
                    print(f"  IMSI {subscriber['imsi']}")
                    for session in subscriber['sessions']:
                        print(f"    {session['dnn']}: {session['ipv4']}")
    except (OSError, ValueError, RuntimeError, ImportError) as exc:
        parser.exit(2, f'error: {exc}\n')
    except Exception:
        parser.exit(2, 'error: MongoDB operation failed; check connectivity and database permissions\n')


if __name__ == '__main__':
    main()
