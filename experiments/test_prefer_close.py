import json

import yaml

from experiments import campaign


def test_prefer_close_plan_matches_arms_and_controller(tmp_path):
    out = tmp_path / 'plan'
    campaign.main(['--comparison', 'full', '--prefer-close', '--plan-only', str(out)])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    assert plan['prefer_close'] is True
    assert plan['campaign_settings']['prefer_close'] is True
    for method in ('baseline', 'proposed'):
        docs = list(yaml.safe_load_all((out / f'{method}-workload.yaml').read_text()))
        service = next(doc for doc in docs if doc and doc.get('kind') == 'Service')
        campaign.check_service_routing(service, True)
        assert service['spec']['trafficDistribution'] == 'PreferClose'
        hpa = yaml.safe_load((out / f'{method}-hpa.yaml').read_text())
        if method == 'baseline':
            assert hpa['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 80
        config = yaml.safe_load((out / f'{method}-controller.yaml').read_text())
        assert config['prefer_close'] == (method == 'proposed')


def test_prefer_close_node_labels_restore_previous_state(monkeypatch):
    labels = {'gnb1': None, 'gnb-2': 'old-zone'}
    commands = []

    def kube(*args, **kwargs):
        if args[:2] == ('get', 'node'):
            return type('Response', (), {'stdout': json.dumps({'metadata': {'labels':
                {'topology.kubernetes.io/zone': labels[args[2]]} if labels[args[2]] else {}}})})()
        commands.append(args)

    monkeypatch.setattr(campaign, 'kube', kube)
    before = campaign.set_prefer_close_zones(labels)
    campaign.restore_prefer_close_zones(before)
    assert commands == [
        ('label', 'node', 'gnb1', 'topology.kubernetes.io/zone=gnb1', '--overwrite'),
        ('label', 'node', 'gnb-2', 'topology.kubernetes.io/zone=gnb-2', '--overwrite'),
        ('label', 'node', 'gnb1', 'topology.kubernetes.io/zone-', '--overwrite'),
        ('label', 'node', 'gnb-2', 'topology.kubernetes.io/zone=old-zone', '--overwrite'),
    ]
