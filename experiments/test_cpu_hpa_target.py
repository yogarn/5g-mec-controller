"""Check custom CPU targets across saved plans and the live configuration path."""
import pytest
import yaml

from experiments import campaign


@pytest.mark.parametrize('comparison', ['scaling', 'full'])
@pytest.mark.parametrize('target', [65, 80, 120])
def test_custom_cpu_target_matches_manifest_and_metadata(tmp_path, comparison, target):
    out = tmp_path / 'plan'
    campaign.main(['--comparison', comparison, '--cpu-hpa-target', str(target),
                   '--plan-only', str(out)])
    plan = yaml.safe_load((out / 'plan.yaml').read_text())
    base = yaml.safe_load((out / 'baseline-hpa.yaml').read_text())
    proposed = yaml.safe_load((out / 'proposed-hpa.yaml').read_text())
    assert base['spec']['metrics'][0]['resource']['target']['averageUtilization'] == target
    assert plan['cpu_hpa_target'] == plan['campaign_settings']['cpu_hpa_target'] == target
    assert plan['hpa_targets']['baseline']['target'] == target
    assert proposed['spec']['metrics'][0]['external']['target']['value'] == '150m'
    if comparison == 'scaling':
        assert plan['node_restriction'] == 'core-5g'
        for method in ('baseline', 'proposed'):
            dep = next(d for d in yaml.safe_load_all((out / f'{method}-workload.yaml').read_text())
                       if d and d.get('kind') == 'Deployment')
            spec = dep['spec']['template']['spec']
            assert spec['schedulerName'] == 'default-scheduler'
            terms = spec['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms']
            assert terms == [{'matchFields': [{'key': 'metadata.name', 'operator': 'In',
                                              'values': ['core-5g']}]}]


@pytest.mark.parametrize('value', ['0', '-1', '80.5', 'nan'])
def test_invalid_cpu_target_fails_before_creating_output(tmp_path, value):
    out = tmp_path / 'plan'
    with pytest.raises(SystemExit):
        campaign.main(['--comparison', 'scaling', '--cpu-hpa-target', value,
                       '--plan-only', str(out)])
    assert not out.exists()


def test_cpu_flag_cannot_silently_affect_placement_comparison(tmp_path):
    out = tmp_path / 'plan'
    with pytest.raises(SystemExit):
        campaign.main(['--scenario', 'mobility9', '--cpu-hpa-target', '80',
                       '--plan-only', str(out)])
    assert not out.exists()


def test_live_method_uses_custom_cpu_target(tmp_path, monkeypatch):
    render = campaign._method_configs

    class RenderComplete(Exception):
        pass

    def render_then_stop(*args, **kwargs):
        render(*args, **kwargs)
        raise RenderComplete

    monkeypatch.setattr(campaign, '_method_configs', render_then_stop)
    with pytest.raises(RenderComplete):
        campaign.run_method({'cpu_hpa_target': 65}, 'example', tmp_path, 'baseline',
                            None, 150, 12, comparison='scaling', autoscaler='hpa')
    hpa = yaml.safe_load((tmp_path / 'baseline-hpa.yaml').read_text())
    assert hpa['spec']['metrics'][0]['resource']['target']['averageUtilization'] == 65
