from collections import defaultdict
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
import json
import pytest
import pyarrow.parquet as pq
from meridian.config import Config, config_load
from meridian.dimensions import build_dimensions
from meridian.generator import generate
from meridian.quality import Counts, contrast, evaluate
from meridian.routing import choose_rule
from meridian.scenario import calibration, phases
from meridian.transactions import Transactions
from meridian.writers.parquet import write_table


def config(): return Config.parse(config_load('config/m2.yaml'))


def test_rollout_history_allocation_and_recovery():
    cfg = config(); dims = build_dimensions(cfg,'history')
    rules = defaultdict(list)
    for r in dims['routing_rules']: rules[r['processor_id']].append(r)
    start, canary_end, end = phases(cfg)
    for ts in (cfg.start, start-timedelta(microseconds=1), start, canary_end-timedelta(microseconds=1), canary_end, end-timedelta(microseconds=1), end, cfg.end-timedelta(microseconds=1)):
        for p in range(cfg.processors):
            active = [r for r in rules[p] if r['effective_from_ts'] <= ts < r['effective_to_ts']]
            assert sum(r['rollout_percentage'] for r in active) == Decimal('100')
            assert choose_rule(cfg,rules,p,ts,1) in [r['routing_rule_id'] for r in active]
    p = cfg.scenario.faulty_processor_id
    for ts, expected in ((start,.2), (canary_end,.8)):
        selected = [choose_rule(cfg,rules,p,ts,k) for k in range(1,10001)]
        fraction = sum(key in (1000+p,2000+p) for key in selected)/len(selected)
        assert abs(fraction-expected) < .02
    assert choose_rule(cfg,rules,p,end,1) == 3000+p
    assert all(r['effective_from_ts'] < r['effective_to_ts'] for r in dims['routing_rules'])


def test_calibration_and_rejection():
    cfg = config()
    result = calibration(cfg)
    assert .625 <= result['candidate_impact_floor_pp'] < .63
    assert abs(result['recommended_candidate_share']-cfg.scenario.target_candidate_share) < .0001
    assert result == calibration(replace(cfg,batch_size=7))
    with pytest.raises(ValueError,match='Infeasible'):
        Config.parse(replace(cfg,scenario=replace(cfg.scenario,target_candidate_share=.001)))
    with pytest.raises(ValueError,match='controls'):
        Config.parse(replace(cfg,scenario=replace(cfg.scenario,steady_rollout=1)))


def test_population_controls_and_determinism():
    cfg = config(); dims = build_dimensions(cfg,'population')
    source = Transactions(cfg,dims,'population')
    rows, flags = source.batch(range(1,20001))
    assert {r['channel'] for r in rows} == {'pos','ecommerce','mobile','recurring'}
    assert any(r['routing_rule_id'] == 3003 for r in rows)
    assert any(r['routing_rule_id'] == 2003 for r in rows)
    assert source.batch([20000,1,501,1])[0] == [rows[i-1] for i in (20000,1,501,1)]
    # A target-segment merchant/network/channel is not sufficient for exposure.
    target = [r for r in rows if dims['merchants'][r['merchant_id']]['merchant_category_code']=='4722' and r['channel']=='ecommerce' and r['card_network']=='Mastercard' and r['cross_border']]
    assert len({r['processor_id'] for r in target}) == cfg.processors
    assert any(not r['approved'] for r,f in zip(rows,flags) if not f[0])


def test_statistical_contrast():
    a, b = Counts(n=10000,declined=2400), Counts(n=10000,declined=350)
    result = contrast(a,b)
    assert result['difference_pp'] == pytest.approx(20.5)
    assert 19 < result['lower_95_pp'] < 20.5 < result['upper_95_pp'] < 22


def test_private_readback_reconciliation_and_tampering(tmp_path):
    cfg = replace(config(), rows=10000, batch_size=1301)
    public, private = tmp_path/'public', tmp_path/'private'
    generate(cfg,public,private_output=private,run_id='quality-test')
    report = evaluate(cfg, public, private)
    assert report['reconciled'] and not report['passed']
    assert not report['checks']['million_rows']
    assert (private/'scenario_quality.json').exists()
    assert not list(public.rglob('*quality*'))
    with pytest.raises(ValueError, match='acceptance failed'):
        evaluate(cfg,public,private,require_m2=True)
    truth = pq.read_table(private/'scenario_ground_truth.parquet').to_pylist()
    notes = json.loads(truth[0]['validation_notes'])
    assert report['counterfactual']['expected_value_excess'] == pytest.approx(notes['expected_value_excess'])
    truth[0]['expected_excess_declines'] += 1
    write_table(private/'scenario_ground_truth.parquet','scenario_ground_truth',truth)
    with pytest.raises(ValueError,match='truth does not reconcile'):
        evaluate(cfg,public,private)
    assert json.loads((private/'scenario_quality.json').read_text())['state'] == 'failed'
    truth[0]['expected_excess_declines'] -= 1
    write_table(private/'scenario_ground_truth.parquet','scenario_ground_truth',truth)
    audit = pq.read_table(private/'generation_batch_audit.parquet').to_pylist()
    audit[0]['batch_hash'] = 'tampered'
    write_table(private/'generation_batch_audit.parquet','generation_batch_audit',audit)
    with pytest.raises(ValueError,match='Batch audit'):
        evaluate(cfg,public,private)


def test_calibration_cli_reproducible_and_repairs_underexposure(tmp_path):
    import subprocess
    import sys
    import yaml
    raw = config_load('config/m2.yaml')
    raw['scenario']['target_candidate_share'] = .001
    source = tmp_path/'underexposed.yaml'
    source.write_text(yaml.safe_dump(raw))
    outputs = [tmp_path/'first.yaml', tmp_path/'second.yaml']
    for path in outputs:
        subprocess.run([sys.executable, 'scripts/calibrate_scenario.py', '--config', str(source), '--output', str(path)], check=True, capture_output=True)
        assert Config.parse(config_load(path)).scenario.target_candidate_share == .1977
        assert path.stat().st_mode & 0o777 == 0o600
    assert outputs[0].read_bytes() == outputs[1].read_bytes()


def test_canary_only_truth_references_existing_rule(tmp_path):
    cfg = replace(config(), rows=100, start_date='2026-10-09', days=1,
                  scenario=replace(config().scenario, target_bankwide_decline_rate_increase_pp=0))
    generate(cfg,tmp_path/'canary',private_output=tmp_path/'private')
    truth = pq.read_table(tmp_path/'private'/'scenario_ground_truth.parquet').to_pylist()[0]
    rules = pq.read_table(tmp_path/'canary'/'dimensions'/'routing_rules.parquet').to_pylist()
    assert truth['injected_routing_rule_id'] == 2003
    assert truth['injected_routing_rule_id'] in {r['routing_rule_id'] for r in rules}
