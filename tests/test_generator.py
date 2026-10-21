import copy
import hashlib
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import pyarrow.parquet as pq
import pytest
from meridian.config import Config, config_load
from meridian.dimensions import build_dimensions
from meridian.generator import generate
from meridian.schema_contract import contracts, table
from meridian.transactions import Transactions
from meridian.validation import Validator
from meridian.writers.parquet import content_hash


def config(): return Config.parse(config_load('config/smoke.yaml'))


def read(root, name):
    path = root/'payment_transactions' if name == 'payment_transactions' else root/'dimensions'/f'{name}.parquet'
    return pq.read_table(path)


@pytest.fixture(scope='module')
def runs(tmp_path_factory):
    root = tmp_path_factory.mktemp('smoke')
    cfg = config()
    for name, size in [('a', 731), ('b', 2048)]:
        generate(replace(cfg, batch_size=size), root/name, run_id='smoke-test')
    return root


def test_frozen_contracts():
    for path in Path('sql/schema').glob('*.sql'):
        assert path.read_bytes() == (Path('meridian-v0.2-codex/sql/schema')/path.name).read_bytes()
    schemas = contracts()
    assert len(schemas) == 12
    assert len(schemas['payment_transactions']) == 28
    assert str(schemas['payment_transactions'].field('amount_usd').type) == 'decimal128(18, 2)'
    assert str(schemas['payment_transactions'].field('transaction_ts').type) == 'timestamp[us]'


def test_exact_schemas_and_counts(runs):
    for name, schema in contracts().items():
        data = pq.read_table(runs/'a_private'/f'{name}.parquet') if name in ('scenario_ground_truth', 'generation_batch_audit') else read(runs/'a', name)
        assert data.schema == schema
    for name, expected in [('customers',1000), ('merchants',200), ('payment_terminals',400), ('payment_processors',8), ('payment_transactions',10000), ('generation_runs',1)]:
        assert read(runs/'a',name).num_rows == expected
    run = read(runs/'a','generation_runs').to_pylist()[0]
    assert run['run_status'] == 'complete' and run['validation_status'] == 'passed'
    assert run['generated_transaction_count'] == 10000


def test_batch_independent_normalized_hashes(runs):
    for name in contracts():
        if name in ('scenario_ground_truth', 'generation_batch_audit', 'generation_runs'): continue
        a, b = read(runs/'a',name).to_pylist(), read(runs/'b',name).to_pylist()
        assert content_hash(a) == content_hash(b), name
    a = pq.read_table(runs/'a_private'/'scenario_ground_truth.parquet')
    b = pq.read_table(runs/'b_private'/'scenario_ground_truth.parquet')
    assert a.equals(b)
    for name in ('a', 'b'):
        audit = pq.read_table(runs/f'{name}_private'/'generation_batch_audit.parquet').to_pylist()
        for batch in audit:
            rows = pq.read_table(runs/name/'payment_transactions'/f"part-{batch['batch_id']:06d}.parquet").to_pylist()
            assert batch['batch_hash'] == content_hash(rows)


def test_referential_integrity_and_order(runs):
    cfg = config(); dims = build_dimensions(cfg,'smoke-test')
    rows = read(runs/'a','payment_transactions').to_pylist()
    Validator(cfg,dims,'smoke-test').validate(rows)
    assert len({r['transaction_id'] for r in rows}) == 10000
    source = Transactions(cfg,dims,'smoke-test')
    shuffled, _ = source.batch([10000, 8, 301, 1, 8])
    assert shuffled == [rows[i-1] for i in [10000,8,301,1,8]]
    bad = dict(rows[0], terminal_id=-1)
    with pytest.raises(ValueError, match='orphan'): Validator(cfg,dims,'smoke-test').validate([bad])


def test_anomaly_counterfactual_and_privacy(runs):
    cfg = config(); dims = build_dimensions(cfg,'smoke-test')
    source = Transactions(cfg,dims,'smoke-test')
    baseline_cfg = replace(cfg, scenario=replace(cfg.scenario, enabled=False))
    baseline = Transactions(baseline_cfg, dims, 'smoke-test')
    rows, flags = source.batch(range(1,10001))
    controls, _ = baseline.batch(range(1,10001))
    increments = [r for r, b in zip(rows, controls) if b['approved'] and not r['approved']]
    truth = pq.read_table(runs/'a_private'/'scenario_ground_truth.parquet').to_pylist()[0]
    assert truth['expected_exposed_transactions'] == sum(f[0] for f in flags)
    assert truth['expected_excess_declines'] == len(increments) == sum(f[1] for f in flags)
    assert truth['expected_affected_amount_usd'] == sum(r['amount_usd'] for r in increments)
    assert len(increments) >= 50  # At least 0.5 percentage points for this fixed 10k seed.
    faulty = [r for r in rows if r['routing_rule_id'] == 1003]
    assert any(r['approved'] for r in faulty) and any(not r['approved'] for r in faulty)
    assert any(not f[0] and r['routing_rule_id'] == 1003 for r, f in zip(rows,flags))
    assert all(r == b for r,b,f in zip(rows,controls,flags) if not f[0])
    assert not list((runs/'a').rglob('*truth*'))
    assert (runs/'a_private').stat().st_mode & 0o777 == 0o700
    manifest = json.loads((runs/'a'/'public_manifest.json').read_text())
    assert 'exposed' not in manifest and 'expected_excess_declines' not in manifest


def test_legacy_config_and_rejections(tmp_path):
    legacy = config_load('config/default.yaml')
    assert Config.parse(legacy).scenario.incident_end_exclusive == '2026-10-31T00:00:00'
    raw = config_load('config/smoke.yaml')
    assert raw['execution']['private_database'] == 'meridian_ground_truth'
    bad = copy.deepcopy(raw); bad['execution']['private_database'] = 'meridian_financial_private'
    with pytest.raises(ValueError, match='private_database must be meridian_ground_truth'):
        Config.parse(bad)
    for key, value in [('customers',0), ('batch_size',0), ('terminals',1), ('days',0)]:
        bad = copy.deepcopy(raw); bad['scale'][key] = value
        with pytest.raises(ValueError): Config.parse(bad)
    for key, value in [('enabled', 'false'), ('baseline_decline_rate', '0.035'), ('faulty_configuration_version', '4.11')]:
        bad = copy.deepcopy(raw); bad['scenario'][key] = value
        with pytest.raises(ValueError): Config.parse(bad)
    bad = copy.deepcopy(raw); bad['scenario']['target_bankwide_decline_rate_increase_pp'] = 99
    with pytest.raises(ValueError, match='Infeasible'): Config.parse(bad)
    bad = copy.deepcopy(raw); bad['execution']['engine'] = 'spark_iceberg'
    with pytest.raises(ValueError, match='M4 blocker'): Config.parse(bad)
    with pytest.raises(ValueError, match='missing/extra'): table('payment_transactions',[{'transaction_id':1}])
    with pytest.raises(ValueError, match='separate trees'): generate(config(),tmp_path/'a',private_output=tmp_path/'a'/'private')


def test_partial_failure_and_new_run(tmp_path):
    cfg = replace(config(),rows=100,batch_size=30)
    result = generate(cfg,tmp_path/'partial',max_batches=1)
    assert result['rows'] == 30 and not result['complete']
    assert read(tmp_path/'partial','generation_runs').to_pylist()[0]['run_status'] == 'partial'
    with pytest.raises(ValueError, match='empty output'): generate(cfg,tmp_path/'partial')
    def fail(done,total): raise RuntimeError('simulated failure')
    with pytest.raises(RuntimeError): generate(cfg,tmp_path/'failed',progress=fail)
    assert not json.loads((tmp_path/'failed'/'public_manifest.json').read_text())['complete']
    assert read(tmp_path/'failed','generation_runs').to_pylist()[0]['run_status'] == 'failed'


def test_incident_time_boundaries():
    from meridian.incident import outcomes
    from meridian.config import timestamp
    from datetime import timedelta
    cfg = config(); s = cfg.scenario
    row = dict(transaction_id=1, transaction_ts=timestamp(s.incident_start), processor_id=s.faulty_processor_id, routing_rule_id=1000+s.faulty_processor_id, card_network=s.targeted_network, channel=s.targeted_channel, cross_border=True)
    merchant = {'merchant_category_code':s.targeted_mcc}
    assert outcomes(cfg,row,merchant,False)[0]
    row['transaction_ts'] -= timedelta(microseconds=1)
    assert not outcomes(cfg,row,merchant,False)[0]
    row['transaction_ts'] = timestamp(s.incident_end_exclusive)
    assert not outcomes(cfg,row,merchant,False)[0]


def test_public_nulls_and_dimensions(runs):
    allowed = {'deployment_events': {'rollback_ts'}, 'transaction_response_codes': {'decline_type'}, 'payment_transactions': {'original_transaction_id'}}
    for name in contracts():
        if name in ('scenario_ground_truth','generation_batch_audit'): continue
        data = read(runs/'a',name)
        for column in data.schema.names:
            if column not in allowed.get(name,set()): assert data[column].null_count == 0, (name,column)
    dims = build_dimensions(config(),'smoke-test')
    rules = {r['routing_rule_id']: r for r in dims['routing_rules']}
    for d in dims['deployment_events']:
        rule = rules[d['routing_rule_id']]
        assert d['processor_id'] == rule['processor_id']
        assert d['deployed_version'] == rule['configuration_version']
        assert d['completed_ts'] == rule['effective_from_ts']


def test_flask_compatibility():
    import importlib.util
    spec = importlib.util.spec_from_file_location('meridian_app', 'app.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    client = module.app.test_client()
    assert client.get('/health').json == {'ok':True}
    assert client.get('/').status_code == 200
    assert 'exposed' not in client.get('/api/status').json
