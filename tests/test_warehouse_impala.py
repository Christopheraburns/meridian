"""M4 integration against a live Impala, in scratch databases this test creates and drops.

Skipped unless MERIDIAN_IT_IMPALA_HOST, MERIDIAN_IT_STAGING_DIR and MERIDIAN_IT_STAGING_URI
are set (the staging directory as this process writes it and as Impala reads it).
Never targets meridian_financial or meridian_ground_truth. Local Optiness quickstart:

    MERIDIAN_IT_IMPALA_HOST=127.0.0.1 \
    MERIDIAN_IT_STAGING_DIR=../Optiness/data/staging/meridian-m4-it \
    MERIDIAN_IT_STAGING_URI=file:///home/cdsw/data/staging/meridian-m4-it \
    PYTHONPATH=src python3 -m pytest -q tests/test_warehouse_impala.py
"""
from dataclasses import replace
from pathlib import Path
import os
import re
import shutil
import uuid
import pytest
from meridian.config import Config, config_load
from meridian.generator import generate
from meridian.schema_contract import CONTRACT_ROOT
from meridian.warehouse import loader
from meridian.warehouse.identity import identities
from meridian.warehouse.loader import LoadError, load
from meridian.warehouse.session import Session, Target

HOST = os.environ.get('MERIDIAN_IT_IMPALA_HOST')
pytestmark = pytest.mark.skipif(not (HOST and os.environ.get('MERIDIAN_IT_STAGING_DIR') and os.environ.get('MERIDIAN_IT_STAGING_URI')),
                                reason='live Impala integration not configured')


@pytest.fixture(scope='module')
def warehouse():
    pytest.importorskip('impala')
    suffix = uuid.uuid4().hex[:8]
    target = Target(f'm4it_{suffix}_public', f'm4it_{suffix}_private', f'm4it_{suffix}_stage', f'm4it_{suffix}_pstage')
    env = {'MERIDIAN_IMPALA_HOST': HOST, 'MERIDIAN_IMPALA_PORT': os.environ.get('MERIDIAN_IT_IMPALA_PORT', '21050'),
           'MERIDIAN_IMPALA_QUERY_OPTIONS': os.environ.get('MERIDIAN_IT_QUERY_OPTIONS', 'MT_DOP=0,DISABLE_CODEGEN=true,EXEC_TIME_LIMIT_S=180')}
    ids = identities(env)
    admin = Session(ids['public_writer'])
    staging = Path(os.environ['MERIDIAN_IT_STAGING_DIR'])/suffix
    try:
        # Test-owned scratch schema from the immutable contracts; production code never runs DDL on tables.
        for path, database in (('meridian_financial_iceberg.sql', target.database), ('meridian_financial_private_iceberg.sql', target.private_database)):
            admin.execute(f'CREATE DATABASE {database}')
            sql = re.sub(r'--[^\n]*', '', (CONTRACT_ROOT/path).read_text())
            sql = sql.replace('meridian_financial.', f'{target.database}.').replace('meridian_ground_truth.', f'{target.private_database}.')
            for statement in (s.strip() for s in sql.split(';')):
                if statement.startswith('CREATE TABLE'): admin.execute(statement)
        yield dict(target=target, ids=ids, admin=admin, staging=staging,
                   uri=os.environ['MERIDIAN_IT_STAGING_URI'].rstrip('/') + f'/{suffix}')
    finally:
        for database in (target.database, target.private_database, target.staging_database, target.private_staging_database):
            admin.execute(f'DROP DATABASE IF EXISTS {database} CASCADE')
        admin.close()
        shutil.rmtree(staging, ignore_errors=True)


def generated(tmp_path, rows, run_id):
    cfg = replace(Config.parse(config_load('config/smoke.yaml')), rows=rows, batch_size=2500)
    generate(cfg, tmp_path/run_id/'public', private_output=tmp_path/run_id/'private', run_id=run_id)
    return cfg, tmp_path/run_id/'public', tmp_path/run_id/'private'


def run_load(w, cfg, public, private, run_id, log):
    return load(cfg, public, private, w['ids'], w['target'], f"{w['uri']}/{run_id}/public", f"{w['uri']}/{run_id}/private",
                w['staging']/run_id/'public', w['staging']/run_id/'private', allow_unauthenticated=True, log=log.append)


def count(w, table, run_id, private=False):
    database = w['target'].private_database if private else w['target'].database
    return int(w['admin'].scalar(f"SELECT COUNT(*) FROM {database}.{table} WHERE generation_run_id = '{run_id}'"))


def test_commit_interrupt_resume_and_occupancy(warehouse, tmp_path, monkeypatch):
    w, run_id, log = warehouse, f'm4it-{uuid.uuid4().hex[:8]}', []
    cfg, public, private = generated(tmp_path, 10000, run_id)

    # Interrupted just before the completion marker: facts committed, run not complete.
    original = loader.Writer.unit
    def fail_marker(self, table, where, label, attempts=3):
        if table == 'generation_runs': raise LoadError('simulated interruption')
        return original(self, table, where, label, attempts)
    monkeypatch.setattr(loader.Writer, 'unit', fail_marker)
    with pytest.raises(LoadError, match='simulated'): run_load(w, cfg, public, private, run_id, log)
    assert count(w, 'payment_transactions', run_id) == 10000 and count(w, 'generation_runs', run_id) == 0
    assert count(w, 'scenario_ground_truth', run_id, private=True) == 1
    monkeypatch.setattr(loader.Writer, 'unit', original)

    log.clear()
    result = run_load(w, cfg, public, private, run_id, log)
    assert result['passed'] and result['rows']['payment_transactions'] == 10000, result['checks']
    assert any('skip payment_transactions' in line for line in log) and any('insert generation_runs' in line for line in log)
    assert not result['production_ready']  # NOSASL: no identity separation or Ranger proof
    assert (private/'warehouse_load.json').stat().st_mode & 0o777 == 0o600

    log.clear()
    again = run_load(w, cfg, public, private, run_id, log)
    assert again['passed'] and not any(' insert ' in line for line in log)
    assert count(w, 'payment_transactions', run_id) == 10000 and count(w, 'generation_batch_audit', run_id, private=True) == 4
    staged = [r[0] for r in w['admin'].execute(f"SHOW TABLES IN {w['target'].staging_database}")]
    assert staged == []

    other = f'm4it-{uuid.uuid4().hex[:8]}'
    cfg2, public2, private2 = generated(tmp_path, 600, other)
    with pytest.raises(LoadError, match='target occupancy'): run_load(w, cfg2, public2, private2, other, [])
    assert count(w, 'payment_transactions', other) == 0
