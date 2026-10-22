"""M4 unit tests: identities, guards, preflight decisions and staging layout. No warehouse needed."""
from dataclasses import replace
import json
import pytest
from meridian.config import Config, config_load
from meridian.generator import generate
from meridian.warehouse.identity import Identity, identities
from meridian.warehouse.loader import LoadError, read_public_run, stage_files
from meridian.warehouse import preflight
from meridian.warehouse.preflight import expected_columns, formatted, inspect
from meridian.warehouse.session import Target, name, run_id, uri

ENV = {'MERIDIAN_IMPALA_HOST': 'impala.example', 'MERIDIAN_IMPALA_AUTH_MECHANISM': 'LDAP',
       'MERIDIAN_IMPALA_USE_HTTP': 'true', 'MERIDIAN_IMPALA_USE_SSL': 'true',
       'MERIDIAN_PUBLIC_WRITER_USER': 'gen-public', 'MERIDIAN_PUBLIC_WRITER_PASSWORD': 'public-secret',
       'MERIDIAN_PRIVATE_WRITER_USER': 'gen-private', 'MERIDIAN_PRIVATE_WRITER_PASSWORD': 'private-secret',
       'MERIDIAN_AGENT_USER': 'agent', 'MERIDIAN_AGENT_PASSWORD': 'agent-secret'}


def test_identities_inherit_endpoint_override_per_role_and_hide_secrets(tmp_path):
    secret = tmp_path/'agent.pw'; secret.write_text('from-file\n')
    env = dict(ENV, MERIDIAN_PRIVATE_WRITER_HOST='private.example', MERIDIAN_AGENT_PASSWORD_FILE=str(secret))
    ids = identities(env)
    assert ids['public_writer'].host == 'impala.example' and ids['private_writer'].host == 'private.example'
    assert ids['agent'].password == 'from-file' and ids['agent'].use_http and ids['agent'].verify_cert
    for identity in ids.values():
        text = repr(identity) + json.dumps(identity.describe())
        assert 'secret' not in text and 'from-file' not in text


def test_identity_configuration_errors():
    assert Identity.environment('agent', {}) is None
    with pytest.raises(ValueError, match='requires'):
        Identity.environment('agent', {'MERIDIAN_IMPALA_HOST': 'h', 'MERIDIAN_IMPALA_AUTH_MECHANISM': 'LDAP', 'MERIDIAN_AGENT_USER': 'a'})
    with pytest.raises(ValueError, match='unsupported'):
        Identity.environment('agent', {'MERIDIAN_IMPALA_HOST': 'h', 'MERIDIAN_IMPALA_AUTH_MECHANISM': 'BASIC'})
    with pytest.raises(ValueError, match='NAME=value'):
        Identity.environment('agent', {'MERIDIAN_IMPALA_HOST': 'h', 'MERIDIAN_IMPALA_QUERY_OPTIONS': "MT_DOP=0;DROP"})
    # Credentials are never inherited from the shared endpoint block.
    shared = Identity.environment('agent', {'MERIDIAN_IMPALA_HOST': 'h', 'MERIDIAN_IMPALA_PASSWORD': 'x', 'MERIDIAN_IMPALA_USER': 'u'})
    assert shared.password == '' and shared.user == ''


def test_sql_guards():
    assert name('meridian_financial') and run_id('8e0ce02e-b11b-4223-accb-290ec0689cb7')
    for bad in ("x'; DROP", 'a.b', ''):
        with pytest.raises(ValueError): name(bad)
    with pytest.raises(ValueError): run_id("x' OR '1'='1")
    assert uri('s3a://bucket/stage/') == 's3a://bucket/stage'
    for bad in ('/no/scheme', "s3a://b/x'y", 's3a://b/x;y'):
        with pytest.raises(ValueError): uri(bad)
    with pytest.raises(ValueError, match='distinct'): Target(staging_database='meridian_financial')


FORMATTED = [('# col_name            ', 'data_type           ', 'comment             '), ('', None, None),
             ('transaction_id', 'bigint', None), ('transaction_ts', 'timestamp', None), ('', None, None),
             ('# Partition Transform Information', None, None), ('# col_name            ', 'transform_type      ', None),
             ('', None, None), ('transaction_ts', 'DAY', None), ('', None, None),
             ('# Detailed Table Information', None, None), ('Database:           ', 'meridian_financial  ', None),
             ('Table Parameters:', None, None), ('', 'storage_handler     ', 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'),
             ('', 'table_type          ', 'ICEBERG             '), ('# Storage Information', None, None),
             ('SerDe Library:      ', 'org.apache.iceberg.mr.hive.HiveIcebergSerDe', None)]


class FakeSession:
    """Answers the preflight's statements from a dictionary of table definitions."""
    def __init__(self, identity, tables, private_readable=False, other_rows=0):
        self.identity, self.tables, self.private_readable, self.other_rows = identity, tables, private_readable, other_rows

    def execute(self, sql, label=None):
        if sql.startswith('DESCRIBE FORMATTED '):
            table = sql.split('.')[-1]
            if table not in self.tables: raise RuntimeError('AnalysisException: Could not resolve table reference')
            columns, iceberg = self.tables[table]
            rows = [(n, t, None) for n, t in columns]
            if table == 'payment_transactions':
                rows += [('# Partition Transform Information', None, None), ('transaction_ts', 'DAY', None)]
            return rows + [('# Detailed Table Information', None, None), ('', 'table_type', 'ICEBERG' if iceberg else 'NATIVE')]
        if 'scenario_ground_truth LIMIT 1' in sql:
            if self.private_readable: return [(1,)]
            raise RuntimeError('AuthorizationException: User agent does not have privileges to execute SELECT')
        if sql == 'SHOW DATABASES': return [('meridian_staging', ''), ('meridian_private_staging', '')]
        if sql.startswith('SELECT COUNT(*)'): return [(self.other_rows,)]
        if sql == 'SELECT version()': return [('impalad version 4.5.3-RELEASE',)]
        raise AssertionError(sql)

    def scalar(self, sql): return self.execute(sql)[0][0]


def contract_tables(drift=None):
    tables = {t: (expected_columns(t), True) for t in preflight.PUBLIC_TABLES + preflight.PRIVATE_TABLES}
    if drift: tables[drift] = (expected_columns(drift)[:-1], True)
    return tables


def sessions(ids, **kwargs):
    return {role: FakeSession(i, contract_tables(kwargs.get('drift')), **{k: v for k, v in kwargs.items() if k != 'drift'})
            for role, i in ids.items()}


def test_formatted_parses_impala_describe_output():
    columns, transforms, params = formatted(type('S', (), {'execute': lambda self, sql: FORMATTED})(), 'db.t')
    assert columns == [('transaction_id', 'bigint'), ('transaction_ts', 'timestamp')]
    assert transforms == [('transaction_ts', 'DAY')] and params['table_type'] == 'ICEBERG'


def test_preflight_ready_with_separated_authenticated_identities():
    ids = identities(ENV)
    report = inspect(ids, Target(), sessions=sessions(ids))
    assert report['ready'] and report['production_ready'], report['blockers'] + report['warnings']
    assert 'secret' not in json.dumps(report)


@pytest.mark.parametrize('kwargs, blocker', [
    (dict(private_readable=True), 'agent private isolation'),
    (dict(other_rows=5), 'target occupancy'),
    (dict(drift='payment_transactions'), 'schema meridian_financial.payment_transactions'),
    (dict(drift='scenario_ground_truth'), 'schema meridian_ground_truth.scenario_ground_truth'),
])
def test_preflight_blockers(kwargs, blocker):
    ids = identities(ENV)
    report = inspect(ids, Target(), sessions=sessions(ids, **kwargs))
    assert not report['ready'] and any(b.startswith(blocker) for b in report['blockers']), report['blockers']


def test_preflight_rejects_shared_agent_login_and_missing_identity():
    ids = identities(dict(ENV, MERIDIAN_AGENT_USER='gen-public'))
    assert any(b.startswith('identity separation') for b in inspect(ids, Target(), sessions=sessions(ids))['blockers'])
    ids = dict(identities(ENV), agent=None)
    assert any(b.startswith('identity agent') for b in inspect(ids, Target(), sessions=sessions(ids))['blockers'])


def test_unauthenticated_local_impala_is_development_only():
    ids = identities({'MERIDIAN_IMPALA_HOST': 'localhost'})
    strict = inspect(ids, Target(), sessions=sessions(ids, private_readable=True))
    assert not strict['ready'] and {b.split(':')[0] for b in strict['blockers']} == {'authentication', 'agent private isolation'}
    local = inspect(ids, Target(), allow_unauthenticated=True, sessions=sessions(ids, private_readable=True))
    assert local['ready'] and not local['production_ready']


def test_stage_layout_and_public_run_guards(tmp_path):
    cfg = replace(Config.parse(config_load('config/smoke.yaml')), rows=600, batch_size=250)
    public, private = tmp_path/'public', tmp_path/'private'
    generate(cfg, public, private_output=private, run_id='m4-unit')
    assert read_public_run(public)['generation_run_id'] == 'm4-unit'
    rows = stage_files(public, private, tmp_path/'stage/public', tmp_path/'stage/private')
    assert rows['payment_transactions'] == 600 and rows['generation_runs'] == 1 and rows['generation_batch_audit'] == 3
    assert len(list((tmp_path/'stage/public/payment_transactions').glob('*.parquet'))) == 3
    assert sorted(p.name for p in (tmp_path/'stage/private').iterdir()) == ['generation_batch_audit', 'scenario_ground_truth']
    assert not list((tmp_path/'stage/public').rglob('*ground_truth*'))
    with pytest.raises(LoadError, match='separate'):
        stage_files(public, private, tmp_path/'stage', tmp_path/'stage/private')
    manifest = json.loads((public/'public_manifest.json').read_text()); manifest['complete'] = False
    (public/'public_manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(LoadError, match='not complete'): read_public_run(public)


def test_shared_login_is_an_explicit_interim_exception():
    ids = identities(dict(ENV, MERIDIAN_AGENT_USER='gen-public', MERIDIAN_PRIVATE_WRITER_USER='gen-public'))
    strict = inspect(ids, Target(), sessions=sessions(ids, private_readable=True))
    assert {b.split(':')[0] for b in strict['blockers']} == {'identity separation', 'agent private isolation'}
    interim = inspect(ids, Target(), sessions=sessions(ids, private_readable=True), accept_shared_login=True)
    assert interim['ready'] and not interim['production_ready']
    assert any(w.startswith('identity separation') and 'accept-shared-login' in w for w in interim['warnings'])
    assert any(w.startswith('agent private isolation') for w in interim['warnings'])
    # The flag never excuses schema drift or an occupied target.
    occupied = inspect(ids, Target(), sessions=sessions(ids, private_readable=True, other_rows=5), accept_shared_login=True)
    assert [b.split(':')[0] for b in occupied['blockers']] == ['target occupancy']
