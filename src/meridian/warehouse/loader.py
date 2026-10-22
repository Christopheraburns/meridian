"""impala_staged_load: commit one validated Parquet run into the existing Iceberg tables.

Every row lands through an Impala INSERT, which is an Iceberg commit; files are never
copied into warehouse locations. Order: dimensions, transactions one week per
statement, private truth (private writer), then the public generation_runs row as
the completion marker. Each unit is skipped when already loaded and cleared when
partial, so a failed load resumes. Tables are never created, dropped, altered or
truncated: a target holding another run is refused, and clearing it is an operator task.
"""
from datetime import datetime, timedelta
from pathlib import Path
import json
import os
import shutil
import pyarrow.parquet as pq
from ..config import Config
from ..validation import validate_public_output
from .preflight import PUBLIC_TABLES, PRIVATE_TABLES, expected_columns, inspect
from .session import Session, run_id as checked_run_id, uri as checked_uri

FACT, MARKER = 'payment_transactions', 'generation_runs'
DIMENSIONS = tuple(t for t in PUBLIC_TABLES if t not in (FACT, MARKER))


class LoadError(RuntimeError):
    pass


def _link_or_copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists(): destination.unlink()
    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)


def read_public_run(public):
    public = Path(public)
    manifest = json.loads((public/'public_manifest.json').read_text())
    if not manifest.get('complete') or manifest.get('status') != 'complete':
        raise LoadError('Public run is not complete; only complete runs are loaded')
    if (public/'scenario_ground_truth.parquet').exists() or any(public.rglob('*ground_truth*')):
        raise LoadError('Public tree contains private truth; refusing to stage it')
    return manifest


def stage_files(public, private, staging_dir, private_staging_dir):
    """Lay out one directory per table, as external tables require. Hard-links where possible."""
    public, private = Path(public), Path(private)
    staging_dir, private_staging_dir = Path(staging_dir), Path(private_staging_dir)
    if staging_dir.resolve() == private_staging_dir.resolve() or staging_dir.resolve() in private_staging_dir.resolve().parents \
            or private_staging_dir.resolve() in staging_dir.resolve().parents:
        raise LoadError('Public and private staging directories must be separate trees')
    files = {}
    for table in PUBLIC_TABLES:
        sources = sorted((public/FACT).glob('*.parquet')) if table == FACT else [public/'dimensions'/f'{table}.parquet']
        if table != FACT and not sources[0].is_file(): raise LoadError(f'missing {sources[0].name}')
        shutil.rmtree(staging_dir/table, ignore_errors=True)
        for source in sources: _link_or_copy(source, staging_dir/table/source.name)
        files[table] = sum(pq.read_metadata(s).num_rows for s in sources)
    for table in PRIVATE_TABLES:
        source = private/f'{table}.parquet'
        if not source.is_file(): raise LoadError(f'missing private {source.name}')
        shutil.rmtree(private_staging_dir/table, ignore_errors=True)
        _link_or_copy(source, private_staging_dir/table/source.name)
        files[table] = pq.read_metadata(source).num_rows
    return files


class Writer:
    """Loads one table family through one identity's session."""
    def __init__(self, session, database, staging_database, staging_uri, run_id):
        self.s, self.db, self.staging, self.uri, self.run_id = session, database, staging_database, staging_uri, run_id

    def count(self, database, table, where='TRUE', run_scoped=True):
        scope = f"generation_run_id = '{self.run_id}' AND " if run_scoped else ''
        return int(self.s.scalar(f'SELECT COUNT(*) FROM {database}.{table} WHERE {scope}({where})'))

    def stage(self, table):
        # Declare staging columns from the contract, so drifted Parquet fails here instead of loading.
        columns = ',\n  '.join(f'`{n}` {t.upper()}' for n, t in expected_columns(table))
        self.s.execute(f'DROP TABLE IF EXISTS {self.staging}.{table}')
        self.s.execute(f"CREATE EXTERNAL TABLE {self.staging}.{table} (\n  {columns}\n) STORED AS PARQUET "
                       f"LOCATION '{self.uri}/{table}'", f'stage {table}')
        staged = self.count(self.staging, table, run_scoped=False)
        foreign = staged - self.count(self.staging, table)
        if foreign: raise LoadError(f'staged {table} holds {foreign:,} rows of another run')
        return staged

    def unit(self, table, where, label, attempts=3):
        names = ', '.join(f'`{n}`' for n, _ in expected_columns(table))
        insert = f'INSERT INTO {self.db}.{table} ({names}) SELECT {names} FROM {self.staging}.{table} WHERE {where}'
        want = self.count(self.staging, table, where)
        if self.count(self.db, table, where) == want:
            self.s.log(f'{self.s.identity.role:14s} skip {label:39s} {want:>10,} rows already committed')
            return want
        for attempt in range(1, attempts+1):
            got = self.count(self.db, table, where)
            if got:
                self.s.execute(f"DELETE FROM {self.db}.{table} WHERE generation_run_id = '{self.run_id}' AND ({where})", f'clear partial {label}')
            try:
                self.s.execute(insert, f'insert {label}')
            except Exception as exc:
                if attempt == attempts: raise LoadError(f'{label} failed {attempts} times: {str(exc)[:200]}; re-run to resume')
                self.s.log(f'{label} attempt {attempt} failed: {str(exc)[:120]}; retrying')
                continue
            if self.count(self.db, table, where) == want: return want
        raise LoadError(f'{label}: committed rows do not match the {want:,} staged rows')

    def weekly(self, table):
        first, last = self.s.execute(f'SELECT MIN(transaction_ts), MAX(transaction_ts) FROM {self.staging}.{table}')[0]
        if first is None: return 0
        start, total = datetime.combine(first.date(), datetime.min.time()), 0
        while start <= last:
            end = start + timedelta(days=7)
            total += self.unit(table, f"transaction_ts >= '{start}' AND transaction_ts < '{end}'", f'{table} {start:%Y-%m-%d}..')
            start = end
        return total

    def drop_staging(self, tables):
        for table in tables:
            try:
                self.s.execute(f'DROP TABLE IF EXISTS {self.staging}.{table}')  # external: staged files remain
            except Exception as exc:
                self.s.log(f'cleanup of {self.staging}.{table} skipped: {type(exc).__name__}')


def verify(agent, private_writer, target, run_id, expected):
    """Post-commit checks through Impala, as the agent sees the data."""
    checks = {}
    db = target.database
    q = lambda sql: int(agent.scalar(sql))
    for table in PUBLIC_TABLES:
        n = q(f"SELECT COUNT(*) FROM {db}.{table} WHERE generation_run_id = '{run_id}'")
        checks[f'count {table}'] = (n == expected[table], f'{n:,} committed, {expected[table]:,} generated')
        columns = [(r[0].lower(), r[1].lower()) for r in agent.execute(f'DESCRIBE {db}.{table}')]
        checks[f'types {table}'] = (columns == expected_columns(table), 'Impala types match the contract')
        snapshots = len(agent.execute(f'DESCRIBE HISTORY {db}.{table}'))
        checks[f'snapshots {table}'] = (snapshots > 0, f'{snapshots} Iceberg snapshots')
    status = agent.execute(f"SELECT run_status, generated_transaction_count FROM {db}.{MARKER} WHERE generation_run_id = '{run_id}'")
    checks['completion marker'] = (len(status) == 1 and status[0][0] == 'complete' and int(status[0][1]) == expected[FACT], f'{status}')
    run = f"t.generation_run_id = '{run_id}'"
    duplicates = q(f'SELECT COUNT(*) - COUNT(DISTINCT transaction_id) FROM {db}.{FACT} t WHERE {run}')
    checks['unique transaction_id'] = (duplicates == 0, f'{duplicates:,} duplicates')
    for dimension, key in (('customers', 'customer_id'), ('merchants', 'merchant_id'), ('payment_terminals', 'terminal_id'),
                           ('payment_processors', 'processor_id'), ('routing_rules', 'routing_rule_id'),
                           ('transaction_response_codes', 'response_code')):
        orphans = q(f'SELECT COUNT(*) FROM {db}.{FACT} t LEFT JOIN {db}.{dimension} d ON d.{key} = t.{key} '
                    f'AND d.generation_run_id = t.generation_run_id WHERE {run} AND d.{key} IS NULL')
        checks[f'join {dimension}'] = (orphans == 0, f'{orphans:,} orphan transactions')
    days = q(f"SELECT COUNT(DISTINCT CAST(transaction_ts AS DATE)) FROM {db}.{FACT} t WHERE {run}")
    checks['daily partitions'] = (days > 0, f'{days} transaction days')
    for table in PRIVATE_TABLES:
        n = int(private_writer.scalar(f"SELECT COUNT(*) FROM {target.private_database}.{table} WHERE generation_run_id = '{run_id}'"))
        checks[f'private count {table}'] = (n == expected[table], f'{n:,} committed, {expected[table]:,} generated')
    return {name: dict(ok=ok, detail=detail) for name, (ok, detail) in checks.items()}


def load(cfg, public, private, identities, target, staging_uri, private_staging_uri, staging_dir=None,
         private_staging_dir=None, allow_unauthenticated=False, validate=True, stats=True, log=print,
         accept_shared_login=False):
    cfg = Config.parse(cfg)
    staging_uri, private_staging_uri = checked_uri(staging_uri), checked_uri(private_staging_uri)
    if staging_uri == private_staging_uri or private_staging_uri.startswith(staging_uri + '/') or staging_uri.startswith(private_staging_uri + '/'):
        raise LoadError('Public and private staging URIs must be separate locations')
    public, private = Path(public), Path(private)
    manifest = read_public_run(public)
    run_id = checked_run_id(manifest['generation_run_id'])
    if validate:
        log(f'validating public Parquet for run {run_id}')
        validate_public_output(cfg, public, run_id)
    if staging_dir:
        log(f'staging files under {staging_dir} and {private_staging_dir}')
        stage_files(public, private, staging_dir, private_staging_dir)
    sessions = {}
    try:
        for role in ('public_writer', 'private_writer', 'agent'):
            if identities.get(role) is None: raise LoadError(f'{role} identity is not configured')
            sessions[role] = Session(identities[role], log)
        report = inspect(identities, target, run_id, allow_unauthenticated, sessions, accept_shared_login)
        for warning in report['warnings']: log(f'WARNING {warning}')
        if not report['ready']: raise LoadError('preflight blockers: ' + '; '.join(report['blockers']))
        public_session, private_session = sessions['public_writer'], sessions['private_writer']
        for session in (public_session, private_session):
            session.execute('SET PARQUET_FALLBACK_SCHEMA_RESOLUTION=NAME')
        public_session.execute(f"CREATE DATABASE IF NOT EXISTS {target.staging_database} COMMENT 'Meridian M4 transient external tables'")
        private_session.execute(f"CREATE DATABASE IF NOT EXISTS {target.private_staging_database} COMMENT 'Meridian M4 private transient external tables'")
        pub = Writer(public_session, target.database, target.staging_database, staging_uri, run_id)
        priv = Writer(private_session, target.private_database, target.private_staging_database, private_staging_uri, run_id)
        try:
            expected = {t: pub.stage(t) for t in PUBLIC_TABLES}
            expected.update({t: priv.stage(t) for t in PRIVATE_TABLES})
            if expected[FACT] != int(manifest['rows_generated']) or expected[MARKER] != 1:
                raise LoadError(f'staged files ({expected[FACT]:,} transactions) do not match the manifest '
                                f'({int(manifest["rows_generated"]):,}); re-stage the run')
            for table in DIMENSIONS: pub.unit(table, 'TRUE', table)
            pub.weekly(FACT)
            # Truth is written only after the public facts it describes are committed.
            for table in PRIVATE_TABLES: priv.unit(table, 'TRUE', table)
            pub.unit(MARKER, 'TRUE', MARKER)
        finally:
            pub.drop_staging(PUBLIC_TABLES)
            priv.drop_staging(PRIVATE_TABLES)
        if stats:
            for table in PUBLIC_TABLES: public_session.execute(f'COMPUTE STATS {target.database}.{table}')
        checks = verify(sessions['agent'], private_session, target, run_id, expected)
        passed = all(c['ok'] for c in checks.values())
        result = dict(generation_run_id=run_id, passed=passed, engine='impala_staged_load', target=vars(target),
                      rows={t: expected[t] for t in PUBLIC_TABLES}, checks=checks, preflight_warnings=report['warnings'],
                      production_ready=report['production_ready'], shared_login_accepted=accept_shared_login, completed_at=datetime.utcnow().isoformat(timespec='seconds'))
        path = private/'warehouse_load.json'
        path.write_text(json.dumps(dict(result, private_rows={t: expected[t] for t in PRIVATE_TABLES}), indent=2))
        path.chmod(0o600)
        if not passed:
            failed = [f"{n}: {c['detail']}" for n, c in checks.items() if not c['ok']]
            raise LoadError('post-commit verification failed: ' + '; '.join(failed))
        return result
    finally:
        for session in sessions.values(): session.close()
