"""Read-only warehouse preflight. Issues no DDL or DML and reports actionable blockers."""
import pyarrow as pa
from ..schema_contract import contracts
from .identity import ROLES
from .session import Session

PUBLIC_TABLES = ('customers', 'merchants', 'payment_processors', 'payment_terminals', 'routing_rules',
                 'deployment_events', 'processor_incidents', 'transaction_response_codes',
                 'payment_transactions', 'generation_runs')
PRIVATE_TABLES = ('scenario_ground_truth', 'generation_batch_audit')
PARTITION_TRANSFORMS = {'payment_transactions': [('transaction_ts', 'DAY')]}
DENIAL_MARKERS = ('authoriz', 'privilege', 'not have', 'permission', 'denied')


def impala_type(dtype):
    if pa.types.is_decimal(dtype): return f'decimal({dtype.precision},{dtype.scale})'
    return {pa.int64(): 'bigint', pa.int32(): 'int', pa.string(): 'string',
            pa.timestamp('us'): 'timestamp', pa.bool_(): 'boolean'}[dtype]


def expected_columns(table):
    return [(f.name, impala_type(f.type)) for f in contracts()[table]]


def formatted(session, qualified):
    """Columns, partition transforms and table parameters from DESCRIBE FORMATTED."""
    columns, transforms, params, section = [], [], {}, 'columns'
    for row in session.execute(f'DESCRIBE FORMATTED {qualified}'):
        first = (row[0] or '').strip()
        second = (row[1] or '').strip() if len(row) > 1 and row[1] is not None else ''
        third = (row[2] or '').strip() if len(row) > 2 and row[2] is not None else ''
        if first.startswith('# Partition Transform'): section = 'transforms'; continue
        if first.startswith('# Detailed Table'): section = 'details'; continue
        if first.startswith('# Storage'): section = 'storage'; continue
        if first.startswith('#') or (not first and not second): continue
        if section == 'columns' and first: columns.append((first.lower(), second.lower()))
        elif section == 'transforms' and first: transforms.append((first.lower(), second.upper()))
        elif section == 'details' and not first and second: params[second] = third
    return columns, transforms, params


class Report:
    def __init__(self):
        self.checks = []

    def add(self, name, ok, detail='', severity='blocker'):
        self.checks.append(dict(name=name, ok=bool(ok), severity=severity, detail=detail))
        return ok

    def problems(self, severity):
        return [c for c in self.checks if not c['ok'] and c['severity'] == severity]

    def result(self, **extra):
        blockers, warnings = self.problems('blocker'), self.problems('warning')
        return dict(ready=not blockers, blockers=[f"{c['name']}: {c['detail']}" for c in blockers],
                    warnings=[f"{c['name']}: {c['detail']}" for c in warnings], checks=self.checks, **extra)


def check_tables(report, session, database, tables):
    for table in tables:
        qualified = f'{database}.{table}'
        try:
            columns, transforms, params = formatted(session, qualified)
        except Exception as exc:
            report.add(f'table {qualified}', False, f'not readable by {session.identity.role}: {str(exc)[:160]}')
            continue
        expected = expected_columns(table)
        if columns != expected:
            drift = sorted(set(columns) ^ set(expected)) or 'column order differs'
            report.add(f'schema {qualified}', False, f'does not match sql/schema contract: {drift}')
        else:
            report.add(f'schema {qualified}', True, f'{len(columns)} columns match the contract')
        report.add(f'iceberg {qualified}', params.get('table_type', '').upper() == 'ICEBERG',
                   f"table_type={params.get('table_type') or 'unknown'}; existing tables are never altered")
        want = PARTITION_TRANSFORMS.get(table, [])
        report.add(f'partitioning {qualified}', transforms == want, f'spec {transforms or "unpartitioned"}; contract {want or "unpartitioned"}')


def other_runs(session, database, tables, run_id):
    found = {}
    for table in tables:
        n = int(session.scalar(f"SELECT COUNT(*) FROM {database}.{table} WHERE generation_run_id IS NULL OR generation_run_id <> '{run_id}'"))
        if n: found[table] = n
    return found


def inspect(identities, target, run_id='-', allow_unauthenticated=False, sessions=None, accept_shared_login=False):
    """Return a secret-free readiness report; ``sessions`` lets callers reuse open sessions."""
    report = Report()
    sessions = dict(sessions or {})
    owned = []
    missing = [role for role in ROLES if identities.get(role) is None]
    for role in missing:
        report.add(f'identity {role}', False, f'set MERIDIAN_{ROLES[role]}_HOST or MERIDIAN_IMPALA_HOST and its credentials')
    configured = {role: i for role, i in identities.items() if i is not None}
    authenticated = all(i.authenticated for i in configured.values()) and not missing
    severity = 'warning' if allow_unauthenticated else 'blocker'
    # Explicit interim choice: load before the agent login exists. Isolation is re-proved later by preflight alone.
    isolation = 'warning' if allow_unauthenticated or accept_shared_login else 'blocker'
    report.add('authentication', authenticated,
               'all identities authenticate' if authenticated else 'NOSASL identities cannot prove separation or Ranger isolation; local development only',
               severity)
    if authenticated:
        agent, public, private = (configured[r].principal for r in ('agent', 'public_writer', 'private_writer'))
        report.add('identity separation', agent not in (public, private), 'agent must not share a login with either writer'
                   + ('; accepted for this load with --accept-shared-login' if accept_shared_login else ''), isolation)
        report.add('writer separation', public != private, 'public and private writers should be distinct logins', 'warning')
    try:
        for role, identity in configured.items():
            if role in sessions: continue
            try:
                sessions[role] = Session(identity)
                owned.append(sessions[role])
            except Exception as exc:
                report.add(f'connect {role}', False, f'{identity.host}:{identity.port}: {type(exc).__name__}: {str(exc)[:160]}')
        for role, session in sessions.items():
            report.add(f'connect {role}', True, session.scalar('SELECT version()').splitlines()[0][:80])
        if 'agent' in sessions:
            check_tables(report, sessions['agent'], target.database, PUBLIC_TABLES)
            try:
                sessions['agent'].execute(f'SELECT 1 FROM {target.private_database}.scenario_ground_truth LIMIT 1')
                report.add('agent private isolation', False, f'agent identity can read {target.private_database}; deny it in Ranger', isolation)
            except Exception as exc:
                message = str(exc).lower()
                report.add('agent private isolation', any(m in message for m in DENIAL_MARKERS),
                           f'agent read of {target.private_database} refused: {str(exc)[:120]}', 'blocker' if any(m in message for m in DENIAL_MARKERS) else 'warning')
        if 'private_writer' in sessions:
            check_tables(report, sessions['private_writer'], target.private_database, PRIVATE_TABLES)
        if 'public_writer' in sessions:
            databases = {r[0].lower() for r in sessions['public_writer'].execute('SHOW DATABASES')}
            for role, staging in (('public_writer', target.staging_database), ('private_writer', target.private_staging_database)):
                report.add(f'staging database {staging}', staging in databases,
                           'exists' if staging in databases else f'{role} will CREATE DATABASE {staging}; grant it or pre-create it', 'warning')
            try:
                occupied = other_runs(sessions['public_writer'], target.database, PUBLIC_TABLES, run_id)
                report.add('target occupancy', not occupied,
                           'empty of other runs' if not occupied else f'{sum(occupied.values()):,} rows of other runs in {sorted(occupied)}; '
                           'new_run never mixes runs (transaction IDs restart per run). Load into an empty target; '
                           'clearing tables is an explicit operator action outside this tool')
            except Exception as exc:
                report.add('target occupancy', False, f'cannot count existing rows: {str(exc)[:160]}')
    finally:
        for session in owned: session.close()
    return report.result(production_ready=authenticated and not report.problems('blocker') and not report.problems('warning'),
                         identities={r: i.describe() for r, i in configured.items()}, target=vars(target))
