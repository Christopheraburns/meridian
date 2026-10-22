"""Thin DB-API session: per-connection query options, timing and safe identifiers."""
from dataclasses import dataclass
import os
import re
import time

_NAME = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,127}$')
_RUN_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9-]{0,63}$')


def name(value):
    if not _NAME.match(value or ''): raise ValueError(f'Invalid SQL identifier: {value!r}')
    return value


def run_id(value):
    if not _RUN_ID.match(value or ''): raise ValueError('generation_run_id must be alphanumeric with hyphens')
    return value


def uri(value):
    value = (value or '').rstrip('/')
    if not re.match(r'^[a-z][a-z0-9+.-]*://', value) or any(c in value for c in "'\"`;\\\n"):
        raise ValueError('Staging URI must be a scheme-qualified location without quotes, e.g. s3a://bucket/path')
    return value


@dataclass(frozen=True)
class Target:
    database: str = 'meridian_financial'
    private_database: str = 'meridian_ground_truth'
    staging_database: str = 'meridian_staging'
    private_staging_database: str = 'meridian_private_staging'

    def __post_init__(self):
        values = [name(v) for v in (self.database, self.private_database, self.staging_database, self.private_staging_database)]
        if len(set(values)) != 4: raise ValueError('Public, private and both staging databases must be distinct')

    @classmethod
    def environment(cls, env=None):
        env = os.environ if env is None else env
        return cls(*(env.get(f'MERIDIAN_{key}', default) for key, default in (
            ('DATABASE', 'meridian_financial'), ('PRIVATE_DATABASE', 'meridian_ground_truth'),
            ('STAGING_DATABASE', 'meridian_staging'), ('PRIVATE_STAGING_DATABASE', 'meridian_private_staging'))))


class Session:
    def __init__(self, identity, log=None, connection=None):
        self.identity = identity
        self.log = log or (lambda message: None)
        self.connection = connection or identity.connect()
        self.cursor = self.connection.cursor()
        for option in identity.query_options: self.execute(f'SET {option}')

    def execute(self, statement, label=None):
        started = time.perf_counter()
        self.cursor.execute(statement)
        rows = self.cursor.fetchall() if self.cursor.description else []
        if label: self.log(f'{self.identity.role:14s} {label:44s} {int((time.perf_counter()-started)*1000):>7} ms')
        return rows

    def scalar(self, statement):
        return self.execute(statement)[0][0]

    def close(self):
        for closer in (self.cursor.close, self.connection.close):
            try:
                closer()
            except Exception:
                pass  # Closing must never mask the error that ended the session.
