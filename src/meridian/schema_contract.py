"""Read-only DDL introspection; SQL is never executed."""
import re
from pathlib import Path
import pyarrow as pa

CONTRACT_ROOT = Path(__file__).resolve().parents[2] / 'sql' / 'schema'


def contracts():
    result = {}
    for path in sorted(CONTRACT_ROOT.glob('*.sql')):
        sql = re.sub(r'--[^\n]*', '', path.read_text())
        for match in re.finditer(r'CREATE TABLE IF NOT EXISTS (\w+)\.(\w+)\s*\((.*?)\)\s*(?:PARTITIONED|STORED)', sql, re.S):
            fields = []
            for name, kind, precision, scale in re.findall(r'(\w+)\s+(BIGINT|INT|STRING|TIMESTAMP|BOOLEAN|DECIMAL)(?:\((\d+),(\d+)\))?', match[3]):
                dtype = pa.decimal128(int(precision), int(scale)) if kind == 'DECIMAL' else {
                    'BIGINT': pa.int64(), 'INT': pa.int32(), 'STRING': pa.string(),
                    'TIMESTAMP': pa.timestamp('us'), 'BOOLEAN': pa.bool_()}[kind]
                fields.append(pa.field(name, dtype))
            result[match[2]] = pa.schema(fields)
    if len(result) != 12:
        raise ValueError('Expected all 12 SQL schema contracts in sql/schema')
    return result


def table(name, rows):
    schema = contracts()[name]
    for row in rows:
        if set(row) != set(schema.names):
            raise ValueError(f'{name}: missing/extra columns: {set(row) ^ set(schema.names)}')
    return pa.Table.from_pylist(rows, schema=schema)
