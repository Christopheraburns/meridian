"""Local-only atomic file writes. No catalog or warehouse operations."""
import hashlib
import json
import os
import pyarrow.parquet as pq
from ..schema_contract import table


def content_hash(rows):
    return hashlib.sha256(json.dumps(rows, sort_keys=True, default=str, separators=(',', ':')).encode()).hexdigest()


def write_table(path, name, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = table(name, rows)
    temporary = path.with_suffix('.tmp')
    pq.write_table(data, temporary, compression='zstd')
    os.replace(temporary, path)
    written = pq.read_metadata(path).num_rows
    if written != data.num_rows: raise ValueError('Written Parquet row count mismatch')
    return written


def write_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str))
    os.replace(temporary, path)
