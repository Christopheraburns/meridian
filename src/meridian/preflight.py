"""Local readiness and warehouse configuration; never connects to or writes the warehouse."""
import importlib.util
import platform
import shutil
import tempfile
from .schema_contract import contracts
from .jobs import worker_alive
from .warehouse.identity import identities


def inspect(settings,registry):
    settings.validate()
    checks = {'schema_contracts':len(contracts())==12, 'registry':True}
    locking_error = None
    try:
        checks['worker_online'] = worker_alive(registry)
        checks['registry_locking'] = True
    except OSError as exc:
        # Network filesystems may not support flock; the worker cannot fence writers there.
        checks['worker_online'] = checks['registry_locking'] = False
        locking_error = f'{type(exc).__name__}: {exc.strerror or exc}'
    try:
        settings.public.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryFile(dir=settings.public) as stream:
            stream.write(b'preflight'); stream.flush()
        checks['public_storage_writable'] = True
    except OSError:
        checks['public_storage_writable'] = False
    return dict(local_parquet=dict(ready=all(checks.values()),checks=checks,locking_error=locking_error,
                                  worker_last_seen=registry.last_seen(),
                                  free_bytes=shutil.disk_usage(registry.root).free),
                runtime=dict(python=platform.python_version(),spark_installed=importlib.util.find_spec('pyspark') is not None),
                spark_iceberg=dict(ready=False,blockers=['Not used: M4 commits through impala_staged_load (docs/m4_warehouse.md)']),
                warehouse=warehouse())


def warehouse():
    # Configuration only: live checks open Impala sessions and belong to scripts/warehouse.py preflight.
    try:
        configured = {role:(i.describe() if i else None) for role,i in identities().items()}
        error = None
    except (OSError, ValueError) as exc:
        configured, error = {}, str(exc)
    return dict(engine='impala_staged_load',identities=configured,error=error,
                configured=bool(configured) and all(configured.values()),
                live_check='python3 scripts/warehouse.py preflight')
