"""Local readiness and explicit M4 blockers; never attempts warehouse writes."""
import importlib.util
import platform
import shutil
import tempfile
from .schema_contract import contracts
from .jobs import worker_alive


def inspect(settings,registry):
    settings.validate()
    checks = {'schema_contracts':len(contracts())==12, 'registry':True,
              'worker_online':worker_alive(registry)}
    try:
        settings.public.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryFile(dir=settings.public) as stream:
            stream.write(b'preflight'); stream.flush()
        checks['public_storage_writable'] = True
    except OSError:
        checks['public_storage_writable'] = False
    return dict(local_parquet=dict(ready=all(checks.values()),checks=checks,
                                  worker_last_seen=registry.last_seen(),
                                  free_bytes=shutil.disk_usage(registry.root).free),
                runtime=dict(python=platform.python_version(),spark_installed=importlib.util.find_spec('pyspark') is not None),
                spark_iceberg=dict(ready=False,blockers=['M4 catalog, credentials, schema access and native Iceberg commit/Impala visibility have not been validated']))
