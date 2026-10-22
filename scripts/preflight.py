"""Local readiness only; M4 requires an environment-specific catalog preflight."""
from pathlib import Path
import sys
import json
def _checkout():
    """Locate the Meridian checkout; Workbench Jobs run scripts as notebook cells without __file__."""
    import os
    if globals().get('__file__'): return Path(__file__).resolve().parents[1]
    configured = os.environ.get('GENERATOR_PROJECT_ROOT')
    cwd = Path.cwd().resolve()
    candidates = [Path(configured).expanduser().resolve()] if configured else [cwd, *cwd.parents, *sorted(p for p in cwd.iterdir() if p.is_dir())]
    for candidate in candidates:
        if (candidate/'src/meridian/__init__.py').is_file():
            os.chdir(candidate)  # Relative arguments resolve as when run from the checkout.
            return candidate
    raise SystemExit('Cannot locate the Meridian checkout; set GENERATOR_PROJECT_ROOT to the directory containing src/meridian')


sys.path.insert(0, str(_checkout()/'src'))
from meridian.jobs import Settings
from meridian.registry import Registry
from meridian.preflight import inspect

if __name__=='__main__':
    settings = Settings.environment()
    report = inspect(settings,Registry(settings.state))
    print(json.dumps(report,indent=2))
    sys.exit(0 if report['local_parquet']['ready'] else 1)
