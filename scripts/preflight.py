"""Local readiness only; M4 requires an environment-specific catalog preflight."""
from pathlib import Path
import sys
import json
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from meridian.jobs import Settings
from meridian.registry import Registry
from meridian.preflight import inspect

if __name__=='__main__':
    settings = Settings.environment()
    report = inspect(settings,Registry(settings.state))
    print(json.dumps(report,indent=2))
    sys.exit(0 if report['local_parquet']['ready'] else 1)
