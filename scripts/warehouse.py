"""M4 warehouse commands: read-only preflight, local staging, and impala_staged_load.

    python3 scripts/warehouse.py preflight
    python3 scripts/warehouse.py stage --public data/run-public --private data/run-private
    python3 scripts/warehouse.py load --config config/smoke.yaml \
        --public data/run-public --private data/run-private

Identities, databases and staging locations come from MERIDIAN_* environment
variables (docs/m4_warehouse.md). Each run stages under <staging>/<run_id>.
Reports never contain secrets; the load report goes to the private directory.
"""
import argparse
import json
import os
from pathlib import Path
import sys
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
from meridian.config import config_load
from meridian.warehouse.identity import identities
from meridian.warehouse.loader import LoadError, load, read_public_run, stage_files
from meridian.warehouse.preflight import inspect
from meridian.warehouse.session import Target


def staging(run_id, required_uri=True):
    def locate(key):
        base = os.environ.get(key, '').rstrip('/')
        return f'{base}/{run_id}' if base else None
    uris = locate('MERIDIAN_STAGING_URI'), locate('MERIDIAN_PRIVATE_STAGING_URI')
    dirs = locate('MERIDIAN_STAGING_DIR'), locate('MERIDIAN_PRIVATE_STAGING_DIR')
    if required_uri and not all(uris):
        raise SystemExit('Set MERIDIAN_STAGING_URI and MERIDIAN_PRIVATE_STAGING_URI: the staging locations as Impala reads them')
    if any(dirs) and not all(dirs):
        raise SystemExit('Set both MERIDIAN_STAGING_DIR and MERIDIAN_PRIVATE_STAGING_DIR, or neither')
    return uris, dirs


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest='command', required=True)
    pre = commands.add_parser('preflight', help='Read-only checks; exit 1 on blockers')
    pre.add_argument('--public', help='Public run, to check occupancy against its run ID')
    pre.add_argument('--allow-unauthenticated', action='store_true', help='Local NOSASL development only')
    stage = commands.add_parser('stage', help='Lay out one directory per table under MERIDIAN_(PRIVATE_)STAGING_DIR')
    run = commands.add_parser('load', help='Validate, stage if configured, commit and verify one run')
    for command in (stage, run):
        command.add_argument('--public', required=True)
        command.add_argument('--private', required=True)
    run.add_argument('--config', required=True, help='The config the run was generated from')
    run.add_argument('--allow-unauthenticated', action='store_true', help='Local NOSASL development only')
    run.add_argument('--skip-public-validation', action='store_true', help='Only when this exact run already passed validation')
    run.add_argument('--no-stats', action='store_true')
    args = parser.parse_args()
    target = Target.environment()

    if args.command == 'preflight':
        run_id = read_public_run(args.public)['generation_run_id'] if args.public else '-'
        report = inspect(identities(), target, run_id, args.allow_unauthenticated)
        print(json.dumps(report, indent=2))
        return 0 if report['ready'] else 1
    run_id = read_public_run(args.public)['generation_run_id']
    if args.command == 'stage':
        _, dirs = staging(run_id, required_uri=False)
        if not all(dirs): raise SystemExit('Set MERIDIAN_STAGING_DIR and MERIDIAN_PRIVATE_STAGING_DIR')
        rows = stage_files(args.public, args.private, *dirs)
        print(json.dumps(dict(generation_run_id=run_id, staged=dict(public=dirs[0], private=dirs[1]),
                              public_rows={k: v for k, v in rows.items() if k not in ('scenario_ground_truth', 'generation_batch_audit')}), indent=2))
        print('Copy both trees to MERIDIAN_STAGING_URI/<run_id> and MERIDIAN_PRIVATE_STAGING_URI/<run_id>, then run load.', file=sys.stderr)
        return 0
    uris, dirs = staging(run_id)
    try:
        result = load(config_load(args.config), args.public, args.private, identities(), target, *uris, *dirs,
                      allow_unauthenticated=args.allow_unauthenticated, validate=not args.skip_public_validation,
                      stats=not args.no_stats, log=lambda message: print(f'[m4] {message}', flush=True))
    except LoadError as exc:
        print(f'[m4] FAIL: {exc}', file=sys.stderr)
        return 1
    for name, check in result['checks'].items():
        print(f"[m4] {'PASS' if check['ok'] else 'FAIL'}  {name:36s} {check['detail']}")
    print(f"[m4] PASS: run {run_id} committed {result['rows']['payment_transactions']:,} transactions to {target.database}; "
          f"production_ready={result['production_ready']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
