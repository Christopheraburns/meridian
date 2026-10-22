"""Private operator/evaluator CLI; never exposed through the web control plane."""
import argparse
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


def _job_arguments():
    """Workbench Jobs run in a kernel whose sys.argv holds kernel flags; MERIDIAN_ARGS carries the script's arguments."""
    import os, shlex
    raw = os.environ.get('MERIDIAN_ARGS') or os.environ.get('JOB_ARGUMENTS')
    if raw:
        sys.argv = [sys.argv[0] if sys.argv else 'meridian', *shlex.split(raw)]
    elif sys.argv and 'ipykernel' in sys.argv[0]:
        sys.argv = [sys.argv[0]]
        print('No arguments: set the Job environment variable MERIDIAN_ARGS to this script\'s arguments', file=sys.stderr)


_job_arguments()
from meridian.config import Config, config_load
from meridian.generator import generate
from meridian.quality import evaluate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='config/m2.yaml')
    parser.add_argument('--output', required=True, help='Public Parquet root')
    parser.add_argument('--private-output', required=True, help='Restricted truth/report root')
    parser.add_argument('--generate', action='store_true', help='Generate before evaluating; requires empty output trees')
    parser.add_argument('--require-m2', action='store_true', help='Exit nonzero unless all M2 acceptance checks pass')
    args = parser.parse_args()
    cfg = Config.parse(config_load(args.config))
    if args.generate:
        generate(cfg, args.output, private_output=args.private_output,
                 progress=lambda n, total: print(f'{n:,}/{total:,}', flush=True))
    report = evaluate(cfg, args.output, args.private_output, require_m2=args.require_m2)
    print(f"Reconciled {cfg.rows:,} rows; M2 acceptance {'passed' if report['passed'] else 'not met'}. Detailed report is in the private output directory.")


if __name__ == '__main__': main()
