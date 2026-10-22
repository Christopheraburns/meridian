"""Launch as a separate Docker service or an approved Workbench Job."""
import argparse
from pathlib import Path
import signal
import sys
def _checkout():
    """Locate the Meridian checkout; Workbench Jobs run scripts as notebook cells without __file__."""
    import os
    if globals().get('__file__'): return Path(__file__).resolve().parents[0]
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
from meridian.jobs import Settings, Worker


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--once',action='store_true',help='Recover interrupted jobs, then process at most one queued task')
    args = parser.parse_args()
    worker = Worker(Settings.environment())
    def stop(signum, frame): worker.stopping = True
    signal.signal(signal.SIGTERM,stop)
    signal.signal(signal.SIGINT,stop)
    worker.run(once=args.once)


if __name__=='__main__': main()
