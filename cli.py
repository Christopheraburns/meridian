"""Local generation command; existing v0.1 command-line options remain valid."""
import sys
from pathlib import Path
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
import argparse
from meridian.generator import config_load, generate


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--config', default='config/default.yaml')
    p.add_argument('--output')
    p.add_argument('--private-output')
    p.add_argument('--rows', type=int)
    p.add_argument('--batch-size', type=int)
    p.add_argument('--run-id', help='Stable logical run identity for reproducibility checks')
    p.add_argument('--max-batches', type=int, help='Development-only partial run, never marked complete')
    a = p.parse_args()
    cfg = config_load(a.config)
    scale = cfg.get('scale', cfg)
    if a.rows is not None: scale['transaction_rows' if 'scale' in cfg else 'rows'] = a.rows
    if a.batch_size is not None: scale['batch_size'] = a.batch_size
    stats = generate(cfg, a.output, progress=lambda n, total: print(f'{n:,}/{total:,} ({n/total:.1%})', flush=True), max_batches=a.max_batches, run_id=a.run_id, private_output=a.private_output)
    print(stats)


if __name__ == '__main__': main()
