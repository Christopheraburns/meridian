"""Local generation command; existing v0.1 command-line options remain valid."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent / 'src'))
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
