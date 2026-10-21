"""Private operator/evaluator CLI; never exposed through the web control plane."""
import argparse
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
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
