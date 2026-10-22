"""Produce a repeatable candidate-traffic calibration with a 25% expectation margin."""
import argparse
import copy
import math
from dataclasses import asdict, replace
from pathlib import Path
import sys
import yaml
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
from meridian.config import Config
from meridian.scenario import calibration


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='config/m2.yaml')
    parser.add_argument('--output', required=True, help='New operator-only YAML file (not public data output)')
    args = parser.parse_args()
    raw = yaml.safe_load(Path(args.config).read_text())
    relaxed = copy.deepcopy(raw)
    settings = relaxed.setdefault('scenario' if 'scenario' in raw or 'incident' not in raw else 'incident', {})
    target_rate = settings.get('target_bankwide_decline_rate_increase_pp', .5)
    if type(target_rate) not in (int, float) or not math.isfinite(target_rate) or target_rate <= 0:
        raise ValueError('Calibration requires a positive finite impact target')
    settings['target_bankwide_decline_rate_increase_pp'] = 0
    cfg = Config.parse(relaxed)
    cfg = replace(cfg, scenario=replace(cfg.scenario, target_bankwide_decline_rate_increase_pp=target_rate))
    share = calibration(cfg)['recommended_candidate_share']
    if share is None or not 0 < share < 1:
        raise ValueError('Target cannot be calibrated with these dates/rates/rollout; change scenario settings')
    # Round up, preserving the expected safety margin.
    share = math.ceil(share*10000)/10000
    if 'scenario' not in raw:
        raw['scenario'] = asdict(cfg.scenario)
        raw.pop('incident', None)
    raw['scenario']['target_candidate_share'] = share
    Config.parse(raw)
    target = Path(args.output)
    # Exclusive creation prevents accidental replacement of operator settings.
    with target.open('x') as stream:
        target.chmod(0o600)
        yaml.safe_dump(raw, stream, sort_keys=False)
    print(f'Wrote calibrated configuration to {target}; run the private evaluator to verify realized impact.')


if __name__ == '__main__': main()
