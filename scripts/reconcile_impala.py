"""Reconcile a warehouse-loaded run with the private M2 evaluator, group by group.

The private evaluator (validate_scenario.py) counts attempts, declines and latency
for each comparison group while reading the local Parquet. This script emits the
same group definitions as one read-only Impala query, built from the scenario
config, then compares the query's output with the private report. Every count
must match exactly, so the loaded tables are proven to carry the planted signal.

The SQL names the target predicate. Run it with an operator identity and keep
its output with the private report, never where investigators or agents can read it.

    python3 scripts/reconcile_impala.py --config config/demo.yaml \
        --private-output data/demo-private --emit-sql > /tmp/reconcile.sql
    impala-shell -B --output_delimiter=, -f /tmp/reconcile.sql > data/demo-private/impala_groups.csv
    python3 scripts/reconcile_impala.py --config config/demo.yaml \
        --private-output data/demo-private --results data/demo-private/impala_groups.csv
"""
import argparse
import csv
import json
import math
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
from meridian.scenario import phases


def group_sql(cfg, run_id, database):
    """Mirror quality.Quality.consume: one labelled SELECT per group family."""
    s = cfg.scenario
    start, canary_end, end = phases(cfg)
    cross = ' AND t.cross_border' if s.cross_border_required else ''
    base = f"""
  SELECT t.transaction_ts AS ts, t.approved, t.authorization_latency_ms AS latency, t.processor_id,
         t.fraud_score > 90 AS fraud_high, r.configuration_version AS version,
         CASE WHEN t.transaction_ts < '{start}' THEN 'pre' WHEN t.transaction_ts < '{end}' THEN 'incident' ELSE 'recovery' END AS phase,
         (t.card_network = '{s.targeted_network}' AND t.channel = '{s.targeted_channel}'
          AND m.merchant_category_code = '{s.targeted_mcc}'{cross}) AS target,
         t.processor_id = {s.faulty_processor_id} AS faulty,
         t.processor_id = n.processor_id AS noise_processor,
         t.transaction_ts >= n.started_ts AND t.transaction_ts < n.resolved_ts AS noise_window
  FROM {database}.payment_transactions t
  JOIN {database}.merchants m ON m.merchant_id = t.merchant_id AND m.generation_run_id = t.generation_run_id
  JOIN {database}.routing_rules r ON r.routing_rule_id = t.routing_rule_id AND r.generation_run_id = t.generation_run_id
  CROSS JOIN (SELECT processor_id, started_ts, resolved_ts FROM {database}.processor_incidents
              WHERE generation_run_id = '{run_id}' ORDER BY processor_incident_id LIMIT 1) n
  WHERE t.generation_run_id = '{run_id}'"""
    v = s.faulty_configuration_version
    labelled = [
        "'all'",
        "concat('bank_', phase)",
        "CASE WHEN fraud_high THEN 'fraud_high' ELSE 'fraud_low' END",
        "CASE WHEN target THEN concat('target_', CASE WHEN faulty THEN 'faulty' ELSE 'other' END, '_', phase) "
        "WHEN faulty THEN concat('unaffected_faulty_', phase) END",
        f"CASE WHEN target AND faulty AND phase = 'incident' THEN CASE WHEN version = '{v}' THEN 'exposed' ELSE 'holdback' END END",
        f"CASE WHEN target AND faulty AND phase = 'incident' THEN CASE WHEN ts < '{canary_end}' THEN 'canary' ELSE 'steady' END END",
        f"CASE WHEN version = '{v}' THEN CASE WHEN target THEN 'new_version_target' ELSE 'new_version_unaffected' END END",
        "CASE WHEN noise_processor THEN CASE WHEN noise_window THEN 'secondary_window' ELSE 'secondary_control' END END",
    ]
    union = '\n  UNION ALL\n'.join(f"  SELECT {label} AS grp, approved, latency FROM base" for label in labelled)
    return (f"WITH base AS ({base}\n)\nSELECT grp, COUNT(*) AS attempts, SUM(CASE WHEN approved THEN 0 ELSE 1 END) AS declined,\n"
            f"       SUM(CAST(latency AS BIGINT)) AS latency_sum\nFROM (\n{union}\n) g\nWHERE grp IS NOT NULL\nGROUP BY grp\nORDER BY grp;\n")


def compare(report, rows):
    """Return (name, ok, detail) per group; counts exact, latency to the evaluator's float precision."""
    found = {r[0]: (int(r[1]), int(r[2]), int(r[3])) for r in rows if r}
    results = []
    for name, want in sorted(report['groups'].items()):
        got = found.pop(name, (0, 0, 0))
        latency_ok = math.isclose(got[2], want['mean_latency_ms']*want['attempts'], rel_tol=1e-9, abs_tol=0.5)
        ok = got[0] == want['attempts'] and got[1] == want['declined'] and latency_ok
        results.append((name, ok, f"attempts {got[0]:,}/{want['attempts']:,} declined {got[1]:,}/{want['declined']:,}"))
    results += [(name, False, 'group not produced by the evaluator') for name in sorted(found)]
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--config', required=True)
    parser.add_argument('--private-output', required=True, help='Directory holding scenario_quality.json')
    parser.add_argument('--database', default='meridian_financial')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--emit-sql', action='store_true', help='Print the reconciliation query')
    mode.add_argument('--results', help='impala-shell -B --output_delimiter=, output of that query')
    args = parser.parse_args()
    cfg = Config.parse(config_load(args.config))
    private = Path(args.private_output)
    report = json.loads((private/'scenario_quality.json').read_text())
    if not report.get('passed'):
        raise SystemExit('The private evaluator report did not pass; reconcile only an accepted run')
    if args.emit_sql:
        print(group_sql(cfg, report['generation_run_id'], args.database), end='')
        return 0
    with open(args.results, newline='') as stream:
        results = compare(report, list(csv.reader(stream)))
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name:28s} {detail}")
    passed = all(ok for _, ok, _ in results)
    summary = dict(generation_run_id=report['generation_run_id'], database=args.database, passed=passed,
                   groups={name: dict(ok=ok, detail=detail) for name, ok, detail in results})
    target = private/'impala_reconciliation.json'
    target.write_text(json.dumps(summary, indent=2))
    target.chmod(0o600)
    print(f"{'PASS' if passed else 'FAIL'}: {sum(ok for _, ok, _ in results)}/{len(results)} groups match; wrote {target}")
    return 0 if passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
