"""Warehouse reconciliation: SQL mirrors the evaluator groups; comparison is exact."""
import importlib.util
from pathlib import Path
from meridian.config import Config, config_load

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('reconcile_impala', ROOT/'scripts/reconcile_impala.py')
reconcile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reconcile)


def test_sql_uses_config_and_run():
    cfg = Config.parse(config_load(ROOT/'config/demo.yaml'))
    sql = reconcile.group_sql(cfg, 'run-1', 'db')
    assert "generation_run_id = 'run-1'" in sql and 'db.payment_transactions' in sql
    assert "'2026-09-11 00:00:00'" in sql and "'2026-10-03 00:00:00'" in sql
    assert sql.count('UNION ALL') == 7 and 't.cross_border' in sql


def test_compare_is_exact():
    report = {'groups': {'all': dict(attempts=10, declined=2, mean_latency_ms=150.0),
                         'exposed': dict(attempts=4, declined=1, mean_latency_ms=100.25)}}
    ok = reconcile.compare(report, [['all', '10', '2', '1500'], ['exposed', '4', '1', '401']])
    assert all(passed for _, passed, _ in ok)
    off_by_one = reconcile.compare(report, [['all', '10', '3', '1500'], ['exposed', '4', '1', '401']])
    assert [name for name, passed, _ in off_by_one if not passed] == ['all']
    extra = reconcile.compare(report, [['all', '10', '2', '1500'], ['exposed', '4', '1', '401'], ['bogus', '1', '0', '0']])
    assert ('bogus', False, 'group not produced by the evaluator') in extra
