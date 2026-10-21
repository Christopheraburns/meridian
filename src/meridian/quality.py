"""Private evaluator for scenario quality, reading persisted Parquet in bounded batches.

Never return this report from an investigator/UI endpoint: it names the target
predicate and reconstructs counterfactuals with the private seed/configuration.
"""
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import pyarrow.parquet as pq
from .config import Config
from .incident import outcomes, baseline_probability
from .randomness import uniform
from .schema_contract import contracts
from .scenario import calibration, phases
from .validation import Validator
from .writers.parquet import write_json


@dataclass
class Counts:
    n: int = 0
    declined: int = 0
    latency: int = 0

    def add(self, row):
        self.n += 1
        self.declined += not row['approved']
        self.latency += row['authorization_latency_ms']

    def summary(self):
        rate = self.declined/self.n if self.n else 0
        return dict(attempts=self.n, declined=self.declined, decline_rate=rate,
                    mean_latency_ms=self.latency/self.n if self.n else 0)


def contrast(a, b):
    """Unpooled large-sample difference interval; used only with adequate counts."""
    p = a.declined/a.n if a.n else 0
    q = b.declined/b.n if b.n else 0
    se = math.sqrt(p*(1-p)/max(1,a.n) + q*(1-q)/max(1,b.n))
    return dict(difference_pp=100*(p-q), lower_95_pp=100*(p-q-1.96*se),
                upper_95_pp=100*(p-q+1.96*se), standard_error_pp=100*se)


class Quality:
    def __init__(self, cfg, dimensions):
        self.cfg = cfg
        self.merchants = {r['merchant_id']: r for r in dimensions['merchants']}
        self.rules = {r['routing_rule_id']: r for r in dimensions['routing_rules']}
        self.noise = dimensions['processor_incidents'][0]
        self.groups = defaultdict(Counts)
        self.daily = defaultdict(int)
        self.exposed = self.excess = self.baseline_declined = self.secondary_excess = 0
        self.affected_amount = Decimal('0.00')
        self.expected_excess = 0.0
        self.start, self.canary_end, self.end = phases(cfg)

    def consume(self, row):
        s = self.cfg.scenario
        ts = row['transaction_ts']
        merchant = self.merchants[row['merchant_id']]
        phase = 'pre' if ts < self.start else ('incident' if ts < self.end else 'recovery')
        target = (row['card_network'] == s.targeted_network and row['channel'] == s.targeted_channel
                  and merchant['merchant_category_code'] == s.targeted_mcc
                  and (row['cross_border'] or not s.cross_border_required))
        faulty = row['processor_id'] == s.faulty_processor_id
        version = self.rules[row['routing_rule_id']]['configuration_version']
        groups = ['all', f'bank_{phase}', 'fraud_high' if row['fraud_score'] > 90 else 'fraud_low']
        if target:
            groups.append(f'target_{"faulty" if faulty else "other"}_{phase}')
            if faulty and phase == 'incident':
                groups.append('exposed' if version == s.faulty_configuration_version else 'holdback')
                groups.append('canary' if ts < self.canary_end else 'steady')
        elif faulty:
            groups.append(f'unaffected_faulty_{phase}')
        if version == s.faulty_configuration_version:
            groups.append('new_version_target' if target else 'new_version_unaffected')
        noisy = (row['processor_id'] == self.noise['processor_id']
                 and self.noise['started_ts'] <= ts < self.noise['resolved_ts'])
        if row['processor_id'] == self.noise['processor_id']:
            groups.append('secondary_window' if noisy else 'secondary_control')
        for name in groups:
            self.groups[name].add(row)
        self.daily[ts.date().isoformat()] += 1
        exposed, declined, incremental = outcomes(self.cfg, row, merchant, noisy)
        if row['approved'] == declined:
            raise ValueError('Persisted outcome differs from deterministic counterfactual reconstruction')
        u = uniform(self.cfg.seed, row['transaction_id'], 'approval')
        baseline = baseline_probability(self.cfg, row, noisy)
        self.baseline_declined += u < baseline
        self.secondary_excess += noisy and baseline_probability(self.cfg, row, False) <= u < baseline
        self.exposed += exposed
        self.excess += incremental
        if incremental:
            self.affected_amount += row['amount_usd']
        if exposed:
            self.expected_excess += max(0, s.affected_decline_rate-baseline)
        return exposed, incremental

    def report(self):
        g = self.groups
        bank = contrast(g['bank_incident'], g['bank_pre'])
        target = contrast(g['exposed'], g['holdback'])
        treated = contrast(g['target_faulty_incident'], g['target_faulty_pre'])
        control = contrast(g['target_other_incident'], g['target_other_pre'])
        did = treated['difference_pp']-control['difference_pp']
        did_se = math.hypot(treated['standard_error_pp'], control['standard_error_pp'])
        n = g['all'].n
        drift = contrast(g['unaffected_faulty_incident'], g['unaffected_faulty_pre'])
        secondary = contrast(g['secondary_window'], g['secondary_control'])
        recovery = contrast(g['target_faulty_recovery'], g['target_faulty_pre'])
        required = ['exposed', 'holdback', 'target_faulty_pre', 'target_other_pre', 'target_other_incident', 'unaffected_faulty_pre', 'unaffected_faulty_incident']
        checks = dict(
            million_rows=n == self.cfg.rows and n >= 1000000,
            adequate_controls=all(g[k].n >= 1000 for k in required),
            bankwide_observable=g['bank_pre'].n >= 1000 and g['bank_incident'].n >= 1000 and bank['lower_95_pp'] > 0,
            realized_impact_target=100*self.excess/max(1,n) >= self.cfg.scenario.target_bankwide_decline_rate_increase_pp,
            target_strong=target['lower_95_pp'] >= 10,
            difference_in_differences=did-1.96*did_se > 5,
            unaffected_stable=abs(drift['difference_pp']) < 1,
            other_processor_control_stable=abs(control['difference_pp']) < 1,
            recovery_observed=g['target_faulty_recovery'].n >= 1000 and abs(recovery['difference_pp']) < 1,
            secondary_visible=g['secondary_window'].n >= 100 and secondary['lower_95_pp'] > 0 and g['secondary_window'].summary()['mean_latency_ms'] > g['secondary_control'].summary()['mean_latency_ms']+400,
            secondary_smaller=0 < self.secondary_excess < self.excess/10,
            fraud_visible=g['fraud_high'].n >= 1000 and contrast(g['fraud_high'],g['fraud_low'])['lower_95_pp'] > 5,
            rule_not_label=g['new_version_unaffected'].n >= 1000 and 0 < g['new_version_target'].declined < g['new_version_target'].n,
            counterfactual_identity=g['all'].declined == self.baseline_declined+self.excess)
        return dict(evaluator_version='m2-v1', passed=all(checks.values()), checks=checks,
                    calibration=calibration(self.cfg), groups={k:v.summary() for k,v in sorted(g.items())},
                    comparisons=dict(bank_incident_minus_pre=bank, exposed_minus_holdback=target,
                                     unaffected_incident_minus_pre=drift, other_processor_incident_minus_pre=control,
                                     recovery_minus_pre=recovery, secondary_window_minus_control=secondary,
                                     difference_in_differences=dict(difference_pp=did, lower_95_pp=did-1.96*did_se, upper_95_pp=did+1.96*did_se)),
                    counterfactual=dict(exposed=self.exposed, baseline_declines=self.baseline_declined,
                                        incremental_declines=self.excess, incremental_payment_volume_usd=str(self.affected_amount),
                                        expected_value_excess=self.expected_excess, secondary_incremental_declines=self.secondary_excess,
                                        bankwide_increase_pp=100*self.excess/max(1,n)),
                    daily_counts=dict(sorted(self.daily.items())))


def _evaluate(cfg, public_root, private_root, require_m2=False):
    """Read back every persisted fact; report lives only in the existing private tree."""
    cfg = Config.parse(cfg)
    public, private = Path(public_root).resolve(), Path(private_root).resolve()
    if public == private or public in private.parents or private in public.parents:
        raise ValueError('Evaluator requires separate public/private trees')
    if not private.is_dir() or private.stat().st_mode & 0o077:
        raise ValueError('Private evaluator directory must exist with mode 0700')
    schemas = contracts()
    dimensions = {}
    for name in schemas:
        if name in ('payment_transactions', 'scenario_ground_truth', 'generation_batch_audit'):
            continue
        data = pq.read_table(public/'dimensions'/f'{name}.parquet')
        if data.schema != schemas[name]: raise ValueError(f'Schema mismatch: {name}')
        dimensions[name] = data.to_pylist()
    run, = dimensions['generation_runs']
    if run['configuration_hash'] != cfg.content_hash or run['run_status'] != 'complete':
        raise ValueError('Configuration mismatch or incomplete run')
    run_id = run['generation_run_id']
    validator = Validator(cfg, dimensions, run_id)
    quality = Quality(cfg, dimensions)
    truth_table = pq.read_table(private/'scenario_ground_truth.parquet')
    audit_table = pq.read_table(private/'generation_batch_audit.parquet')
    if truth_table.schema != schemas['scenario_ground_truth'] or audit_table.schema != schemas['generation_batch_audit']:
        raise ValueError('Private schema mismatch')
    truth, = truth_table.to_pylist()
    audits = audit_table.to_pylist()
    paths = sorted((public/'payment_transactions').glob('*.parquet'))
    if len(paths) != len(audits): raise ValueError('Audit/file count mismatch')
    next_id = 1
    for batch_id, (path, audit) in enumerate(zip(paths, audits)):
        parquet = pq.ParquetFile(path)
        if parquet.schema_arrow != schemas['payment_transactions']: raise ValueError('Fact schema mismatch')
        digest = hashlib.sha256(); digest.update(b'[')
        count = exposed = excess = 0
        for batch in parquet.iter_batches(batch_size=8192):
            rows = batch.to_pylist()
            validator.validate(rows)
            for row in rows:
                if row['transaction_id'] != next_id: raise ValueError('Duplicate, missing or reordered transaction ID')
                next_id += 1
                if count: digest.update(b',')
                digest.update(json.dumps(row, sort_keys=True, default=str, separators=(',', ':')).encode())
                count += 1
                e, x = quality.consume(row)
                exposed += e; excess += x
        digest.update(b']')
        if (audit['generation_run_id'], audit['batch_id'], audit['rows_written'], audit['exposed_transactions'], audit['injected_declines'], audit['batch_hash'], audit['write_status']) != (run_id, batch_id, count, exposed, excess, digest.hexdigest(), 'written'):
            raise ValueError('Batch audit does not reconcile with persisted facts')
    if next_id-1 != cfg.rows or run['generated_transaction_count'] != cfg.rows:
        raise ValueError('Transaction count mismatch')
    if (truth['generation_run_id'], truth['expected_exposed_transactions'], truth['expected_excess_declines'], truth['expected_affected_amount_usd']) != (run_id, quality.exposed, quality.excess, quality.affected_amount):
        raise ValueError('Private truth does not reconcile with persisted facts')
    notes = json.loads(truth['validation_notes'])
    if not math.isclose(notes['expected_value_excess'], quality.expected_excess, rel_tol=1e-10, abs_tol=1e-8):
        raise ValueError('Private expected-value estimate does not reconcile')
    report = quality.report()
    report.update(generation_run_id=run_id, configuration_hash=cfg.content_hash, reconciled=True)
    write_json(private/'scenario_quality.json', report)
    if require_m2 and not report['passed']:
        raise ValueError('M2 scenario acceptance failed; inspect private scenario_quality.json')
    return report


def evaluate(cfg, public_root, private_root, require_m2=False):
    """Invalidate stale success before re-evaluation; keep failures private."""
    public, private = Path(public_root).resolve(), Path(private_root).resolve()
    if public == private or public in private.parents or private in public.parents:
        raise ValueError('Evaluator requires separate public/private trees')
    if not private.is_dir() or private.stat().st_mode & 0o077:
        raise ValueError('Private evaluator directory must exist with mode 0700')
    path = private/'scenario_quality.json'
    write_json(path, dict(passed=False, reconciled=False, state='evaluating'))
    try:
        return _evaluate(cfg, public, private, require_m2)
    except Exception as exc:
        if json.loads(path.read_text()).get('state') == 'evaluating':
            write_json(path, dict(passed=False, reconciled=False, state='failed', error=str(exc)))
        raise
