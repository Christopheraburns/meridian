"""Compatibility entry point for the CLI and existing Flask application."""
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import json
import uuid
from .config import Config, config_load, timestamp
from .dimensions import build_dimensions
from .transactions import Transactions
from .validation import Validator
from .incident import baseline_probability
from .scenario import calibration
from .writers.parquet import write_json, write_table, content_hash


class GenerationCancelled(Exception):
    pass


def generate(cfg, output=None, progress=None, max_batches=None, run_id=None, private_output=None, should_cancel=None):
    cfg = Config.parse(cfg)
    if max_batches is not None and (type(max_batches) is not int or max_batches < 1): raise ValueError('max_batches must be positive')
    run_id = run_id or str(uuid.uuid4())
    root = Path(output or cfg.output_dir).resolve()
    # Private data is a sibling, never nested in the public output tree.
    private = Path(private_output).resolve() if private_output else root.with_name(root.name + '_private')
    if root == private or root in private.parents or private in root.parents:
        raise ValueError('Public and private output directories must be separate trees')
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()): raise ValueError('new_run requires an empty output directory; choose a fresh --output')
    private.mkdir(parents=True, exist_ok=True, mode=0o700)
    if any(private.iterdir()): raise ValueError('Private output must be empty for new_run')
    private.chmod(0o700)
    started = datetime.utcnow()
    manifest = dict(generation_run_id=run_id, rows_generated=0, requested_rows=cfg.rows, complete=False, format='parquet', dataset_path='payment_transactions', status='running')
    write_json(root/'public_manifest.json', manifest)
    total = excess = exposed = declined = 0
    expected_excess = 0.0
    affected_amount = Decimal('0.00')
    audits = []
    run = dict(generation_run_id=run_id, generator_version='0.2-m2', scenario_name=cfg.scenario.id, seed=cfg.seed, configuration_hash=cfg.content_hash, requested_transaction_count=cfg.rows, generated_transaction_count=0, generation_started_ts=started, generation_completed_ts=None, dataset_start_ts=cfg.start, dataset_end_ts=cfg.end, run_status='running', validation_status='pending', notes='Local Parquet; no Iceberg commit. Each row is one original authorization attempt; retries disabled.')
    def check_cancel():
        if should_cancel and should_cancel(): raise GenerationCancelled()

    try:
        check_cancel()
        dimensions = build_dimensions(cfg, run_id)
        source = Transactions(cfg, dimensions, run_id)
        validator = Validator(cfg, dimensions, run_id)
        for name, records in dimensions.items(): write_table(root/'dimensions'/f'{name}.parquet', name, records)
        for batch_id, offset in enumerate(range(0, cfg.rows, cfg.batch_size)):
            check_cancel()
            if max_batches is not None and batch_id >= max_batches: break
            rows, truth = source.batch(range(offset+1, min(offset+cfg.batch_size, cfg.rows)+1))
            validator.validate(rows)
            written = write_table(root/'payment_transactions'/f'part-{batch_id:06d}.parquet', 'payment_transactions', rows)
            n_exposed = sum(t[0] for t in truth); n_excess = sum(t[1] for t in truth)
            exposed += n_exposed; excess += n_excess
            for row, flags in zip(rows, truth):
                if flags[0]:
                    expected_excess += max(0, cfg.scenario.affected_decline_rate-baseline_probability(cfg, row))
            affected_amount += sum((r['amount_usd'] for r, t in zip(rows, truth) if t[1]), Decimal('0.00'))
            declined += sum(not r['approved'] for r in rows); total += written
            audits.append(dict(generation_run_id=run_id, batch_id=batch_id, rows_written=written, exposed_transactions=n_exposed, injected_declines=n_excess, batch_hash=content_hash(rows), written_ts=datetime.utcnow(), write_status='written'))
            manifest.update(rows_generated=total)
            write_json(root/'public_manifest.json', manifest)
            if progress: progress(total, cfg.rows)
        check_cancel()
        s = cfg.scenario
        injected_rules = [r['routing_rule_id'] for r in dimensions['routing_rules'] if r['configuration_version'] == s.faulty_configuration_version]
        representative_rule = 1000+s.faulty_processor_id if 1000+s.faulty_processor_id in injected_rules else (injected_rules[0] if injected_rules else None)
        truth_row = dict(generation_run_id=run_id, incident_id=s.id, incident_name='Payment routing configuration regression', incident_type='routing', incident_start_ts=timestamp(s.incident_start), incident_end_ts=timestamp(s.incident_end_exclusive), injected_processor_id=s.faulty_processor_id, injected_routing_rule_id=representative_rule, injected_configuration_version=s.faulty_configuration_version, predicate_json=json.dumps(dict(network=s.targeted_network, channel=s.targeted_channel, mcc=s.targeted_mcc, cross_border_required=s.cross_border_required)), expected_exposed_transactions=exposed, expected_excess_declines=excess, expected_affected_amount_usd=affected_amount, causal_explanation=f'Routing configuration {s.faulty_configuration_version} applies an overly restrictive issuer-country validation to the targeted card/network/channel/MCC combination. Canary and expanded rollout raise declines only on actual version exposure; holdbacks and recovery retain baseline outcomes.', validation_notes=json.dumps(dict(enabled=s.enabled, expected_value_excess=expected_excess, calibration=calibration(cfg), injected_routing_rule_ids=injected_rules, realized_excess=excess, realized_bankwide_increase_pp=100*excess/total if total else 0, target_increase_pp=s.target_bankwide_decline_rate_increase_pp, amount_definition='USD payment volume of incremental declines; not revenue or bank loss')))
        write_table(private/'scenario_ground_truth.parquet', 'scenario_ground_truth', [truth_row])
        write_table(private/'generation_batch_audit.parquet', 'generation_batch_audit', audits)
        complete = total == cfg.rows
        run.update(generated_transaction_count=total, generation_completed_ts=datetime.utcnow(), run_status='complete' if complete else 'partial', validation_status='passed' if complete else 'partial')
        write_table(root/'dimensions'/'generation_runs.parquet', 'generation_runs', [run])
        check_cancel()
        manifest.update(complete=complete, status=run['run_status'])
        write_json(root/'public_manifest.json', manifest)
    except Exception as exc:
        failure_status = 'cancelled' if isinstance(exc, GenerationCancelled) else 'failed'
        manifest.update(status=failure_status, complete=False)
        write_json(root/'public_manifest.json', manifest)
        run.update(generated_transaction_count=total, run_status=failure_status, validation_status='failed')
        write_table(root/'dimensions'/'generation_runs.parquet', 'generation_runs', [run])
        raise
    # Shared with legacy UI: never expose counterfactuals or cohort counts here.
    return dict(rows=total, declined=declined, generation_run_id=run_id, complete=complete)
