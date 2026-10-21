"""Public-data checks used before local output is marked complete."""
from .transactions import cents


class Validator:
    def __init__(self, cfg, dimensions, run_id):
        self.cfg, self.run_id = cfg, run_id
        self.maps = {name: {r[key]: r for r in dimensions[name]} for name, key in [
            ('customers', 'customer_id'), ('merchants', 'merchant_id'), ('payment_terminals', 'terminal_id'),
            ('payment_processors', 'processor_id'), ('routing_rules', 'routing_rule_id'),
            ('transaction_response_codes', 'response_code'), ('deployment_events', 'routing_rule_id')]}

    def validate(self, rows):
        def require(condition, message):
            if not condition: raise ValueError(message)
        for t in rows:
            require(t['generation_run_id'] == self.run_id, 'run mismatch')
            require(self.cfg.start <= t['transaction_ts'] < self.cfg.end, 'timestamp outside dataset')
            linked = {}
            for name, key in [('customers', 'customer_id'), ('merchants', 'merchant_id'), ('payment_terminals', 'terminal_id'), ('payment_processors', 'processor_id'), ('routing_rules', 'routing_rule_id'), ('transaction_response_codes', 'response_code'), ('deployment_events', 'routing_rule_id')]:
                require(t[key] in self.maps[name], f'orphan {key}')
                linked[name] = self.maps[name][t[key]]
                require(linked[name]['generation_run_id'] == self.run_id, 'dimension run mismatch')
            m, c, terminal, rule, deployment = [linked[n] for n in ('merchants', 'customers', 'payment_terminals', 'routing_rules', 'deployment_events')]
            require(terminal['merchant_id'] == t['merchant_id'] and terminal['terminal_channel'] == t['channel'], 'terminal ownership/channel mismatch')
            require(t['transaction_country'] == m['merchant_country'] and t['issuing_country'] == c['home_country'], 'country mismatch')
            require(t['cross_border'] == (t['transaction_country'] != t['issuing_country']), 'cross_border mismatch')
            require(t['original_currency'] == m['merchant_currency'] and t['amount_usd'] == cents(t['amount_original'] * t['fx_rate_to_usd']), 'currency/amount mismatch')
            require(t['currency_conversion_required'] == (t['original_currency'] != 'USD'), 'FX flag mismatch')
            require(rule['processor_id'] == t['processor_id'] and rule['effective_from_ts'] <= t['transaction_ts'] < rule['effective_to_ts'], 'routing effective window mismatch')
            require(deployment['completed_ts'] <= t['transaction_ts'] and deployment['deployed_version'] == rule['configuration_version'] and deployment['change_ticket_id'] == rule['change_ticket_id'], 'deployment mismatch')
            require(all(t['transaction_ts'] >= value for value in (m['onboarded_ts'], c['onboarding_ts'], terminal['installed_ts'])), 'entity effective date mismatch')
            require(t['approved'] == linked['transaction_response_codes']['is_approved'], 'response code mismatch')


def validate_public_output(cfg, root, run_id, should_cancel=None):
    """Stream public-only validation; never read private truth or derive exposure."""
    import json
    from pathlib import Path
    import pyarrow.parquet as pq
    from .schema_contract import contracts
    from .generator import GenerationCancelled
    root = Path(root)
    schemas = contracts()
    dimensions = {}
    for name,schema in schemas.items():
        if name in ('payment_transactions','scenario_ground_truth','generation_batch_audit'): continue
        table = pq.read_table(root/'dimensions'/f'{name}.parquet')
        if table.schema != schema: raise ValueError('Public schema mismatch')
        dimensions[name] = table.to_pylist()
    for name,key in [('customers','customer_id'),('merchants','merchant_id'),('payment_processors','processor_id'),
                     ('payment_terminals','terminal_id'),('routing_rules','routing_rule_id'),
                     ('deployment_events','deployment_id'),('processor_incidents','processor_incident_id'),
                     ('transaction_response_codes','response_code')]:
        records = dimensions[name]
        if len({r[key] for r in records}) != len(records): raise ValueError('Duplicate dimension key')
        if any(r['generation_run_id'] != run_id for r in records): raise ValueError('Dimension run mismatch')
    for name,expected in [('customers',cfg.customers),('merchants',cfg.merchants),('payment_processors',cfg.processors),('payment_terminals',cfg.terminals)]:
        if len(dimensions[name]) != expected: raise ValueError('Dimension count mismatch')
    validator = Validator(cfg,dimensions,run_id)
    count = 0
    for path in sorted((root/'payment_transactions').glob('*.parquet')):
        data = pq.ParquetFile(path)
        if data.schema_arrow != schemas['payment_transactions']: raise ValueError('Fact schema mismatch')
        for batch in data.iter_batches(batch_size=8192):
            if should_cancel and should_cancel(): raise GenerationCancelled()
            rows = batch.to_pylist()
            validator.validate(rows)
            for row in rows:
                count += 1
                if row['transaction_id'] != count: raise ValueError('Duplicate or missing transaction ID')
    run, = dimensions['generation_runs']
    manifest = json.loads((root/'public_manifest.json').read_text())
    if count != cfg.rows or run['generated_transaction_count'] != count or run['generation_run_id'] != run_id:
        raise ValueError('Run row count/identity mismatch')
    if run['configuration_hash'] != cfg.content_hash or manifest['generation_run_id'] != run_id:
        raise ValueError('Configuration/manifest identity mismatch')
    if run['run_status'] != 'complete' or not manifest['complete'] or manifest['rows_generated'] != count:
        raise ValueError('Incomplete public dataset')
    return dict(passed=True,rows=count,checks=['schema','dimension_counts','unique_ids','referential_integrity','effective_dates','amounts','completion'])
