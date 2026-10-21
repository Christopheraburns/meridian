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
