"""Per-ID facts; dimension lookup determines dependent attributes."""
from datetime import timedelta
from decimal import Decimal as D, ROUND_HALF_UP
import math
from .randomness import uniform, integer, normal
from .incident import outcomes
from .routing import choose_rule
from .scenario import day_weight

FX = dict(USD='1', CAD='.74', GBP='1.31', MXN='.051', EUR='1.10', JPY='.0067', AUD='.67')


def cents(value): return D(value).quantize(D('.01'), rounding=ROUND_HALF_UP)


class Transactions:
    def __init__(self, cfg, dimensions, run_id):
        self.cfg, self.dims, self.run_id = cfg, dimensions, run_id
        self.terminals = {}
        for terminal in dimensions['payment_terminals']:
            self.terminals.setdefault(terminal['merchant_id'], []).append(terminal)
        self.target_merchants = [m['merchant_id'] for m in dimensions['merchants'] if m['merchant_category_code'] == cfg.scenario.targeted_mcc and any(t['terminal_channel'] == cfg.scenario.targeted_channel for t in self.terminals[m['merchant_id']])]
        if cfg.scenario.enabled and not self.target_merchants: raise ValueError('No terminals support the targeted MCC/channel')
        self.rules = {}
        for rule in dimensions['routing_rules']:
            self.rules.setdefault(rule['processor_id'], []).append(rule)
        self.days = []
        for day in range(cfg.days):
            dt = cfg.start+timedelta(days=day)
            weight = day_weight(dt)
            self.days.extend([day]*weight)

    def row(self, key):
        c, s = self.cfg, self.cfg.scenario
        candidate = uniform(c.seed, key, 'candidate') < s.target_candidate_share and bool(self.target_merchants)
        m = self.dims['merchants'][self.target_merchants[integer(c, key, 'target_merchant', len(self.target_merchants))] if candidate else integer(c, key, 'merchant', c.merchants)]
        terminals = self.terminals[m['merchant_id']]
        if candidate: terminals = [t for t in terminals if t['terminal_channel'] == s.targeted_channel]
        terminal = terminals[integer(c, key, 'terminal', len(terminals))]
        customer = self.dims['customers'][integer(c, key, 'customer', c.customers)]
        if candidate and s.cross_border_required and customer['home_country'] == m['merchant_country']:
            customer = self.dims['customers'][(customer['customer_id']+1) % c.customers]
            if customer['home_country'] == m['merchant_country']: customer = self.dims['customers'][1 if m['merchant_country'] == 'US' else 0]
        ts = c.start + timedelta(days=self.days[integer(c, key, 'day', len(self.days))], seconds=integer(c, key, 'second', 86400))
        processor = integer(c, key, 'processor', c.processors)
        if candidate:
            if uniform(c.seed, key, 'processor_affinity') < s.target_processor_affinity:
                processor = s.faulty_processor_id
            else:
                processor = (s.faulty_processor_id+1+integer(c, key, 'control_processor', c.processors-1)) % c.processors
        rule = choose_rule(c, self.rules, processor, ts, key)
        amount = cents(str(min(25000, math.exp(4.2+normal(c, key, 'amount')))))
        currency = m['merchant_currency']; fx = D(FX[currency]).quantize(D('.00000001'))
        noise = self.dims['processor_incidents'][0]
        noisy = processor == noise['processor_id'] and noise['started_ts'] <= ts < noise['resolved_ts']
        row = dict(transaction_id=key, transaction_ts=ts, customer_id=customer['customer_id'], merchant_id=m['merchant_id'], terminal_id=terminal['terminal_id'], processor_id=processor, routing_rule_id=rule, amount_original=amount, original_currency=currency, fx_rate_to_usd=fx, amount_usd=cents(amount*fx), card_network=s.targeted_network if candidate else ('Visa', 'Mastercard', 'Amex', 'Discover')[integer(c, key, 'network', 4)], card_product='credit', payment_method='card', channel=terminal['terminal_channel'], card_present=terminal['terminal_channel']=='pos', transaction_country=m['merchant_country'], issuing_country=customer['home_country'], cross_border=customer['home_country'] != m['merchant_country'], currency_conversion_required=currency != 'USD', authorization_latency_ms=50+integer(c, key, 'latency', 250)+(600 if noisy else 0), retry_attempt=0, original_transaction_id=None, fraud_score=cents(str(uniform(c.seed, key, 'fraud')*100)), generation_run_id=self.run_id)
        exposed, declined, incremental = outcomes(c, row, m, noisy)
        row.update(approved=not declined, response_code='00' if not declined else ('91' if noisy else ('59' if row['fraud_score'] > 90 else '05')), transaction_status='declined' if declined else 'approved')
        return row, (exposed, incremental)

    def batch(self, ids):
        pairs = [self.row(int(key)) for key in ids]
        return [p[0] for p in pairs], [p[1] for p in pairs]
