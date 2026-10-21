"""Coherent synthetic entities and routing histories."""
from datetime import timedelta
from .config import timestamp

COUNTRIES = ('US', 'CA', 'GB', 'MX', 'DE', 'FR', 'JP', 'AU')
CURRENCIES = ('USD', 'CAD', 'GBP', 'MXN', 'EUR', 'EUR', 'JPY', 'AUD')
MCCS = ('5411', '5812', '4722', '5732', '5999', '4111', '7011', '5541')
CATEGORIES = ('Grocery', 'Dining', 'Travel', 'Electronics', 'Retail', 'Transport', 'Hotels', 'Fuel')
CHANNELS = ('ecommerce', 'pos', 'mobile', 'recurring')


def build_dimensions(cfg, run_id):
    result = {}
    def add(name, **row):
        result.setdefault(name, []).append(dict(row, generation_run_id=run_id))
    for i in range(cfg.customers):
        add('customers', customer_id=i, customer_type='individual', customer_segment=('consumer', 'premium', 'small_business', 'enterprise')[i % 4], home_country=COUNTRIES[i % 8], home_region='NA' if i % 8 in (0, 1, 3) else 'International', onboarding_ts=cfg.start-timedelta(days=365+i % 1000), risk_tier='standard', kyc_status='verified', preferred_currency=CURRENCIES[i % 8], is_active=True)
    for i in range(cfg.merchants):
        add('merchants', merchant_id=i, merchant_name=f'Synthetic Merchant {i}', merchant_category_code=MCCS[i % 8], merchant_category=CATEGORIES[i % 8], industry_group=CATEGORIES[i % 8], merchant_country=COUNTRIES[(i // 8) % 8], merchant_region='NA' if (i // 8) % 8 in (0, 1, 3) else 'International', merchant_currency=CURRENCIES[(i // 8) % 8], acquirer_id=i % 5, merchant_size=('small', 'medium', 'large')[i % 3], onboarded_ts=cfg.start-timedelta(days=400), risk_tier='standard', is_active=True)
    for i in range(cfg.processors):
        add('payment_processors', processor_id=i, processor_name=f'Synthetic Processor {i}', processor_type='card', operating_region='Global', headquarters_country='US', settlement_currency='USD', service_tier='standard', is_active=True)
    for i in range(cfg.terminals):
        merchant = result['merchants'][i % cfg.merchants]
        # First terminal for every merchant supports ecommerce; additional terminals vary.
        channel = CHANNELS[(i // cfg.merchants) % 4]
        add('payment_terminals', terminal_id=i, merchant_id=merchant['merchant_id'], terminal_type='virtual' if channel != 'pos' else 'physical', terminal_channel=channel, terminal_country=merchant['merchant_country'], device_vendor='Synthetic Devices', device_model='T1', software_version='2.0', installed_ts=cfg.start-timedelta(days=100), is_active=True)
    from .routing import add_history
    add_history(cfg, add)
    s = cfg.scenario
    # Independent processor degradation overlaps the main incident, but is short-lived.
    noise_start = min(max(cfg.start, timestamp(s.incident_start)+timedelta(days=5)), cfg.end-timedelta(hours=12))
    add('processor_incidents', processor_incident_id=1, processor_id=(s.faulty_processor_id+1) % cfg.processors, incident_type='latency', severity='minor', affected_region='Global', affected_channel='ALL', started_ts=noise_start, resolved_ts=noise_start+timedelta(hours=12), reported_ts=noise_start+timedelta(minutes=5), status='resolved', customer_impact_summary='Temporary elevated authorization latency', ticket_id='OPS-1')
    for code, approved, category, retry in [('00', True, 'approved', False), ('05', False, 'issuer_decline', True), ('59', False, 'suspected_fraud', False), ('91', False, 'processor_unavailable', True)]:
        add('transaction_response_codes', response_code=code, response_category=category, response_description=category.replace('_', ' '), is_approved=approved, is_retryable=retry, decline_type=None if approved else category)
    return result
