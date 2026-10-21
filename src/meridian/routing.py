"""Routing histories with matched holdbacks and a routine unrelated deployment."""
from datetime import timedelta
from decimal import Decimal
from .scenario import phases
from .randomness import uniform


def add_history(cfg, add):
    s = cfg.scenario
    start, canary_end, end = phases(cfg)

    def rule(p, key, version, previous, left, right, share):
        left, right = max(cfg.start, left), min(cfg.end, right)
        if left >= right:
            return
        add('routing_rules', routing_rule_id=key, route_family_id=p,
            configuration_version=version, rule_priority=10, processor_id=p,
            fallback_processor_id=(p+1) % cfg.processors, channel='ALL', card_network='ALL',
            cross_border_required=False, currency_conversion_required=False,
            merchant_category_code='ALL', merchant_country='ALL', effective_from_ts=left,
            effective_to_ts=right, rollout_percentage=Decimal(str(share*100)).quantize(Decimal('.01')),
            routing_strategy='weighted', change_ticket_id=f'CHG-{key}', is_enabled=True)
        add('deployment_events', deployment_id=key, change_ticket_id=f'CHG-{key}',
            component_name='payment_router', component_type='configuration', processor_id=p,
            routing_rule_id=key, previous_version=previous, deployed_version=version,
            environment='production', started_ts=left-timedelta(minutes=5), completed_ts=left,
            deployment_status='complete', rollout_percentage=Decimal(str(share*100)).quantize(Decimal('.01')),
            rollback_ts=None, release_notes='Scheduled routing configuration update')

    for p in range(cfg.processors):
        if p != s.faulty_processor_id:
            routine = cfg.start + timedelta(days=14)
            rule(p, 100+p, '4.10', '4.09', cfg.start, routine, 1)
            rule(p, 500+p, '4.11', '4.10', routine, cfg.end, 1)
        else:
            rule(p, 100+p, '4.11', '4.10', cfg.start, start, 1)
            rule(p, 2000+p, s.faulty_configuration_version, '4.11', start, canary_end, s.canary_rollout)
            rule(p, 300+p, '4.11', '4.11', start, canary_end, 1-s.canary_rollout)
            rule(p, 1000+p, s.faulty_configuration_version, s.faulty_configuration_version, canary_end, end, s.steady_rollout)
            rule(p, 400+p, '4.11', '4.11', canary_end, end, 1-s.steady_rollout)
            rule(p, 3000+p, '4.13', s.faulty_configuration_version, end, cfg.end, 1)


def choose_rule(cfg, rules, processor, ts, key):
    candidates = [r for r in rules[processor] if r['effective_from_ts'] <= ts < r['effective_to_ts']]
    draw = uniform(cfg.seed, key, 'rollout')*100
    cumulative = 0
    for rule in candidates:
        cumulative += float(rule['rollout_percentage'])
        if draw < cumulative:
            return rule['routing_rule_id']
    raise ValueError('Routing histories do not cover transaction time with 100% allocation')
