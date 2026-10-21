"""Potential outcomes stay in private computations, never public facts."""
from .config import timestamp
from .randomness import uniform


def outcomes(cfg, row, merchant, noisy):
    s = cfg.scenario
    exposed = (s.enabled and timestamp(s.incident_start) <= row['transaction_ts'] < timestamp(s.incident_end_exclusive)
               and row['processor_id'] == s.faulty_processor_id and row['routing_rule_id'] == 1000+s.faulty_processor_id
               and row['card_network'] == s.targeted_network and row['channel'] == s.targeted_channel
               and merchant['merchant_category_code'] == s.targeted_mcc
               and (row['cross_border'] or not s.cross_border_required))
    baseline = min(1, s.baseline_decline_rate + (.08 if noisy else 0))
    u = uniform(cfg.seed, row['transaction_id'], 'approval')
    baseline_declined = u < baseline
    declined = u < (max(baseline, s.affected_decline_rate) if exposed else baseline)
    return exposed, declined, declined and not baseline_declined
