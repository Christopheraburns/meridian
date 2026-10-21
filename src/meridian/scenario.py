"""Shared calendar/rollout model and deterministic exposure calibration."""
from datetime import datetime, timedelta


def day_weight(day):
    if (day.month, day.day) in ((1, 1), (12, 25)):
        return 5
    return 7 if day.weekday() >= 5 else 10


def phases(cfg):
    s = cfg.scenario
    start = datetime.fromisoformat(str(s.incident_start))
    end = datetime.fromisoformat(str(s.incident_end_exclusive))
    canary_end = min(end, start + timedelta(days=s.canary_days))
    return start, canary_end, end


def calibration(cfg):
    """Expected impact from explicit candidate traffic alone; organic exposure adds more.

    Integrate the same weighted days and half-open phase windows used by generation.
    The fraud mixture has mean baseline probability equal to baseline_decline_rate.
    """
    s = cfg.scenario
    start, canary_end, end = phases(cfg)
    total_weight = exposure_weight = 0.0
    for day in range(cfg.days):
        a = cfg.start + timedelta(days=day)
        b = a + timedelta(days=1)
        weight = day_weight(a)
        total_weight += weight
        for left, right, rollout in [(start, canary_end, s.canary_rollout), (canary_end, end, s.steady_rollout)]:
            seconds = max(0, (min(b, right)-max(a, left)).total_seconds())
            exposure_weight += weight * seconds/86400 * rollout
    delta = (.9 * max(0, s.affected_decline_rate - min(1, .6*s.baseline_decline_rate))
             + .1 * max(0, s.affected_decline_rate - min(1, 4.6*s.baseline_decline_rate)))
    coefficient = 100 * exposure_weight/total_weight * s.target_processor_affinity * delta
    return dict(candidate_impact_floor_pp=coefficient*s.target_candidate_share,
                recommended_candidate_share=(s.target_bankwide_decline_rate_increase_pp*1.25/coefficient if coefficient else None),
                candidate_exposure_fraction=s.target_candidate_share*s.target_processor_affinity*exposure_weight/total_weight,
                target_increase_pp=s.target_bankwide_decline_rate_increase_pp,
                impact_definition='realized incremental declines / all dataset attempts, in percentage points',
                safety_margin=1.25)
