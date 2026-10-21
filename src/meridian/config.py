"""Validated configuration with a v0.1 YAML compatibility parser."""
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import hashlib
import json
import math
import yaml


def timestamp(value):
    result = datetime.fromisoformat(str(value))
    if result.tzinfo is not None:
        raise ValueError('Use timezone-naive UTC timestamps')
    return result


@dataclass(frozen=True)
class Scenario:
    id: str = 'payment_routing_v1'
    enabled: bool = True
    incident_start: str = '2026-10-09T00:00:00'
    incident_end_exclusive: str = '2026-10-31T00:00:00'
    faulty_processor_id: int = 3
    faulty_configuration_version: str = '4.12'
    targeted_network: str = 'Mastercard'
    targeted_channel: str = 'ecommerce'
    targeted_mcc: str = '4722'
    cross_border_required: bool = True
    baseline_decline_rate: float = 0.035
    affected_decline_rate: float = 0.24
    target_bankwide_decline_rate_increase_pp: float = 0.5
    target_candidate_share: float = 0.20
    target_processor_affinity: float = 0.70
    canary_days: int = 2
    canary_rollout: float = 0.20
    steady_rollout: float = 0.80


@dataclass(frozen=True)
class Config:
    name: str
    seed: int
    rows: int
    batch_size: int
    customers: int
    merchants: int
    terminals: int
    processors: int
    start_date: str
    days: int
    output_dir: str
    scenario: Scenario

    @property
    def start(self): return timestamp(self.start_date)

    @property
    def end(self): return self.start + timedelta(days=self.days)

    @property
    def content_hash(self):
        data = asdict(self)
        data['model_version'] = 'm2-v1'
        for key in ('batch_size', 'output_dir'): data.pop(key)
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    @classmethod
    def parse(cls, raw):
        if isinstance(raw, cls): raw = asdict(raw)
        raw = dict(raw)
        if str(raw.get('schema_version', '0.2')) != '0.2':
            raise ValueError('Unsupported schema_version')
        scale = raw.get('scale', raw)
        execution = raw.get('execution', {})
        if execution.get('engine', 'local_parquet') != 'local_parquet':
            raise ValueError('M4 blocker: Spark/Iceberg is not configured; use local_parquet')
        if execution.get('write_mode', 'new_run') != 'new_run':
            raise ValueError('Only new_run is supported')
        for key, expected in [('database', 'meridian_financial'), ('private_database', 'meridian_ground_truth')]:
            if execution.get(key, expected) != expected: raise ValueError(f'{key} must be {expected}')
        incident = raw.get('scenario')
        if incident is None:
            old = raw.get('incident', {})
            mapping = {'start_date': 'incident_start', 'processor_id': 'faulty_processor_id',
                       'card_network': 'targeted_network', 'channel': 'targeted_channel',
                       'mcc': 'targeted_mcc', 'cross_border': 'cross_border_required',
                       'injected_decline_rate': 'affected_decline_rate'}
            incident = {mapping.get(k, k): v for k, v in old.items() if k != 'end_date'}
            if 'end_date' in old:
                incident['incident_end_exclusive'] = (timestamp(old['end_date']) + timedelta(days=1)).isoformat()
        scenario = Scenario(**incident)
        if type(scenario.enabled) is not bool or type(scenario.cross_border_required) is not bool:
            raise ValueError('Scenario enabled/cross_border_required must be booleans')
        for key in ('baseline_decline_rate', 'affected_decline_rate', 'target_bankwide_decline_rate_increase_pp', 'target_candidate_share', 'target_processor_affinity', 'canary_rollout', 'steady_rollout'):
            value = getattr(scenario, key)
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f'{key} must be a finite number')
        for key in ('target_candidate_share', 'target_processor_affinity', 'canary_rollout', 'steady_rollout'):
            if not 0 < getattr(scenario, key) < 1:
                raise ValueError(f'{key} must be strictly between 0 and 1 to retain controls')
        if type(scenario.canary_days) is not int or scenario.canary_days < 1:
            raise ValueError('canary_days must be a positive integer')
        if scenario.canary_rollout >= scenario.steady_rollout:
            raise ValueError('canary_rollout must be below steady_rollout')
        if not isinstance(scenario.faulty_configuration_version, str) or scenario.faulty_configuration_version in ('4.10', '4.11', '4.13'):
            raise ValueError('Faulty configuration version must be a string distinct from baseline 4.11')
        cfg = cls(name=str(raw.get('name', 'meridian-payments')), seed=raw.get('seed', 20261020),
                  rows=scale.get('transaction_rows', scale.get('rows', 10000)),
                  batch_size=scale.get('batch_size', 1000), customers=scale.get('customers', 1000),
                  merchants=scale.get('merchants', 200), terminals=scale.get('terminals', scale.get('merchants', 200) * 2),
                  processors=scale.get('processors', 8), start_date=str(scale.get('start_date', '2026-09-01')),
                  days=scale.get('days', 60), output_dir=str(execution.get('output_dir', raw.get('output_dir', 'data'))), scenario=scenario)
        for key in ('rows', 'batch_size', 'customers', 'merchants', 'terminals', 'processors', 'days'):
            value = getattr(cfg, key)
            if type(value) is not int or value < 1: raise ValueError(f'{key} must be a positive integer')
        if cfg.processors > 100:
            raise ValueError('M2 supports at most 100 processors in its routing ID namespace')
        if type(cfg.seed) is not int or not 0 <= cfg.seed < 2**63: raise ValueError('seed must fit positive BIGINT')
        if cfg.terminals < cfg.merchants or cfg.customers < 2 or cfg.merchants < 8 or cfg.processors < 2:
            raise ValueError('Need >=2 customers/processors, >=8 merchants, terminals >= merchants')
        if type(scenario.faulty_processor_id) is not int or not 0 <= scenario.faulty_processor_id < cfg.processors:
            raise ValueError('faulty_processor_id outside processor range')
        if not 0 <= scenario.baseline_decline_rate <= scenario.affected_decline_rate <= 1:
            raise ValueError('Require 0 <= baseline <= affected decline rate <= 1')
        if scenario.targeted_network not in ('Visa', 'Mastercard', 'Amex', 'Discover') or scenario.targeted_channel not in ('pos', 'ecommerce', 'mobile', 'recurring') or scenario.targeted_mcc not in ('5411', '5812', '4722', '5732', '5999', '4111', '7011', '5541'):
            raise ValueError('Unsupported scenario network/channel/MCC')
        a, b = timestamp(scenario.incident_start), timestamp(scenario.incident_end_exclusive)
        if not cfg.start < cfg.end or not a < b: raise ValueError('Invalid date window')
        from .scenario import calibration
        estimate = calibration(cfg)
        target = scenario.target_bankwide_decline_rate_increase_pp
        if target < 0: raise ValueError('Impact target must be nonnegative')
        if scenario.enabled and (estimate['candidate_exposure_fraction'] == 0 or target > estimate['candidate_impact_floor_pp']):
            raise ValueError('Infeasible impact target for configured weighted window/rates/exposure; lower target or calibrate target_candidate_share')
        return cfg


def config_load(path):
    with open(path, encoding='utf8') as stream: raw = yaml.safe_load(stream)
    Config.parse(raw)
    return raw
