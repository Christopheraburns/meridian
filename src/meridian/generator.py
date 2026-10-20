"""Seeded, batch-oriented synthetic bank payment generator (no PII)."""
import json
import os
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

NETWORKS = np.array(['Visa', 'Mastercard', 'Amex', 'Discover'])
CHANNELS = np.array(['pos', 'ecommerce', 'mobile', 'recurring'])
MCCS = np.array(['5411','5812','4722','5732','5999','4111','7011','5541'])
COUNTRIES = np.array(['US','CA','GB','MX','DE','FR','JP','AU'])


def config_load(path):
    with open(path, encoding='utf8') as f:
        cfg = yaml.safe_load(f)
    if int(cfg['rows']) < 1 or int(cfg['batch_size']) < 1:
        raise ValueError('rows and batch_size must be positive')
    if int(cfg['days']) < 1:
        raise ValueError('days must be positive')
    if not (0 <= float(cfg['incident']['baseline_decline_rate']) <= 1 and 0 <= float(cfg['incident']['injected_decline_rate']) <= 1):
        raise ValueError('decline rates must be between 0 and 1')
    if int(cfg['incident']['processor_id']) >= int(cfg['processors']) or int(cfg['incident']['processor_id']) < 0:
        raise ValueError('incident processor_id must be within processor range')
    return cfg


def _write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(obj, indent=2, default=str), encoding='utf8')
    os.replace(tmp, path)


def _write_dimensions(root, cfg):
    """Dimension records are deterministic; IDs are non-identifying synthetic keys."""
    dims = root / 'dimensions'
    dims.mkdir(parents=True, exist_ok=True)
    for name, count, attrs in [
        ('customers', cfg['customers'], {'segment': ['consumer','premium','small_business','enterprise'], 'home_country': COUNTRIES}),
        ('merchants', cfg['merchants'], {'mcc': MCCS, 'country': COUNTRIES}),
        ('payment_processors', cfg['processors'], {'name': ['Northstar','Pinnacle','Harbor','Orion','Summit','Apex','Cedar','AtlasPay']}),
    ]:
        count = int(count)
        ids = np.arange(count, dtype=np.int64)
        d = {name[:-1] + '_id' if name != 'payment_processors' else 'processor_id': ids}
        for key, choices in attrs.items():
            choices = np.asarray(choices)
            d[key] = choices[ids % len(choices)]
        pq.write_table(pa.table(d), dims / f'{name}.parquet', compression='zstd')
    inc = cfg['incident']
    deployment = pa.table({'deployment_id': ['DEP-412'], 'processor_id': [int(inc['processor_id'])], 'configuration_version': ['4.12'], 'deployed_on': [inc['start_date']], 'change_type': ['routing_configuration'], 'summary': ['Payment routing rule change']})
    pq.write_table(deployment, dims / 'deployment_events.parquet', compression='zstd')
    routing = pa.table({'routing_rule_id': [412], 'processor_id':[int(inc['processor_id'])], 'configuration_version':['4.12'], 'valid_from':[inc['start_date']], 'rule_family':['cross_border_card_routing']})
    pq.write_table(routing, dims / 'routing_rules.parquet', compression='zstd')


def generate(cfg, output=None, progress=None, max_batches=None):
    root = Path(output or cfg['output_dir']).resolve()
    root.mkdir(parents=True, exist_ok=True)
    transactions = root / 'payment_transactions'
    transactions.mkdir(exist_ok=True)
    _write_dimensions(root, cfg)
    rows = int(cfg['rows']); batch = int(cfg['batch_size']); seed = int(cfg['seed'])
    start = date.fromisoformat(cfg['start_date']); inc = cfg['incident']
    inc_start = (date.fromisoformat(inc['start_date']) - start).days
    inc_end = (date.fromisoformat(inc['end_date']) - start).days
    totals = {'rows': 0, 'declined': 0, 'exposed': 0, 'exposed_declined': 0, 'injected_excess_expected': 0.0}
    manifest_path = root / 'public_manifest.json'
    config_public = {k:v for k,v in cfg.items() if k != 'incident'}
    _write_json(root / 'configuration_public.json', config_public)
    batches = (rows + batch - 1) // batch
    for i in range(batches):
        if max_batches is not None and i >= max_batches: break
        path = transactions / f'part-{i:06d}.parquet'
        # independent RNG per batch allows deterministic resume, without re-running prior batches
        rng = np.random.default_rng(np.random.SeedSequence([seed, i]))
        n = min(batch, rows - i*batch)
        day = rng.integers(0, int(cfg['days']), size=n, dtype=np.int16)
        timestamp = np.datetime64(str(start)) + day.astype('timedelta64[D]') + rng.integers(0,86400,size=n).astype('timedelta64[s]')
        processor = rng.integers(0,int(cfg['processors']),size=n,dtype=np.int16)
        network = rng.choice(NETWORKS,size=n,p=[0.50,0.35,0.10,0.05])
        channel = rng.choice(CHANNELS,size=n,p=[0.48,0.32,0.15,0.05])
        mcc = rng.choice(MCCS,size=n)
        foreign = rng.random(n) < 0.18
        country = np.where(foreign, rng.choice(COUNTRIES[1:],size=n), 'US')
        affected = ((day >= inc_start) & (day <= inc_end) &
                    (processor == int(inc['processor_id'])) &
                    (network == inc['card_network']) & (channel == inc['channel']) &
                    foreign & (mcc == inc['mcc']))
        if not inc['enabled']: affected[:] = False
        # A controlled incident increases decline probability for an identifiable cohort.
        p = np.where(affected, float(inc['injected_decline_rate']), float(inc['baseline_decline_rate']))
        declined = rng.random(n) < p
        routing_rule = np.where(affected,412,100 + processor)
        table = pa.table({
            'transaction_id': np.arange(i*batch,i*batch+n,dtype=np.int64) + 1,
            'transaction_ts': pa.array(timestamp, type=pa.timestamp('s')),
            'customer_id': rng.integers(0,int(cfg['customers']),size=n,dtype=np.int64),
            'merchant_id': rng.integers(0,int(cfg['merchants']),size=n,dtype=np.int64),
            'amount_usd': np.round(rng.lognormal(4.2,1.0,size=n),2),
            'processor_id': processor, 'card_network': network, 'channel': channel,
            'merchant_category_code': mcc, 'cross_border': foreign,
            'merchant_country': country, 'routing_rule_id': routing_rule.astype('int16'),
            'approved': ~declined,
            'response_code': np.where(declined,'05','00'),
        })
        tmp = path.with_suffix('.parquet.tmp')
        pq.write_table(table,tmp,compression='zstd',row_group_size=100000)
        os.replace(tmp,path)
        totals['rows'] += n; totals['declined'] += int(declined.sum())
        totals['exposed'] += int(affected.sum()); totals['exposed_declined'] += int((affected & declined).sum())
        totals['injected_excess_expected'] += float(affected.sum() * (float(inc['injected_decline_rate']) - float(inc['baseline_decline_rate'])))
        _write_json(manifest_path, {'name':cfg['name'],'seed':seed,'rows_generated':totals['rows'],'requested_rows':rows,'complete': totals['rows']==rows,'format':'parquet','dataset_path':'payment_transactions','last_batch':i})
        if progress: progress(totals['rows'],rows)
    # SECRET: keep out of the published Iceberg/data folder and agent MCP scope.
    _write_json(root / '_private_ground_truth' / 'incident.json', {'incident':inc,'generation_stats':totals,'explanation':'Routing version 4.12 raises decline probability for the specified cohort. This private file must not be exposed to the investigation agent.'})
    return totals
