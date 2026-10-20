from datetime import date
from pathlib import Path
import json
import pyarrow.parquet as pq
from meridian.generator import config_load,generate

def test_small_generate(tmp_path):
    cfg=config_load('config/default.yaml')
    cfg.update(rows=1200,batch_size=500,customers=100,merchants=50)
    stats=generate(cfg,tmp_path)
    assert stats['rows']==1200
    paths=sorted((tmp_path/'payment_transactions').glob('*.parquet'))
    assert len(paths)==3
    assert sum(pq.read_metadata(p).num_rows for p in paths)==1200
    assert (tmp_path/'_private_ground_truth'/'incident.json').exists()
    assert json.loads((tmp_path/'public_manifest.json').read_text())['complete']

def test_repeatability(tmp_path):
    cfg=config_load('config/default.yaml');cfg.update(rows=100,batch_size=100,customers=100,merchants=50)
    generate(cfg,tmp_path/'a');generate(cfg,tmp_path/'b')
    a=pq.read_table(tmp_path/'a'/'payment_transactions'/'part-000000.parquet')
    b=pq.read_table(tmp_path/'b'/'payment_transactions'/'part-000000.parquet')
    assert a.equals(b)
