import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
import argparse
from meridian.generator import config_load,generate
p=argparse.ArgumentParser()
p.add_argument('--config',default='config/default.yaml')
p.add_argument('--output',default=None)
p.add_argument('--rows',type=int,default=None)
p.add_argument('--max-batches',type=int,default=None,help='Development-only limited batch run')
a=p.parse_args()
cfg=config_load(a.config)
if a.rows is not None: cfg['rows']=a.rows
stats=generate(cfg,a.output,progress=lambda n,total:print(f'{n:,}/{total:,} ({n/total:.1%})',flush=True),max_batches=a.max_batches)
print(stats)
