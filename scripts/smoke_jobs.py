"""Isolated Docker/HTTP lifecycle acceptance on host 8081, container 8080.

Stops only its own meridian-m3-smoke Compose project; retains evidence in data/m3-smoke.
"""
import json
import os
from pathlib import Path
import subprocess
import time
from urllib.request import Request, urlopen
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ['docker','compose','-p','meridian-m3-smoke','-f','docker-compose.yaml','-f','tests/docker-compose.smoke.yaml']
BASE = 'http://localhost:8081'
ENV = dict(os.environ,GENERATOR_PUBLIC_ORIGIN=BASE)


def compose(*args):
    subprocess.run(COMPOSE+list(args),cwd=ROOT,env=ENV,check=True)


def api(path, method='GET', data=None):
    headers={'Content-Type':'application/json'}
    req=Request(BASE+path,headers=headers,method=method,data=json.dumps(data).encode() if data is not None else None)
    with urlopen(req,timeout=30) as response:
        raw=response.read()
        return json.loads(raw) if response.headers.get_content_type()=='application/json' else raw.decode()


def wait_for(predicate, description, timeout=120):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            result=predicate()
            if result:return result
        except (URLError,ConnectionError): pass
        time.sleep(.25)
    raise AssertionError('Timed out: '+description)


def status(run_id): return api('/api/runs/'+run_id)


def submit(rows=10000,batch=500):
    cfg=api('/api/config/schema')['default_config']
    cfg['scale'].update(transaction_rows=rows,batch_size=batch)
    return api('/api/runs','POST',{'config':cfg})['run_id']


def main():
    try:
        compose('up','--build','-d')
        wait_for(lambda:api('/health')['ok'],'health')
        assert 'Meridian Financial Generator' in api('/')
        wait_for(lambda:api('/api/preflight')['local_parquet']['ready'],'worker readiness')
        run_id=submit()
        result=wait_for(lambda:status(run_id) if status(run_id)['status']=='complete' else None,'10k run')
        assert result['rows_written']==10000
        compose('restart','generator','worker')
        wait_for(lambda:api('/health')['ok'],'restart health')
        assert status(run_id)['status']=='complete'
        api('/api/runs/'+run_id+'/validate','POST',{})
        wait_for(lambda:status(run_id)['validation_status']=='passed','public validation')
        # Cancel during generation, then explicitly retry into a fresh output tree.
        cancelled=submit(100000,500)
        wait_for(lambda:status(cancelled)['rows_written']>=500,'progress before cancel')
        api('/api/runs/'+cancelled+'/cancel','POST',{})
        wait_for(lambda:status(cancelled)['status']=='cancelled','cancellation')
        retry=api('/api/runs/'+cancelled+'/retry','POST',{})['run_id']
        assert retry!=cancelled
        wait_for(lambda:status(retry)['status']=='complete','retry',180)
        assert status(cancelled)['status']=='cancelled'
        # Abrupt stop: restart must reconcile committed files without replaying them.
        interrupted=submit(100000,500)
        wait_for(lambda:status(interrupted)['rows_written']>=500,'progress before kill')
        compose('kill','-s','SIGKILL','worker')
        compose('up','-d','worker')
        wait_for(lambda:status(interrupted)['status']=='failed','interruption recovery')
        partial=status(interrupted)
        assert 0<partial['rows_written']<100000
        manifest=json.loads((ROOT/'data/m3-smoke/public'/interrupted/'public_manifest.json').read_text())
        assert manifest['complete'] is False
        assert manifest['rows_generated']==partial['rows_written']
        print(json.dumps(dict(passed=True,host_port=8081,container_port=8080,completed_run=run_id,
                              cancelled_run=cancelled,retry_run=retry,interrupted_run=interrupted),indent=2))
    except Exception:
        compose('logs','--tail','30')
        raise
    finally:
        compose('down')


if __name__=='__main__': main()
