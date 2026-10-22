from dataclasses import asdict, replace
from pathlib import Path
import importlib.util
import json
import os
import subprocess
import sys
import time
import pyarrow.parquet as pq
import pytest
from meridian.config import Config, config_load
from meridian.jobs import Settings, Worker, worker_lock
from meridian.registry import Registry, Conflict, public_run
from meridian.validation import validate_public_output


def config(): return replace(Config.parse(config_load('config/smoke.yaml')),rows=1200,batch_size=200)


@pytest.fixture
def settings(tmp_path,monkeypatch):
    return Settings(tmp_path/'state',tmp_path/'public',tmp_path/'private',Path('config/smoke.yaml').resolve())


def client(settings):
    spec = importlib.util.spec_from_file_location('meridian_web','app.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module.create_app(settings).test_client()


def test_durable_api_lifecycle_and_privacy(settings):
    web = client(settings)
    assert web.get('/health').status_code == 200
    assert web.get('/').status_code == 200
    assert web.get('/api/runs').status_code == 200
    cfg = asdict(config())
    result = web.post('/api/runs',json={'config':cfg})
    assert result.status_code == 202
    run_id = result.json['run_id']
    # New Flask instance sees the same queue; web process never launches a thread.
    restarted = client(settings)
    assert restarted.get('/api/runs/'+run_id).json['status']=='queued'
    Worker(settings).run(once=True)
    result = restarted.get('/api/runs/'+run_id).json
    assert result['status']=='complete' and result['rows_written']==1200
    assert result['logs'][-1]['message']=='Run complete'
    body = json.dumps(result)
    for secret in ('scenario','faulty','predicate','exposed','counterfactual','config_json'):
        assert secret not in body
    assert web.get('/api/config/schema').json['engine']=='local_parquet'
    assert web.post('/api/runs/'+run_id+'/validate').status_code==202
    assert web.post('/api/runs/'+run_id+'/validate').status_code==409
    Worker(settings).run(once=True)
    detail = web.get('/api/runs/'+run_id).json
    assert detail['validation']['passed'] and detail['validation']['rows']==1200
    assert web.post('/api/runs/'+run_id+'/cancel').status_code==409
    assert web.post('/api/runs/'+run_id+'/retry').status_code==409
    assert web.get('/api/runs/not-a-run').status_code==404
    assert web.get('/api/preflight').json['spark_iceberg']['ready'] is False


def test_cancel_during_generation_retry_without_replay(settings,monkeypatch):
    worker = Worker(settings); reg = worker.registry
    run_id = reg.submit(config())
    original = reg.progress
    def cancel_after_batch(key,n):
        original(key,n)
        reg.cancel(key)
    monkeypatch.setattr(reg,'progress',cancel_after_batch)
    worker.run(once=True)
    row = reg.get(run_id)
    assert row['status']=='cancelled' and row['written']==200
    root = settings.public/run_id
    assert json.loads((root/'public_manifest.json').read_text())['complete'] is False
    assert pq.read_table(root/'dimensions/generation_runs.parquet').to_pylist()[0]['run_status']=='cancelled'
    parts = {p.name:p.read_bytes() for p in (root/'payment_transactions').glob('*.parquet')}
    retry = reg.submit(json.loads(row['config_json']),parent=run_id)
    assert retry != run_id
    Worker(settings).run(once=True)
    assert reg.get(retry)['status']=='complete'
    assert parts == {p.name:p.read_bytes() for p in (root/'payment_transactions').glob('*.parquet')}
    assert validate_public_output(config(),settings.public/retry,retry)['rows']==1200


def test_queued_cancel_validation_and_request_errors(settings):
    web = client(settings); reg = Registry(settings.state)
    run_id = reg.submit(config()); reg.cancel(run_id)
    Worker(settings).run(once=True)
    assert reg.get(run_id)['status']=='cancelled' and not (settings.public/run_id).exists()
    assert web.post('/api/runs/'+run_id+'/validate').status_code==409
    assert web.post('/api/runs',json={'config':{'rows':0}}).status_code==400
    assert web.post('/api/runs',json=[]).status_code==400
    assert web.post('/api/runs',headers={'Origin':'https://untrusted.example'},json={}).status_code==403
    huge = asdict(replace(config(),rows=50000000))
    assert web.post('/api/runs',json={'config':huge}).status_code==400
    retry = web.post('/api/runs/'+run_id+'/retry')
    assert retry.status_code==202 and retry.json['parent_run_id']==run_id


def test_single_worker_fencing_and_completion_cancel_race(settings):
    reg = Registry(settings.state)
    with worker_lock(reg):
        with pytest.raises(RuntimeError,match='Another worker'):
            Worker(settings).run(once=True)
    run_id = reg.submit(config())
    reg.claim(); reg.cancel(run_id)
    calls=[]
    reg.finish(run_id,'complete',1200,before_cancel=lambda:calls.append(True))
    assert calls==[True] and reg.get(run_id)['status']=='cancelled'


def test_killed_worker_recovers_written_count_and_retains_partial(settings):
    cfg = replace(config(),rows=20000,batch_size=200)
    reg = Registry(settings.state); run_id = reg.submit(cfg)
    env = dict(os.environ,GENERATOR_STATE_DIR=str(settings.state),GENERATOR_OUTPUT=str(settings.public),
               GENERATOR_PRIVATE_OUTPUT=str(settings.private),GENERATOR_CONFIG=str(settings.config),PYTHONPATH='src')
    proc = subprocess.Popen([sys.executable,'worker.py'],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    try:
        deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            row=reg.get(run_id)
            if row['written']>=400: break
            if proc.poll() is not None: pytest.fail(proc.stderr.read().decode())
            time.sleep(.02)
        else: pytest.fail('Worker did not write batches')
        proc.kill(); proc.wait(timeout=10)
    finally:
        if proc.poll() is None: proc.kill(); proc.wait(timeout=10)
        proc.stderr.close()
    # Simulate crash between file commit and registry progress: recovery counts files.
    with reg.connect(True) as db: db.execute('UPDATE runs SET written=0 WHERE run_id=?',(run_id,))
    root=settings.public/run_id
    before={p.name:p.read_bytes() for p in (root/'payment_transactions').glob('*.parquet')}
    Worker(settings).run(once=True)
    row=reg.get(run_id)
    actual=sum(pq.read_metadata(p).num_rows for p in (root/'payment_transactions').glob('*.parquet'))
    assert row['status']=='failed' and 400<=row['written']==actual<cfg.rows
    assert not json.loads((root/'public_manifest.json').read_text())['complete']
    assert before=={p.name:p.read_bytes() for p in (root/'payment_transactions').glob('*.parquet')}
    Worker(settings).run(once=True)
    assert reg.get(run_id)['written']==actual
    retry=reg.submit(cfg,parent=run_id)
    Worker(settings).run(once=True)
    assert reg.get(retry)['status']=='complete'
    assert validate_public_output(cfg,settings.public/retry,retry)['rows']==20000


def test_validation_does_not_read_private_truth(settings):
    worker=Worker(settings); run_id=worker.registry.submit(config()); worker.run(once=True)
    # No private path is an argument to the public validator.
    report=validate_public_output(config(),settings.public/run_id,run_id)
    assert report['passed'] and 'exposed' not in json.dumps(report)
    manifest=settings.public/run_id/'public_manifest.json'
    data=json.loads(manifest.read_text()); data['complete']=False; manifest.write_text(json.dumps(data))
    worker.registry.request_validation(run_id); Worker(settings).run(once=True)
    assert worker.registry.get(run_id)['validation_status']=='failed'
    assert worker.registry.get(run_id)['status']=='failed'
    assert not json.loads(manifest.read_text())['complete']


def test_preflight_reports_worker_and_root_safety(settings):
    from meridian.preflight import inspect
    reg=Registry(settings.state)
    assert not inspect(settings,reg)['local_parquet']['ready']
    with worker_lock(reg):
        assert inspect(settings,reg)['local_parquet']['ready']
    with pytest.raises(ValueError,match='separate'):
        replace(settings,private=settings.public/'private').validate()


def test_recovery_invalidates_corrupt_manifest_without_replay(settings):
    worker=Worker(settings); run_id=worker.registry.submit(config())
    worker.run(once=True)
    root=settings.public/run_id
    # Simulate a crash before SQLite finalization, plus a damaged manifest.
    with worker.registry.connect(True) as db:
        db.execute("UPDATE runs SET status='running' WHERE run_id=?",(run_id,))
    (root/'public_manifest.json').write_text('{broken')
    Worker(settings).run(once=True)
    assert worker.registry.get(run_id)['status']=='failed'
    manifest=json.loads((root/'public_manifest.json').read_text())
    assert not manifest['complete'] and manifest['rows_generated']==1200
    assert manifest['requested_rows']==1200
