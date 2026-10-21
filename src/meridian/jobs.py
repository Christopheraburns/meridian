"""Process-based worker; one advisory lock fences writers across web restarts."""
from dataclasses import dataclass
from pathlib import Path
import fcntl
import json
import os
import time
import traceback
import pyarrow.parquet as pq
from .config import Config
from .generator import generate, GenerationCancelled
from .registry import Registry
from .writers.parquet import write_json, write_table
from .validation import validate_public_output


@dataclass(frozen=True)
class Settings:
    state: Path
    public: Path
    private: Path
    config: Path

    @classmethod
    def environment(cls):
        state = Path(os.environ.get('GENERATOR_STATE_DIR', str(Path.home()/'.local/state/meridian'))).resolve()
        public = Path(os.environ.get('GENERATOR_OUTPUT', str(Path.home()/'.local/share/meridian/public'))).resolve()
        private = Path(os.environ.get('GENERATOR_PRIVATE_OUTPUT', str(Path.home()/'.local/share/meridian/private'))).resolve()
        return cls(state,public,private,Path(os.environ.get('GENERATOR_CONFIG','config/smoke.yaml')).resolve())

    def validate(self):
        roots = [self.state.resolve(),self.public.resolve(),self.private.resolve()]
        if any(a == b or a in b.parents or b in a.parents for i,a in enumerate(roots) for b in roots[i+1:]):
            raise ValueError('Registry, public and private roots must be separate directory trees')


def worker_lock(registry):
    handle = (registry.root/'worker.lock').open('a')
    try:
        fcntl.flock(handle,fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError('Another worker owns this registry')
    return handle


def worker_alive(registry):
    try:
        handle = worker_lock(registry)
    except RuntimeError:
        return True
    handle.close()
    return False


def mark_partial(root, status, requested=None):
    """Reconcile files after cooperative cancellation or abrupt worker termination."""
    count = 0
    for path in sorted((root/'payment_transactions').glob('*.parquet')):
        try:
            count += pq.read_metadata(path).num_rows
        except Exception:
            pass  # Unreadable partial files are never counted or replayed.
    manifest_path = root/'public_manifest.json'
    if root.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except (OSError, ValueError):
            manifest = dict(generation_run_id=root.name, requested_rows=requested, format='parquet', dataset_path='payment_transactions')
        manifest.update(complete=False,status=status,rows_generated=count)
        write_json(manifest_path,manifest)
    run_path = root/'dimensions/generation_runs.parquet'
    if run_path.exists():
        try:
            rows = pq.read_table(run_path).to_pylist()
            rows[0].update(run_status=status,validation_status='pending',generated_transaction_count=count)
            write_table(run_path,'generation_runs',rows)
        except (OSError, ValueError, IndexError):
            pass  # The failed manifest/registry remain authoritative for corrupt output.
    return count


class Worker:
    def __init__(self, settings):
        settings.validate()
        self.settings = settings
        self.registry = Registry(settings.state)
        from .security import operator_token
        operator_token(settings.state)
        settings.private.mkdir(parents=True,exist_ok=True,mode=0o700)
        settings.private.chmod(0o700)
        self.stopping = False

    def recover(self):
        # Called only after acquiring the OS lock: no live writer can be displaced.
        for row in self.registry.interrupted():
            status = 'cancelled' if row['cancel'] else 'failed'
            count = mark_partial(self.settings.public/row['run_id'],status,row['requested'])
            self.registry.finish(row['run_id'],status,count,'Worker interrupted; retry creates a new isolated run',
                                 before_cancel=lambda:mark_partial(self.settings.public/row['run_id'],'cancelled',row['requested']))
        self.registry.recover_validation()
        self.registry.heartbeat()

    def execute(self, row):
        run_id = row['run_id']
        root = self.settings.public/run_id
        private = self.settings.private/run_id
        try:
            cfg = Config.parse(json.loads(row['config_json']))
            result = generate(cfg,root,private_output=private,run_id=run_id,
                              progress=lambda n,total:self.registry.progress(run_id,n),
                              should_cancel=lambda:self.stopping or bool(self.registry.get(run_id)['cancel']))
            # Check persisted public output, not generated-in-memory counters.
            report = validate_public_output(cfg,root,run_id,
                                            should_cancel=lambda:self.stopping or bool(self.registry.get(run_id)['cancel']))
            if self.stopping: raise GenerationCancelled()
            if not report['passed']: raise ValueError('Public validation failed')
            self.registry.finish(run_id,'complete',result['rows'],
                                 before_cancel=lambda:mark_partial(root,'cancelled',row['requested']))
        except GenerationCancelled:
            status = 'cancelled' if self.registry.get(run_id)['cancel'] else 'failed'
            count = mark_partial(root,status,row['requested'])
            self.registry.finish(run_id,status,count,None if status=='cancelled' else 'Worker stopped; retry creates a new isolated run',
                                 before_cancel=lambda:mark_partial(root,'cancelled',row['requested']))
        except Exception:
            # Raw exceptions may contain configuration/paths; keep them operator-only.
            (self.registry.root/f'{run_id}.error.log').write_text(traceback.format_exc())
            count = mark_partial(root,'failed',row['requested'])
            self.registry.finish(run_id,'failed',count,'Generation failed; operator diagnostics are available',
                                 before_cancel=lambda:mark_partial(root,'cancelled',row['requested']))

    def step(self):
        row = self.registry.claim()
        if row:
            self.execute(row)
            return True
        row = self.registry.claim_validation()
        if row:
            try:
                report = validate_public_output(Config.parse(json.loads(row['config_json'])),
                                                self.settings.public/row['run_id'],row['run_id'])
            except Exception:
                report = dict(passed=False,error='Public dataset validation failed')
            if not report['passed']:
                mark_partial(self.settings.public/row['run_id'],'failed',row['requested'])
            self.registry.validation_done(row['run_id'],report)
            return True
        self.registry.heartbeat()
        return False

    def run(self, once=False):
        with worker_lock(self.registry):
            self.recover()
            while not self.stopping:
                worked = self.step()
                if once: break
                if not worked: time.sleep(1)
