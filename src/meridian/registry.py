"""Durable local job queue. SQLite and worker lock require reliable POSIX storage."""
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import uuid
from .config import Config


def now():
    return datetime.now(timezone.utc).isoformat()


class Conflict(ValueError):
    pass


class Registry:
    def __init__(self, state_dir):
        self.root = Path(state_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        self.path = self.root/'jobs.sqlite3'
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY, config_json TEXT NOT NULL, status TEXT NOT NULL,
                    requested INTEGER NOT NULL, written INTEGER NOT NULL DEFAULT 0,
                    created TEXT NOT NULL, updated TEXT NOT NULL, cancel INTEGER NOT NULL DEFAULT 0,
                    parent_run_id TEXT, error TEXT, validation_status TEXT NOT NULL DEFAULT 'pending',
                    validation_json TEXT);
                CREATE UNIQUE INDEX IF NOT EXISTS single_writer ON runs(status) WHERE status='running';
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, at TEXT NOT NULL, message TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS worker (id INTEGER PRIMARY KEY, seen TEXT NOT NULL);
            ''')
        self.path.chmod(0o600)

    @contextmanager
    def connect(self, write=False):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA busy_timeout=30000')
            if write: db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _event(self, db, run_id, message):
        db.execute('INSERT INTO events(run_id,at,message) VALUES(?,?,?)', (run_id,now(),message))

    def submit(self, config, parent=None):
        cfg = Config.parse(config)
        if cfg.rows >= 50000000:
            raise ValueError('50M jobs remain disabled until M5 confirmation and capacity checks')
        run_id = str(uuid.uuid4())
        with self.connect(True) as db:
            if parent:
                old = db.execute('SELECT status FROM runs WHERE run_id=?',(parent,)).fetchone()
                if old is None: raise KeyError(parent)
                if old['status'] not in ('failed','cancelled'): raise Conflict('Only failed or cancelled runs may be retried')
            db.execute('INSERT INTO runs(run_id,config_json,status,requested,created,updated,parent_run_id) VALUES(?,?,?,?,?,?,?)',
                       (run_id,json.dumps(asdict(cfg)), 'queued',cfg.rows,now(),now(),parent))
            self._event(db,run_id,'Queued a new isolated run' if not parent else 'Queued retry in a new isolated run')
        return run_id

    def get(self, run_id):
        with self.connect() as db:
            row = db.execute('SELECT * FROM runs WHERE run_id=?',(run_id,)).fetchone()
            if row is None: raise KeyError(run_id)
            return dict(row)

    def list(self):
        with self.connect() as db:
            return [dict(r) for r in db.execute('SELECT * FROM runs ORDER BY created DESC LIMIT 100')]

    def events(self, run_id):
        self.get(run_id)
        with self.connect() as db:
            return [dict(r) for r in db.execute('SELECT at,message FROM events WHERE run_id=? ORDER BY id DESC LIMIT 100',(run_id,))][::-1]

    def claim(self):
        with self.connect(True) as db:
            row = db.execute("SELECT * FROM runs WHERE status='queued' ORDER BY created LIMIT 1").fetchone()
            if row is None: return None
            db.execute("UPDATE runs SET status='running',updated=? WHERE run_id=?",(now(),row['run_id']))
            self._event(db,row['run_id'],'Worker started generation')
            return dict(row)

    def progress(self, run_id, written):
        with self.connect(True) as db:
            db.execute('UPDATE runs SET written=?,updated=? WHERE run_id=?',(written,now(),run_id))
            self._event(db,run_id,f'Verified {written:,} rows written')
        self.heartbeat()

    def cancel(self, run_id):
        with self.connect(True) as db:
            row = db.execute('SELECT status FROM runs WHERE run_id=?',(run_id,)).fetchone()
            if row is None: raise KeyError(run_id)
            if row['status'] not in ('queued','running'): raise Conflict('Run is already terminal')
            status = 'cancelled' if row['status']=='queued' else 'running'
            db.execute('UPDATE runs SET cancel=1,status=?,updated=? WHERE run_id=?',(status,now(),run_id))
            self._event(db,run_id,'Cancellation requested')

    def finish(self, run_id, status, written, error=None, before_cancel=None):
        with self.connect(True) as db:
            row = db.execute('SELECT cancel FROM runs WHERE run_id=?',(run_id,)).fetchone()
            if row['cancel']:
                status = 'cancelled'
                if before_cancel: before_cancel()
            db.execute('UPDATE runs SET status=?,written=?,error=?,updated=?,validation_status=? WHERE run_id=?',
                       (status,written,error,now(),'passed' if status=='complete' else 'pending',run_id))
            self._event(db,run_id,f'Run {status}')

    def request_validation(self, run_id):
        with self.connect(True) as db:
            row = db.execute('SELECT status,validation_status FROM runs WHERE run_id=?',(run_id,)).fetchone()
            if row is None: raise KeyError(run_id)
            if row['status']!='complete': raise Conflict('Only complete runs can be validated')
            if row['validation_status'] in ('queued','running'): raise Conflict('Validation already queued')
            db.execute("UPDATE runs SET validation_status='queued',validation_json=NULL,updated=? WHERE run_id=?",(now(),run_id))
            self._event(db,run_id,'Public-data validation queued')

    def claim_validation(self):
        with self.connect(True) as db:
            row = db.execute("SELECT * FROM runs WHERE validation_status='queued' ORDER BY updated LIMIT 1").fetchone()
            if row is None: return None
            db.execute("UPDATE runs SET validation_status='running',updated=? WHERE run_id=?",(now(),row['run_id']))
            return dict(row)

    def validation_done(self, run_id, report):
        with self.connect(True) as db:
            status = 'passed' if report['passed'] else 'failed'
            db.execute('UPDATE runs SET validation_status=?,validation_json=?,updated=? WHERE run_id=?',
                       (status,json.dumps(report),now(),run_id))
            if not report['passed']:
                db.execute("UPDATE runs SET status='failed',error='Public validation failed; output is incomplete' WHERE run_id=?",(run_id,))
            self._event(db,run_id,f'Public-data validation {status}')

    def interrupted(self):
        with self.connect() as db:
            return [dict(r) for r in db.execute("SELECT * FROM runs WHERE status='running'")]

    def recover_validation(self):
        with self.connect(True) as db:
            db.execute("UPDATE runs SET validation_status='queued' WHERE validation_status='running'")

    def heartbeat(self):
        with self.connect(True) as db:
            db.execute('INSERT OR REPLACE INTO worker(id,seen) VALUES(1,?)',(now(),))

    def last_seen(self):
        with self.connect() as db:
            row = db.execute('SELECT seen FROM worker WHERE id=1').fetchone()
            return row['seen'] if row else None


def public_run(row):
    """Explicit allowlist: no config, seed, private path or exception details."""
    return dict(run_id=row['run_id'], status=row['status'], requested_rows=row['requested'],
                rows_written=row['written'], created_at=row['created'], updated_at=row['updated'],
                cancel_requested=bool(row['cancel']), parent_run_id=row['parent_run_id'],
                error=row['error'], validation_status=row['validation_status'],
                validation=json.loads(row['validation_json']) if row['validation_json'] else None,
                output=f"{row['run_id']}/payment_transactions")
