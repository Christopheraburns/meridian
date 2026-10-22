"""Flask control plane. All generation and dataset validation run in worker.py."""
import json
from pathlib import Path
import os
import sys

def project_root():
    """Workbench cell execution may omit __file__; do not depend on it."""
    configured = os.environ.get('GENERATOR_PROJECT_ROOT')
    if configured:
        candidates = [Path(configured).expanduser().resolve()]
    elif globals().get('__file__'):
        candidates = [Path(__file__).resolve().parent]
    else:
        current = Path.cwd().resolve()
        candidates = [current, *current.parents]
    for candidate in candidates:
        if (candidate/'src/meridian/__init__.py').is_file() and (candidate/'config/smoke.yaml').is_file():
            return candidate
    raise RuntimeError(
        'Cannot locate the Meridian checkout. Start from its root or set '
        'GENERATOR_PROJECT_ROOT to the directory containing app.py and src/meridian.'
    )


PROJECT_ROOT = project_root()
sys.path.insert(0,str(PROJECT_ROOT/'src'))
from flask import Flask, jsonify, request, render_template
from werkzeug.exceptions import BadRequest
from meridian.config import Config, config_load
from meridian.jobs import Settings
from meridian.registry import Registry, Conflict, public_run
from meridian.preflight import inspect


def create_app(settings=None):
    settings = settings or Settings.environment(project_root=PROJECT_ROOT)
    settings.validate()
    app = Flask(__name__,template_folder=str(PROJECT_ROOT/'src/meridian/web/templates'))
    app.config['MAX_CONTENT_LENGTH'] = 64*1024

    def registry(): return Registry(settings.state)

    @app.before_request
    def same_origin():
        # No application login: Workbench authentication fronts the UI. Still refuse cross-site changes.
        if not request.path.startswith('/api/') or request.method=='GET': return None
        if request.headers.get('Origin'):
            if request.headers['Origin'].rstrip('/') != (os.getenv('GENERATOR_PUBLIC_ORIGIN') or request.host_url).rstrip('/'):
                return jsonify(error='Cross-origin changes are not allowed'),403

    @app.after_request
    def headers(response):
        response.headers['Cache-Control']='no-store'
        response.headers['X-Content-Type-Options']='nosniff'
        response.headers['Referrer-Policy']='no-referrer'
        return response

    @app.errorhandler(KeyError)
    def missing(exc): return jsonify(error='Run not found'),404

    @app.errorhandler(Conflict)
    def conflict(exc): return jsonify(error=str(exc)),409

    @app.errorhandler(ValueError)
    @app.errorhandler(TypeError)
    @app.errorhandler(BadRequest)
    def invalid(exc): return jsonify(error='Invalid configuration or request; check field types, ranges and scenario feasibility'),400

    @app.get('/')
    def index(): return render_template('index.html')

    @app.get('/health')
    def health(): return jsonify(ok=True)

    @app.get('/api/preflight')
    def preflight(): return jsonify(inspect(settings,registry()))

    @app.get('/api/config/schema')
    def config_schema():
        return jsonify(schema_version='0.2',engine='local_parquet',write_mode='new_run',
                       fields={'rows':{'type':'integer','minimum':1,'maximum':49999999},
                               'batch_size':{'type':'integer','minimum':1}},
                       default_config=config_load(settings.config),
                       note='Operator-only scenario configuration. Output roots are controlled by deployment settings.')

    @app.post('/api/runs')
    @app.post('/api/start')
    def submit():
        payload = request.get_json(silent=False) if request.data else {}
        if not isinstance(payload,dict): raise ValueError('Expected JSON object')
        cfg = Config.parse(payload['config'] if 'config' in payload else config_load(settings.config))
        if cfg.rows >= 50000000:
            return jsonify(error='50M jobs remain disabled until M5 confirmation and capacity checks'),400
        run_id = registry().submit(cfg)
        return jsonify(run_id=run_id,status='queued'),202

    @app.get('/api/runs')
    def runs(): return jsonify(runs=[public_run(row) for row in registry().list()])

    @app.get('/api/status')
    def legacy_status():
        rows = registry().list()
        return jsonify(public_run(rows[0]) if rows else dict(status='idle',rows_written=0))

    @app.get('/api/runs/<run_id>')
    def get_run(run_id):
        reg = registry()
        result = public_run(reg.get(run_id))
        result['logs'] = reg.events(run_id)
        return jsonify(result)

    @app.post('/api/runs/<run_id>/cancel')
    def cancel(run_id):
        registry().cancel(run_id)
        return jsonify(run_id=run_id,status='cancellation_requested'),202

    @app.post('/api/runs/<run_id>/retry')
    def retry(run_id):
        reg = registry()
        old = reg.get(run_id)
        new_id = reg.submit(json.loads(old['config_json']),parent=run_id)
        return jsonify(run_id=new_id,parent_run_id=run_id,status='queued'),202

    @app.post('/api/runs/<run_id>/validate')
    def validate(run_id):
        registry().request_validation(run_id)
        return jsonify(run_id=run_id,status='validation_queued'),202

    return app


def listeners(port):
    """Processes listening on port, from /proc: Workbench Application pods have no ss/lsof."""
    import glob
    inodes = set()
    for table in ('/proc/net/tcp','/proc/net/tcp6'):
        try:
            rows = Path(table).read_text().splitlines()[1:]
        except OSError:
            continue
        for row in rows:
            fields = row.split()
            if fields[3]=='0A' and int(fields[1].rsplit(':',1)[1],16)==port: inodes.add(fields[9])
    found = []
    for fd in glob.glob('/proc/[0-9]*/fd/*'):
        try:
            target = os.readlink(fd)
            if target.startswith('socket:[') and target[8:-1] in inodes:
                pid = fd.split('/')[2]
                found.append(f"pid {pid}: {Path(f'/proc/{pid}/cmdline').read_text().replace(chr(0),' ').strip()}")
        except OSError:
            pass
    if inodes and not found: found.append('a process this user cannot inspect')
    return found


def serve(app):
    # Workbench proxies to localhost; Docker publishes the container port, so it needs 0.0.0.0.
    host = os.getenv('GENERATOR_BIND_HOST') or ('127.0.0.1' if os.getenv('CDSW_APP_PORT') else '0.0.0.0')
    port = int(os.getenv('CDSW_APP_PORT',os.getenv('PORT','8080')))
    print(f"Meridian control plane on {host}:{port} (CDSW_APP_PORT={os.getenv('CDSW_APP_PORT')}, "
          f"CDSW_READONLY_PORT={os.getenv('CDSW_READONLY_PORT')}, PORT={os.getenv('PORT')})",flush=True)
    try:
        app.run(host=host,port=port)
    except SystemExit:
        holders = listeners(port)
        print(f"Port {port} is held by: {'; '.join(holders) if holders else 'no listener found in /proc'}",flush=True)
        raise


app = create_app()

if __name__=='__main__':
    serve(app)
