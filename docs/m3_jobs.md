# M3 durable jobs and control plane

Flask now submits to SQLite; `worker.py` is a separate process. There are no
in-process generation threads. The same deterministic generator, SQL contracts,
local CLI and private M2 evaluator remain in use. No warehouse writes occur.

## Docker Desktop

```bash
docker compose up --build -d
```

Open http://localhost:8081. The UI shows readiness, configuration, recent runs, verified written rows, validation results,
output locations and sanitized lifecycle logs. The default UI config is the 10k
smoke profile. The existing config/default.yaml is unchanged and remains usable
through the CLI or GENERATOR_CONFIG.

SQLite, worker lock and operator-only error diagnostics live in a
Docker-managed named volume at `/state`. Use this local volume for locking rather
than a macOS host bind mount. The web and worker share this volume. Public output
is `data/public/<run_id>`; private output is `data/private/<run_id>`. Only the worker
has the private-output mount. Do not use `docker compose down -v` if you want to
retain registry history. Normal stop/start, restart and down/up retain it.

The control plane has no login of its own (removed 2026-10-21: it is an internal
loader that only operators reach). Access control is the platform's: the Compose
port binds only to 127.0.0.1, and on Workbench the Application must keep
Workbench authentication on (do not enable unauthenticated access for it).

## API

`/api/*` routes need no credentials. Mutations (POST) are refused when the request's
`Origin` differs from the application's own origin (GENERATOR_PUBLIC_ORIGIN, or the
request host), so other websites cannot trigger runs through a signed-in browser.
Public status/logs use explicit field allowlists and omit seed, scenario predicates,
private paths, truth and raw exception text. Raw worker diagnostics stay in the
restricted registry directory. No private evaluator is exposed over HTTP. Failed public revalidation invalidates
the run and its manifest; it is then eligible for a fresh-run retry.

| Route | Behavior |
| --- | --- |
| GET /api/preflight | Local storage/contracts/registry/worker readiness; explicit M4 blockers |
| GET /api/config/schema | Operator configuration and supported dataset controls |
| POST /api/runs | Validate `{ "config": ... }`, persist queued run, return ID and 202 |
| GET /api/runs | Most recent 100 persisted runs |
| GET /api/runs/{id} | Status, counts, validation, public output and last 100 sanitized events |
| POST /api/runs/{id}/cancel | Cancel queued run or request cooperative stop |
| POST /api/runs/{id}/retry | New isolated run for a failed/cancelled predecessor |
| POST /api/runs/{id}/validate | Enqueue public-only streaming validation; return 202 |

The existing `/api/start` and `/api/status` remain authenticated compatibility
aliases. Malformed inputs return 400; missing runs 404; invalid transitions 409.
50M submissions remain disabled until M5's explicit confirmation/capacity workflow.
Scenario quality is still measured by the separate private M2 evaluator; public
validation never reconstructs counterfactuals or reads private truth.

## Lifecycle and failure semantics

Runs transition `queued → running → complete | cancelled | failed`. Configuration
and run identity are saved at submission. A process-level advisory lock and a
SQLite uniqueness constraint ensure a single generation worker for a registry.
Use one registry for each output-root pair. Direct CLI generation is independent
of the queue; do not aim a CLI job at a managed run directory.

Progress advances only after a Parquet file is renamed and its written row count
is verified from its footer. Final worker completion also requires a streaming
readback of every public fact, schema/count/ID checks, dimension joins, effective
dates and amount validation. Run status is persisted, not reconstructed from a
web process's memory. SQLite transactions serialize cancellation with completion.

Cancellation is checked before each batch, after generation and during final
validation. A queued cancellation writes nothing. A running cancellation retains
committed files for inspection and marks its manifest/run row incomplete; it
never publishes a successful result. Cancellation latency is at most a generation
batch or a validation chunk, plus initial dimension creation. Choose batch sizes
accordingly. Cooperative SIGTERM stops the worker and marks its active run failed
unless cancellation was explicitly requested.

After SIGKILL/crash, the next worker acquires the lock, reconciles committed Parquet
counts, marks the interrupted run failed/incomplete, and resets interrupted
public validation tasks to queued. It does **not** append to or replay an
interrupted run. If the crash happened after the output manifest was complete but
before registry completion, recovery conservatively invalidates that manifest.
The queue/registry status and manifest must both be complete before publication.

M3 retry creates a **new run ID and new output trees** with the saved config and
seed, recording parent_run_id. Failed/cancelled files remain untouched. Thus no
same-run duplicate append occurs. Automatic resume of the same run, distributed
checkpointing and production cleanup tools remain M5. Partial output cleanup is
an explicit operator storage task after the worker has stopped; no automatic
recursive deletion is performed.

## Runtime and Workbench

Set GENERATOR_STATE_DIR, GENERATOR_OUTPUT and GENERATOR_PRIVATE_OUTPUT to three
separate persistent directory trees. Defaults outside Docker are under
`~/.local/state/meridian` and `~/.local/share/meridian`; never rely on the checkout
being writable. State/private directories are restricted to the operator account.
SQLite and advisory locking require reliable shared POSIX semantics. For Workbench
use an approved persistent storage location with verified cross-session locks,
or run worker and registry on a single approved persistent host. An arbitrary
object store or unverified NFS mount is not a supported registry backend. A
multi-host database/queue adapter has not been implemented.

Launch `app.py` as the Workbench Application and `worker.py` in a separate
Workbench Job/approved long-lived process. `worker.py --once` recovers abandoned
state and processes at most one queued generation/validation task, suitable for
scheduled Workbench jobs. The worker lifetime is independent of web requests and
application restarts. Job provisioning and permissions must be configured by the
Workbench operator; this repository does not guess platform submission endpoints.
CDSW_APP_PORT remains supported. For TLS-terminating proxies set
GENERATOR_PUBLIC_ORIGIN to the externally visible scheme/host/port so same-origin
mutation checks accept the Workbench UI. Keep platform access controls enabled.

`python3 scripts/preflight.py` reports local readiness and returns nonzero when
no worker is available or local checks fail. Spark-installed detection is only a
runtime diagnostic; `spark_iceberg.ready` stays false because M4 commits through
Impala instead. The `warehouse` section reports which identities are configured,
without secrets or connections; `python3 scripts/warehouse.py preflight` runs the
live checks ([M4](m4_warehouse.md)).

## Tests

```bash
PYTHONPATH=src python3 -m pytest -q
# Requires Docker Desktop and free localhost:8081; uses its own Compose project.
python3 scripts/smoke_jobs.py
```

The Docker test creates a 10k run, verifies UI/health/auth, restarts both services,
checks history, runs public validation, cancels and retries a 100k job, kills a
separate worker mid-job, and verifies recovered counts/incomplete output. It
stops only its own `meridian-m3-smoke` project, retaining the named registry volume
and `data/m3-smoke` Parquet evidence. No 1M/50M scale or Cloudera test is implied.

## Executed acceptance

- Local pytest: **26 passed**.
- Python 3.11 Docker pytest: **26 passed**.
- Real Compose/HTTP smoke on **8081:8080**: passed health/UI/auth, 10k generation,
  service restart persistence, public validation, running cancellation, fresh-run
  retry, and SIGKILL recovery with verified committed counts.
- Dashboard JavaScript syntax check: passed.

The initial macOS bind-mounted registry failed cross-container lock detection;
the successful Compose acceptance used the Docker-managed local registry volume
now configured by default. SQL contracts and Cloudera tables were not modified.
