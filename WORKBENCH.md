# Cloudera AI Workbench

Use an administrator-approved Python 3.11 runtime and install requirements.txt
from the approved package repository. Keep platform access controls enabled.
The generic Dockerfile remains for Docker Desktop, not a Cloudera runtime image.

Configure these environment variables for both the Application and worker Job:

- GENERATOR_STATE_DIR: persistent restricted directory for SQLite, token, logs and lock.
- GENERATOR_OUTPUT: persistent public Parquet root.
- GENERATOR_PRIVATE_OUTPUT: separate restricted truth root (worker needs access).
- GENERATOR_CONFIG: config/smoke.yaml initially, or another approved config file.
- GENERATOR_API_TOKEN or GENERATOR_API_TOKEN_FILE: operator secret delivered by
  Workbench secrets/mounted file. Without either, the worker creates operator.token
  in the registry directory; retrieve it through an authorized operator session.
- Optional GENERATOR_VIEWER_TOKEN: separate read-only status identity.
- GENERATOR_PUBLIC_ORIGIN: externally visible application origin when TLS terminates
  at a proxy, for example the actual Workbench application's scheme and hostname.

The three directories must be separate trees. Do not assume the project checkout
is writable/persistent. SQLite and the worker lock need verified shared POSIX
locking across the web and Job sessions. Object storage and unverified NFS are
not suitable registry backends. If that storage guarantee is unavailable, use a
single approved persistent host for the worker/control plane or treat durable
registry integration as a deployment blocker; do not claim cross-session safety.

Launch **app.py** as the Application. CDSW_APP_PORT remains honored. Launch
**worker.py** as a separate approved long-lived Workbench Job/process. For scheduled
jobs use `python3 worker.py --once`: it recovers abandoned runs and processes at
most one pending task. The web process never launches an in-process worker.
Workbench-specific job provisioning is an operator task, not an invented API.

The CLI still works independently:

```bash
python3 cli.py --config config/smoke.yaml \
  --output /approved/public/run-001 --private-output /approved/private/run-001
```

Substitute real writable approved paths. CLI directories must be empty and must
not overlap queued runs. Publish only completed public runs. Private output and
registry state must not be mounted into MCP or investigator sessions.

See [M3 job lifecycle and API](docs/m3_jobs.md) for cancellation, retry, authentication,
preflight and restart behavior. M3 local/Docker behavior is tested; live Workbench
job provisioning, storage locks and platform authorization require environment
verification. The Application and worker never write the warehouse.

To commit a run into CDW, install requirements-warehouse.txt in a session or Job,
set the MERIDIAN_* identity and staging variables from Workbench secrets, run
`python3 scripts/warehouse.py preflight` until it reports no blockers, then
`stage`, copy the staged trees to object storage, and `load`. See
[M4 warehouse writer](docs/m4_warehouse.md).

If Workbench executes app.py through a notebook/cell runner, `__file__` may be
absent. The app locates the checkout from the current directory or its parents.
If the checkout is elsewhere, set `GENERATOR_PROJECT_ROOT` to its absolute path
(the directory containing app.py and src/meridian). Template paths and relative
GENERATOR_CONFIG paths are resolved against that root. When copying code manually,
keep the Python identifier `__file__` literal; `**file**` is Markdown, not Python.
