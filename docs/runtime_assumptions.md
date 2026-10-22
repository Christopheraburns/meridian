# Runtime assumptions and milestone boundaries

M0–M3 implement a local deterministic generator, not a distributed engine.
No warehouse connection or credential is needed or used. Supported deployment
remains Python 3.11 in Docker Desktop or an approved Cloudera Workbench runtime.
Requirements remain in the existing requirements files; no additional runtime
packages are needed. Project checkout need not be writable: select writable,
persistent public and private output paths. Tests are run from repository root.

Config supports the original flat YAML keys through `Config.parse`, including
inclusive v0.1 incident end_date → exclusive v0.2 end timestamp. The v0.2 nested
scale/scenario/execution form is demonstrated in config/smoke.yaml. Configs use
naive UTC dates/times. The local engine rejects Spark/other databases/write modes.
Positive sizes, dimension capacity, rates, target predicates, processor references,
windows and analytically infeasible impact targets are checked. M2 adds a shared
weighted-calendar exposure calibration, configurable target traffic/processor
selection and canary/expanded rollout with old-version controls and recovery.
The 1M profile and private evaluator are described in [M2 scenario quality](m2_scenario_quality.md).
A target is an expected impact constraint; the evaluator separately checks its
realized value and statistical strength.


Each transaction uses BLAKE2-derived seed/ID/feature variates, independent of call
order, batching and retries. Dimensions use stable ID-derived attributes. With
the same run ID, dataset config and seed, public data and aggregate truth are
identical across batch sizes. New runs get UUIDs; explicit --run-id supports
reproduction in separate empty directories. Config content hashes exclude physical
batch size and output path. Operational run timestamps and batch audits are
excluded from normalized content comparisons. Monetary values use fixed FX rates;
amounts are capped lognormal draws, dates have weekday/weekend and limited holiday
weights. Each row counts one original authorization attempt. Retry generation is
disabled (retry_attempt=0, original_transaction_id=NULL). Fraud declines and a
separate twelve-hour processor incident supply basic operational noise.

Local new_run requires empty public and private directories, preventing stale
batch mixing. Dimensions are written first; facts are validated and atomically
renamed per file. Complete is recorded only after all expected public/private
files are written. Partial development runs and exceptions never report complete.
Managed-job readers must wait for both registry completion and public_manifest.complete.
M3 provides a durable SQLite registry and separate process worker, authentication,
cancellation, fresh-run retry and restart reconciliation. See [M3 jobs](m3_jobs.md).
This is not a multi-file atomic commit. Same-run resume/distributed recovery remain
M5; dimensions are still held in memory and 50M performance is unvalidated.

The dashboard and worker use separate processes and persistent state/output roots.
Docker uses a local named registry volume with shared POSIX locking. Workbench
requires approved persistent storage and independently launched workers with
verified cross-session file locking; no external database registry adapter exists.
Private files are never served by the UI. Filesystem separation is not a Ranger
policy; private output and registry state must not be published to investigators.

## Information required for M4

- CDP deployment type (on-prem/public cloud), platform and Workbench versions,
  approved Python/Spark runtime, Spark version, Scala/JVM versions and Iceberg
  connector versions/JAR availability.
- Supported Spark execution/submission mechanism, cluster endpoint/session
  provisioning, queue/resource limits, network reachability and Workbench job
  lifecycle/persistent storage guarantees.
- Existing catalog implementation and Spark catalog name/configuration, Hive
  Metastore URI if applicable, authoritative warehouse/storage URI, filesystem
  type and approved filesystem connectors. No guessed catalog settings.
- Authentication mechanism (Kerberos/OAuth/service account/etc.), secret delivery
  and refresh mechanism, separate public-writer/private-writer/read-only agent
  identities, and Ranger grants/denials. Provide secret references, not secrets
  in repository/YAML or chat.
- Impala endpoint/transport/TLS/authentication details and authorized read-only
  schema inspection access for all twelve tables; Iceberg format/spec versions,
  timestamp/decimal interoperability and metadata refresh requirements.
- Approved small test-write authorization, target occupancy and run-ID policy,
  single-writer coordination, staging/commit/retry semantics, quotas and filesystem
  permissions. Existing tables must not be recreated or altered.
- Means to verify native Iceberg read/write/commit and Impala visibility, inspect
  snapshots, and test that the agent cannot access the private database.

M4 must run platform-specific preflight before any mutation, fail with actionable
blockers, then commit a small authorized run and verify through Impala. Copying
Parquet into warehouse locations is not a supported fallback. No live Cloudera,
Spark, Iceberg or Ranger validation is claimed in M0–M3.

M4 status: implemented as `impala_staged_load` ([M4 warehouse writer](m4_warehouse.md)),
which needs only the Impala endpoint, identities, Ranger grants and a staging
location from the list above; the Spark and catalog items apply only if a Spark
writer is added later. Verified against a local Impala 4.5.3 with NOSASL; no CDW
or Ranger validation has been run yet.
