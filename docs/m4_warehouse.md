# M4 warehouse writer: impala_staged_load

M4 commits a validated local Parquet run into the **existing** Iceberg tables
through Impala. The plan preferred a Spark writer; no Spark runtime or catalog
configuration is available for the target CDW environment, so M4 implements the
plan's `impala_staged_load` adapter. Rows land through Impala `INSERT … SELECT`
from transient external tables, so each one is a real Iceberg commit visible to
Impala. Parquet is never copied into warehouse locations. Spark remains unbuilt;
revisit it only if a customer environment provides an approved Spark/Iceberg catalog.

Code: `src/meridian/warehouse/` (identities, read-only preflight, loader). CLI:
`scripts/warehouse.py`. Dependency: `pip install -r requirements-warehouse.txt`
(impyla). The UI, worker and job API stay `local_parquet`; loading is an operator step.

## Identities

Three logins, each with its own credentials. Endpoint settings are shared and can
be overridden per role; credentials are never inherited.

| Role | Env prefix | Needs |
|---|---|---|
| Public writer | `MERIDIAN_PUBLIC_WRITER_` | INSERT/SELECT/DELETE on the 10 `meridian_financial` tables, CREATE on `meridian_staging` (or pre-created), read on the public staging location |
| Private writer | `MERIDIAN_PRIVATE_WRITER_` | INSERT/SELECT/DELETE on the 2 `meridian_ground_truth` tables, CREATE on `meridian_private_staging`, read on the private staging location |
| Agent (investigation) | `MERIDIAN_AGENT_` | SELECT on `meridian_financial` only; **denied** `meridian_ground_truth` |

| Variable | Default | Notes |
|---|---|---|
| `MERIDIAN_IMPALA_HOST` / `_PORT` | none / 21050 | CDW: the virtual warehouse host, port 443 |
| `MERIDIAN_IMPALA_AUTH_MECHANISM` | NOSASL | NOSASL (local only), PLAIN, LDAP, GSSAPI, JWT |
| `MERIDIAN_IMPALA_USE_HTTP` / `_HTTP_PATH` | false / cliservice | CDW: true / cliservice |
| `MERIDIAN_IMPALA_USE_SSL` / `_CA_CERT` / `_VERIFY_CERT` | false / none / true | Certificates are verified unless explicitly disabled |
| `MERIDIAN_IMPALA_QUERY_OPTIONS` | none | `NAME=value,…` set on every session |
| `MERIDIAN_<ROLE>_USER`, `_PASSWORD` or `_PASSWORD_FILE`, `_JWT(_FILE)` | none | Per role; use Workbench secrets or mounted files |
| `MERIDIAN_DATABASE`, `MERIDIAN_PRIVATE_DATABASE` | meridian_financial, meridian_ground_truth | Targets |
| `MERIDIAN_STAGING_DATABASE`, `MERIDIAN_PRIVATE_STAGING_DATABASE` | meridian_staging, meridian_private_staging | Transient external tables |
| `MERIDIAN_STAGING_URI`, `MERIDIAN_PRIVATE_STAGING_URI` | none | Staging roots as Impala reads them (`s3a://…`, `abfs://…`, `file://…`); separate locations |
| `MERIDIAN_STAGING_DIR`, `MERIDIAN_PRIVATE_STAGING_DIR` | none | Optional local paths of the same roots; when set, `load` stages automatically |

Each run stages under `<root>/<generation_run_id>/<table>/`.

## Preflight (read-only)

`python3 scripts/warehouse.py preflight [--public <run>]` exits 1 on blockers and
prints a secret-free JSON report. It checks every identity connects; each of the
12 tables matches its `sql/schema` contract column-for-column, is `ICEBERG`, and
`payment_transactions` is partitioned by `DAY(transaction_ts)`; the agent login
differs from both writers; the agent is refused a read of the private database;
staging databases exist or will be created; and the target holds no other run.

`production_ready` is true only with authenticated, separated identities, a
verified Ranger denial for the agent, and no warnings. NOSASL is refused unless
`--allow-unauthenticated`, which downgrades the authentication and isolation
checks to warnings for local development.

## Load

```bash
python3 cli.py --config config/smoke.yaml --output <public> --private-output <private> --rows 10000
python3 scripts/warehouse.py load --config config/smoke.yaml --public <public> --private <private>
```

1. Refuses incomplete runs or a public tree containing private truth; re-runs the
   streaming public validation (`--skip-public-validation` only for a run that
   already passed it).
2. Stages (when `MERIDIAN_*STAGING_DIR` are set) and runs preflight in the same sessions.
3. Creates external tables declared from the contract, so drifted Parquet fails
   before loading, and checks staged counts against the manifest.
4. Commits dimensions, then transactions one week per statement, then private
   truth with the private writer, then the `generation_runs` row last. A run is
   `complete` in the warehouse only when that marker row exists.
5. Drops the external tables (staged files remain), computes stats, and verifies
   as the agent: per-table counts, Impala types, Iceberg snapshots, one complete
   marker, unique `transaction_id`, no orphan joins; private counts as the private writer.
6. Writes `warehouse_load.json` (0600) to the private directory.

**Resume:** each unit is skipped when already committed and its partial rows
(this run only) deleted and reinserted otherwise; re-running after any failure
continues. A completed run re-loads as a no-op.

**Occupancy:** `new_run` never mixes runs, because transaction IDs restart per
run. The loader refuses a target holding any other run and never truncates,
drops or alters tables. Clearing a target is an explicit operator action.

## Object storage (CDW)

Impala on CDW cannot read the Workbench project filesystem. Run `stage` with
`MERIDIAN_STAGING_DIR`/`MERIDIAN_PRIVATE_STAGING_DIR` set, copy both trees with the
environment's approved tool to `MERIDIAN_STAGING_URI/<run_id>` and
`MERIDIAN_PRIVATE_STAGING_URI/<run_id>`, then run `load` with the staging-dir
variables **unset** so it reads the uploaded copies. The loader's count check
catches incomplete uploads. Restrict the private staging location to the private
writer and the Impala service.

## Verified locally (2026-10-21)

Impala 4.5.3 quickstart (Optiness compose), NOSASL, scratch databases created
from the contracts by `tests/test_warehouse_impala.py`: 10k transactions
committed and verified through Impala; a load interrupted before the marker left
the run incomplete and resumed without duplicates; a repeat load was a no-op; a
second run was refused by the occupancy check. Local query options:
`MT_DOP=0,DISABLE_CODEGEN=true,EXEC_TIME_LIMIT_S=180` (emulated impalad).

**Not verified (needs CDW):** authenticated identities, Ranger denial of the agent,
object-storage staging, CDW Impala versions, and the 10k acceptance run against
the real `meridian_financial` tables.
