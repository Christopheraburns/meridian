# Handoff: continuing on Cloudera AI Workbench (2026-10-21)

Development moves from the laptop to a Cursor instance on Cloudera AI Workbench, which can
reach both the CDW lakehouse and S3. This file carries the state that lived only in the
laptop's session history. It contains no secrets: never commit passwords or tokens.

## Context

- Meridian generates the synthetic payments dataset (planted routing incident, decoys,
  private answer key) for the Optiness demo (repo `primeagent`, Prime Agent on Workbench
  investigating it through CDW Impala). Two goals: a recorded Evolve NY demo on
  **2026-10-27**, and FDE customer builds that must really work (nothing faked).
- Keep Meridian and Optiness as **separate Workbench projects**: the Optiness agent's Python
  runs over its project filesystem and must never see Meridian's private output.

## Status

- M0–M3 done. **M4 implemented** (`impala_staged_load`, see [docs/m4_warehouse.md](docs/m4_warehouse.md)):
  Spark was not pursued because no Spark/catalog configuration exists for CDW; rows commit
  through Impala `INSERT … SELECT` from staged external tables.
- Verified locally: 44 unit tests pass; `tests/test_warehouse_impala.py` passed against a
  local Impala 4.5.3 (10k commit, interrupt/resume, idempotent re-run, occupancy refusal).
- Verified against CDW from the laptop (read-only): Impala 4.5.0, **LDAP over HTTP on 443
  with certificate verification works** (the JDBC string's `AuthMech=12` browser SSO cannot be
  used from Python). All 12 tables in `meridian_financial` / `meridian_ground_truth` match the
  contracts, are Iceberg, `payment_transactions` is `DAY(transaction_ts)`, and all were **empty**.
- Not done: M4 acceptance (10k load into the real CDW tables), S3 staging, Ranger isolation,
  M5 (50M), M6. The laptop had no S3 access, which is why work moves to Workbench.

## Workbench environment variables

Application (`app.py`); project root assumed `/home/cdsw`:

| Variable | Value |
|---|---|
| `GENERATOR_PROJECT_ROOT` | `/home/cdsw` |
| `GENERATOR_STATE_DIR` | `/home/cdsw/data/state` |
| `GENERATOR_OUTPUT` | `/home/cdsw/data/public` |
| `GENERATOR_PRIVATE_OUTPUT` | `/home/cdsw/data/private` |
| `GENERATOR_CONFIG` | `config/smoke.yaml` (the UI refuses 50M; 50M runs from the CLI) |
| `GENERATOR_PUBLIC_ORIGIN` | the application's exact `https://…` URL |

Warehouse (project level or the loading Job; not needed by the Application):

| Variable | Value |
|---|---|
| `MERIDIAN_IMPALA_HOST` | `coordinator-impala-aws-vw.dw-applied-ai-cdp-env.jmgjgh.cloudera.site` |
| `MERIDIAN_IMPALA_PORT` | `443` |
| `MERIDIAN_IMPALA_AUTH_MECHANISM` | `LDAP` |
| `MERIDIAN_IMPALA_USE_HTTP` | `true` |
| `MERIDIAN_IMPALA_HTTP_PATH` | `cliservice` |
| `MERIDIAN_IMPALA_USE_SSL` | `true` |
| `MERIDIAN_IMPALA_TIMEOUT_S` | `3600` |
| `MERIDIAN_PUBLIC_WRITER_USER` | `cburns` |
| `MERIDIAN_PRIVATE_WRITER_USER` | `cburns` (a second login is better) |
| `MERIDIAN_AGENT_USER` | a **different** workload user: SELECT on `meridian_financial`, denied `meridian_ground_truth` in Ranger |
| `MERIDIAN_<ROLE>_PASSWORD` or `_PASSWORD_FILE` | per role, user-private or a 0600 file; never in shared project variables or git |
| `MERIDIAN_STAGING_URI` | `s3a://<env-bucket>/meridian/staging/public` (bucket still to be confirmed) |
| `MERIDIAN_PRIVATE_STAGING_URI` | `s3a://<env-bucket>/meridian/staging/private` |

Leave `MERIDIAN_IMPALA_QUERY_OPTIONS` unset on CDW (local emulation only). Pass
`MERIDIAN_STAGING_DIR` / `MERIDIAN_PRIVATE_STAGING_DIR` only on the `stage` command; if set
during `load`, it re-stages locally instead of reading the S3 copies.

## Known blockers

1. **Agent login:** if `MERIDIAN_AGENT_USER` is `cburns`, preflight blocks on identity
   separation and private isolation, and `load` refuses. Get a second workload user with the
   Ranger deny, or add an explicit, logged override for a one-off test.
2. **Staging databases:** pre-create `meridian_staging` and `meridian_private_staging`, or grant
   the writers CREATE.
3. **S3 copy step:** not automated. Find what the runtime offers (`hdfs dfs -put` to `s3a://`,
   or another approved tool) and add an `upload` subcommand to `scripts/warehouse.py`.
4. **SQLite on `/home/cdsw`** (network filesystem): cross-session locking is unverified. Only
   affects the UI job registry; the CLI and loader don't use it.
5. The CDP CLI key on the laptop was revoked (401), so bucket and app URL were not looked up.

## Next steps, in order

1. Python 3.11 runtime; `pip install -r requirements-warehouse.txt`.
2. Set the variables above; `python3 scripts/warehouse.py preflight` until no blockers.
3. **10k acceptance (M4 sign-off):** `cli.py --config config/smoke.yaml --rows 10000`, `stage`,
   copy to S3, `load`, then `reconcile_impala.py` if a passing `scenario_quality.json` exists.
4. **50M (M5 / Optiness D6):** generate `config/demo-50m.yaml` as a Job (~60 min, ≥4 GB,
   ~1.6 GB Parquet), stage, copy, load, then `reconcile_impala.py` against CDW. Record counts
   in Optiness `docs/dataset.md`.
5. Rotate the `cburns` workload password: it was shared in a chat session on 2026-10-21.
