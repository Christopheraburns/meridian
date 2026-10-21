# Meridian Financial Generator v0.2 — Codex Implementation Plan

**Purpose:** Evolve the existing v0.1 Flask/NumPy/PyArrow project into a repeatable, governed synthetic-financial-data factory. It must run under Docker Desktop and as a Cloudera AI Workbench application, and populate the **existing** Impala-visible Apache Iceberg tables without exposing planted ground truth to the investigation agent.

## 0. Non-negotiable context

- Existing repository: `meridian-financial-generator` v0.1. **Modify it incrementally; do not create a new project from scratch.**
- Authoritative schema files (already executed): `meridian_financial_iceberg.sql` and `meridian_financial_private_iceberg.sql`. Copy both into `sql/schema/` as immutable schema contracts for v0.2.
- Public database: **`meridian_financial`**. Private database: **`meridian_ground_truth`** (corrected to match the existing deployment; the private SQL contract retains its original filename).
- Existing public tables: `customers`, `merchants`, `payment_processors`, `payment_terminals`, `routing_rules`, `deployment_events`, `processor_incidents`, `transaction_response_codes`, `payment_transactions`, `generation_runs`.
- Existing private tables: `scenario_ground_truth`, `generation_batch_audit`.
- Public `payment_transactions` contains **28 columns**, including `generation_run_id`, and is partitioned by `DAYS(transaction_ts)`.
- All tables are already created; do **not** DROP/CREATE/ALTER them automatically. Fail with actionable errors when schema/capabilities do not match.
- First-class goal: **50,000,000 rows in `payment_transactions`**, not a claim that all tables combined equal 50M.
- Preserve local Parquet output and Docker convenience. No production credentials baked into the image or repository.

## 1. Architecture and deployment profiles

Components:
1. Flask **control plane** (`app.py`): create/validate configs, submit/cancel jobs, display durable progress, review output and validation. Must not run long-lived generation work in an in-process daemon thread.
2. Deterministic **generation core**: scenario config, entity creation, transaction distributions, incident injection, counterfactual accounting, validation.
3. **Execution adapters**: `local_parquet` (portable test/development); `spark_iceberg` (preferred scale/Workbench path); optional `impala_staged_load` (fallback only after environment validation).
4. **Job registry**: local SQLite + mounted persistent volume for Docker; configurable durable external backing/store for Workbench. Never assume project root is writable or persistent across app restarts.
5. **Catalog/warehouse clients**: separate identities for public data generation, private truth writing, and agent query access. Do not grant the agent client private access.

**Critical deployment preflight:** Workbench may not have a usable Spark cluster or an Iceberg catalog configured. Add a `preflight` command that verifies runtime, Spark availability, catalog implementation, table accessibility, credentials, schema compatibility, filesystem/warehouse permissions, and native Iceberg read/write/commit capability. If Spark-to-Iceberg cannot be validated, report a blocker and retain the local Parquet workflow; do not fake Iceberg by copying Parquet files into a warehouse path. Do not assume compatibility between CDP on-prem and public-cloud Spark examples.

## 2. Suggested repository structure

```text
app.py
cli.py
Dockerfile
docker-compose.yaml
requirements.txt
requirements-dev.txt
config/default.yaml
config/profiles/local.yaml
config/profiles/workbench.yaml
config/scenarios/payment_routing_v1.yaml
src/meridian/
  config.py
  models.py
  schema_contract.py
  scenario.py
  dimensions.py
  transactions.py
  incident.py
  validation.py
  jobs.py
  registry.py
  security.py
  writers/base.py
  writers/parquet.py
  writers/spark_iceberg.py
  catalog/impala.py
  web/routes.py
  web/templates/index.html
sql/schema/meridian_financial_iceberg.sql
sql/schema/meridian_financial_private_iceberg.sql
sql/validation/*.sql
tests/unit/*.py
tests/integration/*.py
tests/contract/*.py
scripts/preflight.py
scripts/smoke_test.sh
WORKBENCH.md
README.md
```

## 3. Typed configuration and compatibility

Use Pydantic models or dataclasses with explicit validation. Separate dataset parameters, scenario parameters, execution profile, and destination. Preserve existing v0.1 YAML keys through a migration/parser or a documented migration command.

Example:
```yaml
schema_version: '0.2'
name: meridian-payments
seed: 20261020
scale:
  transaction_rows: 50000000
  batch_size: 250000
  customers: 1000000
  merchants: 150000
  terminals: 250000
  processors: 8
  start_date: '2026-09-01'
  days: 60
scenario:
  id: payment_routing_v1
  incident_start: '2026-10-09T00:00:00'
  incident_end_exclusive: '2026-10-31T00:00:00'
  faulty_processor_id: 3
  faulty_configuration_version: '4.12'
  targeted_network: Mastercard
  targeted_channel: ecommerce
  targeted_mcc: '4722'
  cross_border_required: true
  baseline_decline_rate: 0.035
  affected_decline_rate: 0.24
  target_bankwide_decline_rate_increase_pp: 0.5
execution:
  engine: local_parquet  # spark_iceberg for Cloudera
  output_dir: /app/data
  database: meridian_financial
  private_database: meridian_ground_truth
  write_mode: new_run
```

**Important:** The parameters above are illustrative starting values, not proof the target impact is achievable. Model exposure and tune distributions in deterministic tests until bankwide increase and credible segment sample size are achieved. Reject infeasible scenario settings rather than silently producing an invisible incident.

## 4. Synthetic data modeling rules

- Use **keyed deterministic generation**: entity attributes and each transaction's random outcomes depend on `(seed, entity_id or transaction_id, feature namespace)`, not batch order or Spark partition order. Repartition/retry must not change results.
- Build all public tables to **exactly match the supplied DDL**: names, types, enum-like value conventions, decimal precision, timestamps and nullable fields. No extra columns without explicit schema migration.
- Enforce referential integrity: transaction customer, merchant, terminal, processor, routing rule, response code all exist; terminal belongs to transaction merchant; dates fit dimensional effective windows; deployed routing version and timestamps match appropriate rule histories.
- Merchants consistently determine MCC, industry, country, acquirer; customers consistently determine home country/segment; use real currency/country/card/channel-style codes but synthetic names/IDs only.
- Generate plausible daily/weekly payment volume, amount lognormal-type distribution with caps, card networks, regions, currency conversion and FX, authorization latency, declines and retries. Ensure `amount_usd` is correctly derived/rounded from `amount_original * fx_rate_to_usd`. Clearly define whether retry records count toward 50M.
- Generate **routing-rule history** where faulty rule IDs are used by approved *and* declined transactions and are not a perfect label for the incident. Include `route_family_id`, version, `effective_from_ts`/`effective_to_ts`, rollout percentage, processor routing and deployment linkage.
- Generate operational noise: a distinct temporary processor incident, normal fraud-related declines, weekend/holiday variability. Avoid making any distractor more causally explanatory than the primary incident.
- Add one targetable payment routing incident with an explainable **causal mechanism**: after version 4.12 rollout, a specific cross-border/ecommerce/Mastercard/travel segment is disproportionately rejected, conditional on actual routing exposure. Ensure temporal rollout and control groups permit before/after and affected/unaffected comparisons.
- Assign transaction outcomes probabilistically but deterministically using row-specific seeded variates. Track potential outcomes (baseline approval and injected approval) **only in private batch computations or truth**, not public transaction attributes. Never put `is_anomaly`, `root_cause`, or other giveaway fields in public tables.
- Compute `expected_excess_declines` from **realized counterfactual difference** per row, rather than `exposed * (rate delta)` alone; also record expected-value estimates as appropriate in `validation_notes`.
- Make target affected amount measure unambiguous: e.g. sum `amount_usd` for incremental declines, not all failed payments. Document distinction between declined payment *volume* and realized revenue/loss; don't label volume as bank loss.
- If a desired narrative requires a specific impact amount, tune/configure against that target and validate, rather than hard-coding an unsupported $18M loss claim.

## 5. Security and identities

- Separate credential providers: public writer, private truth writer, and read-only inspector/evaluator. UI never returns private truth to users operating as investigation roles.
- Mistral/MCP service identity has SELECT on `meridian_financial` only and must be explicitly denied private database access; test authorization with that identity.
- Do not use a single privileged warehouse login for both agent and generator.
- Secrets via environment variables, Workbench secret facilities or mounted secret files; never in YAML examples with real values, browser responses, or logs.
- Flask UI needs appropriate Workbench platform auth or its own access enforcement for job launch, job logs, and scenario configs. `/health` can stay unprivileged; sensitive endpoints must be protected in real deployment.

## 6. Safe write/commit strategy

- `generation_run_id` is generated per run. Record a config hash, seed, scenario, start/end dates, row counts, status in `meridian_financial.generation_runs`.
- For first prototype, require **single active writer per dataset** and `write_mode: new_run`. Reserve unique transaction IDs per run or enforce empty target for destructive/replace-like runs; never mix IDs from unrelated runs.
- Favor staged and validated run-scoped output before making a run queryable. Spark writes must use proper Iceberg catalog commits. Dimension tables must be available when transaction rows become queryable.
- **Failure semantics:** a multi-table write is not one atomic transaction. Implement commit manifest and recoverable job state, stage where feasible, validate row counts, and mark run `complete` only after every expected table has committed and tests pass. On failure mark `failed`/`partial`, never falsely `complete`.
- On restart, inspect durable manifest/committed batch identifiers and Iceberg data by `generation_run_id`; do not blindly replay committed appends. Avoid duplicate writes on Spark task retries. Define explicit cleanup of failed runs and retry behavior.
- Write private truth only using private credentials; write it **after** derived facts are measured, and do not emit private truth in ordinary UI/status endpoints.
- Optimize file size/partitioning after measuring actual plans; daily `DAYS(transaction_ts)` partition already exists and should not be silently altered. Avoid uncontrolled tiny files. Avoid holding 50M pandas rows in driver memory.

## 7. API and UI changes

Endpoints (with auth and error handling):
- `GET /` — dashboard: active run, recent runs, configuration, progress, outputs
- `GET /health` — liveness
- `GET /api/preflight` — environment and catalog readiness
- `POST /api/runs` — validate config, enqueue a generation run, return `run_id` and 202
- `GET /api/runs` — list persisted runs
- `GET /api/runs/{run_id}` — status, rows committed, metrics, errors **without ground truth**
- `POST /api/runs/{run_id}/cancel` — cooperative stop, safe cancellation; clear partial state
- `POST /api/runs/{run_id}/validate` — run **public-data** validation and report checks; private truth validation requires separate authorized evaluator endpoint/CLI, not generic UI
- `GET /api/config/schema` — configuration options for UI

Use a durable worker process/job queue rather than daemon threads. In Workbench, ensure worker survives normal web request lifetimes and document Workbench-specific job submission support. Use a simple process-based worker for Docker; future distributed engine handles compute. Show progress by verified written/committed row counts, not only generated-in-memory batches.

## 8. Validation and test requirements

Automated checks:
1. Exact transaction count (50M when full-scale); exact or specified counts for dimension tables; one completed `generation_runs` row per run.
2. Referential integrity and effective-date joins; transaction merchant's terminal matches; routing rule's version/deployment lineage matches event timelines.
3. Amount and currency consistency, meaningful distributions, timestamp bounds, row uniqueness, no unintended nulls.
4. Pre/post decline rates; exposure size; injected versus baseline decline counts; meaningful bankwide impact; distinct distractor incident.
5. No private truth available to agent identity; no giveaway columns in public schema or response.
6. Spark output readable from **Impala**, not just Spark; schema and decimal/timestamp handling compatible; Iceberg snapshots properly committed.
7. Determinism independent of batch size / partition count / worker retries.
8. Failure and resume test: interrupt after several batches, restart without duplicate transaction IDs/rows or incorrect `complete` state.
9. Local Docker API smoke test on **host port 8081**, container port **8080**; health and UI roots.
10. `pytest` unit and integration suites plus contract tests based on exact SQL DDL columns.

Sample read-only SQL checks (adapt for run IDs and scale):
```sql
SELECT generation_run_id, COUNT(*)
FROM meridian_financial.payment_transactions
GROUP BY generation_run_id;

SELECT COUNT(*) AS unmatched_merchants
FROM meridian_financial.payment_transactions t
LEFT JOIN meridian_financial.merchants m
  ON t.merchant_id = m.merchant_id
 AND t.generation_run_id = m.generation_run_id
WHERE m.merchant_id IS NULL;

SELECT CAST(transaction_ts AS DATE) AS event_date,
       COUNT(*) AS attempts,
       AVG(CASE WHEN approved THEN 1.0 ELSE 0.0 END) AS approval_rate
FROM meridian_financial.payment_transactions
GROUP BY CAST(transaction_ts AS DATE)
ORDER BY event_date;
```

**Contract caveat:** run-scoped dimensions should use the same `generation_run_id` as transaction facts. Query planners should filter to one run unless data is explicitly modeled for cross-run analysis.

## 9. Milestones and Codex task sequence

### M0 — Audit and freeze contracts
- Read all v0.1 code, README, existing tests and supplied SQL schemas.
- Produce `docs/schema_contract.md` and `docs/runtime_assumptions.md`.
- Identify existing mismatches and unresolved environment dependencies. No DB migrations.
- **Accept:** Python tests can introspect expected columns/types/table names from checked-in schema fixtures; all assumptions documented.

### M1 — Config, generation core and 10k-row test
- Typed config; deterministic per-ID functions; correct public schema for all 10 tables; scenario injection and realized counterfactual measurement.
- Keep a Parquet writer and CLI.
- **Accept:** Generate 10k transactions twice with different batch sizes; normalized output content hashes match; no orphan keys; all schema contract tests pass.

### M2 — Scenario quality and 1M-row statistical validation
- Routing history, deployments, secondary incidents, realistic population exposure, nontrivial investigation; repeatable scenario calibration.
- **Accept:** 1M-run has a bankwide observable impact, statistically strong target group, and competing hypotheses; private truth reconciles with observed data and is not public.

### M3 — Durable job runner + UI
- Durable registry, job lifecycle, start/status/cancel/retry, logs, config view, diagnostics and preflight UI. Fix Docker port host `8081:8080`.
- **Accept:** Docker Compose restart preserves history; cancelled/failed runs are not marked complete; no output duplication on restart.

### M4 — Cloudera Spark/Iceberg writer
- Environment-specific preflight first; integrate a tested Spark + Iceberg catalog writer **using approved CDP platform configuration**; bind tables to already-created names; write small run.
- **Accept:** 10k rows committed via Spark and queryable through Impala; count/type/join checks pass, and private truth is Ranger-isolated. If Spark cannot write to the existing catalog, stop with documented blocker and provide a supported fallback implementation plan.

### M5 — 50M run, recovery and performance
- Benchmark 1M, 10M, 50M. Tune Spark partitions, shuffle, and Iceberg file size. Add checkpoint/resume and validation report.
- **Accept:** 50M transaction rows committed, Impala count matches, all tables load, no duplicate IDs, incident is detectable, validation passes, and restart behavior demonstrated.

### M6 — Demo-readiness and handoff
- Provide a `scenario_manifest` with **public overview only**; keep detailed private truth in private table and secured evaluator.
- Supply demo investigation questions, sample Impala investigative SQL, evidence-capture templates, and clean operator runbook.
- **Accept:** An investigator credential cannot see ground truth; evaluator validates a sample agent report from generated records.

## 10. Codex operating instructions

Implement in **small, reviewable commits/milestones**. At each milestone: (a) show changed files, (b) describe schema compatibility, (c) run tests and provide actual output, (d) list blockers/assumptions, (e) avoid claiming anything tested without executing it. Stop and ask for *only essential Cloudera connection details* if real Iceberg integration is blocked; continue with local functionality otherwise. Do not invent secrets, CDP versions, endpoints, or Spark availability. Do not mutate existing Iceberg tables until explicit test credentials, database access, and write mode are validated. Do not run a 50M job without an explicit confirmation in the application.

## 11. Copy/paste starting prompt for Codex in Cursor

> You are working in the existing `meridian-financial-generator` repository. Implement Meridian Generator v0.2 following `MERIDIAN_V0_2_CODEX_DEVELOPMENT_PLAN.md`. Start with milestone M0 then M1 only; do not jump to the 50-million-row run. Read and preserve `sql/schema/meridian_financial_iceberg.sql` and `sql/schema/meridian_financial_private_iceberg.sql` as the exact contracts for the already-created Cloudera Iceberg tables. Do not CREATE/DROP/ALTER production tables. Keep Docker Desktop and Cloudera AI Workbench profiles, and preserve local Parquet output. Ensure deterministic per-transaction generation independent of batch size/partition order, referential integrity across all dimensions, a statistically meaningful payment-routing incident with private realized counterfactual ground truth, and comprehensive unit/contract tests. Keep the private database `meridian_ground_truth` inaccessible to the investigation agent. For M0/M1, deliver code plus tests, results, and a short list of questions that genuinely block M4 Spark/Iceberg integration. Follow the milestone acceptance criteria; do not claim unexecuted tests passed.
