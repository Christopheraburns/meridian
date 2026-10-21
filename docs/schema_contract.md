# M0: Frozen Iceberg contracts

The two files in `sql/schema/` are byte-for-byte copies of the supplied files in
`meridian-v0.2-codex/sql/schema/`. Both copies now use the corrected deployed private database name,
`meridian_ground_truth`; the original SQL filenames are retained. Table columns
and types are unchanged. They describe tables that already exist. No code
executes these SQL files or creates, drops, or alters a database/table.

`meridian.schema_contract.contracts()` introspects table names, ordered columns,
and types directly from these fixtures. Arrow mapping: BIGINT → int64, INT →
int32, STRING → string, BOOLEAN → bool, TIMESTAMP → timezone-naive timestamp[us]
(UTC convention), DECIMAL(p,s) → decimal128(p,s). All DDL fields are nullable;
generated nulls are intentional only for rollback timestamps, approved response
code decline_type, original_transaction_id (no retries), and unfinished run times.
Local decimal arithmetic uses Decimal with ROUND_HALF_UP, including original
amount × eight-decimal FX rate rounded to two decimals. No float money columns.

Public database `meridian_financial` has ten tables:

| Table | Key | Local location |
| --- | --- | --- |
| customers | customer_id | dimensions/customers.parquet |
| merchants | merchant_id | dimensions/merchants.parquet |
| payment_processors | processor_id | dimensions/payment_processors.parquet |
| payment_terminals | terminal_id | dimensions/payment_terminals.parquet |
| routing_rules | routing_rule_id | dimensions/routing_rules.parquet |
| deployment_events | deployment_id | dimensions/deployment_events.parquet |
| processor_incidents | processor_incident_id | dimensions/processor_incidents.parquet |
| transaction_response_codes | response_code | dimensions/transaction_response_codes.parquet |
| payment_transactions | transaction_id | payment_transactions/part-*.parquet |
| generation_runs | generation_run_id | dimensions/generation_runs.parquet |

Every relationship includes generation_run_id; IDs are local to a run. The fact
contract has exactly 28 fields. MCC, merchant country, industry and acquirer are
obtained by joining merchants, not by adding redundant fact columns. All terminal
references belong to the referenced merchant. Customer country determines issuing
country; merchant country determines transaction country and currency. Reference
entities precede transaction dates. Rule windows are half-open, deployments
complete at rule activation, and old/new rules coexist during 80% rollout.
`ALL` is our documented wildcard convention for unrestricted rule predicates;
the DDL itself does not specify enum constraints. Currency/country codes are ISO
style. Routing `currency_conversion_required=False` means no routing requirement,
not a prohibition on FX transactions.

Private database `meridian_ground_truth` has `scenario_ground_truth` and
`generation_batch_audit`, written to a separate sibling directory (mode 0700).
`expected_excess_declines` contains the realized count of declined rows that would
have been approved absent injection. `expected_affected_amount_usd` sums only
those rows' USD payment volume: it is not realized bank loss or revenue.
Expected-value estimates and realized impact are stored in private validation_notes.
Batch audits are physical-write metadata and naturally differ by batch layout.

M0 audit of v0.1: only 14 fact columns, float amounts, second-resolution timestamps,
incorrect customer key/name fields, incomplete dimension schemas, absent terminal,
response-code, incident and run tables, orphan routing keys, inconsistent merchant
attributes, batch-seeded randomness, and a routing ID that directly labeled the
anomaly. Old private output was nested under the publishable root and statistics
could reach UI status. M1 replaces those paths while preserving entry points.

The existing Iceberg DAYS(transaction_ts) partition is unchanged. Local Parquet
files are not Iceberg commits. Contract tests prove local types/columns, not live
catalog compatibility or authorization. DDL has no uniqueness/FK constraints;
the generation core and validator enforce these locally.
