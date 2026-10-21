-- Run separately as a privileged administrator, NOT with the investigation agent credentials.
-- Do not grant SELECT on meridian_ground_truth to investigation agents.
CREATE DATABASE IF NOT EXISTS meridian_ground_truth;

CREATE TABLE IF NOT EXISTS meridian_ground_truth.scenario_ground_truth (
  generation_run_id STRING,
  incident_id STRING,
  incident_name STRING,
  incident_type STRING,
  incident_start_ts TIMESTAMP,
  incident_end_ts TIMESTAMP,
  injected_processor_id INT,
  injected_routing_rule_id BIGINT,
  injected_configuration_version STRING,
  predicate_json STRING,
  expected_exposed_transactions BIGINT,
  expected_excess_declines BIGINT,
  expected_affected_amount_usd DECIMAL(20,2),
  causal_explanation STRING,
  validation_notes STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS meridian_ground_truth.generation_batch_audit (
  generation_run_id STRING,
  batch_id INT,
  rows_written BIGINT,
  exposed_transactions BIGINT,
  injected_declines BIGINT,
  batch_hash STRING,
  written_ts TIMESTAMP,
  write_status STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');
