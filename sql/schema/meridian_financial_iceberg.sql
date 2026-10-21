-- Meridian Financial synthetic payments lakehouse, schema v0.2
-- Run with an Impala virtual warehouse connected to the desired Hive/Iceberg catalog.
-- Assumes the meridian_financial database already exists.
-- Run this script before launching the v0.2 generator; v0.1 output is not schema-compatible.

CREATE TABLE IF NOT EXISTS meridian_financial.customers (
  customer_id BIGINT,
  customer_type STRING,
  customer_segment STRING,
  home_country STRING,
  home_region STRING,
  onboarding_ts TIMESTAMP,
  risk_tier STRING,
  kyc_status STRING,
  preferred_currency STRING,
  is_active BOOLEAN,
  generation_run_id STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS meridian_financial.merchants (
  merchant_id BIGINT,
  merchant_name STRING,
  merchant_category_code STRING,
  merchant_category STRING,
  industry_group STRING,
  merchant_country STRING,
  merchant_region STRING,
  merchant_currency STRING,
  acquirer_id INT,
  merchant_size STRING,
  onboarded_ts TIMESTAMP,
  risk_tier STRING,
  is_active BOOLEAN,
  generation_run_id STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS meridian_financial.payment_processors (
  processor_id INT,
  processor_name STRING,
  processor_type STRING,
  operating_region STRING,
  headquarters_country STRING,
  settlement_currency STRING,
  service_tier STRING,
  is_active BOOLEAN,
  generation_run_id STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS meridian_financial.payment_terminals (
  terminal_id BIGINT,
  merchant_id BIGINT,
  terminal_type STRING,
  terminal_channel STRING,
  terminal_country STRING,
  device_vendor STRING,
  device_model STRING,
  software_version STRING,
  installed_ts TIMESTAMP,
  is_active BOOLEAN,
  generation_run_id STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS meridian_financial.routing_rules (
  routing_rule_id BIGINT,
  route_family_id BIGINT,
  configuration_version STRING,
  rule_priority INT,
  processor_id INT,
  fallback_processor_id INT,
  channel STRING,
  card_network STRING,
  cross_border_required BOOLEAN,
  currency_conversion_required BOOLEAN,
  merchant_category_code STRING,
  merchant_country STRING,
  effective_from_ts TIMESTAMP,
  effective_to_ts TIMESTAMP,
  rollout_percentage DECIMAL(5,2),
  routing_strategy STRING,
  change_ticket_id STRING,
  is_enabled BOOLEAN,
  generation_run_id STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS meridian_financial.deployment_events (
  deployment_id BIGINT,
  change_ticket_id STRING,
  component_name STRING,
  component_type STRING,
  processor_id INT,
  routing_rule_id BIGINT,
  previous_version STRING,
  deployed_version STRING,
  environment STRING,
  started_ts TIMESTAMP,
  completed_ts TIMESTAMP,
  deployment_status STRING,
  rollout_percentage DECIMAL(5,2),
  rollback_ts TIMESTAMP,
  release_notes STRING,
  generation_run_id STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS meridian_financial.processor_incidents (
  processor_incident_id BIGINT,
  processor_id INT,
  incident_type STRING,
  severity STRING,
  affected_region STRING,
  affected_channel STRING,
  started_ts TIMESTAMP,
  resolved_ts TIMESTAMP,
  reported_ts TIMESTAMP,
  status STRING,
  customer_impact_summary STRING,
  ticket_id STRING,
  generation_run_id STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS meridian_financial.transaction_response_codes (
  response_code STRING,
  response_category STRING,
  response_description STRING,
  is_approved BOOLEAN,
  is_retryable BOOLEAN,
  decline_type STRING,
  generation_run_id STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS meridian_financial.payment_transactions (
  transaction_id BIGINT,
  transaction_ts TIMESTAMP,
  customer_id BIGINT,
  merchant_id BIGINT,
  terminal_id BIGINT,
  processor_id INT,
  routing_rule_id BIGINT,
  amount_original DECIMAL(18,2),
  original_currency STRING,
  fx_rate_to_usd DECIMAL(18,8),
  amount_usd DECIMAL(18,2),
  card_network STRING,
  card_product STRING,
  payment_method STRING,
  channel STRING,
  card_present BOOLEAN,
  transaction_country STRING,
  issuing_country STRING,
  cross_border BOOLEAN,
  currency_conversion_required BOOLEAN,
  authorization_latency_ms INT,
  approved BOOLEAN,
  response_code STRING,
  retry_attempt INT,
  original_transaction_id BIGINT,
  transaction_status STRING,
  fraud_score DECIMAL(5,2),
  generation_run_id STRING
)
PARTITIONED BY SPEC (DAYS(transaction_ts))
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS meridian_financial.generation_runs (
  generation_run_id STRING,
  generator_version STRING,
  scenario_name STRING,
  seed BIGINT,
  configuration_hash STRING,
  requested_transaction_count BIGINT,
  generated_transaction_count BIGINT,
  generation_started_ts TIMESTAMP,
  generation_completed_ts TIMESTAMP,
  dataset_start_ts TIMESTAMP,
  dataset_end_ts TIMESTAMP,
  run_status STRING,
  validation_status STRING,
  notes STRING
) STORED BY ICEBERG TBLPROPERTIES ('format-version'='2');

-- Post-creation inspection (the tables remain empty until a writer commits data):
SHOW TABLES IN meridian_financial;
DESCRIBE meridian_financial.payment_transactions;
SHOW CREATE TABLE meridian_financial.payment_transactions;
