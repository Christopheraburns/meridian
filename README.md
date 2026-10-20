# Meridian Financial Generator — v0.1

Portable generator for the Cloudera/Mistral recursive-lakehouse financial investigation demo. **All persons, institutions, merchants and transactions are synthetic.** No genuine customer data is used.

## Docker Desktop

```bash
docker compose up --build
```

Open http://localhost:8080 and click **Start generation**. Output persists to `./data`. Default generates **50,000,000** rows; edit `config/default.yaml` before startup for a smaller test. Docker Desktop should have enough local disk space for the data and sufficient container memory for a 250,000-row batch. Actual usage varies with compression.

## CLI / Workbench

```bash
python -m pip install -r requirements.txt
export PYTHONPATH=src
python cli.py --rows 100000 --output data
python app.py
```

For **Cloudera AI Workbench**, upload the repository to a project, install `requirements.txt` in a compatible Python runtime, and create an **Application** with entry script `app.py`. The server binds to `CDSW_APP_PORT` when present. For an offline/locked-down environment mirror the Python packages into your approved package repository. A custom runtime image, if needed, must be derived from an administrator-approved compatible Cloudera runtime—not this generic local Dockerfile. Put generated output on durable project storage, not ephemeral application filesystem.

## Design

- Seeded NumPy batches write **Parquet** files readable by Spark and suitable for subsequent Iceberg ingestion.
- Outputs: `payment_transactions/`, `dimensions/`, `public_manifest.json`, `configuration_public.json`, and `_private_ground_truth/incident.json`.
- The injected decline probability increases for cross-border Mastercard ecommerce travel (MCC 4722) routed through processor 3 during the incident period.
- Public transaction data contains `routing_rule_id=412` for the affected cohort; historical routing and deployment lookup tables allow model agents to investigate it.
- **Do not publish** `_private_ground_truth` through MCP or into an agent-accessible lakehouse namespace.
- **Important:** v0.1 writes Parquet, **not committed Iceberg tables**. The next milestone is a deployment-specific Spark/Iceberg ingestion job against a configured Cloudera warehouse/catalog. Do not just point PyIceberg at an unverified Cloudera Hive Metastore configuration.
- **Important:** v0.1 is a local/prototype generator with one worker and an in-process job state. The UI does not implement login itself; rely on Cloudera's app access control and do not expose its unauthenticated port to the public internet. No resumable/parallel generation yet.

## Tests

```bash
python -m pip install -r requirements-dev.txt
PYTHONPATH=src pytest -q
```

## Next phases

1. Add configurable profiles and richer coherent routing/event history, customer and merchant joins, and incident validation metrics.
2. Add durable batch ledger, resume/retry, workers, cancellation and integrity checks.
3. Add Spark-based writes/registration to governed Iceberg tables and Impala validation queries.
4. Add UI job configuration editor, progress charts, health metrics, separate private truth storage, and evaluation scenarios.
