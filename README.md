# Meridian Financial Generator — v0.2 M0/M1

Synthetic financial payments for the existing Cloudera investigation demo. The
local generator now follows the existing Iceberg SQL contracts, with deterministic
per-ID generation, coherent dimensions and separate private counterfactual truth.
No real customer data or warehouse credentials are used.

## 10,000-row smoke test

```bash
python3 -m pip install -r requirements-dev.txt
PYTHONPATH=src python3 -m pytest -q
python3 cli.py --config config/smoke.yaml --output /tmp/meridian-public \
  --private-output /tmp/meridian-private --rows 10000
```

Choose fresh, empty output directories for each run. Or use
`bash scripts/smoke_test.sh`, which runs tests and a 10k CLI job in a fresh temporary
directory. Set PYTHON_BIN if your interpreter has a different name. The tests
generate 10k twice with batch sizes 731 and 2048 and compare normalized hashes,
all schemas, referential integrity, and exact realized anomaly accounting.

The existing flat config/default.yaml and CLI --rows/--max-batches options remain
supported; config/smoke.yaml demonstrates the typed v0.2 form. --batch-size changes
physical output batching; --run-id fixes logical run identity for reproducibility.
--max-batches intentionally creates a partial run, never a completed dataset.

## Docker Desktop

```bash
docker compose up --build
```

Open http://localhost:8081 (container port 8080). Existing output remains mounted
at ./data. Set GENERATOR_CONFIG=config/smoke.yaml for a small UI run and
GENERATOR_OUTPUT to a fresh directory on the mounted volume. For example use
`/app/data/smoke-public`; its private sibling `/app/data/smoke-public_private` will
also persist on that mount. Do not expose the private directory to investigators.
The default config retains your existing settings. The legacy single-user UI
remains a prototype until M3; use the CLI for reliable controlled execution.

## Workbench and output

Run `python3 app.py` as before; CDSW_APP_PORT is honored. See [WORKBENCH.md](WORKBENCH.md).
All ten public tables are written under the selected public output directory:
transaction parts in payment_transactions/, other tables in dimensions/. Private
truth and audits are in a separate 0700 directory, never under the public tree.
Only publish the public tree after public_manifest.json reports complete=true.

The generator writes **local Parquet**, not committed Iceberg tables. The SQL
files in sql/schema/ are immutable read-only contracts for tables already created.
Never execute these files as part of generation, or copy Parquet into an Iceberg
warehouse and treat it as committed data.

[Schema audit](docs/schema_contract.md) describes types and invariants.
[Runtime assumptions and M4 integration requirements](docs/runtime_assumptions.md)
document remaining dependencies and milestone boundaries. M2 statistical
calibration, M3 durable jobs, and M4 Spark integration are not implemented here.
No 50-million-row job has been run.
