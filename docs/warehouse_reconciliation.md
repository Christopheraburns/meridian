# Warehouse reconciliation

After a run is loaded into Impala/Iceberg (for the PrimeAgent demo, by PrimeAgent's
`make data`), `scripts/reconcile_impala.py` proves the warehouse carries exactly the
signal the private evaluator accepted. It emits one read-only query that rebuilds every
evaluator comparison group (bank phases, target/other/unaffected segments, exposed vs
holdback, canary vs steady, secondary incident, fraud bands) from the config, then
requires each group's attempts, declines and latency total to equal
`scenario_quality.json` exactly.

```bash
python3 scripts/reconcile_impala.py --config config/demo.yaml \
  --private-output data/demo-private --emit-sql > data/demo-private/reconcile.sql
# any operator Impala client; with the PrimeAgent stack, from its repo root:
docker compose --env-file .env -f deploy/compose/docker-compose.yml --profile tools run --rm -T \
  -v "$MERIDIAN/data/demo-private:/private:ro" impala-shell -B --output_delimiter=, \
  -f /private/reconcile.sql > "$MERIDIAN/data/demo-private/impala_groups.csv"
python3 scripts/reconcile_impala.py --config config/demo.yaml \
  --private-output data/demo-private --results data/demo-private/impala_groups.csv
```

The query names the target predicate, so its text and output stay in the private
directory (mode 0700) and must never run under, or be readable by, the investigator
identity. Results go to `impala_reconciliation.json` beside the evaluator report.

**2026-10-21, demo profile (2M rows, run `8e0ce02e`), local Impala 4.5.3:** 23/23 groups
matched exactly; the query took 4.9 s.
