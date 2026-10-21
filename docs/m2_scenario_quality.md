# M2: scenario quality and local one-million-row acceptance

M2 extends the existing generator; schemas, local Parquet layout, Docker and
Workbench entry points are unchanged. `meridian_ground_truth` remains the private
database. There are no Spark/catalog operations or database migrations.

## Repeatable calibration and acceptance

```bash
# Optional: derive an operator configuration using the shared calendar/rollout model.
# The destination must be a new file. Do not publish scenario configs to investigators.
python3 scripts/calibrate_scenario.py --config config/m2.yaml --output /tmp/calibrated-m2.yaml

# Generate the checked-in calibrated scenario, then read back and evaluate all rows.
# Both output directories must be new/empty.
python3 scripts/validate_scenario.py --config config/m2.yaml \
  --output data/m2-public --private-output data/m2-private --generate --require-m2

# Re-evaluate existing output without generating or overwriting transactions.
python3 scripts/validate_scenario.py --config config/m2.yaml \
  --output data/m2-public --private-output data/m2-private --require-m2

PYTHONPATH=src python3 -m pytest -q
```

The 1M acceptance job is explicit rather than part of every fast pytest run.
`--require-m2` exits nonzero if any acceptance criterion fails. Detailed results
are written only to `data/m2-private/scenario_quality.json` under mode 0700.
The evaluator invalidates any stale success report before reading files again.
A generation manifest marked complete means the structurally validated dataset
was written successfully; M2 scenario acceptance additionally requires the private
quality report to pass. The evaluator does not change the generated tables.

The calibration integrates the exact weekday/weekend/holiday weights and canary
windows used by generation. It solves the target candidate share for a 25%
expected margin above the requested minimum impact. The bound excludes extra
organic exposure. Configurations that cannot meet their requested expected impact
are rejected; realized acceptance is measured, never assumed from the formula.
The result is deterministic and independent of row batching. This is an intentionally
travel-heavy demonstration portfolio, not a calibrated representation of a real
bank. The checked-in profile includes all four channels and multiple countries,
currencies, networks and merchants. It has 1M attempts over 75 days, including
pre-incident, incident and recovery periods.

## Causal model and competing explanations

The target is cross-border Mastercard ecommerce travel payments. Explicit target
candidate traffic is 19.77% of attempts; 70% of that traffic routes to processor 3,
and the rest supplies same-segment controls on other processors. Ordinary traffic
can independently match the target predicate. Target membership and processor
assignment do not depend on the incident time or approval outcome.

Processor 3 moves from version 4.11 through a two-day 20% canary of 4.12, then an
80% rollout with a 20% contemporaneous old-version holdback. Every phase has
matching rule/deployment records, half-open effective windows, a shared route
family, and allocations summing to 100%. Version 4.13 follows the exclusive
incident end and restores baseline outcomes. Other processors have an unrelated
routine 4.10→4.11 deployment. IDs identify routing histories, not anomaly labels;
new-version transactions include approvals, declines and unaffected segments.
The private truth's singular rule ID identifies the expanded rollout; all injected
rule IDs are recorded in its existing validation_notes JSON.

The causal mechanism is an overly restrictive issuer-country check introduced by
4.12 for the targeted network/channel/MCC combination. Conditional decline
probability rises only for traffic actually assigned to that version. The same
per-transaction uniform draw is compared to baseline and injected thresholds.
High fraud-score traffic has a higher baseline decline probability; the 90%/10%
risk mixture preserves the configured mean baseline rate. A distinct processor
has a 12-hour degradation during the incident, with extra latency and declines.
Neither that degradation nor fraud provides an explanation for the sustained
routing-specific segment increase. Dates retain weekend/holiday volume effects.

Each row is an original authorization attempt; retry simulation remains disabled
and is not required for M2 acceptance. Amounts and FX use the existing Decimal
rules. The dataset model fingerprint is now `m2-v1`; reproducibility is within a
model revision, not equality to M1's simpler distribution. At most 100 processors
are supported by this revision's documented routing ID namespace.

## Acceptance evidence

The evaluator checks all persisted schemas, generation identity, contiguous unique
transaction IDs, dimension joins, effective dates and FX relationships. It
reconstructs each row's baseline/injected outcome and reconciles actual declines,
exposure, incremental declines and Decimal payment volume with private truth. Each
persisted part is also reconciled with its private audit row and content hash.

Statistical acceptance requires:

- At least 1M rows and at least 1,000 observations in each principal control group.
- A positive 95% interval for bankwide incident-versus-before decline-rate change.
- Realized incremental declines divided by **all dataset attempts**, in percentage
  points, at or above the configured 0.5 pp target. This differs from the public
  incident-versus-before comparison, which has different denominators.
- Exposed-versus-holdback decline-rate difference with a 95% lower bound above
  10 pp, and a same-segment/other-processor difference-in-differences lower bound
  above 5 pp.
- Unaffected-segment and other-processor drift below 1 pp, and at least 1,000
  recovery-segment attempts whose decline rate returns to within 1 pp of baseline.
- A visible secondary incident with at least 100 attempts, a positive decline-rate
  interval, and more than 400 ms additional mean latency. Its realized incremental
  declines must be less than one tenth of the primary incident's.
- Detectable fraud-related declines, unaffected new-version traffic, and both
  approvals and declines among new-version target traffic.

Intervals are unpooled large-sample normal difference intervals for fixed,
predeclared synthetic cohorts, not a search over arbitrary segments. Reports show
sample counts, rates, daily counts, intervals and counterfactual totals. Incremental
payment volume is not revenue or bank loss. Config, source, private reports and
truth are evaluator/operator material and must not be included in investigator
mounts; publish only the public output tree. Live Ranger denial tests remain M4.

M3 durable jobs are documented in [M3 jobs](m3_jobs.md). M4 Cloudera integration
remains pending. No 50M job is part of this milestone.

## Executed validation

The final fast suite passed **17 tests** locally. The Python 3.11 / PyArrow 23
Docker environment passed the preceding 15-test suite and independently read back
and accepted the full **1,000,000-row** output. The final two local tests additionally
exercise repeatable calibration from an underexposed configuration and a dataset
that contains only the canary phase. All 14 M2 quality checks passed for the
checked-in config. SQL schema fixtures were not changed. Generated public data
and private reports are excluded from Git; detailed measured results remain in
the private report rather than investigator-facing artifacts.
