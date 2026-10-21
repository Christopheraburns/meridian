#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON_BIN="${PYTHON_BIN:-python3}"
export PYTHONPATH=src
"$PYTHON_BIN" -m pytest -q
# Pick an unused location: new_run intentionally refuses existing output.
SMOKE_ROOT="${1:-$(mktemp -d /tmp/meridian-smoke.XXXXXX)}"
"$PYTHON_BIN" cli.py --config config/smoke.yaml --output "$SMOKE_ROOT/public" --private-output "$SMOKE_ROOT/private" --rows 10000
