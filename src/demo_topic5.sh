#!/usr/bin/env bash
set -euo pipefail

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

RESULTS_ROOT="${RESULTS_ROOT:-$PROJECT_ROOT/results}"

printf '\n[demo] Refreshing generated evidence from benchmark CSV files...\n'
"$PYTHON_BIN" "$PROJECT_ROOT/src/generate_evidence.py" \
  --results-root "$RESULTS_ROOT"

printf '\n[demo] Running terminal-first Spark demo pipeline...\n'
"$PYTHON_BIN" "$PROJECT_ROOT/src/demo_topic5_pipeline.py" "$@"
