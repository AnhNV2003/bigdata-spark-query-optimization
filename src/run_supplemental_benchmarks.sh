#!/usr/bin/env bash
set -euo pipefail

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

RESULTS_ROOT="${RESULTS_ROOT:-$PROJECT_ROOT/results}"

if [ "$#" -gt 0 ]; then
  "$PYTHON_BIN" "$PROJECT_ROOT/src/run_supplemental_benchmarks.py" \
    --results-root "$RESULTS_ROOT" \
    "$@"
else
  "$PYTHON_BIN" "$PROJECT_ROOT/src/run_supplemental_benchmarks.py" \
    --results-root "$RESULTS_ROOT"
fi
