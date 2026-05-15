#!/usr/bin/env bash
set -euo pipefail

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

RESULTS_ROOT="${RESULTS_ROOT:-$PROJECT_ROOT/results}"

"$PYTHON_BIN" "$PROJECT_ROOT/src/generate_evidence.py" \
  --results-root "$RESULTS_ROOT" \
  --update-docs
