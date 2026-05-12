#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ENV_FILE="${ENV_FILE:-$DEFAULT_PROJECT_ROOT/.env}"
if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck source=/dev/null
  . "$ENV_FILE"
  set +a
fi

PROJECT_ROOT="${PROJECT_ROOT:-$DEFAULT_PROJECT_ROOT}"
PYTHON_BIN="${PYTHON_BIN:-$PROJECT_ROOT/venv/bin/python}"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="${PYTHON_BIN_FALLBACK:-python3.12}"
fi

if [ -z "${JAVA_HOME:-}" ] && [ -x "$HOME/.local/share/jdks/temurin-21/bin/java" ]; then
  export JAVA_HOME="$HOME/.local/share/jdks/temurin-21"
  export PATH="$JAVA_HOME/bin:$PATH"
fi

export PROJECT_ROOT
export PYTHONPATH="$PROJECT_ROOT/src:${PYTHONPATH:-}"
