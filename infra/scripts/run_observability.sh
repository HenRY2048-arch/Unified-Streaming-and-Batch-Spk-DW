#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

exec .observability-venv/bin/streamlit run infra/observability/app.py \
  --server.address 0.0.0.0 \
  --server.port "${CRT_OBSERVABILITY_PORT:-8501}"
