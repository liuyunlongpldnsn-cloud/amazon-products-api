#!/usr/bin/env bash
set -euo pipefail

QUERY="${1:-wireless earbuds}"

python3 -m src.run_pipeline --query "$QUERY"
