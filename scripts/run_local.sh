#!/usr/bin/env bash
set -euo pipefail

ORDERS="${ORDERS:-500000}"
CUSTOMERS="${CUSTOMERS:-50000}"
PRODUCTS="${PRODUCTS:-5000}"

python -m src.generate_data \
  --orders "$ORDERS" \
  --customers "$CUSTOMERS" \
  --products "$PRODUCTS"

python -m src.bronze_ingest
python -m src.silver_transform
python -m src.quality_gate
python -m src.gold_models
python -m src.export_silver

(
  cd dbt
  dbt build --profiles-dir .
)

python -m src.benchmark --runs 3
