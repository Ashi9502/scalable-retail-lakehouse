#!/usr/bin/env bash
set -euo pipefail
rm -rf data/raw data/warehouse data/exports data/duckdb
rm -f benchmarks/*.json
rm -rf dbt/target dbt/logs dbt/dbt_packages
echo "local generated artifacts removed"
