#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

jobs=(
  "pipelines/gold/build_daily_sales_gold.py"
  "pipelines/gold/build_category_sales_gold.py"
  "pipelines/gold/build_customer_ltv_gold.py"
)

for job in "${jobs[@]}"; do
  echo
  echo "==> Running ${job}"
  bash "${PROJECT_ROOT}/scripts/spark_submit_container.sh" "${job}" --input-format hudi --output-format hudi
done
