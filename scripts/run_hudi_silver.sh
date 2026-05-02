#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

jobs=(
  "pipelines/silver/load_orders_silver.py"
  "pipelines/silver/load_order_items_silver.py"
  "pipelines/silver/load_customers_silver.py"
  "pipelines/silver/load_payments_silver.py"
  "pipelines/silver/load_products_silver_base.py"
  "pipelines/silver/load_sellers_silver.py"
  "pipelines/silver/load_reviews_silver.py"
  "pipelines/silver/load_geolocation_silver.py"
  "pipelines/silver/load_product_category_translation_silver.py"
  "pipelines/silver/load_products_silver.py"
)

for job in "${jobs[@]}"; do
  echo
  echo "==> Running ${job}"
  bash "${PROJECT_ROOT}/scripts/spark_submit_container.sh" "${job}" --input-format hudi --output-format hudi
done
