#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

jobs=(
  "pipelines/bronze/load_orders_bronze.py"
  "pipelines/bronze/load_order_items_bronze.py"
  "pipelines/bronze/load_customers_bronze.py"
  "pipelines/bronze/load_payments_bronze.py"
  "pipelines/bronze/load_products_bronze.py"
  "pipelines/bronze/load_sellers_bronze.py"
  "pipelines/bronze/load_reviews_bronze.py"
  "pipelines/bronze/load_geolocation_bronze.py"
  "pipelines/bronze/load_product_category_translation_bronze.py"
)

for job in "${jobs[@]}"; do
  echo
  echo "==> Running ${job}"
  bash "${PROJECT_ROOT}/scripts/spark_submit_container.sh" "${job}" --output-format hudi
done
