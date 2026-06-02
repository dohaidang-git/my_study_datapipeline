#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker_compat.sh"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${PROJECT_ROOT}/reports/bi_demo_outputs"

mkdir -p "${OUTPUT_DIR}"

run_export() {
  local file_name="$1"
  local sql_text="$2"

  echo "Exporting ${file_name}.csv"
  docker exec trino trino \
    --catalog hive \
    --schema analytics \
    --output-format CSV_HEADER_UNQUOTED \
    --execute "${sql_text}" \
    > "${OUTPUT_DIR}/${file_name}.csv"
}

run_export "daily_sales_trend" "
SELECT order_date, order_count, item_count, payment_value, avg_order_payment_value
FROM hive.analytics.daily_sales_gold
ORDER BY order_date
"

run_export "monthly_sales_summary" "
SELECT date_trunc('month', order_date) AS sales_month,
       SUM(order_count) AS order_count,
       SUM(item_count) AS item_count,
       SUM(payment_value) AS total_payment_value
FROM hive.analytics.daily_sales_gold
GROUP BY 1
ORDER BY 1
"

run_export "top_categories_by_revenue" "
SELECT category_name,
       SUM(order_count) AS order_count,
       SUM(item_count) AS item_count,
       ROUND(SUM(gross_item_value), 2) AS gross_item_value
FROM hive.analytics.category_sales_gold
GROUP BY 1
ORDER BY gross_item_value DESC
LIMIT 15
"

run_export "top_customers_by_ltv" "
SELECT customer_id,
       customer_unique_id,
       customer_city,
       customer_state,
       order_count,
       ROUND(lifetime_value, 2) AS lifetime_value,
       ROUND(avg_order_value, 2) AS avg_order_value,
       first_order_date,
       last_order_date,
       ltv_rank
FROM hive.analytics.customer_ltv_gold
ORDER BY lifetime_value DESC, customer_id
LIMIT 20
"

echo "BI demo assets exported to ${OUTPUT_DIR}"
