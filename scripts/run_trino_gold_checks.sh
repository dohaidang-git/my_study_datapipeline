#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker_compat.sh"

echo "==> Show tables in hive.analytics"
docker exec trino trino --execute "SHOW TABLES FROM hive.analytics"

echo
echo "==> Count rows from gold tables"
docker exec trino trino --execute "SELECT COUNT(*) FROM hive.analytics.daily_sales_gold"
docker exec trino trino --execute "SELECT COUNT(*) FROM hive.analytics.category_sales_gold"
docker exec trino trino --execute "SELECT COUNT(*) FROM hive.analytics.customer_ltv_gold"
