#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[1/3] Run bronze orders job in Hudi mode on Spark 3.5.8 container"
bash "${PROJECT_ROOT}/scripts/spark_submit_container.sh" \
  pipelines/bronze/load_orders_bronze.py \
  --output-format hudi

echo
echo "[2/3] Read back Hudi table with Spark"
bash "${PROJECT_ROOT}/scripts/spark_submit_container.sh" \
  pipelines/tools/verify_hudi_orders_read.py

echo
echo "[3/3] List Hudi metadata files from MinIO"
docker exec minio mc ls --recursive local/lakehouse/bronze/orders_bronze/.hoodie
