# Project Checklist

## Purpose

Checklist này phản ánh trạng thái hiện tại của project `bigdata_hudi` sau khi đã chạy được:

- pipeline `raw -> bronze -> silver -> gold`
- lưu trữ bằng `Hudi`
- query qua `Trino`
- orchestration bằng `Airflow`

Mục tiêu của file này là giúp trả lời nhanh:

1. phần nào đã xong
2. phần nào đang vận hành ổn
3. hằng ngày cần kiểm tra gì
4. bước tiếp theo nên làm gì

## Status legend

- `[x]` completed
- `[~]` in progress
- `[ ]` not started

## Current status snapshot

### Foundation and docs

- `[x]` repository structure created
- `[x]` architecture docs created
- `[x]` data mapping docs created
- `[x]` Python pipeline docs created
- `[x]` Hudi docs created
- `[x]` Airflow docs created
- `[x]` `.gitignore` created

### Local platform

- `[x]` `docker-compose.yml` created
- `[x]` `MinIO` configured and used as local object storage
- `[x]` `Hive Metastore + Postgres` configured
- `[x]` `Spark master + worker` configured
- `[x]` `Trino` configured
- `[x]` `Airflow 3` configured
- `[x]` execution API, auth, docker socket, and runtime issues fixed

### Data pipeline

- `[x]` raw Olist source files available
- `[x]` bronze jobs implemented
- `[x]` silver jobs implemented
- `[x]` gold jobs implemented
- `[x]` full Hudi pipeline verified with expected row counts
- `[x]` Trino gold query path verified
- `[x]` Airflow DAG `hudi_full_pipeline` runs successfully
- `[x]` data quality checks implemented for key silver and gold tables
- `[x]` freshness and reconciliation checks implemented for key silver and gold tables

### Remaining scope

- `[ ]` secondary ingestion sources such as API and Excel not implemented
- `[ ]` incremental Hudi write strategy not implemented yet
- `[ ]` dashboard / BI demo not implemented yet
- `[ ]` automated unit tests not implemented yet

## Layer status

### Raw

- `[x]` `data/raw/olist/` present
- `[x]` file names match pipeline expectations
- `[x]` source datasets verified manually

### Bronze

- `[x]` core tables loaded
- `[x]` supporting tables loaded
- `[x]` multiline CSV handling fixed for reviews
- `[x]` bronze Hudi record key strategy fixed to avoid dropping raw rows

Expected bronze Hudi tables:

- `[x]` `orders_bronze`
- `[x]` `order_items_bronze`
- `[x]` `customers_bronze`
- `[x]` `payments_bronze`
- `[x]` `products_bronze`
- `[x]` `sellers_bronze`
- `[x]` `reviews_bronze`
- `[x]` `geolocation_bronze`
- `[x]` `product_category_translation_bronze`

### Silver

- `[x]` dedup and clean logic implemented
- `[x]` source-aligned silver tables loaded
- `[x]` derived `products_silver` loaded

Expected silver Hudi tables:

- `[x]` `orders_silver`
- `[x]` `order_items_silver`
- `[x]` `customers_silver`
- `[x]` `payments_silver`
- `[x]` `products_silver_base`
- `[x]` `sellers_silver`
- `[x]` `reviews_silver`
- `[x]` `geolocation_silver`
- `[x]` `product_category_translation_silver`
- `[x]` `products_silver`

### Gold

- `[x]` `daily_sales_gold`
- `[x]` `category_sales_gold`
- `[x]` `customer_ltv_gold`
- `[x]` Hudi output verified
- `[x]` Trino queries verified

### Orchestration

- `[x]` Airflow DAG created
- `[x]` Airflow UI login working
- `[x]` DAG runtime issues fixed
- `[x]` Airflow executes Spark wrapper scripts through Docker
- `[x]` Airflow runs Hudi verify step
- `[x]` Airflow runs data quality checks
- `[x]` Airflow runs freshness and reconciliation checks
- `[x]` Airflow runs Trino smoke checks

## Daily operating checklist

### Infrastructure

- `[ ]` `docker compose ps` shows `minio`, `hive-metastore`, `spark-master`, `spark-worker`, `trino`, `airflow-*` as `Up`
- `[ ]` `spark-master` is reachable before any Airflow run
- `[ ]` `trino` is reachable before Trino smoke checks
- `[ ]` `airflow-scheduler` and `airflow-webserver` show no auth or execution API errors

Commands:

```bash
docker compose ps
docker compose logs --tail=80 airflow-scheduler airflow-webserver airflow-dag-processor
docker compose logs --tail=80 spark-master trino hive-metastore
```

### Data pipeline

- `[ ]` latest Airflow DAG run is `success`
- `[ ]` Hudi row-count verify passes
- `[ ]` data quality checks pass
- `[ ]` freshness and reconciliation checks pass
- `[ ]` Trino gold smoke queries pass

Commands:

```bash
docker exec airflow-webserver airflow dags list-runs -d hudi_full_pipeline
bash scripts/spark_submit_container.sh pipelines/tools/verify_hudi_pipeline.py
bash scripts/spark_submit_container.sh pipelines/tools/run_data_quality_checks.py
bash scripts/spark_submit_container.sh pipelines/tools/run_freshness_reconciliation_checks.py
bash scripts/run_trino_gold_checks.sh
```

## Manual execution checklist

### Start the full local stack

```bash
docker compose up -d minio minio-init metastore-postgres hive-metastore spark-master spark-worker trino
docker compose up -d airflow-postgres airflow-webserver airflow-dag-processor airflow-scheduler
```

### Run the full Hudi pipeline manually

```bash
bash scripts/run_hudi_full_pipeline.sh
```

### Verify the Hudi pipeline manually

```bash
bash scripts/spark_submit_container.sh pipelines/tools/verify_hudi_pipeline.py
```

### Run data quality checks manually

```bash
bash scripts/spark_submit_container.sh pipelines/tools/run_data_quality_checks.py
```

### Run freshness and reconciliation checks manually

```bash
bash scripts/spark_submit_container.sh pipelines/tools/run_freshness_reconciliation_checks.py
```

### Run Trino gold smoke checks manually

```bash
bash scripts/run_trino_gold_checks.sh
```

### Trigger the Airflow DAG

```bash
docker exec airflow-webserver airflow dags trigger hudi_full_pipeline
```

## What the quality checks cover

Current data quality checks focus on semantic integrity for key `silver` and `gold` tables.

### Silver checks

- `[x]` `orders_silver`
  - `order_id`, `customer_id`, `order_purchase_date` are not null
  - `order_id` is unique
  - `order_status` is trimmed lowercase
- `[x]` `customers_silver`
  - `customer_id`, `customer_unique_id`, `customer_state` are not null
  - `customer_id` is unique
  - `customer_state` has length `2`
- `[x]` `payments_silver`
  - `payment_key`, `order_id`, `payment_type` are not null
  - `payment_key` is unique
  - `payment_value >= 0`
  - `payment_installments > 0`
- `[x]` `reviews_silver`
  - `review_id`, `order_id`, `review_score` are not null
  - `review_id` is unique
  - `review_score` is between `1` and `5`
- `[x]` `geolocation_silver`
  - `geolocation_key`, `geolocation_zip_code_prefix`, `geolocation_state` are not null
  - `geolocation_key` is unique
  - `geolocation_state` has length `2`
- `[x]` `products_silver`
  - `product_id` is not null
  - `product_id` is unique

### Gold checks

- `[x]` `daily_sales_gold`
  - `order_date` is not null
  - grain by `order_date` is unique
  - `order_count > 0`
  - `payment_value >= 0`
  - `gross_item_value >= 0`
- `[x]` `category_sales_gold`
  - `order_purchase_date` and `category_name` are not null
  - grain by `order_purchase_date + category_name` is unique
  - gross values are not negative
  - counts are positive
- `[x]` `customer_ltv_gold`
  - `customer_id`, `customer_unique_id`, `order_count`, `lifetime_value`, `ltv_rank` are not null
  - `customer_id` is unique
  - `ltv_rank` is unique
  - `order_count > 0`
  - `lifetime_value >= 0`
  - `first_order_date <= last_order_date`

## Definition of done for current MVP

Current MVP is considered done when all items below hold:

- `[x]` data can be ingested into Hudi
- `[x]` `bronze -> silver -> gold` runs end to end
- `[x]` Hudi outputs can be verified by Spark
- `[x]` gold marts can be queried by Trino
- `[x]` pipeline can be orchestrated by Airflow
- `[x]` quality checks can be run automatically
- `[x]` freshness and reconciliation checks can be run automatically

## Next technical priorities

### Priority 1

- `[ ]` add incremental or upsert-oriented Hudi strategy where appropriate
- `[ ]` add partition and freshness monitoring
- `[ ]` add retry and alerting policy for Airflow tasks

### Priority 2

- `[ ]` add unit tests for shared Python helpers
- `[ ]` add integration tests for critical tables
- `[ ]` add CI commands for syntax and smoke validation

### Priority 3

- `[ ]` add dashboard or BI demo on top of `hive.analytics.*`
- `[ ]` add more business marts beyond the current gold set
