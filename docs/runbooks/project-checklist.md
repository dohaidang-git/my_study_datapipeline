# Project Checklist

## Purpose

This checklist tracks the end-to-end work needed to complete the local Apache Hudi e-commerce pipeline. It is designed to be used as an execution checklist, not just as a high-level roadmap.

Use this file to answer four questions at any point:

1. What has already been completed?
2. What still needs to be implemented?
3. What should be checked every day while developing?
4. How should each phase be tested before moving on?

## Status legend

- `[x]` completed
- `[~]` in progress or partially done
- `[ ]` not started

## Current project status snapshot

### Foundation

- `[x]` repository structure created
- `[x]` architecture docs created
- `[x]` mapping doc created
- `[x]` Docker stack created
- `[x]` core Docker services brought up successfully: `minio`, `metastore-postgres`, `hive-metastore`, `spark-master`, `spark-worker`, `trino`
- `[x]` `.gitignore` added

### Raw data

- `[x]` Olist source files present under `data/raw/olist/`
- `[ ]` `products_api` source not implemented with real data yet
- `[ ]` secondary source `online_retail_II.xlsx` not integrated

### Bronze layer

- `[x]` common bronze helpers implemented
- `[x]` bronze jobs created for core entities
- `[x]` bronze jobs created for supporting entities
- `[~]` core bronze outputs created for main tables
- `[~]` supporting bronze outputs need final rerun and verification after the `reviews` parser fix

### Silver layer

- `[x]` common silver helpers implemented
- `[x]` silver jobs created for core entities
- `[x]` silver jobs created for supporting entities
- `[x]` derived `products_silver` job created
- `[ ]` silver jobs not fully executed and validated yet

### Gold layer

- `[ ]` gold jobs not started
- `[ ]` gold output tables not started
- `[ ]` SQL validation queries for gold not started

### Orchestration and serving

- `[ ]` Airflow DAGs not implemented
- `[ ]` Hudi output mode not fully exercised
- `[ ]` Trino table registration and query flow not implemented
- `[ ]` dashboard or BI layer not implemented

## Phase-by-phase checklist

## Phase 1: Project and environment setup

### Checklist

- `[x]` create repo folder structure
- `[x]` create `README.md`
- `[x]` create architecture docs
- `[x]` create Docker stack docs
- `[x]` create `.gitignore`

### Files to inspect

- [README.md](/home/dohaidang/bigdata_hudi/README.md:1)
- [docs/architecture/project-structure.md](/home/dohaidang/bigdata_hudi/docs/architecture/project-structure.md:1)
- [docs/architecture/system-design.md](/home/dohaidang/bigdata_hudi/docs/architecture/system-design.md:1)
- [docs/architecture/data-mapping.md](/home/dohaidang/bigdata_hudi/docs/architecture/data-mapping.md:1)
- [docs/runbooks/docker-stack.md](/home/dohaidang/bigdata_hudi/docs/runbooks/docker-stack.md:1)

### Verification

- inspect the repo tree
- confirm the docs match the intended architecture

Commands:

```bash
find . -maxdepth 3 \( -type f -o -type d \) | sort
```

## Phase 2: Docker and local platform

### Checklist

- `[x]` create `docker-compose.yml`
- `[x]` configure MinIO
- `[x]` configure Hive Metastore + Postgres
- `[x]` configure Trino
- `[x]` configure Spark master and worker
- `[x]` configure Airflow images and services
- `[x]` fix image tag issues
- `[x]` fix Hive metastore JDBC issue
- `[x]` fix Trino S3 config and writable path issue

### Files to inspect

- [docker-compose.yml](/home/dohaidang/bigdata_hudi/docker-compose.yml:1)
- [docker/hive/Dockerfile](/home/dohaidang/bigdata_hudi/docker/hive/Dockerfile:1)
- [docker/trino/catalog/hive.properties](/home/dohaidang/bigdata_hudi/docker/trino/catalog/hive.properties:1)
- [docker/trino/node.properties](/home/dohaidang/bigdata_hudi/docker/trino/node.properties:1)
- [configs/spark/spark-defaults.conf](/home/dohaidang/bigdata_hudi/configs/spark/spark-defaults.conf:1)
- [docker/spark/start-master.sh](/home/dohaidang/bigdata_hudi/docker/spark/start-master.sh:1)
- [docker/spark/start-worker.sh](/home/dohaidang/bigdata_hudi/docker/spark/start-worker.sh:1)

### Verification

- check service status
- inspect health for MinIO and Trino
- inspect logs for Hive and Trino

Commands:

```bash
docker compose ps
docker compose logs --tail=120 hive-metastore
docker compose logs --tail=120 trino
curl http://localhost:8081/v1/info
```

### Daily checks for this phase

- `docker compose ps` shows critical services as `Up`
- `trino` is `healthy`
- `hive-metastore` stays up and does not restart-loop
- `spark-master` and `spark-worker` remain up

## Phase 3: Raw data readiness

### Checklist

- `[x]` confirm Olist CSV files exist
- `[x]` inspect sample rows from core files
- `[x]` confirm row counts roughly match expectations
- `[ ]` add optional API extraction source later
- `[ ]` decide whether the Excel dataset is in scope later

### Files to inspect

- `data/raw/olist/olist_orders_dataset.csv`
- `data/raw/olist/olist_order_items_dataset.csv`
- `data/raw/olist/olist_customers_dataset.csv`
- `data/raw/olist/olist_order_payments_dataset.csv`
- `data/raw/olist/olist_products_dataset.csv`
- `data/raw/olist/olist_order_reviews_dataset.csv`

### Verification

Commands:

```bash
find data/raw -maxdepth 3 \( -type f -o -type d \) | sort
sed -n '1,5p' data/raw/olist/olist_orders_dataset.csv
sed -n '1,5p' data/raw/olist/olist_order_items_dataset.csv
wc -l data/raw/olist/*.csv
```

### Daily checks for this phase

- source files are still present
- filenames match the expected paths used by jobs
- no one accidentally edits raw files manually

## Phase 4: Bronze layer

### Bronze implementation checklist

#### Core bronze jobs

- `[x]` `load_orders_bronze.py`
- `[x]` `load_order_items_bronze.py`
- `[x]` `load_customers_bronze.py`
- `[x]` `load_payments_bronze.py`
- `[x]` `load_products_bronze.py`

#### Supporting bronze jobs

- `[x]` `load_sellers_bronze.py`
- `[x]` `load_reviews_bronze.py`
- `[x]` `load_geolocation_bronze.py`
- `[x]` `load_product_category_translation_bronze.py`

#### Bronze helper code

- `[x]` Spark session helper
- `[x]` path helper
- `[x]` metadata helper
- `[x]` Hudi writer helper
- `[x]` bronze writer helper
- `[x]` direct script import-path fix
- `[x]` CSV reader override support for multiline files

### Files to inspect

- [pipelines/common/bronze_job.py](/home/dohaidang/bigdata_hudi/pipelines/common/bronze_job.py:1)
- [pipelines/common/hudi_writer.py](/home/dohaidang/bigdata_hudi/pipelines/common/hudi_writer.py:1)
- [pipelines/bronze](/home/dohaidang/bigdata_hudi/pipelines/bronze)

### Bronze execution checklist

- `[x]` `orders_bronze` run and output created
- `[x]` `order_items_bronze` run and output created
- `[x]` `customers_bronze` run and output created
- `[x]` `payments_bronze` run and output created
- `[x]` `products_bronze` run and output created
- `[~]` `reviews_bronze` must be rerun after multiline parser fix and rechecked
- `[~]` `sellers_bronze` verify output exists
- `[~]` `geolocation_bronze` verify output exists
- `[~]` `product_category_translation_bronze` verify output exists

### Commands to run bronze jobs

Core:

```bash
spark-submit pipelines/bronze/load_orders_bronze.py --output-format parquet --output-path data/bronze/orders_bronze
spark-submit pipelines/bronze/load_order_items_bronze.py --output-format parquet --output-path data/bronze/order_items_bronze
spark-submit pipelines/bronze/load_customers_bronze.py --output-format parquet --output-path data/bronze/customers_bronze
spark-submit pipelines/bronze/load_payments_bronze.py --output-format parquet --output-path data/bronze/payments_bronze
spark-submit pipelines/bronze/load_products_bronze.py --output-format parquet --output-path data/bronze/products_bronze
```

Supporting:

```bash
spark-submit pipelines/bronze/load_sellers_bronze.py --output-format parquet --output-path data/bronze/sellers_bronze
spark-submit pipelines/bronze/load_reviews_bronze.py --output-format parquet --output-path data/bronze/reviews_bronze
spark-submit pipelines/bronze/load_geolocation_bronze.py --output-format parquet --output-path data/bronze/geolocation_bronze
spark-submit pipelines/bronze/load_product_category_translation_bronze.py --output-format parquet --output-path data/bronze/product_category_translation_bronze
```

### Bronze validation checklist

For every bronze table:

- `[ ]` output directory exists
- `[ ]` `_SUCCESS` exists when non-partitioned output is used
- `[ ]` partition directories exist where expected
- `[ ]` schema types are correct for key columns
- `[ ]` metadata columns exist
- `[ ]` row count is reasonable compared to source
- `[ ]` sample rows look aligned and not column-shifted

### Bronze validation commands

Check directories:

```bash
find data/bronze -maxdepth 2 \( -type f -o -type d \) | sort
```

Check schema:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("check_bronze_schema").getOrCreate()
for path in [
    "data/bronze/orders_bronze",
    "data/bronze/order_items_bronze",
    "data/bronze/customers_bronze",
    "data/bronze/payments_bronze",
    "data/bronze/products_bronze",
]:
    print(f"\n=== {path} ===")
    spark.read.parquet(path).printSchema()
spark.stop()
PY
```

Check counts:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("check_bronze_counts").getOrCreate()
for path in [
    "data/bronze/orders_bronze",
    "data/bronze/order_items_bronze",
    "data/bronze/customers_bronze",
    "data/bronze/payments_bronze",
    "data/bronze/products_bronze",
]:
    df = spark.read.parquet(path)
    print(path, df.count())
spark.stop()
PY
```

Check sample rows:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("check_bronze_samples").getOrCreate()
spark.read.parquet("data/bronze/orders_bronze").show(5, truncate=False)
spark.stop()
PY
```

### Daily checks for bronze

- rerun failed jobs immediately after code fix
- confirm no table was written with shifted columns
- confirm no cast errors are hidden in logs
- confirm output path names match mapping doc
- avoid mixing `parquet` and `hudi` outputs in the same path

## Phase 5: Silver layer

### Silver implementation checklist

#### Core silver jobs

- `[x]` `load_orders_silver.py`
- `[x]` `load_order_items_silver.py`
- `[x]` `load_customers_silver.py`
- `[x]` `load_payments_silver.py`
- `[x]` `load_products_silver_base.py`

#### Supporting silver jobs

- `[x]` `load_sellers_silver.py`
- `[x]` `load_reviews_silver.py`
- `[x]` `load_geolocation_silver.py`
- `[x]` `load_product_category_translation_silver.py`

#### Derived silver jobs

- `[x]` `load_products_silver.py`
- `[ ]` `products_api_silver` not implemented yet
- `[ ]` `products_current_silver` not implemented yet
- `[ ]` other derived silver views not implemented yet

### Silver execution checklist

- `[ ]` `orders_silver` run and output verified
- `[ ]` `order_items_silver` run and output verified
- `[ ]` `customers_silver` run and output verified
- `[ ]` `payments_silver` run and output verified
- `[ ]` `products_silver_base` run and output verified
- `[ ]` `sellers_silver` run and output verified
- `[ ]` `reviews_silver` run and output verified
- `[ ]` `geolocation_silver` run and output verified
- `[ ]` `product_category_translation_silver` run and output verified
- `[ ]` `products_silver` run and output verified

### Commands to run silver jobs

Core:

```bash
spark-submit pipelines/silver/load_orders_silver.py
spark-submit pipelines/silver/load_order_items_silver.py
spark-submit pipelines/silver/load_customers_silver.py
spark-submit pipelines/silver/load_payments_silver.py
spark-submit pipelines/silver/load_products_silver_base.py
```

Supporting and derived:

```bash
spark-submit pipelines/silver/load_sellers_silver.py
spark-submit pipelines/silver/load_reviews_silver.py
spark-submit pipelines/silver/load_geolocation_silver.py
spark-submit pipelines/silver/load_product_category_translation_silver.py
spark-submit pipelines/silver/load_products_silver.py
```

### Silver validation checklist

For every silver table:

- `[ ]` output directory exists
- `[ ]` deduplicated keys behave as expected
- `[ ]` text normalization is visible in sample rows
- `[ ]` numeric types are preserved
- `[ ]` no obviously malformed rows remain
- `[ ]` derived columns exist where expected
- `[ ]` joins do not duplicate unexpectedly

### Silver validation commands

Schema and samples:

```bash
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("check_silver").getOrCreate()
for path in [
    "data/silver/orders_silver",
    "data/silver/order_items_silver",
    "data/silver/customers_silver",
    "data/silver/payments_silver",
    "data/silver/products_silver",
]:
    print(f"\n=== {path} ===")
    df = spark.read.parquet(path)
    df.printSchema()
    df.show(3, truncate=False)
spark.stop()
PY
```

Example quality checks:

```bash
python - <<'PY'
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("silver_quality").getOrCreate()
orders = spark.read.parquet("data/silver/orders_silver")
orders.select(
    F.count("*").alias("rows"),
    F.countDistinct("order_id").alias("distinct_order_id"),
    F.sum(F.col("order_id").isNull().cast("int")).alias("null_order_id")
).show()
spark.stop()
PY
```

### Daily checks for silver

- rerun upstream bronze if schema changed
- check deduplication did not drop too many rows
- confirm joins and enrichments preserve intended granularity
- inspect one sample table after every code change

## Phase 6: Gold layer

### Implementation checklist

- `[ ]` create `daily_sales_gold`
- `[ ]` create `category_sales_gold`
- `[ ]` create `customer_ltv_gold`
- `[ ]` optionally create `delivery_performance_gold`
- `[ ]` optionally create `seller_performance_gold`

### Required inputs

- `orders_silver`
- `order_items_silver`
- `payments_silver`
- `customers_silver`
- `products_silver`

### Testing checklist

- `[ ]` row counts are non-zero
- `[ ]` daily revenue looks plausible
- `[ ]` no duplicate customer totals unless intentionally grouped
- `[ ]` category totals join correctly to product dimension

## Phase 7: Trino and query layer

### Implementation checklist

- `[ ]` define SQL DDL or external catalog expectations
- `[ ]` expose tables through Trino
- `[ ]` validate `SHOW CATALOGS`
- `[ ]` validate `SHOW SCHEMAS`
- `[ ]` run simple `SELECT COUNT(*)` queries

### Testing checklist

- `[ ]` Trino can see Hive-backed metadata
- `[ ]` Trino can read the expected data locations
- `[ ]` sample gold query returns rows

## Phase 8: Airflow orchestration

### Implementation checklist

- `[ ]` create DAG for bronze loads
- `[ ]` create DAG for silver loads
- `[ ]` create DAG for gold marts
- `[ ]` add retry policy
- `[ ]` add simple post-load checks

### Testing checklist

- `[ ]` DAG parses successfully
- `[ ]` manual DAG run succeeds
- `[ ]` failed task is retryable
- `[ ]` task dependency order is correct

## Phase 9: Hudi mode

### Implementation checklist

- `[ ]` download required Hudi jars into `jars/`
- `[ ]` validate Spark can load Hudi format
- `[ ]` run bronze jobs with `--output-format hudi`
- `[ ]` run silver jobs with `--output-format hudi`
- `[ ]` test record keys and precombine behavior

### Testing checklist

- `[ ]` Spark write with format `hudi` succeeds
- `[ ]` output contains Hudi metadata files
- `[ ]` rerun of the same table behaves as upsert rather than broken duplicate write

## Phase 10: Optional extensions

### Optional implementation checklist

- `[ ]` products API extraction
- `[ ]` `products_api_bronze`
- `[ ]` `products_api_silver`
- `[ ]` `products_current_silver`
- `[ ]` BI dashboard
- `[ ]` data quality framework
- `[ ]` monitoring and metrics
- `[ ]` CI validation

## Daily working checklist

Use this every day before and after coding.

### Before coding

- `[ ]` confirm Docker core services still start
- `[ ]` confirm raw data paths still exist
- `[ ]` decide which layer you are working on today
- `[ ]` check the mapping doc before adding new tables

### While coding

- `[ ]` keep changes within one layer at a time when possible
- `[ ]` after each edit, rerun only the affected job first
- `[ ]` inspect schema and sample rows, not only file existence
- `[ ]` avoid changing raw inputs manually

### After coding

- `[ ]` re-run the affected job
- `[ ]` check output directory
- `[ ]` check row count
- `[ ]` check sample rows
- `[ ]` capture any known limitation in docs if needed

## Test matrix

Use this to know what kind of test belongs where.

### Infrastructure tests

Where:
- Docker services

How:
- `docker compose ps`
- `docker compose logs`
- `curl http://localhost:8081/v1/info`

### Raw data tests

Where:
- `data/raw/`

How:
- `find`
- `sed`
- `wc -l`

### Bronze tests

Where:
- `data/bronze/`

How:
- `spark.read.parquet(...)`
- `printSchema()`
- `count()`
- `show()`

### Silver tests

Where:
- `data/silver/`

How:
- row counts
- distinct key counts
- null checks
- sample joins and enrichment checks

### Gold tests

Where:
- `data/gold/`

How:
- business metric sanity checks
- aggregation correctness
- join correctness

### Serving tests

Where:
- Trino
- Airflow

How:
- SQL smoke queries
- DAG parse and manual run

## Immediate next actions

These are the most useful next steps from the current project state.

1. Re-run and verify `reviews_bronze` after the multiline CSV parser fix.
2. Verify `sellers_bronze`, `geolocation_bronze`, and `product_category_translation_bronze` outputs.
3. Run the full `silver` batch.
4. Validate all silver outputs with schema, count, and sample-row checks.
5. Start implementing the three MVP gold marts.

## Definition of done for MVP

The MVP should be considered complete when all of these are true:

- `[ ]` core Docker services are stable
- `[ ]` all required bronze tables are generated
- `[ ]` all required silver tables are generated
- `[ ]` `products_silver` enrichment works
- `[ ]` at least three gold marts are generated
- `[ ]` outputs are queryable or at least validated locally
- `[ ]` pipeline flow is documented and reproducible

At that point, the project is ready for the next step: hardening, Hudi-mode runs, and orchestration.
