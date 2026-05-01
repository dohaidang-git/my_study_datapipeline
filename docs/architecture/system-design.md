# System Design

## Overview

This document describes the target system design for the local e-commerce data pipeline built around Apache Hudi. It focuses on how services interact, how data moves through the platform, and which design decisions matter for a 4-day MVP.

The system is designed to answer one central requirement:

- ingest e-commerce data with inserts and updates
- store it in a lakehouse-friendly format
- support reliable downstream analytics with SQL access

## System objectives

The MVP is designed to demonstrate these capabilities:

1. Land static and incremental e-commerce data locally.
2. Load source data into Hudi tables with `upsert` support.
3. Build cleaned business tables and reporting marts.
4. Query the results through SQL engines such as Trino.
5. Orchestrate daily execution with Airflow.

## High-level architecture

```text
                    +----------------------+
                    |  Static Dataset      |
                    |  Olist or UCI CSV    |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |  Extract Jobs        |
                    |  File/API Ingestion  |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |  Raw Landing Zone    |
                    |  data/raw            |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |  Spark + Hudi        |
                    |  Bronze Load Jobs    |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |  Bronze Hudi Tables  |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |  Spark + Hudi        |
                    |  Silver Transform    |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |  Silver Hudi Tables  |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |  Spark SQL Jobs      |
                    |  Gold Aggregations   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |  Gold Hudi Tables    |
                    +----------+-----------+
                               |
                 +-------------+-------------+
                 |                           |
                 v                           v
       +-------------------+       +-------------------+
       |  Trino            |       |  Validation SQL   |
       |  Query Layer      |       |  Checks           |
       +---------+---------+       +-------------------+
                 |
                 v
       +-------------------+
       |  Superset or BI   |
       +-------------------+
```

## Main components

### 1. Source data

The project uses two source types.

#### Static historical source

Primary option:
- Olist e-commerce dataset

Purpose:
- provide realistic historical order, item, payment, customer, and product data
- serve as the initial load into the platform

#### Incremental source

Primary option:
- product or supplemental metadata API such as DummyJSON

Purpose:
- simulate daily source updates
- provide repeatable `upsert` scenarios for Hudi

MVP simplification:
- incremental order status updates may also be simulated from prepared delta files instead of real streaming CDC

### 2. Extract layer

Implemented in:
- `pipelines/extract/`

Responsibilities:
- download or copy source files into `data/raw/`
- call external API endpoints and store date-partitioned snapshots
- keep source data untouched after landing

Output:
- CSV, JSON, or JSONL files in raw storage

Why this exists:
- reproducibility
- replayability
- separation of source ingestion from transformation logic

### 3. Processing layer

Implemented with:
- Apache Spark

Responsibilities:
- read raw files
- apply schemas
- write Hudi tables
- execute upserts and downstream transformations

Why Spark:
- simple to run locally
- mature Hudi integration
- enough for batch and pseudo-incremental MVP pipelines

### 4. Storage layer

Implemented with:
- Apache Hudi over local filesystem or MinIO-backed object storage

Responsibilities:
- maintain record-level updates
- support snapshot reads
- support incremental consumption
- store bronze, silver, and gold layers

Why Hudi:
- native `upsert` semantics
- table services like compaction and clustering for future expansion
- good fit for slowly changing or frequently updated business records

### 5. Metadata and query layer

Implemented with:
- Hive Metastore
- Trino

Responsibilities:
- register schemas and table locations
- expose Hudi-backed tables for SQL analytics
- allow BI tools to read curated marts

Why this combination:
- common open data stack
- easy to explain in a portfolio or assignment setting
- decouples processing from consumption

### 6. Orchestration layer

Implemented with:
- Apache Airflow

Responsibilities:
- run jobs in dependency order
- manage retries
- support daily schedules and backfills
- make the pipeline easier to demonstrate end-to-end

### 7. Consumption layer

Implemented with:
- SQL queries under `sql/queries/`
- optional Superset dashboards

Responsibilities:
- provide business-facing analytics
- validate that the gold layer is usable by downstream consumers

## Logical data model

The e-commerce domain should be modeled around a small set of core entities.

### Core entities

- `orders`
- `order_items`
- `customers`
- `products`
- `payments`

### Gold marts

- `daily_sales_gold`
- `category_sales_gold`
- `customer_ltv_gold`
- `delivery_performance_gold`

## Layer-by-layer design

### Raw layer

Purpose:
- preserve source fidelity
- provide replayable input

Characteristics:
- immutable or append-only
- minimal structure beyond source partitioning
- source-specific filenames

Example:

```text
data/raw/olist/olist_orders_dataset.csv
data/raw/products_api/dt=2026-05-01/products.json
```

### Bronze layer

Purpose:
- first managed Hudi representation of source data

Characteristics:
- close to source schema
- light normalization only
- ingestion metadata added

Example metadata columns:
- `_ingested_at`
- `_batch_id`
- `_source`
- `_source_file`

### Silver layer

Purpose:
- trusted, cleaned business data

Characteristics:
- consistent naming and data types
- deduplicated records
- standardized timestamps and statuses
- late-arriving updates merged correctly

Example transformations:
- order status normalization
- order item deduplication
- product update merge logic
- timestamp parsing and timezone normalization

### Gold layer

Purpose:
- fast and stable analytical output

Characteristics:
- aggregated
- denormalized where useful
- business-friendly semantics

Example metrics:
- gross sales per day
- order count by status
- revenue by category
- delayed delivery ratio

## Storage design

## Physical storage

During local development, the pipeline can start on local mounted paths. After the container stack is ready, those paths should map logically to MinIO buckets or object-style prefixes.

Suggested bucket or path layout:

```text
lakehouse/
├── raw/
├── bronze/
├── silver/
└── gold/
```

## Hudi table design

The MVP should use Copy-on-Write tables first because they are easier to reason about locally and simpler for BI query patterns.

Recommended initial settings:
- table type: `COPY_ON_WRITE`
- operation: `upsert`
- cleaner enabled
- Hive sync enabled when catalog integration is added

Future expansion:
- Move selective tables to `MERGE_ON_READ` if write frequency becomes the priority.

## Record key and partition strategy

Good Hudi behavior depends heavily on choosing stable keys and sensible partitions.

### `orders_bronze` or `orders_silver`

- record key: `order_id`
- precombine field: `updated_at` or best available event timestamp
- partition field: `order_purchase_date`

Why:
- one logical order should be updated in place
- partitioning by date supports time-based scans

### `order_items_*`

- record key: composite `order_id` + `order_item_id`
- precombine field: item update timestamp
- partition field: `order_purchase_date`

Why:
- one order can contain multiple rows

### `customers_*`

- record key: `customer_id`
- precombine field: latest update timestamp
- partition field: optional non-partitioned or coarse regional partition

Why:
- customer records change less frequently
- aggressive partitioning may add unnecessary complexity

### `products_*`

- record key: `product_id`
- precombine field: latest product update timestamp
- partition field: `category`

Why:
- product updates are dimension-like
- category partitioning can help exploratory queries if cardinality is controlled

## Execution model

The MVP uses batch-oriented execution with incremental behavior simulated through repeatable job runs.

### Initial load

Flow:
1. land historical files in raw
2. load them into bronze Hudi tables
3. transform them into silver
4. aggregate into gold

### Incremental load

Flow:
1. land a new daily file or API snapshot
2. run the corresponding bronze loader in `upsert` mode
3. refresh dependent silver tables
4. rebuild or incrementally refresh gold marts

This is enough to demonstrate Hudi value without requiring a full Kafka and CDC stack in the first version.

## Orchestration design

Airflow should coordinate the pipeline as a daily DAG.

Suggested task graph:

```text
extract_static_dataset
extract_products_api
load_orders_bronze
load_order_items_bronze
load_customers_bronze
load_products_bronze
build_orders_silver
build_order_items_silver
build_products_silver
build_daily_sales_gold
build_category_sales_gold
validate_gold_tables
```

Execution rules:
- raw extraction finishes before bronze load
- bronze dependencies finish before silver
- silver dependencies finish before gold
- validation runs last

## Query design

Trino is the main SQL access layer for the MVP.

Expected consumers:
- local SQL validation queries
- optional BI dashboards
- ad hoc analysis against silver and gold tables

Main SQL use cases:
- compare row counts across layers
- inspect latest order status
- compute daily revenue
- identify delayed shipments

## Data quality design

The MVP should include lightweight but explicit validation checks.

Recommended checks:
- no null primary keys in silver
- no negative price or payment amounts
- order purchase date is parseable
- `order_items` references a valid order
- gold table row counts are non-zero after load

Where checks can run:
- inside Spark jobs for hard failures
- as post-load SQL checks via Trino
- optionally in Airflow validation tasks

## Observability and logging

The first version does not need a full monitoring stack, but it should still expose enough information to debug failures.

Minimum observability requirements:
- structured logs from extract and Spark jobs
- row counts before and after writes
- batch id or run date in each pipeline run
- Airflow task visibility for orchestration status

Future expansion:
- Prometheus and Grafana
- data freshness metrics
- job runtime tracking

## Failure handling

The system should be resilient enough for repeated local execution.

Expected failure classes:
- source file missing
- API request failure
- schema drift in source files
- duplicate records
- Spark job failure
- Hive or Trino catalog mismatch

Mitigation strategy:
- keep raw data replayable
- fail early on schema-critical columns
- isolate stages by layer
- rerun failed tasks from Airflow instead of redoing everything manually

## Security and local constraints

This project is meant for local development, so security is intentionally lightweight.

MVP assumptions:
- services run in a local Docker network
- no external identity provider
- secrets stay in environment files and local config

Do not optimize early for:
- multi-tenant access control
- enterprise secrets management
- cross-region deployment

## Tradeoffs and design decisions

### Why Hudi over plain Parquet

Because the project needs to showcase:
- record-level updates
- incremental processing
- data version management behavior

### Why batch first instead of streaming first

Because in 4 days:
- batch is easier to finish reliably
- Hudi value can still be demonstrated clearly
- the design stays compatible with future streaming ingestion

### Why Spark instead of Flink for the MVP

Because:
- simpler local setup
- easier debugging in coursework-style environments
- strong enough Hudi integration for this scope

### Why use a static dataset and a small API together

Because:
- the static dataset gives enough business depth
- the API gives repeatable incremental changes
- together they create a convincing end-to-end story

## MVP scope boundary

The 4-day MVP should include:
- local service stack
- one historical e-commerce dataset
- one small incremental source
- bronze, silver, and gold Hudi tables
- Airflow DAG
- SQL validation queries
- optional dashboard

The 4-day MVP should not require:
- Kafka
- Debezium
- full CDC streaming
- complex security hardening
- large-scale performance tuning

## Future extensions

After the MVP is stable, the design can evolve in these directions:

1. Replace simulated increments with Kafka-based CDC.
2. Add MinIO as the default object storage target if not already used.
3. Add Superset dashboards and richer semantic marts.
4. Introduce Great Expectations or similar data quality tooling.
5. Add compaction, clustering, and incremental pull demos for Hudi.
6. Add CI jobs to run tests and smoke checks automatically.

## Recommended implementation order

Follow this order to align implementation with the system design:

1. bootstrap Docker services
2. land raw dataset and API extract
3. build shared Spark and Hudi helpers
4. implement bronze loaders
5. implement silver transformations
6. implement gold marts
7. add Trino SQL checks
8. orchestrate with Airflow
9. add documentation and demo workflow

## Summary

This system design keeps the architecture intentionally simple, but still aligned with real data platform patterns:
- source separation
- layered storage
- Hudi-managed updates
- SQL query access
- orchestration-driven execution

It is large enough to demonstrate Apache Hudi well, but still small enough to complete as a local 4-day project.
