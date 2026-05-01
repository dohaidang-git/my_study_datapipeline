# Project Structure

## Purpose

This document describes the intended repository structure for the local e-commerce data pipeline built with Apache Hudi. The goal is to keep ingestion, transformation, orchestration, query, and operations concerns separated so the project can grow from a 4-day MVP into a more complete data platform without restructuring the repo.

## Project goals

This repository is organized to support the following workflow:

1. Ingest source data from a static e-commerce dataset and a small API-based product feed.
2. Store source files in a raw landing zone.
3. Load and upsert the data into Apache Hudi tables.
4. Transform raw tables into cleaned business tables.
5. Publish analytics-ready marts for SQL queries and dashboards.
6. Orchestrate the steps with Airflow.
7. Validate the output with SQL checks and automated tests.

## Top-level layout

```text
bigdata_hudi/
├── configs/
├── dags/
├── data/
├── docker/
├── docs/
├── pipelines/
├── scripts/
├── sql/
├── tests/
└── README.md
```

## Directory details

### `configs/`

This folder contains runtime configuration files shared by containers, jobs, and local scripts.

Expected usage:
- Spark defaults, session properties, and job-specific settings
- Hudi write option templates and tuning defaults
- Trino catalog and connector configuration
- Airflow environment and connection config

Planned subfolders:
- `configs/spark/`: `spark-defaults.conf`, log config, optional submit templates
- `configs/hudi/`: reusable Hudi table option presets and metadata settings
- `configs/trino/`: catalog properties for Hive and Hudi-backed tables
- `configs/airflow/`: environment variables, connection bootstrap, and DAG support config

Design rule:
- Put declarative configuration here.
- Do not put orchestration logic or Python transformation code here.

### `dags/`

This folder contains Airflow DAG definitions that coordinate the pipeline end to end.

Expected responsibilities:
- Trigger raw ingestion
- Run bronze load jobs
- Run silver transformations
- Run gold mart generation
- Run validation checks after writes

Typical future files:
- `daily_ecommerce_pipeline.py`
- `backfill_orders_pipeline.py`

Design rule:
- DAG files should describe orchestration only.
- Business logic should stay in `pipelines/`.

### `data/`

This folder represents the local storage layout used during development. In a containerized run, the same logical layers can be mounted into MinIO/S3-backed paths or local volumes.

Subfolders:
- `data/raw/`: immutable or append-only landing zone from source systems
- `data/bronze/`: first Hudi-managed layer, close to source shape
- `data/silver/`: cleaned and conformed Hudi tables
- `data/gold/`: curated marts for BI and reporting

#### `data/raw/`

Purpose:
- Store source-of-truth input files before transformation
- Preserve the original format for traceability and replay

Current subfolders:
- `data/raw/olist/`: static e-commerce dataset files
- `data/raw/products_api/`: daily API extracts for product information

Recommended conventions:
- Partition by ingestion date when applicable
- Keep original filenames if possible
- Do not manually edit landed raw files

Example layout:

```text
data/raw/
├── olist/
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   └── olist_customers_dataset.csv
└── products_api/
    └── dt=2026-05-01/
        └── products.json
```

#### `data/bronze/`

Purpose:
- Store source-aligned Hudi tables
- Keep schemas close to upstream data
- Capture ingestion metadata and minimal normalization

Expected tables:
- `orders_bronze`
- `order_items_bronze`
- `customers_bronze`
- `products_bronze`

Typical changes allowed at this layer:
- Type casting
- Column renaming for consistency
- Metadata columns such as `_ingested_at`, `_source_file`, `_batch_id`

#### `data/silver/`

Purpose:
- Store validated, cleaned, deduplicated, and business-ready Hudi tables

Expected tables:
- `orders_silver`
- `order_items_silver`
- `customers_silver`
- `products_silver`
- `payments_silver`

Typical transformations:
- Deduplication
- Status normalization
- Date standardization
- Null handling
- Business key consolidation
- Upsert handling for late-arriving updates

#### `data/gold/`

Purpose:
- Store analytics marts optimized for SQL consumers and dashboards

Expected marts:
- `daily_sales_gold`
- `category_sales_gold`
- `customer_ltv_gold`
- `delivery_performance_gold`

Typical characteristics:
- Denormalized
- Aggregated
- Stable semantics for reporting

### `docker/`

This folder contains container-related assets used to boot the local platform.

Current subfolders:
- `docker/spark/`
- `docker/hive/`
- `docker/trino/`

Expected future content:
- `docker-compose.yml` at the repo root or under `docker/`
- custom images or Dockerfiles when base images are not enough
- service-specific bootstrap files

Design rule:
- Container wiring belongs here.
- Application code does not.

### `docs/`

This folder stores human-facing documentation.

Subfolders:
- `docs/architecture/`: structure, design choices, diagrams, data model notes
- `docs/runbooks/`: execution steps, troubleshooting, operating procedures

Examples of future docs:
- architecture overview
- Hudi table strategy
- partitioning decisions
- local setup runbook
- failure recovery guide

Design rule:
- Keep implementation out of docs.
- Keep rationale and operational knowledge here.

### `pipelines/`

This is the main application code area for extraction and data processing jobs.

Subfolders:
- `pipelines/common/`: shared utilities, schemas, constants, IO helpers
- `pipelines/extract/`: source ingestion jobs
- `pipelines/bronze/`: raw-to-bronze load jobs
- `pipelines/silver/`: cleaning and business-conformance jobs
- `pipelines/gold/`: mart-building jobs

#### `pipelines/common/`

Purpose:
- Centralize code reused across multiple jobs

Expected modules:
- Spark session builder
- Hudi writer wrapper
- schema definitions
- path utilities
- logging helpers
- shared argument parsing

#### `pipelines/extract/`

Purpose:
- Pull data from external systems into the raw landing zone

Expected jobs:
- download static dataset files
- call product API and save daily snapshots
- optional lightweight crawler for supplemental product metadata

Design rule:
- Output raw files only.
- Do not write directly to silver or gold from extract jobs.

#### `pipelines/bronze/`

Purpose:
- Convert raw files into bronze Hudi tables

Typical tasks:
- read CSV/JSON
- apply initial schema
- add ingestion metadata
- write or upsert into bronze

#### `pipelines/silver/`

Purpose:
- Build trusted business tables from bronze

Typical tasks:
- deduplicate keys
- normalize order status
- resolve product updates
- merge late-arriving changes
- enforce data quality rules

#### `pipelines/gold/`

Purpose:
- Produce reporting marts and dashboard sources

Typical tasks:
- join conformed dimensions and facts
- aggregate daily metrics
- materialize KPIs for Trino or BI tools

### `scripts/`

This folder contains utility scripts that help bootstrap or operate the local environment.

Examples:
- create MinIO buckets
- load environment variables
- initialize Hive schema
- seed sample delta files
- run common local commands

Design rule:
- Scripts may call jobs and services.
- Core transformation logic still belongs in `pipelines/`.

### `sql/`

This folder contains SQL assets for setup, validation, and demos.

Subfolders:
- `sql/ddl/`: table definitions, views, schemas, catalogs
- `sql/queries/`: validation checks, exploratory queries, BI-ready reports

Examples:
- create external schemas in Trino
- define reporting views
- compare bronze and silver row counts
- query incremental Hudi changes

### `tests/`

This folder contains automated tests for code and data expectations.

Expected test types:
- unit tests for helpers in `pipelines/common/`
- transformation tests for bronze and silver jobs
- schema validation tests
- integration smoke tests for end-to-end job runs

Design rule:
- Keep test fixtures small and deterministic.
- Do not depend on the full production dataset for every test.

## Data flow mapping

The repository structure follows the logical data flow below:

```text
Source dataset/API
    -> pipelines/extract
    -> data/raw
    -> pipelines/bronze
    -> data/bronze
    -> pipelines/silver
    -> data/silver
    -> pipelines/gold
    -> data/gold
    -> sql/queries and BI tools
```

Airflow DAGs in `dags/` orchestrate the flow, while configs in `configs/` control runtime behavior.

## Naming conventions

To keep the project readable, use these conventions consistently.

### Files

- Python modules: `snake_case.py`
- SQL files: `snake_case.sql`
- Docs: `kebab-case.md`
- DAG files: `snake_case.py`

### Tables

Recommended pattern:
- `<entity>_<layer>`
- examples: `orders_bronze`, `orders_silver`, `daily_sales_gold`

### Jobs

Recommended pattern:
- `<verb>_<entity>.py`
- examples: `extract_products_api.py`, `load_orders_bronze.py`, `build_daily_sales_gold.py`

### Raw folders

Recommended pattern:
- static dataset folder by source name
- incremental feed folder partitioned by date

Examples:
- `data/raw/olist/`
- `data/raw/products_api/dt=2026-05-01/`

## Ownership by layer

This separation helps avoid mixing responsibilities.

- `extract` jobs own raw landing
- `bronze` jobs own source-aligned Hudi tables
- `silver` jobs own cleaned business tables
- `gold` jobs own reporting marts
- `sql` owns consumer-facing query assets
- `dags` owns orchestration only
- `configs` owns runtime settings only

## Recommended first implementation sequence

For this project, the repo should be filled in roughly in this order:

1. `docker/` and `configs/` for local services
2. `pipelines/common/` for Spark and Hudi helpers
3. `pipelines/extract/` to land source data
4. `pipelines/bronze/` for initial Hudi tables
5. `pipelines/silver/` for conformed tables
6. `pipelines/gold/` for marts
7. `sql/` for query validation
8. `dags/` for orchestration
9. `tests/` for repeatable verification

## MVP file map

A minimal 4-day MVP will likely introduce files close to this shape:

```text
configs/
├── spark/spark-defaults.conf
├── trino/hive.properties
└── hudi/base-write-options.yaml

pipelines/
├── common/spark_session.py
├── common/hudi_writer.py
├── extract/extract_products_api.py
├── bronze/load_orders_bronze.py
├── bronze/load_order_items_bronze.py
├── silver/build_orders_silver.py
└── gold/build_daily_sales_gold.py

sql/
├── ddl/create_hudi_schemas.sql
└── queries/check_daily_sales.sql

dags/
└── daily_ecommerce_pipeline.py
```

## What should not go where

Avoid these common mistakes:

- Do not put business SQL inside `docker/`.
- Do not put Spark transformation code inside `dags/`.
- Do not treat `data/raw/` as a working scratch directory.
- Do not mix bronze, silver, and gold output in one path.
- Do not duplicate configuration constants across every pipeline file.

## Summary

This structure is intentionally simple but production-shaped. It supports:
- clear ownership by layer
- straightforward local development
- easy expansion into more services later
- a clean demonstration of Apache Hudi in an e-commerce pipeline

As implementation grows, this structure should remain stable even if more datasets, more tables, or more orchestration steps are added.
