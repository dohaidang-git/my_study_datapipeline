# Project Optimization Roadmap

## Purpose

This document defines the recommended next directions for improving the `bigdata_hudi` project after the current local lakehouse demo is already working.

The current project already demonstrates:

- `raw -> bronze -> silver -> gold` medallion processing
- Hudi-based storage
- Spark ETL jobs
- Airflow orchestration
- Trino SQL access
- Metabase BI demo
- data quality, freshness, and reconciliation checks
- Hudi incremental upsert and time travel demos

The next improvements should focus on making the project more reliable, easier to explain, and closer to a production-style data platform.

## Recommended Priority Order

## 1. Reliability And Failure Handling

### Current state

Airflow already runs the pipeline with:

- `retries=1`
- `retry_delay=5 minutes`
- task-level `execution_timeout`
- `set -e` in Bash commands
- validation scripts that exit with failure when checks fail

If a task fails, Airflow retries it once. If it still fails, its downstream tasks do not run. Other independent branches may still continue if they do not depend on the failed task.

### Why this matters

This is acceptable for a demo, but a production-style pipeline should make failures easier to detect, explain, and recover from.

### Recommended improvements

- Add a clearer `preflight` stage for all required services:
  - `minio`
  - `spark-master`
  - `hive-metastore`
  - `trino`
  - required JAR files
- Add fail-fast gates after each major layer:
  - bronze gate
  - silver gate
  - gold gate
- Add failure callbacks for Airflow tasks.
- Write a short run summary after each successful or failed run.

### Expected outcome

The demo becomes easier to operate because every failed run clearly shows:

- which stage failed
- which table failed
- whether the failure is infrastructure, data quality, or transformation logic

## 2. Stronger Data Quality Checks

### Current state

The project already has data quality checks for important `silver` and `gold` tables.

### Why this matters

The current checks are useful, but they mostly validate final outputs. More checks should be added closer to the source so bad data fails earlier.

### Recommended improvements

- Add bronze-level checks:
  - required raw columns exist
  - row count is greater than zero
  - source file can be parsed correctly
- Add silver-level checks:
  - business keys are unique
  - date fields are valid
  - numeric values are non-negative
  - important dimension values are not null
- Add gold-level checks:
  - totals reconcile with upstream silver tables
  - mart dates match source order dates
  - BI-facing metrics are not unexpectedly zero

### Expected outcome

The pipeline can explain why its outputs are trustworthy, not only that it ran successfully.

## 3. Data Warehouse Schema

### Current state

The current `gold` tables are good BI marts:

- `daily_sales_gold`
- `category_sales_gold`
- `customer_ltv_gold`

However, they are not yet a complete dimensional warehouse model.

### Why this matters

A data warehouse schema makes the analytical model easier to extend. It also makes the project stronger from a data engineering perspective because it separates:

- cleaned entities
- dimensional warehouse tables
- BI marts

### Recommended warehouse phase 1

Build these dimensions:

- `dim_date`
- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_payment_type`
- `dim_order_status`

Build these facts:

- `fact_orders`
- `fact_order_items`
- `fact_payments`

Then rebuild the existing marts from the dimensional layer.

### Expected outcome

The project evolves from a medallion pipeline into a clearer lakehouse warehouse:

```text
raw -> bronze -> silver -> warehouse facts/dimensions -> gold marts -> BI
```

## 4. Hudi-Specific Enhancements

### Current state

The project already demonstrates:

- `COPY_ON_WRITE`
- `upsert`
- `record key`
- `precombine field`
- `time travel`

### Why this matters

Since the project topic is Hudi, the next improvements should make Hudi's strengths more visible and measurable.

### Recommended improvements

- Add an incremental query demo:
  - read records changed between two Hudi instants
  - explain how this supports downstream incremental processing
- Add a commit timeline report:
  - latest instant
  - operation type
  - affected table
  - row count after write
- Add a `COPY_ON_WRITE` vs `MERGE_ON_READ` comparison document:
  - write latency
  - read behavior
  - BI suitability
  - trade-offs
- Add a small delete/update scenario:
  - update one existing record
  - insert one new record
  - optionally delete or mark a record inactive

### Expected outcome

The Hudi part becomes more than "we store tables using Hudi". It becomes a concrete demonstration of table versioning and incremental data management.

## 5. Observability And Run Reports

### Current state

The project has logs and some Markdown reports for Hudi demos.

### Why this matters

During a live demo, logs are often too noisy. A clean run report makes it easier to explain what happened.

### Recommended improvements

- Generate a pipeline run summary under `reports/pipeline_runs/`.
- Include:
  - run timestamp
  - table row counts
  - Hudi commit instants
  - validation status
  - Trino smoke check status
  - failed task names, if any
- Add one command to print the latest run summary.

### Expected outcome

The project becomes easier to present because every run has a compact artifact that summarizes the result.

## 6. BI And Metabase Improvements

### Current state

Metabase can connect to Trino and use the current gold marts.

### Why this matters

The BI part should show business value, not only technical connectivity.

### Recommended improvements

- Create a dashboard layout document with:
  - KPI cards
  - sales trend chart
  - category revenue chart
  - customer LTV table
  - freight efficiency chart
- Add dashboard naming conventions.
- Add recommended filters:
  - date
  - customer state
  - product category
- Add a section explaining which gold table powers each chart.

### Expected outcome

The final demo can move naturally from pipeline execution to business insight.

## 7. Cross-Platform Developer Experience

### Current state

The project now includes:

- `.gitattributes` for shell script line endings
- `scripts/docker_compat.sh` for Git Bash path handling
- Windows notes in `README.md`

### Why this matters

The project is likely to be tested on both Linux and Windows. Cross-platform issues can block demo execution even when the data pipeline logic is correct.

### Recommended improvements

- Add a Windows-specific runbook.
- Document supported execution modes:
  - Linux
  - WSL2
  - Git Bash
- Add a quick check command for:
  - line endings
  - Docker service status
  - JAR files
  - MinIO connectivity

### Expected outcome

New users can run the project with fewer environment-specific failures.

## 8. Testing

### Current state

The project has operational validation scripts, but not many automated unit tests.

### Why this matters

Unit tests help protect transformation logic when the pipeline grows.

### Recommended improvements

- Add tests for transformation functions in:
  - bronze jobs
  - silver jobs
  - gold jobs
- Keep tests small and focused.
- Use sample DataFrames instead of full datasets.
- Test business logic such as:
  - payment installment correction
  - product category fallback
  - delivery delay calculation
  - LTV aggregation

### Expected outcome

Pipeline changes become safer because key transformations are checked without running the full stack.

## Suggested Implementation Phases

## Phase 1: Demo Reliability

Goal:

- make the current demo easier to run and explain

Tasks:

- improve preflight checks
- add layer-level gates
- add run summary report
- add Windows runbook

## Phase 2: Warehouse Modeling

Goal:

- turn the current marts into a more complete data warehouse design

Tasks:

- write warehouse design document
- create `dim_*` and `fact_*` pipeline jobs
- add Trino DDL for warehouse tables
- rebuild current gold marts from warehouse tables

## Phase 3: Stronger Hudi Demo

Goal:

- make Hudi's unique value more visible

Tasks:

- add incremental query demo
- add commit timeline report
- add `COPY_ON_WRITE` vs `MERGE_ON_READ` comparison
- add update/delete scenario

## Phase 4: Production-Style Polish

Goal:

- make the project closer to a real data platform

Tasks:

- add alerting
- add unit tests
- add run-level observability
- improve BI documentation
- document recovery steps for failed pipeline runs

## Recommended Next Step

The best next step is to implement **Phase 1: Demo Reliability** first.

Reason:

- it improves the current project without changing the data model
- it reduces live demo risk
- it makes failures easier to explain
- it prepares the project for warehouse and Hudi-specific extensions later

After Phase 1, the next strongest step is **Phase 2: Warehouse Modeling**, because it adds a clear dimensional data warehouse layer on top of the existing medallion pipeline.
