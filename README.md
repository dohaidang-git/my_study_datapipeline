# Big Data Hudi E-commerce Pipeline

Local lakehouse project for e-commerce analytics built on `Apache Hudi`, `Spark`, `Trino`, `Airflow`, `MinIO`, and `Metabase`. The project ingests raw Olist CSV datasets, organizes them into `bronze -> silver -> gold` layers, validates data quality, exposes marts for BI, and includes Hudi-specific demos such as `incremental upsert` and `time travel`.

## Description

This repository implements a complete local data platform for batch analytics on e-commerce data. The core idea is:

- ingest raw source files into a local S3-compatible lake
- store curated tables in Hudi
- transform them across medallion layers
- orchestrate everything with Airflow
- query marts with Trino
- visualize results in Metabase

It is designed as a reproducible end-to-end pipeline rather than only a collection of Spark scripts.

## Objective

- Build a local lakehouse pipeline around `Apache Hudi`
- Model e-commerce data through `bronze`, `silver`, and `gold` layers
- Validate pipeline correctness with row-count, quality, freshness, and reconciliation checks
- Serve analytical marts for SQL and BI consumption
- Demonstrate Hudi capabilities beyond plain Parquet, especially:
  - `incremental upsert`
  - `time travel`

## Dataset

Main dataset:

- `Olist Brazilian E-Commerce Public Dataset`

Source files are stored under:

- `data/raw/olist/`

Main entities used in the pipeline:

- `orders`
- `order_items`
- `customers`
- `payments`
- `products`
- `sellers`
- `reviews`
- `geolocation`
- `product_category_translation`

These raw CSV files are transformed into:

- `bronze`: raw-preserving Hudi tables
- `silver`: cleaned and standardized Hudi tables
- `gold`: serving marts for analytics and BI

## Tools And Technologies

- `Apache Spark 3.5.8`: ETL and Hudi read/write jobs
- `Apache Hudi 1.1.1`: transactional lake table format
- `MinIO`: S3-compatible object storage
- `Hive Metastore`: metadata catalog for lakehouse tables
- `Trino`: SQL query engine
- `Apache Airflow 3`: orchestration and scheduling
- `Metabase`: BI and dashboard layer
- `Docker Compose`: local platform orchestration
- `Python`: pipeline jobs, helpers, validation scripts, and demos

## Architecture

```mermaid
flowchart LR
    A[Raw CSV files<br/>data/raw/olist] --> B[Bronze jobs<br/>Spark + Hudi]
    B --> C[Bronze Hudi tables<br/>s3a://lakehouse/bronze]
    C --> D[Silver jobs<br/>Spark + Hudi]
    D --> E[Silver Hudi tables<br/>s3a://lakehouse/silver]
    E --> F[Gold jobs<br/>Spark + Hudi]
    F --> G[Gold marts<br/>s3a://lakehouse/gold]

    G --> H[Hive Metastore]
    H --> I[Trino]
    I --> J[Metabase]

    K[Airflow DAGs] --> B
    K --> D
    K --> F
    K --> L[Verify + Quality + Freshness checks]
    L --> G
```

Pipeline flow in practice:

1. Raw Olist CSV files are loaded into `bronze` Hudi tables.
2. `Silver` jobs standardize schemas, clean values, and preserve business keys.
3. `Gold` jobs build marts such as `daily_sales_gold`, `category_sales_gold`, and `customer_ltv_gold`.
4. `Airflow` orchestrates the full run.
5. `Trino` queries the final marts through `Hive Metastore`.
6. `Metabase` consumes those marts for dashboarding.

## Final Result

At the current state, the project already provides:

- a working end-to-end `raw -> bronze -> silver -> gold` Hudi pipeline
- successful orchestration with `Airflow`
- SQL access to `gold` tables via `Trino`
- BI connectivity through `Metabase`
- validation layers for:
  - pipeline row-count verification
  - data quality checks
  - freshness and reconciliation checks
- Hudi demo capabilities for:
  - `incremental upsert`
  - `time travel`

Key analytical outputs:

- `daily_sales_gold`
- `category_sales_gold`
- `customer_ltv_gold`

## Setup

Download the required Hudi runtime JARs first:

```bash
bash scripts/download_hudi_jars.sh
```

This step is recommended immediately after a fresh clone or pull because the Hudi pipeline depends on the runtime JARs stored in `./jars`.

Start the full local stack:

```bash
docker compose up -d \
  minio minio-init \
  metastore-postgres hive-metastore \
  spark-master spark-worker \
  trino \
  airflow-postgres airflow-init airflow-webserver airflow-dag-processor airflow-scheduler \
  metabase-postgres metabase
```

Notes:

- You can still start the Docker stack without downloading the JARs first.
- However, to run the Hudi ETL pipeline successfully, the JARs should already exist in `./jars`.
- If the stack is already running, downloading the JARs afterwards is still fine because `./jars` is mounted into the Spark and Airflow containers.

Run the full Hudi pipeline:

```bash
bash scripts/run_hudi_full_pipeline.sh
```

Verify data and query layer:

```bash
bash scripts/spark_submit_container.sh pipelines/tools/verify_hudi_pipeline.py
bash scripts/spark_submit_container.sh pipelines/tools/run_data_quality_checks.py
bash scripts/spark_submit_container.sh pipelines/tools/run_freshness_reconciliation_checks.py
bash scripts/run_trino_gold_checks.sh
```

Run the Hudi incremental upsert and time-travel demo:

```bash
bash scripts/run_hudi_incremental_demo.sh
```

Open local UIs:

- Airflow: `http://localhost:8080`
- Trino: `http://localhost:8081`
- Spark master UI: `http://localhost:8082`
- Spark worker UI: `http://localhost:8083`
- MinIO console: `http://localhost:9011`
- Metabase: `http://localhost:3000`

Detailed structure documentation: [docs/architecture/project-structure.md](/home/dohaidang/bigdata_hudi/docs/architecture/project-structure.md:1)

System design documentation: [docs/architecture/system-design.md](/home/dohaidang/bigdata_hudi/docs/architecture/system-design.md:1)

Data mapping documentation: [docs/architecture/data-mapping.md](/home/dohaidang/bigdata_hudi/docs/architecture/data-mapping.md:1)

Hudi in project documentation: [docs/architecture/hudi-trong-du-an.md](/home/dohaidang/bigdata_hudi/docs/architecture/hudi-trong-du-an.md:1)

Python files documentation: [docs/architecture/python-files-trong-project.md](/home/dohaidang/bigdata_hudi/docs/architecture/python-files-trong-project.md:1)

Input data and processing documentation: [docs/architecture/du-lieu-dau-vao-va-cach-xu-ly.md](/home/dohaidang/bigdata_hudi/docs/architecture/du-lieu-dau-vao-va-cach-xu-ly.md:1)

Airflow in project documentation: [docs/architecture/airflow-trong-du-an.md](/home/dohaidang/bigdata_hudi/docs/architecture/airflow-trong-du-an.md:1)

Docker stack documentation: [docs/runbooks/docker-stack.md](/home/dohaidang/bigdata_hudi/docs/runbooks/docker-stack.md:1)

Data quality checks documentation: [docs/runbooks/data-quality-checks.md](/home/dohaidang/bigdata_hudi/docs/runbooks/data-quality-checks.md:1)

Freshness and reconciliation checks documentation: [docs/runbooks/freshness-reconciliation-checks.md](/home/dohaidang/bigdata_hudi/docs/runbooks/freshness-reconciliation-checks.md:1)

BI demo guide: [docs/runbooks/bi-demo.md](/home/dohaidang/bigdata_hudi/docs/runbooks/bi-demo.md:1)

Hudi incremental/time travel demo: [docs/runbooks/hudi-incremental-time-travel-demo.md](/home/dohaidang/bigdata_hudi/docs/runbooks/hudi-incremental-time-travel-demo.md:1)

Project checklist: [docs/runbooks/project-checklist.md](/home/dohaidang/bigdata_hudi/docs/runbooks/project-checklist.md:1)

## Parameter Tuning

This section lists the main parameters that are actually used in the current project, where they are defined, and what they control.

### 1. Spark Runtime

Main file:

- [configs/spark/spark-defaults.conf](/home/dohaidang/bigdata_hudi/configs/spark/spark-defaults.conf:1)

Parameters currently used:

- `spark.master=spark://spark-master:7077`
  - Spark jobs submit to the `spark-master` container.
- `spark.eventLog.enabled=false`
  - Event logging is disabled in this local stack.
- `spark.sql.session.timeZone=Asia/Ho_Chi_Minh`
  - Keeps timestamps aligned with the local demo timezone.
- `spark.hadoop.fs.s3a.endpoint=http://minio:9000`
  - Internal Spark-to-MinIO endpoint inside Docker.
- `spark.hadoop.fs.s3a.access.key=minioadmin`
  - MinIO access key used by Spark.
- `spark.hadoop.fs.s3a.secret.key=minioadmin`
  - MinIO secret key used by Spark.
- `spark.hadoop.fs.s3a.path.style.access=true`
  - Required for MinIO compatibility.
- `spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`
  - Enables the `s3a://` filesystem implementation.
- `spark.hadoop.fs.s3a.connection.ssl.enabled=false`
  - SSL is disabled in this local environment.
- `spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083`
  - Spark connects to the Hive Metastore service through Thrift.

Additional Spark session defaults are set in:

- [pipelines/common/spark_session.py](/home/dohaidang/bigdata_hudi/pipelines/common/spark_session.py:1)

Those defaults are:

- `spark.sql.session.timeZone=Asia/Ho_Chi_Minh`
- `spark.sql.sources.partitionOverwriteMode=dynamic`
- `spark.serializer=org.apache.spark.serializer.KryoSerializer`
  - This serializer is enabled automatically when a Hudi JAR is detected.

### 2. Hudi Write Parameters

Main file:

- [pipelines/common/hudi_writer.py](/home/dohaidang/bigdata_hudi/pipelines/common/hudi_writer.py:1)

Common Hudi options currently used for pipeline writes:

- `hoodie.table.name=<table_name>`
  - Logical Hudi table name.
- `hoodie.datasource.write.table.type=COPY_ON_WRITE`
  - The project currently uses `COPY_ON_WRITE`.
- `hoodie.datasource.write.operation=upsert`
  - All writes use `upsert` semantics by default.
- `hoodie.datasource.write.recordkey.field=<record_key>`
  - Business key used to identify each record.
- `hoodie.datasource.write.precombine.field=<precombine_field>`
  - Field used to determine the latest version of a record.
- `hoodie.datasource.write.hive_style_partitioning=true`
  - Enables Hive-style partition folder naming.
- `hoodie.datasource.write.reconcile.schema=true`
  - Allows schema reconciliation during writes.
- `hoodie.upsert.shuffle.parallelism=2`
  - Shuffle parallelism for upsert operations.
- `hoodie.insert.shuffle.parallelism=2`
  - Shuffle parallelism for insert operations.
- `hoodie.clean.automatic=true`
  - Enables automatic Hudi cleaning.
- `hoodie.datasource.write.partitionpath.field=<partition_field>`
  - Only set when the table is partitioned.
- `hoodie.datasource.hive_sync.partition_fields=<partition_field>`
  - Used together with partitioned tables.
- `hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator`
  - Used when the table is non-partitioned.

These parameters are the core of the Hudi behavior in this project:

- `COPY_ON_WRITE` is chosen for BI-friendly reads.
- `upsert` is chosen to support incremental-style table maintenance.
- `recordkey` and `precombine` determine how duplicate business records are resolved.

### 3. MinIO and Hadoop S3A

Main file:

- [configs/hadoop/core-site.xml](/home/dohaidang/bigdata_hudi/configs/hadoop/core-site.xml:1)

Parameters currently used:

- `fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`
- `fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`
- `fs.AbstractFileSystem.s3a.impl=org.apache.hadoop.fs.s3a.S3A`
- `fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`
- `fs.s3a.access.key=minioadmin`
- `fs.s3a.secret.key=minioadmin`
- `fs.s3a.endpoint=http://minio:9000`
- `fs.s3a.path.style.access=true`
- `fs.s3a.connection.ssl.enabled=false`

Important runtime note:

- Host access to MinIO uses:
  - `http://localhost:9010` for the S3 API
  - `http://localhost:9011` for the MinIO console
- Internal service-to-service access still uses:
  - `http://minio:9000`

Those host ports are defined in:

- [docker-compose.yml](/home/dohaidang/bigdata_hudi/docker-compose.yml:1)

### 4. Docker Service Parameters

Main file:

- [docker-compose.yml](/home/dohaidang/bigdata_hudi/docker-compose.yml:1)

Important container-level parameters currently used:

- `MINIO_ROOT_USER=minioadmin`
- `MINIO_ROOT_PASSWORD=minioadmin`
- `minio` ports:
  - `9010:9000`
  - `9011:9001`
- `spark-worker` resources:
  - `SPARK_WORKER_MEMORY=2G`
  - `SPARK_WORKER_CORES=2`
- `trino` host port:
  - `8081:8080`
- `hive-metastore` host port:
  - `9083:9083`
- `airflow-webserver` host port:
  - `8080:8080`
- `metabase` host port:
  - `3000:3000`
- `metastore-postgres` host port:
  - `5433:5432`
- `airflow-postgres` host port:
  - `5434:5432`

### 5. Hive Metastore

Main configuration sources:

- [docker-compose.yml](/home/dohaidang/bigdata_hudi/docker-compose.yml:1)
- [docker/hive/Dockerfile](/home/dohaidang/bigdata_hudi/docker/hive/Dockerfile:1)

Parameters currently used:

- `SERVICE_NAME=metastore`
- `DB_DRIVER=postgres`
- `ConnectionDriverName=org.postgresql.Driver`
- `ConnectionURL=jdbc:postgresql://metastore-postgres:5432/metastore_db`
- `ConnectionUserName=hive`
- `ConnectionPassword=hive`

The Hive image also adds supporting JARs for:

- PostgreSQL JDBC
- `hadoop-aws`
- `aws-java-sdk-bundle`

This is required so Hive Metastore can work correctly with Postgres and S3-compatible storage.

### 6. Trino

Main files:

- [docker/trino/config.properties](/home/dohaidang/bigdata_hudi/docker/trino/config.properties:1)
- [docker/trino/jvm.config](/home/dohaidang/bigdata_hudi/docker/trino/jvm.config:1)
- [docker/trino/node.properties](/home/dohaidang/bigdata_hudi/docker/trino/node.properties:1)
- [docker/trino/catalog/hive.properties](/home/dohaidang/bigdata_hudi/docker/trino/catalog/hive.properties:1)

Coordinator parameters currently used:

- `coordinator=true`
- `node-scheduler.include-coordinator=true`
- `http-server.http.port=8080`
- `discovery.uri=http://trino:8080`
- `protocol.v1.alternate-header-name=Presto`
  - Important for Metabase compatibility.

JVM parameters currently used:

- `-Xmx2G`
- `-XX:+UseG1GC`
- `-XX:G1HeapRegionSize=32M`
- `-XX:+ExplicitGCInvokesConcurrent`
- `-XX:+ExitOnOutOfMemoryError`

Node parameters currently used:

- `node.environment=local`
- `node.id=trino-coordinator`
- `node.data-dir=/tmp/trino`

Hive catalog parameters currently used:

- `connector.name=hive`
- `hive.metastore.uri=thrift://hive-metastore:9083`
- `fs.native-s3.enabled=true`
- `s3.endpoint=http://minio:9000`
- `s3.region=us-east-1`
- `s3.aws-access-key=${ENV:MINIO_ROOT_USER}`
- `s3.aws-secret-key=${ENV:MINIO_ROOT_PASSWORD}`
- `s3.path-style-access=true`
- `hive.non-managed-table-writes-enabled=true`
- `hive.non-managed-table-creates-enabled=true`

### 7. Airflow

Main files:

- [docker-compose.yml](/home/dohaidang/bigdata_hudi/docker-compose.yml:1)
- [dags/hudi_pipeline_dag.py](/home/dohaidang/bigdata_hudi/dags/hudi_pipeline_dag.py:1)

Airflow container environment parameters currently used:

- `AIRFLOW__CORE__EXECUTOR=LocalExecutor`
- `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`
- `AIRFLOW__CORE__LOAD_EXAMPLES=false`
- `AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Ho_Chi_Minh`
- `AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager`
- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/execution/`
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow`
- `AIRFLOW_UID=50000`

DAG-level parameters currently used:

- `dag_id=hudi_full_pipeline`
- `start_date=datetime(2026, 5, 3)`
- `schedule=None`
- `catchup=False`
- `max_active_runs=1`
- `retries=1`
- `retry_delay=5 minutes`
- `execution_timeout=1 hour` for Spark and validation tasks

### 8. Metabase

Main service configuration source:

- [docker-compose.yml](/home/dohaidang/bigdata_hudi/docker-compose.yml:1)

Metabase application parameters currently used:

- `MB_DB_TYPE=postgres`
- `MB_DB_DBNAME=metabaseappdb`
- `MB_DB_PORT=5432`
- `MB_DB_USER=metabase`
- `MB_DB_PASS=metabase`
- `MB_DB_HOST=metabase-postgres`
- `JAVA_TIMEZONE=Asia/Ho_Chi_Minh`
- `MB_SITE_URL=http://localhost:3000`

When connecting Metabase to Trino in the UI, the effective connection settings used by this project are:

- database type: `Presto`
- host: `trino`
- port: `8080`
- catalog: `hive`
- schema: `analytics`
- username: `trino`

## Directory layout

- `docker/`: container definitions and service-specific assets
- `configs/`: runtime configs for Spark, Hudi, Trino, and Airflow
- `data/raw/`: landing zone for source datasets and API extracts
- `data/bronze/`: raw-modeled Hudi tables
- `data/silver/`: cleaned and conformed Hudi tables
- `data/gold/`: serving marts for BI and analytics
- `pipelines/extract/`: ingestion jobs from files or APIs
- `pipelines/bronze/`: raw-to-bronze load jobs
- `pipelines/silver/`: refinement and upsert jobs
- `pipelines/gold/`: mart-building jobs
- `pipelines/common/`: shared helpers, schemas, and utilities
- `sql/ddl/`: external table definitions and setup SQL
- `sql/queries/`: validation and demo queries
- `dags/`: Airflow orchestration
- `docs/architecture/`: diagrams and design notes
- `docs/runbooks/`: operational steps and troubleshooting
- `scripts/`: bootstrap and local utility scripts
- `tests/`: pipeline tests
