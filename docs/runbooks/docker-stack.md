# Docker Stack

## Purpose

This document explains the local Docker stack used by the project, why each service exists, and which repository files are responsible for bootstrapping it.

## Main entrypoint

The stack starts from:

- [docker-compose.yml](/home/dohaidang/bigdata_hudi/docker-compose.yml:1)

This file defines all local services required for the Hudi pipeline MVP:
- object storage
- metadata store
- SQL query engine
- Spark processing
- Airflow orchestration

## Service overview

### `minio`

Role:
- provides S3-compatible object storage for the lakehouse
- acts as the storage target for raw, bronze, silver, and gold data

Why it exists:
- local machines do not have Amazon S3
- MinIO gives the same access pattern through the S3 API

Important ports:
- `9000`: S3 API endpoint
- `9001`: MinIO console

### `minio-init`

Role:
- initializes the MinIO bucket after MinIO becomes healthy

Why it exists:
- avoids manual bucket creation every time the stack starts
- prepares the `lakehouse` bucket automatically

This container is short-lived and exits after setup.

### `metastore-postgres`

Role:
- stores Hive Metastore metadata in PostgreSQL

Why it exists:
- Hive Metastore needs a relational backend for persistent metadata
- keeping it separate from Airflow avoids mixing two concerns

### `hive-metastore`

Role:
- stores and serves table metadata for Spark and Trino

Why it exists:
- Hudi tables need a shared metadata layer when exposed to SQL engines
- Trino reads table definitions through Hive-compatible metadata

Build note:
- this service is built from a local Dockerfile so the PostgreSQL JDBC driver can be added to the Hive image

Important port:
- `9083`: thrift metastore endpoint

### `trino`

Role:
- query engine for SQL access over lakehouse tables

Why it exists:
- lets you validate bronze, silver, and gold tables with SQL
- can later back Superset or ad hoc analytics

Important port:
- `8081` on host maps to Trino `8080` in the container

### `spark-master`

Role:
- Spark cluster coordinator

Why it exists:
- receives Spark jobs and manages execution resources
- gives one stable endpoint for local `spark-submit`

Image choice:
- uses the official `spark` image instead of Bitnami because the Bitnami tags referenced earlier were no longer pullable

Important ports:
- `7077`: Spark master endpoint
- `8082`: Spark UI

### `spark-worker`

Role:
- executes Spark tasks assigned by the master

Why it exists:
- separates control plane from execution plane
- mirrors how distributed processing works, even in a small local stack

Important port:
- `8083`: Spark worker UI

### `airflow-postgres`

Role:
- metadata database for Airflow

Why it exists:
- Airflow needs a persistent metadata database
- stores DAG run history, task state, connections, and users

### `airflow-init`

Role:
- initializes the Airflow database and creates the admin user

Why it exists:
- avoids manual bootstrap commands
- ensures `airflow-webserver` and `airflow-scheduler` only start after setup

This container is short-lived and exits after initialization.

### `airflow-webserver`

Role:
- exposes the Airflow UI

Why it exists:
- gives a visual control plane for running and monitoring DAGs

Important port:
- `8080`

### `airflow-scheduler`

Role:
- schedules and triggers Airflow tasks

Why it exists:
- executes DAG timing and dependency logic
- drives the end-to-end pipeline orchestration

## File-by-file explanation

### [docker-compose.yml](/home/dohaidang/bigdata_hudi/docker-compose.yml:1)

This is the central orchestration file for the local platform.

What it defines:
- all services
- shared network
- persistent volumes
- mounted configuration files
- service dependencies

Why it matters:
- one command can boot the whole local data platform
- all container relationships are visible in one place

### [docker/airflow/Dockerfile](/home/dohaidang/bigdata_hudi/docker/airflow/Dockerfile:1)

Role:
- extends the official Airflow image with extra Python dependencies

Why it exists:
- the base Airflow image does not include every package needed for Spark-related DAG tasks
- adding packages here is cleaner than installing them manually inside a running container

### [docker/airflow/requirements.txt](/home/dohaidang/bigdata_hudi/docker/airflow/requirements.txt:1)

Role:
- declares Python packages installed into the Airflow image

Current purpose:
- Spark provider for Airflow operators
- `boto3` for S3-compatible interactions if needed in DAGs
- `pyspark` for Python-side Spark task integration

### [docker/trino/config.properties](/home/dohaidang/bigdata_hudi/docker/trino/config.properties:1)

Role:
- main Trino server configuration

What it controls:
- whether this node is the coordinator
- HTTP port
- discovery URI

### [docker/trino/jvm.config](/home/dohaidang/bigdata_hudi/docker/trino/jvm.config:1)

Role:
- JVM memory and runtime options for Trino

Why it exists:
- Trino is a JVM application and needs explicit memory tuning

### [docker/trino/node.properties](/home/dohaidang/bigdata_hudi/docker/trino/node.properties:1)

Role:
- identifies the Trino node and local storage path

Why it exists:
- Trino needs stable node identity and data directory settings
- the data directory is set under `/tmp/trino` so Trino can write to an internal temporary path without Docker volume permission issues

### [docker/hive/Dockerfile](/home/dohaidang/bigdata_hudi/docker/hive/Dockerfile:1)

Role:
- extends the Hive image with the PostgreSQL JDBC driver required by the metastore schema tool

Why it exists:
- the base Hive image does not include `org.postgresql.Driver`
- without this jar, Hive cannot initialize or connect to the external Postgres metastore
- the JDBC jar permissions are also normalized so the Hive process can read it at runtime

### [docker/trino/catalog/hive.properties](/home/dohaidang/bigdata_hudi/docker/trino/catalog/hive.properties:1)

Role:
- defines the Hive catalog used by Trino

What it connects:
- Trino to Hive Metastore
- Trino to MinIO through S3-compatible settings

Why it matters:
- without this file, Trino would start but not know where the lakehouse tables are
- on Trino 476, MinIO and S3-compatible access must use `fs.native-s3.enabled=true` with `s3.*` properties instead of the deprecated `hive.s3.*` keys

### [configs/spark/spark-defaults.conf](/home/dohaidang/bigdata_hudi/configs/spark/spark-defaults.conf:1)

Role:
- default Spark configuration shared by master and worker containers

What it configures:
- Spark master endpoint
- timezone
- S3A endpoint for MinIO
- Hive Metastore URI

Why it matters:
- centralizes Spark connectivity to the rest of the platform

### [docker/spark/start-master.sh](/home/dohaidang/bigdata_hudi/docker/spark/start-master.sh:1)

Role:
- starts the Spark master process inside the official Spark container

Why it exists:
- the official Spark image does not expose the same convenience environment variables used by the old Bitnami setup
- keeping the startup command in a script makes the compose file easier to read

### [docker/spark/start-worker.sh](/home/dohaidang/bigdata_hudi/docker/spark/start-worker.sh:1)

Role:
- starts the Spark worker process and connects it to `spark://spark-master:7077`

Why it exists:
- makes worker startup explicit and predictable
- keeps Spark cluster boot logic in one place under `docker/spark/`

### [.env.example](/home/dohaidang/bigdata_hudi/.env.example:1)

Role:
- stores environment variable defaults used by the compose stack

Why it exists:
- keeps credentials and tunable values out of the compose body
- makes it easier to create a local `.env` later

## Data path through the Docker stack

```text
Dataset or API
    -> extract job
    -> MinIO raw area
    -> Spark bronze job
    -> Hudi tables in MinIO
    -> Hive Metastore registration
    -> Trino SQL access
    -> Airflow schedules and monitors the whole flow
```

## Networking model

All services use one Docker network:

- `lakehouse`

Why this is enough:
- services can resolve each other by service name
- examples:
  - `http://minio:9000`
  - `thrift://hive-metastore:9083`
  - `spark://spark-master:7077`
  - `postgresql://airflow-postgres:5432/airflow`

## Persistent volumes

The compose file defines named Docker volumes for stateful services:

- `minio_data`
- `metastore_postgres_data`
- `airflow_postgres_data`
- `warehouse_data`

Why these exist:
- container recreation should not wipe important metadata immediately
- local development becomes repeatable across restarts

## What this stack does not solve yet

This stack is a good MVP base, but it does not yet include:
- Hudi bundle jars for Spark jobs
- automatic Hive sync from Spark writers
- Superset
- Kafka or Debezium
- monitoring with Prometheus or Grafana

Those can be added after the first bronze and silver jobs are working.

## Recommended startup order

Use this sequence during development:

1. build and start the core stack
2. verify MinIO, Hive Metastore, Trino, Spark, and Airflow are healthy
3. place sample data in `data/raw/`
4. run the first Spark bronze job
5. validate results through Trino or Spark SQL

## Summary

The Docker layer is responsible for local infrastructure, not business logic.

In this repository:
- `docker-compose.yml` wires services together
- files under `docker/` configure service internals
- files under `configs/` configure application runtime behavior

That split keeps infrastructure concerns separate from pipeline code and makes the project easier to extend.
