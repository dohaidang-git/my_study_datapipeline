# Big Data Hudi E-commerce Pipeline

Scaffold for a local e-commerce data pipeline built around Apache Hudi.

Detailed structure documentation: [docs/architecture/project-structure.md](/home/dohaidang/bigdata_hudi/docs/architecture/project-structure.md:1)
System design documentation: [docs/architecture/system-design.md](/home/dohaidang/bigdata_hudi/docs/architecture/system-design.md:1)
Data mapping documentation: [docs/architecture/data-mapping.md](/home/dohaidang/bigdata_hudi/docs/architecture/data-mapping.md:1)
Hudi in project documentation: [docs/architecture/hudi-trong-du-an.md](/home/dohaidang/bigdata_hudi/docs/architecture/hudi-trong-du-an.md:1)
Python files documentation: [docs/architecture/python-files-trong-project.md](/home/dohaidang/bigdata_hudi/docs/architecture/python-files-trong-project.md:1)
Input data and processing documentation: [docs/architecture/du-lieu-dau-vao-va-cach-xu-ly.md](/home/dohaidang/bigdata_hudi/docs/architecture/du-lieu-dau-vao-va-cach-xu-ly.md:1)
Airflow in project documentation: [docs/architecture/airflow-trong-du-an.md](/home/dohaidang/bigdata_hudi/docs/architecture/airflow-trong-du-an.md:1)
Docker stack documentation: [docs/runbooks/docker-stack.md](/home/dohaidang/bigdata_hudi/docs/runbooks/docker-stack.md:1)
Data quality checks documentation: [docs/runbooks/data-quality-checks.md](/home/dohaidang/bigdata_hudi/docs/runbooks/data-quality-checks.md:1)
Project checklist: [docs/runbooks/project-checklist.md](/home/dohaidang/bigdata_hudi/docs/runbooks/project-checklist.md:1)

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

## Immediate next steps

1. Expand data quality, freshness, and drift checks beyond the current baseline rules.
2. Add incremental Hudi write patterns where full reload is not the right long-term behavior.
3. Add dashboard or BI demo queries on top of `hive.analytics.*`.
