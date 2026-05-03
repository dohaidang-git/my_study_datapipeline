"""Airflow DAG for the local Hudi lakehouse pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SCRIPTS_DIR = "/opt/airflow/scripts"


def spark_job_task(task_id: str, job_path: str, extra_args: str = "") -> BashOperator:
    command = f"set -e; bash {SCRIPTS_DIR}/spark_submit_container.sh {job_path}"
    if extra_args:
        command = f"{command} {extra_args}"
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        cwd="/opt/airflow",
        execution_timeout=timedelta(hours=1),
    )


def shell_task(task_id: str, command: str, timeout: timedelta) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        cwd="/opt/airflow",
        execution_timeout=timeout,
    )


with DAG(
    dag_id="hudi_full_pipeline",
    description="Run bronze, silver, gold, and validation tasks for the local Hudi pipeline.",
    start_date=datetime(2026, 5, 3),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["hudi", "lakehouse", "spark", "trino"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")
    preflight_bronze_runtime = shell_task(
        "preflight_bronze_runtime",
        f"set -e; /usr/bin/env bash {SCRIPTS_DIR}/check_required_containers.sh minio spark-master ",
        timedelta(minutes=5),
    )
    preflight_trino_runtime = shell_task(
        "preflight_trino_runtime",
        f"set -e; /usr/bin/env bash {SCRIPTS_DIR}/check_required_containers.sh minio hive-metastore trino ",
        timedelta(minutes=5),
    )

    with TaskGroup(group_id="bronze") as bronze_group:
        orders_bronze = spark_job_task(
            "orders_bronze",
            "pipelines/bronze/load_orders_bronze.py",
            "--output-format hudi",
        )
        order_items_bronze = spark_job_task(
            "order_items_bronze",
            "pipelines/bronze/load_order_items_bronze.py",
            "--output-format hudi",
        )
        customers_bronze = spark_job_task(
            "customers_bronze",
            "pipelines/bronze/load_customers_bronze.py",
            "--output-format hudi",
        )
        payments_bronze = spark_job_task(
            "payments_bronze",
            "pipelines/bronze/load_payments_bronze.py",
            "--output-format hudi",
        )
        products_bronze = spark_job_task(
            "products_bronze",
            "pipelines/bronze/load_products_bronze.py",
            "--output-format hudi",
        )
        sellers_bronze = spark_job_task(
            "sellers_bronze",
            "pipelines/bronze/load_sellers_bronze.py",
            "--output-format hudi",
        )
        reviews_bronze = spark_job_task(
            "reviews_bronze",
            "pipelines/bronze/load_reviews_bronze.py",
            "--output-format hudi",
        )
        geolocation_bronze = spark_job_task(
            "geolocation_bronze",
            "pipelines/bronze/load_geolocation_bronze.py",
            "--output-format hudi",
        )
        product_category_translation_bronze = spark_job_task(
            "product_category_translation_bronze",
            "pipelines/bronze/load_product_category_translation_bronze.py",
            "--output-format hudi",
        )

    with TaskGroup(group_id="silver") as silver_group:
        orders_silver = spark_job_task(
            "orders_silver",
            "pipelines/silver/load_orders_silver.py",
            "--input-format hudi --output-format hudi",
        )
        order_items_silver = spark_job_task(
            "order_items_silver",
            "pipelines/silver/load_order_items_silver.py",
            "--input-format hudi --output-format hudi",
        )
        customers_silver = spark_job_task(
            "customers_silver",
            "pipelines/silver/load_customers_silver.py",
            "--input-format hudi --output-format hudi",
        )
        payments_silver = spark_job_task(
            "payments_silver",
            "pipelines/silver/load_payments_silver.py",
            "--input-format hudi --output-format hudi",
        )
        products_silver_base = spark_job_task(
            "products_silver_base",
            "pipelines/silver/load_products_silver_base.py",
            "--input-format hudi --output-format hudi",
        )
        sellers_silver = spark_job_task(
            "sellers_silver",
            "pipelines/silver/load_sellers_silver.py",
            "--input-format hudi --output-format hudi",
        )
        reviews_silver = spark_job_task(
            "reviews_silver",
            "pipelines/silver/load_reviews_silver.py",
            "--input-format hudi --output-format hudi",
        )
        geolocation_silver = spark_job_task(
            "geolocation_silver",
            "pipelines/silver/load_geolocation_silver.py",
            "--input-format hudi --output-format hudi",
        )
        product_category_translation_silver = spark_job_task(
            "product_category_translation_silver",
            "pipelines/silver/load_product_category_translation_silver.py",
            "--input-format hudi --output-format hudi",
        )
        products_silver = spark_job_task(
            "products_silver",
            "pipelines/silver/load_products_silver.py",
            "--input-format hudi --output-format hudi",
        )

    with TaskGroup(group_id="gold") as gold_group:
        daily_sales_gold = spark_job_task(
            "daily_sales_gold",
            "pipelines/gold/build_daily_sales_gold.py",
            "--input-format hudi --output-format hudi",
        )
        category_sales_gold = spark_job_task(
            "category_sales_gold",
            "pipelines/gold/build_category_sales_gold.py",
            "--input-format hudi --output-format hudi",
        )
        customer_ltv_gold = spark_job_task(
            "customer_ltv_gold",
            "pipelines/gold/build_customer_ltv_gold.py",
            "--input-format hudi --output-format hudi",
        )

    verify_hudi_pipeline = shell_task(
        "verify_hudi_pipeline",
        f"set -e; bash {SCRIPTS_DIR}/spark_submit_container.sh pipelines/tools/verify_hudi_pipeline.py",
        timedelta(hours=1),
    )
    quality_checks_hudi = shell_task(
        "quality_checks_hudi",
        f"set -e; bash {SCRIPTS_DIR}/spark_submit_container.sh pipelines/tools/run_data_quality_checks.py",
        timedelta(hours=1),
    )
    freshness_reconciliation_hudi = shell_task(
        "freshness_reconciliation_hudi",
        f"set -e; bash {SCRIPTS_DIR}/spark_submit_container.sh pipelines/tools/run_freshness_reconciliation_checks.py",
        timedelta(hours=1),
    )

    verify_trino_gold = shell_task(
        "verify_trino_gold",
        f"set -e; /usr/bin/env bash {SCRIPTS_DIR}/run_trino_gold_checks.sh ",
        timedelta(minutes=20),
    )

    start >> preflight_bronze_runtime >> bronze_group

    orders_bronze >> orders_silver
    order_items_bronze >> order_items_silver
    customers_bronze >> customers_silver
    payments_bronze >> payments_silver
    products_bronze >> products_silver_base
    sellers_bronze >> sellers_silver
    reviews_bronze >> reviews_silver
    geolocation_bronze >> geolocation_silver
    product_category_translation_bronze >> product_category_translation_silver

    [products_silver_base, product_category_translation_silver] >> products_silver

    [orders_silver, order_items_silver, payments_silver] >> daily_sales_gold
    [order_items_silver, products_silver, orders_silver] >> category_sales_gold
    [orders_silver, customers_silver, payments_silver] >> customer_ltv_gold

    [daily_sales_gold, category_sales_gold, customer_ltv_gold] >> verify_hudi_pipeline
    verify_hudi_pipeline >> quality_checks_hudi >> freshness_reconciliation_hudi
    freshness_reconciliation_hudi >> preflight_trino_runtime >> verify_trino_gold >> finish
