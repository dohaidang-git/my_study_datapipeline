"""Run data quality checks on key Hudi silver and gold tables."""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common.paths import gold_hudi_path, silver_hudi_path
from pipelines.common.spark_session import build_spark_session


def log_check(name: str, passed: bool, detail: str) -> None:
    status = "PASS" if passed else "FAIL"
    print(f"{status} check={name} detail={detail}")


def violation_count(df: DataFrame, condition: Column) -> int:
    return df.filter(condition).count()


def check_no_nulls(df: DataFrame, table_name: str, columns: list[str], failures: list[str]) -> None:
    for column_name in columns:
        count = violation_count(df, F.col(column_name).isNull())
        passed = count == 0
        log_check(f"{table_name}.not_null.{column_name}", passed, f"violations={count}")
        if not passed:
            failures.append(f"{table_name}: column '{column_name}' contains {count} null rows")


def check_unique(df: DataFrame, table_name: str, columns: list[str], failures: list[str]) -> None:
    total_rows = df.count()
    distinct_rows = df.select(*columns).distinct().count()
    duplicates = total_rows - distinct_rows
    column_label = ",".join(columns)
    passed = duplicates == 0
    log_check(f"{table_name}.unique.{column_label}", passed, f"duplicates={duplicates}")
    if not passed:
        failures.append(f"{table_name}: key '{column_label}' has {duplicates} duplicate rows")


def check_condition(df: DataFrame, table_name: str, check_name: str, condition: Column, failures: list[str]) -> None:
    count = violation_count(df, condition)
    passed = count == 0
    log_check(f"{table_name}.{check_name}", passed, f"violations={count}")
    if not passed:
        failures.append(f"{table_name}: check '{check_name}' failed for {count} rows")


def main() -> None:
    spark = build_spark_session("run_data_quality_checks")
    failures: list[str] = []

    orders_silver = spark.read.format("hudi").load(silver_hudi_path("orders_silver"))
    customers_silver = spark.read.format("hudi").load(silver_hudi_path("customers_silver"))
    payments_silver = spark.read.format("hudi").load(silver_hudi_path("payments_silver"))
    reviews_silver = spark.read.format("hudi").load(silver_hudi_path("reviews_silver"))
    geolocation_silver = spark.read.format("hudi").load(silver_hudi_path("geolocation_silver"))
    products_silver = spark.read.format("hudi").load(silver_hudi_path("products_silver"))
    daily_sales_gold = spark.read.format("hudi").load(gold_hudi_path("daily_sales_gold"))
    category_sales_gold = spark.read.format("hudi").load(gold_hudi_path("category_sales_gold"))
    customer_ltv_gold = spark.read.format("hudi").load(gold_hudi_path("customer_ltv_gold"))

    check_no_nulls(orders_silver, "orders_silver", ["order_id", "customer_id", "order_purchase_date"], failures)
    check_unique(orders_silver, "orders_silver", ["order_id"], failures)
    check_condition(
        orders_silver,
        "orders_silver",
        "order_status_trimmed_lowercase",
        F.col("order_status") != F.lower(F.trim(F.col("order_status"))),
        failures,
    )

    check_no_nulls(customers_silver, "customers_silver", ["customer_id", "customer_unique_id", "customer_state"], failures)
    check_unique(customers_silver, "customers_silver", ["customer_id"], failures)
    check_condition(
        customers_silver,
        "customers_silver",
        "customer_state_has_len_2",
        F.length(F.col("customer_state")) != 2,
        failures,
    )

    check_no_nulls(payments_silver, "payments_silver", ["payment_key", "order_id", "payment_type"], failures)
    check_unique(payments_silver, "payments_silver", ["payment_key"], failures)
    check_condition(payments_silver, "payments_silver", "payment_value_non_negative", F.col("payment_value") < 0, failures)
    check_condition(
        payments_silver,
        "payments_silver",
        "payment_installments_positive",
        F.col("payment_installments") <= 0,
        failures,
    )

    check_no_nulls(reviews_silver, "reviews_silver", ["review_id", "order_id", "review_score"], failures)
    check_unique(reviews_silver, "reviews_silver", ["review_id"], failures)
    check_condition(
        reviews_silver,
        "reviews_silver",
        "review_score_between_1_and_5",
        (F.col("review_score") < 1) | (F.col("review_score") > 5),
        failures,
    )

    check_no_nulls(
        geolocation_silver,
        "geolocation_silver",
        ["geolocation_key", "geolocation_zip_code_prefix", "geolocation_state"],
        failures,
    )
    check_unique(geolocation_silver, "geolocation_silver", ["geolocation_key"], failures)
    check_condition(
        geolocation_silver,
        "geolocation_silver",
        "geolocation_state_has_len_2",
        F.length(F.col("geolocation_state")) != 2,
        failures,
    )

    check_no_nulls(products_silver, "products_silver", ["product_id"], failures)
    check_unique(products_silver, "products_silver", ["product_id"], failures)

    check_no_nulls(daily_sales_gold, "daily_sales_gold", ["order_date"], failures)
    check_unique(daily_sales_gold, "daily_sales_gold", ["order_date"], failures)
    check_condition(daily_sales_gold, "daily_sales_gold", "order_count_positive", F.col("order_count") <= 0, failures)
    check_condition(daily_sales_gold, "daily_sales_gold", "payment_value_non_negative", F.col("payment_value") < 0, failures)
    check_condition(
        daily_sales_gold,
        "daily_sales_gold",
        "gross_item_value_non_negative",
        F.col("gross_item_value") < 0,
        failures,
    )

    check_no_nulls(category_sales_gold, "category_sales_gold", ["order_purchase_date", "category_name"], failures)
    check_unique(category_sales_gold, "category_sales_gold", ["order_purchase_date", "category_name"], failures)
    check_condition(
        category_sales_gold,
        "category_sales_gold",
        "gross_values_non_negative",
        (F.col("gross_item_value") < 0) | (F.col("gross_freight_value") < 0),
        failures,
    )
    check_condition(
        category_sales_gold,
        "category_sales_gold",
        "counts_positive",
        (F.col("order_count") <= 0) | (F.col("item_count") <= 0),
        failures,
    )

    check_no_nulls(
        customer_ltv_gold,
        "customer_ltv_gold",
        ["customer_id", "customer_unique_id", "order_count", "lifetime_value", "ltv_rank"],
        failures,
    )
    check_unique(customer_ltv_gold, "customer_ltv_gold", ["customer_id"], failures)
    check_unique(customer_ltv_gold, "customer_ltv_gold", ["ltv_rank"], failures)
    check_condition(customer_ltv_gold, "customer_ltv_gold", "order_count_positive", F.col("order_count") <= 0, failures)
    check_condition(
        customer_ltv_gold,
        "customer_ltv_gold",
        "lifetime_value_non_negative",
        F.col("lifetime_value") < 0,
        failures,
    )
    check_condition(
        customer_ltv_gold,
        "customer_ltv_gold",
        "first_order_before_last_order",
        F.col("first_order_date") > F.col("last_order_date"),
        failures,
    )

    spark.stop()

    if failures:
        raise SystemExit("Data quality checks failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()
