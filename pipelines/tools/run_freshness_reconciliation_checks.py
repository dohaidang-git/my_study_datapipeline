"""Run freshness and reconciliation checks on key Hudi silver and gold tables."""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common.paths import gold_hudi_path, silver_hudi_path
from pipelines.common.spark_session import build_spark_session


DECIMAL_TOLERANCE = Decimal("0.01")


def log_check(name: str, passed: bool, detail: str) -> None:
    status = "PASS" if passed else "FAIL"
    print(f"{status} check={name} detail={detail}")


def normalize_decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def assert_equal_int(name: str, actual: int, expected: int, failures: list[str]) -> None:
    passed = actual == expected
    log_check(name, passed, f"actual={actual} expected={expected}")
    if not passed:
        failures.append(f"{name}: expected {expected}, got {actual}")


def assert_equal_decimal(name: str, actual, expected, failures: list[str], tolerance: Decimal = DECIMAL_TOLERANCE) -> None:
    actual_decimal = normalize_decimal(actual)
    expected_decimal = normalize_decimal(expected)
    diff = abs(actual_decimal - expected_decimal)
    passed = diff <= tolerance
    log_check(name, passed, f"actual={actual_decimal} expected={expected_decimal} diff={diff}")
    if not passed:
        failures.append(f"{name}: expected {expected_decimal}, got {actual_decimal}, diff {diff}")


def assert_equal_value(name: str, actual, expected, failures: list[str]) -> None:
    passed = actual == expected
    log_check(name, passed, f"actual={actual} expected={expected}")
    if not passed:
        failures.append(f"{name}: expected {expected}, got {actual}")


def scalar(df: DataFrame, expression, alias: str = "value"):
    return df.agg(expression.alias(alias)).first()[alias]


def main() -> None:
    spark = build_spark_session("run_freshness_reconciliation_checks")
    failures: list[str] = []

    orders_silver = spark.read.format("hudi").load(silver_hudi_path("orders_silver"))
    order_items_silver = spark.read.format("hudi").load(silver_hudi_path("order_items_silver"))
    payments_silver = spark.read.format("hudi").load(silver_hudi_path("payments_silver"))
    daily_sales_gold = spark.read.format("hudi").load(gold_hudi_path("daily_sales_gold"))
    category_sales_gold = spark.read.format("hudi").load(gold_hudi_path("category_sales_gold"))
    customer_ltv_gold = spark.read.format("hudi").load(gold_hudi_path("customer_ltv_gold"))

    max_orders_date = scalar(orders_silver, F.max("order_purchase_date"), "max_orders_date")
    max_orders_with_items_date = scalar(
        orders_silver.join(order_items_silver.select("order_id").distinct(), on="order_id", how="inner"),
        F.max("order_purchase_date"),
        "max_orders_with_items_date",
    )
    max_daily_date = scalar(daily_sales_gold, F.max("order_date"), "max_daily_date")
    max_category_date = scalar(category_sales_gold, F.max("order_purchase_date"), "max_category_date")
    max_customer_last_order = scalar(customer_ltv_gold, F.max("last_order_date"), "max_customer_last_order")
    min_orders_date = scalar(orders_silver, F.min("order_purchase_date"), "min_orders_date")
    min_daily_date = scalar(daily_sales_gold, F.min("order_date"), "min_daily_date")

    assert_equal_value("freshness.max_date.orders_vs_daily", max_orders_date, max_daily_date, failures)
    assert_equal_value(
        "freshness.max_date.orders_with_items_vs_category",
        max_orders_with_items_date,
        max_category_date,
        failures,
    )
    assert_equal_value("freshness.max_date.orders_vs_customer_ltv", max_orders_date, max_customer_last_order, failures)
    assert_equal_value("freshness.min_date.orders_vs_daily", min_orders_date, min_daily_date, failures)

    orders_count = orders_silver.select("order_id").distinct().count()
    daily_order_count = int(scalar(daily_sales_gold, F.sum("order_count"), "daily_order_count") or 0)
    customer_ltv_order_count = int(scalar(customer_ltv_gold, F.sum("order_count"), "customer_ltv_order_count") or 0)
    assert_equal_int("reconcile.order_count.orders_vs_daily", daily_order_count, orders_count, failures)
    assert_equal_int("reconcile.order_count.orders_vs_customer_ltv", customer_ltv_order_count, orders_count, failures)

    order_items_count = order_items_silver.select("order_item_key").distinct().count()
    category_item_count = int(scalar(category_sales_gold, F.sum("item_count"), "category_item_count") or 0)
    assert_equal_int("reconcile.item_count.order_items_vs_category", category_item_count, order_items_count, failures)

    payments_total = scalar(payments_silver, F.sum("payment_value"), "payments_total")
    daily_payment_total = scalar(daily_sales_gold, F.sum("payment_value"), "daily_payment_total")
    customer_ltv_total = scalar(customer_ltv_gold, F.sum("lifetime_value"), "customer_ltv_total")
    assert_equal_decimal("reconcile.payment_value.payments_vs_daily", daily_payment_total, payments_total, failures)
    assert_equal_decimal("reconcile.payment_value.payments_vs_customer_ltv", customer_ltv_total, payments_total, failures)

    order_items_price_total = scalar(order_items_silver, F.sum("price"), "order_items_price_total")
    daily_gross_item_total = scalar(daily_sales_gold, F.sum("gross_item_value"), "daily_gross_item_total")
    category_gross_item_total = scalar(category_sales_gold, F.sum("gross_item_value"), "category_gross_item_total")
    assert_equal_decimal("reconcile.gross_item_value.order_items_vs_daily", daily_gross_item_total, order_items_price_total, failures)
    assert_equal_decimal(
        "reconcile.gross_item_value.order_items_vs_category",
        category_gross_item_total,
        order_items_price_total,
        failures,
    )

    order_items_freight_total = scalar(order_items_silver, F.sum("freight_value"), "order_items_freight_total")
    daily_gross_freight_total = scalar(daily_sales_gold, F.sum("gross_freight_value"), "daily_gross_freight_total")
    category_gross_freight_total = scalar(category_sales_gold, F.sum("gross_freight_value"), "category_gross_freight_total")
    assert_equal_decimal(
        "reconcile.gross_freight_value.order_items_vs_daily",
        daily_gross_freight_total,
        order_items_freight_total,
        failures,
    )
    assert_equal_decimal(
        "reconcile.gross_freight_value.order_items_vs_category",
        category_gross_freight_total,
        order_items_freight_total,
        failures,
    )

    spark.stop()

    if failures:
        raise SystemExit("Freshness/reconciliation checks failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()
