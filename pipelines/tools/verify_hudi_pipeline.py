"""Verify Hudi tables across bronze, silver, and gold layers."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common.paths import bronze_hudi_path, gold_hudi_path, silver_hudi_path
from pipelines.common.spark_session import build_spark_session


TABLE_SPECS = [
    ("bronze", "orders_bronze", bronze_hudi_path("orders_bronze"), 99441),
    ("bronze", "order_items_bronze", bronze_hudi_path("order_items_bronze"), 112650),
    ("bronze", "customers_bronze", bronze_hudi_path("customers_bronze"), 99441),
    ("bronze", "payments_bronze", bronze_hudi_path("payments_bronze"), 103886),
    ("bronze", "products_bronze", bronze_hudi_path("products_bronze"), 32951),
    ("bronze", "sellers_bronze", bronze_hudi_path("sellers_bronze"), 3095),
    ("bronze", "reviews_bronze", bronze_hudi_path("reviews_bronze"), 99224),
    ("bronze", "geolocation_bronze", bronze_hudi_path("geolocation_bronze"), 1000163),
    (
        "bronze",
        "product_category_translation_bronze",
        bronze_hudi_path("product_category_translation_bronze"),
        71,
    ),
    ("silver", "orders_silver", silver_hudi_path("orders_silver"), 99441),
    ("silver", "order_items_silver", silver_hudi_path("order_items_silver"), 112650),
    ("silver", "customers_silver", silver_hudi_path("customers_silver"), 99441),
    ("silver", "payments_silver", silver_hudi_path("payments_silver"), 103886),
    ("silver", "products_silver_base", silver_hudi_path("products_silver_base"), 32951),
    ("silver", "sellers_silver", silver_hudi_path("sellers_silver"), 3095),
    ("silver", "reviews_silver", silver_hudi_path("reviews_silver"), 98410),
    ("silver", "geolocation_silver", silver_hudi_path("geolocation_silver"), 720154),
    (
        "silver",
        "product_category_translation_silver",
        silver_hudi_path("product_category_translation_silver"),
        71,
    ),
    ("silver", "products_silver", silver_hudi_path("products_silver"), 32951),
    ("gold", "daily_sales_gold", gold_hudi_path("daily_sales_gold"), 634),
    ("gold", "category_sales_gold", gold_hudi_path("category_sales_gold"), 18990),
    ("gold", "customer_ltv_gold", gold_hudi_path("customer_ltv_gold"), 99441),
]


def main() -> None:
    spark = build_spark_session("verify_hudi_pipeline")
    failures: list[str] = []

    for layer, table_name, table_path, expected_count in TABLE_SPECS:
        df = spark.read.format("hudi").load(table_path)
        actual_count = df.count()
        status = "OK" if actual_count == expected_count else "MISMATCH"
        print(
            f"{status} layer={layer} table={table_name} rows={actual_count} expected={expected_count} path={table_path}"
        )
        if actual_count != expected_count:
            failures.append(f"{table_name}: expected {expected_count}, got {actual_count}")

    spark.stop()

    if failures:
        raise SystemExit("Verification failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()
