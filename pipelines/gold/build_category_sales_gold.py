"""Build category_sales_gold from silver tables."""

from __future__ import annotations

from pathlib import Path
import sys

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.common.gold_job import (
    build_spark_session,
    parse_gold_job_args,
    read_parquet_source,
    write_gold_output,
)
from pipelines.common.paths import gold_local_path, silver_local_path

TABLE_NAME = "category_sales_gold"
OUTPUT_PATH = gold_local_path(TABLE_NAME)


def transform(order_items_df: DataFrame, products_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    base_df = (
        order_items_df.alias("oi")
        .join(products_df.alias("p"), on="product_id", how="left")
        .join(
            orders_df.select("order_id", "order_purchase_date", "order_status").alias("o"),
            on="order_id",
            how="left",
        )
        .withColumn(
            "category_name",
            F.coalesce(F.col("product_category_name_english"), F.col("product_category_name"), F.lit("unknown")),
        )
    )

    return (
        base_df.groupBy("order_purchase_date", "category_name")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("order_item_key").alias("item_count"),
            F.sum("price").alias("gross_item_value"),
            F.sum("freight_value").alias("gross_freight_value"),
        )
        .withColumn(
            "gold_key",
            F.concat_ws("_", F.date_format(F.col("order_purchase_date"), "yyyy-MM-dd"), F.col("category_name")),
        )
        .orderBy("order_purchase_date", "category_name")
    )


def main() -> None:
    args = parse_gold_job_args(default_output_path=OUTPUT_PATH)
    spark = build_spark_session("build_category_sales_gold")
    order_items_df = read_parquet_source(spark, silver_local_path("order_items_silver"))
    products_df = read_parquet_source(spark, silver_local_path("products_silver"))
    orders_df = read_parquet_source(spark, silver_local_path("orders_silver"))
    gold_df = transform(order_items_df, products_df, orders_df)
    write_gold_output(
        gold_df,
        table_name=TABLE_NAME,
        output_path=args.output_path,
        output_format=args.output_format,
        mode=args.mode,
        record_key="gold_key",
        precombine_field="order_purchase_date",
        partition_field="order_purchase_date",
    )
    spark.stop()


if __name__ == "__main__":
    main()
