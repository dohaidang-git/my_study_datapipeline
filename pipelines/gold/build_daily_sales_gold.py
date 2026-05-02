"""Build daily_sales_gold from silver tables."""

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
    read_table_source,
    write_gold_output,
)
from pipelines.common.paths import gold_hudi_path, silver_hudi_path

TABLE_NAME = "daily_sales_gold"
OUTPUT_PATH = gold_hudi_path(TABLE_NAME)


def build_order_items_agg(order_items_df: DataFrame) -> DataFrame:
    return order_items_df.groupBy("order_id").agg(
        F.countDistinct("order_item_key").alias("item_count"),
        F.sum("price").alias("gross_item_value"),
        F.sum("freight_value").alias("gross_freight_value"),
    )


def build_payments_agg(payments_df: DataFrame) -> DataFrame:
    return payments_df.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_value"),
        F.countDistinct("payment_key").alias("payment_count"),
    )


def transform(orders_df: DataFrame, order_items_df: DataFrame, payments_df: DataFrame) -> DataFrame:
    order_items_agg = build_order_items_agg(order_items_df)
    payments_agg = build_payments_agg(payments_df)

    order_level = (
        orders_df.alias("o")
        .join(order_items_agg.alias("oi"), on="order_id", how="left")
        .join(payments_agg.alias("p"), on="order_id", how="left")
        .withColumn("order_date", F.col("order_purchase_date"))
        .fillna({"item_count": 0, "gross_item_value": 0, "gross_freight_value": 0, "payment_value": 0, "payment_count": 0})
    )

    return (
        order_level.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("item_count").alias("item_count"),
            F.sum("gross_item_value").alias("gross_item_value"),
            F.sum("gross_freight_value").alias("gross_freight_value"),
            F.sum("payment_value").alias("payment_value"),
            F.avg("payment_value").alias("avg_order_payment_value"),
        )
        .withColumn("gold_key", F.date_format(F.col("order_date"), "yyyy-MM-dd"))
        .orderBy("order_date")
    )


def main() -> None:
    args = parse_gold_job_args(default_output_path=OUTPUT_PATH)
    spark = build_spark_session("build_daily_sales_gold")
    orders_df = read_table_source(spark, silver_hudi_path("orders_silver"), args.input_format)
    order_items_df = read_table_source(spark, silver_hudi_path("order_items_silver"), args.input_format)
    payments_df = read_table_source(spark, silver_hudi_path("payments_silver"), args.input_format)
    gold_df = transform(orders_df, order_items_df, payments_df)
    write_gold_output(
        gold_df,
        table_name=TABLE_NAME,
        output_path=args.output_path,
        output_format=args.output_format,
        mode=args.mode,
        record_key="gold_key",
        precombine_field="order_date",
        partition_field="order_date",
    )
    spark.stop()


if __name__ == "__main__":
    main()
