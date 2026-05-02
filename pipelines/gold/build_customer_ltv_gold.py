"""Build customer_ltv_gold from silver tables."""

from __future__ import annotations

from pathlib import Path
import sys

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from pipelines.common.gold_job import (
    build_spark_session,
    parse_gold_job_args,
    read_parquet_source,
    write_gold_output,
)
from pipelines.common.paths import gold_local_path, silver_local_path

TABLE_NAME = "customer_ltv_gold"
OUTPUT_PATH = gold_local_path(TABLE_NAME)


def build_payments_agg(payments_df: DataFrame) -> DataFrame:
    return payments_df.groupBy("order_id").agg(F.sum("payment_value").alias("payment_value"))


def transform(orders_df: DataFrame, customers_df: DataFrame, payments_df: DataFrame) -> DataFrame:
    payments_agg = build_payments_agg(payments_df)

    customer_orders_df = (
        orders_df.alias("o")
        .join(customers_df.alias("c"), on="customer_id", how="left")
        .join(payments_agg.alias("p"), on="order_id", how="left")
        .fillna({"payment_value": 0})
    )

    aggregated_df = (
        customer_orders_df.groupBy(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
        )
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("payment_value").alias("lifetime_value"),
            F.min("order_purchase_date").alias("first_order_date"),
            F.max("order_purchase_date").alias("last_order_date"),
        )
        .withColumn(
            "customer_lifespan_days",
            F.datediff(F.col("last_order_date"), F.col("first_order_date")),
        )
        .withColumn(
            "avg_order_value",
            F.when(F.col("order_count") > 0, F.col("lifetime_value") / F.col("order_count")).otherwise(F.lit(0)),
        )
    )

    customer_rank_window = Window.orderBy(F.col("lifetime_value").desc(), F.col("customer_id"))
    return aggregated_df.withColumn("ltv_rank", F.row_number().over(customer_rank_window))


def main() -> None:
    args = parse_gold_job_args(default_output_path=OUTPUT_PATH)
    spark = build_spark_session("build_customer_ltv_gold")
    orders_df = read_parquet_source(spark, silver_local_path("orders_silver"))
    customers_df = read_parquet_source(spark, silver_local_path("customers_silver"))
    payments_df = read_parquet_source(spark, silver_local_path("payments_silver"))
    gold_df = transform(orders_df, customers_df, payments_df)
    write_gold_output(
        gold_df,
        table_name=TABLE_NAME,
        output_path=args.output_path,
        output_format=args.output_format,
        mode=args.mode,
        record_key="customer_id",
        precombine_field="last_order_date",
    )
    spark.stop()


if __name__ == "__main__":
    main()
