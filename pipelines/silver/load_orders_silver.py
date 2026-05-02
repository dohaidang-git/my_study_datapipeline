"""Build orders_silver from orders_bronze."""

from __future__ import annotations

from pathlib import Path
import sys

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.common.paths import bronze_local_path
from pipelines.common.silver_job import (
    build_spark_session,
    parse_silver_job_args,
    read_parquet_source,
    write_silver_output,
)

TABLE_NAME = "orders_silver"
INPUT_TABLE = "orders_bronze"
OUTPUT_PATH = "data/silver/orders_silver"


def transform(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["order_id"])
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn("order_purchase_date", F.to_date(F.col("order_purchase_timestamp")))
        .withColumn(
            "delivery_delay_days",
            F.datediff(F.col("order_delivered_customer_date"), F.col("order_estimated_delivery_date")),
        )
    )


def main() -> None:
    args = parse_silver_job_args(
        default_input_path=bronze_local_path(INPUT_TABLE),
        default_output_path=OUTPUT_PATH,
    )
    spark = build_spark_session("load_orders_silver")
    df = read_parquet_source(spark, args.input_path)
    silver_df = transform(df)
    write_silver_output(
        silver_df,
        table_name=TABLE_NAME,
        output_path=args.output_path,
        output_format=args.output_format,
        mode=args.mode,
        record_key="order_id",
        precombine_field="order_purchase_timestamp",
        partition_field="order_purchase_date",
    )
    spark.stop()


if __name__ == "__main__":
    main()
