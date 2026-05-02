"""Build order_items_silver from order_items_bronze."""

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

TABLE_NAME = "order_items_silver"
INPUT_TABLE = "order_items_bronze"
OUTPUT_PATH = "data/silver/order_items_silver"


def transform(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["order_item_key"])
        .withColumn("price", F.col("price").cast("decimal(12,2)"))
        .withColumn("freight_value", F.col("freight_value").cast("decimal(12,2)"))
        .withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))
    )


def main() -> None:
    args = parse_silver_job_args(
        default_input_path=bronze_local_path(INPUT_TABLE),
        default_output_path=OUTPUT_PATH,
    )
    spark = build_spark_session("load_order_items_silver")
    df = read_parquet_source(spark, args.input_path)
    silver_df = transform(df)
    write_silver_output(
        silver_df,
        table_name=TABLE_NAME,
        output_path=args.output_path,
        output_format=args.output_format,
        mode=args.mode,
        record_key="order_item_key",
        precombine_field="shipping_limit_date",
        partition_field="dt",
    )
    spark.stop()


if __name__ == "__main__":
    main()
