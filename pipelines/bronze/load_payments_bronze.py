"""Load raw Olist payments into the bronze layer."""

from __future__ import annotations

from pathlib import Path
import sys

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.common.bronze_job import finalize_bronze_df, read_csv_source, write_bronze_output
from pipelines.common.job_args import parse_bronze_job_args
from pipelines.common.paths import bronze_hudi_path, raw_olist_path
from pipelines.common.spark_session import build_spark_session
TABLE_NAME = "payments_bronze"
SOURCE_FILE = "olist_order_payments_dataset.csv"


def transform(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
        .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
        .withColumn("payment_installments", F.col("payment_installments").cast("int"))
        .withColumn("payment_value", F.col("payment_value").cast("decimal(12,2)"))
        .withColumn(
            "payment_key",
            F.concat_ws("_", F.col("order_id"), F.col("payment_sequential")),
        )
    )


def main() -> None:
    args = parse_bronze_job_args(
        default_input_path=raw_olist_path(SOURCE_FILE),
        default_output_path=bronze_hudi_path(TABLE_NAME),
    )
    spark = build_spark_session("load_payments_bronze")
    df = read_csv_source(spark, args.input_path)
    bronze_df = finalize_bronze_df(
        transform(df),
        batch_id=args.batch_id,
        source_file=SOURCE_FILE,
        source_system=args.source_system,
    )
    write_bronze_output(
        bronze_df,
        table_name=TABLE_NAME,
        output_path=args.output_path,
        output_format=args.output_format,
        mode=args.mode,
        record_key="payment_key",
        precombine_field="_ingested_at",
    )
    spark.stop()


if __name__ == "__main__":
    main()
