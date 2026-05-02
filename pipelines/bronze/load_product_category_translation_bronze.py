"""Load raw product category translations into the bronze layer."""

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

TABLE_NAME = "product_category_translation_bronze"
SOURCE_FILE = "product_category_name_translation.csv"


def transform(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "product_category_name",
            F.lower(F.trim(F.col("product_category_name"))),
        )
        .withColumn(
            "product_category_name_english",
            F.lower(F.trim(F.col("product_category_name_english"))),
        )
    )


def main() -> None:
    args = parse_bronze_job_args(
        default_input_path=raw_olist_path(SOURCE_FILE),
        default_output_path=bronze_hudi_path(TABLE_NAME),
    )
    spark = build_spark_session("load_product_category_translation_bronze")
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
        record_key="product_category_name",
        precombine_field="_ingested_at",
    )
    spark.stop()


if __name__ == "__main__":
    main()
