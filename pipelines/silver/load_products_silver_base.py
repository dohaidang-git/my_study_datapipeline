"""Build products_silver_base from products_bronze."""

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

TABLE_NAME = "products_silver_base"
INPUT_TABLE = "products_bronze"
OUTPUT_PATH = "data/silver/products_silver_base"


def transform(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["product_id"])
        .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
        .fillna(
            {
                "product_name_lenght": 0,
                "product_description_lenght": 0,
                "product_photos_qty": 0,
                "product_weight_g": 0,
                "product_length_cm": 0,
                "product_height_cm": 0,
                "product_width_cm": 0,
            }
        )
    )


def main() -> None:
    args = parse_silver_job_args(
        default_input_path=bronze_local_path(INPUT_TABLE),
        default_output_path=OUTPUT_PATH,
    )
    spark = build_spark_session("load_products_silver_base")
    df = read_parquet_source(spark, args.input_path)
    silver_df = transform(df)
    write_silver_output(
        silver_df,
        table_name=TABLE_NAME,
        output_path=args.output_path,
        output_format=args.output_format,
        mode=args.mode,
        record_key="product_id",
        precombine_field="_ingested_at",
    )
    spark.stop()


if __name__ == "__main__":
    main()
