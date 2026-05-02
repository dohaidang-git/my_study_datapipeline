"""Build products_silver by enriching products_silver_base with translations."""

from __future__ import annotations

from pathlib import Path
import sys

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.common.paths import silver_hudi_path
from pipelines.common.silver_job import build_spark_session, read_table_source, write_silver_output

TABLE_NAME = "products_silver"
BASE_INPUT_PATH = silver_hudi_path("products_silver_base")
TRANSLATION_INPUT_PATH = silver_hudi_path("product_category_translation_silver")
OUTPUT_PATH = silver_hudi_path(TABLE_NAME)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-input-path", default=BASE_INPUT_PATH)
    parser.add_argument("--translation-input-path", default=TRANSLATION_INPUT_PATH)
    parser.add_argument("--input-format", default="hudi", choices=("parquet", "hudi"))
    parser.add_argument("--output-path", default=OUTPUT_PATH)
    parser.add_argument("--output-format", default="hudi", choices=("parquet", "hudi"))
    parser.add_argument("--mode", default="overwrite", choices=("overwrite", "append"))
    return parser.parse_args()


def transform(products_df: DataFrame, translation_df: DataFrame) -> DataFrame:
    translation_df = translation_df.select(
        "product_category_name",
        "product_category_name_english",
    )
    return (
        products_df.alias("p")
        .join(
            translation_df.alias("t"),
            on="product_category_name",
            how="left",
        )
        .withColumn(
            "product_category_name_english",
            F.coalesce(
                F.col("product_category_name_english"),
                F.col("product_category_name"),
            ),
        )
    )


def main() -> None:
    args = parse_args()
    spark = build_spark_session("load_products_silver")
    products_df = read_table_source(spark, args.base_input_path, args.input_format)
    translation_df = read_table_source(spark, args.translation_input_path, args.input_format)
    silver_df = transform(products_df, translation_df)
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
