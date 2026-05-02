"""Build reviews_silver from reviews_bronze."""

from __future__ import annotations

from pathlib import Path
import sys

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.common.paths import bronze_hudi_path, silver_hudi_path
from pipelines.common.silver_job import (
    build_spark_session,
    parse_silver_job_args,
    read_table_source,
    write_silver_output,
)

TABLE_NAME = "reviews_silver"
INPUT_TABLE = "reviews_bronze"
OUTPUT_PATH = silver_hudi_path(TABLE_NAME)


def transform(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["review_id"])
        .withColumn("review_score", F.col("review_score").cast("int"))
        .withColumn("review_comment_title", F.trim(F.col("review_comment_title")))
        .withColumn("review_comment_message", F.trim(F.col("review_comment_message")))
    )


def main() -> None:
    args = parse_silver_job_args(
        default_input_path=bronze_hudi_path(INPUT_TABLE),
        default_output_path=OUTPUT_PATH,
    )
    spark = build_spark_session("load_reviews_silver")
    df = read_table_source(spark, args.input_path, args.input_format)
    silver_df = transform(df)
    write_silver_output(
        silver_df,
        table_name=TABLE_NAME,
        output_path=args.output_path,
        output_format=args.output_format,
        mode=args.mode,
        record_key="review_id",
        precombine_field="review_answer_timestamp",
    )
    spark.stop()


if __name__ == "__main__":
    main()
