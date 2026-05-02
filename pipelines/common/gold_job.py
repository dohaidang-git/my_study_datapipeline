"""Shared gold-layer utilities."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame

from pipelines.common.hudi_writer import write_hudi_table
from pipelines.common.spark_session import build_spark_session


@dataclass(frozen=True)
class GoldJobArgs:
    input_format: str
    output_path: str
    output_format: str
    mode: str


def parse_gold_job_args(*, default_output_path: str) -> GoldJobArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-format", default="hudi", choices=("parquet", "hudi"))
    parser.add_argument("--output-path", default=default_output_path)
    parser.add_argument("--output-format", default="hudi", choices=("parquet", "hudi"))
    parser.add_argument("--mode", default="overwrite", choices=("overwrite", "append"))
    args = parser.parse_args()
    return GoldJobArgs(
        input_format=args.input_format,
        output_path=args.output_path,
        output_format=args.output_format,
        mode=args.mode,
    )


def read_parquet_source(spark, input_path: str) -> DataFrame:
    return spark.read.parquet(input_path)


def read_hudi_source(spark, input_path: str) -> DataFrame:
    return spark.read.format("hudi").load(input_path)


def read_table_source(spark, input_path: str, input_format: str) -> DataFrame:
    if input_format == "hudi":
        return read_hudi_source(spark, input_path)
    return read_parquet_source(spark, input_path)


def write_gold_output(
    df: DataFrame,
    *,
    table_name: str,
    output_path: str,
    output_format: str,
    mode: str,
    record_key: str,
    precombine_field: str,
    partition_field: str | None = None,
) -> None:
    if output_format == "parquet":
        writer = df.write.mode(mode)
        if partition_field:
            writer = writer.partitionBy(partition_field)
        writer.parquet(output_path)
        return

    write_hudi_table(
        df,
        table_name=table_name,
        output_path=output_path,
        mode=mode,
        record_key=record_key,
        precombine_field=precombine_field,
        partition_field=partition_field,
    )


__all__ = [
    "GoldJobArgs",
    "build_spark_session",
    "parse_gold_job_args",
    "read_hudi_source",
    "read_parquet_source",
    "read_table_source",
    "write_gold_output",
]
