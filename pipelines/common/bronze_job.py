"""Shared bronze-layer utilities."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.common.hudi_writer import write_hudi_table
from pipelines.common.metadata import add_ingestion_metadata


def read_csv_source(
    spark,
    input_path: str,
    options: Mapping[str, str] | None = None,
) -> DataFrame:
    reader = (
        spark.read.option("header", "true")
        .option("multiLine", "false")
        .option("escape", '"')
        .option("quote", '"')
    )
    if options:
        for key, value in options.items():
            reader = reader.option(key, value)
    return reader.csv(input_path)


def finalize_bronze_df(
    df: DataFrame,
    *,
    batch_id: str,
    source_file: str,
    source_system: str,
) -> DataFrame:
    return add_ingestion_metadata(
        df,
        batch_id=batch_id,
        source_file=source_file,
        source_system=source_system,
    )


def write_bronze_output(
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
        record_key=record_key,
        precombine_field=precombine_field,
        partition_field=partition_field,
    )


def add_date_partition_column(df: DataFrame, timestamp_col: str, partition_col: str = "dt") -> DataFrame:
    return df.withColumn(partition_col, F.to_date(F.col(timestamp_col)))
