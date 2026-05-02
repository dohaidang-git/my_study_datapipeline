"""Metadata helpers applied across pipeline layers."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(
    df: DataFrame,
    *,
    batch_id: str,
    source_file: str,
    source_system: str,
) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_system", F.lit(source_system))
    )
