"""Minimal Hudi writer wrapper for bronze and later layers."""

from __future__ import annotations

from pyspark.sql import DataFrame


def build_hudi_options(
    *,
    table_name: str,
    record_key: str,
    precombine_field: str,
    partition_field: str | None = None,
) -> dict[str, str]:
    options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.precombine.field": precombine_field,
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.reconcile.schema": "true",
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
        "hoodie.clean.automatic": "true",
    }
    if partition_field:
        options["hoodie.datasource.write.partitionpath.field"] = partition_field
        options["hoodie.datasource.hive_sync.partition_fields"] = partition_field
    else:
        options["hoodie.datasource.write.keygenerator.class"] = (
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
        )
    return options


def write_hudi_table(
    df: DataFrame,
    *,
    table_name: str,
    output_path: str,
    mode: str,
    record_key: str,
    precombine_field: str,
    partition_field: str | None = None,
) -> None:
    options = build_hudi_options(
        table_name=table_name,
        record_key=record_key,
        precombine_field=precombine_field,
        partition_field=partition_field,
    )
    df.write.format("hudi").options(**options).mode(mode).save(output_path)
