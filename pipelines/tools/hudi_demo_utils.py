"""Helpers for Hudi incremental upsert and time-travel demos."""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import DataFrame

from pipelines.common.spark_session import build_spark_session

HUDI_COMMIT_PATTERN = re.compile(r"^(\d+)\.(commit|deltacommit|replacecommit)$")
HUDI_DEMO_ROOT = "s3a://lakehouse/demo"
PAYMENTS_DEMO_TABLE = "payments_bronze_demo"
PAYMENTS_DEMO_PATH = f"{HUDI_DEMO_ROOT}/{PAYMENTS_DEMO_TABLE}"


def build_demo_spark_session(app_name: str):
    return build_spark_session(app_name)


def read_hudi_table(spark, table_path: str, *, as_of_instant: str | None = None) -> DataFrame:
    reader = spark.read.format("hudi")
    if as_of_instant:
        reader = reader.option("as.of.instant", as_of_instant)
    return reader.load(table_path)


def list_hudi_instants(spark, table_path: str) -> list[str]:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(f"{table_path}/.hoodie")
    fs = path.getFileSystem(hadoop_conf)
    if not fs.exists(path):
        return []

    instants: list[str] = []
    for status in fs.listStatus(path):
        name = status.getPath().getName()
        match = HUDI_COMMIT_PATTERN.match(name)
        if match:
            instants.append(match.group(1))
    if instants:
        return sorted(set(instants))

    snapshot_df = spark.read.format("hudi").load(table_path)
    snapshot_instants = [
        row["_hoodie_commit_time"]
        for row in snapshot_df.select("_hoodie_commit_time").distinct().collect()
        if row["_hoodie_commit_time"]
    ]
    return sorted(set(snapshot_instants))


def latest_hudi_instant(spark, table_path: str) -> str | None:
    instants = list_hudi_instants(spark, table_path)
    if not instants:
        return None
    return instants[-1]


def print_table_rows(df: DataFrame, *, title: str, limit: int = 20) -> None:
    print(title)
    df.show(limit, truncate=False)


def ensure_report_dir() -> Path:
    report_dir = Path(__file__).resolve().parents[2] / "reports" / "hudi_demo"
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir
