"""Spark session bootstrap helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark_session(app_name: str, extra_conf: dict[str, str] | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)
    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
