"""Spark session bootstrap helpers."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

PREFERRED_JAR_NAMES = [
    "hudi-spark3.5-bundle_2.12-1.1.1.jar",
    "hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar",
]


def discover_local_jars() -> list[str]:
    project_root = Path(__file__).resolve().parents[2]
    candidate_dirs = [
        project_root / "jars",
        Path("/opt/spark/work-dir/jars"),
    ]
    discovered: list[str] = []
    seen: set[str] = set()

    for directory in candidate_dirs:
        if not directory.exists():
            continue
        preferred_matches = [directory / jar_name for jar_name in PREFERRED_JAR_NAMES]
        matched_preferred = False

        for jar_path in preferred_matches:
            if not jar_path.exists():
                continue
            jar_str = str(jar_path.resolve())
            if jar_str not in seen:
                seen.add(jar_str)
                discovered.append(jar_str)
                matched_preferred = True

        if matched_preferred:
            continue

        for jar_path in sorted(directory.glob("*.jar")):
            jar_str = str(jar_path.resolve())
            if jar_str not in seen:
                seen.add(jar_str)
                discovered.append(jar_str)

    return discovered


def build_spark_session(
    app_name: str,
    extra_conf: dict[str, str] | None = None,
    enable_hive_support: bool = False,
) -> SparkSession:
    extra_conf = dict(extra_conf or {})
    discovered_jars = discover_local_jars()

    if any("hudi" in Path(jar).name.lower() for jar in discovered_jars):
        extra_conf.setdefault("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)
    if enable_hive_support:
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
