"""Prepare a small Hudi demo table from payments_bronze."""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common.hudi_writer import write_hudi_table
from pipelines.common.paths import bronze_hudi_path
from pipelines.tools.hudi_demo_utils import (
    PAYMENTS_DEMO_PATH,
    PAYMENTS_DEMO_TABLE,
    build_demo_spark_session,
    latest_hudi_instant,
    print_table_rows,
    read_hudi_table,
)

DEMO_PAYMENT_KEYS = [
    "b81ef226f3fe1789b1e8b2acac839d17_1",
    "a9810da82917af2d9aefd1278f1dcfa0_1",
    "25e8ea4e93396b6fa0d3dd708e76c1bd_1",
]


def main() -> None:
    spark = build_demo_spark_session("prepare_hudi_payments_demo")
    source_df = read_hudi_table(spark, bronze_hudi_path("payments_bronze"))
    demo_df = source_df.filter(F.col("payment_key").isin(DEMO_PAYMENT_KEYS))

    write_hudi_table(
        demo_df,
        table_name=PAYMENTS_DEMO_TABLE,
        output_path=PAYMENTS_DEMO_PATH,
        mode="overwrite",
        record_key="payment_key",
        precombine_field="_ingested_at",
    )

    prepared_df = read_hudi_table(spark, PAYMENTS_DEMO_PATH).orderBy("payment_key")
    latest_instant = latest_hudi_instant(spark, PAYMENTS_DEMO_PATH)
    print(f"Prepared demo table at {PAYMENTS_DEMO_PATH}")
    print(f"Current instant: {latest_instant}")
    print_table_rows(prepared_df, title="Initial demo snapshot")
    spark.stop()


if __name__ == "__main__":
    main()
