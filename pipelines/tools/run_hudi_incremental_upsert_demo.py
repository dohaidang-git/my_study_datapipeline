"""Apply an incremental Hudi upsert to the payments demo table."""

from __future__ import annotations

import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common.hudi_writer import write_hudi_table
from pipelines.tools.hudi_demo_utils import (
    PAYMENTS_DEMO_PATH,
    PAYMENTS_DEMO_TABLE,
    build_demo_spark_session,
    ensure_report_dir,
    latest_hudi_instant,
    print_table_rows,
    read_hudi_table,
)


def build_patch_df(spark, reference_schema):
    payload_rows = [
        {
            "order_id": "a9810da82917af2d9aefd1278f1dcfa0",
            "payment_sequential": 1,
            "payment_type": "credit_card",
            "payment_installments": 6,
            "payment_value": Decimal("30.00"),
            "payment_key": "a9810da82917af2d9aefd1278f1dcfa0_1",
            "_ingested_at": datetime(2026, 5, 3, 10, 0, 0),
            "_batch_id": "demo_upsert_001",
            "_source_file": "synthetic_demo_patch",
            "_source_system": "demo",
        },
        {
            "order_id": "demo_order_001",
            "payment_sequential": 1,
            "payment_type": "voucher",
            "payment_installments": 1,
            "payment_value": Decimal("49.90"),
            "payment_key": "demo_order_001_1",
            "_ingested_at": datetime(2026, 5, 3, 10, 0, 0),
            "_batch_id": "demo_upsert_001",
            "_source_file": "synthetic_demo_patch",
            "_source_system": "demo",
        },
    ]
    ordered_rows = [
        Row(*(payload[field.name] for field in reference_schema))
        for payload in payload_rows
    ]
    return (
        spark.createDataFrame(ordered_rows, schema=reference_schema)
        .withColumn("_ingested_at", F.current_timestamp())
    )


def main() -> None:
    spark = build_demo_spark_session("run_hudi_incremental_upsert_demo")
    before_instant = latest_hudi_instant(spark, PAYMENTS_DEMO_PATH)
    before_df = read_hudi_table(spark, PAYMENTS_DEMO_PATH).orderBy("payment_key")
    business_schema = before_df.drop(
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
    ).schema
    print(f"Before upsert instant: {before_instant}")
    print_table_rows(before_df, title="Snapshot before upsert")

    patch_df = build_patch_df(spark, business_schema)
    print_table_rows(patch_df.orderBy("payment_key"), title="Patch rows to upsert")

    write_hudi_table(
        patch_df,
        table_name=PAYMENTS_DEMO_TABLE,
        output_path=PAYMENTS_DEMO_PATH,
        mode="append",
        record_key="payment_key",
        precombine_field="_ingested_at",
    )

    after_instant = latest_hudi_instant(spark, PAYMENTS_DEMO_PATH)
    after_df = read_hudi_table(spark, PAYMENTS_DEMO_PATH).orderBy("payment_key")
    print(f"After upsert instant: {after_instant}")
    print_table_rows(after_df, title="Snapshot after upsert")

    report_dir = ensure_report_dir()
    report_path = report_dir / "incremental_upsert_summary.md"
    report_content = "\n".join(
        [
            "# Incremental Upsert Summary",
            "",
            f"- Demo table path: `{PAYMENTS_DEMO_PATH}`",
            f"- Before instant: `{before_instant}`",
            f"- After instant: `{after_instant}`",
            "- Updated key: `a9810da82917af2d9aefd1278f1dcfa0_1`",
            "- Inserted key: `demo_order_001_1`",
        ]
    )
    try:
        report_path.write_text(report_content, encoding="utf-8")
        print(f"Wrote summary report to {report_path}")
    except PermissionError:
        print(f"WARNING: Could not write summary report to {report_path} due to filesystem permissions.")
        print(report_content)
    spark.stop()


if __name__ == "__main__":
    main()
