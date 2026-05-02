"""Read back bronze orders from Hudi for smoke testing."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.common.spark_session import build_spark_session


def main() -> None:
    spark = build_spark_session("verify_hudi_orders_read")
    df = spark.read.format("hudi").load("s3a://lakehouse/bronze/orders_bronze")
    print("row_count =", df.count())
    df.select("order_id", "order_status", "order_purchase_timestamp", "dt").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
