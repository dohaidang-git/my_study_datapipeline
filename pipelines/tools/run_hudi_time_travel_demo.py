"""Read the payments demo table using Hudi time travel."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.tools.hudi_demo_utils import (
    PAYMENTS_DEMO_PATH,
    build_demo_spark_session,
    ensure_report_dir,
    list_hudi_instants,
    print_table_rows,
    read_hudi_table,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instant", help="Specific Hudi commit instant to read")
    parser.add_argument(
        "--use-previous",
        action="store_true",
        help="Read the snapshot at the previous instant in the demo timeline",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_demo_spark_session("run_hudi_time_travel_demo")
    instants = list_hudi_instants(spark, PAYMENTS_DEMO_PATH)
    print(f"Available instants: {instants}")
    if not instants:
        raise SystemExit(f"No instants found for demo table at {PAYMENTS_DEMO_PATH}")

    selected_instant = args.instant
    if args.use_previous:
        if len(instants) < 2:
            raise SystemExit("Need at least two instants to use --use-previous")
        selected_instant = instants[-2]
    elif selected_instant is None:
        selected_instant = instants[-1]

    df = read_hudi_table(spark, PAYMENTS_DEMO_PATH, as_of_instant=selected_instant).orderBy("payment_key")
    print(f"Reading demo table as of instant: {selected_instant}")
    print_table_rows(df, title="Time travel snapshot")

    updated_key = "a9810da82917af2d9aefd1278f1dcfa0_1"
    inserted_key = "demo_order_001_1"
    updated_row = (
        df.filter(df.payment_key == updated_key)
        .select("payment_installments", "payment_value", "_hoodie_commit_time")
        .first()
    )
    inserted_exists = df.filter(df.payment_key == inserted_key).count() > 0

    report_dir = ensure_report_dir()
    report_path = report_dir / f"time_travel_summary_{selected_instant}.md"
    report_content = "\n".join(
        [
            "# Time Travel Summary",
            "",
            f"- Demo table path: `{PAYMENTS_DEMO_PATH}`",
            f"- Selected instant: `{selected_instant}`",
            f"- Available instants: `{', '.join(instants)}`",
            f"- Updated key: `{updated_key}`",
            f"- Updated key payment_installments: `{updated_row['payment_installments']}`",
            f"- Updated key payment_value: `{updated_row['payment_value']}`",
            f"- Inserted key visible at this instant: `{inserted_exists}`",
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
