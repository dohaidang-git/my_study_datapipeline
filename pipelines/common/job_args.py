"""Argument parsing shared by pipeline jobs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeJobArgs:
    input_path: str
    output_path: str
    output_format: str
    batch_id: str
    source_system: str
    mode: str


def build_bronze_job_parser(
    *,
    default_input_path: str,
    default_output_path: str,
    default_source_system: str = "olist",
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", default=default_input_path)
    parser.add_argument("--output-path", default=default_output_path)
    parser.add_argument(
        "--output-format",
        default="hudi",
        choices=("hudi", "parquet"),
        help="Use parquet when validating transforms before adding Hudi jars.",
    )
    parser.add_argument("--batch-id", default="manual")
    parser.add_argument("--source-system", default=default_source_system)
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=("overwrite", "append"),
        help="Writer mode for parquet output. Hudi always uses upsert semantics.",
    )
    return parser


def parse_bronze_job_args(
    *,
    default_input_path: str,
    default_output_path: str,
    default_source_system: str = "olist",
) -> BronzeJobArgs:
    parser = build_bronze_job_parser(
        default_input_path=default_input_path,
        default_output_path=default_output_path,
        default_source_system=default_source_system,
    )
    namespace = parser.parse_args()
    return BronzeJobArgs(
        input_path=namespace.input_path,
        output_path=namespace.output_path,
        output_format=namespace.output_format,
        batch_id=namespace.batch_id,
        source_system=namespace.source_system,
        mode=namespace.mode,
    )
