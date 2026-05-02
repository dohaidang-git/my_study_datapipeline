"""Shared path helpers for project storage layout."""

from __future__ import annotations

import os


RAW_OLIST_DIR = "data/raw/olist"
BRONZE_BASE_DIR = "data/bronze"
LAKEHOUSE_BRONZE_ROOT = os.environ.get("LAKEHOUSE_BRONZE_ROOT", "s3a://lakehouse/bronze")


def raw_olist_path(filename: str) -> str:
    return f"{RAW_OLIST_DIR}/{filename}"


def bronze_local_path(table_name: str) -> str:
    return f"{BRONZE_BASE_DIR}/{table_name}"


def bronze_hudi_path(table_name: str) -> str:
    return f"{LAKEHOUSE_BRONZE_ROOT}/{table_name}"
