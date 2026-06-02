#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker_compat.sh"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SQL_FILE="${PROJECT_ROOT}/sql/queries/bi_demo_queries.sql"

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "SQL file not found: ${SQL_FILE}"
  exit 1
fi

docker exec -i trino trino \
  --catalog hive \
  --schema analytics \
  --output-format MARKDOWN \
  < "${SQL_FILE}"
