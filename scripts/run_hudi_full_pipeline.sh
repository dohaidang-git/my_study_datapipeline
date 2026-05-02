#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

bash "${PROJECT_ROOT}/scripts/run_hudi_bronze.sh"
bash "${PROJECT_ROOT}/scripts/run_hudi_silver.sh"
bash "${PROJECT_ROOT}/scripts/run_hudi_gold.sh"
