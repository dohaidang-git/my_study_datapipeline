#!/usr/bin/env bash

set -euo pipefail

bash scripts/spark_submit_container.sh pipelines/tools/prepare_hudi_payments_demo.py
bash scripts/spark_submit_container.sh pipelines/tools/run_hudi_incremental_upsert_demo.py
bash scripts/spark_submit_container.sh pipelines/tools/run_hudi_time_travel_demo.py --use-previous
bash scripts/spark_submit_container.sh pipelines/tools/run_hudi_time_travel_demo.py
