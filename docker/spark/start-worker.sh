#!/usr/bin/env bash
set -euo pipefail

exec /opt/spark/bin/spark-class \
  org.apache.spark.deploy.worker.Worker \
  --webui-port 8081 \
  spark://spark-master:7077
