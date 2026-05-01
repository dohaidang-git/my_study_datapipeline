#!/usr/bin/env bash
set -euo pipefail

exec /opt/spark/bin/spark-class \
  org.apache.spark.deploy.master.Master \
  --host spark-master \
  --port 7077 \
  --webui-port 8080
