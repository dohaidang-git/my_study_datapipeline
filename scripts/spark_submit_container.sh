#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: bash scripts/spark_submit_container.sh <job.py> [job args...]"
  exit 1
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOB_PATH="$1"
shift

CONTAINER_WORKDIR="/opt/spark/work-dir"
CONTAINER_JAR_DIR="${CONTAINER_WORKDIR}/jars"
PREFERRED_JAR_NAMES=(
  "hudi-spark3.5-bundle_2.12-1.1.1.jar"
  "hadoop-aws-3.3.4.jar"
  "aws-java-sdk-bundle-1.12.262.jar"
)

host_jars=()
for jar_name in "${PREFERRED_JAR_NAMES[@]}"; do
  jar_path="${PROJECT_ROOT}/jars/${jar_name}"
  if [[ -f "${jar_path}" ]]; then
    host_jars+=("${jar_path}")
  fi
done

if [[ ${#host_jars[@]} -eq 0 ]]; then
  mapfile -t host_jars < <(find "${PROJECT_ROOT}/jars" -maxdepth 1 -type f -name '*.jar' | sort)
fi

submit_args=(
  /opt/spark/bin/spark-submit
  --master
  local[2]
)

if [[ ${#host_jars[@]} -gt 0 ]]; then
  container_jars=()
  for host_jar in "${host_jars[@]}"; do
    container_jars+=("${CONTAINER_JAR_DIR}/$(basename "${host_jar}")")
  done

  jar_csv="$(IFS=,; echo "${container_jars[*]}")"
  jar_classpath="$(IFS=:; echo "${container_jars[*]}")"

  submit_args+=(
    --jars
    "${jar_csv}"
    --conf
    "spark.driver.extraClassPath=${jar_classpath}"
    --conf
    "spark.executor.extraClassPath=${jar_classpath}"
  )

  if printf '%s\n' "${container_jars[@]}" | grep -qi 'hudi'; then
    submit_args+=(
      --conf
      "spark.serializer=org.apache.spark.serializer.KryoSerializer"
    )
  fi
fi

docker exec \
  -w "${CONTAINER_WORKDIR}" \
  spark-master \
  "${submit_args[@]}" \
  "${JOB_PATH}" \
  "$@"
