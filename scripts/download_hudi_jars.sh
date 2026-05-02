#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_DIR="${PROJECT_ROOT}/jars"

HUDI_VERSION="${HUDI_VERSION:-1.1.1}"
SPARK_MINOR_VERSION="${SPARK_MINOR_VERSION:-3.5}"
SCALA_BINARY_VERSION="${SCALA_BINARY_VERSION:-2.12}"
HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION:-3.3.4}"
AWS_BUNDLE_VERSION="${AWS_BUNDLE_VERSION:-1.12.262}"

HUDI_ARTIFACT="hudi-spark${SPARK_MINOR_VERSION}-bundle_${SCALA_BINARY_VERSION}"

mkdir -p "${TARGET_DIR}"

download_jar() {
  local url="$1"
  local output_file="$2"

  if [[ -f "${output_file}" ]]; then
    echo "Skip existing: ${output_file}"
    return 0
  fi

  echo "Downloading $(basename "${output_file}")"
  curl --fail --location --retry 3 --retry-delay 2 --output "${output_file}" "${url}"
}

HUDI_JAR="hudi-spark${SPARK_MINOR_VERSION}-bundle_${SCALA_BINARY_VERSION}-${HUDI_VERSION}.jar"
HADOOP_AWS_JAR="hadoop-aws-${HADOOP_AWS_VERSION}.jar"
AWS_BUNDLE_JAR="aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar"

download_jar \
  "https://repo.maven.apache.org/maven2/org/apache/hudi/${HUDI_ARTIFACT}/${HUDI_VERSION}/${HUDI_JAR}" \
  "${TARGET_DIR}/${HUDI_JAR}"

download_jar \
  "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/${HADOOP_AWS_JAR}" \
  "${TARGET_DIR}/${HADOOP_AWS_JAR}"

download_jar \
  "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE_VERSION}/${AWS_BUNDLE_JAR}" \
  "${TARGET_DIR}/${AWS_BUNDLE_JAR}"

echo
echo "Downloaded jars:"
find "${TARGET_DIR}" -maxdepth 1 -type f -name "*.jar" | sort
