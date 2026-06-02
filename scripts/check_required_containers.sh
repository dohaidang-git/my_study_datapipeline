#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/docker_compat.sh"

if [[ $# -eq 0 ]]; then
  echo "Usage: bash scripts/check_required_containers.sh <container> [container ...]"
  exit 1
fi

missing=()
for container_name in "$@"; do
  if ! docker inspect -f '{{.State.Running}}' "${container_name}" 2>/dev/null | grep -qx 'true'; then
    missing+=("${container_name}")
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "These containers are missing or not running: ${missing[*]}"
  echo "Start the stack before triggering the DAG."
  exit 1
fi

echo "All required containers are running: $*"
