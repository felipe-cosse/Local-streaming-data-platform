#!/usr/bin/env bash
set -euo pipefail

readonly MIN_HOST_KB="${MIN_HOST_KB:-15728640}"
readonly MIN_DOCKER_KB="${MIN_DOCKER_KB:-8388608}"
readonly WORKSPACE="${WORKSPACE:-$(pwd)}"

available_kb() {
  df -Pk "$1" | awk 'NR == 2 {print $4}'
}

format_gib() {
  awk -v kb="$1" 'BEGIN {printf "%.1f", kb / 1024 / 1024}'
}

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker is not running or is not accessible." >&2
  exit 1
fi

host_available_kb="$(available_kb "$WORKSPACE")"
if (( host_available_kb < MIN_HOST_KB )); then
  echo "ERROR: only $(format_gib "$host_available_kb") GiB is free on the host; at least $(format_gib "$MIN_HOST_KB") GiB is required." >&2
  exit 1
fi

if docker image inspect apache/kafka:4.1.2 >/dev/null 2>&1; then
  if ! docker_available_kb="$(
    docker run --rm --pull never --entrypoint /bin/sh apache/kafka:4.1.2 \
      -c "df -Pk / | awk 'NR == 2 {print \$4}'" 2>/dev/null
  )"; then
    echo "ERROR: Docker could not create a preflight container. Its storage allocation may be full." >&2
    exit 1
  fi
  if [[ ! "$docker_available_kb" =~ ^[0-9]+$ ]]; then
    echo "ERROR: could not determine free space inside Docker's storage filesystem." >&2
    exit 1
  fi
  if (( docker_available_kb < MIN_DOCKER_KB )); then
    echo "ERROR: only $(format_gib "$docker_available_kb") GiB is free inside Docker; at least $(format_gib "$MIN_DOCKER_KB") GiB is required for core initialization." >&2
    echo "Increase Docker Desktop's disk allocation or remove unused Docker data, then run 'make clean' and retry." >&2
    exit 1
  fi
else
  echo "INFO: Kafka is not downloaded yet; Docker-internal disk capacity will be checked after the first pull."
fi

echo "Preflight passed: host=$(format_gib "$host_available_kb") GiB free."
