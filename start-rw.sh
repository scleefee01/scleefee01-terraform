#!/bin/bash
set -euo pipefail

###############################################################################
# start-rw.sh — Submit a NEXMark query to RisingWave
#
# Usage: ./start-rw.sh <query>   (e.g. ./start-rw.sh q0)
#
# Creates Kafka sources (combined topic with watermarks) and a sink for the
# specified query. Uses BACKGROUND_DDL so CREATE SINK returns immediately.
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERY="${1:?Usage: $0 <query> (e.g. q0, q1, q2, ...)}"

# Load env vars from TOML
set -a
source <("${SCRIPT_DIR}/tomlenv/bin/tomlenv" "${SCRIPT_DIR}/toml/env.toml")
export TRICK_SYMBOLS_EMPTY=""
set +a

RW_NS="${BENCHMARK_RISINGWAVE_NAMESPACE}"
RW_SVC="${BENCHMARK_RISINGWAVE_NAME}-frontend"
LOCAL_RW_PORT=14567

echo "=== Starting RisingWave Nexmark query: ${QUERY} (ns: ${RW_NS}) ==="

###############################################################################
# Helper: cleanup background port-forwards on exit
###############################################################################
PF_PIDS=()
cleanup() {
  for pid in "${PF_PIDS[@]}"; do
    kill "${pid}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

###############################################################################
# Helper: wait for a local port to be ready
###############################################################################
wait_port() {
  local port=$1
  for _ in $(seq 1 30); do
    if (echo > /dev/tcp/localhost/${port}) 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: port ${port} not ready" >&2
  return 1
}

###############################################################################
# Check psql is available
###############################################################################
if ! command -v psql &>/dev/null; then
  echo "ERROR: psql not found. Install with: sudo yum install -y postgresql15" >&2
  exit 1
fi

###############################################################################
# 1. Apply ConfigMaps (idempotent)
###############################################################################
echo "=== Applying ConfigMaps ==="
envsubst < "${SCRIPT_DIR}/manifests/risingwave-nexmark/nexmark-kafka-sources.template.yaml" | kubectl apply -f -
envsubst < "${SCRIPT_DIR}/manifests/risingwave-nexmark/nexmark-sinks.template.yaml" | kubectl apply -f -

###############################################################################
# 2. Port-forward to RisingWave frontend
###############################################################################
echo "=== Setting up port-forward to RisingWave ==="

# Kill any existing port-forward on our port
pid=$(lsof -ti :"${LOCAL_RW_PORT}" 2>/dev/null || true)
if [[ -n "${pid}" ]]; then
  echo "  Killing existing process on port ${LOCAL_RW_PORT} (pid=${pid})"
  kill ${pid} 2>/dev/null || true
  sleep 1
fi

kubectl port-forward -n "${RW_NS}" "svc/${RW_SVC}" "${LOCAL_RW_PORT}:4567" &
PF_PIDS+=($!)
wait_port "${LOCAL_RW_PORT}"
echo "  Port-forward ready."

###############################################################################
# 3. Cancel any in-progress DDL jobs and drop existing objects
###############################################################################
echo "=== Cancelling any in-progress DDL ==="

# Get creating DDL job IDs and cancel them
DDL_IDS=$(psql -h localhost -p "${LOCAL_RW_PORT}" -U root -d dev -t -A \
  -c "SELECT ddl_id FROM rw_catalog.rw_ddl_progress;" 2>/dev/null || true)

if [[ -n "${DDL_IDS}" ]]; then
  for ddl_id in ${DDL_IDS}; do
    echo "  Cancelling DDL job: ${ddl_id}"
    psql -h localhost -p "${LOCAL_RW_PORT}" -U root -d dev \
      -c "CANCEL JOBS ${ddl_id};" 2>/dev/null || true
  done
  sleep 3
fi

echo "=== Dropping existing objects (if any) ==="

# Drop sinks first, then source with CASCADE
psql -h localhost -p "${LOCAL_RW_PORT}" -U root -d dev \
  -c "DROP SINK IF EXISTS nexmark_${QUERY};" 2>/dev/null || true

psql -h localhost -p "${LOCAL_RW_PORT}" -U root -d dev \
  -c "DROP SOURCE IF EXISTS nexmark CASCADE;" 2>/dev/null || true

echo "  Cleanup done."

###############################################################################
# 4. Create Kafka sources (combined topic with watermarks)
###############################################################################
echo "=== Creating Kafka sources ==="

SOURCES_SQL=$(kubectl get configmap "rw-nexmark-kafka-sources-${BENCHMARK_JOB_NAME}" \
  -n "${RW_NS}" -o go-template='{{index .data "create_combined_watermark.sql"}}')

echo "${SOURCES_SQL}" | psql -h localhost -p "${LOCAL_RW_PORT}" -U root -d dev

echo "  Sources created."

###############################################################################
# 5. Create sink with BACKGROUND_DDL (returns immediately)
###############################################################################
echo "=== Creating sink for ${QUERY} (background DDL) ==="

QUERY_SQL=$(kubectl get configmap "rw-nexmark-sinks-${BENCHMARK_JOB_NAME}" \
  -n "${RW_NS}" -o go-template='{{index .data "'"${QUERY}.sql"'"}}')

# Record start time BEFORE creating sink
date +%s%3N > /tmp/rw-bench-start-time

# Use BACKGROUND_DDL so CREATE SINK returns immediately instead of blocking
psql -h localhost -p "${LOCAL_RW_PORT}" -U root -d dev <<EOSQL
SET BACKGROUND_DDL = true;
${QUERY_SQL}
EOSQL

echo "=== Sink submitted ==="
echo "  Query:   ${QUERY}"
echo "  Sources: nexmark (combined) → bid, auction, person views"
echo "  Sink:    nexmark_${QUERY}"
echo "  Mode:    BACKGROUND_DDL (async)"
echo ""
echo "  Monitor: psql -h localhost -p ${LOCAL_RW_PORT} -U root -d dev"
echo "           SELECT * FROM rw_catalog.rw_ddl_progress;"
