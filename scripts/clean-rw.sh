#!/bin/bash
set -euo pipefail

###############################################################################
# clean-rw.sh — Clean up RisingWave NEXMark benchmark objects
#
# Usage: ./clean-rw.sh              (drop sources + sinks)
#        ./clean-rw.sh --keep-topic (same, does not affect Kafka topic)
#
# Drops all NEXMark sinks, views, and sources from RisingWave.
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load env vars from TOML
set -a
source <("${PROJECT_DIR}/tomlenv/bin/tomlenv" "${PROJECT_DIR}/toml/env.toml")
set +a

RW_NS="${BENCHMARK_RISINGWAVE_NAMESPACE}"
RW_SVC="${BENCHMARK_RISINGWAVE_NAME}-frontend"
LOCAL_RW_PORT=14567

# Parse flags
KEEP_TOPIC=false
for arg in "$@"; do
  case "${arg}" in
    --keep-topic) KEEP_TOPIC=true ;;
  esac
done

echo "=== Cleaning RisingWave NEXMark objects (ns: ${RW_NS}) ==="

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
# Check psql
###############################################################################
if ! command -v psql &>/dev/null; then
  echo "WARNING: psql not found. Skipping RisingWave cleanup." >&2
  exit 0
fi

###############################################################################
# Port-forward to RisingWave
###############################################################################
pid=$(lsof -ti :"${LOCAL_RW_PORT}" 2>/dev/null || true)
if [[ -n "${pid}" ]]; then
  kill ${pid} 2>/dev/null || true
  sleep 1
fi

kubectl port-forward -n "${RW_NS}" "svc/${RW_SVC}" "${LOCAL_RW_PORT}:4567" &
PF_PIDS+=($!)

# Wait for port
for _ in $(seq 1 15); do
  if (echo > /dev/tcp/localhost/${LOCAL_RW_PORT}) 2>/dev/null; then
    break
  fi
  sleep 1
done

###############################################################################
# Cancel any in-progress DDL jobs first
###############################################################################
echo "=== Cancelling any in-progress DDL ==="
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

###############################################################################
# Drop all known NEXMark sinks
###############################################################################
echo "=== Dropping NEXMark sinks ==="

SINKS=(nexmark_q0 nexmark_q1 nexmark_q2 nexmark_q3 nexmark_q4 nexmark_q5
       nexmark_q7 nexmark_q8 nexmark_q9 nexmark_q10 nexmark_q14 nexmark_q15
       nexmark_q16 nexmark_q17 nexmark_q18 nexmark_q20 nexmark_q21 nexmark_q22)

for sink in "${SINKS[@]}"; do
  psql -h localhost -p "${LOCAL_RW_PORT}" -U root -d dev \
    -c "DROP SINK IF EXISTS ${sink};" 2>/dev/null || true
done
echo "  Sinks dropped."

###############################################################################
# Drop views and sources (CASCADE)
###############################################################################
echo "=== Dropping sources and views ==="

psql -h localhost -p "${LOCAL_RW_PORT}" -U root -d dev <<'EOSQL' 2>/dev/null || true
DROP SOURCE IF EXISTS nexmark CASCADE;
DROP SOURCE IF EXISTS person CASCADE;
DROP SOURCE IF EXISTS auction CASCADE;
DROP SOURCE IF EXISTS bid CASCADE;
EOSQL

echo "  Sources dropped."

###############################################################################
# Delete stale RisingWave Kafka consumer groups
###############################################################################
echo "=== Cleaning stale Kafka consumer groups ==="
RW_GROUPS=$(kubectl exec -n "${BENCHMARK_KAFKA_NAMESPACE}" "${BENCHMARK_KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
  | grep -E '^rw-consumer-' || true)

if [[ -n "${RW_GROUPS}" ]]; then
  while IFS= read -r group; do
    echo "  Deleting consumer group: ${group}"
    kubectl exec -n "${BENCHMARK_KAFKA_NAMESPACE}" "${BENCHMARK_KAFKA_NAME}-controller-0" -- \
      /opt/bitnami/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
      --delete --group "${group}" 2>/dev/null || true
  done <<< "${RW_GROUPS}"
else
  echo "  No stale rw-consumer groups found."
fi

###############################################################################
# Optionally delete Kafka topic
###############################################################################
if [[ "${KEEP_TOPIC}" == "true" ]]; then
  echo "=== Skipping Kafka topic deletion (--keep-topic) ==="
else
  echo "=== Deleting Kafka topic: nexmark-events ==="
  kubectl exec -n "${BENCHMARK_KAFKA_NAMESPACE}" "${BENCHMARK_KAFKA_NAME}-controller-0" -- \
    /opt/bitnami/kafka/bin/kafka-topics.sh --delete \
    --bootstrap-server localhost:9092 \
    --topic nexmark-events 2>/dev/null || true
fi

echo "=== Clean done ==="
