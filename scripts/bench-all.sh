#!/bin/bash
set -euo pipefail

###############################################################################
# bench-all.sh — Run all NEXMark queries on Flink sequentially
#
# Usage: ./bench-all.sh              (run all queries)
#        nohup ./bench-all.sh &      (fire-and-forget overnight)
#
# Assumes Kafka topic nexmark-events is already populated (via prepare.sh).
# Each query cancels previous job and resets consumer group offset.
#
# Common queries (18): q0 q1 q2 q3 q4 q5 q7 q8 q9 q10 q14 q15 q16 q17 q18 q20 q21 q22
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_DIR}"

QUERIES=(q0 q1 q2 q3 q4 q5 q7 q8 q9 q10 q14 q15 q16 q17 q18 q20 q21 q22)
TIMEOUT_MIN=60

RESULTS_DIR="${PROJECT_DIR}/results"
mkdir -p "${RESULTS_DIR}"
LOG_FILE="${RESULTS_DIR}/bench-all-$(date +%Y%m%d-%H%M%S).log"

# Tee all output to log file
exec > >(tee -a "${LOG_FILE}") 2>&1

echo "============================================================"
echo " NEXMark Benchmark - Flink"
echo " Started: $(date)"
echo " Queries: ${QUERIES[*]}"
echo " Count:   ${#QUERIES[@]} queries"
echo " Timeout: ${TIMEOUT_MIN} min per query"
echo " Log:     ${LOG_FILE}"
echo "============================================================"
echo ""

###############################################################################
# Pre-flight checks
###############################################################################
echo "=== Pre-flight checks ==="

# Load env vars
set -a
source <("${PROJECT_DIR}/tomlenv/bin/tomlenv" "${PROJECT_DIR}/toml/env.toml")
export TRICK_SYMBOLS_EMPTY=""
set +a

FLINK_NS="${BENCHMARK_FLINK_NAMESPACE}"
KAFKA_NS="${BENCHMARK_KAFKA_NAMESPACE}"
KAFKA_NAME="${BENCHMARK_KAFKA_NAME}"

# Check Flink is running
FLINK_TM=$(kubectl get pods -n "${FLINK_NS}" -l component=taskmanager --no-headers 2>/dev/null | grep Running | wc -l)
if [[ "${FLINK_TM}" -eq 0 ]]; then
  echo "ERROR: No running Flink TaskManager in namespace '${FLINK_NS}'"
  exit 1
fi
echo "  Flink: ${FLINK_TM} TaskManager(s) running"

# Check Kafka data exists
LATEST_OFFSETS=$(kubectl exec -n "${KAFKA_NS}" "${KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic nexmark-events --time latest 2>/dev/null \
  | awk -F: '{s+=$3} END {print s+0}')
EARLIEST_OFFSETS=$(kubectl exec -n "${KAFKA_NS}" "${KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic nexmark-events --time earliest 2>/dev/null \
  | awk -F: '{s+=$3} END {print s+0}')
TOTAL_MESSAGES=$(( LATEST_OFFSETS - EARLIEST_OFFSETS ))

if [[ "${TOTAL_MESSAGES}" -eq 0 ]]; then
  echo "ERROR: No data in nexmark-events topic. Run prepare.sh first."
  exit 1
fi
echo "  Kafka data: ${TOTAL_MESSAGES} messages in nexmark-events"

# Print resource info
echo ""
echo "=== Flink Resource Config ==="
echo "  Parallelism:     ${BENCHMARK_FLINK_PARALLELISM}"
echo "  TaskManagers:    ${BENCHMARK_FLINK_TASKMANAGER_REPLICAS}"
echo "  TM Slots:        ${BENCHMARK_FLINK_TASKMANAGER_SLOTS}"
echo "  TM Memory:       ${BENCHMARK_FLINK_TASKMANAGER_MEMORY}"
echo "  JM Memory:       ${BENCHMARK_FLINK_JOBMANAGER_MEMORY}"
echo "  Checkpoint:      ${BENCHMARK_FLINK_CHECKPOINT_INTERVAL}"
echo ""

###############################################################################
# Run queries
###############################################################################
PASSED=()
FAILED=()

for query in "${QUERIES[@]}"; do
  echo "============================================================"
  echo " [$(date +%H:%M:%S)] Starting: ${query}"
  echo "============================================================"

  if (
    set -e

    # 1. Cancel any running Flink jobs
    "${SCRIPT_DIR}/clean.sh" --keep-topic || true

    # 2. Reset consumer group offset to earliest
    kubectl exec -n "${KAFKA_NS}" "${KAFKA_NAME}-controller-0" -- \
      /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 --group nexmark-events \
      --topic nexmark-events --reset-offsets --to-earliest --execute 2>/dev/null || true

    # 3. Submit query
    "${SCRIPT_DIR}/start.sh" "${query}"

    # 4. Wait for job to initialize
    sleep 30

    # 5. Wait for lag=0 and collect metrics
    "${SCRIPT_DIR}/report.sh" "${query}" --timeout "${TIMEOUT_MIN}"
  ); then
    PASSED+=("${query}")
    echo "  >> ${query}: PASSED"
  else
    FAILED+=("${query}")
    echo "  >> ${query}: FAILED (see log for details)"
  fi

  # Brief pause between queries
  sleep 10

  echo ""
done

echo "============================================================"
echo " NEXMark Benchmark - Flink - Complete"
echo " Finished: $(date)"
echo " Passed:   ${#PASSED[@]}/${#QUERIES[@]} (${PASSED[*]:-none})"
echo " Failed:   ${#FAILED[@]}/${#QUERIES[@]} (${FAILED[*]:-none})"
echo "============================================================"
echo ""

# Print summary table from summary.jsonl
SUMMARY_FILE="${RESULTS_DIR}/summary.jsonl"
if [[ -f "${SUMMARY_FILE}" ]]; then
  echo "=== Results Summary ==="
  echo ""
  python3 -c "
import json, sys

summary_file = '${SUMMARY_FILE}'
rows = []
with open(summary_file) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        m = r['metadata']
        p = r['processing']
        t = r['throughput']
        l = r['latency']
        res = r['resources']
        rows.append({
            'query': m['query'],
            'time_min': p['total_time_seconds'] / 60,
            'events_m': p['total_messages'] / 1_000_000,
            'tput_kr': float(t.get('calc_records_per_sec', t['avg_records_per_sec'])) / 1000,
            'p50': float(l['p50_ms']),
            'p95': float(l['p95_ms']),
            'p99': float(l['p99_ms']),
            'cpu': float(res['avg_cpu_cores']),
            'mem_gib': float(res['avg_memory_bytes']) / (1024**3),
        })

if not rows:
    print('  No results found.')
    sys.exit(0)

hdr = f'{\"Query\":<6} {\"Time(m)\":>7} {\"Events(M)\":>9} {\"Tput(kr/s)\":>10} {\"p50(ms)\":>8} {\"p95(ms)\":>8} {\"p99(ms)\":>8} {\"CPU\":>5} {\"Mem(G)\":>6}'
print(hdr)
print('-' * len(hdr))
for r in rows:
    print(f'{r[\"query\"]:<6} {r[\"time_min\"]:>7.1f} {r[\"events_m\"]:>9.0f} {r[\"tput_kr\"]:>10.2f} {r[\"p50\"]:>8.1f} {r[\"p95\"]:>8.1f} {r[\"p99\"]:>8.1f} {r[\"cpu\"]:>5.1f} {r[\"mem_gib\"]:>6.1f}')
"
  echo ""
fi

echo "Log: ${LOG_FILE}"
