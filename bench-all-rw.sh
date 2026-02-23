#!/bin/bash
set -euo pipefail

###############################################################################
# bench-all-rw.sh — Run all NEXMark queries on RisingWave sequentially
#
# Usage: ./bench-all-rw.sh              (run all queries)
#        nohup ./bench-all-rw.sh &      (fire-and-forget overnight)
#
# Assumes Kafka topic nexmark-events is already populated (via prepare.sh).
# Each query recreates sources from scratch to ensure fresh read from earliest.
#
# Available queries (18): q0 q1 q2 q3 q4 q5 q7 q8 q9 q10 q14 q15 q16 q17 q18 q20 q21 q22
# Missing vs Flink (3): q11 (session window), q12 (PROCTIME window), q19 (TOP-N)
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

QUERIES=(q1 q2 q3 q4 q5 q7 q8 q9 q10 q14 q15 q16 q17 q18 q20 q21 q22)

RESULTS_DIR="${SCRIPT_DIR}/results-rw"
mkdir -p "${RESULTS_DIR}"
LOG_FILE="${RESULTS_DIR}/bench-all-rw-$(date +%Y%m%d-%H%M%S).log"

# Tee all output to log file
exec > >(tee -a "${LOG_FILE}") 2>&1

echo "============================================================"
echo " NEXMark Benchmark - RisingWave"
echo " Started: $(date)"
echo " Queries: ${QUERIES[*]}"
echo " Count:   ${#QUERIES[@]} queries"
echo " Log:     ${LOG_FILE}"
echo "============================================================"
echo ""

###############################################################################
# Pre-flight checks
###############################################################################
echo "=== Pre-flight checks ==="

# Check psql
if ! command -v psql &>/dev/null; then
  echo "ERROR: psql not found. Install with: sudo yum install -y postgresql15" >&2
  exit 1
fi
echo "  psql: OK"

# Load env vars
set -a
source <("${SCRIPT_DIR}/tomlenv/bin/tomlenv" "${SCRIPT_DIR}/toml/env.toml")
export TRICK_SYMBOLS_EMPTY=""
set +a

# Check RisingWave is running
RW_NS="${BENCHMARK_RISINGWAVE_NAMESPACE}"
RW_NAME="${BENCHMARK_RISINGWAVE_NAME}"
RW_PHASE=$(kubectl get risingwave "${RW_NAME}" -n "${RW_NS}" \
  -o jsonpath='{.status.conditions[?(@.type=="Running")].status}' 2>/dev/null || echo "NotFound")
if [[ "${RW_PHASE}" != "True" ]]; then
  echo "ERROR: RisingWave '${RW_NAME}' is not running in namespace '${RW_NS}' (status: ${RW_PHASE})"
  echo "       Run ./setup-rw.sh first."
  exit 1
fi
echo "  RisingWave: Running (${RW_NAME} in ${RW_NS})"

# Check Kafka data exists
KAFKA_NS="${BENCHMARK_KAFKA_NAMESPACE}"
KAFKA_NAME="${BENCHMARK_KAFKA_NAME}"
TOTAL_MESSAGES=$(kubectl exec -n "${KAFKA_NS}" "${KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic nexmark-events 2>/dev/null \
  | awk -F: '{s+=$3} END {print s+0}' || echo "0")

if [[ "${TOTAL_MESSAGES}" -eq 0 ]]; then
  echo "WARNING: No data in nexmark-events topic. Running prepare.sh..."
  ./prepare.sh
  TOTAL_MESSAGES=$(kubectl exec -n "${KAFKA_NS}" "${KAFKA_NAME}-controller-0" -- \
    /opt/bitnami/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic nexmark-events 2>/dev/null \
    | awk -F: '{s+=$3} END {print s+0}')
fi
echo "  Kafka data: ${TOTAL_MESSAGES} messages in nexmark-events"

# Print resource info
echo ""
echo "=== RisingWave Resource Config ==="
echo "  Compute:   ${BENCHMARK_RISINGWAVE_REPLICAS_COMPUTE} replica(s), CPU limit=${BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_CPU_LIMIT}, Mem limit=${BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_MEM_LIMIT}"
echo "  Compactor:  ${BENCHMARK_RISINGWAVE_REPLICAS_COMPACTOR} replica(s), CPU limit=${BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_CPU_LIMIT}, Mem limit=${BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_MEM_LIMIT}"
echo "  Frontend:  ${BENCHMARK_RISINGWAVE_REPLICAS_FRONTEND} replica(s)"
echo "  Meta:      ${BENCHMARK_RISINGWAVE_REPLICAS_META} replica(s)"
echo "  Version:   ${BENCHMARK_RISINGWAVE_VERSION}"
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

    # 1. Create sources + submit sink
    ./start-rw.sh "${query}"

    # 2. Wait for job to initialize
    sleep 30

    # 3. Wait for lag=0 and collect metrics (30 min timeout)
    ./report-rw.sh "${query}" --timeout 60
  ); then
    PASSED+=("${query}")
    echo "  >> ${query}: PASSED"
  else
    FAILED+=("${query}")
    echo "  >> ${query}: FAILED (see log for details)"
  fi

  # 4. Drop sink + sources for clean slate
  ./clean-rw.sh --keep-topic || true

  # 5. Brief pause between queries
  sleep 10

  echo ""
done

echo "============================================================"
echo " NEXMark Benchmark - RisingWave - Complete"
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
        res = r['resources']['total']
        comp = r['resources']['compute_only']
        rows.append({
            'query': m['query'],
            'time_min': p['total_time_seconds'] / 60,
            'events_m': p['total_messages'] / 1_000_000,
            'tput_kr': float(t['avg_records_per_sec']) / 1000,
            'p50': float(l['p50_ms']),
            'p95': float(l['p95_ms']),
            'p99': float(l['p99_ms']),
            'cpu': float(res['avg_cpu_cores']),
            'mem_gib': float(res['avg_memory_bytes']) / (1024**3),
            'cpu_c': float(comp['avg_cpu_cores']),
        })

if not rows:
    print('  No results found.')
    sys.exit(0)

hdr = f'{\"Query\":<6} {\"Time(m)\":>7} {\"Events(M)\":>9} {\"Tput(kr/s)\":>10} {\"p50(ms)\":>8} {\"p95(ms)\":>8} {\"p99(ms)\":>8} {\"CPU\":>5} {\"CpuC\":>5} {\"Mem(G)\":>6}'
print(hdr)
print('-' * len(hdr))
for r in rows:
    print(f'{r[\"query\"]:<6} {r[\"time_min\"]:>7.1f} {r[\"events_m\"]:>9.0f} {r[\"tput_kr\"]:>10.2f} {r[\"p50\"]:>8.1f} {r[\"p95\"]:>8.1f} {r[\"p99\"]:>8.1f} {r[\"cpu\"]:>5.1f} {r[\"cpu_c\"]:>5.1f} {r[\"mem_gib\"]:>6.1f}')
"
  echo ""
  echo "  CPU  = Total RisingWave CPU (all pods)"
  echo "  CpuC = Compute node CPU only"
  echo ""
fi

echo "Log: ${LOG_FILE}"
