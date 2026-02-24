#!/bin/bash
set -euo pipefail

###############################################################################
# bench-compare.sh — Run NEXMark benchmark: Flink first, then RisingWave
#
# Usage: ./bench-compare.sh                    (run both, all queries)
#        ./bench-compare.sh --flink-only       (Flink only)
#        ./bench-compare.sh --rw-only          (RisingWave only)
#        nohup ./bench-compare.sh &            (fire-and-forget overnight)
#
# IMPORTANT: Flink and RisingWave run SEQUENTIALLY to avoid resource contention.
# Kafka topic data is prepared once and reused for both systems.
#
# Estimated total time: ~20-24 hours (10-12h per system)
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_DIR}"

# Parse flags
RUN_FLINK=true
RUN_RW=true
for arg in "$@"; do
  case "${arg}" in
    --flink-only) RUN_RW=false ;;
    --rw-only)    RUN_FLINK=false ;;
  esac
done

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_FILE="${PROJECT_DIR}/results/bench-compare-${TIMESTAMP}.log"
mkdir -p "${PROJECT_DIR}/results" "${PROJECT_DIR}/results-rw"

exec > >(tee -a "${LOG_FILE}") 2>&1

echo "============================================================"
echo " NEXMark Benchmark - Flink vs RisingWave Comparison"
echo " Started: $(date)"
echo " Run Flink:      ${RUN_FLINK}"
echo " Run RisingWave: ${RUN_RW}"
echo " Log:            ${LOG_FILE}"
echo "============================================================"
echo ""

# Load env vars
set -a
source <("${PROJECT_DIR}/tomlenv/bin/tomlenv" "${PROJECT_DIR}/toml/env.toml")
export TRICK_SYMBOLS_EMPTY=""
set +a

###############################################################################
# Print resource comparison
###############################################################################
echo "=== Resource Configuration ==="
echo ""
echo "  [Flink]"
echo "    TaskManager: ${BENCHMARK_FLINK_TASKMANAGER_REPLICAS} replica(s), ${BENCHMARK_FLINK_TASKMANAGER_SLOTS} slots"
echo "    TM Memory:   ${BENCHMARK_FLINK_TASKMANAGER_MEMORY} (process.size)"
echo "    JM Memory:   ${BENCHMARK_FLINK_JOBMANAGER_MEMORY}"
echo "    Parallelism: ${BENCHMARK_FLINK_PARALLELISM}"
echo ""
echo "  [RisingWave ${BENCHMARK_RISINGWAVE_VERSION}]"
echo "    Compute:     ${BENCHMARK_RISINGWAVE_REPLICAS_COMPUTE} replica(s), CPU=${BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_CPU_LIMIT}, Mem=${BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_MEM_LIMIT}"
echo "    Compactor:   ${BENCHMARK_RISINGWAVE_REPLICAS_COMPACTOR} replica(s), CPU=${BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_CPU_LIMIT}, Mem=${BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_MEM_LIMIT}"
echo ""
echo "  [Kafka]"
echo "    Events:      ${BENCHMARK_NEXMARK_KAFKA_MAX_EVENTS}"
echo "    Rate:        ${BENCHMARK_NEXMARK_KAFKA_EVENT_RATE}/s"
echo "    Partitions:  ${BENCHMARK_NEXMARK_KAFKA_PARTITION}"
echo ""

###############################################################################
# Prepare Kafka data (shared by both systems)
###############################################################################
echo "=== Checking Kafka data ==="

KAFKA_NS="${BENCHMARK_KAFKA_NAMESPACE}"
KAFKA_NAME="${BENCHMARK_KAFKA_NAME}"
TOTAL_MESSAGES=$(kubectl exec -n "${KAFKA_NS}" "${KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic nexmark-events 2>/dev/null \
  | awk -F: '{s+=$3} END {print s+0}' || echo "0")

if [[ "${TOTAL_MESSAGES}" -lt "${BENCHMARK_NEXMARK_KAFKA_MAX_EVENTS}" ]]; then
  echo "  Need to prepare data: have ${TOTAL_MESSAGES}, need ${BENCHMARK_NEXMARK_KAFKA_MAX_EVENTS}"
  "${SCRIPT_DIR}/prepare.sh"
else
  echo "  Kafka data ready: ${TOTAL_MESSAGES} messages"
fi
echo ""

###############################################################################
# Phase 1: Flink Benchmark
###############################################################################
FLINK_START_TIME=""
FLINK_END_TIME=""

if [[ "${RUN_FLINK}" == "true" ]]; then
  echo "============================================================"
  echo " Phase 1: Flink Benchmark"
  echo " Started: $(date)"
  echo "============================================================"
  echo ""

  FLINK_START_TIME=$(date +%s)

  # Run Flink bench-all (q0 already in summary.jsonl)
  "${SCRIPT_DIR}/bench-all.sh" || echo "WARNING: Flink benchmark had failures"

  FLINK_END_TIME=$(date +%s)
  FLINK_DURATION=$(( (FLINK_END_TIME - FLINK_START_TIME) / 60 ))

  echo ""
  echo "  Flink phase completed in ${FLINK_DURATION} minutes."
  echo ""

  # Clean up Flink jobs but keep Kafka data for RisingWave
  echo "=== Cleaning up Flink (keeping Kafka data) ==="
  "${SCRIPT_DIR}/clean.sh" --keep-topic || true
  sleep 10
fi

###############################################################################
# Phase 2: RisingWave Benchmark
###############################################################################
RW_START_TIME=""
RW_END_TIME=""

if [[ "${RUN_RW}" == "true" ]]; then
  echo "============================================================"
  echo " Phase 2: RisingWave Benchmark"
  echo " Started: $(date)"
  echo "============================================================"
  echo ""

  RW_START_TIME=$(date +%s)

  # Run RisingWave bench-all (q0 already in summary.jsonl)
  "${SCRIPT_DIR}/bench-all-rw.sh" || echo "WARNING: RisingWave benchmark had failures"

  RW_END_TIME=$(date +%s)
  RW_DURATION=$(( (RW_END_TIME - RW_START_TIME) / 60 ))

  echo ""
  echo "  RisingWave phase completed in ${RW_DURATION} minutes."
  echo ""

  # Clean up RisingWave
  "${SCRIPT_DIR}/clean-rw.sh" --keep-topic || true
fi

###############################################################################
# Phase 3: Comparison Summary
###############################################################################
echo ""
echo "============================================================"
echo " Comparison Summary"
echo " Finished: $(date)"
echo "============================================================"
echo ""

python3 -c "
import json, sys, os

flink_file = '${PROJECT_DIR}/results/summary.jsonl'
rw_file = '${PROJECT_DIR}/results-rw/summary.jsonl'

flink = {}
rw = {}

if os.path.exists(flink_file):
    with open(flink_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            q = r['metadata']['query']
            flink[q] = r

if os.path.exists(rw_file):
    with open(rw_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            q = r['metadata']['query']
            rw[q] = r

all_queries = sorted(set(list(flink.keys()) + list(rw.keys())),
                     key=lambda x: int(x[1:]))

if not all_queries:
    print('  No results found.')
    sys.exit(0)

# Header
print('NOTE: Throughput comparison uses calc_records_per_sec (wall-clock: total_events / total_time)')
print('      Latency is NOT directly comparable: Flink=per-record event latency, RW=barrier epoch latency')
print()
hdr = f'{\"Query\":<6} {\"Flink(kr/s)\":>11} {\"RW(kr/s)\":>11} {\"Ratio\":>7} {\"Flink(s)\":>8} {\"RW(s)\":>8} {\"Flink p99\":>10} {\"RW p99\":>10}'
print(hdr)
print('-' * len(hdr))

for q in all_queries:
    f = flink.get(q)
    r = rw.get(q)

    # Throughput: use calc if available, else wall-clock from total_time
    if f:
        f_time = f['processing']['total_time_seconds']
        f_total = f['processing']['total_messages']
        f_calc = float(f['throughput'].get('calc_records_per_sec', 0))
        if f_calc == 0 and f_time > 0:
            f_calc = f_total / f_time
        f_tput = f_calc / 1000
        f_lat = float(f['latency']['p99_ms'])
    else:
        f_tput = 0
        f_time = 0
        f_lat = 0

    if r:
        r_time = r['processing']['total_time_seconds']
        r_total = r['processing']['total_messages']
        r_calc = float(r['throughput'].get('calc_records_per_sec', 0))
        if r_calc == 0 and r_time > 0:
            r_calc = r_total / r_time
        r_tput = r_calc / 1000
        r_lat = float(r['latency']['p99_ms'])
    else:
        r_tput = 0
        r_time = 0
        r_lat = 0

    # Ratio
    if f_tput > 0 and r_tput > 0:
        ratio = r_tput / f_tput
        if ratio >= 1:
            ratio_str = f'RW {ratio:.1f}x'
        else:
            ratio_str = f'FL {1/ratio:.1f}x'
    elif f_tput > 0:
        ratio_str = 'FL only'
    elif r_tput > 0:
        ratio_str = 'RW only'
    else:
        ratio_str = 'N/A'

    f_tput_str = f'{f_tput:.1f}' if f_tput > 0 else '-'
    r_tput_str = f'{r_tput:.1f}' if r_tput > 0 else '-'
    f_time_str = f'{f_time}' if f_time > 0 else '-'
    r_time_str = f'{r_time}' if r_time > 0 else '-'
    f_lat_str = f'{f_lat:.1f}' if f and f_tput > 0 else '-'
    r_lat_str = f'{r_lat:.1f}' if r and r_tput > 0 else '-'

    print(f'{q:<6} {f_tput_str:>11} {r_tput_str:>11} {ratio_str:>7} {f_time_str:>8} {r_time_str:>8} {f_lat_str:>10} {r_lat_str:>10}')

print()
print('Latency note:')
print('  Flink p99  = per-record event latency (LatencyMarker: source -> operator)')
print('  RW p99     = barrier epoch completion time (NOT per-record, includes state flush)')
print('  These measure DIFFERENT things and should NOT be directly compared.')
print()
print('Throughput note:')
print('  calc_records_per_sec = total_messages / total_wall_clock_seconds')
print('  This includes backpressure, buffering, and all processing overhead.')
"

echo ""
echo "  Flink results:      ${PROJECT_DIR}/results/summary.jsonl"
echo "  RisingWave results: ${PROJECT_DIR}/results-rw/summary.jsonl"
echo "  Full log:           ${LOG_FILE}"
