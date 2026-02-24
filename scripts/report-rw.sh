#!/bin/bash
set -euo pipefail

###############################################################################
# report-rw.sh — NEXMark Benchmark Auto-Report for RisingWave
#
# Usage: ./report-rw.sh <query>        (e.g. ./report-rw.sh q0)
#        ./report-rw.sh <query> --skip-wait   (skip consumer lag polling)
#
# Waits for Kafka consumer lag to reach 0, then queries Prometheus to generate
# a JSON report under results-rw/.
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
QUERY="${1:?Usage: $0 <query> [--skip-wait] [--timeout <minutes>]}"
SKIP_WAIT=""
TIMEOUT_MIN=60
shift
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-wait) SKIP_WAIT="--skip-wait" ;;
    --timeout) TIMEOUT_MIN="$2"; shift ;;
  esac
  shift
done

# Load env vars from TOML
set -a
source <("${PROJECT_DIR}/tomlenv/bin/tomlenv" "${PROJECT_DIR}/toml/env.toml")
export TRICK_SYMBOLS_EMPTY=""
set +a

RW_NS="${BENCHMARK_RISINGWAVE_NAMESPACE}"
RW_NAME="${BENCHMARK_RISINGWAVE_NAME}"
KAFKA_NS="${BENCHMARK_KAFKA_NAMESPACE}"
KAFKA_NAME="${BENCHMARK_KAFKA_NAME}"
PROM_NS="monitoring"
PROM_SVC="kps-kube-prometheus-stack-prometheus"
PROM_PORT=9090
LOCAL_PROM_PORT=19090
RESULTS_DIR="${PROJECT_DIR}/results-rw"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
REPORT_FILE="${RESULTS_DIR}/${QUERY}-${TIMESTAMP}.json"
SUMMARY_FILE="${RESULTS_DIR}/summary.jsonl"

mkdir -p "${RESULTS_DIR}"

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
  for _ in $(seq 1 20); do
    if curl -sf "http://localhost:${port}/" >/dev/null 2>&1 || \
       curl -sf "http://localhost:${port}/-/healthy" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: port ${port} not ready" >&2
  return 1
}

###############################################################################
# Helper: query Prometheus instant query
###############################################################################
prom_query() {
  local query="$1"
  curl -sf --data-urlencode "query=${query}" \
    "http://localhost:${LOCAL_PROM_PORT}/api/v1/query" \
    | jq -r '.data.result[0].value[1] // "0"'
}

###############################################################################
# 1. Start port-forwards (Prometheus + RisingWave)
###############################################################################
echo "=== Setting up port-forwards ==="

LOCAL_RW_PORT=14567
RW_SVC="${RW_NAME}-frontend"

# Kill any existing port-forwards on our ports
for port in "${LOCAL_PROM_PORT}" "${LOCAL_RW_PORT}"; do
  pid=$(lsof -ti :"${port}" 2>/dev/null || true)
  if [[ -n "${pid}" ]]; then
    echo "  Killing existing process on port ${port} (pid=${pid})"
    kill ${pid} 2>/dev/null || true
    sleep 1
  fi
done

kubectl port-forward -n "${PROM_NS}" "svc/${PROM_SVC}" "${LOCAL_PROM_PORT}:${PROM_PORT}" &
PF_PIDS+=($!)

kubectl port-forward -n "${RW_NS}" "svc/${RW_SVC}" "${LOCAL_RW_PORT}:4567" &
PF_PIDS+=($!)

wait_port "${LOCAL_PROM_PORT}"
echo "  Prometheus port-forward ready."

###############################################################################
# 2. Get start time (recorded by start-rw.sh)
###############################################################################
JOB_START=0
if [[ -f /tmp/rw-bench-start-time ]]; then
  JOB_START=$(cat /tmp/rw-bench-start-time)
fi
echo "=== Job start time: ${JOB_START} ==="

###############################################################################
# 3. Get total Kafka messages
###############################################################################
echo "=== Fetching Kafka total messages ==="
LATEST_OFFSETS=$(kubectl exec -n "${KAFKA_NS}" "${KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic nexmark-events --time latest 2>/dev/null \
  | awk -F: '{s+=$3} END {print s+0}')
EARLIEST_OFFSETS=$(kubectl exec -n "${KAFKA_NS}" "${KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic nexmark-events --time earliest 2>/dev/null \
  | awk -F: '{s+=$3} END {print s+0}')
TOTAL_MESSAGES=$(( LATEST_OFFSETS - EARLIEST_OFFSETS ))
echo "  Total messages: ${TOTAL_MESSAGES} (latest=${LATEST_OFFSETS} earliest=${EARLIEST_OFFSETS})"

###############################################################################
# 4. Wait for processing to complete
#    - Track source_partition_input_count (total rows consumed from Kafka)
#    - When total >= expected AND rate ~= 0, processing is done
#    - Also show consumer lag for visibility
###############################################################################
if [[ "${SKIP_WAIT}" != "--skip-wait" ]]; then
  echo "=== Waiting for processing to complete (target: ${TOTAL_MESSAGES} messages) ==="
  DONE_COUNT=0
  REQUIRED_DONES=3
  MAX_POLLS=$(( TIMEOUT_MIN * 6 ))  # poll every 10s

  for i in $(seq 1 "${MAX_POLLS}"); do
    # Source input rate (rows/s)
    INPUT_RATE=$(prom_query "sum(rate(source_partition_input_count{namespace=\"${RW_NS}\", source_name=\"nexmark\"}[1m]))" 2>/dev/null || echo "0")

    # Total rows consumed so far
    TOTAL_CONSUMED=$(prom_query "sum(source_partition_input_count{namespace=\"${RW_NS}\", source_name=\"nexmark\"})" 2>/dev/null || echo "0")

    # Consumer lag (rw-consumer-* groups only)
    RW_LAG=$(prom_query 'sum(kafka_consumergroup_lag{topic="nexmark-events",consumergroup=~"rw-consumer-.*"})' 2>/dev/null || echo "NaN")

    # Check if done: rate near 0 AND consumed close to total
    RATE_INT=$(python3 -c "print(int(float('${INPUT_RATE}')))" 2>/dev/null || echo "999999")
    CONSUMED_INT=$(python3 -c "print(int(float('${TOTAL_CONSUMED}')))" 2>/dev/null || echo "0")
    THRESHOLD=$(( TOTAL_MESSAGES * 99 / 100 ))  # 99% of total

    if [[ ${RATE_INT} -lt 1000 && ${CONSUMED_INT} -ge ${THRESHOLD} ]]; then
      DONE_COUNT=$((DONE_COUNT + 1))
      echo "  [${i}] DONE consumed=${CONSUMED_INT} rate=${RATE_INT} r/s lag=${RW_LAG} (${DONE_COUNT}/${REQUIRED_DONES})"
      if [[ ${DONE_COUNT} -ge ${REQUIRED_DONES} ]]; then
        echo "  Processing confirmed complete."
        break
      fi
    else
      DONE_COUNT=0
      # Show human-friendly progress
      PCT=$(python3 -c "print(f'{${CONSUMED_INT}/${TOTAL_MESSAGES}*100:.1f}')" 2>/dev/null || echo "?")
      RATE_K=$(python3 -c "print(f'{${RATE_INT}/1000:.0f}')" 2>/dev/null || echo "?")
      echo "  [${i}] ${PCT}% (${CONSUMED_INT}/${TOTAL_MESSAGES}) | ${RATE_K}k r/s | lag=${RW_LAG}"
    fi
    sleep 10
  done

  if [[ ${DONE_COUNT} -lt ${REQUIRED_DONES} ]]; then
    echo "WARNING: Timed out waiting for processing to complete"
  fi
else
  echo "=== Skipping wait (--skip-wait) ==="
fi

###############################################################################
# 5. Compute processing time
###############################################################################
JOB_END_MS=$(date +%s%3N)
if [[ ${JOB_START} -gt 0 ]]; then
  TOTAL_TIME_SEC=$(( (JOB_END_MS - JOB_START) / 1000 ))
else
  TOTAL_TIME_SEC=0
fi
echo "=== Processing time: ${TOTAL_TIME_SEC}s ==="

# Dynamic Prometheus window based on actual job duration (avoid including idle time)
PROM_WINDOW="${TOTAL_TIME_SEC}s"
echo "  Prometheus query window: ${PROM_WINDOW}"

###############################################################################
# 6. Collect metrics from Prometheus
###############################################################################
echo "=== Collecting metrics from Prometheus ==="

# Throughput (calculated from total messages / time)
if [[ ${TOTAL_TIME_SEC} -gt 0 ]]; then
  THROUGHPUT_CALC=$(python3 -c "print(${TOTAL_MESSAGES} / ${TOTAL_TIME_SEC})")
else
  THROUGHPUT_CALC="0"
fi

# Throughput from Prometheus (RisingWave source metrics)
# source_partition_input_count = actual rows read from Kafka per partition
THROUGHPUT_AVG=$(prom_query "avg_over_time(sum(rate(source_partition_input_count{namespace=\"${RW_NS}\", source_name=\"nexmark\"}[1m]))[${PROM_WINDOW}:])" || echo "0")
THROUGHPUT_MAX=$(prom_query "max_over_time(sum(rate(source_partition_input_count{namespace=\"${RW_NS}\", source_name=\"nexmark\"}[1m]))[${PROM_WINDOW}:])" || echo "0")

# If Prometheus metrics are 0, fall back to calculated throughput
if [[ "${THROUGHPUT_AVG}" == "0" || "${THROUGHPUT_AVG}" == "null" ]]; then
  THROUGHPUT_AVG="${THROUGHPUT_CALC}"
  THROUGHPUT_MAX="${THROUGHPUT_CALC}"
fi

echo "  Throughput: avg=${THROUGHPUT_AVG} max=${THROUGHPUT_MAX} rec/s (calc=${THROUGHPUT_CALC})"

# Barrier latency (RisingWave equivalent of Flink checkpoint latency)
# meta_barrier_duration_seconds is a histogram; convert to milliseconds
LATENCY_P50=$(prom_query "histogram_quantile(0.5, sum(rate(meta_barrier_duration_seconds_bucket{namespace=\"${RW_NS}\"}[5m])) by (le)) * 1000")
LATENCY_P95=$(prom_query "histogram_quantile(0.95, sum(rate(meta_barrier_duration_seconds_bucket{namespace=\"${RW_NS}\"}[5m])) by (le)) * 1000")
LATENCY_P99=$(prom_query "histogram_quantile(0.99, sum(rate(meta_barrier_duration_seconds_bucket{namespace=\"${RW_NS}\"}[5m])) by (le)) * 1000")

echo "  Barrier latency: p50=${LATENCY_P50} p95=${LATENCY_P95} p99=${LATENCY_P99} ms"

# Barriers (equivalent to Flink checkpoints)
BARRIER_COMPLETED=$(prom_query "sum(all_barrier_nums{namespace=\"${RW_NS}\"})")
BARRIER_INFLIGHT=$(prom_query "sum(meta_barrier_inflight_duration_seconds_count{namespace=\"${RW_NS}\"})")

echo "  Barriers: completed=${BARRIER_COMPLETED} inflight_count=${BARRIER_INFLIGHT}"

# Resources (CPU / Memory) - all RisingWave pods
RES_AVG_CPU=$(prom_query "avg_over_time(sum(rate(container_cpu_usage_seconds_total{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-.*\", container=~\".+\", container!=\"POD\"}[1m]))[${PROM_WINDOW}:])")
RES_MAX_CPU=$(prom_query "max_over_time(sum(rate(container_cpu_usage_seconds_total{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-.*\", container=~\".+\", container!=\"POD\"}[1m]))[${PROM_WINDOW}:])")
RES_AVG_MEM=$(prom_query "avg_over_time(sum(container_memory_working_set_bytes{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-.*\", container=~\".+\", container!=\"POD\"})[${PROM_WINDOW}:])")
RES_MAX_MEM=$(prom_query "max_over_time(sum(container_memory_working_set_bytes{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-.*\", container=~\".+\", container!=\"POD\"})[${PROM_WINDOW}:])")

echo "  Resources (all RW): avg_cpu=${RES_AVG_CPU} max_cpu=${RES_MAX_CPU} avg_mem=${RES_AVG_MEM} max_mem=${RES_MAX_MEM}"

# Resources (compute node only - for fairer comparison with Flink TM)
COMPUTE_AVG_CPU=$(prom_query "avg_over_time(sum(rate(container_cpu_usage_seconds_total{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-compute-.*\", container=~\".+\", container!=\"POD\"}[1m]))[${PROM_WINDOW}:])")
COMPUTE_MAX_CPU=$(prom_query "max_over_time(sum(rate(container_cpu_usage_seconds_total{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-compute-.*\", container=~\".+\", container!=\"POD\"}[1m]))[${PROM_WINDOW}:])")
COMPUTE_AVG_MEM=$(prom_query "avg_over_time(sum(container_memory_working_set_bytes{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-compute-.*\", container=~\".+\", container!=\"POD\"})[${PROM_WINDOW}:])")
COMPUTE_MAX_MEM=$(prom_query "max_over_time(sum(container_memory_working_set_bytes{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-compute-.*\", container=~\".+\", container!=\"POD\"})[${PROM_WINDOW}:])")

echo "  Resources (compute): avg_cpu=${COMPUTE_AVG_CPU} max_cpu=${COMPUTE_MAX_CPU} avg_mem=${COMPUTE_AVG_MEM} max_mem=${COMPUTE_MAX_MEM}"

# Resources (compute + compactor - includes compaction overhead)
CC_AVG_CPU=$(prom_query "avg_over_time(sum(rate(container_cpu_usage_seconds_total{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-(compute|compactor)-.*\", container=~\".+\", container!=\"POD\"}[1m]))[${PROM_WINDOW}:])")
CC_MAX_CPU=$(prom_query "max_over_time(sum(rate(container_cpu_usage_seconds_total{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-(compute|compactor)-.*\", container=~\".+\", container!=\"POD\"}[1m]))[${PROM_WINDOW}:])")
CC_AVG_MEM=$(prom_query "avg_over_time(sum(container_memory_working_set_bytes{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-(compute|compactor)-.*\", container=~\".+\", container!=\"POD\"})[${PROM_WINDOW}:])")
CC_MAX_MEM=$(prom_query "max_over_time(sum(container_memory_working_set_bytes{namespace=\"${RW_NS}\", pod=~\"${RW_NAME}-(compute|compactor)-.*\", container=~\".+\", container!=\"POD\"})[${PROM_WINDOW}:])")

echo "  Resources (compute+compactor): avg_cpu=${CC_AVG_CPU} max_cpu=${CC_MAX_CPU} avg_mem=${CC_AVG_MEM} max_mem=${CC_MAX_MEM}"

# S3 state store size (hummock)
S3_HUMMOCK_PATH="s3://${BENCHMARK_RISINGWAVE_STORAGE_S3_BUCKET}/${BENCHMARK_RISINGWAVE_STORAGE_S3_DATA_DIRECTORY}/"
S3_SIZE_BYTES=$(aws s3 ls --recursive "${S3_HUMMOCK_PATH}" 2>/dev/null | awk '{s+=$3} END {print s+0}')
echo "  S3 state size: ${S3_SIZE_BYTES} bytes"

###############################################################################
# 7. Generate JSON report
###############################################################################
echo "=== Generating report ==="

jq -n \
  --arg query "${QUERY}" \
  --arg system "risingwave" \
  --arg version "${BENCHMARK_RISINGWAVE_VERSION}" \
  --argjson compute_replicas "${BENCHMARK_RISINGWAVE_REPLICAS_COMPUTE}" \
  --arg compute_cpu_limit "${BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_CPU_LIMIT}" \
  --arg compute_mem_limit "${BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_MEM_LIMIT}" \
  --argjson max_events "${BENCHMARK_NEXMARK_KAFKA_MAX_EVENTS}" \
  --argjson event_rate "${BENCHMARK_NEXMARK_KAFKA_EVENT_RATE}" \
  --arg timestamp "${TIMESTAMP}" \
  --argjson total_time_seconds "${TOTAL_TIME_SEC}" \
  --argjson total_messages "${TOTAL_MESSAGES}" \
  --arg throughput_avg "${THROUGHPUT_AVG}" \
  --arg throughput_max "${THROUGHPUT_MAX}" \
  --arg throughput_calc "${THROUGHPUT_CALC}" \
  --arg latency_p50 "${LATENCY_P50}" \
  --arg latency_p95 "${LATENCY_P95}" \
  --arg latency_p99 "${LATENCY_P99}" \
  --arg barrier_completed "${BARRIER_COMPLETED}" \
  --arg barrier_inflight "${BARRIER_INFLIGHT}" \
  --arg res_avg_cpu "${RES_AVG_CPU}" \
  --arg res_max_cpu "${RES_MAX_CPU}" \
  --arg res_avg_mem "${RES_AVG_MEM}" \
  --arg res_max_mem "${RES_MAX_MEM}" \
  --arg compute_avg_cpu "${COMPUTE_AVG_CPU}" \
  --arg compute_max_cpu "${COMPUTE_MAX_CPU}" \
  --arg compute_avg_mem "${COMPUTE_AVG_MEM}" \
  --arg compute_max_mem "${COMPUTE_MAX_MEM}" \
  --arg cc_avg_cpu "${CC_AVG_CPU}" \
  --arg cc_max_cpu "${CC_MAX_CPU}" \
  --arg cc_avg_mem "${CC_AVG_MEM}" \
  --arg cc_max_mem "${CC_MAX_MEM}" \
  --argjson s3_size_bytes "${S3_SIZE_BYTES}" \
  '{
    metadata: {
      query: $query,
      system: $system,
      version: $version,
      compute_replicas: $compute_replicas,
      compute_cpu_limit: $compute_cpu_limit,
      compute_mem_limit: $compute_mem_limit,
      max_events: $max_events,
      event_rate: $event_rate,
      timestamp: $timestamp
    },
    processing: {
      total_time_seconds: $total_time_seconds,
      total_messages: $total_messages
    },
    throughput: {
      avg_records_per_sec: $throughput_avg,
      max_records_per_sec: $throughput_max,
      calc_records_per_sec: $throughput_calc
    },
    latency: {
      type: "barrier_latency",
      description: "barrier duration (ms) — comparable to Flink checkpoint latency",
      p50_ms: $latency_p50,
      p95_ms: $latency_p95,
      p99_ms: $latency_p99
    },
    barriers: {
      completed: $barrier_completed,
      inflight_count: $barrier_inflight
    },
    resources: {
      total: {
        avg_cpu_cores: $res_avg_cpu,
        max_cpu_cores: $res_max_cpu,
        avg_memory_bytes: $res_avg_mem,
        max_memory_bytes: $res_max_mem
      },
      compute_only: {
        avg_cpu_cores: $compute_avg_cpu,
        max_cpu_cores: $compute_max_cpu,
        avg_memory_bytes: $compute_avg_mem,
        max_memory_bytes: $compute_max_mem
      },
      compute_compactor: {
        avg_cpu_cores: $cc_avg_cpu,
        max_cpu_cores: $cc_max_cpu,
        avg_memory_bytes: $cc_avg_mem,
        max_memory_bytes: $cc_max_mem
      }
    },
    storage: {
      s3_state_bytes: $s3_size_bytes
    }
  }' > "${REPORT_FILE}"

echo "  Report: ${REPORT_FILE}"

###############################################################################
# 8. Append to summary JSONL
###############################################################################
jq -c . "${REPORT_FILE}" >> "${SUMMARY_FILE}"
echo "  Summary: ${SUMMARY_FILE}"

###############################################################################
# 9. Print human-readable summary
###############################################################################
echo ""
echo "=== Benchmark Result ==="
echo ""
python3 -c "
import json, sys

with open('${REPORT_FILE}') as f:
    r = json.load(f)

m = r['metadata']
p = r['processing']
t = r['throughput']
l = r['latency']
b = r['barriers']
res_total = r['resources']['total']
res_compute = r['resources']['compute_only']
res_cc = r['resources']['compute_compactor']
s = r['storage']

throughput_krs = float(t['avg_records_per_sec']) / 1000
throughput_max_krs = float(t['max_records_per_sec']) / 1000
throughput_calc_krs = float(t['calc_records_per_sec']) / 1000

total_cpu_avg = float(res_total['avg_cpu_cores'])
total_mem_gib = float(res_total['avg_memory_bytes']) / (1024**3)
compute_cpu_avg = float(res_compute['avg_cpu_cores'])
compute_mem_gib = float(res_compute['avg_memory_bytes']) / (1024**3)
cc_cpu_avg = float(res_cc['avg_cpu_cores'])
cc_mem_gib = float(res_cc['avg_memory_bytes']) / (1024**3)

duration_min = p['total_time_seconds'] / 60
events_million = p['total_messages'] / 1_000_000

s3_bytes = s['s3_state_bytes']
if s3_bytes > 1024**3:
    s3_str = f'{s3_bytes / (1024**3):.1f} GiB'
elif s3_bytes > 1024**2:
    s3_str = f'{s3_bytes / (1024**2):.1f} MiB'
elif s3_bytes > 0:
    s3_str = f'{s3_bytes / 1024:.1f} KiB'
else:
    s3_str = 'NA'

print(f'Query:                  {m[\"query\"]}')
print(f'System:                 {m[\"system\"]} {m[\"version\"]}')
print(f'Throughput (avg):       {throughput_krs:.2f} kr/s (source partition input rate)')
print(f'Throughput (max):       {throughput_max_krs:.2f} kr/s')
print(f'Throughput (calc):      {throughput_calc_krs:.2f} kr/s (wall-clock: events/time)')
print(f'CPU total (avg):        {total_cpu_avg:.2f} cores')
print(f'CPU compute (avg):      {compute_cpu_avg:.2f} cores')
print(f'CPU compute+compact:    {cc_cpu_avg:.2f} cores')
print(f'Mem total (avg):        {total_mem_gib:.2f} GiB')
print(f'Mem compute (avg):      {compute_mem_gib:.2f} GiB')
print(f'Mem compute+compact:    {cc_mem_gib:.2f} GiB')
print(f'Duration:               {duration_min:.0f} min')
print(f'Kafka events:           {events_million:.0f} M')
print(f'Barrier latency p50/p95/p99: {float(l[\"p50_ms\"]):.1f} / {float(l[\"p95_ms\"]):.1f} / {float(l[\"p99_ms\"]):.1f} ms (NOT per-record)')
print(f'Barriers completed:     {b[\"completed\"]}')
print(f'S3 State Size:          {s3_str}')
print(f'Compute Replicas:       {m[\"compute_replicas\"]}')
print(f'Compute CPU Limit:      {m[\"compute_cpu_limit\"]}')
print(f'Compute Mem Limit:      {m[\"compute_mem_limit\"]}')
"
echo ""
echo "  Full report: ${REPORT_FILE}"
