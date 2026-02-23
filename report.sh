#!/bin/bash
set -euo pipefail

###############################################################################
# report.sh — NEXMark Benchmark Auto-Report
#
# Usage: ./report.sh <query>        (e.g. ./report.sh q0)
#        ./report.sh <query> --skip-wait   (skip consumer lag polling)
#
# Waits for Kafka consumer lag to reach 0, then queries Prometheus and Flink
# REST APIs to generate a JSON report under results/.
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
source <("${SCRIPT_DIR}/tomlenv/bin/tomlenv" "${SCRIPT_DIR}/toml/env.toml")
export TRICK_SYMBOLS_EMPTY=""
set +a

FLINK_NS="${BENCHMARK_FLINK_NAMESPACE}"
KAFKA_NS="${BENCHMARK_KAFKA_NAMESPACE}"
KAFKA_NAME="${BENCHMARK_KAFKA_NAME}"
PROM_NS="monitoring"
PROM_SVC="kps-kube-prometheus-stack-prometheus"
PROM_PORT=9090
LOCAL_PROM_PORT=19090
LOCAL_FLINK_PORT=18081
RESULTS_DIR="${SCRIPT_DIR}/results"
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
# 1. Start port-forwards (Prometheus + Flink REST)
###############################################################################
echo "=== Setting up port-forwards ==="

# Kill any existing port-forwards on our ports to avoid bind conflicts
for port in "${LOCAL_PROM_PORT}" "${LOCAL_FLINK_PORT}"; do
  pid=$(lsof -ti :"${port}" 2>/dev/null || true)
  if [[ -n "${pid}" ]]; then
    echo "  Killing existing process on port ${port} (pid=${pid})"
    kill ${pid} 2>/dev/null || true
    sleep 1
  fi
done

kubectl port-forward -n "${PROM_NS}" "svc/${PROM_SVC}" "${LOCAL_PROM_PORT}:${PROM_PORT}" &
PF_PIDS+=($!)

kubectl port-forward -n "${FLINK_NS}" svc/flink-jobmanager "${LOCAL_FLINK_PORT}:8081" &
PF_PIDS+=($!)

wait_port "${LOCAL_PROM_PORT}"
wait_port "${LOCAL_FLINK_PORT}"
echo "  Port-forwards ready."

###############################################################################
# 2. Get Flink job info
###############################################################################
echo "=== Fetching Flink job info ==="
JOBS_JSON=$(curl -sf "http://localhost:${LOCAL_FLINK_PORT}/jobs")
JOB_ID=$(echo "${JOBS_JSON}" | jq -r '.jobs[] | select(.status == "RUNNING") | .id' | head -1)

if [[ -z "${JOB_ID}" ]]; then
  echo "WARNING: No running Flink job found. Using most recent job."
  JOB_ID=$(echo "${JOBS_JSON}" | jq -r '.jobs[0].id')
fi

JOB_DETAIL=$(curl -sf "http://localhost:${LOCAL_FLINK_PORT}/jobs/${JOB_ID}")
JOB_NAME=$(echo "${JOB_DETAIL}" | jq -r '.name')
JOB_START=$(echo "${JOB_DETAIL}" | jq -r '.["start-time"]')
JOB_STATE=$(echo "${JOB_DETAIL}" | jq -r '.state')
PARALLELISM="${BENCHMARK_FLINK_PARALLELISM}"
echo "  Job: ${JOB_NAME} (${JOB_ID}) state=${JOB_STATE} parallelism=${PARALLELISM}"

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
# 4. Wait for consumer lag to reach 0 (poll every 10s, 3 consecutive zeros)
###############################################################################
if [[ "${SKIP_WAIT}" != "--skip-wait" ]]; then
  echo "=== Waiting for consumer lag to reach 0 ==="
  ZERO_COUNT=0
  REQUIRED_ZEROS=3
  MAX_POLLS=$(( TIMEOUT_MIN * 6 ))  # poll every 10s

  for i in $(seq 1 "${MAX_POLLS}"); do
    LAG=$(prom_query 'sum(kafka_consumergroup_lag{topic="nexmark-events", consumergroup="nexmark-events"})' 2>/dev/null || echo "NaN")

    # Treat non-numeric or missing as non-zero
    if [[ "${LAG}" == "0" || "${LAG}" == "0.0" ]]; then
      ZERO_COUNT=$((ZERO_COUNT + 1))
      echo "  [${i}] lag=0 (${ZERO_COUNT}/${REQUIRED_ZEROS} consecutive zeros)"
      if [[ ${ZERO_COUNT} -ge ${REQUIRED_ZEROS} ]]; then
        echo "  Consumer lag confirmed at 0."
        break
      fi
    else
      ZERO_COUNT=0
      echo "  [${i}] lag=${LAG} - waiting..."
    fi
    sleep 10
  done

  if [[ ${ZERO_COUNT} -lt ${REQUIRED_ZEROS} ]]; then
    echo "WARNING: Timed out waiting for consumer lag to reach 0 (last lag=${LAG})"
  fi
else
  echo "=== Skipping consumer lag wait (--skip-wait) ==="
fi

###############################################################################
# 5. Compute processing time
###############################################################################
JOB_END_MS=$(date +%s%3N)
TOTAL_TIME_SEC=$(( (JOB_END_MS - JOB_START) / 1000 ))
echo "=== Processing time: ${TOTAL_TIME_SEC}s ==="

# Dynamic Prometheus window based on actual job duration (avoid including idle time)
PROM_WINDOW="${TOTAL_TIME_SEC}s"
echo "  Prometheus query window: ${PROM_WINDOW}"

###############################################################################
# 6. Collect metrics from Prometheus
###############################################################################
echo "=== Collecting metrics from Prometheus ==="

# Throughput (Prometheus-based: Source operator numRecordsIn rate)
THROUGHPUT_AVG=$(prom_query "avg_over_time(sum(rate(flink_taskmanager_job_task_operator_numRecordsIn{namespace=\"${FLINK_NS}\", operator_name=~\"Source.*\"}[1m]))[${PROM_WINDOW}:])")
THROUGHPUT_MAX=$(prom_query "max_over_time(sum(rate(flink_taskmanager_job_task_operator_numRecordsIn{namespace=\"${FLINK_NS}\", operator_name=~\"Source.*\"}[1m]))[${PROM_WINDOW}:])")
THROUGHPUT_OUT_AVG=$(prom_query "avg_over_time(sum(rate(flink_taskmanager_job_task_operator_numRecordsIn{namespace=\"${FLINK_NS}\", operator_name=~\"Sink.*\"}[1m]))[${PROM_WINDOW}:])")

# Throughput (wall-clock: total_messages / total_time_seconds)
if [[ ${TOTAL_TIME_SEC} -gt 0 ]]; then
  THROUGHPUT_CALC=$(python3 -c "print(${TOTAL_MESSAGES} / ${TOTAL_TIME_SEC})")
else
  THROUGHPUT_CALC="0"
fi

echo "  Throughput: avg=${THROUGHPUT_AVG} max=${THROUGHPUT_MAX} calc=${THROUGHPUT_CALC} rec/s"

# Checkpoint duration latency (p50, p95, p99) — comparable to RisingWave barrier latency
# lastCheckpointDuration is a gauge (ms) updated after each checkpoint
LATENCY_P50=$(prom_query "quantile_over_time(0.5, flink_jobmanager_job_lastCheckpointDuration{namespace=\"${FLINK_NS}\"}[${PROM_WINDOW}])")
LATENCY_P95=$(prom_query "quantile_over_time(0.95, flink_jobmanager_job_lastCheckpointDuration{namespace=\"${FLINK_NS}\"}[${PROM_WINDOW}])")
LATENCY_P99=$(prom_query "quantile_over_time(0.99, flink_jobmanager_job_lastCheckpointDuration{namespace=\"${FLINK_NS}\"}[${PROM_WINDOW}])")

echo "  Checkpoint latency: p50=${LATENCY_P50} p95=${LATENCY_P95} p99=${LATENCY_P99} ms"

# Checkpoints
CKPT_COMPLETED=$(prom_query "sum(flink_jobmanager_job_numberOfCompletedCheckpoints{namespace=\"${FLINK_NS}\"})")
CKPT_FAILED=$(prom_query "sum(flink_jobmanager_job_numberOfFailedCheckpoints{namespace=\"${FLINK_NS}\"})")
CKPT_DURATION=$(prom_query "flink_jobmanager_job_lastCheckpointDuration{namespace=\"${FLINK_NS}\"}")
CKPT_SIZE=$(prom_query "flink_jobmanager_job_lastCheckpointSize{namespace=\"${FLINK_NS}\"}")

echo "  Checkpoints: completed=${CKPT_COMPLETED} failed=${CKPT_FAILED} last_duration=${CKPT_DURATION}ms last_size=${CKPT_SIZE}B"

# Resources (CPU / Memory)
RES_AVG_CPU=$(prom_query "avg_over_time(sum(rate(container_cpu_usage_seconds_total{namespace=\"${FLINK_NS}\", pod=~\"flink-taskmanager.*\", container=~\".+\", container!=\"POD\"}[1m]))[${PROM_WINDOW}:])")
RES_MAX_CPU=$(prom_query "max_over_time(sum(rate(container_cpu_usage_seconds_total{namespace=\"${FLINK_NS}\", pod=~\"flink-taskmanager.*\", container=~\".+\", container!=\"POD\"}[1m]))[${PROM_WINDOW}:])")
RES_AVG_MEM=$(prom_query "avg_over_time(sum(container_memory_working_set_bytes{namespace=\"${FLINK_NS}\", pod=~\"flink-taskmanager.*\", container=~\".+\", container!=\"POD\"})[${PROM_WINDOW}:])")
RES_MAX_MEM=$(prom_query "max_over_time(sum(container_memory_working_set_bytes{namespace=\"${FLINK_NS}\", pod=~\"flink-taskmanager.*\", container=~\".+\", container!=\"POD\"})[${PROM_WINDOW}:])")

echo "  Resources: avg_cpu=${RES_AVG_CPU} max_cpu=${RES_MAX_CPU} avg_mem=${RES_AVG_MEM} max_mem=${RES_MAX_MEM}"

# S3 checkpoint size
S3_CKPT_PATH="s3://${BENCHMARK_FLINK_S3_BUCKET}/${BENCHMARK_FLINK_S3_BUCKET_FOLDER}/${JOB_ID}/"
S3_SIZE_BYTES=$(aws s3 ls --recursive "${S3_CKPT_PATH}" 2>/dev/null | awk '{s+=$3} END {print s+0}')
echo "  S3 checkpoint size: ${S3_SIZE_BYTES} bytes"

###############################################################################
# 7. Generate JSON report
###############################################################################
echo "=== Generating report ==="

jq -n \
  --arg query "${QUERY}" \
  --arg job_name "${JOB_NAME}" \
  --arg job_id "${JOB_ID}" \
  --argjson parallelism "${PARALLELISM}" \
  --argjson tm_replicas "${BENCHMARK_FLINK_TASKMANAGER_REPLICAS}" \
  --argjson tm_slots "${BENCHMARK_FLINK_TASKMANAGER_SLOTS}" \
  --arg tm_memory "${BENCHMARK_FLINK_TASKMANAGER_MEMORY}" \
  --arg jm_memory "${BENCHMARK_FLINK_JOBMANAGER_MEMORY}" \
  --arg checkpoint_interval "${BENCHMARK_FLINK_CHECKPOINT_INTERVAL}" \
  --argjson max_events "${BENCHMARK_NEXMARK_KAFKA_MAX_EVENTS}" \
  --argjson event_rate "${BENCHMARK_NEXMARK_KAFKA_EVENT_RATE}" \
  --arg timestamp "${TIMESTAMP}" \
  --argjson total_time_seconds "${TOTAL_TIME_SEC}" \
  --argjson total_messages "${TOTAL_MESSAGES}" \
  --arg throughput_avg "${THROUGHPUT_AVG}" \
  --arg throughput_max "${THROUGHPUT_MAX}" \
  --arg throughput_out_avg "${THROUGHPUT_OUT_AVG}" \
  --arg throughput_calc "${THROUGHPUT_CALC}" \
  --arg latency_p50 "${LATENCY_P50}" \
  --arg latency_p95 "${LATENCY_P95}" \
  --arg latency_p99 "${LATENCY_P99}" \
  --arg ckpt_completed "${CKPT_COMPLETED}" \
  --arg ckpt_failed "${CKPT_FAILED}" \
  --arg ckpt_duration "${CKPT_DURATION}" \
  --arg ckpt_size "${CKPT_SIZE}" \
  --arg res_avg_cpu "${RES_AVG_CPU}" \
  --arg res_max_cpu "${RES_MAX_CPU}" \
  --arg res_avg_mem "${RES_AVG_MEM}" \
  --arg res_max_mem "${RES_MAX_MEM}" \
  --argjson s3_size_bytes "${S3_SIZE_BYTES}" \
  '{
    metadata: {
      query: $query,
      job_name: $job_name,
      job_id: $job_id,
      parallelism: $parallelism,
      taskmanager_replicas: $tm_replicas,
      taskmanager_slots: $tm_slots,
      taskmanager_memory: $tm_memory,
      jobmanager_memory: $jm_memory,
      checkpoint_interval: $checkpoint_interval,
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
      avg_records_out_per_sec: $throughput_out_avg,
      calc_records_per_sec: $throughput_calc
    },
    latency: {
      type: "checkpoint_latency",
      description: "checkpoint duration (ms) — comparable to RisingWave barrier latency",
      p50_ms: $latency_p50,
      p95_ms: $latency_p95,
      p99_ms: $latency_p99
    },
    checkpoints: {
      completed: $ckpt_completed,
      failed: $ckpt_failed,
      last_duration_ms: $ckpt_duration,
      last_size_bytes: $ckpt_size
    },
    resources: {
      avg_cpu_cores: $res_avg_cpu,
      max_cpu_cores: $res_max_cpu,
      avg_memory_bytes: $res_avg_mem,
      max_memory_bytes: $res_max_mem
    },
    storage: {
      s3_checkpoint_bytes: $s3_size_bytes
    }
  }' > "${REPORT_FILE}"

echo "  Report: ${REPORT_FILE}"

###############################################################################
# 8. Append to summary JSONL
###############################################################################
jq -c . "${REPORT_FILE}" >> "${SUMMARY_FILE}"
echo "  Summary: ${SUMMARY_FILE}"

###############################################################################
# 9. Print human-readable summary (matching NEXMark report format)
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
c = r['checkpoints']
res = r['resources']
s = r['storage']

throughput_krs = float(t['avg_records_per_sec']) / 1000
throughput_max_krs = float(t['max_records_per_sec']) / 1000
throughput_calc_krs = float(t.get('calc_records_per_sec', 0)) / 1000
cpu_pct = float(res['avg_cpu_cores']) * 100
max_cpu_pct = float(res['max_cpu_cores']) * 100
mem_gib = float(res['avg_memory_bytes']) / (1024**3)
max_mem_gib = float(res['max_memory_bytes']) / (1024**3)
duration_min = p['total_time_seconds'] / 60
events_million = p['total_messages'] / 1_000_000
s3_bytes = s['s3_checkpoint_bytes']

if s3_bytes > 1024**3:
    s3_str = f'{s3_bytes / (1024**3):.1f} GiB'
elif s3_bytes > 1024**2:
    s3_str = f'{s3_bytes / (1024**2):.1f} MiB'
elif s3_bytes > 0:
    s3_str = f'{s3_bytes / 1024:.1f} KiB'
else:
    s3_str = 'NA'

print(f'Query:              {m[\"query\"]}')
print(f'Throughput (avg):   {throughput_krs:.2f} kr/s (source operator rate)')
print(f'Throughput (max):   {throughput_max_krs:.2f} kr/s')
print(f'Throughput (calc):  {throughput_calc_krs:.2f} kr/s (wall-clock: events/time)')
print(f'CPU (avg/max):      {cpu_pct:.1f}% / {max_cpu_pct:.1f}%')
print(f'Memory (avg/max):   {mem_gib:.2f} GiB / {max_mem_gib:.2f} GiB')
print(f'Duration:           {duration_min:.0f} min')
print(f'Kafka events:       {events_million:.0f} M')
print(f'Ckpt latency p50/p95/p99: {float(l[\"p50_ms\"]):.1f} / {float(l[\"p95_ms\"]):.1f} / {float(l[\"p99_ms\"]):.1f} ms')
print(f'Checkpoints:        {c[\"completed\"]} completed, {c[\"failed\"]} failed')
print(f'S3 Size:            {s3_str}')
print(f'Parallelism:        {m[\"parallelism\"]}')
print(f'TM Memory:          {m[\"taskmanager_memory\"]}')
"
echo ""
echo "  Full report: ${REPORT_FILE}"
