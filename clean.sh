#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load env vars from TOML
set -a
source <("${SCRIPT_DIR}/tomlenv/bin/tomlenv" "${SCRIPT_DIR}/toml/env.toml")
set +a

FLINK_NS="${BENCHMARK_FLINK_NAMESPACE}"

echo "=== Cancelling running Flink jobs (ns: ${FLINK_NS}) ==="

# Get running job IDs from Flink REST API
RUNNING_JOBS=$(kubectl exec -n "${FLINK_NS}" deploy/flink-jobmanager -- \
  curl -s localhost:8081/jobs 2>/dev/null | \
  python3 -c "
import sys, json
data = json.load(sys.stdin)
for job in data.get('jobs', []):
    if job['status'] == 'RUNNING':
        print(job['id'])
" 2>/dev/null || true)

if [ -n "${RUNNING_JOBS}" ]; then
  while IFS= read -r job_id; do
    echo "  Cancelling Flink job: ${job_id}"
    kubectl exec -n "${FLINK_NS}" deploy/flink-jobmanager -- \
      curl -s -X PATCH "localhost:8081/jobs/${job_id}?mode=cancel" > /dev/null 2>&1 || true
  done <<< "${RUNNING_JOBS}"
  echo "  Waiting for jobs to cancel..."
  sleep 5
else
  echo "  No running Flink jobs found."
fi

# Delete the start K8s Job
echo "=== Deleting start job ==="
kubectl delete job "${BENCHMARK_JOB_NAME}" -n "${FLINK_NS}" --ignore-not-found

# Delete Kafka topic to clear accumulated data
echo "=== Deleting Kafka topic: nexmark-events ==="
kubectl exec -n "${BENCHMARK_KAFKA_NAMESPACE}" "${BENCHMARK_KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-topics.sh --delete \
  --bootstrap-server localhost:9092 \
  --topic nexmark-events 2>/dev/null || true

# Delete the prepare K8s Job so prepare.sh can run again cleanly
echo "=== Deleting prepare job ==="
kubectl delete job "${BENCHMARK_JOB_NAME}-prepare" -n "${BENCHMARK_NAMESPACE}" --ignore-not-found

echo "=== Clean done ==="
