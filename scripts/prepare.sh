#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load env vars from TOML
set -a
source <("${PROJECT_DIR}/tomlenv/bin/tomlenv" "${PROJECT_DIR}/toml/env.toml")
export TRICK_SYMBOLS_EMPTY=""
set +a

echo "=== Preparing Nexmark data (Job: ${BENCHMARK_JOB_NAME}-prepare, ns: ${BENCHMARK_NAMESPACE}) ==="

# Delete old prepare job if exists (K8s doesn't allow re-creating a Job with the same name)
kubectl delete job "${BENCHMARK_JOB_NAME}-prepare" -n "${BENCHMARK_NAMESPACE}" --ignore-not-found

# Apply prepare job
envsubst < "${PROJECT_DIR}/manifests/nexmark-kafka/prepare.template.yaml" | kubectl apply -f -

# Wait for pod to be scheduled and running
JOB_NAME="${BENCHMARK_JOB_NAME}-prepare"
NS="${BENCHMARK_NAMESPACE}"
echo "=== Waiting for pod to start... ==="
for i in $(seq 1 60); do
  POD=$(kubectl get pods -n "${NS}" -l job-name="${JOB_NAME}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "${POD}" ]]; then
    PHASE=$(kubectl get pod "${POD}" -n "${NS}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    # Show init container status
    INIT_STATUS=$(kubectl get pod "${POD}" -n "${NS}" -o jsonpath='{.status.initContainerStatuses[0].state}' 2>/dev/null || true)
    echo "  Pod: ${POD}  Phase: ${PHASE}  Init: ${INIT_STATUS}"
    if [[ "${PHASE}" == "Running" || "${PHASE}" == "Succeeded" || "${PHASE}" == "Failed" ]]; then
      break
    fi
  else
    echo "  Waiting for pod to be created... (${i}/60)"
  fi
  sleep 3
done

if [[ -z "${POD}" ]]; then
  echo "ERROR: Pod was not created within 180s"
  kubectl describe job "${JOB_NAME}" -n "${NS}"
  exit 1
fi

PHASE=$(kubectl get pod "${POD}" -n "${NS}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
if [[ "${PHASE}" == "Failed" ]]; then
  echo "ERROR: Pod failed during init"
  kubectl logs "${POD}" -n "${NS}" --all-containers --prefix
  exit 1
fi

# Stream logs in real-time (follows until container exits)
echo "=== Streaming nexmark-bench logs (live) ==="
kubectl logs "${POD}" -n "${NS}" -c nexmark-bench -f 2>/dev/null &
LOG_PID=$!

# Wait for job to complete or fail
if kubectl wait --for=condition=complete "job/${JOB_NAME}" -n "${NS}" --timeout=7200s 2>/dev/null; then
  wait "${LOG_PID}" 2>/dev/null || true
  echo ""
  echo "=== Job completed successfully ==="
else
  wait "${LOG_PID}" 2>/dev/null || true
  echo ""
  echo "=== Job FAILED ==="
  # Show failure reason
  REASON=$(kubectl get pod "${POD}" -n "${NS}" -o jsonpath='{.status.containerStatuses[0].state.terminated.reason}' 2>/dev/null || true)
  EXIT_CODE=$(kubectl get pod "${POD}" -n "${NS}" -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || true)
  echo "  Reason: ${REASON:-unknown}  ExitCode: ${EXIT_CODE:-unknown}"
  kubectl describe pod "${POD}" -n "${NS}" | tail -20
  exit 1
fi

KAFKA_BROKER="${BENCHMARK_KAFKA_NAME}-controller-0.${BENCHMARK_KAFKA_NAME}-controller-headless.${BENCHMARK_KAFKA_NAMESPACE}:9092"

echo "=== Topic info ==="
kubectl exec -n "${BENCHMARK_KAFKA_NAMESPACE}" "${BENCHMARK_KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic nexmark-events

echo "=== Partition offsets ==="
kubectl exec -n "${BENCHMARK_KAFKA_NAMESPACE}" "${BENCHMARK_KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic nexmark-events

TOTAL=$(kubectl exec -n "${BENCHMARK_KAFKA_NAMESPACE}" "${BENCHMARK_KAFKA_NAME}-controller-0" -- \
  /opt/bitnami/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic nexmark-events 2>/dev/null \
  | awk -F: '{s+=$3} END {print s}')
echo "=== Total messages: ${TOTAL} ==="
