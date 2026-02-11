#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load env vars from TOML
set -a
source <("${SCRIPT_DIR}/tomlenv/bin/tomlenv" "${SCRIPT_DIR}/toml/env.toml")
export TRICK_SYMBOLS_EMPTY=""
set +a

# Allow query override from argument
if [[ $# -ge 1 ]]; then
  BENCHMARK_NEXMARK_QUERIES="$1"
fi

echo "=== Starting Nexmark query: ${BENCHMARK_NEXMARK_QUERIES} (ns: ${BENCHMARK_FLINK_NAMESPACE}) ==="

# Apply ConfigMaps (idempotent)
envsubst < "${SCRIPT_DIR}/manifests/flink-nexmark/nexmark-kafka-sources.template.yaml" | kubectl apply -f -
envsubst < "${SCRIPT_DIR}/manifests/flink-nexmark/nexmark-queries.template.yaml" | kubectl apply -f -

# Delete old start job if exists (K8s doesn't allow re-creating a Job with the same name)
kubectl delete job "${BENCHMARK_JOB_NAME}" -n "${BENCHMARK_FLINK_NAMESPACE}" --ignore-not-found

# Apply start job
envsubst < "${SCRIPT_DIR}/manifests/nexmark-kafka/start.template.yaml" | kubectl apply -f -

echo "=== Job submitted ==="
echo "  Logs:  kubectl logs -f job/${BENCHMARK_JOB_NAME} -n ${BENCHMARK_FLINK_NAMESPACE}"
echo "  Flink: kubectl port-forward -n ${BENCHMARK_FLINK_NAMESPACE} svc/flink-jobmanager 8081:8081"
