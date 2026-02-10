#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load env vars from TOML
set -a
source <("${SCRIPT_DIR}/tomlenv/bin/tomlenv" "${SCRIPT_DIR}/toml/env.toml")
export TRICK_SYMBOLS_EMPTY=""
set +a

echo "=== Preparing Nexmark data (Job: ${BENCHMARK_JOB_NAME}-prepare, ns: ${BENCHMARK_NAMESPACE}) ==="

# Delete old prepare job if exists (K8s doesn't allow re-creating a Job with the same name)
kubectl delete job "${BENCHMARK_JOB_NAME}-prepare" -n "${BENCHMARK_NAMESPACE}" --ignore-not-found

# Apply prepare job
envsubst < "${SCRIPT_DIR}/manifests/nexmark-kafka/prepare.template.yaml" | kubectl apply -f -

echo "=== Waiting for prepare job to complete... ==="
kubectl wait --for=condition=complete "job/${BENCHMARK_JOB_NAME}-prepare" -n "${BENCHMARK_NAMESPACE}" --timeout=600s

echo "=== Prepare done ==="
