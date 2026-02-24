#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load env vars from TOML
set -a
source <("${PROJECT_DIR}/tomlenv/bin/tomlenv" "${PROJECT_DIR}/toml/env.toml")
export TRICK_SYMBOLS_EMPTY=""
set +a

NS="${BENCHMARK_RISINGWAVE_NAMESPACE}"
RW_NAME="${BENCHMARK_RISINGWAVE_NAME}"
ETCD_NAME="${BENCHMARK_ETCD_NAME}"

echo "=== RisingWave Setup (ns: ${NS}) ==="

# --- Generate dynamic variables (like original benchmark::env::overrides) ---

# stateStore JSON for S3 with IRSA
export BENCHMARK_RISINGWAVE_STORAGE_GEN_OBJECTS=$(cat <<EOF | jq -c
{
  "dataDirectory": "${BENCHMARK_RISINGWAVE_STORAGE_S3_DATA_DIRECTORY}",
  "s3": {
    "region": "${BENCHMARK_RISINGWAVE_STORAGE_S3_REGION}",
    "bucket": "${BENCHMARK_RISINGWAVE_STORAGE_S3_BUCKET}",
    "credentials": {
      "useServiceAccount": true
    }
  }
}
EOF
)

# Pod distribution (no special affinity for benchmark)
export BENCHMARK_PODS_DISTRIBUTION_GEN_AFFINITY="null"
export BENCHMARK_PODS_DISTRIBUTION_GEN_AFFINITY_FRONTEND_META="null"
export BENCHMARK_PODS_DISTRIBUTION_GEN_AFFINITY_COMPUTE="null"

# Node envs
export BENCHMARK_RISINGWAVE_GEN_NODE_ENVS="{}"

# Compactor/Compute resources (use dedicated values from toml)
export BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_CPU_LIMIT="${BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_CPU_LIMIT}"
export BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_CPU_REQUEST="${BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_CPU_REQUEST}"
export BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_MEM_LIMIT="${BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_MEM_LIMIT}"
export BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_MEM_REQUEST="${BENCHMARK_RISINGWAVE_RESOURCES_COMPACTOR_MEM_REQUEST}"
export BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_CPU_LIMIT="${BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_CPU_LIMIT}"
export BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_CPU_REQUEST="${BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_CPU_REQUEST}"
export BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_MEM_LIMIT="${BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_MEM_LIMIT}"
export BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_MEM_REQUEST="${BENCHMARK_RISINGWAVE_RESOURCES_COMPUTE_MEM_REQUEST}"

# Use RisingWave namespace as BENCHMARK_NAMESPACE for templates
export BENCHMARK_NAMESPACE="${NS}"

# --- 1. Create namespace ---
echo "=== Creating namespace: ${NS} ==="
kubectl create ns "${NS}" --dry-run=client -o yaml | kubectl apply -f -

# --- 2. Create ServiceAccount (IRSA) ---
echo "=== Creating ServiceAccount with IRSA ==="
envsubst < "${PROJECT_DIR}/manifests/risingwave/serviceaccount.template.yaml" | kubectl apply -f -

# --- 3. Deploy etcd via Helm ---
echo "=== Deploying etcd: ${ETCD_NAME} ==="
envsubst < "${PROJECT_DIR}/manifests/etcd/values.template.yaml" > /tmp/etcd-values.yaml
helm upgrade --install --wait "${ETCD_NAME}" bitnami/etcd \
  -n "${NS}" \
  -f /tmp/etcd-values.yaml

# Wait for etcd
kubectl -n "${NS}" rollout status statefulset/"${ETCD_NAME}" --timeout=120s

# --- 4. Create RisingWave ConfigMap ---
echo "=== Creating RisingWave ConfigMap ==="
envsubst < "${PROJECT_DIR}/manifests/risingwave/config/risingwave.toml" > /tmp/risingwave-rendered.toml
kubectl create configmap "${RW_NAME}-config-template" \
  --from-file=risingwave.toml=/tmp/risingwave-rendered.toml \
  -n "${NS}" \
  --dry-run=client -o yaml | kubectl apply -f -

# --- 5. Apply RisingWave CR ---
echo "=== Applying RisingWave CR ==="
envsubst < "${PROJECT_DIR}/manifests/risingwave/risingwave.template.yaml" | kubectl apply -f -

# --- 6. Wait for RisingWave rollout ---
echo "=== Waiting for RisingWave to be ready... ==="
for i in $(seq 1 60); do
  PHASE=$(kubectl get risingwave "${RW_NAME}" -n "${NS}" -o jsonpath='{.status.conditions[?(@.type=="Running")].status}' 2>/dev/null || true)
  if [[ "${PHASE}" == "True" ]]; then
    echo "=== RisingWave is Running! ==="
    break
  fi
  echo "  Waiting... (${i}/60) phase=${PHASE:-pending}"
  sleep 5
done

# --- 7. Show status ---
echo ""
echo "=== Final Status ==="
kubectl get pods -n "${NS}"
echo ""
kubectl get risingwave -n "${NS}"
