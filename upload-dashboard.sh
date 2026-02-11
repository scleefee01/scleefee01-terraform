#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARD_FILE="${SCRIPT_DIR}/manifests/grafana/flink-benchmark-dashboard.json"
LOCAL_PORT="${1:-3000}"
GRAFANA_NS="monitoring"
GRAFANA_SVC="kps-grafana"

# Get Grafana admin password from K8s secret
GRAFANA_PASS=$(kubectl get secret -n "${GRAFANA_NS}" "${GRAFANA_SVC}" \
  -o jsonpath='{.data.admin-password}' | base64 -d)

echo "=== Starting port-forward to Grafana (${GRAFANA_NS}/${GRAFANA_SVC}:80 -> localhost:${LOCAL_PORT}) ==="
kubectl port-forward -n "${GRAFANA_NS}" "svc/${GRAFANA_SVC}" "${LOCAL_PORT}:80" >/dev/null 2>&1 &
PF_PID=$!

# Wait for port-forward to be ready
for i in $(seq 1 15); do
  if curl -s "http://localhost:${LOCAL_PORT}/api/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -s "http://localhost:${LOCAL_PORT}/api/health" >/dev/null 2>&1; then
  echo "ERROR: port-forward failed to become ready"
  kill "${PF_PID}" 2>/dev/null || true
  exit 1
fi

echo "=== Uploading dashboard ==="
PAYLOAD=$(jq -n --slurpfile db "${DASHBOARD_FILE}" '{
  dashboard: $db[0],
  overwrite: true,
  folderId: 0
} | .dashboard.id = null | .dashboard.version = 1')

RESPONSE=$(curl -s -w "\n%{http_code}" \
  -u "admin:${GRAFANA_PASS}" \
  -H "Content-Type: application/json" \
  -X POST "http://localhost:${LOCAL_PORT}/api/dashboards/db" \
  -d "${PAYLOAD}")

HTTP_CODE=$(echo "${RESPONSE}" | tail -1)
BODY=$(echo "${RESPONSE}" | sed '$d')

kill "${PF_PID}" 2>/dev/null || true
wait "${PF_PID}" 2>/dev/null || true

if [[ "${HTTP_CODE}" == "200" ]]; then
  echo "=== Dashboard uploaded successfully ==="
  echo "${BODY}" | jq .
else
  echo "ERROR: Upload failed (HTTP ${HTTP_CODE})"
  echo "${BODY}"
  exit 1
fi
