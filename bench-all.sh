#!/bin/bash
set -euo pipefail

###############################################################################
# bench-all.sh — Run all NEXMark queries sequentially
#
# Usage: ./bench-all.sh              (run all queries)
#        nohup ./bench-all.sh &      (fire-and-forget overnight)
#
# Assumes Kafka topic nexmark-events is already populated (via prepare.sh).
# Each query reuses the same topic data.
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

QUERIES=(q0 q1 q2 q3 q4 q5 q7 q8 q9 q10 q11 q12 q14 q15 q16 q17 q18 q19 q20 q21 q22)

RESULTS_DIR="${SCRIPT_DIR}/results"
mkdir -p "${RESULTS_DIR}"
LOG_FILE="${RESULTS_DIR}/bench-all-$(date +%Y%m%d-%H%M%S).log"

# Tee all output to log file
exec > >(tee -a "${LOG_FILE}") 2>&1

echo "============================================================"
echo " NEXMark Benchmark - All Queries"
echo " Started: $(date)"
echo " Queries: ${QUERIES[*]}"
echo " Log:     ${LOG_FILE}"
echo "============================================================"
echo ""

PASSED=()
FAILED=()

for query in "${QUERIES[@]}"; do
  echo "============================================================"
  echo " [$(date +%H:%M:%S)] Starting: ${query}"
  echo "============================================================"

  if (
    set -e

    # 1. Submit Flink SQL job
    ./start.sh "${query}"

    # 2. Wait for job to initialize
    sleep 30

    # 3. Wait for lag=0 and collect metrics
    ./report.sh "${query}"
  ); then
    PASSED+=("${query}")
    echo "  >> ${query}: PASSED"
  else
    FAILED+=("${query}")
    echo "  >> ${query}: FAILED (see log for details)"
  fi

  # 4. Cancel Flink job, keep Kafka data for next query
  ./clean.sh --keep-topic || true

  # 5. Brief pause between queries
  sleep 10

  echo ""
done

echo "============================================================"
echo " NEXMark Benchmark - Complete"
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
        rows.append({
            'query': m['query'],
            'time_min': p['total_time_seconds'] / 60,
            'events_m': p['total_messages'] / 1_000_000,
            'tput_kr': float(t['avg_records_per_sec']) / 1000,
            'p50': float(l['p50_ms']),
            'p95': float(l['p95_ms']),
            'p99': float(l['p99_ms']),
        })

if not rows:
    print('  No results found.')
    sys.exit(0)

hdr = f'{\"Query\":<6} {\"Time(min)\":>9} {\"Events(M)\":>9} {\"Tput(kr/s)\":>10} {\"p50(ms)\":>8} {\"p95(ms)\":>8} {\"p99(ms)\":>8}'
print(hdr)
print('-' * len(hdr))
for r in rows:
    print(f'{r[\"query\"]:<6} {r[\"time_min\"]:>9.1f} {r[\"events_m\"]:>9.0f} {r[\"tput_kr\"]:>10.2f} {r[\"p50\"]:>8.1f} {r[\"p95\"]:>8.1f} {r[\"p99\"]:>8.1f}')
"
  echo ""
fi

echo "Log: ${LOG_FILE}"
