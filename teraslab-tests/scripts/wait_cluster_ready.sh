#!/usr/bin/env bash
set -euo pipefail

# Wait until all nodes report the expected cluster size via /status.
# Usage: wait_cluster_ready.sh [expected_size] [timeout_seconds]

EXPECTED_SIZE=${1:-3}
TIMEOUT=${2:-30}

echo "Waiting for $EXPECTED_SIZE-node cluster (timeout: ${TIMEOUT}s)..."
start=$(date +%s)

while true; do
    ready=0
    for i in $(seq 0 $((EXPECTED_SIZE - 1))); do
        port=$((19100 + i))
        if status=$(curl -sf "http://localhost:$port/status" 2>/dev/null); then
            size=$(echo "$status" | jq -r '.cluster_size // 0')
            if [ "$size" -eq "$EXPECTED_SIZE" ]; then
                ready=$((ready + 1))
            fi
        fi
    done
    if [ "$ready" -eq "$EXPECTED_SIZE" ]; then
        echo "OK: all $EXPECTED_SIZE nodes healthy and clustered"
        exit 0
    fi
    elapsed=$(( $(date +%s) - start ))
    if [ "$elapsed" -ge "$TIMEOUT" ]; then
        echo "FAIL: $ready/$EXPECTED_SIZE ready after ${TIMEOUT}s"
        exit 1
    fi
    sleep 0.5
done
