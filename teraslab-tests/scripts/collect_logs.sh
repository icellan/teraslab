#!/usr/bin/env bash
set -uo pipefail

# Gather logs and diagnostics from all TeraSlab containers.
# Usage: collect_logs.sh [output_dir]
#
# Each docker command is bounded with a short timeout so a hung Docker daemon
# cannot stall the parent test run.

OUTPUT_DIR=${1:-"results/logs_$(date +%Y%m%d_%H%M%S)"}
mkdir -p "$OUTPUT_DIR"

TIMEOUT_CMD="timeout"
command -v timeout >/dev/null 2>&1 || TIMEOUT_CMD="gtimeout"

# Collect from scenario containers (ts{NN}-node{N}). Skip if docker is slow.
containers=$($TIMEOUT_CMD 10 docker ps -a --filter "name=ts" --format '{{.Names}}' 2>/dev/null || true)
for c in $containers; do
    $TIMEOUT_CMD 10 docker logs "$c" > "$OUTPUT_DIR/${c}.log" 2>&1 || true
    $TIMEOUT_CMD 10 docker inspect "$c" > "$OUTPUT_DIR/${c}.inspect.json" 2>&1 || true
done

# Collect final metrics snapshot
for i in 1 2 3 4 5; do
    port=$((19100 + i - 1))
    curl --max-time 3 -sf "http://localhost:$port/metrics" \
        > "$OUTPUT_DIR/node${i}_final_metrics.txt" 2>/dev/null || true
done

# Docker resource usage — only if we actually have scenario containers; an
# empty container list would cause `docker stats` to watch ALL containers.
scenario_ids=$($TIMEOUT_CMD 10 docker ps --filter "name=ts" -q 2>/dev/null || true)
if [ -n "$scenario_ids" ]; then
    $TIMEOUT_CMD 10 docker stats --no-stream \
        --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}" \
        $scenario_ids > "$OUTPUT_DIR/resource_usage.txt" 2>/dev/null || true
fi

echo "Logs and diagnostics in $OUTPUT_DIR"
