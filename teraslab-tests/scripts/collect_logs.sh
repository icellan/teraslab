#!/usr/bin/env bash
set -euo pipefail

# Gather logs and diagnostics from all TeraSlab containers.
# Usage: collect_logs.sh [output_dir]

OUTPUT_DIR=${1:-"results/logs_$(date +%Y%m%d_%H%M%S)"}
mkdir -p "$OUTPUT_DIR"

# Collect from all teraslab containers
for c in $(docker ps -a --filter "name=teraslab-" --format '{{.Names}}'); do
    docker logs "$c" > "$OUTPUT_DIR/${c}.log" 2>&1 || true
    docker inspect "$c" > "$OUTPUT_DIR/${c}.inspect.json" 2>&1 || true
done

# Collect final metrics snapshot
for i in 1 2 3 4 5; do
    port=$((19100 + i - 1))
    curl -sf "http://localhost:$port/metrics" > "$OUTPUT_DIR/node${i}_final_metrics.txt" 2>/dev/null || true
done

# Docker resource usage
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}" \
    $(docker ps --filter "name=teraslab-" -q 2>/dev/null) > "$OUTPUT_DIR/resource_usage.txt" 2>/dev/null || true

echo "Logs and diagnostics in $OUTPUT_DIR"
