#!/usr/bin/env bash
set -euo pipefail

# Scrape prometheus metrics and status JSON from all nodes.
# Usage: collect_metrics.sh [output_dir]

OUTPUT_DIR=${1:-"results/metrics_$(date +%Y%m%d_%H%M%S)"}
mkdir -p "$OUTPUT_DIR"

for i in 1 2 3 4 5; do
    port=$((19100 + i - 1))
    curl -sf "http://localhost:$port/metrics" > "$OUTPUT_DIR/node${i}_metrics.txt" 2>/dev/null || true
    curl -sf "http://localhost:$port/status" > "$OUTPUT_DIR/node${i}_status.json" 2>/dev/null || true
    curl -sf "http://localhost:$port/debug/index" > "$OUTPUT_DIR/node${i}_index.json" 2>/dev/null || true
done

echo "Metrics collected in $OUTPUT_DIR"
