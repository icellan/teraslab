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

# Collect final metrics snapshot.
#
# The host HTTP port is NOT fixed: `DockerCluster::http_port` maps it to
# `19000 + scenario_id * 10 + (node_num - 1)`. This loop used to hardcode
# 19100..19104, which is scenario 10's range — so for every OTHER scenario the
# curl hit a closed port, and because the redirect creates the file before curl
# runs, each failure left a 0-byte `nodeN_final_metrics.txt` that looked like a
# successful collection. Every metrics file in every archived nightly run was
# empty.
#
# Ask Docker for the actual mapping instead of guessing, and only keep a file
# when the scrape really returned something — an absent file is honest, an empty
# one is not.
for c in $containers; do
    hostport=$($TIMEOUT_CMD 10 docker port "$c" 9100/tcp 2>/dev/null | head -1 | sed 's/.*://')
    if [ -z "$hostport" ]; then
        echo "  warning: no host mapping for $c 9100/tcp — metrics not collected"
        continue
    fi
    if ! curl --max-time 3 -sf "http://localhost:$hostport/metrics" \
        > "$OUTPUT_DIR/${c}_final_metrics.txt" 2>/dev/null; then
        rm -f "$OUTPUT_DIR/${c}_final_metrics.txt"
        echo "  warning: metrics scrape failed for $c on host port $hostport"
    fi
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
