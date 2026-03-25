#!/usr/bin/env bash
#
# Start a 3-node TeraSlab cluster with replication factor 2 in Docker.
#
# Uses host networking so SWIM UDP probes work reliably on Docker Desktop
# for macOS (Docker's bridge network can silently drop UDP between containers).
#
# Usage:
#   ./scripts/start-cluster.sh                          # use ghcr.io/icellan/teraslab:latest
#   TERASLAB_IMAGE=teraslab-server:latest ./scripts/start-cluster.sh  # use local image
#
# Host ports per node:
#   Node 1: wire 3300, HTTP 9100, SWIM 3301
#   Node 2: wire 3310, HTTP 9110, SWIM 3311
#   Node 3: wire 3320, HTTP 9120, SWIM 3321
#
# Data is persisted in Docker volumes (teraslab-node1-data, etc.).
# Stop with:   ./scripts/start-cluster.sh stop

set -euo pipefail

IMAGE="${TERASLAB_IMAGE:-ghcr.io/icellan/teraslab:latest}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/cluster"

stop_cluster() {
    echo "Stopping cluster..."
    docker rm -f teraslab-node1 teraslab-node2 teraslab-node3 2>/dev/null || true
    echo "All nodes stopped."
}

if [ "${1:-}" = "stop" ]; then
    stop_cluster
    exit 0
fi

# Clean up any previous run
stop_cluster

start_node() {
    local id=$1
    local name="teraslab-node$id"
    local config="$CONFIG_DIR/node$id.toml"

    if [ ! -f "$config" ]; then
        echo "ERROR: config file not found: $config"
        exit 1
    fi

    local wire_port http_port swim_port
    wire_port=$(grep listen_addr "$config" | head -1 | grep -oE '[0-9]+' | tail -1)
    http_port=$(grep http_listen_addr "$config" | grep -oE '[0-9]+' | tail -1)
    swim_port=$(grep swim_port "$config" | grep -oE '[0-9]+')

    echo "  Node $id ($name): wire=:$wire_port  http=:$http_port  swim=:$swim_port"

    docker run -d \
        --name "$name" \
        --network host \
        -v "$name-data:/data" \
        -v "$name-blobstore:/blobstore" \
        -v "$config:/etc/teraslab/node.toml:ro" \
        "$IMAGE" \
        > /dev/null
}

echo "Starting 3-node TeraSlab cluster (replication_factor=2, host networking)..."
echo "  Image: $IMAGE"
echo ""

start_node 1
start_node 2
start_node 3

echo ""
echo "Waiting for health checks..."

for port in 9100 9110 9120; do
    for i in $(seq 1 30); do
        if curl -sf "http://127.0.0.1:$port/health/live" > /dev/null 2>&1; then
            echo "  Node on :$port is healthy"
            break
        fi
        if [ "$i" -eq 30 ]; then
            echo "  WARNING: Node on :$port did not become healthy within 30s"
        fi
        sleep 1
    done
done

# Wait for cluster formation
echo ""
echo "Waiting for cluster formation..."
for i in $(seq 1 15); do
    size=$(curl -sf http://127.0.0.1:9100/status 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('cluster_size',0))" 2>/dev/null || echo 0)
    if [ "$size" -ge 3 ]; then
        echo "  Cluster formed: $size nodes"
        break
    fi
    if [ "$i" -eq 15 ]; then
        echo "  WARNING: cluster_size=$size after 15s (expected 3)"
    fi
    sleep 1
done

echo ""
echo "Cluster ready."
echo "  Connect to any node: localhost:3300, localhost:3310, localhost:3320"
echo "  Health endpoints:    localhost:9100, localhost:9110, localhost:9120"
echo "  Web UI:              http://localhost:9100/ui/"
echo ""
echo "Stop with: ./scripts/start-cluster.sh stop"
