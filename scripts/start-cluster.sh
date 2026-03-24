#!/usr/bin/env bash
#
# Start a 3-node TeraSlab cluster with replication factor 2 in Docker.
#
# Usage:
#   ./scripts/start-cluster.sh                          # use ghcr.io/icellan/teraslab:latest
#   TERASLAB_IMAGE=teraslab-server:latest ./scripts/start-cluster.sh  # use local image
#
# Host ports per node:
#   Node 1: wire 3300, HTTP 9100
#   Node 2: wire 3310, HTTP 9110
#   Node 3: wire 3320, HTTP 9120
#
# Data is persisted in Docker volumes (teraslab-node1-data, etc.).
# Stop with:   ./scripts/start-cluster.sh stop
# Remove all:  docker rm -f teraslab-node1 teraslab-node2 teraslab-node3
#              docker volume rm teraslab-node{1,2,3}-data teraslab-node{1,2,3}-blobstore
#              docker network rm teraslab-cluster

set -euo pipefail

IMAGE="${TERASLAB_IMAGE:-ghcr.io/icellan/teraslab:latest}"
NETWORK="teraslab-cluster"

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

# Create network for inter-node communication
docker network create "$NETWORK" 2>/dev/null || true

start_node() {
    local id=$1
    local wire_port=$2
    local http_port=$3
    local name="teraslab-node$id"

    # Generate config with container-internal addresses
    local config
    config=$(cat <<EOF
listen_addr          = "0.0.0.0:3300"
http_listen_addr     = "0.0.0.0:9100"
device_paths         = ["/data/teraslab.dat"]
device_size          = 1073741824
index_snapshot_path  = "/data/index.snap"
blobstore_path       = "/blobstore"

node_id              = $id
swim_port            = 3301
seed_nodes           = ["teraslab-node1:3301", "teraslab-node2:3301", "teraslab-node3:3301"]
replication_factor   = 2
cluster_secret       = "dev-cluster-secret"
EOF
)

    echo "  Node $id ($name): wire=localhost:$wire_port  http=localhost:$http_port"

    docker run -d \
        --name "$name" \
        --network "$NETWORK" \
        -p "$wire_port:3300" \
        -p "$http_port:9100" \
        -v "$name-data:/data" \
        -v "$name-blobstore:/blobstore" \
        "$IMAGE" \
        sh -c "echo '$config' > /etc/teraslab/node.toml && exec teraslab-server --config /etc/teraslab/node.toml" \
        > /dev/null
}

echo "Starting 3-node TeraSlab cluster (replication_factor=2)..."
echo "  Image: $IMAGE"
echo ""

start_node 1 3300 9100
start_node 2 3310 9110
start_node 3 3320 9120

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

echo ""
echo "Cluster ready."
echo "  Connect to any node: localhost:3300, localhost:3310, localhost:3320"
echo "  Health endpoints:    localhost:9100, localhost:9110, localhost:9120"
echo ""
echo "Stop with: ./scripts/start-cluster.sh stop"
