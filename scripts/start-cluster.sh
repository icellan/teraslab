#!/usr/bin/env bash
#
# Start a 3-node TeraSlab cluster with replication factor 2.
#
# Usage:
#   ./scripts/start-cluster.sh              # defaults: data in ./data/cluster/
#   ./scripts/start-cluster.sh /tmp/ts      # custom data directory
#
# Ports per node:
#   Node 1: wire 3300, SWIM 3301, HTTP 9100
#   Node 2: wire 3310, SWIM 3311, HTTP 9110
#   Node 3: wire 3320, SWIM 3321, HTTP 9120
#
# Stop with Ctrl-C (sends SIGINT to all nodes).
# Data is preserved between restarts.

set -euo pipefail

DATA_DIR="${1:-./data/cluster}"

PIDS=()

cleanup() {
    echo ""
    echo "Stopping cluster..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null
    echo "All nodes stopped."
}
trap cleanup EXIT INT TERM

start_node() {
    local id=$1
    local wire_port=$2
    local swim_port=$3
    local http_port=$4
    local dir="$DATA_DIR/node$id"

    mkdir -p "$dir/blobstore"

    local config="$dir/node.toml"
    cat > "$config" <<EOF
listen_addr          = "127.0.0.1:$wire_port"
http_listen_addr     = "127.0.0.1:$http_port"
device_paths         = ["$dir/teraslab.dat"]
device_size          = 1073741824  # 1 GiB
index_snapshot_path  = "$dir/index.snap"
blobstore_path       = "$dir/blobstore"

node_id              = $id
swim_port            = $swim_port
seed_nodes           = ["127.0.0.1:3301", "127.0.0.1:3311", "127.0.0.1:3321"]
replication_factor   = 2
cluster_secret       = "dev-cluster-secret"
EOF

    echo "  Node $id: wire=:$wire_port  swim=:$swim_port  http=:$http_port  dir=$dir"
    cargo run --release --bin teraslab-server -- --config "$config" &
    PIDS+=($!)
}

echo "Starting 3-node TeraSlab cluster (replication_factor=2)..."
echo ""

start_node 1 3300 3301 9100
start_node 2 3310 3311 9110
start_node 3 3320 3321 9120

echo ""
echo "Cluster starting. Waiting for health checks..."

# Wait for all nodes to be healthy
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
echo "Press Ctrl-C to stop all nodes."

wait
