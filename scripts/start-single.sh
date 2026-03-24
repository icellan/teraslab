#!/usr/bin/env bash
#
# Start a single TeraSlab node for local development.
#
# Usage:
#   ./scripts/start-single.sh              # defaults: data in ./data/single/
#   ./scripts/start-single.sh /tmp/ts      # custom data directory
#
# Ports:
#   3300  - wire protocol (client connections)
#   9100  - HTTP (health, metrics, status)
#
# Stop with Ctrl-C. Data is preserved between restarts.

set -euo pipefail

DATA_DIR="${1:-./data/single}"
mkdir -p "$DATA_DIR"

CONFIG="$DATA_DIR/node.toml"

cat > "$CONFIG" <<EOF
listen_addr      = "0.0.0.0:3300"
http_listen_addr = "0.0.0.0:9100"
device_paths     = ["$DATA_DIR/teraslab.dat"]
device_size      = 1073741824  # 1 GiB
index_snapshot_path = "$DATA_DIR/index.snap"
blobstore_path   = "$DATA_DIR/blobstore"
EOF

mkdir -p "$DATA_DIR/blobstore"

echo "Starting TeraSlab single node..."
echo "  Wire protocol: localhost:3300"
echo "  HTTP/health:   localhost:9100"
echo "  Data dir:      $DATA_DIR"
echo ""

exec cargo run --release --bin teraslab-server -- --config "$CONFIG"
