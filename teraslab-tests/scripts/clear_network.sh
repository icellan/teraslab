#!/usr/bin/env bash
set -euo pipefail

# Remove tc netem rules from a node (or all nodes).
# Usage: clear_network.sh [node_num|all]

TARGET=${1:-all}

if [ "$TARGET" = "all" ]; then
    for n in 1 2 3 4 5; do
        docker exec "teraslab-node${n}" tc qdisc del dev eth0 root 2>/dev/null || true
    done
    echo "All network effects cleared"
else
    docker exec "teraslab-node${TARGET}" tc qdisc del dev eth0 root 2>/dev/null || true
    echo "Node $TARGET network effects cleared"
fi
