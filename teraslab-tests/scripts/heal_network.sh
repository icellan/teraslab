#!/usr/bin/env bash
set -euo pipefail

# Remove all iptables partition rules from a node (or all nodes).
# Usage: heal_network.sh [node_num|all]

TARGET=${1:-all}

if [ "$TARGET" = "all" ]; then
    for n in 1 2 3 4 5; do
        docker exec "teraslab-node${n}" iptables -F 2>/dev/null || true
    done
    echo "All network partitions healed"
else
    docker exec "teraslab-node${TARGET}" iptables -F
    echo "Node $TARGET network healed"
fi
