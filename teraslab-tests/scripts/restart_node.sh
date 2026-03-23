#!/usr/bin/env bash
set -euo pipefail

# Restart a stopped TeraSlab node container.
# Usage: restart_node.sh <node_num>

NODE=${1:?"Usage: restart_node.sh <node_num>"}
CONTAINER="teraslab-node${NODE}"

docker start "$CONTAINER"

# Wait for the container to be running and healthy
for i in $(seq 1 30); do
    if docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q true; then
        echo "Node $NODE restarted"
        exit 0
    fi
    sleep 0.5
done
echo "ERROR: Node $NODE failed to restart within 15s"
exit 1
