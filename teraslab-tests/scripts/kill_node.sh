#!/usr/bin/env bash
set -euo pipefail

# Kill a TeraSlab node container.
# Usage: kill_node.sh <node_num> [hard|graceful]

NODE=${1:?"Usage: kill_node.sh <node_num> [hard|graceful]"}
MODE=${2:-hard}
CONTAINER="teraslab-node${NODE}"

if [ "$MODE" = "hard" ]; then
    docker kill --signal=SIGKILL "$CONTAINER"
elif [ "$MODE" = "graceful" ]; then
    docker stop --time=10 "$CONTAINER"
else
    echo "ERROR: mode must be 'hard' or 'graceful', got '$MODE'"
    exit 1
fi

# Verify the container stopped
for i in $(seq 1 10); do
    if docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q false; then
        echo "Node $NODE killed ($MODE)"
        exit 0
    fi
    sleep 0.5
done
echo "ERROR: container $CONTAINER still running after kill"
exit 1
