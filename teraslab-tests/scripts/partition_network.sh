#!/usr/bin/env bash
set -euo pipefail

# Isolate a node from one or more other nodes via iptables.
# Rules are applied on BOTH sides for a clean bidirectional partition.
# Usage: partition_network.sh <node_num> <target_num> [target_num ...]
#        partition_network.sh <node_num> all

NODE=${1:?"Usage: partition_network.sh <node_num> <target|all> ..."}
shift
TARGETS="${*:-all}"

declare -A NODE_IPS=(
    [1]="172.30.0.11"
    [2]="172.30.0.12"
    [3]="172.30.0.13"
    [4]="172.30.0.14"
    [5]="172.30.0.15"
)

SOURCE_CONTAINER="teraslab-node${NODE}"

if [ "$TARGETS" = "all" ]; then
    target_list=""
    for target_num in "${!NODE_IPS[@]}"; do
        [ "$target_num" = "$NODE" ] && continue
        target_list="$target_list $target_num"
    done
else
    target_list="$TARGETS"
fi

for target_num in $target_list; do
    TARGET_IP="${NODE_IPS[$target_num]}"
    SOURCE_IP="${NODE_IPS[$NODE]}"
    TARGET_CONTAINER="teraslab-node${target_num}"

    # Block on the source node
    docker exec "$SOURCE_CONTAINER" iptables -A INPUT -s "$TARGET_IP" -j DROP 2>/dev/null || true
    docker exec "$SOURCE_CONTAINER" iptables -A OUTPUT -d "$TARGET_IP" -j DROP 2>/dev/null || true

    # Block on the target node (bidirectional)
    docker exec "$TARGET_CONTAINER" iptables -A INPUT -s "$SOURCE_IP" -j DROP 2>/dev/null || true
    docker exec "$TARGET_CONTAINER" iptables -A OUTPUT -d "$SOURCE_IP" -j DROP 2>/dev/null || true
done

echo "Node $NODE partitioned from: $target_list"
