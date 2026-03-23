#!/usr/bin/env bash
set -euo pipefail

# Add latency and/or packet loss to a node's network via tc netem.
# Usage: slow_network.sh <node_num> [latency_ms] [loss_percent] [jitter_ms]

NODE=${1:?"Usage: slow_network.sh <node_num> [latency_ms] [loss_pct] [jitter_ms]"}
LATENCY=${2:-200}
LOSS=${3:-0}
JITTER=${4:-0}
CONTAINER="teraslab-node${NODE}"

# Remove any existing netem (ignore errors)
docker exec "$CONTAINER" tc qdisc del dev eth0 root 2>/dev/null || true

# Build the tc command
CMD="tc qdisc add dev eth0 root netem delay ${LATENCY}ms"
if [ "$JITTER" -gt 0 ]; then
    CMD="$CMD ${JITTER}ms distribution normal"
fi
if [ "$(echo "$LOSS > 0" | bc -l 2>/dev/null || echo 0)" = "1" ] || [ "$LOSS" != "0" ]; then
    CMD="$CMD loss ${LOSS}%"
fi

docker exec "$CONTAINER" $CMD
echo "Node $NODE: +${LATENCY}ms latency, ${LOSS}% loss, ${JITTER}ms jitter"
