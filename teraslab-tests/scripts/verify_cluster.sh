#!/usr/bin/env bash
set -euo pipefail

# Verify cluster health: all nodes agree on cluster size, shard table
# version, and total master shards sum to 4096.
# Usage: verify_cluster.sh [expected_size]

EXPECTED_SIZE=${1:-3}

echo "=== Cluster Verification ==="
total_masters=0
versions=()

for i in $(seq 0 $((EXPECTED_SIZE - 1))); do
    port=$((19100 + i))
    status=$(curl -sf "http://localhost:$port/status")
    nid=$(echo "$status" | jq -r '.node_id')
    cs=$(echo "$status" | jq -r '.cluster_size')
    sv=$(echo "$status" | jq -r '.shard_table_version')
    ms=$(echo "$status" | jq -r '.master_shard_count')
    rs=$(echo "$status" | jq -r '.replica_shard_count')
    am=$(echo "$status" | jq -r '.active_migrations')
    echo "  Node $nid: cluster=$cs masters=$ms replicas=$rs migrations=$am (v$sv)"
    total_masters=$((total_masters + ms))
    versions+=("$sv")

    # Check cluster size matches expected
    if [ "$cs" -ne "$EXPECTED_SIZE" ]; then
        echo "FAIL: node $nid reports cluster_size=$cs, expected $EXPECTED_SIZE"
        exit 1
    fi
done

# Check all versions agree
unique=$(printf '%s\n' "${versions[@]}" | sort -u | wc -l | tr -d ' ')
if [ "$unique" -ne 1 ]; then
    echo "FAIL: shard table versions disagree: ${versions[*]}"
    exit 1
fi

# Check total masters = 4096
if [ "$total_masters" -ne 4096 ]; then
    echo "FAIL: total masters=$total_masters != 4096"
    exit 1
fi

echo "=== OK ==="
