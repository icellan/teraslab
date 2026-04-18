#!/usr/bin/env bash
# Note: deliberately no `set -e` — a flaky diagnostic helper must never
# abort the 17-scenario run. Individual commands use explicit `|| true`.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RESULTS_DIR="results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

# Parse arguments
TIER="release"
SINGLE=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --tier) TIER="$2"; shift 2 ;;
        --scenario) SINGLE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# Select scenarios based on tier
case "$TIER" in
    pr)      SCENARIOS=(01 02 03) ;;
    nightly) SCENARIOS=(01 02 03 04 05 06 07 08 09 10 11 17) ;;
    weekly)  SCENARIOS=(01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 17) ;;
    release) SCENARIOS=(01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17) ;;
    *) echo "Unknown tier: $TIER (use pr, nightly, weekly, release)"; exit 1 ;;
esac

# Override with single scenario if specified
if [ -n "$SINGLE" ]; then
    SCENARIOS=("$SINGLE")
fi

# Scenario name and timeout lookup (avoids bash octal issues with 08/09 keys).
scenario_name() {
    case "$1" in
        01) echo "Cluster Formation" ;;      02) echo "Basic Operations" ;;
        03) echo "Replication Correctness" ;; 04) echo "Node Hard Kill" ;;
        05) echo "Node Recovery" ;;           06) echo "Scale Up" ;;
        07) echo "Scale Down" ;;              08) echo "Network Partitions" ;;
        09) echo "Rolling Restart" ;;         10) echo "Sustained Load" ;;
        11) echo "Large Transactions" ;;      12) echo "Concurrent Failures" ;;
        13) echo "Migration Under Load" ;;    14) echo "Split-Brain Prevention" ;;
        15) echo "Crash Recovery" ;;          16) echo "CHAOS" ;;
        17) echo "Failure Recovery Hardening" ;; *) echo "Scenario $1" ;;
    esac
}
scenario_timeout() {
    case "$1" in
        01) echo 120 ;;  10) echo 900 ;;  14|15) echo 1200 ;;
        08|16) echo 900 ;;  17) echo 600 ;;  *) echo 300 ;;
    esac
}

echo "============================================"
echo "TeraSlab Docker Cluster Test Suite"
echo "Tier: $TIER"
echo "Results: $RESULTS_DIR"
echo "============================================"

# Build Docker image
echo ""
echo "Building Docker image..."
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
docker build -t teraslab:test -f "$SCRIPT_DIR/docker/Dockerfile" "$PROJECT_ROOT" 2>&1 | tail -5

# Build test client
echo ""
echo "Building test client..."
cd client && cargo build --release 2>&1 | tail -5 && cd ..

PASS=0
FAIL=0

for NUM in "${SCENARIOS[@]}"; do
    NAME="$(scenario_name "$NUM")"
    TIMEOUT="$(scenario_timeout "$NUM")"
    TEST="scenario_${NUM}_*"

    echo ""
    echo "--- $NAME (scenario $NUM, timeout ${TIMEOUT}s) ---"

    # Find the exact test binary name
    TEST_NAME=$(ls client/tests/scenario_${NUM}_*.rs 2>/dev/null | head -1 | sed 's|.*/||;s|\.rs$||' || echo "")
    if [ -z "$TEST_NAME" ]; then
        echo "  SKIP (test file not found)"
        continue
    fi

    TIMEOUT_CMD="timeout"
    command -v timeout >/dev/null 2>&1 || TIMEOUT_CMD="gtimeout"
    if $TIMEOUT_CMD "$TIMEOUT" cargo test --manifest-path client/Cargo.toml \
        --release --test "$TEST_NAME" -- --nocapture > "$RESULTS_DIR/${TEST_NAME}.log" 2>&1; then
        echo "  PASS"
        PASS=$((PASS + 1))
    else
        echo "  FAIL (see $RESULTS_DIR/${TEST_NAME}.log)"
        FAIL=$((FAIL + 1))
        # collect_logs must never block the loop — timeout+ignore failures.
        $TIMEOUT_CMD 30 ./scripts/collect_logs.sh \
            "$RESULTS_DIR/${TEST_NAME}_diag" 2>&1 \
            | sed 's/^/  diag: /' || true
    fi

    # Clean up THIS scenario's Docker resources after the test completes.
    # Use -p to match the project name used by the test code (ts{NN}).
    SID="ts${NUM}"
    compose_file="docker/docker-compose.${SID}.yml"
    if [ -f "$compose_file" ]; then
        docker compose -p "$SID" -f "$compose_file" down -v --remove-orphans 2>/dev/null || true
    fi
    # Force-remove any remaining containers for this scenario.
    docker ps -aq --filter "name=${SID}-node" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
    # Remove volumes and networks that might linger.
    docker volume ls -q --filter "name=${SID}" 2>/dev/null | xargs -r docker volume rm -f 2>/dev/null || true
    docker network ls -q --filter "name=${SID}" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
    # Also clean the "wrong config" scenario (99) if it exists.
    docker ps -aq --filter "name=ts99-node" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
    docker volume ls -q --filter "name=ts99" 2>/dev/null | xargs -r docker volume rm -f 2>/dev/null || true
    docker network ls -q --filter "name=ts99" 2>/dev/null | xargs -r docker network rm 2>/dev/null || true
    # Brief cooldown to let Docker Desktop stabilize port forwarding.
    sleep 2
done

echo ""
echo "============================================"
echo "RESULTS: $PASS passed, $FAIL failed"
echo "============================================"
[ "$FAIL" -eq 0 ] || exit 1
