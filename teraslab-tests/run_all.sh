#!/usr/bin/env bash
set -e

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

# Scenario names for logging
declare -A NAMES=(
    [01]="Cluster Formation"
    [02]="Basic Operations"
    [03]="Replication Correctness"
    [04]="Node Hard Kill"
    [05]="Node Recovery"
    [06]="Scale Up"
    [07]="Scale Down"
    [08]="Network Partitions"
    [09]="Rolling Restart"
    [10]="Sustained Load"
    [11]="Large Transactions"
    [12]="Concurrent Failures"
    [13]="Migration Under Load"
    [14]="Split-Brain Prevention"
    [15]="Crash Recovery"
    [16]="CHAOS"
    [17]="Failure Recovery Hardening"
)

# Timeouts per scenario (seconds)
declare -A TIMEOUTS=(
    [01]=120 [02]=300 [03]=300 [04]=300 [05]=300
    [06]=300 [07]=300 [08]=600 [09]=300 [10]=900
    [11]=300 [12]=300 [13]=300 [14]=300 [15]=600
    [16]=2400 [17]=600
)

echo "============================================"
echo "TeraSlab Docker Cluster Test Suite"
echo "Tier: $TIER"
echo "Results: $RESULTS_DIR"
echo "============================================"

# Build Docker image
echo ""
echo "Building Docker image..."
docker build -t teraslab:test -f docker/Dockerfile ../.. 2>&1 | tail -5

# Build test client
echo ""
echo "Building test client..."
cd client && cargo build --release 2>&1 | tail -5 && cd ..

PASS=0
FAIL=0

for NUM in "${SCENARIOS[@]}"; do
    NAME="${NAMES[$NUM]:-Scenario $NUM}"
    TIMEOUT="${TIMEOUTS[$NUM]:-300}"
    TEST="scenario_${NUM}_*"

    echo ""
    echo "--- $NAME (scenario $NUM, timeout ${TIMEOUT}s) ---"

    # Find the exact test binary name
    TEST_NAME=$(ls client/tests/scenario_${NUM}_*.rs 2>/dev/null | head -1 | sed 's|.*/||;s|\.rs$||' || echo "")
    if [ -z "$TEST_NAME" ]; then
        echo "  SKIP (test file not found)"
        continue
    fi

    if timeout "$TIMEOUT" cargo test --manifest-path client/Cargo.toml \
        --release --test "$TEST_NAME" -- --nocapture > "$RESULTS_DIR/${TEST_NAME}.log" 2>&1; then
        echo "  PASS"
        PASS=$((PASS + 1))
    else
        echo "  FAIL (see $RESULTS_DIR/${TEST_NAME}.log)"
        FAIL=$((FAIL + 1))
        ./scripts/collect_logs.sh "$RESULTS_DIR/${TEST_NAME}_diag"
    fi

    # Clean up between scenarios
    docker compose -f docker/docker-compose.3node.yml down -v 2>/dev/null || true
    docker compose -f docker/docker-compose.3node.yml -f docker/docker-compose.5node.yml down -v 2>/dev/null || true
done

echo ""
echo "============================================"
echo "RESULTS: $PASS passed, $FAIL failed"
echo "============================================"
[ "$FAIL" -eq 0 ] || exit 1
