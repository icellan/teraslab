#!/usr/bin/env bash
# Run all release-tier scenarios with TERASLAB_TEST_TIMING=1
# and capture per-test pass/fail + timing output.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RESULTS_DIR="${1:-results/timing_run}"
mkdir -p "$RESULTS_DIR"

SCENARIOS=(01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17)

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
        01) echo 120 ;;  10) echo 900 ;;  08|15) echo 600 ;;
        16) echo 600 ;;  17) echo 600 ;;  *) echo 300 ;;
    esac
}

echo "============================================"
echo "TeraSlab Docker Cluster Test Suite (TIMING)"
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
SUMMARY_FILE="$RESULTS_DIR/SUMMARY.txt"
echo "Scenario | Status | Duration" > "$SUMMARY_FILE"

for NUM in "${SCENARIOS[@]}"; do
    NAME="$(scenario_name "$NUM")"
    TIMEOUT="$(scenario_timeout "$NUM")"

    TEST_NAME=$(ls client/tests/scenario_${NUM}_*.rs 2>/dev/null | head -1 | sed 's|.*/||;s|\.rs$||' || echo "")
    if [ -z "$TEST_NAME" ]; then
        echo "SKIP $NUM: $NAME (test file not found)"
        echo "$NUM $NAME | SKIP | -" >> "$SUMMARY_FILE"
        continue
    fi

    echo ""
    echo "--- [$NUM] $NAME (timeout ${TIMEOUT}s) ---"
    START_TS=$(date +%s)

    TIMEOUT_CMD="timeout"
    command -v timeout >/dev/null 2>&1 || TIMEOUT_CMD="gtimeout"

    LOG="$RESULTS_DIR/${TEST_NAME}.log"
    STATUS="PASS"
    TERASLAB_TEST_TIMING=1 $TIMEOUT_CMD "$TIMEOUT" cargo test \
        --manifest-path client/Cargo.toml \
        --release --test "$TEST_NAME" -- --nocapture > "$LOG" 2>&1 \
        && STATUS="PASS" || STATUS="FAIL"

    END_TS=$(date +%s)
    DURATION=$((END_TS - START_TS))

    if [ "$STATUS" = "PASS" ]; then
        echo "  PASS (${DURATION}s)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL (${DURATION}s) — see $LOG"
        FAIL=$((FAIL + 1))
        # Collect diagnostic logs on failure
        if [ -f ./scripts/collect_logs.sh ]; then
            ./scripts/collect_logs.sh "$RESULTS_DIR/${TEST_NAME}_diag" 2>/dev/null || true
        fi
    fi

    echo "$NUM | $NAME | $STATUS | ${DURATION}s" >> "$SUMMARY_FILE"

    # Clean up Docker resources for this scenario
    docker ps -aq --filter "name=ts${NUM}-node" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
    for cf in docker/docker-compose.3node.yml docker/docker-compose.5node.yml docker/docker-compose.ts${NUM}.yml; do
        [ -f "$cf" ] && docker compose -f "$cf" down -v 2>/dev/null || true
    done
done

echo ""
echo "============================================"
echo "RESULTS: $PASS passed, $FAIL failed"
echo "============================================"
cat "$SUMMARY_FILE"
[ "$FAIL" -eq 0 ] || exit 1
