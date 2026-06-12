#!/usr/bin/env bash
# Note: deliberately no `set -e` — a flaky diagnostic helper must never
# abort the 17-scenario run. Individual commands use explicit `|| true`.
set -o pipefail

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
# Invariant: external = scenario-internal timeout + 60s headroom so the
# in-test teardown/diagnostics run before we SIGKILL.
scenario_timeout() {
    case "$1" in
        01) echo 300 ;;                       # internal 240
        02|03|04|06|07) echo 360 ;;           # internal 300
        05|09|11|12|13) echo 660 ;;           # internal 600
        08|14|16) echo 960 ;;                 # internal 900
        10) echo 240 ;;                       # internal 180 (60s load + 120s)
        15|17) echo 1260 ;;                   # internal 1200
        *) echo 360 ;;
    esac
}

# Timestamp-prefixing filter for scenario stdout/stderr. Each line coming
# through stdin is prefixed with [HH:MM:SS] so a wedged scenario is
# distinguishable from a slow-but-progressing one. Uses awk for portability
# (moreutils' `ts` is not installed by default on macOS).
if command -v ts >/dev/null 2>&1; then
    ts_filter() { ts '[%H:%M:%S]'; }
else
    ts_filter() { awk '{ cmd = "date +%H:%M:%S"; cmd | getline t; close(cmd); printf "[%s] %s\n", t, $0; fflush(); }'; }
fi

# Wait until no containers matching the scenario-container pattern remain
# and `docker ps` responds cleanly. Protects scenario 15 (next up) from
# starting on a Docker daemon still recovering from the previous scenario's
# mass tear-down. Bounded at 30 seconds.
wait_docker_ready() {
    local deadline=$(( $(date +%s) + 30 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if docker ps --format '{{.Names}}' >/dev/null 2>&1; then
            local lingering
            lingering="$(docker ps -aq --filter 'name=^ts[0-9]\{2\}-node' 2>/dev/null | wc -l | tr -d ' ')"
            if [ "${lingering:-0}" -eq 0 ]; then
                return 0
            fi
        fi
        sleep 1
    done
    echo "  warning: docker still has lingering ts*-node containers after 30s"
    return 0
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

# Prebuild ALL test binaries outside any scenario timeout window. Without
# this, the first scenario's `cargo test` pays the full compile cost inside
# its `timeout` budget and gets SIGKILLed on cold caches.
echo ""
echo "Prebuilding test binaries (untimed)..."
cargo test --manifest-path client/Cargo.toml --release --no-run 2>&1 | tail -5

PASS=0
FAIL=0
# Per-scenario results tracked for SUMMARY.md.
declare -a SUMMARY_ROWS=()

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
        SUMMARY_ROWS+=("| $NUM | $NAME | SKIP | 0s | _test file not found_ |")
        continue
    fi

    TIMEOUT_CMD="timeout"
    command -v timeout >/dev/null 2>&1 || TIMEOUT_CMD="gtimeout"
    SCENARIO_START=$(date +%s)
    # In-test failure diagnostics land here (collect_failure_diagnostics in
    # tests/common/mod.rs). The in-test teardown on the failure path removes
    # the containers before collect_logs.sh below can see them, so the test
    # itself must capture container logs/status first.
    export TERASLAB_DIAG_DIR="$SCRIPT_DIR/$RESULTS_DIR/${TEST_NAME}_intest"
    # Pipe stdout/stderr through the timestamp filter before redirecting
    # into the log so a wedged scenario can be distinguished from a
    # slow-but-progressing one during post-mortem review.
    if $TIMEOUT_CMD "$TIMEOUT" cargo test --manifest-path client/Cargo.toml \
        --release --test "$TEST_NAME" -- --nocapture 2>&1 \
        | ts_filter > "$RESULTS_DIR/${TEST_NAME}.log"; then
        STATUS="PASS"
        echo "  PASS"
        PASS=$((PASS + 1))
    else
        STATUS="FAIL"
        echo "  FAIL (see $RESULTS_DIR/${TEST_NAME}.log)"
        FAIL=$((FAIL + 1))
        # collect_logs must never block the loop — timeout+ignore failures.
        $TIMEOUT_CMD 30 ./scripts/collect_logs.sh \
            "$RESULTS_DIR/${TEST_NAME}_diag" 2>&1 \
            | sed 's/^/  diag: /' || true
    fi
    SCENARIO_DURATION=$(( $(date +%s) - SCENARIO_START ))

    # For FAIL rows capture the first panic line (if any) for the summary.
    FIRST_PANIC=""
    if [ "$STATUS" = "FAIL" ]; then
        FIRST_PANIC="$(grep -m1 -E 'panicked at|scenario failed' "$RESULTS_DIR/${TEST_NAME}.log" 2>/dev/null \
            | sed 's/[|]/\\|/g' | head -c 200 || true)"
    fi
    SUMMARY_ROWS+=("| $NUM | $NAME | $STATUS | ${SCENARIO_DURATION}s | ${FIRST_PANIC} |")

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
    # Wait for Docker to actually finish the tear-down (and its daemon to
    # settle) before the next scenario spins up. Specifically protects
    # scenario 15 from starting on a daemon still draining the previous
    # scenario's containers.
    wait_docker_ready
done

# Emit SUMMARY.md for cheap historical comparison across results/ runs.
SUMMARY_FILE="$RESULTS_DIR/SUMMARY.md"
{
    echo "# Run summary"
    echo ""
    echo "- Tier: $TIER"
    echo "- Results dir: $RESULTS_DIR"
    echo "- Passed: $PASS"
    echo "- Failed: $FAIL"
    echo ""
    echo "| # | Name | Status | Duration | First panic line (FAIL only) |"
    echo "|---|------|--------|----------|------------------------------|"
    for row in "${SUMMARY_ROWS[@]}"; do
        echo "$row"
    done
} > "$SUMMARY_FILE"

echo ""
echo "============================================"
echo "RESULTS: $PASS passed, $FAIL failed"
echo "Summary: $SUMMARY_FILE"
echo "============================================"
[ "$FAIL" -eq 0 ] || exit 1
