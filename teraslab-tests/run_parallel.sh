#!/bin/bash
# Run scenarios in PARALLEL by invoking pre-built test binaries directly.
# Usage: ./run_parallel.sh [results_dir] [scenario_list...]
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RESULTS_DIR="${1:-results/parallel_$(date +%Y%m%d_%H%M%S)}"
shift 2>/dev/null || true
mkdir -p "$RESULTS_DIR"

if [ $# -gt 0 ]; then
    SCENARIOS="$@"
else
    SCENARIOS="01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17"
fi

scenario_timeout() {
    case "$1" in
        01) echo 120 ;;  10) echo 900 ;;  08|15) echo 600 ;;
        16) echo 600 ;;  17) echo 600 ;;  *) echo 300 ;;
    esac
}

TIMEOUT_CMD="timeout"
command -v timeout >/dev/null 2>&1 || TIMEOUT_CMD="gtimeout"

echo "============================================"
echo "TeraSlab Parallel Test Suite"
echo "Results: $RESULTS_DIR"
echo "============================================"

# Build Docker image
echo "Building Docker image..."
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
docker build -t teraslab:test -f "$SCRIPT_DIR/docker/Dockerfile" "$PROJECT_ROOT" 2>&1 | tail -5

# Build test binaries
echo "Building test client..."
cd client && cargo test --release --no-run 2>&1 | tail -3 && cd ..

# Launch scenarios
echo ""
echo "Launching scenarios in parallel..."
PIDS=""
for NUM in $SCENARIOS; do
    TEST_NAME=$(ls client/tests/scenario_${NUM}_*.rs 2>/dev/null | head -1 | sed 's|.*/||;s|\.rs$||')
    [ -z "$TEST_NAME" ] && continue
    BIN=$(ls -t client/target/release/deps/${TEST_NAME}-* 2>/dev/null | grep -v '\.d$' | head -1)
    [ -z "$BIN" ] && continue
    TO=$(scenario_timeout "$NUM")
    LOG="$RESULTS_DIR/${TEST_NAME}.log"

    (
        START_TS=$(date +%s)
        TERASLAB_TEST_TIMING=1 $TIMEOUT_CMD "$TO" "$BIN" --nocapture > "$LOG" 2>&1
        RC=$?
        END_TS=$(date +%s)
        DUR=$((END_TS - START_TS))
        if [ $RC -eq 0 ]; then S="PASS"; elif [ $RC -eq 124 ]; then S="TIMEOUT"; else S="FAIL"; fi
        echo "$S ${DUR}s" > "$RESULTS_DIR/.result_${NUM}"
        # Docker logs for failures
        if [ $RC -ne 0 ]; then
            for n in 1 2 3 4 5; do docker logs "ts${NUM}-node${n}" > "$RESULTS_DIR/${TEST_NAME}_node${n}.log" 2>&1 || true; done
        fi
        docker ps -aq --filter "name=ts${NUM}-node" 2>/dev/null | xargs -r docker rm -f 2>/dev/null || true
    ) &
    PIDS="$PIDS $!"
    echo "  [$NUM] $TEST_NAME (timeout ${TO}s)"
done

echo "Waiting..."
for pid in $PIDS; do wait $pid 2>/dev/null || true; done

# Results
echo ""
echo "============================================"
PASS=0; FAIL=0; TOUT=0
for NUM in $SCENARIOS; do
    RFILE="$RESULTS_DIR/.result_${NUM}"
    if [ -f "$RFILE" ]; then
        read S D < "$RFILE"
        printf "  %s: %-8s %s\n" "$NUM" "$S" "$D"
        case "$S" in PASS) PASS=$((PASS+1));; FAIL) FAIL=$((FAIL+1));; TIMEOUT) TOUT=$((TOUT+1));; esac
    fi
done | tee "$RESULTS_DIR/SUMMARY.txt"
echo ""
echo "RESULTS: $PASS passed, $FAIL failed, $TOUT timed out"
echo "Wall-clock: ${SECONDS}s"
echo "============================================"
