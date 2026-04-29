#!/usr/bin/env bash
# Phase J — runs the full 17-scenario release tier suite N times in
# sequence. Asserts every scenario green on every iteration.
#
# This is the broadest verification gate for the Phase A–I work: it
# proves the chronic failure modes (scenarios 12, 17, plus the recently
# regression-flaky 4–9, 11–16) hold across consecutive runs without
# operator intervention.
#
# Usage: bash teraslab-tests/scripts/release_tier_10x.sh [iterations]
#
# Default iterations: 10 (matches the Phase J acceptance bar).
#
# Exit codes:
#   0  → every iteration passed every scenario
#   1  → at least one iteration had at least one scenario failure
#        (the script stops at the first failed iteration so the diag
#        dump is preserved in `teraslab-tests/results/`)
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

ITERATIONS="${1:-10}"

if ! [[ "$ITERATIONS" =~ ^[0-9]+$ ]]; then
    echo "release_tier_10x.sh: iterations must be a non-negative integer (got '$ITERATIONS')" >&2
    exit 2
fi

echo "============================================"
echo "Phase J: release tier × $ITERATIONS"
echo "  17 scenarios per iteration"
echo "  total = $((ITERATIONS * 17)) scenario runs"
echo "============================================"

OVERALL_START=$(date +%s)
PASS=0
FAIL=0
FAILED_RUNS=()

for i in $(seq 1 "$ITERATIONS"); do
    echo
    echo "============================================"
    echo "iteration $i / $ITERATIONS"
    echo "============================================"
    ITER_START=$(date +%s)
    if bash run_all.sh --tier release; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        FAILED_RUNS+=("$i")
        echo "FAIL on iteration $i — diagnostics preserved under teraslab-tests/results/" >&2
        break
    fi
    ITER_END=$(date +%s)
    echo "iteration $i complete in $((ITER_END - ITER_START))s"
done

OVERALL_END=$(date +%s)
echo
echo "============================================"
echo "release_tier_10x result: $PASS passed, $FAIL failed (of $ITERATIONS iterations)"
echo "wall-clock time: $((OVERALL_END - OVERALL_START))s"
if [ "$FAIL" -gt 0 ]; then
    echo "Failed iterations: ${FAILED_RUNS[*]}"
    exit 1
fi
echo "============================================"
exit 0
