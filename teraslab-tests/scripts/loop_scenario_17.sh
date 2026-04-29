#!/usr/bin/env bash
# Phase J — runs scenario 17 (Failure Recovery Hardening) repeatedly to
# prove the Phase A–I fixes hold under repeated cluster churn. Asserts
# zero failures across the loop.
#
# Usage: bash teraslab-tests/scripts/loop_scenario_17.sh [iterations]
#
# Default iterations: 50 (matches the Phase J acceptance bar).
#
# Exit codes:
#   0  → every iteration passed
#   1  → at least one iteration failed (the script stops at the first
#        failure so the diagnostic logs from the failing run are
#        preserved in `teraslab-tests/results/`)
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

ITERATIONS="${1:-50}"

if ! [[ "$ITERATIONS" =~ ^[0-9]+$ ]]; then
    echo "loop_scenario_17.sh: iterations must be a non-negative integer (got '$ITERATIONS')" >&2
    exit 2
fi

echo "============================================"
echo "Phase J loop: scenario 17 × $ITERATIONS"
echo "============================================"

PASS=0
FAIL=0
FAILED_RUNS=()

for i in $(seq 1 "$ITERATIONS"); do
    echo
    echo "--- iteration $i / $ITERATIONS ---"
    if bash run_all.sh --scenario 17; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        FAILED_RUNS+=("$i")
        echo "FAIL on iteration $i — preserving diagnostics under teraslab-tests/results/" >&2
        # Stop at the first failure so the diagnostic dump for that run
        # is the most recent one in the results directory.
        break
    fi
done

echo
echo "============================================"
echo "loop_scenario_17 result: $PASS passed, $FAIL failed (of $ITERATIONS attempts)"
if [ "$FAIL" -gt 0 ]; then
    echo "Failed iterations: ${FAILED_RUNS[*]}"
    exit 1
fi
echo "============================================"
exit 0
