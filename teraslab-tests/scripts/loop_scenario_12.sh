#!/usr/bin/env bash
# Phase J — runs scenario 12 (Concurrent Failures) repeatedly under
# throttled Docker so the cluster startup race the Phase I readiness
# work targets is amplified. Asserts zero failures across the loop.
#
# Usage: bash teraslab-tests/scripts/loop_scenario_12.sh [iterations]
#
# Default iterations: 50 (matches the Phase J acceptance bar).
#
# CPU throttling is applied via the TERASLAB_DOCKER_CPU_QUOTA environment
# variable, consumed by the docker-compose templates. The default
# (`50000`, i.e. 0.5 CPU per container) reproduces the original
# scenario 12 race window. Override by exporting before invoking, e.g.:
#
#   TERASLAB_DOCKER_CPU_QUOTA=25000 \
#       bash teraslab-tests/scripts/loop_scenario_12.sh 50
#
# Exit codes:
#   0  → every iteration passed
#   1  → at least one iteration failed (script stops at first failure)
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SCRIPT_DIR"

ITERATIONS="${1:-50}"
export TERASLAB_DOCKER_CPU_QUOTA="${TERASLAB_DOCKER_CPU_QUOTA:-50000}"

if ! [[ "$ITERATIONS" =~ ^[0-9]+$ ]]; then
    echo "loop_scenario_12.sh: iterations must be a non-negative integer (got '$ITERATIONS')" >&2
    exit 2
fi

echo "============================================"
echo "Phase J loop: scenario 12 × $ITERATIONS"
echo "(throttled Docker, TERASLAB_DOCKER_CPU_QUOTA=$TERASLAB_DOCKER_CPU_QUOTA)"
echo "============================================"

PASS=0
FAIL=0
FAILED_RUNS=()

for i in $(seq 1 "$ITERATIONS"); do
    echo
    echo "--- iteration $i / $ITERATIONS ---"
    if bash run_all.sh --scenario 12; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        FAILED_RUNS+=("$i")
        echo "FAIL on iteration $i — diagnostics preserved under teraslab-tests/results/" >&2
        break
    fi
done

echo
echo "============================================"
echo "loop_scenario_12 result: $PASS passed, $FAIL failed (of $ITERATIONS attempts)"
if [ "$FAIL" -gt 0 ]; then
    echo "Failed iterations: ${FAILED_RUNS[*]}"
    exit 1
fi
echo "============================================"
exit 0
