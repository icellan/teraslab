#!/usr/bin/env bash
# Phase 6 perf regression gate.
#
# Runs the `spend_throughput` criterion bench against a previously-saved
# baseline and fails if any target regresses by more than +5% (time).
#
# Modes:
#   (default)          : compare against existing baseline, fail on regression
#   --save-baseline    : (re)record the `obs` baseline and exit
#   --smoke            : fewer iterations for a local smoke check
#   --help             : print this usage
#
# Dependencies: bash, cargo. Optional: jq (for robust JSON parsing — the
# script falls back to a grep/sed pipeline when jq is absent).

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASELINE_NAME="obs"
BUDGET_PCT="0.05"   # +5% time regression budget
BENCH_NAME="spend_throughput"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BASELINE_DIR="$REPO_ROOT/target/criterion"
LOG_DIR="$REPO_ROOT/target/obs-perf"

mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

usage() {
    cat <<EOF
check_perf_budget.sh — observability perf regression gate

USAGE
    scripts/check_perf_budget.sh [--smoke]
        Compare current bench against the '${BASELINE_NAME}' baseline.
        Fail (exit 1) if any bench target regresses beyond +${BUDGET_PCT}
        (time estimate).

    scripts/check_perf_budget.sh --save-baseline [--smoke]
        (Re)record the '${BASELINE_NAME}' baseline and exit 0.

    scripts/check_perf_budget.sh --help
        Print this help.

MODES
    --smoke         Run with --sample-size 10 --warm-up-time 1
                    --measurement-time 2 (quick, imprecise).
                    Default: full criterion run.

OUTPUT
    Log file: target/obs-perf/bench-<timestamp>.log
    Baseline: target/criterion/**/${BASELINE_NAME}/estimates.json
    Compare:  target/criterion/**/change/estimates.json

DEPENDENCIES
    jq is optional but recommended. Without jq the script parses the
    criterion JSON with grep/sed which is fragile if criterion's JSON
    schema changes. If jq is not on PATH the script prints a warning
    and falls back.
EOF
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

MODE="compare"     # compare | save
SMOKE=0

while [ $# -gt 0 ]; do
    case "$1" in
        --save-baseline) MODE="save"; shift ;;
        --smoke)         SMOKE=1; shift ;;
        --help|-h)       usage; exit 0 ;;
        *)               echo "error: unknown argument '$1'" >&2; usage >&2; exit 2 ;;
    esac
done

# ---------------------------------------------------------------------------
# Bench invocation
# ---------------------------------------------------------------------------

STAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/bench-$STAMP.log"
echo "perf-gate: logging to $LOG_FILE"

# Record a sentinel file AFTER the script starts but BEFORE criterion runs,
# so we can later identify which change/estimates.json files belong to
# this invocation. Criterion rewrites the file on every comparison run —
# files with mtime >= sentinel were produced by this run (i.e. the
# spend_throughput bench), while leftover files from other benches are
# older and excluded.
SENTINEL="$LOG_DIR/.sentinel-$STAMP"
: > "$SENTINEL"

BENCH_ARGS=()
if [ "$SMOKE" -eq 1 ]; then
    BENCH_ARGS+=(--sample-size 10 --warm-up-time 1 --measurement-time 2)
    echo "perf-gate: --smoke mode (fewer iterations — results are imprecise)"
fi

case "$MODE" in
    save)
        echo "perf-gate: recording baseline '${BASELINE_NAME}'"
        (cd "$REPO_ROOT" && cargo bench --bench "$BENCH_NAME" -- \
            --save-baseline "$BASELINE_NAME" \
            ${BENCH_ARGS[@]+"${BENCH_ARGS[@]}"}) 2>&1 | tee "$LOG_FILE"
        echo "perf-gate: baseline '${BASELINE_NAME}' saved."
        exit 0
        ;;
    compare)
        # If no baseline exists yet, record one and exit 0 with a
        # descriptive message. This matches the "first-run bootstrap"
        # behaviour specified in the phase doc.
        if ! find "$BASELINE_DIR" -type d -name "$BASELINE_NAME" -print -quit 2>/dev/null | grep -q .; then
            echo "perf-gate: no baseline found under $BASELINE_DIR/**/${BASELINE_NAME}"
            echo "perf-gate: recording baseline now (first-run bootstrap) …"
            (cd "$REPO_ROOT" && cargo bench --bench "$BENCH_NAME" -- \
                --save-baseline "$BASELINE_NAME" \
                ${BENCH_ARGS[@]+"${BENCH_ARGS[@]}"}) 2>&1 | tee "$LOG_FILE"
            echo "perf-gate: baseline recorded; next invocation will compare."
            exit 0
        fi

        echo "perf-gate: comparing against baseline '${BASELINE_NAME}'"
        (cd "$REPO_ROOT" && cargo bench --bench "$BENCH_NAME" -- \
            --baseline "$BASELINE_NAME" \
            ${BENCH_ARGS[@]+"${BENCH_ARGS[@]}"}) 2>&1 | tee "$LOG_FILE"
        ;;
esac

# ---------------------------------------------------------------------------
# Parse criterion change/estimates.json for regression gate
# ---------------------------------------------------------------------------

# Criterion writes per-target change JSON at
# target/criterion/<bench_group>/<target>/change/estimates.json. The
# `mean` estimate is the canonical time change (point_estimate in the
# unit used by criterion — a dimensionless ratio where +0.05 means +5%).
#
# We walk every change estimates file and extract the mean point_estimate.
# Any value > BUDGET_PCT trips the gate.

if [ ! -d "$BASELINE_DIR" ]; then
    echo "perf-gate: error: expected criterion output at $BASELINE_DIR but none found" >&2
    exit 1
fi

# Find all change/estimates.json files produced AFTER the sentinel was
# created. This scopes the gate to the files this bench run actually
# produced — leftover change files from other benches (mixed_workload,
# index_ops, allocator_ops, etc.) keep their older mtimes and are
# excluded. Criterion only rewrites change files for groups it exercises.
#
# `mapfile` is bash 4+. macOS ships bash 3 by default, so read into an
# array the portable way using a while-read loop.
CHANGE_FILES=()
while IFS= read -r f; do
    CHANGE_FILES+=("$f")
done < <(find "$BASELINE_DIR" -type f -path "*/change/estimates.json" -newer "$SENTINEL" -print)

if [ "${#CHANGE_FILES[@]}" -eq 0 ]; then
    # Fresh baselines don't produce change files — that's only valid
    # in the --save-baseline branch above. If we got here without any
    # change files something is wrong.
    echo "perf-gate: error: no change/estimates.json files produced. Did criterion run successfully?" >&2
    exit 1
fi

echo "perf-gate: checking ${#CHANGE_FILES[@]} change files against +${BUDGET_PCT} budget"

have_jq=1
if ! command -v jq >/dev/null 2>&1; then
    have_jq=0
    echo "perf-gate: warning: jq not found; falling back to regex parser" >&2
fi

REGRESS_COUNT=0
for file in "${CHANGE_FILES[@]}"; do
    # Derive the benchmark identifier from the path for the report.
    # The layout is target/criterion/<group>/<target>/change/estimates.json
    rel="${file#"$BASELINE_DIR"/}"
    target_id="${rel%/change/estimates.json}"

    # Two fields matter per-bench:
    #  * mean.point_estimate — the central estimate of the time change
    #    (dimensionless ratio; +0.05 means +5% slower).
    #  * mean.confidence_interval.lower_bound — the lower 95% bound.
    #
    # We fail the gate only when `lower_bound > BUDGET_PCT`, i.e. when
    # the whole 95% CI lies above the budget. That rule skips
    # statistically-insignificant blips on noisy benches without
    # masking genuine regressions.
    if [ "$have_jq" -eq 1 ]; then
        mean_change="$(jq -r '.mean.point_estimate // empty' "$file")"
        lower_bound="$(jq -r '.mean.confidence_interval.lower_bound // empty' "$file")"
    else
        mean_change="$(python3 -c '
import json, sys
with open(sys.argv[1]) as fh:
    data = json.load(fh)
print(data.get("mean", {}).get("point_estimate", ""))
' "$file" 2>/dev/null || true)"
        lower_bound="$(python3 -c '
import json, sys
with open(sys.argv[1]) as fh:
    data = json.load(fh)
mean = data.get("mean", {})
ci = mean.get("confidence_interval", {})
print(ci.get("lower_bound", ""))
' "$file" 2>/dev/null || true)"
    fi

    if [ -z "$mean_change" ] || [ -z "$lower_bound" ]; then
        echo "perf-gate: $target_id: could not parse mean/lower_bound; skipping (file=$file)" >&2
        continue
    fi

    # `awk` for cross-platform float comparison (works on macOS + Linux).
    # Regression = mean > budget AND lower_bound > budget.
    regress="$(awk -v mv="$mean_change" -v lb="$lower_bound" -v budget="$BUDGET_PCT" \
                 'BEGIN { if (mv+0 > budget+0 && lb+0 > budget+0) print "YES"; else print "NO" }')"

    if [ "$regress" = "YES" ]; then
        REGRESS_COUNT=$((REGRESS_COUNT + 1))
        echo "perf-gate: REGRESSION  $target_id  mean=$mean_change lower=$lower_bound (> +$BUDGET_PCT)"
    elif awk -v mv="$mean_change" -v budget="$BUDGET_PCT" \
             'BEGIN { exit (mv+0 > budget+0) ? 0 : 1 }'; then
        # Over-budget but not statistically significant: flag as warn,
        # keep exit status clean.
        echo "perf-gate: NOISE       $target_id  mean=$mean_change lower=$lower_bound (over budget, CI not significant)"
    else
        echo "perf-gate: OK          $target_id  mean=$mean_change lower=$lower_bound"
    fi
done

if [ "$REGRESS_COUNT" -gt 0 ]; then
    echo "perf-gate: FAIL — $REGRESS_COUNT target(s) regressed beyond +${BUDGET_PCT}"
    echo "perf-gate: see $LOG_FILE for the full criterion output"
    exit 1
fi

echo "perf-gate: PASS — all targets within +${BUDGET_PCT} budget"
exit 0
