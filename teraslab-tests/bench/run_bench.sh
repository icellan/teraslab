#!/usr/bin/env bash
# Single-node TeraSlab load test + pipeline-bottleneck profiling harness.
#
# What it does, per store-count variant (1store, 4store):
#   1. boots a FRESH container on a clean volume, so every /metrics counter
#      starts at 0 and the post-run scrape is the whole-run cumulative;
#   2. drives the over-the-wire mixed workload via `teraslab-loadgen`
#      (create / spend / read / set_mined);
#   3. scrapes the server's Prometheus /metrics, which carries the per-stage
#      pipeline histograms used for bottleneck attribution:
#        - teraslab_lock_wait_*            (stripe-lock contention)
#        - teraslab_redo_flush_latency_*   (the fsync / group-commit cost)
#        - teraslab_redo_entries_per_flush_* (group-commit coalescing degree)
#        - teraslab_<op>_latency_*         (end-to-end handler latency per op)
#
# Comparing 1store vs 4store isolates the multi-device effect: 4 stores give
# 4 independent redo logs that fsync in parallel, so if the redo flush is the
# bottleneck, 4store shows lower per-op latency / higher throughput.
#
# Prereqs: docker, curl, and the loadgen binary (auto-built if missing).
# Usage:
#   teraslab-tests/bench/run_bench.sh                  # both variants
#   DUR=60 WORKERS=48 teraslab-tests/bench/run_bench.sh
#   VARIANTS="1store" teraslab-tests/bench/run_bench.sh   # one variant
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

IMAGE="${IMAGE:-teraslab:test}"
DUR="${DUR:-45}"
WORKERS="${WORKERS:-32}"
RATE="${RATE:-100000000}"        # effectively unthrottled -> saturate the server
VARIANTS="${VARIANTS:-1store 4store}"
HOST_CLIENT_PORT="${HOST_CLIENT_PORT:-13300}"
HOST_HTTP_PORT="${HOST_HTTP_PORT:-19100}"
OUT="$REPO_ROOT/teraslab-tests/results/bench_$(date +%Y%m%d_%H%M%S)"

LOADGEN="${LOADGEN:-$REPO_ROOT/client/rust/target/release/teraslab-loadgen}"

mkdir -p "$OUT"

# Build the loadgen if it is not already present.
if [ ! -x "$LOADGEN" ]; then
  echo "loadgen not found at $LOADGEN — building..."
  cargo build --release --manifest-path "$REPO_ROOT/client/rust/Cargo.toml" --bin teraslab-loadgen
fi

# Build the server image if it is not present.
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "image $IMAGE not found — building..."
  docker build -f "$REPO_ROOT/teraslab-tests/docker/Dockerfile" \
    --build-arg CACHE_BUST="$(git -C "$REPO_ROOT" rev-parse --short HEAD)" \
    -t "$IMAGE" "$REPO_ROOT"
fi

run_variant() {
  local name="$1"
  local cfg="$SCRIPT_DIR/bench-${name}.toml"
  local cname="teraslab-bench" vol="teraslab-bench-vol"
  echo "==================================================================="
  echo "VARIANT: $name   ($cfg)"
  echo "==================================================================="
  [ -f "$cfg" ] || { echo "missing config $cfg"; return 1; }

  docker rm -f "$cname" >/dev/null 2>&1
  docker volume rm "$vol" >/dev/null 2>&1
  docker volume create "$vol" >/dev/null

  docker run -d --name "$cname" \
    --ulimit memlock=-1:-1 \
    -p ${HOST_CLIENT_PORT}:3300 \
    -p ${HOST_HTTP_PORT}:9100 \
    -v "$vol":/data \
    -v "$cfg":/etc/teraslab/node.toml:ro \
    "$IMAGE" >/dev/null || { echo "docker run failed"; return 1; }

  echo -n "waiting for health"
  for _ in $(seq 1 60); do
    curl -sf "http://localhost:${HOST_HTTP_PORT}/health/live" >/dev/null 2>&1 && { echo " ... up"; break; }
    echo -n "."; sleep 1
  done
  if ! curl -sf "http://localhost:${HOST_HTTP_PORT}/health/live" >/dev/null 2>&1; then
    echo " FAILED to come up"; docker logs "$cname" 2>&1 | tail -40
    docker rm -f "$cname" >/dev/null; return 1
  fi

  echo "--- loadgen: workers=$WORKERS rate=$RATE dur=${DUR}s ---"
  "$LOADGEN" --addr 127.0.0.1:${HOST_CLIENT_PORT} \
    --rate "$RATE" --workers "$WORKERS" --duration "$DUR" \
    2>&1 | tee "$OUT/${name}_loadgen.txt"

  curl -sf "http://localhost:${HOST_HTTP_PORT}/metrics" > "$OUT/${name}_metrics.txt"
  curl -sf "http://localhost:${HOST_HTTP_PORT}/status"  > "$OUT/${name}_status.json" 2>/dev/null
  docker logs "$cname" > "$OUT/${name}_server.log" 2>&1
  docker rm -f "$cname" >/dev/null
  docker volume rm "$vol" >/dev/null 2>&1
  echo "metrics -> $OUT/${name}_metrics.txt"
}

for v in $VARIANTS; do run_variant "$v"; done
echo
echo "DONE. Results in $OUT"
