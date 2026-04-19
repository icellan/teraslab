# Observability

This document describes the metrics, traces, and log surfaces exposed by
the TeraSlab server, how to enable them, and the perf-regression gate
that protects the observability stack from creeping overhead.

- [Enabling OTLP](#enabling-otlp)
- [Exposed metrics](#exposed-metrics)
- [Prometheus format notes](#prometheus-format-notes)
- [Tracing / `RUST_LOG`](#tracing--rust_log)
- [Perf regression gate](#perf-regression-gate)

---

## Enabling OTLP

OTLP span export is off by default — initialising the subscriber without
an endpoint skips the background batch processor entirely. To turn it on
add an `[observability]` section to the server TOML:

```toml
[observability]
otlp_endpoint        = "http://otel-collector:4317"   # gRPC
trace_sampling_ratio = 0.01                            # 1 %
service_name         = "teraslab"                      # resource attribute
```

Every field can also be overridden at startup via environment variables:

| TOML field              | Env var                         |
|-------------------------|---------------------------------|
| `otlp_endpoint`         | `TERASLAB_OTLP_ENDPOINT`        |
| `trace_sampling_ratio`  | `TERASLAB_TRACE_SAMPLING_RATIO` |
| `service_name`          | `TERASLAB_SERVICE_NAME`         |

An empty `TERASLAB_OTLP_ENDPOINT` (or absent `otlp_endpoint` field)
disables export — the process runs with the fmt-layer-only subscriber
installed in Phase 3.

The `[observability]` config validator rejects sampling ratios outside
`[0.0, 1.0]` so operator mistakes fail fast. Sampling uses
`ParentBased(TraceIdRatioBased(ratio))`: parent spans sampled upstream
(e.g. a replica inheriting a W3C wire trace context) are always exported
regardless of the local ratio.

## Exposed metrics

`GET /metrics` on the HTTP observability port renders Prometheus text
format. The rendering function is
[`src/server/http.rs::render_metrics_text`][render] — it is decoupled
from the HTTP stack so unit tests scrape it directly. Series fall into
five groups:

1. **Operation counters (Phase 1)** — `teraslab_spends_attempted_total`,
   `teraslab_spends_succeeded_total`, `teraslab_spends_idempotent_total`,
   `teraslab_spends_failed_total`, `teraslab_creates_*_total`,
   `teraslab_set_mined_*_total`, `teraslab_freezes_*_total`,
   `teraslab_deletes_*_total`, and a matching set for unspends,
   unfreezes, reassigns, set_conflicting, set_locked, preserve_until,
   mark_longest_chain, and gets.
2. **Labeled outcomes (Phase 2)** — `teraslab_operations_total{op,outcome}`
   emits the full `OpCode × Outcome` grid, even zero cells. Dashboards
   should prefer this series for `rate()` queries; the scalar counters
   remain for legacy alerts.
3. **Latency histograms (Phase 1)** — one `teraslab_<op>_latency_ns`
   histogram per operation plus `teraslab_lock_wait_ns`. Buckets are
   cumulative, the `+Inf` terminator is always emitted, and every
   histogram emits `_sum` and `_count` lines.
4. **Subsystem surfaces (Phase 5)** — replication, io_uring, redo,
   migration, SWIM, and allocator blocks, each behind a `OnceLock`
   install hook. In production the server binary calls every
   `init_*_metrics` in `src/bin/server.rs`; in tests the blocks resolve
   to `Option::None` when not installed, which keeps test payloads
   minimal.
5. **Gauges** — `teraslab_index_entries`, `teraslab_dah_index_entries`,
   `teraslab_unmined_index_entries`, `teraslab_active_connections`,
   `teraslab_migration_active`, `teraslab_migration_phase_*`,
   `teraslab_freelist_region_count`, and
   `teraslab_freelist_largest_region_bytes`.

### Prometheus format notes

- Every series carries a `# TYPE <name> <kind>` declaration; the
  conformance test `tests/prometheus_conformance.rs` fails CI if any
  sample is emitted without a matching TYPE line.
- `# HELP` lines are **optional** in the Prometheus exposition spec and
  the current renderer omits them by design — the PaddedCounter /
  LatencyHistogram abstractions do not carry documentation metadata. The
  conformance test tolerates this: it validates HELP *when present*
  (non-empty text, matching name), and does not require it. If a future
  phase wants to expose HELP text, extending `prom_counter` /
  `prom_gauge` / `prom_histogram_ns` is the right place.
- Histograms use cumulative bucket counts with a `le="+Inf"` terminator,
  as required by Prometheus query semantics.

## Tracing / `RUST_LOG`

Production log output is structured JSON (Phase 3) via
`tracing_subscriber::fmt::Layer::json()`. Filter with `RUST_LOG`:

```sh
# default — INFO level, every subsystem
RUST_LOG=info teraslab-server --config server.toml

# chase the replication layer at DEBUG, keep everything else at INFO
RUST_LOG='info,teraslab::replication=debug' teraslab-server ...

# silence warnings you can't fix right now
RUST_LOG='info,teraslab::cluster::swim=error' teraslab-server ...

# reserved-for-development: span tracing at trace level, plus axum access logs
RUST_LOG='teraslab=trace,tower_http=debug' teraslab-server ...
```

Hot-path span macros (`#[tracing::instrument(level = "debug", skip_all)]`
on `spend_multi`, `set_mined`, `create`, `delete`, `freeze`,
`replicate_batch`, `redo::flush`, etc.) short-circuit below `DEBUG`, so
the default `INFO` filter is cheap — see the 5% perf budget below.

Wire-protocol batches carry a 24-byte W3C trace context in their header
(Phase 4). The replication receiver attaches the inbound context as a
remote parent on `handle_replica_batch`, so a sampled leader span stitches
through the replicas it fans out to. `tests/tracing_integration.rs`
asserts this end-to-end.

### Runtime log-level control

The HTTP server exposes a read/write log-level endpoint:

```sh
# read
curl -s http://127.0.0.1:9090/debug/log-level

# write — one of trace|debug|info|warn|error
curl -X PUT -d 'debug' http://127.0.0.1:9090/debug/log-level
```

This flips the atomic `log_level` in `HttpState` which the fmt layer
consults for enable/disable decisions. It is a process-global toggle —
no per-subscriber scoping.

## Perf regression gate

Observability must cost ≤ 5% throughput regression on the
`spend_throughput` bench. The gate is
[`scripts/check_perf_budget.sh`](../scripts/check_perf_budget.sh):

```sh
# first run: record the baseline (~5 min, full criterion)
scripts/check_perf_budget.sh --save-baseline

# every subsequent invocation: compare against the baseline
scripts/check_perf_budget.sh
# → exit 0 when all targets are within +5% time
# → exit 1 with a per-target regression report otherwise

# quick local smoke check (less precision, more noise)
scripts/check_perf_budget.sh --smoke

# help
scripts/check_perf_budget.sh --help
```

The gate parses the per-target `mean.point_estimate` from
`target/criterion/<group>/<target>/change/estimates.json`. Any target
whose mean time change exceeds `+0.05` (5%) flips the script's exit
status and is reported on stderr along with the actual percentage.

The script writes a timestamped log to `target/obs-perf/bench-*.log`
containing the full criterion stdout/stderr.

### Notes for CI

- `jq` is optional but recommended — falls back to a Python JSON parser.
- Smoke mode uses `--sample-size 10 --warm-up-time 1 --measurement-time 2`
  and is **too noisy** for a hard gate: treat it as a developer aid,
  not a pass/fail signal.
- Baselines are stored under `target/criterion/**/obs/estimates.json`
  and are **not** committed to the repo. Regenerate after intentional
  perf-affecting changes: `scripts/check_perf_budget.sh --save-baseline`.

## Known follow-ups

- **Cluster-level metric validation under sustained load** is not yet
  wired into `teraslab-tests/client/tests/`. `scenario_10_sustained_load`
  tracks client-side counters (`WorkloadRunner::metrics`) but does not
  scrape the server's `/metrics` endpoint and assert post-run invariants
  on `teraslab_operations_total`, `teraslab_repl_lag_sequences`, or
  `teraslab_redo_flush_latency_ns`. The unit-level
  `tests/prometheus_conformance.rs` validates the emitted shape; a
  future scenario should validate the values **under real cluster load**.
- **Always-sample-on-error** span policy is not in effect. The sampler
  is `ParentBased(TraceIdRatioBased)` — operators who need full error
  coverage set `trace_sampling_ratio = 1.0`. Re-sampling at event time
  is non-trivial in the current `opentelemetry_sdk` `ShouldSample` API
  and is left as a follow-up.
- **Cluster-wide aggregation** of the six new metric blocks
  (`replication_metrics`, `uring_metrics`, `redo_metrics`,
  `migration_metrics`, `swim_metrics`, `allocator_metrics`) is not
  summed across nodes in `aggregate_snapshots`. The UI reads them
  from the first node only. Multi-node aggregation is a follow-up.

[render]: ../src/server/http.rs
