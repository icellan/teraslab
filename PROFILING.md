# Profiling TeraSlab

TeraSlab targets 10M+ ops/sec. When throughput is gated by a hot path, you need
to *see* where CPU goes, not guess. This doc covers the three ways to profile a
running or test-driven server, and the read/decoration load profile used to
reproduce the serving bottleneck.

## 1. In-process pprof endpoint (live server, no rebuild)

The admin HTTP server exposes a CPU sampling profiler behind the bearer-token
gate. It samples the **whole process** via `ITIMER_PROF` + `SIGPROF` and renders
an [inferno](https://github.com/jonhoo/inferno) flamegraph SVG. It is
**single-flight** (one profile at a time → second request gets `409`) and runs
the blocking sample on a dedicated thread, so it never parks the async HTTP
runtime.

```
GET /debug/pprof/profile?seconds=N&frequency=Hz
```

| param       | default      | range      | meaning                                   |
|-------------|--------------|------------|-------------------------------------------|
| `seconds`   | `5`          | `1..=60`   | sampling window                           |
| `frequency` | `99`         | `10..=1000`| samples/sec (99 Hz avoids lock-step bias) |

Requires `enable_admin_endpoints = true` and an `Authorization: Bearer <token>`
header. Example:

```bash
# 15s flamegraph SVG → open in a browser
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://127.0.0.1:9090/debug/pprof/profile?seconds=15" > cpu.svg
```

The endpoint serves only the SVG flamegraph: pprof's protobuf codec pulls a
vulnerable / duplicate dependency (rust-protobuf 2.x → RUSTSEC-2024-0437, or a
second prost), so for a `go tool pprof` / pprof.me protobuf use `samply` or
`cargo flamegraph` below — they attach to the running process and export pprof
format without that dependency.

`SIGPROF` interrupts blocking syscalls with `EINTR`. The device `pread`/`pwrite`
loops (`src/device.rs`) and std socket reads already retry on `EINTR`, so taking
a profile does not perturb in-flight serving.

## 2. samply (live server, attach by PID — recommended for production)

[`samply`](https://github.com/mstange/samply) is an external sampling profiler
with no in-process signal handler and no code changes. Best for attaching to a
production binary without a rebuild or admin endpoint:

```bash
cargo install samply
# Launch under samply:
samply record ./target/release/teraslab-server --config server.toml
# …or attach to an already-running server:
samply record -p $(pgrep -f teraslab-server)
```

It opens the Firefox Profiler UI with an inverted call tree and flamegraph.

## 3. cargo flamegraph (perf/dtrace, whole-run)

```bash
cargo install flamegraph
# Linux (perf) / macOS (dtrace, needs sudo):
cargo flamegraph --bin teraslab-server -- --config server.toml
```

## Reproducing the read/decoration bottleneck

`tests/common/mod.rs` carries a read/decoration load harness that mirrors
teranode's parent decoration: one connection issuing fat `OP_GET_BATCH`
requests with `FieldMask::COLD_DATA` set (which forces the slow device-read
path — `read_metadata` + `read_cold_data` per item).

- `seed_cold_records(port, count, cold_bytes)` — seed parents with outputs cold data.
- `drive_decoration_reads(port, &txids, batch_size, batches)` — one connection, fat batches.
- `run_read_clients(port, &txids, clients, batch_size, batches)` — concurrent connections.

The slow-tests baseline reports the CPU/wall ratio (cores):

```bash
cargo test --test write_scaling --features slow-tests \
  read_scaling_single_batch_uses_multiple_cores -- --nocapture
```

A single connection sending one fat 826-item batch at a time pins **~1.0 core**
regardless of how many cores are free: the batch is walked in a single serial
loop on the connection thread. That is the read-path equivalent of the write
baseline's cores figure, and the number Phase B drives above 1.0 by parallelizing
the per-item loop.
