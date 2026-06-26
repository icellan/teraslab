# TeraSlab

A purpose-built Rust database server designed as a UTXO store backend for BSV Teranode.

TeraSlab exploits the fixed, known workload patterns of UTXO storage to achieve dramatically better performance than general-purpose databases. By using in-place mutation on raw block devices instead of copy-on-write, it **targets** 10M+ ops/sec sustained throughput, dramatically lower SSD wear, and better tail latency than a general-purpose KV store.

> **Performance claims are design targets, not measured production numbers.** The 10M+ ops/sec figure is observed on `MemoryDevice` (anonymous `mmap`, no `O_DIRECT`, no `fsync`, no redo log). The same workload on `DirectDevice` + redo durability is bounded by NVMe sector writes and fsync, and is expected to deliver low-100s of K ops/sec per core in current form. NVMe bench results are not yet published — see [Performance methodology](#performance-methodology) below.

## Key design

TeraSlab pre-allocates UTXO slots at full size during creation and mutates them in place. The *logical* mutation footprint of a spend is small (1-byte status + 36-byte spending data + 4-byte slot CRC = 41 bytes, then a metadata bump), but the *on-device write* rewrites the full 73-byte slot and the full 320-byte metadata header — and that is bounded below by the device sector size anyway.

| Property | Value |
|----------|-------|
| Logical spend mutation | 41 bytes per slot (1-byte status + 36-byte spending data + 4-byte slot CRC) + a metadata bump |
| Slot total size on disk | 73 bytes (32-byte hash + slot payload + CRC) |
| On-device write size | The full 73-byte slot is rewritten (not just the 41-byte footer), plus the full 320-byte metadata header. On `MemoryDevice` this is the exact byte range; on `DirectDevice` with `O_DIRECT` each rewrite amplifies to 4096-byte sectors anyway. |
| p99.9 latency target | Low (no copy-on-write, no defrag spikes) — not yet measured on production hardware |
| Replication bandwidth | ~120 MB/s target (operation-based, not full-record) |
| Memory per record | 64-byte hash-table bucket (one cache line) in-memory — effective bytes/record is 64 ÷ load factor; or ~0 with on-disk redb backend |

### Performance methodology

The published benchmarks (`benches/`) all run against `MemoryDevice` (anonymous `mmap`). They measure the in-memory throughput ceiling — useful for catching algorithmic regressions, but **not** representative of production deployment on NVMe + `O_DIRECT` + redo-log durability. NVMe bench results, fault-injection numbers, and tail-latency histograms on a real device are tracked as outstanding work; the README will be updated to cite them when they exist. Until then, treat any throughput figure as "MemoryDevice ceiling, no fsync."

The production write path is synchronous `O_DIRECT` I/O via `src/device.rs` (`DirectDevice`), at queue-depth-1. There is no async/io_uring backend: the earlier `src/device_io/` scaffolding (a `DeviceIo` trait plus io_uring/sync fallback backends, never wired into any caller) was deleted on 2026-05-28.

## Status

**Pre-production.** All 14 build phases (0–13) are implemented; phases 1–10, 12, 13 are shipped, while phases 0 and 11 are partial — production code paths work but specific follow-ups remain (phase 0: the standalone `SPEC_VALIDATION_REPORT.md` was folded into the per-phase docs; phase 11: the separate-NVMe middle tier is intentionally not enabled). Each `phases/NN_*.md` carries its own `Status:` header.

| Probe | Result |
|-------|-------:|
| `cargo test --all` | 0 failed / 0 ignored (pass count grows with the suite — see CI) |
| `cargo test --features fault-injection` (gated binaries) | 0 failed (see CI) |
| `cd client/rust && cargo test` | 0 failed (see CI) |
| `cargo clippy --all-targets -- -D warnings` | clean (with and without `--features fault-injection`) |
| `cargo fmt --all -- --check` | clean |

**Documented design choices** (deliberate scope decisions, not bugs):

- **Single-interval freeze model.** `spendable_height` is a single `u32` per output (mirrors Aerospike). svnode's `enforceAtHeight` supports a multi-interval array. If Teranode's contract evolves to require multi-interval freezes (e.g. two disjoint locked windows on the same UTXO), a new op type (`OP_FREEZE_INTERVAL_BATCH` or similar) is required; the on-disk slot layout reserves enough spending-data bytes to extend without a format break, but the engine match arms and the wire protocol need additions.

**Known residual coverage gaps** (tracked, low risk):

- The Linux `BLKGETSIZE64` raw block-device size query is unit-tested for its arithmetic and exercised against a real macOS RAM-disk node (`tests/block_device_size.rs`), but not yet against a real Linux `/dev/nvme` device (needs a root loop-device CI job).
- Wire-decoder fuzzing runs as a seeded in-tree smoke test on every CI run; the deep `cargo-fuzz` target (`fuzz/`) is run manually rather than on a scheduled nightly job.

**License.** Open BSV License Version 6 — see [`LICENSE`](LICENSE). Not yet certified for production deployment.

## Building

```bash
cargo build --release
```

The binaries are at `target/release/teraslab-server` and `target/release/teraslab-cli`.

### Requirements

- Rust 2024 edition (1.85+)
- Linux or macOS (direct I/O support recommended for production on Linux)

### Running tests

```bash
cargo test --all
```

### Linting

```bash
cargo clippy --all -- -D warnings
```

## Running the server

### Quick start (single-node, defaults)

```bash
# Uses in-memory device, default ports, no clustering
./target/release/teraslab-server
```

This starts TeraSlab with all defaults:
- **TCP wire protocol** on `127.0.0.1:3300` (loopback only by default)
- **HTTP observability** on `127.0.0.1:9100` (loopback only by default)
- Data file: `teraslab-data.dat` (1 GiB, created if missing)
- Index: in-memory (snapshot: `teraslab-index.snap`). See [Index backends](#index-backends) for the on-disk alternative.
- Single-node mode (no clustering)

> **Loopback by default.** Both listeners default to `127.0.0.1`, so a remote Teranode
> client cannot connect to a no-config server. To accept remote connections you must both
> bind a non-loopback address (e.g. `0.0.0.0:3300`) **and** set `enable_remote_bind = true`
> — the server refuses to start on a non-loopback bind without it (`ConfigError::RemoteBindRefused`).
> See the [full configuration reference](#full-configuration-reference) and [security knobs](#security-and-access-control).

### Configuration file

```bash
./target/release/teraslab-server --config /path/to/config.toml
```

All settings have sensible defaults. Only specify what you want to override.

#### Minimal single-node config

```toml
listen_addr = "0.0.0.0:3300"
enable_remote_bind = true  # required for any non-loopback bind — only safe on a private network
# Use stable paths — /dev/nvme* numbers can change on reboot
device_paths = ["/dev/disk/by-id/nvme-Samsung_990_PRO_S73WNJ0X123456-part1"]
device_size = 107374182400  # 100 GiB (ignored for raw block devices; actual size is queried)
expected_records = 50000000
```

#### Low-RAM deployment with on-disk index

```toml
listen_addr = "0.0.0.0:3300"
enable_remote_bind = true  # required for any non-loopback bind — only safe on a private network
device_paths = ["/dev/disk/by-id/nvme-Samsung_990_PRO_S73WNJ0X123456-part1"]
device_size = 107374182400  # 100 GiB

[index]
backend = "redb"
redb_path = "/data/teraslab-index.redb"
redb_dah_path = "/data/teraslab-dah.redb"
redb_unmined_path = "/data/teraslab-unmined.redb"
redb_cache_size = 268435456  # 256 MiB
```

This uses the redb on-disk index, keeping RAM usage under 512 MiB regardless of record count. See [Index backends](#index-backends) for details on tradeoffs.

#### Full configuration reference

```toml
# --- Network ---
# Defaults are loopback (127.0.0.1:3300 / 127.0.0.1:9100). Binding any
# non-loopback address requires enable_remote_bind = true or the server
# refuses to start (ConfigError::RemoteBindRefused). See "Security and
# access control" below.
listen_addr = "0.0.0.0:3300"       # TCP binary protocol (default 127.0.0.1:3300)
http_listen_addr = "0.0.0.0:9100"  # HTTP observability endpoints (default 127.0.0.1:9100)
enable_remote_bind = true          # default false; must be true for any non-loopback bind above
max_connections = 1024              # Max concurrent TCP connections
max_connections_per_ip = 64         # Per-source-IP connection cap (NAT'd client fleets share one)
max_batch_size = 8192               # Max items per batch request
max_active_streams_per_connection = 64  # Max in-progress blob uploads one connection may hold open (0 disables)
stream_idle_timeout_secs = 60       # Idle blob-upload stream reaped after this many secs (0 disables reaper)

# --- Security and access control (all default to off/unset) ---
enable_admin_endpoints = false      # mount the gated /debug/* and /admin/* HTTP routes + admin opcodes
admin_token = ""                    # required when enable_admin_endpoints = true; bearer token for every gated request
                                    # (overridable via the TERASLAB_ADMIN_TOKEN env var)
strict_auth = true                  # default true: refuse to start a clustered config (node_id > 0 OR
                                    # replication_factor > 1) without a cluster_secret
max_inflight_request_bytes = 268435456  # 256 MiB aggregate in-flight request memory; exhaustion → ERR_RATE_LIMITED (31)

# --- Storage ---
# Both raw block devices and regular file paths are supported.
# For block devices the actual kernel-reported size is always used and
# device_size is ignored. For regular files, device_size is only used to
# grow a new (or smaller) file — existing data is never truncated.
device_paths = ["teraslab-data.dat"]  # Raw device or file path(s)
device_size = 1073741824              # Bytes per device (regular files only; block devices are auto-detected)
device_alignment = 4096               # I/O alignment (4096 for NVMe/SSD)

# --- Recovery ---
redo_log_size = 67108864              # Redo log size in bytes (64 MiB)
redo_log_path = "teraslab-data.dat.redo"  # Optional explicit redo log path
recovery_missing_primary_tolerance = 65536  # Max MissingPrimary replay failures tolerated during
                                      # startup recovery before aborting (default 65536)

# --- Deletion & tombstones (on by default; see "Deletion & tombstones" below) ---
tombstones_enabled = true             # default TRUE: the engine writes a durable deletion tombstone on
                                      # every physical record delete and recovery reconstructs the index.
                                      # When true, startup provisions a `.tombstone` device file +
                                      # the redb tombstone index ([index] redb_tombstone_path).
tombstone_region_size = 67108864      # On-device tombstone log region size in bytes (default 64 MiB).
                                      # Unlike the redo log it is NOT reset on checkpoint; bounded only
                                      # by GC compaction (when gc is enabled).
# tombstone_log_path = "..."          # Optional explicit tombstone log path
                                      # (default: first device path + ".tombstone")
# last_durable_height_path = "..."    # Optional path for the durable node-height file
                                      # (default: index_snapshot_path + ".height"). ALWAYS maintained,
                                      # independent of the tombstone flags.
tombstone_reconciliation_enabled = false  # SOAK-GATED, default FALSE: tombstone-driven migration
                                      # reconciliation. When false, migration behaves as the pre-Phase-8 path.
tombstone_gc_enabled = false          # SOAK-GATED, default FALSE: bounded-retention tombstone GC + the
                                      # coupled rejoin-eligibility gate. When false, tombstones are retained
                                      # unboundedly and the rejoin gate is inert.
rejoin_grace_blocks = 100000          # Max staleness (block heights) a rejoining node may carry before it
                                      # is forced into a full resync. Only consulted when tombstone_gc_enabled.
tombstone_gc_poll_interval_ms = 60000 # Cadence the background GC daemon evaluates the GC horizon
                                      # (default 60000 = 1 min). Only active when tombstone_gc_enabled.

# --- Index ---
index_snapshot_path = "teraslab-index.snap"
expected_records = 100000             # Hint for initial hash table sizing

# --- Index backend (optional, defaults to in-memory) ---
[index]
backend = "memory"                        # "memory" (default), "redb", or "file_backed"
redb_path = "teraslab-index.redb"         # Primary index redb file
redb_dah_path = "teraslab-dah.redb"       # DAH secondary index redb file
redb_unmined_path = "teraslab-unmined.redb" # Unmined secondary index redb file
redb_tombstone_path = "teraslab-tombstone.redb" # Deletion-tombstone lookup index (used regardless of backend; see "Deletion & tombstones")
redb_cache_size = 268435456               # redb page cache in bytes (256 MiB default)
file_backed_path = "teraslab-index.dat"   # mmap primary index file (only used when backend = "file_backed")

# --- Concurrency ---
lock_stripes = 65536                  # Per-transaction lock stripes (power of 2)

# --- Pruning ---
block_height_retention = 288          # Blocks to retain fully-spent mined records before DAH deletion
                                      # (reorg-safety window). Unmined txs are governed separately by the
                                      # client-supplied OP_QUERY_OLD_UNMINED cutoff, not by this knob.

# --- Cold data ---
blobstore_path = "./teraslab-blobstore" # Directory for large transaction blobs (cold data the client routes external via FLAG_EXTERNAL_BLOB)

# --- Cluster (set node_id > 0 to enable) ---
node_id = 0                           # 0 = single-node mode
swim_port = 3301                      # UDP port for SWIM membership protocol
seed_nodes = []                       # e.g. ["10.0.0.2:3301", "10.0.0.3:3301"]
replication_factor = 1                # 1 = no replication, 2 = master + 1 replica
swim_probe_interval_ms = 200          # SWIM heartbeat interval
swim_suspicion_timeout_ms = 5000      # Time before suspect node is declared dead
topology_propose_timeout_ms = 0       # 0 = max(swim_probe_interval_ms * 3, 500)
topology_debounce_ms = 0              # Debounce window for coalescing SWIM membership changes before
                                      # proposing a new topology term. 0 = derive from
                                      # max(swim_probe_interval_ms * 2, swim_suspicion_timeout_ms / 2)
cluster_secret = ""                   # Shared secret for HMAC-SHA256 SWIM + inter-node TCP auth
max_migration_threads = 16            # Max concurrent migration threads per topology change

# --- Replication durability ---
ack_policy = "auto"                   # "auto", "write_all", "write_majority", or "best_effort".
                                      # "best_effort" is rejected at startup when replication_factor > 1.
replication_timeout_ms = 3000         # Timeout for each replication batch ACK
replication_timeout_during_migration_ms = 30000  # Timeout floor for foreground replication ACKs while
                                      # local migration pressure is active (default 30000)
replication_degraded_mode = "reject"  # "reject" or "best_effort" when ack policy fails.
                                      # "best_effort" is rejected at startup when replication_factor > 1
                                      # (acknowledged writes could be lost if the master crashes before
                                      # replicas catch up), so status 5 (DEGRADED_DURABILITY) is only
                                      # reachable with RF = 1 — see the response-status table below.

# --- Migration performance ---
migration_pool_size = 128             # Parallel TCP connections per migration target
migration_batch_size = 500            # Records per baseline streaming batch
replica_lag_check_interval_secs = 30  # Interval between replica lag checks (0 to disable)
replica_lag_warn_threshold_ops = 10000 # Replica lag (ops) that degrades /health/ready

# --- Cluster identity / device pinning (optional) ---
# cluster_id   = "..."   # 32 hex chars (16 bytes); pins cluster membership
# device_id    = "..."   # 32 hex chars; startup refuses if the device's stored id mismatches
# advertise_addr = "..." # address peers should dial if different from listen_addr
```

### Security and access control

Several knobs gate whether the server starts at all and whether the admin surface exists. All default to the safe (closed) setting:

| Knob | Default | Effect |
|------|---------|--------|
| `enable_remote_bind` | `false` | Required to bind any non-loopback `listen_addr` / `http_listen_addr`. Without it a non-loopback bind fails at startup (`ConfigError::RemoteBindRefused`). Only safe on a trusted private network. |
| `enable_admin_endpoints` | `false` | Mounts the gated `/debug/*` and `/admin/*` HTTP routes and the admin opcodes. When `false`, those routes return 404. |
| `admin_token` | unset | Bearer token required on every gated HTTP request when `enable_admin_endpoints = true` (the config is rejected if the endpoints are enabled without a token). Settable via TOML or the `TERASLAB_ADMIN_TOKEN` env var. When both `enable_admin_endpoints` and `enable_remote_bind` are on, a minimum token length is enforced. |
| `strict_auth` | `true` | Refuses to start a clustered config (`node_id > 0` OR `replication_factor > 1`) without a `cluster_secret`. |
| `cluster_secret` | unset | Shared secret for HMAC-SHA256 SWIM + inter-node TCP auth. Required under `strict_auth` for clustered configs. With a secret set, the cluster-authority opcodes (including 104 `AdminDiagnoseKey` and 106 `AdminClusterHealth`) require HMAC-signed frames; unsigned clients get `CLUSTER_AUTH_FAILED` (27). |
| `max_connections_per_ip` | `64` | Per-source-IP connection cap. A NAT'd client fleet sharing one source IP hits this. |
| `max_inflight_request_bytes` | `256 MiB` | Aggregate in-flight request memory budget; exhaustion returns `RATE_LIMITED` (31). |

> **`strict_auth = false` opens an unauthenticated cluster path.** With `strict_auth = false` **and** no
> `cluster_secret`, the server runs in trusted-overlay mode and accepts inter-node opcodes
> (replica batches op 240, migration frames, SWIM/topology frames) **UNAUTHENTICATED** — any peer that can
> reach the data port can inject them (a per-peer rate-limited warning is logged, but the frame is still
> accepted). This is a production risk. Keep the default `strict_auth = true` **and** set a `cluster_secret`
> on any network you do not fully trust.

Other operationally significant knobs (all read, mostly with safe defaults): `max_stream_total_bytes`
(4 GiB per-connection streaming cap; env `TERASLAB_MAX_STREAM_TOTAL_BYTES`),
`replica_lag_warn_threshold_ops` (10000 — replica lag past this degrades `/health/ready`),
`device_id` / `cluster_id` (32-hex-char identifiers; a pinned `device_id` mismatch refuses startup),
`advertise_addr`, `blob_gc_interval_secs`, the `checkpoint_high_water` / `checkpoint_low_water` /
`checkpoint_poll_interval_ms` watermarks, and the `[observability]` block. Most migration/observability
knobs accept a `TERASLAB_*` environment override.

#### Wire-protocol size caps

The wire protocol enforces fixed caps (`src/protocol/opcodes.rs`): max frame 16 MiB
(`MAX_FRAME_SIZE`), max cold data per create item 4 MiB (`MAX_COLD_DATA_PER_ITEM`), max UTXO
hashes per create item 131072, max parent txids per create item 65536. Oversize requests are
rejected with `PAYLOAD_MALFORMED` (28).

### Cluster deployment (3 nodes, RF=2)

Node 1:
```toml
listen_addr = "0.0.0.0:3300"
enable_remote_bind = true  # required for the non-loopback bind — only safe on a private network
node_id = 1
swim_port = 3301
cluster_secret = "change-me-shared-cluster-secret"  # required: strict_auth (default on) rejects clustered configs without one
seed_nodes = ["10.0.0.2:3301", "10.0.0.3:3301"]
replication_factor = 2
device_paths = ["/dev/disk/by-id/nvme-Samsung_990_PRO_S73WNJ0X000001-part1"]
device_size = 107374182400
```

Node 2:
```toml
listen_addr = "0.0.0.0:3300"
enable_remote_bind = true  # required for the non-loopback bind — only safe on a private network
node_id = 2
swim_port = 3301
cluster_secret = "change-me-shared-cluster-secret"  # required: strict_auth (default on) rejects clustered configs without one
seed_nodes = ["10.0.0.1:3301", "10.0.0.3:3301"]
replication_factor = 2
device_paths = ["/dev/disk/by-id/nvme-Samsung_990_PRO_S73WNJ0X000002-part1"]
device_size = 107374182400
```

Node 3:
```toml
listen_addr = "0.0.0.0:3300"
enable_remote_bind = true  # required for the non-loopback bind — only safe on a private network
node_id = 3
swim_port = 3301
cluster_secret = "change-me-shared-cluster-secret"  # required: strict_auth (default on) rejects clustered configs without one
seed_nodes = ["10.0.0.1:3301", "10.0.0.2:3301"]
replication_factor = 2
device_paths = ["/dev/disk/by-id/nvme-Samsung_990_PRO_S73WNJ0X000003-part1"]
device_size = 107374182400
```

Nodes discover each other via SWIM protocol. Shards rebalance automatically when nodes join or leave. With RF=2, each shard has a master and one replica for fault tolerance.

### Docker

Build the image:
```bash
docker build -t teraslab -f teraslab-tests/docker/Dockerfile .
```

Run a single node:
```bash
docker run -p 3300:3300 -p 9100:9100 \
  -v /data:/data \
  -v /blobstore:/blobstore \
  teraslab --config /etc/teraslab/node.toml
```

Multi-node clusters via Docker Compose are in `teraslab-tests/docker/`:
```bash
cd teraslab-tests/docker
docker compose -f docker-compose.3node.yml up
```

Ports exposed per container:
| Port | Protocol | Purpose |
|------|----------|---------|
| 3300 | TCP | Client binary protocol |
| 3301 | UDP | SWIM membership |
| 9100 | HTTP | Observability |

## Load testing & profiling

A self-contained, reusable harness lives in `teraslab-tests/bench/`. It boots a
single node in Docker, drives a mixed over-the-wire workload, and scrapes the
server's Prometheus `/metrics` for per-stage pipeline histograms — so you can
attribute latency to a stage instead of guessing.

```bash
# Builds the image + loadgen if missing, runs both store-count variants.
teraslab-tests/bench/run_bench.sh

# Tunables (env): DUR (seconds), WORKERS (in-flight concurrency), RATE
# (target ops/s; default is effectively unthrottled to saturate the server),
# VARIANTS ("1store", "4store", or both).
DUR=60 WORKERS=48 VARIANTS="4store" teraslab-tests/bench/run_bench.sh
```

Files:
- `bench-1store.toml` / `bench-4store.toml` — standalone single-node configs
  (no cluster, `strict_auth=false`, admin off). They differ only in
  `device_paths`: one device vs four, i.e. `num_stores = 1` vs `4`. Four stores
  means four independent redo logs that fsync in parallel.
- `run_bench.sh` — orchestrates a clean boot per variant (fresh volume so every
  `/metrics` counter starts at 0), runs `teraslab-loadgen`, and writes results
  to `teraslab-tests/results/bench_<timestamp>/` (gitignored):
  `<variant>_loadgen.txt`, `<variant>_metrics.txt`, `<variant>_status.json`,
  `<variant>_server.log`.

The load generator is `client/rust/src/bin/loadgen.rs` (`teraslab-loadgen`),
which issues a mixed create / spend / read / set_mined workload via the Rust
client.

### Reading the pipeline metrics

The key histograms in `<variant>_metrics.txt`:

| Metric | What it tells you |
|--------|-------------------|
| `teraslab_<op>_latency_ns` | End-to-end server handler time per op (create/spend/get/set_mined) |
| `teraslab_redo_flush_latency_ns` | The redo fsync (group-commit) cost — usually the write bottleneck |
| `teraslab_redo_entries_per_flush` | Group-commit coalescing degree. `≈ flush_count` means one fsync per op (no coalescing) |
| `teraslab_lock_wait_ns` | Stripe-lock contention (empty ⇒ not lock-bound) |

### Findings (single-node, mixed workload, macOS Docker Desktop)

**1. Single-item RPCs (`--batch 1`) — fsync-floor bound.** 32 workers:

| | 1 store | 4 stores |
|---|---|---|
| Throughput | 241 ops/s | **290 ops/s** (+20%) |
| Write latency p50 (create/spend) | 4.19 ms | **2.10 ms** |
| Redo fsync mean / p99 | 1.54 / 4.19 ms | 1.22 / 2.10 ms |
| Read (get) p50 | 16 µs | 16 µs |

`lock_wait` is empty (not lock-bound) and `flush_count ≈ write_ops`,
`entries_per_flush ≈ 1.5` — i.e. **one fsync per op, no group-commit coalescing
across concurrent single-item RPCs**. Write throughput is gated by
`write_concurrency / fsync_latency`. Reads are ~free (in-memory index).
Multi-device helps (4 independent redo logs fsync in parallel) even on one
shared volume.

**2. Batched RPCs (`--batch 256`) — fsync floor removed, now device-I/O bound.**
32 workers:

| | 1 store | 4 stores |
|---|---|---|
| Throughput | ~16–17k ops/s | ~17k ops/s |
| `entries_per_flush` | ~300 | ~87 |
| `create` latency / batch | ~0.7 ms | — |
| `spend` latency / 256-item batch | ~20 ms | — |
| `set_mined` latency / 256-item batch | ~14 ms | — |

Batching amortizes the fsync across ~300 entries/flush, lifting throughput ~65×
(250 → ~17k). The bottleneck then moves to the **read-modify-write device I/O**
on `spend`/`set_mined` (each reads the record from the device, mutates, writes
back). At this point the server uses only **~0.75 of 8 cores** (RAM ~150 MiB,
client ~1% CPU) — it is **I/O-wait bound, not CPU/lock/client bound**.

> ⚠️ **Absolute throughput here is a floor, not the target**, and on this host
> it is gated by the **macOS Docker Desktop virtual disk**, which behaves like an
> effective **queue depth of ~1**: concurrent `pread`/`pwrite` do not overlap at
> the virtualization layer (~1–1.5 ms fsync, slow random reads, vs <100 µs on
> NVMe). The read fan-out in `handle_spend_batch`/`handle_set_mined_batch`
> (mirroring `get_batch`) is correct and tested, but yields **no measurable gain
> on this disk** because the device serializes the I/O it issues — CPU stays
> pinned at ~0.75 core regardless. On bare-metal NVMe (deep queues, parallel
> channels) that fan-out, plus separate `device_paths` per physical device and
> large client batches, is what unlocks the design target. Use this harness for
> **relative** comparisons and **stage attribution**, not headline numbers — and
> to actually measure the I/O-parallel paths, run it on real NVMe.

**3. Optional data-device cache (`[cache]`) — see `docs/WRITE_CACHE_SPEC.md`.**
Batched (`--batch 256`), 32 workers, 1 GiB cache covering the device:

| metric (server-side, per 256-item batch) | no cache | write-through | write-back |
|---|---|---|---|
| `spend` latency | 21.3 ms | 20.5 ms | **7.3 ms** |
| `set_mined` latency | 13.3 ms | 12.2 ms | **6.3 ms** |
| `get` latency | 1.54 ms | 0.71 ms | **0.56 ms** |
| overall throughput | ~17k ops/s | ~17k ops/s | ~17k ops/s |

The cache does what it should at the device layer: write-through serves the
read-modify-write **reads** from RAM (~2–3× lower `get`); write-back also defers
the **writes**, cutting `spend`/`set_mined` ~2–3×. But **overall throughput is
unchanged** — once the data device is cached, the limiter moves to (a) the
redo-log fsync, which is correctly **never** cached (durability), and (b) the
loadgen's single shared `tokio::Mutex` work-queue (a client-side artifact). So
the cache is a **latency** win on read-modify-write paths; raising end-to-end
throughput needs a faster WAL path and a lock-free client.

## Wire protocol

TeraSlab uses a compact binary protocol over TCP. Every request and response is a length-prefixed frame:

```
[total_length: u32][request_id: u64][op_code: u16][flags: u16][payload: ...]
```

All operations are batch-oriented. Single-item operations are batches of size 1.

### Operations

**Mutations:**

| Opcode | Name | Description |
|--------|------|-------------|
| 1 | `SpendBatch` | Mark UTXOs as spent |
| 2 | `UnspendBatch` | Reverse a spend |
| 3 | `SetMinedBatch` | Attach block entry to transaction |
| 4 | `CreateBatch` | Create transaction records with UTXO slots |
| 5 | `FreezeBatch` | Freeze UTXOs (prevent spending) |
| 6 | `UnfreezeBatch` | Unfreeze UTXOs |
| 7 | `ReassignBatch` | Replace frozen UTXO hash |
| 8 | `SetConflictingBatch` | Mark transaction as conflicting |
| 9 | `SetLockedBatch` | Lock transaction (prevent all spending) |
| 10 | `PreserveUntilBatch` | Prevent pruning until block height |
| 11 | `DeleteBatch` | Delete transaction records |
| 12 | `MarkLongestChainBatch` | Mark block entry as on longest chain |
| 13 | `RemoveConflictingChildBatch` | Remove children from parents' conflicting-children lists (backs Teranode's `RemoveFromConflictingChildren`) |

**Reads:**

| Opcode | Name | Description |
|--------|------|-------------|
| 20 | `GetBatch` | Fetch transaction data with field mask |
| 21 | `GetSpendBatch` | Check UTXO spend status by txid/vout/hash (lightweight) |

**Pruner:**

| Opcode | Name | Description |
|--------|------|-------------|
| 30 | `QueryOldUnmined` | Find unmined transactions before height |
| 31 | `PreserveTransactions` | Prevent pruning of parent transactions |
| 32 | `ProcessExpiredPreservations` | Delete expired preserved transactions |
| 33 | `QueryConflicting` | Return all txids currently carrying the CONFLICTING flag (backs Teranode's `GetConflictingTxIterator`) |

**Streaming (large cold data upload):**

| Opcode | Name | Description |
|--------|------|-------------|
| 200 | `StreamChunk` | Upload a chunk of blob data for a pending create |
| 201 | `StreamEnd` | Finalize a blob upload |

**Admin:**

| Opcode | Name | Description |
|--------|------|-------------|
| 100 | `GetPartitionMap` | Fetch shard-to-node mapping (cluster) |
| 101 | `Health` | Health check |
| 102 | `Ping` | Latency measurement |
| 103 | `GetCommittedTopology` | Fetch the latest quorum-committed topology |
| 104 | `AdminDiagnoseKey` | Diagnose per-key routing and local shard state. **Cluster-authority opcode:** when a `cluster_secret` is configured, requires an HMAC-signed frame; unsigned clients get `CLUSTER_AUTH_FAILED` (27). |
| 105 | `PartitionVersionReport` | Inter-node shard version report after topology commit |
| 106 | `AdminClusterHealth` | Cluster readiness snapshot for clients/tests. **Cluster-authority opcode:** when a `cluster_secret` is configured, requires an HMAC-signed frame; unsigned clients get `CLUSTER_AUTH_FAILED` (27). |
| 107 | `Hello` | Protocol-version handshake; empty request, response is the server's 2-byte LE protocol version (pre-v2 servers reject with `OPCODE_UNSUPPORTED` or `INTERNAL`) |
| 108 | `GetNodeHeight` | Return this node's `last_durable_height` as a 4-byte LE payload. HMAC-gated as an inter-node opcode (mirrors `GetPartitionMap`) |

**Inter-node replication, migration, and topology:**

| Opcode | Name | Description |
|--------|------|-------------|
| 240 | `ReplicaBatch` | Send a batch of replica operations |
| 241 | `ReplicaAck` | Acknowledge a replica batch |
| 242 | `MigrationComplete` | Verify and complete a single shard migration |
| 243 | `MigrationBatchComplete` | Verify and complete multiple shard migrations |
| 244 | `MigrationTransferRequest` | Pull-based migration repair: a target asks each source to re-run the outbound migration for the listed shards (idempotent, epoch-validated) |
| 250 | `Heartbeat` | Inter-node heartbeat |
| 251 | `TopologyPropose` | Propose a new topology term |
| 252 | `TopologyVote` | Vote for a topology term |
| 253 | `TopologyCommit` | Commit a quorum-approved topology term |
| 255 | `IncrementSpentExtraRecs` | Compatibility no-op |

### Error codes

| Code | Name | Meaning |
|------|------|---------|
| 0 | `OK` | Success |
| 1 | `TX_NOT_FOUND` | Transaction does not exist |
| 2 | `UTXO_HASH_MISMATCH` | Provided hash doesn't match stored hash |
| 3 | `ALREADY_SPENT` | UTXO is already spent (error data: 36-byte existing spending data) |
| 4 | `ALREADY_FROZEN` | UTXO is already frozen |
| 5 | `UTXO_NOT_FROZEN` | Expected frozen UTXO but it is not frozen |
| 6 | `INVALID_SPEND` | Spending data targets a deleted/pruned UTXO (error data: 36-byte stored spending data) |
| 7 | `FROZEN` | Cannot spend a frozen UTXO |
| 8 | `CONFLICTING` | Transaction is marked conflicting |
| 9 | `LOCKED` | Transaction is locked |
| 10 | `COINBASE_IMMATURE` | Coinbase UTXO not yet spendable (error data: 4-byte required height) |
| 11 | `VOUT_OUT_OF_RANGE` | UTXO index exceeds slot count |
| 12 | `ALREADY_EXISTS` | Duplicate transaction creation |
| 13 | `FROZEN_UNTIL` | Reassignment cooldown not met (error data: 4-byte spendable-at height) |
| 14 | `REDIRECT` | Shard owned by another node (payload contains target address) |
| 15 | `NO_QUORUM` | Cluster quorum not met, mutations rejected |
| 16 | `STREAM_NOT_FOUND` | Blob stream not found for this txid on this connection |
| 17 | `BLOB_NOT_FOUND` | Blob not found in blobstore (EXTERNAL_BLOB flag set but no upload) |
| 18 | `STREAM_OFFSET_MISMATCH` | Chunk offset does not match expected stream position |
| 19 | `MIGRATION_IN_PROGRESS` | Shard being migrated, retry shortly |
| 20 | `REPLICATION_FAILED` | Required replication ACKs not received within timeout |
| 21 | `MIGRATION_MANIFEST_REQUIRED` | Migration completion omitted the required manifest, including for empty shards |
| 22 | `MIGRATION_MANIFEST_MISMATCH` | Migration manifest hash/count does not match received shard data |
| 23 | `TOPOLOGY_PERSIST_FAILED` | Topology vote was accepted in memory but could not be fsynced |
| 24 | `STALE_EPOCH` | Sender used an obsolete topology epoch |
| 25 | `CLUSTER_NOT_READY` | Node has not observed its first quorum-committed topology |
| 26 | `INDEX_DEGRADED` | Required secondary index is unavailable after startup rebuild/open failure |
| 27 | `CLUSTER_AUTH_FAILED` | Inter-node HMAC frame authentication failed |
| 28 | `PAYLOAD_MALFORMED` | Request payload failed wire-decode (truncated header, malformed count prefix, oversize batch, invalid UTF-8); do not retry blindly |
| 29 | `OPCODE_UNSUPPORTED` | Dispatcher does not recognise the opcode (frame itself was decodable) |
| 30 | `STORAGE_IO` | Device read/write failure surfaced from the engine or blobstore; likely to recur until the operator resolves the underlying issue |
| 31 | `RATE_LIMITED` | Listener's aggregate in-flight request memory limit exhausted; retry after backoff |
| 32 | `NOT_CLUSTERED` | Cluster control opcode sent to a server running in single-node mode; do not retry against this server |
| 33 | `INVARIANT_VIOLATION` | Wire-protocol invariant violated by the caller (e.g. upper 48 bits set in a shard-carrying `request_id`) |
| 34 | `STREAM_INVARIANT` | Stream-protocol invariant violated (chunk offset mismatch, byte counter overflow, stream byte cap exceeded) |
| 35 | `DELETED_CHILDREN` | Idempotent re-spend rejected: the child txid is present in the parent's deleted-children audit list (error data: 1-byte child_count) |
| 36 | `NOT_DUE` | A guarded DAH-sweep delete re-validated the record under lock and found it no longer due (e.g. a concurrent `PreserveUntilBatch`); the record is kept, not deleted. Produced only by the internal `ProcessExpiredPreservations` sweep — a direct client `DeleteBatch` is unconditional and never returns this |
| 37 | `MIGRATION_TARGET_NOT_READY` | A migration completion/transfer handshake arrived stamped with a topology epoch the receiver has not yet activated; retryable (the target will activate the term shortly — never treat as a completed handoff) |
| 255 | `INTERNAL` | Unexpected server error |

### Response status codes

| Code | Name | Meaning |
|------|------|---------|
| 0 | `OK` | Request succeeded |
| 1 | `ERROR` | Request failed with an error payload |
| 2 | `NOT_FOUND` | Requested object was not found |
| 3 | `REDIRECT` | Retry against the shard owner in the payload |
| 4 | `PARTIAL_ERROR` | Batch partially succeeded; per-item errors are encoded in the payload |
| 5 | `DEGRADED_DURABILITY` | Local mutation succeeded, but best-effort replication did not satisfy the configured ACK policy. Only reachable with `replication_factor = 1` and a best-effort mode — startup rejects best-effort when RF > 1, so a validated multi-replica config never emits this status. |

## HTTP observability

The HTTP server (default `127.0.0.1:9100`) exposes health checks, Prometheus metrics, and debug endpoints.

> **Only five routes are public:** `/metrics`, `/health/live`, `/health/ready`, `/status`, and the
> `/ui/*` assets. **Every `/debug/*`, `/admin/*`, and `/ws/top` route below is gated** — it is mounted
> only when `enable_admin_endpoints = true` **and** a non-empty `admin_token` is configured, and every
> request to a gated route must carry `Authorization: Bearer <admin_token>`. With the defaults
> (`enable_admin_endpoints = false`), the gated routes return **404**. Set the token in TOML
> (`admin_token = "..."`) or via the `TERASLAB_ADMIN_TOKEN` env var; the `teraslab-cli`'s `--admin-token`
> flag supplies it for CLI commands that hit gated endpoints. The `curl` examples below omit the
> `Authorization` header for brevity — add `-H "Authorization: Bearer $TOKEN"` to each gated call.

### Health checks

```bash
# Liveness (is the process running?)
curl http://localhost:9100/health/live

# Readiness (is the index loaded and ready to serve?)
curl http://localhost:9100/health/ready
```

### Prometheus metrics

```bash
curl http://localhost:9100/metrics
```

Exports counters for every operation type:
- `teraslab_spends_attempted_total`, `teraslab_spends_succeeded_total`, `teraslab_spends_failed_total`
- Same pattern for `unspends`, `creates`, `set_mined`, `freezes`, `gets`, etc.

### Debug endpoints (gated — requires `enable_admin_endpoints` + bearer `admin_token`)

```bash
# Index statistics (load factor, entry count, capacity)
curl http://localhost:9100/debug/index

# Allocator state
curl http://localhost:9100/debug/freelist

# Redo log position
curl http://localhost:9100/debug/redo

# Inspect a specific record by txid (hex)
curl http://localhost:9100/debug/records/<txid_hex>

# Get/set log level at runtime
curl http://localhost:9100/debug/log-level
curl -X PUT http://localhost:9100/debug/log-level -d 'DEBUG'
```

### Status overview

```bash
# Cluster health overview (JSON)
curl http://localhost:9100/status
```

### Admin endpoints (gated — requires `enable_admin_endpoints` + bearer `admin_token`)

```bash
# Shard migration status
curl http://localhost:9100/admin/migration_status

# List all cluster nodes with shard counts
curl http://localhost:9100/admin/nodes

# Memory breakdown
curl http://localhost:9100/admin/memory

# Record inventory summary
curl http://localhost:9100/admin/records

# Replication configuration and status
curl http://localhost:9100/admin/replication

# Full metrics snapshot (like Unix top)
curl http://localhost:9100/admin/top

# Drain shards from this node (graceful shutdown prep)
curl -X PUT http://localhost:9100/admin/quiesce

# Drain a specific node by ID
curl -X PUT http://localhost:9100/admin/drain/2

# Trigger cluster rebalance
curl -X PUT http://localhost:9100/admin/rebalance
```

### WebSocket (gated — requires `enable_admin_endpoints` + bearer `admin_token`)

```bash
# Real-time metrics push (updates every second)
wscat -c ws://localhost:9100/ws/top
```

### Web UI

An embedded admin dashboard is served at `http://localhost:9100/ui/`. It provides a real-time view of node status, shard distribution, and key metrics.

## Client libraries

### Go client

```go
import teraslab "github.com/icellan/teraslab/client/go"

client, err := teraslab.New(ctx, teraslab.ClientConfig{
    Addr: "localhost:3300",
})
defer client.Close()

// Create a transaction with 3 UTXOs
res, err := client.CreateBatch(ctx, []teraslab.CreateItem{{
    TxID:        txid,
    TxVersion:   1,
    Fee:         500,
    SizeInBytes: 225,
    UtxoHashes:  []teraslab.UtxoHash{hash0, hash1, hash2},
    BlockHeight: 800000,
}})

// Spend a UTXO — params come first, then the items
results, err := client.SpendBatch(ctx, teraslab.SpendBatchParams{CurrentBlockHeight: 800001}, []teraslab.SpendItem{{
    TxID:         txid,
    Vout:         0,
    UtxoHash:     hash0,
    SpendingData: spendingData, // 36 bytes: spending txid + vout
}})
```

Full documentation in [`client/go/README.md`](client/go/README.md).

### Rust client

```rust
use teraslab_client::{Client, ClientConfig};

let client = Client::new(ClientConfig {
    addr: Some("localhost:3300".to_string()),
    ..Default::default()
}).await?;

client.create_batch(&[CreateItem {
    txid,
    tx_version: 1,
    fee: 500,
    size_in_bytes: 225,
    utxo_hashes: vec![hash0, hash1, hash2],
    block_height: 800000,
    ..Default::default()
}]).await?;
```

Source in [`client/rust/`](client/rust/).

## Clustering

TeraSlab uses a SWIM protocol for membership and failure detection, with consistent hashing across 4096 shards.

### Shard assignment

Each transaction is mapped to a shard by its txid: `shard = u16_le(txid[0..2]) & 0x0FFF`. Shards are distributed across nodes via round-robin assignment. With replication factor 2, each shard has a primary master and one replica.

### Failure detection

Nodes probe each other via UDP (SWIM protocol). If a node fails to respond to direct and indirect probes within the suspicion timeout, it is declared dead and the shard table recomputes. Shards owned by the failed node are migrated to surviving nodes.

### Quorum

In a multi-node cluster, mutations require quorum (majority of the peak observed cluster size must be alive). This prevents split-brain: an isolated node that was previously part of a 3-node cluster will reject writes until it can see at least 2 nodes. The peak cluster size is persisted to disk so this safety property survives restarts.

### Migration

When the shard table changes (node join/leave), data migrates automatically:
- Master migrations: shard data streams from old master to new master
- Replica backfill: newly assigned replicas receive shard data from the current master
- During migration, reads on the old master continue serving locally. Reads and writes routed to the new master while inbound data is still pending return `MIGRATION_IN_PROGRESS` quickly; clients should poll/retry with backoff instead of treating this as a permanent miss.

## Architecture

### On-disk layout

Each transaction occupies a contiguous region on the block device:

```
[TxMetadata: 320 bytes][UtxoSlot 0: 73 bytes][UtxoSlot 1: 73 bytes]...[UtxoSlot N-1: 73 bytes]
```

**TxMetadata** (320 bytes, compile-asserted, 64-byte aligned) contains: txid, version, locktime, fee, size, extended size, flags (conflicting, locked, external, coinbase, last_spent_all), block entries (up to 3 inline, overflow stored separately), spending height, creation timestamp, generation counter, update timestamp, unmined_since, delete_at_height, preserve_until, reassignment tracking, external storage reference, conflicting children tracking, and a trailing CRC32 over the whole header.

**UtxoSlot** (73 bytes each): 32-byte hash, 1-byte status (unspent/spent/frozen/pruned), 36-byte spending data (spending txid + vout), 4-byte CRC32 (torn-write protection per slot — BC-02 / F-X-007). Slots are pre-allocated at full size during creation. A spend's *logical* mutation is the 41-byte status+spending+CRC region, but the production write path (`io.rs` `write_utxo_slot_direct`) rewrites the full 73-byte slot in place and (`io.rs` `write_metadata_direct`) rewrites the full 320-byte metadata header (generation, counters, timestamps) — not just a 41-byte footer. On `DirectDevice` (`O_DIRECT`), each write amplifies to the device's sector size (4096 bytes on most NVMe drives) regardless.

### Tiered storage

- **Hot path** (NVMe): Metadata + UTXO slots. All spend/setMined/freeze operations touch only this tier.
- **Cold data** (filesystem blob store): Transaction inputs, outputs, and inpoints. Placement is **client-driven**: the client sets the `FLAG_EXTERNAL_BLOB` request flag to route cold data to the external blob store (pre-uploaded via the streaming chunk protocol); without the flag, cold data is written inline in the same NVMe allocation as the hot record. The server does not second-guess this choice — by the time it receives the frame, the client has already decided whether to stream the payload externally or inline it on the wire (inline payloads are bounded by `MAX_COLD_DATA_PER_ITEM` at the wire decoder). The `tier_for_size` / `INLINE_THRESHOLD` (8 KiB) helpers in `src/storage/tiers.rs` are an **advisory size guideline** for clients, not a server-enforced threshold. The earlier separate-device middle tier is not enabled because current metadata has no durable offset/length fields for it.

### Crash recovery

A write-ahead redo log records all mutations. On crash recovery:
1. Open the redo log and scan for the last checkpoint
2. Replay all entries after the checkpoint
3. Resume normal operation

Replay is **idempotent and identity-checked**: each replay handler re-validates
the same precondition the live operation enforced before mutating. Some handlers
(unspend, freeze, unfreeze, reassign) write their redo entry *before* the engine
validates the request, so a request the engine then rejects (e.g.
`UTXO_HASH_MISMATCH`, `UTXO_NOT_FROZEN`) can still leave a durable redo entry.
To prevent a rejected operation from becoming a durable mutation after a crash,
those entries carry the slot's prior `utxo_hash` (the `*V2`/`*V3` redo variants)
and replay skips any entry whose carried identity no longer matches the on-disk
slot — exactly mirroring the live rejection. Replaying an entry that was already
applied is a no-op via generation guards and slot-state checks.

The redo log is a fixed-size **linear** log on a separate device file (not a circular buffer): `write_pos` advances monotonically and never wraps in place. When the log fills before the next checkpoint, appends return `RedoError::LogFull` and writers stall until the periodic checkpoint task snapshots engine state and resets `write_pos` back to the start. Size `redo_log_size` so the log holds the mutations produced between checkpoints under peak load.

### Deletion & tombstones

`tombstones_enabled` defaults to **`true`**. With it on, every physical record delete also appends a durable **deletion tombstone**, and startup provisions two extra artifacts: a `.tombstone` device file (sibling to the `.redo` log, default path = first device path + `.tombstone`, sized by `tombstone_region_size`, default 64 MiB) holding the append-only on-device tombstone log, and the redb **tombstone lookup index** at `[index] redb_tombstone_path` (default `teraslab-tombstone.redb`). The on-device log is the durable source of truth; the redb file is a derived index rebuilt from the log on recovery. On startup, recovery reconstructs the index from the log (R1) and self-purges any record this node resurrected for a key the cluster authoritatively deleted (R2). Unlike the redo log, the tombstone log is **not** reset on checkpoint — it is bounded only by GC compaction (see below).

The node's `last_durable_height` is persisted to a tiny CRC-protected file (`last_durable_height_path`, default = index snapshot path + `.height`). This is **always maintained**, independent of the tombstone flags.

Two related capabilities are **soak-gated and ship off by default**:

- `tombstone_reconciliation_enabled` (default `false`) — tombstone-driven migration reconciliation (Phase 8). When off, migration completion behaves byte-identically to the pre-Phase-8 path.
- `tombstone_gc_enabled` (default `false`) — bounded-retention tombstone GC plus its coupled rejoin-eligibility gate. When off, tombstones are retained unboundedly and a catching-up node is admitted as before. When on, a tombstone becomes GC-eligible once `min_member_finalized_height − deletion_height ≥ rejoin_grace_blocks` (default `100000`), and a node more than `rejoin_grace_blocks` behind the cluster tip is refused incremental rejoin and full-resynced. The GC daemon evaluates the horizon every `tombstone_gc_poll_interval_ms` (default 60000 ms).

Enable the two soak-gated flags only after CI soak validates convergence, no-loss, and no-resurrection.

## Index backends

TeraSlab supports three index backends for the primary index and secondary indexes (DAH, unmined): `memory` (default), `redb`, and `file_backed`. The backend is selected at startup via configuration and cannot be changed at runtime. The two documented, supported backends are `memory` and `redb`; `file_backed` (`backend = "file_backed"`) is also accepted — it uses a memory-mapped persistent primary index with in-memory secondaries and redo-based crash recovery — but it is not the recommended path and is excluded from the index export/import tooling (its secondaries can only be rebuilt from a device scan).

### In-memory (default)

The default backend stores the index in a Robin Hood hash table backed by anonymous `mmap`. This is the fastest option, targeting the 10M+ ops/sec design ceiling on `MemoryDevice` (not yet measured on NVMe — see [Performance methodology](#performance-methodology)). Each entry occupies a **64-byte hash-table bucket** (one cache line, compile-asserted). The table is sized for a 0.7 target load factor and rounded up to a power-of-two capacity, so effective RAM per record is 64 ÷ load factor (≥ ~91 bytes/record, higher right after a power-of-two resize). For 100M records, budget on the order of 9-18 GB of RAM for the index alone depending on where the count falls relative to the next power of two.

On clean shutdown the index is snapshotted to `index_snapshot_path` and restored on next startup. On crash, the index is rebuilt from the device scan + redo log replay.

No configuration is needed — this is the default.

### On-disk via redb

The `redb` backend stores all three indexes (primary, DAH, unmined) in [redb](https://github.com/cberner/redb) B+ tree database files on disk. This trades throughput for dramatically lower RAM requirements — the index memory footprint drops to the redb page cache size (default 256 MiB) regardless of record count.

Use this backend when:
- The host has limited RAM (e.g., <16 GB) but fast NVMe storage
- You are running many TeraSlab instances on the same host
- You need crash-durable indexes without snapshot/rebuild cycles

#### Configuration

```toml
[index]
backend = "redb"
redb_path = "/data/teraslab-index.redb"
redb_dah_path = "/data/teraslab-dah.redb"
redb_unmined_path = "/data/teraslab-unmined.redb"
redb_cache_size = 268435456  # 256 MiB (default)
```

| Setting | Default | Description |
|---------|---------|-------------|
| `backend` | `"memory"` | `"memory"` or `"redb"` |
| `redb_path` | `teraslab-index.redb` | Primary index database file |
| `redb_dah_path` | `teraslab-dah.redb` | DAH (delete-at-height) secondary index file |
| `redb_unmined_path` | `teraslab-unmined.redb` | Unmined secondary index file |
| `redb_cache_size` | `268435456` (256 MiB) | Page cache size in bytes. Larger cache = more data kept in RAM = faster reads |

#### Tradeoffs

| | In-memory | redb |
|--|-----------|------|
| **Throughput** | 10M+ ops/sec target on `MemoryDevice`; lower on NVMe (not yet measured) | ~100K-500K ops/sec (I/O bound) |
| **RAM per 10M records** | ~0.9-1.2 GB (64-byte buckets at 0.7 load factor, power-of-two capacity) | ~256 MB (page cache only) |
| **Crash recovery** | Rebuild from device + redo replay | Instant (already on disk) |
| **Startup time** | Seconds (snapshot restore) to minutes (full rebuild) | Instant (open existing files) |
| **Snapshot needed** | Yes (`index_snapshot_path`) | No (crash-durable by default) |
| **SSD write overhead** | None (index is in RAM) | B+ tree writes per mutation |

#### Error recovery

redb startup is fail-closed for the primary index. The server first attempts to open the configured primary redb file. If that fails, it rebuilds the primary redb index from a device scan. If the rebuild also fails, startup exits without deleting the existing redb file, so operators can capture diagnostics before an explicit rescan or repair.

Secondary redb indexes (DAH and unmined) are isolated from the primary. If a secondary redb file cannot be opened, the node starts in degraded readiness with an empty in-memory replacement for that secondary; endpoints that depend on the missing secondary return `INDEX_DEGRADED` until the operator fixes the underlying issue and restarts. The server does not silently delete corrupt redb files or automatically fall back to a fully in-memory backend for the primary.

#### Migration between backends

Use `teraslab-cli` to export and import index data between backends. Both commands are OFFLINE: stop the server first (redb's file lock refuses a live database). They read the index layout from the same TOML config the server uses.

```bash
# Export current index (memory or redb backend) to a portable snapshot
teraslab-cli export-index --config /etc/teraslab/server.toml --output /tmp/index-export.snap

# Import into a redb-configured instance
teraslab-cli import-index --config /etc/teraslab/server-redb.toml --input /tmp/index-export.snap
```

The export format is the same binary snapshot format used for in-memory index persistence, making it backend-agnostic. For the memory backend, export reads the on-disk index snapshot (shut the server down cleanly first so it is current) and import persists the result back to `index_snapshot_path`. The `file_backed` backend is not supported (its secondaries can only be rebuilt from a device scan).

If an `import-index` is interrupted mid-write, it leaves an import-in-progress sentinel and the **next server startup deliberately refuses to start** (rather than open a partially-imported redb file) until the sentinel is cleaned up; the startup error includes the remediation steps.

## Admin CLI

The `teraslab-cli` binary provides operator commands that consume the HTTP observability endpoints and binary wire protocol. Supports both table-formatted and JSON output.

```bash
# --addr must include the scheme (it is used as-is to build request URLs); default is http://localhost:9100
./target/release/teraslab-cli --addr http://localhost:9100 <command>
```

Commands that hit gated `/debug/*` or `/admin/*` endpoints (e.g. `nodes`, `memory`, `records`,
`record`, `index`, `replication`, `redo`, `rebalance`, `drain`, `log-level`, `top`) require the
server to have `enable_admin_endpoints = true` and an `admin_token`; pass the matching token with
`--admin-token <token>` (or `TERASLAB_ADMIN_TOKEN`). Only `status`, `healthcheck`, and `bench`
(against the public surface) work without it.

Available commands:

| Command | Description |
|---------|-------------|
| `status` | Cluster health overview |
| `nodes` | List cluster nodes with shard counts |
| `shards` | Shard distribution details |
| `storage` | Storage utilization |
| `memory` | Memory breakdown |
| `records` | Record inventory summary |
| `record <txid>` | Inspect a specific record by txid |
| `index` | Index statistics (load factor, capacity) |
| `replication` | Replication configuration and status |
| `redo` | Redo log position and utilization |
| `rebalance` | Trigger cluster rebalance |
| `drain <node_id>` | Drain shards from a node |
| `log-level [LEVEL]` | Get or set runtime log level |
| `bench` | Run a quick benchmark against the server |
| `healthcheck` | Health check (exit code 0 on success) |
| `top` | Live metrics dashboard (TUI, updates each second) |

## Project structure

```
teraslab/
├── src/
│   ├── bin/server.rs         Server binary entry point
│   ├── bin/cli.rs            Admin CLI binary
│   ├── config.rs             Configuration (TOML)
│   ├── device.rs             Block device abstraction (MemoryDevice, DirectDevice; synchronous O_DIRECT)
│   ├── record.rs             On-disk record types (TxMetadata, UtxoSlot)
│   ├── allocator.rs          Freelist-based slot allocator
│   ├── index/                Primary + secondary indexes (in-memory, redb, and file_backed backends)
│   ├── locks.rs              Striped per-transaction locking
│   ├── redo.rs               Write-ahead redo log (fixed-size linear log with checkpoint reset)
│   ├── recovery.rs           Crash recovery replay
│   ├── io.rs                 Aligned I/O utilities
│   ├── metrics.rs            Operation counters and latency histograms
│   ├── ops/                  All UTXO operations (spend, create, delete_eval, etc.)
│   ├── protocol/             Wire protocol (frames, codecs, opcodes)
│   ├── server/               TCP server, dispatch, HTTP observability, WebSocket
│   ├── cluster/              SWIM membership, sharding, migration, topology authority
│   ├── replication/          Master-replica replication with durable sequencing
│   └── storage/              Tiered storage (inline, external blob)
├── client/
│   ├── go/                   Go client library
│   └── rust/                 Rust client library (async, Tokio)
├── ui/                       Embedded web dashboard (HTML/CSS/JS)
├── tests/                    Integration, stress, simulation tests
├── benches/                  Criterion benchmarks
├── teraslab-tests/           Docker-based cluster integration tests
├── scripts/                  Helper scripts (start-single, start-cluster)
├── specs/                    Architecture specs and Teranode Lua reference
└── phases/                   Build phase specifications (00-13)
```

## License

Open BSV License Version 6 — see [`LICENSE`](LICENSE).
