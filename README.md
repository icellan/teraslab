# TeraSlab

A purpose-built Rust database server designed as a UTXO store backend for BSV Teranode.

TeraSlab exploits the fixed, known workload patterns of UTXO storage to achieve dramatically better performance than general-purpose databases. By using in-place mutation on raw block devices instead of copy-on-write, it **targets** 10M+ ops/sec sustained throughput, dramatically lower SSD wear, and better tail latency than a general-purpose KV store.

> **Performance claims are design targets, not measured production numbers.** The 10M+ ops/sec figure is observed on `MemoryDevice` (anonymous `mmap`, no `O_DIRECT`, no `fsync`, no redo log). The same workload on `DirectDevice` + redo durability is bounded by NVMe sector writes and fsync, and is expected to deliver low-100s of K ops/sec per core in current form. NVMe bench results are not yet published — see [Performance methodology](#performance-methodology) below.

## Key design

TeraSlab pre-allocates UTXO slots at full size during creation and mutates them in place. The logical mutation footprint of a spend is small (1-byte status + 36-byte spending data + 4-byte slot CRC, then a metadata bump), but the on-device write is bounded below by the device sector size.

| Property | Value |
|----------|-------|
| Logical spend payload | 41 bytes per slot (1-byte status + 36-byte spending data + 4-byte slot CRC) + 256-byte metadata update |
| Slot total size on disk | 73 bytes (32-byte hash + slot payload + CRC) |
| On-device write size | One device sector per touched slot, one per metadata block. On `MemoryDevice` this is the exact byte range; on `DirectDevice` with `O_DIRECT` it amplifies to 4096-byte sectors. |
| p99.9 latency target | Low (no copy-on-write, no defrag spikes) — not yet measured on production hardware |
| Replication bandwidth | ~120 MB/s target (operation-based, not full-record) |
| Memory per record | 72 bytes in-memory (hash table bucket) or ~0 with on-disk redb backend |

### Performance methodology

The published benchmarks (`benches/`) all run against `MemoryDevice` (anonymous `mmap`). They measure the in-memory throughput ceiling — useful for catching algorithmic regressions, but **not** representative of production deployment on NVMe + `O_DIRECT` + redo-log durability. NVMe bench results, fault-injection numbers, and tail-latency histograms on a real device are tracked as outstanding work; the README will be updated to cite them when they exist. Until then, treat any throughput figure as "MemoryDevice ceiling, no fsync."

The `io_uring` backend in `src/device_io/` is scaffolding only and is NOT wired into the production write path today; every device write goes through the synchronous `pwrite` fallback at queue-depth-1.

## Status

**Pre-production.** All 14 build phases (0–13) are implemented; phases 1–7, 12, 13 are fully shipped, while phases 0, 8–11 are partial — production code paths work but specific telemetry / lifecycle / scale-up follow-ups remain (see `_review/follow_ups.md`). Each `phases/NN_*.md` carries its own `Status:` header.

| Probe | Result |
|-------|-------:|
| `cargo test --all` | 2092 passed / 0 failed / 0 ignored (at `c87339c`, per `_review/ROADMAP_TO_100.md`) |
| `cargo check --lib` | clean |
| `cargo check --bins` | clean |
| `cargo clippy --lib --no-deps` | 8 dead-code warnings in `src/device_io/*` (tracked: ROADMAP P3.1) |
| `cargo fmt --all -- --check` | clean |

**Known limitations** (production bugs from `_review/follow_ups.md` § A — refresh this list when items close):

- A-2b — shard table can stay at 2-node assignment after a fresh 3-node bootstrap even though `committed_topology_members()` reports three; suspected `topology_commit_already_activated` dedup interaction.
- A-3 — `replica_unauthenticated_accept_total` counter exists but is not incremented at the auth gate in `handle_connection_inner`.
- A-4 — engine-side atomic apply (F-G5-022) is a concurrency hypothesis with no live repro yet.

**Documented design choices** (deliberate scope decisions, not bugs):

- **Single-interval freeze model.** `spendable_height` is a single `u32` per output (mirrors Aerospike). svnode's `enforceAtHeight` supports a multi-interval array. If Teranode's contract evolves to require multi-interval freezes (e.g. two disjoint locked windows on the same UTXO), a new op type (`OP_FREEZE_INTERVAL_BATCH` or similar) is required; the on-disk slot layout reserves enough spending-data bytes to extend without a format break, but the engine match arms and the wire protocol need additions.
- A-5 — `SWIM_PING_REQ_DROPPED_TOTAL` lives inside `cluster::swim` instead of the registry exposed by `/metrics`.

See `_review/ROADMAP_TO_100.md` for the sequenced finish plan and `REVIEW_REPORT.md` for the May 2026 review campaign that surfaced these.

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
- **TCP wire protocol** on `0.0.0.0:3300`
- **HTTP observability** on `0.0.0.0:9100`
- Data file: `teraslab-data.dat` (1 GiB, created if missing)
- Index: in-memory (snapshot: `teraslab-index.snap`). See [Index backends](#index-backends) for the on-disk alternative.
- Single-node mode (no clustering)

### Configuration file

```bash
./target/release/teraslab-server --config /path/to/config.toml
```

All settings have sensible defaults. Only specify what you want to override.

#### Minimal single-node config

```toml
listen_addr = "0.0.0.0:3300"
# Use stable paths — /dev/nvme* numbers can change on reboot
device_paths = ["/dev/disk/by-id/nvme-Samsung_990_PRO_S73WNJ0X123456-part1"]
device_size = 107374182400  # 100 GiB (ignored for raw block devices; actual size is queried)
expected_records = 50000000
```

#### Low-RAM deployment with on-disk index

```toml
listen_addr = "0.0.0.0:3300"
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
listen_addr = "0.0.0.0:3300"       # TCP binary protocol
http_listen_addr = "0.0.0.0:9100"  # HTTP observability endpoints
max_connections = 1024              # Max concurrent TCP connections
max_batch_size = 8192               # Max items per batch request

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

# --- Index ---
index_snapshot_path = "teraslab-index.snap"
expected_records = 100000             # Hint for initial hash table sizing

# --- Index backend (optional, defaults to in-memory) ---
[index]
backend = "memory"                        # "memory" (default) or "redb"
redb_path = "teraslab-index.redb"         # Primary index redb file
redb_dah_path = "teraslab-dah.redb"       # DAH secondary index redb file
redb_unmined_path = "teraslab-unmined.redb" # Unmined secondary index redb file
redb_cache_size = 268435456               # redb page cache in bytes (256 MiB default)

# --- Concurrency ---
lock_stripes = 65536                  # Per-transaction lock stripes (power of 2)

# --- Pruning ---
block_height_retention = 288          # Blocks to retain unmined transactions

# --- Cold data ---
blobstore_path = "/blobstore"         # Directory for large transaction blobs (>1 MiB)

# --- Cluster (set node_id > 0 to enable) ---
node_id = 0                           # 0 = single-node mode
swim_port = 3301                      # UDP port for SWIM membership protocol
seed_nodes = []                       # e.g. ["10.0.0.2:3301", "10.0.0.3:3301"]
replication_factor = 1                # 1 = no replication, 2 = master + 1 replica
swim_probe_interval_ms = 200          # SWIM heartbeat interval
swim_suspicion_timeout_ms = 5000      # Time before suspect node is declared dead
topology_propose_timeout_ms = 0       # 0 = max(swim_probe_interval_ms * 3, 500)
cluster_secret = ""                   # Shared secret for HMAC-SHA256 SWIM + inter-node TCP auth
max_migration_threads = 16            # Max concurrent migration threads per topology change

# --- Replication durability ---
ack_policy = "auto"                   # "auto", "write_all", "write_majority", or "best_effort"
replication_timeout_ms = 3000         # Timeout for each replication batch ACK
replication_degraded_mode = "reject"  # "reject" or "best_effort" when ack policy fails

# --- Migration performance ---
migration_pool_size = 128             # Parallel TCP connections per migration target
migration_batch_size = 500            # Records per baseline streaming batch
replica_lag_check_interval_secs = 30  # Interval between replica lag checks (0 to disable)
```

### Cluster deployment (3 nodes, RF=2)

Node 1:
```toml
listen_addr = "0.0.0.0:3300"
node_id = 1
swim_port = 3301
seed_nodes = ["10.0.0.2:3301", "10.0.0.3:3301"]
replication_factor = 2
device_paths = ["/dev/disk/by-id/nvme-Samsung_990_PRO_S73WNJ0X000001-part1"]
device_size = 107374182400
```

Node 2:
```toml
listen_addr = "0.0.0.0:3300"
node_id = 2
swim_port = 3301
seed_nodes = ["10.0.0.1:3301", "10.0.0.3:3301"]
replication_factor = 2
device_paths = ["/dev/disk/by-id/nvme-Samsung_990_PRO_S73WNJ0X000002-part1"]
device_size = 107374182400
```

Node 3:
```toml
listen_addr = "0.0.0.0:3300"
node_id = 3
swim_port = 3301
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
| 104 | `AdminDiagnoseKey` | Diagnose per-key routing and local shard state |
| 105 | `PartitionVersionReport` | Inter-node shard version report after topology commit |
| 106 | `AdminClusterHealth` | Cluster readiness snapshot for clients/tests |
| 107 | `Hello` | Protocol-version handshake; empty request, response is the server's 2-byte LE protocol version (pre-v2 servers reject with `OPCODE_UNSUPPORTED` or `INTERNAL`) |

**Inter-node replication, migration, and topology:**

| Opcode | Name | Description |
|--------|------|-------------|
| 240 | `ReplicaBatch` | Send a batch of replica operations |
| 241 | `ReplicaAck` | Acknowledge a replica batch |
| 242 | `MigrationComplete` | Verify and complete a single shard migration |
| 243 | `MigrationBatchComplete` | Verify and complete multiple shard migrations |
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
| 6 | `INVALID_SPEND` | Spending data targets a deleted/pruned UTXO |
| 7 | `FROZEN` | Cannot spend a frozen UTXO |
| 8 | `CONFLICTING` | Transaction is marked conflicting |
| 9 | `LOCKED` | Transaction is locked |
| 10 | `COINBASE_IMMATURE` | Coinbase UTXO not yet spendable (error data: 4-byte required height) |
| 11 | `VOUT_OUT_OF_RANGE` | UTXO index exceeds slot count |
| 12 | `ALREADY_EXISTS` | Duplicate transaction creation |
| 13 | `FROZEN_UNTIL` | Reassignment cooldown not met |
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
| 255 | `INTERNAL` | Unexpected server error |

### Response status codes

| Code | Name | Meaning |
|------|------|---------|
| 0 | `OK` | Request succeeded |
| 1 | `ERROR` | Request failed with an error payload |
| 2 | `NOT_FOUND` | Requested object was not found |
| 3 | `REDIRECT` | Retry against the shard owner in the payload |
| 4 | `PARTIAL_ERROR` | Batch partially succeeded; per-item errors are encoded in the payload |
| 5 | `DEGRADED_DURABILITY` | Local mutation succeeded, but best-effort replication did not satisfy the configured ACK policy |

## HTTP observability

The HTTP server (default port 9100) exposes health checks, Prometheus metrics, and debug endpoints.

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

### Debug endpoints

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

### Admin endpoints

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

### WebSocket

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
err = client.CreateBatch(ctx, []teraslab.CreateItem{{
    TxID:         txid,
    TxVersion:    1,
    Fee:          500,
    SizeInBytes:  225,
    UTXOHashes:   [][32]byte{hash0, hash1, hash2},
    BlockHeight:  800000,
}})

// Spend a UTXO
results, err := client.SpendBatch(ctx, []teraslab.SpendItem{{
    TxID:         txid,
    Vout:         0,
    UTXOHash:     hash0,
    SpendingData: spendingData, // 36 bytes: spending txid + vout
}}, teraslab.SpendParams{CurrentBlockHeight: 800001})
```

Full documentation in [`client/go/README.md`](client/go/README.md).

### Rust client

```rust
use teraslab_client::{Client, ClientConfig};

let client = Client::connect(ClientConfig {
    addr: "localhost:3300".to_string(),
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
[TxMetadata: 256 bytes][UtxoSlot 0: 69 bytes][UtxoSlot 1: 69 bytes]...[UtxoSlot N-1: 69 bytes]
```

**TxMetadata** (256 bytes, padded for alignment) contains: txid, version, locktime, fee, size, extended size, flags (conflicting, locked, external, coinbase, last_spent_all), block entries (up to 3 inline, overflow stored separately), spending height, creation timestamp, generation counter, update timestamp, unmined_since, delete_at_height, preserve_until, reassignment tracking, external storage reference, and conflicting children tracking.

**UtxoSlot** (73 bytes each): 32-byte hash, 1-byte status (unspent/spent/frozen/pruned), 36-byte spending data (spending txid + vout), 4-byte CRC32 (torn-write protection per slot — BC-02 / F-X-007). Slots are pre-allocated at full size during creation. A spend writes the 41-byte status+spending+CRC region in place, plus updates the 256-byte metadata (generation, counters, timestamps). On `DirectDevice` (`O_DIRECT`), each in-place write amplifies to the device's sector size (4096 bytes on most NVMe drives).

### Tiered storage

- **Hot path** (NVMe): Metadata + UTXO slots. All spend/setMined/freeze operations touch only this tier.
- **Cold data** (filesystem blob store): Transaction inputs, outputs, and inpoints. Stored inline if <8 KiB, otherwise in the external blob store. The earlier separate-device middle tier is not enabled because current metadata has no durable offset/length fields for it.

### Crash recovery

A write-ahead redo log records all mutations. On crash recovery:
1. Open the redo log and scan for the last checkpoint
2. Replay all entries after the checkpoint (operations are idempotent)
3. Resume normal operation

The redo log is a fixed-size circular buffer on a separate device file.

## Index backends

TeraSlab supports two index backends for the primary index and secondary indexes (DAH, unmined). The backend is selected at startup via configuration and cannot be changed at runtime.

### In-memory (default)

The default backend stores the index in a Robin Hood hash table backed by anonymous `mmap`. This is the fastest option, targeting the 10M+ ops/sec design ceiling on `MemoryDevice` (not yet measured on NVMe — see [Performance methodology](#performance-methodology)). It requires approximately **72 bytes per record** of RAM. For 100M records, this means ~7.2 GB of RAM for the index alone.

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
| **RAM per 10M records** | ~720 MB | ~256 MB (page cache only) |
| **Crash recovery** | Rebuild from device + redo replay | Instant (already on disk) |
| **Startup time** | Seconds (snapshot restore) to minutes (full rebuild) | Instant (open existing files) |
| **Snapshot needed** | Yes (`index_snapshot_path`) | No (crash-durable by default) |
| **SSD write overhead** | None (index is in RAM) | B+ tree writes per mutation |

#### Error recovery

redb startup is fail-closed for the primary index. The server first attempts to open the configured primary redb file. If that fails, it rebuilds the primary redb index from a device scan. If the rebuild also fails, startup exits without deleting the existing redb file, so operators can capture diagnostics before an explicit rescan or repair.

Secondary redb indexes (DAH and unmined) are isolated from the primary. If a secondary redb file cannot be opened, the node starts in degraded readiness with an empty in-memory replacement for that secondary; endpoints that depend on the missing secondary return `INDEX_DEGRADED` until the operator fixes the underlying issue and restarts. The server does not silently delete corrupt redb files or automatically fall back to a fully in-memory backend for the primary.

#### Migration between backends

Use `teraslab-cli` to export and import index data between backends:

```bash
# Export current index (any backend) to a portable snapshot
teraslab-cli export-index --output /tmp/index-export.snap

# Import into a redb-configured instance
teraslab-cli import-index --input /tmp/index-export.snap
```

The export format is the same binary snapshot format used for in-memory index persistence, making it backend-agnostic.

## Admin CLI

The `teraslab-cli` binary provides operator commands that consume the HTTP observability endpoints and binary wire protocol. Supports both table-formatted and JSON output.

```bash
./target/release/teraslab-cli --addr localhost:9100 <command>
```

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
│   ├── device.rs             Block device abstraction (MemoryDevice, DirectDevice)
│   ├── device_io/            I/O backend scaffolding (sync fallback; io_uring not production-wired)
│   ├── record.rs             On-disk record types (TxMetadata, UtxoSlot)
│   ├── allocator.rs          Freelist-based slot allocator
│   ├── index/                Primary + secondary indexes (in-memory and redb on-disk backends)
│   ├── locks.rs              Striped per-transaction locking
│   ├── redo.rs               Write-ahead redo log (circular buffer)
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
