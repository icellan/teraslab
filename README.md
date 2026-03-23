# TeraSlab

A purpose-built Rust database server designed as a UTXO store backend for BSV Teranode.

TeraSlab exploits the fixed, known workload patterns of UTXO storage to achieve dramatically better performance than general-purpose databases. By using in-place mutation on raw block devices instead of copy-on-write, it targets **10M+ ops/sec sustained throughput**, **10-50x less SSD wear**, and significantly better tail latency.

## Key design

A UTXO spend only changes one 69-byte slot. TeraSlab pre-allocates slots at full size during creation and mutates them in place with a single `pwrite` call, eliminating copy-on-write overhead entirely.

| Property | Value |
|----------|-------|
| Spend write size | 69 bytes (in-place) |
| p99.9 latency | Low (no copy-on-write, no defrag spikes) |
| Replication bandwidth | ~120 MB/s (operation-based, not full-record) |
| SSD wear per spend | 69 bytes (vs full record in copy-on-write designs) |
| Memory per record | ~16 bytes (index entry) |

## Building

```bash
cargo build --release
```

The binary is at `target/release/teraslab-server`.

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
- Index snapshot: `teraslab-index.snap`
- Single-node mode (no clustering)

### Configuration file

```bash
./target/release/teraslab-server --config /path/to/config.toml
```

All settings have sensible defaults. Only specify what you want to override.

#### Minimal single-node config

```toml
listen_addr = "0.0.0.0:3300"
device_paths = ["/dev/nvme0n1p1"]
device_size = 107374182400  # 100 GiB
expected_records = 50000000
```

#### Full configuration reference

```toml
# --- Network ---
listen_addr = "0.0.0.0:3300"       # TCP binary protocol
http_listen_addr = "0.0.0.0:9100"  # HTTP observability endpoints
max_connections = 1024              # Max concurrent TCP connections
max_batch_size = 8192               # Max items per batch request

# --- Storage ---
device_paths = ["teraslab-data.dat"]  # Raw device or file path(s)
device_size = 1073741824              # Bytes per device (used when creating new files)
device_alignment = 4096               # I/O alignment (4096 for NVMe/SSD)

# --- Recovery ---
redo_log_size = 67108864              # Redo log size in bytes (64 MiB)
redo_log_path = "teraslab-data.dat.redo"  # Optional explicit redo log path

# --- Index ---
index_snapshot_path = "teraslab-index.snap"
expected_records = 100000             # Hint for initial hash table sizing

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
```

### Cluster deployment (3 nodes, RF=2)

Node 1:
```toml
listen_addr = "0.0.0.0:3300"
node_id = 1
swim_port = 3301
seed_nodes = ["10.0.0.2:3301", "10.0.0.3:3301"]
replication_factor = 2
device_paths = ["/dev/nvme0n1p1"]
device_size = 107374182400
```

Node 2:
```toml
listen_addr = "0.0.0.0:3300"
node_id = 2
swim_port = 3301
seed_nodes = ["10.0.0.1:3301", "10.0.0.3:3301"]
replication_factor = 2
device_paths = ["/dev/nvme0n1p1"]
device_size = 107374182400
```

Node 3:
```toml
listen_addr = "0.0.0.0:3300"
node_id = 3
swim_port = 3301
seed_nodes = ["10.0.0.1:3301", "10.0.0.2:3301"]
replication_factor = 2
device_paths = ["/dev/nvme0n1p1"]
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
| 21 | `GetSpendBatch` | Check UTXO spend status (lightweight) |

**Pruner:**

| Opcode | Name | Description |
|--------|------|-------------|
| 30 | `QueryOldUnmined` | Find unmined transactions before height |
| 31 | `PreserveTransactions` | Prevent pruning of parent transactions |
| 32 | `ProcessExpiredPreservations` | Delete expired preserved transactions |

**Admin:**

| Opcode | Name | Description |
|--------|------|-------------|
| 100 | `GetPartitionMap` | Fetch shard-to-node mapping (cluster) |
| 101 | `Health` | Health check |
| 102 | `Ping` | Latency measurement |

### Error codes

| Code | Name | Meaning |
|------|------|---------|
| 0 | `OK` | Success |
| 1 | `TX_NOT_FOUND` | Transaction does not exist |
| 2 | `UTXO_HASH_MISMATCH` | Provided hash doesn't match stored hash |
| 3 | `ALREADY_SPENT` | UTXO is already spent |
| 4 | `ALREADY_FROZEN` | UTXO is already frozen |
| 7 | `FROZEN` | Cannot spend a frozen UTXO |
| 8 | `CONFLICTING` | Transaction is marked conflicting |
| 9 | `LOCKED` | Transaction is locked |
| 10 | `COINBASE_IMMATURE` | Coinbase UTXO not yet spendable |
| 11 | `VOUT_OUT_OF_RANGE` | UTXO index exceeds slot count |
| 12 | `ALREADY_EXISTS` | Duplicate transaction creation |
| 14 | `REDIRECT` | Shard owned by another node (payload contains target address) |
| 15 | `NO_QUORUM` | Cluster quorum not met, mutations rejected |
| 19 | `MIGRATION_IN_PROGRESS` | Shard being migrated, retry shortly |

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

### Cluster admin

```bash
# Shard migration status
curl http://localhost:9100/admin/migration_status

# Drain shards from this node (graceful shutdown prep)
curl -X PUT http://localhost:9100/admin/quiesce
```

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
- During migration, reads on the old master continue serving locally; reads on the new master wait briefly for data to arrive. Writes to shards with pending inbound migration return `MIGRATION_IN_PROGRESS` so clients retry.

## Architecture

### On-disk layout

Each transaction occupies a contiguous region on the block device:

```
[TxMetadata: ~200 bytes][UtxoSlot 0: 69 bytes][UtxoSlot 1: 69 bytes]...[UtxoSlot N-1: 69 bytes]
```

**TxMetadata** contains: txid, version, locktime, fee, size, flags (frozen, conflicting, locked, external, coinbase), block entries (up to 4 inline), spending height, creation timestamp, and counters.

**UtxoSlot** (69 bytes each): 32-byte hash, 1-byte status (unspent/spent/frozen/pruned), 36-byte spending data (spending txid + vout). Slots are pre-allocated at full size during creation. A spend writes only the status byte and spending data in place.

### Tiered storage

- **Hot path** (NVMe): Metadata + UTXO slots. All spend/setMined/freeze operations touch only this tier.
- **Cold data** (filesystem blob store): Transaction inputs, outputs, and inpoints. Stored inline if <8 KiB, in a separate device block if 8 KiB-1 MiB, or in the external blob store if >1 MiB.

### Crash recovery

A write-ahead redo log records all mutations. On crash recovery:
1. Open the redo log and scan for the last checkpoint
2. Replay all entries after the checkpoint (operations are idempotent)
3. Resume normal operation

The redo log is a fixed-size circular buffer on a separate device file.

## Project structure

```
teraslab/
├── src/
│   ├── bin/server.rs         Server binary entry point
│   ├── config.rs             Configuration (TOML)
│   ├── device.rs             Block device abstraction
│   ├── record.rs             On-disk record types
│   ├── allocator.rs          Slot allocator
│   ├── index/                Primary + secondary indexes
│   ├── locks.rs              Striped per-transaction locking
│   ├── redo.rs               Write-ahead redo log
│   ├── recovery.rs           Crash recovery replay
│   ├── io.rs                 Aligned I/O utilities
│   ├── metrics.rs            Operation counters
│   ├── ops/                  All UTXO operations (spend, create, etc.)
│   ├── protocol/             Wire protocol (frames, codecs, opcodes)
│   ├── server/               TCP server, dispatch, HTTP, streaming
│   ├── cluster/              SWIM membership, sharding, migration
│   ├── replication/          Master-replica replication
│   └── storage/              Blob store for cold data
├── client/
│   ├── go/                   Go client library
│   └── rust/                 Rust client library (async, Tokio)
├── tests/                    Integration, stress, simulation tests
├── specs/                    Architecture specs and Teranode Lua reference
└── phases/                   Build phase specifications (00-13)
```

## License

See repository for license details.
