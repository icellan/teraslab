# TeraSlab Rust Client

Async Rust client library for the TeraSlab binary wire protocol. Built on Tokio with connection pooling, request pipelining, and cluster-aware shard routing.

## Add to Cargo.toml

```toml
[dependencies]
teraslab-client = { path = "../client/rust" }
tokio = { version = "1", features = ["full"] }
```

## Quick Start

### Single node

```rust
use teraslab_client::*;

let client = Client::new(ClientConfig {
    addr: Some("localhost:3300".to_string()),
    ..Default::default()
}).await?;

let rtt = client.ping().await?;
println!("pong: {:?}", rtt);

client.close().await;
```

### Cluster mode

```rust
let client = Client::new(ClientConfig {
    seeds: vec!["node1:3300".into(), "node2:3300".into(), "node3:3300".into()],
    ..Default::default()
}).await?;

client.close().await;
```

In cluster mode, batch operations are automatically routed to the correct node(s) by txid shard. The client periodically refreshes the partition map and handles redirects transparently.

## Configuration

```rust
pub struct ClientConfig {
    pub addr: Option<String>,                      // Single-node address
    pub seeds: Vec<String>,                        // Cluster seed nodes (overrides addr)
    pub pool: PoolConfig,                          // Per-node connection pool config
    pub cluster_refresh_interval: Duration,        // Partition map refresh (default: 30s)
    pub max_redirects: u32,                        // Redirect retries per request (default: 3)
    pub addr_map: HashMap<String, String>,         // Address remapping for Docker/NAT
    pub cluster_secret: Option<Vec<u8>>,           // HMAC secret for OP_GET_PARTITION_MAP (strict_auth clusters)
    pub request_timeout: Duration,                 // Per-request round-trip timeout (default: 30s)
}

pub struct PoolConfig {
    pub min_conns: usize,         // Minimum idle connections (default: 2)
    pub max_conns: usize,         // Maximum connections per node (default: 16)
    pub dial_timeout: Duration,   // Connection timeout (default: 5s)
    pub health_check: Duration,   // Idle connection health check interval (default: 15s)
}
```

The `addr_map` allows remapping server-advertised internal addresses to host-reachable addresses, useful for Docker or NAT environments.

## Operations

All methods are async and safe for concurrent use from multiple Tokio tasks. The `Client` is `Send + Sync`.

### Create

```rust
let items = vec![CreateItem {
    txid: [0u8; 32],
    tx_version: 1,
    locktime: 0,
    fee: 500,
    size_in_bytes: 250,
    is_coinbase: false,
    block_height: 800000,
    utxo_hashes: vec![[0xAA; 32]],
    cold_data: serialized_tx_data,
    ..Default::default()
}];

let result = client.create_batch(&items).await?;
```

Transactions with cold data larger than 1 MiB are automatically uploaded via chunked streaming before the batch request.

### Spend

```rust
let params = SpendBatchParams {
    ignore_conflicting: false,
    ignore_locked: false,
    current_block_height: 800100,
    block_height_retention: 288,
};

let resp = client.spend_batch(&params, &items).await?;
for s in &resp.successes {
    println!("item {}: signal={}", s.item_index, s.signal);
}
```

### Unspend

```rust
let result = client.unspend_batch(&params, &items).await?;
```

### Set Mined

```rust
let params = SetMinedBatchParams {
    block_id: 42,
    block_height: 800000,
    subtree_idx: 7,
    on_longest_chain: true,
    ..Default::default()
};

let resp = client.set_mined_batch(&params, &txids).await?;
```

### Freeze / Unfreeze / Reassign

```rust
client.freeze_batch(&items).await?;
client.unfreeze_batch(&items).await?;
client.reassign_batch(&params, &items).await?;
```

### Set Conflicting / Locked

```rust
client.set_conflicting_batch(&params, &txids).await?;
client.set_locked_batch(true, &txids).await?;
```

### Get

```rust
use teraslab::protocol::codec::FieldMask;

let results = client.get_batch(FieldMask::ALL, &txids).await?;
for r in &results {
    if r.status == 0 {
        // Parse r.data according to the field mask
    }
}
```

### Get Spend Status

```rust
let items = vec![GetSpendItem {
    txid,
    vout: 0,
    utxo_hash: [0xAA; 32],
}];
let results = client.get_spend_batch(&items).await?;
for r in &results {
    match r.slot_status {
        0x00 => println!("unspent"),
        0x01 => println!("spent"),
        0xFF => println!("frozen"),
        _ => {}
    }
}
```

### Pruner Operations

```rust
let txids = client.query_old_unmined(cutoff_height).await?;
client.preserve_transactions(preserve_until, &txids).await?;
let result = client
    .process_expired_preservations(current_height, block_height_retention)
    .await?;
```

### Admin

```rust
let rtt = client.ping().await?;
client.health().await?;
let pm = client.get_partition_map().await?;
client.delete_batch(&txids).await?;
client.refresh_routing().await?;
```

## Signal Constants

Mutation operations return per-item signals indicating state transitions:

| Constant | Value | Meaning |
|----------|-------|---------|
| `SIGNAL_NONE` | 0 | No state transition |
| `SIGNAL_ALL_SPENT` | 1 | All UTXOs in this transaction are now spent |
| `SIGNAL_NOT_ALL_SPENT` | 2 | Not all UTXOs are spent (e.g. after unspend) |
| `SIGNAL_DELETE_AT_HEIGHT_SET` | 3 | Transaction queued for pruning at a block height |
| `SIGNAL_DELETE_AT_HEIGHT_UNSET` | 4 | Transaction removed from pruning queue |
| `SIGNAL_PRESERVE` | 5 | Transaction marked for preservation |

## Error Handling

```rust
pub enum ClientError {
    Connection(String),                     // TCP connection failure
    Server { code: u16, message: String },  // Global server error
    NotFound,                               // Record not found
    Redirect(String),                       // Shard on another node
    Partial(PartialError),                  // Mixed success/failure
    Protocol(String),                       // Response decode failure
    Timeout,                                // Request timed out
    NoPartitionMap,                         // No partition map available (cluster mode)
    PoolClosed,                             // Connection pool has been closed
}
```

Partial errors contain per-item failures with original batch indices:

```rust
match client.spend_batch(params, items).await {
    Err(ClientError::Partial(pe)) => {
        for e in &pe.errors {
            eprintln!("item {}: code {}", e.item_index, e.code);
        }
    }
    Err(e) => panic!("{}", e),
    Ok(result) => { /* all items succeeded */ }
}
```

## Additional APIs

```rust
// Manually refresh the cluster partition map
client.refresh_routing().await?;

// Upload a large blob before create_batch (done automatically for cold_data > 1 MiB)
client.upload_blob(&txid, &data).await?;
```

## Cluster Routing

In cluster mode, the client computes a 12-bit shard from each txid (`txid[0:2] as LE u16 & 0x0FFF`). The 4096 shards are mapped to nodes via the partition map. For batch operations spanning multiple txids, the client automatically splits by target node, sends sub-batches in parallel, and merges results.

## Thread Safety

The `Client` is `Send + Sync` and safe for concurrent use from multiple Tokio tasks. Each connection pool manages a set of pipelined TCP connections with independent request/response multiplexing.
