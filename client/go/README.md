# TeraSlab Go Client

Go client library for the [TeraSlab](../../) binary wire protocol. Provides full coverage of all
server operations with connection pooling, request pipelining, cluster-aware shard routing, and
typed error handling.

- **Zero external dependencies** -- stdlib only
- **Goroutine-safe** -- the `Client` is safe for concurrent use
- **Pipelined connections** -- multiple in-flight requests per TCP connection
- **Cluster-aware** -- automatic partition map routing and redirect handling
- **Batch-first** -- all operations are batch operations (single items are batches of size 1)

## Install

```
go get github.com/icellan/teraslab/client/go
```

## Connecting

### Single node

```go
import teraslab "github.com/icellan/teraslab/client/go"

ctx := context.Background()

client, err := teraslab.New(ctx, teraslab.ClientConfig{
    Addr: "localhost:3300",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### Cluster

When connecting to a cluster, provide seed addresses. The client fetches the partition map
on startup and automatically routes requests to the correct node based on the txid's shard.

```go
client, err := teraslab.New(ctx, teraslab.ClientConfig{
    Seeds: []string{"node1:3300", "node2:3300", "node3:3300"},
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### Connection pool tuning

```go
client, err := teraslab.New(ctx, teraslab.ClientConfig{
    Addr: "localhost:3300",
    Pool: teraslab.PoolConfig{
        MinConns:    4,              // minimum idle connections (default: 2)
        MaxConns:    32,             // maximum connections (default: 16)
        DialTimeout: 5 * time.Second, // connect timeout (default: 5s)
        HealthCheck: 15 * time.Second, // ping interval (default: 15s)
    },
    ClusterRefreshInterval: 30 * time.Second, // partition map refresh (default: 30s)
    MaxRedirects:           3,                // redirect retries (default: 3)
})
```

## Operations

All methods accept a `context.Context` for cancellation and timeouts. Every operation is
batch-first -- pass a slice of items and receive results for the entire batch.

### Create transactions

```go
blockID := uint32(100)
blockHeight := uint32(800000)
subtreeIdx := uint32(0)

items := []teraslab.CreateItem{
    {
        TxID:         txid,
        TxVersion:    2,
        Locktime:     0,
        Fee:          1500,
        SizeInBytes:  250,
        IsCoinbase:   false,
        CreatedAt:    uint64(time.Now().UnixMilli()),
        UtxoHashes:   []teraslab.UtxoHash{utxoHash0, utxoHash1},
        TxData: teraslab.TxData{         // optional: transaction inputs/outputs/inpoints
            Inputs:   inputBytes,
            Outputs:  outputBytes,
            Inpoints: inpointBytes,
        },
        BlockHeight:      800000,        // current block height (sets unmined_since)
        MinedBlockID:     &blockID,      // optional: set if already mined
        MinedBlockHeight: &blockHeight,
        MinedSubtreeIdx:  &subtreeIdx,
    },
}

result, err := client.CreateBatch(ctx, items)
if err != nil {
    log.Fatal(err)
}
```

### Spend UTXOs

```go
params := teraslab.SpendBatchParams{
    IgnoreConflicting:    false,
    IgnoreLocked:         false,
    CurrentBlockHeight:   800100,
    BlockHeightRetention: 288,
}

items := []teraslab.SpendItem{
    {
        TxID:         txid,
        Vout:         0,
        UtxoHash:     utxoHash,
        SpendingData: spendingData, // 36 bytes: spending txid (32) + vin (4 LE)
    },
}

resp, err := client.SpendBatch(ctx, params, items)
if err != nil {
    // Check for partial errors (some items failed, some succeeded).
    var pe *teraslab.PartialError
    if errors.As(err, &pe) {
        for _, e := range pe.Errors {
            fmt.Printf("item %d failed: %s\n", e.ItemIndex, teraslab.ErrorCodeString(e.Code))
            if e.Code == teraslab.ErrCodeAlreadySpent {
                // e.Data contains the 36-byte existing spending data.
            }
        }
        // Successes are still available:
        for _, s := range pe.Successes {
            if s.Signal == teraslab.SignalAllSpent {
                // All UTXOs in this tx are now spent.
            }
        }
    } else {
        log.Fatal(err)
    }
}

// On full success, inspect signals:
for _, s := range resp.Successes {
    switch s.Signal {
    case teraslab.SignalAllSpent:
        fmt.Printf("tx at index %d: all UTXOs spent\n", s.ItemIndex)
    case teraslab.SignalDeleteAtHeightSet:
        fmt.Printf("tx at index %d: queued for pruning\n", s.ItemIndex)
    }
}
```

### Unspend (reverse a spend)

```go
params := teraslab.UnspendBatchParams{
    CurrentBlockHeight:   800100,
    BlockHeightRetention: 288,
}

items := []teraslab.UnspendItem{
    // SpendingData is REQUIRED: the server only reverses a spend whose
    // recorded spending data matches. It must be the original spending child's
    // txid (32) + vin (4 LE); omitting it makes the unspend a no-op.
    {TxID: txid, Vout: 0, UtxoHash: utxoHash, SpendingData: spendingData},
}

result, err := client.UnspendBatch(ctx, params, items)
```

### Set mined

```go
params := teraslab.SetMinedBatchParams{
    BlockID:              42,
    BlockHeight:          800000,
    SubtreeIdx:           7,
    OnLongestChain:       true,
    UnsetMined:           false,
    CurrentBlockHeight:   800000,
    BlockHeightRetention: 288,
}

resp, err := client.SetMinedBatch(ctx, params, []teraslab.TxID{txid1, txid2})
// resp.Successes contains per-item signals and block IDs.
```

### Freeze / unfreeze UTXOs

```go
items := []teraslab.FreezeItem{
    {TxID: txid, Vout: 0, UtxoHash: utxoHash},
}

_, err := client.FreezeBatch(ctx, items)

// Later, unfreeze:
_, err = client.UnfreezeBatch(ctx, items)
```

### Reassign frozen UTXOs

```go
params := teraslab.ReassignBatchParams{
    BlockHeight:    800000,
    SpendableAfter: 800100, // cooldown: not spendable until this height
}

items := []teraslab.ReassignItem{
    {
        TxID:        txid,
        Vout:        0,
        UtxoHash:    currentHash,
        NewUtxoHash: newHash,
    },
}

_, err := client.ReassignBatch(ctx, params, items)
```

### Set conflicting

```go
params := teraslab.SetConflictingParams{
    Value:                true,
    CurrentBlockHeight:   800100,
    BlockHeightRetention: 288,
}

_, err := client.SetConflictingBatch(ctx, params, []teraslab.TxID{txid})
```

### Set locked

```go
// Lock transactions from being spent.
_, err := client.SetLockedBatch(ctx, true, []teraslab.TxID{txid1, txid2})

// Unlock:
_, err = client.SetLockedBatch(ctx, false, []teraslab.TxID{txid1, txid2})
```

### Preserve until

```go
// Prevent pruning until block height 900000.
_, err := client.PreserveUntilBatch(ctx, 900000, []teraslab.TxID{txid})
```

### Mark longest chain

```go
params := teraslab.MarkLongestChainParams{
    OnLongestChain:       true,
    CurrentBlockHeight:   800100,
    BlockHeightRetention: 288,
}

_, err := client.MarkLongestChainBatch(ctx, params, []teraslab.TxID{txid})
```

### Delete transactions

```go
_, err := client.DeleteBatch(ctx, []teraslab.TxID{txid1, txid2, txid3})
```

### Get transaction data

Use field mask constants to select which data to fetch. The response contains raw serialized
data per item which can be parsed with the `DecodeTxMetadata`, `DecodeUtxoSlots`, and
`DecodeBlockEntries` helpers.

> **Block entries are capped at `MaxInlineBlockEntries` (3) inline.** A transaction mined
> into more blocks keeps the surplus in on-disk overflow, which the inline `GetBatch`
> response does not carry. `DecodeBlockEntriesWithCount` returns the declared total so
> truncation is detectable, and `TxRecord.BlockEntriesTruncated` is set when entries were
> omitted. Reading the overflow requires repair tooling that follows `block_overflow_offset`.

```go
// Use GetRecordBatch for automatic decoding based on field mask.
records, err := client.GetRecordBatch(ctx, teraslab.FieldAllMetadata|teraslab.FieldUtxoSlots, []teraslab.TxID{txid})
if err != nil {
    log.Fatal(err)
}

for _, rec := range records {
    if !rec.Found {
        fmt.Println("not found")
        continue
    }

    fmt.Printf("fee=%d utxo_count=%d spent=%d\n", rec.Metadata.Fee, rec.Metadata.UtxoCount, rec.Metadata.SpentUtxos)

    for i, slot := range rec.Slots {
        switch slot.Status {
        case teraslab.SlotUnspent:
            fmt.Printf("  vout %d: unspent\n", i)
        case teraslab.SlotSpent:
            fmt.Printf("  vout %d: spent\n", i)
        case teraslab.SlotFrozen:
            fmt.Printf("  vout %d: frozen\n", i)
        }
    }
}
```

To fetch only fee and utxo count (zero-alloc access, no TxMetadata struct allocated):

```go
batch, err := client.GetBatch(ctx, teraslab.FieldFee|teraslab.FieldUtxoCount, []teraslab.TxID{txid})
fee, _ := batch.Fee(0)           // reads directly from wire bytes
count, _ := batch.UtxoCount(0)   // zero allocation
```

Full decode when you need the struct:

```go
batch, err := client.GetBatch(ctx, teraslab.FieldAllMetadata, txids)
meta, _, _ := batch.DecodeMetadata(0)  // allocates TxMetadata
```

Available field masks (combine with `|`):

| Constant | Bit | Description |
|----------|-----|-------------|
| `FieldTxVersion` | `1 << 0` | Transaction version |
| `FieldLocktime` | `1 << 1` | Transaction locktime |
| `FieldFee` | `1 << 2` | Transaction fee |
| `FieldSizeInBytes` | `1 << 3` | Transaction size in bytes |
| `FieldExtendedSize` | `1 << 4` | Extended transaction size |
| `FieldFlags` | `1 << 5` | Transaction flags |
| `FieldSpendingHeight` | `1 << 6` | Spending height |
| `FieldCreatedAt` | `1 << 7` | Creation timestamp |
| `FieldSpentUtxos` | `1 << 8` | Spent UTXO count |
| `FieldPrunedUtxos` | `1 << 9` | Pruned UTXO count |
| `FieldUtxoCount` | `1 << 10` | Total UTXO count |
| `FieldGeneration` | `1 << 11` | Record generation number |
| `FieldUpdatedAt` | `1 << 12` | Last updated timestamp |
| `FieldUnminedSince` | `1 << 13` | Block height since unmined |
| `FieldDeleteAtHeight` | `1 << 14` | Delete-at block height |
| `FieldPreserveUntil` | `1 << 15` | Preserve-until block height |
| `FieldExternalRef` | `1 << 16` | External storage reference |
| `FieldReassignCount` | `1 << 17` | Reassignment count |
| `FieldBlockEntryCount` | `1 << 18` | Block entry count |
| `FieldUtxoSlots` | `1 << 19` | UTXO slot data (hash, status, spending data) |
| `FieldColdData` | `1 << 20` | Full transaction inputs/outputs |
| `FieldBlockEntries` | `1 << 21` | Block entries (block ID, height, subtree index) |
| `FieldConflictingChildren` | `1 << 22` | Conflicting child transaction IDs |
| `FieldRawMetadata` | `1 << 23` | Full 256-byte on-disk metadata (debugging only) |
| `FieldAllMetadata` | `0x0007_FFFF` | All metadata fields (bits 0-18) |
| `FieldAll` | `0x007F_FFFF` | All client-facing fields (bits 0-22) |

### Get spend status

Check whether specific UTXOs are spent without fetching the full record.

```go
items := []teraslab.GetSpendItem{
    // UtxoHash is required so the server can validate the slot after a
    // reassignment (a mismatched hash returns ErrCodeUtxoHashMismatch).
    {TxID: txid, Vout: 0, UtxoHash: utxoHash0},
    {TxID: txid, Vout: 1, UtxoHash: utxoHash1},
}

results, err := client.GetSpendBatch(ctx, items)
if err != nil {
    log.Fatal(err)
}

for i, r := range results {
    switch r.SlotStatus {
    case teraslab.SlotUnspent:
        fmt.Printf("vout %d: unspent\n", items[i].Vout)
    case teraslab.SlotSpent:
        fmt.Printf("vout %d: spent by %x\n", items[i].Vout, r.SpendingData)
    case teraslab.SlotPruned:
        fmt.Printf("vout %d: pruned\n", items[i].Vout)
    case teraslab.SlotFrozen:
        fmt.Printf("vout %d: frozen\n", items[i].Vout)
    }
}
```

### Pruner operations

```go
// Find transactions unmined since before height 799000.
txids, err := client.QueryOldUnmined(ctx, 799000)

// Find all transactions currently flagged CONFLICTING.
conflicting, err := client.QueryConflicting(ctx)

// Preserve parent transactions from being pruned.
_, err = client.PreserveTransactions(ctx, 900000, txids)

// Delete expired preserved transactions.
result, err := client.ProcessExpiredPreservations(ctx, 800100)
fmt.Printf("deleted %d, failed %d\n", result.Deleted, result.Failed)
```

### Admin operations

```go
// Ping -- returns round-trip time.
rtt, err := client.Ping(ctx)
fmt.Printf("RTT: %v\n", rtt)

// Health check.
err = client.Health(ctx)

// Fetch cluster partition map.
pm, err := client.GetPartitionMap(ctx)
fmt.Printf("version=%d nodes=%d\n", pm.Version, len(pm.Nodes))
for _, node := range pm.Nodes {
    fmt.Printf("  node %d: %s\n", node.ID, node.Addr)
}
```

## Error Handling

The client uses typed errors that work with `errors.Is` and `errors.As`:

| Error type | When | Contents |
|------------|------|----------|
| `*PartialError` | Some items in a batch failed | `.Successes` (signals), `.Errors` (per-item) |
| `*ServerError` | Global server error (all items failed) | `.Code`, `.Message` |
| `*RedirectError` | Shard owned by another node (single-node mode) | `.Addr` |
| `*NotFoundError` | Record not found | -- |
| `*BatchItemError` | Individual item failure (inside `PartialError`) | `.ItemIndex`, `.Code`, `.Data` |

### Partial error handling pattern

```go
result, err := client.SpendBatch(ctx, params, items)
if err != nil {
    var pe *teraslab.PartialError
    if errors.As(err, &pe) {
        // Mixed success/failure. Inspect per-item results.
        for _, e := range pe.Errors {
            switch e.Code {
            case teraslab.ErrCodeTxNotFound:
                // Transaction doesn't exist.
            case teraslab.ErrCodeAlreadySpent:
                // e.Data has the existing 36-byte spending data.
            case teraslab.ErrCodeFrozen:
                // UTXO is frozen.
            case teraslab.ErrCodeCoinbaseImmature:
                // e.Data has the 4-byte required block height (LE).
            }
        }
        return
    }

    var se *teraslab.ServerError
    if errors.As(err, &se) {
        // Global failure (e.g., batch too large, internal error).
        log.Printf("server error %d: %s", se.Code, se.Message)
        return
    }

    log.Fatal(err) // connection error, context cancelled, etc.
}
```

### Signal constants

Mutation operations return per-item signals indicating state transitions:

| Constant | Value | Meaning |
|----------|-------|---------|
| `SignalNone` | 0 | No state transition |
| `SignalAllSpent` | 1 | All UTXOs in this transaction are now spent |
| `SignalNotAllSpent` | 2 | Not all UTXOs are spent (e.g. after unspend) |
| `SignalDeleteAtHeightSet` | 3 | Transaction queued for pruning at a block height |
| `SignalDeleteAtHeightUnset` | 4 | Transaction removed from pruning queue |
| `SignalPreserve` | 5 | Transaction marked for preservation |

### Error codes

| Constant | Code | Description |
|----------|------|-------------|
| `ErrCodeTxNotFound` | 1 | Transaction not in index |
| `ErrCodeUtxoHashMismatch` | 2 | Expected hash does not match stored hash |
| `ErrCodeAlreadySpent` | 3 | UTXO already spent (error data: 36-byte spending data) |
| `ErrCodeAlreadyFrozen` | 4 | UTXO already frozen |
| `ErrCodeUtxoNotFrozen` | 5 | Expected frozen but is not |
| `ErrCodeInvalidSpend` | 6 | Attempting to spend a pruned/deleted UTXO |
| `ErrCodeFrozen` | 7 | UTXO is frozen |
| `ErrCodeConflicting` | 8 | Transaction is marked conflicting |
| `ErrCodeLocked` | 9 | Transaction is locked |
| `ErrCodeCoinbaseImmature` | 10 | Coinbase not mature (error data: 4-byte required height) |
| `ErrCodeVoutOutOfRange` | 11 | Output index exceeds UTXO count |
| `ErrCodeAlreadyExists` | 12 | Transaction already exists (on create) |
| `ErrCodeFrozenUntil` | 13 | Reassignment cooldown not met |
| `ErrCodeRedirect` | 14 | Shard owned by another node |
| `ErrCodeNoQuorum` | 15 | Replication quorum not met (triggers refresh + retry) |
| `ErrCodeStreamNotFound` | 16 | Blob stream id not found |
| `ErrCodeBlobNotFound` | 17 | External blob not found |
| `ErrCodeStreamOffsetMismatch` | 18 | Blob chunk offset mismatch |
| `ErrCodeMigrationInProgress` | 19 | Shard handoff in flight (retryable) |
| `ErrCodeReplicationFailed` | 20 | Ambiguous replication outcome (idempotent-retryable) |
| `ErrCodeMigrationManifest` | 21 | Migration manifest error |
| `ErrCodeMigrationManifestStale` | 22 | Migration manifest stale |
| `ErrCodeTopologyPersistFailed` | 23 | Topology persistence failed |
| `ErrCodeStaleEpoch` | 24 | Local epoch mismatch (retryable) |
| `ErrCodeClusterNotReady` | 25 | Cluster still starting up |
| `ErrCodeIndexDegraded` | 26 | Secondary index degraded |
| `ErrCodeClusterAuthFailed` | 27 | Inter-node HMAC auth failed |
| `ErrCodePayloadMalformed` | 28 | Request payload malformed |
| `ErrCodeOpcodeUnsupported` | 29 | Server does not support the opcode |
| `ErrCodeStorageIO` | 30 | Storage I/O error |
| `ErrCodeRateLimited` | 31 | Rate limited |
| `ErrCodeNotClustered` | 32 | Cluster op issued against a single node |
| `ErrCodeInvariantViolation` | 33 | Server invariant violation |
| `ErrCodeStreamInvariant` | 34 | Blob stream invariant violation |
| `ErrCodeDeletedChildren` | 35 | Operation blocked by deleted children |
| `ErrCodeNotDue` | 36 | Preservation not yet due |
| `ErrCodeMigrationTargetNotReady` | 37 | Migration target not ready |
| `ErrCodeInternal` | 255 | Unexpected server error |

Response status `StatusDegradedDurability` (5) is a successful-but-weak ack: the
mutation was committed locally under a relaxed replication policy. The client
treats it as success.

## Cluster Routing

In cluster mode, the client computes a 12-bit shard from each txid:

```go
shard := teraslab.ShardForTxID(txid) // txid[0:2] as LE uint16, masked to 0x0FFF
```

The 4096 shards are mapped to nodes via the partition map fetched from the server.

**Fan-out.** All multi-txid batch operations split by target node, dispatch
sub-batches in parallel, and merge per-item results/errors back into the
caller's original index order. This covers `SpendBatch`, `UnspendBatch`,
`CreateBatch`, `SetMinedBatch`, `FreezeBatch`, `UnfreezeBatch`, `ReassignBatch`,
`GetBatch`, `GetSpendBatch`, `SetConflictingBatch`, `SetLockedBatch`,
`PreserveUntilBatch`, `DeleteBatch`, `MarkLongestChainBatch`, and
`RemoveConflictingChildBatch` (routed by parent txid). The parameterless query
ops (`QueryOldUnmined`, `QueryConflicting`) fan out to every node and return the
deduplicated union — each node answers for the shards it masters.

**Redirects.** If a node returns a redirect (a shard moved during rebalancing),
the client follows it and triggers a background partition-map refresh. Redirects
carry the server's shard-table version: a redirect whose version is not newer
than the client's known map is treated as stale (loop guard) and stops the
chain. Per-item `ErrCodeRedirect` results in a partial response cause only the
redirected items to be re-sent after a refresh.

**Transient retry.** `ErrCodeMigrationInProgress`, `ErrCodeStaleEpoch`, and
`ErrCodeReplicationFailed` are retried against the same node with bounded
backoff; `ErrCodeNoQuorum` triggers a partition-map refresh and retry.

**Authentication.** Set `ClientConfig.ClusterSecret` to HMAC-sign the
`OP_GET_PARTITION_MAP` bootstrap against clusters configured with a shared
secret. The client also performs an `OP_HELLO` protocol-version handshake on
connect (`Client.NegotiatedVersion()`), degrading to version 1 against older
servers.

## Testing

```bash
# Unit tests (no server needed)
go test ./...

# Integration tests (requires running TeraSlab server)
TERASLAB_ADDR=localhost:3300 go test -tags integration -v ./...
```
