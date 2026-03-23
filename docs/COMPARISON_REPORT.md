# TeraSlab Architecture Comparison

## Architecture Differences

| Aspect | Previous Design | TeraSlab |
|--------|-----------|----------|
| Storage model | General-purpose key-value | Purpose-built for UTXO workload |
| Record layout | Variable-size bins | Fixed-size metadata + pre-allocated UTXO slots |
| Index | In-memory hash (sprigs) | mmap'd hash table with hugepage support |
| Write path | Log-structured (requires defrag) | Direct-placement with freelist (no defrag) |
| UTXO logic | Lua UDF on server | Native Rust implementation |
| Spend I/O | Read full record → Lua → write full record | Read slot (69B) → validate → write slot (69B) |
| Replication | Built-in (complex) | Operation-based, purpose-built |
| Tiered storage | None (all inline) | Inline / separate NVMe / external blob |

## Expected Performance Advantages

### Write Amplification

The original implementation writes the entire record on every mutation
(the Lua UDF modifies bins, then the full record is written). For a
transaction with 100 UTXOs:

- **Original**: ~7.5 KB written per spend (full record rewrite)
- **TeraSlab**: ~69 bytes written per spend (single slot update) + ~256 bytes metadata

**Reduction: ~10-30x less SSD wear per spend operation.**

### Memory Per Record

- **Original**: 64 bytes per record (in-memory index)
- **TeraSlab**: 58 bytes per record (TxKey + TxIndexEntry)

**TeraSlab meets the <64 byte target.**

### Latency

The previous design's Lua UDF adds interpretation overhead. TeraSlab's
native Rust spend path eliminates this:

- No Lua VM startup per operation
- No record deserialization/reserialization
- Direct byte-level slot access with known offsets
- Lock striping for concurrent access (vs per-record locking)

### Defragmentation

The original implementation uses log-structured storage requiring
continuous defragmentation. Under sustained write load, defrag competes
with application I/O:

- **Original**: Defrag death spiral at high utilization (>60%)
- **TeraSlab**: Freelist-based allocation, no defrag needed, stable performance at 80%+ utilization

## Running Comparison Benchmarks

To produce actual comparison numbers, run both systems with identical workloads:

```bash
# 1. Start TeraSlab
# 2. Run the workload:
TERASLAB_FULL_WORKLOAD=1 cargo test --test e2e_workload -- perf_ --nocapture
# 3. Measure: throughput, latency, SSD bytes written, RSS memory
```

Detailed comparison requires production hardware and is planned for
the Teratestnet deployment phase.
