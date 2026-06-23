# TeraSlab Architecture Comparison

## Architecture Differences

| Aspect | Previous Design | TeraSlab |
|--------|-----------|----------|
| Storage model | General-purpose key-value | Purpose-built for UTXO workload |
| Record layout | Variable-size bins | Fixed-size metadata + pre-allocated UTXO slots |
| Index | In-memory hash (sprigs) | mmap'd hash table with hugepage support |
| Write path | Log-structured (requires defrag) | Direct-placement with freelist (no defrag) |
| UTXO logic | Lua UDF on server | Native Rust implementation |
| Spend I/O | Read full record → Lua → write full record | Read slot (73B incl. 4-byte CRC) → validate → write 73B slot + 320B metadata |
| Replication | Built-in (complex) | Operation-based, purpose-built |
| Tiered storage | None (all inline) | Inline / external blob (separate-NVMe middle tier **not implemented** — see below) |

## Expected Performance Advantages

### Write Amplification

The original implementation writes the entire record on every mutation
(the Lua UDF modifies bins, then the full record is written). For a
transaction with 100 UTXOs:

- **Original**: ~7.5 KB written per spend (full record rewrite)
- **TeraSlab**: ~73 bytes written per UTXO slot (69-byte payload + 4-byte CRC32 footer) + 320 bytes metadata

**Reduction: ~10-30x less SSD wear per spend operation.**

### Memory Per Record

- **Original**: 64 bytes per record (in-memory index)
- **TeraSlab**: 64 bytes per hash table bucket = 1 probe_distance + 32 txid + 31 TxIndexEntry (one cache line, `#[repr(C, packed)]`). The core index entry (TxIndexEntry) is 31 bytes; the full bucket including the Robin Hood probe byte and key is exactly 64 bytes.

**TeraSlab uses 64 bytes per bucket (one cache line) including all overhead.** At load factor 0.5 (recommended), effective memory per record is ~128 bytes counting empty buckets. Actual resident memory depends on load factor.

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
