# BSV UTXO Store — Recommended Rust Crates

Companion document to `BSV_UTXO_STORE_SPEC.md`. Lists recommended crates for each subsystem with trade-off analysis.

---

## 1. io_uring

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`io-uring`** | 0.7+ | Low-level, thin wrapper over `liburing`. Full control over SQE/CQE lifecycle. No runtime dependency. | **Primary choice** — maximum control for custom batching architecture |
| `tokio-uring` | 0.5+ | Tokio integration for io_uring. Provides async/await over io_uring ops. | Consider if using Tokio for networking layer |
| `glommio` | 0.9+ | Thread-per-core runtime built on io_uring. Opinionated architecture. | Too opinionated — conflicts with custom thread model |
| `nuclei` | 0.4+ | Proactive I/O runtime. Less mature. | Not recommended — insufficient maturity |

**Recommendation**: Use **`io-uring`** directly. The custom batching architecture (per-device submission threads, ring buffers, batch coalescing) requires low-level control that higher-level wrappers abstract away. The submission thread draining model described in the spec maps directly to `io-uring`'s API.

---

## 2. Networking

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`tokio`** | 1.x | Industry-standard async runtime. Excellent for TCP server, client connections, timers. | **Primary choice for networking** |
| `mio` | 1.x | Low-level, non-blocking I/O. Foundation for tokio. | Only if avoiding tokio entirely |
| `quinn` | 0.11+ | QUIC protocol implementation over tokio. | Future option for inter-node replication |
| `hyper` | 1.x | HTTP/1 and HTTP/2. | For metrics/health endpoints only |
| `axum` | 0.8+ | Web framework on hyper+tokio. | For observability HTTP endpoints |
| `tonic` | 0.12+ | gRPC framework on tokio. | Alternative wire protocol (protobuf-based) |

**Recommendation**: Use **`tokio`** for the networking layer (client connections, replication streams, heartbeat). Use raw TCP with a custom binary protocol (as spec'd) rather than gRPC — the UTXO workload benefits from a purpose-built framing protocol. Use **`axum`** for the HTTP observability endpoints (`/metrics`, `/health`, `/status`).

**Note on io_uring + tokio**: The storage layer uses `io-uring` directly (not tokio's I/O), while the networking layer uses tokio's TCP. These can coexist — storage I/O runs on dedicated submission/completion threads, networking runs on tokio's runtime.

---

## 3. Hash Table (Index)

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`hashbrown`** | 0.15+ | Robin Hood hashing, based on Google's SwissTable. Rust stdlib's HashMap backend. | **Starting point** for prototype |
| Custom implementation | — | Fixed-size, open-addressing with Robin Hood. Mmap'd with hugepages. | **Production choice** — spec requires hugepage backing, NUMA-aware placement |

**Recommendation**: **Custom implementation** for production. The index has very specific requirements:
- Fixed 16-byte entries (fingerprint + offset)
- Mmap'd with `MAP_HUGETLB` (2MB or 1GB hugepages)
- NUMA-pinned allocation
- No heap allocation per entry
- Checkpoint to disk for fast recovery

Use `hashbrown` for prototyping and testing. The actual production index should be a purpose-built open-addressing table using Robin Hood probing, backed by hugepage mmap.

**Supporting crate for hugepages:**

| Crate | Notes |
|-------|-------|
| **`memmap2`** | Mmap wrapper; use with `MAP_HUGETLB` via raw `libc::mmap` for hugepages |
| `libc` | Direct access to `mmap`, `MAP_HUGETLB`, `MADV_HUGEPAGE`, NUMA syscalls |
| `nix` | Higher-level Unix API wrapper, includes mmap |

---

## 4. Serialization (Wire Protocol)

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`bytes`** | 1.x | Efficient byte buffer management. Zero-copy slicing. | **Essential** — use for all buffer management |
| **`byteorder`** | 1.x | Read/write integers in big/little endian. | **Essential** — for protocol encoding |
| `bincode` | 2.x | Compact binary serialization. | Good for redo log entries |
| `rkyv` | 0.8+ | Zero-copy deserialization. | For index checkpoint format |
| `serde` | 1.x | Serialization framework. | For config files, not hot path |
| `protobuf` / `prost` | — | Protocol Buffers. | Not needed — custom binary protocol preferred |
| `flatbuffers` | — | Zero-copy serialization. | Over-engineered for this use case |

**Recommendation**: Use **`bytes`** + **`byteorder`** for the wire protocol (manual encoding/decoding of the custom binary frames). The protocol is simple enough that a derive macro isn't needed, and manual encoding gives full control over layout. Use **`bincode`** for redo log entry serialization (compact, fast, derives easily). Use **`rkyv`** if zero-copy deserialization of index checkpoints proves important.

---

## 5. Cryptography

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`sha2`** | 0.10+ | SHA-256 implementation. Pure Rust with hardware acceleration. | **Primary** — for content hashing |
| **`ripemd`** | 0.1+ | RIPEMD-160 implementation. | For any RIPEMD-160 needs (legacy digest compat) |
| `ring` | 0.17+ | Crypto library with SHA-256. C/ASM backend. | Alternative if maximum SHA-256 perf needed |
| `crc32fast` | 1.x | Hardware-accelerated CRC32. | **Essential** — for redo log and protocol checksums |

**Recommendation**: Use **`sha2`** for SHA-256 (content addressing of blobs), **`ripemd`** if RIPEMD-160 compatibility is needed (the previous implementation used RIPEMD-160 for key digests), and **`crc32fast`** for protocol/redo checksums.

**Note**: The BSV txid is already a double-SHA-256 hash computed by the Go client. The Rust store doesn't need to recompute it — it's passed as a 32-byte key.

---

## 6. Lock-Free Structures & Synchronization

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`parking_lot`** | 0.12+ | Fast mutex/rwlock implementations. 2-3x faster than std. | **Primary** — for lock striping (65,536 mutexes) |
| **`crossbeam`** | 0.8+ | Lock-free data structures (queues, deques, epoch GC). | **Essential** — for per-device ring buffers |
| `flume` | 0.11+ | Fast MPMC channel. | For inter-thread communication |
| `dashmap` | 6.x | Concurrent HashMap. | For external tx cache (txid → blob data) |

**Recommendation**:
- **`parking_lot::Mutex`** for the 65,536-stripe lock table (smaller, faster than std Mutex)
- **`crossbeam::queue::ArrayQueue`** for per-device lock-free ring buffers (bounded, lock-free MPMC)
- **`crossbeam::utils::CachePadded`** to prevent false sharing on per-thread counters
- **`dashmap`** or **`moka`** for the external transaction cache (with TTL expiry)
- **`flume`** for completion notification channels (faster than tokio mpsc for pure data)

---

## 7. Memory Mapping

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`memmap2`** | 0.9+ | Safe mmap wrapper. Cross-platform. | **Primary** — for index checkpoint files |
| `libc` | 0.2+ | Raw mmap with hugepage flags. | **Essential** — for hugepage-backed index |

**Recommendation**: Use **`libc::mmap`** directly for the index (need `MAP_HUGETLB`, `MAP_HUGE_2MB`, `MAP_POPULATE`, NUMA placement via `mbind`). Use **`memmap2`** for simpler mmap needs (index checkpoint files, config files).

**Hugepage setup example:**
```rust
use libc::{mmap, MAP_ANONYMOUS, MAP_HUGETLB, MAP_PRIVATE, MAP_POPULATE, PROT_READ, PROT_WRITE};

let size = num_entries * 16; // 16 bytes per IndexEntry
let ptr = unsafe {
    mmap(
        std::ptr::null_mut(),
        size,
        PROT_READ | PROT_WRITE,
        MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB | MAP_POPULATE,
        -1,
        0,
    )
};
```

---

## 8. Metrics & Observability

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`metrics`** | 0.24+ | Metrics facade (like `log` for logging). | **Primary** — standard metrics API |
| **`metrics-exporter-prometheus`** | 0.16+ | Prometheus exporter for `metrics`. | **Primary** — for /metrics endpoint |
| **`hdrhistogram`** | 7.x | High Dynamic Range histogram. Accurate percentiles. | **Essential** — for latency histograms |
| `prometheus` | 0.13+ | Prometheus client library. | Alternative to metrics facade |
| `tracing` | 0.1+ | Structured logging/tracing. | For debug logging, not hot-path metrics |

**Recommendation**: Use the **`metrics`** facade with **`metrics-exporter-prometheus`** for exporting. Use **`hdrhistogram`** for accurate percentile tracking (p50, p95, p99, p99.9) on latency metrics. Use **`tracing`** for structured debug logging (not on hot path).

**Per-thread counter pattern:**
```rust
use std::sync::atomic::{AtomicU64, Ordering};
use crossbeam::utils::CachePadded;

#[repr(align(64))]  // cache-line aligned
struct ThreadMetrics {
    ops_spend: CachePadded<AtomicU64>,
    ops_create: CachePadded<AtomicU64>,
    // ...
}
```

---

## 9. Testing

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`proptest`** | 1.x | Property-based testing. Shrinking, strategy composition. | **Essential** — for invariant testing |
| **`criterion`** | 0.5+ | Statistical benchmarking. | **Essential** — for performance benchmarks |
| `quickcheck` | 1.x | Property testing (simpler than proptest). | Alternative to proptest |
| `loom` | 0.7+ | Concurrency testing. Explores thread interleavings. | **Highly recommended** — for lock-free code |
| `tempfile` | 3.x | Temporary files for tests. | For device simulation in tests |
| `test-log` | 0.2+ | Tracing subscriber for tests. | For debugging test failures |
| `insta` | 1.x | Snapshot testing. | For protocol serialization tests |

**Recommendation**:
- **`proptest`** for property-based testing of UTXO operations (idempotency, counter consistency, spend/unspend reversibility)
- **`criterion`** for all performance benchmarks (spend throughput, index lookup, io_uring batching)
- **`loom`** for testing lock-free data structures (ring buffers, concurrent index operations)
- **`tempfile`** for creating temporary block devices in tests

**Simulation testing framework**: Build custom using `proptest` strategies + deterministic scheduler. No off-the-shelf crate provides FoundationDB-style simulation — this must be purpose-built.

---

## 10. Configuration & CLI

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`clap`** | 4.x | Command-line argument parser. | For server CLI |
| **`serde`** + **`toml`** | — | Config file parsing. | For server config (TOML format) |
| `figment` | 0.10+ | Layered configuration. | For config merging (file + env + CLI) |

---

## 11. Error Handling

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`thiserror`** | 2.x | Derive macro for error types. | For library error types |
| **`anyhow`** | 1.x | Flexible error handling. | For application-level errors |

---

## 12. NUMA & CPU Affinity

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`libc`** | 0.2+ | `sched_setaffinity`, `mbind`, `set_mempolicy`. | **Essential** — for NUMA pinning |
| `hwloc2` | 0.6+ | Hardware locality (topology detection). | For automatic NUMA/NVMe affinity |
| `core_affinity` | 0.8+ | Simple CPU pinning. | For thread-to-core binding |

**Recommendation**: Use **`hwloc2`** to detect NUMA topology and NVMe-to-NUMA-node mapping at startup. Pin submission/completion threads to the NUMA node closest to their NVMe device. Pin index memory via `mbind` to the appropriate NUMA node.

---

## 13. External Blob Storage

| Crate | Version | Notes | Recommendation |
|-------|---------|-------|---------------|
| **`aws-sdk-s3`** | 1.x | Official AWS S3 SDK. | For S3-compatible blob storage |
| `opendal` | 0.50+ | Unified storage access (S3, FS, HTTP, etc.). | **Recommended** — single API for all blob backends |
| `reqwest` | 0.12+ | HTTP client. | For HTTP-based blob storage |
| `moka` | 0.12+ | Concurrent cache with TTL. | For external tx cache (replacing the legacy in-memory cache) |

**Recommendation**: Use **`opendal`** for blob storage abstraction (supports S3, MinIO, local filesystem, HTTP out of the box). Use **`moka`** for the 10-second TTL cache that replaces the Go `externalTxCache`.

---

## 14. Crate Summary by Subsystem

| Subsystem | Primary Crates |
|-----------|---------------|
| **Storage I/O** | `io-uring`, `libc` |
| **Networking** | `tokio`, `bytes`, `byteorder` |
| **Cluster Membership** | `foca` (SWIM protocol) |
| **Index** | Custom + `libc` (hugepages), `memmap2` (checkpoints) |
| **Wire Protocol** | `bytes`, `byteorder`, `crc32fast` |
| **Concurrency** | `parking_lot`, `crossbeam`, `flume`, `dashmap` |
| **Replication** | `tokio`, `bincode` |
| **Crash Safety** | `bincode`, `crc32fast` |
| **Metrics** | `metrics`, `metrics-exporter-prometheus`, `hdrhistogram` |
| **HTTP Endpoints** | `axum`, `hyper` |
| **Crypto** | `sha2`, `ripemd`, `crc32fast` |
| **Blob Storage** | `opendal`, `moka` |
| **Testing** | `proptest`, `criterion`, `loom`, `tempfile` |
| **Config/CLI** | `clap`, `serde`, `toml` |
| **Error Handling** | `thiserror`, `anyhow` |
| **Logging** | `tracing`, `tracing-subscriber` |
| **NUMA** | `hwloc2`, `core_affinity`, `libc` |
