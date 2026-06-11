# Phase 12: Integration testing and benchmarks

**Status:** shipped — integration suites under `tests/`, Docker-based cluster harness under `teraslab-tests/`, criterion benches under `benches/`; `cargo test --all` reports 2234 / 0 / 0.

## Goal

End-to-end testing of the complete system under realistic workloads. Verify that all components work together correctly, measure performance, and validate against the design targets. This phase produces the evidence that TeraSlab is ready for production.

## Dependencies

ALL previous phases (1-11) must be complete with all tests passing.

## What to build

### 12.1 Workload generator — `tests/workload/generator.rs`

A configurable workload generator that produces realistic BSV UTXO operations:

```rust
pub struct WorkloadConfig {
    pub total_operations: u64,
    pub tx_creation_rate: f64,      // fraction of ops that are creates (e.g., 0.1 = 10%)
    pub spend_rate: f64,            // fraction that are spends (e.g., 0.6)
    pub set_mined_rate: f64,        // fraction that are setMined (e.g., 0.2)
    pub read_rate: f64,             // fraction that are reads (e.g., 0.1)
    pub other_rate: f64,            // freeze, unfreeze, reassign, etc.
    pub utxos_per_tx: Distribution, // distribution of output counts per tx
    pub spend_batch_size: Distribution, // distribution of spendMulti batch sizes
    pub large_tx_fraction: f64,     // fraction of txs that are "large" (external storage)
    pub concurrent_clients: usize,
    pub target_ops_per_sec: Option<u64>, // rate limit (None = as fast as possible)
}

pub enum Distribution {
    Fixed(u32),
    Uniform(u32, u32),
    Zipfian { max: u32, exponent: f64 },
}
```

The generator:
1. Creates a pool of transactions
2. Generates a stream of operations against those transactions
3. Operations are realistically sequenced: create → spend/setMined → eventually delete
4. Spends reference UTXOs that actually exist (not random)
5. Tracks expected state for verification

### 12.2 State verifier — `tests/workload/verifier.rs`

An independent in-memory model of the expected state. Every operation is applied to both TeraSlab and the verifier. After the workload completes, verify they match.

```rust
pub struct StateVerifier {
    records: HashMap<TxKey, ExpectedRecord>,
}

struct ExpectedRecord {
    metadata: ExpectedMetadata,
    utxo_slots: Vec<ExpectedSlot>,
}

impl StateVerifier {
    pub fn apply(&mut self, op: &WorkloadOp) -> Result<()>;
    pub fn verify_against(&self, server: &ServerContext) -> Vec<Mismatch>;
}
```

### 12.3 Deterministic simulation — `tests/simulation/mod.rs`

A deterministic simulation framework for testing crash recovery and replication under adversarial conditions:

```rust
pub struct Simulation {
    rng: StdRng,                    // seeded for reproducibility
    clock: SimulatedClock,
    network: SimulatedNetwork,      // with configurable latency, loss, partition
    devices: Vec<SimulatedDevice>,  // with injectable failures
    nodes: Vec<SimulatedNode>,
}

impl Simulation {
    /// Run a workload with random fault injection.
    pub fn run_with_faults(
        &mut self,
        config: SimulationConfig,
    ) -> SimulationResult;
}

pub struct SimulationConfig {
    pub operations: u64,
    pub crash_probability: f64,       // per-operation probability of crashing a node
    pub network_partition_probability: f64,
    pub io_error_probability: f64,
    pub seed: u64,                    // for reproducibility
}

pub struct SimulationResult {
    pub operations_completed: u64,
    pub crashes_injected: u64,
    pub recoveries_completed: u64,
    pub partitions_injected: u64,
    pub data_loss_detected: bool,
    pub inconsistencies_found: Vec<String>,
}
```

### 12.4 Performance benchmarks — `benches/`

Using `criterion` for rigorous benchmarking:

#### Single-node benchmarks:

```
- Spend throughput (1, 4, 8, 16, 32 threads)
- SpendMulti throughput (batch sizes 1, 5, 10, 50, 100)
- SetMined throughput (1, 4, 8, 16 threads)
- Create throughput (varying UTXO counts: 1, 10, 100, 1000)
- Read throughput (point reads)
- Mixed workload throughput (realistic ratio)
- Latency histograms (p50, p90, p99, p99.9, p99.99)
```

#### Cluster benchmarks:

```
- 3-node cluster throughput with RF=2
- Write latency with synchronous replication
- Rebalancing time after node addition
- Recovery time after node crash
- Catchup time for stale replica
```

#### Comparison benchmarks:

```
- Compare against the previous implementation with an identical workload
  (requires running both systems with the same data and ops)
- Measure: throughput, latency, SSD write amplification, memory usage
```

### 12.5 Stress tests — `tests/stress/`

Long-running tests designed to surface rare bugs:

```rust
/// Run random operations for hours, verify consistency periodically.
fn stress_random_operations() {
    // 8 threads, 10 million operations, verify every 100K ops
}

/// Fill device to 90% capacity, then churn (create + delete), verify no fragmentation death spiral.
fn stress_device_fill_and_churn() {
    // Run until the device has been filled and freed 10x
}

/// Rapid cluster topology changes during load.
fn stress_cluster_churn() {
    // 5-node cluster, randomly add/remove nodes every 10 seconds, verify no data loss
}
```

## Acceptance criteria

### End-to-end correctness tests

```
- [ ] 100K operations (mixed workload), single node: state verifier finds zero mismatches
- [ ] 100K operations with concurrent clients (10 threads): zero mismatches
- [ ] 100K operations with crash injection (1% crash rate, 10 seeds): zero data loss after recovery
- [ ] 100K operations with replication (RF=2): replica state matches master exactly
- [ ] 100K operations with network partitions: states converge after partition heals
```

### Realistic workload tests

```
- [ ] Simulate block arrival: create 3000 txs, setMined all, spend 50% of UTXOs:
      correct state, all signals match expected
- [ ] Simulate block reorg: setMined, then unsetMined on same block: state reverted correctly
- [ ] Simulate mempool churn: create 10000 txs, spend random UTXOs, mark conflicting,
      prune old txs: all operations complete correctly
- [ ] Large transaction handling: create 10MB tx, spend UTXOs, read back:
      cold data on correct tier, UTXO operations unaffected by data size
```

### Tiered storage integration tests

```
- [ ] Mixed tier workload: 10000 small txs (inline), 100 medium (separate), 5 large (external):
      all data accessible, all operations work, all tiers clean up on delete
- [ ] External blob upload delay: create large tx, spend UTXO before upload completes: succeeds
- [ ] Read large tx: cold data streamed correctly from blob store
```

### Performance targets (from SPEC_BRIEFING.md)

```
- [ ] Single-node spend throughput: > 500K ops/sec (on NVMe device)
- [ ] Single-node spend p99 latency: < 1ms
- [ ] Single-node spend p99.9 latency: < 5ms
- [ ] SpendMulti (batch 10) throughput: > 200K batches/sec
- [ ] SetMined throughput: > 500K ops/sec
- [ ] Create throughput (10 UTXOs): > 100K ops/sec
- [ ] Cluster (3-node RF=2) spend throughput: > 300K ops/sec per node
- [ ] Cluster replication overhead: < 30% throughput reduction vs single-node
- [ ] SSD write amplification for spend: < 10x (compare bytes written to device vs logical data changed)
- [ ] Memory per record: < 64 bytes (the previous implementation's baseline is 64 bytes)
```

Note: Exact targets may vary by hardware. Document the test hardware spec and provide context for all numbers.

### Deterministic simulation tests

```
- [ ] 10 different seeds, each 50K ops, crash probability 1%: zero inconsistencies
- [ ] 10 different seeds, each 50K ops, network partition probability 5%: states converge
- [ ] 5 different seeds, 100K ops, combined crash + partition + IO errors: zero data loss
- [ ] Reproduce a failure: given seed X that found a bug, re-run produces same failure
```

### Long-running stability tests

```
- [ ] 1 hour continuous mixed workload: no memory growth (RSS stable within 10%)
- [ ] 1 hour continuous workload: no file descriptor leak
- [ ] 1 hour continuous workload: throughput does not degrade over time
- [ ] Device fill to 80% + sustained workload: performance remains stable
      (no defrag death spiral since we use freelist, not defrag)
```

### Documentation produced by this phase

```
- [ ] Performance report: throughput and latency for all operation types
- [ ] Hardware recommendations: NVMe device requirements, memory sizing, network bandwidth
- [ ] Tuning guide: key configuration parameters and their effect on performance
- [ ] Comparison report: TeraSlab vs the previous implementation on identical workload
```

## This is the final phase

After completing this phase, TeraSlab is a production-ready UTXO store server. The remaining work is:
- Writing `teraslab-client-go` (reusable Go client library, separate repo)
- Writing the Teranode adapter (`stores/utxo/teraslab/` in Teranode repo)
- Admin CLI and Web UI (Phase 13)
- Deploying to Teratestnet
- Production hardening based on real-world feedback
