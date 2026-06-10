//! Deterministic simulation framework for testing crash recovery
//! under adversarial conditions.
//!
//! Uses seeded RNG for reproducibility. Every simulation run with the
//! same seed produces the same sequence of operations and faults.
//!
//! N-01 (2026-05-29 audit): crashes exercise the REAL recovery path.
//! Every mutation is WAL-first (mirroring `server/dispatch.rs`): the
//! redo entry is appended + fsynced to a redo device that survives the
//! crash, and `recover()` rebuilds the engine via
//! `recovery::recover_all_with_allocator` — the same pipeline
//! production startup uses. The in-memory reference model is NEVER
//! re-synced from engine state (or vice versa) after a crash, so the
//! final verification is a true differential check: a redo entry lost,
//! double-applied, or replayed non-idempotently shows up as a
//! `utxo_count`/`spent_utxos` mismatch.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;
use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, DeviceError, MemoryDevice};
use teraslab::index::{DahBackend, PrimaryBackend, TxKey, UnminedBackend};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::*;
use teraslab::ops::engine::Engine;
use teraslab::ops::set_mined::*;
use teraslab::ops::spend::*;
use teraslab::recovery::recover_all_with_allocator;
use teraslab::redo::{RedoLog, RedoOp};

/// Redo-log capacity for simulated nodes. The log is never checkpointed
/// during a run (every recovery replays from genesis), so it must hold
/// the full op history: full-mode runs write ~25 MB.
const SIM_REDO_LOG_SIZE: u64 = 128 * 1024 * 1024;

/// Configuration for a deterministic simulation run.
#[derive(Debug, Clone)]
pub struct SimulationConfig {
    /// Total operations to execute.
    pub operations: u64,
    /// Per-operation probability of crashing a node (0.0–1.0).
    pub crash_probability: f64,
    /// Per-device-I/O probability of an injected read/write error.
    pub io_error_probability: f64,
    /// RNG seed for reproducibility.
    pub seed: u64,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            operations: 10_000,
            crash_probability: 0.0,
            io_error_probability: 0.0,
            seed: 42,
        }
    }
}

/// Results from a simulation run.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SimulationResult {
    /// Total operations completed.
    pub operations_completed: u64,
    /// Number of simulated crashes injected (including crashes forced by
    /// an injected I/O error after the WAL fsync).
    pub crashes_injected: u64,
    /// Number of successful recoveries.
    pub recoveries_completed: u64,
    /// Number of injected device I/O errors observed by an operation.
    pub io_errors_injected: u64,
    /// Whether any data loss was detected.
    pub data_loss_detected: bool,
    /// List of inconsistencies found.
    pub inconsistencies_found: Vec<String>,
}

/// Simulated clock for deterministic time control.
#[derive(Debug)]
pub struct SimulatedClock {
    current_ms: u64,
}

impl SimulatedClock {
    /// Create a new simulated clock starting at the given time.
    pub fn new(start_ms: u64) -> Self {
        Self {
            current_ms: start_ms,
        }
    }

    /// Advance the clock by the given number of milliseconds.
    pub fn advance(&mut self, ms: u64) {
        self.current_ms += ms;
    }

    /// Get the current simulated time in milliseconds.
    pub fn now_ms(&self) -> u64 {
        self.current_ms
    }
}

/// Simple xorshift64 RNG (matches the one in workload generator).
#[derive(Debug, Clone)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() & 0xFFFF_FFFF) as u32
    }

    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn chance(&mut self, p: f64) -> bool {
        self.next_f64() < p
    }
}

/// `BlockDevice` wrapper that fails reads/writes with a configured
/// probability (N-02: honest `io_error_probability` implementation).
///
/// Uses its OWN seeded RNG so injected-fault decisions do not perturb
/// the op-sequence RNG — runs stay reproducible per seed. Injection is
/// disabled during recovery and final verification: those phases must
/// see the true device bytes (a recovery that tolerates injected read
/// errors silently would mask real corruption).
pub struct FlakyDevice {
    inner: Arc<MemoryDevice>,
    enabled: AtomicBool,
    /// Failure probability, stored as f64 bits.
    probability_bits: AtomicU64,
    rng: Mutex<SimRng>,
}

impl FlakyDevice {
    pub fn new(inner: Arc<MemoryDevice>) -> Self {
        Self {
            inner,
            enabled: AtomicBool::new(false),
            probability_bits: AtomicU64::new(0f64.to_bits()),
            rng: Mutex::new(SimRng::new(1)),
        }
    }

    /// Set the failure probability and reseed the fault RNG. Called once
    /// at the start of each run.
    pub fn configure(&self, probability: f64, seed: u64) {
        self.probability_bits
            .store(probability.to_bits(), Ordering::Relaxed);
        *self.rng.lock() = SimRng::new(seed);
        self.enabled
            .store(probability > 0.0, Ordering::Relaxed);
    }

    /// Enable/disable injection; returns the previous state.
    pub fn set_enabled(&self, enabled: bool) -> bool {
        self.enabled.swap(enabled, Ordering::Relaxed)
    }

    fn should_fail(&self) -> bool {
        if !self.enabled.load(Ordering::Relaxed) {
            return false;
        }
        let p = f64::from_bits(self.probability_bits.load(Ordering::Relaxed));
        p > 0.0 && self.rng.lock().chance(p)
    }

    fn injected() -> DeviceError {
        DeviceError::Io(std::io::Error::other("simulated I/O fault"))
    }
}

impl BlockDevice for FlakyDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> teraslab::device::Result<usize> {
        if self.should_fail() {
            return Err(Self::injected());
        }
        self.inner.pread(buf, offset)
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> teraslab::device::Result<usize> {
        if self.should_fail() {
            return Err(Self::injected());
        }
        self.inner.pwrite(buf, offset)
    }

    fn alignment(&self) -> usize {
        self.inner.alignment()
    }

    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn sync(&self) -> teraslab::device::Result<()> {
        self.inner.sync()
    }

    // Deliberately NOT forwarding `as_raw_ptr`: the engine caches the raw
    // pointer at construction (Engine::new) and the zero-copy fast path
    // would bypass injection entirely.
    fn as_raw_ptr(&self) -> Option<*mut u8> {
        None
    }
}

/// A simulated node that wraps an Engine and can be "crashed" and "recovered".
pub struct SimulatedNode {
    engine: Option<Arc<Engine>>,
    /// Data device (FlakyDevice over a MemoryDevice). Survives crashes —
    /// it is the "platter".
    device: Arc<dyn BlockDevice>,
    /// Injection control handle for the data device.
    flaky: Arc<FlakyDevice>,
    /// WAL device. Survives crashes; never flaky (torn-WAL modelling is
    /// out of scope — the redo log's own CRC framing covers that).
    redo_device: Arc<MemoryDevice>,
    /// Live redo log handle; dropped on crash, reopened on recover.
    redo: Option<Arc<Mutex<RedoLog>>>,
    /// Whether this node is currently "down".
    is_crashed: bool,
}

impl SimulatedNode {
    /// Create a new simulated node with a fresh engine.
    pub fn new(device_size: u64, block_size: u32) -> Self {
        let inner = Arc::new(MemoryDevice::new(device_size, block_size as usize).unwrap());
        let flaky = Arc::new(FlakyDevice::new(inner));
        let device: Arc<dyn BlockDevice> = flaky.clone();
        let redo_device =
            Arc::new(MemoryDevice::new(SIM_REDO_LOG_SIZE, block_size as usize).unwrap());
        let mut node = Self {
            engine: None,
            device,
            flaky,
            redo_device,
            redo: None,
            is_crashed: true,
        };
        node.recover();
        node
    }

    /// Get a reference to the engine, if the node is up.
    pub fn engine(&self) -> Option<&Arc<Engine>> {
        if self.is_crashed {
            None
        } else {
            self.engine.as_ref()
        }
    }

    /// The node's live redo log handle (panics if crashed).
    pub fn redo_handle(&self) -> Arc<Mutex<RedoLog>> {
        self.redo.as_ref().expect("node is up").clone()
    }

    /// Injection control for the node's data device.
    pub fn flaky(&self) -> &Arc<FlakyDevice> {
        &self.flaky
    }

    /// Simulate a crash: all in-memory state (engine, index, allocator,
    /// redo handle) is dropped. Only the device bytes survive.
    pub fn crash(&mut self) {
        self.engine = None;
        self.redo = None;
        self.is_crashed = true;
    }

    /// Recover from a crash through the REAL production pipeline:
    /// reopen the surviving redo log, rebuild fresh index/allocator/
    /// secondaries, and replay via `recover_all_with_allocator` —
    /// exactly what `teraslab-server` startup does.
    pub fn recover(&mut self) {
        // Recovery must observe the true device bytes.
        let was_enabled = self.flaky.set_enabled(false);

        let redo = RedoLog::open(
            self.redo_device.clone() as Arc<dyn BlockDevice>,
            0,
            SIM_REDO_LOG_SIZE,
        )
        .unwrap();
        let mut primary = PrimaryBackend::new_in_memory(100_000).unwrap();
        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        let mut alloc = SlotAllocator::new(self.device.clone()).unwrap();

        let stats = recover_all_with_allocator(
            &*self.device,
            &redo,
            &mut primary,
            &mut dah,
            &mut unmined,
            Some(&mut alloc),
        )
        .expect("recovery must not error");
        // Replaying the full log over whatever state survived the crash
        // must converge: every entry is Applied or Skipped, never Failed.
        // A Failed entry IS the recovery bug this harness exists to catch.
        assert_eq!(
            stats.entries_failed, 0,
            "redo replay must be idempotent over crashed device state",
        );

        let redo_arc = Arc::new(Mutex::new(redo));
        alloc.set_redo_log(redo_arc.clone());
        let engine = Arc::new(Engine::new(
            self.device.clone(),
            primary,
            alloc,
            StripedLocks::new(1024),
            dah,
            unmined,
        ));
        engine.set_redo_log(redo_arc.clone());

        self.redo = Some(redo_arc);
        self.engine = Some(engine);
        self.is_crashed = false;
        self.flaky.set_enabled(was_enabled);
    }

    /// Whether this node is currently operational.
    pub fn is_up(&self) -> bool {
        !self.is_crashed
    }
}

/// Deterministic simulation framework.
///
/// Runs a workload with random fault injection against one or more
/// simulated nodes, verifying data consistency throughout.
pub struct Simulation {
    rng: SimRng,
    clock: SimulatedClock,
    nodes: Vec<SimulatedNode>,
}

impl Simulation {
    /// Create a new simulation with a single node.
    pub fn new_single_node(seed: u64) -> Self {
        let node = SimulatedNode::new(512 * 1024 * 1024, 4096);
        Self {
            rng: SimRng::new(seed),
            clock: SimulatedClock::new(1710000000000),
            nodes: vec![node],
        }
    }

    /// Create a simulation with multiple nodes.
    #[allow(dead_code)]
    pub fn new_multi_node(seed: u64, node_count: usize) -> Self {
        let nodes: Vec<SimulatedNode> = (0..node_count)
            .map(|_| SimulatedNode::new(256 * 1024 * 1024, 4096))
            .collect();
        Self {
            rng: SimRng::new(seed),
            clock: SimulatedClock::new(1710000000000),
            nodes,
        }
    }

    /// Crash node 0 and bring it back through real recovery.
    fn crash_and_recover(&mut self, result: &mut SimulationResult) {
        self.nodes[0].crash();
        result.crashes_injected += 1;
        self.nodes[0].recover();
        result.recoveries_completed += 1;
    }

    /// Run a workload with random fault injection.
    ///
    /// Operations are applied to node 0 (the "primary") with the same
    /// WAL-first discipline as `server/dispatch.rs`: redo append+fsync
    /// BEFORE the engine mutation. Fault semantics:
    ///
    /// - Failure BEFORE the redo fsync → the op never happened; the
    ///   reference model is untouched.
    /// - Failure AFTER the redo fsync (engine apply hit an injected I/O
    ///   error) → the WAL is authoritative: the op is committed in the
    ///   reference model and the node is immediately crashed + recovered,
    ///   forcing replay to materialize the lost device write. This is the
    ///   "redo durable, apply lost" window — the highest-value recovery
    ///   scenario.
    ///
    /// After the workload, the reference model is compared against the
    /// surviving engine state with no re-syncing in either direction.
    pub fn run_with_faults(&mut self, config: SimulationConfig) -> SimulationResult {
        let mut result = SimulationResult {
            operations_completed: 0,
            crashes_injected: 0,
            recoveries_completed: 0,
            io_errors_injected: 0,
            data_loss_detected: false,
            inconsistencies_found: Vec::new(),
        };

        // Fault RNG is independent of the op RNG so the op sequence is
        // identical across runs with the same seed regardless of where
        // faults land.
        self.nodes[0]
            .flaky()
            .configure(config.io_error_probability, config.seed ^ 0xF1A4_DEAD_BEEF);

        // Reference model: txid -> (utxo_count, spent_count, utxo_hashes)
        type ReferenceMap = HashMap<[u8; 32], (u32, u32, Vec<[u8; 32]>)>;
        let mut reference: ReferenceMap = HashMap::new();

        let mut next_tx: u32 = 0;
        let mut current_block_height: u32 = 1000;
        let mut next_block_id: u32 = 1;

        for op_idx in 0..config.operations {
            // Random crash injection: all in-memory state is lost; the
            // WAL + device survive; recovery must reconstruct.
            if self.rng.chance(config.crash_probability) && self.nodes[0].is_up() {
                self.crash_and_recover(&mut result);
            }

            if !self.nodes[0].is_up() {
                continue;
            }

            let engine = self.nodes[0].engine().unwrap().clone();
            let redo = self.nodes[0].redo_handle();

            // Periodically advance block height
            if op_idx % 200 == 0 {
                current_block_height += 1;
            }
            self.clock.advance(1);

            // Choose operation type
            let roll = self.rng.next_f64();

            if roll < 0.2 || reference.is_empty() {
                // Create (WAL-first, mirrors dispatch handle_create_batch)
                let tx_n = next_tx;
                next_tx += 1;
                let tx_id = make_sim_tx_id(tx_n);
                let utxo_count = 1 + (self.rng.next_u32() % 10);
                let utxo_hashes: Vec<[u8; 32]> = (0..utxo_count)
                    .map(|v| make_sim_utxo_hash(tx_n, v))
                    .collect();

                let req = CreateRequest {
                    tx_id,
                    tx_version: 1,
                    locktime: 0,
                    fee: 500,
                    size_in_bytes: 250,
                    extended_size: 0,
                    is_coinbase: false,
                    spending_height: 0,
                    utxo_hashes: &utxo_hashes,
                    inputs: None,
                    outputs: None,
                    inpoints: None,
                    is_external: false,
                    created_at: self.clock.now_ms(),
                    block_height: current_block_height,
                    mined_block_infos: &[],
                    frozen: false,
                    conflicting: false,
                    locked: false,
                    external_ref: None,
                    parent_txids: &[],
                };

                let Ok((record_bytes, built_count)) = engine.build_create_record_bytes(&req)
                else {
                    continue;
                };
                debug_assert_eq!(built_count, utxo_count);
                // Allocation journals AllocateRegion via the attached
                // redo log (same as production).
                let Ok(record_offset) = engine
                    .allocator()
                    .lock()
                    .allocate(record_bytes.len() as u64)
                else {
                    continue;
                };
                // WAL fsync. Failure before this point: op never happened.
                if redo
                    .lock()
                    .append_and_flush(RedoOp::CreateV2 {
                        tx_key: TxKey { txid: tx_id },
                        record_offset,
                        utxo_count,
                        is_conflicting: false,
                        record_bytes,
                        parent_txids: Vec::new(),
                    })
                    .is_err()
                {
                    continue;
                }
                match engine.create_at_offset(&req, record_offset) {
                    Ok(_) => {
                        reference.insert(tx_id, (utxo_count, 0, utxo_hashes));
                        result.operations_completed += 1;
                    }
                    Err(_) => {
                        // WAL durable, device write lost (injected I/O
                        // error): the create IS committed. Recovery must
                        // materialize it from the redo entry.
                        result.io_errors_injected += 1;
                        reference.insert(tx_id, (utxo_count, 0, utxo_hashes));
                        result.operations_completed += 1;
                        self.crash_and_recover(&mut result);
                    }
                }
            } else if roll < 0.6 {
                // Spend (WAL-first, mirrors dispatch handle_spend_batch:
                // validate under lock → redo fsync → apply)
                let txids: Vec<[u8; 32]> = reference
                    .iter()
                    .filter(|(_, (count, spent, _))| *spent < *count)
                    .map(|(id, _)| *id)
                    .collect();
                let Some(&txid) =
                    txids.get(self.rng.next_u32() as usize % txids.len().max(1))
                else {
                    continue;
                };
                let Some((count, spent, hashes)) = reference.get(&txid) else {
                    continue;
                };
                let offset = *spent;
                if offset >= *count {
                    continue;
                }
                let utxo_hash = hashes[offset as usize];
                let mut spending_data = [0u8; 36];
                let sd_val = self.rng.next_u64().to_le_bytes();
                spending_data[..8].copy_from_slice(&sd_val);
                spending_data[32..36].copy_from_slice(&offset.to_le_bytes());

                let multi_req = SpendMultiRequest {
                    tx_key: TxKey { txid },
                    spends: vec![SpendItem {
                        offset,
                        utxo_hash,
                        spending_data,
                        idx: 0,
                    }],
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height,
                    block_height_retention: 288,
                };

                // Phase 1: validate under the stripe lock (no writes).
                // Validation reads can hit injected I/O errors — the op
                // is rejected before any WAL append, reference untouched.
                let validated = match engine.validate_spend_multi(&multi_req) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if !validated.errors.is_empty() || validated.transitions().is_empty() {
                    continue;
                }
                let new_spent_count = validated.pre_spent_count().wrapping_add(1);
                let target_generation = validated.pre_generation.wrapping_add(1);

                // Phase 2: WAL fsync while the lock is held.
                if redo
                    .lock()
                    .append_and_flush(RedoOp::SpendV2 {
                        tx_key: TxKey { txid },
                        offset,
                        spending_data,
                        new_spent_count,
                        current_block_height,
                        block_height_retention: 288,
                        target_generation,
                        updated_at: self.clock.now_ms(),
                    })
                    .is_err()
                {
                    continue;
                }

                // Phase 3: apply. After the WAL fsync the spend is
                // committed regardless of whether the device write lands.
                match validated.apply(&engine) {
                    Ok(_) => {
                        reference.get_mut(&txid).unwrap().1 += 1;
                        result.operations_completed += 1;
                    }
                    Err(_) => {
                        result.io_errors_injected += 1;
                        reference.get_mut(&txid).unwrap().1 += 1;
                        result.operations_completed += 1;
                        self.crash_and_recover(&mut result);
                    }
                }
            } else if roll < 0.8 {
                // SetMined (WAL-first, mirrors dispatch handle_set_mined_batch)
                let txids: Vec<[u8; 32]> = reference.keys().copied().collect();
                let Some(&txid) =
                    txids.get(self.rng.next_u32() as usize % txids.len().max(1))
                else {
                    continue;
                };
                let block_id = next_block_id;
                next_block_id += 1;

                let req = SetMinedRequest {
                    tx_key: TxKey { txid },
                    block_id,
                    block_height: current_block_height,
                    subtree_idx: 0,
                    current_block_height,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                };

                if redo
                    .lock()
                    .append_and_flush(RedoOp::SetMined {
                        tx_key: TxKey { txid },
                        block_id,
                        block_height: current_block_height,
                        subtree_idx: 0,
                        unset: false,
                    })
                    .is_err()
                {
                    continue;
                }
                match engine.set_mined(&req) {
                    Ok(_) => result.operations_completed += 1,
                    Err(_) => {
                        // WAL durable; replay applies (or skips, for
                        // entries past the inline block-entry cap —
                        // either way state converges).
                        result.io_errors_injected += 1;
                        result.operations_completed += 1;
                        self.crash_and_recover(&mut result);
                    }
                }
            } else {
                // Read (verify). Injection is suspended: this is a
                // verification probe of committed state, and an injected
                // read error would register as a false data-loss signal.
                let txids: Vec<[u8; 32]> = reference.keys().copied().collect();
                if let Some(&txid) = txids.get(self.rng.next_u32() as usize % txids.len().max(1)) {
                    let was = self.nodes[0].flaky().set_enabled(false);
                    let key = TxKey { txid };
                    match engine.read_metadata(&key) {
                        Ok(meta) => {
                            let (expected_count, _, _) = &reference[&txid];
                            if { meta.utxo_count } != *expected_count {
                                result.inconsistencies_found.push(format!(
                                    "op {}: utxo_count mismatch for tx {:?}",
                                    op_idx, txid
                                ));
                                result.data_loss_detected = true;
                            }
                            result.operations_completed += 1;
                        }
                        Err(_) => {
                            result.inconsistencies_found.push(format!(
                                "op {}: tx {:?} not found but expected",
                                op_idx, txid
                            ));
                            result.data_loss_detected = true;
                        }
                    }
                    self.nodes[0].flaky().set_enabled(was);
                }
            }
        }

        // Final verification: every committed record must exist with the
        // exact cumulative spent count. Injection off — we are checking
        // the true device/engine state.
        self.nodes[0].flaky().set_enabled(false);
        if self.nodes[0].is_up() {
            let engine = self.nodes[0].engine().unwrap();
            for (txid, (expected_count, expected_spent, _)) in &reference {
                let key = TxKey { txid: *txid };
                match engine.read_metadata(&key) {
                    Ok(meta) => {
                        if { meta.utxo_count } != *expected_count {
                            result.inconsistencies_found.push(format!(
                                "final: utxo_count mismatch for tx {:?}: expected {}, got {}",
                                txid,
                                expected_count,
                                { meta.utxo_count }
                            ));
                            result.data_loss_detected = true;
                        }
                        if { meta.spent_utxos } != *expected_spent {
                            result.inconsistencies_found.push(format!(
                                "final: spent_utxos mismatch for tx {:?}: expected {}, got {}",
                                txid,
                                expected_spent,
                                { meta.spent_utxos }
                            ));
                            result.data_loss_detected = true;
                        }
                    }
                    Err(_) => {
                        result
                            .inconsistencies_found
                            .push(format!("final: tx {:?} not found", txid));
                        result.data_loss_detected = true;
                    }
                }
            }
        }

        result
    }
}

fn make_sim_tx_id(n: u32) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[0..4].copy_from_slice(&n.to_le_bytes());
    txid[8..12].copy_from_slice(&(n.wrapping_mul(0x9E37)).to_le_bytes());
    txid[16..18].copy_from_slice(&(n as u16).to_le_bytes());
    txid
}

fn make_sim_utxo_hash(tx_n: u32, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = (vout & 0xFF) as u8;
    h[1] = ((vout >> 8) & 0xFF) as u8;
    h[4..8].copy_from_slice(&tx_n.to_le_bytes());
    h
}
