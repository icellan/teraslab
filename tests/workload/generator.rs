//! Configurable workload generator for realistic BSV UTXO operations.
//!
//! Produces a stream of operations that follow realistic sequencing:
//! create → spend/setMined → eventually delete. Spends reference UTXOs
//! that actually exist, and the generator tracks live state for correct
//! operation generation.

use std::collections::{HashMap, HashSet};

use teraslab::index::TxKey;

/// Distribution for generating random values.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Distribution {
    /// Always returns the same value.
    Fixed(u32),
    /// Uniform distribution between min and max (inclusive).
    Uniform(u32, u32),
    /// Zipfian distribution with a maximum value and exponent.
    Zipfian { max: u32, exponent: f64 },
}

impl Distribution {
    /// Sample a value from this distribution using a simple RNG state.
    pub fn sample(&self, rng: &mut SimpleRng) -> u32 {
        match self {
            Distribution::Fixed(v) => *v,
            Distribution::Uniform(lo, hi) => {
                if lo == hi {
                    return *lo;
                }
                let range = hi - lo + 1;
                lo + (rng.next_u32() % range)
            }
            Distribution::Zipfian { max, exponent } => {
                // Inverse CDF approximation of Zipfian distribution
                let u = rng.next_f64();
                let n = *max as f64;
                let s = *exponent;
                // Use power-law inverse: rank = ceil(n * u^(1/(s-1))) clamped to [1, max]
                if s <= 1.0 {
                    // Degenerate to uniform
                    1 + (rng.next_u32() % max)
                } else {
                    let rank = (n * u.powf(1.0 / (s - 1.0))).ceil() as u32;
                    rank.clamp(1, *max)
                }
            }
        }
    }
}

/// Configuration for workload generation.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WorkloadConfig {
    /// Total number of operations to generate.
    pub total_operations: u64,
    /// Fraction of ops that are creates (e.g., 0.1 = 10%).
    pub tx_creation_rate: f64,
    /// Fraction that are spends (e.g., 0.6).
    pub spend_rate: f64,
    /// Fraction that are setMined (e.g., 0.2).
    pub set_mined_rate: f64,
    /// Fraction that are reads (e.g., 0.1).
    pub read_rate: f64,
    /// Fraction for other ops: freeze, unfreeze, reassign, etc.
    pub other_rate: f64,
    /// Distribution of output counts per tx.
    pub utxos_per_tx: Distribution,
    /// Distribution of spendMulti batch sizes.
    pub spend_batch_size: Distribution,
    /// Fraction of txs that are "large" (external storage).
    pub large_tx_fraction: f64,
    /// Number of concurrent clients.
    pub concurrent_clients: usize,
    /// Rate limit in ops per second (None = as fast as possible).
    pub target_ops_per_sec: Option<u64>,
    /// RNG seed for reproducibility.
    pub seed: u64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            total_operations: 10_000,
            tx_creation_rate: 0.15,
            spend_rate: 0.50,
            set_mined_rate: 0.20,
            read_rate: 0.10,
            other_rate: 0.05,
            utxos_per_tx: Distribution::Uniform(1, 20),
            spend_batch_size: Distribution::Fixed(1),
            large_tx_fraction: 0.001,
            concurrent_clients: 1,
            target_ops_per_sec: None,
            seed: 42,
        }
    }
}

/// A single workload operation.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum WorkloadOp {
    /// Create a new transaction with the given UTXO hashes.
    Create {
        tx_id: [u8; 32],
        utxo_hashes: Vec<[u8; 32]>,
        is_coinbase: bool,
        spending_height: u32,
        is_external: bool,
        block_height: u32,
    },
    /// Spend a single UTXO.
    Spend {
        tx_key: TxKey,
        offset: u32,
        utxo_hash: [u8; 32],
        spending_data: [u8; 36],
        current_block_height: u32,
    },
    /// Spend multiple UTXOs in a single batch.
    SpendMulti {
        tx_key: TxKey,
        items: Vec<(u32, [u8; 32], [u8; 36])>, // (offset, hash, spending_data)
        current_block_height: u32,
    },
    /// Set a transaction as mined in a block.
    SetMined {
        tx_key: TxKey,
        block_id: u32,
        block_height: u32,
        current_block_height: u32,
    },
    /// Unset mined (reorg simulation).
    UnsetMined {
        tx_key: TxKey,
        block_id: u32,
        block_height: u32,
        current_block_height: u32,
    },
    /// Read metadata for a transaction.
    ReadMetadata { tx_key: TxKey },
    /// Read a specific UTXO slot.
    ReadSlot { tx_key: TxKey, offset: u32 },
    /// Freeze a UTXO.
    Freeze {
        tx_key: TxKey,
        offset: u32,
        utxo_hash: [u8; 32],
    },
    /// Unfreeze a UTXO.
    Unfreeze {
        tx_key: TxKey,
        offset: u32,
        utxo_hash: [u8; 32],
    },
    /// Set conflicting flag.
    SetConflicting {
        tx_key: TxKey,
        value: bool,
        current_block_height: u32,
    },
    /// Set locked flag.
    SetLocked { tx_key: TxKey, value: bool },
    /// Delete a transaction record.
    Delete { tx_key: TxKey },
    /// Set preserve_until on a record.
    PreserveUntil { tx_key: TxKey, block_height: u32 },
}

/// Simple xorshift64 RNG for deterministic test workloads.
#[derive(Debug, Clone)]
pub struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    /// Create a new RNG with the given seed.
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    /// Generate the next u64 value.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Generate the next u32 value.
    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() & 0xFFFF_FFFF) as u32
    }

    /// Generate a float in [0, 1).
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Generate a boolean with given probability of being true.
    pub fn chance(&mut self, p: f64) -> bool {
        self.next_f64() < p
    }
}

/// Tracks the live state of generated transactions for correct operation generation.
struct LiveState {
    /// All created transactions: txid → (utxo_count, which slots are unspent, which are spent, mined block_ids).
    txs: HashMap<[u8; 32], TxState>,
    /// Transaction IDs in creation order (for random access).
    tx_ids: Vec<[u8; 32]>,
    /// Next tx counter for unique IDs.
    next_tx: u32,
    /// Current simulated block height.
    current_block_height: u32,
    /// Next block ID.
    next_block_id: u32,
}

#[allow(dead_code)]
struct TxState {
    utxo_count: u32,
    utxo_hashes: Vec<[u8; 32]>,
    unspent: HashSet<u32>,
    spent: HashSet<u32>,
    frozen: HashSet<u32>,
    mined_block_ids: Vec<u32>,
    conflicting: bool,
    locked: bool,
    is_coinbase: bool,
    spending_height: u32,
    deleted: bool,
}

impl LiveState {
    fn new(seed: u64) -> Self {
        // Derive starting tx counter from seed to avoid collisions
        // when multiple generators are used against the same engine.
        let start = ((seed.wrapping_mul(2654435761)) >> 16) as u32;
        Self {
            txs: HashMap::new(),
            tx_ids: Vec::new(),
            next_tx: start,
            current_block_height: 1000,
            next_block_id: 1,
        }
    }

    fn make_tx_id(&self, n: u32) -> [u8; 32] {
        let mut txid = [0u8; 32];
        txid[0..4].copy_from_slice(&n.to_le_bytes());
        txid[8..12].copy_from_slice(&(n.wrapping_mul(0x9E37)).to_le_bytes());
        txid[16..18].copy_from_slice(&(n as u16).to_le_bytes());
        txid
    }

    fn make_utxo_hash(&self, tx_n: u32, vout: u32) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = (vout & 0xFF) as u8;
        h[1] = ((vout >> 8) & 0xFF) as u8;
        h[4..8].copy_from_slice(&tx_n.to_le_bytes());
        h
    }

    /// Get a random live (non-deleted) tx that has unspent UTXOs.
    fn random_spendable_tx(&self, rng: &mut SimpleRng) -> Option<[u8; 32]> {
        let live: Vec<&[u8; 32]> = self
            .tx_ids
            .iter()
            .filter(|id| {
                let st = &self.txs[*id];
                !st.deleted && !st.unspent.is_empty() && !st.conflicting && !st.locked
            })
            .collect();
        if live.is_empty() {
            return None;
        }
        let idx = rng.next_u32() as usize % live.len();
        Some(*live[idx])
    }

    /// Get a random live (non-deleted) tx.
    fn random_live_tx(&self, rng: &mut SimpleRng) -> Option<[u8; 32]> {
        let live: Vec<&[u8; 32]> = self
            .tx_ids
            .iter()
            .filter(|id| !self.txs[*id].deleted)
            .collect();
        if live.is_empty() {
            return None;
        }
        let idx = rng.next_u32() as usize % live.len();
        Some(*live[idx])
    }

    /// Get a random live tx that is not mined yet.
    fn random_unmined_tx(&self, rng: &mut SimpleRng) -> Option<[u8; 32]> {
        let unmined: Vec<&[u8; 32]> = self
            .tx_ids
            .iter()
            .filter(|id| {
                let st = &self.txs[*id];
                !st.deleted && st.mined_block_ids.is_empty()
            })
            .collect();
        if unmined.is_empty() {
            return None;
        }
        let idx = rng.next_u32() as usize % unmined.len();
        Some(*unmined[idx])
    }

    /// Get a random live tx with unspent, unfrozen UTXOs for freeze.
    fn random_freezable_tx(&self, rng: &mut SimpleRng) -> Option<([u8; 32], u32)> {
        let candidates: Vec<(&[u8; 32], u32)> = self
            .tx_ids
            .iter()
            .filter_map(|id| {
                let st = &self.txs[id];
                if st.deleted {
                    return None;
                }
                let unfrozen_unspent: Vec<u32> = st
                    .unspent
                    .iter()
                    .filter(|o| !st.frozen.contains(o))
                    .copied()
                    .collect();
                if unfrozen_unspent.is_empty() {
                    return None;
                }
                Some((id, unfrozen_unspent[0]))
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let idx = rng.next_u32() as usize % candidates.len();
        Some((*candidates[idx].0, candidates[idx].1))
    }
}

/// Workload generator that produces realistic BSV UTXO operation streams.
pub struct WorkloadGenerator {
    config: WorkloadConfig,
    rng: SimpleRng,
    state: LiveState,
    ops_generated: u64,
}

impl WorkloadGenerator {
    /// Create a new workload generator with the given configuration.
    pub fn new(config: WorkloadConfig) -> Self {
        let seed = config.seed;
        let rng = SimpleRng::new(seed);
        Self {
            config,
            rng,
            state: LiveState::new(seed),
            ops_generated: 0,
        }
    }

    /// Generate all operations as a vector.
    pub fn generate_all(&mut self) -> Vec<WorkloadOp> {
        let total = self.config.total_operations;
        let mut ops = Vec::with_capacity(total as usize);

        // Bootstrap: create initial transactions so spends have targets
        let bootstrap_count = std::cmp::max(10, (total as f64 * self.config.tx_creation_rate * 0.3) as u64);
        for _ in 0..bootstrap_count {
            ops.push(self.generate_create());
        }

        // Generate remaining operations according to rates
        while self.ops_generated < total {
            let op = self.generate_one();
            ops.push(op);
        }

        ops
    }

    /// Generate a single operation based on the configured rates.
    pub fn generate_one(&mut self) -> WorkloadOp {
        self.ops_generated += 1;

        // Periodically advance block height
        if self.ops_generated % 500 == 0 {
            self.state.current_block_height += 1;
        }

        let roll = self.rng.next_f64();
        let mut cumulative = 0.0;

        // Create
        cumulative += self.config.tx_creation_rate;
        if roll < cumulative {
            return self.generate_create();
        }

        // Spend
        cumulative += self.config.spend_rate;
        if roll < cumulative {
            return self.generate_spend();
        }

        // SetMined
        cumulative += self.config.set_mined_rate;
        if roll < cumulative {
            return self.generate_set_mined();
        }

        // Read
        cumulative += self.config.read_rate;
        if roll < cumulative {
            return self.generate_read();
        }

        // Other (freeze, conflicting, locked, delete, preserve)
        self.generate_other()
    }

    fn generate_create(&mut self) -> WorkloadOp {
        let tx_n = self.state.next_tx;
        self.state.next_tx += 1;

        let tx_id = self.state.make_tx_id(tx_n);
        let utxo_count = self.config.utxos_per_tx.sample(&mut self.rng).max(1);
        let is_coinbase = self.rng.chance(0.02); // 2% coinbase
        let is_external = self.rng.chance(self.config.large_tx_fraction);

        let mut utxo_hashes = Vec::with_capacity(utxo_count as usize);
        let mut unspent = HashSet::new();
        for v in 0..utxo_count {
            let hash = self.state.make_utxo_hash(tx_n, v);
            utxo_hashes.push(hash);
            unspent.insert(v);
        }

        let spending_height = if is_coinbase {
            self.state.current_block_height + 100
        } else {
            0
        };

        let tx_state = TxState {
            utxo_count,
            utxo_hashes: utxo_hashes.clone(),
            unspent,
            spent: HashSet::new(),
            frozen: HashSet::new(),
            mined_block_ids: Vec::new(),
            conflicting: false,
            locked: false,
            is_coinbase,
            spending_height,
            deleted: false,
        };

        self.state.txs.insert(tx_id, tx_state);
        self.state.tx_ids.push(tx_id);

        WorkloadOp::Create {
            tx_id,
            utxo_hashes,
            is_coinbase,
            spending_height,
            is_external,
            block_height: self.state.current_block_height,
        }
    }

    fn generate_spend(&mut self) -> WorkloadOp {
        let batch_size = self.config.spend_batch_size.sample(&mut self.rng);

        if let Some(txid) = self.state.random_spendable_tx(&mut self.rng) {
            let tx_state = self.state.txs.get(&txid).unwrap();
            let unspent: Vec<u32> = tx_state.unspent.iter().copied().collect();

            if batch_size <= 1 || unspent.len() <= 1 {
                // Single spend
                let offset = unspent[self.rng.next_u32() as usize % unspent.len()];
                let utxo_hash = tx_state.utxo_hashes[offset as usize];
                let mut spending_data = [0u8; 36];
                let spend_txid_bytes = self.rng.next_u64().to_le_bytes();
                spending_data[..8].copy_from_slice(&spend_txid_bytes);
                spending_data[32..36].copy_from_slice(&offset.to_le_bytes());

                let current_block_height = self.state.current_block_height;

                // Update live state
                let tx_state = self.state.txs.get_mut(&txid).unwrap();
                tx_state.unspent.remove(&offset);
                tx_state.spent.insert(offset);

                WorkloadOp::Spend {
                    tx_key: TxKey { txid },
                    offset,
                    utxo_hash,
                    spending_data,
                    current_block_height,
                }
            } else {
                // Multi-spend
                let count = (batch_size as usize).min(unspent.len());
                let mut items = Vec::with_capacity(count);
                let current_block_height = self.state.current_block_height;
                let offsets: Vec<u32> = unspent.iter().copied().take(count).collect();

                for &offset in &offsets {
                    let utxo_hash = tx_state.utxo_hashes[offset as usize];
                    let mut spending_data = [0u8; 36];
                    let spend_txid_bytes = self.rng.next_u64().to_le_bytes();
                    spending_data[..8].copy_from_slice(&spend_txid_bytes);
                    spending_data[32..36].copy_from_slice(&offset.to_le_bytes());
                    items.push((offset, utxo_hash, spending_data));
                }

                // Update live state
                let tx_state = self.state.txs.get_mut(&txid).unwrap();
                for &offset in &offsets {
                    tx_state.unspent.remove(&offset);
                    tx_state.spent.insert(offset);
                }

                WorkloadOp::SpendMulti {
                    tx_key: TxKey { txid },
                    items,
                    current_block_height,
                }
            }
        } else {
            // No spendable tx; fall back to create
            self.generate_create()
        }
    }

    fn generate_set_mined(&mut self) -> WorkloadOp {
        if let Some(txid) = self.state.random_unmined_tx(&mut self.rng) {
            let block_id = self.state.next_block_id;
            self.state.next_block_id += 1;
            let block_height = self.state.current_block_height;

            let tx_state = self.state.txs.get_mut(&txid).unwrap();
            tx_state.mined_block_ids.push(block_id);
            tx_state.locked = false; // setMined clears lock

            WorkloadOp::SetMined {
                tx_key: TxKey { txid },
                block_id,
                block_height,
                current_block_height: block_height,
            }
        } else {
            // Fall back to create if nothing to mine
            self.generate_create()
        }
    }

    fn generate_read(&mut self) -> WorkloadOp {
        if let Some(txid) = self.state.random_live_tx(&mut self.rng) {
            if self.rng.chance(0.5) {
                WorkloadOp::ReadMetadata {
                    tx_key: TxKey { txid },
                }
            } else {
                let tx_state = &self.state.txs[&txid];
                let offset = self.rng.next_u32() % tx_state.utxo_count;
                WorkloadOp::ReadSlot {
                    tx_key: TxKey { txid },
                    offset,
                }
            }
        } else {
            self.generate_create()
        }
    }

    fn generate_other(&mut self) -> WorkloadOp {
        let choice = self.rng.next_u32() % 5;
        match choice {
            0 => {
                // Freeze
                if let Some((txid, offset)) = self.state.random_freezable_tx(&mut self.rng) {
                    let utxo_hash = self.state.txs[&txid].utxo_hashes[offset as usize];
                    let tx_state = self.state.txs.get_mut(&txid).unwrap();
                    tx_state.unspent.remove(&offset);
                    tx_state.frozen.insert(offset);
                    WorkloadOp::Freeze {
                        tx_key: TxKey { txid },
                        offset,
                        utxo_hash,
                    }
                } else {
                    self.generate_create()
                }
            }
            1 => {
                // SetConflicting
                if let Some(txid) = self.state.random_live_tx(&mut self.rng) {
                    let value = self.rng.chance(0.5);
                    let tx_state = self.state.txs.get_mut(&txid).unwrap();
                    tx_state.conflicting = value;
                    WorkloadOp::SetConflicting {
                        tx_key: TxKey { txid },
                        value,
                        current_block_height: self.state.current_block_height,
                    }
                } else {
                    self.generate_create()
                }
            }
            2 => {
                // SetLocked
                if let Some(txid) = self.state.random_live_tx(&mut self.rng) {
                    let value = self.rng.chance(0.5);
                    let tx_state = self.state.txs.get_mut(&txid).unwrap();
                    tx_state.locked = value;
                    WorkloadOp::SetLocked {
                        tx_key: TxKey { txid },
                        value,
                    }
                } else {
                    self.generate_create()
                }
            }
            3 => {
                // PreserveUntil
                if let Some(txid) = self.state.random_live_tx(&mut self.rng) {
                    let height = self.state.current_block_height + 1000;
                    WorkloadOp::PreserveUntil {
                        tx_key: TxKey { txid },
                        block_height: height,
                    }
                } else {
                    self.generate_create()
                }
            }
            _ => {
                // Delete — only delete fully-spent txs
                let candidate: Option<[u8; 32]> = {
                    let live: Vec<&[u8; 32]> = self
                        .state
                        .tx_ids
                        .iter()
                        .filter(|id| {
                            let st = &self.state.txs[*id];
                            !st.deleted && st.unspent.is_empty() && st.frozen.is_empty()
                        })
                        .collect();
                    if live.is_empty() {
                        None
                    } else {
                        let idx = self.rng.next_u32() as usize % live.len();
                        Some(*live[idx])
                    }
                };

                if let Some(txid) = candidate {
                    self.state.txs.get_mut(&txid).unwrap().deleted = true;
                    WorkloadOp::Delete {
                        tx_key: TxKey { txid },
                    }
                } else {
                    self.generate_create()
                }
            }
        }
    }
}
