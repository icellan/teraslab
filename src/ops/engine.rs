//! Store engine — thread-safe coordinator for all UTXO operations.
//!
//! Owns the index, device, locks, and secondary indexes. Provides the
//! spend/unspend methods that are the public API for this phase.

use crate::device::BlockDevice;
use crate::index::{DahIndex, Index, TxIndexEntry, TxKey, UnminedIndex};
use crate::io;
use crate::locks::StripedLocks;
use crate::ops::delete_eval::{evaluate_delete_at_height, DahPatch};
use crate::ops::error::SpendError;
use crate::ops::signal::Signal;
use crate::ops::spend::*;
use crate::ops::unspend::*;
use crate::record::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Thread-safe store engine for UTXO operations.
///
/// All mutation operations acquire a per-transaction stripe lock, ensuring
/// that concurrent operations on different transactions run in parallel
/// while operations on the same transaction are serialized.
pub struct Engine {
    device: Arc<dyn BlockDevice>,
    index: parking_lot::RwLock<Index>,
    locks: StripedLocks,
    dah_index: parking_lot::Mutex<DahIndex>,
    #[expect(dead_code)]
    unmined_index: parking_lot::Mutex<UnminedIndex>,
}

impl Engine {
    /// Create a new engine with the given components.
    pub fn new(
        device: Arc<dyn BlockDevice>,
        index: Index,
        locks: StripedLocks,
        dah_index: DahIndex,
        unmined_index: UnminedIndex,
    ) -> Self {
        Self {
            device,
            index: parking_lot::RwLock::new(index),
            locks,
            dah_index: parking_lot::Mutex::new(dah_index),
            unmined_index: parking_lot::Mutex::new(unmined_index),
        }
    }

    /// Register a transaction in the index (for test setup).
    pub fn register(&self, key: TxKey, entry: TxIndexEntry) -> Result<(), SpendError> {
        self.index
            .write()
            .register(key, entry)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })
    }

    /// Look up a transaction in the index.
    pub fn lookup(&self, key: &TxKey) -> Option<TxIndexEntry> {
        self.index.read().lookup(key)
    }

    /// Execute a batch of spends on a single transaction.
    ///
    /// All spends target the same txid. The per-txid lock is held for the
    /// entire operation: read metadata → read slots → validate → write slots
    /// → write metadata → update secondary indexes.
    pub fn spend_multi(
        &self,
        req: &SpendMultiRequest,
    ) -> Result<SpendMultiResponse, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);

        // 1. Index lookup
        let entry = self
            .index
            .read()
            .lookup(&req.tx_key)
            .ok_or(SpendError::TxNotFound)?;
        let record_offset = entry.record_offset;

        // 2. Read metadata
        let mut metadata = io::read_metadata(&*self.device, record_offset)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;

        // 3. Record-level validation
        if metadata.flags.contains(TxFlags::CONFLICTING) && !req.ignore_conflicting {
            return Err(SpendError::Conflicting);
        }
        if metadata.flags.contains(TxFlags::LOCKED) && !req.ignore_locked {
            return Err(SpendError::Locked);
        }
        let spending_height = { metadata.spending_height };
        if metadata.flags.contains(TxFlags::IS_COINBASE)
            && spending_height > 0
            && spending_height > req.current_block_height
        {
            return Err(SpendError::CoinbaseImmature {
                spending_height,
                current_height: req.current_block_height,
            });
        }

        let utxo_count = { metadata.utxo_count };

        // Handle empty spends list
        if req.spends.is_empty() {
            let block_ids = collect_block_ids(&metadata);
            return Ok(SpendMultiResponse {
                signal: Signal::None,
                block_ids,
                errors: HashMap::new(),
                spent_count: 0,
            });
        }

        // 4. Batch read all requested UTXO slots
        let slot_indices: Vec<u32> = req.spends.iter().map(|s| s.offset).collect();
        let mut slot_map: HashMap<u32, UtxoSlot> = HashMap::new();
        for &si in &slot_indices {
            if si < utxo_count {
                let slot = io::read_utxo_slot(&*self.device, record_offset, si)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("{e}"),
                    })?;
                slot_map.insert(si, slot);
            }
        }

        // 5. Validate each spend item
        let mut errors: HashMap<u32, SpendError> = HashMap::new();
        let mut valid_spends: Vec<(u32, UtxoSlot)> = Vec::new(); // (offset, new_slot)
        let mut spent_count: u32 = 0;

        for item in &req.spends {
            // Validate offset in range
            if item.offset >= utxo_count {
                errors.insert(item.idx, SpendError::UtxoNotFound { offset: item.offset });
                continue;
            }

            let slot = match slot_map.get(&item.offset) {
                Some(s) => *s,
                None => {
                    errors.insert(item.idx, SpendError::UtxoNotFound { offset: item.offset });
                    continue;
                }
            };

            // Hash check
            if slot.hash != item.utxo_hash {
                errors.insert(
                    item.idx,
                    SpendError::UtxoHashMismatch { offset: item.offset },
                );
                continue;
            }

            match slot.status {
                UTXO_UNSPENT => {
                    // Check spendable height (first 4 bytes of spending_data)
                    let spendable_height =
                        u32::from_le_bytes(slot.spending_data[0..4].try_into().unwrap());
                    if spendable_height != 0
                        && spendable_height >= req.current_block_height
                    {
                        errors.insert(
                            item.idx,
                            SpendError::FrozenUntil {
                                offset: item.offset,
                                spendable_at_height: spendable_height,
                            },
                        );
                        continue;
                    }

                    // Valid spend — create the new slot
                    let new_slot = UtxoSlot::new_spent(item.utxo_hash, item.spending_data);
                    // Update the in-memory slot map so subsequent items
                    // in the same batch see the updated state
                    slot_map.insert(item.offset, new_slot);
                    valid_spends.push((item.offset, new_slot));
                    spent_count += 1;
                }
                UTXO_SPENT => {
                    // Check if same spending data (idempotent)
                    if slot.spending_data == item.spending_data {
                        // Idempotent re-spend — not an error, counter NOT incremented
                        continue;
                    }
                    // Check if frozen (all 0xFF)
                    if slot.spending_data == [FROZEN_BYTE; 36] {
                        errors.insert(
                            item.idx,
                            SpendError::Frozen { offset: item.offset },
                        );
                        continue;
                    }
                    // Different spender
                    errors.insert(
                        item.idx,
                        SpendError::AlreadySpent {
                            offset: item.offset,
                            spending_data: slot.spending_data,
                        },
                    );
                }
                UTXO_PRUNED => {
                    errors.insert(
                        item.idx,
                        SpendError::Pruned { offset: item.offset },
                    );
                }
                UTXO_FROZEN => {
                    errors.insert(
                        item.idx,
                        SpendError::Frozen { offset: item.offset },
                    );
                }
                _ => {
                    errors.insert(
                        item.idx,
                        SpendError::StorageError {
                            detail: format!("unknown status byte: {:#04x}", slot.status),
                        },
                    );
                }
            }
        }

        // 6. Batch write all valid slot mutations
        for &(offset, ref new_slot) in &valid_spends {
            io::write_utxo_slot(&*self.device, record_offset, offset, new_slot)
                .map_err(|e| SpendError::StorageError {
                    detail: format!("{e}"),
                })?;
        }

        // 7. Update metadata
        let old_dah = { metadata.delete_at_height };
        metadata.spent_utxos = { metadata.spent_utxos }.wrapping_add(spent_count);
        metadata.generation = { metadata.generation }.wrapping_add(1);
        metadata.updated_at = now_millis();

        // 8. Evaluate deleteAtHeight
        let (signal, dah_patch) = evaluate_delete_at_height(
            &metadata,
            req.current_block_height,
            req.block_height_retention,
        );

        if let Some(ref patch) = dah_patch {
            apply_dah_patch(&mut metadata, patch);
        }

        // 9. Write metadata
        io::write_metadata(&*self.device, record_offset, &metadata)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;

        // 10. Update DAH secondary index
        let new_dah = { metadata.delete_at_height };
        if new_dah != old_dah {
            let mut dah = self.dah_index.lock();
            if old_dah != 0 {
                dah.remove(&req.tx_key);
            }
            if new_dah != 0 {
                dah.insert(new_dah, req.tx_key);
            }
        }

        let block_ids = collect_block_ids(&metadata);

        Ok(SpendMultiResponse {
            signal,
            block_ids,
            errors,
            spent_count,
        })
    }

    /// Execute a single spend (convenience wrapper around spend_multi).
    pub fn spend(&self, req: &SpendRequest) -> Result<SpendResponse, SpendError> {
        let multi_req = SpendMultiRequest {
            tx_key: req.tx_key,
            spends: vec![SpendItem {
                offset: req.offset,
                utxo_hash: req.utxo_hash,
                spending_data: req.spending_data,
                idx: 0,
            }],
            ignore_conflicting: req.ignore_conflicting,
            ignore_locked: req.ignore_locked,
            current_block_height: req.current_block_height,
            block_height_retention: req.block_height_retention,
        };

        let resp = self.spend_multi(&multi_req)?;

        // If there's a per-item error for idx 0, return it
        if let Some(err) = resp.errors.into_values().next() {
            return Err(err);
        }

        Ok(SpendResponse {
            signal: resp.signal,
            block_ids: resp.block_ids,
        })
    }

    /// Unspend a UTXO — reverse a previous spend.
    ///
    /// Clears the spending data and decrements the counter. If the UTXO
    /// is already unspent, this is a no-op.
    pub fn unspend(&self, req: &UnspendRequest) -> Result<UnspendResponse, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);

        // 1. Index lookup
        let entry = self
            .index
            .read()
            .lookup(&req.tx_key)
            .ok_or(SpendError::TxNotFound)?;
        let record_offset = entry.record_offset;

        // 2. Read metadata
        let mut metadata = io::read_metadata(&*self.device, record_offset)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;

        let utxo_count = { metadata.utxo_count };
        if req.offset >= utxo_count {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        // 3. Read the specific slot
        let slot = io::read_utxo_slot(&*self.device, record_offset, req.offset)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;

        // 4. Validate hash
        if slot.hash != req.utxo_hash {
            return Err(SpendError::UtxoHashMismatch { offset: req.offset });
        }

        // 5. Check status
        match slot.status {
            UTXO_UNSPENT => {
                // Already unspent — no-op, no counter change, no generation bump
                return Ok(UnspendResponse { signal: Signal::None });
            }
            UTXO_SPENT => {
                // Check if frozen (spending_data all 0xFF)
                if slot.spending_data == [FROZEN_BYTE; 36] {
                    return Err(SpendError::Frozen { offset: req.offset });
                }
                // Valid unspend
                let new_slot = UtxoSlot::new_unspent(req.utxo_hash);
                io::write_utxo_slot(&*self.device, record_offset, req.offset, &new_slot)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("{e}"),
                    })?;

                let current = { metadata.spent_utxos };
                if current > 0 {
                    metadata.spent_utxos = current - 1;
                }
            }
            UTXO_PRUNED => {
                return Err(SpendError::Pruned { offset: req.offset });
            }
            UTXO_FROZEN => {
                return Err(SpendError::Frozen { offset: req.offset });
            }
            _ => {
                return Err(SpendError::StorageError {
                    detail: format!("unknown status: {:#04x}", slot.status),
                });
            }
        }

        // 6. Mutation bookkeeping
        let old_dah = { metadata.delete_at_height };
        metadata.generation = { metadata.generation }.wrapping_add(1);
        metadata.updated_at = now_millis();

        // 7. Evaluate deleteAtHeight
        let (signal, dah_patch) = evaluate_delete_at_height(
            &metadata,
            req.current_block_height,
            req.block_height_retention,
        );

        if let Some(ref patch) = dah_patch {
            apply_dah_patch(&mut metadata, patch);
        }

        // 8. Write metadata
        io::write_metadata(&*self.device, record_offset, &metadata)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;

        // 9. Update DAH secondary index
        let new_dah = { metadata.delete_at_height };
        if new_dah != old_dah {
            let mut dah = self.dah_index.lock();
            if old_dah != 0 {
                dah.remove(&req.tx_key);
            }
            if new_dah != 0 {
                dah.insert(new_dah, req.tx_key);
            }
        }

        Ok(UnspendResponse { signal })
    }

    /// Read metadata for a transaction (for testing).
    pub fn read_metadata(&self, key: &TxKey) -> Result<TxMetadata, SpendError> {
        let entry = self
            .index
            .read()
            .lookup(key)
            .ok_or(SpendError::TxNotFound)?;
        io::read_metadata(&*self.device, entry.record_offset)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })
    }

    /// Read a UTXO slot (for testing).
    pub fn read_slot(&self, key: &TxKey, offset: u32) -> Result<UtxoSlot, SpendError> {
        let entry = self
            .index
            .read()
            .lookup(key)
            .ok_or(SpendError::TxNotFound)?;
        io::read_utxo_slot(&*self.device, entry.record_offset, offset)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })
    }

    /// Get the DAH index (for testing).
    pub fn dah_index(&self) -> parking_lot::MutexGuard<'_, DahIndex> {
        self.dah_index.lock()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn apply_dah_patch(metadata: &mut TxMetadata, patch: &DahPatch) {
    metadata.delete_at_height = patch.new_delete_at_height;
    if patch.last_spent_all {
        metadata.flags |= TxFlags::LAST_SPENT_ALL;
    } else {
        metadata.flags -= metadata.flags & TxFlags::LAST_SPENT_ALL;
    }
}

fn collect_block_ids(metadata: &TxMetadata) -> Vec<u32> {
    let count = metadata.block_entry_count as usize;
    let inline = count.min(INLINE_BLOCK_ENTRIES);
    metadata.block_entries_inline[..inline]
        .iter()
        .map(|e| { e.block_id })
        .collect()
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::MemoryDevice;
    use std::sync::Arc;

    /// Build a test engine with a pre-created record.
    struct TestHarness {
        engine: Arc<Engine>,
        key: TxKey,
    }

    impl TestHarness {
        fn new(utxo_count: u32, flags: TxFlags) -> Self {
            Self::with_metadata(utxo_count, flags, |_| {})
        }

        fn with_metadata(
            utxo_count: u32,
            flags: TxFlags,
            customize: impl FnOnce(&mut TxMetadata),
        ) -> Self {
            let dev: Arc<dyn BlockDevice> =
                Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
            let mut alloc = SlotAllocator::new(dev.clone());
            let mut index = Index::new(100).unwrap();

            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&1u64.to_le_bytes());
            txid[8..16].copy_from_slice(&0x1234567890ABCDEFu64.to_le_bytes());
            txid[16..18].copy_from_slice(&42u16.to_le_bytes());
            let key = TxKey { txid };

            let record_size = TxMetadata::record_size_for(utxo_count);
            let offset = alloc.allocate(record_size).unwrap();

            let mut meta = TxMetadata::new(utxo_count);
            meta.tx_id = txid;
            meta.flags = flags;
            customize(&mut meta);

            let slots: Vec<UtxoSlot> = (0..utxo_count)
                .map(|i| {
                    let mut hash = [0u8; 32];
                    hash[0] = (i & 0xFF) as u8;
                    hash[1] = ((i >> 8) & 0xFF) as u8;
                    UtxoSlot::new_unspent(hash)
                })
                .collect();

            io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

            let ie = TxIndexEntry {
                device_id: 0,
                record_offset: offset,
                utxo_count,
                cold_offset: 0,
                cold_size: 0,
                flags: flags.bits(),
            };
            index.register(key, ie).unwrap();

            let engine = Arc::new(Engine::new(
                dev,
                index,
                StripedLocks::new(1024),
                DahIndex::new(),
                UnminedIndex::new(),
            ));

            Self { engine, key }
        }

        fn slot_hash(&self, offset: u32) -> [u8; 32] {
            let mut hash = [0u8; 32];
            hash[0] = (offset & 0xFF) as u8;
            hash[1] = ((offset >> 8) & 0xFF) as u8;
            hash
        }

        fn make_spending_data(&self, n: u8) -> [u8; 36] {
            let mut sd = [0u8; 36];
            sd[0] = n;
            sd[32..36].copy_from_slice(&1u32.to_le_bytes());
            sd
        }

        fn spend_req(&self, offset: u32) -> SpendRequest {
            SpendRequest {
                tx_key: self.key,
                offset,
                utxo_hash: self.slot_hash(offset),
                spending_data: self.make_spending_data(0xAB),
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            }
        }
    }

    // -- Spend correctness tests --

    #[test]
    fn spend_unspent_succeeds() {
        let h = TestHarness::new(10, TxFlags::empty());
        let result = h.engine.spend(&h.spend_req(5));
        assert!(result.is_ok());

        let slot = h.engine.read_slot(&h.key, 5).unwrap();
        assert!(slot.is_spent());
        assert_eq!(slot.spending_data, h.make_spending_data(0xAB));
    }

    #[test]
    fn spend_nonexistent_tx() {
        let h = TestHarness::new(10, TxFlags::empty());
        let mut req = h.spend_req(0);
        req.tx_key = TxKey { txid: [0xFF; 32] };
        match h.engine.spend(&req) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn spend_conflicting_blocked() {
        let h = TestHarness::new(10, TxFlags::CONFLICTING);
        match h.engine.spend(&h.spend_req(0)) {
            Err(SpendError::Conflicting) => {}
            other => panic!("expected Conflicting, got {other:?}"),
        }
    }

    #[test]
    fn spend_conflicting_ignored() {
        let h = TestHarness::new(10, TxFlags::CONFLICTING);
        let mut req = h.spend_req(0);
        req.ignore_conflicting = true;
        assert!(h.engine.spend(&req).is_ok());
    }

    #[test]
    fn spend_locked_blocked() {
        let h = TestHarness::new(10, TxFlags::LOCKED);
        match h.engine.spend(&h.spend_req(0)) {
            Err(SpendError::Locked) => {}
            other => panic!("expected Locked, got {other:?}"),
        }
    }

    #[test]
    fn spend_locked_ignored() {
        let h = TestHarness::new(10, TxFlags::LOCKED);
        let mut req = h.spend_req(0);
        req.ignore_locked = true;
        assert!(h.engine.spend(&req).is_ok());
    }

    #[test]
    fn spend_immature_coinbase() {
        let h = TestHarness::with_metadata(10, TxFlags::IS_COINBASE, |m| {
            m.spending_height = 100;
        });
        let mut req = h.spend_req(0);
        req.current_block_height = 50;
        match h.engine.spend(&req) {
            Err(SpendError::CoinbaseImmature {
                spending_height: 100,
                current_height: 50,
            }) => {}
            other => panic!("expected CoinbaseImmature, got {other:?}"),
        }
    }

    #[test]
    fn spend_mature_coinbase_equal() {
        let h = TestHarness::with_metadata(10, TxFlags::IS_COINBASE, |m| {
            m.spending_height = 100;
        });
        let mut req = h.spend_req(0);
        req.current_block_height = 100;
        assert!(h.engine.spend(&req).is_ok());
    }

    #[test]
    fn spend_mature_coinbase_above() {
        let h = TestHarness::with_metadata(10, TxFlags::IS_COINBASE, |m| {
            m.spending_height = 100;
        });
        let mut req = h.spend_req(0);
        req.current_block_height = 200;
        assert!(h.engine.spend(&req).is_ok());
    }

    #[test]
    fn spend_hash_mismatch() {
        let h = TestHarness::new(10, TxFlags::empty());
        let mut req = h.spend_req(0);
        req.utxo_hash = [0xFF; 32]; // Wrong hash
        match h.engine.spend(&req) {
            Err(SpendError::UtxoHashMismatch { offset: 0 }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    #[test]
    fn spend_idempotent_same_data() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(5)).unwrap();
        let meta_after_first = h.engine.read_metadata(&h.key).unwrap();
        let spent_after_first = { meta_after_first.spent_utxos };

        // Spend again with same data — should be idempotent
        h.engine.spend(&h.spend_req(5)).unwrap();
        let meta_after_second = h.engine.read_metadata(&h.key).unwrap();
        let spent_after_second = { meta_after_second.spent_utxos };

        assert_eq!(spent_after_first, 1);
        assert_eq!(spent_after_second, 1); // NOT incremented again
    }

    #[test]
    fn spend_already_spent_different_data() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(5)).unwrap();

        let mut req = h.spend_req(5);
        req.spending_data[0] = 0xCD; // Different spending data
        match h.engine.spend(&req) {
            Err(SpendError::AlreadySpent { offset: 5, .. }) => {}
            other => panic!("expected AlreadySpent, got {other:?}"),
        }
    }

    #[test]
    fn spend_frozen_utxo() {
        let h = TestHarness::new(10, TxFlags::empty());
        // Manually write a frozen slot
        let entry = h.engine.lookup(&h.key).unwrap();
        let frozen = UtxoSlot::new_frozen(h.slot_hash(3));
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 3, &frozen).unwrap();

        match h.engine.spend(&h.spend_req(3)) {
            Err(SpendError::Frozen { offset: 3 }) => {}
            other => panic!("expected Frozen, got {other:?}"),
        }
    }

    #[test]
    fn spend_pruned_utxo() {
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();
        let mut pruned_slot = UtxoSlot::new_spent(h.slot_hash(4), h.make_spending_data(0x11));
        pruned_slot.status = UTXO_PRUNED;
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 4, &pruned_slot).unwrap();

        match h.engine.spend(&h.spend_req(4)) {
            Err(SpendError::Pruned { offset: 4 }) => {}
            other => panic!("expected Pruned, got {other:?}"),
        }
    }

    #[test]
    fn spend_frozen_until() {
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();
        // Write a slot with spendable_height = 2000
        let mut slot = UtxoSlot::new_unspent(h.slot_hash(2));
        slot.spending_data[0..4].copy_from_slice(&2000u32.to_le_bytes());
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 2, &slot).unwrap();

        let mut req = h.spend_req(2);
        req.current_block_height = 1000;
        match h.engine.spend(&req) {
            Err(SpendError::FrozenUntil {
                offset: 2,
                spendable_at_height: 2000,
            }) => {}
            other => panic!("expected FrozenUntil, got {other:?}"),
        }
    }

    #[test]
    fn spend_frozen_until_equal_height() {
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();
        let mut slot = UtxoSlot::new_unspent(h.slot_hash(2));
        slot.spending_data[0..4].copy_from_slice(&1000u32.to_le_bytes());
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 2, &slot).unwrap();

        let mut req = h.spend_req(2);
        req.current_block_height = 1000;
        match h.engine.spend(&req) {
            Err(SpendError::FrozenUntil { .. }) => {}
            other => panic!("expected FrozenUntil (>= check), got {other:?}"),
        }
    }

    #[test]
    fn spend_frozen_until_past() {
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();
        let mut slot = UtxoSlot::new_unspent(h.slot_hash(2));
        slot.spending_data[0..4].copy_from_slice(&500u32.to_le_bytes());
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 2, &slot).unwrap();

        let mut req = h.spend_req(2);
        req.current_block_height = 1000;
        assert!(h.engine.spend(&req).is_ok());
    }

    #[test]
    fn spend_offset_out_of_range() {
        let h = TestHarness::new(10, TxFlags::empty());
        let mut req = h.spend_req(99);
        req.utxo_hash = [0; 32]; // Won't matter
        match h.engine.spend(&req) {
            Err(SpendError::UtxoNotFound { offset: 99 }) => {}
            other => panic!("expected UtxoNotFound, got {other:?}"),
        }
    }

    #[test]
    fn spend_counter_increments() {
        let h = TestHarness::new(10, TxFlags::empty());
        let before = { h.engine.read_metadata(&h.key).unwrap().spent_utxos };
        assert_eq!(before, 0);

        h.engine.spend(&h.spend_req(0)).unwrap();
        let after = { h.engine.read_metadata(&h.key).unwrap().spent_utxos };
        assert_eq!(after, 1);
    }

    #[test]
    fn spend_counter_not_incremented_on_failure() {
        let h = TestHarness::new(10, TxFlags::empty());
        let mut req = h.spend_req(0);
        req.utxo_hash = [0xFF; 32];
        let _ = h.engine.spend(&req);
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.spent_utxos }, 0);
    }

    #[test]
    fn spend_counter_not_incremented_on_idempotent() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(0)).unwrap(); // Idempotent

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.spent_utxos }, 1);
    }

    #[test]
    fn spend_generation_increments() {
        let h = TestHarness::new(10, TxFlags::empty());
        let g0 = { h.engine.read_metadata(&h.key).unwrap().generation };

        h.engine.spend(&h.spend_req(0)).unwrap();
        let g1 = { h.engine.read_metadata(&h.key).unwrap().generation };
        assert_eq!(g1, g0 + 1);
    }

    #[test]
    fn spend_updated_at_set() {
        let h = TestHarness::new(10, TxFlags::empty());
        let before = now_millis();
        h.engine.spend(&h.spend_req(0)).unwrap();
        let after = now_millis();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        let updated = { meta.updated_at };
        assert!(updated >= before && updated <= after + 1);
    }

    // -- SpendMulti tests --

    #[test]
    fn spend_multi_10_valid() {
        let h = TestHarness::new(20, TxFlags::empty());
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: (0..10)
                .map(|i| SpendItem {
                    offset: i,
                    utxo_hash: h.slot_hash(i),
                    spending_data: h.make_spending_data(i as u8),
                    idx: i,
                })
                .collect(),
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = h.engine.spend_multi(&req).unwrap();
        assert!(resp.errors.is_empty());
        assert_eq!(resp.spent_count, 10);

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.spent_utxos }, 10);
    }

    #[test]
    fn spend_multi_partial_errors() {
        let h = TestHarness::new(20, TxFlags::empty());
        let mut spends: Vec<SpendItem> = (0..10)
            .map(|i| SpendItem {
                offset: i,
                utxo_hash: h.slot_hash(i),
                spending_data: h.make_spending_data(i as u8),
                idx: i,
            })
            .collect();
        // Corrupt hash for items 3 and 7
        spends[3].utxo_hash = [0xFF; 32];
        spends[7].utxo_hash = [0xFF; 32];

        let req = SpendMultiRequest {
            tx_key: h.key,
            spends,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = h.engine.spend_multi(&req).unwrap();
        assert_eq!(resp.errors.len(), 2);
        assert!(resp.errors.contains_key(&3));
        assert!(resp.errors.contains_key(&7));
        assert_eq!(resp.spent_count, 8);
    }

    #[test]
    fn spend_multi_empty() {
        let h = TestHarness::new(10, TxFlags::empty());
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = h.engine.spend_multi(&req).unwrap();
        assert!(resp.errors.is_empty());
        assert_eq!(resp.spent_count, 0);
    }

    #[test]
    fn spend_multi_generation_increments_once() {
        let h = TestHarness::new(20, TxFlags::empty());
        let g0 = { h.engine.read_metadata(&h.key).unwrap().generation };

        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: (0..5)
                .map(|i| SpendItem {
                    offset: i,
                    utxo_hash: h.slot_hash(i),
                    spending_data: h.make_spending_data(i as u8),
                    idx: i,
                })
                .collect(),
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        h.engine.spend_multi(&req).unwrap();
        let g1 = { h.engine.read_metadata(&h.key).unwrap().generation };
        assert_eq!(g1, g0 + 1);
    }

    #[test]
    fn spend_multi_dah_index_updated() {
        let h = TestHarness::with_metadata(10, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
            };
        });

        // Spend all UTXOs
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: (0..10)
                .map(|i| SpendItem {
                    offset: i,
                    utxo_hash: h.slot_hash(i),
                    spending_data: h.make_spending_data(i as u8),
                    idx: i,
                })
                .collect(),
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        h.engine.spend_multi(&req).unwrap();

        // DAH index should have an entry
        let dah = h.engine.dah_index();
        let results = dah.range_query(2000);
        assert!(!results.is_empty());
    }

    // -- Unspend tests --

    #[test]
    fn unspend_spent_utxo() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(5)).unwrap();

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 5,
            utxo_hash: h.slot_hash(5),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();

        let slot = h.engine.read_slot(&h.key, 5).unwrap();
        assert!(slot.is_unspent());
        assert_eq!(slot.spending_data, [0u8; 36]);
    }

    #[test]
    fn unspend_already_unspent_noop() {
        let h = TestHarness::new(10, TxFlags::empty());
        let g0 = { h.engine.read_metadata(&h.key).unwrap().generation };

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 5,
            utxo_hash: h.slot_hash(5),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();

        // Generation should NOT increment for no-op
        let g1 = { h.engine.read_metadata(&h.key).unwrap().generation };
        assert_eq!(g1, g0);
    }

    #[test]
    fn unspend_frozen_error() {
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();
        let frozen = UtxoSlot::new_frozen(h.slot_hash(3));
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 3, &frozen).unwrap();

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 3,
            utxo_hash: h.slot_hash(3),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        match h.engine.unspend(&req) {
            Err(SpendError::Frozen { offset: 3 }) => {}
            other => panic!("expected Frozen, got {other:?}"),
        }
    }

    #[test]
    fn unspend_nonexistent_tx() {
        let h = TestHarness::new(10, TxFlags::empty());
        let req = UnspendRequest {
            tx_key: TxKey { txid: [0xFF; 32] },
            offset: 0,
            utxo_hash: [0; 32],
            current_block_height: 1000,
            block_height_retention: 288,
        };
        match h.engine.unspend(&req) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn unspend_hash_mismatch() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(5)).unwrap();

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 5,
            utxo_hash: [0xFF; 32],
            current_block_height: 1000,
            block_height_retention: 288,
        };
        match h.engine.unspend(&req) {
            Err(SpendError::UtxoHashMismatch { offset: 5 }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    #[test]
    fn unspend_decrements_counter() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(5)).unwrap();
        assert_eq!({ h.engine.read_metadata(&h.key).unwrap().spent_utxos }, 1);

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 5,
            utxo_hash: h.slot_hash(5),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();
        assert_eq!({ h.engine.read_metadata(&h.key).unwrap().spent_utxos }, 0);
    }

    #[test]
    fn unspend_generation_increments() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(5)).unwrap();
        let g1 = { h.engine.read_metadata(&h.key).unwrap().generation };

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 5,
            utxo_hash: h.slot_hash(5),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();
        let g2 = { h.engine.read_metadata(&h.key).unwrap().generation };
        assert_eq!(g2, g1 + 1);
    }

    #[test]
    fn unspend_clears_dah() {
        let h = TestHarness::with_metadata(10, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        // Spend all 10
        for i in 0..10 {
            h.engine.spend(&h.spend_req(i)).unwrap();
        }
        // DAH should be set
        assert!(!h.engine.dah_index().range_query(u32::MAX).is_empty());

        // Unspend one
        let req = UnspendRequest {
            tx_key: h.key,
            offset: 5,
            utxo_hash: h.slot_hash(5),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();

        // DAH should be cleared
        assert!(h.engine.dah_index().range_query(u32::MAX).is_empty());
    }

    // -- Signal / deleteAtHeight tests --

    #[test]
    fn spend_last_utxo_sets_dah() {
        let h = TestHarness::with_metadata(2, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        // Spend first UTXO
        h.engine.spend(&h.spend_req(0)).unwrap();
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.delete_at_height }, 0); // Not yet all spent

        // Spend second (last) UTXO
        h.engine.spend(&h.spend_req(1)).unwrap();
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.delete_at_height }, 1288); // 1000 + 288
    }

    #[test]
    fn spend_last_no_blocks_no_dah() {
        let h = TestHarness::new(2, TxFlags::empty());
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(1)).unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.delete_at_height }, 0); // No blocks → no DAH
    }

    #[test]
    fn retention_zero_no_dah() {
        let h = TestHarness::with_metadata(1, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        let mut req = h.spend_req(0);
        req.block_height_retention = 0;
        h.engine.spend(&req).unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn preserve_until_blocks_dah() {
        let h = TestHarness::with_metadata(1, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
            m.preserve_until = 5000;
        });

        h.engine.spend(&h.spend_req(0)).unwrap();
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.delete_at_height }, 0);
    }

    // -- Concurrency tests --

    #[test]
    fn concurrent_spend_different_utxos() {
        let h = TestHarness::new(100, TxFlags::empty());
        let engine = h.engine.clone();
        let key = h.key;

        let handles: Vec<_> = (0..100u32)
            .map(|i| {
                let engine = engine.clone();
                let mut hash = [0u8; 32];
                hash[0] = (i & 0xFF) as u8;
                hash[1] = ((i >> 8) & 0xFF) as u8;
                let mut sd = [0u8; 36];
                sd[0] = i as u8;
                sd[32..36].copy_from_slice(&1u32.to_le_bytes());

                std::thread::spawn(move || {
                    let req = SpendRequest {
                        tx_key: key,
                        offset: i,
                        utxo_hash: hash,
                        spending_data: sd,
                        ignore_conflicting: false,
                        ignore_locked: false,
                        current_block_height: 1000,
                        block_height_retention: 288,
                    };
                    engine.spend(&req).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 100);
    }

    #[test]
    fn concurrent_spend_same_utxo_same_data() {
        let h = TestHarness::new(10, TxFlags::empty());
        let engine = h.engine.clone();
        let key = h.key;
        let hash = h.slot_hash(5);
        let sd = h.make_spending_data(0xAB);

        let handles: Vec<_> = (0..100)
            .map(|_| {
                let engine = engine.clone();
                std::thread::spawn(move || {
                    let req = SpendRequest {
                        tx_key: key,
                        offset: 5,
                        utxo_hash: hash,
                        spending_data: sd,
                        ignore_conflicting: false,
                        ignore_locked: false,
                        current_block_height: 1000,
                        block_height_retention: 288,
                    };
                    engine.spend(&req).unwrap(); // All should succeed (idempotent)
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 1); // Only incremented once
    }

    #[test]
    fn concurrent_spend_same_utxo_different_data() {
        let h = TestHarness::new(10, TxFlags::empty());
        let engine = h.engine.clone();
        let key = h.key;
        let hash = h.slot_hash(5);

        let results: Vec<_> = (0..100u8)
            .map(|i| {
                let engine = engine.clone();
                std::thread::spawn(move || {
                    let mut sd = [0u8; 36];
                    sd[0] = i;
                    sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                    let req = SpendRequest {
                        tx_key: key,
                        offset: 5,
                        utxo_hash: hash,
                        spending_data: sd,
                        ignore_conflicting: false,
                        ignore_locked: false,
                        current_block_height: 1000,
                        block_height_retention: 288,
                    };
                    engine.spend(&req)
                })
            })
            .collect();

        let mut successes = 0;
        let mut already_spent = 0;
        for handle in results {
            match handle.join().unwrap() {
                Ok(_) => successes += 1,
                Err(SpendError::AlreadySpent { .. }) => already_spent += 1,
                other => panic!("unexpected result: {other:?}"),
            }
        }

        assert_eq!(successes, 1);
        assert_eq!(already_spent, 99);

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 1);
    }

    #[test]
    fn concurrent_different_transactions() {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(128 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone());
        let mut index = Index::new(200).unwrap();

        let mut keys = Vec::new();
        for i in 0..50u64 {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            txid[8..16].copy_from_slice(&(i * 7).to_le_bytes());
            txid[16..18].copy_from_slice(&(i as u16).to_le_bytes());
            let key = TxKey { txid };
            keys.push(key);

            let record_size = TxMetadata::record_size_for(10);
            let offset = alloc.allocate(record_size).unwrap();

            let mut meta = TxMetadata::new(10);
            meta.tx_id = txid;
            let slots: Vec<UtxoSlot> = (0..10u32)
                .map(|s| {
                    let mut h = [0u8; 32];
                    h[0] = (s & 0xFF) as u8;
                    UtxoSlot::new_unspent(h)
                })
                .collect();
            io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

            index.register(key, TxIndexEntry {
                device_id: 0,
                record_offset: offset,
                utxo_count: 10,
                cold_offset: 0,
                cold_size: 0,
                flags: 0,
            }).unwrap();
        }

        let engine = Arc::new(Engine::new(
            dev,
            index,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        ));

        let handles: Vec<_> = keys
            .iter()
            .map(|&key| {
                let engine = engine.clone();
                std::thread::spawn(move || {
                    let req = SpendRequest {
                        tx_key: key,
                        offset: 0,
                        utxo_hash: {
                            let mut h = [0u8; 32];
                            h[0] = 0;
                            h
                        },
                        spending_data: {
                            let mut sd = [0u8; 36];
                            sd[0] = 0xAA;
                            sd
                        },
                        ignore_conflicting: false,
                        ignore_locked: false,
                        current_block_height: 1000,
                        block_height_retention: 288,
                    };
                    engine.spend(&req).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // All 50 transactions should have slot 0 spent
        for key in &keys {
            let slot = engine.read_slot(key, 0).unwrap();
            assert!(slot.is_spent());
        }
    }
}
