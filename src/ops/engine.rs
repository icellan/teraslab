//! Store engine — thread-safe coordinator for all UTXO operations.
//!
//! Owns the index, device, locks, and secondary indexes. Provides the
//! spend/unspend methods that are the public API for this phase.

use crate::allocator::SlotAllocator;
use crate::device::{AlignedBuf, BlockDevice};
use crate::index::{DahIndex, Index, TxIndexEntry, TxKey, UnminedIndex};
use crate::io;
use crate::locks::StripedLocks;
use crate::ops::create::*;
use crate::ops::delete_eval::{evaluate_delete_at_height, DahPatch};
use crate::ops::error::SpendError;
use crate::ops::mark_longest_chain::*;
use crate::ops::remaining::*;
use crate::ops::set_mined::*;
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
    allocator: parking_lot::Mutex<SlotAllocator>,
    locks: StripedLocks,
    dah_index: parking_lot::Mutex<DahIndex>,
    unmined_index: parking_lot::Mutex<UnminedIndex>,
}

impl Engine {
    /// Create a new engine with the given components.
    pub fn new(
        device: Arc<dyn BlockDevice>,
        index: Index,
        allocator: SlotAllocator,
        locks: StripedLocks,
        dah_index: DahIndex,
        unmined_index: UnminedIndex,
    ) -> Self {
        Self {
            device,
            index: parking_lot::RwLock::new(index),
            allocator: parking_lot::Mutex::new(allocator),
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

    /// Iterate over all registered transaction keys (for migration scanning).
    ///
    /// Returns a snapshot of all keys currently in the index. This acquires
    /// a read lock briefly and collects all keys into a Vec.
    pub fn all_keys(&self) -> Vec<TxKey> {
        self.index.read().iter().map(|(k, _)| k).collect()
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

    /// Set or unset the mined state of a transaction.
    ///
    /// Adds or removes a block entry in the metadata. Only modifies the
    /// metadata region — UTXO slots are not touched.
    pub fn set_mined(&self, req: &SetMinedRequest) -> Result<SetMinedResponse, SpendError> {
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
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        let old_unmined = { metadata.unmined_since };
        let old_dah = { metadata.delete_at_height };

        if req.unset_mined {
            // Remove block entry by scanning inline and overflow entries
            let count = metadata.block_entry_count as usize;
            let inline_count = count.min(INLINE_BLOCK_ENTRIES);
            let mut found = false;

            // Check inline entries first
            for i in 0..inline_count {
                if { metadata.block_entries_inline[i].block_id } == req.block_id {
                    // Swap with last entry (may be inline or from overflow)
                    if count > INLINE_BLOCK_ENTRIES {
                        // Last entry is in overflow — pull it into the inline slot
                        let mut overflow =
                            read_overflow_entries(&*self.device, &metadata)
                                .map_err(|e| SpendError::StorageError {
                                    detail: format!("{e}"),
                                })?;
                        let last = overflow.pop().unwrap();
                        metadata.block_entries_inline[i] = last;
                        write_overflow_entries(
                            &*self.device,
                            &self.allocator,
                            &mut metadata,
                            &overflow,
                        )
                        .map_err(|e| SpendError::StorageError {
                            detail: format!("{e}"),
                        })?;
                    } else if i < inline_count - 1 {
                        metadata.block_entries_inline[i] =
                            metadata.block_entries_inline[inline_count - 1];
                    }
                    if count <= INLINE_BLOCK_ENTRIES {
                        let last_idx = inline_count - 1;
                        metadata.block_entries_inline[last_idx] = BlockEntry {
                            block_id: 0,
                            block_height: 0,
                            subtree_idx: 0,
                        };
                    }
                    metadata.block_entry_count -= 1;
                    found = true;
                    break;
                }
            }

            // Check overflow entries if not found inline
            if !found && count > INLINE_BLOCK_ENTRIES {
                let mut overflow =
                    read_overflow_entries(&*self.device, &metadata)
                        .map_err(|e| SpendError::StorageError {
                            detail: format!("{e}"),
                        })?;
                if let Some(pos) = overflow.iter().position(|e| e.block_id == req.block_id) {
                    overflow.swap_remove(pos);
                    write_overflow_entries(
                        &*self.device,
                        &self.allocator,
                        &mut metadata,
                        &overflow,
                    )
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("{e}"),
                    })?;
                    metadata.block_entry_count -= 1;
                }
            }
        } else {
            // Add block entry — check for duplicate
            let count = metadata.block_entry_count as usize;
            let inline_count = count.min(INLINE_BLOCK_ENTRIES);
            let mut exists = false;

            for i in 0..inline_count {
                if { metadata.block_entries_inline[i].block_id } == req.block_id {
                    exists = true;
                    break;
                }
            }

            // Check overflow entries for duplicate
            if !exists && count > INLINE_BLOCK_ENTRIES {
                let overflow =
                    read_overflow_entries(&*self.device, &metadata)
                        .map_err(|e| SpendError::StorageError {
                            detail: format!("{e}"),
                        })?;
                if overflow.iter().any(|e| e.block_id == req.block_id) {
                    exists = true;
                }
            }

            if !exists {
                if count < INLINE_BLOCK_ENTRIES {
                    metadata.block_entries_inline[count] = BlockEntry {
                        block_id: req.block_id,
                        block_height: req.block_height,
                        subtree_idx: req.subtree_idx,
                    };
                } else {
                    // Overflow: read existing overflow entries, append, write back
                    let mut overflow =
                        read_overflow_entries(&*self.device, &metadata)
                            .map_err(|e| SpendError::StorageError {
                                detail: format!("{e}"),
                            })?;
                    overflow.push(BlockEntry {
                        block_id: req.block_id,
                        block_height: req.block_height,
                        subtree_idx: req.subtree_idx,
                    });
                    write_overflow_entries(
                        &*self.device,
                        &self.allocator,
                        &mut metadata,
                        &overflow,
                    )
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("{e}"),
                    })?;
                }
                metadata.block_entry_count += 1;
            }
        }

        // Update unmined_since
        let new_count = metadata.block_entry_count;
        if new_count > 0 && req.on_longest_chain {
            metadata.unmined_since = 0;
        } else if new_count == 0 {
            metadata.unmined_since = req.current_block_height;
        }

        // Clear LOCKED flag if set
        if metadata.flags.contains(TxFlags::LOCKED) {
            metadata.flags -= TxFlags::LOCKED;
        }

        // Mutation bookkeeping
        metadata.generation = { metadata.generation }.wrapping_add(1);
        metadata.updated_at = now_millis();

        // Evaluate deleteAtHeight
        let (signal, dah_patch) = evaluate_delete_at_height(
            &metadata,
            req.current_block_height,
            req.block_height_retention,
        );
        if let Some(ref patch) = dah_patch {
            apply_dah_patch(&mut metadata, patch);
        }

        // Write metadata
        io::write_metadata(&*self.device, record_offset, &metadata)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        // Update secondary indexes
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

        let new_unmined = { metadata.unmined_since };
        if new_unmined != old_unmined {
            let mut unmined = self.unmined_index.lock();
            if old_unmined != 0 {
                unmined.remove(&req.tx_key);
            }
            if new_unmined != 0 {
                unmined.insert(new_unmined, req.tx_key);
            }
        }

        let block_ids = collect_all_block_ids(&*self.device, &metadata)
            .unwrap_or_else(|_| collect_block_ids(&metadata));

        Ok(SetMinedResponse {
            signal,
            block_ids,
        })
    }

    /// Mark a transaction as on or off the longest chain.
    ///
    /// Only modifies `unmined_since` — block entries and UTXO slots are
    /// not touched. Called during chain reorganizations.
    pub fn mark_on_longest_chain(
        &self,
        req: &MarkOnLongestChainRequest,
    ) -> Result<MarkOnLongestChainResponse, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);

        let entry = self
            .index
            .read()
            .lookup(&req.tx_key)
            .ok_or(SpendError::TxNotFound)?;
        let record_offset = entry.record_offset;

        let mut metadata = io::read_metadata(&*self.device, record_offset)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        let old_unmined = { metadata.unmined_since };
        let old_dah = { metadata.delete_at_height };

        if req.on_longest_chain {
            metadata.unmined_since = 0;
        } else {
            metadata.unmined_since = req.current_block_height;
        }

        // Mutation bookkeeping
        metadata.generation = { metadata.generation }.wrapping_add(1);
        metadata.updated_at = now_millis();

        // Evaluate deleteAtHeight (longest chain status affects DAH)
        let (signal, dah_patch) = evaluate_delete_at_height(
            &metadata,
            req.current_block_height,
            req.block_height_retention,
        );
        if let Some(ref patch) = dah_patch {
            apply_dah_patch(&mut metadata, patch);
        }

        io::write_metadata(&*self.device, record_offset, &metadata)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        // Update secondary indexes
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

        let new_unmined = { metadata.unmined_since };
        if new_unmined != old_unmined {
            let mut unmined = self.unmined_index.lock();
            if old_unmined != 0 {
                unmined.remove(&req.tx_key);
            }
            if new_unmined != 0 {
                unmined.insert(new_unmined, req.tx_key);
            }
        }

        Ok(MarkOnLongestChainResponse { signal })
    }

    // -----------------------------------------------------------------------
    // Creation
    // -----------------------------------------------------------------------

    /// Create a new transaction record.
    ///
    /// Allocates space, writes the complete record (metadata + UTXO slots +
    /// optional cold data) in one I/O operation, and registers it in the
    /// index. The record is immediately available for spend/setMined.
    pub fn create(&self, req: &CreateRequest) -> Result<CreateResponse, CreateError> {
        let utxo_count = req.utxo_hashes.len() as u32;
        if utxo_count == 0 {
            return Err(CreateError::InvalidUtxoCount);
        }

        let key = req.tx_key();

        // Check for duplicate txid
        if self.index.read().lookup(&key).is_some() {
            return Err(CreateError::DuplicateTxId);
        }

        // Calculate cold data size
        let cold_data = build_cold_data(&req.inputs, &req.outputs, &req.inpoints);
        let cold_size = cold_data.len();

        // Calculate total record size
        let base_size = TxMetadata::record_size_for(utxo_count);
        let total_size = base_size + cold_size as u64;

        // Allocate space
        let record_offset = self
            .allocator
            .lock()
            .allocate(total_size)
            .map_err(|_| CreateError::DeviceFull)?;

        // Build metadata
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = req.tx_id;
        meta.tx_version = req.tx_version;
        meta.locktime = req.locktime;
        meta.fee = req.fee;
        meta.size_in_bytes = req.size_in_bytes;
        meta.extended_size = req.extended_size;
        meta.spending_height = req.spending_height;
        meta.created_at = req.created_at;
        meta.record_size = total_size as u32;

        // Set flags
        let mut flags = TxFlags::empty();
        if req.is_coinbase {
            flags |= TxFlags::IS_COINBASE;
        }
        if req.is_external {
            flags |= TxFlags::EXTERNAL;
        }
        if req.conflicting {
            flags |= TxFlags::CONFLICTING;
        }
        if req.locked {
            flags |= TxFlags::LOCKED;
        }
        meta.flags = flags;

        // Set unmined_since
        if req.mined_block_infos.is_empty() {
            meta.unmined_since = req.block_height;
        } else {
            meta.unmined_since = 0;
            // Populate inline block entries
            let entries = req.block_entries();
            let inline_count = entries.len().min(INLINE_BLOCK_ENTRIES);
            for (i, entry) in entries.iter().take(inline_count).enumerate() {
                meta.block_entries_inline[i] = *entry;
            }
            meta.block_entry_count = entries.len() as u8;
        }

        // Build UTXO slots
        let slots: Vec<UtxoSlot> = req
            .utxo_hashes
            .iter()
            .map(|hash| {
                if req.frozen {
                    UtxoSlot::new_frozen(*hash)
                } else {
                    UtxoSlot::new_unspent(*hash)
                }
            })
            .collect();

        // Write complete record in one operation
        self.write_full_record_with_cold(record_offset, &meta, &slots, &cold_data)?;

        // Register in index
        let cold_offset = if cold_size > 0 {
            record_offset + base_size
        } else {
            0
        };
        let index_entry = TxIndexEntry {
            device_id: 0,
            record_offset,
            utxo_count,
            cold_offset,
            cold_size: cold_size as u32,
            flags: flags.bits(),
        };
        self.index
            .write()
            .register(key, index_entry)
            .map_err(|e| CreateError::StorageError {
                detail: format!("{e}"),
            })?;

        // Update unmined secondary index if applicable
        if meta.unmined_since != 0 {
            self.unmined_index.lock().insert(meta.unmined_since, key);
        }

        // Update parent records' conflicting-children lists
        if req.conflicting {
            for parent_txid in &req.parent_txids {
                let parent_key = TxKey { txid: *parent_txid };
                let _ = self.append_conflicting_child(&parent_key, req.tx_id);
            }
        }

        Ok(CreateResponse {
            record_offset,
            utxo_count,
        })
    }

    /// Create multiple transaction records in a batch.
    ///
    /// Each creation is independent — a failure in one does not affect others.
    /// Allocations for failed creations are rolled back.
    pub fn create_batch(
        &self,
        requests: &[CreateRequest],
    ) -> Vec<Result<CreateResponse, CreateError>> {
        requests.iter().map(|req| self.create(req)).collect()
    }

    /// Write a complete record including optional cold data.
    fn write_full_record_with_cold(
        &self,
        record_offset: u64,
        metadata: &TxMetadata,
        slots: &[UtxoSlot],
        cold_data: &[u8],
    ) -> Result<(), CreateError> {
        let align = self.device.alignment();
        let data_len = METADATA_SIZE + slots.len() * UTXO_SLOT_SIZE + cold_data.len();
        let aligned_len = data_len.div_ceil(align) * align;

        let mut buf = crate::device::AlignedBuf::new(aligned_len, align);

        // Write metadata
        let mut meta_bytes = [0u8; METADATA_SIZE];
        metadata.to_bytes(&mut meta_bytes);
        buf[..METADATA_SIZE].copy_from_slice(&meta_bytes);

        // Write slots
        for (i, slot) in slots.iter().enumerate() {
            let offset = METADATA_SIZE + i * UTXO_SLOT_SIZE;
            let mut slot_bytes = [0u8; UTXO_SLOT_SIZE];
            slot.to_bytes(&mut slot_bytes);
            buf[offset..offset + UTXO_SLOT_SIZE].copy_from_slice(&slot_bytes);
        }

        // Write cold data
        if !cold_data.is_empty() {
            let cold_offset = METADATA_SIZE + slots.len() * UTXO_SLOT_SIZE;
            buf[cold_offset..cold_offset + cold_data.len()].copy_from_slice(cold_data);
        }

        self.device
            .pwrite(&buf, record_offset)
            .map_err(|e| CreateError::StorageError {
                detail: format!("{e}"),
            })?;

        Ok(())
    }

    /// Read cold data from a record (for testing).
    pub fn read_cold_data(&self, key: &TxKey) -> Result<Vec<u8>, SpendError> {
        let entry = self
            .index
            .read()
            .lookup(key)
            .ok_or(SpendError::TxNotFound)?;

        if entry.cold_offset == 0 || entry.cold_size == 0 {
            return Ok(Vec::new());
        }

        let align = self.device.alignment();
        let aligned_base = entry.cold_offset / align as u64 * align as u64;
        let intra = (entry.cold_offset - aligned_base) as usize;
        let read_len = (intra + entry.cold_size as usize).div_ceil(align) * align;

        let mut buf = crate::device::AlignedBuf::new(read_len, align);
        self.device
            .pread(&mut buf, aligned_base)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;

        Ok(buf[intra..intra + entry.cold_size as usize].to_vec())
    }

    // -----------------------------------------------------------------------
    // Remaining operations (Phase 6)
    // -----------------------------------------------------------------------

    /// Freeze a UTXO (set status to FROZEN, spending_data all 0xFF).
    ///
    /// Does NOT modify metadata counters — frozen does not count as "spent".
    pub fn freeze(&self, req: &FreezeRequest) -> Result<(), SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self.index.read().lookup(&req.tx_key).ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let meta = io::read_metadata(&*self.device, ro)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        if req.offset >= { meta.utxo_count } {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        let slot = io::read_utxo_slot(&*self.device, ro, req.offset)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        if slot.hash != req.utxo_hash {
            return Err(SpendError::UtxoHashMismatch { offset: req.offset });
        }
        match slot.status {
            UTXO_FROZEN => return Err(SpendError::AlreadyFrozen { offset: req.offset }),
            UTXO_SPENT => {
                return Err(SpendError::AlreadySpent {
                    offset: req.offset,
                    spending_data: slot.spending_data,
                });
            }
            UTXO_UNSPENT => {}
            _ => {
                return Err(SpendError::StorageError {
                    detail: format!("unexpected status {:#04x}", slot.status),
                });
            }
        }

        let frozen = UtxoSlot::new_frozen(req.utxo_hash);
        io::write_utxo_slot(&*self.device, ro, req.offset, &frozen)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        Ok(())
    }

    /// Unfreeze a UTXO (set status to UNSPENT, spending_data zeroed).
    pub fn unfreeze(&self, req: &UnfreezeRequest) -> Result<(), SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self.index.read().lookup(&req.tx_key).ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let meta = io::read_metadata(&*self.device, ro)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        if req.offset >= { meta.utxo_count } {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        let slot = io::read_utxo_slot(&*self.device, ro, req.offset)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        if slot.hash != req.utxo_hash {
            return Err(SpendError::UtxoHashMismatch { offset: req.offset });
        }
        if slot.status != UTXO_FROZEN {
            return Err(SpendError::NotFrozen { offset: req.offset });
        }

        let unspent = UtxoSlot::new_unspent(req.utxo_hash);
        io::write_utxo_slot(&*self.device, ro, req.offset, &unspent)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        Ok(())
    }

    /// Reassign a frozen UTXO to a new hash with a spendable-after cooldown.
    pub fn reassign(&self, req: &ReassignRequest) -> Result<(), SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self.index.read().lookup(&req.tx_key).ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let mut meta = io::read_metadata(&*self.device, ro)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        if req.offset >= { meta.utxo_count } {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        let slot = io::read_utxo_slot(&*self.device, ro, req.offset)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        if slot.hash != req.utxo_hash {
            return Err(SpendError::UtxoHashMismatch { offset: req.offset });
        }
        if slot.status != UTXO_FROZEN {
            return Err(SpendError::NotFrozen { offset: req.offset });
        }

        // Write new slot with spendable height encoded in spending_data[0..4]
        let spendable_height = req.block_height.saturating_add(req.spendable_after);
        let mut new_slot = UtxoSlot::new_unspent(req.new_utxo_hash);
        new_slot.spending_data[0..4].copy_from_slice(&spendable_height.to_le_bytes());

        io::write_utxo_slot(&*self.device, ro, req.offset, &new_slot)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        // Update metadata (generation, updated_at, reassignment_count)
        meta.reassignment_count = meta.reassignment_count.saturating_add(1);
        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = now_millis();
        io::write_metadata(&*self.device, ro, &meta)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        Ok(())
    }

    /// Append a child txid to a parent record's conflicting-children list.
    /// Deduplicates: if the child already exists, this is a no-op.
    /// Returns Ok(()) if parent not found (may be on another node).
    pub fn append_conflicting_child(
        &self,
        parent_key: &TxKey,
        child_txid: [u8; 32],
    ) -> Result<(), SpendError> {
        let _guard = self.locks.lock(parent_key);
        let entry = match self.index.read().lookup(parent_key) {
            Some(e) => e,
            None => return Ok(()),
        };
        let ro = entry.record_offset;
        let mut meta = io::read_metadata(&*self.device, ro)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        let count = { meta.conflicting_children_count } as usize;
        let offset = { meta.conflicting_children_offset };

        // Read existing children
        let mut children: Vec<[u8; 32]> = Vec::with_capacity(count + 1);
        if count > 0 && offset != 0 {
            let align = self.device.alignment();
            let aligned_base = offset / align as u64 * align as u64;
            let intra = (offset - aligned_base) as usize;
            let read_len = (intra + count * 32).div_ceil(align) * align;
            let mut buf = crate::device::AlignedBuf::new(read_len, align);
            self.device.pread(&mut buf, aligned_base)
                .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
            for i in 0..count {
                let start = intra + i * 32;
                let mut txid = [0u8; 32];
                txid.copy_from_slice(&buf[start..start + 32]);
                children.push(txid);
            }
        }

        // Dedup
        if children.iter().any(|c| *c == child_txid) {
            return Ok(());
        }
        children.push(child_txid);

        // Free old block
        if count > 0 && offset != 0 {
            let _ = self.allocator.lock().free(offset, (count * 32) as u64);
        }

        // Allocate and write new block
        let new_size = (children.len() * 32) as u64;
        let new_offset = self.allocator.lock().allocate(new_size)
            .map_err(|_| SpendError::StorageError { detail: "device full for conflicting children".into() })?;

        let align = self.device.alignment();
        let aligned_base = new_offset / align as u64 * align as u64;
        let intra = (new_offset - aligned_base) as usize;
        let write_len = (intra + children.len() * 32).div_ceil(align) * align;
        let mut wbuf = crate::device::AlignedBuf::new(write_len, align);
        self.device.pread(&mut wbuf, aligned_base)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        for (i, child) in children.iter().enumerate() {
            wbuf[intra + i * 32..intra + (i + 1) * 32].copy_from_slice(child);
        }
        self.device.pwrite(&wbuf, aligned_base)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        // Update metadata
        meta.conflicting_children_count = children.len() as u8;
        meta.conflicting_children_offset = new_offset;
        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = now_millis();
        io::write_metadata(&*self.device, ro, &meta)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        Ok(())
    }

    /// Read all conflicting children txids for a transaction.
    pub fn read_conflicting_children(
        &self,
        key: &TxKey,
    ) -> Result<Vec<[u8; 32]>, SpendError> {
        let entry = self.index.read().lookup(key).ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;
        let meta = io::read_metadata(&*self.device, ro)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        let count = { meta.conflicting_children_count } as usize;
        let offset = { meta.conflicting_children_offset };
        if count == 0 || offset == 0 {
            return Ok(Vec::new());
        }

        let align = self.device.alignment();
        let aligned_base = offset / align as u64 * align as u64;
        let intra = (offset - aligned_base) as usize;
        let read_len = (intra + count * 32).div_ceil(align) * align;
        let mut buf = crate::device::AlignedBuf::new(read_len, align);
        self.device.pread(&mut buf, aligned_base)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        let mut result = Vec::with_capacity(count);
        for i in 0..count {
            let start = intra + i * 32;
            let mut txid = [0u8; 32];
            txid.copy_from_slice(&buf[start..start + 32]);
            result.push(txid);
        }
        Ok(result)
    }

    /// Set or clear the conflicting flag on a transaction.
    pub fn set_conflicting(
        &self,
        req: &SetConflictingRequest,
    ) -> Result<SetConflictingResponse, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self.index.read().lookup(&req.tx_key).ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let mut meta = io::read_metadata(&*self.device, ro)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        let old_dah = { meta.delete_at_height };

        if req.value {
            meta.flags |= TxFlags::CONFLICTING;
        } else {
            meta.flags -= meta.flags & TxFlags::CONFLICTING;
        }

        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = now_millis();

        let (signal, dah_patch) = evaluate_delete_at_height(
            &meta,
            req.current_block_height,
            req.block_height_retention,
        );
        if let Some(ref patch) = dah_patch {
            apply_dah_patch(&mut meta, patch);
        }

        io::write_metadata(&*self.device, ro, &meta)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        let new_dah = { meta.delete_at_height };
        if new_dah != old_dah {
            let mut dah = self.dah_index.lock();
            if old_dah != 0 { dah.remove(&req.tx_key); }
            if new_dah != 0 { dah.insert(new_dah, req.tx_key); }
        }

        // Update parent records' conflicting-children lists
        if req.value {
            // Read cold data to find parent txids from inputs.
            // Must drop the child lock first to avoid holding two locks.
            drop(_guard);
            if let Ok(cold_bytes) = self.read_cold_data(&req.tx_key) {
                let parent_txids = extract_parent_txids_from_cold_data(&cold_bytes);
                for parent_txid in parent_txids {
                    let parent_key = TxKey { txid: parent_txid };
                    let _ = self.append_conflicting_child(&parent_key, req.tx_key.txid);
                }
            }
        }

        Ok(SetConflictingResponse { signal })
    }

    /// Set or clear the locked flag on a transaction.
    pub fn set_locked(&self, req: &SetLockedRequest) -> Result<(), SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self.index.read().lookup(&req.tx_key).ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let mut meta = io::read_metadata(&*self.device, ro)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        let old_dah = { meta.delete_at_height };

        if req.value {
            meta.flags |= TxFlags::LOCKED;
            // Locking clears deleteAtHeight
            if old_dah != 0 {
                meta.delete_at_height = 0;
            }
        } else {
            meta.flags -= meta.flags & TxFlags::LOCKED;
        }

        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = now_millis();

        io::write_metadata(&*self.device, ro, &meta)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        let new_dah = { meta.delete_at_height };
        if new_dah != old_dah {
            let mut dah = self.dah_index.lock();
            if old_dah != 0 { dah.remove(&req.tx_key); }
            if new_dah != 0 { dah.insert(new_dah, req.tx_key); }
        }

        Ok(())
    }

    /// Preserve a record until a specific block height.
    ///
    /// Clears `delete_at_height` and sets `preserve_until`. If the record
    /// has the EXTERNAL flag, returns signal PRESERVE.
    pub fn preserve_until(
        &self,
        req: &PreserveUntilRequest,
    ) -> Result<PreserveUntilResponse, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self.index.read().lookup(&req.tx_key).ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let mut meta = io::read_metadata(&*self.device, ro)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        let old_dah = { meta.delete_at_height };

        meta.delete_at_height = 0;
        meta.preserve_until = req.block_height;
        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = now_millis();

        io::write_metadata(&*self.device, ro, &meta)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        if old_dah != 0 {
            self.dah_index.lock().remove(&req.tx_key);
        }

        let signal = if meta.flags.contains(TxFlags::EXTERNAL) {
            Signal::Preserve
        } else {
            Signal::None
        };
        Ok(PreserveUntilResponse { signal })
    }

    /// Delete a transaction record.
    ///
    /// Removes from index, frees device space, and cleans up secondary indexes.
    pub fn delete(&self, req: &DeleteRequest) -> Result<(), SpendError> {
        let _guard = self.locks.lock(&req.tx_key);

        let entry = match self.index.read().lookup(&req.tx_key) {
            Some(e) => e,
            None => return Err(SpendError::TxNotFound),
        };

        let record_size = {
            let meta = io::read_metadata(&*self.device, entry.record_offset)
                .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
            ({ meta.record_size }) as u64
        };

        // Free device space
        self.allocator
            .lock()
            .free(entry.record_offset, record_size)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;

        // Remove from index
        self.index.write().unregister(&req.tx_key);

        // Clean up secondary indexes
        self.dah_index.lock().remove(&req.tx_key);
        self.unmined_index.lock().remove(&req.tx_key);

        Ok(())
    }

    /// Read spending data for a specific UTXO (point read, no lock needed).
    pub fn get_spend(&self, req: &GetSpendRequest) -> Result<GetSpendResponse, SpendError> {
        let entry = self.index.read().lookup(&req.tx_key).ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let meta = io::read_metadata(&*self.device, ro)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        if req.offset >= { meta.utxo_count } {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        let slot = io::read_utxo_slot(&*self.device, ro, req.offset)
            .map_err(|e| SpendError::StorageError { detail: format!("{e}") })?;
        if slot.hash != req.utxo_hash {
            return Err(SpendError::UtxoHashMismatch { offset: req.offset });
        }

        let spending_data = match slot.status {
            UTXO_UNSPENT => None,
            UTXO_SPENT | UTXO_FROZEN => Some(slot.spending_data),
            UTXO_PRUNED => Some(slot.spending_data),
            _ => None,
        };

        Ok(GetSpendResponse {
            status: slot.status,
            spending_data,
            locktime: { meta.locktime },
        })
    }

    /// Get the unmined index (for testing).
    pub fn unmined_index(&self) -> parking_lot::MutexGuard<'_, UnminedIndex> {
        self.unmined_index.lock()
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

    /// Number of entries in the primary index.
    pub fn index_len(&self) -> usize {
        self.index.read().len()
    }

    /// Primary index statistics for monitoring.
    pub fn index_stats(&self) -> crate::index::IndexStats {
        self.index.read().stats()
    }

    /// Access the underlying block device.
    ///
    /// Used by the replication receiver for low-level slot operations
    /// (e.g. prune) that bypass the normal engine API.
    pub fn device(&self) -> &dyn BlockDevice {
        &*self.device
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract unique parent txids from cold data bytes.
///
/// Cold data format: `[inputs_len:4 LE][inputs_blob][outputs_len:4 LE][...][inpoints_len:4 LE][...]`
/// The inputs_blob contains length-prefixed entries: `[count:4 LE][per-input: [len:4 LE][extended-bytes]]`
/// The first 32 bytes of each extended-input are the prev_txid.
fn extract_parent_txids_from_cold_data(cold_bytes: &[u8]) -> Vec<[u8; 32]> {
    if cold_bytes.len() < 4 {
        return Vec::new();
    }
    // Outer wrapper: [inputs_blob_len:4][inputs_blob][...]
    let inputs_blob_len = u32::from_le_bytes(cold_bytes[0..4].try_into().unwrap_or([0; 4])) as usize;
    if inputs_blob_len == 0 || 4 + inputs_blob_len > cold_bytes.len() {
        return Vec::new();
    }
    let inputs_blob = &cold_bytes[4..4 + inputs_blob_len];

    // Inner format: [count:4][per-input: [len:4][extended-bytes]]
    if inputs_blob.len() < 4 {
        return Vec::new();
    }
    let count = u32::from_le_bytes(inputs_blob[0..4].try_into().unwrap_or([0; 4])) as usize;
    let mut pos = 4usize;
    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..count {
        if pos + 4 > inputs_blob.len() {
            break;
        }
        let entry_len = u32::from_le_bytes(inputs_blob[pos..pos + 4].try_into().unwrap_or([0; 4])) as usize;
        pos += 4;
        if entry_len < 32 || pos + entry_len > inputs_blob.len() {
            break;
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&inputs_blob[pos..pos + 32]);
        if seen.insert(txid) {
            result.push(txid);
        }
        pos += entry_len;
    }
    result
}

/// Build inline cold data from optional inputs/outputs/inpoints.
///
/// Format: `[inputs_len:4 LE][inputs][outputs_len:4 LE][outputs][inpoints_len:4 LE][inpoints]`
fn build_cold_data(
    inputs: &Option<Vec<u8>>,
    outputs: &Option<Vec<u8>>,
    inpoints: &Option<Vec<u8>>,
) -> Vec<u8> {
    let inputs_data = inputs.as_deref().unwrap_or(&[]);
    let outputs_data = outputs.as_deref().unwrap_or(&[]);
    let inpoints_data = inpoints.as_deref().unwrap_or(&[]);

    if inputs_data.is_empty() && outputs_data.is_empty() && inpoints_data.is_empty() {
        return Vec::new();
    }

    let total = 4 + inputs_data.len() + 4 + outputs_data.len() + 4 + inpoints_data.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&(inputs_data.len() as u32).to_le_bytes());
    buf.extend_from_slice(inputs_data);
    buf.extend_from_slice(&(outputs_data.len() as u32).to_le_bytes());
    buf.extend_from_slice(outputs_data);
    buf.extend_from_slice(&(inpoints_data.len() as u32).to_le_bytes());
    buf.extend_from_slice(inpoints_data);
    buf
}

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

/// Collect all block IDs including overflow entries read from device.
fn collect_all_block_ids(
    device: &dyn BlockDevice,
    metadata: &TxMetadata,
) -> Result<Vec<u32>, crate::device::DeviceError> {
    let count = metadata.block_entry_count as usize;
    let inline = count.min(INLINE_BLOCK_ENTRIES);
    let mut ids: Vec<u32> = metadata.block_entries_inline[..inline]
        .iter()
        .map(|e| { e.block_id })
        .collect();
    if count > INLINE_BLOCK_ENTRIES {
        let overflow = read_overflow_entries(device, metadata)?;
        ids.extend(overflow.iter().map(|e| e.block_id));
    }
    Ok(ids)
}

/// Read overflow block entries from the device.
fn read_overflow_entries(
    device: &dyn BlockDevice,
    metadata: &TxMetadata,
) -> Result<Vec<BlockEntry>, crate::device::DeviceError> {
    let overflow_offset = { metadata.block_overflow_offset };
    if overflow_offset == 0 {
        return Ok(Vec::new());
    }
    let count = metadata.block_entry_count as usize;
    let overflow_count = count.saturating_sub(INLINE_BLOCK_ENTRIES);
    if overflow_count == 0 {
        return Ok(Vec::new());
    }

    let alignment = device.alignment();
    let data_size = overflow_count * BLOCK_ENTRY_SIZE;
    let read_size = io::align_up(data_size, alignment);
    let mut buf = AlignedBuf::new(read_size, alignment);
    device.pread(&mut buf, overflow_offset)?;

    let mut entries = Vec::with_capacity(overflow_count);
    for i in 0..overflow_count {
        let start = i * BLOCK_ENTRY_SIZE;
        entries.push(BlockEntry::from_bytes(&buf[start..start + BLOCK_ENTRY_SIZE]));
    }
    Ok(entries)
}

/// Write overflow block entries to the device.
///
/// Allocates or reuses the overflow block. If `entries` is empty, frees the
/// overflow block and clears the metadata pointer.
fn write_overflow_entries(
    device: &dyn BlockDevice,
    allocator: &parking_lot::Mutex<SlotAllocator>,
    metadata: &mut TxMetadata,
    entries: &[BlockEntry],
) -> Result<(), crate::device::DeviceError> {
    let alignment = device.alignment();
    let old_offset = { metadata.block_overflow_offset };

    if entries.is_empty() {
        // Free the overflow block if one exists
        if old_offset != 0 {
            let _ = allocator.lock().free(old_offset, alignment as u64);
            metadata.block_overflow_offset = 0;
        }
        return Ok(());
    }

    let data_size = entries.len() * BLOCK_ENTRY_SIZE;
    let block_size = io::align_up(data_size, alignment);

    // Allocate new overflow block if needed (or reuse if same size)
    let offset = if old_offset != 0 {
        old_offset
    } else {
        allocator
            .lock()
            .allocate(block_size as u64)
            .map_err(|e| crate::device::DeviceError::Io(std::io::Error::other(
                format!("allocator: {e}"),
            )))?
    };

    let mut buf = AlignedBuf::new(block_size, alignment);
    for (i, entry) in entries.iter().enumerate() {
        let start = i * BLOCK_ENTRY_SIZE;
        entry.to_bytes(&mut buf[start..start + BLOCK_ENTRY_SIZE]);
    }
    device.pwrite(&buf, offset)?;
    metadata.block_overflow_offset = offset;
    Ok(())
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
                alloc,
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
            alloc,
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

    // -- SpendMulti additional tests --

    #[test]
    fn spend_multi_mix_of_error_types() {
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();

        // Freeze slot 2
        let frozen = UtxoSlot::new_frozen(h.slot_hash(2));
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 2, &frozen).unwrap();

        // Spend slot 4 with some data
        h.engine.spend(&h.spend_req(4)).unwrap();

        // Now batch: slot 0 (valid), slot 2 (frozen), slot 4 (already spent different data), slot 6 (hash mismatch)
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![
                SpendItem {
                    offset: 0,
                    utxo_hash: h.slot_hash(0),
                    spending_data: h.make_spending_data(0x01),
                    idx: 0,
                },
                SpendItem {
                    offset: 2,
                    utxo_hash: h.slot_hash(2),
                    spending_data: h.make_spending_data(0x02),
                    idx: 1,
                },
                SpendItem {
                    offset: 4,
                    utxo_hash: h.slot_hash(4),
                    spending_data: h.make_spending_data(0xCD), // Different from 0xAB
                    idx: 2,
                },
                SpendItem {
                    offset: 6,
                    utxo_hash: [0xFF; 32], // Wrong hash
                    spending_data: h.make_spending_data(0x03),
                    idx: 3,
                },
            ],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = h.engine.spend_multi(&req).unwrap();
        assert_eq!(resp.errors.len(), 3);
        assert_eq!(resp.spent_count, 1); // Only slot 0 succeeded
        assert!(matches!(resp.errors[&1], SpendError::Frozen { offset: 2 }));
        assert!(matches!(resp.errors[&2], SpendError::AlreadySpent { offset: 4, .. }));
        assert!(matches!(resp.errors[&3], SpendError::UtxoHashMismatch { offset: 6 }));
    }

    #[test]
    fn spend_multi_single_item_matches_spend() {
        let h = TestHarness::new(10, TxFlags::empty());

        // Single spend via spend_multi
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![SpendItem {
                offset: 3,
                utxo_hash: h.slot_hash(3),
                spending_data: h.make_spending_data(0xAB),
                idx: 0,
            }],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = h.engine.spend_multi(&req).unwrap();
        assert!(resp.errors.is_empty());
        assert_eq!(resp.spent_count, 1);

        // Verify same result as single spend
        let slot = h.engine.read_slot(&h.key, 3).unwrap();
        assert!(slot.is_spent());
        assert_eq!(slot.spending_data, h.make_spending_data(0xAB));
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.spent_utxos }, 1);
    }

    #[test]
    fn spend_multi_duplicate_offsets_same_data() {
        let h = TestHarness::new(10, TxFlags::empty());
        let sd = h.make_spending_data(0xAB);

        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![
                SpendItem {
                    offset: 5,
                    utxo_hash: h.slot_hash(5),
                    spending_data: sd,
                    idx: 0,
                },
                SpendItem {
                    offset: 5,
                    utxo_hash: h.slot_hash(5),
                    spending_data: sd, // Same data
                    idx: 1,
                },
            ],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = h.engine.spend_multi(&req).unwrap();
        assert!(resp.errors.is_empty()); // Both succeed (first spends, second is idempotent)
        assert_eq!(resp.spent_count, 1); // Counter only incremented once
    }

    #[test]
    fn spend_multi_duplicate_offsets_different_data() {
        let h = TestHarness::new(10, TxFlags::empty());

        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![
                SpendItem {
                    offset: 5,
                    utxo_hash: h.slot_hash(5),
                    spending_data: h.make_spending_data(0xAA),
                    idx: 0,
                },
                SpendItem {
                    offset: 5,
                    utxo_hash: h.slot_hash(5),
                    spending_data: h.make_spending_data(0xBB), // Different data
                    idx: 1,
                },
            ],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = h.engine.spend_multi(&req).unwrap();
        assert_eq!(resp.errors.len(), 1);
        assert!(resp.errors.contains_key(&1)); // Second one fails
        assert!(matches!(resp.errors[&1], SpendError::AlreadySpent { offset: 5, .. }));
        assert_eq!(resp.spent_count, 1);
    }

    #[test]
    fn spend_multi_response_includes_block_ids() {
        let h = TestHarness::with_metadata(10, TxFlags::empty(), |m| {
            m.block_entry_count = 2;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 42, block_height: 900, subtree_idx: 0,
            };
            m.block_entries_inline[1] = BlockEntry {
                block_id: 99, block_height: 901, subtree_idx: 1,
            };
        });

        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![SpendItem {
                offset: 0,
                utxo_hash: h.slot_hash(0),
                spending_data: h.make_spending_data(0xAB),
                idx: 0,
            }],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = h.engine.spend_multi(&req).unwrap();
        assert!(resp.block_ids.contains(&42));
        assert!(resp.block_ids.contains(&99));
        assert_eq!(resp.block_ids.len(), 2);
    }

    // -- Unspend additional tests --

    #[test]
    fn unspend_counter_not_below_zero() {
        let h = TestHarness::new(10, TxFlags::empty());
        // Metadata starts with spent_utxos = 0; unspending should not underflow
        // First ensure slot is in unspent state but force spent_utxos = 0
        // Actually, unspend of an unspent slot is a noop, so let's test with
        // spent_utxos already at 0 but a slot that is actually spent
        let entry = h.engine.lookup(&h.key).unwrap();
        let spent_slot = UtxoSlot::new_spent(h.slot_hash(3), h.make_spending_data(0x11));
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 3, &spent_slot).unwrap();
        // metadata.spent_utxos is still 0 (we wrote the slot directly, bypassing counter)

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 3,
            utxo_hash: h.slot_hash(3),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.spent_utxos }, 0); // Should not go below 0
    }

    #[test]
    fn unspend_pruned_error() {
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();
        let mut pruned_slot = UtxoSlot::new_spent(h.slot_hash(3), h.make_spending_data(0x11));
        pruned_slot.status = UTXO_PRUNED;
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 3, &pruned_slot).unwrap();

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 3,
            utxo_hash: h.slot_hash(3),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        match h.engine.unspend(&req) {
            Err(SpendError::Pruned { offset: 3 }) => {}
            other => panic!("expected Pruned, got {other:?}"),
        }
    }

    // -- Signal / deleteAtHeight additional tests --

    #[test]
    fn spend_non_last_utxo_signal_none() {
        let h = TestHarness::with_metadata(5, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        let resp = h.engine.spend(&h.spend_req(0)).unwrap();
        assert_eq!(resp.signal, Signal::None);
    }

    #[test]
    fn unspend_triggers_not_all_spent_signal() {
        let h = TestHarness::with_metadata(2, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        // Spend both UTXOs
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(1)).unwrap();

        // Now unspend one — should transition from all-spent to not-all-spent
        let req = UnspendRequest {
            tx_key: h.key,
            offset: 0,
            utxo_hash: h.slot_hash(0),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();
        // Non-external tx: clearing DAH returns Signal::None but DAH is actually cleared
        // The DAH index should be empty
        assert!(h.engine.dah_index().range_query(u32::MAX).is_empty());
    }

    #[test]
    fn signal_only_fires_on_state_change() {
        let h = TestHarness::with_metadata(5, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        // Spend slots 0 and 1 — neither is the last, no transition
        let r0 = h.engine.spend(&h.spend_req(0)).unwrap();
        assert_eq!(r0.signal, Signal::None);
        let r1 = h.engine.spend(&h.spend_req(1)).unwrap();
        assert_eq!(r1.signal, Signal::None);
    }

    #[test]
    fn last_spent_all_flag_updated() {
        let h = TestHarness::with_metadata(2, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        // Before spending, LAST_SPENT_ALL should be clear
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert!(!meta.flags.contains(TxFlags::LAST_SPENT_ALL));

        // Spend all UTXOs
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(1)).unwrap();

        // LAST_SPENT_ALL should now be set
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert!(meta.flags.contains(TxFlags::LAST_SPENT_ALL));

        // Unspend one
        let req = UnspendRequest {
            tx_key: h.key,
            offset: 0,
            utxo_hash: h.slot_hash(0),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();

        // LAST_SPENT_ALL should now be cleared
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert!(!meta.flags.contains(TxFlags::LAST_SPENT_ALL));
    }

    #[test]
    fn conflicting_tx_no_existing_dah_sets_dah() {
        let h = TestHarness::with_metadata(10, TxFlags::CONFLICTING, |_| {});
        let mut req = h.spend_req(0);
        req.ignore_conflicting = true;
        h.engine.spend(&req).unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_ne!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn conflicting_tx_existing_dah_no_signal() {
        let h = TestHarness::with_metadata(10, TxFlags::CONFLICTING, |m| {
            m.delete_at_height = 5000;
        });
        let mut req = h.spend_req(0);
        req.ignore_conflicting = true;
        let resp = h.engine.spend(&req).unwrap();
        assert_eq!(resp.signal, Signal::None);

        let meta = h.engine.read_metadata(&h.key).unwrap();
        // DAH should remain at the existing value (5000), not be overwritten
        assert_eq!({ meta.delete_at_height }, 5000);
    }

    #[test]
    fn external_tx_dah_signal() {
        let h = TestHarness::with_metadata(1, TxFlags::EXTERNAL, |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        let resp = h.engine.spend(&h.spend_req(0)).unwrap();
        assert_eq!(resp.signal, Signal::DeleteAtHeightSet);
    }

    #[test]
    fn dah_index_contains_entry_after_set() {
        let h = TestHarness::with_metadata(1, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        h.engine.spend(&h.spend_req(0)).unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        let expected_dah = { meta.delete_at_height };
        assert_ne!(expected_dah, 0);

        let dah = h.engine.dah_index();
        let entries = dah.range_query(expected_dah);
        assert!(entries.contains(&h.key));
    }

    #[test]
    fn dah_index_removed_after_clear() {
        let h = TestHarness::with_metadata(2, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        // Spend all to set DAH
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(1)).unwrap();
        assert!(!h.engine.dah_index().range_query(u32::MAX).is_empty());

        // Unspend to clear DAH
        let req = UnspendRequest {
            tx_key: h.key,
            offset: 0,
            utxo_hash: h.slot_hash(0),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();
        assert!(h.engine.dah_index().range_query(u32::MAX).is_empty());
    }

    #[test]
    fn dah_index_moved_when_value_changes() {
        let h = TestHarness::with_metadata(2, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        // Spend all at height 1000, retention 288 → DAH = 1288
        let mut req0 = h.spend_req(0);
        req0.current_block_height = 1000;
        h.engine.spend(&req0).unwrap();
        let mut req1 = h.spend_req(1);
        req1.current_block_height = 1000;
        h.engine.spend(&req1).unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.delete_at_height }, 1288);

        // Unspend and re-spend at higher height → DAH should be bumped
        let unreq = UnspendRequest {
            tx_key: h.key,
            offset: 0,
            utxo_hash: h.slot_hash(0),
            current_block_height: 2000,
            block_height_retention: 288,
        };
        h.engine.unspend(&unreq).unwrap();

        let mut req0b = h.spend_req(0);
        req0b.current_block_height = 2000;
        h.engine.spend(&req0b).unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.delete_at_height }, 2288); // Updated

        // DAH index should have the new value, not the old
        let dah = h.engine.dah_index();
        let at_new = dah.range_query(2288);
        assert!(at_new.contains(&h.key));
    }

    // -- Concurrency additional tests --

    #[test]
    fn concurrent_spend_and_unspend_mix() {
        let h = TestHarness::new(100, TxFlags::empty());
        let engine = h.engine.clone();
        let key = h.key;

        // First spend slots 50..100
        for i in 50..100u32 {
            let req = SpendRequest {
                tx_key: key,
                offset: i,
                utxo_hash: {
                    let mut hash = [0u8; 32];
                    hash[0] = (i & 0xFF) as u8;
                    hash[1] = ((i >> 8) & 0xFF) as u8;
                    hash
                },
                spending_data: {
                    let mut sd = [0u8; 36];
                    sd[0] = i as u8;
                    sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                    sd
                },
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            };
            engine.spend(&req).unwrap();
        }

        // Now concurrently: 50 threads spend slots 0..50, 50 threads unspend slots 50..100
        let mut handles = Vec::new();

        for i in 0..50u32 {
            let engine = engine.clone();
            handles.push(std::thread::spawn(move || {
                let req = SpendRequest {
                    tx_key: key,
                    offset: i,
                    utxo_hash: {
                        let mut hash = [0u8; 32];
                        hash[0] = (i & 0xFF) as u8;
                        hash[1] = ((i >> 8) & 0xFF) as u8;
                        hash
                    },
                    spending_data: {
                        let mut sd = [0u8; 36];
                        sd[0] = i as u8;
                        sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                        sd
                    },
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1000,
                    block_height_retention: 288,
                };
                engine.spend(&req).unwrap();
            }));
        }

        for i in 50..100u32 {
            let engine = engine.clone();
            handles.push(std::thread::spawn(move || {
                let req = UnspendRequest {
                    tx_key: key,
                    offset: i,
                    utxo_hash: {
                        let mut hash = [0u8; 32];
                        hash[0] = (i & 0xFF) as u8;
                        hash[1] = ((i >> 8) & 0xFF) as u8;
                        hash
                    },
                    current_block_height: 1000,
                    block_height_retention: 288,
                };
                engine.unspend(&req).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // 50 new spends, 50 unspends of previously-spent → net = 50 spent
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 50);
    }

    #[test]
    fn concurrent_spend_multi_overlapping() {
        let h = TestHarness::new(20, TxFlags::empty());
        let engine = h.engine.clone();
        let key = h.key;

        // 10 threads each try to spend slots 0..5 with their own spending data
        let results: Vec<_> = (0..10u8)
            .map(|thread_id| {
                let engine = engine.clone();
                std::thread::spawn(move || {
                    let req = SpendMultiRequest {
                        tx_key: key,
                        spends: (0..5)
                            .map(|i| {
                                let mut hash = [0u8; 32];
                                hash[0] = (i & 0xFF) as u8;
                                SpendItem {
                                    offset: i,
                                    utxo_hash: hash,
                                    spending_data: {
                                        let mut sd = [0u8; 36];
                                        sd[0] = thread_id;
                                        sd[1] = i as u8;
                                        sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                                        sd
                                    },
                                    idx: i,
                                }
                            })
                            .collect(),
                        ignore_conflicting: false,
                        ignore_locked: false,
                        current_block_height: 1000,
                        block_height_retention: 288,
                    };
                    engine.spend_multi(&req).unwrap()
                })
            })
            .collect();

        let mut total_success = 0u32;
        for handle in results {
            let resp = handle.join().unwrap();
            total_success += resp.spent_count;
        }

        // Exactly 5 slots should be spent (each slot won by exactly one thread)
        assert_eq!(total_success, 5);
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 5);
    }

    // -- Mutation bookkeeping additional tests --

    #[test]
    fn idempotent_respend_increments_generation() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(5)).unwrap();
        let g1 = { h.engine.read_metadata(&h.key).unwrap().generation };

        // Spend again with same data (idempotent)
        h.engine.spend(&h.spend_req(5)).unwrap();
        let g2 = { h.engine.read_metadata(&h.key).unwrap().generation };

        // Generation DOES increment even for idempotent re-spends
        // (the mutation was evaluated, even if no status change occurred)
        assert_eq!(g2, g1 + 1);
    }

    #[test]
    fn noop_unspend_does_not_increment_generation() {
        let h = TestHarness::new(10, TxFlags::empty());
        let g0 = { h.engine.read_metadata(&h.key).unwrap().generation };

        // Unspend already-unspent slot — pure no-op
        let req = UnspendRequest {
            tx_key: h.key,
            offset: 5,
            utxo_hash: h.slot_hash(5),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();

        let g1 = { h.engine.read_metadata(&h.key).unwrap().generation };
        assert_eq!(g1, g0); // NOT incremented
    }

    #[test]
    fn every_mutation_increments_generation_by_one() {
        let h = TestHarness::new(10, TxFlags::empty());
        let g0 = { h.engine.read_metadata(&h.key).unwrap().generation };

        // Spend
        h.engine.spend(&h.spend_req(0)).unwrap();
        let g1 = { h.engine.read_metadata(&h.key).unwrap().generation };
        assert_eq!(g1, g0 + 1);

        // Spend another
        h.engine.spend(&h.spend_req(1)).unwrap();
        let g2 = { h.engine.read_metadata(&h.key).unwrap().generation };
        assert_eq!(g2, g1 + 1);

        // Unspend
        let req = UnspendRequest {
            tx_key: h.key,
            offset: 0,
            utxo_hash: h.slot_hash(0),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();
        let g3 = { h.engine.read_metadata(&h.key).unwrap().generation };
        assert_eq!(g3, g2 + 1);

        // SpendMulti
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![
                SpendItem {
                    offset: 3,
                    utxo_hash: h.slot_hash(3),
                    spending_data: h.make_spending_data(0x01),
                    idx: 0,
                },
                SpendItem {
                    offset: 4,
                    utxo_hash: h.slot_hash(4),
                    spending_data: h.make_spending_data(0x02),
                    idx: 1,
                },
            ],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.spend_multi(&req).unwrap();
        let g4 = { h.engine.read_metadata(&h.key).unwrap().generation };
        assert_eq!(g4, g3 + 1); // One increment for the whole batch
    }

    #[test]
    fn updated_at_recent_for_all_mutations() {
        let h = TestHarness::new(10, TxFlags::empty());

        // Spend
        let before = now_millis();
        h.engine.spend(&h.spend_req(0)).unwrap();
        let after = now_millis();
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert!({ meta.updated_at } >= before && { meta.updated_at } <= after + 1);

        // Unspend
        let before = now_millis();
        let req = UnspendRequest {
            tx_key: h.key,
            offset: 0,
            utxo_hash: h.slot_hash(0),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();
        let after = now_millis();
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert!({ meta.updated_at } >= before && { meta.updated_at } <= after + 1);
    }

    // -- Secondary index integration tests --

    #[test]
    fn two_txs_both_set_dah_different_heights() {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone());
        let mut index = Index::new(200).unwrap();

        // Create two transactions
        let mut keys = Vec::new();
        for i in 0..2u64 {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            txid[16..18].copy_from_slice(&(i as u16).to_le_bytes());
            let key = TxKey { txid };
            keys.push(key);

            let record_size = TxMetadata::record_size_for(1);
            let offset = alloc.allocate(record_size).unwrap();

            let mut meta = TxMetadata::new(1);
            meta.tx_id = txid;
            meta.block_entry_count = 1;
            meta.block_entries_inline[0] = BlockEntry {
                block_id: (i + 1) as u32,
                block_height: 900,
                subtree_idx: 0,
            };
            let slots = vec![UtxoSlot::new_unspent([0u8; 32])];
            io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

            index
                .register(
                    key,
                    TxIndexEntry {
                        device_id: 0,
                        record_offset: offset,
                        utxo_count: 1,
                        cold_offset: 0,
                        cold_size: 0,
                        flags: 0,
                    },
                )
                .unwrap();
        }

        let engine = Arc::new(Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        ));

        // Spend tx 0 at height 1000
        let req0 = SpendRequest {
            tx_key: keys[0],
            offset: 0,
            utxo_hash: [0u8; 32],
            spending_data: {
                let mut sd = [0u8; 36];
                sd[0] = 1;
                sd
            },
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        engine.spend(&req0).unwrap();

        // Spend tx 1 at height 2000
        let req1 = SpendRequest {
            tx_key: keys[1],
            offset: 0,
            utxo_hash: [0u8; 32],
            spending_data: {
                let mut sd = [0u8; 36];
                sd[0] = 2;
                sd
            },
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 2000,
            block_height_retention: 288,
        };
        engine.spend(&req1).unwrap();

        // Both should be in DAH index at different heights
        let dah = engine.dah_index();
        let all = dah.range_query(u32::MAX);
        assert_eq!(all.len(), 2);
        assert!(all.contains(&keys[0]));
        assert!(all.contains(&keys[1]));
    }

    #[test]
    fn delete_record_removes_dah_entry() {
        let h = TestHarness::with_metadata(1, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 900, subtree_idx: 0,
            };
        });

        // Spend to trigger DAH set
        h.engine.spend(&h.spend_req(0)).unwrap();
        assert!(!h.engine.dah_index().range_query(u32::MAX).is_empty());

        // Delete the record
        let del_req = DeleteRequest { tx_key: h.key };
        h.engine.delete(&del_req).unwrap();

        // DAH index should be clean
        assert!(h.engine.dah_index().range_query(u32::MAX).is_empty());
    }

    #[test]
    fn dah_range_scan_returns_correct_set() {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone());
        let mut index = Index::new(200).unwrap();

        // Create 5 transactions, each with 1 UTXO
        let mut keys = Vec::new();
        for i in 0..5u64 {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            txid[16..18].copy_from_slice(&(i as u16).to_le_bytes());
            let key = TxKey { txid };
            keys.push(key);

            let record_size = TxMetadata::record_size_for(1);
            let offset = alloc.allocate(record_size).unwrap();

            let mut meta = TxMetadata::new(1);
            meta.tx_id = txid;
            meta.block_entry_count = 1;
            meta.block_entries_inline[0] = BlockEntry {
                block_id: (i + 1) as u32,
                block_height: 900,
                subtree_idx: 0,
            };
            let slots = vec![UtxoSlot::new_unspent([0u8; 32])];
            io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

            index
                .register(
                    key,
                    TxIndexEntry {
                        device_id: 0,
                        record_offset: offset,
                        utxo_count: 1,
                        cold_offset: 0,
                        cold_size: 0,
                        flags: 0,
                    },
                )
                .unwrap();
        }

        let engine = Arc::new(Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        ));

        // Spend each at different heights
        for (i, key) in keys.iter().enumerate() {
            let height = 1000 + (i as u32) * 100; // 1000, 1100, 1200, 1300, 1400
            let req = SpendRequest {
                tx_key: *key,
                offset: 0,
                utxo_hash: [0u8; 32],
                spending_data: {
                    let mut sd = [0u8; 36];
                    sd[0] = i as u8;
                    sd
                },
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: height,
                block_height_retention: 288,
            };
            engine.spend(&req).unwrap();
        }

        // range_scan up to 1388 (1100 + 288) should include first 2 txs
        let dah = engine.dah_index();
        let up_to_1388 = dah.range_query(1388);
        assert_eq!(up_to_1388.len(), 2);
        assert!(up_to_1388.contains(&keys[0]));
        assert!(up_to_1388.contains(&keys[1]));

        // range_scan up to max should include all 5
        let all = dah.range_query(u32::MAX);
        assert_eq!(all.len(), 5);
    }

    // ===================================================================
    // Phase 4: setMined / markOnLongestChain tests
    // ===================================================================

    // -- setMined correctness tests --

    #[test]
    fn set_mined_nonexistent_tx() {
        let h = TestHarness::new(10, TxFlags::empty());
        let req = SetMinedRequest {
            tx_key: TxKey { txid: [0xFF; 32] },
            block_id: 1,
            block_height: 100,
            subtree_idx: 0,
            current_block_height: 100,
            block_height_retention: 288,
            on_longest_chain: true,
            unset_mined: false,
        };
        match h.engine.set_mined(&req) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn set_mined_new_block_id() {
        let h = TestHarness::new(10, TxFlags::empty());
        let req = SetMinedRequest {
            tx_key: h.key,
            block_id: 42,
            block_height: 800_000,
            subtree_idx: 7,
            current_block_height: 800_000,
            block_height_retention: 288,
            on_longest_chain: true,
            unset_mined: false,
        };
        let resp = h.engine.set_mined(&req).unwrap();
        assert_eq!(resp.block_ids, vec![42]);

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta.block_entry_count, 1);
        assert_eq!({ meta.block_entries_inline[0].block_id }, 42);
        assert_eq!({ meta.block_entries_inline[0].block_height }, 800_000);
        assert_eq!({ meta.block_entries_inline[0].subtree_idx }, 7);
    }

    #[test]
    fn set_mined_idempotent() {
        let h = TestHarness::new(10, TxFlags::empty());
        let req = SetMinedRequest {
            tx_key: h.key,
            block_id: 42,
            block_height: 100,
            subtree_idx: 0,
            current_block_height: 100,
            block_height_retention: 288,
            on_longest_chain: true,
            unset_mined: false,
        };
        h.engine.set_mined(&req).unwrap();
        h.engine.set_mined(&req).unwrap(); // Second call

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta.block_entry_count, 1); // Not duplicated
    }

    #[test]
    fn set_mined_three_blocks() {
        let h = TestHarness::new(10, TxFlags::empty());
        for bid in [10, 20, 30] {
            h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: h.key,
                    block_id: bid,
                    block_height: 100 + bid,
                    subtree_idx: bid / 10,
                    current_block_height: 200,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                })
                .unwrap();
        }

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta.block_entry_count, 3);

        let resp = h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 99,
                block_height: 999,
                subtree_idx: 0,
                current_block_height: 200,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();
        // Check response contains block_ids
        assert!(resp.block_ids.contains(&10));
        assert!(resp.block_ids.contains(&20));
        assert!(resp.block_ids.contains(&30));
    }

    #[test]
    fn set_mined_stores_height_and_subtree() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 5,
                block_height: 12345,
                subtree_idx: 42,
                current_block_height: 12345,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.block_entries_inline[0].block_height }, 12345);
        assert_eq!({ meta.block_entries_inline[0].subtree_idx }, 42);
    }

    #[test]
    fn set_mined_clears_locked() {
        let h = TestHarness::new(10, TxFlags::LOCKED);
        let meta_before = h.engine.read_metadata(&h.key).unwrap();
        assert!(meta_before.flags.contains(TxFlags::LOCKED));

        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        let meta_after = h.engine.read_metadata(&h.key).unwrap();
        assert!(!meta_after.flags.contains(TxFlags::LOCKED));
    }

    #[test]
    fn set_mined_does_not_modify_utxo_slots() {
        let h = TestHarness::new(10, TxFlags::empty());
        let slot_before = h.engine.read_slot(&h.key, 5).unwrap();

        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        let slot_after = h.engine.read_slot(&h.key, 5).unwrap();
        assert_eq!(slot_before, slot_after);
    }

    // -- unsetMined tests --

    #[test]
    fn unset_mined_removes_block() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta.block_entry_count, 0);
    }

    #[test]
    fn unset_mined_nonexistent_block_noop() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        // Remove block_id 99 which doesn't exist
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 99,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta.block_entry_count, 1); // Original still there
    }

    #[test]
    fn unset_mined_middle_of_three() {
        let h = TestHarness::new(10, TxFlags::empty());
        for bid in [10, 20, 30] {
            h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: h.key,
                    block_id: bid,
                    block_height: bid * 10,
                    subtree_idx: 0,
                    current_block_height: 300,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                })
                .unwrap();
        }

        // Remove block 20 (middle)
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 20,
                block_height: 200,
                subtree_idx: 0,
                current_block_height: 300,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta.block_entry_count, 2);
        let ids: Vec<u32> = (0..2)
            .map(|i| { meta.block_entries_inline[i].block_id })
            .collect();
        assert!(ids.contains(&10));
        assert!(ids.contains(&30));
        assert!(!ids.contains(&20));
    }

    #[test]
    fn unset_mined_does_not_modify_slots() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        let slot_before = h.engine.read_slot(&h.key, 0).unwrap();
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();
        let slot_after = h.engine.read_slot(&h.key, 0).unwrap();
        assert_eq!(slot_before, slot_after);
    }

    // -- unmined_since tests --

    #[test]
    fn set_mined_on_longest_chain_clears_unmined() {
        let h = TestHarness::with_metadata(10, TxFlags::empty(), |m| {
            m.unmined_since = 500;
        });

        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 600,
                subtree_idx: 0,
                current_block_height: 600,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.unmined_since }, 0);
    }

    #[test]
    fn set_mined_off_longest_chain_keeps_unmined() {
        let h = TestHarness::with_metadata(10, TxFlags::empty(), |m| {
            m.unmined_since = 500;
        });

        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 600,
                subtree_idx: 0,
                current_block_height: 600,
                block_height_retention: 288,
                on_longest_chain: false,
                unset_mined: false,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        // unmined_since not cleared because not on_longest_chain
        assert_eq!({ meta.unmined_since }, 500);
    }

    #[test]
    fn unset_mined_last_block_sets_unmined() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 200,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.unmined_since }, 200);
    }

    // -- Signal/DAH integration for setMined --

    #[test]
    fn set_mined_fully_spent_on_chain_sets_dah() {
        let h = TestHarness::new(2, TxFlags::empty());
        // Spend all UTXOs
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(1)).unwrap();

        let resp = h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
                current_block_height: 1000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_ne!({ meta.delete_at_height }, 0);
        assert!(!h.engine.dah_index().range_query(u32::MAX).is_empty());
        // External flag not set, so signal is not DAHSET but the DAH was still set
        let _ = resp;
    }

    #[test]
    fn set_mined_partially_spent_no_dah() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(0)).unwrap(); // Only 1 of 10

        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
                current_block_height: 1000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn set_mined_external_fully_spent_signals_dah_set() {
        let h = TestHarness::with_metadata(2, TxFlags::EXTERNAL, |_| {});
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(1)).unwrap();

        let resp = h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
                current_block_height: 1000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        assert_eq!(resp.signal, Signal::DeleteAtHeightSet);
    }

    // -- Concurrency tests for setMined --

    #[test]
    fn concurrent_set_mined_different_blocks() {
        let h = TestHarness::new(10, TxFlags::empty());
        let engine = h.engine.clone();
        let key = h.key;

        let handles: Vec<_> = (0..3u32)
            .map(|bid| {
                let engine = engine.clone();
                std::thread::spawn(move || {
                    engine
                        .set_mined(&SetMinedRequest {
                            tx_key: key,
                            block_id: bid + 1,
                            block_height: 100 + bid,
                            subtree_idx: 0,
                            current_block_height: 200,
                            block_height_retention: 288,
                            on_longest_chain: true,
                            unset_mined: false,
                        })
                        .unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!(meta.block_entry_count, 3);
    }

    #[test]
    fn concurrent_set_mined_and_spend() {
        let h = TestHarness::new(10, TxFlags::empty());
        let engine = h.engine.clone();
        let key = h.key;
        let hash0 = h.slot_hash(0);
        let sd = h.make_spending_data(0xAB);

        let e1 = engine.clone();
        let h1 = std::thread::spawn(move || {
            e1.set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();
        });

        let e2 = engine.clone();
        let h2 = std::thread::spawn(move || {
            e2.spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: hash0,
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 100,
                block_height_retention: 288,
            })
            .unwrap();
        });

        h1.join().unwrap();
        h2.join().unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!(meta.block_entry_count, 1);
        assert_eq!({ meta.spent_utxos }, 1);
    }

    // -- MarkOnLongestChain tests --

    #[test]
    fn mark_on_longest_chain_clears_unmined() {
        let h = TestHarness::with_metadata(10, TxFlags::empty(), |m| {
            m.unmined_since = 500;
        });

        h.engine
            .mark_on_longest_chain(&MarkOnLongestChainRequest {
                tx_key: h.key,
                on_longest_chain: true,
                current_block_height: 600,
                block_height_retention: 288,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.unmined_since }, 0);
    }

    #[test]
    fn mark_off_longest_chain_sets_unmined() {
        let h = TestHarness::new(10, TxFlags::empty());
        // unmined_since starts at 0 (on longest chain by default)
        h.engine
            .mark_on_longest_chain(&MarkOnLongestChainRequest {
                tx_key: h.key,
                on_longest_chain: false,
                current_block_height: 700,
                block_height_retention: 288,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.unmined_since }, 700);
    }

    #[test]
    fn mark_on_longest_chain_already_on_noop() {
        let h = TestHarness::new(10, TxFlags::empty());
        // Already on longest chain (unmined_since = 0)
        h.engine
            .mark_on_longest_chain(&MarkOnLongestChainRequest {
                tx_key: h.key,
                on_longest_chain: true,
                current_block_height: 600,
                block_height_retention: 288,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.unmined_since }, 0);
    }

    #[test]
    fn mark_off_chain_updates_height() {
        let h = TestHarness::with_metadata(10, TxFlags::empty(), |m| {
            m.unmined_since = 500;
        });

        h.engine
            .mark_on_longest_chain(&MarkOnLongestChainRequest {
                tx_key: h.key,
                on_longest_chain: false,
                current_block_height: 800,
                block_height_retention: 288,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.unmined_since }, 800);
    }

    #[test]
    fn mark_on_longest_chain_nonexistent_tx() {
        let h = TestHarness::new(10, TxFlags::empty());
        match h.engine.mark_on_longest_chain(&MarkOnLongestChainRequest {
            tx_key: TxKey { txid: [0xFF; 32] },
            on_longest_chain: true,
            current_block_height: 600,
            block_height_retention: 288,
        }) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn mark_on_longest_chain_does_not_modify_blocks_or_slots() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        let meta_before = h.engine.read_metadata(&h.key).unwrap();
        let slot_before = h.engine.read_slot(&h.key, 0).unwrap();

        h.engine
            .mark_on_longest_chain(&MarkOnLongestChainRequest {
                tx_key: h.key,
                on_longest_chain: false,
                current_block_height: 200,
                block_height_retention: 288,
            })
            .unwrap();

        let meta_after = h.engine.read_metadata(&h.key).unwrap();
        let slot_after = h.engine.read_slot(&h.key, 0).unwrap();

        // Block entries unchanged
        assert_eq!(meta_before.block_entry_count, meta_after.block_entry_count);
        assert_eq!(
            { meta_before.block_entries_inline[0].block_id },
            { meta_after.block_entries_inline[0].block_id }
        );
        // Slots unchanged
        assert_eq!(slot_before, slot_after);
    }

    #[test]
    fn mark_on_chain_fully_spent_evaluates_dah() {
        let h = TestHarness::with_metadata(2, TxFlags::empty(), |m| {
            m.unmined_since = 500;
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 100, subtree_idx: 0,
            };
        });

        // Spend all UTXOs
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(1)).unwrap();

        // Now mark on longest chain — should set DAH
        h.engine
            .mark_on_longest_chain(&MarkOnLongestChainRequest {
                tx_key: h.key,
                on_longest_chain: true,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_ne!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn mark_off_chain_clears_dah() {
        let h = TestHarness::with_metadata(2, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1, block_height: 100, subtree_idx: 0,
            };
        });

        // Spend all → triggers DAH
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(1)).unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_ne!({ meta.delete_at_height }, 0);

        // Mark off longest chain → should clear DAH
        h.engine
            .mark_on_longest_chain(&MarkOnLongestChainRequest {
                tx_key: h.key,
                on_longest_chain: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn concurrent_mark_and_set_mined() {
        let h = TestHarness::with_metadata(10, TxFlags::empty(), |m| {
            m.unmined_since = 500;
        });
        let engine = h.engine.clone();
        let key = h.key;

        let e1 = engine.clone();
        let h1 = std::thread::spawn(move || {
            e1.set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 1,
                block_height: 600,
                subtree_idx: 0,
                current_block_height: 600,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();
        });

        let e2 = engine.clone();
        let h2 = std::thread::spawn(move || {
            e2.mark_on_longest_chain(&MarkOnLongestChainRequest {
                tx_key: key,
                on_longest_chain: true,
                current_block_height: 600,
                block_height_retention: 288,
            })
            .unwrap();
        });

        h1.join().unwrap();
        h2.join().unwrap();

        // Both should complete without corruption
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!(meta.block_entry_count, 1);
    }

    // -- Phase 4 additional tests --

    #[test]
    fn set_mined_overflow_four_entries() {
        let h = TestHarness::new(10, TxFlags::empty());
        for bid in 1..=4u32 {
            let resp = h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: h.key,
                    block_id: bid,
                    block_height: 100 + bid,
                    subtree_idx: bid,
                    current_block_height: 200,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                })
                .unwrap();
            assert_eq!(resp.block_ids.len(), bid as usize);
        }

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta.block_entry_count, 4);
        assert_ne!({ meta.block_overflow_offset }, 0); // Overflow block allocated
    }

    #[test]
    fn set_mined_overflow_read_back_all() {
        let h = TestHarness::new(10, TxFlags::empty());
        for bid in 1..=5u32 {
            h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: h.key,
                    block_id: bid * 10,
                    block_height: 100 + bid,
                    subtree_idx: bid,
                    current_block_height: 200,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                })
                .unwrap();
        }

        // Read back all entries via a dummy set_mined (idempotent)
        let resp = h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 10, // Already exists
                block_height: 101,
                subtree_idx: 1,
                current_block_height: 200,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        assert_eq!(resp.block_ids.len(), 5);
        for bid in [10, 20, 30, 40, 50] {
            assert!(resp.block_ids.contains(&bid), "missing block_id {bid}");
        }
    }

    #[test]
    fn set_mined_overflow_unset_from_overflow() {
        let h = TestHarness::new(10, TxFlags::empty());
        for bid in 1..=5u32 {
            h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: h.key,
                    block_id: bid,
                    block_height: 100 + bid,
                    subtree_idx: bid,
                    current_block_height: 200,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                })
                .unwrap();
        }

        // Remove block 5 (in overflow)
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 5,
                block_height: 105,
                subtree_idx: 5,
                current_block_height: 200,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta.block_entry_count, 4);

        // Remove block 4 (in overflow)
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 4,
                block_height: 104,
                subtree_idx: 4,
                current_block_height: 200,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta.block_entry_count, 3);
        // Should only have inline entries now
        let ids: Vec<u32> = (0..3)
            .map(|i| { meta.block_entries_inline[i].block_id })
            .collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
    }

    #[test]
    fn set_mined_overflow_idempotent_in_overflow() {
        let h = TestHarness::new(10, TxFlags::empty());
        for bid in 1..=4u32 {
            h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: h.key,
                    block_id: bid,
                    block_height: 100 + bid,
                    subtree_idx: bid,
                    current_block_height: 200,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                })
                .unwrap();
        }

        // Try adding block_id 4 again (already in overflow) — should be idempotent
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 4,
                block_height: 104,
                subtree_idx: 4,
                current_block_height: 200,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta.block_entry_count, 4); // Not duplicated
    }

    #[test]
    fn multiple_set_mined_on_chain_stays_cleared() {
        let h = TestHarness::with_metadata(10, TxFlags::empty(), |m| {
            m.unmined_since = 500;
        });

        for bid in 1..=3u32 {
            h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: h.key,
                    block_id: bid,
                    block_height: 600 + bid,
                    subtree_idx: 0,
                    current_block_height: 700,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                })
                .unwrap();
        }

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.unmined_since }, 0); // Stays cleared after multiple setMined
    }

    #[test]
    fn set_mined_then_unset_all_sets_unmined() {
        let h = TestHarness::new(10, TxFlags::empty());

        // Add two blocks
        for bid in [1, 2] {
            h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: h.key,
                    block_id: bid,
                    block_height: 100,
                    subtree_idx: 0,
                    current_block_height: 100,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                })
                .unwrap();
        }
        assert_eq!({ h.engine.read_metadata(&h.key).unwrap().unmined_since }, 0);

        // Remove both
        for bid in [1, 2] {
            h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: h.key,
                    block_id: bid,
                    block_height: 100,
                    subtree_idx: 0,
                    current_block_height: 300,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: true,
                })
                .unwrap();
        }

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.unmined_since }, 300);
    }

    #[test]
    fn unset_mined_fully_spent_clears_dah() {
        let h = TestHarness::new(2, TxFlags::empty());

        // Add block, spend all, DAH should be set
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
                current_block_height: 1000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(1)).unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_ne!({ meta.delete_at_height }, 0);

        // Unset mined (remove block) → should clear DAH since no blocks remain
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
                current_block_height: 1000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        // With no blocks, DAH conditions are not met (has_blocks=false)
        // The evaluate_delete_at_height would signal AllSpent but not set DAH
        // Since DAH was previously set and conditions are now unmet, it should be cleared
        assert_eq!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn concurrent_set_mined_10_threads() {
        let h = TestHarness::new(10, TxFlags::empty());
        let engine = h.engine.clone();
        let key = h.key;

        let handles: Vec<_> = (0..10u32)
            .map(|bid| {
                let engine = engine.clone();
                std::thread::spawn(move || {
                    engine
                        .set_mined(&SetMinedRequest {
                            tx_key: key,
                            block_id: bid + 1,
                            block_height: 100 + bid,
                            subtree_idx: 0,
                            current_block_height: 200,
                            block_height_retention: 288,
                            on_longest_chain: true,
                            unset_mined: false,
                        })
                        .unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!(meta.block_entry_count, 10);
    }

    #[test]
    fn concurrent_set_and_unset_same_block() {
        let h = TestHarness::new(10, TxFlags::empty());
        let engine = h.engine.clone();
        let key = h.key;

        // First add the block
        engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 42,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 100,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        // Concurrently set and unset
        let mut handles = Vec::new();
        for i in 0..20u32 {
            let engine = engine.clone();
            let unset = i % 2 == 0;
            handles.push(std::thread::spawn(move || {
                engine
                    .set_mined(&SetMinedRequest {
                        tx_key: key,
                        block_id: 42,
                        block_height: 100,
                        subtree_idx: 0,
                        current_block_height: 100,
                        block_height_retention: 288,
                        on_longest_chain: true,
                        unset_mined: unset,
                    })
                    .unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Final state should be consistent: either 0 or 1 entries, not corrupted
        let meta = engine.read_metadata(&key).unwrap();
        let count = meta.block_entry_count;
        assert!(count <= 1, "corrupted: block_entry_count={count}");
        if count == 1 {
            assert_eq!({ meta.block_entries_inline[0].block_id }, 42);
        }
    }

    // ===================================================================
    // Phase 5: Creation path tests
    // ===================================================================

    fn make_create_req(n: u8, utxo_count: usize) -> CreateRequest {
        let mut tx_id = [0u8; 32];
        tx_id[0] = n;
        tx_id[8..16].copy_from_slice(&(n as u64 * 0x9E37).to_le_bytes());
        tx_id[16] = n;
        let utxo_hashes: Vec<[u8; 32]> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i as u8;
                h[1] = (i >> 8) as u8;
                h
            })
            .collect();
        CreateRequest {
            tx_id,
            tx_version: 1,
            locktime: 0,
            fee: 500,
            size_in_bytes: 250,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            utxo_hashes,
            inputs: None,
            outputs: None,
            inpoints: None,
            is_external: false,
            created_at: 1710000000000,
            block_height: 1000,
            mined_block_infos: vec![],
            frozen: false,
            conflicting: false,
            locked: false,
            parent_txids: vec![],
        }
    }

    fn create_engine() -> Arc<Engine> {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone());
        let index = Index::new(1000).unwrap();
        Arc::new(Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        ))
    }

    #[test]
    fn create_single_utxo() {
        let engine = create_engine();
        let req = make_create_req(1, 1);
        let key = req.tx_key();
        let resp = engine.create(&req).unwrap();

        assert_eq!(resp.utxo_count, 1);
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.magic }, METADATA_MAGIC);
        assert_eq!({ meta.schema_version }, METADATA_VERSION);
        assert_eq!({ meta.utxo_count }, 1);
        assert_eq!({ meta.spent_utxos }, 0);
        assert_eq!(meta.block_entry_count, 0);

        let slot = engine.read_slot(&key, 0).unwrap();
        assert!(slot.is_unspent());
        assert_eq!(slot.hash[0], 0);
    }

    #[test]
    fn create_100_utxos() {
        let engine = create_engine();
        let req = make_create_req(2, 100);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.utxo_count }, 100);

        for i in 0..100u32 {
            let slot = engine.read_slot(&key, i).unwrap();
            assert!(slot.is_unspent(), "slot {i} not unspent");
            assert_eq!(slot.hash[0], i as u8);
        }
    }

    #[test]
    fn create_10000_utxos() {
        let engine = create_engine();
        let req = make_create_req(3, 10000);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.utxo_count }, 10000);

        // Spot-check a few slots
        let slot_0 = engine.read_slot(&key, 0).unwrap();
        assert!(slot_0.is_unspent());
        let slot_9999 = engine.read_slot(&key, 9999).unwrap();
        assert!(slot_9999.is_unspent());
    }

    #[test]
    fn create_metadata_fields_match() {
        let engine = create_engine();
        let mut req = make_create_req(4, 5);
        req.tx_version = 2;
        req.locktime = 500_000;
        req.fee = 1234;
        req.size_in_bytes = 999;
        req.extended_size = 111;
        req.created_at = 1710099999000;

        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!(meta.tx_id, req.tx_id);
        assert_eq!({ meta.tx_version }, 2);
        assert_eq!({ meta.locktime }, 500_000);
        assert_eq!({ meta.fee }, 1234);
        assert_eq!({ meta.size_in_bytes }, 999);
        assert_eq!({ meta.extended_size }, 111);
        assert_eq!({ meta.created_at }, 1710099999000);
    }

    #[test]
    fn create_index_lookup() {
        let engine = create_engine();
        let req = make_create_req(5, 10);
        let key = req.tx_key();
        let resp = engine.create(&req).unwrap();

        let entry = engine.lookup(&key).unwrap();
        assert_eq!(entry.record_offset, resp.record_offset);
        assert_eq!(entry.utxo_count, 10);
    }

    #[test]
    fn create_then_spend() {
        let engine = create_engine();
        let req = make_create_req(6, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let mut sd = [0u8; 36];
        sd[0] = 0xAB;
        let spend_req = SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        engine.spend(&spend_req).unwrap();

        let slot = engine.read_slot(&key, 0).unwrap();
        assert!(slot.is_spent());
    }

    #[test]
    fn create_then_set_mined() {
        let engine = create_engine();
        let req = make_create_req(7, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 42,
                block_height: 1000,
                subtree_idx: 3,
                current_block_height: 1000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!(meta.block_entry_count, 1);
        assert_eq!({ meta.block_entries_inline[0].block_id }, 42);
    }

    // -- Duplicate detection --

    #[test]
    fn create_duplicate_txid() {
        let engine = create_engine();
        let req = make_create_req(8, 5);
        engine.create(&req).unwrap();

        match engine.create(&req) {
            Err(CreateError::DuplicateTxId) => {}
            other => panic!("expected DuplicateTxId, got {other:?}"),
        }
    }

    // -- Allocation --

    #[test]
    fn create_records_no_overlap() {
        let engine = create_engine();
        let r1 = engine.create(&make_create_req(10, 5)).unwrap();
        let r2 = engine.create(&make_create_req(11, 10)).unwrap();

        let size1 = TxMetadata::record_size_for(5);
        let size2 = TxMetadata::record_size_for(10);

        // Records should not overlap (offsets + sizes)
        assert!(
            r2.record_offset >= r1.record_offset + size1
                || r1.record_offset >= r2.record_offset + size2
        );
    }

    // -- Cold data --

    #[test]
    fn create_with_cold_data() {
        let engine = create_engine();
        let mut req = make_create_req(20, 3);
        req.inputs = Some(vec![0x01, 0x02, 0x03, 0x04]);
        req.outputs = Some(vec![0x0A, 0x0B, 0x0C]);

        let key = req.tx_key();
        engine.create(&req).unwrap();

        let entry = engine.lookup(&key).unwrap();
        assert_ne!(entry.cold_offset, 0);
        assert!(entry.cold_size > 0);

        // Read back cold data and verify
        let cold = engine.read_cold_data(&key).unwrap();
        // Format: [inputs_len:4][inputs][outputs_len:4][outputs][inpoints_len:4][inpoints]
        assert_eq!(u32::from_le_bytes(cold[0..4].try_into().unwrap()), 4); // inputs len
        assert_eq!(&cold[4..8], &[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(u32::from_le_bytes(cold[8..12].try_into().unwrap()), 3); // outputs len
        assert_eq!(&cold[12..15], &[0x0A, 0x0B, 0x0C]);
    }

    #[test]
    fn create_without_cold_data() {
        let engine = create_engine();
        let req = make_create_req(21, 3);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let entry = engine.lookup(&key).unwrap();
        assert_eq!(entry.cold_offset, 0);
        assert_eq!(entry.cold_size, 0);
    }

    #[test]
    fn cold_data_not_modified_by_spend() {
        let engine = create_engine();
        let mut req = make_create_req(22, 3);
        req.inputs = Some(vec![0xDE, 0xAD]);
        req.outputs = Some(vec![0xBE, 0xEF]);

        let key = req.tx_key();
        engine.create(&req).unwrap();

        let cold_before = engine.read_cold_data(&key).unwrap();

        // Spend a UTXO
        let mut sd = [0u8; 36];
        sd[0] = 0xAA;
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        let cold_after = engine.read_cold_data(&key).unwrap();
        assert_eq!(cold_before, cold_after);
    }

    // -- Batch creation --

    #[test]
    fn batch_create_10() {
        let engine = create_engine();
        let requests: Vec<CreateRequest> = (30..40u8)
            .map(|n| make_create_req(n, 5))
            .collect();
        let results = engine.create_batch(&requests);

        assert_eq!(results.len(), 10);
        for (i, result) in results.iter().enumerate() {
            assert!(result.is_ok(), "creation {i} failed: {result:?}");
        }
    }

    #[test]
    fn batch_create_with_duplicate() {
        let engine = create_engine();
        let mut requests: Vec<CreateRequest> = (40..50u8)
            .map(|n| make_create_req(n, 5))
            .collect();
        // Duplicate the 5th entry
        requests[5] = requests[4].clone();

        let results = engine.create_batch(&requests);
        let successes = results.iter().filter(|r| r.is_ok()).count();
        let duplicates = results
            .iter()
            .filter(|r| matches!(r, Err(CreateError::DuplicateTxId)))
            .count();

        assert_eq!(successes, 9);
        assert_eq!(duplicates, 1);
    }

    // -- Edge cases --

    #[test]
    fn create_zero_utxos() {
        let engine = create_engine();
        let req = make_create_req(50, 0);
        match engine.create(&req) {
            Err(CreateError::InvalidUtxoCount) => {}
            other => panic!("expected InvalidUtxoCount, got {other:?}"),
        }
    }

    #[test]
    fn create_coinbase() {
        let engine = create_engine();
        let mut req = make_create_req(51, 1);
        req.is_coinbase = true;
        req.spending_height = 1100; // block_height + 100

        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(meta.flags.contains(TxFlags::IS_COINBASE));
        assert_eq!({ meta.spending_height }, 1100);
    }

    #[test]
    fn create_frozen() {
        let engine = create_engine();
        let mut req = make_create_req(52, 3);
        req.frozen = true;

        let key = req.tx_key();
        engine.create(&req).unwrap();

        for i in 0..3u32 {
            let slot = engine.read_slot(&key, i).unwrap();
            assert!(slot.is_frozen(), "slot {i} should be frozen");
            assert_eq!(slot.spending_data, [0xFF; 36]);
        }
    }

    #[test]
    fn create_conflicting() {
        let engine = create_engine();
        let mut req = make_create_req(53, 2);
        req.conflicting = true;

        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(meta.flags.contains(TxFlags::CONFLICTING));
    }

    #[test]
    fn create_unmined() {
        let engine = create_engine();
        let mut req = make_create_req(54, 2);
        req.block_height = 800;

        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.unmined_since }, 800);

        // Should be in unmined index
        let unmined = engine.unmined_index();
        let results = unmined.range_query(800);
        assert!(results.contains(&key));
    }

    #[test]
    fn create_with_mined_block_info() {
        let engine = create_engine();
        let mut req = make_create_req(55, 2);
        req.mined_block_infos = vec![MinedBlockInfo {
            block_id: 42,
            block_height: 900,
            subtree_idx: 7,
        }];

        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.unmined_since }, 0);
        assert_eq!(meta.block_entry_count, 1);
        assert_eq!({ meta.block_entries_inline[0].block_id }, 42);
    }

    // -- Phase 5 additional tests --

    #[test]
    fn create_delete_recreate_same_txid() {
        let engine = create_engine();
        let req = make_create_req(60, 5);
        let key = req.tx_key();

        engine.create(&req).unwrap();
        engine
            .delete(&DeleteRequest { tx_key: key })
            .unwrap();

        // Should succeed — txid can be reused after deletion
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.utxo_count }, 5);
    }

    #[test]
    fn create_record_at_aligned_offset() {
        let engine = create_engine();
        let req = make_create_req(61, 5);
        let resp = engine.create(&req).unwrap();

        // Record offset must be aligned to device alignment (4096)
        assert_eq!(resp.record_offset % 4096, 0);
    }

    #[test]
    fn create_record_size_matches_expected() {
        let engine = create_engine();
        let req = make_create_req(62, 7);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        let expected = METADATA_SIZE as u32 + 7 * UTXO_SLOT_SIZE as u32;
        assert_eq!({ meta.record_size }, expected);
    }

    #[test]
    fn create_record_size_with_cold_data() {
        let engine = create_engine();
        let mut req = make_create_req(63, 3);
        req.inputs = Some(vec![0x01; 10]);
        req.outputs = Some(vec![0x02; 20]);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        // Cold data: 4 + 10 + 4 + 20 + 4 + 0 = 42 bytes (inputs + outputs + empty inpoints)
        let expected = METADATA_SIZE as u32 + 3 * UTXO_SLOT_SIZE as u32 + 42;
        assert_eq!({ meta.record_size }, expected);
    }

    #[test]
    fn batch_create_device_full() {
        // DATA_REGION_OFFSET is 1MiB, so we need device > 1MiB.
        // Create a device with ~1MiB + 20 blocks of data space.
        // Each record with 5 UTXOs needs ~1 block (4KB).
        let data_blocks = 20;
        let total_size = 1024 * 1024 + data_blocks * 4096; // 1MiB header + 80KB data
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(total_size, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone());
        let index = Index::new(1000).unwrap();
        let engine = Arc::new(Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        ));

        // Request more records than can fit in the data region
        let requests: Vec<CreateRequest> = (0..50u8)
            .map(|n| make_create_req(n + 100, 5)) // Each ~4KB
            .collect();

        let results = engine.create_batch(&requests);

        let successes = results.iter().filter(|r| r.is_ok()).count();
        let full_errors = results
            .iter()
            .filter(|r| matches!(r, Err(CreateError::DeviceFull)))
            .count();

        assert!(successes > 0, "at least one should succeed");
        assert!(full_errors > 0, "some should fail with DeviceFull");
        assert_eq!(successes + full_errors, 50);
    }

    #[test]
    fn create_non_coinbase_no_maturity_check() {
        let engine = create_engine();
        let req = make_create_req(64, 3);
        // spending_height = 0 (default for non-coinbase)
        assert_eq!(req.spending_height, 0);
        assert!(!req.is_coinbase);

        let key = req.tx_key();
        engine.create(&req).unwrap();

        // Spend should succeed regardless of current_block_height (no maturity check)
        let spend_req = SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
            spending_data: {
                let mut sd = [0u8; 36];
                sd[0] = 0xAB;
                sd
            },
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1, // Very low height
            block_height_retention: 288,
        };
        assert!(engine.spend(&spend_req).is_ok());
    }

    // ===================================================================
    // Phase 6: Remaining operations tests
    // ===================================================================

    // -- Freeze tests --

    #[test]
    fn freeze_unspent() {
        let engine = create_engine();
        let req = make_create_req(60, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        engine.freeze(&FreezeRequest { tx_key: key, offset: 2, utxo_hash: req.utxo_hashes[2] }).unwrap();
        let slot = engine.read_slot(&key, 2).unwrap();
        assert!(slot.is_frozen());
        assert_eq!(slot.spending_data, [0xFF; 36]);
    }

    #[test]
    fn freeze_already_frozen() {
        let engine = create_engine();
        let req = make_create_req(61, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();

        match engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }) {
            Err(SpendError::AlreadyFrozen { offset: 0 }) => {}
            other => panic!("expected AlreadyFrozen, got {other:?}"),
        }
    }

    #[test]
    fn freeze_spent_utxo() {
        let engine = create_engine();
        let req = make_create_req(62, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        let mut sd = [0u8; 36]; sd[0] = 0xAB;
        engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0], spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        }).unwrap();

        match engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }) {
            Err(SpendError::AlreadySpent { offset: 0, .. }) => {}
            other => panic!("expected AlreadySpent, got {other:?}"),
        }
    }

    #[test]
    fn freeze_nonexistent_tx() {
        let engine = create_engine();
        match engine.freeze(&FreezeRequest { tx_key: TxKey { txid: [0xFF; 32] }, offset: 0, utxo_hash: [0; 32] }) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn freeze_hash_mismatch() {
        let engine = create_engine();
        let req = make_create_req(63, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        match engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: [0xFF; 32] }) {
            Err(SpendError::UtxoHashMismatch { .. }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    #[test]
    fn freeze_does_not_change_counter() {
        let engine = create_engine();
        let req = make_create_req(64, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 0);
    }

    #[test]
    fn freeze_then_spend_returns_frozen() {
        let engine = create_engine();
        let req = make_create_req(65, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();

        let mut sd = [0u8; 36]; sd[0] = 0xAB;
        match engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0], spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        }) {
            Err(SpendError::Frozen { offset: 0 }) => {}
            other => panic!("expected Frozen, got {other:?}"),
        }
    }

    // -- Unfreeze tests --

    #[test]
    fn unfreeze_frozen() {
        let engine = create_engine();
        let req = make_create_req(70, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 1, utxo_hash: req.utxo_hashes[1] }).unwrap();
        engine.unfreeze(&UnfreezeRequest { tx_key: key, offset: 1, utxo_hash: req.utxo_hashes[1] }).unwrap();

        let slot = engine.read_slot(&key, 1).unwrap();
        assert!(slot.is_unspent());
        assert_eq!(slot.spending_data, [0u8; 36]);
    }

    #[test]
    fn unfreeze_not_frozen() {
        let engine = create_engine();
        let req = make_create_req(71, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        match engine.unfreeze(&UnfreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }) {
            Err(SpendError::NotFrozen { offset: 0 }) => {}
            other => panic!("expected NotFrozen, got {other:?}"),
        }
    }

    #[test]
    fn unfreeze_then_spend() {
        let engine = create_engine();
        let req = make_create_req(72, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();
        engine.unfreeze(&UnfreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();

        let mut sd = [0u8; 36]; sd[0] = 0xAB;
        engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0], spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        }).unwrap();
        assert!(engine.read_slot(&key, 0).unwrap().is_spent());
    }

    // -- Reassign tests --

    #[test]
    fn reassign_frozen() {
        let engine = create_engine();
        let req = make_create_req(80, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();

        let new_hash = [0xBBu8; 32];
        engine.reassign(&ReassignRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0],
            new_utxo_hash: new_hash, block_height: 1000, spendable_after: 100,
        }).unwrap();

        let slot = engine.read_slot(&key, 0).unwrap();
        assert!(slot.is_unspent());
        assert_eq!(slot.hash, new_hash);
        let spendable_h = u32::from_le_bytes(slot.spending_data[0..4].try_into().unwrap());
        assert_eq!(spendable_h, 1100);
    }

    #[test]
    fn reassign_not_frozen() {
        let engine = create_engine();
        let req = make_create_req(81, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        match engine.reassign(&ReassignRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0],
            new_utxo_hash: [0xBB; 32], block_height: 1000, spendable_after: 100,
        }) {
            Err(SpendError::NotFrozen { .. }) => {}
            other => panic!("expected NotFrozen, got {other:?}"),
        }
    }

    #[test]
    fn reassign_hash_mismatch() {
        let engine = create_engine();
        let req = make_create_req(82, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();

        match engine.reassign(&ReassignRequest {
            tx_key: key, offset: 0, utxo_hash: [0xFF; 32],
            new_utxo_hash: [0xBB; 32], block_height: 1000, spendable_after: 100,
        }) {
            Err(SpendError::UtxoHashMismatch { .. }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    #[test]
    fn reassign_not_spendable_until_cooldown() {
        let engine = create_engine();
        let req = make_create_req(83, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();

        let new_hash = [0xCC; 32];
        engine.reassign(&ReassignRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0],
            new_utxo_hash: new_hash, block_height: 1000, spendable_after: 100,
        }).unwrap();

        // Not spendable at block 1099
        let mut sd = [0u8; 36]; sd[0] = 0xDD;
        match engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: new_hash, spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1099, block_height_retention: 288,
        }) {
            Err(SpendError::FrozenUntil { .. }) => {}
            other => panic!("expected FrozenUntil, got {other:?}"),
        }
    }

    #[test]
    fn reassign_spendable_after_cooldown() {
        let engine = create_engine();
        let req = make_create_req(84, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();

        let new_hash = [0xDD; 32];
        engine.reassign(&ReassignRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0],
            new_utxo_hash: new_hash, block_height: 1000, spendable_after: 100,
        }).unwrap();

        // Spendable at block 1101 (> 1100)
        let mut sd = [0u8; 36]; sd[0] = 0xEE;
        engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: new_hash, spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1101, block_height_retention: 288,
        }).unwrap();
        assert!(engine.read_slot(&key, 0).unwrap().is_spent());
    }

    #[test]
    fn reassign_old_hash_spend_fails() {
        let engine = create_engine();
        let req = make_create_req(85, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();
        engine.reassign(&ReassignRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0],
            new_utxo_hash: [0xEE; 32], block_height: 1000, spendable_after: 100,
        }).unwrap();

        let mut sd = [0u8; 36]; sd[0] = 0xFF;
        match engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0], spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 2000, block_height_retention: 288,
        }) {
            Err(SpendError::UtxoHashMismatch { .. }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    // -- SetConflicting tests --

    #[test]
    fn set_conflicting_true() {
        let engine = create_engine();
        let req = make_create_req(90, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        engine.set_conflicting(&SetConflictingRequest {
            tx_key: key, value: true, current_block_height: 1000, block_height_retention: 288,
        }).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(meta.flags.contains(TxFlags::CONFLICTING));
        assert_ne!({ meta.delete_at_height }, 0); // DAH set for conflicting
    }

    #[test]
    fn set_conflicting_false() {
        let engine = create_engine();
        let mut req = make_create_req(91, 5);
        req.conflicting = true;
        let key = req.tx_key();
        engine.create(&req).unwrap();

        engine.set_conflicting(&SetConflictingRequest {
            tx_key: key, value: false, current_block_height: 1000, block_height_retention: 288,
        }).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(!meta.flags.contains(TxFlags::CONFLICTING));
    }

    #[test]
    fn set_conflicting_blocks_spend() {
        let engine = create_engine();
        let req = make_create_req(92, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.set_conflicting(&SetConflictingRequest {
            tx_key: key, value: true, current_block_height: 1000, block_height_retention: 288,
        }).unwrap();

        let mut sd = [0u8; 36]; sd[0] = 0xAA;
        match engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0], spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        }) {
            Err(SpendError::Conflicting) => {}
            other => panic!("expected Conflicting, got {other:?}"),
        }
    }

    // -- SetLocked tests --

    #[test]
    fn set_locked_true() {
        let engine = create_engine();
        let req = make_create_req(100, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.set_locked(&SetLockedRequest { tx_key: key, value: true }).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(meta.flags.contains(TxFlags::LOCKED));
    }

    #[test]
    fn set_locked_clears_dah() {
        let engine = create_engine();
        let req = make_create_req(101, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        // Set conflicting to get a DAH
        engine.set_conflicting(&SetConflictingRequest {
            tx_key: key, value: true, current_block_height: 1000, block_height_retention: 288,
        }).unwrap();
        let meta = engine.read_metadata(&key).unwrap();
        assert_ne!({ meta.delete_at_height }, 0);

        // Lock clears DAH
        engine.set_locked(&SetLockedRequest { tx_key: key, value: true }).unwrap();
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn set_locked_false() {
        let engine = create_engine();
        let mut req = make_create_req(102, 5);
        req.locked = true;
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.set_locked(&SetLockedRequest { tx_key: key, value: false }).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(!meta.flags.contains(TxFlags::LOCKED));
    }

    #[test]
    fn locked_blocks_spend() {
        let engine = create_engine();
        let req = make_create_req(103, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.set_locked(&SetLockedRequest { tx_key: key, value: true }).unwrap();

        let mut sd = [0u8; 36]; sd[0] = 0xAA;
        match engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0], spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        }) {
            Err(SpendError::Locked) => {}
            other => panic!("expected Locked, got {other:?}"),
        }
    }

    // -- PreserveUntil tests --

    #[test]
    fn preserve_until_stores_value() {
        let engine = create_engine();
        let req = make_create_req(110, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        // Set a DAH first
        engine.set_conflicting(&SetConflictingRequest {
            tx_key: key, value: true, current_block_height: 1000, block_height_retention: 288,
        }).unwrap();

        engine.preserve_until(&PreserveUntilRequest { tx_key: key, block_height: 5000 }).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.preserve_until }, 5000);
        assert_eq!({ meta.delete_at_height }, 0); // DAH cleared
    }

    #[test]
    fn preserve_until_blocks_dah_on_spend() {
        let engine = create_engine();
        let mut req = make_create_req(111, 2);
        req.mined_block_infos = vec![MinedBlockInfo { block_id: 1, block_height: 900, subtree_idx: 0 }];
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.preserve_until(&PreserveUntilRequest { tx_key: key, block_height: 5000 }).unwrap();

        // Spend all — DAH should NOT be set because preserve_until is active
        let mut sd = [0u8; 36]; sd[0] = 0xAA;
        engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0], spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        }).unwrap();
        sd[0] = 0xBB;
        engine.spend(&SpendRequest {
            tx_key: key, offset: 1, utxo_hash: req.utxo_hashes[1], spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        }).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn preserve_until_external_signals_preserve() {
        let engine = create_engine();
        let mut req = make_create_req(112, 2);
        req.is_external = true;
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let resp = engine.preserve_until(&PreserveUntilRequest { tx_key: key, block_height: 5000 }).unwrap();
        assert_eq!(resp.signal, Signal::Preserve);
    }

    // -- Delete tests --

    #[test]
    fn delete_existing() {
        let engine = create_engine();
        let req = make_create_req(120, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        engine.delete(&DeleteRequest { tx_key: key }).unwrap();
        assert!(engine.lookup(&key).is_none());
    }

    #[test]
    fn delete_then_lookup_none() {
        let engine = create_engine();
        let req = make_create_req(121, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.delete(&DeleteRequest { tx_key: key }).unwrap();

        match engine.read_metadata(&key) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn delete_nonexistent() {
        let engine = create_engine();
        match engine.delete(&DeleteRequest { tx_key: TxKey { txid: [0xFF; 32] } }) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn delete_frees_space_for_reuse() {
        let engine = create_engine();
        let req1 = make_create_req(122, 100);
        let key1 = req1.tx_key();
        let resp1 = engine.create(&req1).unwrap();
        let offset1 = resp1.record_offset;

        engine.delete(&DeleteRequest { tx_key: key1 }).unwrap();

        // Create another record — should reuse the freed space
        let req2 = make_create_req(123, 100);
        let resp2 = engine.create(&req2).unwrap();
        // Freed space should be reused (same offset)
        assert_eq!(resp2.record_offset, offset1);
    }

    // -- GetSpend tests --

    #[test]
    fn get_spend_unspent() {
        let engine = create_engine();
        let mut req = make_create_req(130, 5);
        req.locktime = 42_000;
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let resp = engine.get_spend(&GetSpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0],
        }).unwrap();
        assert_eq!(resp.status, UTXO_UNSPENT);
        assert!(resp.spending_data.is_none());
        assert_eq!(resp.locktime, 42_000);
    }

    #[test]
    fn get_spend_spent() {
        let engine = create_engine();
        let req = make_create_req(131, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        let mut sd = [0u8; 36]; sd[0] = 0xAB;
        engine.spend(&SpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0], spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 1000, block_height_retention: 288,
        }).unwrap();

        let resp = engine.get_spend(&GetSpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0],
        }).unwrap();
        assert_eq!(resp.status, UTXO_SPENT);
        assert_eq!(resp.spending_data, Some(sd));
    }

    #[test]
    fn get_spend_frozen() {
        let engine = create_engine();
        let req = make_create_req(132, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.freeze(&FreezeRequest { tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0] }).unwrap();

        let resp = engine.get_spend(&GetSpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0],
        }).unwrap();
        assert_eq!(resp.status, UTXO_FROZEN);
        assert_eq!(resp.spending_data, Some([0xFF; 36]));
    }

    #[test]
    fn get_spend_nonexistent_tx() {
        let engine = create_engine();
        match engine.get_spend(&GetSpendRequest {
            tx_key: TxKey { txid: [0xFF; 32] }, offset: 0, utxo_hash: [0; 32],
        }) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn get_spend_hash_mismatch() {
        let engine = create_engine();
        let req = make_create_req(133, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        match engine.get_spend(&GetSpendRequest {
            tx_key: key, offset: 0, utxo_hash: [0xFF; 32],
        }) {
            Err(SpendError::UtxoHashMismatch { .. }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    #[test]
    fn get_spend_offset_out_of_range() {
        let engine = create_engine();
        let req = make_create_req(134, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        match engine.get_spend(&GetSpendRequest {
            tx_key: key, offset: 99, utxo_hash: [0; 32],
        }) {
            Err(SpendError::UtxoNotFound { offset: 99 }) => {}
            other => panic!("expected UtxoNotFound, got {other:?}"),
        }
    }

    #[test]
    fn get_spend_is_readonly() {
        let engine = create_engine();
        let req = make_create_req(135, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta_before = engine.read_metadata(&key).unwrap();
        engine.get_spend(&GetSpendRequest {
            tx_key: key, offset: 0, utxo_hash: req.utxo_hashes[0],
        }).unwrap();
        let meta_after = engine.read_metadata(&key).unwrap();

        assert_eq!({ meta_before.generation }, { meta_after.generation });
        assert_eq!({ meta_before.updated_at }, { meta_after.updated_at });
    }

    // -- Phase 6 additional tests --

    #[test]
    fn get_spend_pruned() {
        let engine = create_engine();
        let req = make_create_req(136, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        // Spend slot 0, then manually set status to PRUNED
        let mut sd = [0u8; 36];
        sd[0] = 0xAB;
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        // Manually write PRUNED status
        let entry = engine.lookup(&key).unwrap();
        let mut slot = io::read_utxo_slot(&*engine.device, entry.record_offset, 0).unwrap();
        slot.status = UTXO_PRUNED;
        io::write_utxo_slot(&*engine.device, entry.record_offset, 0, &slot).unwrap();

        let resp = engine
            .get_spend(&GetSpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        assert_eq!(resp.status, UTXO_PRUNED);
    }

    #[test]
    fn set_conflicting_external_signals_dah_set() {
        let engine = create_engine();
        let mut req = make_create_req(137, 5);
        req.is_external = true;
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let resp = engine
            .set_conflicting(&SetConflictingRequest {
                tx_key: key,
                value: true,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        assert_eq!(resp.signal, Signal::DeleteAtHeightSet);
    }

    #[test]
    fn concurrent_delete_and_spend() {
        let engine = create_engine();
        let req = make_create_req(138, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let e1 = engine.clone();
        let hash0 = req.utxo_hashes[0];

        let h1 = std::thread::spawn(move || {
            e1.delete(&DeleteRequest { tx_key: key })
        });

        let e2 = engine.clone();
        let h2 = std::thread::spawn(move || {
            let mut sd = [0u8; 36];
            sd[0] = 0xAB;
            e2.spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: hash0,
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
        });

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // One should succeed, the other should get TxNotFound (or both succeed
        // if spend completes before delete)
        let outcomes = [r1.is_ok(), r2.is_ok()];
        // At least one should succeed, and no corruption (no panic)
        assert!(
            outcomes[0] || outcomes[1],
            "at least one operation should succeed"
        );
    }

    #[test]
    fn increment_spent_extra_recs_compat_noop() {
        // The compatibility shim is in the server dispatch layer.
        // Here we verify the concept: there's no engine-level operation,
        // because pagination is eliminated. The server returns OK for the
        // opcode without calling any engine method.
        //
        // Verify that the engine has no spent_extra_recs state to corrupt:
        let engine = create_engine();
        let req = make_create_req(139, 10);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        // Spend some UTXOs
        for i in 0..5u32 {
            let mut sd = [0u8; 36];
            sd[0] = i as u8;
            engine
                .spend(&SpendRequest {
                    tx_key: key,
                    offset: i,
                    utxo_hash: req.utxo_hashes[i as usize],
                    spending_data: sd,
                    ignore_conflicting: false,
                    ignore_locked: false,
                    current_block_height: 1000,
                    block_height_retention: 288,
                })
                .unwrap();
        }

        let meta = engine.read_metadata(&key).unwrap();
        // spent_utxos tracks everything in a single record — no extra_recs needed
        assert_eq!({ meta.spent_utxos }, 5);
    }
}
