//! Primary index backend abstraction.
//!
//! Uses enum dispatch (not trait objects) so the in-memory variant has zero
//! overhead — the compiler inlines through match arms.

use crate::allocator::RecordAllocator;
use crate::config::IndexConfig;
use crate::device::BlockDevice;
use crate::index::hashtable::{TxIndexEntry, TxKey};
use crate::index::redb_primary::{CachedFieldsUpdate, RedbPrimary, RedbPrimaryIter};
use crate::index::secondary_backend::{DahBackend, UnminedBackend};
use crate::index::{DahIndex, Index, IndexError, IndexStats, RestoreFlags, UnminedIndex};

/// Primary index backend selection.
///
/// Uses enum dispatch so the in-memory variant has zero overhead: the compiler
/// can inline through match arms. The single branch prediction site per call
/// is negligible compared to the redb I/O cost.
pub enum PrimaryBackend {
    /// In-memory Robin Hood hash table (mmap-backed). Default and fastest.
    InMemory(Index),
    /// On-disk B+ tree via redb. Low RAM, crash-durable.
    OnDisk(RedbPrimary),
    /// File-backed mmap. Same Robin Hood hash table as InMemory but backed
    /// by `mmap(MAP_SHARED)` over a persistent file. Trades crash durability
    /// (relies on redo log) for 100x better write throughput than redb.
    FileBacked(Index),
}

impl std::fmt::Debug for PrimaryBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InMemory(_) => f.write_str("PrimaryBackend::InMemory"),
            Self::OnDisk(_) => f.write_str("PrimaryBackend::OnDisk(redb)"),
            Self::FileBacked(_) => f.write_str("PrimaryBackend::FileBacked"),
        }
    }
}

impl PrimaryBackend {
    /// Create a new in-memory backend sized for `expected_records` entries.
    pub fn new_in_memory(expected_records: usize) -> Result<Self, IndexError> {
        Ok(Self::InMemory(Index::new(expected_records)?))
    }

    /// Open or create a redb on-disk backend at the configured path.
    pub fn new_on_disk(config: &IndexConfig) -> Result<Self, IndexError> {
        Ok(Self::OnDisk(RedbPrimary::open(
            &config.redb_path,
            config.redb_cache_size,
        )?))
    }

    /// Open or create a file-backed mmap backend.
    ///
    /// Uses the same Robin Hood hash table as the in-memory backend but
    /// backed by `mmap(MAP_SHARED)` over a persistent file. Writes are
    /// `memcpy` into the mapped region (no transaction overhead). Crash
    /// recovery relies on TeraSlab's redo log.
    pub fn new_file_backed(
        path: &std::path::Path,
        expected_records: usize,
    ) -> Result<Self, IndexError> {
        Ok(Self::FileBacked(Index::open_file_backed(
            path,
            expected_records,
        )?))
    }

    /// Restore a file-backed index by reopening the existing file.
    ///
    /// Returns an error if the file does not exist.
    pub fn restore_file_backed(
        path: &std::path::Path,
        expected_records: usize,
    ) -> Result<Self, IndexError> {
        if !path.exists() {
            return Err(IndexError::FormatError {
                detail: format!("file-backed index not found: {}", path.display()),
            });
        }
        Self::new_file_backed(path, expected_records)
    }

    /// Look up where a transaction's record lives on disk.
    ///
    /// Returns the entry if present, `None` if the key is absent.
    ///
    /// For the on-disk redb backend, any underlying read failure
    /// (begin_read, open_table, get) is escalated via `tracing::error!`
    /// with `target = "teraslab::index"`, then collapsed to `None`. The
    /// in-memory and file-backed variants are infallible.
    ///
    /// G-4: the engine hot paths (spend, create-duplicate check, delete,
    /// set_mined, freeze, conflict/child resolution, GET fast path) have
    /// been migrated to [`Self::lookup_checked`] / [`Self::unregister_checked`]
    /// and now propagate the [`IndexError`] as a storage error instead of
    /// collapsing it to "absent". This infallible shim is retained only for
    /// callers where treating a read failure as "absent" is acceptable —
    /// recovery replay (a backend error there is surfaced by the
    /// replay-failure path) and best-effort/diagnostic readers (blob-GC
    /// orphan reconciliation, startup self-check). Client-visible read
    /// paths MUST use [`Self::lookup_checked`].
    pub fn lookup(&self, key: &TxKey) -> Option<TxIndexEntry> {
        match self.lookup_checked(key) {
            Ok(found) => found,
            Err(e) => {
                tracing::error!(
                    target: "teraslab::index",
                    err = %e,
                    "PrimaryBackend::lookup: redb read failed; returning None (this can mask a real entry as missing — caller should migrate to lookup_checked)",
                );
                None
            }
        }
    }

    /// Fallible variant of [`Self::lookup`] that propagates redb read errors.
    ///
    /// Returns `Ok(Some(entry))` if the key is present, `Ok(None)` if the
    /// key is absent, and an [`IndexError`] if the redb backend's read
    /// transaction fails. The in-memory and file-backed variants are
    /// infallible.
    pub fn lookup_checked(&self, key: &TxKey) -> Result<Option<TxIndexEntry>, IndexError> {
        match self {
            Self::InMemory(idx) => Ok(idx.lookup(key)),
            Self::OnDisk(redb) => redb.lookup(key),
            Self::FileBacked(idx) => Ok(idx.lookup(key)),
        }
    }

    /// Test-only: arm a synthetic failure in the next redb read so the next
    /// `lookup_checked` returns an [`IndexError`]. No-op for the in-memory
    /// and file-backed variants (their reads are infallible). Used by the
    /// G-4 engine tests to verify backend read errors surface as real
    /// storage errors rather than collapsing to "absent".
    #[cfg(test)]
    pub fn arm_fail_next_read(&self) {
        if let Self::OnDisk(redb) = self {
            redb.arm_fail_next_read();
        }
    }

    /// Register a newly created transaction record in the index.
    pub fn register(&mut self, key: TxKey, entry: TxIndexEntry) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => idx.register(key, entry),
            Self::OnDisk(redb) => redb.register(key, entry),
            Self::FileBacked(idx) => idx.register(key, entry),
        }
    }

    /// Register or update an entry without performing the mmap hash-table
    /// resize inline. Redb has no mmap resize path, so it uses its normal
    /// transactional register implementation.
    pub(crate) fn register_without_resize(
        &mut self,
        key: TxKey,
        entry: TxIndexEntry,
    ) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => idx.register_without_resize(key, entry),
            Self::OnDisk(redb) => redb.register(key, entry),
        }
    }

    /// Register an entry only if `key` is absent, without the inline mmap
    /// resize. Returns `Ok(true)` if inserted (key was absent), `Ok(false)` if
    /// the key was already present (the index is left unchanged — the existing
    /// entry is NEVER overwritten).
    ///
    /// For the mmap-backed variants this is a single fused Robin Hood probe
    /// (one walk does both the duplicate check and the placement), replacing a
    /// separate `lookup` + re-probing `insert`. The redb variant has no fused
    /// path, so it composes a `lookup` followed by a `register` only when
    /// absent — behaviourally identical to the engine's previous
    /// `lookup_checked` + `register_without_resize` under the same shard write
    /// guard, so redb gains no race and loses no semantics.
    pub(crate) fn register_without_resize_if_absent(
        &mut self,
        key: TxKey,
        entry: TxIndexEntry,
    ) -> Result<bool, IndexError> {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => {
                idx.register_without_resize_if_absent(key, entry)
            }
            Self::OnDisk(redb) => {
                if redb.lookup(&key)?.is_some() {
                    Ok(false)
                } else {
                    redb.register(key, entry)?;
                    Ok(true)
                }
            }
        }
    }

    /// Target capacity for a pending mmap hash-table resize, if this backend
    /// needs one. Redb does not use this path.
    pub(crate) fn resize_target_capacity(&self) -> Option<usize> {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => idx.resize_target_capacity(),
            Self::OnDisk(_) => None,
        }
    }

    /// G-2: mark a displaced mmap-backed index defunct before the engine
    /// swaps a freshly [`Self::resized_copy`]'d backend over it, so the old
    /// table's `Drop` does not write the clean-shutdown sentinel for a path
    /// the resized copy now owns. A no-op for the redb backend (no sentinel)
    /// and for anonymous-backed in-memory tables.
    pub(crate) fn mark_defunct_for_resize(&mut self) {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => idx.mark_defunct_for_resize(),
            Self::OnDisk(_) => {}
        }
    }

    /// Build a resized copy of an mmap-backed index without mutating the
    /// currently visible table. The engine swaps the returned backend under an
    /// exclusive lock after the copy has been built under an upgradable read
    /// lock, so concurrent lookups continue during the rehash.
    pub(crate) fn resized_copy(&self, target_capacity: usize) -> Result<Self, IndexError> {
        match self {
            Self::InMemory(idx) => Ok(Self::InMemory(idx.resized_copy(target_capacity)?)),
            Self::FileBacked(idx) => Ok(Self::FileBacked(idx.resized_copy(target_capacity)?)),
            Self::OnDisk(_) => Err(IndexError::FormatError {
                detail: "redb primary backend does not support hash-table resize".into(),
            }),
        }
    }

    /// Resize this backend in-place if its hash table currently needs one.
    ///
    /// Checks [`Self::resize_target_capacity`]; returns `Ok(())` immediately if
    /// no resize is needed. Otherwise builds a [`Self::resized_copy`], marks
    /// `self` defunct (so its `Drop` does not write the clean-shutdown sentinel
    /// for a path the new table now owns — G-2), and swaps the new backend into
    /// `self`. Redb backends never need a resize, so this is a no-op for them.
    ///
    /// This performs the exact build-resized + mark-defunct + swap sequence the
    /// engine previously ran via a drop-then-reacquire round-trip; callers that
    /// already hold the shard write guard can run it inline without releasing
    /// the lock.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] if the underlying resize allocation fails.
    pub(crate) fn resize_if_needed(&mut self) -> Result<(), IndexError> {
        let Some(target_capacity) = self.resize_target_capacity() else {
            return Ok(());
        };
        let resized = self.resized_copy(target_capacity)?;
        self.mark_defunct_for_resize();
        *self = resized;
        Ok(())
    }

    /// Remove a transaction from the index (on deletion/pruning).
    ///
    /// Returns the removed entry, or `None` if the key was not present.
    ///
    /// For the on-disk redb backend, any underlying write failure
    /// (begin_write, open_table, remove, commit) is escalated via
    /// `tracing::error!` with `target = "teraslab::index"`, then collapsed
    /// to `None`. The in-memory and file-backed variants are infallible.
    ///
    /// G-4: the engine delete path now uses [`Self::unregister_checked`]
    /// (via `Engine::unregister_with_shard_count`) and propagates the
    /// [`IndexError`] — a commit failure aborts the delete before the
    /// region is freed or secondaries are touched, so the entry can never
    /// be left on disk while the caller skips downstream cleanup. This
    /// infallible shim is retained only for recovery replay
    /// (`recovery::replay_delete`), where a backend error is surfaced by
    /// the replay-failure path. New mutation paths MUST use
    /// [`Self::unregister_checked`].
    pub fn unregister(&mut self, key: &TxKey) -> Option<TxIndexEntry> {
        match self.unregister_checked(key) {
            Ok(found) => found,
            Err(e) => {
                tracing::error!(
                    target: "teraslab::index",
                    err = %e,
                    "PrimaryBackend::unregister: redb write failed; returning None (the entry may still be on disk — caller should migrate to unregister_checked)",
                );
                None
            }
        }
    }

    /// Fallible variant of [`Self::unregister`] that propagates redb write
    /// errors.
    ///
    /// Returns `Ok(Some(entry))` if the key was found and removed,
    /// `Ok(None)` if the key was not present, and an [`IndexError`] if the
    /// redb backend's write transaction fails. The in-memory and
    /// file-backed variants are infallible.
    pub fn unregister_checked(&mut self, key: &TxKey) -> Result<Option<TxIndexEntry>, IndexError> {
        match self {
            Self::InMemory(idx) => Ok(idx.unregister(key)),
            Self::OnDisk(redb) => redb.unregister(key),
            Self::FileBacked(idx) => Ok(idx.unregister(key)),
        }
    }

    /// Update the cached fields in the index entry for `key`.
    ///
    /// Returns `Ok(true)` if the key was found and updated, `Ok(false)` if the
    /// key was not present, and an [`IndexError`] if the on-disk (redb) backend
    /// fails to commit. Callers MUST propagate the error — silently dropping
    /// it causes `dah_or_preserve`, `unmined_since`, and `generation` to drift
    /// relative to the persisted state. The in-memory and file-backed variants
    /// are infallible and always return `Ok(bool)`.
    #[allow(clippy::too_many_arguments)]
    pub fn update_cached_fields(
        &mut self,
        key: &TxKey,
        tx_flags: u8,
        block_entry_count: u8,
        spent_utxos: u32,
        dah_or_preserve: u32,
        unmined_since: u32,
        generation: u32,
    ) -> Result<bool, IndexError> {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => Ok(idx.update_cached_fields(
                key,
                tx_flags,
                block_entry_count,
                spent_utxos,
                dah_or_preserve,
                unmined_since,
                generation,
            )),
            Self::OnDisk(redb) => redb.update_cached_fields(
                key,
                tx_flags,
                block_entry_count,
                spent_utxos,
                dah_or_preserve,
                unmined_since,
                generation,
            ),
        }
    }

    /// Update cached fields for multiple entries in a single transaction.
    ///
    /// For the redb backend, all updates are performed within one write
    /// transaction, amortizing the `begin_write()/commit()` overhead.
    /// For the in-memory backend, updates are applied individually (no
    /// batching benefit for direct memory writes).
    ///
    /// Returns `Ok(n)` where `n` is the number of entries successfully updated.
    /// Returns an [`IndexError`] if the on-disk (redb) backend fails to commit.
    /// Callers MUST propagate the error — silently returning `0` on commit
    /// failure would cause durability-critical cached fields (DAH,
    /// `unmined_since`, `generation`) to drift relative to the persisted state,
    /// leading to incorrect pruning and replication decisions downstream.
    pub fn update_cached_fields_batch(
        &mut self,
        updates: &[CachedFieldsUpdate],
    ) -> Result<usize, IndexError> {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => {
                let mut count = 0;
                for u in updates {
                    if idx.update_cached_fields(
                        &u.key,
                        u.tx_flags,
                        u.block_entry_count,
                        u.spent_utxos,
                        u.dah_or_preserve,
                        u.unmined_since,
                        u.generation,
                    ) {
                        count += 1;
                    }
                }
                Ok(count)
            }
            Self::OnDisk(redb) => redb.update_cached_fields_batch(updates),
        }
    }

    /// Remove multiple transactions in a single transaction.
    ///
    /// Returns a `Vec` parallel to the input: `Some(entry)` for keys that
    /// were found and removed, `None` for missing keys. Returns an
    /// [`IndexError`] if the on-disk backend fails to commit; the in-memory
    /// and file-backed variants are infallible.
    pub fn unregister_batch(
        &mut self,
        keys: &[TxKey],
    ) -> Result<Vec<Option<TxIndexEntry>>, IndexError> {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => {
                Ok(keys.iter().map(|k| idx.unregister(k)).collect())
            }
            Self::OnDisk(redb) => redb.unregister_batch(keys),
        }
    }

    /// Number of entries in the primary index.
    pub fn len(&self) -> usize {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => idx.len(),
            Self::OnDisk(redb) => redb.len(),
        }
    }

    /// Whether the primary index is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => idx.is_empty(),
            Self::OnDisk(redb) => redb.is_empty(),
        }
    }

    /// Iterate over all `(TxKey, TxIndexEntry)` pairs in the primary index.
    ///
    /// The on-disk redb variant streams bounded batches instead of
    /// materializing the full table in memory.
    pub fn iter(&self) -> PrimaryIter<'_> {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => PrimaryIter::InMemory(idx.iter()),
            Self::OnDisk(redb) => PrimaryIter::Redb(redb.iter_streaming()),
        }
    }

    /// Statistics for monitoring.
    pub fn stats(&self) -> IndexStats {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => idx.stats(),
            Self::OnDisk(redb) => redb.stats(),
        }
    }

    /// The name of the active backend (for monitoring/logging).
    pub fn backend_name(&self) -> &'static str {
        match self {
            Self::InMemory(_) => "memory",
            Self::OnDisk(_) => "redb",
            Self::FileBacked(_) => "file_backed",
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot / restore
    // -----------------------------------------------------------------------

    /// Borrow the inner in-memory [`Index`] if this is the `InMemory` variant.
    ///
    /// Returns `None` for the `OnDisk` (redb) and `FileBacked` variants, which
    /// are self-persistent and never participate in the in-memory snapshot
    /// path. Exposed to the crate so the v2 sharded snapshot
    /// ([`crate::index::sharded::ShardedIndex::snapshot_all`]) can serialize
    /// each shard's primary payload region.
    pub(crate) fn as_in_memory_index(&self) -> Option<&Index> {
        match self {
            Self::InMemory(idx) => Some(idx),
            Self::OnDisk(_) | Self::FileBacked(_) => None,
        }
    }

    /// Snapshot the primary index to a file.
    ///
    /// For the redb backend, this is a no-op (redb is already crash-durable).
    pub fn snapshot(&self, path: &std::path::Path) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => idx.snapshot(path),
            Self::OnDisk(redb) => redb.snapshot(path),
            Self::FileBacked(idx) => {
                idx.sync();
                Ok(())
            }
        }
    }

    /// Flush dirty pages to the backing file (file-backed backend only).
    ///
    /// No-op for InMemory and OnDisk backends. For FileBacked, schedules
    /// an asynchronous writeback of dirty pages.
    pub fn sync(&self) {
        if let Self::FileBacked(idx) = self {
            idx.sync();
        }
    }

    /// Force all backend state durable on its own storage (G-1 audit fix).
    ///
    /// For the `OnDisk` (redb) backend this commits an empty
    /// `Durability::Immediate` transaction, fsyncing every prior
    /// `Durability::Eventual` commit. Per-op redb commits are only
    /// crash-safe while their covering redo entries are replayable, so
    /// the checkpoint MUST call this — and abort on failure — before it
    /// fences and compacts the redo log.
    ///
    /// `InMemory` is a no-op (its durability comes from the snapshot
    /// file the checkpoint writes). `FileBacked` schedules an mmap
    /// writeback; its crash story is the unclean-shutdown sentinel +
    /// device-scan rebuild, not this flush.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] if the redb flush transaction fails;
    /// the caller must treat the backend state as NOT durable.
    pub fn flush_durable(&self) -> Result<(), IndexError> {
        match self {
            Self::InMemory(_) => Ok(()),
            Self::OnDisk(redb) => redb.flush_durable(),
            Self::FileBacked(idx) => {
                idx.sync();
                Ok(())
            }
        }
    }

    /// Attach a redo log for journaling crash-atomic file-backed hash
    /// table resizes.
    ///
    /// Only meaningful for [`PrimaryBackend::FileBacked`] (which holds
    /// the Robin Hood hash table on an mmap'd file). No-op for the
    /// [`PrimaryBackend::OnDisk`] redb variant (redb manages its own
    /// durability) and for [`PrimaryBackend::InMemory`] (anonymous mmap
    /// does not persist across restarts). Also attaches to `InMemory`
    /// so the redo log survives any future in-memory → file-backed
    /// migration path, but the attachment has no effect there.
    pub fn set_redo_log(
        &mut self,
        redo_log: std::sync::Arc<parking_lot::Mutex<crate::redo::RedoLog>>,
    ) {
        match self {
            Self::InMemory(idx) | Self::FileBacked(idx) => idx.set_redo_log(redo_log),
            Self::OnDisk(_) => {}
        }
    }

    /// Restore the primary index from a snapshot file (in-memory backend).
    pub fn restore(path: &std::path::Path) -> Result<Self, IndexError> {
        Ok(Self::InMemory(Index::restore(path)?))
    }

    /// Restore the primary index from an existing redb database.
    pub fn restore_redb(config: &IndexConfig) -> Result<Self, IndexError> {
        if !config.redb_path.exists() {
            return Err(IndexError::FormatError {
                detail: format!("redb file not found: {}", config.redb_path.display()),
            });
        }
        Self::new_on_disk(config)
    }

    /// Snapshot primary index + both secondary indexes to a single file.
    ///
    /// Accepts the pluggable backend wrappers for the secondary indexes.
    /// For the in-memory primary backend the secondary backends must be
    /// `InMemory` variants; if they are `OnDisk` variants no secondary
    /// data is written (redb is already durable so no snapshot is needed).
    /// For the on-disk primary backend this is always a no-op.
    pub fn snapshot_all(
        &self,
        dah: &DahBackend,
        unmined: &UnminedBackend,
        path: &std::path::Path,
    ) -> Result<(), IndexError> {
        match self {
            Self::InMemory(idx) => {
                match (dah, unmined) {
                    (DahBackend::InMemory(d), UnminedBackend::InMemory(u)) => {
                        idx.snapshot_all(d, u, path)
                    }
                    // Secondary indexes are on-disk (redb already durable) — skip snapshot.
                    _ => Ok(()),
                }
            }
            Self::OnDisk(_) => Ok(()), // No-op: redb is already durable
            Self::FileBacked(_) => Ok(()), // No-op: file IS persistence
        }
    }

    /// Restore all indexes from a snapshot file (in-memory backend).
    pub fn restore_all(
        path: &std::path::Path,
    ) -> Result<(Self, DahIndex, UnminedIndex, RestoreFlags), IndexError> {
        let (idx, dah, unmined, flags) = Index::restore_all(path)?;
        Ok((Self::InMemory(idx), dah, unmined, flags))
    }

    // -----------------------------------------------------------------------
    // Rebuild from device scan
    // -----------------------------------------------------------------------

    /// Rebuild the primary index by scanning all records on the device.
    pub fn rebuild(
        device: &dyn BlockDevice,
        allocator: &dyn crate::allocator::RecordAllocator,
    ) -> Result<Self, IndexError> {
        Ok(Self::InMemory(Index::rebuild(device, allocator)?))
    }

    /// Rebuild the primary index into a redb database by scanning all records.
    ///
    /// Free holes from the allocator freelist are skipped. I/O errors or
    /// invalid metadata in allocated regions fail the rebuild so startup
    /// cannot silently create a partial primary index.
    pub fn rebuild_redb(
        config: &IndexConfig,
        device: &dyn BlockDevice,
        allocator: &dyn RecordAllocator,
    ) -> Result<Self, IndexError> {
        let mut primary = RedbPrimary::open(&config.redb_path, config.redb_cache_size)?;

        let align = allocator.device_alignment();
        let start = allocator.data_region_start();
        let end = allocator.next_offset();

        let read_size = align.max(crate::record::METADATA_SIZE);
        let aligned_read = read_size.div_ceil(align) * align;

        const BATCH_SIZE: usize = 10_000;
        let mut batch = Vec::with_capacity(BATCH_SIZE);

        let mut offset = start;
        let mut skipped_malformed: u64 = 0;
        while offset + aligned_read as u64 <= end {
            if let Some((free_offset, free_size)) = allocator.free_region_containing(offset) {
                let free_end = free_offset.saturating_add(free_size).min(end);
                offset = free_end.max(offset + align as u64);
                continue;
            }

            let mut buf = crate::device::AlignedBuf::new(aligned_read, align);
            device.pread_exact_at(&mut buf, offset)?;

            // A deleted-record tombstone or never-written reservation, NOT
            // corruption. `classify_scan_header` skips the WHOLE record for a
            // length-bearing deleted marker (multi-block boot-loop fix) and
            // one block for a legacy all-zero header, bounded by `end`. A live
            // record yields `None`. See the matching comment in
            // `crate::index::mod`'s in-memory rebuild.
            if let Some(next) = crate::index::classify_scan_header(
                &buf[..crate::record::METADATA_SIZE],
                offset,
                align as u64,
                end,
            )? {
                offset = next;
                continue;
            }

            // Issue #14: tolerate a malformed/orphan region (skip + resync one
            // aligned block) instead of fatal-aborting the rebuild. Mirrors
            // `Index::rebuild`.
            let (meta, aligned_advance) = match crate::index::classify_scanned_record(
                &buf[..crate::record::METADATA_SIZE],
                offset,
                align,
                end,
            ) {
                crate::index::ScannedRecord::Valid {
                    meta,
                    aligned_advance,
                } => (meta, aligned_advance),
                crate::index::ScannedRecord::Skip { advance, reason } => {
                    skipped_malformed += 1;
                    tracing::warn!(
                        target: "teraslab::recovery",
                        offset,
                        reason,
                        "rebuild_redb: skipping malformed/orphan region (issue #14)"
                    );
                    offset += advance;
                    continue;
                }
            };

            let utxo_count = { meta.utxo_count };
            let key = TxKey { txid: meta.tx_id };
            let entry = TxIndexEntry {
                device_id: 0,
                record_offset: offset,
                utxo_count,
                block_entry_count: meta.block_entry_count,
                tx_flags: meta.flags.bits(),
                spent_utxos: meta.spent_utxos,
                dah_or_preserve: 0,
                unmined_since: 0,
                generation: 0,
            };
            batch.push((key, entry));

            if batch.len() >= BATCH_SIZE {
                primary.register_batch(&batch)?;
                batch.clear();
            }
            offset += aligned_advance;
        }

        // Flush remaining entries
        if !batch.is_empty() {
            primary.register_batch(&batch)?;
        }

        if skipped_malformed > 0 {
            tracing::error!(
                target: "teraslab::recovery",
                skipped_malformed,
                "rebuild_redb: completed with malformed/orphan region(s) skipped — possible \
                 data loss; investigate device integrity (issue #14)"
            );
        }

        Ok(Self::OnDisk(primary))
    }

    /// Rebuild the primary index into a file-backed mmap by scanning all records.
    ///
    /// Free holes from the allocator freelist are skipped. I/O errors or
    /// invalid metadata in allocated regions fail the rebuild so startup
    /// cannot silently create a partial primary index.
    pub fn rebuild_file_backed(
        path: &std::path::Path,
        device: &dyn BlockDevice,
        allocator: &dyn RecordAllocator,
    ) -> Result<Self, IndexError> {
        let _ = std::fs::remove_file(path);

        let mut index = Index::open_file_backed(path, 1024)?;
        let align = allocator.device_alignment();
        let start = allocator.data_region_start();
        let end = allocator.next_offset();

        let read_size = align.max(crate::record::METADATA_SIZE);
        let aligned_read = read_size.div_ceil(align) * align;

        let mut offset = start;
        let mut skipped_malformed: u64 = 0;
        while offset + aligned_read as u64 <= end {
            if let Some((free_offset, free_size)) = allocator.free_region_containing(offset) {
                let free_end = free_offset.saturating_add(free_size).min(end);
                offset = free_end.max(offset + align as u64);
                continue;
            }

            let mut buf = crate::device::AlignedBuf::new(aligned_read, align);
            device.pread_exact_at(&mut buf, offset)?;

            // A deleted-record tombstone or never-written reservation, NOT
            // corruption. `classify_scan_header` skips the WHOLE record for a
            // length-bearing deleted marker (multi-block boot-loop fix) and
            // one block for a legacy all-zero header, bounded by `end`. A live
            // record yields `None`. See the matching comment in
            // `crate::index::mod`'s in-memory rebuild.
            if let Some(next) = crate::index::classify_scan_header(
                &buf[..crate::record::METADATA_SIZE],
                offset,
                align as u64,
                end,
            )? {
                offset = next;
                continue;
            }

            // Issue #14: tolerate a malformed/orphan region (skip + resync one
            // aligned block) instead of fatal-aborting the rebuild. Mirrors
            // `Index::rebuild`.
            let (meta, aligned_advance) = match crate::index::classify_scanned_record(
                &buf[..crate::record::METADATA_SIZE],
                offset,
                align,
                end,
            ) {
                crate::index::ScannedRecord::Valid {
                    meta,
                    aligned_advance,
                } => (meta, aligned_advance),
                crate::index::ScannedRecord::Skip { advance, reason } => {
                    skipped_malformed += 1;
                    tracing::warn!(
                        target: "teraslab::recovery",
                        offset,
                        reason,
                        "rebuild_file_backed: skipping malformed/orphan region (issue #14)"
                    );
                    offset += advance;
                    continue;
                }
            };

            let utxo_count = { meta.utxo_count };
            let key = TxKey { txid: meta.tx_id };
            let entry = TxIndexEntry {
                device_id: 0,
                record_offset: offset,
                utxo_count,
                block_entry_count: meta.block_entry_count,
                tx_flags: meta.flags.bits(),
                spent_utxos: meta.spent_utxos,
                dah_or_preserve: 0,
                unmined_since: 0,
                generation: 0,
            };
            index.register(key, entry)?;
            offset += aligned_advance;
        }

        if skipped_malformed > 0 {
            tracing::error!(
                target: "teraslab::recovery",
                skipped_malformed,
                "rebuild_file_backed: completed with malformed/orphan region(s) skipped — \
                 possible data loss; investigate device integrity (issue #14)"
            );
        }

        index.sync();
        Ok(Self::FileBacked(index))
    }

    /// Rebuild secondary indexes by scanning all records on the device.
    pub fn rebuild_secondary(
        device: &dyn BlockDevice,
        allocator: &dyn RecordAllocator,
    ) -> Result<(DahIndex, UnminedIndex), IndexError> {
        Index::rebuild_secondary(device, allocator)
    }
}

impl From<Index> for PrimaryBackend {
    fn from(idx: Index) -> Self {
        Self::InMemory(idx)
    }
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

/// Iterator over all `(TxKey, TxIndexEntry)` pairs, dispatching to the
/// active backend.
pub enum PrimaryIter<'a> {
    /// In-memory hash table iterator.
    InMemory(crate::index::hashtable::HashTableIter<'a>),
    /// Bounded-batch redb iterator.
    Redb(RedbPrimaryIter<'a>),
}

impl Iterator for PrimaryIter<'_> {
    type Item = (TxKey, TxIndexEntry);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::InMemory(it) => it.next(),
            Self::Redb(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::InMemory(it) => it.size_hint(),
            Self::Redb(it) => it.size_hint(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::config::{IndexBackendMode, IndexConfig};
    use crate::device::{AlignedBuf, MemoryDevice};
    use crate::io::write_full_record;
    use crate::record::{CRC32_OFFSET, METADATA_SIZE, TxMetadata, UtxoSlot};
    use std::sync::Arc;

    /// Mirror of the helper in `crate::index::tests`: corrupt the magic
    /// bytes AND restamp the CRC so `TxMetadata::from_bytes` accepts the
    /// header and the magic check is the gate that fails.
    fn corrupt_magic_and_restamp_crc(dev: &dyn BlockDevice, offset: u64) {
        let align = dev.alignment();
        let mut buf = AlignedBuf::new(align, align);
        dev.pread_exact_at(&mut buf, offset).unwrap();
        buf[0..4].copy_from_slice(&[0u8; 4]);
        let mut hash_buf = [0u8; METADATA_SIZE];
        hash_buf.copy_from_slice(&buf[..METADATA_SIZE]);
        hash_buf[CRC32_OFFSET..CRC32_OFFSET + 4].copy_from_slice(&[0u8; 4]);
        let crc = crc32fast::hash(&hash_buf);
        buf[CRC32_OFFSET..CRC32_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());
        dev.pwrite_all_at(&buf, offset).unwrap();
    }

    fn make_key(n: u64) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&n.to_le_bytes());
        txid[8..16].copy_from_slice(&(n.wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes());
        TxKey { txid }
    }

    fn make_entry(offset: u64) -> TxIndexEntry {
        TxIndexEntry {
            device_id: 0,
            record_offset: offset,
            utxo_count: 10,
            block_entry_count: 2,
            tx_flags: 0x05,
            spent_utxos: 3,
            dah_or_preserve: 100,
            unmined_since: 500,
            generation: 7,
        }
    }

    fn redb_config(dir: &std::path::Path) -> IndexConfig {
        IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.join("primary.redb"),
            redb_dah_path: dir.join("dah.redb"),
            redb_unmined_path: dir.join("unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        }
    }

    /// Helper that runs the same test body against all three backends.
    fn with_all_backends(f: impl Fn(&mut PrimaryBackend)) {
        // In-memory
        let mut mem = PrimaryBackend::new_in_memory(1000).unwrap();
        f(&mut mem);

        // On-disk (redb)
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let mut disk = PrimaryBackend::new_on_disk(&config).unwrap();
        f(&mut disk);

        // File-backed mmap
        let fb_dir = tempfile::tempdir().unwrap();
        let fb_path = fb_dir.path().join("primary.idx");
        let mut fb = PrimaryBackend::new_file_backed(&fb_path, 1000).unwrap();
        f(&mut fb);
    }

    // -----------------------------------------------------------------------
    // Parameterized tests: both backends produce identical results
    // -----------------------------------------------------------------------

    #[test]
    fn both_backends_lookup_register_unregister() {
        with_all_backends(|backend| {
            // Empty lookup
            assert!(backend.lookup(&make_key(1)).is_none());
            assert!(backend.is_empty());
            assert_eq!(backend.len(), 0);

            // Register
            backend.register(make_key(1), make_entry(4096)).unwrap();
            assert_eq!(backend.len(), 1);
            assert!(!backend.is_empty());

            let e = backend.lookup(&make_key(1)).expect("should find entry");
            assert_eq!(e.record_offset, 4096);
            assert_eq!(e.utxo_count, 10);
            assert_eq!(e.tx_flags, 0x05);
            assert_eq!(e.generation, 7);

            // Missing key
            assert!(backend.lookup(&make_key(999)).is_none());

            // Unregister
            let removed = backend
                .unregister(&make_key(1))
                .expect("should return entry");
            assert_eq!(removed.record_offset, 4096);
            assert!(backend.is_empty());

            // Unregister missing
            assert!(backend.unregister(&make_key(1)).is_none());
        });
    }

    #[test]
    fn both_backends_register_many_and_iterate() {
        with_all_backends(|backend| {
            for i in 0..100u64 {
                backend.register(make_key(i), make_entry(i * 100)).unwrap();
            }
            assert_eq!(backend.len(), 100);

            // Iterate and verify all entries present
            let entries: Vec<_> = backend.iter().collect();
            assert_eq!(entries.len(), 100);
            for i in 0..100u64 {
                let found = entries
                    .iter()
                    .any(|(k, e)| *k == make_key(i) && e.record_offset == i * 100);
                assert!(found, "entry {i} not found in iter");
            }

            // size_hint: Collected variant gives exact, InMemory may not
            let iter = backend.iter();
            let (_lower, _upper) = iter.size_hint();
        });
    }

    #[test]
    fn both_backends_update_cached_fields() {
        with_all_backends(|backend| {
            backend.register(make_key(1), make_entry(4096)).unwrap();

            let updated = backend
                .update_cached_fields(&make_key(1), 0xFF, 5, 8, 200, 600, 99)
                .unwrap();
            assert!(updated);

            let e = backend.lookup(&make_key(1)).unwrap();
            assert_eq!(e.tx_flags, 0xFF);
            assert_eq!(e.block_entry_count, 5);
            assert_eq!(e.spent_utxos, 8);
            assert_eq!(e.dah_or_preserve, 200);
            assert_eq!(e.unmined_since, 600);
            assert_eq!(e.generation, 99);
            // Unchanged fields
            assert_eq!(e.record_offset, 4096);
            assert_eq!(e.utxo_count, 10);

            // Update missing key
            let missing = backend
                .update_cached_fields(&make_key(999), 0, 0, 0, 0, 0, 0)
                .unwrap();
            assert!(!missing);
        });
    }

    #[test]
    fn both_backends_stats() {
        with_all_backends(|backend| {
            let stats = backend.stats();
            assert_eq!(stats.entry_count, 0);

            for i in 0..10u64 {
                backend.register(make_key(i), make_entry(i)).unwrap();
            }
            let stats = backend.stats();
            assert_eq!(stats.entry_count, 10);
        });
    }

    // -----------------------------------------------------------------------
    // OnDisk-specific tests
    // -----------------------------------------------------------------------

    #[test]
    fn on_disk_new_on_disk_creates_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let backend = PrimaryBackend::new_on_disk(&config).unwrap();
        assert!(backend.is_empty());
        assert_eq!(backend.backend_name(), "redb");
    }

    #[test]
    fn in_memory_backend_name() {
        let backend = PrimaryBackend::new_in_memory(16).unwrap();
        assert_eq!(backend.backend_name(), "memory");
    }

    #[test]
    fn on_disk_restore_redb_opens_existing() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());

        // Create and populate
        {
            let mut backend = PrimaryBackend::new_on_disk(&config).unwrap();
            for i in 0..50u64 {
                backend.register(make_key(i), make_entry(i * 100)).unwrap();
            }
        }

        // Restore from existing file
        let restored = PrimaryBackend::restore_redb(&config).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.backend_name(), "redb");
        for i in 0..50u64 {
            let e = restored
                .lookup(&make_key(i))
                .expect("entry should survive reopen");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn on_disk_restore_redb_missing_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let config = IndexConfig {
            backend: IndexBackendMode::Redb,
            redb_path: dir.path().join("nonexistent.redb"),
            redb_dah_path: dir.path().join("dah.redb"),
            redb_unmined_path: dir.path().join("unmined.redb"),
            redb_cache_size: 64 * 1024 * 1024,
            ..IndexConfig::default()
        };
        let result = PrimaryBackend::restore_redb(&config);
        // N-LOW: the following match is the real variant check; the bare
        // assert!(result.is_err()) it replaced was redundant.
        match result.unwrap_err() {
            IndexError::FormatError { detail } => {
                assert!(detail.contains("not found"), "error was: {detail}");
            }
            other => panic!("expected FormatError, got {other:?}"),
        }
    }

    #[test]
    fn on_disk_snapshot_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let mut backend = PrimaryBackend::new_on_disk(&config).unwrap();
        backend.register(make_key(1), make_entry(100)).unwrap();

        // Snapshot should succeed (no-op)
        let snap_path = dir.path().join("noop.snap");
        backend.snapshot(&snap_path).unwrap();
        // File should NOT be created (it's a no-op)
        assert!(!snap_path.exists());
    }

    #[test]
    fn on_disk_snapshot_all_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let mut backend = PrimaryBackend::new_on_disk(&config).unwrap();
        backend.register(make_key(1), make_entry(100)).unwrap();

        let dah = DahBackend::InMemory(DahIndex::new());
        let unmined = UnminedBackend::InMemory(UnminedIndex::new());
        let snap_path = dir.path().join("all.snap");
        backend.snapshot_all(&dah, &unmined, &snap_path).unwrap();
        // No-op for OnDisk
        assert!(!snap_path.exists());
    }

    #[test]
    fn on_disk_debug_format() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let backend = PrimaryBackend::new_on_disk(&config).unwrap();
        let debug = format!("{backend:?}");
        assert!(debug.contains("OnDisk"), "debug was: {debug}");

        let mem = PrimaryBackend::new_in_memory(16).unwrap();
        let debug = format!("{mem:?}");
        assert!(debug.contains("InMemory"), "debug was: {debug}");
    }

    #[test]
    fn from_index_produces_in_memory() {
        let idx = Index::new(16).unwrap();
        let backend: PrimaryBackend = idx.into();
        assert_eq!(backend.backend_name(), "memory");
    }

    // -----------------------------------------------------------------------
    // rebuild_redb tests
    // -----------------------------------------------------------------------

    fn setup_device_with_records(
        record_count: usize,
    ) -> (Arc<MemoryDevice>, SlotAllocator, Vec<(TxKey, u64)>) {
        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
        let mut records = Vec::new();

        for i in 0..record_count {
            let mut meta = TxMetadata::new(5);
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            txid[8..16]
                .copy_from_slice(&((i as u64).wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes());
            meta.tx_id = txid;

            let record_size = TxMetadata::record_size_for(5);
            let offset = alloc.allocate(record_size).unwrap();

            let slots: Vec<UtxoSlot> = (0..5)
                .map(|s| {
                    let mut h = [0u8; 32];
                    h[0] = s;
                    UtxoSlot::new_unspent(h)
                })
                .collect();

            write_full_record(&*dev, offset, &meta, &slots).unwrap();
            records.push((TxKey { txid }, offset));
        }

        (dev, alloc, records)
    }

    #[test]
    fn rebuild_redb_from_device() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let (dev, alloc, records) = setup_device_with_records(10);

        let rebuilt = PrimaryBackend::rebuild_redb(&config, &*dev, &alloc).unwrap();
        assert_eq!(rebuilt.len(), 10);
        assert_eq!(rebuilt.backend_name(), "redb");

        for (key, offset) in &records {
            let e = rebuilt.lookup(key).expect("record should be indexed");
            assert_eq!(e.record_offset, *offset);
            assert_eq!(e.utxo_count, 5);
        }
    }

    #[test]
    fn rebuild_redb_empty_device() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let dev = Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();

        let rebuilt = PrimaryBackend::rebuild_redb(&config, &*dev, &alloc).unwrap();
        assert_eq!(rebuilt.len(), 0);
    }

    // Issue #14: redb rebuild tolerates a malformed allocated region (skips +
    // recovers the rest) rather than fatal-aborting. (Pre-#14: fatal.)
    #[test]
    fn rebuild_redb_skips_corrupted_magic_in_allocated_region() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let (dev, alloc, records) = setup_device_with_records(10);

        let offset = records[3].1;
        corrupt_magic_and_restamp_crc(&*dev, offset);

        let rebuilt = PrimaryBackend::rebuild_redb(&config, &*dev, &alloc)
            .expect("rebuild must tolerate the bad region");
        assert_eq!(rebuilt.len(), 9);
        assert!(rebuilt.lookup(&records[3].0).is_none());
    }

    #[test]
    fn rebuild_redb_skips_crc_mismatch_in_allocated_region() {
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let (dev, alloc, records) = setup_device_with_records(10);

        let offset = records[3].1;
        let align = dev.alignment();
        let mut buf = AlignedBuf::new(align, align);
        dev.pread_exact_at(&mut buf, offset).unwrap();
        buf[0..4].copy_from_slice(&[0u8; 4]);
        dev.pwrite_all_at(&buf, offset).unwrap();

        let rebuilt = PrimaryBackend::rebuild_redb(&config, &*dev, &alloc)
            .expect("rebuild must tolerate the bad region");
        assert_eq!(rebuilt.len(), 9);
        assert!(rebuilt.lookup(&records[3].0).is_none());
    }

    #[test]
    fn rebuild_redb_matches_in_memory_rebuild() {
        let (dev, alloc, records) = setup_device_with_records(20);

        // Rebuild in-memory
        let mem = PrimaryBackend::rebuild(&*dev, &alloc).unwrap();

        // Rebuild redb
        let dir = tempfile::tempdir().unwrap();
        let config = redb_config(dir.path());
        let redb = PrimaryBackend::rebuild_redb(&config, &*dev, &alloc).unwrap();

        // Both should have the same entries
        assert_eq!(mem.len(), redb.len());
        for (key, offset) in &records {
            let mem_entry = mem.lookup(key).expect("mem should have key");
            let redb_entry = redb.lookup(key).expect("redb should have key");
            assert_eq!(mem_entry.record_offset, redb_entry.record_offset);
            assert_eq!(mem_entry.utxo_count, redb_entry.utxo_count);
            assert_eq!(mem_entry.record_offset, *offset);
        }
    }

    // -----------------------------------------------------------------------
    // In-memory snapshot/restore tests
    // -----------------------------------------------------------------------

    #[test]
    fn in_memory_snapshot_and_restore() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snap");

        // Create and populate an in-memory backend
        let mut backend = PrimaryBackend::new_in_memory(1000).unwrap();
        for i in 0..50u64 {
            backend.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        // Snapshot to disk
        backend.snapshot(&snap_path).unwrap();
        assert!(snap_path.exists());

        // Restore from snapshot
        let restored = PrimaryBackend::restore(&snap_path).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.backend_name(), "memory");

        for i in 0..50u64 {
            let e = restored
                .lookup(&make_key(i))
                .expect("entry should survive snapshot");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn in_memory_snapshot_all_and_restore_all() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("all.snap");

        // Create populated in-memory backend with secondary indexes
        let mut backend = PrimaryBackend::new_in_memory(1000).unwrap();
        for i in 0..20u64 {
            backend.register(make_key(i), make_entry(i * 100)).unwrap();
        }

        let mut dah_inner = DahIndex::new();
        dah_inner.insert(100, make_key(1));
        dah_inner.insert(200, make_key(2));
        let dah = DahBackend::InMemory(dah_inner);

        let mut unmined_inner = UnminedIndex::new();
        unmined_inner.insert(300, make_key(3));
        let unmined = UnminedBackend::InMemory(unmined_inner);

        // Snapshot all
        backend.snapshot_all(&dah, &unmined, &snap_path).unwrap();
        assert!(snap_path.exists());

        // Restore all
        let (restored, restored_dah, restored_unmined, _flags) =
            PrimaryBackend::restore_all(&snap_path).unwrap();

        assert_eq!(restored.len(), 20);
        assert_eq!(restored.backend_name(), "memory");
        assert_eq!(restored_dah.len(), 2);
        assert_eq!(restored_unmined.len(), 1);

        // Verify primary data
        for i in 0..20u64 {
            let e = restored
                .lookup(&make_key(i))
                .expect("entry should survive snapshot_all");
            assert_eq!(e.record_offset, i * 100);
        }

        // Verify secondary data
        let dah_result = restored_dah.range_query(200);
        assert_eq!(dah_result.len(), 2);
        let unmined_result = restored_unmined.range_query(300);
        assert_eq!(unmined_result.len(), 1);
    }

    #[test]
    fn restore_from_nonexistent_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("does_not_exist.snap");

        let result = PrimaryBackend::restore(&snap_path);
        // Restore reads the file; a missing file surfaces as IndexError::Io
        // (the #[from] std::io::Error) (N-LOW: assert the variant).
        assert!(
            matches!(result, Err(IndexError::Io(_))),
            "expected Io error for missing snapshot, got {result:?}",
        );
    }

    #[test]
    fn restore_all_from_nonexistent_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("does_not_exist.snap");

        let result = PrimaryBackend::restore_all(&snap_path);
        // Same as restore(): a missing file is an IndexError::Io. (The Ok
        // tuple is not Debug, so match on the error variant directly rather
        // than formatting the whole Result.)
        match result {
            Err(IndexError::Io(_)) => {}
            Err(other) => panic!("expected Io error for missing snapshot, got {other:?}"),
            Ok(_) => panic!("restore_all of a missing snapshot must fail"),
        }
    }

    #[test]
    fn both_backends_overwrite_same_key() {
        with_all_backends(|backend| {
            let key = make_key(1);
            backend.register(key, make_entry(100)).unwrap();
            backend.register(key, make_entry(200)).unwrap();

            assert_eq!(backend.len(), 1);
            let e = backend.lookup(&key).unwrap();
            assert_eq!(e.record_offset, 200);
        });
    }

    #[test]
    fn both_backends_unregister_then_reregister() {
        with_all_backends(|backend| {
            let key = make_key(1);
            backend.register(key, make_entry(100)).unwrap();
            backend.unregister(&key);
            assert!(backend.is_empty());

            backend.register(key, make_entry(999)).unwrap();
            assert_eq!(backend.len(), 1);
            let e = backend.lookup(&key).unwrap();
            assert_eq!(e.record_offset, 999);
        });
    }

    #[test]
    fn both_backends_update_cached_fields_batch() {
        with_all_backends(|backend| {
            for i in 0..5u64 {
                backend.register(make_key(i), make_entry(i * 100)).unwrap();
            }

            let updates: Vec<crate::index::CachedFieldsUpdate> = (0..5u64)
                .map(|i| crate::index::CachedFieldsUpdate {
                    key: make_key(i),
                    tx_flags: 0xAA,
                    block_entry_count: 3,
                    spent_utxos: (i as u32) * 10,
                    dah_or_preserve: 500,
                    unmined_since: 600,
                    generation: 42,
                })
                .collect();

            let updated = backend.update_cached_fields_batch(&updates).unwrap();
            assert_eq!(updated, 5);

            for i in 0..5u64 {
                let e = backend.lookup(&make_key(i)).unwrap();
                assert_eq!(e.tx_flags, 0xAA);
                assert_eq!(e.generation, 42);
                assert_eq!(e.record_offset, i * 100); // unchanged
            }
        });
    }

    #[test]
    fn both_backends_update_cached_fields_batch_empty() {
        with_all_backends(|backend| {
            backend.register(make_key(1), make_entry(100)).unwrap();
            let updated = backend.update_cached_fields_batch(&[]).unwrap();
            assert_eq!(updated, 0);
        });
    }

    #[test]
    fn both_backends_unregister_batch() {
        with_all_backends(|backend| {
            for i in 0..5u64 {
                backend.register(make_key(i), make_entry(i * 100)).unwrap();
            }

            let keys: Vec<_> = vec![make_key(1), make_key(2), make_key(99)];
            let results = backend.unregister_batch(&keys).unwrap();

            assert_eq!(results.len(), 3);
            assert!(results[0].is_some());
            assert!(results[1].is_some());
            assert!(results[2].is_none());
            assert_eq!(backend.len(), 3);
        });
    }

    #[test]
    fn both_backends_unregister_batch_empty() {
        with_all_backends(|backend| {
            backend.register(make_key(1), make_entry(100)).unwrap();
            let results = backend.unregister_batch(&[]).unwrap();
            assert!(results.is_empty());
            assert_eq!(backend.len(), 1);
        });
    }

    #[test]
    fn rebuild_secondary_from_device() {
        let (dev, alloc, _records) = setup_device_with_records(5);
        let (dah, unmined) = PrimaryBackend::rebuild_secondary(&*dev, &alloc).unwrap();

        // rebuild_secondary scans device for DAH/unmined metadata flags.
        // Our test records have no DAH/unmined flags set, so both should be empty.
        assert!(dah.is_empty());
        assert!(unmined.is_empty());
    }

    // -----------------------------------------------------------------------
    // FileBacked-specific tests
    // -----------------------------------------------------------------------

    #[test]
    fn file_backed_backend_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("primary.idx");
        let backend = PrimaryBackend::new_file_backed(&path, 16).unwrap();
        assert_eq!(backend.backend_name(), "file_backed");
    }

    #[test]
    fn file_backed_debug_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("primary.idx");
        let backend = PrimaryBackend::new_file_backed(&path, 16).unwrap();
        let debug = format!("{backend:?}");
        assert!(debug.contains("FileBacked"), "debug was: {debug}");
    }

    #[test]
    fn file_backed_restore_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("primary.idx");

        {
            let mut backend = PrimaryBackend::new_file_backed(&path, 1000).unwrap();
            for i in 0..50u64 {
                backend.register(make_key(i), make_entry(i * 100)).unwrap();
            }
        }

        let restored = PrimaryBackend::restore_file_backed(&path, 1000).unwrap();
        assert_eq!(restored.len(), 50);
        assert_eq!(restored.backend_name(), "file_backed");
        for i in 0..50u64 {
            let e = restored
                .lookup(&make_key(i))
                .expect("should survive reopen");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn file_backed_restore_missing_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.idx");
        let result = PrimaryBackend::restore_file_backed(&path, 1000);
        // N-LOW: the following match is the real variant check; the bare
        // assert!(result.is_err()) it replaced was redundant.
        match result.unwrap_err() {
            IndexError::FormatError { detail } => {
                assert!(detail.contains("not found"), "error was: {detail}");
            }
            other => panic!("expected FormatError, got {other:?}"),
        }
    }

    #[test]
    fn file_backed_snapshot_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("primary.idx");
        let mut backend = PrimaryBackend::new_file_backed(&path, 1000).unwrap();
        backend.register(make_key(1), make_entry(100)).unwrap();
        backend.snapshot(&dir.path().join("noop.snap")).unwrap();
    }

    #[test]
    fn file_backed_sync_method() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("primary.idx");
        let mut backend = PrimaryBackend::new_file_backed(&path, 1000).unwrap();
        backend.register(make_key(1), make_entry(100)).unwrap();
        backend.sync();
    }

    #[test]
    fn rebuild_file_backed_from_device() {
        let (dev, alloc, records) = setup_device_with_records(10);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("primary.idx");

        let rebuilt = PrimaryBackend::rebuild_file_backed(&path, &*dev, &alloc).unwrap();
        assert_eq!(rebuilt.len(), 10);
        assert_eq!(rebuilt.backend_name(), "file_backed");

        for (key, offset) in &records {
            let e = rebuilt.lookup(key).expect("record should be indexed");
            assert_eq!(e.record_offset, *offset);
            assert_eq!(e.utxo_count, 5);
        }
    }

    #[test]
    fn rebuild_file_backed_matches_in_memory() {
        let (dev, alloc, records) = setup_device_with_records(20);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("primary.idx");

        let mem = PrimaryBackend::rebuild(&*dev, &alloc).unwrap();
        let fb = PrimaryBackend::rebuild_file_backed(&path, &*dev, &alloc).unwrap();

        assert_eq!(mem.len(), fb.len());
        for (key, offset) in &records {
            let mem_entry = mem.lookup(key).expect("mem should have key");
            let fb_entry = fb.lookup(key).expect("fb should have key");
            assert_eq!(mem_entry.record_offset, fb_entry.record_offset);
            assert_eq!(mem_entry.utxo_count, fb_entry.utxo_count);
            assert_eq!(mem_entry.record_offset, *offset);
        }
    }
}
