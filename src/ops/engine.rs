//! Store engine — thread-safe coordinator for all UTXO operations.
//!
//! Owns the index, device, locks, and secondary indexes. Provides the
//! spend/unspend methods that are the public API for this phase.

use crate::allocator::SlotAllocator;
use crate::device::{AlignedBuf, BlockDevice};
use crate::index::{DahBackend, PrimaryBackend, TxIndexEntry, TxKey, UnminedBackend};
use crate::io;
use crate::locks::StripedLocks;
use crate::ops::create::*;
use crate::ops::delete_eval::{DahPatch, evaluate_delete_at_height};
use crate::ops::error::SpendError;
use crate::ops::mark_longest_chain::*;
use crate::ops::remaining::*;
use crate::ops::set_mined::*;
use crate::ops::signal::Signal;
use crate::ops::spend::*;
use crate::ops::unspend::*;
use crate::record::*;
use crate::storage::blobstore::BlobStore;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Thread-safe store engine for UTXO operations.
///
/// All mutation operations acquire a per-transaction stripe lock, ensuring
/// that concurrent operations on different transactions run in parallel
/// while operations on the same transaction are serialized.
///
/// # Atomic-apply invariant (F-G5-022 / A-4)
///
/// Each mutation entry point ([`Self::spend`], [`Self::unspend`],
/// [`Self::set_mined`] / `Self::set_mined_inner`, [`Self::set_locked`],
/// [`Self::create`], [`Self::delete`], [`Self::freeze`], [`Self::unfreeze`],
/// [`Self::reassign`], [`Self::mark_on_longest_chain`], etc.) acquires the
/// per-tx stripe mutex *as its first action* and holds it for the entire
/// read → validate → write → index-sync sequence. The mutex is released
/// only after every observable side effect (slot write, metadata write,
/// primary-index cache update, secondary-index two-phase update) has
/// landed.
///
/// Consequence: there is no validate-then-apply window where two
/// concurrent same-key spends could both observe `UTXO_UNSPENT` and both
/// commit. The reproduction test
/// `tests/g2_atomic_apply.rs::concurrent_spend_same_utxo_yields_exactly_one_winner`
/// runs 16 threads × 200 iterations against the same UTXO and asserts
/// exactly one `Ok` and `N-1` `AlreadySpent`. Any future refactor that
/// splits the validate-and-apply sequence across the lock boundary, or
/// downgrades the stripe mutex to an RwLock with shared validation, will
/// surface as ≥2 winners and a panicking test.
///
/// Read-only paths ([`Self::read_metadata`], [`Self::read_slot`],
/// [`Self::read_slots`], [`Self::read_block_entry`], [`Self::get_spend`])
/// intentionally skip the per-tx stripe mutex for throughput. Torn-read
/// safety on these paths comes from the record-keyed [`crate::locks::StripedRwLocks`]
/// table inside [`crate::io`] (F-X-007 / BC-02): every `*_direct` helper
/// acquires a record-level read guard while copying bytes off the device,
/// and every writer holds the corresponding write guard for the bulk
/// memcpy + CRC restamp. The CRC32 over `TxMetadata` does NOT provide
/// torn-read protection — its role is detecting on-disk corruption. The
/// regression test `direct_read_write_concurrent_stress_never_returns_torn_data`
/// (in `io.rs`) proves CRC alone is empirically insufficient on AArch64:
/// NEON memcpy can publish the new CRC bytes before the new field bytes,
/// so a concurrent reader can observe a CRC that validates against
/// partially-old state. See the F-X-007 / BC-02 commentary at the top of
/// [`crate::io`] for the full mechanism.
///
/// WARNING: the read-side `io_locks()` acquisitions in the `io.rs`
/// `*_direct` helpers MUST NOT be removed as a "redundant given CRC"
/// optimization — that exact reasoning was the original BC-02 contract
/// and it is disproven by the stress test above.
///
/// In addition, the `meta.tx_id == key.txid` re-check in
/// `read_metadata_for_key` (F-G2-001) defends against
/// `delete + create_at_offset` aliasing. Callers that need a
/// mutation-stable view (e.g. dispatch-side before-image capture for
/// replication compensation) MUST either already hold the appropriate
/// stripe lock or accept that the snapshot may be staler than the
/// committed engine state by the time their follow-up mutation runs.
pub struct Engine {
    device: Arc<dyn BlockDevice>,
    /// Raw pointer to device memory for zero-copy I/O on the hot path.
    /// `null_mut()` when the device does not support direct access (falls
    /// back to `pread`/`pwrite` with `AlignedBuf`).
    device_ptr: *mut u8,
    index: parking_lot::RwLock<PrimaryBackend>,
    allocator: parking_lot::Mutex<SlotAllocator>,
    locks: StripedLocks,
    dah_index: parking_lot::Mutex<DahBackend>,
    unmined_index: parking_lot::Mutex<UnminedBackend>,
    /// Per-engine visibility barrier used by TCP dispatch and checkpointing.
    ///
    /// Client-facing reads take this barrier so they cannot observe the local
    /// commit window between engine apply and failed-replication compensation.
    /// Checkpointing also takes it to fence against in-flight local mutations.
    /// The barrier deliberately lives on the engine, not in a process-global
    /// static, because integration tests and embedded deployments can host
    /// multiple independent nodes in one process.
    /// Read-shared, write-exclusive lock. Dispatches take the SHARED
    /// (read) side so they run concurrently with each other; the
    /// checkpoint task takes the EXCLUSIVE (write) side so it can
    /// snapshot a quiescent engine without waiting on individual op
    /// locks. Previously a plain `Mutex<()>` here serialized every
    /// dispatch through a single global mutex — a 100-op create batch
    /// thus ran sequentially, and replicas added a 3 s per-RPC stall
    /// because OP_REPLICA_BATCH also acquires this guard. RwLock keeps
    /// the checkpoint guarantee while restoring per-op parallelism.
    dispatch_visibility_barrier: parking_lot::RwLock<()>,
    /// Shared redo log used by secondary indexes for two-phase durability.
    ///
    /// When `Some`, the engine appends and fsyncs a
    /// [`RedoOp::SecondaryDahUpdate`] / [`RedoOp::SecondaryUnminedUpdate`]
    /// entry BEFORE committing the on-disk (redb) secondary index. This
    /// closes the window where a crash between redb commit and the caller's
    /// primary redo flush could leave the secondary index out of sync with
    /// the primary index. In-memory secondary indexes ignore the log — they
    /// are rebuilt on startup from the primary redo replay + device scan.
    redo_log: std::sync::OnceLock<Arc<parking_lot::Mutex<crate::redo::RedoLog>>>,
    blob_store: Option<Arc<dyn BlobStore>>,
    /// In-flight external-blob pins (F-IJ-002).
    ///
    /// The create dispatch pins a txid here BEFORE its blob digest check and
    /// releases the pin after the index registration; the periodic blob-GC
    /// sweep re-verifies "not pinned AND still unreferenced" under the pin
    /// stripe lock immediately before unlinking a candidate. Closes the
    /// TOCTOU where an aged blob (older than the F-G9-004 grace window) was
    /// deleted between a create's digest check and its registration.
    blob_pins: crate::storage::blobstore::BlobPinSet,
    /// Per-shard record counts for migration verification.
    ///
    /// Startup leaves these counters uninitialized so `Engine::new` does not
    /// scan the full primary index. The first `shard_record_count` call scans
    /// the primary index once and publishes all counters; create/delete then
    /// maintain them atomically while holding the primary index write lock.
    shard_counts: Vec<std::sync::atomic::AtomicU64>,
    /// True once `shard_counts` has been populated from the primary index.
    shard_counts_initialized: std::sync::atomic::AtomicBool,
    /// Cached wall-clock time in milliseconds since Unix epoch.
    ///
    /// Avoids a `clock_gettime` syscall on every mutation. The dispatch
    /// layer calls [`refresh_clock`] once per batch; individual operations
    /// read the cached value via [`Self::now_millis`].
    cached_millis: std::sync::atomic::AtomicU64,
    /// KO-5: count of conflicting-child appends dropped because the parent's
    /// on-disk children list was already at the `u8::MAX` (255) capacity.
    ///
    /// The 256th conflicting child of a parent cannot be recorded (the
    /// on-device count is a single `u8`), so the best-effort propagation
    /// wrapper records the loss here and escalates it to a `tracing::error!`
    /// instead of letting it vanish into a `warn`. Read via
    /// [`Self::conflicting_children_dropped`] so operators and tests can see
    /// that a cascade was truncated rather than discovering it only in the
    /// ops log.
    conflicting_children_dropped: std::sync::atomic::AtomicU64,
    /// Test-only fault injector: when set to `true`, the next call to
    /// [`Self::register_with_shard_count`] returns an error WITHOUT
    /// performing the backend `register` or incrementing `shard_counts`.
    /// This is the only way to exercise the "backend register failed"
    /// branch of the atomicity fix, since the in-memory hashtable backend
    /// has no intrinsic failure modes for fresh inserts.
    #[cfg(test)]
    fail_next_register: std::sync::atomic::AtomicBool,
}

// SAFETY (C-6): `Engine::device_ptr` is a raw `*mut u8` into an `Arc`'d
// device whose allocation outlives every `Engine` that holds the pointer
// (the `Arc<dyn BlockDevice>` is kept in `Engine::device`), so the pointer
// is never dangling. The `*mut u8` is the only non-`Send`/`Sync` field; all
// other fields are themselves `Send + Sync`. Sharing the pointer across
// threads is sound because EVERY access goes through the `io::*_direct`
// helpers, and concurrency on the bytes is serialized NOT by the engine's
// per-record `locks` (`StripedLocks`) — those do not cover the read or
// replica-receiver paths — but by:
//
//   1. `io::io_locks()`, a process-global per-record-offset `StripedRwLocks`
//      that every `read_metadata_direct` / `write_metadata_direct` (and the
//      slot variants) takes read- or write-side. This is what prevents torn
//      reads of the metadata footer + CRC pair (see the BC-02 / F-X-007
//      commentary in `crate::io`); the engine stripe locks alone do NOT,
//      because client reads and the replica receiver bypass them.
//   2. The `dispatch_visibility_barrier` (RwLock), which fences client-facing
//      reads out of the local apply → failed-replication-compensation window
//      and lets the checkpoint task snapshot a quiescent engine.
//   3. The read-path discipline documented on `Engine` above: a reader that
//      needs a mutation-stable view must hold the relevant stripe lock or
//      accept a possibly-stale snapshot — it must never assume the stripe
//      lock alone makes raw `device_ptr` access torn-read-safe.
//
// In short: `device_ptr` is valid for the engine's lifetime, and all access
// is mediated by `io_locks()` + the visibility barrier, so `Engine` is
// `Send + Sync`.
unsafe impl Send for Engine {}
unsafe impl Sync for Engine {}

impl Engine {
    fn external_ref_for_create(req: &CreateRequest) -> Result<Option<ExternalRef>, CreateError> {
        if !req.is_external {
            return Ok(None);
        }
        req.external_ref
            .map(Some)
            .ok_or(CreateError::MissingExternalRef)
    }

    /// Create a new engine with the given components.
    pub fn new(
        device: Arc<dyn BlockDevice>,
        index: impl Into<PrimaryBackend>,
        allocator: SlotAllocator,
        locks: StripedLocks,
        dah_index: impl Into<DahBackend>,
        unmined_index: impl Into<UnminedBackend>,
    ) -> Self {
        let device_ptr = device.as_raw_ptr().unwrap_or(std::ptr::null_mut());
        let index = index.into();
        let shard_count_capacity = crate::cluster::shards::NUM_SHARDS;
        let shard_counts: Vec<std::sync::atomic::AtomicU64> = (0..shard_count_capacity)
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect();
        Self {
            device,
            device_ptr,
            index: parking_lot::RwLock::new(index),
            allocator: parking_lot::Mutex::new(allocator),
            locks,
            dah_index: parking_lot::Mutex::new(dah_index.into()),
            unmined_index: parking_lot::Mutex::new(unmined_index.into()),
            dispatch_visibility_barrier: parking_lot::RwLock::new(()),
            redo_log: std::sync::OnceLock::new(),
            blob_store: None,
            blob_pins: crate::storage::blobstore::BlobPinSet::new(),
            shard_counts,
            shard_counts_initialized: std::sync::atomic::AtomicBool::new(false),
            cached_millis: std::sync::atomic::AtomicU64::new(sys_millis()),
            conflicting_children_dropped: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            fail_next_register: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Attach a redo log for secondary-index two-phase durability.
    ///
    /// Once attached, every on-disk (redb) secondary index mutation appends
    /// and fsyncs an intent record to the redo log BEFORE committing the
    /// redb transaction. Call this after constructing the engine and before
    /// accepting client traffic. The same redo log handle used by the
    /// dispatch layer for primary-op durability should be passed here so
    /// that primary and secondary entries share a single log.
    pub fn set_redo_log(&self, redo_log: Arc<parking_lot::Mutex<crate::redo::RedoLog>>) {
        if self.redo_log.set(redo_log).is_err() {
            tracing::warn!("engine redo log already attached; ignoring replacement");
        }
    }

    /// Clone the engine's redo log handle for use as an `Option<&Mutex<_>>`
    /// in secondary index calls.
    fn redo_log_handle(&self) -> Option<Arc<parking_lot::Mutex<crate::redo::RedoLog>>> {
        self.redo_log.get().cloned()
    }

    /// Public accessor for the engine's redo log handle.
    ///
    /// Used by the replication receiver (R-034) so replica-applied
    /// mutations can also be journaled to the local redo log. Without
    /// this, a master crash followed by failover would require a full
    /// resync of every replica because replica recovery would have no
    /// log to replay.
    ///
    /// Returns `None` when no redo log has been attached (test paths,
    /// unconfigured deployments).
    pub fn redo_log(&self) -> Option<Arc<parking_lot::Mutex<crate::redo::RedoLog>>> {
        self.redo_log_handle()
    }

    /// Acquire the SHARED (read-side) dispatch visibility barrier — used
    /// by client READ ops so they run concurrently with each other while
    /// remaining mutually exclusive with mutations, replica-batch applies,
    /// and checkpoints (all of which take the EXCLUSIVE side).
    ///
    /// The barrier discipline preserves the "no observable rollback
    /// window" invariant: a client read MUST NOT observe a mutation that
    /// the master might still compensate. Mutations therefore hold the
    /// exclusive side from before-apply through replication-ack /
    /// compensation, and reads are blocked for the full window. Among
    /// themselves, reads are concurrent.
    pub(crate) fn acquire_dispatch_visibility_guard(
        &self,
    ) -> parking_lot::RwLockReadGuard<'_, ()> {
        self.dispatch_visibility_barrier.read()
    }

    /// Acquire the EXCLUSIVE (write-side) dispatch visibility barrier —
    /// used by mutation ops and `OP_REPLICA_BATCH` apply so the apply +
    /// replicate + (optional) compensation window cannot be observed by
    /// concurrent reads. Also used by the checkpoint task to drain
    /// in-flight dispatches before snapshotting a quiescent engine.
    pub(crate) fn acquire_mutation_visibility_guard(
        &self,
    ) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.dispatch_visibility_barrier.write()
    }

    /// Backwards-compatible alias of [`Self::acquire_mutation_visibility_guard`]
    /// kept for the checkpoint task, which needs the same exclusive side
    /// but for a different reason (snapshot quiescence). The two callers
    /// share a lock so a checkpoint cannot start mid-mutation, and a
    /// mutation cannot start mid-checkpoint.
    pub(crate) fn acquire_checkpoint_visibility_guard(
        &self,
    ) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.dispatch_visibility_barrier.write()
    }

    /// Update the DAH secondary index with two-phase durability.
    ///
    /// Emits a transition from `old_height` to `new_height` (either may be
    /// zero). When the engine has a redo log attached, the intent record is
    /// fsynced before the redb commit. Errors from the redo flush or redb
    /// commit are mapped to [`SpendError::StorageError`].
    fn update_dah_index(
        &self,
        key: &TxKey,
        old_height: u32,
        new_height: u32,
    ) -> Result<(), SpendError> {
        if old_height == new_height {
            return Ok(());
        }
        let log_arc = self.redo_log_handle();
        let log_ref = log_arc.as_deref();
        let mut dah = self.dah_index.lock();
        if old_height != 0 {
            dah.remove(key, log_ref)
                .map_err(|e| SpendError::StorageError {
                    detail: format!("dah secondary remove: {e}"),
                })?;
        }
        if new_height != 0 {
            dah.insert(new_height, *key, log_ref)
                .map_err(|e| SpendError::StorageError {
                    detail: format!("dah secondary insert: {e}"),
                })?;
        }
        Ok(())
    }

    /// Update the unmined secondary index with two-phase durability.
    fn update_unmined_index(
        &self,
        key: &TxKey,
        old_height: u32,
        new_height: u32,
    ) -> Result<(), SpendError> {
        if old_height == new_height {
            return Ok(());
        }
        let log_arc = self.redo_log_handle();
        let log_ref = log_arc.as_deref();
        let mut unmined = self.unmined_index.lock();
        if old_height != 0 {
            unmined
                .remove(key, log_ref)
                .map_err(|e| SpendError::StorageError {
                    detail: format!("unmined secondary remove: {e}"),
                })?;
        }
        if new_height != 0 {
            unmined
                .insert(new_height, *key, log_ref)
                .map_err(|e| SpendError::StorageError {
                    detail: format!("unmined secondary insert: {e}"),
                })?;
        }
        Ok(())
    }

    /// Apply a combined DAH + unmined update with a single redo fsync.
    ///
    /// When both secondary indexes change in the same operation (e.g.
    /// `mark_on_longest_chain`), this batches both intent records into one
    /// `RedoLog::append_batch_and_flush` so there is exactly one fsync for
    /// the pair. Both redb commits then follow.
    fn update_both_secondary_indexes(
        &self,
        key: &TxKey,
        old_dah: u32,
        new_dah: u32,
        old_unmined: u32,
        new_unmined: u32,
    ) -> Result<(), SpendError> {
        let dah_changed = old_dah != new_dah;
        let unmined_changed = old_unmined != new_unmined;
        if !dah_changed && !unmined_changed {
            return Ok(());
        }

        let log_arc = self.redo_log_handle();

        // Phase 1: one fsync covering both secondary intents (if both change).
        if let Some(ref log) = log_arc {
            let mut ops = Vec::with_capacity(2);
            if dah_changed {
                ops.push(crate::redo::RedoOp::SecondaryDahUpdate {
                    tx_key: *key,
                    old_height: old_dah,
                    new_height: new_dah,
                });
            }
            if unmined_changed {
                ops.push(crate::redo::RedoOp::SecondaryUnminedUpdate {
                    tx_key: *key,
                    old_height: old_unmined,
                    new_height: new_unmined,
                });
            }
            let mut guard = log.lock();
            guard
                .append_batch_and_flush(&ops)
                .map_err(|e| SpendError::StorageError {
                    detail: format!("secondary batch append_and_flush: {e}"),
                })?;
        }

        // Phase 2: commit both redb transactions. The redo log already has the
        // durable record; recovery replay handles any redb commit failure.
        if dah_changed {
            let mut dah = self.dah_index.lock();
            if old_dah != 0 {
                dah.remove(key, None)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("dah secondary remove (post-fsync): {e}"),
                    })?;
            }
            if new_dah != 0 {
                dah.insert(new_dah, *key, None)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("dah secondary insert (post-fsync): {e}"),
                    })?;
            }
        }
        if unmined_changed {
            let mut unmined = self.unmined_index.lock();
            if old_unmined != 0 {
                unmined
                    .remove(key, None)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("unmined secondary remove (post-fsync): {e}"),
                    })?;
            }
            if new_unmined != 0 {
                unmined
                    .insert(new_unmined, *key, None)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("unmined secondary insert (post-fsync): {e}"),
                    })?;
            }
        }
        Ok(())
    }

    /// Atomically update the primary in-memory cache AND both secondary
    /// indexes under a single critical section.
    ///
    /// This is the reorg-safe mutation path used by `mark_on_longest_chain`
    /// (and any other op that moves both `unmined_since` and
    /// `delete_at_height` simultaneously). Ordering:
    ///
    /// 1. Redo log: append DAH + unmined intents in one batch, single fsync.
    /// 2. Acquire the primary index write lock, then DAH, then unmined.
    ///    This matches the project-wide acquisition order used by
    ///    [`Engine::snapshot_index`] and the set_mined fast path
    ///    (index → dah → unmined), so the three operations can never
    ///    deadlock against each other.
    /// 3. Apply the primary in-memory cache update
    ///    (`update_cached_fields`) while both secondary mutexes are also
    ///    held, so any reader that consults a secondary index and then
    ///    cross-checks the primary (which requires the index read lock,
    ///    forcing it to wait for the write lock to drop) observes a
    ///    consistent pair (H1).
    /// 4. Apply the DAH redb mutation.
    /// 5. Apply the unmined redb mutation.
    /// 6. Release all locks.
    ///
    /// Because any reader that wants to consult a secondary index and
    /// then cross-check the primary MUST acquire the secondary mutex
    /// first, holding both secondary mutexes across the primary update
    /// closes the window where a reader could observe a primary whose
    /// `unmined_since` moved while the DAH still references the old
    /// height.
    fn sync_primary_and_both_secondary_atomic(
        &self,
        key: &TxKey,
        metadata: &TxMetadata,
        old_dah: u32,
        new_dah: u32,
        old_unmined: u32,
        new_unmined: u32,
    ) -> Result<(), SpendError> {
        let dah_changed = old_dah != new_dah;
        let unmined_changed = old_unmined != new_unmined;

        // Phase 1: one fsync covering both secondary intents (if any change).
        let log_arc = self.redo_log_handle();
        if (dah_changed || unmined_changed) && log_arc.is_some() {
            let mut ops = Vec::with_capacity(2);
            if dah_changed {
                ops.push(crate::redo::RedoOp::SecondaryDahUpdate {
                    tx_key: *key,
                    old_height: old_dah,
                    new_height: new_dah,
                });
            }
            if unmined_changed {
                ops.push(crate::redo::RedoOp::SecondaryUnminedUpdate {
                    tx_key: *key,
                    old_height: old_unmined,
                    new_height: new_unmined,
                });
            }
            if let Some(ref log) = log_arc {
                let mut guard = log.lock();
                guard
                    .append_batch_and_flush(&ops)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("atomic primary+secondary batch append_and_flush: {e}"),
                    })?;
            }
        }

        // Phase 2: lock order = primary.write → dah → unmined (matches
        // Engine::snapshot_index and the set_mined fast path).
        //
        // Inline the primary cache update here rather than calling
        // `sync_index_cache` so the write guard is held across the
        // secondary mutations — any secondary reader that tries to
        // cross-check the primary will have to wait for our index write
        // to drop, and by then the dah/unmined mutations are durable.
        let preserve = { metadata.preserve_until };
        let meta_dah = { metadata.delete_at_height };
        let has_preserve = preserve != 0;
        let dah_or_preserve = if has_preserve { preserve } else { meta_dah };
        let mut tf = metadata.flags.bits();
        if has_preserve {
            tf |= TxFlags::HAS_PRESERVE_UNTIL.bits();
        } else {
            tf &= !TxFlags::HAS_PRESERVE_UNTIL.bits();
        }
        let mut primary_guard = self.index.write();
        primary_guard
            .update_cached_fields(
                key,
                tf,
                metadata.block_entry_count,
                metadata.spent_utxos,
                dah_or_preserve,
                metadata.unmined_since,
                metadata.generation,
            )
            .map_err(|e| SpendError::StorageError {
                detail: format!("index update_cached_fields failed: {e}"),
            })?;

        let mut dah = self.dah_index.lock();
        let mut unmined = self.unmined_index.lock();

        if dah_changed {
            if old_dah != 0 {
                dah.remove(key, None)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("atomic dah remove: {e}"),
                    })?;
            }
            if new_dah != 0 {
                dah.insert(new_dah, *key, None)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("atomic dah insert: {e}"),
                    })?;
            }
        }

        if unmined_changed {
            if old_unmined != 0 {
                unmined
                    .remove(key, None)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("atomic unmined remove: {e}"),
                    })?;
            }
            if new_unmined != 0 {
                unmined
                    .insert(new_unmined, *key, None)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("atomic unmined insert: {e}"),
                    })?;
            }
        }

        drop(unmined);
        drop(dah);
        drop(primary_guard);

        Ok(())
    }

    /// Restore migrated lifecycle metadata, keeping the secondary indexes and
    /// primary-index cached fields consistent with the on-device footer.
    ///
    /// The baseline migration path streams a record via [`Self::create`] with
    /// `block_height = 0`, then must replay the master's real lifecycle state
    /// (`generation`, `updated_at`, `unmined_since`, `delete_at_height`,
    /// `preserve_until`). Patching the device footer alone (raw
    /// `io::write_metadata`) left three derived structures stale on the live
    /// migration target until its next restart:
    ///
    /// - the **unmined** secondary index (never inserted for unmined records),
    /// - the **DAH** secondary index (never inserted for records with a pending
    ///   delete-at-height),
    /// - the **primary-index cached fields** (`unmined_since`,
    ///   `dah_or_preserve`, `generation`, `HAS_PRESERVE_UNTIL`).
    ///
    /// This entry point writes the lifecycle fields to the device footer and
    /// then routes the index updates through
    /// [`Self::sync_primary_and_both_secondary_atomic`] — the same helper the
    /// normal mutation path uses — so the DAH index, unmined index, and primary
    /// cached fields all land for migrated records exactly as for locally
    /// created ones. Mined records (`unmined_since == 0`,
    /// `delete_at_height == 0`, `preserve_until == 0`) are handled correctly:
    /// no secondary entries are created.
    ///
    /// The "old" heights for the secondary transitions are read from the
    /// current on-device footer, so this is also correct when the create path
    /// replaced a pre-existing record that already carried DAH/unmined state.
    ///
    /// # Errors
    ///
    /// Returns [`SpendError::TxNotFound`] if the key is absent from the primary
    /// index, or [`SpendError::StorageError`] if the device write, redo fsync,
    /// or any index mutation fails. The caller MUST propagate the error so the
    /// migration batch is NACKed rather than ACKed with a divergent target.
    pub fn restore_migrated_lifecycle(
        &self,
        key: &TxKey,
        generation: u32,
        updated_at: u64,
        unmined_since: u32,
        delete_at_height: u32,
        preserve_until: u32,
    ) -> Result<(), SpendError> {
        let _guard = self.locks.lock(key);
        let entry = self
            .index
            .read()
            .lookup_checked(key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let mut meta = self.read_metadata_for_key(key, entry.record_offset)?;

        // Old secondary heights come from the current footer so a create that
        // replaced an existing record (with prior DAH/unmined state) transitions
        // cleanly rather than leaking a stale index entry.
        let old_unmined = { meta.unmined_since };
        let old_dah = { meta.delete_at_height };

        meta.generation = generation;
        meta.updated_at = updated_at;
        meta.unmined_since = unmined_since;
        meta.delete_at_height = delete_at_height;
        meta.preserve_until = preserve_until;

        self.write_metadata_fast(entry.record_offset, &meta)?;

        self.sync_primary_and_both_secondary_atomic(
            key,
            &meta,
            old_dah,
            delete_at_height,
            old_unmined,
            unmined_since,
        )
    }

    /// Refresh the cached wall-clock time from the system clock.
    ///
    /// Call this once per request batch in the dispatch layer so that all
    /// operations within the batch share the same timestamp without
    /// issuing individual `clock_gettime` syscalls.
    pub fn refresh_clock(&self) {
        self.cached_millis
            .store(sys_millis(), std::sync::atomic::Ordering::SeqCst);
    }

    /// Read the cached wall-clock time (milliseconds since Unix epoch).
    pub(crate) fn now_millis(&self) -> u64 {
        self.cached_millis.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Set the blobstore for external cold data storage.
    ///
    /// This is an initialization hook. Call it before wrapping the engine in
    /// an `Arc` and before accepting client traffic; runtime reconfiguration
    /// is intentionally not supported because blob references must remain
    /// stable for already-created external records.
    pub fn set_blob_store(&mut self, store: Arc<dyn BlobStore>) {
        self.blob_store = Some(store);
    }

    /// Get allocator statistics for observability.
    ///
    /// Locks the allocator briefly to compute the snapshot.
    pub fn allocator_stats(&self) -> crate::allocator::AllocatorStats {
        self.allocator.lock().stats()
    }

    /// Get a reference to the allocator mutex.
    ///
    /// Used by the dispatch layer to free pre-allocated space when a redo
    /// flush fails after [`Self::pre_allocate_create`] succeeded.
    pub fn allocator(&self) -> &parking_lot::Mutex<SlotAllocator> {
        &self.allocator
    }

    /// Get a reference to the blobstore, if configured.
    pub fn blob_store(&self) -> Option<&dyn BlobStore> {
        self.blob_store.as_deref()
    }

    /// In-flight external-blob pin set (F-IJ-002).
    ///
    /// Create dispatch: pin the txid BEFORE the blob digest check and hold
    /// the guard until after index registration (drop on every failure
    /// path). Blob-GC: route every candidate unlink through
    /// [`crate::storage::blobstore::BlobPinSet::delete_orphan_guarded`].
    pub fn blob_pins(&self) -> &crate::storage::blobstore::BlobPinSet {
        &self.blob_pins
    }

    /// Get the record count for a shard.
    ///
    /// The first call after startup lazily initializes all shard counters by
    /// scanning the primary index once. Subsequent calls are O(1) and
    /// lock-free.
    pub fn shard_record_count(&self, shard: u16) -> u64 {
        let counter = &self.shard_counts[shard as usize];
        if !self
            .shard_counts_initialized
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.initialize_shard_counts();
        }
        counter.load(std::sync::atomic::Ordering::Acquire)
    }

    fn initialize_shard_counts(&self) {
        if self
            .shard_counts_initialized
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }

        let guard = self.index.read();
        if self
            .shard_counts_initialized
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }

        let mut counts = vec![0u64; crate::cluster::shards::NUM_SHARDS];
        for (key, _) in guard.iter() {
            let shard = crate::cluster::shards::ShardTable::shard_for_key(&key) as usize;
            counts[shard] += 1;
        }
        for (counter, count) in self.shard_counts.iter().zip(counts) {
            counter.store(count, std::sync::atomic::Ordering::Relaxed);
        }
        self.shard_counts_initialized
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    fn shard_counts_initialized_for_test(&self) -> bool {
        self.shard_counts_initialized
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Register a primary-index entry and, if shard counters have been
    /// initialized, increment the matching shard count atomically within the
    /// same index write-lock critical section only when this is a new key.
    ///
    /// Before lazy initialization, count mutations are intentionally skipped:
    /// the first `shard_record_count` call will scan the primary index after
    /// any active writer drops this lock. After initialization, this guarantees
    /// `shard_counts` never drifts from the primary index: if backend
    /// `register` fails, no count mutation is observed; if it succeeds with a
    /// newly inserted key, the matching `fetch_add` executes before the write
    /// lock is released.
    ///
    /// # Errors
    /// Returns [`IndexError`](crate::index::IndexError) from the underlying
    /// primary backend. On error, `shard_counts` is left unchanged.
    fn register_with_shard_count(
        &self,
        key: TxKey,
        entry: TxIndexEntry,
    ) -> Result<(), crate::index::IndexError> {
        // Test-only fault injection: consume the flag and short-circuit
        // BEFORE touching the backend so we can verify that a failed
        // register leaves `shard_counts` untouched.
        #[cfg(test)]
        {
            if self
                .fail_next_register
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(crate::index::IndexError::FormatError {
                    detail: "injected register failure (test-only)".into(),
                });
            }
        }
        let shard = crate::cluster::shards::ShardTable::shard_for_key(&key) as usize;
        let resize_target = {
            let mut guard = self.index.write();
            let len_before = guard.len();
            guard.register_without_resize(key, entry)?;
            let inserted = guard.len() > len_before;
            // Commit the count mutation BEFORE releasing the write lock once
            // counters have been lazily initialized. Before that point, the first
            // reader will scan the primary index after this write lock releases.
            let counts_initialized = self
                .shard_counts_initialized
                .load(std::sync::atomic::Ordering::Acquire);
            if inserted && counts_initialized {
                self.shard_counts[shard].fetch_add(1, std::sync::atomic::Ordering::Release);
            }
            guard.resize_target_capacity()
        };
        if let Some(target_capacity) = resize_target {
            self.resize_primary_index_without_blocking_readers(target_capacity)?;
        }
        Ok(())
    }

    /// Register a primary-index entry only if `key` is not already present,
    /// incrementing the matching shard count exactly like
    /// [`Self::register_with_shard_count`].
    ///
    /// The existence check and the insert run inside the same primary-index
    /// write-lock critical section, so check-then-insert can never interleave
    /// with another writer. This matters because the backend `insert`
    /// silently OVERWRITES an existing key (`hashtable.rs` `insert` returns
    /// the old entry): a non-atomic lookup-then-register on the create path
    /// let two concurrent creates of the same txid both succeed, orphaning
    /// one record and leaking its allocation (audit A — create duplicate
    /// guard). Mirrors the F-G3-013 hardening on `remove()`: the decision is
    /// made inside the table's critical section, never from a stale earlier
    /// read.
    ///
    /// Returns `Ok(true)` if the entry was inserted, `Ok(false)` if the key
    /// already exists (the index and shard counts are left unmodified).
    ///
    /// # Errors
    /// Returns [`IndexError`](crate::index::IndexError) from the underlying
    /// primary backend. On error, neither the index nor `shard_counts` is
    /// modified.
    fn register_new_with_shard_count(
        &self,
        key: TxKey,
        entry: TxIndexEntry,
    ) -> Result<bool, crate::index::IndexError> {
        // Test-only fault injection — same contract as
        // `register_with_shard_count` (a failed register leaves the index
        // and `shard_counts` untouched).
        #[cfg(test)]
        {
            if self
                .fail_next_register
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(crate::index::IndexError::FormatError {
                    detail: "injected register failure (test-only)".into(),
                });
            }
        }
        let shard = crate::cluster::shards::ShardTable::shard_for_key(&key) as usize;
        let resize_target = {
            let mut guard = self.index.write();
            // Reject-not-overwrite: the only safe insert-if-absent is a
            // check under the same write lock that performs the insert.
            if guard.lookup_checked(&key)?.is_some() {
                return Ok(false);
            }
            let len_before = guard.len();
            guard.register_without_resize(key, entry)?;
            debug_assert!(
                guard.len() > len_before,
                "register_new_with_shard_count: insert did not grow the index \
                 despite the key being absent under the same write lock"
            );
            let counts_initialized = self
                .shard_counts_initialized
                .load(std::sync::atomic::Ordering::Acquire);
            if counts_initialized {
                self.shard_counts[shard].fetch_add(1, std::sync::atomic::Ordering::Release);
            }
            guard.resize_target_capacity()
        };
        if let Some(target_capacity) = resize_target {
            self.resize_primary_index_without_blocking_readers(target_capacity)?;
        }
        Ok(true)
    }

    /// Return a freshly allocated create region to the allocator after the
    /// create failed before (or at) index registration.
    ///
    /// Best-effort: a failure to free is logged rather than propagated — the
    /// caller is already on an error path and the original error is the one
    /// it must surface. The record bytes at `record_offset` are unreachable
    /// (no index entry points at them), so the worst case of a failed free
    /// is the same leaked region this call is trying to reclaim.
    fn free_create_allocation_best_effort(&self, record_offset: u64, total_size: u64) {
        if let Err(e) = self.allocator.lock().free(record_offset, total_size) {
            tracing::error!(
                target: "teraslab::engine",
                record_offset,
                total_size,
                err = %e,
                "create rollback: failed to free reserved region; device space leaked",
            );
        }
    }

    fn resize_primary_index_without_blocking_readers(
        &self,
        requested_capacity: usize,
    ) -> Result<(), crate::index::IndexError> {
        let guard = self.index.upgradable_read();
        let Some(target_capacity) = guard
            .resize_target_capacity()
            .map(|target| target.max(requested_capacity))
        else {
            return Ok(());
        };

        let resized = guard.resized_copy(target_capacity)?;
        let mut write_guard = parking_lot::RwLockUpgradableReadGuard::upgrade(guard);
        // G-2: the resized copy renamed a fresh file over the backing path
        // and now owns it. Mark the displaced index defunct before the swap
        // so its `Drop` does not write the clean-shutdown sentinel for a
        // path it no longer owns (which would disable torn-write detection
        // for the rest of uptime).
        write_guard.mark_defunct_for_resize();
        *write_guard = resized;
        Ok(())
    }

    /// Unregister a primary-index entry and, if shard counters have been
    /// initialized, decrement the matching shard count atomically within the
    /// same index write-lock critical section.
    ///
    /// Returns the removed entry (or `None` if the key was not present), or
    /// an [`IndexError`] if the (redb) backend's write transaction fails.
    /// After lazy initialization, the shard count is only decremented when an
    /// entry was actually removed. Before initialization, the first
    /// `shard_record_count` call will scan the primary index after any active
    /// writer drops this lock.
    ///
    /// G-4: propagates the backend error instead of collapsing it to `None`.
    /// A collapsed `unregister` failure would leave the row in redb while the
    /// caller proceeds to free the device region and remove secondary
    /// entries — a torn delete.
    fn unregister_with_shard_count(
        &self,
        key: &TxKey,
    ) -> Result<Option<TxIndexEntry>, crate::index::IndexError> {
        let shard = crate::cluster::shards::ShardTable::shard_for_key(key) as usize;
        let mut guard = self.index.write();
        let removed = guard.unregister_checked(key)?;
        if removed.is_some()
            && self
                .shard_counts_initialized
                .load(std::sync::atomic::Ordering::Acquire)
        {
            self.shard_counts[shard].fetch_sub(1, std::sync::atomic::Ordering::Release);
        }
        drop(guard);
        Ok(removed)
    }

    // -----------------------------------------------------------------------
    // Fast-path I/O helpers: direct memory when available, pread/pwrite fallback
    // -----------------------------------------------------------------------

    /// Read metadata from device, using direct memory access when available.
    #[inline(always)]
    fn read_metadata_fast(
        &self,
        record_offset: u64,
    ) -> std::result::Result<TxMetadata, SpendError> {
        if !self.device_ptr.is_null() {
            // SAFETY: the enclosing `is_null` check guarantees `device_ptr`
            // is the live base pointer of this engine's `Arc`'d device (which
            // outlives the engine). `record_offset` is an allocator-issued,
            // in-bounds record offset. `read_metadata_direct` takes the
            // per-offset `io_locks()` read side internally, so this read is
            // serialized against concurrent direct writers (no torn read).
            unsafe { io::read_metadata_direct(self.device_ptr, record_offset) }.map_err(|e| {
                SpendError::StorageError {
                    detail: format!("{e}"),
                }
            })
        } else {
            io::read_metadata(&*self.device, record_offset).map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })
        }
    }

    /// Read metadata from device and verify it matches the requested transaction.
    ///
    /// F-G2-001 defense-in-depth: the lock-free read paths (`read_metadata`,
    /// `read_slot`, `read_slots`, `read_block_entry`, `get_spend`,
    /// `read_cold_data`) all resolve `TxKey → record_offset` via the primary
    /// index and then dereference the offset on the device without holding the
    /// per-tx stripe lock. If a concurrent `delete` re-orders its index
    /// unregistration against the allocator free (or any future refactor
    /// regresses that ordering), a different transaction's metadata can sit at
    /// the same offset with a valid CRC. Reading it back would silently
    /// satisfy the original lookup with unrelated data.
    ///
    /// This helper closes the gap by comparing `meta.tx_id` against
    /// `key.txid` after the read. A mismatch is surfaced as `TxNotFound` —
    /// the same answer the caller would have received had they observed the
    /// post-unregister state of the primary index.
    #[inline]
    fn read_metadata_for_key(
        &self,
        key: &TxKey,
        record_offset: u64,
    ) -> std::result::Result<TxMetadata, SpendError> {
        let meta = self.read_metadata_fast(record_offset)?;
        if meta.tx_id != key.txid {
            return Err(SpendError::TxNotFound);
        }
        Ok(meta)
    }

    /// Write metadata to device, using direct memory access when available.
    #[inline(always)]
    fn write_metadata_fast(
        &self,
        record_offset: u64,
        metadata: &TxMetadata,
    ) -> std::result::Result<(), SpendError> {
        if !self.device_ptr.is_null() {
            // SAFETY: `device_ptr` is non-null (checked above) and is the
            // live base of this engine's owned device; `record_offset` is an
            // allocator-issued in-bounds offset. `write_metadata_direct`
            // takes the per-offset `io_locks()` write side and publishes the
            // footer+CRC via the chunked atomic transfer, so concurrent
            // direct readers never observe a torn header.
            unsafe { io::write_metadata_direct(self.device_ptr, record_offset, metadata) };
            Ok(())
        } else {
            io::write_metadata(&*self.device, record_offset, metadata).map_err(|e| {
                SpendError::StorageError {
                    detail: format!("{e}"),
                }
            })
        }
    }

    fn write_zeroed_metadata_header(
        &self,
        record_offset: u64,
    ) -> std::result::Result<(), SpendError> {
        if !self.device_ptr.is_null() {
            // SAFETY: `device_ptr` is non-null (checked above) and points to
            // this engine's owned device region; `record_offset` is an
            // allocator-aligned, in-bounds record offset, so
            // `device_ptr.add(record_offset)` is in-bounds and writing
            // `METADATA_SIZE` zero bytes from it stays within the record. The
            // caller holds the record's stripe lock for this tombstone write
            // (delete path), and the following `Release` fence publishes the
            // zeroing to other threads.
            unsafe {
                std::ptr::write_bytes(
                    self.device_ptr.add(record_offset as usize),
                    0,
                    METADATA_SIZE,
                );
            }
            std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
            Ok(())
        } else {
            let align = self.device.alignment();
            let aligned_base = record_offset / align as u64 * align as u64;
            let intra_offset = (record_offset - aligned_base) as usize;
            let total_size = io::align_up(intra_offset + METADATA_SIZE, align);

            let mut buf = AlignedBuf::new(total_size, align);
            if intra_offset != 0 || !METADATA_SIZE.is_multiple_of(align) {
                self.device
                    .pread_exact_at(&mut buf, aligned_base)
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("{e}"),
                    })?;
            }
            buf[intra_offset..intra_offset + METADATA_SIZE].fill(0);
            self.device
                .pwrite_all_at(&buf, aligned_base)
                .map_err(|e| SpendError::StorageError {
                    detail: format!("{e}"),
                })
        }
    }

    /// Read a UTXO slot, using direct memory access when available.
    #[inline(always)]
    fn read_slot_fast(
        &self,
        record_offset: u64,
        slot_index: u32,
    ) -> std::result::Result<UtxoSlot, SpendError> {
        if !self.device_ptr.is_null() {
            // SAFETY: `device_ptr` is non-null (checked above) and live for
            // the engine's lifetime; `record_offset` + `slot_index` address
            // an allocator-valid slot within the record.
            // `read_utxo_slot_direct` takes the per-offset `io_locks()` read
            // side, serializing against concurrent direct writers.
            unsafe { io::read_utxo_slot_direct(self.device_ptr, record_offset, slot_index) }
                .map_err(|e| SpendError::StorageError {
                    detail: format!("{e}"),
                })
        } else {
            io::read_utxo_slot(&*self.device, record_offset, slot_index).map_err(|e| {
                SpendError::StorageError {
                    detail: format!("{e}"),
                }
            })
        }
    }

    /// Write a UTXO slot, using direct memory access when available.
    #[inline(always)]
    fn write_slot_fast(
        &self,
        record_offset: u64,
        slot_index: u32,
        slot: &UtxoSlot,
    ) -> std::result::Result<(), SpendError> {
        if !self.device_ptr.is_null() {
            // SAFETY: `device_ptr` is non-null (checked above) and live for
            // the engine's lifetime; `record_offset` + `slot_index` address
            // an allocator-valid slot within the record.
            // `write_utxo_slot_direct` takes the per-offset `io_locks()`
            // write side, so the slot publish is serialized against
            // concurrent direct readers/writers.
            unsafe { io::write_utxo_slot_direct(self.device_ptr, record_offset, slot_index, slot) };
            Ok(())
        } else {
            io::write_utxo_slot(&*self.device, record_offset, slot_index, slot).map_err(|e| {
                SpendError::StorageError {
                    detail: format!("{e}"),
                }
            })
        }
    }

    /// Update the cached fields in the primary index entry after a mutation.
    /// Acquires a brief write lock on the index.
    ///
    /// Encodes `preserve_until` / `delete_at_height` into the shared
    /// `dah_or_preserve` field with the `HAS_PRESERVE_UNTIL` discriminant bit.
    ///
    /// Returns an error if the index backend fails to persist the update
    /// (only possible for the on-disk redb backend). Callers MUST propagate
    /// the error: a silent failure here would leave the primary-index
    /// durability-critical fields (DAH, `unmined_since`, `generation`) out of
    /// sync with the on-device metadata footer.
    #[inline]
    fn sync_index_cache(&self, key: &TxKey, metadata: &TxMetadata) -> Result<(), SpendError> {
        let preserve = { metadata.preserve_until };
        let dah = { metadata.delete_at_height };
        let has_preserve = preserve != 0;
        let dah_or_preserve = if has_preserve { preserve } else { dah };
        let mut tf = metadata.flags.bits();
        if has_preserve {
            tf |= TxFlags::HAS_PRESERVE_UNTIL.bits();
        } else {
            tf &= !TxFlags::HAS_PRESERVE_UNTIL.bits();
        }
        self.index
            .write()
            .update_cached_fields(
                key,
                tf,
                metadata.block_entry_count,
                metadata.spent_utxos,
                dah_or_preserve,
                metadata.unmined_since,
                metadata.generation,
            )
            .map(|_| ())
            .map_err(|e| SpendError::StorageError {
                detail: format!("index update_cached_fields failed: {e}"),
            })
    }

    /// Register a transaction in the index (for test setup).
    ///
    /// Also increments the matching shard count atomically with the
    /// primary-index insert — see `register_with_shard_count` —
    /// so callers that use this helper to seed data still observe
    /// consistent `shard_record_count` values afterwards.
    pub fn register(&self, key: TxKey, entry: TxIndexEntry) -> Result<(), SpendError> {
        self.register_with_shard_count(key, entry)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })
    }

    /// Look up a transaction in the index, propagating backend read errors.
    ///
    /// Returns `Ok(Some(entry))` if present, `Ok(None)` if absent, and an
    /// [`IndexError`] if the (redb) backend's read transaction fails.
    /// Client-visible read paths MUST use this variant so a transient
    /// backend error is reported as a storage error rather than being
    /// collapsed into "transaction not found" (G-4).
    pub fn lookup_checked(
        &self,
        key: &TxKey,
    ) -> Result<Option<TxIndexEntry>, crate::index::IndexError> {
        self.index.read().lookup_checked(key)
    }

    /// Look up a transaction in the index (infallible convenience).
    ///
    /// G-4: this collapses a backend read error into `None` after logging
    /// it. It exists for tests and internal diagnostics where "absent on
    /// error" is acceptable. Client-visible read paths MUST instead call
    /// [`Self::lookup_checked`] and surface the error — collapsing a redb
    /// I/O failure to `None` here would tell a client a present
    /// transaction does not exist.
    pub fn lookup(&self, key: &TxKey) -> Option<TxIndexEntry> {
        match self.index.read().lookup_checked(key) {
            Ok(found) => found,
            Err(e) => {
                tracing::error!(
                    target: "teraslab::engine",
                    err = %e,
                    "Engine::lookup: index read failed; returning None (caller should use lookup_checked)",
                );
                None
            }
        }
    }

    /// Iterate over all registered transaction keys (for migration scanning).
    ///
    /// Returns a snapshot of all keys currently in the index. This acquires
    /// a read lock briefly and collects all keys into a Vec.
    pub fn all_keys(&self) -> Vec<TxKey> {
        self.index.read().iter().map(|(k, _)| k).collect()
    }

    /// Scan the primary index for records whose preservation has expired.
    ///
    /// Returns the keys of records with an active `preserve_until` in
    /// `[1, current_height]` — i.e. preservations whose window has elapsed
    /// and which the caller should now schedule for deletion via
    /// [`Self::expire_preservation_set_dah`] (KO-1 / spec §3.18 Phase 3).
    ///
    /// There is no dedicated `preserve_until` secondary index; this filters
    /// the primary index by the cached `HAS_PRESERVE_UNTIL` discriminant and
    /// the `dah_or_preserve` field (which holds `preserve_until` while that
    /// bit is set), so it never touches the device. The returned keys are a
    /// point-in-time snapshot; the caller re-validates each under the stripe
    /// lock, so a `preserve_until` cleared or pushed forward after this scan
    /// is handled correctly there.
    pub fn scan_expired_preservations(&self, current_height: u32) -> Vec<TxKey> {
        self.index
            .read()
            .iter()
            .filter_map(|(k, e)| {
                let has_preserve =
                    TxFlags::from_bits_truncate(e.tx_flags).contains(TxFlags::HAS_PRESERVE_UNTIL);
                if has_preserve && e.dah_or_preserve != 0 && e.dah_or_preserve <= current_height {
                    Some(k)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Return keys belonging to a specific shard.
    ///
    /// More efficient than `all_keys()` followed by filtering when only
    /// a subset of shards is needed. Acquires the index read lock once
    /// and filters inline, avoiding a full clone + filter pass.
    pub fn keys_for_shard(&self, shard: u16) -> Vec<TxKey> {
        self.index
            .read()
            .iter()
            .filter_map(|(k, _)| {
                if crate::cluster::shards::ShardTable::shard_for_key(&k) == shard {
                    Some(k)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Group all keys by shard in a single index scan.
    ///
    /// Returns a HashMap from shard number to Vec of keys. This is O(N)
    /// where N is the total number of index entries, compared to O(N * S)
    /// if calling `keys_for_shard` for each shard S.
    pub fn keys_by_shard(&self) -> std::collections::HashMap<u16, Vec<TxKey>> {
        let mut result: std::collections::HashMap<u16, Vec<TxKey>> =
            std::collections::HashMap::new();
        for (k, _) in self.index.read().iter() {
            let shard = crate::cluster::shards::ShardTable::shard_for_key(&k);
            result.entry(shard).or_default().push(k);
        }
        result
    }

    /// Group keys by shard, but only for a specified set of shards.
    ///
    /// More memory-efficient than `keys_by_shard()` when only a subset
    /// of shards need migration (common case: only outbound shards).
    /// Keys belonging to shards NOT in the filter are skipped entirely.
    pub fn keys_by_shard_filtered(
        &self,
        shard_filter: &std::collections::HashSet<u16>,
    ) -> std::collections::HashMap<u16, Vec<TxKey>> {
        let mut result: std::collections::HashMap<u16, Vec<TxKey>> =
            std::collections::HashMap::new();
        for (k, _) in self.index.read().iter() {
            let shard = crate::cluster::shards::ShardTable::shard_for_key(&k);
            if shard_filter.contains(&shard) {
                result.entry(shard).or_default().push(k);
            }
        }
        result
    }

    /// Execute a batch of spends on a single transaction.
    ///
    /// All spends target the same txid. The per-txid lock is held for the
    /// entire operation: validate → write slots → write metadata → update
    /// secondary indexes.
    ///
    /// This is the combined validate+apply path. For WAL-first ordering
    /// (write redo log between validation and application), use
    /// [`validate_spend_multi`](Engine::validate_spend_multi) followed by
    /// [`ValidatedSpend::apply`].
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn spend_multi(&self, req: &SpendMultiRequest) -> Result<SpendMultiResponse, SpendError> {
        let validated = self.validate_spend_multi(req)?;
        validated.apply(self)
    }

    /// Validate a batch of spends WITHOUT applying them.
    ///
    /// Acquires the per-transaction lock, reads metadata and UTXO slots,
    /// validates each item, and returns a [`ValidatedSpend`] that holds the
    /// lock guard. The caller can write redo log entries (WAL) while the
    /// lock is held, then call [`ValidatedSpend::apply`] to commit the
    /// mutation.
    ///
    /// The lock is released when the `ValidatedSpend` is dropped (without
    /// applying) or consumed by [`ValidatedSpend::apply`] (after writing).
    pub fn validate_spend_multi<'a>(
        &'a self,
        req: &SpendMultiRequest,
    ) -> Result<ValidatedSpend<'a>, SpendError> {
        let guard = self.locks.lock(&req.tx_key);

        // 1. Index lookup
        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let record_offset = entry.record_offset;

        // 2. Read metadata (zero-alloc when device supports direct access)
        let metadata = self.read_metadata_fast(record_offset)?;

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
            let block_ids = collect_block_ids(&metadata).to_vec();
            return Ok(ValidatedSpend {
                _guard: guard,
                tx_key: req.tx_key,
                valid_spends: Vec::new(),
                errors: BTreeMap::new(),
                spent_count: 0,
                idempotent_count: 0,
                pre_generation: metadata.generation,
                block_ids,
                record_offset,
                metadata,
                current_block_height: req.current_block_height,
                block_height_retention: req.block_height_retention,
            });
        }

        // 4+5. Read each slot inline and validate immediately.
        // No intermediate lookup map. For duplicate vouts in the same batch, we
        // check valid_spends to find the already-spent state (since device
        // writes haven't happened yet during validation).
        let mut errors: BTreeMap<u32, SpendError> = BTreeMap::new();
        let mut valid_spends: Vec<(u32, UtxoSlot)> = Vec::with_capacity(req.spends.len());
        let mut spent_count: u32 = 0;
        let mut idempotent_count: u32 = 0;

        for item in &req.spends {
            // F-G2-002: reject the reserved all-`0xFF` sentinel before any
            // slot read. Recorded as a per-item error so the rest of the
            // batch can still succeed (deterministic by idx); a single
            // malformed item must not abort the whole batch.
            if item.spending_data == [FROZEN_BYTE; 36] {
                errors.insert(
                    item.idx,
                    SpendError::ReservedSpendingData {
                        offset: item.offset,
                    },
                );
                continue;
            }

            if item.offset >= utxo_count {
                errors.insert(
                    item.idx,
                    SpendError::UtxoNotFound {
                        offset: item.offset,
                    },
                );
                continue;
            }

            // Check if this vout was already spent earlier in this batch.
            // This handles duplicate offsets without a HashMap lookup table.
            let slot = if let Some((_, prev)) = valid_spends
                .iter()
                .rev()
                .find(|(off, _)| *off == item.offset)
            {
                *prev
            } else {
                self.read_slot_fast(record_offset, item.offset)?
            };

            if slot.hash != item.utxo_hash {
                errors.insert(
                    item.idx,
                    SpendError::UtxoHashMismatch {
                        offset: item.offset,
                    },
                );
                continue;
            }

            match slot.status {
                UTXO_UNSPENT => {
                    // F-G2-004: avoid `unwrap()` in library code even on
                    // an infallible 4-byte conversion — future slot-layout
                    // changes must not silently become a panic on the
                    // spend hot-path.
                    let mut buf = [0u8; 4];
                    buf.copy_from_slice(&slot.spending_data[0..4]);
                    let spendable_height = u32::from_le_bytes(buf);
                    // Spendable AT stop: half-open interval `[0, spendable_height)`.
                    // At `current_block_height == spendable_height` the UTXO is
                    // unfrozen — matches Teranode PR #949 / svnode / Aerospike
                    // post-fix. Pre-fix this used `>=` which false-rejected at
                    // the exact unlock height — i.e. accepting txs the network
                    // would reject.
                    if spendable_height != 0 && spendable_height > req.current_block_height {
                        errors.insert(
                            item.idx,
                            SpendError::FrozenUntil {
                                offset: item.offset,
                                spendable_at_height: spendable_height,
                            },
                        );
                        continue;
                    }

                    let new_slot = UtxoSlot::new_spent(item.utxo_hash, item.spending_data);
                    valid_spends.push((item.offset, new_slot));
                    spent_count += 1;
                }
                UTXO_SPENT => {
                    if slot.spending_data == item.spending_data {
                        // F-X-022: same defense-in-depth check as the
                        // single-spend path. See `Engine::spend` for
                        // the full rationale. The metadata read at
                        // step 2 above already carries
                        // `deleted_children_count`; the on-device list
                        // is only read when count > 0.
                        let deleted_count = { metadata.deleted_children_count };
                        if deleted_count > 0 {
                            let deleted_offset = { metadata.deleted_children_offset };
                            let deleted = self.read_deleted_children_at(
                                deleted_count as usize,
                                deleted_offset,
                            )?;
                            let mut child_txid = [0u8; 32];
                            child_txid.copy_from_slice(&item.spending_data[..32]);
                            if deleted.contains(&child_txid) {
                                errors.insert(
                                    item.idx,
                                    SpendError::DeletedChildren {
                                        offset: item.offset,
                                        child_count: deleted_count,
                                    },
                                );
                                continue;
                            }
                        }
                        idempotent_count += 1;
                        continue;
                    }
                    if slot.spending_data == [FROZEN_BYTE; 36] {
                        errors.insert(
                            item.idx,
                            SpendError::Frozen {
                                offset: item.offset,
                            },
                        );
                        continue;
                    }
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
                        SpendError::Pruned {
                            offset: item.offset,
                            spending_data: slot.spending_data,
                        },
                    );
                }
                UTXO_FROZEN => {
                    errors.insert(
                        item.idx,
                        SpendError::Frozen {
                            offset: item.offset,
                        },
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

        let block_ids = collect_block_ids(&metadata).to_vec();

        Ok(ValidatedSpend {
            _guard: guard,
            tx_key: req.tx_key,
            valid_spends,
            errors,
            spent_count,
            idempotent_count,
            pre_generation: metadata.generation,
            block_ids,
            record_offset,
            metadata,
            current_block_height: req.current_block_height,
            block_height_retention: req.block_height_retention,
        })
    }

    /// Execute a single spend — zero-allocation fast path.
    ///
    /// Inlines the validate-and-apply logic for exactly one UTXO,
    /// avoiding the `Vec` and ordered-map allocations that `spend_multi` uses.
    pub fn spend(&self, req: &SpendRequest) -> Result<SpendResponse, SpendError> {
        // F-G2-002: reject the all-`0xFF` reserved sentinel up front. That
        // byte pattern is the on-disk frozen marker; accepting it under
        // `status=UTXO_SPENT` would let any client permanently brick the
        // UTXO against unspend (frozen-marker short-circuit) and unfreeze
        // (rejects non-`UTXO_FROZEN` status). The 36-byte payload is also
        // not a valid BSV `txid + vin` — txid cannot be all `0xFF`.
        if req.spending_data == [FROZEN_BYTE; 36] {
            return Err(SpendError::ReservedSpendingData { offset: req.offset });
        }

        let _guard = self.locks.lock(&req.tx_key);

        // 1. Index lookup
        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let record_offset = entry.record_offset;

        // 2. Read metadata
        let mut metadata = self.read_metadata_fast(record_offset)?;

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
        if req.offset >= utxo_count {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        // 4. Read and validate the UTXO slot
        let slot = self.read_slot_fast(record_offset, req.offset)?;
        if slot.hash != req.utxo_hash {
            return Err(SpendError::UtxoHashMismatch { offset: req.offset });
        }

        match slot.status {
            UTXO_UNSPENT => {
                // F-G2-004: avoid `unwrap()` in library code (see batch
                // path for rationale).
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&slot.spending_data[0..4]);
                let spendable_height = u32::from_le_bytes(buf);
                // Spendable AT stop — see batch path for rationale.
                if spendable_height != 0 && spendable_height > req.current_block_height {
                    return Err(SpendError::FrozenUntil {
                        offset: req.offset,
                        spendable_at_height: spendable_height,
                    });
                }
            }
            UTXO_SPENT => {
                if slot.spending_data == req.spending_data {
                    // R-021 (BC-25 / BC-35): idempotent re-spend is a
                    // true no-op — no slot change, no counter change,
                    // no metadata write, no generation bump. Pre-fix
                    // this branch bumped `metadata.generation` and
                    // wrote the metadata back to disk WITHOUT emitting
                    // a redo entry, so a crash between the metadata
                    // write and its fsync could leave the on-device
                    // generation lower than the value already returned
                    // to the client (and propagated to replicas via
                    // any subsequent ReplicaOp). Recovery had no redo
                    // entry to replay, so the gap was permanent and
                    // replication staleness checks would mismatch.
                    // Aligning with `unspend`'s already-unspent branch
                    // (lines above) — which also returns the unchanged
                    // generation — eliminates the WAL gap entirely:
                    // no write means nothing to recover.
                    //
                    // F-X-022 — `addDeletedChildren` parity with
                    // Aerospike. Before returning the no-op, consult
                    // the parent record's `deleted_children` list. If
                    // the requesting child txid is present, the
                    // chain history has been altered ("resurrected-
                    // then-pruned") and the re-spend MUST be rejected
                    // even though the slot still reads SPENT by this
                    // exact child. The PRIMARY defense remains the
                    // slot's `UTXO_PRUNED` status (matched in the arm
                    // below); this check is defense-in-depth for the
                    // unusual code path where the slot was flipped
                    // back to SPENT after a prune. The lookup is
                    // cheap: `deleted_children_count` is already in the
                    // metadata we read in step 2, and the on-device
                    // list is read only when count > 0. The first 32
                    // bytes of `spending_data` are the child txid
                    // (BSV spending data is `txid(32) + vin(4)`).
                    let deleted_count = { metadata.deleted_children_count };
                    if deleted_count > 0 {
                        let deleted_offset = { metadata.deleted_children_offset };
                        let deleted = self.read_deleted_children_at(
                            deleted_count as usize,
                            deleted_offset,
                        )?;
                        let mut child_txid = [0u8; 32];
                        child_txid.copy_from_slice(&req.spending_data[..32]);
                        if deleted.contains(&child_txid) {
                            return Err(SpendError::DeletedChildren {
                                offset: req.offset,
                                child_count: deleted_count,
                            });
                        }
                    }
                    let block_ids = collect_block_ids(&metadata).to_vec();
                    return Ok(SpendResponse {
                        signal: Signal::None,
                        block_ids,
                    });
                }
                if slot.spending_data == [FROZEN_BYTE; 36] {
                    return Err(SpendError::Frozen { offset: req.offset });
                }
                return Err(SpendError::AlreadySpent {
                    offset: req.offset,
                    spending_data: slot.spending_data,
                });
            }
            UTXO_PRUNED => {
                return Err(SpendError::Pruned {
                    offset: req.offset,
                    spending_data: slot.spending_data,
                });
            }
            UTXO_FROZEN => return Err(SpendError::Frozen { offset: req.offset }),
            _ => {
                return Err(SpendError::StorageError {
                    detail: format!("unknown status byte: {:#04x}", slot.status),
                });
            }
        }

        // 5. Write the spent slot. R-004: propagate the write error
        // rather than logging-and-continuing. The dispatcher returns
        // ERR_INTERNAL to the client and the redo log drives replay
        // on the next startup. Silently ignoring the failure was a
        // double-spend invitation (slot stays UNSPENT on disk while
        // metadata says SPENT, and a follow-up spend with different
        // spending_data succeeds).
        let new_slot = UtxoSlot::new_spent(req.utxo_hash, req.spending_data);
        self.write_slot_fast(record_offset, req.offset, &new_slot)?;

        // 6. Update metadata
        let old_dah = { metadata.delete_at_height };
        metadata.spent_utxos = { metadata.spent_utxos }.wrapping_add(1);
        metadata.generation = { metadata.generation }.wrapping_add(1);
        metadata.updated_at = self.now_millis();

        // 7. Evaluate deleteAtHeight
        let (signal, dah_patch) = evaluate_delete_at_height(
            &metadata,
            req.current_block_height,
            req.block_height_retention,
        )?;

        if let Some(ref patch) = dah_patch {
            apply_dah_patch(&mut metadata, patch);
        }

        // 8. Write metadata. R-004: propagate the write error rather
        // than logging-and-continuing.
        if !self.device_ptr.is_null() {
            // SAFETY: `device_ptr` is non-null (checked above) and live for
            // the engine's lifetime; `record_offset` is allocator-valid. The
            // caller holds this record's stripe lock, and
            // `write_metadata_direct` additionally takes the per-offset
            // `io_locks()` write side for torn-read-safe publication.
            unsafe { io::write_metadata_direct(self.device_ptr, record_offset, &metadata) };
        } else {
            self.write_metadata_fast(record_offset, &metadata)?;
        }

        self.sync_index_cache(&req.tx_key, &metadata)?;

        // 9. Update DAH secondary index (two-phase durable)
        let new_dah = { metadata.delete_at_height };
        self.update_dah_index(&req.tx_key, old_dah, new_dah)?;

        let block_ids = collect_block_ids(&metadata).to_vec();

        Ok(SpendResponse { signal, block_ids })
    }

    /// Unspend a UTXO — reverse a previous spend.
    ///
    /// Implements the ownership-with-idempotent-semantics contract of the Lua
    /// reference (`teranode.lua` lines 484–555). The slot is only cleared when
    /// the caller owns the stored spend, i.e. the stored spending data is
    /// present (slot is SPENT) and byte-equal to `req.spending_data`. When the
    /// caller does **not** own the spend — the slot is already unspent (stored
    /// is nil), the stored spend belongs to a different transaction, or the
    /// slot carries the frozen marker (whose stored data is all-`0xFF` and can
    /// never equal a real caller's expected data) — this is a silent no-op that
    /// returns `STATUS_OK` after running the same DAH lifecycle housekeeping the
    /// mutating path runs. The safety guarantee is "never wipe a spend we don't
    /// own", not "error on every no-op": `ProcessConflicting` builds its unspend
    /// set from every input of every losing tx, including parents whose stored
    /// spend is nil or belongs to the conflict winner, and the Go caller aborts
    /// the whole loop on any non-OK status other than `TX_NOT_FOUND`.
    ///
    /// Errors are reserved for: record not found (`TX_NOT_FOUND`), offset out of
    /// range or hash mismatch (`UTXO_NOT_FOUND` / `UTXO_HASH_MISMATCH`, evaluated
    /// before the ownership check), a `PRUNED` slot (chain history actually
    /// diverged — TeraSlab postdates the Lua, which had no PRUNED state), an
    /// owned-spend on a frozen slot (`FROZEN`, structurally unreachable since a
    /// real caller's expected data is never the frozen marker, but preserved to
    /// mirror the Lua), counter corruption, and storage failures.
    pub fn unspend(&self, req: &UnspendRequest) -> Result<UnspendResponse, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);

        // 1. Index lookup
        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let record_offset = entry.record_offset;

        // 2. Read metadata
        let mut metadata = self.read_metadata_fast(record_offset)?;

        let utxo_count = { metadata.utxo_count };
        if req.offset >= utxo_count {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        // 3. Read the specific slot
        let slot = self.read_slot_fast(record_offset, req.offset)?;

        // 4. Validate hash
        if slot.hash != req.utxo_hash {
            return Err(SpendError::UtxoHashMismatch { offset: req.offset });
        }

        // 5. Ownership decision (mirrors the Lua `callerOwnsSpend` check).
        //
        // PRUNED is a hard error in both directions — the Lua predates the
        // PRUNED state, and a pruned slot means chain history actually
        // diverged, which must not be silently reversed.
        if slot.status == UTXO_PRUNED {
            return Err(SpendError::Pruned {
                offset: req.offset,
                spending_data: slot.spending_data,
            });
        }
        if slot.status != UTXO_UNSPENT
            && slot.status != UTXO_SPENT
            && slot.status != UTXO_FROZEN
        {
            return Err(SpendError::StorageError {
                detail: format!("unknown status: {:#04x}", slot.status),
            });
        }

        // `callerOwnsSpend = existingSpendingData ~= nil AND
        //  bytes_equal(existingSpendingData, expectedSpendingData)`.
        // The stored spend is "present" only on a SPENT slot; an UNSPENT slot
        // has no stored spend (nil), and a FROZEN slot's stored data is the
        // all-0xFF marker, which a real caller's expected data never matches.
        let stored_is_frozen_marker =
            slot.status == UTXO_FROZEN || slot.spending_data == [FROZEN_BYTE; 36];
        let caller_owns_spend = slot.status == UTXO_SPENT
            && !stored_is_frozen_marker
            && slot.spending_data == req.spending_data;

        if caller_owns_spend {
            // Owned + frozen would be FROZEN per the Lua; structurally
            // unreachable here because `stored_is_frozen_marker` already
            // excludes it from `caller_owns_spend`, but kept explicit so the
            // contract is auditable against the reference.
            if stored_is_frozen_marker {
                return Err(SpendError::Frozen { offset: req.offset });
            }

            let current = { metadata.spent_utxos };
            if current == 0 {
                return Err(SpendError::StorageError {
                    detail: format!(
                        "metadata spent_utxos is zero while slot {} is spent",
                        req.offset
                    ),
                });
            }

            // Valid unspend: clear the slot and decrement the counter.
            let new_slot = UtxoSlot::new_unspent(req.utxo_hash);
            self.write_slot_fast(record_offset, req.offset, &new_slot)?;
            metadata.spent_utxos = current - 1;
            metadata.generation = { metadata.generation }.wrapping_add(1);
            metadata.updated_at = self.now_millis();
        }
        // else: silent no-op. The slot, counter, and generation are left
        // untouched; we still fall through to DAH housekeeping exactly as the
        // Lua does (`setDeleteAtHeight` runs on every non-error path).

        // 6. Evaluate deleteAtHeight (runs on both the mutating and no-op paths,
        //    matching the Lua which calls setDeleteAtHeight before every OK
        //    return). On a pure no-op this may still forward-extend an
        //    all-spent record's DAH.
        let old_dah = { metadata.delete_at_height };
        let (signal, dah_patch) = evaluate_delete_at_height(
            &metadata,
            req.current_block_height,
            req.block_height_retention,
        )?;

        if let Some(ref patch) = dah_patch {
            apply_dah_patch(&mut metadata, patch);
        }

        let new_dah = { metadata.delete_at_height };

        // 7. Persist only when something actually changed. The owned-spend path
        //    always changed (slot + counter + generation). A no-op persists only
        //    if DAH housekeeping moved the deleteAtHeight, and does so without
        //    bumping the generation — keeping a pure no-op generation-stable so
        //    the dispatch layer can still classify it as idempotent.
        if caller_owns_spend || new_dah != old_dah {
            if !self.device_ptr.is_null() {
                // SAFETY: `device_ptr` is non-null (checked above) and live
                // for the engine's lifetime; `record_offset` is
                // allocator-valid. The unspend caller holds this record's
                // stripe lock, and `write_metadata_direct` takes the
                // per-offset `io_locks()` write side for safe publication.
                unsafe { io::write_metadata_direct(self.device_ptr, record_offset, &metadata) };
            } else {
                self.write_metadata_fast(record_offset, &metadata)?;
            }

            self.sync_index_cache(&req.tx_key, &metadata)?;

            // Update DAH secondary index (two-phase durable).
            self.update_dah_index(&req.tx_key, old_dah, new_dah)?;
        }

        Ok(UnspendResponse {
            signal,
            generation: { metadata.generation },
        })
    }

    /// Set or unset the mined state of a transaction.
    ///
    /// Adds or removes a block entry in the metadata. Only modifies the
    /// metadata region — UTXO slots are not touched.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn set_mined(&self, req: &SetMinedRequest) -> Result<SetMinedResponse, SpendError> {
        let params = SetMinedSharedParams {
            block_id: req.block_id,
            block_height: req.block_height,
            subtree_idx: req.subtree_idx,
            current_block_height: req.current_block_height,
            block_height_retention: req.block_height_retention,
            on_longest_chain: req.on_longest_chain,
            unset_mined: req.unset_mined,
        };
        self.set_mined_inner(&req.tx_key, &params)
    }

    /// Core set_mined logic, taking shared params by reference.
    ///
    /// Used by both [`set_mined`] (single request) and [`set_mined_batch`]
    /// (batch with shared params). Acquires the per-transaction stripe lock.
    fn set_mined_inner(
        &self,
        tx_key: &TxKey,
        req: &SetMinedSharedParams,
    ) -> Result<SetMinedResponse, SpendError> {
        let _guard = self.locks.lock(tx_key);

        // 1. Index lookup
        let entry = self
            .index
            .read()
            .lookup_checked(tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let record_offset = entry.record_offset;

        // ---------------------------------------------------------------
        // FAST PATH: first-ever setMined (count == 0), write-only.
        //
        // When no block entries exist yet, we can skip the metadata read
        // entirely: no duplicates to check, no existing block_ids to
        // return, DAH evaluation runs from cached index fields.
        // ---------------------------------------------------------------
        let cached_count = entry.block_entry_count;
        if !req.unset_mined && cached_count == 0 && !self.device_ptr.is_null() {
            // F-G2-011 / KO-11: read the authoritative on-device metadata
            // up front and derive EVERY DAH/flag/counter input from it,
            // never from the cached `entry`. The fast path already needs
            // this read for the RMW (CRC must cover the full post-state),
            // so it is free. The cached `entry` can be stale after a prior
            // mutation that wrote metadata but failed at `sync_index_cache`
            // (the device advanced while the cache did not). F-G2-011 fixed
            // only `generation` from `meta`; KO-11 extends that to the DAH,
            // preserve discriminant, flags, and the spent/unmined counters
            // so a stale-cache scenario cannot write a wrong `old_dah`,
            // mis-flagged `tf`, or wrong DAH-index delta.
            // SAFETY: `device_ptr` is non-null (the fast path is gated on
            // `!self.device_ptr.is_null()`) and live for the engine's
            // lifetime; `record_offset` is allocator-valid. The set_mined
            // caller holds this record's stripe lock, and
            // `read_metadata_direct` takes the per-offset `io_locks()` read
            // side, so the read is torn-read-safe.
            let mut meta = unsafe {
                io::read_metadata_direct(self.device_ptr, record_offset).map_err(|e| {
                    SpendError::StorageError {
                        detail: format!("{e}"),
                    }
                })?
            };

            // Fast-path eligibility is the cache's `count == 0` signal; the
            // fresh read must agree, otherwise the cache was stale about the
            // block-entry count and writing into inline slot 0 would clobber
            // an existing entry. Fall through to the slow path, which reads
            // and reconciles the full entry list.
            if { meta.block_entry_count } != 0 {
                // Re-read via the slow path below (it re-reads metadata).
            } else {
                let new_count = 1u8;
                let new_entry = BlockEntry {
                    block_id: req.block_id,
                    block_height: req.block_height,
                    subtree_idx: req.subtree_idx,
                };

                // Derive flags + DAH inputs from the FRESH meta (not cache).
                let mut tf = meta.flags;
                tf.remove(TxFlags::LOCKED); // setMined clears LOCKED
                let meta_unmined = { meta.unmined_since };
                let new_unmined = if req.on_longest_chain { 0u32 } else { meta_unmined };
                let old_unmined = meta_unmined;
                let preserve = { meta.preserve_until };
                let has_preserve = preserve != 0;
                let meta_dah = { meta.delete_at_height };
                // `old_dah` is the DAH the secondary index currently
                // reflects: the on-device DAH when not preserved, else 0
                // (preservation clears the DAH).
                let old_dah = if has_preserve { 0 } else { meta_dah };
                let meta_spent = { meta.spent_utxos };
                let meta_utxo_count = { meta.utxo_count };

                // DAH evaluation from fresh-meta fields.
                let (signal, dah_patch) = crate::ops::delete_eval::evaluate_dah_cached(
                    tf,
                    meta_spent,
                    meta_utxo_count,
                    new_count,
                    new_unmined,
                    has_preserve,
                    if has_preserve { preserve } else { meta_dah },
                    req.current_block_height,
                    req.block_height_retention,
                )?;
                let mut new_dah = old_dah;
                if let Some(ref patch) = dah_patch {
                    tf.set(TxFlags::LAST_SPENT_ALL, patch.last_spent_all);
                    new_dah = patch.new_delete_at_height;
                }

                let updated_at = self.now_millis();

                // Read-modify-write so CRC covers the full post-state
                // (block-entry-count, inline entry, and footer fields).
                // Generation is taken from the on-device value (F-G2-011).
                let generation = { meta.generation }.wrapping_add(1);
                meta.flags = tf;
                meta.generation = generation;
                meta.updated_at = updated_at;
                meta.delete_at_height = new_dah;
                meta.unmined_since = new_unmined;
                meta.block_entry_count = new_count;
                meta.block_entries_inline[0] = new_entry;
                // SAFETY: `device_ptr` is non-null (fast-path gate) and live
                // for the engine's lifetime; `record_offset` is
                // allocator-valid. The caller holds this record's stripe
                // lock; `write_metadata_direct` takes the per-offset
                // `io_locks()` write side for torn-read-safe publication.
                unsafe {
                    io::write_metadata_direct(self.device_ptr, record_offset, &meta);
                }

                // Sync all cached fields to index from the post-state.
                let dah_or_preserve = if has_preserve { preserve } else { new_dah };
                let mut sync_tf = tf;
                if has_preserve {
                    sync_tf.insert(TxFlags::HAS_PRESERVE_UNTIL);
                }
                self.index
                    .write()
                    .update_cached_fields(
                        tx_key,
                        sync_tf.bits(),
                        new_count,
                        meta_spent,
                        dah_or_preserve,
                        new_unmined,
                        generation,
                    )
                    .map_err(|e| SpendError::StorageError {
                        detail: format!("index update_cached_fields failed: {e}"),
                    })?;

                // Update secondary indexes with two-phase durability.
                // Batched into a single redo fsync when both change.
                self.update_both_secondary_indexes(
                    tx_key,
                    old_dah,
                    new_dah,
                    old_unmined,
                    new_unmined,
                )?;

                return Ok(SetMinedResponse {
                    signal,
                    block_ids: vec![req.block_id],
                    generation,
                });
            }
        }

        // ---------------------------------------------------------------
        // SLOW PATH: unset_mined, overflow (count >= 3), or no direct ptr.
        // Full metadata read + write.
        // ---------------------------------------------------------------

        // 2. Read metadata
        let mut metadata = self.read_metadata_fast(record_offset)?;

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
                        let mut overflow = read_overflow_entries(&*self.device, &metadata)
                            .map_err(|e| SpendError::StorageError {
                                detail: format!("{e}"),
                            })?;
                        // F-G2-004: `count > INLINE_BLOCK_ENTRIES` implies a
                        // non-empty overflow, so this pop is unreachable-None
                        // in current code. Surface as a StorageError instead
                        // of a panic so any future divergence between the
                        // in-memory count and the on-device overflow list is
                        // reported, not crashed on.
                        let last = overflow.pop().ok_or_else(|| SpendError::StorageError {
                            detail: format!(
                                "overflow read returned no entries despite \
                                 block_entry_count={count} > INLINE_BLOCK_ENTRIES"
                            ),
                        })?;
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
                    read_overflow_entries(&*self.device, &metadata).map_err(|e| {
                        SpendError::StorageError {
                            detail: format!("{e}"),
                        }
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
            // Add block entry (slow path — overflow or no direct ptr)
            let count = metadata.block_entry_count as usize;
            let inline_count = count.min(INLINE_BLOCK_ENTRIES);
            let mut exists = false;

            for i in 0..inline_count {
                if { metadata.block_entries_inline[i].block_id } == req.block_id {
                    exists = true;
                    break;
                }
            }

            if !exists && count > INLINE_BLOCK_ENTRIES {
                let overflow = read_overflow_entries(&*self.device, &metadata).map_err(|e| {
                    SpendError::StorageError {
                        detail: format!("{e}"),
                    }
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
                    let mut overflow =
                        read_overflow_entries(&*self.device, &metadata).map_err(|e| {
                            SpendError::StorageError {
                                detail: format!("{e}"),
                            }
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
        metadata.updated_at = self.now_millis();

        // Evaluate deleteAtHeight
        let (signal, dah_patch) = evaluate_delete_at_height(
            &metadata,
            req.current_block_height,
            req.block_height_retention,
        )?;
        if let Some(ref patch) = dah_patch {
            apply_dah_patch(&mut metadata, patch);
        }

        // Write full metadata (slow path)
        self.write_metadata_fast(record_offset, &metadata)?;
        self.sync_index_cache(tx_key, &metadata)?;

        // Update secondary indexes with two-phase durability, batched.
        let new_dah = { metadata.delete_at_height };
        let new_unmined = { metadata.unmined_since };
        self.update_both_secondary_indexes(tx_key, old_dah, new_dah, old_unmined, new_unmined)?;

        let block_ids = if (metadata.block_entry_count as usize) <= INLINE_BLOCK_ENTRIES {
            collect_block_ids(&metadata).to_vec()
        } else {
            collect_all_block_ids(&*self.device, &metadata)
                .unwrap_or_else(|_| collect_block_ids(&metadata).to_vec())
        };

        Ok(SetMinedResponse {
            signal,
            block_ids,
            generation: { metadata.generation },
        })
    }

    /// Apply set_mined to a batch of transactions sharing the same params.
    ///
    /// This is the dispatch-layer entry point for `OP_SET_MINED_BATCH`.
    /// Shared parameters are passed once by reference; only the `tx_key`
    /// varies per item. This avoids copying 28 bytes of params per item.
    ///
    /// Atomicity is per transaction, not per batch: each key takes its own
    /// stripe lock inside `Self::set_mined_inner`, and earlier items remain
    /// visible if a later item fails.
    ///
    /// Returns one `Result` per key, in the same order as `keys`.
    pub fn set_mined_batch(
        &self,
        params: &SetMinedSharedParams,
        keys: &[TxKey],
    ) -> Vec<Result<SetMinedResponse, SpendError>> {
        keys.iter()
            .map(|key| self.set_mined_inner(key, params))
            .collect()
    }

    /// Mark a transaction as on or off the longest chain.
    ///
    /// Only modifies `unmined_since` — block entries and UTXO slots are
    /// not touched. Called during chain reorganizations.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn mark_on_longest_chain(
        &self,
        req: &MarkOnLongestChainRequest,
    ) -> Result<MarkOnLongestChainResponse, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);

        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let record_offset = entry.record_offset;

        let mut metadata = self.read_metadata_fast(record_offset)?;

        let old_unmined = { metadata.unmined_since };
        let old_dah = { metadata.delete_at_height };

        if req.on_longest_chain {
            metadata.unmined_since = 0;
        } else {
            metadata.unmined_since = req.current_block_height;
        }

        // Mutation bookkeeping
        metadata.generation = { metadata.generation }.wrapping_add(1);
        metadata.updated_at = self.now_millis();

        // Evaluate deleteAtHeight (longest chain status affects DAH)
        let (signal, dah_patch) = evaluate_delete_at_height(
            &metadata,
            req.current_block_height,
            req.block_height_retention,
        )?;
        if let Some(ref patch) = dah_patch {
            apply_dah_patch(&mut metadata, patch);
        }

        // Targeted mined footer when direct, full write otherwise.
        // The on-device metadata is the primary durable source of truth.
        if !self.device_ptr.is_null() {
            // SAFETY: `device_ptr` is non-null (checked above) and live for
            // the engine's lifetime; `record_offset` is allocator-valid. The
            // set_mined slow path holds this record's stripe lock;
            // `write_metadata_direct` takes the per-offset `io_locks()` write
            // side for torn-read-safe publication.
            unsafe { io::write_metadata_direct(self.device_ptr, record_offset, &metadata) };
        } else {
            self.write_metadata_fast(record_offset, &metadata)?;
        }

        // H1: atomic primary + DAH + unmined update under one critical
        // section. Any reader that locks dah_index or unmined_index observes
        // a consistent view with the primary in-memory cache — no window
        // where DAH references a stale height while primary has moved on.
        let new_dah = { metadata.delete_at_height };
        let new_unmined = { metadata.unmined_since };
        self.sync_primary_and_both_secondary_atomic(
            &req.tx_key,
            &metadata,
            old_dah,
            new_dah,
            old_unmined,
            new_unmined,
        )?;

        Ok(MarkOnLongestChainResponse {
            signal,
            generation: { metadata.generation },
        })
    }

    // -----------------------------------------------------------------------
    // Creation
    // -----------------------------------------------------------------------

    /// Create a new transaction record.
    ///
    /// Allocates space, writes the complete record (metadata + UTXO slots +
    /// optional cold data) in one I/O operation, and registers it in the
    /// index. The record is immediately available for spend/setMined.
    ///
    /// Holds the per-tx stripe lock across duplicate check → allocation →
    /// write → registration, so concurrent creates of the same txid yield
    /// exactly one `Ok` and N−1 [`CreateError::DuplicateTxId`], and a
    /// same-txid delete can never interleave with those steps. On any
    /// failure after allocation the reserved region is returned to the
    /// allocator.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create(&self, req: &CreateRequest) -> Result<CreateResponse, CreateError> {
        let utxo_count = req.utxo_hashes.len() as u32;
        if utxo_count == 0 {
            return Err(CreateError::InvalidUtxoCount);
        }

        let key = req.tx_key();

        // Serialize with every other mutating op on this txid (audit A —
        // create was the only mutating op that took no stripe lock, so the
        // duplicate check below could interleave with a concurrent create or
        // delete of the same txid and both creates could "win"). The guard
        // is held across duplicate check → allocation → record write →
        // index registration so those steps are atomic with respect to this
        // stripe; it is dropped before the parent-record updates at the end
        // of this function because `append_conflicting_child_best_effort`
        // takes the *parent's* stripe lock, which may collide with this
        // key's stripe (self-deadlock on a non-reentrant mutex).
        let stripe_guard = self.locks.lock(&key);

        // Check for duplicate txid. The authoritative recheck happens
        // atomically inside `register_new_with_shard_count`; this early
        // return avoids allocating and writing for the common
        // already-exists case.
        //
        // G-4: use `lookup_checked` so a transient backend read error
        // surfaces as a storage error rather than collapsing to "absent"
        // — the latter would let this early-return pass and write a
        // duplicate record over an existing txid.
        if self
            .index
            .read()
            .lookup_checked(&key)
            .map_err(|e| CreateError::StorageError {
                detail: format!("duplicate-check index lookup failed: {e}"),
            })?
            .is_some()
        {
            return Err(CreateError::DuplicateTxId);
        }
        let external_ref = Self::external_ref_for_create(req)?;

        // Calculate cold data size
        let cold_data = if req.is_external && req.inputs.is_none() {
            // Cold data was pre-uploaded to blobstore via OP_STREAM_CHUNK.
            // Write only metadata + UTXO slots; cold_data is read from blobstore on demand.
            vec![]
        } else {
            build_cold_data(req.inputs, req.outputs, req.inpoints)
        };
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

        // Populate ExternalRef for externally-stored cold data.
        if let Some(ext) = external_ref {
            meta.external_ref = ext;
        }

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

        // Write complete record in one operation. On failure, return the
        // freshly allocated region — the bytes at `record_offset` are
        // unreachable without an index entry and would otherwise leak
        // (audit A — create leaks its allocation on post-allocation failure).
        if let Err(e) = self.write_full_record_with_cold(record_offset, &meta, &slots, &cold_data)
        {
            self.free_create_allocation_best_effort(record_offset, total_size);
            return Err(e);
        }

        // Register in index
        let index_entry = TxIndexEntry {
            device_id: 0,
            record_offset,
            utxo_count,
            block_entry_count: meta.block_entry_count,
            tx_flags: flags.bits(),
            spent_utxos: { meta.spent_utxos },
            dah_or_preserve: { meta.delete_at_height },
            unmined_since: { meta.unmined_since },
            generation: 0,
        };
        // Register in primary index AND increment shard_counts in the same
        // critical section so the two can never drift (H2 correctness fix).
        // `register_new_with_shard_count` rejects (never overwrites) an
        // existing key atomically with the insert; together with the stripe
        // guard this guarantees exactly one create can win for a txid.
        let inserted = match self.register_new_with_shard_count(key, index_entry) {
            Ok(inserted) => inserted,
            Err(e) => {
                self.free_create_allocation_best_effort(record_offset, total_size);
                return Err(CreateError::StorageError {
                    detail: format!("{e}"),
                });
            }
        };
        if !inserted {
            // Defense in depth: the stripe guard means no same-txid writer
            // can have registered since the lookup above, but if it ever
            // happens, refuse to overwrite the live entry and release the
            // losing reservation instead of leaking it.
            self.free_create_allocation_best_effort(record_offset, total_size);
            return Err(CreateError::DuplicateTxId);
        }

        // Update unmined secondary index if applicable (two-phase durable).
        if meta.unmined_since != 0 {
            self.update_unmined_index(&key, 0, meta.unmined_since)
                .map_err(|e| CreateError::StorageError {
                    detail: format!("{e}"),
                })?;
        }

        // Update parent records' conflicting-children lists. Drop the stripe
        // guard first: `append_conflicting_child_best_effort` locks each
        // parent's stripe, and a parent txid may hash to this key's stripe.
        drop(stripe_guard);
        if req.conflicting {
            for parent_txid in req.parent_txids {
                let parent_key = TxKey { txid: *parent_txid };
                self.append_conflicting_child_best_effort(&parent_key, req.tx_id, "create");
            }
        }

        Ok(CreateResponse {
            record_offset,
            utxo_count,
        })
    }

    /// Pre-allocate space for a create operation without writing any data.
    ///
    /// Validates the request, computes the record size, and allocates device
    /// space. Returns `(record_offset, utxo_count)` on success. The caller
    /// must subsequently call [`Self::create_at_offset`] with the same request and
    /// the returned `record_offset` to finalize the create.
    ///
    /// If the caller decides not to finalize (e.g., redo flush fails), it
    /// must free the allocated space via `self.allocator.lock().free(offset, size)`.
    pub fn pre_allocate_create(&self, req: &CreateRequest) -> Result<(u64, u32, u64), CreateError> {
        let utxo_count = req.utxo_hashes.len() as u32;
        if utxo_count == 0 {
            return Err(CreateError::InvalidUtxoCount);
        }

        let key = req.tx_key();

        // Take the stripe lock for the duplicate check + reservation so this
        // snapshot cannot interleave with a same-txid create/delete that is
        // mid-mutation (audit A). The guard does NOT extend to the later
        // `create_at_offset` call — the authoritative, atomic duplicate
        // rejection lives in `create_at_offset_inner`, which re-takes the
        // stripe and registers via insert-if-absent; a duplicate detected
        // there surfaces as `DuplicateTxId` and the caller releases this
        // reservation.
        let _stripe_guard = self.locks.lock(&key);

        // Check for duplicate txid. G-4: `lookup_checked` so a backend
        // read error does not collapse to "absent" and pass the guard.
        if self
            .index
            .read()
            .lookup_checked(&key)
            .map_err(|e| CreateError::StorageError {
                detail: format!("duplicate-check index lookup failed: {e}"),
            })?
            .is_some()
        {
            return Err(CreateError::DuplicateTxId);
        }
        Self::external_ref_for_create(req)?;

        // Compute cold data size to determine total record size
        let cold_data = if req.is_external && req.inputs.is_none() {
            vec![]
        } else {
            build_cold_data(req.inputs, req.outputs, req.inpoints)
        };
        let cold_size = cold_data.len();

        let base_size = TxMetadata::record_size_for(utxo_count);
        let total_size = base_size + cold_size as u64;

        let record_offset = self
            .allocator
            .lock()
            .allocate(total_size)
            .map_err(|_| CreateError::DeviceFull)?;

        // F-G2-006: return the computed `total_size` so the caller can
        // pass it through to `create_at_offset` and we can defend the
        // implicit contract that both sites recompute the same value.
        // Pre-fix the two sites both rebuilt `cold_data` independently
        // from `req`; any future divergence (mutated `req`, swapped
        // `req`, non-deterministic builder) would silently desync the
        // on-device `record_size` from the allocator reservation and
        // corrupt the adjacent record.
        Ok((record_offset, utxo_count, total_size))
    }

    /// Create a transaction record at a pre-allocated device offset.
    ///
    /// Same as [`Self::create`] but skips allocation — the caller provides the
    /// `record_offset` obtained from [`Self::pre_allocate_create`]. Used by the
    /// WAL-first write path where the redo entry must be fsynced before
    /// the engine mutation.
    pub fn create_at_offset(
        &self,
        req: &CreateRequest,
        record_offset: u64,
    ) -> Result<CreateResponse, CreateError> {
        self.create_at_offset_inner(req, record_offset, None)
    }

    /// Variant of [`Self::create_at_offset`] that verifies the caller's
    /// reservation size matches the on-device `record_size` this function
    /// computes from `req`. F-G2-006: the dispatch layer reserves bytes via
    /// `pre_allocate_create` and then calls `create_at_offset` with what is
    /// supposed to be the same `req`. The recomputation is now defended
    /// with a `debug_assert_eq!` so any divergence (mutated request, swapped
    /// request, non-deterministic cold-data builder) panics in debug builds
    /// and surfaces a `StorageError` in release.
    pub fn create_at_offset_verified(
        &self,
        req: &CreateRequest,
        record_offset: u64,
        expected_total_size: u64,
    ) -> Result<CreateResponse, CreateError> {
        self.create_at_offset_inner(req, record_offset, Some(expected_total_size))
    }

    fn create_at_offset_inner(
        &self,
        req: &CreateRequest,
        record_offset: u64,
        expected_total_size: Option<u64>,
    ) -> Result<CreateResponse, CreateError> {
        let utxo_count = req.utxo_hashes.len() as u32;
        if utxo_count == 0 {
            return Err(CreateError::InvalidUtxoCount);
        }

        let key = req.tx_key();

        // Serialize with every other mutating op on this txid (audit A —
        // see `create`). Held across duplicate check → record write → index
        // registration; dropped before the parent-record updates below
        // because the parent's stripe may collide with this key's stripe.
        let stripe_guard = self.locks.lock(&key);

        // Duplicate check — another thread may have created it between
        // pre_allocate and now. (`register_new_with_shard_count` below
        // re-checks atomically; this early return skips the device write
        // for the common case.) G-4: `lookup_checked` so a backend read
        // error does not collapse to "absent" and pass the guard.
        if self
            .index
            .read()
            .lookup_checked(&key)
            .map_err(|e| CreateError::StorageError {
                detail: format!("duplicate-check index lookup failed: {e}"),
            })?
            .is_some()
        {
            return Err(CreateError::DuplicateTxId);
        }
        let external_ref = Self::external_ref_for_create(req)?;

        // Build cold data
        let cold_data = if req.is_external && req.inputs.is_none() {
            vec![]
        } else {
            build_cold_data(req.inputs, req.outputs, req.inpoints)
        };

        // F-G2-006: if the caller passed `pre_allocate_create`'s
        // `total_size`, defend the implicit contract that both sites
        // compute the same record layout. A mismatch means the request
        // was mutated between the two calls (or a different request
        // reached us) — the on-device `record_size` would otherwise
        // disagree with the allocator reservation and writes would
        // either under-fill or spill into the adjacent record.
        if let Some(expected) = expected_total_size {
            let base_size = TxMetadata::record_size_for(utxo_count);
            let actual = base_size + cold_data.len() as u64;
            debug_assert_eq!(
                actual, expected,
                "create_at_offset record_size diverged from pre_allocate_create \
                 reservation: pre_allocate={expected}, recomputed={actual}",
            );
            if actual != expected {
                return Err(CreateError::StorageError {
                    detail: format!(
                        "create_at_offset record_size {actual} != reservation {expected}",
                    ),
                });
            }
        }

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
        let base_size = TxMetadata::record_size_for(utxo_count);
        meta.record_size = (base_size + cold_data.len() as u64) as u32;

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

        if let Some(ext) = external_ref {
            meta.external_ref = ext;
        }

        if req.mined_block_infos.is_empty() {
            meta.unmined_since = req.block_height;
        } else {
            meta.unmined_since = 0;
            let entries = req.block_entries();
            let inline_count = entries.len().min(INLINE_BLOCK_ENTRIES);
            for (i, entry) in entries.iter().take(inline_count).enumerate() {
                meta.block_entries_inline[i] = *entry;
            }
            meta.block_entry_count = entries.len() as u8;
        }

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

        self.write_full_record_with_cold(record_offset, &meta, &slots, &cold_data)?;

        let index_entry = TxIndexEntry {
            device_id: 0,
            record_offset,
            utxo_count,
            block_entry_count: meta.block_entry_count,
            tx_flags: flags.bits(),
            spent_utxos: { meta.spent_utxos },
            dah_or_preserve: { meta.delete_at_height },
            unmined_since: { meta.unmined_since },
            generation: 0,
        };
        // Register in primary index AND increment shard_counts in the same
        // critical section so the two can never drift (H2 correctness fix).
        // `register_new_with_shard_count` rejects (never overwrites) an
        // existing key atomically with the insert.
        let inserted = self
            .register_new_with_shard_count(key, index_entry)
            .map_err(|e| CreateError::StorageError {
                detail: format!("{e}"),
            })?;
        if !inserted {
            // The reservation at `record_offset` is owned by the caller
            // (`pre_allocate_create` contract / dispatch batch path), which
            // releases it on any `Err` — do not free it here, that would be
            // a double free.
            return Err(CreateError::DuplicateTxId);
        }

        if meta.unmined_since != 0 {
            self.update_unmined_index(&key, 0, meta.unmined_since)
                .map_err(|e| CreateError::StorageError {
                    detail: format!("{e}"),
                })?;
        }

        // Drop the stripe guard before touching parent records:
        // `append_conflicting_child_best_effort` locks each parent's stripe,
        // and a parent txid may hash to this key's stripe.
        drop(stripe_guard);
        if req.conflicting {
            for parent_txid in req.parent_txids {
                let parent_key = TxKey { txid: *parent_txid };
                self.append_conflicting_child_best_effort(
                    &parent_key,
                    req.tx_id,
                    "create_at_offset",
                );
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

    /// Build the exact byte buffer that [`Self::create_at_offset`] would
    /// `pwrite` at `record_offset` (metadata header + UTXO slots + cold
    /// data, no device-alignment padding).
    ///
    /// Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): the WAL-first
    /// dispatch path captures these bytes inside `RedoOp::CreateV2` so
    /// crash recovery can reconstruct the on-device record byte-for-
    /// byte without re-running the engine's create logic. Mirrors
    /// `create_at_offset`'s flag/metadata derivation exactly so the
    /// captured bytes match the bytes the engine subsequently writes;
    /// any divergence would cause replay to leave a different record
    /// state than a successful create did, which is exactly the
    /// behaviour the gap is asking us to eliminate.
    ///
    /// Returns `(bytes, utxo_count)`.
    pub fn build_create_record_bytes(
        &self,
        req: &CreateRequest,
    ) -> Result<(Vec<u8>, u32), CreateError> {
        let utxo_count = req.utxo_hashes.len() as u32;
        if utxo_count == 0 {
            return Err(CreateError::InvalidUtxoCount);
        }
        let external_ref = Self::external_ref_for_create(req)?;

        // Mirror `create_at_offset` exactly. Any divergence here would
        // create a redo entry that, on replay, leaves the record in a
        // different state than a successful create did.
        let cold_data = if req.is_external && req.inputs.is_none() {
            vec![]
        } else {
            build_cold_data(req.inputs, req.outputs, req.inpoints)
        };

        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = req.tx_id;
        meta.tx_version = req.tx_version;
        meta.locktime = req.locktime;
        meta.fee = req.fee;
        meta.size_in_bytes = req.size_in_bytes;
        meta.extended_size = req.extended_size;
        meta.spending_height = req.spending_height;
        meta.created_at = req.created_at;
        let base_size = TxMetadata::record_size_for(utxo_count);
        meta.record_size = (base_size + cold_data.len() as u64) as u32;

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

        if let Some(ext) = external_ref {
            meta.external_ref = ext;
        }

        if req.mined_block_infos.is_empty() {
            meta.unmined_since = req.block_height;
        } else {
            meta.unmined_since = 0;
            let entries = req.block_entries();
            let inline_count = entries.len().min(INLINE_BLOCK_ENTRIES);
            for (i, entry) in entries.iter().take(inline_count).enumerate() {
                meta.block_entries_inline[i] = *entry;
            }
            meta.block_entry_count = entries.len() as u8;
        }

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

        // Serialize: METADATA_SIZE bytes of metadata, then each slot,
        // then cold data — exactly the layout `write_full_record_with_cold`
        // copies into the aligned buffer.
        let total_len = METADATA_SIZE + slots.len() * UTXO_SLOT_SIZE + cold_data.len();
        let mut out = Vec::with_capacity(total_len);
        let mut meta_bytes = [0u8; METADATA_SIZE];
        meta.to_bytes(&mut meta_bytes);
        out.extend_from_slice(&meta_bytes);
        for slot in &slots {
            let mut slot_bytes = [0u8; UTXO_SLOT_SIZE];
            slot.to_bytes(&mut slot_bytes);
            out.extend_from_slice(&slot_bytes);
        }
        out.extend_from_slice(&cold_data);
        debug_assert_eq!(out.len(), total_len);
        Ok((out, utxo_count))
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

        // F-X-007: the write must hold the record-level write guard. When
        // the allocator hands create a *reused* region, a lock-free reader
        // can still hold this offset from the previous occupant's index
        // entry and would otherwise observe this record half-written —
        // see `io::write_record_bytes` for the full aliasing scenario.
        io::write_record_bytes(&*self.device, record_offset, &buf).map_err(|e| {
            CreateError::StorageError {
                detail: format!("{e}"),
            }
        })?;

        Ok(())
    }

    /// Read cold data from a record.
    ///
    /// If cold data is stored inline on the device, reads it directly.
    /// If the record has the EXTERNAL flag and no inline cold data, falls
    /// back to the blobstore keyed by txid.
    ///
    /// F-G2-001: metadata reads on this lock-free path go through
    /// `read_metadata_for_key` so a `delete + create_at_offset` race never
    /// returns another transaction's cold data.
    pub fn read_cold_data(&self, key: &TxKey) -> Result<Vec<u8>, SpendError> {
        let entry = self
            .index
            .read()
            .lookup_checked(key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;

        // Check if cold data is in the external blobstore.
        //
        // F-IJ-005: branch on the EXTERNAL flag ALONE — not on the flag AND a
        // configured blob store. An EXTERNAL record whose store is `None`
        // previously fell through to the inline branch, where
        // `record_size == metadata + slots` yields `cold_size == 0` and the
        // call returned `Ok(vec![])` — the resurrected pre-F-G9-001 "silent
        // empty" bug. There is no inline cold data for an EXTERNAL record, so
        // an unresolvable external blob is a typed integrity error, never
        // empty bytes.
        if entry.tx_flags & TxFlags::EXTERNAL.bits() != 0 {
            let Some(ref blob_store) = self.blob_store else {
                // F-IJ-005: no store configured to resolve the external blob.
                return Err(SpendError::BlobNotFound { txid: key.txid });
            };
            let meta = self.read_metadata_for_key(key, entry.record_offset)?;
            match blob_store.get(&key.txid) {
                Ok(Some(data)) => {
                    if data.len() as u64 != meta.external_ref.total_size {
                        return Err(SpendError::StorageError {
                            detail: "blobstore read: external blob length does not match record ExternalRef"
                                .to_string(),
                        });
                    }
                    let mut hasher = Sha256::new();
                    hasher.update(&data);
                    let mut actual = [0u8; 32];
                    actual.copy_from_slice(&hasher.finalize());
                    if actual != meta.external_ref.content_hash {
                        return Err(SpendError::StorageError {
                            detail: "blobstore read: external blob digest does not match record ExternalRef"
                                .to_string(),
                        });
                    }
                    return Ok(data);
                }
                // F-IJ-001: a missing blob for an existing EXTERNAL record is
                // a data-integrity violation, NOT a missing transaction. The
                // record and its UTXOs are present and spendable; only the
                // cold data is gone. Surface `BlobNotFound` so the dispatcher
                // maps it to `ERR_BLOB_NOT_FOUND` (17) instead of
                // `ERR_TX_NOT_FOUND`, which would mask the loss.
                Ok(None) => return Err(SpendError::BlobNotFound { txid: key.txid }),
                Err(e) => {
                    return Err(SpendError::StorageError {
                        detail: format!("blobstore read: {e}"),
                    });
                }
            }
        }

        // Read metadata to determine record_size, then compute inline cold offset.
        let meta = self.read_metadata_for_key(key, entry.record_offset)?;
        let cold_intra = crate::storage::tiers::inline_cold_offset(entry.utxo_count);
        let cold_size = (meta.record_size as u64).saturating_sub(cold_intra);
        if cold_size == 0 {
            return Ok(vec![]);
        }

        let cold_offset = entry.record_offset + cold_intra;
        let align = self.device.alignment();
        let aligned_base = cold_offset / align as u64 * align as u64;
        let intra = (cold_offset - aligned_base) as usize;
        let read_len = (intra + cold_size as usize).div_ceil(align) * align;

        let mut buf = crate::device::AlignedBuf::new(read_len, align);
        self.device
            .pread_exact_at(&mut buf, aligned_base)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;

        Ok(buf[intra..intra + cold_size as usize].to_vec())
    }

    /// Return the distinct parent txids encoded in a child's cold-data
    /// input blob.
    pub fn parent_txids_for_child(&self, child_key: &TxKey) -> Result<Vec<[u8; 32]>, SpendError> {
        let cold_bytes = self.read_cold_data(child_key)?;
        extract_parent_txids_from_cold_data(&cold_bytes).map_err(|err| SpendError::StorageError {
            detail: format!("parse child parent txids: {err}"),
        })
    }

    /// Find parent UTXO slots currently spent by `child_txid`.
    ///
    /// Missing parents are treated as an empty result: parent records can
    /// legitimately have been pruned first or live on another shard in
    /// callers that do not perform ownership routing.
    pub fn slots_spent_by_child(
        &self,
        parent_key: &TxKey,
        child_txid: [u8; 32],
    ) -> Result<Vec<u32>, SpendError> {
        let _guard = self.locks.lock(parent_key);
        // G-4: a backend read error must not collapse to "parent absent"
        // (which would silently report no spent slots).
        let entry = match self.index.read().lookup_checked(parent_key).map_err(|e| {
            SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            }
        })? {
            Some(entry) => entry,
            None => return Ok(Vec::new()),
        };
        let meta = self.read_metadata_fast(entry.record_offset)?;
        let mut offsets = Vec::new();
        let utxo_count = { meta.utxo_count };
        for offset in 0..utxo_count {
            let slot = self.read_slot_fast(entry.record_offset, offset)?;
            if slot.status == UTXO_SPENT && slot.spending_data[..32] == child_txid[..] {
                offsets.push(offset);
            }
        }
        Ok(offsets)
    }

    /// Mark a parent UTXO slot as PRUNED if it is still spent by the
    /// supplied child txid.
    ///
    /// This is idempotent: already-pruned slots and slots no longer spent
    /// by `child_txid` are left unchanged.
    pub fn prune_slot_if_spent_by_child(
        &self,
        parent_key: &TxKey,
        offset: u32,
        child_txid: [u8; 32],
    ) -> Result<bool, SpendError> {
        let _guard = self.locks.lock(parent_key);
        // G-4: a backend read error must not collapse to "parent absent"
        // (which would silently report the slot was not pruned).
        let entry = match self.index.read().lookup_checked(parent_key).map_err(|e| {
            SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            }
        })? {
            Some(entry) => entry,
            None => return Ok(false),
        };
        let mut meta = self.read_metadata_fast(entry.record_offset)?;
        if offset >= { meta.utxo_count } {
            return Ok(false);
        }
        let mut slot = self.read_slot_fast(entry.record_offset, offset)?;
        if slot.status == UTXO_PRUNED {
            return Ok(false);
        }
        if slot.status != UTXO_SPENT || slot.spending_data[..32] != child_txid[..] {
            return Ok(false);
        }
        slot.status = UTXO_PRUNED;
        self.write_slot_fast(entry.record_offset, offset, &slot)?;
        // F-G2-017: switch from `saturating_sub`/`saturating_add` to
        // `checked_*` so a violation of the per-record invariant
        // surfaces as a `StorageError` instead of silently clamping.
        // The earlier `slot.status == UTXO_PRUNED` short-circuit
        // (line above) and `UTXO_SPENT` guard mean these arithmetic
        // ops are unreachable-overflow in current code; the explicit
        // check is defense-in-depth for any future change that
        // re-orders the guards.
        meta.spent_utxos =
            { meta.spent_utxos }
                .checked_sub(1)
                .ok_or_else(|| SpendError::StorageError {
                    detail: "prune_slot_if_spent_by_child: spent_utxos underflow".into(),
                })?;
        meta.pruned_utxos =
            { meta.pruned_utxos }
                .checked_add(1)
                .ok_or_else(|| SpendError::StorageError {
                    detail: "prune_slot_if_spent_by_child: pruned_utxos overflow".into(),
                })?;
        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = self.now_millis();
        self.write_metadata_fast(entry.record_offset, &meta)?;
        self.sync_index_cache(parent_key, &meta)?;
        // F-X-022: Aerospike `addDeletedChildren` parity. The prune above
        // is the PRIMARY defense (UTXO_PRUNED is the on-disk slot status
        // every spend path checks first). The append below is the
        // SECONDARY audit/diagnostic trail + defense-in-depth at the
        // idempotent-respend short-circuit in `spend`. Best-effort: a
        // failure here logs but does NOT roll back the prune — the prune
        // is the safety-critical mutation and `UTXO_PRUNED` already
        // protects against re-spend. Replicas receive the prune via the
        // existing `ReplicaOp::PruneSlotIfSpentBy` path and run the same
        // `prune_slot_if_spent_by_child` here, so they each append to
        // their own local deleted-children list (no separate replicated
        // op needed — the append is derived state).
        //
        // Lock ordering: this is OUTSIDE the parent's stripe lock guard
        // (the `let _guard = ...` at the top of this function dropped at
        // the natural `}` below). `append_deleted_child` re-acquires
        // the parent lock internally via the same CAS-retry pattern as
        // `append_conflicting_child` (R-143).
        drop(_guard);
        self.append_deleted_child_best_effort(parent_key, child_txid, "prune_slot_if_spent_by_child");
        Ok(true)
    }

    // -----------------------------------------------------------------------
    // Remaining operations (Phase 6)
    // -----------------------------------------------------------------------

    /// Freeze a UTXO (set status to FROZEN, spending_data all 0xFF).
    ///
    /// Does NOT modify metadata counters — frozen does not count as "spent".
    ///
    /// F-G2-012: freeze/unfreeze does NOT touch `spent_utxos` and therefore
    /// cannot cross the all-spent boundary — DAH eval is intentionally
    /// omitted. `evaluate_delete_at_height` gates eligibility on
    /// `spent_utxos == utxo_count`; a freeze can never change that
    /// equality, so re-evaluating after a freeze would be a guaranteed
    /// no-op that adds an index round-trip. Do not add an eval call here
    /// without changing the invariant.
    pub fn freeze(&self, req: &FreezeRequest) -> Result<u32, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let mut meta = self.read_metadata_fast(ro)?;
        if req.offset >= { meta.utxo_count } {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        let slot = self.read_slot_fast(ro, req.offset)?;
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

        // LP-4: preserve any reassignment cooldown sitting in the unspent
        // slot's `spending_data[0..4]` instead of overwriting it with the
        // all-`0xFF` frozen marker. The Lua reference keeps the cooldown in a
        // separate `utxoSpendableIn` bin that freeze/unfreeze never touch
        // (teranode.lua:928-942 vs 707-779/789-852); TeraSlab stores it in the
        // slot, so freeze must not wipe it. The cooldown is carried through
        // the frozen state in the first 4 bytes; the `UTXO_FROZEN` status byte
        // remains the authoritative frozen signal. `cooldown == 0` → identical
        // to a plain frozen marker.
        let cooldown = slot.reassignment_cooldown();
        let frozen = if cooldown == 0 {
            UtxoSlot::new_frozen(req.utxo_hash)
        } else {
            UtxoSlot::new_frozen_with_cooldown(req.utxo_hash, cooldown)
        };
        self.write_slot_fast(ro, req.offset, &frozen)?;
        // R-016 (A-08): bump generation, write metadata back, sync the
        // index cache so subsequent fast-path ops (set_mined,
        // set_conflicting, set_locked, preserve_until) see the
        // post-freeze flags. Without this, the cached `tx_flags`
        // diverges from the on-device state and fast paths miscompute
        // DAH eligibility.
        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = self.now_millis();
        self.write_metadata_fast(ro, &meta)?;
        self.sync_index_cache(&req.tx_key, &meta)?;
        Ok(meta.generation)
    }

    /// Unfreeze a UTXO (set status to UNSPENT, spending_data zeroed).
    ///
    /// F-G2-012: like `freeze`, this does NOT touch `spent_utxos` and is
    /// not a candidate for DAH evaluation. See [`Self::freeze`] for the
    /// full rationale.
    pub fn unfreeze(&self, req: &UnfreezeRequest) -> Result<u32, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let mut meta = self.read_metadata_fast(ro)?;
        if req.offset >= { meta.utxo_count } {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        let slot = self.read_slot_fast(ro, req.offset)?;
        if slot.hash != req.utxo_hash {
            return Err(SpendError::UtxoHashMismatch { offset: req.offset });
        }
        if slot.status != UTXO_FROZEN {
            return Err(SpendError::NotFrozen { offset: req.offset });
        }

        // LP-4: restore any reassignment cooldown that `freeze` preserved in
        // the frozen slot's `spending_data[0..4]`. A legacy all-`0xFF` frozen
        // slot reads back `u32::MAX` there — that is the "no cooldown" marker,
        // not a real (absurdly-far-future) spendable height, so it restores to
        // an immediately-spendable unspent slot. A cooldown written by
        // `new_frozen_with_cooldown` is well below `u32::MAX` (guarded by the
        // `checked_add` in `reassign`) and is restored verbatim, so the
        // safety window survives the freeze/unfreeze round-trip.
        let cooldown = slot.reassignment_cooldown();
        let unspent = if cooldown == 0 || cooldown == u32::MAX {
            UtxoSlot::new_unspent(req.utxo_hash)
        } else {
            UtxoSlot::new_unspent_with_cooldown(req.utxo_hash, cooldown)
        };
        self.write_slot_fast(ro, req.offset, &unspent)?;
        // R-016 (A-08): see `freeze` — bump gen + sync cache so the
        // next mutation sees the post-unfreeze flags.
        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = self.now_millis();
        self.write_metadata_fast(ro, &meta)?;
        self.sync_index_cache(&req.tx_key, &meta)?;
        Ok(meta.generation)
    }

    /// Reassign a frozen UTXO to a new hash with a spendable-after cooldown.
    pub fn reassign(&self, req: &ReassignRequest) -> Result<u32, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let mut meta = self.read_metadata_fast(ro)?;
        if req.offset >= { meta.utxo_count } {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        // R-017 (A-09): a reassign IS a spend-equivalent state
        // transition (it produces a new spendable UTXO under a fresh
        // hash, with a cooldown). The same record-level guards that
        // protect Spend must therefore also guard Reassign — pre-fix
        // a record marked LOCKED, CONFLICTING, or IS_COINBASE-immature
        // could still be reassigned, bypassing the protections those
        // flags exist to enforce. Coinbase maturity uses the request's
        // `block_height` as the "current height" of the reassign — the
        // request lacks a separate `current_block_height` field, but
        // `block_height` is the block in which the reassign is being
        // committed, which serves the same purpose for the maturity
        // comparison.
        if meta.flags.contains(TxFlags::CONFLICTING) {
            return Err(SpendError::Conflicting);
        }
        if meta.flags.contains(TxFlags::LOCKED) {
            return Err(SpendError::Locked);
        }
        let spending_height = { meta.spending_height };
        if meta.flags.contains(TxFlags::IS_COINBASE)
            && spending_height > 0
            && spending_height > req.block_height
        {
            return Err(SpendError::CoinbaseImmature {
                spending_height,
                current_height: req.block_height,
            });
        }

        let slot = self.read_slot_fast(ro, req.offset)?;
        if slot.hash != req.utxo_hash {
            return Err(SpendError::UtxoHashMismatch { offset: req.offset });
        }
        if slot.status != UTXO_FROZEN {
            return Err(SpendError::NotFrozen { offset: req.offset });
        }

        // R-063 (A-13): use checked_add. Pre-fix the engine used
        // `saturating_add`, which silently clamped to `u32::MAX` and
        // pinned the UTXO unspendable forever — the
        // `spendable_height > req.current_block_height` gate in the
        // spend path would always be true. Now surfaces as
        // `SpendError::ReassignOverflow` so the operator catches the
        // pathological input.
        let spendable_height = req.block_height.checked_add(req.spendable_after).ok_or(
            SpendError::ReassignOverflow {
                block_height: req.block_height,
                spendable_after: req.spendable_after,
            },
        )?;
        let mut new_slot = UtxoSlot::new_unspent(req.new_utxo_hash);
        new_slot.spending_data[0..4].copy_from_slice(&spendable_height.to_le_bytes());

        self.write_slot_fast(ro, req.offset, &new_slot)?;

        // Update metadata (generation, updated_at, reassignment_count).
        // LP-3: mark the record REASSIGNED so the all-spent DAH path in
        // `evaluate_delete_at_height` permanently excludes it — the Lua
        // reference inflates `recordUtxos` by 1 (`teranode.lua:945`) for the
        // same effect: a reassigned (court-ordered) record is never pruned,
        // preserving the old-hash → new-hash audit trail. A live reassigned
        // UTXO is already safe from deletion (frozen does not count toward
        // `spent_utxos`, so the all-spent check is false until the reassigned
        // slot is itself spent); this flag also covers the after-final-spend
        // window, matching the reference's permanent retention.
        meta.flags |= TxFlags::REASSIGNED;
        meta.reassignment_count = meta.reassignment_count.saturating_add(1);
        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = self.now_millis();
        self.write_metadata_fast(ro, &meta)?;

        self.sync_index_cache(&req.tx_key, &meta)?;

        let generation = { meta.generation };
        Ok(generation)
    }

    /// Append a child txid to a parent record's conflicting-children list.
    /// Deduplicates: if the child already exists, this is a no-op.
    /// Returns Ok(()) if parent not found (may be on another node).
    pub fn append_conflicting_child(
        &self,
        parent_key: &TxKey,
        child_txid: [u8; 32],
    ) -> Result<(), SpendError> {
        // F-G2-005: bound the retry loop. Pre-fix this loop had no cap;
        // pathological contention (many simultaneous reorgs against the
        // same parent) could burn allocator/device cycles indefinitely.
        // 16 retries with exponential back-off (1us..32ms) gives the
        // contending writers time to drain while still surfacing the
        // problem to the operator instead of stalling silently.
        const MAX_RETRIES: u32 = 16;
        let mut intent_logged = false;
        let mut attempt: u32 = 0;
        loop {
            let (ro, count, offset, mut children) = {
                let _guard = self.locks.lock(parent_key);
                // G-4: a backend read error must not collapse to "parent
                // absent" (which would silently no-op the child append).
                let entry = match self.index.read().lookup_checked(parent_key).map_err(|e| {
                    SpendError::StorageError {
                        detail: format!("index lookup failed: {e}"),
                    }
                })? {
                    Some(e) => e,
                    None => return Ok(()),
                };
                let ro = entry.record_offset;
                let meta = self.read_metadata_fast(ro)?;
                let count = { meta.conflicting_children_count } as usize;
                let offset = { meta.conflicting_children_offset };

                let children = self.read_conflicting_children_at(count, offset)?;
                if children.contains(&child_txid) {
                    return Ok(());
                }

                (ro, count, offset, children)
            };

            children.push(child_txid);
            if children.len() > u8::MAX as usize {
                // KO-5: the on-device count is a single `u8`; the 256th child
                // cannot be recorded. Surface a distinct, typed overflow so
                // the best-effort wrapper can escalate + count the loss
                // instead of swallowing a generic I/O error into a warn.
                return Err(SpendError::ConflictingChildrenFull {
                    cap: u8::MAX as usize,
                });
            }

            // R-221: the parent metadata update below points at a newly
            // allocated children-list block. Persist the high-level append
            // intent before any allocator/new-block work so a crash after the
            // replacement block write but before the metadata write can be
            // recovered by replaying this idempotent append after engine
            // construction.
            if !intent_logged {
                if let Some(log) = self.redo_log_handle() {
                    log.lock()
                        .append_and_flush(crate::redo::RedoOp::AppendConflictingChild {
                            parent_key: *parent_key,
                            child_txid,
                        })
                        .map_err(|e| SpendError::StorageError {
                            detail: format!("append conflicting child redo: {e}"),
                        })?;
                }
                intent_logged = true;
            }

            // R-024 keeps the old block allocated until metadata points at a
            // fully-written replacement. R-143 additionally keeps allocator
            // work outside the parent stripe lock: prepare the replacement
            // unlocked, then re-lock only to validate the snapshot and commit.
            let new_offset = self.allocate_conflicting_children_block(&children)?;

            let mut parent_gone = false;
            let committed = {
                let _guard = self.locks.lock(parent_key);
                // G-4: a backend read error must not collapse to "parent
                // absent" (which would free the freshly-allocated block as
                // if the parent vanished). Surface it as a storage error.
                let looked_up =
                    self.index
                        .read()
                        .lookup_checked(parent_key)
                        .map_err(|e| SpendError::StorageError {
                            detail: format!("index lookup failed: {e}"),
                        })?;
                match looked_up {
                    None => {
                        parent_gone = true;
                        false
                    }
                    Some(entry) if entry.record_offset != ro => false,
                    Some(_) => {
                        let mut meta = self.read_metadata_fast(ro)?;
                        let latest_count = { meta.conflicting_children_count } as usize;
                        let latest_offset = { meta.conflicting_children_offset };
                        if latest_count != count || latest_offset != offset {
                            false
                        } else {
                            meta.conflicting_children_count = children.len() as u8;
                            meta.conflicting_children_offset = new_offset;
                            meta.generation = { meta.generation }.wrapping_add(1);
                            meta.updated_at = self.now_millis();
                            self.write_metadata_fast(ro, &meta)?;
                            true
                        }
                    }
                }
            };

            if parent_gone {
                self.free_conflicting_children_block(new_offset, children.len())?;
                return Ok(());
            }

            if committed {
                if count > 0 && offset != 0 {
                    // Post-commit cleanup: parent metadata now points at
                    // `new_offset`; the old block at `offset` is orphaned
                    // until freed. If `free` fails we cannot propagate
                    // because the user-visible append SUCCEEDED — surface
                    // the leak via a high-cardinality tracing event so
                    // operators can correlate and reclaim manually (see
                    // R-049 orphan-blob GC for the periodic sweep).
                    if let Err(err) =
                        self.free_conflicting_children_block(offset, count)
                    {
                        tracing::error!(
                            target: "teraslab::engine::orphan",
                            orphan = true,
                            kind = "conflicting_children_old_block",
                            offset = offset,
                            bytes = (count * 32) as u64,
                            error = %err,
                            "post-commit free of old conflicting-children block failed; bytes leaked until R-049 sweep"
                        );
                    }
                }
                return Ok(());
            }

            self.free_conflicting_children_block(new_offset, children.len())?;

            attempt += 1;
            if attempt >= MAX_RETRIES {
                return Err(SpendError::StorageError {
                    detail: format!(
                        "append_conflicting_child: CAS contention exceeded \
                         {MAX_RETRIES} retries on parent — likely concurrent \
                         reorg storm against the same parent record",
                    ),
                });
            }
            // Exponential back-off (1us → 2us → ... capped at ~32ms) to
            // give the contending writer a chance to commit so the next
            // attempt sees a stable snapshot.
            let backoff_us = 1u64 << attempt.min(15);
            std::thread::sleep(std::time::Duration::from_micros(backoff_us));
        }
    }

    fn read_conflicting_children_at(
        &self,
        count: usize,
        offset: u64,
    ) -> Result<Vec<[u8; 32]>, SpendError> {
        let mut children: Vec<[u8; 32]> = Vec::with_capacity(count + 1);
        if count == 0 || offset == 0 {
            return Ok(children);
        }

        let align = self.device.alignment();
        let aligned_base = offset / align as u64 * align as u64;
        let intra = (offset - aligned_base) as usize;
        let read_len = (intra + count * 32).div_ceil(align) * align;
        let mut buf = crate::device::AlignedBuf::new(read_len, align);
        self.device
            .pread_exact_at(&mut buf, aligned_base)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;
        for i in 0..count {
            let start = intra + i * 32;
            let mut txid = [0u8; 32];
            txid.copy_from_slice(&buf[start..start + 32]);
            children.push(txid);
        }
        Ok(children)
    }

    fn allocate_conflicting_children_block(
        &self,
        children: &[[u8; 32]],
    ) -> Result<u64, SpendError> {
        let new_size = (children.len() * 32) as u64;
        let new_offset =
            self.allocator
                .lock()
                .allocate(new_size)
                .map_err(|_| SpendError::StorageError {
                    detail: "device full for conflicting children".into(),
                })?;

        let align = self.device.alignment();
        let aligned_base = new_offset / align as u64 * align as u64;
        let intra = (new_offset - aligned_base) as usize;
        let write_len = (intra + children.len() * 32).div_ceil(align) * align;
        let mut wbuf = crate::device::AlignedBuf::new(write_len, align);
        for (i, child) in children.iter().enumerate() {
            wbuf[intra + i * 32..intra + (i + 1) * 32].copy_from_slice(child);
        }
        if let Err(err) = self.device.pwrite_all_at(&wbuf, aligned_base) {
            // pwrite failed — roll back the freshly-allocated extent so
            // we don't leak it. If the rollback itself fails, surface
            // the leak via tracing (we still need to return the
            // original pwrite error to the caller) so operators can
            // correlate against the R-049 orphan-blob sweep.
            if let Err(free_err) =
                self.free_conflicting_children_block(new_offset, children.len())
            {
                tracing::error!(
                    target: "teraslab::engine::orphan",
                    orphan = true,
                    kind = "conflicting_children_alloc_rollback",
                    offset = new_offset,
                    bytes = (children.len() * 32) as u64,
                    pwrite_error = %err,
                    free_error = %free_err,
                    "rollback free after failed conflicting-children pwrite also failed; bytes leaked until R-049 sweep"
                );
            }
            return Err(SpendError::StorageError {
                detail: format!("{err}"),
            });
        }

        Ok(new_offset)
    }

    fn free_conflicting_children_block(&self, offset: u64, count: usize) -> Result<(), SpendError> {
        self.allocator
            .lock()
            .free(offset, (count * 32) as u64)
            .map_err(|e| SpendError::StorageError {
                detail: format!("allocator free for conflicting children failed: {e}"),
            })
    }

    /// Read all conflicting children txids for a transaction.
    pub fn read_conflicting_children(&self, key: &TxKey) -> Result<Vec<[u8; 32]>, SpendError> {
        let entry = self
            .index
            .read()
            .lookup_checked(key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;
        let meta = self.read_metadata_fast(ro)?;

        let count = { meta.conflicting_children_count } as usize;
        let offset = { meta.conflicting_children_offset };
        self.read_conflicting_children_at(count, offset)
    }

    fn append_conflicting_child_best_effort(
        &self,
        parent_key: &TxKey,
        child_txid: [u8; 32],
        source: &'static str,
    ) {
        if let Err(err) = self.append_conflicting_child(parent_key, child_txid) {
            if let SpendError::ConflictingChildrenFull { cap } = err {
                // KO-5: capacity overflow is a correctness-relevant loss (a
                // counter-conflicting descendant is dropped from the parent's
                // cascade list), not a transient I/O hiccup. Record it on an
                // engine counter and escalate to ERROR so it is observable
                // rather than vanishing into a warn-level ops-log Easter egg.
                self.conflicting_children_dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    target: "teraslab::engine::conflicting",
                    overflow = true,
                    cap,
                    ?parent_key,
                    ?child_txid,
                    source,
                    "conflicting-children list full; child dropped — counter-conflicting cascade truncated"
                );
            } else {
                tracing::warn!(
                    ?parent_key,
                    ?child_txid,
                    ?err,
                    source,
                    "failed to append conflicting child"
                );
            }
        }
    }

    /// KO-5: number of conflicting-child appends that have been dropped
    /// because the parent record's on-disk children list was already at the
    /// `u8::MAX` (255) capacity.
    ///
    /// A non-zero value means at least one counter-conflicting cascade was
    /// truncated server-side: the dropped child txid is NOT present in the
    /// parent's `CONFLICTING_CHILDREN` field, so a client relying on that
    /// field to enumerate descendants will miss it. Operators should alert
    /// on any increase. Monotonic for the lifetime of the engine.
    pub fn conflicting_children_dropped(&self) -> u64 {
        self.conflicting_children_dropped
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    // -----------------------------------------------------------------------
    // F-X-022 — Deleted children list (Aerospike `addDeletedChildren` parity)
    //
    // The deleted-children list is a per-parent-record audit/diagnostic of
    // every child txid that has been pruned against the parent. It mirrors
    // the conflicting-children list bit-for-bit (count u8 + offset u64 in
    // metadata, separately-allocated block of 32-byte txids on device), and
    // the engine methods below are intentional clones of the
    // `conflicting_children` variants — same CAS retry loop, same
    // allocate-out-of-lock pattern, same orphan-blob tracing on rollback.
    //
    // The list is the SECONDARY defense against the
    // resurrected-then-pruned re-spend pattern. The PRIMARY defense remains
    // the slot's `UTXO_PRUNED` status flipped by `prune_slot_if_spent_by_child`
    // — every spend path consults the slot status first and rejects PRUNED
    // outright. The deleted-children list adds (1) operator-visible audit of
    // which children pruned which parent slots, and (2) defense-in-depth at
    // the idempotent-respend short-circuit where the slot LOOKS spent (so
    // the PRUNED check has already passed) but the spending child has been
    // pruned by some unusual code path that flipped the slot back to SPENT.
    // -----------------------------------------------------------------------

    /// Append a child txid to a parent record's deleted-children list.
    /// Deduplicates: if the child already exists, this is a no-op.
    /// Returns Ok(()) if the parent is not found (may be on another node).
    ///
    /// F-X-022: Aerospike `addDeletedChildren` parity. Mirrors
    /// [`Self::append_conflicting_child`] — see that method for the full
    /// rationale on the CAS retry loop, the allocate-out-of-lock pattern,
    /// and the orphan-blob tracing fall-back on rollback.
    pub fn append_deleted_child(
        &self,
        parent_key: &TxKey,
        child_txid: [u8; 32],
    ) -> Result<(), SpendError> {
        const MAX_RETRIES: u32 = 16;
        let mut intent_logged = false;
        let mut attempt: u32 = 0;
        loop {
            let (ro, count, offset, mut children) = {
                let _guard = self.locks.lock(parent_key);
                // G-4: a backend read error must not collapse to "parent
                // absent" (which would silently no-op the child append).
                let entry = match self.index.read().lookup_checked(parent_key).map_err(|e| {
                    SpendError::StorageError {
                        detail: format!("index lookup failed: {e}"),
                    }
                })? {
                    Some(e) => e,
                    None => return Ok(()),
                };
                let ro = entry.record_offset;
                let meta = self.read_metadata_fast(ro)?;
                let count = { meta.deleted_children_count } as usize;
                let offset = { meta.deleted_children_offset };

                let children = self.read_deleted_children_at(count, offset)?;
                if children.contains(&child_txid) {
                    return Ok(());
                }

                (ro, count, offset, children)
            };

            children.push(child_txid);
            if children.len() > u8::MAX as usize {
                return Err(SpendError::StorageError {
                    detail: "deleted children limit exceeded".into(),
                });
            }

            // Persist the high-level append intent before any
            // allocator/new-block work so a crash after the replacement
            // block write but before the metadata write can be recovered
            // by replaying this idempotent append. The redo entry is
            // emitted AFTER the prune entry (the prune is logically
            // primary — UTXO_PRUNED remains the primary defense).
            if !intent_logged {
                if let Some(log) = self.redo_log_handle() {
                    log.lock()
                        .append_and_flush(crate::redo::RedoOp::AppendDeletedChild {
                            parent_key: *parent_key,
                            child_txid,
                        })
                        .map_err(|e| SpendError::StorageError {
                            detail: format!("append deleted child redo: {e}"),
                        })?;
                }
                intent_logged = true;
            }

            let new_offset = self.allocate_deleted_children_block(&children)?;

            let mut parent_gone = false;
            let committed = {
                let _guard = self.locks.lock(parent_key);
                // G-4: a backend read error must not collapse to "parent
                // absent" (which would free the freshly-allocated block as
                // if the parent vanished). Surface it as a storage error.
                let looked_up =
                    self.index
                        .read()
                        .lookup_checked(parent_key)
                        .map_err(|e| SpendError::StorageError {
                            detail: format!("index lookup failed: {e}"),
                        })?;
                match looked_up {
                    None => {
                        parent_gone = true;
                        false
                    }
                    Some(entry) if entry.record_offset != ro => false,
                    Some(_) => {
                        let mut meta = self.read_metadata_fast(ro)?;
                        let latest_count = { meta.deleted_children_count } as usize;
                        let latest_offset = { meta.deleted_children_offset };
                        if latest_count != count || latest_offset != offset {
                            false
                        } else {
                            meta.deleted_children_count = children.len() as u8;
                            meta.deleted_children_offset = new_offset;
                            meta.generation = { meta.generation }.wrapping_add(1);
                            meta.updated_at = self.now_millis();
                            self.write_metadata_fast(ro, &meta)?;
                            true
                        }
                    }
                }
            };

            if parent_gone {
                self.free_deleted_children_block(new_offset, children.len())?;
                return Ok(());
            }

            if committed {
                if count > 0
                    && offset != 0
                    && let Err(err) = self.free_deleted_children_block(offset, count)
                {
                    tracing::error!(
                        target: "teraslab::engine::orphan",
                        orphan = true,
                        kind = "deleted_children_old_block",
                        offset = offset,
                        bytes = (count * 32) as u64,
                        error = %err,
                        "post-commit free of old deleted-children block failed; bytes leaked until R-049 sweep"
                    );
                }
                return Ok(());
            }

            self.free_deleted_children_block(new_offset, children.len())?;

            attempt += 1;
            if attempt >= MAX_RETRIES {
                return Err(SpendError::StorageError {
                    detail: format!(
                        "append_deleted_child: CAS contention exceeded \
                         {MAX_RETRIES} retries on parent — likely concurrent \
                         prune storm against the same parent record",
                    ),
                });
            }
            let backoff_us = 1u64 << attempt.min(15);
            std::thread::sleep(std::time::Duration::from_micros(backoff_us));
        }
    }

    fn read_deleted_children_at(
        &self,
        count: usize,
        offset: u64,
    ) -> Result<Vec<[u8; 32]>, SpendError> {
        let mut children: Vec<[u8; 32]> = Vec::with_capacity(count + 1);
        if count == 0 || offset == 0 {
            return Ok(children);
        }

        let align = self.device.alignment();
        let aligned_base = offset / align as u64 * align as u64;
        let intra = (offset - aligned_base) as usize;
        let read_len = (intra + count * 32).div_ceil(align) * align;
        let mut buf = crate::device::AlignedBuf::new(read_len, align);
        self.device
            .pread_exact_at(&mut buf, aligned_base)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;
        for i in 0..count {
            let start = intra + i * 32;
            let mut txid = [0u8; 32];
            txid.copy_from_slice(&buf[start..start + 32]);
            children.push(txid);
        }
        Ok(children)
    }

    fn allocate_deleted_children_block(
        &self,
        children: &[[u8; 32]],
    ) -> Result<u64, SpendError> {
        let new_size = (children.len() * 32) as u64;
        let new_offset =
            self.allocator
                .lock()
                .allocate(new_size)
                .map_err(|_| SpendError::StorageError {
                    detail: "device full for deleted children".into(),
                })?;

        let align = self.device.alignment();
        let aligned_base = new_offset / align as u64 * align as u64;
        let intra = (new_offset - aligned_base) as usize;
        let write_len = (intra + children.len() * 32).div_ceil(align) * align;
        let mut wbuf = crate::device::AlignedBuf::new(write_len, align);
        for (i, child) in children.iter().enumerate() {
            wbuf[intra + i * 32..intra + (i + 1) * 32].copy_from_slice(child);
        }
        if let Err(err) = self.device.pwrite_all_at(&wbuf, aligned_base) {
            if let Err(free_err) =
                self.free_deleted_children_block(new_offset, children.len())
            {
                tracing::error!(
                    target: "teraslab::engine::orphan",
                    orphan = true,
                    kind = "deleted_children_alloc_rollback",
                    offset = new_offset,
                    bytes = (children.len() * 32) as u64,
                    pwrite_error = %err,
                    free_error = %free_err,
                    "rollback free after failed deleted-children pwrite also failed; bytes leaked until R-049 sweep"
                );
            }
            return Err(SpendError::StorageError {
                detail: format!("{err}"),
            });
        }

        Ok(new_offset)
    }

    fn free_deleted_children_block(&self, offset: u64, count: usize) -> Result<(), SpendError> {
        self.allocator
            .lock()
            .free(offset, (count * 32) as u64)
            .map_err(|e| SpendError::StorageError {
                detail: format!("allocator free for deleted children failed: {e}"),
            })
    }

    /// Read all deleted children txids for a transaction.
    ///
    /// F-X-022: Aerospike `addDeletedChildren` parity. Returns an empty
    /// vec when the parent has never had a child pruned against it.
    pub fn read_deleted_children(&self, key: &TxKey) -> Result<Vec<[u8; 32]>, SpendError> {
        let entry = self
            .index
            .read()
            .lookup_checked(key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;
        let meta = self.read_metadata_fast(ro)?;

        let count = { meta.deleted_children_count } as usize;
        let offset = { meta.deleted_children_offset };
        self.read_deleted_children_at(count, offset)
    }

    fn append_deleted_child_best_effort(
        &self,
        parent_key: &TxKey,
        child_txid: [u8; 32],
        source: &'static str,
    ) {
        if let Err(err) = self.append_deleted_child(parent_key, child_txid) {
            tracing::warn!(
                ?parent_key,
                ?child_txid,
                ?err,
                source,
                "failed to append deleted child"
            );
        }
    }

    fn append_conflicting_children_from_cold_data(&self, child_key: &TxKey, source: &'static str) {
        let cold_bytes = match self.read_cold_data(child_key) {
            Ok(cold_bytes) => cold_bytes,
            Err(err) => {
                tracing::warn!(
                    ?child_key,
                    ?err,
                    source,
                    "failed to read cold data for conflicting-child propagation"
                );
                return;
            }
        };

        let parent_txids = match extract_parent_txids_from_cold_data(&cold_bytes) {
            Ok(parent_txids) => parent_txids,
            Err(err) => {
                tracing::warn!(
                    ?child_key,
                    err,
                    source,
                    "failed to parse cold data for conflicting-child propagation"
                );
                return;
            }
        };

        for parent_txid in parent_txids {
            let parent_key = TxKey { txid: parent_txid };
            self.append_conflicting_child_best_effort(&parent_key, child_key.txid, source);
        }
    }

    /// Set or clear the conflicting flag on a transaction.
    pub fn set_conflicting(
        &self,
        req: &SetConflictingRequest,
    ) -> Result<SetConflictingResponse, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        // Fast path: read the authoritative on-device metadata once and
        // derive every flag/DAH/counter/generation input from it. The RMW
        // already needs this read for the CRC, so it is free.
        //
        // KO-11: pre-fix this path sourced `tf`, `has_preserve`, `old_dah`,
        // the `evaluate_dah_cached` inputs, AND `generation` from the cached
        // `entry`. After a prior mutation that wrote metadata but failed at
        // `sync_index_cache`, those cached fields are stale — the device had
        // already advanced. Using stale `old_dah`/flags here would compute a
        // wrong DAH-index delta and stamp a mis-flagged post-state. F-G2-011
        // had fixed only `generation` in the set_mined path; this brings the
        // set_conflicting path fully onto fresh `meta`.
        let response = if !self.device_ptr.is_null() {
            // SAFETY: `device_ptr` is non-null (fast-path gate) and live for
            // the engine's lifetime; `ro` is the allocator-valid record
            // offset from the index entry. The set_conflicting caller holds
            // this record's stripe lock; `read_metadata_direct` takes the
            // per-offset `io_locks()` read side, so the read is
            // torn-read-safe.
            let mut meta = unsafe {
                io::read_metadata_direct(self.device_ptr, ro).map_err(|e| {
                    SpendError::StorageError {
                        detail: format!("{e}"),
                    }
                })?
            };

            let mut tf = meta.flags;
            let preserve = { meta.preserve_until };
            let has_preserve = preserve != 0;
            let meta_dah = { meta.delete_at_height };
            let old_dah = if has_preserve { 0 } else { meta_dah };
            let meta_spent = { meta.spent_utxos };
            let meta_utxo_count = { meta.utxo_count };
            let meta_block_count = { meta.block_entry_count };
            let meta_unmined = { meta.unmined_since };

            if req.value {
                tf.insert(TxFlags::CONFLICTING);
            } else {
                tf.remove(TxFlags::CONFLICTING);
            }

            let (signal, dah_patch) = crate::ops::delete_eval::evaluate_dah_cached(
                tf,
                meta_spent,
                meta_utxo_count,
                meta_block_count,
                meta_unmined,
                has_preserve,
                if has_preserve { preserve } else { meta_dah },
                req.current_block_height,
                req.block_height_retention,
            )?;
            let mut new_dah = old_dah;
            if let Some(ref patch) = dah_patch {
                tf.set(TxFlags::LAST_SPENT_ALL, patch.last_spent_all);
                new_dah = patch.new_delete_at_height;
            }

            // Generation derives from the on-device value, not the cache.
            let generation = { meta.generation }.wrapping_add(1);
            let updated_at = self.now_millis();

            // Read-modify-write so CRC is computed over the complete
            // post-state. One mmap memcpy for the 320-byte header.
            meta.flags = tf;
            meta.generation = generation;
            meta.updated_at = updated_at;
            meta.delete_at_height = new_dah;
            // SAFETY: `device_ptr` is non-null (fast-path gate) and live for
            // the engine's lifetime; `ro` is allocator-valid. The caller
            // holds this record's stripe lock; `write_metadata_direct` takes
            // the per-offset `io_locks()` write side for torn-read-safe
            // publication.
            unsafe {
                io::write_metadata_direct(self.device_ptr, ro, &meta);
            }

            // Sync index cache from the post-state.
            let dah_or_preserve = if has_preserve { preserve } else { new_dah };
            let mut sync_tf = tf;
            if has_preserve {
                sync_tf.insert(TxFlags::HAS_PRESERVE_UNTIL);
            }
            self.index
                .write()
                .update_cached_fields(
                    &req.tx_key,
                    sync_tf.bits(),
                    meta_block_count,
                    meta_spent,
                    dah_or_preserve,
                    meta_unmined,
                    generation,
                )
                .map_err(|e| SpendError::StorageError {
                    detail: format!("index update_cached_fields failed: {e}"),
                })?;

            // Update DAH secondary index (two-phase durable)
            self.update_dah_index(&req.tx_key, old_dah, new_dah)?;

            SetConflictingResponse { signal, generation }
        } else {
            // Slow path: no direct pointer
            let mut meta = self.read_metadata_fast(ro)?;
            let old_dah = { meta.delete_at_height };

            if req.value {
                meta.flags |= TxFlags::CONFLICTING;
            } else {
                meta.flags -= meta.flags & TxFlags::CONFLICTING;
            }

            meta.generation = { meta.generation }.wrapping_add(1);
            meta.updated_at = self.now_millis();

            let (signal, dah_patch) = evaluate_delete_at_height(
                &meta,
                req.current_block_height,
                req.block_height_retention,
            )?;
            if let Some(ref patch) = dah_patch {
                apply_dah_patch(&mut meta, patch);
            }

            self.write_metadata_fast(ro, &meta)?;
            self.sync_index_cache(&req.tx_key, &meta)?;

            let new_dah = { meta.delete_at_height };
            self.update_dah_index(&req.tx_key, old_dah, new_dah)?;

            SetConflictingResponse {
                signal,
                generation: { meta.generation },
            }
        };

        // Update parent records' conflicting-children lists. The helper writes
        // its own R-221 redo intent before allocating the replacement list
        // block; this call remains best-effort for availability, but failures
        // must be visible.
        // Drop the child lock before taking parent locks.
        if req.value {
            drop(_guard);
            self.append_conflicting_children_from_cold_data(&req.tx_key, "set_conflicting");
        }

        Ok(response)
    }

    /// Set or clear the locked flag on a transaction.
    ///
    /// Returns the post-mutation generation.
    ///
    /// # F-G2-013: rollback hazard for callers that need DAH restore
    ///
    /// Locking clears `delete_at_height`; unlocking does NOT restore the
    /// pre-lock DAH (the engine has no memory of it). Any caller that
    /// might need to compensate a `set_locked(true)` (e.g. on a
    /// replication failure) MUST go through
    /// [`Self::set_locked_with_before_image`] to capture
    /// `prior_delete_at_height` and use `Self::restore_set_locked_for_compensation`
    /// on rollback. Plain `set_locked_idempotent(false)` after a failed
    /// `set_locked_idempotent(true)` silently drops the DAH and the record
    /// becomes unprunable on the next sweep. This `u32` return signature
    /// exists only for callers that have no compensation requirement
    /// (e.g. benchmarks, idempotent replica replay).
    ///
    /// # Foot-gun
    ///
    /// Do NOT use this on master-side replication paths or anywhere a
    /// failure could trigger a rollback. The function is named
    /// `_idempotent` (not the historical `set_locked`) precisely to
    /// trip compile-time review of any new call site — see
    /// May-2026 external review P1 "set_locked plain variant DAH-loss
    /// foot-gun".
    pub fn set_locked_idempotent(&self, req: &SetLockedRequest) -> Result<u32, SpendError> {
        Ok(self.set_locked_with_before_image(req)?.generation)
    }

    /// Backwards-compat alias for [`Self::set_locked_idempotent`].
    ///
    /// Deprecated 2026-05-28: the original `set_locked` name was a
    /// foot-gun — callers reaching for the "obvious" name on a
    /// compensation path would silently drop DAH. New code MUST pick
    /// either [`Self::set_locked_with_before_image`] (compensation-safe)
    /// or [`Self::set_locked_idempotent`] (the explicit idempotent
    /// shorthand). This alias exists only to ease the in-tree call-
    /// site migration; remove it once there are zero remaining
    /// callers.
    #[deprecated(
        since = "0.4.0",
        note = "use `set_locked_idempotent` for replica-replay / benches, or \
                `set_locked_with_before_image` for any path that can trigger \
                a compensation rollback. The unqualified `set_locked` name \
                makes the DAH-compensation requirement invisible at the call \
                site."
    )]
    pub fn set_locked(&self, req: &SetLockedRequest) -> Result<u32, SpendError> {
        self.set_locked_idempotent(req)
    }

    /// Set or clear the locked flag and return the pre-apply lock/DAH state.
    ///
    /// Dispatch uses this for replication-failure compensation. A locked
    /// transition clears `delete_at_height`; blindly applying the inverse
    /// `set_locked(false)` would leave DAH at zero and change pruning
    /// behaviour after rollback.
    pub fn set_locked_with_before_image(
        &self,
        req: &SetLockedRequest,
    ) -> Result<SetLockedResponse, SpendError> {
        let _guard = self.locks.lock(&req.tx_key);
        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        // Fast path: all needed state is in the index cache + 4-byte generation read.
        if !self.device_ptr.is_null() {
            let mut tf = TxFlags::from_bits_truncate(entry.tx_flags);
            let prior_locked = tf.contains(TxFlags::LOCKED);
            let has_preserve = tf.contains(TxFlags::HAS_PRESERVE_UNTIL);
            let old_dah = if has_preserve {
                0
            } else {
                entry.dah_or_preserve
            };

            let new_dah = if req.value {
                tf.insert(TxFlags::LOCKED);
                0 // Locking clears deleteAtHeight
            } else {
                tf.remove(TxFlags::LOCKED);
                old_dah // Unlocking doesn't change DAH
            };

            // Generation is cached in the index — zero device reads.
            let generation = entry.generation.wrapping_add(1);
            let updated_at = self.now_millis();

            // Read-modify-write so CRC is computed over the complete
            // post-state. One mmap memcpy for the 320-byte header.
            //
            // SAFETY: `device_ptr` is non-null (fast-path gate) and live for
            // the engine's lifetime; `ro` is the allocator-valid record
            // offset. `set_locked_with_before_image` holds this record's
            // stripe lock (`self.locks.lock(&req.tx_key)` at the top). Both
            // `read_metadata_direct` and `write_metadata_direct` take the
            // per-offset `io_locks()` read/write side, so the RMW is
            // torn-read-safe against concurrent direct accessors.
            unsafe {
                let mut meta = io::read_metadata_direct(self.device_ptr, ro).map_err(|e| {
                    SpendError::StorageError {
                        detail: format!("{e}"),
                    }
                })?;
                meta.flags = tf;
                meta.generation = generation;
                meta.updated_at = updated_at;
                meta.delete_at_height = new_dah;
                io::write_metadata_direct(self.device_ptr, ro, &meta);
            }

            // Sync index cache
            let dah_or_preserve = if has_preserve {
                entry.dah_or_preserve
            } else {
                new_dah
            };
            let mut sync_tf = tf;
            if has_preserve {
                sync_tf.insert(TxFlags::HAS_PRESERVE_UNTIL);
            }
            self.index
                .write()
                .update_cached_fields(
                    &req.tx_key,
                    sync_tf.bits(),
                    entry.block_entry_count,
                    entry.spent_utxos,
                    dah_or_preserve,
                    entry.unmined_since,
                    generation,
                )
                .map_err(|e| SpendError::StorageError {
                    detail: format!("index update_cached_fields failed: {e}"),
                })?;

            // Update DAH secondary index (two-phase durable)
            self.update_dah_index(&req.tx_key, old_dah, new_dah)?;

            return Ok(SetLockedResponse {
                generation,
                prior_locked,
                prior_delete_at_height: old_dah,
            });
        }

        // Slow path: no direct pointer
        let mut meta = self.read_metadata_fast(ro)?;
        let old_dah = { meta.delete_at_height };
        let prior_locked = meta.flags.contains(TxFlags::LOCKED);

        if req.value {
            meta.flags |= TxFlags::LOCKED;
            if old_dah != 0 {
                meta.delete_at_height = 0;
            }
        } else {
            meta.flags -= meta.flags & TxFlags::LOCKED;
        }

        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = self.now_millis();

        self.write_metadata_fast(ro, &meta)?;
        self.sync_index_cache(&req.tx_key, &meta)?;

        let new_dah = { meta.delete_at_height };
        self.update_dah_index(&req.tx_key, old_dah, new_dah)?;

        Ok(SetLockedResponse {
            generation: { meta.generation },
            prior_locked,
            prior_delete_at_height: old_dah,
        })
    }

    /// Restore the exact pre-`set_locked` lock state and DAH during rollback.
    ///
    /// This is intentionally a rare-path helper: it uses metadata read/write
    /// rather than the mmap fast path so compensation can update flags, primary
    /// cache, and DAH secondary index in one place.
    pub(crate) fn restore_set_locked_for_compensation(
        &self,
        key: &TxKey,
        locked: bool,
        delete_at_height: u32,
    ) -> Result<u32, SpendError> {
        let _guard = self.locks.lock(key);
        let entry = self
            .index
            .read()
            .lookup_checked(key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let mut meta = self.read_metadata_fast(entry.record_offset)?;
        let old_dah = { meta.delete_at_height };

        if locked {
            meta.flags |= TxFlags::LOCKED;
        } else {
            meta.flags -= meta.flags & TxFlags::LOCKED;
        }
        meta.delete_at_height = delete_at_height;
        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = self.now_millis();

        self.write_metadata_fast(entry.record_offset, &meta)?;
        self.sync_index_cache(key, &meta)?;
        self.update_dah_index(key, old_dah, delete_at_height)?;

        Ok(meta.generation)
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
        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let mut meta = self.read_metadata_fast(ro)?;
        let old_dah = { meta.delete_at_height };

        meta.delete_at_height = 0;
        meta.preserve_until = req.block_height;
        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = self.now_millis();

        self.write_metadata_fast(ro, &meta)?;

        // R-019 (A-12): sync the index cache so subsequent fast-path
        // ops (set_mined / set_conflicting / set_locked) see
        // HAS_PRESERVE_UNTIL and skip DAH eviction. Pre-fix the
        // metadata was written but the cached `tx_flags` did not get
        // the discriminant bit; fast paths consulted the cache,
        // concluded `has_preserve = false`, and bypassed the
        // protection — premature pruning of preserved records.
        self.sync_index_cache(&req.tx_key, &meta)?;

        if old_dah != 0 {
            self.update_dah_index(&req.tx_key, old_dah, 0)?;
        }

        let signal = if meta.flags.contains(TxFlags::EXTERNAL) {
            Signal::Preserve
        } else {
            Signal::None
        };
        Ok(PreserveUntilResponse {
            signal,
            generation: { meta.generation },
        })
    }

    /// Whether a record is genuinely due for DAH-sweep deletion at
    /// `current_block_height`, evaluated against fresh metadata.
    ///
    /// This is the delete-side mirror of
    /// [`crate::ops::delete_eval::evaluate_delete_at_height`]'s DAH-*set*
    /// policy, and the authoritative predicate the KO-3 guarded delete
    /// ([`Self::delete`] with `due_guard`), the KO-2 sweep re-validation, and
    /// [`Self::is_due_for_sweep`] all use:
    ///
    /// - `preserve_until == 0` — an active preservation always wins.
    /// - `delete_at_height` set and `<= current_block_height` — DAH is due.
    /// - CONFLICTING records are due unconditionally (KO-2): the
    ///   `setConflicting` path (Lua lines 985-995) DAH's losers regardless
    ///   of spent/longest-chain state, so the sweep MUST be able to delete
    ///   them — a conflicting double-spend loser is never all-spent and is
    ///   usually unmined.
    /// - Otherwise the normal mined-record path: all-spent ∧ on-longest-chain
    ///   (`spent_utxos == utxo_count && unmined_since == 0`).
    fn record_due_for_sweep(meta: &TxMetadata, current_block_height: u32) -> bool {
        if { meta.preserve_until } != 0 {
            return false;
        }
        let dah = { meta.delete_at_height };
        if dah == 0 || dah > current_block_height {
            return false;
        }
        if meta.flags.contains(TxFlags::CONFLICTING) {
            return true;
        }
        // LP-3: a reassigned record is never all-spent (it is retained for the
        // audit trail), so it is never due via the mined-record path. The
        // CONFLICTING branch above still applies.
        if meta.flags.contains(TxFlags::REASSIGNED) {
            return false;
        }
        let all_spent = { meta.spent_utxos } == { meta.utxo_count };
        let on_longest_chain = { meta.unmined_since } == 0;
        all_spent && on_longest_chain
    }

    /// Re-validate a DAH-sweep candidate under the per-tx stripe lock.
    ///
    /// KO-2 + KO-3: the DAH sweep selects candidates from the (cached, lagging)
    /// DAH index, then must confirm each against the authoritative on-device
    /// metadata before scheduling a delete. Taking the stripe lock here means
    /// the decision a sweep records cannot be invalidated by a concurrent
    /// mutation between this check and the redo-log write — only records that
    /// pass get a `Delete` redo op, so a concurrently-preserved record is
    /// never even scheduled (and so never wrongly replayed on recovery). The
    /// final delete still re-checks under the lock via `due_guard` as
    /// defense-in-depth.
    ///
    /// Returns `true` iff [`Self::record_due_for_sweep`] holds for the
    /// freshly-read metadata. A missing / aliased record returns `false`.
    pub fn is_due_for_sweep(&self, key: &TxKey, current_block_height: u32) -> bool {
        let _guard = self.locks.lock(key);
        // G-4 (justified want-absent-on-error): this is an advisory
        // pre-filter for the DAH sweep. Returning `false` means "do not
        // delete this candidate", which is always the safe direction — a
        // present-but-unreadable record is preserved, never deleted, and
        // the final delete re-validates under the stripe lock via
        // `due_guard`. We therefore treat a backend read error the same as
        // a missing record (and the same as the existing metadata-read
        // error below), logging it so the operator still sees the fault.
        let entry = match self.index.read().lookup_checked(key) {
            Ok(Some(e)) => e,
            Ok(None) => return false,
            Err(e) => {
                tracing::error!(
                    target: "teraslab::engine",
                    err = %e,
                    "is_due_for_sweep: index read failed; treating candidate as not-due (record preserved)",
                );
                return false;
            }
        };
        match self.read_metadata_for_key(key, entry.record_offset) {
            Ok(meta) => Self::record_due_for_sweep(&meta, current_block_height),
            Err(_) => false,
        }
    }

    /// Delete a transaction record.
    ///
    /// Removes from index, frees device space, and cleans up secondary indexes.
    ///
    /// # Ordering (F-G2-001)
    ///
    /// The on-device tombstone, primary-index removal, and allocator free
    /// MUST happen in the order:
    ///
    /// 1. Tombstone the metadata header (so any rebuild-from-device can no
    ///    longer parse the record).
    /// 2. `sync()` the device so the tombstone is durable before any future
    ///    overwrite of the same region.
    /// 3. Unregister the key from the primary index.
    /// 4. Return the region to the allocator.
    ///
    /// Steps 3 and 4 are deliberately ordered: a concurrent reader that
    /// holds an offset obtained from the primary index could otherwise see
    /// the region after it has been re-allocated and rewritten by a parallel
    /// `create_at_offset`, and would return an unrelated transaction's
    /// metadata as if it belonged to the deleted key. Unregistering BEFORE
    /// freeing closes the window — any subsequent `lookup(key)` returns
    /// `None`, so no reader can dereference the post-free offset under this
    /// key. Even if the ordering ever regresses, `read_metadata_for_key`
    /// verifies `meta.tx_id == key.txid` and surfaces a mismatch as
    /// `TxNotFound`.
    ///
    /// # External blob reclamation is DEFERRED (IJ-LOW)
    ///
    /// For an `EXTERNAL`-flagged record, this method frees only the on-device
    /// inline record (metadata footer + slots) and removes the index entries.
    /// It does NOT synchronously unlink the external cold-data blob. The blob
    /// is reclaimed asynchronously by the periodic blob-GC sweep (R-049 /
    /// F-G9-004), which unlinks blobs that are no longer referenced by any
    /// index entry and are older than the GC grace window (and not currently
    /// pinned by an in-flight create). Callers MUST NOT assume the blob bytes
    /// are gone the instant `delete` returns; between the delete and the next
    /// sweep the blob remains on the blob store (orphaned but harmless — no
    /// index entry references it). This keeps the delete hot path off the
    /// blob-store I/O path and lets the sweep batch unlinks.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn delete(&self, req: &DeleteRequest) -> Result<(), SpendError> {
        let _guard = self.locks.lock(&req.tx_key);

        // G-4: a backend read error must not collapse to "absent".
        let entry = match self.index.read().lookup_checked(&req.tx_key).map_err(|e| {
            SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            }
        })? {
            Some(e) => e,
            None => return Err(SpendError::TxNotFound),
        };

        // KO-3: when invoked by the DAH sweep (`due_guard == Some(height)`),
        // re-validate the delete predicate against fresh metadata *under this
        // stripe lock* before destroying the record. The sweep's earlier
        // re-validation is lock-free, so a `PreserveUntilBatch` (or any state
        // change clearing the all-spent / longest-chain predicate) that
        // landed in the meantime would otherwise be silently overridden. The
        // recheck reads metadata the unguarded path reads anyway, so the cost
        // is one extra `TxFlags` test. Direct client deletes
        // (`due_guard == None`) skip this and stay unconditional (spec §3.18).
        let record_size = {
            let meta = self.read_metadata_for_key(&req.tx_key, entry.record_offset)?;
            if let Some(current_height) = req.due_guard
                && !Self::record_due_for_sweep(&meta, current_height)
            {
                return Err(SpendError::NotDue);
            }
            ({ meta.record_size }) as u64
        };

        // Step 1: Tombstone the metadata before freeing the region so crash-time
        // index rebuilds cannot resurrect this record from stale bytes in freed
        // space. Zero the full header, not just magic/record_size: freed
        // regions can be reallocated later, and old tx metadata must not
        // remain readable.
        self.write_zeroed_metadata_header(entry.record_offset)?;
        // Step 2: Sync so the tombstone is durable before any reuse.
        self.device.sync().map_err(|e| SpendError::StorageError {
            detail: format!("delete tombstone sync failed: {e}"),
        })?;

        // Step 3: Remove from primary index AND decrement shard_counts in
        // the same critical section so the two can never drift (H2
        // correctness fix). `unregister_with_shard_count` only decrements
        // when an entry was actually removed, preventing underflow if the
        // key was concurrently removed between the earlier `lookup` and
        // this point. This MUST precede the allocator free (F-G2-001):
        // otherwise a concurrent `create_at_offset` could re-allocate the
        // same offset and write a fresh, CRC-valid `TxMetadata` for a
        // different transaction; a lock-free reader holding the offset
        // returned by the still-live primary-index entry would then read
        // that unrelated metadata back as if it belonged to `tx_key`.
        //
        // G-4: if the backend remove fails, propagate it and DO NOT free
        // the region or touch secondaries — otherwise the row would remain
        // in redb pointing at a region we just returned to the allocator.
        self.unregister_with_shard_count(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index unregister failed: {e}"),
            })?;

        // Step 4: Return the region to the allocator. From this point on
        // the offset can be handed out to a future `create`/`create_at_offset`.
        // Because step 3 already removed the primary-index entry, no
        // reader can reach this offset via `lookup(req.tx_key)` any longer.
        self.allocator
            .lock()
            .free(entry.record_offset, record_size)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;

        // Clean up secondary indexes with two-phase durability.
        // The cached entry captured before unregister carries the heights we
        // must transition from. Whether or not each was set, `update_*_index`
        // is a no-op when old == new.
        let has_preserve =
            TxFlags::from_bits_truncate(entry.tx_flags).contains(TxFlags::HAS_PRESERVE_UNTIL);
        let old_dah = if has_preserve {
            0
        } else {
            entry.dah_or_preserve
        };
        let old_unmined = entry.unmined_since;
        if old_dah != 0 {
            self.update_dah_index(&req.tx_key, old_dah, 0)?;
        }
        if old_unmined != 0 {
            self.update_unmined_index(&req.tx_key, old_unmined, 0)?;
        }

        Ok(())
    }

    /// Expire a preservation whose `preserve_until` height has been reached.
    ///
    /// Mirror of the Aerospike pruner's `ProcessExpiredPreservations`
    /// (`teranode/stores/utxo/aerospike/aerospike.go:999-1100`) and spec
    /// §3.18 Phase 3 ("Expired preservation processing"): for a record whose
    /// `preserve_until` is in `[1, current_height]`, schedule deletion by
    /// setting `delete_at_height = current_height + block_height_retention`
    /// and clearing `preserve_until`. The record then becomes a normal DAH
    /// candidate and is deleted `block_height_retention` blocks later by the
    /// sweep.
    ///
    /// This deliberately does NOT re-run `evaluate_delete_at_height`: the Go
    /// pruner sets DAH unconditionally on expiry (the all-spent /
    /// on-longest-chain gating does not apply to an expired preservation —
    /// the preservation window having elapsed is itself the signal that the
    /// record may be reclaimed). The record's spent/unmined/conflicting state
    /// is irrelevant at this point; the parent it protected has had its full
    /// `ParentPreservationBlocks` window.
    ///
    /// Runs under the per-tx stripe lock and re-reads the on-device metadata,
    /// so a `preserve_until` that was pushed forward (a fresh
    /// `PreserveUntilBatch`) between the index scan and this call is honored:
    /// if the re-read `preserve_until` is 0 or still in the future, this is a
    /// no-op and returns `Ok(false)`.
    ///
    /// Returns `Ok(true)` if the preservation was expired and DAH scheduled,
    /// `Ok(false)` if the record no longer qualifies (preserve cleared,
    /// pushed forward, or record gone).
    ///
    /// # Errors
    ///
    /// Returns [`SpendError::DahOverflow`] if `current_block_height +
    /// block_height_retention` overflows `u32`, and [`SpendError::StorageError`]
    /// on device / index failures.
    pub fn expire_preservation_set_dah(
        &self,
        key: &TxKey,
        current_block_height: u32,
        block_height_retention: u32,
    ) -> Result<bool, SpendError> {
        if block_height_retention == 0 {
            return Ok(false);
        }
        let _guard = self.locks.lock(key);
        // G-4: a backend read error must not collapse to "absent".
        let entry = match self.index.read().lookup_checked(key).map_err(|e| {
            SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            }
        })? {
            Some(e) => e,
            None => return Ok(false),
        };
        let ro = entry.record_offset;

        let mut meta = self.read_metadata_for_key(key, ro)?;
        let preserve = { meta.preserve_until };
        // Re-validate under the lock: only expire a preservation that is
        // genuinely set and genuinely due. A `preserve_until` that was
        // cleared or pushed past `current_block_height` since the scan must
        // be left untouched.
        if preserve == 0 || preserve > current_block_height {
            return Ok(false);
        }

        let new_dah = current_block_height
            .checked_add(block_height_retention)
            .ok_or(SpendError::DahOverflow {
                current_height: current_block_height,
                retention: block_height_retention,
            })?;

        // While `preserve_until` is set the record carries no DAH index entry
        // (see `preserve_until`, which removes any prior DAH entry), so there
        // is no stale entry to remove here — the transition is 0 → new_dah.
        meta.preserve_until = 0;
        meta.delete_at_height = new_dah;
        meta.generation = { meta.generation }.wrapping_add(1);
        meta.updated_at = self.now_millis();

        self.write_metadata_fast(ro, &meta)?;
        self.sync_index_cache(key, &meta)?;
        self.update_dah_index(key, 0, new_dah)?;

        Ok(true)
    }

    /// Read spending data for a specific UTXO (point read, no lock needed).
    ///
    /// This is a lock-free path: it does not acquire the per-tx stripe lock.
    /// Reads rely on (a) the CRC32 check on metadata (`io.rs:206`) for torn
    /// headers, and (b) `read_metadata_for_key`'s `tx_id` check (F-G2-001)
    /// to defend against cross-tx aliasing if a concurrent
    /// `delete + create_at_offset` ever reused this offset.
    pub fn get_spend(&self, req: &GetSpendRequest) -> Result<GetSpendResponse, SpendError> {
        let entry = self
            .index
            .read()
            .lookup_checked(&req.tx_key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let ro = entry.record_offset;

        let meta = self.read_metadata_for_key(&req.tx_key, ro)?;
        if req.offset >= { meta.utxo_count } {
            return Err(SpendError::UtxoNotFound { offset: req.offset });
        }

        let slot = self.read_slot_fast(ro, req.offset)?;
        if slot.hash != req.utxo_hash {
            return Err(SpendError::UtxoHashMismatch { offset: req.offset });
        }

        // F-G2-001 (re-verify): re-read metadata after the slot read so a
        // `delete + create_at_offset` race that landed between the
        // earlier `read_metadata_for_key` and this `read_slot_fast`
        // surfaces as `TxNotFound` instead of returning a different
        // transaction's slot data under `key`. The `slot.hash` check
        // above catches the *type* of aliasing where utxo_hash differs;
        // this catches the case where another transaction happens to
        // share a slot hash by accident (small but not zero given
        // 32-byte preimage-controllable hashes from upstream).
        let _meta_check = self.read_metadata_for_key(&req.tx_key, ro)?;

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
    pub fn unmined_index(&self) -> parking_lot::MutexGuard<'_, UnminedBackend> {
        self.unmined_index.lock()
    }

    /// Read on-device metadata for a transaction.
    ///
    /// This is used by production read/diagnostic paths as well as tests.
    /// The method takes the primary-index read lock only long enough to get
    /// the record offset, then performs a device read without taking the
    /// transaction's stripe lock. Callers that need a mutation-stable view
    /// must already hold the appropriate stripe lock or must tolerate a
    /// point-in-time diagnostic snapshot.
    ///
    /// Lock-free torn-write protection comes from the CRC32 on `TxMetadata`
    /// (see `io::read_metadata_direct`'s safety doc). F-G2-001 adds a
    /// second-line defense: the read goes through `read_metadata_for_key`,
    /// which compares `meta.tx_id` against `key.txid` and surfaces a
    /// mismatch as `TxNotFound` so a `delete + create_at_offset` race can
    /// never deliver an unrelated transaction's metadata.
    pub fn read_metadata(&self, key: &TxKey) -> Result<TxMetadata, SpendError> {
        let entry = self
            .index
            .read()
            .lookup_checked(key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        self.read_metadata_for_key(key, entry.record_offset)
    }

    /// Look up a transaction's cached index fields without reading device memory.
    ///
    /// Returns the `TxIndexEntry` directly from the primary index. Fields like
    /// `tx_flags`, `spent_utxos`, `utxo_count`, `block_entry_count`,
    /// `dah_or_preserve`, and `unmined_since` are cached and updated on every
    /// mutation via `sync_index_cache`.
    ///
    /// Use this for GET requests where the field mask only covers cached fields
    /// (see [`crate::protocol::codec::FieldMask::fully_cached`]).
    ///
    /// Propagates backend read errors (G-4): a transient redb failure
    /// surfaces as an [`IndexError`] rather than collapsing to `None`,
    /// which on a client GET path would falsely report the transaction as
    /// absent.
    pub fn lookup_cached_checked(
        &self,
        key: &TxKey,
    ) -> Result<Option<TxIndexEntry>, crate::index::IndexError> {
        self.index.read().lookup_checked(key)
    }

    /// Infallible convenience variant of [`Self::lookup_cached_checked`].
    ///
    /// G-4: collapses a backend read error into `None` after logging it.
    /// For tests / internal diagnostics only; client-visible read paths
    /// MUST use [`Self::lookup_cached_checked`].
    pub fn lookup_cached(&self, key: &TxKey) -> Option<TxIndexEntry> {
        match self.index.read().lookup_checked(key) {
            Ok(found) => found,
            Err(e) => {
                tracing::error!(
                    target: "teraslab::engine",
                    err = %e,
                    "Engine::lookup_cached: index read failed; returning None (caller should use lookup_cached_checked)",
                );
                None
            }
        }
    }

    /// Read a single on-device UTXO slot.
    ///
    /// This is used by production GET/debug paths and tests. Like
    /// [`Self::read_metadata`], it resolves the record offset under the
    /// primary-index read lock and then reads the slot without holding the
    /// transaction's stripe lock. Mutation handlers should not use this as a
    /// validate-then-write primitive unless they already hold that stripe.
    ///
    /// F-G2-001: a metadata read is performed first via
    /// `read_metadata_for_key` to verify the record at `record_offset` still
    /// belongs to `key.txid` — closing the `delete + create_at_offset`
    /// aliasing race for lock-free readers.
    pub fn read_slot(&self, key: &TxKey, offset: u32) -> Result<UtxoSlot, SpendError> {
        let entry = self
            .index
            .read()
            .lookup_checked(key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        // Verify the offset still belongs to this key before reading the slot
        // (F-G2-001 second-line defense; subsumes F-G2-010 doc concern).
        let _meta = self.read_metadata_for_key(key, entry.record_offset)?;
        let slot = self.read_slot_fast(entry.record_offset, offset)?;
        // F-G2-001 (re-verify): a `delete + create_at_offset` race that
        // landed between the metadata check and the slot read can give
        // us another transaction's slot. Re-confirm tx_id ownership of
        // the offset; a mismatch surfaces as `TxNotFound`.
        let _meta_check = self.read_metadata_for_key(key, entry.record_offset)?;
        Ok(slot)
    }

    /// Read every UTXO slot for a transaction.
    ///
    /// This resolves the primary index once, reads metadata once to get the
    /// authoritative slot count, then performs one aligned slot-region read.
    /// The metadata read goes through `read_metadata_for_key` so a stale
    /// offset (post-delete + reuse) is surfaced as `TxNotFound` instead of
    /// returning slots from an unrelated transaction (F-G2-001).
    pub fn read_slots(&self, key: &TxKey) -> Result<Vec<UtxoSlot>, SpendError> {
        let entry = self
            .index
            .read()
            .lookup_checked(key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let meta = self.read_metadata_for_key(key, entry.record_offset)?;
        let slots = io::read_all_utxo_slots(&*self.device, entry.record_offset, meta.utxo_count)
            .map_err(|e| SpendError::StorageError {
                detail: format!("{e}"),
            })?;
        // F-G2-001 (re-verify): the slot read above is not held under the
        // stripe lock. A concurrent `delete + create_at_offset` between
        // the metadata check and the slot read can reuse this offset for
        // a different transaction. Re-read metadata and confirm the
        // record at `record_offset` still belongs to `key`. A mismatch
        // surfaces as `TxNotFound` — same outcome the caller would have
        // seen had they observed the post-unregister state.
        let _meta_check = self.read_metadata_for_key(key, entry.record_offset)?;
        Ok(slots)
    }

    /// Read one mined-block entry, including entries stored in overflow.
    ///
    /// This is used by dispatch before-image capture. Like [`Self::read_metadata`],
    /// it is a diagnostic snapshot unless the caller already holds the
    /// transaction's mutation stripe. The metadata fetch verifies
    /// `meta.tx_id == key.txid` (F-G2-001).
    pub fn read_block_entry(
        &self,
        key: &TxKey,
        block_id: u32,
    ) -> Result<Option<BlockEntry>, SpendError> {
        let entry = self
            .index
            .read()
            .lookup_checked(key)
            .map_err(|e| SpendError::StorageError {
                detail: format!("index lookup failed: {e}"),
            })?
            .ok_or(SpendError::TxNotFound)?;
        let metadata = self.read_metadata_for_key(key, entry.record_offset)?;
        let count = metadata.block_entry_count as usize;
        let inline = count.min(INLINE_BLOCK_ENTRIES);
        for i in 0..inline {
            if { metadata.block_entries_inline[i].block_id } == block_id {
                return Ok(Some(metadata.block_entries_inline[i]));
            }
        }
        if count <= INLINE_BLOCK_ENTRIES {
            return Ok(None);
        }
        let overflow = read_overflow_entries(&*self.device, &metadata).map_err(|e| {
            SpendError::StorageError {
                detail: format!("{e}"),
            }
        })?;
        // F-G2-001 (re-verify): the overflow read is unlocked; a delete +
        // create_at_offset race between the metadata read and the
        // overflow read can give us entries from an unrelated record.
        // Re-check tx_id ownership.
        let _meta_check = self.read_metadata_for_key(key, entry.record_offset)?;
        Ok(overflow
            .into_iter()
            .find(|entry| entry.block_id == block_id))
    }

    /// Get the DAH index (for testing).
    pub fn dah_index(&self) -> parking_lot::MutexGuard<'_, DahBackend> {
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

    /// Test-only: arm a synthetic failure in the next primary-index read so
    /// the next `lookup_checked` on the (redb) backend returns an
    /// [`crate::index::IndexError`]. Used by the G-4 tests to confirm a
    /// transient backend read error surfaces as a storage error instead of
    /// collapsing to "transaction not found".
    #[cfg(test)]
    pub fn arm_fail_next_index_read(&self) {
        self.index.read().arm_fail_next_read();
    }

    /// Access the underlying block device.
    ///
    /// Used by the replication receiver for low-level slot operations
    /// (e.g. prune) that bypass the normal engine API.
    pub fn device(&self) -> &dyn BlockDevice {
        &*self.device
    }

    /// Snapshot the primary index and both secondary indexes to a file.
    ///
    /// Acquires read locks on the primary index and short-lived locks on the
    /// secondary indexes, then writes a consistent snapshot to `path` via an
    /// atomic rename. Called during graceful shutdown so the next startup can
    /// restore from snapshot instead of scanning the device.
    ///
    /// # Errors
    ///
    /// Returns [`crate::index::IndexError`] on I/O failure or if the snapshot
    /// directory is not writable.
    pub fn snapshot_index(&self, path: &std::path::Path) -> crate::index::Result<()> {
        let index = self.index.read();
        let dah = self.dah_index.lock();
        let unmined = self.unmined_index.lock();
        index.snapshot_all(&dah, &unmined, path)
    }

    /// Persist the allocator's freelist and high-water mark to the device header.
    ///
    /// Called during graceful shutdown to avoid a full device scan on the next
    /// startup. Acquires the allocator mutex briefly to serialize the freelist.
    ///
    /// # Errors
    ///
    /// Returns [`crate::allocator::AllocatorError`] on device I/O failure.
    pub fn persist_allocator(&self) -> crate::allocator::Result<()> {
        self.allocator.lock().persist()
    }

    /// Force the primary, DAH, and unmined index backends durable on
    /// their own storage (G-1 audit fix).
    ///
    /// On-disk (redb) backends commit with `Durability::Eventual` per op,
    /// relying on the redo log for crash recovery — that is only sound
    /// while the covering redo entries remain replayable. The checkpoint
    /// task calls this BEFORE writing its recovery-progress fence and
    /// compacting the redo prefix; a failure here MUST abort the
    /// checkpoint (no fence, no compaction). In-memory backends are
    /// no-ops (their durability comes from the snapshot file).
    ///
    /// # Errors
    ///
    /// Returns [`crate::index::IndexError`] if any backend's durability
    /// flush fails; the caller must treat all index state as NOT durable.
    pub fn flush_index_durable(&self) -> crate::index::Result<()> {
        self.index.read().flush_durable()?;
        self.dah_index.lock().flush_durable()?;
        self.unmined_index.lock().flush_durable()
    }
}

impl<'a> ValidatedSpend<'a> {
    /// Apply a previously validated spend batch.
    ///
    /// Consumes `self` by value — the contained per-transaction lock guard
    /// is moved into this call and released only after the mutation has
    /// been written to the device. Because `self` is moved, the compiler
    /// rejects any attempt to call `apply` twice or to reuse the
    /// `ValidatedSpend` after applying. If the caller instead drops the
    /// `ValidatedSpend` without calling `apply`, the lock is released and
    /// no writes occur — the desired failure mode.
    ///
    /// Writes UTXO slot mutations and metadata to the device, updates
    /// secondary indexes, and returns the response.
    ///
    /// This is the second half of the WAL-first pattern:
    /// `validate_spend_multi → write redo → ValidatedSpend::apply`.
    ///
    /// # Errors
    ///
    /// Returns [`SpendError::DahOverflow`] when the configured
    /// `block_height_retention` combined with `current_block_height` would
    /// overflow `u32`. Config validation bounds `block_height_retention`
    /// well below the overflow threshold, so this only fires on
    /// misconfiguration. On error, slot mutations have already been written
    /// (WAL-first pattern), but the metadata footer update is skipped and
    /// the per-transaction lock is released on return. The operator must
    /// correct the config; the redo log will re-drive recovery.
    #[must_use = "apply returns the operation response including per-item errors"]
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn apply(self, engine: &Engine) -> Result<SpendMultiResponse, SpendError> {
        let ValidatedSpend {
            _guard,
            tx_key,
            valid_spends,
            errors,
            spent_count,
            idempotent_count: _,
            pre_generation: _,
            block_ids,
            record_offset,
            mut metadata,
            current_block_height,
            block_height_retention,
        } = self;

        // Fault-injection: simulate a crash AFTER redo fsync but BEFORE
        // any data-region pwrite. Recovery must replay the redo entries
        // and produce the final slot bytes.
        crate::fault_injection::check(crate::fault_injection::SyncPoint::BeforeDataPwrite);

        if spent_count == 0 {
            let generation = { metadata.generation };
            drop(_guard);
            return Ok(SpendMultiResponse {
                signal: Signal::None,
                block_ids,
                errors,
                spent_count,
                generation,
            });
        }

        // 6. Batch write all valid slot mutations (zero-alloc when direct).
        // R-004: stop on first write failure and propagate it. Continuing
        // through the batch and pretending success on partial-write would
        // leave `metadata.spent_utxos` (incremented unconditionally below)
        // disagreeing with the actual on-disk slot states — invariants
        // covering "spent_utxos == count(slots in SPENT state)" would
        // break, premature pruning would follow, and a follow-up spend on
        // the same UTXO with different spending_data would succeed.
        for &(offset, ref new_slot) in &valid_spends {
            engine.write_slot_fast(record_offset, offset, new_slot)?;
        }

        crate::fault_injection::check(crate::fault_injection::SyncPoint::AfterDataPwrite);

        // 7. Update metadata. F-G2-007: switch the spent counter to a
        // checked add and verify it stays within `utxo_count`. Pre-fix
        // the `wrapping_add` would silently mask a violation of the
        // invariant `spent_utxos <= utxo_count` (e.g. if on-device
        // metadata was somehow corrupted with a high `spent_utxos`
        // value but still passed CRC). The wrap then misled
        // `delete_eval`'s `all_spent` check and could prematurely
        // declare the record DAH-eligible. Today no reachable path
        // produces that input — duplicate offsets are absorbed into
        // the already-spent branches in `validate_spend_multi` — but
        // surfacing the invariant violation loudly is cheap insurance.
        let old_dah = { metadata.delete_at_height };
        let pre_spent = { metadata.spent_utxos };
        let new_spent = pre_spent.checked_add(spent_count).ok_or_else(|| {
            SpendError::StorageError {
                detail: format!("spent_utxos overflow: {pre_spent} + {spent_count} > u32::MAX",),
            }
        })?;
        if new_spent > { metadata.utxo_count } {
            return Err(SpendError::StorageError {
                detail: format!(
                    "spent_utxos invariant violated: {new_spent} > utxo_count={}",
                    { metadata.utxo_count },
                ),
            });
        }
        metadata.spent_utxos = new_spent;
        metadata.generation = { metadata.generation }.wrapping_add(1);
        metadata.updated_at = engine.now_millis();

        // 8. Evaluate deleteAtHeight. A DAH-overflow error here indicates
        // misconfiguration (current_height + retention > u32::MAX) and
        // surfaces to the caller as SpendError::DahOverflow — we never
        // silently clamp, which would pin UTXOs as unprunable forever.
        let (signal, dah_patch) =
            evaluate_delete_at_height(&metadata, current_block_height, block_height_retention)?;

        if let Some(ref patch) = dah_patch {
            apply_dah_patch(&mut metadata, patch);
        }

        // 9. Write metadata (targeted spend footer when direct, full otherwise).
        // R-004: propagate the write error.
        if !engine.device_ptr.is_null() {
            // SAFETY: `engine.device_ptr` is non-null (checked above) and
            // live for the engine's lifetime; `record_offset` is
            // allocator-valid. This spend `apply` still holds the record's
            // stripe lock (`_guard`, captured at prepare time);
            // `write_metadata_direct` takes the per-offset `io_locks()` write
            // side for torn-read-safe publication.
            unsafe { io::write_metadata_direct(engine.device_ptr, record_offset, &metadata) };
        } else {
            engine.write_metadata_fast(record_offset, &metadata)?;
        }

        engine.sync_index_cache(&tx_key, &metadata)?;

        // 10. Update DAH secondary index (two-phase durable)
        let new_dah = { metadata.delete_at_height };
        engine.update_dah_index(&tx_key, old_dah, new_dah)?;

        // _guard dropped here, releasing the per-transaction stripe lock.
        drop(_guard);

        // Reuse block_ids from validation — block entries don't change
        // during spend (only spent_utxos, generation, updated_at, DAH).
        Ok(SpendMultiResponse {
            signal,
            block_ids,
            errors,
            spent_count,
            generation: { metadata.generation },
        })
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
fn extract_parent_txids_from_cold_data(cold_bytes: &[u8]) -> Result<Vec<[u8; 32]>, &'static str> {
    if cold_bytes.is_empty() {
        return Ok(Vec::new());
    }
    if cold_bytes.len() < 4 {
        return Err("cold data missing inputs length");
    }

    // Outer wrapper: [inputs_blob_len:4][inputs_blob][...]
    let mut u32_bytes = [0u8; 4];
    u32_bytes.copy_from_slice(&cold_bytes[0..4]);
    let inputs_blob_len = u32::from_le_bytes(u32_bytes) as usize;
    if inputs_blob_len == 0 {
        return Ok(Vec::new());
    }
    let inputs_end = 4usize
        .checked_add(inputs_blob_len)
        .ok_or("inputs blob length overflow")?;
    if inputs_end > cold_bytes.len() {
        return Err("inputs blob length exceeds cold data");
    }
    let inputs_blob = &cold_bytes[4..inputs_end];

    // Inner format: [count:4][per-input: [len:4][extended-bytes]]
    if inputs_blob.len() < 4 {
        return Err("inputs blob missing count");
    }
    u32_bytes.copy_from_slice(&inputs_blob[0..4]);
    let count = u32::from_le_bytes(u32_bytes) as usize;
    let mut pos = 4usize;
    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for _ in 0..count {
        if pos + 4 > inputs_blob.len() {
            return Err("input entry length truncated");
        }
        u32_bytes.copy_from_slice(&inputs_blob[pos..pos + 4]);
        let entry_len = u32::from_le_bytes(u32_bytes) as usize;
        pos += 4;
        if entry_len < 32 {
            return Err("input entry shorter than parent txid");
        }
        let entry_end = pos
            .checked_add(entry_len)
            .ok_or("input entry length overflow")?;
        if entry_end > inputs_blob.len() {
            return Err("input entry data truncated");
        }
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&inputs_blob[pos..pos + 32]);
        if seen.insert(txid) {
            result.push(txid);
        }
        pos = entry_end;
    }
    Ok(result)
}

/// Build inline cold data from optional inputs/outputs/inpoints.
///
/// Format: `[inputs_len:4 LE][inputs][outputs_len:4 LE][outputs][inpoints_len:4 LE][inpoints]`
/// Build the on-disk cold data blob from optional input/output/inpoint fields.
///
/// Public so the dispatch layer can compute record sizes for pre-allocation.
pub fn build_cold_data(
    inputs: Option<&[u8]>,
    outputs: Option<&[u8]>,
    inpoints: Option<&[u8]>,
) -> Vec<u8> {
    let inputs_data = inputs.unwrap_or(&[]);
    let outputs_data = outputs.unwrap_or(&[]);
    let inpoints_data = inpoints.unwrap_or(&[]);

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

/// Inline block IDs stored on the stack (max `INLINE_BLOCK_ENTRIES`).
struct InlineBlockIds {
    ids: [u32; INLINE_BLOCK_ENTRIES],
    len: u8,
}

impl InlineBlockIds {
    /// Convert to a `Vec<u32>` for use in response types.
    fn to_vec(&self) -> Vec<u32> {
        self.ids[..self.len as usize].to_vec()
    }
}

fn collect_block_ids(metadata: &TxMetadata) -> InlineBlockIds {
    let count = metadata.block_entry_count as usize;
    let inline = count.min(INLINE_BLOCK_ENTRIES);
    let mut ids = [0u32; INLINE_BLOCK_ENTRIES];
    for (id_slot, entry) in ids
        .iter_mut()
        .zip(metadata.block_entries_inline[..inline].iter())
    {
        *id_slot = entry.block_id;
    }
    InlineBlockIds {
        ids,
        len: inline as u8,
    }
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
        .map(|e| e.block_id)
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
    device.pread_exact_at(&mut buf, overflow_offset)?;

    let mut entries = Vec::with_capacity(overflow_count);
    for i in 0..overflow_count {
        let start = i * BLOCK_ENTRY_SIZE;
        entries.push(BlockEntry::from_bytes(
            &buf[start..start + BLOCK_ENTRY_SIZE],
        ));
    }
    Ok(entries)
}

/// Compute the on-device byte size of the overflow block that backs the
/// current `metadata.block_overflow_offset`.
///
/// Pre-fix (F-G2-003) the free path always freed exactly `alignment`
/// bytes — correct for the 4 KiB device alignment in production but a
/// silent leak on a 512-byte-aligned device (`align_up(252 * 12, 512) =
/// 3072` allocated, only 512 freed). The new helper rederives the
/// previously-allocated size from `block_entry_count`: overflow holds
/// the count past the inline cap, rounded up to the device's alignment.
/// Callers must invoke this BEFORE mutating `block_entry_count` so the
/// returned size matches the live allocation.
#[inline]
fn overflow_block_size(metadata: &TxMetadata, alignment: usize) -> usize {
    let total = metadata.block_entry_count as usize;
    if total <= INLINE_BLOCK_ENTRIES {
        return 0;
    }
    let overflow_count = total - INLINE_BLOCK_ENTRIES;
    io::align_up(overflow_count * BLOCK_ENTRY_SIZE, alignment)
}

/// Write overflow block entries to the device.
///
/// Allocates, reuses, or frees the overflow block.
///
/// # F-G2-003: exact-size free + grow-aware reuse
///
/// The free path now passes the actual allocated size (rederived from
/// `metadata.block_entry_count`) to `allocator.free`. The grow path
/// detects when `new_size > old_size` and reallocates rather than writing
/// past the existing allocation. The allocator free error is propagated
/// instead of being silently swallowed via `let _ = ...`.
fn write_overflow_entries(
    device: &dyn BlockDevice,
    allocator: &parking_lot::Mutex<SlotAllocator>,
    metadata: &mut TxMetadata,
    entries: &[BlockEntry],
) -> Result<(), crate::device::DeviceError> {
    let alignment = device.alignment();
    let old_offset = { metadata.block_overflow_offset };
    let old_block_size = overflow_block_size(metadata, alignment);

    if entries.is_empty() {
        // Free the overflow block if one exists. F-G2-003: free the
        // *full* allocated size, not just one alignment unit, and
        // propagate the error instead of swallowing it.
        if old_offset != 0 {
            let free_size = if old_block_size > 0 {
                old_block_size as u64
            } else {
                // Defensive: if `block_entry_count` already reflected
                // the post-shrink state (count <= INLINE) but the
                // overflow pointer is still live, fall back to one
                // alignment unit to avoid double-free of unallocated
                // space. This matches the legacy behaviour for the case
                // it was correct for.
                alignment as u64
            };
            allocator.lock().free(old_offset, free_size).map_err(|e| {
                crate::device::DeviceError::Io(std::io::Error::other(format!("allocator: {e}")))
            })?;
            metadata.block_overflow_offset = 0;
        }
        return Ok(());
    }

    let data_size = entries.len() * BLOCK_ENTRY_SIZE;
    let new_block_size = io::align_up(data_size, alignment);

    // Decide allocate / reuse / reallocate.
    // - No prior block: fresh allocation.
    // - Same alignment-rounded size as prior: reuse in place (writes are
    //   overwrites, no allocator churn).
    // - Different size (grow OR shrink across alignment boundary): free
    //   the old allocation and grab a fresh one. Shrinking-but-reusing
    //   would leak the trailing alignment unit(s) on the next free
    //   (which only sees the new, smaller size). The allocator free
    //   error is propagated; pre-fix it was swallowed via `let _ =`.
    let offset = if old_offset == 0 {
        allocator
            .lock()
            .allocate(new_block_size as u64)
            .map_err(|e| {
                crate::device::DeviceError::Io(std::io::Error::other(format!("allocator: {e}")))
            })?
    } else if new_block_size == old_block_size {
        old_offset
    } else {
        let mut a = allocator.lock();
        a.free(old_offset, old_block_size as u64).map_err(|e| {
            crate::device::DeviceError::Io(std::io::Error::other(format!("allocator: {e}")))
        })?;
        a.allocate(new_block_size as u64).map_err(|e| {
            crate::device::DeviceError::Io(std::io::Error::other(format!("allocator: {e}")))
        })?
    };

    let mut buf = AlignedBuf::new(new_block_size, alignment);
    for (i, entry) in entries.iter().enumerate() {
        let start = i * BLOCK_ENTRY_SIZE;
        entry.to_bytes(&mut buf[start..start + BLOCK_ENTRY_SIZE]);
    }
    device.pwrite_all_at(&buf, offset)?;
    metadata.block_overflow_offset = offset;
    Ok(())
}

/// Get the current wall-clock time in milliseconds since Unix epoch.
///
/// Used by [`Engine::refresh_clock`] and test code. Production engine
/// code reads the cached value via [`Engine::now_millis`] instead.
fn sys_millis() -> u64 {
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
    use crate::device::{DeviceError, MemoryDevice};
    use crate::index::{DahIndex, Index, UnminedIndex};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    /// Wrap a device and reject pwrites once a kill-switch flag is set.
    /// Used by R-004 regression tests that prove `Engine::spend` and
    /// `ValidatedSpend::apply` propagate slot/metadata write errors
    /// instead of silently returning `Ok` with a torn on-disk state.
    struct WriteFailingDevice {
        inner: Arc<dyn BlockDevice>,
        fail: Arc<AtomicBool>,
    }

    impl WriteFailingDevice {
        fn new(inner: Arc<dyn BlockDevice>) -> (Arc<Self>, Arc<AtomicBool>) {
            let fail = Arc::new(AtomicBool::new(false));
            (
                Arc::new(Self {
                    inner,
                    fail: fail.clone(),
                }),
                fail,
            )
        }
    }

    impl BlockDevice for WriteFailingDevice {
        fn alignment(&self) -> usize {
            self.inner.alignment()
        }
        fn size(&self) -> u64 {
            self.inner.size()
        }
        fn pread(&self, buf: &mut [u8], offset: u64) -> crate::device::Result<usize> {
            self.inner.pread(buf, offset)
        }
        fn pwrite(&self, buf: &[u8], offset: u64) -> crate::device::Result<usize> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(DeviceError::Io(std::io::Error::other(
                    "simulated pwrite failure (R-004)",
                )));
            }
            self.inner.pwrite(buf, offset)
        }
        fn sync(&self) -> crate::device::Result<()> {
            self.inner.sync()
        }
        fn as_raw_ptr(&self) -> Option<*mut u8> {
            // R-004 tests must hit the pwrite path, not the direct mmap
            // shortcut, so always report no raw pointer.
            None
        }
    }

    struct SyncCountingDevice {
        inner: Arc<dyn BlockDevice>,
        syncs: Arc<AtomicU64>,
    }

    impl SyncCountingDevice {
        fn new(inner: Arc<dyn BlockDevice>) -> (Arc<Self>, Arc<AtomicU64>) {
            let syncs = Arc::new(AtomicU64::new(0));
            (
                Arc::new(Self {
                    inner,
                    syncs: syncs.clone(),
                }),
                syncs,
            )
        }
    }

    impl BlockDevice for SyncCountingDevice {
        fn alignment(&self) -> usize {
            self.inner.alignment()
        }
        fn size(&self) -> u64 {
            self.inner.size()
        }
        fn pread(&self, buf: &mut [u8], offset: u64) -> crate::device::Result<usize> {
            self.inner.pread(buf, offset)
        }
        fn pwrite(&self, buf: &[u8], offset: u64) -> crate::device::Result<usize> {
            self.inner.pwrite(buf, offset)
        }
        fn sync(&self) -> crate::device::Result<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            self.inner.sync()
        }
        fn as_raw_ptr(&self) -> Option<*mut u8> {
            None
        }
    }

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
            let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
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

            let preserve = { meta.preserve_until };
            let dah = { meta.delete_at_height };
            let has_preserve = preserve != 0;
            let mut ie_flags = meta.flags.bits();
            if has_preserve {
                ie_flags |= TxFlags::HAS_PRESERVE_UNTIL.bits();
            }
            let ie = TxIndexEntry {
                device_id: 0,
                record_offset: offset,
                utxo_count,
                block_entry_count: meta.block_entry_count,
                tx_flags: ie_flags,
                spent_utxos: { meta.spent_utxos },
                dah_or_preserve: if has_preserve { preserve } else { dah },
                unmined_since: { meta.unmined_since },
                generation: 0,
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

    /// Build an engine whose underlying device fails pwrites once a
    /// kill-switch flag is set. Used by the R-004 regression tests.
    /// The flag is off when the seed record is written; tests flip it
    /// before issuing the mutation under test.
    fn make_engine_with_failable_device(utxo_count: u32) -> (Arc<Engine>, TxKey, Arc<AtomicBool>) {
        let inner: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let (failing, fail) = WriteFailingDevice::new(inner);
        let dev: Arc<dyn BlockDevice> = failing;
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
        let mut index = Index::new(100).unwrap();

        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&7u64.to_le_bytes());
        let key = TxKey { txid };

        let record_size = TxMetadata::record_size_for(utxo_count);
        let offset = alloc.allocate(record_size).unwrap();

        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = txid;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| {
                let mut hash = [0u8; 32];
                hash[0] = (i & 0xFF) as u8;
                hash[1] = ((i >> 8) & 0xFF) as u8;
                UtxoSlot::new_unspent(hash)
            })
            .collect();
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

        let ie_flags = meta.flags.bits();
        let ie = TxIndexEntry {
            device_id: 0,
            record_offset: offset,
            utxo_count,
            block_entry_count: meta.block_entry_count,
            tx_flags: ie_flags,
            spent_utxos: { meta.spent_utxos },
            dah_or_preserve: { meta.delete_at_height },
            unmined_since: { meta.unmined_since },
            generation: 0,
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
        (engine, key, fail)
    }

    /// R-004: a single-slot `Engine::spend` whose on-disk slot write
    /// fails MUST return `Err(SpendError::StorageError)`. Pre-fix this
    /// returned `Ok` and left the slot UNSPENT on disk while the
    /// metadata's `spent_utxos` was incremented — a follow-up spend
    /// with different `spending_data` would then succeed (double-spend).
    #[test]
    fn spend_propagates_slot_write_failure() {
        let (engine, key, fail) = make_engine_with_failable_device(4);
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
                sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                sd
            },
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        // Arm the failure ON the device.
        fail.store(true, Ordering::SeqCst);

        let result = engine.spend(&req);
        assert!(
            matches!(result, Err(SpendError::StorageError { .. })),
            "spend must propagate slot write failures, got {result:?}"
        );

        // Disarm and verify on-disk state is consistent: slot is still
        // UNSPENT (the write failed) and metadata.spent_utxos is still 0
        // (because the failure short-circuited before the counter bump).
        fail.store(false, Ordering::SeqCst);
        let slot = engine.read_slot(&key, 0).unwrap();
        assert!(
            !slot.is_spent(),
            "after a failed spend the slot must remain UNSPENT on disk"
        );
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!(
            { meta.spent_utxos },
            0,
            "after a failed spend the counter must not have been bumped"
        );
    }

    /// R-004: companion to `spend_propagates_slot_write_failure`. A
    /// `spend_multi` whose first slot write fails MUST return
    /// `Err(SpendError::StorageError)` rather than continuing through
    /// the batch and returning OK with `metadata.spent_utxos` ahead of
    /// the actual on-disk slot state.
    #[test]
    fn spend_multi_propagates_slot_write_failure() {
        let (engine, key, fail) = make_engine_with_failable_device(4);
        let mut sd = [0u8; 36];
        sd[0] = 0xBB;
        sd[32..36].copy_from_slice(&1u32.to_le_bytes());
        let multi = SpendMultiRequest {
            tx_key: key,
            spends: vec![
                SpendItem {
                    idx: 0,
                    offset: 0,
                    utxo_hash: {
                        let mut h = [0u8; 32];
                        h[0] = 0;
                        h
                    },
                    spending_data: sd,
                },
                SpendItem {
                    idx: 1,
                    offset: 1,
                    utxo_hash: {
                        let mut h = [0u8; 32];
                        h[0] = 1;
                        h
                    },
                    spending_data: sd,
                },
            ],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        fail.store(true, Ordering::SeqCst);
        let validated = engine.validate_spend_multi(&multi).unwrap();
        let result = validated.apply(&engine);
        assert!(
            matches!(result, Err(SpendError::StorageError { .. })),
            "spend_multi must propagate the first slot write failure, got {result:?}"
        );

        fail.store(false, Ordering::SeqCst);
        // Both slots must remain UNSPENT — the partial-write contract
        // is "either all succeed and the counter matches, or none do
        // and the counter matches that."
        let slot0 = engine.read_slot(&key, 0).unwrap();
        let slot1 = engine.read_slot(&key, 1).unwrap();
        assert!(
            !slot0.is_spent(),
            "slot 0 must remain UNSPENT on partial-write failure"
        );
        assert!(
            !slot1.is_spent(),
            "slot 1 must remain UNSPENT on partial-write failure"
        );
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
    fn spend_coinbase_zero_spending_height_boundary() {
        // The storage spec defines the maturity gate as
        // `spending_height > 0 && spending_height > current_block_height`.
        // A zero height therefore means "no maturity height recorded" and
        // must not accidentally behave as immature at genesis/low heights.
        let h = TestHarness::with_metadata(10, TxFlags::IS_COINBASE, |m| {
            m.spending_height = 0;
        });
        let mut req = h.spend_req(0);
        req.current_block_height = 0;
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
            Err(SpendError::Pruned {
                offset: 4,
                spending_data,
            }) => assert_eq!(spending_data, h.make_spending_data(0x11)),
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

    /// Spendable AT stop — half-open interval `[0, spendable_height)`.
    /// At `current_block_height == spendable_height` the UTXO MUST be
    /// spendable. See `reassign_spendable_height_boundary_at_exact_height`
    /// for the matching reassign-side boundary test.
    #[test]
    fn spend_frozen_until_equal_height_is_spendable() {
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();
        let mut slot = UtxoSlot::new_unspent(h.slot_hash(2));
        slot.spending_data[0..4].copy_from_slice(&1000u32.to_le_bytes());
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 2, &slot).unwrap();

        let mut req = h.spend_req(2);
        req.current_block_height = 1000;
        h.engine
            .spend(&req)
            .expect("spend at exact spendable_height must succeed");
    }

    /// One block below `spendable_height` is still frozen.
    #[test]
    fn spend_frozen_until_one_below_height() {
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();
        let mut slot = UtxoSlot::new_unspent(h.slot_hash(2));
        slot.spending_data[0..4].copy_from_slice(&1000u32.to_le_bytes());
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 2, &slot).unwrap();

        let mut req = h.spend_req(2);
        req.current_block_height = 999;
        match h.engine.spend(&req) {
            Err(SpendError::FrozenUntil {
                spendable_at_height: 1000,
                ..
            }) => {}
            other => panic!("expected FrozenUntil(1000), got {other:?}"),
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
        let before = sys_millis();
        h.engine.spend(&h.spend_req(0)).unwrap();
        let after = sys_millis();

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
    fn spend_multi_errors_deterministic_iteration() {
        let h = TestHarness::new(20, TxFlags::empty());
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![
                SpendItem {
                    offset: 9,
                    utxo_hash: [0xFF; 32],
                    spending_data: h.make_spending_data(0x09),
                    idx: 90,
                },
                SpendItem {
                    offset: 1,
                    utxo_hash: [0xEE; 32],
                    spending_data: h.make_spending_data(0x01),
                    idx: 10,
                },
                SpendItem {
                    offset: 5,
                    utxo_hash: [0xDD; 32],
                    spending_data: h.make_spending_data(0x05),
                    idx: 50,
                },
            ],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = h.engine.spend_multi(&req).unwrap();
        let keys: Vec<u32> = resp.errors.keys().copied().collect();
        assert_eq!(
            keys,
            vec![10, 50, 90],
            "spend_multi error iteration order must be stable for response encoding"
        );
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
    fn spend_multi_idempotent_does_not_bump_generation() {
        let h = TestHarness::new(10, TxFlags::empty());
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![SpendItem {
                offset: 3,
                utxo_hash: h.slot_hash(3),
                spending_data: h.make_spending_data(0x33),
                idx: 0,
            }],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        h.engine.spend_multi(&req).unwrap();
        let generation_after_first = { h.engine.read_metadata(&h.key).unwrap().generation };

        let resp = h.engine.spend_multi(&req).unwrap();
        let generation_after_second = { h.engine.read_metadata(&h.key).unwrap().generation };

        assert!(resp.errors.is_empty());
        assert_eq!(resp.spent_count, 0);
        assert_eq!(resp.generation, generation_after_first);
        assert_eq!(generation_after_second, generation_after_first);
    }

    #[test]
    fn spend_idempotent_count_direct_not_subtracted() {
        let h = TestHarness::new(3, TxFlags::empty());
        let spending_data = h.make_spending_data(0x33);
        h.engine
            .spend(&SpendRequest {
                tx_key: h.key,
                offset: 0,
                utxo_hash: h.slot_hash(0),
                spending_data,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![
                SpendItem {
                    offset: 0,
                    utxo_hash: h.slot_hash(0),
                    spending_data,
                    idx: 10,
                },
                SpendItem {
                    offset: 1,
                    utxo_hash: h.slot_hash(1),
                    spending_data: h.make_spending_data(0x44),
                    idx: 20,
                },
            ],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let validated = h.engine.validate_spend_multi(&req).unwrap();
        assert_eq!(validated.idempotent_count(), 1);
        assert_eq!(validated.spent_count, 1);
        assert!(validated.errors.is_empty());
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

    // -- ValidatedSpend type-state tests (C2: spend lock lifetime) --

    /// The WAL-first path: validate, then apply on the returned
    /// [`ValidatedSpend`]. The lock is held across validate → apply, so no
    /// concurrent mutation can interleave. This exercises the consuming
    /// `apply(self, &Engine)` signature end-to-end.
    #[test]
    fn validated_spend_apply_consumes_and_writes() {
        let h = TestHarness::new(10, TxFlags::empty());
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![SpendItem {
                offset: 3,
                utxo_hash: h.slot_hash(3),
                spending_data: h.make_spending_data(0x11),
                idx: 0,
            }],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        // Validate under lock — returns a ValidatedSpend holding the guard.
        let validated = h.engine.validate_spend_multi(&req).unwrap();
        assert_eq!(validated.spent_count, 1);
        let pre_gen = validated.pre_generation;

        // Apply consumes the ValidatedSpend by value. The response carries
        // the post-mutation generation and the per-item errors from
        // validation (empty for this case).
        let resp = validated.apply(&h.engine).unwrap();
        assert!(resp.errors.is_empty());
        assert_eq!(resp.spent_count, 1);
        assert_eq!(resp.generation, pre_gen.wrapping_add(1));

        // The mutation was actually written.
        let slot = h.engine.read_slot(&h.key, 3).unwrap();
        assert!(slot.is_spent());
        assert_eq!(slot.spending_data, h.make_spending_data(0x11));

        // NOTE: attempting `validated.apply(&h.engine)` again here would
        // fail to compile with `use of moved value`. The compile_fail
        // doctests on `ValidatedSpend` assert the Copy/Clone bounds that
        // make this move-based API sound.
    }

    /// Dropping a ValidatedSpend without applying must leave the record
    /// untouched and release the stripe lock so a subsequent operation on
    /// the same txid can proceed.
    #[test]
    fn validated_spend_dropped_without_apply_is_noop_and_releases_lock() {
        let h = TestHarness::new(10, TxFlags::empty());
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![SpendItem {
                offset: 4,
                utxo_hash: h.slot_hash(4),
                spending_data: h.make_spending_data(0x22),
                idx: 0,
            }],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let meta_before = h.engine.read_metadata(&h.key).unwrap();
        let gen_before = { meta_before.generation };
        let spent_before = { meta_before.spent_utxos };

        // Validate and then explicitly drop without applying.
        {
            let validated = h.engine.validate_spend_multi(&req).unwrap();
            // Guard is alive right now — a concurrent validate_spend_multi
            // on the same tx_key would block on the stripe lock until this
            // scope ends. We don't try to demonstrate that here (would
            // deadlock the test), but we *do* demonstrate that after the
            // drop, the lock is released and the next call succeeds.
            drop(validated);
        }

        // No writes: slot still unspent, metadata unchanged.
        let slot = h.engine.read_slot(&h.key, 4).unwrap();
        assert!(
            !slot.is_spent(),
            "slot must not have been mutated when apply was skipped"
        );
        let meta_after = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta_after.generation }, gen_before);
        assert_eq!({ meta_after.spent_utxos }, spent_before);

        // Lock was released — a fresh validate (and apply) on the same tx
        // acquires the same stripe lock cleanly and mutates the record.
        let v2 = h.engine.validate_spend_multi(&req).unwrap();
        let resp = v2.apply(&h.engine).unwrap();
        assert_eq!(resp.spent_count, 1);
        let slot = h.engine.read_slot(&h.key, 4).unwrap();
        assert!(slot.is_spent());
    }

    /// The combined `spend_multi` wrapper threads through the same
    /// validate → apply pipeline via `ValidatedSpend::apply`. It must
    /// produce identical observable behaviour to the split path.
    #[test]
    fn validated_spend_matches_spend_multi_wrapper() {
        let h = TestHarness::new(5, TxFlags::empty());
        let req = SpendMultiRequest {
            tx_key: h.key,
            spends: vec![SpendItem {
                offset: 0,
                utxo_hash: h.slot_hash(0),
                spending_data: h.make_spending_data(0x33),
                idx: 0,
            }],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let direct = h.engine.spend_multi(&req).unwrap();
        // Idempotent re-spend via the split path — same spending_data, so
        // spent_count should be 0 and errors empty.
        let v = h.engine.validate_spend_multi(&req).unwrap();
        let split = v.apply(&h.engine).unwrap();
        assert!(direct.errors.is_empty() && split.errors.is_empty());
        assert_eq!(direct.spent_count, 1);
        assert_eq!(split.spent_count, 0, "idempotent re-spend should not count");
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
            spending_data: h.make_spending_data(0xAB),
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
            spending_data: h.make_spending_data(0xAB),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();

        // Generation should NOT increment for no-op
        let g1 = { h.engine.read_metadata(&h.key).unwrap().generation };
        assert_eq!(g1, g0);
    }

    #[test]
    fn unspend_frozen_slot_is_noop_not_error() {
        // The Lua only emits FROZEN inside the `callerOwnsSpend` branch. A
        // frozen slot's stored spending data is the all-0xFF marker, which a
        // real caller's expected data never equals, so ownership is false and
        // the unspend is a silent no-op (STATUS_OK), leaving the slot frozen.
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();
        let frozen = UtxoSlot::new_frozen(h.slot_hash(3));
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 3, &frozen).unwrap();
        let g0 = { h.engine.read_metadata(&h.key).unwrap().generation };

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 3,
            utxo_hash: h.slot_hash(3),
            spending_data: h.make_spending_data(0xAB),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let resp = h.engine.unspend(&req).expect("frozen unspend must be a no-op OK");
        assert_eq!(resp.signal, Signal::None);

        // Slot stays frozen, generation unchanged.
        let slot = h.engine.read_slot(&h.key, 3).unwrap();
        assert!(slot.is_frozen());
        assert_eq!({ h.engine.read_metadata(&h.key).unwrap().generation }, g0);
    }

    #[test]
    fn unspend_spent_slot_with_frozen_marker_data_is_noop() {
        // Defensive: a SPENT slot whose stored data is somehow the frozen
        // marker AND whose caller's expected data is that same marker would
        // hit the structural FROZEN branch. This is unreachable via legitimate
        // writers (no real spend records all-0xFF) but pins the Lua-mirrored
        // branch so it cannot silently rot.
        let h = TestHarness::new(10, TxFlags::empty());
        let entry = h.engine.lookup(&h.key).unwrap();
        let spent_frozen = UtxoSlot::new_spent(h.slot_hash(3), [FROZEN_BYTE; 36]);
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 3, &spent_frozen).unwrap();

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 3,
            utxo_hash: h.slot_hash(3),
            spending_data: [FROZEN_BYTE; 36],
            current_block_height: 1000,
            block_height_retention: 288,
        };
        // The frozen marker excludes ownership, so this is a no-op OK, not an
        // error — the FROZEN branch is genuinely dead for real callers.
        let resp = h.engine.unspend(&req).expect("frozen-marker unspend is a no-op OK");
        assert_eq!(resp.signal, Signal::None);
        let slot = h.engine.read_slot(&h.key, 3).unwrap();
        assert!(slot.is_spent());
        assert_eq!(slot.spending_data, [FROZEN_BYTE; 36]);
    }

    #[test]
    fn unspend_nonexistent_tx() {
        let h = TestHarness::new(10, TxFlags::empty());
        let req = UnspendRequest {
            tx_key: TxKey { txid: [0xFF; 32] },
            offset: 0,
            utxo_hash: [0; 32],
            spending_data: [0; 36],
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
            spending_data: h.make_spending_data(0xAB),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        match h.engine.unspend(&req) {
            Err(SpendError::UtxoHashMismatch { offset: 5 }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    #[test]
    fn unspend_mismatched_spending_data_is_noop_without_mutating_slot() {
        // Lua contract (`teranode.lua:513-540`): when the caller does NOT own
        // the stored spend (stored 0xAB belongs to a different tx than the
        // caller's 0xCD), unspend is a silent idempotent no-op returning
        // STATUS_OK — "never wipe a spend we don't own", not "error on every
        // no-op". The slot stays spent by the original spender, the counter is
        // unchanged, and the generation is NOT bumped.
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(5)).unwrap();
        let g0 = { h.engine.read_metadata(&h.key).unwrap().generation };

        let wrong_spending_data = h.make_spending_data(0xCD);
        let req = UnspendRequest {
            tx_key: h.key,
            offset: 5,
            utxo_hash: h.slot_hash(5),
            spending_data: wrong_spending_data,
            current_block_height: 1000,
            block_height_retention: 288,
        };

        let resp = h
            .engine
            .unspend(&req)
            .expect("mismatched spending data must be a no-op OK, not an error");
        assert_eq!(resp.signal, Signal::None);
        assert_eq!(resp.generation, g0, "no-op must not bump generation");

        // Slot still spent by the original spender (0xAB); counter unchanged.
        let slot = h.engine.read_slot(&h.key, 5).unwrap();
        assert!(slot.is_spent());
        assert_eq!(slot.spending_data, h.make_spending_data(0xAB));
        assert_eq!({ h.engine.read_metadata(&h.key).unwrap().spent_utxos }, 1);
        assert_eq!({ h.engine.read_metadata(&h.key).unwrap().generation }, g0);
    }

    #[test]
    fn prune_slot_if_spent_by_child_updates_counters_once() {
        let h = TestHarness::new(3, TxFlags::empty());
        h.engine.spend(&h.spend_req(1)).unwrap();
        let mut child_txid = [0u8; 32];
        child_txid.copy_from_slice(&h.make_spending_data(0xAB)[..32]);

        let applied = h
            .engine
            .prune_slot_if_spent_by_child(&h.key, 1, child_txid)
            .unwrap();
        assert!(applied);
        let slot = h.engine.read_slot(&h.key, 1).unwrap();
        assert_eq!(slot.status, UTXO_PRUNED);
        assert_eq!(slot.spending_data, h.make_spending_data(0xAB));
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.spent_utxos }, 0);
        assert_eq!({ meta.pruned_utxos }, 1);

        let applied_again = h
            .engine
            .prune_slot_if_spent_by_child(&h.key, 1, child_txid)
            .unwrap();
        assert!(!applied_again);
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.spent_utxos }, 0);
        assert_eq!({ meta.pruned_utxos }, 1);
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
            spending_data: h.make_spending_data(0xAB),
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
            spending_data: h.make_spending_data(0xAB),
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
            spending_data: h.make_spending_data(0xAB),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();

        // DAH should be cleared
        assert!(h.engine.dah_index().range_query(u32::MAX).is_empty());
    }

    #[test]
    fn unspend_noop_still_runs_dah_housekeeping() {
        // The Lua runs `setDeleteAtHeight` before every OK return, including the
        // no-op path. Here we drive an all-spent record into a state where its
        // DAH has not yet been set, then issue a NON-owning (mismatched) unspend
        // and confirm the no-op path still evaluates and SETS the DAH — proving
        // housekeeping runs even when no slot/counter mutation occurs.
        let h = TestHarness::with_metadata(2, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
            };
        });

        // Spend both UTXOs → all-spent, DAH set to 1000 + 288 at height 1000.
        h.engine.spend(&h.spend_req(0)).unwrap();
        h.engine.spend(&h.spend_req(1)).unwrap();

        // Manually clear the DAH (and its index entry) to model a record that is
        // all-spent but whose DAH housekeeping has not yet fired.
        {
            let entry = h.engine.lookup(&h.key).unwrap();
            let mut meta = h.engine.read_metadata(&h.key).unwrap();
            let old_dah = { meta.delete_at_height };
            meta.delete_at_height = 0;
            io::write_metadata(&*h.engine.device, entry.record_offset, &meta).unwrap();
            h.engine.update_dah_index(&h.key, old_dah, 0).unwrap();
        }
        assert!(h.engine.dah_index().range_query(u32::MAX).is_empty());

        // Non-owning unspend: stored spend on slot 0 is 0xAB, caller passes 0xCD.
        let g0 = { h.engine.read_metadata(&h.key).unwrap().generation };
        let req = UnspendRequest {
            tx_key: h.key,
            offset: 0,
            utxo_hash: h.slot_hash(0),
            spending_data: h.make_spending_data(0xCD),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();

        // Slot/counter untouched (still all-spent), but housekeeping re-set DAH.
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert!(h.engine.read_slot(&h.key, 0).unwrap().is_spent());
        assert_eq!({ meta.spent_utxos }, 2, "counter unchanged by no-op");
        assert_eq!({ meta.delete_at_height }, 1288, "no-op ran DAH housekeeping");
        assert!(!h.engine.dah_index().range_query(u32::MAX).is_empty());
        // The no-op itself did not bump the generation; only the DAH housekeeping
        // persisted (generation is left stable so the dispatch layer classifies
        // it as idempotent).
        assert_eq!({ meta.generation }, g0);
    }

    // -- Signal / deleteAtHeight tests --

    #[test]
    fn spend_last_utxo_sets_dah() {
        let h = TestHarness::with_metadata(2, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
        let slot = engine.read_slot(&key, 5).unwrap();
        assert_eq!(slot.spending_data, sd);
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
        let mut already_spent_payloads = Vec::new();
        for handle in results {
            match handle.join().unwrap() {
                Ok(_) => successes += 1,
                Err(SpendError::AlreadySpent { spending_data, .. }) => {
                    already_spent += 1;
                    already_spent_payloads.push(spending_data);
                }
                other => panic!("unexpected result: {other:?}"),
            }
        }

        assert_eq!(successes, 1);
        assert_eq!(already_spent, 99);

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 1);
        let winning_spending_data = engine.read_slot(&key, 5).unwrap().spending_data;
        assert!(
            already_spent_payloads
                .iter()
                .all(|payload| *payload == winning_spending_data),
            "every AlreadySpent error must return the winning spending_data"
        );
    }

    #[test]
    fn concurrent_different_transactions() {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(128 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
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

            index
                .register(
                    key,
                    TxIndexEntry {
                        device_id: 0,
                        record_offset: offset,
                        utxo_count: 10,
                        block_entry_count: 0,
                        tx_flags: 0,
                        spent_utxos: 0,
                        dah_or_preserve: 0,
                        unmined_since: 0,
                        generation: 0,
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
        assert!(matches!(
            resp.errors[&2],
            SpendError::AlreadySpent { offset: 4, .. }
        ));
        assert!(matches!(
            resp.errors[&3],
            SpendError::UtxoHashMismatch { offset: 6 }
        ));
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
        assert!(matches!(
            resp.errors[&1],
            SpendError::AlreadySpent { offset: 5, .. }
        ));
        assert_eq!(resp.spent_count, 1);
    }

    #[test]
    fn spend_multi_response_includes_block_ids() {
        let h = TestHarness::with_metadata(10, TxFlags::empty(), |m| {
            m.block_entry_count = 2;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 42,
                block_height: 900,
                subtree_idx: 0,
            };
            m.block_entries_inline[1] = BlockEntry {
                block_id: 99,
                block_height: 901,
                subtree_idx: 1,
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
    fn unspend_rejects_spent_slot_when_counter_is_zero() {
        let h = TestHarness::new(10, TxFlags::empty());
        // Metadata starts with spent_utxos = 0. A spent slot with a zero
        // counter is corruption, not a valid unspend: clearing the slot would
        // hide the mismatch and make recovery/accounting impossible.
        let entry = h.engine.lookup(&h.key).unwrap();
        let spent_slot = UtxoSlot::new_spent(h.slot_hash(3), h.make_spending_data(0x11));
        io::write_utxo_slot(&*h.engine.device, entry.record_offset, 3, &spent_slot).unwrap();

        let req = UnspendRequest {
            tx_key: h.key,
            offset: 3,
            utxo_hash: h.slot_hash(3),
            spending_data: h.make_spending_data(0x11),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        match h.engine.unspend(&req) {
            Err(SpendError::StorageError { detail }) => {
                assert!(
                    detail.contains("spent_utxos is zero"),
                    "detail was: {detail}"
                );
            }
            other => panic!("expected StorageError for inconsistent counter, got {other:?}"),
        }

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.spent_utxos }, 0);
        let slot = h.engine.read_slot(&h.key, 3).unwrap();
        assert!(
            slot.is_spent(),
            "slot must remain spent after rejected unspend"
        );
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
            spending_data: h.make_spending_data(0x11),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        match h.engine.unspend(&req) {
            Err(SpendError::Pruned {
                offset: 3,
                spending_data,
            }) => assert_eq!(spending_data, h.make_spending_data(0x11)),
            other => panic!("expected Pruned, got {other:?}"),
        }
    }

    // -- Signal / deleteAtHeight additional tests --

    #[test]
    fn spend_non_last_utxo_signal_none() {
        let h = TestHarness::with_metadata(5, TxFlags::empty(), |m| {
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
            spending_data: h.make_spending_data(0xAB),
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
            spending_data: h.make_spending_data(0xAB),
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
            spending_data: h.make_spending_data(0xAB),
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
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
            spending_data: h.make_spending_data(0xAB),
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

    /// KO-11 regression (set_conflicting fast path): when the cached index
    /// entry is stale relative to the on-device metadata, the fast path must
    /// derive `old_dah` / preserve / flags from the FRESH `meta`, never from
    /// the stale cache.
    ///
    /// Repro: the on-device record is PRESERVED (`preserve_until` set, so its
    /// DAH is necessarily 0), but the index cache lies — it shows a non-zero
    /// `dah_or_preserve` interpreted as a DAH (no `HAS_PRESERVE_UNTIL`), as
    /// would happen after a prior mutation wrote metadata but failed at
    /// `sync_index_cache`. Pre-fix, set_conflicting read `has_preserve=false`
    /// and `old_dah=1288` from the cache and re-synced the cache to a bogus
    /// DAH of 1288 — resurrecting a deletable-looking record that is actually
    /// preserved. Post-fix it reads `preserve_until` from `meta`, keeps the
    /// DAH cleared, and re-syncs the cache to `dah_or_preserve=5000` with the
    /// `HAS_PRESERVE_UNTIL` discriminant.
    #[test]
    fn set_conflicting_fast_path_uses_fresh_meta_not_stale_cache() {
        // On-device record is preserved until height 5000 (DAH must be 0).
        let h = TestHarness::with_metadata(1, TxFlags::empty(), |m| {
            m.preserve_until = 5000;
            m.delete_at_height = 0;
        });

        // Poison the cache: pretend a prior failed `sync_index_cache` left a
        // stale DAH (1288) with NO preserve discriminant, while the device
        // sits at preserve_until=5000.
        {
            let mut idx = h.engine.index.write();
            let updated = idx
                .update_cached_fields(
                    &h.key,
                    TxFlags::empty().bits(), // no HAS_PRESERVE_UNTIL
                    0,                        // block_entry_count
                    0,                        // spent_utxos
                    1288,                     // stale dah_or_preserve (as DAH)
                    0,                        // unmined_since
                    0,                        // generation
                )
                .unwrap();
            assert!(updated, "cache poison must hit the entry");
        }

        let req = SetConflictingRequest {
            tx_key: h.key,
            value: true,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.set_conflicting(&req).unwrap();

        // Device DAH must remain 0 (record is preserved).
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(
            { meta.delete_at_height },
            0,
            "preserved record must not gain a DAH",
        );
        assert_eq!({ meta.preserve_until }, 5000, "preserve must survive");

        // Cache must now reflect the device truth: preserve discriminant set,
        // dah_or_preserve == preserve_until (5000) — NOT the stale 1288.
        let entry = h.engine.index.read().lookup(&h.key).unwrap();
        assert_eq!(
            entry.dah_or_preserve, 5000,
            "cache must resync to preserve_until from fresh meta, not the stale DAH",
        );
        assert!(
            TxFlags::from_bits_truncate(entry.tx_flags).contains(TxFlags::HAS_PRESERVE_UNTIL),
            "cache must carry the HAS_PRESERVE_UNTIL discriminant from fresh meta",
        );

        // The DAH secondary index must hold no entry for a preserved record.
        let dah = h.engine.dah_index();
        assert!(
            !dah.range_query(u32::MAX).contains(&h.key),
            "preserved record must not leak a DAH-index entry",
        );
    }

    /// KO-11 regression (set_mined fast path): identical stale-cache hazard.
    /// The first-ever setMined fast path must take `old_dah` / preserve /
    /// flags / counters from the fresh `meta`, not the cached entry. F-G2-011
    /// had fixed only `generation`; this asserts the DAH/preserve fields too.
    #[test]
    fn set_mined_fast_path_uses_fresh_meta_not_stale_cache() {
        // Preserved, unmined record (no block entries yet → fast path).
        let h = TestHarness::with_metadata(1, TxFlags::empty(), |m| {
            m.preserve_until = 5000;
            m.delete_at_height = 0;
            m.block_entry_count = 0;
        });

        // Poison cache: stale DAH 1288, no preserve discriminant.
        {
            let mut idx = h.engine.index.write();
            idx.update_cached_fields(
                &h.key,
                TxFlags::empty().bits(),
                0,
                0,
                1288,
                0,
                0,
            )
            .unwrap();
        }

        let req = SetMinedRequest {
            tx_key: h.key,
            block_id: 7,
            block_height: 900,
            subtree_idx: 0,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let resp = h.engine.set_mined(&req).unwrap();
        assert_eq!(resp.block_ids, vec![7]);

        // Preserved record keeps DAH == 0.
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(
            { meta.delete_at_height },
            0,
            "preserved record must not gain a DAH on setMined",
        );
        assert_eq!({ meta.preserve_until }, 5000);
        assert_eq!({ meta.block_entry_count }, 1, "block entry must be recorded");

        let entry = h.engine.index.read().lookup(&h.key).unwrap();
        assert_eq!(
            entry.dah_or_preserve, 5000,
            "cache must resync to preserve_until from fresh meta, not the stale DAH",
        );
        assert!(
            TxFlags::from_bits_truncate(entry.tx_flags).contains(TxFlags::HAS_PRESERVE_UNTIL),
            "cache must carry HAS_PRESERVE_UNTIL from fresh meta",
        );

        let dah = h.engine.dah_index();
        assert!(
            !dah.range_query(u32::MAX).contains(&h.key),
            "preserved record must not leak a DAH-index entry on setMined",
        );
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
                    spending_data: {
                        let mut sd = [0u8; 36];
                        sd[0] = i as u8;
                        sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                        sd
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

    /// R-024 (BC-09 / BC-44 / Codex F5) regression: appending multiple
    /// conflicting-children to a parent record must keep the parent's
    /// metadata coherent with the children-list block. Pre-fix the
    /// engine freed the OLD children block BEFORE allocating + writing
    /// the new one, opening a window where the parent's metadata still
    /// referenced an offset the allocator had already returned to its
    /// freelist (and could re-hand out to a different allocation).
    /// The new ordering — allocate-new → write-new → meta-update →
    /// free-old — keeps the parent metadata referring to a valid block
    /// at every step. This test exercises the happy path through
    /// multiple appends and verifies the children list resolves
    /// correctly on read-back, indirectly catching any regression in
    /// the ordering (a freed-then-reallocated block would corrupt the
    /// list).
    #[test]
    fn append_conflicting_child_preserves_list_across_multiple_appends() {
        let h = TestHarness::new(1, TxFlags::empty());

        let c1 = [0xAAu8; 32];
        let c2 = [0xBBu8; 32];
        let c3 = [0xCCu8; 32];

        h.engine.append_conflicting_child(&h.key, c1).unwrap();
        h.engine.append_conflicting_child(&h.key, c2).unwrap();
        h.engine.append_conflicting_child(&h.key, c3).unwrap();

        let children = h.engine.read_conflicting_children(&h.key).unwrap();
        assert_eq!(
            children,
            vec![c1, c2, c3],
            "children list must reflect every successful append in order",
        );

        // Idempotent re-append must not duplicate (existing dedup).
        h.engine.append_conflicting_child(&h.key, c2).unwrap();
        let children_after_dup = h.engine.read_conflicting_children(&h.key).unwrap();
        assert_eq!(
            children_after_dup,
            vec![c1, c2, c3],
            "duplicate child must be deduped",
        );

        // Verify parent metadata fields are coherent: count matches list,
        // offset is non-zero (a real allocation), and the cached
        // generation tracks the appends (one bump per real append).
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.conflicting_children_count }, 3);
        assert_ne!({ meta.conflicting_children_offset }, 0);
    }

    /// KO-5 regression: the conflicting-children list is capped at the
    /// on-disk `u8::MAX` (255) count. The 256th distinct child MUST NOT be
    /// silently dropped while reporting success.
    ///
    /// Two guarantees are asserted:
    ///  1. The direct, fallible entry point `append_conflicting_child`
    ///     returns the typed [`SpendError::ConflictingChildrenFull`] for the
    ///     256th child (pre-fix it returned an opaque `StorageError`; the
    ///     real hazard was the best-effort wrapper swallowing it).
    ///  2. The best-effort propagation wrapper does not let the loss vanish:
    ///     it increments the observable
    ///     [`Engine::conflicting_children_dropped`] counter (pre-fix the
    ///     drop was invisible — a `tracing::warn!` only).
    #[test]
    fn append_conflicting_child_overflow_is_not_silent() {
        let h = TestHarness::new(1, TxFlags::empty());

        // Fill the list to the u8 capacity (255 distinct children).
        for i in 0..255u32 {
            let mut child = [0u8; 32];
            child[0..4].copy_from_slice(&i.to_le_bytes());
            // Disambiguate beyond the first 4 bytes so no two collide.
            child[4] = 0x5A;
            h.engine
                .append_conflicting_child(&h.key, child)
                .expect("first 255 children must append successfully");
        }

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(
            { meta.conflicting_children_count },
            255,
            "list must be exactly at capacity before the overflowing append",
        );

        // Child #256 — direct path must surface the typed overflow, not OK
        // and not an opaque StorageError.
        let mut child_256 = [0u8; 32];
        child_256[0] = 0xFF;
        child_256[1] = 0xEE;
        let err = h
            .engine
            .append_conflicting_child(&h.key, child_256)
            .expect_err("the 256th child must NOT silently succeed");
        assert!(
            matches!(err, SpendError::ConflictingChildrenFull { cap } if cap == 255),
            "256th child must yield ConflictingChildrenFull, got {err:?}",
        );

        // The on-disk list must still hold exactly 255 (the overflow did not
        // corrupt the count).
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(
            { meta.conflicting_children_count },
            255,
            "overflowing append must leave the on-disk count at capacity",
        );

        // Best-effort path: the drop must be observable on the counter.
        assert_eq!(
            h.engine.conflicting_children_dropped(),
            0,
            "no drop counted before the best-effort overflow",
        );
        h.engine
            .append_conflicting_child_best_effort(&h.key, child_256, "test");
        assert_eq!(
            h.engine.conflicting_children_dropped(),
            1,
            "best-effort overflow must increment the dropped counter, not vanish into a warn",
        );
    }

    /// R-143 regression: `append_conflicting_child` must not hold the parent
    /// stripe lock while waiting on allocator work for the replacement
    /// children-list block.
    #[test]
    fn append_conflicting_child_lock_order() {
        let h = TestHarness::new(1, TxFlags::empty());
        let c1 = [0x11u8; 32];
        let c2 = [0x22u8; 32];

        h.engine.append_conflicting_child(&h.key, c1).unwrap();

        let allocator_guard = h.engine.allocator.lock();
        let engine = h.engine.clone();
        let key = h.key;
        let append_started = Arc::new(AtomicBool::new(false));
        let append_started_thread = append_started.clone();
        let append_handle = std::thread::spawn(move || {
            append_started_thread.store(true, Ordering::SeqCst);
            engine.append_conflicting_child(&key, c2)
        });

        while !append_started.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        std::thread::sleep(std::time::Duration::from_millis(50));

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let engine = h.engine.clone();
        let key = h.key;
        let probe_handle = std::thread::spawn(move || {
            let _guard = engine.locks.lock(&key);
            locked_tx.send(()).unwrap();
        });

        let parent_lock_available = locked_rx.recv_timeout(std::time::Duration::from_millis(250));
        drop(allocator_guard);

        append_handle.join().unwrap().unwrap();
        probe_handle.join().unwrap();
        assert!(
            parent_lock_available.is_ok(),
            "append_conflicting_child held the parent stripe lock while blocked on allocator"
        );

        let children = h.engine.read_conflicting_children(&h.key).unwrap();
        assert_eq!(children, vec![c1, c2]);
    }

    /// R-064/R-081 regression: `set_conflicting(true)` must update parent
    /// records' conflicting-child lists on the fast mmap path too. Pre-fix
    /// the fast path returned before the cold-data parent propagation block,
    /// so the child was marked conflicting but the parent had no backlink.
    #[test]
    fn set_conflicting_fast_path_updates_parent_children() {
        let h = TestHarness::new(1, TxFlags::empty());

        let mut child_txid = [0x22u8; 32];
        child_txid[0] = 2;
        let child_key = TxKey { txid: child_txid };
        let child_hashes = [[0xABu8; 32]];

        let mut extended_input = vec![0u8; 36];
        extended_input[..32].copy_from_slice(&h.key.txid);

        let mut inputs_blob = Vec::new();
        inputs_blob.extend_from_slice(&1u32.to_le_bytes());
        inputs_blob.extend_from_slice(&(extended_input.len() as u32).to_le_bytes());
        inputs_blob.extend_from_slice(&extended_input);

        h.engine
            .create(&CreateRequest {
                tx_id: child_txid,
                tx_version: 1,
                locktime: 0,
                fee: 0,
                size_in_bytes: 100,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                utxo_hashes: &child_hashes,
                inputs: Some(&inputs_blob),
                outputs: None,
                inpoints: None,
                is_external: false,
                created_at: 0,
                block_height: 1000,
                mined_block_infos: &[],
                frozen: false,
                conflicting: false,
                locked: false,
                external_ref: None,
                parent_txids: &[],
            })
            .unwrap();

        h.engine
            .set_conflicting(&SetConflictingRequest {
                tx_key: child_key,
                value: true,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        let children = h.engine.read_conflicting_children(&h.key).unwrap();
        assert_eq!(children, vec![child_txid]);
    }

    /// R-118 regression: when a children-list allocation reuses a freed
    /// block, alignment padding in the new block must not preserve stale
    /// bytes from the prior owner. Pre-fix `append_conflicting_child`
    /// pre-read the destination block and only overwrote the 32-byte child
    /// entry, leaving the rest of the allocated 4 KiB block unchanged.
    #[test]
    fn append_conflicting_child_no_stale_bytes_leak() {
        let h = TestHarness::new(1, TxFlags::empty());
        let align = h.engine.device.alignment();

        let stale_offset = h.engine.allocator.lock().allocate(align as u64).unwrap();
        let mut stale = AlignedBuf::new(align, align);
        stale.fill(0xA5);
        h.engine.device.pwrite_all_at(&stale, stale_offset).unwrap();
        h.engine
            .allocator
            .lock()
            .free(stale_offset, align as u64)
            .unwrap();

        let child = [0xDDu8; 32];
        h.engine.append_conflicting_child(&h.key, child).unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(
            { meta.conflicting_children_offset },
            stale_offset,
            "test setup expects allocator to reuse the stale block"
        );

        let mut read_back = AlignedBuf::new(align, align);
        h.engine
            .device
            .pread_exact_at(&mut read_back, stale_offset)
            .unwrap();
        assert_eq!(&read_back[..32], &child);
        assert!(
            read_back[32..].iter().all(|b| *b == 0),
            "children-list padding must be zeroed, not stale bytes from the freed block"
        );
    }

    // -----------------------------------------------------------------------
    // F-X-022 — Aerospike `addDeletedChildren` parity tests
    // -----------------------------------------------------------------------

    /// F-X-022: a fresh record has no deleted children — `read_deleted_children`
    /// must return an empty vec without doing any device reads against
    /// `deleted_children_offset = 0`.
    #[test]
    fn read_deleted_children_returns_empty_vec_when_count_zero() {
        let h = TestHarness::new(3, TxFlags::empty());
        let children = h.engine.read_deleted_children(&h.key).unwrap();
        assert!(children.is_empty(), "fresh record must have no deleted children");

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.deleted_children_count }, 0);
        assert_eq!({ meta.deleted_children_offset }, 0);
    }

    /// rust-engineer P0: a failed `free_conflicting_children_block` must be
    /// SURFACED as a typed `SpendError::StorageError`, never silently
    /// dropped. The pre-fix code did `let _ = self.free_conflicting_children_block(...)`
    /// at two sites, leaking device space permanently. The fix makes the
    /// method return a meaningful `Result` that the call sites act on — two
    /// propagate it with `?` (parent-gone rollback, CAS-retry rollback) and
    /// the post-commit cleanup logs it as an orphan-blob leak for the R-049
    /// sweep. This test pins the contract the whole fix rests on: the error
    /// reaches the caller. An out-of-range offset is the deterministic
    /// allocator-error trigger — `SlotAllocator::free` rejects it with
    /// `AllocatorError::InvalidFree` (offset + size overflows `u64`).
    #[test]
    fn free_conflicting_children_block_surfaces_allocator_error() {
        let h = TestHarness::new(3, TxFlags::empty());
        let err = h
            .engine
            .free_conflicting_children_block(u64::MAX, 1)
            .expect_err("freeing an out-of-range offset must error, not swallow");
        assert!(
            matches!(err, SpendError::StorageError { .. }),
            "allocator free failure must surface as SpendError::StorageError, got {err:?}",
        );
    }

    /// F-X-022: pruning a parent slot via `prune_slot_if_spent_by_child`
    /// must append the child txid to the parent's deleted-children list,
    /// observable through `read_deleted_children`. The primary UTXO_PRUNED
    /// transition still fires (verified separately by
    /// `prune_slot_if_spent_by_child_updates_counters_once`); this test
    /// pins the SECONDARY audit-trail invariant.
    #[test]
    fn prune_slot_if_spent_by_child_appends_to_deleted_children_list() {
        let h = TestHarness::new(3, TxFlags::empty());
        h.engine.spend(&h.spend_req(1)).unwrap();
        let mut child_txid = [0u8; 32];
        child_txid.copy_from_slice(&h.make_spending_data(0xAB)[..32]);

        let applied = h
            .engine
            .prune_slot_if_spent_by_child(&h.key, 1, child_txid)
            .unwrap();
        assert!(applied, "prune must apply against a SPENT slot");

        let deleted = h.engine.read_deleted_children(&h.key).unwrap();
        assert_eq!(
            deleted,
            vec![child_txid],
            "pruning the child must append its txid to the deleted-children list"
        );

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.deleted_children_count }, 1);
        assert_ne!(
            { meta.deleted_children_offset },
            0,
            "deleted_children_offset must point at a real allocation"
        );

        // Idempotent re-prune: slot already PRUNED so the prune is a
        // no-op (returns false) and must not re-append the same child.
        let applied_again = h
            .engine
            .prune_slot_if_spent_by_child(&h.key, 1, child_txid)
            .unwrap();
        assert!(!applied_again);
        let deleted_after = h.engine.read_deleted_children(&h.key).unwrap();
        assert_eq!(
            deleted_after,
            vec![child_txid],
            "idempotent re-prune must not duplicate the child entry"
        );
    }

    /// F-X-022: multiple distinct children pruned against different slots
    /// must all be preserved in the deleted-children list in declaration
    /// order. Exercises the CAS retry loop in `append_deleted_child` across
    /// sequential appends.
    #[test]
    fn deleted_children_list_survives_multiple_appends() {
        let h = TestHarness::new(3, TxFlags::empty());

        // Spend three distinct slots, each by a distinct child txid.
        let mut child_txids = Vec::with_capacity(3);
        for (i, marker) in [0xA1u8, 0xB2, 0xC3].iter().enumerate() {
            let sd = h.make_spending_data(*marker);
            let req = SpendRequest {
                tx_key: h.key,
                offset: i as u32,
                utxo_hash: h.slot_hash(i as u32),
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            };
            h.engine.spend(&req).unwrap();
            let mut child = [0u8; 32];
            child.copy_from_slice(&sd[..32]);
            child_txids.push(child);
        }

        // Prune each slot by its respective child.
        for (i, child) in child_txids.iter().enumerate() {
            let applied = h
                .engine
                .prune_slot_if_spent_by_child(&h.key, i as u32, *child)
                .unwrap();
            assert!(applied, "prune {i} must apply");
        }

        let deleted = h.engine.read_deleted_children(&h.key).unwrap();
        assert_eq!(
            deleted, child_txids,
            "all three pruned child txids must be preserved in declaration order"
        );

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.deleted_children_count }, 3);
    }

    /// F-X-022: the idempotent-respend short-circuit in `spend` must
    /// reject the request when the spending child txid is present in
    /// the parent's deleted-children list. This is the defense-in-depth
    /// guard for the resurrected-then-pruned re-spend pattern — the
    /// slot LOOKS spent by the requesting child (so the regular
    /// PRUNED-slot rejection has been bypassed by some unusual code
    /// path) but the audit list contradicts it.
    #[test]
    fn idempotent_respend_rejects_when_child_in_deleted_list() {
        let h = TestHarness::new(3, TxFlags::empty());

        // Spend the slot, then prune it (slot → PRUNED, child appended
        // to deleted-children list).
        h.engine.spend(&h.spend_req(1)).unwrap();
        let mut child_txid = [0u8; 32];
        child_txid.copy_from_slice(&h.make_spending_data(0xAB)[..32]);
        h.engine
            .prune_slot_if_spent_by_child(&h.key, 1, child_txid)
            .unwrap();

        // Sanity: deleted-children list now contains the child.
        let deleted = h.engine.read_deleted_children(&h.key).unwrap();
        assert_eq!(deleted, vec![child_txid]);

        // Manually flip the slot back to SPENT-by-this-child to simulate
        // the unusual code path where the slot was reverted after the
        // prune. The deleted-children list is the only thing standing
        // between this re-spend and an accidental accept.
        let entry = h.engine.index.read().lookup(&h.key).unwrap();
        let mut slot = h
            .engine
            .read_slot_fast(entry.record_offset, 1)
            .unwrap();
        slot.status = UTXO_SPENT;
        slot.spending_data = h.make_spending_data(0xAB);
        h.engine
            .write_slot_fast(entry.record_offset, 1, &slot)
            .unwrap();

        // Re-spend with the same (now-deleted) child txid. The
        // idempotent-respend short-circuit must consult the
        // deleted-children list and reject.
        let resp = h.engine.spend(&h.spend_req(1));
        match resp {
            Err(SpendError::DeletedChildren { offset, child_count }) => {
                assert_eq!(offset, 1);
                assert_eq!(child_count, 1);
            }
            other => panic!(
                "expected SpendError::DeletedChildren, got {other:?}"
            ),
        }
    }

    /// R-021 (BC-25 / BC-35) regression: an idempotent re-spend (same
    /// `spending_data` already on the slot) MUST be a true no-op — no
    /// generation bump, no metadata write. Pre-fix the engine
    /// incremented `metadata.generation` and wrote the new metadata
    /// back to disk without emitting a redo entry, opening a window
    /// where a crash between the metadata write and its fsync left
    /// the on-device generation below the value the master had
    /// already advertised to the client (and propagated to replicas).
    /// Test pins the symmetry with `noop_unspend_does_not_increment_generation`.
    #[test]
    fn idempotent_respend_does_not_increment_generation() {
        let h = TestHarness::new(10, TxFlags::empty());
        h.engine.spend(&h.spend_req(5)).unwrap();
        let g1 = { h.engine.read_metadata(&h.key).unwrap().generation };

        // Spend again with same data (idempotent) — must not bump.
        h.engine.spend(&h.spend_req(5)).unwrap();
        let g2 = { h.engine.read_metadata(&h.key).unwrap().generation };

        assert_eq!(
            g2, g1,
            "idempotent re-spend must not bump generation (R-021)",
        );
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
            spending_data: h.make_spending_data(0xAB),
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
            spending_data: h.make_spending_data(0xAB),
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

        // Spend — `refresh_clock` is normally called by the dispatch layer
        // once per batch; calling it explicitly here lets the direct-engine
        // test compare against a fresh wall-clock reading instead of the
        // stale cached value from `Engine::new`.
        h.engine.refresh_clock();
        let before = sys_millis();
        h.engine.spend(&h.spend_req(0)).unwrap();
        let after = sys_millis();
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert!({ meta.updated_at } >= before && { meta.updated_at } <= after + 1);

        // Unspend
        h.engine.refresh_clock();
        let before = sys_millis();
        let req = UnspendRequest {
            tx_key: h.key,
            offset: 0,
            utxo_hash: h.slot_hash(0),
            spending_data: h.make_spending_data(0xAB),
            current_block_height: 1000,
            block_height_retention: 288,
        };
        h.engine.unspend(&req).unwrap();
        let after = sys_millis();
        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert!({ meta.updated_at } >= before && { meta.updated_at } <= after + 1);
    }

    // -- Secondary index integration tests --

    #[test]
    fn two_txs_both_set_dah_different_heights() {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
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
                        block_entry_count: 0,
                        tx_flags: 0,
                        spent_utxos: 0,
                        dah_or_preserve: 0,
                        unmined_since: 0,
                        generation: 0,
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
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
            };
        });

        // Spend to trigger DAH set
        h.engine.spend(&h.spend_req(0)).unwrap();
        assert!(!h.engine.dah_index().range_query(u32::MAX).is_empty());

        // Delete the record
        let del_req = DeleteRequest { tx_key: h.key, due_guard: None };
        h.engine.delete(&del_req).unwrap();

        // DAH index should be clean
        assert!(h.engine.dah_index().range_query(u32::MAX).is_empty());
    }

    #[test]
    fn dah_range_scan_returns_correct_set() {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
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
                        block_entry_count: 0,
                        tx_flags: 0,
                        spent_utxos: 0,
                        dah_or_preserve: 0,
                        unmined_since: 0,
                        generation: 0,
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
    fn set_mined_batch_applies_shared_params() {
        let engine = create_engine();

        // Create 3 txs.
        let mut keys = Vec::new();
        for n in 0..3u8 {
            let (_, req) = make_create_req(n + 100, 2);
            let key = req.tx_key();
            engine.create(&req).unwrap();
            keys.push(key);
        }

        let params = SetMinedSharedParams {
            block_id: 42,
            block_height: 800_000,
            subtree_idx: 7,
            current_block_height: 800_000,
            block_height_retention: 288,
            on_longest_chain: true,
            unset_mined: false,
        };

        let results = engine.set_mined_batch(&params, &keys);
        assert_eq!(results.len(), 3);
        for (i, r) in results.iter().enumerate() {
            let resp = r
                .as_ref()
                .unwrap_or_else(|e| panic!("item {i} failed: {e}"));
            assert!(
                resp.block_ids.contains(&42),
                "item {i} should have block_id 42"
            );
            assert!(
                resp.generation > 0,
                "item {i} should have incremented generation"
            );
        }

        // Verify all three txs have the block entry.
        for key in &keys {
            let meta = engine.read_metadata(key).unwrap();
            assert_eq!(meta.block_entry_count, 1);
            assert_eq!({ meta.block_entries_inline[0].block_id }, 42);
        }
    }

    #[test]
    fn set_mined_batch_handles_missing_tx() {
        let h = TestHarness::new(5, TxFlags::empty());
        let missing_key = TxKey { txid: [0xFF; 32] };
        let params = SetMinedSharedParams {
            block_id: 1,
            block_height: 100,
            subtree_idx: 0,
            current_block_height: 100,
            block_height_retention: 288,
            on_longest_chain: true,
            unset_mined: false,
        };

        let results = h.engine.set_mined_batch(&params, &[h.key, missing_key]);
        assert!(results[0].is_ok(), "existing tx should succeed");
        assert!(results[1].is_err(), "missing tx should fail");
    }

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

        let resp = h
            .engine
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
            .map(|i| meta.block_entries_inline[i].block_id)
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

        let resp = h
            .engine
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

        let resp = h
            .engine
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
        assert_eq!({ meta_before.block_entries_inline[0].block_id }, {
            meta_after.block_entries_inline[0].block_id
        });
        // Slots unchanged
        assert_eq!(slot_before, slot_after);
    }

    #[test]
    fn mark_on_chain_fully_spent_evaluates_dah() {
        let h = TestHarness::with_metadata(2, TxFlags::empty(), |m| {
            m.unmined_since = 500;
            m.block_entry_count = 1;
            m.block_entries_inline[0] = BlockEntry {
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
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
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
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
            let resp = h
                .engine
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
        let resp = h
            .engine
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
    fn read_block_entry_finds_overflow_entry() {
        let h = TestHarness::new(10, TxFlags::empty());
        for bid in 1..=5u32 {
            h.engine
                .set_mined(&SetMinedRequest {
                    tx_key: h.key,
                    block_id: bid,
                    block_height: 700_000 + bid,
                    subtree_idx: bid + 10,
                    current_block_height: 800_000,
                    block_height_retention: 288,
                    on_longest_chain: true,
                    unset_mined: false,
                })
                .unwrap();
        }

        let entry = h
            .engine
            .read_block_entry(&h.key, 5)
            .unwrap()
            .expect("overflow block entry");
        assert_eq!({ entry.block_id }, 5);
        assert_eq!({ entry.block_height }, 700_005);
        assert_eq!({ entry.subtree_idx }, 15);
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
            .map(|i| meta.block_entries_inline[i].block_id)
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

    fn make_create_req(n: u8, utxo_count: usize) -> (Vec<[u8; 32]>, CreateRequest<'static>) {
        // SAFETY: We leak the Vec to get a 'static lifetime for test convenience.
        // This is fine in tests — the memory is small and the process exits after tests.
        let hashes: Vec<[u8; 32]> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i as u8;
                h[1] = (i >> 8) as u8;
                h
            })
            .collect();
        let hashes_ref: &'static [[u8; 32]] = Box::leak(hashes.clone().into_boxed_slice());
        let mut tx_id = [0u8; 32];
        tx_id[0] = n;
        tx_id[8..16].copy_from_slice(&(n as u64 * 0x9E37).to_le_bytes());
        tx_id[16] = n;
        let req = CreateRequest {
            tx_id,
            tx_version: 1,
            locktime: 0,
            fee: 500,
            size_in_bytes: 250,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            utxo_hashes: hashes_ref,
            inputs: None,
            outputs: None,
            inpoints: None,
            is_external: false,
            created_at: 1710000000000,
            block_height: 1000,
            mined_block_infos: &[],
            frozen: false,
            conflicting: false,
            locked: false,
            external_ref: None,
            parent_txids: &[],
        };
        (hashes, req)
    }

    fn test_external_ref(tx_id: [u8; 32]) -> ExternalRef {
        ExternalRef {
            store_type: 1,
            content_hash: tx_id,
            total_size: 250,
            input_count: 0,
            output_count: 0,
            inputs_offset: 0,
            outputs_offset: 0,
        }
    }

    #[test]
    fn external_create_without_external_ref_is_rejected_before_allocation() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(31, 2);
        req.is_external = true;
        req.inputs = None;
        req.outputs = None;
        req.inpoints = None;
        req.external_ref = None;

        let next_before = engine.allocator().lock().next_offset();
        match engine.create(&req) {
            Err(CreateError::MissingExternalRef) => {}
            other => panic!("expected MissingExternalRef, got {other:?}"),
        }
        assert!(engine.lookup(&req.tx_key()).is_none());
        assert_eq!(engine.allocator().lock().next_offset(), next_before);

        match engine.pre_allocate_create(&req) {
            Err(CreateError::MissingExternalRef) => {}
            other => panic!("expected MissingExternalRef from pre_allocate_create, got {other:?}"),
        }
        assert_eq!(engine.allocator().lock().next_offset(), next_before);
    }

    #[test]
    fn external_create_persists_authoritative_external_ref() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(32, 2);
        req.is_external = true;
        req.inputs = None;
        req.outputs = None;
        req.inpoints = None;
        let external_ref = test_external_ref(req.tx_id);
        req.external_ref = Some(external_ref);

        engine.create(&req).unwrap();
        let meta = engine.read_metadata(&req.tx_key()).unwrap();
        assert!(meta.flags.contains(TxFlags::EXTERNAL));
        let actual = meta.external_ref;
        assert_eq!(actual, external_ref);
    }

    #[test]
    fn external_record_missing_blob_returns_blob_not_found_not_tx_not_found() {
        // IJ-1: an EXTERNAL record that exists in the index but whose blob is
        // absent from the configured store must surface a typed
        // `SpendError::BlobNotFound` — NOT `TxNotFound`, which would tell the
        // caller the transaction never existed and mask the data loss.
        let mut engine = create_engine_inner();
        let blob = Arc::new(crate::storage::blobstore::MemoryBlobStore::new());
        engine.set_blob_store(blob);
        let engine = Arc::new(engine);

        let (_, mut req) = make_create_req(80, 2);
        req.is_external = true;
        req.inputs = None;
        req.outputs = None;
        req.inpoints = None;
        req.external_ref = Some(test_external_ref(req.tx_id));
        let key = req.tx_key();
        engine.create(&req).unwrap();

        // The record is registered (the tx exists), but no blob was ever
        // uploaded for it — exactly the lost/GC'd/never-written case.
        assert!(engine.lookup(&key).is_some());

        match engine.read_cold_data(&key) {
            Err(SpendError::BlobNotFound { txid }) => {
                assert_eq!(txid, req.tx_id);
            }
            other => panic!("expected BlobNotFound, got {other:?}"),
        }
    }

    #[test]
    fn external_record_with_no_blob_store_configured_errors_not_empty() {
        // IJ-5: an engine with NO blob store configured must NOT silently
        // return empty cold data for an EXTERNAL record (the resurrected
        // pre-F-G9-001 "silent empty" bug). It must error.
        let engine = create_engine(); // no blob store set
        let (_, mut req) = make_create_req(81, 2);
        req.is_external = true;
        req.inputs = None;
        req.outputs = None;
        req.inpoints = None;
        req.external_ref = Some(test_external_ref(req.tx_id));
        let key = req.tx_key();
        engine.create(&req).unwrap();
        assert!(engine.lookup(&key).is_some());

        match engine.read_cold_data(&key) {
            Err(SpendError::BlobNotFound { txid }) => {
                assert_eq!(txid, req.tx_id);
            }
            other => panic!("expected BlobNotFound for unconfigured store, got {other:?}"),
        }
    }

    fn create_engine() -> Arc<Engine> {
        Arc::new(create_engine_inner())
    }

    fn create_engine_inner() -> Engine {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(1000).unwrap();
        Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        )
    }

    fn create_engine_without_direct_ptr() -> Arc<Engine> {
        let inner: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let (dev, _fail) = crate::device::ReadFailingDevice::new(inner);
        let dev: Arc<dyn BlockDevice> = dev;
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
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

    /// Build a redb-backed engine. The redb backend is the only one whose
    /// reads are fallible, so the G-4 fault-injection tests use it. The
    /// `tempfile::TempDir` is returned so the caller keeps the redb files
    /// alive for the duration of the test.
    fn create_redb_engine() -> (Arc<Engine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let redb =
            crate::index::redb_primary::RedbPrimary::open(&dir.path().join("primary.redb"), 0)
                .unwrap();
        let engine = Engine::new(
            dev,
            crate::index::backend::PrimaryBackend::OnDisk(redb),
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        );
        (Arc::new(engine), dir)
    }

    /// G-4: a transient backend (redb) read failure on the spend path must
    /// surface as a storage error, NOT collapse into `TX_NOT_FOUND` for a
    /// transaction that actually exists.
    #[test]
    fn g4_spend_surfaces_backend_read_error_not_tx_not_found() {
        let (engine, _dir) = create_redb_engine();
        let (_, req) = make_create_req(7, 1);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        // Sanity: the record is present and spendable without the fault.
        assert!(engine.lookup_checked(&key).unwrap().is_some());

        // Arm a synthetic redb read failure, then spend.
        engine.arm_fail_next_index_read();
        let req = SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: [0u8; 32],
            spending_data: {
                let mut sd = [0u8; 36];
                sd[0] = 0xAB;
                sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                sd
            },
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        match engine.spend(&req) {
            Err(SpendError::StorageError { .. }) => {}
            Err(SpendError::TxNotFound) => panic!(
                "G-4: backend read error collapsed to TX_NOT_FOUND for a present record"
            ),
            other => panic!("expected StorageError, got {other:?}"),
        }
    }

    /// G-4: a transient backend (redb) read failure during the create
    /// duplicate-check must surface as a storage error, NOT collapse into
    /// "absent" and let a duplicate record be written over an existing txid.
    #[test]
    fn g4_create_dup_check_surfaces_backend_read_error_not_duplicate() {
        let (engine, _dir) = create_redb_engine();
        let (_, req) = make_create_req(8, 2);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        let first = engine.lookup_checked(&key).unwrap().unwrap();

        // A second create of the same txid must normally be a DuplicateTxId.
        // With a backend read fault armed on the duplicate-check lookup it
        // must instead surface a storage error — never silently proceed.
        engine.arm_fail_next_index_read();
        match engine.create(&req) {
            Err(CreateError::StorageError { .. }) => {}
            Err(CreateError::DuplicateTxId) => panic!(
                "G-4: backend read error during dup-check should surface as StorageError, \
                 not be (coincidentally) caught as DuplicateTxId by a later layer"
            ),
            Ok(_) => panic!(
                "G-4: backend read error collapsed to 'absent' and wrote a duplicate record"
            ),
            other => panic!("expected StorageError, got {other:?}"),
        }

        // The original record must be intact (record_offset unchanged).
        let after = engine.lookup_checked(&key).unwrap().unwrap();
        assert_eq!(
            after.record_offset, first.record_offset,
            "G-4: original record must not be overwritten by a faulted create"
        );
    }

    #[test]
    fn create_single_utxo() {
        let engine = create_engine();
        let (_, req) = make_create_req(1, 1);
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
        let (_, req) = make_create_req(2, 100);
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
        let (_, req) = make_create_req(3, 10000);
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
        let (_, mut req) = make_create_req(4, 5);
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
        let (_, req) = make_create_req(5, 10);
        let key = req.tx_key();
        let resp = engine.create(&req).unwrap();

        let entry = engine.lookup(&key).unwrap();
        assert_eq!(entry.record_offset, resp.record_offset);
        assert_eq!(entry.utxo_count, 10);
    }

    #[test]
    fn create_then_spend() {
        let engine = create_engine();
        let (_, req) = make_create_req(6, 5);
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
        let (_, req) = make_create_req(7, 5);
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
        let (_, req) = make_create_req(8, 5);
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
        let (_, req1) = make_create_req(10, 5);
        let r1 = engine.create(&req1).unwrap();
        let (_, req2) = make_create_req(11, 10);
        let r2 = engine.create(&req2).unwrap();

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
        let (_, mut req) = make_create_req(20, 3);
        let inp = vec![0x01, 0x02, 0x03, 0x04];
        let out = vec![0x0A, 0x0B, 0x0C];
        req.inputs = Some(&inp);
        req.outputs = Some(&out);

        let key = req.tx_key();
        engine.create(&req).unwrap();

        let _entry = engine.lookup(&key).unwrap();

        // Read back cold data and verify it was stored
        let cold = engine.read_cold_data(&key).unwrap();
        assert!(!cold.is_empty(), "cold data should be present");
        // Format: [inputs_len:4][inputs][outputs_len:4][outputs][inpoints_len:4][inpoints]
        assert_eq!(u32::from_le_bytes(cold[0..4].try_into().unwrap()), 4); // inputs len
        assert_eq!(&cold[4..8], &[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(u32::from_le_bytes(cold[8..12].try_into().unwrap()), 3); // outputs len
        assert_eq!(&cold[12..15], &[0x0A, 0x0B, 0x0C]);
    }

    #[test]
    fn create_without_cold_data() {
        let engine = create_engine();
        let (_, req) = make_create_req(21, 3);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let _entry = engine.lookup(&key).unwrap();
        // Without cold data, read_cold_data should return empty
        let cold = engine.read_cold_data(&key).unwrap();
        assert!(
            cold.is_empty(),
            "cold data should be empty when not provided"
        );
    }

    #[test]
    fn cold_data_not_modified_by_spend() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(22, 3);
        let inp = vec![0xDE, 0xAD];
        let out = vec![0xBE, 0xEF];
        req.inputs = Some(&inp);
        req.outputs = Some(&out);

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
        let requests: Vec<CreateRequest> = (30..40u8).map(|n| make_create_req(n, 5).1).collect();
        let results = engine.create_batch(&requests);

        assert_eq!(results.len(), 10);
        for (i, result) in results.iter().enumerate() {
            assert!(result.is_ok(), "creation {i} failed: {result:?}");
        }
    }

    #[test]
    fn batch_create_with_duplicate() {
        let engine = create_engine();
        let mut requests: Vec<CreateRequest> =
            (40..50u8).map(|n| make_create_req(n, 5).1).collect();
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
        let (_, req) = make_create_req(50, 0);
        match engine.create(&req) {
            Err(CreateError::InvalidUtxoCount) => {}
            other => panic!("expected InvalidUtxoCount, got {other:?}"),
        }
    }

    #[test]
    fn create_coinbase() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(51, 1);
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
        let (_, mut req) = make_create_req(52, 3);
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
        let (_, mut req) = make_create_req(53, 2);
        req.conflicting = true;

        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(meta.flags.contains(TxFlags::CONFLICTING));
    }

    #[test]
    fn create_unmined() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(54, 2);
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
        let (_, mut req) = make_create_req(55, 2);
        let infos = vec![MinedBlockInfo {
            block_id: 42,
            block_height: 900,
            subtree_idx: 7,
        }];
        req.mined_block_infos = &infos;

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
        let (_, req) = make_create_req(60, 5);
        let key = req.tx_key();

        engine.create(&req).unwrap();
        engine.delete(&DeleteRequest { tx_key: key, due_guard: None }).unwrap();

        // Should succeed — txid can be reused after deletion
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.utxo_count }, 5);
    }

    #[test]
    fn create_record_at_aligned_offset() {
        let engine = create_engine();
        let (_, req) = make_create_req(61, 5);
        let resp = engine.create(&req).unwrap();

        // Record offset must be aligned to device alignment (4096)
        assert_eq!(resp.record_offset % 4096, 0);
    }

    #[test]
    fn create_record_size_matches_expected() {
        let engine = create_engine();
        let (_, req) = make_create_req(62, 7);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        let expected = METADATA_SIZE as u32 + 7 * UTXO_SLOT_SIZE as u32;
        assert_eq!({ meta.record_size }, expected);
    }

    #[test]
    fn create_record_size_with_cold_data() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(63, 3);
        let inp = vec![0x01; 10];
        let out = vec![0x02; 20];
        req.inputs = Some(&inp);
        req.outputs = Some(&out);
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
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(total_size, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
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
            .map(|n| make_create_req(n + 100, 5).1) // Each ~4KB
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
        let (_, req) = make_create_req(64, 3);
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
        let (_, req) = make_create_req(60, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 2,
                utxo_hash: req.utxo_hashes[2],
            })
            .unwrap();
        let slot = engine.read_slot(&key, 2).unwrap();
        assert!(slot.is_frozen());
        assert_eq!(slot.spending_data, [0xFF; 36]);
    }

    /// R-016 (A-08): freeze must bump generation, write metadata
    /// back, and sync the index cache. Pre-fix the generation stayed
    /// flat and the cached `tx_flags` diverged from on-device state,
    /// causing fast-path ops (set_mined / set_conflicting / set_locked
    /// / preserve_until) to miscompute DAH eligibility.
    #[test]
    fn freeze_bumps_generation_and_syncs_cache() {
        let engine = create_engine();
        let (_, req) = make_create_req(0xF1, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        let pre_gen = engine.read_metadata(&key).unwrap().generation;
        let pre_cache_gen = engine.lookup(&key).unwrap().generation;

        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 1,
                utxo_hash: req.utxo_hashes[1],
            })
            .unwrap();

        let post_meta_gen = engine.read_metadata(&key).unwrap().generation;
        let post_cache_gen = engine.lookup(&key).unwrap().generation;
        assert!(
            post_meta_gen > pre_gen,
            "freeze must bump on-device generation"
        );
        assert!(
            post_cache_gen > pre_cache_gen,
            "freeze must sync the cache so index entry matches on-device generation"
        );
        assert_eq!(
            post_meta_gen, post_cache_gen,
            "cache and on-device generation must match after sync"
        );
    }

    /// R-016 (A-08): unfreeze must also bump generation + sync cache.
    #[test]
    fn unfreeze_bumps_generation_and_syncs_cache() {
        let engine = create_engine();
        let (_, req) = make_create_req(0xF2, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        let pre_gen = engine.read_metadata(&key).unwrap().generation;

        engine
            .unfreeze(&UnfreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        let post_meta_gen = engine.read_metadata(&key).unwrap().generation;
        let post_cache_gen = engine.lookup(&key).unwrap().generation;
        assert!(post_meta_gen > pre_gen, "unfreeze must bump generation");
        assert_eq!(
            post_meta_gen, post_cache_gen,
            "unfreeze must sync the cache"
        );
    }

    #[test]
    fn freeze_already_frozen() {
        let engine = create_engine();
        let (_, req) = make_create_req(61, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        match engine.freeze(&FreezeRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
        }) {
            Err(SpendError::AlreadyFrozen { offset: 0 }) => {}
            other => panic!("expected AlreadyFrozen, got {other:?}"),
        }
    }

    #[test]
    fn freeze_already_frozen_wrong_hash_returns_hash_mismatch() {
        let engine = create_engine();
        let (_, req) = make_create_req(161, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        let mut wrong_hash = req.utxo_hashes[0];
        wrong_hash[0] ^= 0xFF;
        match engine.freeze(&FreezeRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: wrong_hash,
        }) {
            Err(SpendError::UtxoHashMismatch { offset: 0 }) => {}
            other => panic!("expected UtxoHashMismatch before AlreadyFrozen, got {other:?}"),
        }
    }

    #[test]
    fn freeze_spent_utxo() {
        let engine = create_engine();
        let (_, req) = make_create_req(62, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
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

        match engine.freeze(&FreezeRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
        }) {
            Err(SpendError::AlreadySpent { offset: 0, .. }) => {}
            other => panic!("expected AlreadySpent, got {other:?}"),
        }
    }

    #[test]
    fn freeze_nonexistent_tx() {
        let engine = create_engine();
        match engine.freeze(&FreezeRequest {
            tx_key: TxKey { txid: [0xFF; 32] },
            offset: 0,
            utxo_hash: [0; 32],
        }) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn freeze_hash_mismatch() {
        let engine = create_engine();
        let (_, req) = make_create_req(63, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        match engine.freeze(&FreezeRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: [0xFF; 32],
        }) {
            Err(SpendError::UtxoHashMismatch { .. }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    #[test]
    fn freeze_does_not_change_counter() {
        let engine = create_engine();
        let (_, req) = make_create_req(64, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 0);
    }

    #[test]
    fn freeze_then_spend_returns_frozen() {
        let engine = create_engine();
        let (_, req) = make_create_req(65, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        let mut sd = [0u8; 36];
        sd[0] = 0xAB;
        match engine.spend(&SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        }) {
            Err(SpendError::Frozen { offset: 0 }) => {}
            other => panic!("expected Frozen, got {other:?}"),
        }
    }

    // -- Unfreeze tests --

    #[test]
    fn unfreeze_frozen() {
        let engine = create_engine();
        let (_, req) = make_create_req(70, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 1,
                utxo_hash: req.utxo_hashes[1],
            })
            .unwrap();
        engine
            .unfreeze(&UnfreezeRequest {
                tx_key: key,
                offset: 1,
                utxo_hash: req.utxo_hashes[1],
            })
            .unwrap();

        let slot = engine.read_slot(&key, 1).unwrap();
        assert!(slot.is_unspent());
        assert_eq!(slot.spending_data, [0u8; 36]);
    }

    #[test]
    fn unfreeze_not_frozen() {
        let engine = create_engine();
        let (_, req) = make_create_req(71, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        match engine.unfreeze(&UnfreezeRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
        }) {
            Err(SpendError::NotFrozen { offset: 0 }) => {}
            other => panic!("expected NotFrozen, got {other:?}"),
        }
    }

    #[test]
    fn unfreeze_then_spend() {
        let engine = create_engine();
        let (_, req) = make_create_req(72, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        engine
            .unfreeze(&UnfreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

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
        assert!(engine.read_slot(&key, 0).unwrap().is_spent());
    }

    // -- Reassign tests --

    /// R-017 (A-09): reassign must reject LOCKED records — the LOCKED
    /// flag exists to prevent ANY further state change on the record,
    /// not just spends. Pre-fix the reassign skipped this check, so
    /// a record marked LOCKED could still be reassigned, bypassing
    /// the flag's purpose.
    #[test]
    fn reassign_rejects_locked() {
        let engine = create_engine();
        let mut create = make_create_req(0xA0, 5).1;
        create.locked = true;
        let key = create.tx_key();
        engine.create(&create).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: create.utxo_hashes[0],
            })
            .unwrap();

        let result = engine.reassign(&ReassignRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: create.utxo_hashes[0],
            new_utxo_hash: [0xCC; 32],
            block_height: 1000,
            spendable_after: 100,
        });
        assert!(
            matches!(result, Err(SpendError::Locked)),
            "reassign on LOCKED record must return Locked, got {result:?}"
        );
    }

    /// R-017 (A-09): reassign must reject CONFLICTING records.
    #[test]
    fn reassign_rejects_conflicting() {
        let engine = create_engine();
        let mut create = make_create_req(0xA1, 5).1;
        create.conflicting = true;
        let key = create.tx_key();
        engine.create(&create).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: create.utxo_hashes[0],
            })
            .unwrap();

        let result = engine.reassign(&ReassignRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: create.utxo_hashes[0],
            new_utxo_hash: [0xDD; 32],
            block_height: 1000,
            spendable_after: 100,
        });
        assert!(
            matches!(result, Err(SpendError::Conflicting)),
            "reassign on CONFLICTING record must return Conflicting, got {result:?}"
        );
    }

    /// R-063 (A-13) regression: when the operator-supplied
    /// `block_height + spendable_after` would overflow `u32`, reassign
    /// MUST return `SpendError::ReassignOverflow` instead of silently
    /// clamping with `saturating_add` and pinning the UTXO unspendable
    /// forever (the spend path's `spendable_height > current_block_height`
    /// gate would always be true).
    #[test]
    fn reassign_overflow_checked_add_rejects_u32_max() {
        let engine = create_engine();
        let (_, req) = make_create_req(0xA3, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        let result = engine.reassign(&ReassignRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
            new_utxo_hash: [0xCC; 32],
            block_height: u32::MAX - 50,
            spendable_after: 100, // u32::MAX - 50 + 100 overflows
        });
        match result {
            Err(SpendError::ReassignOverflow {
                block_height,
                spendable_after,
            }) => {
                assert_eq!(block_height, u32::MAX - 50);
                assert_eq!(spendable_after, 100);
            }
            other => panic!(
                "reassign with overflowing spendable_height must return ReassignOverflow, got {other:?}",
            ),
        }
    }

    /// R-017 (A-09): reassign must reject coinbase records that have
    /// not yet matured.
    #[test]
    fn reassign_rejects_immature_coinbase() {
        let engine = create_engine();
        let mut create = make_create_req(0xA2, 5).1;
        create.is_coinbase = true;
        create.spending_height = 2000; // matures at block 2000
        let key = create.tx_key();
        engine.create(&create).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: create.utxo_hashes[0],
            })
            .unwrap();

        // Try to reassign at block 1500 — before maturity.
        let result = engine.reassign(&ReassignRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: create.utxo_hashes[0],
            new_utxo_hash: [0xEE; 32],
            block_height: 1500,
            spendable_after: 100,
        });
        assert!(
            matches!(result, Err(SpendError::CoinbaseImmature { .. })),
            "reassign on immature coinbase must return CoinbaseImmature, got {result:?}"
        );
    }

    #[test]
    fn reassign_frozen() {
        let engine = create_engine();
        let (_, req) = make_create_req(80, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        let new_hash = [0xBBu8; 32];
        engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                new_utxo_hash: new_hash,
                block_height: 1000,
                spendable_after: 100,
            })
            .unwrap();

        let slot = engine.read_slot(&key, 0).unwrap();
        assert!(slot.is_unspent());
        assert_eq!(slot.hash, new_hash);
        let spendable_h = u32::from_le_bytes(slot.spending_data[0..4].try_into().unwrap());
        assert_eq!(spendable_h, 1100);
    }

    #[test]
    fn reassign_not_frozen() {
        let engine = create_engine();
        let (_, req) = make_create_req(81, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        match engine.reassign(&ReassignRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
            new_utxo_hash: [0xBB; 32],
            block_height: 1000,
            spendable_after: 100,
        }) {
            Err(SpendError::NotFrozen { .. }) => {}
            other => panic!("expected NotFrozen, got {other:?}"),
        }
    }

    #[test]
    fn reassign_hash_mismatch() {
        let engine = create_engine();
        let (_, req) = make_create_req(82, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        match engine.reassign(&ReassignRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: [0xFF; 32],
            new_utxo_hash: [0xBB; 32],
            block_height: 1000,
            spendable_after: 100,
        }) {
            Err(SpendError::UtxoHashMismatch { .. }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    #[test]
    fn reassign_not_spendable_until_cooldown() {
        let engine = create_engine();
        let (_, req) = make_create_req(83, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        let new_hash = [0xCC; 32];
        engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                new_utxo_hash: new_hash,
                block_height: 1000,
                spendable_after: 100,
            })
            .unwrap();

        // Not spendable at block 1099
        let mut sd = [0u8; 36];
        sd[0] = 0xDD;
        match engine.spend(&SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: new_hash,
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1099,
            block_height_retention: 288,
        }) {
            Err(SpendError::FrozenUntil { .. }) => {}
            other => panic!("expected FrozenUntil, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // LP-3 — a reassigned record is never DAH'd, even after the reassigned
    // UTXO is itself spent (Lua `recordUtxos + 1`, teranode.lua:945). Verified
    // first that a LIVE reassigned UTXO is never deletable (frozen does not
    // count toward spent_utxos), then that the after-final-spend window is
    // also retained.
    // -----------------------------------------------------------------------

    #[test]
    fn lp3_reassign_sets_reassigned_flag() {
        let engine = create_engine();
        let (_, req) = make_create_req(0xB1, 2);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                new_utxo_hash: [0x11; 32],
                block_height: 1000,
                spendable_after: 100,
            })
            .unwrap();
        let meta = engine.read_metadata(&key).unwrap();
        assert!(
            meta.flags.contains(TxFlags::REASSIGNED),
            "reassign must set the REASSIGNED flag (LP-3)"
        );
    }

    #[test]
    fn lp3_reassigned_record_never_gets_dah_after_final_spend() {
        let engine = create_engine();
        // Single-UTXO record so that spending the reassigned slot would, for a
        // non-reassigned record, make it all-spent and DAH-eligible.
        let (_, req) = make_create_req(0xB2, 1);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        // Mine it on the longest chain so the all-spent DAH path is reachable.
        engine
            .set_mined(&crate::ops::set_mined::SetMinedRequest {
                tx_key: key,
                block_id: 7,
                block_height: 900,
                subtree_idx: 0,
                on_longest_chain: true,
                unset_mined: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        // Freeze + reassign the only output.
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        let new_hash = [0x22; 32];
        engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                new_utxo_hash: new_hash,
                block_height: 1000,
                spendable_after: 100,
            })
            .unwrap();

        // Pre-condition (audit claim): a LIVE reassigned UTXO is not
        // all-spent — spent_utxos stays 0 while the slot is unspent — so it
        // cannot be DAH-deleted.
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 0, "reassigned slot is live, not spent");
        assert_eq!(
            { meta.delete_at_height },
            0,
            "live reassigned UTXO must never carry a DAH"
        );

        // Now spend the reassigned UTXO at/after the cooldown (1100). For a
        // NON-reassigned single-output mined record this would set DAH =
        // current + retention. LP-3: the REASSIGNED flag keeps the all-spent
        // check false, so NO DAH is set — the audit/reorg evidence is retained.
        let mut sd = [0u8; 36];
        sd[0] = 0x99;
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: new_hash,
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1100,
                block_height_retention: 288,
            })
            .expect("spend of reassigned UTXO at cooldown must succeed");

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 1, "reassigned UTXO now spent");
        assert_eq!(
            { meta.delete_at_height },
            0,
            "LP-3: a reassigned record must never be DAH'd, even after final spend"
        );
        assert!(
            !engine
                .is_due_for_sweep(&key, 100_000),
            "reassigned record must never be due for the sweep (LP-3)"
        );
    }

    /// Contrast: an IDENTICAL flow WITHOUT reassignment DOES get a DAH on
    /// final spend — proving the LP-3 difference is the REASSIGNED flag, not
    /// some other property of the setup.
    #[test]
    fn lp3_non_reassigned_record_gets_dah_on_final_spend() {
        let engine = create_engine();
        let (_, req) = make_create_req(0xB3, 1);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .set_mined(&crate::ops::set_mined::SetMinedRequest {
                tx_key: key,
                block_id: 7,
                block_height: 900,
                subtree_idx: 0,
                on_longest_chain: true,
                unset_mined: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();
        let mut sd = [0u8; 36];
        sd[0] = 0x77;
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1100,
                block_height_retention: 288,
            })
            .unwrap();
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!(
            { meta.delete_at_height },
            1100 + 288,
            "non-reassigned all-spent mined record must get DAH"
        );
    }

    // -----------------------------------------------------------------------
    // LP-4 — the reassignment cooldown survives a freeze → unfreeze cycle
    // (Lua keeps it in a separate `utxoSpendableIn` bin; TeraSlab preserves
    // it in the slot's spending_data[0..4] across the freeze marker).
    // -----------------------------------------------------------------------

    #[test]
    fn lp4_freeze_unfreeze_preserves_reassign_cooldown() {
        let engine = create_engine();
        let (_, req) = make_create_req(0xB4, 2);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        // Freeze + reassign offset 0 with cooldown 1000 + 100 = 1100.
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        let new_hash = [0x33; 32];
        engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                new_utxo_hash: new_hash,
                block_height: 1000,
                spendable_after: 100,
            })
            .unwrap();
        let slot = engine.read_slot(&key, 0).unwrap();
        assert_eq!(slot.reassignment_cooldown(), 1100, "cooldown set by reassign");

        // Freeze the reassigned (unspent, cooled-down) slot, then unfreeze.
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: new_hash,
            })
            .unwrap();
        // The frozen slot keeps the cooldown in the first 4 bytes; status is
        // the authoritative frozen signal.
        let frozen = engine.read_slot(&key, 0).unwrap();
        assert!(frozen.is_frozen());
        assert_eq!(
            frozen.reassignment_cooldown(),
            1100,
            "freeze must NOT wipe the cooldown (LP-4)"
        );

        engine
            .unfreeze(&UnfreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: new_hash,
            })
            .unwrap();
        let restored = engine.read_slot(&key, 0).unwrap();
        assert!(restored.is_unspent());
        assert_eq!(
            restored.reassignment_cooldown(),
            1100,
            "unfreeze must restore the cooldown (LP-4)"
        );

        // The cooldown is still enforced: spend below 1100 → FrozenUntil.
        let mut sd = [0u8; 36];
        sd[0] = 0xAA;
        match engine.spend(&SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: new_hash,
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1099,
            block_height_retention: 288,
        }) {
            Err(SpendError::FrozenUntil {
                spendable_at_height: 1100,
                ..
            }) => {}
            other => panic!("cooldown must survive freeze/unfreeze; expected FrozenUntil(1100), got {other:?}"),
        }

        // And spendable at the cooldown height (1100).
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: new_hash,
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1100,
                block_height_retention: 288,
            })
            .expect("spend at cooldown height must succeed after freeze/unfreeze");
    }

    /// A plain freeze/unfreeze on a slot with NO cooldown stays
    /// immediately-spendable (the LP-4 change must not introduce a phantom
    /// cooldown on ordinary outputs).
    #[test]
    fn lp4_freeze_unfreeze_no_cooldown_stays_spendable() {
        let engine = create_engine();
        let (_, req) = make_create_req(0xB5, 2);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 1,
                utxo_hash: req.utxo_hashes[1],
            })
            .unwrap();
        engine
            .unfreeze(&UnfreezeRequest {
                tx_key: key,
                offset: 1,
                utxo_hash: req.utxo_hashes[1],
            })
            .unwrap();
        let slot = engine.read_slot(&key, 1).unwrap();
        assert_eq!(
            slot.spending_data, [0u8; 36],
            "no-cooldown slot must unfreeze to fully-zeroed spending_data"
        );
        assert_eq!(slot.reassignment_cooldown(), 0);
    }

    /// Boundary semantics — spendable AT stop (half-open `[start, stop)`).
    /// At `current_block_height == spendable_height` the UTXO MUST be
    /// spendable. Matches Teranode PR #949 fix to `teranode.lua:373`
    /// and svnode/Aerospike post-fix behaviour. Pre-2026-05 this asserted
    /// the inverse (FrozenUntil at exact height) — that was the bug.
    #[test]
    fn reassign_spendable_height_boundary_at_exact_height() {
        let engine = create_engine();
        let (_, req) = make_create_req(85, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        let new_hash = [0xEF; 32];
        engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                new_utxo_hash: new_hash,
                block_height: 1000,
                spendable_after: 100,
            })
            .unwrap();

        let mut sd = [0u8; 36];
        sd[0] = 0xF0;
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: new_hash,
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1100,
                block_height_retention: 288,
            })
            .expect("spend at exact spendable_height must succeed");

        // And one block below — still frozen.
        let (_, req2) = make_create_req(86, 5);
        let key2 = req2.tx_key();
        engine.create(&req2).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key2,
                offset: 0,
                utxo_hash: req2.utxo_hashes[0],
            })
            .unwrap();
        engine
            .reassign(&ReassignRequest {
                tx_key: key2,
                offset: 0,
                utxo_hash: req2.utxo_hashes[0],
                new_utxo_hash: new_hash,
                block_height: 1000,
                spendable_after: 100,
            })
            .unwrap();
        match engine.spend(&SpendRequest {
            tx_key: key2,
            offset: 0,
            utxo_hash: new_hash,
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1099,
            block_height_retention: 288,
        }) {
            Err(SpendError::FrozenUntil {
                spendable_at_height: 1100,
                ..
            }) => {}
            other => panic!("one block below spendable_height must be frozen; got {other:?}"),
        }
    }

    #[test]
    fn reassign_spendable_after_cooldown() {
        let engine = create_engine();
        let (_, req) = make_create_req(84, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        let new_hash = [0xDD; 32];
        engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                new_utxo_hash: new_hash,
                block_height: 1000,
                spendable_after: 100,
            })
            .unwrap();

        // Spendable at block 1101 (> 1100)
        let mut sd = [0u8; 36];
        sd[0] = 0xEE;
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: new_hash,
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1101,
                block_height_retention: 288,
            })
            .unwrap();
        assert!(engine.read_slot(&key, 0).unwrap().is_spent());
    }

    #[test]
    fn reassign_old_hash_spend_fails() {
        let engine = create_engine();
        let (_, req) = make_create_req(85, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        engine
            .reassign(&ReassignRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
                new_utxo_hash: [0xEE; 32],
                block_height: 1000,
                spendable_after: 100,
            })
            .unwrap();

        let mut sd = [0u8; 36];
        sd[0] = 0xFF;
        match engine.spend(&SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 2000,
            block_height_retention: 288,
        }) {
            Err(SpendError::UtxoHashMismatch { .. }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    // -- SetConflicting tests --

    #[test]
    fn set_conflicting_true() {
        let engine = create_engine();
        let (_, req) = make_create_req(90, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        engine
            .set_conflicting(&SetConflictingRequest {
                tx_key: key,
                value: true,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(meta.flags.contains(TxFlags::CONFLICTING));
        assert_ne!({ meta.delete_at_height }, 0); // DAH set for conflicting
    }

    #[test]
    fn set_conflicting_false() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(91, 5);
        req.conflicting = true;
        let key = req.tx_key();
        engine.create(&req).unwrap();

        engine
            .set_conflicting(&SetConflictingRequest {
                tx_key: key,
                value: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(!meta.flags.contains(TxFlags::CONFLICTING));
    }

    #[test]
    fn set_conflicting_blocks_spend() {
        let engine = create_engine();
        let (_, req) = make_create_req(92, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .set_conflicting(&SetConflictingRequest {
                tx_key: key,
                value: true,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        let mut sd = [0u8; 36];
        sd[0] = 0xAA;
        match engine.spend(&SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        }) {
            Err(SpendError::Conflicting) => {}
            other => panic!("expected Conflicting, got {other:?}"),
        }
    }

    #[test]
    fn set_locked_conflicting_fast_slow_generation_parity() {
        fn run(engine: Arc<Engine>) -> (u32, u32, u32, u32, u8, u8) {
            let (_, req) = make_create_req(124, 5);
            let key = req.tx_key();
            engine.create(&req).unwrap();

            let conflicting = engine
                .set_conflicting(&SetConflictingRequest {
                    tx_key: key,
                    value: true,
                    current_block_height: 1000,
                    block_height_retention: 288,
                })
                .unwrap();
            let after_conflict = engine.read_metadata(&key).unwrap();
            let conflict_entry = engine.index.read().lookup(&key).unwrap();
            assert_eq!(conflicting.generation, { after_conflict.generation });
            assert_eq!(conflict_entry.generation, { after_conflict.generation });
            assert_eq!(conflict_entry.tx_flags, after_conflict.flags.bits());
            assert_ne!({ after_conflict.delete_at_height }, 0);

            let locked_generation = engine
                .set_locked_idempotent(&SetLockedRequest {
                    tx_key: key,
                    value: true,
                })
                .unwrap();
            let after_locked = engine.read_metadata(&key).unwrap();
            let locked_entry = engine.index.read().lookup(&key).unwrap();
            assert_eq!(locked_generation, { after_locked.generation });
            assert_eq!(locked_entry.generation, { after_locked.generation });
            assert_eq!(locked_entry.tx_flags, after_locked.flags.bits());
            assert_eq!({ after_locked.delete_at_height }, 0);

            (
                conflicting.generation,
                locked_generation,
                { after_conflict.delete_at_height },
                { after_locked.delete_at_height },
                after_conflict.flags.bits(),
                after_locked.flags.bits(),
            )
        }

        let fast = run(create_engine());
        let slow = run(create_engine_without_direct_ptr());
        assert_eq!(fast, slow);
    }

    // -- SetLocked tests --

    #[test]
    fn set_locked_true() {
        let engine = create_engine();
        let (_, req) = make_create_req(100, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .set_locked_idempotent(&SetLockedRequest {
                tx_key: key,
                value: true,
            })
            .unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(meta.flags.contains(TxFlags::LOCKED));
    }

    #[test]
    fn set_locked_clears_dah() {
        let engine = create_engine();
        let (_, req) = make_create_req(101, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        // Set conflicting to get a DAH
        engine
            .set_conflicting(&SetConflictingRequest {
                tx_key: key,
                value: true,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();
        let meta = engine.read_metadata(&key).unwrap();
        assert_ne!({ meta.delete_at_height }, 0);

        // Lock clears DAH
        engine
            .set_locked_idempotent(&SetLockedRequest {
                tx_key: key,
                value: true,
            })
            .unwrap();
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn set_locked_false() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(102, 5);
        req.locked = true;
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .set_locked_idempotent(&SetLockedRequest {
                tx_key: key,
                value: false,
            })
            .unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert!(!meta.flags.contains(TxFlags::LOCKED));
    }

    #[test]
    fn locked_blocks_spend() {
        let engine = create_engine();
        let (_, req) = make_create_req(103, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .set_locked_idempotent(&SetLockedRequest {
                tx_key: key,
                value: true,
            })
            .unwrap();

        let mut sd = [0u8; 36];
        sd[0] = 0xAA;
        match engine.spend(&SpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: req.utxo_hashes[0],
            spending_data: sd,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        }) {
            Err(SpendError::Locked) => {}
            other => panic!("expected Locked, got {other:?}"),
        }
    }

    // -- PreserveUntil tests --

    #[test]
    fn preserve_until_stores_value() {
        let engine = create_engine();
        let (_, req) = make_create_req(110, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        // Set a DAH first
        engine
            .set_conflicting(&SetConflictingRequest {
                tx_key: key,
                value: true,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        engine
            .preserve_until(&PreserveUntilRequest {
                tx_key: key,
                block_height: 5000,
            })
            .unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.preserve_until }, 5000);
        assert_eq!({ meta.delete_at_height }, 0); // DAH cleared
    }

    #[test]
    fn preserve_until_blocks_dah_on_spend() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(111, 2);
        let infos = vec![MinedBlockInfo {
            block_id: 1,
            block_height: 900,
            subtree_idx: 0,
        }];
        req.mined_block_infos = &infos;
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .preserve_until(&PreserveUntilRequest {
                tx_key: key,
                block_height: 5000,
            })
            .unwrap();

        // Spend all — DAH should NOT be set because preserve_until is active
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
        sd[0] = 0xBB;
        engine
            .spend(&SpendRequest {
                tx_key: key,
                offset: 1,
                utxo_hash: req.utxo_hashes[1],
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1000,
                block_height_retention: 288,
            })
            .unwrap();

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn preserve_until_external_signals_preserve() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(112, 2);
        req.is_external = true;
        req.external_ref = Some(test_external_ref(req.tx_id));
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let resp = engine
            .preserve_until(&PreserveUntilRequest {
                tx_key: key,
                block_height: 5000,
            })
            .unwrap();
        assert_eq!(resp.signal, Signal::Preserve);
    }

    // -- Delete tests --

    #[test]
    fn delete_existing() {
        let engine = create_engine();
        let (_, req) = make_create_req(120, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        engine.delete(&DeleteRequest { tx_key: key, due_guard: None }).unwrap();
        assert!(engine.lookup(&key).is_none());
    }

    #[test]
    fn delete_syncs_tombstone_before_freeing_region() {
        let inner: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let (dev, syncs) = SyncCountingDevice::new(inner);
        let dev: Arc<dyn BlockDevice> = dev;
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(1000).unwrap();
        let engine = Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        );
        let (_, req) = make_create_req(126, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        syncs.store(0, Ordering::SeqCst);
        engine.delete(&DeleteRequest { tx_key: key, due_guard: None }).unwrap();

        assert!(
            syncs.load(Ordering::SeqCst) >= 1,
            "delete must sync the tombstone before allocator.free can reuse the region",
        );
    }

    #[test]
    fn delete_then_lookup_none() {
        let engine = create_engine();
        let (_, req) = make_create_req(121, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.delete(&DeleteRequest { tx_key: key, due_guard: None }).unwrap();

        match engine.read_metadata(&key) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn delete_nonexistent() {
        let engine = create_engine();
        match engine.delete(&DeleteRequest {
            tx_key: TxKey { txid: [0xFF; 32] },
            due_guard: None,
        }) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn delete_frees_space_for_reuse() {
        let engine = create_engine();
        let (_, req1) = make_create_req(122, 100);
        let key1 = req1.tx_key();
        let resp1 = engine.create(&req1).unwrap();
        let offset1 = resp1.record_offset;

        engine.delete(&DeleteRequest { tx_key: key1, due_guard: None }).unwrap();

        // Create another record — should reuse the freed space
        let (_, req2) = make_create_req(123, 100);
        let resp2 = engine.create(&req2).unwrap();
        // Freed space should be reused (same offset)
        assert_eq!(resp2.record_offset, offset1);
    }

    #[test]
    fn delete_tombstone_prevents_rebuild_resurrection() {
        let engine = create_engine();
        let (_, req) = make_create_req(124, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine.delete(&DeleteRequest { tx_key: key, due_guard: None }).unwrap();

        let rebuilt = PrimaryBackend::rebuild(&*engine.device, &engine.allocator.lock()).unwrap();
        assert!(
            rebuilt.lookup(&key).is_none(),
            "rebuild must ignore freed records whose metadata was tombstoned",
        );
    }

    #[test]
    fn tombstone_overwrites_metadata_header() {
        let engine = create_engine();
        let (_, req) = make_create_req(125, 5);
        let key = req.tx_key();
        let created = engine.create(&req).unwrap();

        engine.delete(&DeleteRequest { tx_key: key, due_guard: None }).unwrap();

        let align = engine.device.alignment();
        let mut buf = AlignedBuf::new(io::align_up(METADATA_SIZE, align), align);
        engine
            .device
            .pread_exact_at(&mut buf, created.record_offset)
            .unwrap();
        assert!(
            buf[..METADATA_SIZE].iter().all(|b| *b == 0),
            "delete tombstone must zero the full metadata header"
        );
    }

    // -- GetSpend tests --

    #[test]
    fn get_spend_unspent() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(130, 5);
        req.locktime = 42_000;
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let resp = engine
            .get_spend(&GetSpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        assert_eq!(resp.status, UTXO_UNSPENT);
        assert!(resp.spending_data.is_none());
        assert_eq!(resp.locktime, 42_000);
    }

    #[test]
    fn get_spend_spent() {
        let engine = create_engine();
        let (_, req) = make_create_req(131, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
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

        let resp = engine
            .get_spend(&GetSpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        assert_eq!(resp.status, UTXO_SPENT);
        assert_eq!(resp.spending_data, Some(sd));
    }

    #[test]
    fn get_spend_frozen() {
        let engine = create_engine();
        let (_, req) = make_create_req(132, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();
        engine
            .freeze(&FreezeRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();

        let resp = engine
            .get_spend(&GetSpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        assert_eq!(resp.status, UTXO_FROZEN);
        assert_eq!(resp.spending_data, Some([0xFF; 36]));
    }

    #[test]
    fn get_spend_nonexistent_tx() {
        let engine = create_engine();
        match engine.get_spend(&GetSpendRequest {
            tx_key: TxKey { txid: [0xFF; 32] },
            offset: 0,
            utxo_hash: [0; 32],
        }) {
            Err(SpendError::TxNotFound) => {}
            other => panic!("expected TxNotFound, got {other:?}"),
        }
    }

    #[test]
    fn get_spend_hash_mismatch() {
        let engine = create_engine();
        let (_, req) = make_create_req(133, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        match engine.get_spend(&GetSpendRequest {
            tx_key: key,
            offset: 0,
            utxo_hash: [0xFF; 32],
        }) {
            Err(SpendError::UtxoHashMismatch { .. }) => {}
            other => panic!("expected UtxoHashMismatch, got {other:?}"),
        }
    }

    #[test]
    fn get_spend_offset_out_of_range() {
        let engine = create_engine();
        let (_, req) = make_create_req(134, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        match engine.get_spend(&GetSpendRequest {
            tx_key: key,
            offset: 99,
            utxo_hash: [0; 32],
        }) {
            Err(SpendError::UtxoNotFound { offset: 99 }) => {}
            other => panic!("expected UtxoNotFound, got {other:?}"),
        }
    }

    #[test]
    fn get_spend_is_readonly() {
        let engine = create_engine();
        let (_, req) = make_create_req(135, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let meta_before = engine.read_metadata(&key).unwrap();
        engine
            .get_spend(&GetSpendRequest {
                tx_key: key,
                offset: 0,
                utxo_hash: req.utxo_hashes[0],
            })
            .unwrap();
        let meta_after = engine.read_metadata(&key).unwrap();

        assert_eq!({ meta_before.generation }, { meta_after.generation });
        assert_eq!({ meta_before.updated_at }, { meta_after.updated_at });
    }

    // -- Phase 6 additional tests --

    #[test]
    fn get_spend_pruned() {
        let engine = create_engine();
        let (_, req) = make_create_req(136, 5);
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
        let (_, mut req) = make_create_req(137, 5);
        req.is_external = true;
        req.external_ref = Some(test_external_ref(req.tx_id));
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
        let (_, req) = make_create_req(138, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let e1 = engine.clone();
        let hash0 = req.utxo_hashes[0];

        let h1 = std::thread::spawn(move || e1.delete(&DeleteRequest { tx_key: key, due_guard: None }));

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
        let (_, req) = make_create_req(139, 10);
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

    // ===================================================================
    // Coverage gap tests
    // ===================================================================

    // -- set_mined gaps --

    #[test]
    fn set_mined_duplicate_block_entry_idempotent() {
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

        h.engine.set_mined(&req).unwrap();
        let meta_after_first = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta_after_first.block_entry_count, 1);

        // Call set_mined again with same block_id — should be idempotent
        h.engine.set_mined(&req).unwrap();
        let meta_after_second = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!(meta_after_second.block_entry_count, 1); // NOT double-counted
        assert_eq!({ meta_after_second.block_entries_inline[0].block_id }, 42);
    }

    #[test]
    fn set_mined_clears_locked_flag() {
        let engine = create_engine();
        let (_, req) = make_create_req(200, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        // Set locked
        engine
            .set_locked_idempotent(&SetLockedRequest {
                tx_key: key,
                value: true,
            })
            .unwrap();
        let meta = engine.read_metadata(&key).unwrap();
        assert!(meta.flags.contains(TxFlags::LOCKED));

        // set_mined should clear LOCKED
        engine
            .set_mined(&SetMinedRequest {
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

        let meta = engine.read_metadata(&key).unwrap();
        assert!(
            !meta.flags.contains(TxFlags::LOCKED),
            "LOCKED flag should be cleared after set_mined"
        );
    }

    #[test]
    fn set_mined_clears_creating_flag() {
        // The CREATING flag does not exist in TeraSlab (it was eliminated
        // because TeraSlab uses single-record design). Verify that the
        // only flags that exist are the 5 defined bits, and set_mined
        // does not leave any stray bits set.
        let engine = create_engine();
        let (_, mut req) = make_create_req(201, 5);
        req.locked = true;
        let key = req.tx_key();
        engine.create(&req).unwrap();

        // Verify LOCKED is set before mining
        let meta_before = engine.read_metadata(&key).unwrap();
        assert!(meta_before.flags.contains(TxFlags::LOCKED));

        engine
            .set_mined(&SetMinedRequest {
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

        let meta_after = engine.read_metadata(&key).unwrap();
        // LOCKED should be cleared — no stray flags remain from any
        // "creating" concept (which doesn't exist in this codebase)
        assert!(!meta_after.flags.contains(TxFlags::LOCKED));
        // Only known flags should be set
        let known_mask = TxFlags::IS_COINBASE
            | TxFlags::CONFLICTING
            | TxFlags::LOCKED
            | TxFlags::EXTERNAL
            | TxFlags::LAST_SPENT_ALL;
        let stray = TxFlags::from_bits_truncate(meta_after.flags.bits() & !known_mask.bits());
        assert!(
            stray.is_empty(),
            "stray flag bits found: {:#010b}",
            stray.bits()
        );
    }

    #[test]
    fn unset_mined_sets_unmined_since() {
        let h = TestHarness::new(10, TxFlags::empty());
        // Add a block
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

        let meta = h.engine.read_metadata(&h.key).unwrap();
        assert_eq!({ meta.unmined_since }, 0); // On chain

        // Unmine the last block at current_block_height=750
        h.engine
            .set_mined(&SetMinedRequest {
                tx_key: h.key,
                block_id: 1,
                block_height: 100,
                subtree_idx: 0,
                current_block_height: 750,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();

        let meta = h.engine.read_metadata(&h.key).unwrap();
        // unmined_since should be set to the provided current_block_height, not 0
        assert_eq!(
            { meta.unmined_since },
            750,
            "unmined_since should equal current_block_height after unmining last block"
        );
    }

    #[test]
    fn set_mined_does_not_modify_utxo_slots_with_mixed_state() {
        let engine = create_engine();
        let (_, req) = make_create_req(202, 5);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        // Spend 2 of the 5 UTXOs
        for i in [1u32, 3] {
            let mut sd = [0u8; 36];
            sd[0] = i as u8;
            sd[32..36].copy_from_slice(&1u32.to_le_bytes());
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

        // Read all 5 slots before set_mined
        let slots_before: Vec<UtxoSlot> = (0..5u32)
            .map(|i| engine.read_slot(&key, i).unwrap())
            .collect();

        // Verify pre-conditions: slots 1 and 3 are spent, rest unspent
        assert!(slots_before[0].is_unspent());
        assert!(slots_before[1].is_spent());
        assert!(slots_before[2].is_unspent());
        assert!(slots_before[3].is_spent());
        assert!(slots_before[4].is_unspent());

        // set_mined
        engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 42,
                block_height: 1000,
                subtree_idx: 0,
                current_block_height: 1000,
                block_height_retention: 288,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();

        // Read all 5 slots after set_mined — must be identical
        for i in 0..5u32 {
            let slot_after = engine.read_slot(&key, i).unwrap();
            assert_eq!(
                slots_before[i as usize], slot_after,
                "slot {i} was modified by set_mined"
            );
        }
    }

    // -- delete gaps --

    #[test]
    fn delete_with_cold_data_frees_space() {
        let engine = create_engine();
        let (_, mut req) = make_create_req(210, 5);
        let inp = vec![0x01; 100];
        let out = vec![0x02; 200];
        req.inputs = Some(&inp);
        req.outputs = Some(&out);
        let key = req.tx_key();
        let resp = engine.create(&req).unwrap();
        let record_offset = resp.record_offset;

        // Verify cold data exists
        let _entry = engine.lookup(&key).unwrap();
        let cold = engine.read_cold_data(&key).unwrap();
        assert!(!cold.is_empty(), "cold data should be present");

        // Delete
        engine.delete(&DeleteRequest { tx_key: key, due_guard: None }).unwrap();

        // Verify lookup returns None
        assert!(engine.lookup(&key).is_none());

        // Verify freed space is reusable: create a new record and confirm
        // it reuses the same offset (allocator hands out freed space first)
        let (_, req2) = make_create_req(211, 5);
        let resp2 = engine.create(&req2).unwrap();
        assert_eq!(
            resp2.record_offset, record_offset,
            "freed space should be reused by allocator"
        );
    }

    // -- Concurrency tests --

    #[test]
    fn concurrent_100_threads_spend_different_utxos() {
        let engine = create_engine();
        let (_, req) = make_create_req(220, 100);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        std::thread::scope(|s| {
            for i in 0..100u32 {
                let engine = &engine;
                let utxo_hash = req.utxo_hashes[i as usize];
                s.spawn(move || {
                    let mut sd = [0u8; 36];
                    sd[0] = (i & 0xFF) as u8;
                    sd[1] = ((i >> 8) & 0xFF) as u8;
                    sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                    engine
                        .spend(&SpendRequest {
                            tx_key: key,
                            offset: i,
                            utxo_hash,
                            spending_data: sd,
                            ignore_conflicting: false,
                            ignore_locked: false,
                            current_block_height: 1000,
                            block_height_retention: 288,
                        })
                        .unwrap();
                });
            }
        });

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 100, "all 100 UTXOs should be spent");

        // Verify all slots are actually spent
        for i in 0..100u32 {
            let slot = engine.read_slot(&key, i).unwrap();
            assert!(slot.is_spent(), "slot {i} should be spent");
        }
    }

    #[test]
    fn concurrent_100_threads_spend_same_utxo_same_data() {
        let engine = create_engine();
        let (_, req) = make_create_req(221, 1);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let utxo_hash = req.utxo_hashes[0];
        let mut sd = [0u8; 36];
        sd[0] = 0xAB;
        sd[32..36].copy_from_slice(&1u32.to_le_bytes());

        std::thread::scope(|s| {
            for _ in 0..100 {
                let engine = &engine;
                s.spawn(move || {
                    // All threads use identical spending_data — should be idempotent
                    engine
                        .spend(&SpendRequest {
                            tx_key: key,
                            offset: 0,
                            utxo_hash,
                            spending_data: sd,
                            ignore_conflicting: false,
                            ignore_locked: false,
                            current_block_height: 1000,
                            block_height_retention: 288,
                        })
                        .unwrap(); // All should succeed (idempotent)
                });
            }
        });

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!(
            { meta.spent_utxos },
            1,
            "counter should be 1 (idempotent — not incremented 100 times)"
        );
        let slot = engine.read_slot(&key, 0).unwrap();
        assert_eq!(slot.spending_data, sd);
    }

    #[test]
    fn concurrent_100_threads_spend_same_utxo_different_data() {
        let engine = create_engine();
        let (_, req) = make_create_req(222, 1);
        let key = req.tx_key();
        engine.create(&req).unwrap();

        let utxo_hash = req.utxo_hashes[0];

        let results: Vec<Result<_, _>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..100u8)
                .map(|i| {
                    let engine = &engine;
                    s.spawn(move || {
                        let mut sd = [0u8; 36];
                        sd[0] = i;
                        sd[32..36].copy_from_slice(&1u32.to_le_bytes());
                        engine.spend(&SpendRequest {
                            tx_key: key,
                            offset: 0,
                            utxo_hash,
                            spending_data: sd,
                            ignore_conflicting: false,
                            ignore_locked: false,
                            current_block_height: 1000,
                            block_height_retention: 288,
                        })
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let mut successes = 0u32;
        let mut already_spent = 0u32;
        let mut already_spent_payloads = Vec::new();
        for result in &results {
            match result {
                Ok(_) => successes += 1,
                Err(SpendError::AlreadySpent { spending_data, .. }) => {
                    already_spent += 1;
                    already_spent_payloads.push(*spending_data);
                }
                other => panic!("unexpected result: {other:?}"),
            }
        }

        assert_eq!(successes, 1, "exactly one thread should succeed");
        assert_eq!(already_spent, 99, "99 threads should get AlreadySpent");

        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.spent_utxos }, 1);
        let winning_spending_data = engine.read_slot(&key, 0).unwrap().spending_data;
        assert!(
            already_spent_payloads
                .iter()
                .all(|payload| *payload == winning_spending_data),
            "every AlreadySpent error must return the winning spending_data"
        );
    }

    #[test]
    fn concurrent_create_duplicate_txid() {
        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let index = Index::new(1000).unwrap();
        let engine = Arc::new(Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        ));

        // All 10 threads try to create the same txid
        let (_, create_req) = make_create_req(230, 5);
        let used_baseline = engine.allocator_stats().used_bytes;

        let results: Vec<Result<_, _>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..10)
                .map(|_| {
                    let engine = &engine;
                    let req = &create_req;
                    s.spawn(move || engine.create(req))
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let mut successes = 0u32;
        let mut duplicates = 0u32;
        for result in &results {
            match result {
                Ok(_) => successes += 1,
                Err(CreateError::DuplicateTxId) => duplicates += 1,
                other => panic!("unexpected result: {other:?}"),
            }
        }

        // Audit A (create duplicate guard): the pre-fix non-atomic
        // lookup-then-insert allowed multiple concurrent creates of the same
        // txid to all return Ok, overwriting the index entry and orphaning
        // the losers' records. Exactly one winner is the contract.
        assert_eq!(successes, 1, "exactly one create may win for a given txid");
        assert_eq!(
            duplicates, 9,
            "all other concurrent creates must observe DuplicateTxId"
        );

        // After all threads complete, exactly one record should exist in the index
        let key = create_req.tx_key();
        let entry = engine.lookup(&key);
        assert!(
            entry.is_some(),
            "the txid should exist in the index after concurrent creates"
        );
        let meta = engine.read_metadata(&key).unwrap();
        assert_eq!({ meta.utxo_count }, 5);

        // No allocation leak: the losing creates must not retain device
        // space. Deleting the single winner must return `used_bytes` to the
        // pre-race baseline — any orphaned region from a losing create would
        // leave it permanently inflated.
        let used_after_race = engine.allocator_stats().used_bytes;
        engine.delete(&DeleteRequest { tx_key: key, due_guard: None }).unwrap();
        assert_eq!(
            engine.allocator_stats().used_bytes,
            used_baseline,
            "losing creates leaked allocator space (used after race: {used_after_race})"
        );
    }

    #[test]
    fn concurrent_create_delete_same_txid_no_leak_no_alias() {
        let engine = create_engine();
        let (_, req) = make_create_req(231, 3);
        let key = req.tx_key();
        let used_baseline = engine.allocator_stats().used_bytes;

        // Hammer create/delete on one txid from several threads. The stripe
        // lock serializes each create's duplicate-check + allocation +
        // register against each delete's tombstone + unregister + free, so
        // the store quiesces in one of exactly two states: record present
        // (one live allocation) or absent (baseline) — never an orphaned
        // region, and never an index entry pointing at another record.
        std::thread::scope(|s| {
            for _ in 0..4 {
                let engine = &engine;
                let req = &req;
                s.spawn(move || {
                    for _ in 0..200 {
                        match engine.create(req) {
                            Ok(_) | Err(CreateError::DuplicateTxId) => {}
                            Err(e) => panic!("unexpected create error: {e:?}"),
                        }
                        match engine.delete(&DeleteRequest { tx_key: key, due_guard: None }) {
                            Ok(()) | Err(SpendError::TxNotFound) => {}
                            Err(e) => panic!("unexpected delete error: {e:?}"),
                        }
                    }
                });
            }
        });

        // Quiesced: if the record survived, its metadata must belong to this
        // txid; drain it so the allocator must be back at baseline.
        if engine.lookup(&key).is_some() {
            let meta = engine.read_metadata(&key).unwrap();
            assert_eq!(
                { meta.tx_id },
                key.txid,
                "index entry aliases another transaction's record"
            );
            engine.delete(&DeleteRequest { tx_key: key, due_guard: None }).unwrap();
        }
        assert_eq!(
            engine.allocator_stats().used_bytes,
            used_baseline,
            "create/delete race leaked allocator space"
        );
    }

    #[test]
    fn create_register_failure_releases_allocation() {
        let engine = create_engine();
        let used_baseline = engine.allocator_stats().used_bytes;

        engine
            .fail_next_register
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let (_, req) = make_create_req(232, 2);
        match engine.create(&req) {
            Err(CreateError::StorageError { detail }) => {
                assert!(
                    detail.contains("injected register failure"),
                    "unexpected error detail: {detail}"
                );
            }
            other => panic!("expected injected StorageError, got {other:?}"),
        }

        // Audit A (allocation leak on register failure): the failed create
        // must roll its reservation back — no index entry, no retained space.
        assert!(
            engine.lookup(&req.tx_key()).is_none(),
            "failed create must not leave an index entry"
        );
        assert_eq!(
            engine.allocator_stats().used_bytes,
            used_baseline,
            "failed create leaked its allocation"
        );
    }

    #[test]
    fn keys_for_shard_filters_correctly() {
        let h = TestHarness::new(2, TxFlags::empty());
        let shard = crate::cluster::shards::ShardTable::shard_for_key(&h.key);

        // The key should appear in its own shard.
        let shard_keys = h.engine.keys_for_shard(shard);
        assert_eq!(shard_keys.len(), 1);
        assert_eq!(shard_keys[0], h.key);

        // A different shard should be empty (unless hash collision, but
        // with a single key this is guaranteed for at least one other shard).
        let other_shard = if shard == 0 { 1 } else { 0 };
        let other_keys = h.engine.keys_for_shard(other_shard);
        assert!(other_keys.is_empty());
    }

    #[test]
    fn keys_by_shard_groups_all_keys() {
        let h = TestHarness::new(2, TxFlags::empty());
        let by_shard = h.engine.keys_by_shard();

        // With one key, exactly one shard should have one entry.
        let total: usize = by_shard.values().map(|v| v.len()).sum();
        assert_eq!(total, 1);

        let shard = crate::cluster::shards::ShardTable::shard_for_key(&h.key);
        assert_eq!(by_shard.get(&shard).unwrap().len(), 1);
    }

    // -- Cached clock tests --

    #[test]
    fn cached_clock_initialized_on_construction() {
        let h = TestHarness::new(2, TxFlags::empty());
        let cached = h
            .engine
            .cached_millis
            .load(std::sync::atomic::Ordering::SeqCst);
        // Should be close to current time (within 2 seconds).
        let now = sys_millis();
        assert!(cached > 0, "cached clock should be initialized");
        assert!(
            now.abs_diff(cached) < 2000,
            "cached clock should be near current time"
        );
    }

    #[test]
    fn refresh_clock_updates_cached_value() {
        let h = TestHarness::new(2, TxFlags::empty());
        let before = h
            .engine
            .cached_millis
            .load(std::sync::atomic::Ordering::SeqCst);
        // Sleep briefly so the clock advances.
        std::thread::sleep(std::time::Duration::from_millis(5));
        h.engine.refresh_clock();
        let after = h
            .engine
            .cached_millis
            .load(std::sync::atomic::Ordering::SeqCst);
        assert!(after >= before, "refresh_clock should advance cached time");
    }

    #[test]
    fn clock_refresh_staleness_bounded() {
        let h = TestHarness::new(2, TxFlags::empty());
        h.engine
            .cached_millis
            .store(1, std::sync::atomic::Ordering::SeqCst);

        h.engine.refresh_clock();

        let cached = h.engine.now_millis();
        let now = sys_millis();
        assert!(cached > 1, "refresh_clock should publish a fresh timestamp");
        assert!(
            now.abs_diff(cached) < 2000,
            "cached clock should be close to current time"
        );
    }

    #[test]
    fn mutations_use_cached_clock() {
        let h = TestHarness::new(5, TxFlags::empty());
        // Refresh the clock so cached value is current.
        h.engine.refresh_clock();
        let cached = h
            .engine
            .cached_millis
            .load(std::sync::atomic::Ordering::SeqCst);

        // Perform a mutation.
        h.engine.spend(&h.spend_req(0)).unwrap();
        let meta = h.engine.read_metadata(&h.key).unwrap();

        // The updated_at should equal the cached clock value exactly
        // (since we refreshed just before and the method reads cached).
        assert_eq!(
            { meta.updated_at },
            cached,
            "mutation should use the cached clock value"
        );
    }

    // -- H2: atomic shard-count update tests --

    #[test]
    fn engine_startup_shard_counts_lazy() {
        fn key_for_shard(shard: u16, salt: u8) -> TxKey {
            assert!(shard < crate::cluster::shards::NUM_SHARDS as u16);
            let mut txid = [0u8; 32];
            txid[0..2].copy_from_slice(&shard.to_le_bytes());
            txid[2] = salt;
            txid[8..16].copy_from_slice(&((shard as u64) << 8 | salt as u64).to_le_bytes());
            TxKey { txid }
        }

        fn dummy_entry() -> TxIndexEntry {
            TxIndexEntry {
                device_id: 0,
                record_offset: 0,
                utxo_count: 1,
                block_entry_count: 0,
                tx_flags: 0,
                spent_utxos: 0,
                dah_or_preserve: 0,
                unmined_since: 0,
                generation: 0,
            }
        }

        const EXISTING_SHARD: u16 = 1234;
        const OTHER_EXISTING_SHARD: u16 = 1235;
        const PRE_INIT_CREATE_SHARD: u16 = 1236;

        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let mut index = Index::new(1000).unwrap();
        index
            .register(key_for_shard(EXISTING_SHARD, 1), dummy_entry())
            .unwrap();
        index
            .register(key_for_shard(EXISTING_SHARD, 2), dummy_entry())
            .unwrap();
        index
            .register(key_for_shard(OTHER_EXISTING_SHARD, 1), dummy_entry())
            .unwrap();

        let engine = Arc::new(Engine::new(
            dev,
            index,
            alloc,
            StripedLocks::new(1024),
            DahIndex::new(),
            UnminedIndex::new(),
        ));

        assert!(
            !engine.shard_counts_initialized_for_test(),
            "Engine::new must not eagerly initialize shard_counts",
        );

        let (_, mut pre_init_req) = make_create_req(71, 1);
        pre_init_req.tx_id = key_for_shard(PRE_INIT_CREATE_SHARD, 1).txid;
        engine
            .create(&pre_init_req)
            .expect("create before lazy count initialization should succeed");
        assert!(
            !engine.shard_counts_initialized_for_test(),
            "create before first shard_record_count should not force a full index scan",
        );

        assert_eq!(
            engine.shard_record_count(EXISTING_SHARD),
            2,
            "first shard_record_count must not return zero for existing records",
        );
        assert!(engine.shard_counts_initialized_for_test());
        assert_eq!(engine.shard_record_count(OTHER_EXISTING_SHARD), 1);
        assert_eq!(engine.shard_record_count(PRE_INIT_CREATE_SHARD), 1);

        let (_, mut post_init_req) = make_create_req(72, 1);
        post_init_req.tx_id = key_for_shard(EXISTING_SHARD, 3).txid;
        engine
            .create(&post_init_req)
            .expect("create after lazy count initialization should succeed");
        assert_eq!(engine.shard_record_count(EXISTING_SHARD), 3);

        engine
            .delete(&DeleteRequest {
                tx_key: post_init_req.tx_key(),
                due_guard: None,
            })
            .expect("delete after lazy count initialization should succeed");
        assert_eq!(engine.shard_record_count(EXISTING_SHARD), 2);

        engine
            .register(key_for_shard(EXISTING_SHARD, 1), dummy_entry())
            .expect("updating an existing index key should succeed");
        assert_eq!(
            engine.shard_record_count(EXISTING_SHARD),
            2,
            "updating an existing key must not increment the shard count",
        );
    }

    #[test]
    fn primary_resize_preserves_entries_without_inline_write_lock_rehash() {
        fn key(i: u64) -> TxKey {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            txid[8..16].copy_from_slice(&(i.wrapping_mul(17)).to_le_bytes());
            TxKey { txid }
        }

        fn entry(i: u64) -> TxIndexEntry {
            TxIndexEntry {
                device_id: 0,
                record_offset: i * 4096,
                utxo_count: 1,
                block_entry_count: 0,
                tx_flags: 0,
                spent_utxos: 0,
                dah_or_preserve: 0,
                unmined_since: 0,
                generation: 0,
            }
        }

        let dev: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let alloc = SlotAllocator::new(dev.clone()).unwrap();
        let engine = Engine::new(
            dev,
            Index::new(1).unwrap(),
            alloc,
            StripedLocks::new(64),
            DahIndex::new(),
            UnminedIndex::new(),
        );

        let initial_capacity = engine.index.read().stats().capacity;
        for i in 0..20 {
            engine
                .register_with_shard_count(key(i), entry(i))
                .expect("register should resize without losing entries");
        }

        let resized_capacity = engine.index.read().stats().capacity;
        assert!(
            resized_capacity > initial_capacity,
            "test must cross the resize threshold"
        );
        for i in 0..20 {
            assert_eq!(
                engine.lookup(&key(i)).unwrap().record_offset,
                i * 4096,
                "resized primary index lost entry {i}"
            );
        }
    }

    #[test]
    fn primary_resize_lock_mode_allows_concurrent_lookups() {
        let h = TestHarness::new(1, TxFlags::empty());
        let engine = h.engine.clone();
        let key = h.key;

        let resize_like_guard = engine.index.upgradable_read();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader_engine = engine.clone();
        std::thread::spawn(move || {
            tx.send(reader_engine.lookup(&key).is_some()).unwrap();
        });

        assert!(
            rx.recv_timeout(std::time::Duration::from_secs(1))
                .expect("lookup must not block behind an upgradable resize guard"),
            "lookup should find the existing key while resize copy lock is held"
        );
        drop(resize_like_guard);
    }

    /// Sum of per-shard counts observed on `engine`, computed from the
    /// `shard_counts` field used in migration decisions.
    fn shard_count_total(engine: &Engine) -> u64 {
        (0..4096u16).map(|s| engine.shard_record_count(s)).sum()
    }

    /// Reference map of per-shard counts computed by scanning the primary
    /// index directly.  This is what `shard_counts` MUST match after every
    /// operation for migration correctness.
    fn reference_shard_counts(engine: &Engine) -> HashMap<u16, u64> {
        let mut out: HashMap<u16, u64> = HashMap::new();
        for k in engine.all_keys() {
            let s = crate::cluster::shards::ShardTable::shard_for_key(&k);
            *out.entry(s).or_insert(0) += 1;
        }
        out
    }

    fn assert_counts_match_primary(engine: &Engine) {
        let reference = reference_shard_counts(engine);
        // 1. Every shard that the primary believes is populated must have
        //    the exact same count in `shard_counts`.
        for (&shard, &expected) in reference.iter() {
            assert_eq!(
                engine.shard_record_count(shard),
                expected,
                "shard_counts drift: shard {shard} expected {expected}",
            );
        }
        // 2. Every shard NOT in the reference must read zero.
        for shard in 0..4096u16 {
            if !reference.contains_key(&shard) {
                assert_eq!(
                    engine.shard_record_count(shard),
                    0,
                    "shard_counts drift: shard {shard} should be 0 but is {}",
                    engine.shard_record_count(shard),
                );
            }
        }
        // 3. Totals agree.
        let total: u64 = reference.values().sum();
        assert_eq!(
            total,
            shard_count_total(engine),
            "shard_counts total disagrees with primary index",
        );
        assert_eq!(
            total as usize,
            engine.all_keys().len(),
            "reference total disagrees with primary index size",
        );
    }

    #[test]
    fn shard_counts_match_primary_after_concurrent_register_unregister() {
        // Spin up N threads that each create a batch of distinct records
        // and then delete a subset, intermixed.  The bug we guard against
        // is drift between `shard_counts` and the primary index when the
        // two are mutated outside a single critical section.
        let engine = create_engine();

        const THREADS: usize = 8;
        const RECORDS_PER_THREAD: u8 = 32;

        let mut handles = Vec::with_capacity(THREADS);
        for t in 0..THREADS {
            let engine = engine.clone();
            handles.push(std::thread::spawn(move || {
                // Create RECORDS_PER_THREAD records unique to this thread.
                for i in 0..RECORDS_PER_THREAD {
                    let n = (t as u8).wrapping_mul(RECORDS_PER_THREAD).wrapping_add(i);
                    let (_, req) = make_create_req(n, 1);
                    // make_create_req(0, _) produces tx_id with leading 0 —
                    // skip it so every thread gets a distinct, non-empty id.
                    if n == 0 {
                        continue;
                    }
                    engine.create(&req).expect("create should succeed");
                }
                // Delete every other record.
                for i in 0..RECORDS_PER_THREAD {
                    if i % 2 != 0 {
                        continue;
                    }
                    let n = (t as u8).wrapping_mul(RECORDS_PER_THREAD).wrapping_add(i);
                    if n == 0 {
                        continue;
                    }
                    let (_, req) = make_create_req(n, 1);
                    let del = DeleteRequest {
                        tx_key: req.tx_key(),
                        due_guard: None,
                    };
                    match engine.delete(&del) {
                        Ok(()) => {}
                        Err(SpendError::TxNotFound) => {
                            // Another thread may not yet have inserted this
                            // slot if tx_ids collided, but our encoding is
                            // unique per (t, i) so this must not happen.
                            panic!("unexpected TxNotFound for distinct key t={t} i={i}");
                        }
                        Err(e) => panic!("unexpected delete error: {e:?}"),
                    }
                }
            }));
        }
        for h in handles {
            h.join().expect("thread panicked");
        }

        // Invariant: for every shard, shard_record_count matches the
        // number of keys the primary actually holds in that shard.
        assert_counts_match_primary(&engine);

        // Sanity: we created THREADS*RECORDS_PER_THREAD - collisions, then
        // deleted the evens.  Exact total depends on the skipped n==0 cases,
        // but it must be strictly positive.
        let total = shard_count_total(&engine);
        assert!(
            total > 0,
            "expected some records to remain, got 0 (likely all deletes ran)",
        );
    }

    #[test]
    fn shard_counts_unchanged_on_register_failure() {
        // With the fault injector armed, the next register attempt returns
        // an IndexError::FormatError WITHOUT touching the primary index or
        // shard_counts.  If the fix is correct, the count observed after
        // the failed call equals the count observed before.
        let engine = create_engine();

        // Seed with a successful create so there's a concrete shard that
        // we can check both before and after the failed call.
        let (_, seed_req) = make_create_req(1, 1);
        engine
            .create(&seed_req)
            .expect("seed create should succeed");
        let seed_shard = crate::cluster::shards::ShardTable::shard_for_key(&seed_req.tx_key());
        let seed_count = engine.shard_record_count(seed_shard);
        assert_eq!(seed_count, 1, "seed record should set shard count to 1");

        // Arm the injector and confirm a fresh create now fails WITHOUT
        // leaking into shard_counts.
        engine
            .fail_next_register
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let (_, failing_req) = make_create_req(2, 1);
        let failing_shard =
            crate::cluster::shards::ShardTable::shard_for_key(&failing_req.tx_key());
        let before_failing = engine.shard_record_count(failing_shard);

        match engine.create(&failing_req) {
            Ok(_) => panic!("expected injected register failure"),
            Err(CreateError::StorageError { detail }) => {
                assert!(
                    detail.contains("injected register failure"),
                    "unexpected error detail: {detail}",
                );
            }
            Err(e) => panic!("expected StorageError, got {e:?}"),
        }

        // shard_counts on the failing shard must NOT have incremented.
        assert_eq!(
            engine.shard_record_count(failing_shard),
            before_failing,
            "shard_counts incremented despite register failure — drift!",
        );

        // And the previously-seeded shard must be untouched.
        assert_eq!(
            engine.shard_record_count(seed_shard),
            seed_count,
            "seed shard count changed on unrelated failure",
        );

        // And the invariant still holds: counts match what the primary
        // actually contains (which is just the seed record).
        assert_counts_match_primary(&engine);

        // Finally, confirm the injector is consumed (swap cleared it) so
        // the subsequent successful call proves recovery works.
        let (_, recovery_req) = make_create_req(3, 1);
        engine
            .create(&recovery_req)
            .expect("create should succeed after injector consumed");
        assert_counts_match_primary(&engine);
    }
}
