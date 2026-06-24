//! Crash recovery by replaying redo log entries.
//!
//! ## Durability Contract (WAL-first)
//!
//! Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): TeraSlab uses a
//! WAL-first commit model with a mandatory redo log. Every mutation
//! that the dispatch path acknowledges to a client has the following
//! ordering:
//!
//! 1. Validate under lock.
//! 2. Append the redo entry and fsync the log (`RedoLog::append` +
//!    `flush`).
//! 3. Apply the mutation to the block device via `pwrite_all_at`. This
//!    write is NOT necessarily durable on return — even with `O_DIRECT`
//!    it can sit in the drive's volatile write cache (and, for
//!    file-backed devices, in unjournalled filesystem extent
//!    allocations). That is acceptable only because the fsynced redo
//!    entry from step 2 can replay it; the checkpoint therefore issues a
//!    data-device sync barrier before it fences/compacts any redo entry
//!    (see [`crate::checkpoint`]).
//! 4. Replicate.
//!
//! On crash, the redo log is the single durable source of truth for
//! the post-checkpoint window: the on-device record bytes may be the
//! pre-mutation state (steps 1-2 ran but step 3 didn't), the
//! post-mutation state (step 3 ran), or torn (a write straddled the
//! crash). Recovery replays every entry after the last checkpoint:
//!
//! * `RedoOp::Create` carries the full record bytes (metadata header + UTXO slots + cold data) so replay can reconstruct the on-device record byte-for-byte. The legacy `RedoOp::ReplicaCreate` (logs predating gap #2) registers the index only — old logs continue to replay for back-compat.
//! * `RedoOp::Spend` / `RedoOp::Unspend` carry a `new_spent_count`, but recovery does NOT trust it: the dispatcher snapshots it before taking the per-tx lock, so it can be stale, and accumulating `±1` per entry is not idempotent across spend→unspend→respend (reorg) histories. Instead, replay writes the absolute slot state and then RECOMPUTES `meta.spent_utxos` from the count of SPENT slots (B-4). This converges to the same counter no matter how much of the log was already applied before the crash, and prevents an over-counted record from satisfying the all-spent condition and getting a stale `delete_at_height` while a UTXO is still live.
//! * Other ops carry the same per-key payload they always did and replay against the on-device metadata header.
//!
//! All replays are idempotent: each entry reads the current device or
//! index state before writing and skips when the post-state already
//! matches. Replaying an already-applied operation is therefore safe
//! across multiple recovery passes (e.g. crash mid-replay).

use crate::allocator::SlotAllocator;
use crate::device::{AlignedBuf, BlockDevice, DeviceError};
use crate::index::{
    DahBackend, DahRedoEntry, ShardedIndex, TxIndexEntry, TxKey, UnminedBackend, UnminedRedoEntry,
};
use crate::io;
use crate::ops::delete_eval::{DahPatch, evaluate_delete_at_height};
use crate::record::*;
use crate::redo::{RedoEntry, RedoLog, RedoOp};
use crate::storage::blob_gc::{self, BlobGcStats};
use crate::storage::blobstore::{BlobError, BlobStore};
use thiserror::Error;

/// F-G4-011: how often `recover_*_progress` writes a durable
/// recovery-progress marker mid-replay. Each marker is a separate
/// `RedoLog::mark_recovery_progress` (append + fsync); too frequent a
/// cadence amplifies recovery latency and exposes one more I/O failure
/// surface per marker. The original value (1024) was conservatively
/// fine-grained; widening it to 16 384 cuts the marker count by 16×
/// without meaningfully growing the worst-case re-replay span if a
/// crash interrupts the recovery (recovery is idempotent, so re-doing
/// 16 K entries is not a correctness concern — only a startup-latency
/// one, dominated by the per-entry I/O which is far larger than the
/// per-1024 fsync). The final marker is still always written at the
/// end of the recovered range so the next startup can skip the bulk.
const RECOVERY_PROGRESS_INTERVAL_ENTRIES: u64 = 16384;

/// Errors during recovery.
#[derive(Error, Debug)]
pub enum RecoveryError {
    /// Redo log error.
    #[error("redo error: {0}")]
    Redo(#[from] crate::redo::RedoError),

    /// Device I/O error.
    #[error("device error: {0}")]
    Device(#[from] crate::device::DeviceError),

    /// Index error.
    #[error("index error: {0}")]
    Index(#[from] crate::index::IndexError),

    /// Deletion-tombstone log error (R1 reconstruct, design §5.1).
    #[error("tombstone log error: {0}")]
    Tombstone(#[from] crate::tombstone::TombstoneError),

    /// Deletion-tombstone redb index error (R1 reconstruct).
    #[error("tombstone index error: {0}")]
    TombstoneIndex(#[from] crate::index::redb_tombstone::TombstoneIndexError),
}

/// Cause classification for a single entry that could not be replayed.
///
/// Gap #5: the previous recovery path treated all `entries_failed` as a
/// single number and applied a blanket `MAX_TOLERATED_FAILURES = 32`
/// tolerance. That bundled benign cases (an entry's primary-index
/// reference disappeared because the record was pruned later in the log)
/// with serious cases (a device read returned an I/O error, a record
/// header was unparseable) and could mask real corruption. Classifying
/// failures at the failure site lets the startup path apply a strict
/// per-cause policy: tolerate only the benign class, fail closed on any
/// other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayCause {
    /// The redo entry references a `tx_key` that is not present in the
    /// primary index. Benign during idempotent replay: a later entry in
    /// the same log may delete the record, or the engine snapshot already
    /// captured the post-delete state. Tolerable at startup.
    MissingPrimary,
    /// A device read or write call returned an error during replay. NOT
    /// tolerable — the device is unreachable or returning corrupt blocks
    /// and continuing to start would risk serving stale or wrong data.
    IoError,
    /// A record header / metadata block could not be parsed (checksum or
    /// magic mismatch, decoded fields out of range). NOT tolerable.
    CorruptEntry,
    /// A logic-level inconsistency that does not fit the above classes
    /// (unknown metadata version, secondary-index update returned an
    /// error after the primary lookup succeeded, etc.). NOT tolerable.
    LogicError,
    /// Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): a `Create` redo
    /// entry referenced an on-device record area that returned fewer
    /// bytes than the entry asked for, or the device write of the
    /// record bytes returned a short count. NOT tolerable — short I/O
    /// means the device is misbehaving and continuing would silently
    /// register an index entry pointing at incomplete record bytes.
    MissingRecordBytes,
    /// A legacy (payload-less) `RedoOp::ReplicaCreate` referenced an on-device
    /// record that is not durable on THIS node — `read_metadata` at the
    /// entry's `record_offset` failed (the record bytes were never synced
    /// before the node stopped, or the offset was later reclaimed).
    ///
    /// Unlike [`ReplayCause::MissingRecordBytes`] (a `Create` short device I/O, which
    /// signals a misbehaving device), a legacy `Create` carries NO captured
    /// bytes and is only ever written by the replication / migration
    /// receiver (`replication::receiver`) for a SECONDARY copy whose
    /// authoritative record lives on the master. The receiver's documented
    /// durability contract (fsync data device, then flush redo, then ACK)
    /// allows a stop between the two flushes to leave a redo `Create` whose
    /// record bytes are absent; the master re-replicates / resyncs the key
    /// on rejoin. Aborting startup here would strand the whole node (it can
    /// never boot → cluster wedged at 0/N ready, scenario_09). Therefore
    /// TOLERABLE up to a cap: the index registration is skipped (no entry
    /// pointing at unreadable bytes) and the node boots and resyncs.
    ReplicaRecordAbsent,
}

/// Statistics from a recovery run.
///
/// Gap #5: per-cause counters classify each replay failure at the failure
/// site so startup can apply the correct policy. `entries_failed` is the
/// sum of all per-cause counters and is preserved for back-compat with
/// existing log lines and external dashboards.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Entries that were replayed (applied to device).
    pub entries_replayed: u64,
    /// Entries that were already applied (skipped).
    pub entries_skipped: u64,
    /// Entries that could not be replayed (sum of `failed_*` counters).
    pub entries_failed: u64,
    /// Failures whose cause was a missing primary-index entry (benign).
    pub failed_missing_primary: u64,
    /// Failures caused by a device I/O error (NOT tolerable).
    pub failed_io: u64,
    /// Failures caused by a corrupt redo / metadata record (NOT tolerable).
    pub failed_corrupt: u64,
    /// Failures from a logic-level inconsistency (NOT tolerable).
    pub failed_logic: u64,
    /// Gap #2: `Create` replay could not write the full record bytes
    /// the entry carried (short device read/write). NOT tolerable.
    pub failed_missing_record_bytes: u64,
    /// Legacy `RedoOp::ReplicaCreate` (replica/migration-received copy) whose
    /// on-device record bytes are not durable on this node. TOLERABLE up
    /// to a cap — the master re-replicates the key on rejoin. See
    /// [`ReplayCause::ReplicaRecordAbsent`].
    pub failed_replica_record_absent: u64,
    /// Height subsystem (deletion-tombstone design §4; BUG3): the maximum
    /// block height observed across all replayed height-bearing redo entries
    /// (via [`crate::redo::RedoOp::observed_block_height`]), regardless of
    /// whether the entry applied/skipped — a skipped (already-applied) entry
    /// still proves the node durably saw that height. `0` when the replayed
    /// range carried no height-bearing op.
    ///
    /// Startup folds this into the node's `last_durable_height` floor so a
    /// node whose durable `.height` file was lost still reports a height no
    /// lower than its own records prove, keeping the GC horizon / rejoin gate
    /// from regressing to 0. Independent of `tombstones_enabled`.
    pub max_observed_block_height: u32,
}

impl RecoveryStats {
    /// Accumulate another pass's stats into this one (counters summed,
    /// `max_observed_block_height` maxed). Used by multi-store recovery, which
    /// replays one store at a time and merges the per-store results.
    fn merge(&mut self, other: RecoveryStats) {
        self.entries_replayed += other.entries_replayed;
        self.entries_skipped += other.entries_skipped;
        self.entries_failed += other.entries_failed;
        self.failed_missing_primary += other.failed_missing_primary;
        self.failed_io += other.failed_io;
        self.failed_corrupt += other.failed_corrupt;
        self.failed_logic += other.failed_logic;
        self.failed_missing_record_bytes += other.failed_missing_record_bytes;
        self.failed_replica_record_absent += other.failed_replica_record_absent;
        self.max_observed_block_height = self
            .max_observed_block_height
            .max(other.max_observed_block_height);
    }
}

/// B-7: how recovery reconciles the DAH / unmined secondary indexes
/// after replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecondaryReconcile {
    /// Clear both secondaries and re-derive them by scanning EVERY primary
    /// index entry. Correct but O(store size); used when a secondary
    /// backend was not cleanly closed / its snapshot section is missing.
    FullScan,
    /// Reconcile only the keys touched by replayed redo entries against
    /// the durable (crash-safe) secondaries. O(redo size). Used when the
    /// secondaries were loaded clean.
    TouchedOnly,
}

/// Engine-level conflicting-child append intent collected during recovery.
///
/// Low-level recovery cannot replay this safely because the operation needs
/// the engine's allocator and per-key stripe locks. Startup drains these after
/// constructing the engine by calling `Engine::append_conflicting_child`, which
/// deduplicates an already-applied append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingAppendConflictingChild {
    pub parent_key: TxKey,
    pub child_txid: [u8; 32],
    /// `false` = append the child (`Engine::append_conflicting_child`);
    /// `true` = remove it (`Engine::remove_conflicting_child`). Both are
    /// drained in redo-log order after engine construction; both idempotent.
    pub is_remove: bool,
}

/// AUDIT M2.6 — engine-level deleted-child append intent collected during
/// recovery. Like [`PendingAppendConflictingChild`], low-level replay cannot
/// apply it (it needs the engine's allocator and stripe locks), so startup
/// drains these after constructing the engine via `Engine::append_deleted_child`
/// (idempotent: a re-applied append deduplicates). Without this, a crash between
/// `PruneSlotIfSpentBy` and the deleted-child append dropped the audit /
/// idempotent-respend-defense entry — the spend was still rejected via
/// `UTXO_PRUNED`, but the defense-in-depth trail was lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingAppendDeletedChild {
    pub parent_key: TxKey,
    pub child_txid: [u8; 32],
}

impl RecoveryStats {
    /// Account for a per-entry [`ReplayCause`] failure. Updates both the
    /// per-cause counter and the back-compat `entries_failed` total.
    pub(crate) fn record_failure(&mut self, cause: ReplayCause) {
        self.entries_failed += 1;
        match cause {
            ReplayCause::MissingPrimary => self.failed_missing_primary += 1,
            ReplayCause::IoError => self.failed_io += 1,
            ReplayCause::CorruptEntry => self.failed_corrupt += 1,
            ReplayCause::LogicError => self.failed_logic += 1,
            ReplayCause::MissingRecordBytes => self.failed_missing_record_bytes += 1,
            ReplayCause::ReplicaRecordAbsent => self.failed_replica_record_absent += 1,
        }
    }
}

/// F-G4-007: classify a replay failure as fatal for the recovery loop.
///
/// `MissingPrimary` is benign during idempotent replay (the record was
/// deleted later in the log, or by a later snapshot) so the loop keeps
/// going. `ReplicaRecordAbsent` is the analogous benign case for a legacy
/// replica/migration `Create` whose secondary copy was never durable on
/// this node — the master re-replicates it on rejoin, so the loop keeps
/// going rather than stranding the node. Any other cause indicates the
/// device or on-disk data is misbehaving; continuing the replay risks
/// landing later entries on top of an already-broken intermediate state
/// that `check_replay_tolerance` cannot roll back.
#[inline]
fn is_fatal_replay_cause(cause: ReplayCause) -> bool {
    !matches!(
        cause,
        ReplayCause::MissingPrimary | ReplayCause::ReplicaRecordAbsent
    )
}

/// B-6: append a recovery-progress marker, treating a full redo log as a
/// non-fatal condition.
///
/// A crash is most likely precisely when the redo log is nearly full
/// (checkpoint pressure). On the next boot the marker append can hit
/// [`RedoError::LogFull`] — and since nothing reclaims space before
/// recovery runs, propagating that error would abort startup on every
/// restart (a deterministic boot-loop). The marker is only an
/// optimization that bounds *repeated* recovery work; recovery itself is
/// idempotent, so a missing marker merely means a subsequent crash
/// re-replays some already-applied (idempotent) entries. We therefore
/// log-and-skip on `LogFull` and let recovery finish. Any other redo
/// error (a real device fault) still propagates.
fn mark_recovery_progress_non_fatal(
    log: &mut RedoLog,
    through_sequence: u64,
) -> Result<(), RecoveryError> {
    match log.mark_recovery_progress(through_sequence) {
        Ok(()) => Ok(()),
        Err(crate::redo::RedoError::LogFull { used, capacity }) => {
            tracing::warn!(
                through_sequence,
                used,
                capacity,
                "recovery: redo log full while writing progress marker — skipping \
                 marker (recovery is idempotent, will complete without it)",
            );
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// Replay redo log entries after the last checkpoint.
///
/// For each entry, checks whether the operation has already been applied
/// (idempotent check) and re-executes it if not.
///
/// `index` uses interior locking (`&ShardedIndex`), so no `&mut` is required.
pub fn recover(
    device: &dyn BlockDevice,
    redo_log: &RedoLog,
    index: &ShardedIndex,
) -> Result<RecoveryStats, RecoveryError> {
    let entries = redo_log.recover()?;
    let mut stats = RecoveryStats::default();
    // BUG-1 offset-uniqueness: build the offset→owner reverse map ONCE from
    // the loaded index (O(N)). `register_unique_offset` then evicts any
    // pre-existing alias in O(1) per create instead of scanning the whole
    // index each time.
    let mut offset_owners = build_offset_owners(index);

    for entry in &entries {
        // Height subsystem (design §4; BUG3): fold the max block height across
        // height-bearing entries, independent of replay outcome.
        if let Some(h) = entry.op.observed_block_height() {
            stats.max_observed_block_height = stats.max_observed_block_height.max(h);
        }
        match replay_entry(device, index, &mut offset_owners, entry) {
            ReplayResult::Applied => stats.entries_replayed += 1,
            ReplayResult::Skipped => stats.entries_skipped += 1,
            ReplayResult::Failed(cause) => {
                stats.record_failure(cause);
                // F-G4-007: stop on first non-tolerable failure so
                // subsequent entries cannot land partially-applied
                // state on top of an already-broken replay.
                if is_fatal_replay_cause(cause) {
                    break;
                }
            }
        }
    }

    Ok(stats)
}

/// Statistics from the deletion-tombstone recovery pass
/// ([`recover_tombstones`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TombstoneRecoveryStats {
    /// R1: number of tombstone entries reconstructed into the redb index
    /// from the on-device log (after CRC validation + torn-tail drop).
    pub tombstones_reconstructed: usize,
    /// R2: number of live primary-index records purged because an
    /// authoritative tombstone (generation at-or-ahead of the live record)
    /// existed for them.
    pub records_self_purged: usize,
    /// R2: number of tombstoned keys left in place because the live record
    /// had a STRICTLY HIGHER generation than the tombstone (a legitimate
    /// post-deletion re-creation — design §8.4). These are NOT over-purged.
    pub kept_newer_generation: usize,
}

/// Reconstruct the deletion-tombstone index from the on-device log (R1) and
/// run the R2 self-purge pass (deletion-tombstone design §5).
///
/// This runs once at boot, AFTER redo replay + primary-index load + the redb
/// tombstone index has been opened, and AFTER the engine is constructed (R2
/// routes record removal through the engine's delete primitive).
///
/// ## R1 — durability across restart (design §5.1)
///
/// Scans the durable, never-reset tombstone log and rebuilds the redb
/// tombstone index from it via
/// [`crate::index::redb_tombstone::RedbTombstoneIndex::rebuild_from`]. CRC
/// validation and torn-tail dropping are handled by the log's
/// [`crate::tombstone::TombstoneLog::scan`]; a half-written final entry is
/// dropped exactly like a torn redo tail.
///
/// ## R2 — self-purge (design §5.2 — the critical correctness step)
///
/// For every tombstone key, if the primary index STILL holds that key AND the
/// tombstone's generation is at-or-ahead of the live record's generation
/// (wrapping comparison, [`crate::record::generation_at_or_ahead`]), the
/// record is removed locally via `crate::ops::engine::Engine::delete_for_purge`
/// (which does NOT write a new tombstone — the tombstone already exists). This
/// collapses any record the node resurrected from its own redo/device load of
/// a key the cluster authoritatively deleted, making the node safe even before
/// it rejoins.
///
/// Generation guard (design §8.4): a record whose live generation is STRICTLY
/// HIGHER than the tombstone's is a legitimate post-deletion re-creation and is
/// KEPT — the tombstone only authorizes dropping the version it recorded or an
/// older one.
///
/// ## Index/allocator inconsistency the R2 purge must tolerate
///
/// The record R2 finds "live" is one this node resurrected from its OWN durable
/// state (redo replay + on-device record load) for a key the cluster
/// authoritatively deleted. That resurrection can leave the primary index and
/// the allocator's recovered free-list DISAGREEING about the record's region:
/// the index entry points at `record_offset`, but the allocator's recovered
/// free-list ALREADY considers `[record_offset, record_offset + record_size)`
/// free (the deletion freed it pre-crash and that free was journaled, while the
/// stale index entry was re-derived from an older redo/record image). The two
/// recovered structures are rebuilt from independent durable sources, so this
/// skew is expected, not corruption.
///
/// Consequently R2's removal cannot blindly free the region: the allocator's
/// free-list is AUTHORITATIVE that the region is dead, and a second `free` of an
/// already-free range is (correctly) rejected as a double-free. R2's actual
/// goal is narrower — drop the stale primary-index entry so the resurrected key
/// is gone. `crate::ops::engine::Engine::delete_for_purge` therefore tolerates
/// an already-free region: it removes the index entry (durably, via the redb
/// primary index `commit`, so the key cannot reappear on the next restart — no
/// per-restart purge loop) and SKIPS the redundant free when the region is fully
/// contained in an existing free region. A PARTIAL overlap (the record range
/// extending past the free region into possibly-live space) is NOT tolerated and
/// still surfaces as an error. This makes the already-free case a SUCCESSFUL
/// purge (counted, no warning), because the record is gone from the index (R2's
/// goal) and the region is correctly free.
///
/// ## Idempotency
///
/// Re-running yields the same state: `rebuild_from` is a deterministic
/// clear-and-repopulate, and R2 only purges keys still live + tombstoned, so a
/// second run finds the purged keys absent and does nothing.
///
/// Gated by the caller on `tombstones_enabled` — when the feature is off this
/// is not called at all.
///
/// # Errors
/// * [`RecoveryError::Tombstone`] if scanning the log fails (a torn TAIL is
///   tolerated by the scan and is NOT an error; only a device read failure
///   propagates).
/// * [`RecoveryError::TombstoneIndex`] if the redb rebuild fails.
///
/// An R2 purge that fails for a single key is logged and counted but does NOT
/// abort the pass — a transient per-key delete error must not wedge boot; the
/// next restart re-derives the index and retries the purge (idempotent).
pub fn recover_tombstones(
    engine: &crate::ops::engine::Engine,
    log: &crate::tombstone::TombstoneLog,
    index: &mut crate::index::redb_tombstone::RedbTombstoneIndex,
) -> Result<TombstoneRecoveryStats, RecoveryError> {
    let mut stats = TombstoneRecoveryStats::default();

    // R1: rebuild the redb tombstone index from the durable log.
    let entries = log.scan()?;
    stats.tombstones_reconstructed = entries.len();
    index.rebuild_from(entries.iter().copied())?;

    // R2: self-purge. Iterate the reconstructed tombstone entries (the log
    // is the source of truth; the redb index we just rebuilt mirrors it).
    // For a key that appears multiple times in the log, the LAST entry is
    // authoritative — that matches `rebuild_from`'s last-writer-wins, so we
    // also fold to the last generation seen per key to avoid an earlier,
    // lower-generation entry wrongly purging a key the redb index now records
    // at a higher generation.
    let mut latest: std::collections::HashMap<[u8; 32], u32> = std::collections::HashMap::new();
    for t in &entries {
        let txid = t.txid;
        let generation = t.generation;
        latest
            .entry(txid)
            .and_modify(|g| {
                // Keep the generation that is at-or-ahead under wrapping
                // order (the authoritative latest tombstone for the key).
                if crate::record::generation_at_or_ahead(generation, *g) {
                    *g = generation;
                }
            })
            .or_insert(generation);
    }

    for (txid, tombstone_gen) in latest {
        let key = TxKey { txid };
        // Read the live primary-index entry. A read error is surfaced (we
        // must not collapse a backend error into "absent" and skip a purge
        // that is actually needed).
        let live = engine.lookup_checked(&key).map_err(RecoveryError::Index)?;
        let Some(entry) = live else {
            // Not resurrected — nothing to purge.
            continue;
        };
        let live_gen = entry.generation;

        // BUG-1 fix #4: the generation keep-guard below trusts that the
        // live entry's cached `generation` belongs to THIS key. That is only
        // true if the on-device record at the entry's offset is actually
        // this key's record. An aliased entry (index A→offset but the offset
        // holds record B) carries B's `generation`, so the keep-guard would
        // compare the tombstone against a FOREIGN generation and could
        // wrongly "keep" the corrupt alias. Verify ownership by reading the
        // on-device metadata: `read_metadata` returns `TxNotFound` precisely
        // when `meta.tx_id != key.txid`. Only a clean `TxNotFound` proves
        // the alias; any other error (transient device/backend read) is
        // ambiguous, so we conservatively treat the entry as owning its
        // offset and fall back to the normal generation keep-guard rather
        // than force a removal on an unclear read.
        let is_alias = matches!(
            engine.read_metadata(&key),
            Err(crate::ops::error::SpendError::TxNotFound)
        );

        if is_alias {
            // The resurrected entry points at a record that belongs to a
            // DIFFERENT transaction — pure corruption. Remove the stale index
            // row UNCONDITIONALLY (no generation guard: its generation is
            // foreign). This MUST go through the index-only purge: the normal
            // `delete_for_purge` would itself reject on the tx_id mismatch
            // (its `read_metadata_for_key` returns `TxNotFound`) and leave the
            // alias in place. The on-device bytes are left intact — they are
            // the rightful owner's record, not this key's.
            match engine.purge_aliased_index_entry(&key) {
                Ok(true) => stats.records_self_purged += 1,
                // Already gone (raced away / concurrently removed) — benign.
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(
                        txid = ?&txid[..4],
                        err = %e,
                        "tombstone recovery: R2 aliased-entry purge failed; will \
                         retry on next restart (idempotent)",
                    );
                }
            }
            continue;
        }

        // Generation guard (design §8.4): purge only when the tombstone is
        // at-or-ahead of the live record. A strictly-higher live generation
        // is a legitimate re-creation post-deletion — keep it.
        if !crate::record::generation_at_or_ahead(tombstone_gen, live_gen) {
            stats.kept_newer_generation += 1;
            continue;
        }

        // The record was resurrected from this node's own durable state for a
        // key the cluster authoritatively deleted. Purge it WITHOUT writing a
        // new tombstone (the tombstone already exists). `due_guard: None` so
        // the purge is unconditional — we are not re-validating a DAH
        // predicate, we are enforcing an existing authoritative deletion.
        let req = crate::ops::remaining::DeleteRequest {
            tx_key: key,
            due_guard: None,
        };
        match engine.delete_for_purge(&req) {
            Ok(()) => stats.records_self_purged += 1,
            Err(crate::ops::error::SpendError::TxNotFound) => {
                // Raced away between the lookup and the delete (e.g. a
                // concurrent op) — already gone, nothing to do.
            }
            Err(e) => {
                tracing::warn!(
                    txid = ?&txid[..4],
                    err = %e,
                    "tombstone recovery: R2 self-purge failed for a key; will retry \
                     on next restart (idempotent)",
                );
            }
        }
    }

    Ok(stats)
}

/// Outcome of an offline [`repair_torn_slots`] pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RepairReport {
    /// Spend/Unspend redo entries inspected.
    pub entries_scanned: u64,
    /// CRC-failing slots that were rebuilt from a V3 redo entry's
    /// `utxo_hash`.
    pub slots_repaired: u64,
    /// CRC-failing slots covered only by a legacy V2/V1 redo entry (no
    /// `utxo_hash`) — the WAL cannot rebuild them. Reported as
    /// `(txid, slot_index)` so an operator can target them manually.
    pub unrecoverable: Vec<([u8; 32], u32)>,
    /// Entries whose primary-index key was absent (record deleted later
    /// in the log / by a snapshot). Benign.
    pub missing_primary: u64,
}

/// B-5: offline repair pass that rebuilds CRC-failing UTXO slots from the
/// redo log.
///
/// Walks every `SpendV2`/`UnspendV2` redo entry. For each, reads the
/// target slot; when the slot fails its CRC (a torn intra-sector write in
/// the WAL-protected window, or latent bitrot) the slot is reconstructed
/// from the entry's `utxo_hash` (V3 entries) exactly as recovery does
/// inline. Slots covered only by a legacy entry without the hash are
/// reported in [`RepairReport::unrecoverable`] rather than silently
/// skipped, so a torn slot becomes operator-recoverable instead of a
/// permanent boot-loop brick.
///
/// This is an OFFLINE tool: the server must be stopped so no concurrent
/// mutation races the slot rewrites. It does not mutate the redo log.
///
/// # Errors
///
/// Returns [`RecoveryError`] only if reading the redo log itself fails;
/// per-slot device errors are recorded in the report, not propagated, so
/// a single bad region does not abort the whole pass.
pub fn repair_torn_slots(
    device: &dyn BlockDevice,
    redo_log: &RedoLog,
    index: &ShardedIndex,
) -> Result<RepairReport, RecoveryError> {
    let entries = redo_log.recover()?;
    let mut report = RepairReport::default();

    // How a torn slot is reconstructed from its redo entry. SpendV2/UnspendV2
    // carry an optional hash (V3); FreezeV2/UnfreezeV2 always carry it.
    enum Rebuild {
        Spent([u8; 36]),
        Unspent,
        Frozen,
    }

    for entry in &entries {
        let (tx_key, offset, hash, kind) = match &entry.op {
            RedoOp::SpendV2 {
                tx_key,
                offset,
                spending_data,
                utxo_hash,
                ..
            } => (tx_key, *offset, *utxo_hash, Rebuild::Spent(*spending_data)),
            RedoOp::UnspendV2 {
                tx_key,
                offset,
                utxo_hash,
                ..
            } => (tx_key, *offset, *utxo_hash, Rebuild::Unspent),
            // AUDIT M2.7 — the repair CLI must reconstruct the same torn freeze
            // slots that `replay_freeze`/`replay_unfreeze` now self-heal (M1.4),
            // so an operator-run repair recovers FreezeV2/UnfreezeV2 too.
            RedoOp::FreezeV2 {
                tx_key,
                offset,
                utxo_hash,
            } => (tx_key, *offset, Some(*utxo_hash), Rebuild::Frozen),
            RedoOp::UnfreezeV2 {
                tx_key,
                offset,
                utxo_hash,
            } => (tx_key, *offset, Some(*utxo_hash), Rebuild::Unspent),
            _ => continue,
        };
        report.entries_scanned += 1;

        let ie = match index.lookup(tx_key) {
            Some(e) => e,
            None => {
                report.missing_primary += 1;
                continue;
            }
        };

        // Only act on a CRC-failing slot — a readable slot needs no
        // repair (normal recovery already handles its state).
        match io::read_utxo_slot(device, ie.record_offset, offset) {
            Ok(_) => continue,
            Err(DeviceError::RecordCorruption { .. }) => {}
            // A non-corruption device error is a hardware problem the WAL
            // cannot fix; surface it as unrecoverable for this slot.
            Err(_) => {
                report.unrecoverable.push((tx_key.txid, offset));
                continue;
            }
        }

        let Some(hash) = hash else {
            report.unrecoverable.push((tx_key.txid, offset));
            continue;
        };

        let rebuilt = match kind {
            Rebuild::Spent(spending_data) => UtxoSlot::new_spent(hash, spending_data),
            Rebuild::Unspent => UtxoSlot::new_unspent(hash),
            Rebuild::Frozen => UtxoSlot::new_frozen(hash),
        };
        if io::write_utxo_slot(device, ie.record_offset, offset, &rebuilt).is_ok() {
            report.slots_repaired += 1;
        } else {
            report.unrecoverable.push((tx_key.txid, offset));
        }
    }

    // The rewrites land in the device's write cache; make them durable
    // before returning so a crash right after repair does not lose the
    // reconstructed slots.
    device.sync()?;
    Ok(report)
}

/// Replay redo log entries, reconciling secondary indexes as well.
///
/// Like [`recover`], but also replays [`RedoOp::SecondaryUnminedUpdate`] and
/// [`RedoOp::SecondaryDahUpdate`] entries against the provided secondary
/// backends. The secondary replay is idempotent: the current primary-index
/// value is checked, and the secondary update is only applied if the
/// secondary index's current state is stale (i.e. does not match the
/// primary's authoritative `unmined_since` / `delete_at_height`).
///
/// Call this instead of [`recover`] when the secondary indexes (particularly
/// the on-disk redb-backed ones) need to be reconciled against the redo log
/// after a crash.
pub fn recover_all(
    device: &dyn BlockDevice,
    redo_log: &RedoLog,
    index: &ShardedIndex,
    dah: &mut DahBackend,
    unmined: &mut UnminedBackend,
) -> Result<RecoveryStats, RecoveryError> {
    recover_all_with_allocator(device, redo_log, index, dah, unmined, None)
}

/// Replay redo entries, reconciling secondary indexes and — if provided —
/// the allocator's freelist and high-water mark.
///
/// This is the full-recovery entry point. When `allocator` is `Some`, every
/// [`RedoOp::AllocateRegion`] and [`RedoOp::FreeRegion`] entry is applied
/// via [`SlotAllocator::replay_redo`], which is idempotent. Callers that
/// have already persisted the allocator snapshot may still call this — the
/// idempotency check skips allocations already reflected in the snapshot.
///
/// Index and secondary reconciliation happens in the same single pass,
/// preserving redo log ordering.
///
/// After a successful call, callers SHOULD invoke
/// [`SlotAllocator::persist`] and then checkpoint/truncate the redo log so
/// the next startup can skip replay.
pub fn recover_all_with_allocator(
    device: &dyn BlockDevice,
    redo_log: &RedoLog,
    index: &ShardedIndex,
    dah: &mut DahBackend,
    unmined: &mut UnminedBackend,
    allocator: Option<&mut SlotAllocator>,
) -> Result<RecoveryStats, RecoveryError> {
    let (stats, _, _) = recover_all_with_allocator_collecting_pending_conflicts(
        device, redo_log, index, dah, unmined, allocator,
    )?;
    Ok(stats)
}

/// Full recovery variant that also returns engine-level conflicting-child
/// append intents that must be drained after [`crate::ops::engine::Engine`]
/// construction.
pub fn recover_all_with_allocator_collecting_pending_conflicts(
    device: &dyn BlockDevice,
    redo_log: &RedoLog,
    index: &ShardedIndex,
    dah: &mut DahBackend,
    unmined: &mut UnminedBackend,
    allocator: Option<&mut SlotAllocator>,
) -> Result<
    (
        RecoveryStats,
        Vec<PendingAppendConflictingChild>,
        Vec<PendingAppendDeletedChild>,
    ),
    RecoveryError,
> {
    let entries = redo_log.recover()?;
    recover_entries_with_allocator_collecting_pending_conflicts(
        device,
        entries,
        index,
        dah,
        unmined,
        allocator,
        None,
        // Conservative default: callers of this entry point do not signal
        // secondary cleanliness, so re-derive from a full scan.
        SecondaryReconcile::FullScan,
    )
}

/// Full recovery with durable progress markers written every
/// `RECOVERY_PROGRESS_INTERVAL_ENTRIES` safely processed entries and once
/// at the end of the recovered range.
///
/// `full_secondary_rebuild` (B-7): when `true` the DAH / unmined
/// secondaries are re-derived by scanning the entire primary index
/// (O(store size)); pass this when a secondary backend was not cleanly
/// closed or its snapshot section is missing. When `false` — the common
/// crash-of-a-clean-store case — only the keys the redo log touched are
/// reconciled against the durable secondaries (O(redo size)), so boot
/// time is bounded by the redo log, not the store.
pub fn recover_all_with_allocator_collecting_pending_conflicts_progress(
    device: &dyn BlockDevice,
    redo_log: &mut RedoLog,
    index: &ShardedIndex,
    dah: &mut DahBackend,
    unmined: &mut UnminedBackend,
    allocator: Option<&mut SlotAllocator>,
    full_secondary_rebuild: bool,
) -> Result<
    (
        RecoveryStats,
        Vec<PendingAppendConflictingChild>,
        Vec<PendingAppendDeletedChild>,
    ),
    RecoveryError,
> {
    let entries = redo_log.recover()?;
    let secondary_reconcile = if full_secondary_rebuild {
        SecondaryReconcile::FullScan
    } else {
        SecondaryReconcile::TouchedOnly
    };
    recover_entries_with_allocator_collecting_pending_conflicts(
        device,
        entries,
        index,
        dah,
        unmined,
        allocator,
        Some((redo_log, RECOVERY_PROGRESS_INTERVAL_ENTRIES)),
        secondary_reconcile,
    )
}

/// Determine which store a redo entry belongs to.
///
/// - Create / AllocateRegion / FreeRegion carry their own `device_id`.
/// - Ops keyed by a txid route to the store recorded for that key — first from
///   `owner` (records created in THIS log, pre-replay) then the loaded index
///   (records created before the snapshot). Defaults to store 0 (also the
///   single-store and legacy `ReplicaCreate` case).
fn entry_store_id(
    op: &RedoOp,
    owner: &std::collections::HashMap<TxKey, u8>,
    index: &ShardedIndex,
) -> u8 {
    match op {
        RedoOp::Create { device_id, .. }
        | RedoOp::AllocateRegion { device_id, .. }
        | RedoOp::FreeRegion { device_id, .. } => *device_id,
        other => match other.tx_key() {
            Some(k) => owner
                .get(k)
                .copied()
                .or_else(|| index.lookup(k).map(|e| e.device_id))
                .unwrap_or(0),
            None => 0,
        },
    }
}

/// Partition a single shared redo log's entries by owning store, preserving
/// per-store log order. Each store's slice is then recovered independently by
/// the existing single-store replay (the durability-critical path is unchanged;
/// stores are independent because a record lives entirely on one store).
fn partition_entries_by_store(
    entries: Vec<RedoEntry>,
    num_stores: usize,
    index: &ShardedIndex,
) -> Vec<Vec<RedoEntry>> {
    // Pass 1: key -> device_id for records created within this log.
    let mut owner: std::collections::HashMap<TxKey, u8> = std::collections::HashMap::new();
    for e in &entries {
        if let RedoOp::Create {
            tx_key, device_id, ..
        } = &e.op
        {
            owner.insert(*tx_key, *device_id);
        }
    }
    // Pass 2: route each entry to its store (clamped defensively in range).
    let mut parts: Vec<Vec<RedoEntry>> = (0..num_stores).map(|_| Vec::new()).collect();
    for e in entries {
        let store = (entry_store_id(&e.op, &owner, index) as usize).min(num_stores - 1);
        parts[store].push(e);
    }
    parts
}

/// Multi-store recovery: replay one shared redo log across N stores.
///
/// Partitions the log by owning store (see [`partition_entries_by_store`]) and
/// runs the existing single-store replay once per store with that store's
/// device + allocator, sharing the index / DAH / unmined backends. Per-store
/// pending conflicting/deleted-child drains and stats are merged. The
/// single-store path (`num_stores == 1`) is exactly the prior behaviour.
#[allow(clippy::too_many_arguments)]
pub fn recover_all_multi_store(
    devices: &[std::sync::Arc<dyn BlockDevice>],
    allocators: &mut [SlotAllocator],
    redo_log: &mut RedoLog,
    index: &ShardedIndex,
    dah: &mut DahBackend,
    unmined: &mut UnminedBackend,
    full_secondary_rebuild: bool,
) -> Result<
    (
        RecoveryStats,
        Vec<PendingAppendConflictingChild>,
        Vec<PendingAppendDeletedChild>,
    ),
    RecoveryError,
> {
    assert_eq!(
        devices.len(),
        allocators.len(),
        "devices and allocators must be 1:1 per store"
    );
    let entries = redo_log.recover()?;
    let partitions = partition_entries_by_store(entries, devices.len(), index);
    let reconcile = if full_secondary_rebuild {
        SecondaryReconcile::FullScan
    } else {
        SecondaryReconcile::TouchedOnly
    };
    let mut total = RecoveryStats::default();
    let mut pending_cc = Vec::new();
    let mut pending_dc = Vec::new();
    for (store_id, part) in partitions.into_iter().enumerate() {
        let (stats, cc, dc) = recover_entries_with_allocator_collecting_pending_conflicts(
            &*devices[store_id],
            part,
            index,
            dah,
            unmined,
            Some(&mut allocators[store_id]),
            None,
            reconcile,
        )?;
        total.merge(stats);
        pending_cc.extend(cc);
        pending_dc.extend(dc);
    }
    Ok((total, pending_cc, pending_dc))
}

// Each argument is a distinct recovery input (device, the three index
// backends, optional allocator, optional redo-progress fence, and the
// secondary-reconcile mode); they have independent lifetimes/mutability and do
// not form a natural cohesive struct, so the count is warranted here.
#[allow(clippy::too_many_arguments)]
fn recover_entries_with_allocator_collecting_pending_conflicts(
    device: &dyn BlockDevice,
    entries: Vec<RedoEntry>,
    index: &ShardedIndex,
    dah: &mut DahBackend,
    unmined: &mut UnminedBackend,
    mut allocator: Option<&mut SlotAllocator>,
    mut progress: Option<(&mut RedoLog, u64)>,
    secondary_reconcile: SecondaryReconcile,
) -> Result<
    (
        RecoveryStats,
        Vec<PendingAppendConflictingChild>,
        Vec<PendingAppendDeletedChild>,
    ),
    RecoveryError,
> {
    let mut stats = RecoveryStats::default();
    let mut pending_conflicting_children = Vec::new();
    let mut pending_deleted_children: Vec<PendingAppendDeletedChild> = Vec::new();
    let mut processed_since_progress = 0u64;
    let mut last_progress_sequence = 0u64;
    let mut last_safe_sequence = 0u64;
    // BUG-1 offset-uniqueness: build the offset→owner reverse map ONCE from
    // the loaded index (O(N)). Each replayed create then evicts any
    // pre-existing alias in O(1) via `register_unique_offset` instead of
    // re-scanning the entire index per create.
    let mut offset_owners = build_offset_owners(index);
    // B-7: keys touched by replayed entries. On a clean recovery only
    // these are reconciled against the durable secondaries, instead of
    // re-scanning the whole primary index.
    let mut touched_keys: std::collections::HashSet<TxKey> = std::collections::HashSet::new();

    // Track pending hash-table-resize intents by capacity. A Begin adds an
    // entry; a matching Commit removes it. After the replay loop, any
    // remaining Begin indicates a partial resize whose tmp file must be
    // removed (the primary index file itself is untouched until rename).
    let mut pending_resizes: std::collections::HashMap<u64, Vec<u8>> =
        std::collections::HashMap::new();

    for entry in &entries {
        // B-7: record every key the redo log touches so a clean recovery
        // can reconcile just these against the durable secondaries.
        if let Some(key) = entry.op.tx_key() {
            touched_keys.insert(*key);
        }
        // Height subsystem (design §4; BUG3): fold the max block height across
        // height-bearing entries — BEFORE the replay outcome, since a skipped
        // (already-applied) entry still proves the height was durably seen.
        if let Some(h) = entry.op.observed_block_height() {
            stats.max_observed_block_height = stats.max_observed_block_height.max(h);
        }
        let outcome = match &entry.op {
            RedoOp::SecondaryUnminedUpdate {
                tx_key,
                old_height,
                new_height,
            } => replay_secondary_unmined(device, index, unmined, tx_key, *old_height, *new_height),
            RedoOp::SecondaryDahUpdate {
                tx_key,
                old_height,
                new_height,
            } => replay_secondary_dah(device, index, dah, tx_key, *old_height, *new_height),
            RedoOp::AppendConflictingChild {
                parent_key,
                child_txid,
            } => {
                pending_conflicting_children.push(PendingAppendConflictingChild {
                    parent_key: *parent_key,
                    child_txid: *child_txid,
                    is_remove: false,
                });
                ReplayResult::Skipped
            }
            RedoOp::RemoveConflictingChild {
                parent_key,
                child_txid,
            } => {
                // Same deferred-drain model as the append: the engine applies
                // it post-construction via `remove_conflicting_child`. Order is
                // preserved (log order), and both ops are idempotent.
                pending_conflicting_children.push(PendingAppendConflictingChild {
                    parent_key: *parent_key,
                    child_txid: *child_txid,
                    is_remove: true,
                });
                ReplayResult::Skipped
            }
            // AUDIT M2.6 — collect deleted-child appends for the same
            // post-engine deferred drain as conflicting children. Drained in
            // log order via `Engine::append_deleted_child` (idempotent).
            RedoOp::AppendDeletedChild {
                parent_key,
                child_txid,
            } => {
                pending_deleted_children.push(PendingAppendDeletedChild {
                    parent_key: *parent_key,
                    child_txid: *child_txid,
                });
                ReplayResult::Skipped
            }
            RedoOp::AllocateRegion { .. } | RedoOp::FreeRegion { .. } => {
                match allocator.as_deref_mut() {
                    Some(alloc) => {
                        if alloc.replay_redo(&entry.op) {
                            ReplayResult::Applied
                        } else {
                            ReplayResult::Skipped
                        }
                    }
                    None => ReplayResult::Skipped,
                }
            }
            RedoOp::Delete {
                tx_key,
                record_offset,
                record_size,
            } => {
                let delete_outcome =
                    replay_delete(device, index, tx_key, *record_offset, *record_size);
                if matches!(delete_outcome, ReplayResult::Failed(_)) {
                    delete_outcome
                } else if *record_offset != 0 && *record_size != 0 {
                    match allocator.as_deref_mut() {
                        Some(alloc) => {
                            let free = RedoOp::FreeRegion {
                                offset: *record_offset,
                                size: *record_size,
                                device_id: 0,
                            };
                            if alloc.replay_redo(&free)
                                || matches!(delete_outcome, ReplayResult::Applied)
                            {
                                ReplayResult::Applied
                            } else {
                                ReplayResult::Skipped
                            }
                        }
                        None => delete_outcome,
                    }
                } else {
                    delete_outcome
                }
            }
            RedoOp::HashtableResizeBegin {
                tmp_path_bytes,
                new_capacity,
            } => {
                pending_resizes.insert(*new_capacity, tmp_path_bytes.clone());
                ReplayResult::Applied
            }
            RedoOp::HashtableResizeCommit { new_capacity } => {
                // Matching Begin → resize is durable, nothing to clean up.
                pending_resizes.remove(new_capacity);
                ReplayResult::Applied
            }
            RedoOp::Create {
                record_offset,
                record_bytes,
                ..
            } => {
                if let Some(alloc) = allocator.as_deref()
                    && !alloc.is_allocated_range(*record_offset, record_bytes.len() as u64)
                {
                    ReplayResult::Failed(ReplayCause::LogicError)
                } else {
                    replay_entry(device, index, &mut offset_owners, entry)
                }
            }
            // BUG-1 fix #1: route the legacy `RedoOp::ReplicaCreate` through the
            // SAME `is_allocated_range` gate as `Create`. The legacy
            // create carries no payload, so the range length is derived
            // from `utxo_count` via `record_size_for`. Without this gate a
            // stale legacy Create — whose `record_offset` was since freed
            // and re-handed to a DIFFERENT record — would register an index
            // entry aliasing another key's record, corrupting reads. The
            // replication / migration receiver emits this legacy op for
            // every replicated create, so every replica replays through
            // this arm on recovery; it must be guarded exactly like V2.
            RedoOp::ReplicaCreate {
                tx_key,
                record_offset,
                utxo_count,
            } => {
                let range_len = TxMetadata::record_size_for(*utxo_count);
                if let Some(alloc) = allocator.as_deref()
                    && !alloc.is_allocated_range(*record_offset, range_len)
                {
                    ReplayResult::Failed(ReplayCause::LogicError)
                } else {
                    replay_replica_create(
                        device,
                        index,
                        &mut offset_owners,
                        tx_key,
                        *record_offset,
                        *utxo_count,
                    )
                }
            }
            RedoOp::CompensateUnsetMined {
                tx_key,
                block_id,
                block_height,
                subtree_idx,
            } => replay_compensate_unset_mined_with_allocator(
                device,
                index,
                allocator.as_deref_mut(),
                tx_key,
                *block_id,
                *block_height,
                *subtree_idx,
            ),
            // SetMined may need the overflow region (4th+ block entry,
            // or unset of an overflow-resident entry) — route through
            // the allocator-aware replay so it can allocate/free it.
            RedoOp::SetMined {
                tx_key,
                block_id,
                block_height,
                subtree_idx,
                unset,
            } => replay_set_mined_with_allocator(
                device,
                index,
                allocator.as_deref_mut(),
                tx_key,
                *block_id,
                *block_height,
                *subtree_idx,
                *unset,
            ),
            _ => replay_entry(device, index, &mut offset_owners, entry),
        };
        let progress_safe = matches!(
            outcome,
            ReplayResult::Applied
                | ReplayResult::Skipped
                | ReplayResult::Failed(ReplayCause::MissingPrimary)
        );
        // F-G4-007: capture cause BEFORE we move `outcome` into the
        // match below, so the post-match break can use it.
        let fatal = matches!(
            outcome,
            ReplayResult::Failed(c) if is_fatal_replay_cause(c)
        );
        match outcome {
            ReplayResult::Applied => stats.entries_replayed += 1,
            ReplayResult::Skipped => stats.entries_skipped += 1,
            ReplayResult::Failed(cause) => stats.record_failure(cause),
        }
        if progress_safe {
            last_safe_sequence = entry.sequence;
            processed_since_progress = processed_since_progress.saturating_add(1);
            if let Some((log, interval)) = progress.as_mut()
                && *interval > 0
                && processed_since_progress >= *interval
            {
                mark_recovery_progress_non_fatal(log, entry.sequence)?;
                last_progress_sequence = entry.sequence;
                processed_since_progress = 0;
            }
        }
        // F-G4-007: stop on first non-tolerable failure so subsequent
        // entries cannot land partially-applied state on top of an
        // already-broken intermediate replay.
        if fatal {
            break;
        }
    }

    if let Some((log, _)) = progress.as_mut()
        && last_safe_sequence > last_progress_sequence
    {
        mark_recovery_progress_non_fatal(log, last_safe_sequence)?;
    }

    match secondary_reconcile {
        SecondaryReconcile::FullScan => {
            reconcile_secondary_indexes_from_metadata(device, index, dah, unmined)?;
        }
        SecondaryReconcile::TouchedOnly => {
            reconcile_secondary_indexes_for_keys(device, index, dah, unmined, &touched_keys)?;
        }
    }

    // Clean up any orphan tmp files from resizes that started but never
    // committed. The original index file is intact (rename is atomic and
    // only happens after the tmp write completes), so removing the tmp
    // file is safe and the server will re-attempt the resize on the next
    // load-factor trigger.
    for (_capacity, tmp_bytes) in pending_resizes {
        let tmp_path = path_from_bytes(&tmp_bytes);
        if tmp_path.exists() {
            if let Err(e) = std::fs::remove_file(&tmp_path) {
                tracing::warn!(
                    tmp_path = %tmp_path.display(),
                    err = %e,
                    "recovery: failed to remove orphan resize tmp file",
                );
            } else {
                tracing::info!(
                    tmp_path = %tmp_path.display(),
                    "recovery: removed orphan resize tmp file",
                );
            }
        }
    }

    // Persist the allocator snapshot so next startup can skip replay of the
    // allocator redo entries. The index and secondary indexes are persisted
    // through their own paths (snapshot on shutdown / per-op redb commit).
    if let Some(alloc) = allocator {
        // Failure here is non-fatal for recovery — the next startup will
        // simply replay the same entries again, which is idempotent. But
        // it is NOT silent: surface the error so operators can spot
        // chronic snapshot-persist failures (disk full, permission drift)
        // before they compound into a multi-second replay on next boot.
        if let Err(err) = alloc.persist() {
            if let Some(m) = crate::metrics::allocator_metrics() {
                m.snapshot_persist_failures_total.inc();
            }
            tracing::warn!(
                target: "teraslab::recovery::allocator",
                error = %err,
                "recovery: allocator snapshot persist failed — next startup will replay allocator redo entries (idempotent, but a sustained climb in `allocator_snapshot_persist_failures_total` indicates a real disk/permission problem)"
            );
        }
    }

    Ok((
        stats,
        pending_conflicting_children,
        pending_deleted_children,
    ))
}

fn reconcile_secondary_indexes_from_metadata(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    dah: &mut DahBackend,
    unmined: &mut UnminedBackend,
) -> Result<(), RecoveryError> {
    // F-G3-002: propagate failures from the redb drop+recreate so we do not
    // start the reconcile loop over a half-cleared table whose cached count
    // disagrees with the on-disk rows.
    dah.clear().map_err(RecoveryError::Index)?;
    unmined.clear().map_err(RecoveryError::Index)?;

    // Collect results from the for_each pass — for_each takes a FnMut closure
    // that cannot return errors, so we accumulate failures and surface the first.
    let mut first_error: Option<RecoveryError> = None;
    let mut dah_pairs: Vec<(u32, TxKey)> = Vec::new();
    let mut unmined_pairs: Vec<(u32, TxKey)> = Vec::new();

    index.for_each(|key, entry| {
        if first_error.is_some() {
            return;
        }
        match io::read_metadata(device, entry.record_offset) {
            Ok(meta) => {
                let dah_height = { meta.delete_at_height };
                if dah_height != 0 {
                    dah_pairs.push((dah_height, key));
                }
                let unmined_height = { meta.unmined_since };
                if unmined_height != 0 {
                    unmined_pairs.push((unmined_height, key));
                }
            }
            Err(_) => {
                first_error = Some(RecoveryError::Index(
                    crate::index::IndexError::FormatError {
                        detail: format!(
                            "secondary reconcile failed to read metadata for {:?}",
                            key.txid
                        ),
                    },
                ));
            }
        }
    });

    if let Some(e) = first_error {
        return Err(e);
    }

    for (height, key) in dah_pairs {
        dah.insert(height, key, None)?;
    }
    for (height, key) in unmined_pairs {
        unmined.insert(height, key, None)?;
    }
    Ok(())
}

/// B-7: reconcile the DAH / unmined secondaries for ONLY the keys the
/// redo replay touched, against the durable (crash-safe) secondary state.
///
/// This is the O(redo) counterpart to
/// [`reconcile_secondary_indexes_from_metadata`]'s O(store) full scan. It
/// is sound only when the secondaries were loaded clean (they already
/// reflect every key the redo log did NOT touch); the touched keys are
/// the only ones whose secondary state may be stale w.r.t. the just
/// replayed primary metadata.
///
/// For each touched key the primary index is authoritative: if the record
/// is gone, any secondary entry for it is removed; otherwise the
/// secondaries are set to exactly the record's `delete_at_height` /
/// `unmined_since` (removing first so a height *change* does not leave a
/// stale entry under the old height bucket).
fn reconcile_secondary_indexes_for_keys(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    dah: &mut DahBackend,
    unmined: &mut UnminedBackend,
    keys: &std::collections::HashSet<TxKey>,
) -> Result<(), RecoveryError> {
    for key in keys {
        let entry = match index.lookup(key) {
            Some(e) => e,
            None => {
                // Record no longer exists — drop any stale secondary
                // entries keyed on it.
                dah.remove(key, None).map_err(RecoveryError::Index)?;
                unmined.remove(key, None).map_err(RecoveryError::Index)?;
                continue;
            }
        };
        let meta = match io::read_metadata(device, entry.record_offset) {
            Ok(meta) => meta,
            Err(_) => {
                return Err(RecoveryError::Index(
                    crate::index::IndexError::FormatError {
                        detail: format!(
                            "secondary reconcile failed to read metadata for {:?}",
                            key.txid
                        ),
                    },
                ));
            }
        };
        // Remove first so a changed height does not leave a stale entry
        // under the previous bucket, then re-insert the current value.
        dah.remove(key, None).map_err(RecoveryError::Index)?;
        unmined.remove(key, None).map_err(RecoveryError::Index)?;
        let dah_height = { meta.delete_at_height };
        if dah_height != 0 {
            dah.insert(dah_height, *key, None)?;
        }
        let unmined_height = { meta.unmined_since };
        if unmined_height != 0 {
            unmined.insert(unmined_height, *key, None)?;
        }
    }
    Ok(())
}

/// Reconcile the external blob store against the primary index after
/// recovery has finished replaying the redo log (R-049).
///
/// Walks every blob returned by [`BlobStore::list`] and deletes any blob
/// whose primary-index entry is absent OR present without
/// [`crate::record::TxFlags::EXTERNAL`]. Both signal an orphan from a failed
/// create / aborted upload / cancelled migration: the foreground pipeline
/// will never reference the blob again, so leaving it on disk would leak
/// inodes forever.
///
/// Call this from startup AFTER [`recover_all_with_allocator`] returns
/// successfully and BEFORE accepting client connections — the reconciliation
/// is race-free at that point because no concurrent dispatch can write a new
/// blob whose registration has not yet landed.
///
/// Errors from the underlying blob enumeration are surfaced; per-blob delete
/// failures are logged at warn and counted in `delete_failed`.
pub fn reconcile_blobs_after_recovery(
    blob_store: &dyn BlobStore,
    index: &ShardedIndex,
) -> Result<BlobGcStats, BlobError> {
    let started = std::time::Instant::now();
    let stats = blob_gc::reconcile_orphan_blobs_against_index(blob_store, index)?;
    tracing::info!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        total_blobs = stats.total_blobs,
        kept = stats.kept,
        deleted_no_index = stats.deleted_no_index,
        deleted_not_external = stats.deleted_not_external,
        delete_failed = stats.delete_failed,
        "recovery: blob-store reconciliation complete",
    );
    Ok(stats)
}

/// Rebuild a filesystem path from raw bytes captured in a
/// [`RedoOp::HashtableResizeBegin`] entry. On Unix the bytes are the raw
/// `OsStr` (POSIX paths are not guaranteed UTF-8). On non-Unix platforms
/// we fall back to `String::from_utf8_lossy` (this server targets
/// Linux/Unix).
fn path_from_bytes(bytes: &[u8]) -> std::path::PathBuf {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        std::path::PathBuf::from(std::ffi::OsStr::from_bytes(bytes))
    }
    #[cfg(not(unix))]
    {
        std::path::PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
    }
}

/// Reconcile the unmined secondary index with a redo intent record.
///
/// Idempotency rule: the secondary update is applied only when the
/// secondary's current state does not already match the redo's `new_height`.
/// The primary index's authoritative `unmined_since` is used as the
/// ground-truth reference — if the redo record's `new_height` does not
/// match the primary, the redo entry is stale (primary moved on) and we
/// skip the secondary update entirely.
fn replay_secondary_unmined(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    unmined: &mut UnminedBackend,
    tx_key: &TxKey,
    _old_height: u32,
    new_height: u32,
) -> ReplayResult {
    // The on-device record is authoritative here. R-077: after a crash
    // between the primary metadata write and a primary-index/redb cache
    // commit, the cached TxIndexEntry can be stale while the record already
    // contains the redo target value.
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        None => return ReplayResult::Skipped,
    };
    let primary_unmined = match io::read_metadata(device, ie.record_offset) {
        Ok(meta) => meta.unmined_since,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };
    if primary_unmined != new_height {
        // Redo is stale relative to primary — a later redo has already
        // superseded this one. Skip.
        return ReplayResult::Skipped;
    }
    let entry = UnminedRedoEntry {
        txid: tx_key.txid,
        old_height: _old_height,
        new_height,
    };
    match unmined.replay_redo(&entry) {
        Ok(()) => ReplayResult::Applied,
        // Secondary backend's `replay_redo` returned `Err`. The primary
        // lookup already succeeded (so this isn't a missing-primary case),
        // and the redo entry passed parsing — anything left is a
        // logic-level inconsistency at the secondary backend.
        Err(_) => ReplayResult::Failed(ReplayCause::LogicError),
    }
}

fn replay_secondary_dah(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    dah: &mut DahBackend,
    tx_key: &TxKey,
    old_height: u32,
    new_height: u32,
) -> ReplayResult {
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        None => return ReplayResult::Skipped,
    };
    let primary_dah = match io::read_metadata(device, ie.record_offset) {
        Ok(meta) => meta.delete_at_height,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };
    if primary_dah != new_height {
        return ReplayResult::Skipped;
    }
    let entry = DahRedoEntry {
        txid: tx_key.txid,
        old_height,
        new_height,
    };
    match dah.replay_redo(&entry) {
        Ok(()) => ReplayResult::Applied,
        // Same reasoning as `replay_secondary_unmined`: a backend error
        // after a successful primary lookup is a logic-level failure.
        Err(_) => ReplayResult::Failed(ReplayCause::LogicError),
    }
}

/// Outcome of replaying a single redo entry.
///
/// - `Applied`: the entry's effect was written to the device (or index).
/// - `Skipped`: the entry was idempotent against current state, or
///   pointed to a record that was concurrently deleted between the
///   redo append and recovery (a benign, non-fatal condition).
/// - `Failed(cause)`: replay could not proceed; `cause` carries the
///   classification used by the startup tolerance policy. See
///   [`ReplayCause`] for the per-cause semantics.
#[derive(Debug)]
enum ReplayResult {
    Applied,
    Skipped,
    Failed(ReplayCause),
}

#[derive(Debug, Clone, Copy)]
struct ReplayDerivedContext {
    current_block_height: u32,
    block_height_retention: u32,
    target_generation: u32,
    updated_at: u64,
}

/// Count the SPENT slots of a record by reading its on-device slot set.
///
/// This is the authoritative source for `spent_utxos`: the counter is, by
/// definition, the number of slots in `UTXO_SPENT` status. Recomputing it
/// from the slots (rather than accumulating `±1` per replayed redo entry)
/// is what makes Spend/Unspend replay idempotent — replaying an already
/// applied prefix, or replaying the whole log twice, converges to the same
/// counter because the slot states are absolute, not incremental. A drifted
/// on-device counter (e.g. from a spend→unspend→respend reorg history where
/// some entries were already applied before the crash) is corrected here
/// rather than perpetuated.
///
/// `utxo_count` is the record's slot count (from the metadata header).
/// Returns `Err(())` on any device read error so the caller can map it to
/// [`ReplayCause::IoError`].
fn count_spent_slots(
    device: &dyn BlockDevice,
    record_offset: u64,
    utxo_count: u32,
) -> Result<u32, ()> {
    let slots = io::read_all_utxo_slots(device, record_offset, utxo_count).map_err(|_| ())?;
    let spent = slots.iter().filter(|s| s.status == UTXO_SPENT).count();
    Ok(spent as u32)
}

fn apply_replay_dah_patch(metadata: &mut TxMetadata, patch: &DahPatch) {
    metadata.delete_at_height = patch.new_delete_at_height;
    if patch.last_spent_all {
        metadata.flags |= TxFlags::LAST_SPENT_ALL;
    } else {
        // F-G4-015: use the idiomatic bitflags clear pattern.
        metadata.flags.remove(TxFlags::LAST_SPENT_ALL);
    }
}

fn replay_entry(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    offset_owners: &mut OffsetOwners,
    entry: &RedoEntry,
) -> ReplayResult {
    match &entry.op {
        RedoOp::Spend {
            tx_key,
            offset,
            spending_data,
            new_spent_count,
        } => replay_spend(
            device,
            index,
            tx_key,
            *offset,
            spending_data,
            *new_spent_count,
            None,
            None,
        ),
        RedoOp::SpendV2 {
            tx_key,
            offset,
            spending_data,
            new_spent_count,
            current_block_height,
            block_height_retention,
            target_generation,
            updated_at,
            utxo_hash,
        } => replay_spend(
            device,
            index,
            tx_key,
            *offset,
            spending_data,
            *new_spent_count,
            Some(ReplayDerivedContext {
                current_block_height: *current_block_height,
                block_height_retention: *block_height_retention,
                target_generation: *target_generation,
                updated_at: *updated_at,
            }),
            utxo_hash.as_ref(),
        ),
        RedoOp::Unspend {
            tx_key,
            offset,
            spending_data,
            new_spent_count,
        } => replay_unspend(
            device,
            index,
            tx_key,
            *offset,
            spending_data.as_ref(),
            *new_spent_count,
            None,
            None,
        ),
        RedoOp::UnspendV2 {
            tx_key,
            offset,
            spending_data,
            new_spent_count,
            current_block_height,
            block_height_retention,
            target_generation,
            updated_at,
            utxo_hash,
        } => replay_unspend(
            device,
            index,
            tx_key,
            *offset,
            Some(spending_data),
            *new_spent_count,
            Some(ReplayDerivedContext {
                current_block_height: *current_block_height,
                block_height_retention: *block_height_retention,
                target_generation: *target_generation,
                updated_at: *updated_at,
            }),
            utxo_hash.as_ref(),
        ),
        RedoOp::SetMined {
            tx_key,
            block_id,
            block_height,
            subtree_idx,
            unset,
        } => replay_set_mined(
            device,
            index,
            tx_key,
            *block_id,
            *block_height,
            *subtree_idx,
            *unset,
        ),
        RedoOp::Freeze { tx_key, offset } => replay_freeze(device, index, tx_key, *offset, None),
        RedoOp::FreezeV2 {
            tx_key,
            offset,
            utxo_hash,
        } => replay_freeze(device, index, tx_key, *offset, Some(utxo_hash)),
        RedoOp::Unfreeze { tx_key, offset } => {
            replay_unfreeze(device, index, tx_key, *offset, None)
        }
        RedoOp::UnfreezeV2 {
            tx_key,
            offset,
            utxo_hash,
        } => replay_unfreeze(device, index, tx_key, *offset, Some(utxo_hash)),
        RedoOp::ReplicaCreate {
            tx_key,
            record_offset,
            utxo_count,
        } => replay_replica_create(
            device,
            index,
            offset_owners,
            tx_key,
            *record_offset,
            *utxo_count,
        ),
        RedoOp::Create {
            tx_key,
            // The new record's store: replay reconstructs it on this store's
            // device (the `device` passed here is already that store's, routed
            // by partition_entries_by_store) and stamps the index entry's
            // device_id so reads route back correctly. 0 in single-store.
            device_id,
            record_offset,
            utxo_count,
            is_conflicting,
            record_bytes,
            parent_txids,
        } => replay_create(
            device,
            *device_id,
            index,
            offset_owners,
            tx_key,
            *record_offset,
            *utxo_count,
            *is_conflicting,
            record_bytes,
            parent_txids,
        ),
        RedoOp::Delete {
            tx_key,
            record_offset,
            record_size,
        } => replay_delete(device, index, tx_key, *record_offset, *record_size),
        RedoOp::AppendConflictingChild { .. } => ReplayResult::Skipped,
        RedoOp::RemoveConflictingChild { .. } => ReplayResult::Skipped,
        // F-X-022: `AppendDeletedChild` is audit/diagnostic + defense-in-depth
        // at the idempotent-respend short-circuit. The primary spend-rejection
        // path is the slot's `UTXO_PRUNED` status, which the
        // `PruneSlotIfSpentBy` redo entry (logically prior) already replays.
        // A crash between the prune and the deleted-child append loses only
        // the audit/diagnostic information for the lost append — the spend
        // still gets rejected via UTXO_PRUNED. Future work: drain pending
        // appends after engine construction the same way
        // `AppendConflictingChild` is drained today.
        RedoOp::AppendDeletedChild { .. } => ReplayResult::Skipped,
        RedoOp::Checkpoint | RedoOp::RecoveryProgress { .. } => ReplayResult::Skipped,
        // SecondaryUnminedUpdate / SecondaryDahUpdate are durability-intent
        // records for redb secondary indexes — the primary index has no
        // state to reconcile from them. `recover_all` handles them via the
        // secondary backends; the single-backend `recover` path skips.
        RedoOp::SecondaryUnminedUpdate { .. } | RedoOp::SecondaryDahUpdate { .. } => {
            ReplayResult::Skipped
        }
        // AllocateRegion / FreeRegion are allocator-scoped records. The
        // single-backend `recover` path has no allocator handle — skip
        // here and rely on `recover_all_with_allocator` to process them.
        RedoOp::AllocateRegion { .. } | RedoOp::FreeRegion { .. } => ReplayResult::Skipped,
        // HashtableResizeBegin / HashtableResizeCommit are file-backed
        // index durability records handled by `recover_all_with_allocator`
        // (which tracks the pending-resize set and cleans up orphan tmp
        // files after replay). The single-backend `recover` path treats
        // them as no-ops.
        RedoOp::HashtableResizeBegin { .. } | RedoOp::HashtableResizeCommit { .. } => {
            ReplayResult::Skipped
        }
        // Gap #8: compensation intents recorded mid-rollback. Replay
        // restores the captured pre-apply state. Replay is idempotent —
        // each handler reads the current device state and skips when it
        // already matches the captured before-image.
        RedoOp::CompensateUnsetMined {
            tx_key,
            block_id,
            block_height,
            subtree_idx,
        } => replay_compensate_unset_mined(
            device,
            index,
            tx_key,
            *block_id,
            *block_height,
            *subtree_idx,
        ),
        RedoOp::CompensateReassign {
            tx_key,
            offset,
            prior_utxo_hash,
        } => replay_compensate_reassign(device, index, tx_key, *offset, prior_utxo_hash),
        RedoOp::CompensatePrune {
            tx_key,
            offset,
            prior_status,
        } => replay_compensate_prune(device, index, tx_key, *offset, *prior_status),
        RedoOp::CompensateSetLocked {
            tx_key,
            prior_locked,
            prior_delete_at_height,
        } => replay_compensate_set_locked(
            device,
            index,
            tx_key,
            *prior_locked,
            *prior_delete_at_height,
        ),
        // Remaining ops (Reassign, PruneSlot, SetConflicting, SetLocked,
        // PreserveUntil, MarkOnLongestChain) are metadata-only writes.
        // They're idempotent: the metadata pwrite is atomic at the block
        // level. If it completed, the data is there. If not, we re-apply.
        _ => replay_metadata_op(device, index, entry),
    }
}

// Hot per-entry replay path: each argument maps directly to a field decoded
// from the spend redo entry (key, offset, spending data, counts, derived
// context, utxo hash). Grouping them into a struct would just add a copy on a
// performance-sensitive path without improving clarity, so the count stands.
#[allow(clippy::too_many_arguments)]
fn replay_spend(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    tx_key: &TxKey,
    offset: u32,
    spending_data: &[u8; 36],
    _new_spent_count: u32,
    derived: Option<ReplayDerivedContext>,
    utxo_hash: Option<&[u8; 32]>,
) -> ReplayResult {
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
    };

    // B-5: a CRC-failing (torn) slot in the WAL window is exactly the
    // artifact this redo entry exists to cover. If the V3 entry carries
    // the slot's `utxo_hash`, rebuild the slot from the durable intent
    // instead of fail-closed-bricking the node (boot loop). A
    // non-corruption device I/O error still fails — that is not something
    // the WAL can repair.
    let read = match io::read_utxo_slot(device, ie.record_offset, offset) {
        Ok(s) => Some(s),
        Err(DeviceError::RecordCorruption { .. }) => None,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };

    // Determine the slot hash to write. On a healthy slot it is the
    // slot's existing hash; on a CRC-failing slot it comes from the V3
    // redo entry (slot rebuilt from the durable intent).
    let hash = match read {
        Some(slot) => {
            // Idempotent check: already spent with same data?
            if slot.status == UTXO_SPENT && slot.spending_data == *spending_data {
                return ReplayResult::Skipped;
            }
            slot.hash
        }
        None => match utxo_hash {
            // Reconstruct the spent slot directly from the redo entry.
            Some(h) => *h,
            // Legacy V2/V1 entry without the hash: unrepairable here.
            // Fail closed so the operator can run the repair CLI.
            None => return ReplayResult::Failed(ReplayCause::IoError),
        },
    };

    // Apply: write spent slot
    let new_slot = UtxoSlot::new_spent(hash, *spending_data);
    if io::write_utxo_slot(device, ie.record_offset, offset, &new_slot).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }

    // R-010 (BC-04) / B-4: re-derive the counter from on-device state by
    // RECOMPUTING it from the slots rather than overwriting with
    // `new_spent_count` or accumulating `±1` per entry. The dispatcher
    // computes `new_spent_count` from `engine.lookup` BEFORE taking the
    // per-tx stripe lock, so two concurrent spend/unspend batches on the
    // same record can compute conflicting absolute counts — so the redo
    // snapshot can't be trusted. The previous fix incremented by `+1`,
    // but that is NOT idempotent across spend→unspend→respend (reorg)
    // histories: replaying a prefix already reflected on-device
    // double-counts and drifts the counter `+1` per cycle, which can
    // satisfy the all-spent condition and stamp `delete_at_height` on a
    // record that still has a live (unspent) UTXO. The counter is, by
    // definition, the number of SPENT slots; recomputing it from the
    // slots after writing the slot above makes replay converge to the
    // same value regardless of how much of the log was already applied.
    //
    // R-013 (A-06 / BC-12): metadata read AND write errors propagate as
    // ReplayResult::Failed. Pre-fix this used `if let Ok(mut meta)` and
    // `let _ = io::write_metadata(...)` which silently dropped both
    // failure modes — the spend was reported Applied but the on-device
    // counter never got updated. Replicas resyncing from the
    // generation watermark would then see counter divergence.
    let mut meta = match io::read_metadata(device, ie.record_offset) {
        Ok(m) => m,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };
    meta.spent_utxos = match count_spent_slots(device, ie.record_offset, meta.utxo_count) {
        Ok(c) => c,
        Err(()) => return ReplayResult::Failed(ReplayCause::IoError),
    };
    if let Some(ctx) = derived {
        meta.generation = ctx.target_generation;
        meta.updated_at = ctx.updated_at;
        let dah_patch = match evaluate_delete_at_height(
            &meta,
            ctx.current_block_height,
            ctx.block_height_retention,
        ) {
            Ok((_signal, patch)) => patch,
            Err(_) => return ReplayResult::Failed(ReplayCause::LogicError),
        };
        if let Some(ref patch) = dah_patch {
            apply_replay_dah_patch(&mut meta, patch);
        }
    }
    if io::write_metadata(device, ie.record_offset, &meta).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }

    ReplayResult::Applied
}

// Hot per-entry replay path: arguments mirror the fields decoded from the
// unspend redo entry (key, offset, expected spending data, counts, derived
// context, utxo hash). Same rationale as `replay_spend` — a struct adds a copy
// without clarifying intent.
#[allow(clippy::too_many_arguments)]
fn replay_unspend(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    tx_key: &TxKey,
    offset: u32,
    expected_spending_data: Option<&[u8; 36]>,
    _new_spent_count: u32,
    derived: Option<ReplayDerivedContext>,
    utxo_hash: Option<&[u8; 32]>,
) -> ReplayResult {
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
    };

    // B-5: a CRC-failing slot is rebuilt to UNSPENT from the V3 redo
    // entry's `utxo_hash` rather than fail-closed-bricking. A
    // non-corruption I/O error still fails.
    let read = match io::read_utxo_slot(device, ie.record_offset, offset) {
        Ok(s) => Some(s),
        Err(DeviceError::RecordCorruption { .. }) => None,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };

    let hash = match read {
        Some(slot) => {
            if slot.status == UTXO_UNSPENT {
                return ReplayResult::Skipped;
            }
            if slot.status != UTXO_SPENT {
                return ReplayResult::Skipped;
            }
            // F-A1: the live `engine.unspend` rejects a hash mismatch
            // (ERR_UTXO_HASH_MISMATCH) BEFORE mutating. Recovery must be
            // symmetric: a redo entry whose `utxo_hash` no longer matches the
            // on-disk slot identity is replaying an operation the master
            // reported as an error, so skip it rather than flipping the slot
            // to UNSPENT. Mirrors the `replay_freeze`/`replay_unfreeze` guard.
            if let Some(expected_hash) = utxo_hash
                && slot.hash != *expected_hash
            {
                return ReplayResult::Skipped;
            }
            if let Some(expected_spending_data) = expected_spending_data
                && slot.spending_data != *expected_spending_data
            {
                return ReplayResult::Skipped;
            }
            slot.hash
        }
        None => match utxo_hash {
            // Rebuild the slot's hash from the durable intent; the slot
            // is then written UNSPENT below.
            Some(h) => *h,
            None => return ReplayResult::Failed(ReplayCause::IoError),
        },
    };

    let new_slot = UtxoSlot::new_unspent(hash);
    if io::write_utxo_slot(device, ie.record_offset, offset, &new_slot).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }

    // R-010 (BC-04) / B-4: see `replay_spend` — recompute the counter from
    // the SPENT slots (after writing the unspent slot above) rather than
    // accumulating `-1`, so replay is idempotent across re-spend histories.
    // R-013: propagate read AND write errors instead of dropping them.
    let mut meta = match io::read_metadata(device, ie.record_offset) {
        Ok(m) => m,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };
    meta.spent_utxos = match count_spent_slots(device, ie.record_offset, meta.utxo_count) {
        Ok(c) => c,
        Err(()) => return ReplayResult::Failed(ReplayCause::IoError),
    };
    if let Some(ctx) = derived {
        meta.generation = ctx.target_generation;
        meta.updated_at = ctx.updated_at;
        let dah_patch = match evaluate_delete_at_height(
            &meta,
            ctx.current_block_height,
            ctx.block_height_retention,
        ) {
            Ok((_signal, patch)) => patch,
            Err(_) => return ReplayResult::Failed(ReplayCause::LogicError),
        };
        if let Some(ref patch) = dah_patch {
            apply_replay_dah_patch(&mut meta, patch);
        }
    }
    if io::write_metadata(device, ie.record_offset, &meta).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }

    ReplayResult::Applied
}

fn replay_set_mined(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    tx_key: &TxKey,
    block_id: u32,
    block_height: u32,
    subtree_idx: u32,
    unset: bool,
) -> ReplayResult {
    replay_set_mined_with_allocator(
        device,
        index,
        None,
        tx_key,
        block_id,
        block_height,
        subtree_idx,
        unset,
    )
}

/// Allocator-aware `SetMined` replay, mirroring the live `set_mined`
/// path's overflow handling: the 4th+ block entry spills to the
/// separately-allocated overflow region, dedup checks scan inline AND
/// overflow entries, and unset can remove an overflow-resident entry
/// (pulling the last overflow entry into a vacated inline slot, exactly
/// like `ops/engine.rs`).
///
/// Pre-fix the inline-only version silently dropped the 4th+ entry on
/// the append path (a crash in the WAL-to-device window lost block
/// entries past the inline cap on replay) and could not find
/// overflow-resident entries on the unset path. When overflow storage
/// must be touched but no allocator is available (the legacy
/// single-backend `recover` path), the entry fails closed with
/// `LogicError` instead of silently diverging — production startup
/// always supplies the allocator via `recover_all_with_allocator`.
// Per-entry replay path: arguments are the decoded set-mined fields (key,
// block id/height, subtree index, unset flag) plus the device, index, and
// optional allocator they act on. Independent inputs with no cohesive grouping,
// so the count is warranted.
#[allow(clippy::too_many_arguments)]
fn replay_set_mined_with_allocator(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    mut allocator: Option<&mut SlotAllocator>,
    tx_key: &TxKey,
    block_id: u32,
    block_height: u32,
    subtree_idx: u32,
    unset: bool,
) -> ReplayResult {
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
    };

    let mut meta = match io::read_metadata(device, ie.record_offset) {
        Ok(m) => m,
        // `read_metadata` returns `Err` for both raw I/O failures and
        // corrupt magic / version mismatches in the metadata block.
        // Treat both as fatal — they indicate the record on device is
        // unreadable, which is more severe than a missing-primary case.
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };

    let count = meta.block_entry_count as usize;
    let inline = count.min(INLINE_BLOCK_ENTRIES);
    let has_overflow = count > INLINE_BLOCK_ENTRIES;

    if unset {
        let mut found_inline = None;
        for i in 0..inline {
            if { meta.block_entries_inline[i].block_id } == block_id {
                found_inline = Some(i);
                break;
            }
        }
        if let Some(i) = found_inline {
            if has_overflow {
                // Mirror live set_mined: pull the last overflow entry
                // into the vacated inline slot, shrink the overflow.
                let mut overflow = match read_recovery_overflow_entries(device, &meta) {
                    Ok(v) => v,
                    Err(RecoveryOverflowError::Io) => {
                        return ReplayResult::Failed(ReplayCause::IoError);
                    }
                    Err(RecoveryOverflowError::Logic) => {
                        return ReplayResult::Failed(ReplayCause::LogicError);
                    }
                };
                let Some(last) = overflow.pop() else {
                    return ReplayResult::Failed(ReplayCause::LogicError);
                };
                meta.block_entries_inline[i] = last;
                let Some(alloc) = allocator.as_deref_mut() else {
                    return ReplayResult::Failed(ReplayCause::LogicError);
                };
                match write_recovery_overflow_entries(device, alloc, &mut meta, &overflow) {
                    Ok(()) => {}
                    Err(RecoveryOverflowError::Io) => {
                        return ReplayResult::Failed(ReplayCause::IoError);
                    }
                    Err(RecoveryOverflowError::Logic) => {
                        return ReplayResult::Failed(ReplayCause::LogicError);
                    }
                }
            } else {
                if i < inline - 1 {
                    meta.block_entries_inline[i] = meta.block_entries_inline[inline - 1];
                }
                meta.block_entries_inline[inline - 1] = BlockEntry {
                    block_id: 0,
                    block_height: 0,
                    subtree_idx: 0,
                };
                meta.block_entry_count -= 1;
            }
        } else if has_overflow {
            let mut overflow = match read_recovery_overflow_entries(device, &meta) {
                Ok(v) => v,
                Err(RecoveryOverflowError::Io) => {
                    return ReplayResult::Failed(ReplayCause::IoError);
                }
                Err(RecoveryOverflowError::Logic) => {
                    return ReplayResult::Failed(ReplayCause::LogicError);
                }
            };
            let Some(pos) = overflow.iter().position(|e| { e.block_id } == block_id) else {
                return ReplayResult::Skipped;
            };
            overflow.swap_remove(pos);
            let Some(alloc) = allocator.as_deref_mut() else {
                return ReplayResult::Failed(ReplayCause::LogicError);
            };
            match write_recovery_overflow_entries(device, alloc, &mut meta, &overflow) {
                Ok(()) => {}
                Err(RecoveryOverflowError::Io) => {
                    return ReplayResult::Failed(ReplayCause::IoError);
                }
                Err(RecoveryOverflowError::Logic) => {
                    return ReplayResult::Failed(ReplayCause::LogicError);
                }
            }
        } else {
            return ReplayResult::Skipped;
        }
    } else {
        // Duplicate check: inline entries first, then overflow — a
        // replayed SetMined whose entry already lives in overflow must
        // be a Skipped no-op (no second generation bump).
        for i in 0..inline {
            if { meta.block_entries_inline[i].block_id } == block_id {
                return ReplayResult::Skipped;
            }
        }
        if has_overflow {
            let overflow = match read_recovery_overflow_entries(device, &meta) {
                Ok(v) => v,
                Err(RecoveryOverflowError::Io) => {
                    return ReplayResult::Failed(ReplayCause::IoError);
                }
                Err(RecoveryOverflowError::Logic) => {
                    return ReplayResult::Failed(ReplayCause::LogicError);
                }
            };
            if overflow.iter().any(|e| { e.block_id } == block_id) {
                return ReplayResult::Skipped;
            }
        }
        if count < INLINE_BLOCK_ENTRIES {
            meta.block_entries_inline[count] = BlockEntry {
                block_id,
                block_height,
                subtree_idx,
            };
            meta.block_entry_count += 1;
        } else {
            // 4th+ entry: needs the overflow region. Pre-fix this case
            // fell through silently (entry dropped, generation still
            // bumped). Fail closed when no allocator is available.
            let Some(alloc) = allocator else {
                return ReplayResult::Failed(ReplayCause::LogicError);
            };
            match append_recovery_overflow_block_entry(
                device,
                alloc,
                &mut meta,
                BlockEntry {
                    block_id,
                    block_height,
                    subtree_idx,
                },
            ) {
                Ok(()) => {}
                Err(RecoveryOverflowError::Io) => {
                    return ReplayResult::Failed(ReplayCause::IoError);
                }
                Err(RecoveryOverflowError::Logic) => {
                    return ReplayResult::Failed(ReplayCause::LogicError);
                }
            }
        }
    }

    meta.generation = { meta.generation }.wrapping_add(1);

    // R-013: propagate metadata write failure instead of returning Applied with a dropped error.
    if io::write_metadata(device, ie.record_offset, &meta).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }
    ReplayResult::Applied
}

fn replay_freeze(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    tx_key: &TxKey,
    offset: u32,
    expected_hash: Option<&[u8; 32]>,
) -> ReplayResult {
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
    };

    // B-5 parity with SpendV3: a CRC-failing (torn) slot in the WAL window is
    // exactly what this redo entry exists to repair. A FreezeV2 entry carries
    // the slot's `utxo_hash` (passed as `expected_hash`), so rebuild the frozen
    // slot from the durable intent instead of fail-closed-bricking recovery. A
    // non-corruption device I/O error still fails — the WAL cannot repair that.
    let read = match io::read_utxo_slot(device, ie.record_offset, offset) {
        Ok(s) => Some(s),
        Err(DeviceError::RecordCorruption { .. }) => None,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };

    let frozen = match read {
        Some(slot) => {
            if let Some(expected_hash) = expected_hash
                && slot.hash != *expected_hash
            {
                return ReplayResult::Skipped;
            }

            if slot.status == UTXO_FROZEN {
                return ReplayResult::Skipped;
            }
            // F-G4-005: a legacy Freeze entry (no expected_hash) cannot verify
            // that the slot at (record_offset, offset) is still the same UTXO
            // the original Freeze targeted. Conservatively skip replay over
            // anything other than UNSPENT — SPENT/PRUNED/LOCKED states have
            // moved on and re-stamping FROZEN would silently overwrite a
            // status another replay path depends on. Log the unusual case so
            // operators can correlate with upstream reorderings.
            if slot.status != UTXO_UNSPENT {
                if expected_hash.is_none() {
                    tracing::warn!(
                        target: "teraslab::recovery",
                        slot_status = slot.status,
                        offset,
                        "F-G4-005: skipping legacy Freeze replay over non-UNSPENT slot",
                    );
                }
                return ReplayResult::Skipped;
            }
            UtxoSlot::new_frozen(slot.hash)
        }
        None => match expected_hash {
            // Torn slot rebuilt directly from the FreezeV2 redo entry's hash.
            Some(h) => UtxoSlot::new_frozen(*h),
            // Legacy V1 entry without the hash: unrepairable here. Fail closed
            // so the operator can run the repair CLI.
            None => return ReplayResult::Failed(ReplayCause::IoError),
        },
    };

    if io::write_utxo_slot(device, ie.record_offset, offset, &frozen).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }
    ReplayResult::Applied
}

fn replay_unfreeze(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    tx_key: &TxKey,
    offset: u32,
    expected_hash: Option<&[u8; 32]>,
) -> ReplayResult {
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
    };

    // B-5 parity with SpendV3 (see replay_freeze): rebuild a torn slot from the
    // UnfreezeV2 entry's `utxo_hash` rather than fail-closed-bricking recovery.
    let read = match io::read_utxo_slot(device, ie.record_offset, offset) {
        Ok(s) => Some(s),
        Err(DeviceError::RecordCorruption { .. }) => None,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };

    let unspent = match read {
        Some(slot) => {
            if let Some(expected_hash) = expected_hash
                && slot.hash != *expected_hash
            {
                return ReplayResult::Skipped;
            }

            if slot.status == UTXO_UNSPENT {
                return ReplayResult::Skipped;
            }
            if slot.status != UTXO_FROZEN {
                return ReplayResult::Skipped;
            }
            UtxoSlot::new_unspent(slot.hash)
        }
        None => match expected_hash {
            // Torn slot rebuilt directly from the UnfreezeV2 redo entry's hash.
            Some(h) => UtxoSlot::new_unspent(*h),
            None => return ReplayResult::Failed(ReplayCause::IoError),
        },
    };

    if io::write_utxo_slot(device, ie.record_offset, offset, &unspent).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }
    ReplayResult::Applied
}

/// BUG-1 fix #3: register a recovered create entry while enforcing the
/// offset-uniqueness invariant — no two keys may map to the same
/// `record_offset`.
///
/// `index.register` rejects a duplicate KEY but NOT a duplicate OFFSET, so
/// a stale aliased entry `A → record_offset` left in the index (e.g. by a
/// pre-fix recovery run, or a snapshot taken before this fix) would coexist
/// with the rightful owner being registered here, and `lookup(A)` would
/// read the wrong record's bytes.
///
/// The caller has already verified (BUG-1 fix #2) that the on-device
/// metadata at `record_offset` carries `key.txid`, so `key` is the rightful
/// owner of whatever record currently lives there. Any OTHER key already
/// mapped to the same offset is therefore the stale alias and is
/// unregistered before `key` is registered. After this call exactly one
/// key maps to `record_offset`.
///
/// Cost: a single index scan to locate a conflicting different-key entry.
/// This runs only on the recovery (startup) create path, never the serving
/// hot path. Returns the underlying [`crate::index::IndexError`] if the
/// final `register` fails.
/// Reverse map from `record_offset` to the single key that currently owns
/// that offset in the primary index.
///
/// BUG-1 offset-uniqueness (fix #3) requires that, after recovery, no two
/// keys map to the same `record_offset`. The original implementation
/// enforced this by scanning the entire primary index (`index.iter()`) on
/// EVERY recovered create to find a pre-existing alias — O(N) per create,
/// O(M×N) total (M = creates replayed, N = loaded index size). This map
/// replaces that scan: it is built ONCE at the start of recovery from
/// `index.iter()` (O(N)), then each create consults it in O(1) and keeps it
/// in sync. Total cost is therefore O(N) once + O(1) per create.
///
/// At the 10M-record target the map holds up to 10M `(u64, [u8; 32])`
/// pairs ≈ 40 bytes/entry of payload (~400 MB transient, plus `HashMap`
/// bucket overhead). It lives only for the duration of recovery and is
/// dropped immediately after, so the peak is a startup-only cost.
// Keyed by (device_id, record_offset): record offsets are store-LOCAL, so two
// records on different stores can legitimately share the same offset value.
// Keying on offset alone would make a create on store 1 evict the same-offset
// record on store 0 (multi-store aliasing false-positive).
type OffsetOwners = std::collections::HashMap<(u8, u64), TxKey>;

/// Build the [`OffsetOwners`] reverse map from the loaded primary index in a
/// single O(N) pass.
///
/// Called ONCE per recovery run, before the replay loop. After this point
/// the map is the authoritative record of which key owns each offset and is
/// updated incrementally by [`register_unique_offset`]; the per-create
/// `index.iter()` scan is gone.
///
/// If the loaded index already contains two keys aliasing one offset (an
/// impossible-but-defensive case from a corrupt snapshot), the last one
/// visited by `iter()` wins in the map. That does not weaken correctness:
/// the first legitimate create replayed against that offset will still
/// evict whichever stale key the map records, and any remaining alias is
/// caught by the R2 tx_id-mismatch purge.
/// Build the [`OffsetOwners`] reverse map from the loaded primary index in a
/// single O(N) pass, fanning out across all shards via [`ShardedIndex::for_each`].
///
/// Called ONCE per recovery run, before the replay loop.
fn build_offset_owners(index: &ShardedIndex) -> OffsetOwners {
    let mut owners = OffsetOwners::new();
    index.for_each(|key, entry| {
        owners.insert((entry.device_id, entry.record_offset), key);
    });
    owners
}

/// Register `key → entry` while preserving the BUG-1 offset-uniqueness
/// invariant: no two keys may map to the same `record_offset`.
///
/// Complexity: O(1). The pre-existing alias (a DIFFERENT key carried in
/// from a persisted/snapshotted index that maps to the same offset) is
/// found via an O(1) `offset_owners.get(&record_offset)` instead of a full
/// `index.iter()` scan. With BUG-1 fix #2 in force no NEW alias can be
/// created during this recovery run (registration only proceeds when the
/// on-device tx_id matches the key, and one offset holds exactly one record
/// / tx_id), so the only alias this evicts is that pre-existing one.
///
/// The correctness guarantee is identical to the prior O(N)-scan version:
/// after the call the offset maps to exactly `key`, and any other key that
/// previously aliased it has been `unregister`ed. `offset_owners` is kept
/// in sync so subsequent creates see the new owner.
fn register_unique_offset(
    index: &ShardedIndex,
    offset_owners: &mut OffsetOwners,
    key: TxKey,
    entry: TxIndexEntry,
) -> Result<(), crate::index::IndexError> {
    let record_offset = entry.record_offset;
    let owner_key = (entry.device_id, record_offset);

    // O(1) lookup of any DIFFERENT key already aliasing this (store, offset).
    if let Some(&stale) = offset_owners.get(&owner_key)
        && stale != key
    {
        // The rightful owner is `key` (its txid matches the on-device
        // record per fix #2); drop the stale alias so the offset maps to
        // exactly one key.
        index.unregister(&stale);
        tracing::warn!(
            target: "teraslab::recovery",
            stale_txid_prefix = ?&stale.txid[..4],
            owner_txid_prefix = ?&key.txid[..4],
            record_offset,
            "BUG-1: dropped stale index entry aliasing a record offset now owned by another key",
        );
    }

    index.register(key, entry)?;
    // Record the rightful owner so a later create for the same (store, offset)
    // (or a re-replay of this one) sees `key`, not a stale snapshot alias.
    offset_owners.insert(owner_key, key);
    Ok(())
}

/// Legacy (pre-`Create`) create replay.
///
/// Replays a `RedoOp::ReplicaCreate` entry written before gap #2 added the
/// full-payload `RedoOp::Create` variant. The entry only carries
/// `record_offset + utxo_count` — there are no captured record bytes —
/// so this function can only validate that the on-device record at
/// `record_offset` is coherent enough to register an index entry that
/// doesn't lie about the cached metadata fields.
///
/// R-031 (BC-53): pre-fix the function blindly registered an index
/// entry with all-zero cached fields (`tx_flags`, `spent_utxos`,
/// `dah_or_preserve`, `unmined_since`, `generation`) and zero
/// `block_entry_count`. If the on-device metadata had been written
/// before the crash but the redo entry never made it through, that
/// was correct; but if the device write was incomplete or torn, the
/// recovery would still register a perfectly-cached zero-state index
/// entry pointing at unreadable bytes, then start serving reads from
/// it. Aligning with `replay_create`'s validate-then-register
/// pattern: read the metadata header, fail closed on I/O / corruption,
/// require the redo entry's `utxo_count` to match the on-device
/// `utxo_count`, and seed the index entry's cached fields from the
/// validated metadata so subsequent reads reflect the actual record
/// state (not zeros).
fn replay_replica_create(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    offset_owners: &mut OffsetOwners,
    tx_key: &TxKey,
    record_offset: u64,
    utxo_count: u32,
) -> ReplayResult {
    // Idempotent: if already in index, skip — but if the existing
    // index entry's `record_offset` does NOT match the redo entry's,
    // surface a warning (F-G4-014). Skipping is still correct (a
    // later replay of Delete + Create restamped the index entry), but
    // the reordering may indicate an upstream bug worth investigating.
    if let Some(existing) = index.lookup(tx_key) {
        if existing.record_offset != record_offset || existing.utxo_count != utxo_count {
            tracing::warn!(
                target: "teraslab::recovery",
                txid_prefix = ?&tx_key.txid[..4],
                expected_record_offset = record_offset,
                actual_record_offset = existing.record_offset,
                expected_utxo_count = utxo_count,
                actual_utxo_count = existing.utxo_count,
                "F-G4-014: replay_replica_create skipped — existing index entry diverges from redo entry; \
                 likely a delete+recreate that crossed the redo log",
            );
        }
        return ReplayResult::Skipped;
    }

    // Read the on-device metadata header. A read error here means this
    // node has no durable record bytes at `record_offset`. A legacy
    // `Create` carries NO captured payload and is only written by the
    // replication / migration receiver for a SECONDARY copy whose
    // authoritative record lives on the master, so the bytes being absent
    // is a recoverable replica condition (the master resyncs the key on
    // rejoin), NOT the device-fault that `replay_create`'s identical
    // read-back guards against. We still fail-closed for THIS entry (skip
    // the index registration so no entry points at unreadable bytes), but
    // classify it as the tolerable `ReplicaRecordAbsent` so the node boots
    // instead of crash-looping (scenario_09: 0/N ready forever).
    let meta = match crate::io::read_metadata(device, record_offset) {
        Ok(m) => m,
        Err(_) => return ReplayResult::Failed(ReplayCause::ReplicaRecordAbsent),
    };

    // The redo entry's `utxo_count` MUST match the on-device metadata's
    // `utxo_count` — otherwise the redo entry is referring to a record
    // that no longer exists at `record_offset` (someone else's data, or
    // a torn write). Fail closed.
    if { meta.utxo_count } != utxo_count {
        return ReplayResult::Failed(ReplayCause::CorruptEntry);
    }

    // BUG-1 fix #2: the on-device metadata MUST belong to THIS key.
    // `utxo_count` alone is insufficient — a DIFFERENT record B with the
    // same `utxo_count` can occupy `record_offset` after the offset was
    // freed and re-handed out. Seeding the index entry's cached fields
    // (including `generation`) from B and registering A→record_offset
    // would alias two keys onto one record, so `lookup(A)` returns B's
    // bytes. The metadata `tx_id` is write-once, so comparing it to the
    // key's txid is the decisive, cheap aliasing detector. On mismatch
    // this legacy Create is stale (the offset was reallocated): do NOT
    // register. Classify as `CorruptEntry` to match the utxo_count guard.
    if { meta.tx_id } != tx_key.txid {
        return ReplayResult::Failed(ReplayCause::CorruptEntry);
    }

    let entry = TxIndexEntry {
        device_id: 0,
        record_offset,
        utxo_count,
        block_entry_count: meta.block_entry_count,
        tx_flags: meta.flags.bits(),
        spent_utxos: { meta.spent_utxos },
        dah_or_preserve: { meta.delete_at_height },
        unmined_since: { meta.unmined_since },
        generation: { meta.generation },
    };
    match register_unique_offset(index, offset_owners, *tx_key, entry) {
        Ok(()) => ReplayResult::Applied,
        // `register_unique_offset` returns `Err` for capacity /
        // duplicate-key / offset-aliasing / structural problems — none of
        // which are I/O against the device, so classify as logic-level so
        // startup fails closed instead of silently dropping the create.
        Err(_) => ReplayResult::Failed(ReplayCause::LogicError),
    }
}

/// Tombstone a record's metadata header during redo replay of a delete.
///
/// Writes the same length-bearing [`DeletedRecordMarker`] the live delete path
/// writes (carrying `record_size`) into the first bytes of the header and
/// zeroes the rest of the `METADATA_SIZE` window. This keeps a replayed delete
/// indistinguishable on disk from a live-path delete, so a device-scan rebuild
/// after a second crash mid-replay still skips the WHOLE deleted record rather
/// than boot-looping on a multi-block body.
fn write_zeroed_metadata_header(
    device: &dyn BlockDevice,
    record_offset: u64,
    record_size: u64,
) -> ReplayResult {
    let align = device.alignment();
    let aligned_base = record_offset / align as u64 * align as u64;
    let intra_offset = (record_offset - aligned_base) as usize;
    let total_size = io::align_up(intra_offset + METADATA_SIZE, align);

    let mut buf = crate::device::AlignedBuf::new(total_size, align);
    if (intra_offset != 0 || !METADATA_SIZE.is_multiple_of(align))
        && device.pread_exact_at(&mut buf, aligned_base).is_err()
    {
        return ReplayResult::Failed(ReplayCause::IoError);
    }
    let mut header = [0u8; METADATA_SIZE];
    DeletedRecordMarker::new(record_size).to_bytes(&mut header);
    buf[intra_offset..intra_offset + METADATA_SIZE].copy_from_slice(&header);
    if device.pwrite_all_at(&buf, aligned_base).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }
    if device.sync().is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }
    ReplayResult::Applied
}

fn replay_delete(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    tx_key: &TxKey,
    record_offset: u64,
    record_size: u64,
) -> ReplayResult {
    let mut applied = false;
    if record_offset != 0 && record_size != 0 {
        match write_zeroed_metadata_header(device, record_offset, record_size) {
            ReplayResult::Applied => applied = true,
            ReplayResult::Skipped => {}
            failed @ ReplayResult::Failed(_) => return failed,
        }
    }

    if index.unregister(tx_key).is_some() {
        applied = true;
    }

    if applied {
        ReplayResult::Applied
    } else {
        ReplayResult::Skipped
    }
}

/// Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): full-payload create
/// replay.
///
/// Reconstructs the on-device record bit-for-bit from the bytes captured
/// in the redo entry, then registers the primary index entry. The pwrite
/// uses [`crate::device::AlignedBuf`] so the call works against
/// `DirectDevice` (`O_DIRECT` requires aligned buffers and aligned
/// offset/length); the source record was written by the engine with the
/// same alignment policy so copying the captured bytes into an
/// alignment-padded buffer reproduces the original device state byte-for-
/// byte at the populated prefix. Trailing alignment padding is zero in
/// both write paths so the device contents are identical.
///
/// Conflicting-child links: when `is_conflicting` is set, every parent
/// txid in `parent_txids` receives the new txid via
/// [`PrimaryBackend::append_conflicting_child`]. Idempotent: if the link
/// already exists the call is a no-op.
///
/// Idempotency overall: when the primary index already has an entry for
/// `tx_key`, the entire replay is a [`ReplayResult::Skipped`] — the
/// previous run already applied this redo entry. Otherwise we always
/// rewrite the record bytes (overwriting any partial bytes left from a
/// crashed write) and then register.
// Per-entry replay path: arguments are the decoded create-v2 fields (key,
// record offset, utxo count, conflicting flag, raw record bytes, parent txids)
// plus the device, index, and offset-owner map they act on. Independent inputs,
// no cohesive grouping, so the count is warranted.
#[allow(clippy::too_many_arguments)]
fn replay_create(
    device: &dyn BlockDevice,
    device_id: u8,
    index: &ShardedIndex,
    offset_owners: &mut OffsetOwners,
    tx_key: &TxKey,
    record_offset: u64,
    utxo_count: u32,
    is_conflicting: bool,
    record_bytes: &[u8],
    parent_txids: &[[u8; 32]],
) -> ReplayResult {
    use crate::device::AlignedBuf;

    // Idempotent: if already registered, this redo entry has been
    // replayed — skip.
    if index.lookup(tx_key).is_some() {
        return ReplayResult::Skipped;
    }

    // The redo entry must carry at least a metadata header for the
    // record to be reconstructable. A shorter payload is corrupt.
    if record_bytes.len() < crate::record::METADATA_SIZE {
        return ReplayResult::Failed(ReplayCause::CorruptEntry);
    }

    // Allocate an aligned buffer big enough to hold the captured record
    // bytes. AlignedBuf zero-initializes, so any tail padding matches
    // what the engine writes.
    let align = device.alignment();
    let aligned_len = record_bytes.len().div_ceil(align) * align;
    let mut buf = AlignedBuf::new(aligned_len, align);
    buf[..record_bytes.len()].copy_from_slice(record_bytes);
    if let Err(_e) = device.pwrite_all_at(&buf, record_offset) {
        // Short / failed writes on the record area are non-tolerable —
        // continuing would register an index entry pointing at
        // incomplete bytes.
        return ReplayResult::Failed(ReplayCause::MissingRecordBytes);
    }

    // Read the metadata back so we can populate the index entry's
    // cached fields (tx_flags, spent_utxos, dah_or_preserve,
    // unmined_since, generation, block_entry_count). This also gives us
    // a verify-after-write check: if the device returns short or
    // corrupt bytes after the pwrite, fail closed instead of silently
    // registering an entry pointing at unreadable data.
    let meta = match crate::io::read_metadata(device, record_offset) {
        Ok(m) => m,
        Err(_) => return ReplayResult::Failed(ReplayCause::MissingRecordBytes),
    };

    // The redo entry's `utxo_count` must match what the metadata says —
    // mismatch indicates either a corrupt redo payload or a write that
    // landed on unexpected bytes.
    if { meta.utxo_count } != utxo_count {
        return ReplayResult::Failed(ReplayCause::CorruptEntry);
    }

    // BUG-1 fix #2: the reconstructed metadata's `tx_id` MUST match the
    // redo entry's key. The bytes were just written from the captured
    // payload, so a mismatch means the captured payload belongs to a
    // different transaction than the redo entry claims — a corrupt redo
    // record. Registering it would seed cached fields (and `generation`)
    // from the wrong record and could alias A→record_offset onto B's
    // bytes. Fail closed before registering.
    if { meta.tx_id } != tx_key.txid {
        return ReplayResult::Failed(ReplayCause::CorruptEntry);
    }

    let entry = TxIndexEntry {
        device_id,
        record_offset,
        utxo_count,
        block_entry_count: meta.block_entry_count,
        tx_flags: meta.flags.bits(),
        spent_utxos: { meta.spent_utxos },
        dah_or_preserve: { meta.delete_at_height },
        unmined_since: { meta.unmined_since },
        generation: { meta.generation },
    };
    if let Err(_e) = register_unique_offset(index, offset_owners, *tx_key, entry) {
        return ReplayResult::Failed(ReplayCause::LogicError);
    }

    // Conflicting-child link replay is intentionally NOT performed in
    // this low-level create replay path. Establishing the link requires
    // writing a child-list block and mutating the parent's metadata via
    // `Engine::append_conflicting_child`, which depends on the engine's
    // allocator and stripe locks. R-221 covers that parent mutation with
    // a separate `RedoOp::AppendConflictingChild` intent; full startup
    // recovery collects those entries and drains them after constructing
    // the engine. Keep these Create fields bound so old entries still
    // round-trip exactly.
    let _ = (is_conflicting, parent_txids);

    ReplayResult::Applied
}

fn replay_metadata_op(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    entry: &RedoEntry,
) -> ReplayResult {
    match &entry.op {
        RedoOp::Reassign {
            tx_key,
            offset,
            new_hash,
            block_height,
            spendable_after,
        } => {
            let ie = match index.lookup(tx_key) {
                Some(e) => e,
                None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
            };
            let slot = match io::read_utxo_slot(device, ie.record_offset, *offset) {
                Ok(s) => s,
                Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
            };
            // Idempotent: already reassigned if hash matches new_hash and status is UNSPENT
            if slot.hash == *new_hash && slot.status == UTXO_UNSPENT {
                return ReplayResult::Skipped;
            }
            let spendable_height = block_height.saturating_add(*spendable_after);
            let mut new_slot = UtxoSlot::new_unspent(*new_hash);
            new_slot.spending_data[0..4].copy_from_slice(&spendable_height.to_le_bytes());
            if io::write_utxo_slot(device, ie.record_offset, *offset, &new_slot).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            ReplayResult::Applied
        }
        RedoOp::ReassignV2 {
            tx_key,
            offset,
            new_hash,
            block_height,
            spendable_after,
            prior_utxo_hash,
        } => {
            let ie = match index.lookup(tx_key) {
                Some(e) => e,
                None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
            };
            let slot = match io::read_utxo_slot(device, ie.record_offset, *offset) {
                Ok(s) => s,
                Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
            };
            // Idempotent: already reassigned if hash matches new_hash and status is UNSPENT.
            if slot.hash == *new_hash && slot.status == UTXO_UNSPENT {
                return ReplayResult::Skipped;
            }
            // F-A1 (reassign): the live `engine.reassign` requires the slot to
            // be FROZEN with `slot.hash == request.utxo_hash` BEFORE mutating
            // (ERR_UTXO_HASH_MISMATCH / ERR_UTXO_NOT_FROZEN otherwise). Recovery
            // must be symmetric: a redo entry whose `prior_utxo_hash` no longer
            // matches the on-disk slot identity, or whose slot is no longer
            // FROZEN, is replaying an operation the master reported as an error
            // — skip it rather than stamping a fresh slot the live path refused.
            // Mirrors the `replay_freeze`/`replay_unspend` identity guards.
            if slot.status != UTXO_FROZEN || slot.hash != *prior_utxo_hash {
                return ReplayResult::Skipped;
            }
            let spendable_height = block_height.saturating_add(*spendable_after);
            let mut new_slot = UtxoSlot::new_unspent(*new_hash);
            new_slot.spending_data[0..4].copy_from_slice(&spendable_height.to_le_bytes());
            if io::write_utxo_slot(device, ie.record_offset, *offset, &new_slot).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            ReplayResult::Applied
        }
        RedoOp::PruneSlot { tx_key, offset } => {
            let ie = match index.lookup(tx_key) {
                Some(e) => e,
                None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
            };
            let slot = match io::read_utxo_slot(device, ie.record_offset, *offset) {
                Ok(s) => s,
                Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
            };
            if slot.status == UTXO_PRUNED {
                return ReplayResult::Skipped;
            }
            let mut pruned = slot;
            pruned.status = UTXO_PRUNED;
            if io::write_utxo_slot(device, ie.record_offset, *offset, &pruned).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            ReplayResult::Applied
        }
        RedoOp::PruneSlotIfSpentBy {
            tx_key,
            offset,
            child_txid,
        } => {
            let ie = match index.lookup(tx_key) {
                Some(e) => e,
                None => return ReplayResult::Skipped,
            };
            let slot = match io::read_utxo_slot(device, ie.record_offset, *offset) {
                Ok(s) => s,
                Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
            };
            if slot.status == UTXO_PRUNED {
                return ReplayResult::Skipped;
            }
            if slot.status != UTXO_SPENT || slot.spending_data[..32] != child_txid[..] {
                return ReplayResult::Skipped;
            }
            let mut pruned = slot;
            pruned.status = UTXO_PRUNED;
            if io::write_utxo_slot(device, ie.record_offset, *offset, &pruned).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            let mut meta = match io::read_metadata(device, ie.record_offset) {
                Ok(meta) => meta,
                Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
            };
            meta.spent_utxos = { meta.spent_utxos }.saturating_sub(1);
            meta.pruned_utxos = { meta.pruned_utxos }.saturating_add(1);
            meta.generation = { meta.generation }.wrapping_add(1);
            if io::write_metadata(device, ie.record_offset, &meta).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            ReplayResult::Applied
        }
        RedoOp::SetConflicting { tx_key, value, .. } => {
            let ie = match index.lookup(tx_key) {
                Some(e) => e,
                None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
            };
            let mut meta = match io::read_metadata(device, ie.record_offset) {
                Ok(m) => m,
                Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
            };
            let has_flag = meta.flags.contains(TxFlags::CONFLICTING);
            if has_flag == *value {
                return ReplayResult::Skipped;
            }
            if *value {
                meta.flags |= TxFlags::CONFLICTING;
            } else {
                // F-G4-015: idiomatic bitflags clear.
                meta.flags.remove(TxFlags::CONFLICTING);
            }
            // R-013: propagate write failure.
            if io::write_metadata(device, ie.record_offset, &meta).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            ReplayResult::Applied
        }
        RedoOp::SetLocked { tx_key, value } => {
            let ie = match index.lookup(tx_key) {
                Some(e) => e,
                None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
            };
            let mut meta = match io::read_metadata(device, ie.record_offset) {
                Ok(m) => m,
                Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
            };
            let has_flag = meta.flags.contains(TxFlags::LOCKED);
            if has_flag == *value {
                return ReplayResult::Skipped;
            }
            if *value {
                meta.flags |= TxFlags::LOCKED;
                if { meta.delete_at_height } != 0 {
                    meta.delete_at_height = 0;
                }
            } else {
                // F-G4-015: idiomatic bitflags clear.
                meta.flags.remove(TxFlags::LOCKED);
            }
            // R-013: propagate write failure.
            if io::write_metadata(device, ie.record_offset, &meta).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            ReplayResult::Applied
        }
        RedoOp::PreserveUntil {
            tx_key,
            block_height,
        } => {
            let ie = match index.lookup(tx_key) {
                Some(e) => e,
                None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
            };
            let mut meta = match io::read_metadata(device, ie.record_offset) {
                Ok(m) => m,
                Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
            };
            if { meta.preserve_until } == *block_height {
                return ReplayResult::Skipped;
            }
            meta.preserve_until = *block_height;
            meta.delete_at_height = 0;
            // R-013: propagate write failure.
            if io::write_metadata(device, ie.record_offset, &meta).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            ReplayResult::Applied
        }
        RedoOp::MarkOnLongestChain {
            tx_key,
            on_longest_chain,
            current_block_height,
            generation,
            ..
        } => {
            let ie = match index.lookup(tx_key) {
                Some(e) => e,
                None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
            };
            let mut meta = match io::read_metadata(device, ie.record_offset) {
                Ok(m) => m,
                Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
            };
            // H7: generation-based idempotency. The redo entry declares the
            // target generation after applying the op. Skip when the
            // on-device generation is already at-or-ahead of the target — a
            // later op (or this op itself already replayed) has equal-or-newer
            // state. On apply, bump the generation to the target so a
            // subsequent replay of the same entry is correctly observed as
            // already-applied. This prevents replay from leaving the
            // generation counter stale and tripping replication staleness
            // checks on otherwise-valid future ops.
            //
            // Generation comparison uses wrapping serial-number ordering:
            // target is newer only when it is within the next half of the
            // u32 space from the current generation. A target of 0 still
            // means legacy/unknown unless it is modularly ahead of the
            // current generation, which is the real u32::MAX -> 0 wrap case.
            let target_generation = *generation;
            let current_generation = { meta.generation };
            let has_generation_token = target_generation != 0
                || generation_target_ahead(current_generation, target_generation);
            let target_unmined = if *on_longest_chain {
                0
            } else {
                *current_block_height
            };
            if !has_generation_token {
                // No generation supplied — fall back to value-equality
                // idempotency on unmined_since alone.
                if { meta.unmined_since } == target_unmined {
                    return ReplayResult::Skipped;
                }
            } else if generation_at_or_ahead(current_generation, target_generation) {
                // Already caught up (or ahead).
                return ReplayResult::Skipped;
            }
            meta.unmined_since = target_unmined;
            if has_generation_token {
                meta.generation = target_generation;
            }
            // R-013: propagate write failure.
            if io::write_metadata(device, ie.record_offset, &meta).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            ReplayResult::Applied
        }
        _ => ReplayResult::Skipped,
    }
}

/// Gap #8 (TERANODE_PRODUCTION_READINESS_GAPS.md): replay a
/// `CompensateUnsetMined` redo entry recorded mid-rollback.
///
/// Re-adds the captured `block_id` / `block_height` / `subtree_idx` triple
/// to the record's block-entry list, restoring the state that existed
/// BEFORE the failed-replication unset-mined was applied. Idempotent: if
/// the block entry is already present (with matching height/subtree),
/// the call is a no-op.
fn replay_compensate_unset_mined(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    tx_key: &TxKey,
    block_id: u32,
    block_height: u32,
    subtree_idx: u32,
) -> ReplayResult {
    replay_compensate_unset_mined_with_allocator(
        device,
        index,
        None,
        tx_key,
        block_id,
        block_height,
        subtree_idx,
    )
}

fn replay_compensate_unset_mined_with_allocator(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    allocator: Option<&mut SlotAllocator>,
    tx_key: &TxKey,
    block_id: u32,
    block_height: u32,
    subtree_idx: u32,
) -> ReplayResult {
    let mut allocator = allocator;
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        // Compensation against a record that was deleted later in the log
        // is benign — the record state we'd restore no longer exists.
        // Use MissingPrimary which is the tolerable class.
        None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
    };

    let mut meta = match io::read_metadata(device, ie.record_offset) {
        Ok(m) => m,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };

    let count = meta.block_entry_count as usize;
    let inline = count.min(INLINE_BLOCK_ENTRIES);

    // Idempotency: if the entry is already present with matching values,
    // skip. This handles re-replay of the same compensation entry.
    for i in 0..inline {
        if { meta.block_entries_inline[i].block_id } == block_id {
            let existing = meta.block_entries_inline[i];
            if { existing.block_height } == block_height && { existing.subtree_idx } == subtree_idx
            {
                return ReplayResult::Skipped;
            }
            // A different height/subtree for the same block_id is an
            // unexpected divergence — overwrite to the captured values.
            meta.block_entries_inline[i] = BlockEntry {
                block_id,
                block_height,
                subtree_idx,
            };
            if io::write_metadata(device, ie.record_offset, &meta).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            return ReplayResult::Applied;
        }
    }

    // Idempotency must also cover OVERFLOW-resident entries: pre-fix
    // the dup-check stopped at the inline cap, so re-replaying a
    // compensation for an entry that lives in overflow appended a
    // duplicate.
    if count > INLINE_BLOCK_ENTRIES {
        let mut overflow = match read_recovery_overflow_entries(device, &meta) {
            Ok(v) => v,
            Err(RecoveryOverflowError::Io) => return ReplayResult::Failed(ReplayCause::IoError),
            Err(RecoveryOverflowError::Logic) => {
                return ReplayResult::Failed(ReplayCause::LogicError);
            }
        };
        if let Some(pos) = overflow.iter().position(|e| { e.block_id } == block_id) {
            let existing = overflow[pos];
            if { existing.block_height } == block_height && { existing.subtree_idx } == subtree_idx
            {
                return ReplayResult::Skipped;
            }
            // Divergence: overwrite in place (same entry count, so the
            // region size is unchanged and the rewrite reuses it).
            overflow[pos] = BlockEntry {
                block_id,
                block_height,
                subtree_idx,
            };
            let Some(alloc) = allocator.as_deref_mut() else {
                return ReplayResult::Failed(ReplayCause::LogicError);
            };
            match write_recovery_overflow_entries(device, alloc, &mut meta, &overflow) {
                Ok(()) => {}
                Err(RecoveryOverflowError::Io) => {
                    return ReplayResult::Failed(ReplayCause::IoError);
                }
                Err(RecoveryOverflowError::Logic) => {
                    return ReplayResult::Failed(ReplayCause::LogicError);
                }
            }
            if io::write_metadata(device, ie.record_offset, &meta).is_err() {
                return ReplayResult::Failed(ReplayCause::IoError);
            }
            return ReplayResult::Applied;
        }
    }

    // Not present — append to inline if room.
    if count < INLINE_BLOCK_ENTRIES {
        meta.block_entries_inline[count] = BlockEntry {
            block_id,
            block_height,
            subtree_idx,
        };
        meta.block_entry_count += 1;
        if io::write_metadata(device, ie.record_offset, &meta).is_err() {
            return ReplayResult::Failed(ReplayCause::IoError);
        }
        ReplayResult::Applied
    } else {
        let Some(alloc) = allocator else {
            // The legacy `recover` path has no allocator handle. Fail
            // closed rather than silently dropping a compensation entry
            // that needs overflow storage.
            return ReplayResult::Failed(ReplayCause::LogicError);
        };
        match append_recovery_overflow_block_entry(
            device,
            alloc,
            &mut meta,
            BlockEntry {
                block_id,
                block_height,
                subtree_idx,
            },
        ) {
            Ok(()) => {
                if io::write_metadata(device, ie.record_offset, &meta).is_err() {
                    ReplayResult::Failed(ReplayCause::IoError)
                } else {
                    ReplayResult::Applied
                }
            }
            Err(RecoveryOverflowError::Io) => ReplayResult::Failed(ReplayCause::IoError),
            Err(RecoveryOverflowError::Logic) => ReplayResult::Failed(ReplayCause::LogicError),
        }
    }
}

#[derive(Debug)]
enum RecoveryOverflowError {
    Io,
    Logic,
}

fn append_recovery_overflow_block_entry(
    device: &dyn BlockDevice,
    allocator: &mut SlotAllocator,
    metadata: &mut TxMetadata,
    entry: BlockEntry,
) -> std::result::Result<(), RecoveryOverflowError> {
    let count = metadata.block_entry_count as usize;
    let overflow_count = count.saturating_sub(INLINE_BLOCK_ENTRIES);
    let mut overflow = read_recovery_overflow_entries(device, metadata)?;
    if overflow.len() != overflow_count {
        return Err(RecoveryOverflowError::Logic);
    }
    overflow.push(entry);
    write_recovery_overflow_entries(device, allocator, metadata, &overflow)
}

/// Rewrite a record's overflow block-entry region during replay.
///
/// Mirrors the live `write_overflow_entries` in `ops/engine.rs`,
/// including the F-G2-003 exact-size free + realloc-on-size-change
/// discipline: growing the region across an alignment boundary
/// REALLOCATES instead of writing past the existing allocation into
/// whatever the allocator placed next (silent neighbour corruption —
/// the pre-fix recovery-side writer reused the old offset
/// unconditionally). An emptied region is freed and the overflow
/// pointer cleared.
///
/// Precondition: the inline slots are full (`block_entry_count >=
/// INLINE_BLOCK_ENTRIES` on entry — overflow only exists past the
/// inline cap). On success `block_overflow_offset` and
/// `block_entry_count` reflect `entries`.
fn write_recovery_overflow_entries(
    device: &dyn BlockDevice,
    allocator: &mut SlotAllocator,
    metadata: &mut TxMetadata,
    entries: &[BlockEntry],
) -> std::result::Result<(), RecoveryOverflowError> {
    let alignment = device.alignment();
    let old_offset = { metadata.block_overflow_offset };
    // Derive the OLD allocation size from the pre-mutation count (the
    // caller has not touched `block_entry_count` yet). Defensive
    // fallback to one alignment unit matches `overflow_block_size`'s
    // contract in ops/engine.rs for a live pointer with a stale count.
    let old_total = metadata.block_entry_count as usize;
    let old_block_size = if old_total <= INLINE_BLOCK_ENTRIES {
        alignment
    } else {
        io::align_up(
            (old_total - INLINE_BLOCK_ENTRIES) * BLOCK_ENTRY_SIZE,
            alignment,
        )
    };

    let new_total = INLINE_BLOCK_ENTRIES
        .checked_add(entries.len())
        .filter(|&t| t <= u8::MAX as usize)
        .ok_or(RecoveryOverflowError::Logic)?;

    if entries.is_empty() {
        if old_offset != 0 {
            allocator
                .free(old_offset, old_block_size as u64)
                .map_err(|_| RecoveryOverflowError::Logic)?;
            metadata.block_overflow_offset = 0;
        }
        metadata.block_entry_count = INLINE_BLOCK_ENTRIES as u8;
        return Ok(());
    }

    let data_size = entries.len() * BLOCK_ENTRY_SIZE;
    let new_block_size = io::align_up(data_size, alignment);
    let offset = if old_offset == 0 {
        allocator
            .allocate(new_block_size as u64)
            .map_err(|_| RecoveryOverflowError::Logic)?
    } else if new_block_size == old_block_size {
        // Same alignment-rounded size: overwrite in place.
        old_offset
    } else {
        // Grow or shrink across an alignment boundary: exact-size free
        // of the old region, fresh allocation for the new size.
        allocator
            .free(old_offset, old_block_size as u64)
            .map_err(|_| RecoveryOverflowError::Logic)?;
        allocator
            .allocate(new_block_size as u64)
            .map_err(|_| RecoveryOverflowError::Logic)?
    };

    let mut buf = AlignedBuf::new(new_block_size, alignment);
    for (i, overflow_entry) in entries.iter().enumerate() {
        let start = i * BLOCK_ENTRY_SIZE;
        overflow_entry.to_bytes(&mut buf[start..start + BLOCK_ENTRY_SIZE]);
    }
    device
        .pwrite_all_at(&buf, offset)
        .map_err(|_| RecoveryOverflowError::Io)?;
    metadata.block_overflow_offset = offset;
    metadata.block_entry_count = new_total as u8;
    Ok(())
}

fn read_recovery_overflow_entries(
    device: &dyn BlockDevice,
    metadata: &TxMetadata,
) -> std::result::Result<Vec<BlockEntry>, RecoveryOverflowError> {
    let overflow_count = (metadata.block_entry_count as usize).saturating_sub(INLINE_BLOCK_ENTRIES);
    if overflow_count == 0 {
        return Ok(Vec::new());
    }
    let overflow_offset = metadata.block_overflow_offset;
    if overflow_offset == 0 {
        return Err(RecoveryOverflowError::Logic);
    }

    let alignment = device.alignment();
    let data_size = overflow_count * BLOCK_ENTRY_SIZE;
    let read_size = io::align_up(data_size, alignment);
    let mut buf = AlignedBuf::new(read_size, alignment);
    device
        .pread_exact_at(&mut buf, overflow_offset)
        .map_err(|_| RecoveryOverflowError::Io)?;

    let mut entries = Vec::with_capacity(overflow_count);
    for i in 0..overflow_count {
        let start = i * BLOCK_ENTRY_SIZE;
        entries.push(BlockEntry::from_bytes(
            &buf[start..start + BLOCK_ENTRY_SIZE],
        ));
    }
    Ok(entries)
}

/// Gap #8 (TERANODE_PRODUCTION_READINESS_GAPS.md): replay a
/// `CompensateReassign` redo entry recorded mid-rollback.
///
/// Restores the slot's `utxo_hash` to the captured pre-reassign value
/// and resets status to `UTXO_UNSPENT`. Idempotent: if the slot already
/// has the prior hash and is UNSPENT, skip.
fn replay_compensate_reassign(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    tx_key: &TxKey,
    offset: u32,
    prior_utxo_hash: &[u8; 32],
) -> ReplayResult {
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
    };

    let slot = match io::read_utxo_slot(device, ie.record_offset, offset) {
        Ok(s) => s,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };

    if slot.hash == *prior_utxo_hash && slot.status == UTXO_UNSPENT {
        return ReplayResult::Skipped;
    }

    let restored = UtxoSlot::new_unspent(*prior_utxo_hash);
    if io::write_utxo_slot(device, ie.record_offset, offset, &restored).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }
    ReplayResult::Applied
}

/// Gap #8 (TERANODE_PRODUCTION_READINESS_GAPS.md): replay a
/// `CompensatePrune` redo entry recorded mid-rollback.
///
/// Restores the slot's `status` byte to the captured pre-prune value
/// (UNSPENT, SPENT, FROZEN, etc.). The slot's hash and spending_data are
/// preserved verbatim from the on-device bytes — the prune only mutates
/// the status byte, so the rest of the slot is already what it was
/// before. Idempotent: if `slot.status` already matches `prior_status`,
/// skip.
fn replay_compensate_prune(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    tx_key: &TxKey,
    offset: u32,
    prior_status: u8,
) -> ReplayResult {
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
    };

    let mut slot = match io::read_utxo_slot(device, ie.record_offset, offset) {
        Ok(s) => s,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };

    if slot.status == prior_status {
        return ReplayResult::Skipped;
    }

    slot.status = prior_status;
    if io::write_utxo_slot(device, ie.record_offset, offset, &slot).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }
    ReplayResult::Applied
}

/// Replay a set-locked compensation intent recorded mid-rollback.
///
/// Restores both the locked flag and `delete_at_height` captured before the
/// failed-replication SetLocked mutation. Idempotent: if both fields already
/// match, replay skips.
fn replay_compensate_set_locked(
    device: &dyn BlockDevice,
    index: &ShardedIndex,
    tx_key: &TxKey,
    prior_locked: bool,
    prior_delete_at_height: u32,
) -> ReplayResult {
    let ie = match index.lookup(tx_key) {
        Some(e) => e,
        None => return ReplayResult::Failed(ReplayCause::MissingPrimary),
    };

    let mut meta = match io::read_metadata(device, ie.record_offset) {
        Ok(m) => m,
        Err(_) => return ReplayResult::Failed(ReplayCause::IoError),
    };

    let is_locked = meta.flags.contains(TxFlags::LOCKED);
    if is_locked == prior_locked && { meta.delete_at_height } == prior_delete_at_height {
        return ReplayResult::Skipped;
    }

    if prior_locked {
        meta.flags |= TxFlags::LOCKED;
    } else {
        // F-G4-015: idiomatic bitflags clear.
        meta.flags.remove(TxFlags::LOCKED);
    }
    meta.delete_at_height = prior_delete_at_height;

    if io::write_metadata(device, ie.record_offset, &meta).is_err() {
        return ReplayResult::Failed(ReplayCause::IoError);
    }
    ReplayResult::Applied
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::MemoryDevice;
    use crate::index::PrimaryBackend;
    use crate::locks::StripedLocks;
    use crate::ops::engine::Engine;
    use crate::redo::RedoLog;
    use std::sync::Arc;

    /// Setup: device with data region + separate redo log device.
    ///
    /// The `index` field is a [`ShardedIndex`] (N=1 in-memory shard) so
    /// all recovery functions can be called directly without wrapping.
    struct RecoveryTestHarness {
        data_dev: Arc<MemoryDevice>,
        redo_dev: Arc<MemoryDevice>,
        index: ShardedIndex,
        alloc: SlotAllocator,
    }

    impl RecoveryTestHarness {
        fn new() -> Self {
            let data_dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
            let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
            let alloc = SlotAllocator::new(data_dev.clone()).unwrap();
            let primary = PrimaryBackend::new_in_memory(1000).unwrap();
            let index = ShardedIndex::from_single(primary);
            Self {
                data_dev,
                redo_dev,
                index,
                alloc,
            }
        }

        fn create_record(&mut self, n: u8, utxo_count: u32) -> TxKey {
            let mut txid = [0u8; 32];
            txid[0] = n;
            let key = TxKey { txid };

            let record_size = TxMetadata::record_size_for(utxo_count);
            let offset = self.alloc.allocate(record_size).unwrap();

            let mut meta = TxMetadata::new(utxo_count);
            meta.tx_id = txid;

            let slots: Vec<UtxoSlot> = (0..utxo_count)
                .map(|i| {
                    let mut h = [0u8; 32];
                    h[0] = i as u8;
                    UtxoSlot::new_unspent(h)
                })
                .collect();

            io::write_full_record(&*self.data_dev, offset, &meta, &slots).unwrap();

            self.index
                .register(
                    key,
                    TxIndexEntry {
                        device_id: 0,
                        record_offset: offset,
                        utxo_count,
                        block_entry_count: 0,
                        tx_flags: 0,
                        spent_utxos: 0,
                        dah_or_preserve: 0,
                        unmined_since: 0,
                        generation: 0,
                    },
                )
                .unwrap();

            key
        }

        fn redo_log(&self) -> RedoLog {
            RedoLog::open(self.redo_dev.clone(), 0, 1024 * 1024).unwrap()
        }

        /// Overwrite a record's primary-index `generation` (keeping its
        /// offset and other fields), so a test can stage a live record whose
        /// generation is higher than a tombstone's (the §8.4 keep case).
        fn set_index_generation(&self, key: &TxKey, generation: u32) {
            let mut ie = self.index.lookup(key).unwrap();
            ie.generation = generation;
            self.index.register(*key, ie).unwrap();
        }

        /// Free a record's region in the ALLOCATOR only, leaving the
        /// primary-index entry and the on-device bytes in place. This stages
        /// the exact index/allocator inconsistency recovery can produce: the
        /// index still resurrects the key at `record_offset`, but the
        /// allocator's free-list already considers that region dead. Returns
        /// `(record_offset, record_size)` of the now-already-free region.
        fn free_region_in_allocator_only(&mut self, key: &TxKey, utxo_count: u32) -> (u64, u64) {
            let ie = self.index.lookup(key).unwrap();
            let record_size = TxMetadata::record_size_for(utxo_count);
            self.alloc.free(ie.record_offset, record_size).unwrap();
            (ie.record_offset, record_size)
        }

        /// Write `delete_at_height` / `unmined_since` into a record's
        /// on-device metadata (for secondary-reconcile tests).
        fn set_record_heights(&self, key: &TxKey, dah: u32, unmined: u32) {
            let ie = self.index.lookup(key).unwrap();
            let mut meta = io::read_metadata(&*self.data_dev, ie.record_offset).unwrap();
            meta.delete_at_height = dah;
            meta.unmined_since = unmined;
            io::write_metadata(&*self.data_dev, ie.record_offset, &meta).unwrap();
        }

        /// Return the deterministic UTXO hash `create_record` wrote for a
        /// given slot index.
        fn slot_hash(&self, slot: u32) -> [u8; 32] {
            let mut h = [0u8; 32];
            h[0] = slot as u8;
            h
        }

        /// Corrupt a UTXO slot's bytes on the device so its CRC fails
        /// (simulating an intra-sector tear inside the WAL-protected
        /// window). Flips a byte in the slot's hash field while leaving
        /// the stored CRC unchanged.
        fn corrupt_slot(&self, key: &TxKey, slot: u32) {
            let ie = self.index.lookup(key).unwrap();
            let align = self.data_dev.alignment();
            let slot_off = ie.record_offset + TxMetadata::utxo_slot_offset(slot);
            let aligned = slot_off / align as u64 * align as u64;
            let intra = (slot_off - aligned) as usize;
            let mut buf = AlignedBuf::new(align, align);
            self.data_dev.pread_exact_at(&mut buf, aligned).unwrap();
            // Flip the first hash byte; the stored CRC no longer matches.
            buf[intra] ^= 0xFF;
            self.data_dev.pwrite_all_at(&buf, aligned).unwrap();
        }
    }

    /// B-5: a SpendV2 entry WITHOUT the slot hash (legacy V2) cannot
    /// rebuild a CRC-failing slot — recovery fails closed (fatal). This is
    /// the boot-loop reproduction the fix must avoid for V3 entries.
    #[test]
    fn corrupt_slot_with_legacy_v2_entry_fails_closed() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xE0, 2);

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SpendV2 {
            tx_key: key,
            offset: 0,
            spending_data: [0xAA; 36],
            new_spent_count: 1,
            current_block_height: 1000,
            block_height_retention: 288,
            target_generation: 1,
            updated_at: 10,
            utxo_hash: None, // legacy V2: no hash, unrepairable
        })
        .unwrap();
        drop(redo);

        h.corrupt_slot(&key, 0);

        let redo = h.redo_log();
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(
            stats.failed_io, 1,
            "legacy V2 entry over a torn slot must fail closed (boot-loop)",
        );
        assert_eq!(stats.entries_replayed, 0);
    }

    /// B-5: a SpendV2 V3 entry carrying the slot hash rebuilds a
    /// CRC-failing slot from the durable redo intent — no fatal brick.
    #[test]
    fn corrupt_slot_with_v3_entry_self_heals() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xE1, 2);
        let hash0 = h.slot_hash(0);

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SpendV2 {
            tx_key: key,
            offset: 0,
            spending_data: [0xAB; 36],
            new_spent_count: 1,
            current_block_height: 1000,
            block_height_retention: 288,
            target_generation: 1,
            updated_at: 10,
            utxo_hash: Some(hash0), // V3: carries the slot hash
        })
        .unwrap();
        drop(redo);

        h.corrupt_slot(&key, 0);

        let redo = h.redo_log();
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.failed_io, 0, "V3 entry must not fail closed");
        assert_eq!(stats.entries_replayed, 1, "torn slot rebuilt and applied");

        // The rebuilt slot reads back SPENT with the correct hash and
        // spending data.
        let ie = h.index.lookup(&key).unwrap();
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert_eq!(slot.status, UTXO_SPENT);
        assert_eq!(slot.hash, hash0, "rebuilt slot carries the redo-entry hash");
        assert_eq!(slot.spending_data, [0xAB; 36]);
        // The counter recomputed from on-device slots equals 1.
        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        let spent_utxos = meta.spent_utxos;
        assert_eq!(spent_utxos, 1);
    }

    /// B-7: a touched-keys-only secondary reconcile produces the SAME
    /// secondary state as a full primary-index scan, and leaves an
    /// untouched (already-correct) secondary entry in place rather than
    /// re-deriving the whole store.
    #[test]
    fn touched_only_reconcile_matches_full_scan() {
        let mut h = RecoveryTestHarness::new();
        let a = h.create_record(0xA0, 1); // touched, has DAH
        let b = h.create_record(0xA1, 1); // touched, has unmined
        let c = h.create_record(0xA2, 1); // NOT touched, has DAH
        h.set_record_heights(&a, 900, 0);
        h.set_record_heights(&b, 0, 800);
        h.set_record_heights(&c, 950, 0);

        // Redo log touches only A and B.
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Freeze {
            tx_key: a,
            offset: 0,
        })
        .unwrap();
        redo.append_and_flush(RedoOp::Freeze {
            tx_key: b,
            offset: 0,
        })
        .unwrap();
        drop(redo);
        let entries = h.redo_log().recover().unwrap();

        // Reference: a FULL scan over a fresh pair of secondaries. The
        // Freeze replays do not mutate the primary index, so the same
        // `h.index` can drive both passes.
        let mut dah_full = DahBackend::new_in_memory();
        let mut unmined_full = UnminedBackend::new_in_memory();
        recover_entries_with_allocator_collecting_pending_conflicts(
            &*h.data_dev,
            entries.clone(),
            &h.index,
            &mut dah_full,
            &mut unmined_full,
            None,
            None,
            SecondaryReconcile::FullScan,
        )
        .unwrap();
        // Full scan derives all three: A(900)+C(950) in DAH, B(800) unmined.
        let sort_keys = |v: &mut Vec<TxKey>| v.sort_by_key(|k| k.txid);
        let mut dah_full_keys = dah_full.range_query(u32::MAX);
        sort_keys(&mut dah_full_keys);
        assert_eq!(dah_full_keys.len(), 2, "full scan finds A and C in DAH");

        // Touched-only: start from durable secondaries that already hold
        // the correct entries for ALL keys (as a clean redb load would),
        // then reconcile only A and B.
        let mut dah_touch = DahBackend::new_in_memory();
        let mut unmined_touch = UnminedBackend::new_in_memory();
        dah_touch.insert(900, a, None).unwrap();
        dah_touch.insert(950, c, None).unwrap();
        unmined_touch.insert(800, b, None).unwrap();
        recover_entries_with_allocator_collecting_pending_conflicts(
            &*h.data_dev,
            entries.clone(),
            &h.index,
            &mut dah_touch,
            &mut unmined_touch,
            None,
            None,
            SecondaryReconcile::TouchedOnly,
        )
        .unwrap();

        // Equivalence: the touched-only result matches the full scan.
        let mut dah_touch_keys = dah_touch.range_query(u32::MAX);
        sort_keys(&mut dah_touch_keys);
        assert_eq!(
            dah_touch_keys.iter().map(|k| k.txid).collect::<Vec<_>>(),
            dah_full_keys.iter().map(|k| k.txid).collect::<Vec<_>>(),
            "touched-only DAH must equal full-scan DAH",
        );
        let mut un_full = unmined_full.range_query(u32::MAX);
        sort_keys(&mut un_full);
        let mut un_touch = unmined_touch.range_query(u32::MAX);
        sort_keys(&mut un_touch);
        assert_eq!(
            un_touch.iter().map(|k| k.txid).collect::<Vec<_>>(),
            un_full.iter().map(|k| k.txid).collect::<Vec<_>>(),
            "touched-only unmined must equal full-scan",
        );
        // C is still present in the touched-only DAH even though it was
        // never scanned — proving the reconcile is O(redo), not O(store).
        assert!(
            dah_touch_keys.iter().any(|k| k.txid == c.txid),
            "untouched C preserved",
        );
    }

    /// B-6: the recovery-progress marker append is non-fatal on a full
    /// log. `mark_recovery_progress_non_fatal` returns Ok even when the
    /// underlying append hits `LogFull`.
    #[test]
    fn recovery_progress_marker_non_fatal_on_full_log() {
        // Small dedicated redo log so we can fill it cheaply.
        let dev = Arc::new(MemoryDevice::new(64 * 1024, 4096).unwrap());
        let mut log = RedoLog::open(dev.clone(), 0, 64 * 1024).unwrap();

        // Fill to within a hair of capacity.
        let mut last_seq = 0;
        loop {
            match log.append_and_flush(RedoOp::Freeze {
                tx_key: TxKey { txid: [7u8; 32] },
                offset: 0,
            }) {
                Ok(seq) => last_seq = seq,
                Err(crate::redo::RedoError::LogFull { .. }) => break,
                Err(e) => panic!("unexpected redo error: {e:?}"),
            }
        }
        assert!(last_seq > 0, "log should have accepted some entries");
        // A direct marker append now fails LogFull.
        assert!(matches!(
            log.mark_recovery_progress(last_seq),
            Err(crate::redo::RedoError::LogFull { .. })
        ));
        // But the non-fatal wrapper swallows it.
        let r = mark_recovery_progress_non_fatal(&mut log, last_seq);
        assert!(
            r.is_ok(),
            "marker append must be non-fatal on a full log: {r:?}"
        );
    }

    /// B-6: a full recovery on a near-full redo log completes instead of
    /// aborting with LogFull when the final progress marker cannot be
    /// appended.
    #[test]
    fn recovery_completes_on_full_log() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xB6, 1);

        // Use a small redo log device so we can fill it.
        let redo_dev = Arc::new(MemoryDevice::new(64 * 1024, 4096).unwrap());
        let mut log = RedoLog::open(redo_dev.clone(), 0, 64 * 1024).unwrap();

        // One genuinely replayable, progress-safe entry.
        log.append_and_flush(RedoOp::Freeze {
            tx_key: key,
            offset: 0,
        })
        .unwrap();

        // Pad the rest of the log to capacity so the end-of-recovery
        // marker append will hit LogFull.
        loop {
            match log.append_and_flush(RedoOp::Freeze {
                tx_key: key,
                offset: 0,
            }) {
                Ok(_) => {}
                Err(crate::redo::RedoError::LogFull { .. }) => break,
                Err(e) => panic!("unexpected redo error: {e:?}"),
            }
        }

        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        // Must NOT return Err(LogFull); recovery must finish.
        let result = recover_all_with_allocator_collecting_pending_conflicts_progress(
            &*h.data_dev,
            &mut log,
            &h.index,
            &mut dah,
            &mut unmined,
            Some(&mut h.alloc),
            true,
        );
        let (stats, _pending, _deleted) =
            result.expect("recovery must complete on a full log, not abort with LogFull");
        // The freeze entries replayed/skipped; none failed fatally.
        assert_eq!(stats.failed_io, 0);
        assert_eq!(stats.failed_corrupt, 0);
    }

    /// AUDIT M2.7 — the offline repair pass also rebuilds torn slots covered by
    /// FreezeV2 / UnfreezeV2 entries, matching the M1.4 self-heal in `recover`.
    #[test]
    fn repair_torn_slots_rebuilds_freeze_and_unfreeze() {
        let mut h = RecoveryTestHarness::new();
        let key_freeze = h.create_record(0xF5, 2);
        let key_unfreeze = h.create_record(0xF6, 2);
        let freeze_hash = [0x55u8; 32];
        let unfreeze_hash = [0x66u8; 32];

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::FreezeV2 {
            tx_key: key_freeze,
            offset: 0,
            utxo_hash: freeze_hash,
        })
        .unwrap();
        redo.append_and_flush(RedoOp::UnfreezeV2 {
            tx_key: key_unfreeze,
            offset: 0,
            utxo_hash: unfreeze_hash,
        })
        .unwrap();
        drop(redo);

        h.corrupt_slot(&key_freeze, 0);
        h.corrupt_slot(&key_unfreeze, 0);

        let redo = h.redo_log();
        let report = repair_torn_slots(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(report.entries_scanned, 2);
        assert_eq!(
            report.slots_repaired, 2,
            "freeze + unfreeze torn slots rebuilt"
        );
        assert!(
            report.unrecoverable.is_empty(),
            "V2 freeze/unfreeze entries always carry the hash"
        );

        let fie = h.index.lookup(&key_freeze).unwrap();
        let fslot = io::read_utxo_slot(&*h.data_dev, fie.record_offset, 0).unwrap();
        assert_eq!(fslot.status, UTXO_FROZEN);
        assert_eq!(fslot.hash, freeze_hash);

        let uie = h.index.lookup(&key_unfreeze).unwrap();
        let uslot = io::read_utxo_slot(&*h.data_dev, uie.record_offset, 0).unwrap();
        assert_eq!(uslot.status, UTXO_UNSPENT);
        assert_eq!(uslot.hash, unfreeze_hash);
    }

    /// B-5: the offline repair pass rebuilds a torn slot from a V3 entry
    /// and reports a torn slot covered only by a legacy V2 entry as
    /// unrecoverable.
    #[test]
    fn repair_torn_slots_rebuilds_v3_and_reports_v2() {
        let mut h = RecoveryTestHarness::new();
        let key_v3 = h.create_record(0xF0, 2);
        let key_v2 = h.create_record(0xF1, 2);
        let hash_v3 = h.slot_hash(0);

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SpendV2 {
            tx_key: key_v3,
            offset: 0,
            spending_data: [0x11; 36],
            new_spent_count: 1,
            current_block_height: 1000,
            block_height_retention: 288,
            target_generation: 1,
            updated_at: 10,
            utxo_hash: Some(hash_v3),
        })
        .unwrap();
        redo.append_and_flush(RedoOp::SpendV2 {
            tx_key: key_v2,
            offset: 0,
            spending_data: [0x22; 36],
            new_spent_count: 1,
            current_block_height: 1000,
            block_height_retention: 288,
            target_generation: 1,
            updated_at: 10,
            utxo_hash: None, // legacy — unrepairable
        })
        .unwrap();
        drop(redo);

        h.corrupt_slot(&key_v3, 0);
        h.corrupt_slot(&key_v2, 0);

        let redo = h.redo_log();
        let report = repair_torn_slots(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(report.entries_scanned, 2);
        assert_eq!(report.slots_repaired, 1, "V3 slot rebuilt");
        assert_eq!(
            report.unrecoverable,
            vec![(key_v2.txid, 0)],
            "legacy V2 torn slot reported unrecoverable",
        );

        // The repaired V3 slot now reads back cleanly as SPENT.
        let ie = h.index.lookup(&key_v3).unwrap();
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert_eq!(slot.status, UTXO_SPENT);
        assert_eq!(slot.hash, hash_v3);
        // The V2 slot is still corrupt (untouched).
        let ie2 = h.index.lookup(&key_v2).unwrap();
        assert!(io::read_utxo_slot(&*h.data_dev, ie2.record_offset, 0).is_err());
    }

    /// B-5: an UnspendV2 V3 entry rebuilds a CRC-failing slot to UNSPENT.
    #[test]
    fn corrupt_slot_with_v3_unspend_self_heals() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xE2, 2);
        let hash0 = h.slot_hash(0);

        // Spend slot 0 durably first so unspend has a SPENT slot intent.
        let spent = UtxoSlot::new_spent(hash0, [0xCD; 36]);
        let ie = h.index.lookup(&key).unwrap();
        io::write_utxo_slot(&*h.data_dev, ie.record_offset, 0, &spent).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::UnspendV2 {
            tx_key: key,
            offset: 0,
            spending_data: [0xCD; 36],
            new_spent_count: 0,
            current_block_height: 1000,
            block_height_retention: 288,
            target_generation: 2,
            updated_at: 20,
            utxo_hash: Some(hash0),
        })
        .unwrap();
        drop(redo);

        h.corrupt_slot(&key, 0);

        let redo = h.redo_log();
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.failed_io, 0);
        assert_eq!(stats.entries_replayed, 1);

        let ie = h.index.lookup(&key).unwrap();
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
        assert_eq!(slot.hash, hash0);
    }

    #[test]
    fn append_conflicting_child_recovery_replays_pending_intent() {
        let mut h = RecoveryTestHarness::new();
        let parent_key = h.create_record(0xD0, 1);
        let child_txid = [0xD1; 32];

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::AppendConflictingChild {
            parent_key,
            child_txid,
        })
        .unwrap();

        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        let (stats, pending, _deleted) = recover_all_with_allocator_collecting_pending_conflicts(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah,
            &mut unmined,
            Some(&mut h.alloc),
        )
        .unwrap();

        assert_eq!(stats.entries_replayed, 0);
        assert_eq!(stats.entries_skipped, 1);
        assert_eq!(
            pending,
            vec![PendingAppendConflictingChild {
                parent_key,
                child_txid,
                is_remove: false,
            }]
        );

        let data_dev = h.data_dev.clone();
        let engine = Engine::new_with_sharded_index(
            data_dev,
            h.index,
            h.alloc,
            StripedLocks::new(1024),
            dah,
            unmined,
        );

        for intent in &pending {
            engine
                .append_conflicting_child(&intent.parent_key, intent.child_txid)
                .unwrap();
        }
        assert_eq!(
            engine.read_conflicting_children(&parent_key).unwrap(),
            vec![child_txid]
        );

        for intent in &pending {
            engine
                .append_conflicting_child(&intent.parent_key, intent.child_txid)
                .unwrap();
        }
        assert_eq!(
            engine.read_conflicting_children(&parent_key).unwrap(),
            vec![child_txid],
            "draining the same pending intent twice must not duplicate the child",
        );
    }

    /// AUDIT M2.6 — an `AppendDeletedChild` redo entry is collected as a pending
    /// deferred drain and applied post-engine via `append_deleted_child`,
    /// idempotently, restoring the deleted-children audit/defense trail that a
    /// crash between prune and append would otherwise drop.
    #[test]
    fn append_deleted_child_recovery_replays_pending_intent() {
        let mut h = RecoveryTestHarness::new();
        let parent_key = h.create_record(0xDA, 1);
        let child_txid = [0xDB; 32];

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::AppendDeletedChild {
            parent_key,
            child_txid,
        })
        .unwrap();

        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        let (stats, _pending, deleted) = recover_all_with_allocator_collecting_pending_conflicts(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah,
            &mut unmined,
            Some(&mut h.alloc),
        )
        .unwrap();

        // Low-level replay skips it (needs the engine); it is surfaced as a
        // pending deferred drain instead.
        assert_eq!(stats.entries_replayed, 0);
        assert_eq!(stats.entries_skipped, 1);
        assert_eq!(
            deleted,
            vec![PendingAppendDeletedChild {
                parent_key,
                child_txid,
            }]
        );

        let data_dev = h.data_dev.clone();
        let engine = Engine::new_with_sharded_index(
            data_dev,
            h.index,
            h.alloc,
            StripedLocks::new(1024),
            dah,
            unmined,
        );

        for intent in &deleted {
            engine
                .append_deleted_child(&intent.parent_key, intent.child_txid)
                .unwrap();
        }
        assert_eq!(
            engine.read_deleted_children(&parent_key).unwrap(),
            vec![child_txid],
        );

        // Idempotent: draining the same intent again must not duplicate.
        for intent in &deleted {
            engine
                .append_deleted_child(&intent.parent_key, intent.child_txid)
                .unwrap();
        }
        assert_eq!(
            engine.read_deleted_children(&parent_key).unwrap(),
            vec![child_txid],
            "draining the same deleted-child intent twice must not duplicate",
        );
    }

    /// R-010 (BC-04) / B-4: the per-entry `new_spent_count` carried in
    /// `RedoOp::Spend` and `RedoOp::Unspend` is computed from a
    /// pre-lock `engine.lookup` snapshot, so concurrent batches on the
    /// same record can compute conflicting absolute counts and persist
    /// redo entries whose `new_spent_count` is wrong by the time
    /// replay runs. Replay must therefore re-derive the counter from
    /// on-device state — recomputing it as the number of SPENT slots
    /// after writing the slot transition — rather than overwriting
    /// `meta.spent_utxos` with the redo entry's snapshot.
    #[test]
    fn replay_spend_rederives_counter_ignoring_redo_snapshot() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xA0, 5);
        let ie = h.index.lookup(&key).unwrap();

        // On-device truth: slots 1,2,3 already SPENT (3 spent), slot 0
        // about to be spent by the redo entry below. Replay must IGNORE
        // the redo snapshot's `new_spent_count = 99` and instead recompute
        // the counter from the slots: after applying the spend of slot 0
        // there are 4 SPENT slots. The metadata counter is deliberately
        // stamped to a WRONG value (3) to prove the recompute does not
        // trust the on-device counter either.
        for i in 1..4u32 {
            let s = io::read_utxo_slot(&*h.data_dev, ie.record_offset, i).unwrap();
            let spent = UtxoSlot::new_spent(s.hash, [0x11; 36]);
            io::write_utxo_slot(&*h.data_dev, ie.record_offset, i, &spent).unwrap();
        }
        let mut prior_meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        prior_meta.spent_utxos = 99;
        io::write_metadata(&*h.data_dev, ie.record_offset, &prior_meta).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: [0xCD; 36],
            new_spent_count: 99, // intentionally wrong
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let post_meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!(
            { post_meta.spent_utxos },
            4,
            "replay must recompute spent_utxos from the SPENT-slot count, not trust the redo snapshot's 99"
        );
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert!(slot.is_spent());
    }

    /// Companion to `replay_spend_rederives_counter_ignoring_redo_snapshot`
    /// for the unspend path. The redo entry carries
    /// `new_spent_count = 99` (wrong); replay must recompute the counter
    /// from the SPENT-slot count after unspending slot 0.
    #[test]
    fn replay_unspend_rederives_counter_ignoring_redo_snapshot() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xA1, 5);
        let ie = h.index.lookup(&key).unwrap();

        // On-device truth: all 5 slots SPENT. After unspending slot 0 the
        // recomputed counter must be 4 (the four remaining SPENT slots).
        // The metadata counter is stamped to a WRONG value (99) to prove
        // replay does not trust it.
        for i in 0..5u32 {
            let s = io::read_utxo_slot(&*h.data_dev, ie.record_offset, i).unwrap();
            let spent = UtxoSlot::new_spent(s.hash, [0xEE; 36]);
            io::write_utxo_slot(&*h.data_dev, ie.record_offset, i, &spent).unwrap();
        }
        let mut prior_meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        prior_meta.spent_utxos = 99;
        io::write_metadata(&*h.data_dev, ie.record_offset, &prior_meta).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Unspend {
            tx_key: key,
            offset: 0,
            spending_data: Some([0xEE; 36]),
            new_spent_count: 99, // intentionally wrong
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let post_meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!(
            { post_meta.spent_utxos },
            4,
            "replay must recompute spent_utxos from the SPENT-slot count, not trust the redo snapshot's 99"
        );
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert!(slot.is_unspent());
    }

    /// B-4 (HIGH): a spend→unspend→respend (reorg) history that is ALREADY
    /// fully applied on-device before the crash must replay idempotently —
    /// `spent_utxos` must equal the true SPENT-slot count (1), not drift up
    /// by `+1` per cycle. Before the fix (incremental `±1`), replaying the
    /// three entries against the already-applied state yields 2 (drift +1).
    #[test]
    fn replay_spend_unspend_respend_history_does_not_drift_counter() {
        let mut h = RecoveryTestHarness::new();
        // 2-slot record. The reorg history acts on slot 0; slot 1 stays
        // UNSPENT throughout (the "live UTXO").
        let key = h.create_record(0xB4, 2);
        let ie = h.index.lookup(&key).unwrap();

        // On-device truth (all three entries already applied before crash):
        // slot 0 = SPENT with spending_data B, slot 1 = UNSPENT, counter = 1.
        let slot0 = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        let spent_b = UtxoSlot::new_spent(slot0.hash, [0xBB; 36]);
        io::write_utxo_slot(&*h.data_dev, ie.record_offset, 0, &spent_b).unwrap();
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.spent_utxos = 1;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();

        // Redo log: Spend(A) -> Unspend(A) -> Spend(B). All three already
        // reflected on-device, so replaying them must not change the counter.
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: [0xAA; 36],
            new_spent_count: 1,
        })
        .unwrap();
        redo.append_and_flush(RedoOp::Unspend {
            tx_key: key,
            offset: 0,
            spending_data: Some([0xAA; 36]),
            new_spent_count: 0,
        })
        .unwrap();
        redo.append_and_flush(RedoOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: [0xBB; 36],
            new_spent_count: 1,
        })
        .unwrap();

        recover(&*h.data_dev, &redo, &h.index).unwrap();

        let post_meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!(
            { post_meta.spent_utxos },
            1,
            "spend->unspend->respend replay must recompute the counter to the true SPENT-slot count (1), not drift",
        );
        // Slot states converge to the final history state.
        let s0 = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        let s1 = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 1).unwrap();
        assert!(s0.is_spent());
        assert!(s1.is_unspent());
    }

    /// B-4: replaying the FULL spend/unspend/respend log twice (e.g. a
    /// crash mid-recovery re-runs the whole log on top of a state that
    /// already reflects a prefix) must converge to the identical final
    /// state — counter, slot statuses, and `delete_at_height`.
    #[test]
    fn replay_spend_history_double_replay_is_idempotent() {
        let build = || {
            let mut h = RecoveryTestHarness::new();
            let key = h.create_record(0xB5, 2);
            // Record has blocks + on longest chain so DAH would be set IF
            // (and only if) all slots were spent.
            let ie = h.index.lookup(&key).unwrap();
            let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
            meta.block_entry_count = 1;
            meta.block_entries_inline[0] = BlockEntry {
                block_id: 1,
                block_height: 900,
                subtree_idx: 0,
            };
            io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();
            h.index
                .update_cached_fields(&key, 0, 1, 0, 0, 0, 0)
                .unwrap();
            (h, key)
        };

        let make_log = |h: &RecoveryTestHarness, key: TxKey| {
            let mut redo = h.redo_log();
            redo.append_and_flush(RedoOp::SpendV2 {
                tx_key: key,
                offset: 0,
                spending_data: [0xAA; 36],
                new_spent_count: 1,
                current_block_height: 1000,
                block_height_retention: 288,
                target_generation: 1,
                updated_at: 10,
                utxo_hash: None,
            })
            .unwrap();
            redo.append_and_flush(RedoOp::UnspendV2 {
                tx_key: key,
                offset: 0,
                spending_data: [0xAA; 36],
                new_spent_count: 0,
                current_block_height: 1000,
                block_height_retention: 288,
                target_generation: 2,
                updated_at: 20,
                utxo_hash: None,
            })
            .unwrap();
            redo.append_and_flush(RedoOp::SpendV2 {
                tx_key: key,
                offset: 0,
                spending_data: [0xBB; 36],
                new_spent_count: 1,
                current_block_height: 1000,
                block_height_retention: 288,
                target_generation: 3,
                updated_at: 30,
                utxo_hash: None,
            })
            .unwrap();
            redo
        };

        // Pass 1: replay once.
        let (h, key) = build();
        let ie = h.index.lookup(&key).unwrap();
        let offset = ie.record_offset;
        let redo = make_log(&h, key);
        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        recover_all(&*h.data_dev, &redo, &h.index, &mut dah, &mut unmined).unwrap();
        let after_one = io::read_metadata(&*h.data_dev, offset).unwrap();

        // Pass 2: replay the same full log AGAIN on top of the post-pass-1
        // state (simulating a crash that forces a from-scratch re-replay).
        let redo2 = make_log(&h, key);
        let mut dah2 = DahBackend::new_in_memory();
        let mut unmined2 = UnminedBackend::new_in_memory();
        recover_all(&*h.data_dev, &redo2, &h.index, &mut dah2, &mut unmined2).unwrap();
        let after_two = io::read_metadata(&*h.data_dev, offset).unwrap();

        assert_eq!(
            { after_one.spent_utxos },
            { after_two.spent_utxos },
            "double replay must not drift spent_utxos",
        );
        assert_eq!({ after_one.spent_utxos }, 1);
        assert_eq!(
            { after_one.delete_at_height },
            { after_two.delete_at_height },
            "double replay must produce the same delete_at_height",
        );
        // slot 1 is still live (unspent) → record is NOT all-spent → no DAH.
        assert_eq!(
            { after_two.delete_at_height },
            0,
            "a record with a live UTXO must never get delete_at_height stamped on replay",
        );
        let s1 = io::read_utxo_slot(&*h.data_dev, offset, 1).unwrap();
        assert!(s1.is_unspent());
    }

    /// B-4: a record left partially-spent after replay (one live UTXO) must
    /// not get `delete_at_height` set, even if a stale/over-counted metadata
    /// counter would have satisfied the all-spent condition pre-fix.
    #[test]
    fn replay_partially_spent_record_does_not_get_dah() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xB6, 2);
        let ie = h.index.lookup(&key).unwrap();

        // Record has blocks + on longest chain. Pre-stamp an OVER-COUNTED
        // counter (2 == utxo_count) that would falsely read "all spent",
        // while only slot 0 is actually spent and slot 1 is live.
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.block_entry_count = 1;
        meta.block_entries_inline[0] = BlockEntry {
            block_id: 1,
            block_height: 900,
            subtree_idx: 0,
        };
        meta.spent_utxos = 2; // over-counted: pretends all-spent
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();
        h.index
            .update_cached_fields(&key, 0, 1, 0, 0, 0, 0)
            .unwrap();

        // Spend only slot 0 via replay. Recompute must yield 1 (not 2), so
        // the all-spent condition is NOT satisfied and no DAH is stamped.
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SpendV2 {
            tx_key: key,
            offset: 0,
            spending_data: [0xAA; 36],
            new_spent_count: 2, // wrong snapshot claiming all-spent
            current_block_height: 1000,
            block_height_retention: 288,
            target_generation: 5,
            updated_at: 50,
            utxo_hash: None,
        })
        .unwrap();

        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        recover_all(&*h.data_dev, &redo, &h.index, &mut dah, &mut unmined).unwrap();

        let post = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!(
            { post.spent_utxos },
            1,
            "recompute must reflect only slot 0 spent"
        );
        assert_eq!(
            { post.delete_at_height },
            0,
            "partially-spent record must not get delete_at_height",
        );
        assert!(
            !post.flags.contains(TxFlags::LAST_SPENT_ALL),
            "LAST_SPENT_ALL must not be set while a UTXO is live",
        );
        assert!(dah.range_query(u32::MAX).is_empty(), "no DAH index entry");
        let s1 = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 1).unwrap();
        assert!(s1.is_unspent());
    }

    #[test]
    fn replay_unspend_rejects_wrong_spending_data_without_clearing_slot() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xA2, 1);
        let ie = h.index.lookup(&key).unwrap();

        let slot0 = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        let stored_spending_data = [0x11; 36];
        let spent = UtxoSlot::new_spent(slot0.hash, stored_spending_data);
        io::write_utxo_slot(&*h.data_dev, ie.record_offset, 0, &spent).unwrap();
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.spent_utxos = 1;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Unspend {
            tx_key: key,
            offset: 0,
            spending_data: Some([0x22; 36]),
            new_spent_count: 0,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_skipped, 1);

        let post_slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert!(post_slot.is_spent());
        assert_eq!(post_slot.spending_data, stored_spending_data);
        let post_meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!({ post_meta.spent_utxos }, 1);
    }

    #[test]
    fn replay_unspend_rejects_wrong_hash_without_clearing_slot() {
        // F-A1: a UnspendV2 redo entry whose `utxo_hash` no longer matches the
        // on-disk spent slot is replaying an unspend the live engine rejected
        // (ERR_UTXO_HASH_MISMATCH). Recovery must skip it and leave the slot
        // SPENT — otherwise a rejected unspend becomes a durable un-spend after
        // crash, re-opening an already-spent UTXO (double-spend risk).
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xA3, 1);
        let ie = h.index.lookup(&key).unwrap();

        let slot0 = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        let real_hash = slot0.hash;
        let stored_spending_data = [0x11; 36];
        let spent = UtxoSlot::new_spent(real_hash, stored_spending_data);
        io::write_utxo_slot(&*h.data_dev, ie.record_offset, 0, &spent).unwrap();
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.spent_utxos = 1;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();

        let wrong_hash = [0xEE; 32];
        assert_ne!(real_hash, wrong_hash);
        let mut redo = h.redo_log();
        // Correct spending_data, WRONG hash — exactly what the live engine
        // rejects with ERR_UTXO_HASH_MISMATCH before mutating.
        redo.append_and_flush(RedoOp::UnspendV2 {
            tx_key: key,
            offset: 0,
            spending_data: stored_spending_data,
            new_spent_count: 0,
            current_block_height: 1000,
            block_height_retention: 288,
            target_generation: 2,
            updated_at: 0,
            utxo_hash: Some(wrong_hash),
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_skipped, 1);

        // Slot remains SPENT with its real hash and spending data; counter unchanged.
        let post_slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert!(post_slot.is_spent());
        assert_eq!(post_slot.hash, real_hash);
        assert_eq!(post_slot.spending_data, stored_spending_data);
        let post_meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!({ post_meta.spent_utxos }, 1);
    }

    #[test]
    fn crash_between_redo_and_data_write_spend() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(1, 5);
        let ie = h.index.lookup(&key).unwrap();

        // Simulate: redo logged but data NOT written (crash before pwrite)
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: [0xAB; 36],
            new_spent_count: 1,
        })
        .unwrap();

        // Slot is still unspent (crash prevented the data write)
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert!(slot.is_unspent());

        // Recovery replays the spend
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);
        assert_eq!(stats.entries_skipped, 0);

        // Now slot is spent
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert!(slot.is_spent());
        assert_eq!(slot.spending_data, [0xAB; 36]);
    }

    #[test]
    fn crash_between_redo_and_data_write_set_mined() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(2, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SetMined {
            tx_key: key,
            block_id: 42,
            block_height: 1000,
            subtree_idx: 7,
            unset: false,
        })
        .unwrap();

        // Block entry not yet written
        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!(meta.block_entry_count, 0);

        // Recovery replays
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!(meta.block_entry_count, 1);
        assert_eq!({ meta.block_entries_inline[0].block_id }, 42);
    }

    #[test]
    fn no_crash_entries_already_applied() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(3, 5);
        let ie = h.index.lookup(&key).unwrap();

        // Actually apply the spend to data
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        let spent = UtxoSlot::new_spent(slot.hash, [0xAB; 36]);
        io::write_utxo_slot(&*h.data_dev, ie.record_offset, 0, &spent).unwrap();

        // Also log it
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: [0xAB; 36],
            new_spent_count: 1,
        })
        .unwrap();

        // Recovery sees it's already applied
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 0);
        assert_eq!(stats.entries_skipped, 1);
    }

    #[test]
    fn idempotent_spend_counter_once() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(4, 5);

        let mut redo = h.redo_log();
        // Log the same spend twice
        redo.append_and_flush(RedoOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: [0xAB; 36],
            new_spent_count: 1,
        })
        .unwrap();
        redo.append_and_flush(RedoOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: [0xAB; 36],
            new_spent_count: 1,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        // First is applied, second is skipped (idempotent)
        assert_eq!(stats.entries_replayed, 1);
        assert_eq!(stats.entries_skipped, 1);

        let ie = h.index.lookup(&key).unwrap();
        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!({ meta.spent_utxos }, 1);
    }

    #[test]
    fn idempotent_set_mined() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(5, 5);

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SetMined {
            tx_key: key,
            block_id: 10,
            block_height: 100,
            subtree_idx: 0,
            unset: false,
        })
        .unwrap();
        redo.append_and_flush(RedoOp::SetMined {
            tx_key: key,
            block_id: 10,
            block_height: 100,
            subtree_idx: 0,
            unset: false,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);
        assert_eq!(stats.entries_skipped, 1);

        let ie = h.index.lookup(&key).unwrap();
        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!(meta.block_entry_count, 1);
    }

    #[test]
    fn idempotent_create() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(6, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::ReplicaCreate {
            tx_key: key,
            record_offset: ie.record_offset,
            utxo_count: 5,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_skipped, 1); // Already in index
    }

    /// Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md) part 4:
    /// `RedoOp::Create` carries the full record bytes; replay must
    /// reconstruct the on-device record byte-for-byte and register a
    /// correctly-populated index entry. Simulates the
    /// `redo-fsynced-but-engine-write-lost` boundary by writing the
    /// Create entry to the log, leaving the device area untouched
    /// (zeroed), and asserting that recovery writes the full record
    /// PROOF: multi-store recovery routes each Create to its own store.
    /// Two stores, two records (device_id 0 and 1, each with a store-local
    /// offset). A single shared redo log holds both Creates. Recovery must
    /// reconstruct each record on the RIGHT store's device and register the
    /// correct device_id — the gate single-store recovery cannot exercise.
    #[test]
    fn multi_store_recovery_routes_creates_to_their_store() {
        let dev0: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let dev1: Arc<dyn BlockDevice> =
            Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut alloc0 = SlotAllocator::new(dev0.clone()).unwrap();
        let mut alloc1 = SlotAllocator::new(dev1.clone()).unwrap();
        let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1000).unwrap());

        // Build a record's bytes + allocate its region on the given store.
        let build = |txid_byte: u8, alloc: &mut SlotAllocator| -> (TxKey, u64, Vec<u8>) {
            let utxo_count: u32 = 3;
            let mut txid = [0u8; 32];
            txid[0] = txid_byte;
            let mut meta = TxMetadata::new(utxo_count);
            meta.tx_id = txid;
            meta.tx_version = 7;
            let base = TxMetadata::record_size_for(utxo_count);
            meta.record_size = base as u32;
            let slots: Vec<UtxoSlot> = (0..utxo_count)
                .map(|i| {
                    let mut h = [0u8; 32];
                    h[0] = txid_byte;
                    h[1] = (i + 1) as u8;
                    UtxoSlot::new_unspent(h)
                })
                .collect();
            let offset = alloc.allocate(base).unwrap();
            let mut rb = Vec::with_capacity(METADATA_SIZE + slots.len() * UTXO_SLOT_SIZE);
            let mut mb = [0u8; METADATA_SIZE];
            meta.to_bytes(&mut mb);
            rb.extend_from_slice(&mb);
            for s in &slots {
                let mut sb = [0u8; UTXO_SLOT_SIZE];
                s.to_bytes(&mut sb);
                rb.extend_from_slice(&sb);
            }
            (TxKey { txid }, offset, rb)
        };

        let (key_a, off_a, rb_a) = build(0xA0, &mut alloc0); // store 0
        let (key_b, off_b, rb_b) = build(0xB0, &mut alloc1); // store 1

        // One shared redo log carries both creates (interleaved by store).
        let mut redo = RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();
        redo.append_and_flush(RedoOp::Create {
            tx_key: key_a,
            device_id: 0,
            record_offset: off_a,
            utxo_count: 3,
            is_conflicting: false,
            record_bytes: rb_a.clone(),
            parent_txids: Vec::new(),
        })
        .unwrap();
        redo.append_and_flush(RedoOp::Create {
            tx_key: key_b,
            device_id: 1,
            record_offset: off_b,
            utxo_count: 3,
            is_conflicting: false,
            record_bytes: rb_b.clone(),
            parent_txids: Vec::new(),
        })
        .unwrap();

        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        let devices = [dev0.clone(), dev1.clone()];
        let mut allocators = [alloc0, alloc1];
        let (stats, _, _) = recover_all_multi_store(
            &devices,
            &mut allocators,
            &mut redo,
            &index,
            &mut dah,
            &mut unmined,
            true,
        )
        .unwrap();
        assert_eq!(stats.entries_replayed, 2, "both creates must replay");
        assert_eq!(stats.entries_failed, 0);

        // Index records the correct store + offset for each key.
        let ea = index.lookup(&key_a).expect("A registered");
        let eb = index.lookup(&key_b).expect("B registered");
        assert_eq!(ea.device_id, 0, "A must be on store 0");
        assert_eq!(eb.device_id, 1, "B must be on store 1");
        assert_eq!(ea.record_offset, off_a);
        assert_eq!(eb.record_offset, off_b);

        // Each record was reconstructed on its OWN store's device, and NOT on
        // the other store — this is the routing proof.
        let meta_a = io::read_metadata(&*dev0, off_a).expect("A on dev0");
        let meta_b = io::read_metadata(&*dev1, off_b).expect("B on dev1");
        assert_eq!(meta_a.tx_id, key_a.txid, "A must read back from store 0");
        assert_eq!(meta_b.tx_id, key_b.txid, "B must read back from store 1");
        // Cross-store: B's bytes must NOT be on store 0 at B's offset, and
        // A's must NOT be on store 1 at A's offset.
        assert_ne!(
            io::read_metadata(&*dev1, off_a).map(|m| m.tx_id).ok(),
            Some(key_a.txid),
            "A must not have been written to store 1"
        );
        assert_ne!(
            io::read_metadata(&*dev0, off_b).map(|m| m.tx_id).ok(),
            Some(key_b.txid),
            "B must not have been written to store 0"
        );
    }

    /// bytes and registers the index with cached fields populated from
    /// the reconstructed metadata header (not zeros).
    #[test]
    fn replay_create_reconstructs_full_record() {
        // Fresh harness — DO NOT pre-create the record. We will only
        // append a Create redo entry and recover.
        let data_dev = std::sync::Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = std::sync::Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
        let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1000).unwrap());

        let txid = {
            let mut t = [0u8; 32];
            t[0] = 0xCC;
            t
        };
        let key = TxKey { txid };
        let utxo_count: u32 = 4;

        // Construct the metadata + slot bytes that a successful create
        // would have written. Allocate a real region so the offset is
        // valid for the device.
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = txid;
        meta.tx_version = 7;
        meta.fee = 1234;
        meta.spent_utxos = 0;
        meta.flags = TxFlags::IS_COINBASE;
        meta.unmined_since = 12345;
        meta.generation = 0;
        let base_size = TxMetadata::record_size_for(utxo_count);
        meta.record_size = base_size as u32;

        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = (i + 1) as u8;
                UtxoSlot::new_unspent(h)
            })
            .collect();

        let record_offset = alloc.allocate(base_size).unwrap();

        // Build the captured record bytes (no alignment padding — that's
        // added by the device write path inside replay_create).
        let mut record_bytes = Vec::with_capacity(METADATA_SIZE + slots.len() * UTXO_SLOT_SIZE);
        let mut meta_bytes = [0u8; METADATA_SIZE];
        meta.to_bytes(&mut meta_bytes);
        record_bytes.extend_from_slice(&meta_bytes);
        for slot in &slots {
            let mut sb = [0u8; UTXO_SLOT_SIZE];
            slot.to_bytes(&mut sb);
            record_bytes.extend_from_slice(&sb);
        }

        // Open the redo log and append a Create entry.
        let mut redo = RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();
        redo.append_and_flush(RedoOp::Create {
            tx_key: key,
            device_id: 0,
            record_offset,
            utxo_count,
            is_conflicting: false,
            record_bytes: record_bytes.clone(),
            parent_txids: Vec::new(),
        })
        .unwrap();

        // Sanity: the device area is *not* yet populated (allocate
        // doesn't write the record itself; only reserves space). A
        // metadata read should fail or return zeros.
        let _ = io::read_metadata(&*data_dev as &dyn BlockDevice, record_offset);

        // Recover.
        let stats = recover(&*data_dev as &dyn BlockDevice, &redo, &index).unwrap();
        assert_eq!(stats.entries_replayed, 1, "Create must be applied");
        assert_eq!(stats.entries_skipped, 0);
        assert_eq!(stats.entries_failed, 0);

        // The index must now have the entry, with cached fields
        // populated from the reconstructed metadata.
        let recovered = index
            .lookup(&key)
            .expect("Create replay must register the index entry");
        assert_eq!(recovered.record_offset, record_offset);
        assert_eq!(recovered.utxo_count, utxo_count);
        assert_eq!(
            recovered.tx_flags,
            TxFlags::IS_COINBASE.bits(),
            "tx_flags must come from reconstructed metadata, not zero"
        );
        assert_eq!(
            recovered.unmined_since, 12345,
            "unmined_since must come from reconstructed metadata"
        );

        // The on-device bytes must be byte-identical to what a
        // successful create would have written: re-read metadata + each
        // slot and compare.
        let recovered_meta =
            io::read_metadata(&*data_dev as &dyn BlockDevice, record_offset).unwrap();
        assert_eq!({ recovered_meta.tx_version }, 7);
        assert_eq!({ recovered_meta.fee }, 1234);
        assert_eq!({ recovered_meta.utxo_count }, utxo_count);
        assert_eq!(recovered_meta.flags, TxFlags::IS_COINBASE);
        for (i, original_slot) in slots.iter().enumerate() {
            let on_device =
                io::read_utxo_slot(&*data_dev as &dyn BlockDevice, record_offset, i as u32)
                    .unwrap();
            assert_eq!(
                on_device.hash, original_slot.hash,
                "slot {i} hash must match original",
            );
            assert!(
                on_device.is_unspent(),
                "slot {i} must be UNSPENT after replay",
            );
        }
    }

    #[test]
    fn recover_all_rejects_create_offset_not_owned_by_allocator() {
        let data_dev = std::sync::Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = std::sync::Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
        let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1000).unwrap());
        let mut dah = DahBackend::from(crate::index::DahIndex::new());
        let mut unmined = UnminedBackend::from(crate::index::UnminedIndex::new());

        let mut txid = [0u8; 32];
        txid[0] = 0xCD;
        let key = TxKey { txid };
        let utxo_count = 1;
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = txid;
        meta.record_size = TxMetadata::record_size_for(utxo_count) as u32;

        let mut record_bytes = Vec::new();
        let mut meta_bytes = [0u8; METADATA_SIZE];
        meta.to_bytes(&mut meta_bytes);
        record_bytes.extend_from_slice(&meta_bytes);
        let mut slot_bytes = [0u8; UTXO_SLOT_SIZE];
        UtxoSlot::new_unspent([0x44; 32]).to_bytes(&mut slot_bytes);
        record_bytes.extend_from_slice(&slot_bytes);

        let mut redo = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        redo.append_and_flush(RedoOp::Create {
            tx_key: key,
            device_id: 0,
            // DATA_REGION_OFFSET is inside the data area, but this fresh
            // allocator has not replayed/observed any allocation yet.
            record_offset: crate::allocator::DATA_REGION_OFFSET,
            utxo_count,
            is_conflicting: false,
            record_bytes,
            parent_txids: Vec::new(),
        })
        .unwrap();

        let stats = recover_all_with_allocator(
            &*data_dev,
            &redo,
            &index,
            &mut dah,
            &mut unmined,
            Some(&mut alloc),
        )
        .unwrap();
        assert_eq!(stats.entries_failed, 1);
        assert_eq!(stats.failed_logic, 1);
        assert!(
            index.lookup(&key).is_none(),
            "invalid Create offset must not register primary index entry"
        );
    }

    /// Build record B's full on-device bytes (metadata `tx_id = b_txid`,
    /// `utxo_count` unspent slots) at an allocated offset, then FREE that
    /// offset in the allocator only — leaving B's bytes on the device but
    /// the region marked free. Returns the offset. This stages the exact
    /// aliasing precondition: a later legacy `Create` for a DIFFERENT key A
    /// names this offset, which now holds B's record.
    fn write_record_b_then_free_in_allocator(
        data_dev: &MemoryDevice,
        alloc: &mut SlotAllocator,
        b_txid: [u8; 32],
        utxo_count: u32,
    ) -> u64 {
        let record_size = TxMetadata::record_size_for(utxo_count);
        let offset = alloc.allocate(record_size).unwrap();
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = b_txid;
        meta.record_size = record_size as u32;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i as u8;
                UtxoSlot::new_unspent(h)
            })
            .collect();
        io::write_full_record(data_dev, offset, &meta, &slots).unwrap();
        // Free in the allocator ONLY — B's bytes remain on the device.
        alloc.free(offset, record_size).unwrap();
        offset
    }

    /// BUG-1 fix #1: a legacy `RedoOp::ReplicaCreate` whose `record_offset` is NOT
    /// owned by the allocator (it was freed and the bytes there belong to a
    /// DIFFERENT record B) must be rejected by the `is_allocated_range`
    /// gate — exactly like `Create`. Pre-fix this path skipped the gate
    /// and registered A → offset, aliasing B's record so `lookup(A)` read
    /// B's bytes.
    #[test]
    fn recover_all_rejects_legacy_create_offset_not_owned_by_allocator() {
        let data_dev = std::sync::Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = std::sync::Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
        let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1000).unwrap());
        let mut dah = DahBackend::from(crate::index::DahIndex::new());
        let mut unmined = UnminedBackend::from(crate::index::UnminedIndex::new());

        let utxo_count = 2;
        // Record B occupies the offset on device; the region is then freed
        // in the allocator. B's tx_id starts with 0xBB.
        let mut b_txid = [0u8; 32];
        b_txid[0] = 0xBB;
        let offset =
            write_record_b_then_free_in_allocator(&data_dev, &mut alloc, b_txid, utxo_count);

        // Key A (≠ B) names B's now-freed offset via a legacy Create.
        let mut a_txid = [0u8; 32];
        a_txid[0] = 0xAA;
        let key_a = TxKey { txid: a_txid };

        let mut redo = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        redo.append_and_flush(RedoOp::ReplicaCreate {
            tx_key: key_a,
            record_offset: offset,
            utxo_count,
        })
        .unwrap();

        let stats = recover_all_with_allocator(
            &*data_dev,
            &redo,
            &index,
            &mut dah,
            &mut unmined,
            Some(&mut alloc),
        )
        .unwrap();

        // The allocator gate fails the entry as a logic error.
        assert_eq!(stats.entries_failed, 1);
        assert_eq!(stats.failed_logic, 1);
        // A must NOT be registered — no A → offset aliasing.
        assert!(
            index.lookup(&key_a).is_none(),
            "legacy Create on a freed offset must not register an aliasing index entry"
        );
    }

    /// BUG-1 fix #2: even if the allocator still owns the offset (so the
    /// `is_allocated_range` gate passes), the on-device metadata `tx_id`
    /// MUST match the legacy Create's key. Here the offset holds record B's
    /// bytes (tx_id = B) but the redo Create names key A with a MATCHING
    /// `utxo_count` — the old `meta.utxo_count == redo.utxo_count` guard is
    /// satisfied, so only the tx_id guard can catch the alias.
    #[test]
    fn recover_all_legacy_create_tx_id_guard_rejects_alias() {
        let data_dev = std::sync::Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = std::sync::Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
        let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1000).unwrap());
        let mut dah = DahBackend::from(crate::index::DahIndex::new());
        let mut unmined = UnminedBackend::from(crate::index::UnminedIndex::new());

        let utxo_count = 2;
        // Record B at an offset that STAYS allocated (gate passes).
        let record_size = TxMetadata::record_size_for(utxo_count);
        let offset = alloc.allocate(record_size).unwrap();
        let mut b_txid = [0u8; 32];
        b_txid[0] = 0xBB;
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = b_txid;
        meta.record_size = record_size as u32;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i as u8;
                UtxoSlot::new_unspent(h)
            })
            .collect();
        io::write_full_record(&*data_dev, offset, &meta, &slots).unwrap();

        // Legacy Create for key A (≠ B) with the SAME utxo_count → only the
        // tx_id guard distinguishes it.
        let mut a_txid = [0u8; 32];
        a_txid[0] = 0xAA;
        let key_a = TxKey { txid: a_txid };

        let mut redo = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        redo.append_and_flush(RedoOp::ReplicaCreate {
            tx_key: key_a,
            record_offset: offset,
            utxo_count,
        })
        .unwrap();

        let stats = recover_all_with_allocator(
            &*data_dev,
            &redo,
            &index,
            &mut dah,
            &mut unmined,
            Some(&mut alloc),
        )
        .unwrap();

        assert_eq!(
            stats.entries_failed, 1,
            "tx_id mismatch must fail the legacy Create entry"
        );
        assert!(
            index.lookup(&key_a).is_none(),
            "legacy Create whose on-device tx_id != key must not register (tx_id guard)"
        );
    }

    /// BUG-1 fix #3 (offset-uniqueness), O(N) reverse-map version: a STALE
    /// alias `A → X` carried in from a persisted/snapshot index must be
    /// evicted when the rightful owner `B` (on-device tx_id = B) is replayed
    /// via a legitimate `Create(B, X)`. After recovery ONLY `B → X` survives
    /// and `A` is gone — identical to the prior O(N²) `index.iter()` scan,
    /// but now via the O(1) `offset_owners.get(&offset)` path
    /// (`build_offset_owners` builds the map once, O(N)).
    #[test]
    fn recover_offset_uniqueness_evicts_preexisting_snapshot_alias_via_reverse_map() {
        let data_dev = std::sync::Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = std::sync::Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
        let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1000).unwrap());
        let mut dah = DahBackend::from(crate::index::DahIndex::new());
        let mut unmined = UnminedBackend::from(crate::index::UnminedIndex::new());

        let utxo_count = 2;
        // Offset X holds record B (on-device tx_id = B) and STAYS allocated,
        // so the `is_allocated_range` gate passes and B is the rightful owner.
        let record_size = TxMetadata::record_size_for(utxo_count);
        let offset = alloc.allocate(record_size).unwrap();
        let mut b_txid = [0u8; 32];
        b_txid[0] = 0xBB;
        let key_b = TxKey { txid: b_txid };
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = b_txid;
        meta.generation = 5;
        meta.record_size = record_size as u32;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = (i + 1) as u8;
                UtxoSlot::new_unspent(h)
            })
            .collect();
        io::write_full_record(&*data_dev, offset, &meta, &slots).unwrap();

        // Pre-seed the loaded index with a STALE alias A → X (as if carried
        // in from a persisted/snapshot index). A's cached fields are seeded
        // from B's record image — the wrong owner — which is exactly the
        // corruption offset-uniqueness exists to undo.
        let mut a_txid = [0u8; 32];
        a_txid[0] = 0xAA;
        let key_a = TxKey { txid: a_txid };
        let stale_entry = TxIndexEntry {
            device_id: 0,
            record_offset: offset,
            utxo_count,
            block_entry_count: 0,
            tx_flags: 0,
            spent_utxos: 0,
            dah_or_preserve: 0,
            unmined_since: 0,
            generation: 5,
        };
        index.register(key_a, stale_entry).unwrap();
        assert!(
            index.lookup(&key_a).is_some(),
            "precondition: stale alias A → X is present before recovery"
        );

        // Legitimate legacy Create for the rightful owner B at offset X.
        let mut redo = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        redo.append_and_flush(RedoOp::ReplicaCreate {
            tx_key: key_b,
            record_offset: offset,
            utxo_count,
        })
        .unwrap();

        let stats = recover_all_with_allocator(
            &*data_dev,
            &redo,
            &index,
            &mut dah,
            &mut unmined,
            Some(&mut alloc),
        )
        .unwrap();

        // The rightful owner B registered successfully (the create applied).
        assert_eq!(
            stats.entries_replayed, 1,
            "rightful-owner Create(B, X) must apply"
        );
        assert_eq!(stats.entries_failed, 0);

        // B → X survives.
        let b_entry = index
            .lookup(&key_b)
            .expect("rightful owner B must be registered at offset X");
        assert_eq!(b_entry.record_offset, offset);

        // The stale alias A was evicted — offset-uniqueness restored.
        assert!(
            index.lookup(&key_a).is_none(),
            "stale snapshot alias A → X must be evicted by offset-uniqueness"
        );

        // Exactly one key maps to X across the WHOLE index (the invariant).
        let mut owners_of_x: Vec<TxKey> = Vec::new();
        index.for_each(|k, e| {
            if e.record_offset == offset {
                owners_of_x.push(k);
            }
        });
        assert_eq!(
            owners_of_x,
            vec![key_b],
            "exactly one key (B) may map to offset X after recovery"
        );
    }

    /// BUG-1 fix #4 (R2): a resurrected primary entry that ALIASES another
    /// key's record (index A → offset, but on-device tx_id = B) must be
    /// purged UNCONDITIONALLY by R2 self-purge when a tombstone exists for
    /// A — the generation keep-guard must NOT apply, because the entry's
    /// cached `generation` was seeded from the FOREIGN record B.
    #[test]
    fn recover_tombstones_r2_purges_aliased_entry_unconditionally() {
        // Create record B (on-device tx_id = B) under key B, then RE-POINT
        // the index so key A maps to B's offset and unregister B. The index
        // now holds the alias A → B's record, with A's cached `generation`
        // seeded from B (a HIGH value). A LOW-generation tombstone for A
        // would, under the pre-fix generation keep-guard, be KEPT (tombstone
        // gen 1 < live gen 9). The BUG-1 fix #4 tx_id guard must override
        // that and purge A unconditionally.
        let mut h = RecoveryTestHarness::new();
        let key_b = h.create_record(0xBB, 2);
        // Give B a high generation in the index so the keep-guard WOULD fire.
        h.set_index_generation(&key_b, 9);
        let b_entry = h.index.lookup(&key_b).unwrap();

        let mut a_txid = [0u8; 32];
        a_txid[0] = 0xAA;
        let key_a = TxKey { txid: a_txid };
        // Alias: A → B's offset/entry (carrying B's generation 9).
        h.index.register(key_a, b_entry).unwrap();
        // Remove B so only the aliased A entry remains pointing at the offset.
        h.index.unregister(&key_b);

        let engine = engine_from_harness(h);
        // Sanity: A resolves in the index but the on-device record is B's.
        assert!(engine.lookup_cached(&key_a).is_some());
        assert!(
            matches!(
                engine.read_metadata(&key_a),
                Err(crate::ops::error::SpendError::TxNotFound)
            ),
            "on-device tx_id must NOT match key A (it is B's record)"
        );

        let (mut log, mut idx, _dir) = fresh_tombstone_storage();
        // Tombstone for A at LOW generation 1 — below A's live (aliased) gen 9.
        seed_tombstone(&mut log, &key_a, 1, 150);

        let stats = recover_tombstones(&engine, &log, &mut idx).unwrap();

        assert_eq!(
            stats.kept_newer_generation, 0,
            "aliased entry must NOT be kept by the generation guard (tx_id mismatch)"
        );
        assert_eq!(
            stats.records_self_purged, 1,
            "aliased entry must be purged unconditionally on tx_id mismatch"
        );
        assert!(
            engine.lookup_cached(&key_a).is_none(),
            "aliased entry A must be removed from the index after R2"
        );
    }

    /// Gap #2: replay must be idempotent — running recovery twice over
    /// the same redo log produces the same final state. Verifies the
    /// "primary already registered → skip" path.
    #[test]
    fn replay_create_idempotent_on_double_recovery() {
        let data_dev = std::sync::Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = std::sync::Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
        let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1000).unwrap());

        let txid = {
            let mut t = [0u8; 32];
            t[0] = 0xDD;
            t
        };
        let key = TxKey { txid };
        let utxo_count: u32 = 2;
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = txid;
        let base_size = TxMetadata::record_size_for(utxo_count);
        meta.record_size = base_size as u32;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = i as u8;
                UtxoSlot::new_unspent(h)
            })
            .collect();
        let record_offset = alloc.allocate(base_size).unwrap();

        let mut record_bytes = Vec::with_capacity(METADATA_SIZE + slots.len() * UTXO_SLOT_SIZE);
        let mut meta_bytes = [0u8; METADATA_SIZE];
        meta.to_bytes(&mut meta_bytes);
        record_bytes.extend_from_slice(&meta_bytes);
        for slot in &slots {
            let mut sb = [0u8; UTXO_SLOT_SIZE];
            slot.to_bytes(&mut sb);
            record_bytes.extend_from_slice(&sb);
        }

        let mut redo = RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();
        redo.append_and_flush(RedoOp::Create {
            tx_key: key,
            device_id: 0,
            record_offset,
            utxo_count,
            is_conflicting: false,
            record_bytes: record_bytes.clone(),
            parent_txids: Vec::new(),
        })
        .unwrap();

        // First recovery: applies.
        let stats1 = recover(&*data_dev as &dyn BlockDevice, &redo, &index).unwrap();
        assert_eq!(stats1.entries_replayed, 1);
        assert_eq!(stats1.entries_skipped, 0);

        // Second recovery: skipped (index already has the entry).
        let stats2 = recover(&*data_dev as &dyn BlockDevice, &redo, &index).unwrap();
        assert_eq!(stats2.entries_replayed, 0);
        assert_eq!(stats2.entries_skipped, 1);
    }

    /// R-031 (BC-53) regression: legacy `RedoOp::ReplicaCreate` replay must
    /// read on-device metadata and populate cached index fields from
    /// it, NOT register a zero-filled placeholder. Pre-fix the function
    /// blindly registered an entry with all-zero `tx_flags`,
    /// `spent_utxos`, `dah_or_preserve`, `unmined_since`, `generation`,
    /// and `block_entry_count`, so subsequent fast-path reads returned
    /// stale state for any record whose redo entry was the legacy
    /// variant (e.g. logs written before gap #2 / `Create` landed).
    #[test]
    fn legacy_replay_create_populates_cached_fields_from_metadata() {
        let data_dev = std::sync::Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = std::sync::Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
        let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1000).unwrap());

        let txid = {
            let mut t = [0u8; 32];
            t[0] = 0xEE;
            t
        };
        let key = TxKey { txid };
        let utxo_count: u32 = 3;

        // Write a real on-device record (mimicking the engine path
        // that the legacy Create entry would have followed).
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = txid;
        meta.flags = TxFlags::IS_COINBASE;
        meta.spent_utxos = 0;
        meta.unmined_since = 99_999;
        meta.generation = 17;
        let base_size = TxMetadata::record_size_for(utxo_count);
        meta.record_size = base_size as u32;
        let slots: Vec<UtxoSlot> = (0..utxo_count)
            .map(|i| {
                let mut h = [0u8; 32];
                h[0] = (i + 1) as u8;
                UtxoSlot::new_unspent(h)
            })
            .collect();
        let record_offset = alloc.allocate(base_size).unwrap();
        io::write_full_record(&*data_dev as &dyn BlockDevice, record_offset, &meta, &slots)
            .unwrap();

        // Append a LEGACY Create entry (no record_bytes) and recover.
        let mut redo = RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();
        redo.append_and_flush(RedoOp::ReplicaCreate {
            tx_key: key,
            record_offset,
            utxo_count,
        })
        .unwrap();

        let stats = recover(&*data_dev as &dyn BlockDevice, &redo, &index).unwrap();
        assert_eq!(stats.entries_replayed, 1);
        assert_eq!(stats.entries_failed, 0);

        // Cached fields MUST come from the on-device metadata — not zeros.
        let recovered = index
            .lookup(&key)
            .expect("legacy Create replay must register the index entry");
        assert_eq!(recovered.utxo_count, utxo_count);
        assert_eq!(
            recovered.tx_flags,
            TxFlags::IS_COINBASE.bits(),
            "tx_flags must reflect on-device flags, not zero",
        );
        assert_eq!(
            recovered.unmined_since, 99_999,
            "unmined_since must reflect on-device value, not zero",
        );
        assert_eq!(
            recovered.generation, 17,
            "generation must reflect on-device value, not zero",
        );
    }

    /// R-031 regression (negative path): legacy `RedoOp::ReplicaCreate` whose
    /// `record_offset` does not point at a coherent on-device record
    /// MUST fail closed instead of registering a zero-cached entry
    /// pointing at unreadable bytes. Pre-fix the function silently
    /// registered the index entry, then the engine's fast-path read
    /// would return junk on first access.
    ///
    /// scenario_09 follow-up: the failure is classified as the TOLERABLE
    /// [`ReplayCause::ReplicaRecordAbsent`] (not the fatal
    /// `MissingRecordBytes`) — a legacy `Create` is a replica/migration
    /// SECONDARY copy whose master re-replicates on rejoin, so the node
    /// must still boot. The index entry is NOT registered either way.
    #[test]
    fn legacy_replay_create_fails_closed_on_missing_record_bytes() {
        let data_dev = std::sync::Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = std::sync::Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
        let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1000).unwrap());

        let txid = {
            let mut t = [0u8; 32];
            t[0] = 0xEF;
            t
        };
        let key = TxKey { txid };
        let utxo_count: u32 = 2;
        let base_size = TxMetadata::record_size_for(utxo_count);
        // Allocate the offset but DO NOT write any record bytes — the
        // metadata read will see zeros (which fail CRC validation).
        let record_offset = alloc.allocate(base_size).unwrap();

        let mut redo = RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();
        redo.append_and_flush(RedoOp::ReplicaCreate {
            tx_key: key,
            record_offset,
            utxo_count,
        })
        .unwrap();

        let stats = recover(&*data_dev as &dyn BlockDevice, &redo, &index).unwrap();
        assert_eq!(stats.entries_replayed, 0);
        assert_eq!(
            stats.failed_replica_record_absent, 1,
            "legacy Create with no on-device record must fail closed as the \
             tolerable ReplicaRecordAbsent (master re-replicates on rejoin)",
        );
        assert_eq!(
            stats.failed_missing_record_bytes, 0,
            "the legacy read-back path must NOT be classified as the fatal \
             Create short-I/O cause",
        );
        assert!(
            index.lookup(&key).is_none(),
            "no index entry must be registered when the record bytes are missing",
        );
    }

    /// scenario_09 root cause: a legacy replica `Create` whose on-device
    /// record bytes are not durable on this node must NOT abort startup.
    /// `check_replay_tolerance` must accept the `ReplicaRecordAbsent`
    /// failure so the node boots and resyncs from the master, instead of
    /// crash-looping and wedging the cluster at 0/N ready.
    #[test]
    fn replica_record_absent_is_tolerable_at_startup() {
        use crate::server::startup::check_replay_tolerance;

        let mut stats = RecoveryStats::default();
        stats.record_failure(ReplayCause::ReplicaRecordAbsent);
        stats.record_failure(ReplayCause::ReplicaRecordAbsent);
        assert_eq!(stats.failed_replica_record_absent, 2);
        assert!(
            check_replay_tolerance(&stats).is_ok(),
            "a handful of absent replica records must not abort startup",
        );

        // The recovery loop must KEEP GOING past this cause (not break on
        // the first one) so later durable entries still replay.
        assert!(
            !is_fatal_replay_cause(ReplayCause::ReplicaRecordAbsent),
            "ReplicaRecordAbsent must be a non-fatal (continue) replay cause",
        );

        // The genuine Create device-fault class stays fatal.
        let mut fatal = RecoveryStats::default();
        fatal.record_failure(ReplayCause::MissingRecordBytes);
        assert!(
            check_replay_tolerance(&fatal).is_err(),
            "a Create short-I/O (MissingRecordBytes) must still abort startup",
        );
        assert!(is_fatal_replay_cause(ReplayCause::MissingRecordBytes));
    }

    #[test]
    fn idempotent_freeze() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(7, 5);

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Freeze {
            tx_key: key,
            offset: 0,
        })
        .unwrap();
        redo.append_and_flush(RedoOp::Freeze {
            tx_key: key,
            offset: 0,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);
        assert_eq!(stats.entries_skipped, 1);
    }

    #[test]
    fn replay_set_mined_bumps_on_device_generation_when_applied() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(79, 1);
        let record_offset = h.index.lookup(&key).unwrap().record_offset;
        assert_eq!(
            {
                io::read_metadata(&*h.data_dev, record_offset)
                    .unwrap()
                    .generation
            },
            0,
        );

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SetMined {
            tx_key: key,
            block_id: 42,
            block_height: 800_000,
            subtree_idx: 7,
            unset: false,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let meta = io::read_metadata(&*h.data_dev, record_offset).unwrap();
        assert_eq!({ meta.block_entry_count }, 1);
        assert_eq!({ meta.generation }, 1);
    }

    /// AUDIT M1.4 regression — a torn (CRC-failing) slot covered by a FreezeV2
    /// redo entry must be rebuilt from the entry's `utxo_hash`, exactly like
    /// SpendV3, rather than failing closed and bricking recovery.
    #[test]
    fn corrupt_slot_with_freeze_v2_entry_self_heals() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xF1, 2);
        let hash0 = h.slot_hash(0);

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::FreezeV2 {
            tx_key: key,
            offset: 0,
            utxo_hash: hash0,
        })
        .unwrap();
        drop(redo);

        h.corrupt_slot(&key, 0);

        let redo = h.redo_log();
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.failed_io, 0, "FreezeV2 entry must not fail closed");
        assert_eq!(stats.entries_replayed, 1, "torn slot rebuilt and frozen");

        let ie = h.index.lookup(&key).unwrap();
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert_eq!(slot.status, UTXO_FROZEN);
        assert_eq!(slot.hash, hash0, "rebuilt slot carries the redo-entry hash");
    }

    /// AUDIT M1.4 regression — a torn slot covered by an UnfreezeV2 entry is
    /// rebuilt to UNSPENT from the entry's `utxo_hash` instead of failing closed.
    #[test]
    fn corrupt_slot_with_unfreeze_v2_entry_self_heals() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xF2, 2);
        let hash0 = h.slot_hash(0);

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::UnfreezeV2 {
            tx_key: key,
            offset: 0,
            utxo_hash: hash0,
        })
        .unwrap();
        drop(redo);

        h.corrupt_slot(&key, 0);

        let redo = h.redo_log();
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.failed_io, 0, "UnfreezeV2 entry must not fail closed");
        assert_eq!(stats.entries_replayed, 1, "torn slot rebuilt and unspent");

        let ie = h.index.lookup(&key).unwrap();
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
        assert_eq!(slot.hash, hash0, "rebuilt slot carries the redo-entry hash");
    }

    #[test]
    fn freeze_v2_replay_skips_hash_mismatch_without_mutating_slot() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(77, 1);
        let original =
            io::read_utxo_slot(&*h.data_dev, h.index.lookup(&key).unwrap().record_offset, 0)
                .unwrap();

        let mut wrong_hash = original.hash;
        wrong_hash[0] ^= 0xFF;

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::FreezeV2 {
            tx_key: key,
            offset: 0,
            utxo_hash: wrong_hash,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 0);
        assert_eq!(stats.entries_skipped, 1);

        let after =
            io::read_utxo_slot(&*h.data_dev, h.index.lookup(&key).unwrap().record_offset, 0)
                .unwrap();
        assert_eq!(after.status, UTXO_UNSPENT);
        assert_eq!(after.hash, original.hash);
    }

    #[test]
    fn unfreeze_v2_replay_skips_non_frozen_slot() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(78, 1);
        let original =
            io::read_utxo_slot(&*h.data_dev, h.index.lookup(&key).unwrap().record_offset, 0)
                .unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::UnfreezeV2 {
            tx_key: key,
            offset: 0,
            utxo_hash: original.hash,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 0);
        assert_eq!(stats.entries_skipped, 1);

        let after =
            io::read_utxo_slot(&*h.data_dev, h.index.lookup(&key).unwrap().record_offset, 0)
                .unwrap();
        assert_eq!(after.status, UTXO_UNSPENT);
        assert_eq!(after.hash, original.hash);
    }

    #[test]
    fn recovery_of_spend_multi_batch() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(8, 10);

        let mut redo = h.redo_log();
        // Log 5 spends as a batch
        for i in 0..5u32 {
            redo.append(RedoOp::Spend {
                tx_key: key,
                offset: i,
                spending_data: {
                    let mut sd = [0u8; 36];
                    sd[0] = i as u8;
                    sd
                },
                new_spent_count: i + 1,
            })
            .unwrap();
        }
        redo.flush().unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 5);

        // All 5 slots should be spent
        let ie = h.index.lookup(&key).unwrap();
        for i in 0..5u32 {
            let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, i).unwrap();
            assert!(slot.is_spent(), "slot {i} should be spent after recovery");
        }
    }

    #[test]
    fn recovery_after_index_consistent() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(9, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: [0xAA; 36],
            new_spent_count: 1,
        })
        .unwrap();

        recover(&*h.data_dev, &redo, &h.index).unwrap();

        // Verify index still points to valid record
        let ie2 = h.index.lookup(&key).unwrap();
        assert_eq!(ie2.record_offset, ie.record_offset);
        let meta = io::read_metadata(&*h.data_dev, ie2.record_offset).unwrap();
        assert_eq!({ meta.magic }, METADATA_MAGIC);
    }

    #[test]
    fn crash_between_redo_and_delete() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(10, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Delete {
            tx_key: key,
            record_offset: ie.record_offset,
            record_size: 1024,
        })
        .unwrap();

        // Index entry still exists (crash before index removal)
        assert!(h.index.lookup(&key).is_some());

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);
        assert!(h.index.lookup(&key).is_none());
    }

    #[test]
    fn recover_all_delete_tombstones_and_frees_region() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xD0, 5);
        let ie = h.index.lookup(&key).unwrap();
        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        let record_size = { meta.record_size } as u64;
        assert!(h.alloc.is_allocated_range(ie.record_offset, record_size));

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Delete {
            tx_key: key,
            record_offset: ie.record_offset,
            record_size,
        })
        .unwrap();

        let mut dah_backend = DahBackend::new_in_memory();
        let mut unmined_backend = UnminedBackend::new_in_memory();
        let stats = recover_all_with_allocator(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah_backend,
            &mut unmined_backend,
            Some(&mut h.alloc),
        )
        .unwrap();

        assert_eq!(stats.entries_replayed, 1);
        assert!(h.index.lookup(&key).is_none());
        assert!(
            !h.alloc.is_allocated_range(ie.record_offset, record_size),
            "delete redo replay must release the deleted record's allocator range"
        );
        assert!(
            io::read_metadata(&*h.data_dev, ie.record_offset).is_err(),
            "delete redo replay must tombstone the on-device metadata"
        );
    }

    #[test]
    fn unspend_already_unspent_skipped() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(11, 5);

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Unspend {
            tx_key: key,
            offset: 0,
            spending_data: Some([0; 36]),
            new_spent_count: 0,
        })
        .unwrap();

        // Slot is already unspent
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_skipped, 1);
    }

    #[test]
    fn crash_between_redo_and_create() {
        let mut h = RecoveryTestHarness::new();
        let mut txid = [0u8; 32];
        txid[0] = 20;
        let key = TxKey { txid };

        // Log a Create but don't actually create the record or add to index
        let offset = h.alloc.allocate(TxMetadata::record_size_for(5)).unwrap();
        let mut meta = TxMetadata::new(5);
        meta.tx_id = txid;
        let slots: Vec<UtxoSlot> = (0..5u32)
            .map(|i| {
                let mut hash = [0u8; 32];
                hash[0] = i as u8;
                UtxoSlot::new_unspent(hash)
            })
            .collect();
        io::write_full_record(&*h.data_dev, offset, &meta, &slots).unwrap();
        // Record is on device but NOT in index (simulating crash before index update)

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::ReplicaCreate {
            tx_key: key,
            record_offset: offset,
            utxo_count: 5,
        })
        .unwrap();

        assert!(h.index.lookup(&key).is_none()); // Not in index yet

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);
        assert!(h.index.lookup(&key).is_some()); // Now in index
    }

    #[test]
    fn double_spend_after_recovery() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(21, 5);

        // Log a spend but don't apply it (crash)
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data: [0xAB; 36],
            new_spent_count: 1,
        })
        .unwrap();

        // Recovery applies it
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        // Now try to re-spend with same data via another recovery
        let mut redo2 = RedoLog::open(h.redo_dev.clone(), 0, 1024 * 1024).unwrap();
        redo2
            .append_and_flush(RedoOp::Spend {
                tx_key: key,
                offset: 0,
                spending_data: [0xAB; 36],
                new_spent_count: 1,
            })
            .unwrap();

        let stats2 = recover(&*h.data_dev, &redo2, &h.index).unwrap();
        // Already applied — skipped (idempotent). The reopened redo log
        // contains both the first spend entry and the newly appended retry,
        // so both are observed and skipped.
        assert_eq!(stats2.entries_skipped, 2);
        assert_eq!(stats2.entries_replayed, 0);

        let ie = h.index.lookup(&key).unwrap();
        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!({ meta.spent_utxos }, 1); // Not double-incremented
    }

    #[test]
    fn replay_reassign() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(22, 5);
        let ie = h.index.lookup(&key).unwrap();

        // Freeze slot 0 first (reassign requires frozen state)
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        let frozen = UtxoSlot::new_frozen(slot.hash);
        io::write_utxo_slot(&*h.data_dev, ie.record_offset, 0, &frozen).unwrap();

        // Log a reassign
        let new_hash = [0xCC; 32];
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::Reassign {
            tx_key: key,
            offset: 0,
            new_hash,
            block_height: 1000,
            spendable_after: 100,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert_eq!(slot.hash, new_hash);
        assert_eq!(slot.status, UTXO_UNSPENT);
        let spendable_h = u32::from_le_bytes(slot.spending_data[0..4].try_into().unwrap());
        assert_eq!(spendable_h, 1100);
    }

    #[test]
    fn replay_reassign_v2_applies_on_matching_prior_hash() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0x52, 5);
        let ie = h.index.lookup(&key).unwrap();

        // Freeze slot 0 (reassign requires frozen state); capture the prior hash.
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        let prior_hash = slot.hash;
        let frozen = UtxoSlot::new_frozen(prior_hash);
        io::write_utxo_slot(&*h.data_dev, ie.record_offset, 0, &frozen).unwrap();

        let new_hash = [0xCC; 32];
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::ReassignV2 {
            tx_key: key,
            offset: 0,
            new_hash,
            block_height: 1000,
            spendable_after: 100,
            prior_utxo_hash: prior_hash,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert_eq!(slot.hash, new_hash);
        assert_eq!(slot.status, UTXO_UNSPENT);
    }

    #[test]
    fn replay_reassign_v2_skips_on_prior_hash_mismatch() {
        // F-A1 (reassign): a ReassignV2 redo entry whose `prior_utxo_hash` no
        // longer matches the on-disk frozen slot is replaying a reassign the
        // live engine would reject (ERR_UTXO_HASH_MISMATCH). Recovery must
        // skip it and leave the slot FROZEN — NOT stamp a fresh UNSPENT slot.
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0x53, 5);
        let ie = h.index.lookup(&key).unwrap();

        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        let real_hash = slot.hash;
        let frozen = UtxoSlot::new_frozen(real_hash);
        io::write_utxo_slot(&*h.data_dev, ie.record_offset, 0, &frozen).unwrap();

        let new_hash = [0xCC; 32];
        let wrong_prior = [0xEE; 32];
        assert_ne!(real_hash, wrong_prior);
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::ReassignV2 {
            tx_key: key,
            offset: 0,
            new_hash,
            block_height: 1000,
            spendable_after: 100,
            prior_utxo_hash: wrong_prior,
        })
        .unwrap();

        recover(&*h.data_dev, &redo, &h.index).unwrap();

        // Slot is untouched: still FROZEN with the real hash, NOT reassigned.
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert_eq!(slot.status, UTXO_FROZEN);
        assert_eq!(slot.hash, real_hash);
        assert_ne!(slot.hash, new_hash);
    }

    #[test]
    fn replay_reassign_v2_skips_when_slot_not_frozen() {
        // A ReassignV2 entry over a slot that is no longer FROZEN (e.g. the
        // engine rejected it with ERR_UTXO_NOT_FROZEN) must be skipped.
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0x54, 5);
        let ie = h.index.lookup(&key).unwrap();

        // Leave slot 0 in its created UNSPENT state (not frozen).
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        let prior_hash = slot.hash;
        assert_eq!(slot.status, UTXO_UNSPENT);

        let new_hash = [0xCC; 32];
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::ReassignV2 {
            tx_key: key,
            offset: 0,
            new_hash,
            block_height: 1000,
            spendable_after: 100,
            prior_utxo_hash: prior_hash,
        })
        .unwrap();

        recover(&*h.data_dev, &redo, &h.index).unwrap();

        // Untouched: still the original UNSPENT slot, not re-stamped to new_hash.
        let slot = io::read_utxo_slot(&*h.data_dev, ie.record_offset, 0).unwrap();
        assert_eq!(slot.status, UTXO_UNSPENT);
        assert_eq!(slot.hash, prior_hash);
        assert_ne!(slot.hash, new_hash);
    }

    #[test]
    fn replay_set_conflicting() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(23, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SetConflicting {
            tx_key: key,
            value: true,
            current_block_height: 1000,
            block_height_retention: 288,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert!(meta.flags.contains(TxFlags::CONFLICTING));
    }

    #[test]
    fn replay_set_locked() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(24, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SetLocked {
            tx_key: key,
            value: true,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert!(meta.flags.contains(TxFlags::LOCKED));
    }

    #[test]
    fn replay_compensate_set_locked_restores_dah() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(124, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.flags |= TxFlags::LOCKED;
        meta.delete_at_height = 0;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::CompensateSetLocked {
            tx_key: key,
            prior_locked: false,
            prior_delete_at_height: 1288,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert!(!meta.flags.contains(TxFlags::LOCKED));
        assert_eq!({ meta.delete_at_height }, 1288);
    }

    #[test]
    fn replay_preserve_until() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(25, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::PreserveUntil {
            tx_key: key,
            block_height: 5000,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!({ meta.preserve_until }, 5000);
        assert_eq!({ meta.delete_at_height }, 0);
    }

    #[test]
    fn replay_mark_on_longest_chain() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(26, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::MarkOnLongestChain {
            tx_key: key,
            on_longest_chain: false,
            current_block_height: 800,
            block_height_retention: 288,
            generation: 1,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!({ meta.unmined_since }, 800);
        // H7: replay bumps the on-device generation to the entry target.
        assert_eq!({ meta.generation }, 1);
    }

    #[test]
    fn generation_wraparound_idempotency() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0x79, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.generation = u32::MAX;
        meta.unmined_since = 0;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();

        let mut redo = h.redo_log();

        // Fresh across wrap: target generation 0 is one step ahead of MAX.
        // Plain numeric `current >= target` would incorrectly skip this.
        redo.append_and_flush(RedoOp::MarkOnLongestChain {
            tx_key: key,
            on_longest_chain: false,
            current_block_height: 1000,
            block_height_retention: 288,
            generation: 0,
        })
        .unwrap();

        // Stale pre-wrap op: after the first entry applies, generation MAX is
        // behind local generation 0 in modular order and must not overwrite
        // the post-wrap state.
        redo.append_and_flush(RedoOp::MarkOnLongestChain {
            tx_key: key,
            on_longest_chain: false,
            current_block_height: 1001,
            block_height_retention: 288,
            generation: u32::MAX,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1, "{stats:?}");
        assert_eq!(stats.entries_skipped, 1, "{stats:?}");
        assert_eq!(stats.entries_failed, 0, "{stats:?}");

        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!({ meta.unmined_since }, 1000);
        assert_eq!(
            { meta.generation },
            0,
            "wrapped generation 0 must be persisted as the applied target"
        );
    }

    #[test]
    fn replay_mark_on_longest_chain_generation_idempotency() {
        // H7: two redo entries with the same `unmined_since` target but
        // different generations — first applies (generation bumped to the
        // entry's target), second is skipped because the on-device
        // generation is at-or-ahead of the second entry's generation.
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0x7A, 5);
        let ie = h.index.lookup(&key).unwrap();

        // Pre-state: generation = 0, unmined_since = 0.
        let pre = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!({ pre.generation }, 0);
        assert_eq!({ pre.unmined_since }, 0);

        let mut redo = h.redo_log();

        // Entry A: target generation = 7, unmined_since target = 1000.
        redo.append_and_flush(RedoOp::MarkOnLongestChain {
            tx_key: key,
            on_longest_chain: false,
            current_block_height: 1000,
            block_height_retention: 288,
            generation: 7,
        })
        .unwrap();

        // Entry B: SAME target (unmined_since = 1000) but earlier
        // generation = 5. Should be skipped because after entry A the
        // on-device generation is at-or-ahead of 5.
        redo.append_and_flush(RedoOp::MarkOnLongestChain {
            tx_key: key,
            on_longest_chain: false,
            current_block_height: 1000,
            block_height_retention: 288,
            generation: 5,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 1, "first entry must apply");
        assert_eq!(stats.entries_skipped, 1, "second entry must be skipped");
        assert_eq!(stats.entries_failed, 0);

        // Concrete post-state: unmined_since = 1000, generation = 7.
        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!({ meta.unmined_since }, 1000);
        assert_eq!(
            { meta.generation },
            7,
            "generation must be the first entry's target, not overwritten by the skipped replay"
        );
    }

    #[test]
    fn replay_mark_on_longest_chain_newer_generation_applies() {
        // H7: a second redo entry with a newer generation than the first
        // still applies, and the on-device generation is pushed to the newer
        // target.
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0x7B, 5);
        let ie = h.index.lookup(&key).unwrap();

        let mut redo = h.redo_log();

        redo.append_and_flush(RedoOp::MarkOnLongestChain {
            tx_key: key,
            on_longest_chain: false,
            current_block_height: 1000,
            block_height_retention: 288,
            generation: 3,
        })
        .unwrap();

        redo.append_and_flush(RedoOp::MarkOnLongestChain {
            tx_key: key,
            on_longest_chain: true,
            current_block_height: 1100,
            block_height_retention: 288,
            generation: 9,
        })
        .unwrap();

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_replayed, 2, "{stats:?}");
        assert_eq!(stats.entries_skipped, 0);

        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!(
            { meta.unmined_since },
            0,
            "second entry (on_longest_chain=true) wins"
        );
        assert_eq!({ meta.generation }, 9);
    }

    // -----------------------------------------------------------------------
    // Secondary index two-phase durability recovery tests (C4).
    // -----------------------------------------------------------------------

    #[test]
    fn recover_all_applies_unmined_secondary_when_stale() {
        // Simulate the bug window: redo of unmined intent was fsynced but the
        // redb commit never happened. Primary has `unmined_since = 500`
        // (matches the redo entry's new_height), so recovery MUST apply the
        // secondary update to reconcile the on-disk index.
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(30, 5);

        let ie = h.index.lookup(&key).unwrap();
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.unmined_since = 500;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();

        // Deliberately keep the primary index cache stale. R-077 recovery
        // must use the on-device metadata as the authority after a crash
        // between the metadata write and the primary cache commit.
        assert_eq!(ie.unmined_since, 0);

        // Redo log: the intent record (as if fsynced) but redb commit skipped.
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SecondaryUnminedUpdate {
            tx_key: key,
            old_height: 0,
            new_height: 500,
        })
        .unwrap();

        let mut dah_backend = DahBackend::new_in_memory();
        let mut unmined_backend = UnminedBackend::new_in_memory();
        // Secondary is currently EMPTY — stale relative to primary (500).

        let stats = recover_all(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah_backend,
            &mut unmined_backend,
        )
        .unwrap();
        assert_eq!(stats.entries_replayed, 1);

        // Secondary index should now contain the entry.
        let result = unmined_backend.range_query(500);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key);
    }

    #[test]
    fn recover_all_skips_stale_unmined_redo_relative_to_primary() {
        // Primary has unmined_since = 0 (record got MARK_ON_LONGEST_CHAIN
        // after the secondary intent was fsynced). The redo's new_height
        // (500) does not match the primary's current (0), so we must NOT
        // replay — another redo entry later in the log supersedes this one.
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(31, 5);

        // Primary: unmined_since = 0.
        let ie = h.index.lookup(&key).unwrap();
        assert_eq!(ie.unmined_since, 0);

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SecondaryUnminedUpdate {
            tx_key: key,
            old_height: 0,
            new_height: 500,
        })
        .unwrap();

        let mut dah_backend = DahBackend::new_in_memory();
        let mut unmined_backend = UnminedBackend::new_in_memory();

        let stats = recover_all(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah_backend,
            &mut unmined_backend,
        )
        .unwrap();
        // The redo entry is stale — skipped.
        assert_eq!(stats.entries_skipped, 1);
        assert!(unmined_backend.is_empty());
    }

    #[test]
    fn recover_all_skips_when_secondary_already_matches_primary() {
        // Secondary already has the entry — replay must be a no-op (idempotent).
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(32, 5);

        let ie = h.index.lookup(&key).unwrap();
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.unmined_since = 500;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();
        h.index
            .update_cached_fields(
                &key,
                ie.tx_flags,
                ie.block_entry_count,
                ie.spent_utxos,
                ie.dah_or_preserve,
                500,
                ie.generation,
            )
            .unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SecondaryUnminedUpdate {
            tx_key: key,
            old_height: 0,
            new_height: 500,
        })
        .unwrap();

        let mut dah_backend = DahBackend::new_in_memory();
        let mut unmined_backend = UnminedBackend::new_in_memory();
        // Pre-populate secondary — matches primary already.
        unmined_backend.insert(500, key, None).unwrap();

        let stats = recover_all(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah_backend,
            &mut unmined_backend,
        )
        .unwrap();
        // Idempotent replay — backend reports it as Applied (no-op commit).
        // Per our replay_redo contract, the redb backend no-ops on same-state
        // so ReplayResult::Applied here means the replay path returned Ok
        // without actually mutating. That's still correct behavior.
        assert!(stats.entries_replayed + stats.entries_skipped == 1);
        assert_eq!(unmined_backend.len(), 1);
    }

    #[test]
    fn recover_all_applies_dah_secondary_when_stale() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(33, 5);

        // Set on-device DAH to 900. The device record is recovery's
        // authoritative source; the primary cache update below only keeps
        // this older test setup internally consistent.
        let ie = h.index.lookup(&key).unwrap();
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.delete_at_height = 900;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();
        // Ensure HAS_PRESERVE_UNTIL is cleared so dah_or_preserve == DAH.
        let tf = TxFlags::from_bits_truncate(ie.tx_flags) - TxFlags::HAS_PRESERVE_UNTIL;
        h.index
            .update_cached_fields(
                &key,
                tf.bits(),
                ie.block_entry_count,
                ie.spent_utxos,
                900,
                ie.unmined_since,
                ie.generation,
            )
            .unwrap();
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.flags = tf;
        meta.delete_at_height = 900;
        meta.unmined_since = 500;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SecondaryDahUpdate {
            tx_key: key,
            old_height: 0,
            new_height: 900,
        })
        .unwrap();

        let mut dah_backend = DahBackend::new_in_memory();
        let mut unmined_backend = UnminedBackend::new_in_memory();

        let stats = recover_all(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah_backend,
            &mut unmined_backend,
        )
        .unwrap();
        assert_eq!(stats.entries_replayed, 1);

        let result = dah_backend.range_query(900);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], key);
    }

    #[test]
    fn recover_all_skips_missing_primary_record() {
        let h = RecoveryTestHarness::new();

        // Fabricate a key that is NOT in the primary index (as if the record
        // was already deleted).
        let mut txid = [0u8; 32];
        txid[0] = 99;
        let key = TxKey { txid };

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SecondaryUnminedUpdate {
            tx_key: key,
            old_height: 0,
            new_height: 500,
        })
        .unwrap();

        let mut dah_backend = DahBackend::new_in_memory();
        let mut unmined_backend = UnminedBackend::new_in_memory();

        let stats = recover_all(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah_backend,
            &mut unmined_backend,
        )
        .unwrap();
        // Skipped — primary has no entry for this key.
        assert_eq!(stats.entries_skipped, 1);
        assert!(unmined_backend.is_empty());
    }

    #[test]
    fn compensate_unset_mined_recovery_allocates_overflow() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xE9, 1);
        let ie = h.index.lookup(&key).unwrap();

        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        for i in 0..INLINE_BLOCK_ENTRIES {
            meta.block_entries_inline[i] = BlockEntry {
                block_id: (i + 1) as u32,
                block_height: 900_000 + i as u32,
                subtree_idx: i as u32,
            };
        }
        meta.block_entry_count = INLINE_BLOCK_ENTRIES as u8;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::CompensateUnsetMined {
            tx_key: key,
            block_id: 99,
            block_height: 901_999,
            subtree_idx: 7,
        })
        .unwrap();

        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        let stats = recover_all_with_allocator(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah,
            &mut unmined,
            Some(&mut h.alloc),
        )
        .unwrap();

        assert_eq!(stats.entries_replayed, 1);
        assert_eq!(stats.entries_failed, 0);

        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!(meta.block_entry_count, 4);
        let overflow_offset = meta.block_overflow_offset;
        assert_ne!(overflow_offset, 0);
        let overflow = read_recovery_overflow_entries(&*h.data_dev, &meta).unwrap();
        assert_eq!(
            overflow,
            vec![BlockEntry {
                block_id: 99,
                block_height: 901_999,
                subtree_idx: 7,
            }]
        );
    }

    // -----------------------------------------------------------------------
    // Allocator redo-journaling recovery tests (C6).
    // -----------------------------------------------------------------------

    #[test]
    fn recover_all_replays_allocator_free_region() {
        // Scenario: a free happened after the allocator snapshot but
        // before a crash. The redo log contains the FreeRegion entry.
        // Recovery must replay it into the rebuilt allocator.
        let mut h = RecoveryTestHarness::new();

        // Allocate a region, snapshot, then free — only the redo log
        // captures the free.
        let offset = h.alloc.allocate(8192).unwrap();
        h.alloc.persist().unwrap();

        // Simulate a free that was journaled but not snapshotted.
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::FreeRegion {
            offset,
            size: 8192,
            device_id: 0,
        })
        .unwrap();

        // Rebuild the allocator from snapshot.
        let mut recovered = SlotAllocator::recover(h.data_dev.clone()).unwrap();
        assert_eq!(recovered.free_region_count(), 0, "snapshot lacks the free");

        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        let stats = recover_all_with_allocator(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah,
            &mut unmined,
            Some(&mut recovered),
        )
        .unwrap();

        assert_eq!(stats.entries_replayed, 1, "the free entry must be replayed");
        assert_eq!(
            recovered.free_region_count(),
            1,
            "replayed free must appear in the rebuilt freelist"
        );
        // And the region is reusable.
        let reused = recovered.allocate(8192).unwrap();
        assert_eq!(reused, offset);
    }

    #[test]
    fn recover_all_replays_allocator_allocate_region() {
        let h = RecoveryTestHarness::new();
        h.alloc.persist().unwrap();

        // A redo log containing an allocate that was never snapshotted.
        let mut redo = h.redo_log();
        let offset = crate::allocator::DATA_REGION_OFFSET;
        redo.append_and_flush(RedoOp::AllocateRegion {
            offset,
            size: 4096,
            device_id: 0,
        })
        .unwrap();

        let mut recovered = SlotAllocator::recover(h.data_dev.clone()).unwrap();
        let before_next = recovered.next_offset();

        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        recover_all_with_allocator(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah,
            &mut unmined,
            Some(&mut recovered),
        )
        .unwrap();

        assert!(
            recovered.next_offset() >= offset + 4096,
            "next_offset must cover the replayed allocate (was {before_next}, now {})",
            recovered.next_offset(),
        );
    }

    #[test]
    fn recover_all_is_idempotent_for_allocator_ops() {
        // Replaying the same allocator redo stream twice must yield the
        // same allocator state.
        let h = RecoveryTestHarness::new();
        h.alloc.persist().unwrap();

        let mut redo = h.redo_log();
        let offset = crate::allocator::DATA_REGION_OFFSET;
        redo.append_and_flush(RedoOp::AllocateRegion {
            offset,
            size: 4096,
            device_id: 0,
        })
        .unwrap();
        redo.append_and_flush(RedoOp::AllocateRegion {
            offset: offset + 4096,
            size: 8192,
            device_id: 0,
        })
        .unwrap();
        redo.append_and_flush(RedoOp::FreeRegion {
            offset,
            size: 4096,
            device_id: 0,
        })
        .unwrap();

        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();

        let mut once = SlotAllocator::recover(h.data_dev.clone()).unwrap();
        recover_all_with_allocator(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah,
            &mut unmined,
            Some(&mut once),
        )
        .unwrap();

        let mut twice = SlotAllocator::recover(h.data_dev.clone()).unwrap();
        for _ in 0..2 {
            recover_all_with_allocator(
                &*h.data_dev,
                &redo,
                &h.index,
                &mut dah,
                &mut unmined,
                Some(&mut twice),
            )
            .unwrap();
        }

        assert_eq!(
            once.next_offset(),
            twice.next_offset(),
            "next_offset must be identical after any number of replays"
        );
        assert_eq!(
            once.free_region_count(),
            twice.free_region_count(),
            "freelist size must be identical after any number of replays"
        );
    }

    #[test]
    fn recover_all_batched_pair_reconciles_both_indexes() {
        // End-to-end: a MarkOnLongestChain-style update produces TWO secondary
        // intent records (DAH + unmined) in a single fsync batch. Both are
        // fsynced but the redb commits never happened (crash scenario).
        // `recover_all` should apply both.
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(34, 5);

        // Primary's post-mutation state: both fields set.
        let ie = h.index.lookup(&key).unwrap();
        let tf = TxFlags::from_bits_truncate(ie.tx_flags) - TxFlags::HAS_PRESERVE_UNTIL;
        h.index
            .update_cached_fields(
                &key,
                tf.bits(),
                ie.block_entry_count,
                ie.spent_utxos,
                900,
                500,
                ie.generation,
            )
            .unwrap();
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.flags = tf;
        meta.delete_at_height = 900;
        meta.unmined_since = 500;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();

        let mut redo = h.redo_log();
        // Batched fsync — both ops in one flush, as the engine would do.
        let ops = vec![
            RedoOp::SecondaryDahUpdate {
                tx_key: key,
                old_height: 0,
                new_height: 900,
            },
            RedoOp::SecondaryUnminedUpdate {
                tx_key: key,
                old_height: 0,
                new_height: 500,
            },
        ];
        redo.append_batch_and_flush(&ops).unwrap();

        let mut dah_backend = DahBackend::new_in_memory();
        let mut unmined_backend = UnminedBackend::new_in_memory();

        let stats = recover_all(
            &*h.data_dev,
            &redo,
            &h.index,
            &mut dah_backend,
            &mut unmined_backend,
        )
        .unwrap();
        assert_eq!(stats.entries_replayed, 2, "{stats:?}");

        assert_eq!(dah_backend.range_query(900).len(), 1);
        assert_eq!(unmined_backend.range_query(500).len(), 1);
    }

    #[test]
    fn recovery_post_replay_generation_matches_live_engine() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(35, 1);
        let ie = h.index.lookup(&key).unwrap();
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.block_entry_count = 1;
        meta.block_entries_inline[0] = BlockEntry {
            block_id: 1,
            block_height: 900,
            subtree_idx: 0,
        };
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();
        h.index
            .update_cached_fields(&key, 0, 1, 0, 0, 0, 0)
            .unwrap();

        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SpendV2 {
            tx_key: key,
            offset: 0,
            spending_data: [0x51; 36],
            new_spent_count: 1,
            current_block_height: 1000,
            block_height_retention: 288,
            target_generation: 7,
            updated_at: 123_456,
            utxo_hash: None,
        })
        .unwrap();

        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        let stats = recover_all(&*h.data_dev, &redo, &h.index, &mut dah, &mut unmined).unwrap();
        assert_eq!(stats.entries_replayed, 1, "{stats:?}");

        let meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        assert_eq!({ meta.spent_utxos }, 1);
        assert_eq!({ meta.generation }, 7);
        assert_eq!({ meta.updated_at }, 123_456);
        assert_eq!({ meta.delete_at_height }, 1288);
        assert_eq!(dah.range_query(1288), vec![key]);
    }

    #[test]
    fn recovery_post_replay_dah_index_matches_live_engine() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(36, 1);
        let ie = h.index.lookup(&key).unwrap();
        let mut meta = io::read_metadata(&*h.data_dev, ie.record_offset).unwrap();
        meta.delete_at_height = 900;
        meta.unmined_since = 500;
        io::write_metadata(&*h.data_dev, ie.record_offset, &meta).unwrap();
        h.index
            .update_cached_fields(&key, 0, 0, 0, 900, 500, 0)
            .unwrap();

        let redo = h.redo_log();
        let mut dah = DahBackend::new_in_memory();
        let mut unmined = UnminedBackend::new_in_memory();
        recover_all(&*h.data_dev, &redo, &h.index, &mut dah, &mut unmined).unwrap();

        assert_eq!(dah.range_query(900), vec![key]);
        assert_eq!(unmined.range_query(500), vec![key]);
    }

    // -----------------------------------------------------------------------
    // Gap #5 — replay failure cause classification
    //
    // Each test seeds a redo log with entries whose primary references are
    // missing or whose device reads fail, then verifies that
    // [`RecoveryStats`] increments the correct per-cause counter.
    // -----------------------------------------------------------------------

    /// 100 redo entries that reference a primary key that does NOT exist in
    /// the index must classify every failure as `MissingPrimary` (benign)
    /// and the recovery call itself must succeed.
    #[test]
    fn replay_classifies_missing_primary_for_unknown_keys() {
        let h = RecoveryTestHarness::new();
        let mut redo = h.redo_log();

        // Append 100 spend ops referencing keys that are not in the index.
        let n = 100u8;
        for i in 1..=n {
            let mut txid = [0u8; 32];
            txid[0] = i;
            let key = TxKey { txid };
            redo.append_and_flush(RedoOp::Spend {
                tx_key: key,
                offset: 0,
                spending_data: [0xAB; 36],
                new_spent_count: 1,
            })
            .unwrap();
        }

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.entries_failed, n as u64);
        assert_eq!(stats.failed_missing_primary, n as u64);
        assert_eq!(stats.failed_io, 0);
        assert_eq!(stats.failed_corrupt, 0);
        assert_eq!(stats.failed_logic, 0);
    }

    /// `MissingPrimary` accumulated below the cap passes the tolerance check.
    #[test]
    fn replay_tolerance_passes_high_missing_primary_count() {
        let h = RecoveryTestHarness::new();
        let mut redo = h.redo_log();

        // 100 missing-primary entries — well below
        // `MAX_TOLERATED_MISSING_PRIMARY`.
        for i in 1..=100u8 {
            let mut txid = [0u8; 32];
            txid[0] = i;
            redo.append_and_flush(RedoOp::Spend {
                tx_key: TxKey { txid },
                offset: 0,
                spending_data: [0xAB; 36],
                new_spent_count: 1,
            })
            .unwrap();
        }

        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        crate::server::startup::check_replay_tolerance(&stats)
            .expect("100 missing-primary failures must be tolerated");
    }

    /// A single I/O-class failure during replay must trip the tolerance
    /// check immediately. We construct the stats directly because forcing
    /// a real device I/O error during replay through `MemoryDevice` is
    /// impractical — the per-cause classification already lives at the
    /// failure site, so the integration boundary we need to test is
    /// "stats with `failed_io > 0` ⇒ tolerance returns Err".
    #[test]
    fn replay_tolerance_rejects_one_io_failure() {
        let stats = RecoveryStats {
            failed_io: 1,
            entries_failed: 1,
            ..RecoveryStats::default()
        };
        let err = crate::server::startup::check_replay_tolerance(&stats)
            .expect_err("any I/O failure must abort startup");
        assert!(err.contains("device I/O"), "msg: {err}");
    }

    /// Sanity: `record_failure` increments per-cause counters in lock step
    /// with `entries_failed`, so the back-compat field stays consistent.
    #[test]
    fn recovery_stats_record_failure_increments_both_counters() {
        let mut stats = RecoveryStats::default();
        stats.record_failure(ReplayCause::MissingPrimary);
        stats.record_failure(ReplayCause::IoError);
        stats.record_failure(ReplayCause::CorruptEntry);
        stats.record_failure(ReplayCause::LogicError);
        assert_eq!(stats.entries_failed, 4);
        assert_eq!(stats.failed_missing_primary, 1);
        assert_eq!(stats.failed_io, 1);
        assert_eq!(stats.failed_corrupt, 1);
        assert_eq!(stats.failed_logic, 1);
    }

    // -----------------------------------------------------------------------
    // Deletion-tombstone recovery (Phase 7: R1 reconstruct + R2 self-purge)
    // -----------------------------------------------------------------------

    use crate::index::redb_tombstone::RedbTombstoneIndex;
    use crate::tombstone::{Tombstone, TombstoneCause, TombstoneLog};

    /// Build a fresh in-memory-device tombstone log + tempdir redb index.
    fn fresh_tombstone_storage() -> (TombstoneLog, RedbTombstoneIndex, tempfile::TempDir) {
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
        let log = TombstoneLog::create(dev, 0, 8 * 1024 * 1024).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let idx =
            RedbTombstoneIndex::open(&dir.path().join("tombstone.redb"), 16 * 1024 * 1024).unwrap();
        (log, idx, dir)
    }

    /// Consume the harness into an engine (so R2 can route record removal
    /// through `delete_for_purge`).
    fn engine_from_harness(h: RecoveryTestHarness) -> Arc<Engine> {
        Arc::new(Engine::new_with_sharded_index(
            h.data_dev.clone(),
            h.index,
            h.alloc,
            StripedLocks::new(1024),
            DahBackend::new_in_memory(),
            UnminedBackend::new_in_memory(),
        ))
    }

    fn seed_tombstone(log: &mut TombstoneLog, key: &TxKey, generation: u32, height: u32) {
        let shard = crate::cluster::shards::ShardTable::shard_for_key(key);
        let t = Tombstone::new(
            key.txid,
            shard,
            height,
            generation,
            TombstoneCause::SpentDah,
            0,
        );
        log.append_synced(&t).unwrap();
    }

    #[test]
    fn r1_rebuild_reconstructs_all_entries() {
        // N tombstone log entries → recover rebuilds the redb index with all N.
        let h = RecoveryTestHarness::new();
        let engine = engine_from_harness(h);
        let (mut log, mut idx, _dir) = fresh_tombstone_storage();

        let mut keys = Vec::new();
        for n in 0..7u8 {
            let mut txid = [0u8; 32];
            txid[0] = n;
            txid[1] = n.wrapping_mul(3);
            let key = TxKey { txid };
            seed_tombstone(&mut log, &key, n as u32, 100 + n as u32);
            keys.push(key);
        }

        let stats = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(stats.tombstones_reconstructed, 7);
        // No records exist in the primary index, so nothing is purged.
        assert_eq!(stats.records_self_purged, 0);
        assert_eq!(idx.len(), 7);
        for (n, key) in keys.iter().enumerate() {
            let v = idx.get(key).unwrap();
            assert_eq!(v.deletion_height, 100 + n as u32);
            assert_eq!(v.generation, n as u32);
        }
    }

    #[test]
    fn r2_self_purges_resurrected_record_gen_equal() {
        // Live record at gen 0 + tombstone at gen 0 (tombstone gen >= live
        // gen) → recover purges the record.
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xA1, 2);
        let engine = engine_from_harness(h);
        assert!(engine.lookup(&key).is_some());

        let (mut log, mut idx, _dir) = fresh_tombstone_storage();
        seed_tombstone(&mut log, &key, 0, 150);

        let stats = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(stats.records_self_purged, 1);
        assert_eq!(stats.kept_newer_generation, 0);
        assert!(
            engine.lookup(&key).is_none(),
            "R2 must purge a resurrected record whose tombstone gen >= live gen",
        );
        // The tombstone itself survives in the index (still needed later).
        assert!(idx.is_tombstoned(&key));
    }

    #[test]
    fn r2_keeps_record_with_strictly_higher_generation() {
        // Live record re-created post-deletion at gen 6; tombstone is for the
        // older gen 3 → recover KEEPS the live record (design §8.4).
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xB2, 1);
        h.set_index_generation(&key, 6);
        let engine = engine_from_harness(h);

        let (mut log, mut idx, _dir) = fresh_tombstone_storage();
        seed_tombstone(&mut log, &key, 3, 150);

        let stats = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(stats.records_self_purged, 0);
        assert_eq!(stats.kept_newer_generation, 1);
        assert!(
            engine.lookup(&key).is_some(),
            "R2 must NOT purge a record newer than its tombstone",
        );
    }

    #[test]
    fn r2_keeps_record_with_no_tombstone() {
        // A live record with no tombstone at all is untouched.
        let mut h = RecoveryTestHarness::new();
        let kept = h.create_record(0xC3, 1);
        let purged = h.create_record(0xC4, 1);
        let engine = engine_from_harness(h);

        let (mut log, mut idx, _dir) = fresh_tombstone_storage();
        // Only `purged` gets a tombstone.
        seed_tombstone(&mut log, &purged, 0, 150);

        let stats = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(stats.records_self_purged, 1);
        assert!(engine.lookup(&purged).is_none());
        assert!(
            engine.lookup(&kept).is_some(),
            "a record with no tombstone is never purged",
        );
    }

    #[test]
    fn r2_is_idempotent_across_two_recoveries() {
        // Running recovery twice yields the same state.
        let mut h = RecoveryTestHarness::new();
        let purged = h.create_record(0xD5, 2);
        let newer = h.create_record(0xD6, 1);
        h.set_index_generation(&newer, 9);
        let kept_no_ts = h.create_record(0xD7, 1);
        let engine = engine_from_harness(h);

        let (mut log, mut idx, _dir) = fresh_tombstone_storage();
        seed_tombstone(&mut log, &purged, 0, 150);
        seed_tombstone(&mut log, &newer, 4, 150); // older than live gen 9

        let s1 = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(s1.records_self_purged, 1);
        assert_eq!(s1.kept_newer_generation, 1);
        assert!(engine.lookup(&purged).is_none());
        assert!(engine.lookup(&newer).is_some());
        assert!(engine.lookup(&kept_no_ts).is_some());

        // Second run: identical reconstruct count, nothing left to purge.
        let s2 = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(s2.tombstones_reconstructed, s1.tombstones_reconstructed);
        assert_eq!(s2.records_self_purged, 0, "nothing left to purge on rerun");
        assert_eq!(s2.kept_newer_generation, 1);
        assert!(engine.lookup(&purged).is_none());
        assert!(engine.lookup(&newer).is_some());
        assert!(engine.lookup(&kept_no_ts).is_some());
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn r2_purges_resurrected_key_whose_region_allocator_already_freed() {
        // The bug: recovery resurrects a primary-index entry at an offset the
        // allocator's recovered free-list ALREADY freed. R2's delete must NOT
        // double-free that region; it must drop the stale index entry and count
        // the purge as a SUCCESS (no spurious "self-purge failed" warning).
        let mut h = RecoveryTestHarness::new();
        let utxo_count = 2;
        let key = h.create_record(0xE1, utxo_count);

        // Stage the index/allocator inconsistency: free the region in the
        // allocator only, leaving the index entry + device bytes in place.
        let (offset, _size) = h.free_region_in_allocator_only(&key, utxo_count);
        // Sanity: the allocator now considers the region free (the precondition
        // that makes delete_inner's step-5 free a double-free).
        assert!(
            !h.alloc
                .is_allocated_range(offset, TxMetadata::record_size_for(utxo_count)),
            "precondition: allocator must already consider the region free",
        );

        let engine = engine_from_harness(h);
        // The index still resurrects the key.
        assert!(
            engine.lookup(&key).is_some(),
            "precondition: key resurrected"
        );

        let (mut log, mut idx, _dir) = fresh_tombstone_storage();
        seed_tombstone(&mut log, &key, 0, 150);

        let stats = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(
            stats.records_self_purged, 1,
            "an already-free region must count as a SUCCESSFUL purge, not a failure",
        );
        assert_eq!(stats.kept_newer_generation, 0);
        assert!(
            engine.lookup(&key).is_none(),
            "the resurrected stale index entry must be removed (R2's goal)",
        );
        // The tombstone survives for later use.
        assert!(idx.is_tombstoned(&key));
    }

    #[test]
    fn r2_already_free_purge_is_idempotent_no_loop() {
        // Running R2 twice on an already-free resurrected key yields the same
        // state: the key stays gone (the redb-backed primary-index removal is
        // durable, so there is no per-restart re-purge loop) and the second run
        // finds nothing to purge.
        let mut h = RecoveryTestHarness::new();
        let utxo_count = 1;
        let key = h.create_record(0xE2, utxo_count);
        let _ = h.free_region_in_allocator_only(&key, utxo_count);
        let engine = engine_from_harness(h);

        let (mut log, mut idx, _dir) = fresh_tombstone_storage();
        seed_tombstone(&mut log, &key, 0, 150);

        let s1 = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(s1.records_self_purged, 1);
        assert!(engine.lookup(&key).is_none());

        // Second run: the key is already absent, so nothing is purged again
        // and no double-free is attempted.
        let s2 = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(
            s2.records_self_purged, 0,
            "key already purged + durably removed → no re-purge, no loop",
        );
        assert!(engine.lookup(&key).is_none());
        assert_eq!(idx.len(), 1, "tombstone count is stable across reruns");
    }

    #[test]
    fn r2_partial_overlap_free_is_not_tolerated() {
        // SAFETY boundary: the already-free tolerance is restricted to a record
        // region FULLY CONTAINED in an existing free region. A PARTIAL overlap
        // (the record range extends past the free region into still-allocated
        // space) is real corruption and must NOT be silently tolerated — the
        // allocator's DoubleFree error must surface so the purge is NOT counted.
        let mut h = RecoveryTestHarness::new();
        // A record spanning two 4096 blocks (256 + 100*64 = 6656 → 8192 aligned).
        let utxo_count = 100;
        let key = h.create_record(0xE4, utxo_count);
        let ie = h.index.lookup(&key).unwrap();
        let offset = ie.record_offset;
        let full_size = TxMetadata::record_size_for(utxo_count); // > 4096
        assert!(
            full_size > 4096,
            "record must span >1 block for a partial overlap"
        );
        // Free ONLY the first 4096 of the record's region, leaving the rest
        // still allocated. The record's [offset, offset+full_size) now only
        // PARTIALLY overlaps the free region [offset, offset+4096).
        h.alloc.free(offset, 4096).unwrap();
        // Precondition: not fully allocated (overlaps the free block), and the
        // containing free region does NOT cover the whole record.
        assert!(!h.alloc.is_allocated_range(offset, full_size));
        let (free_off, free_size) = h.alloc.free_region_containing(offset).unwrap();
        assert_eq!(free_off, offset);
        assert!(
            offset + full_size > free_off + free_size,
            "must be a partial overlap"
        );

        let engine = engine_from_harness(h);
        assert!(engine.lookup(&key).is_some());

        let (mut log, mut idx, _dir) = fresh_tombstone_storage();
        seed_tombstone(&mut log, &key, 0, 150);

        let stats = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(
            stats.records_self_purged, 0,
            "a partial-overlap free is corruption and must NOT count as a purge",
        );
    }

    #[test]
    fn r2_genuine_delete_error_is_not_counted_as_purged() {
        // A GENUINE failure (not the benign already-free case) must keep the
        // existing warn+retry behavior: the purge is NOT counted, and the
        // record is left for the next restart. We force a real failure by
        // making the on-device metadata header unreadable for the resurrected
        // key, so `delete_inner`'s `read_metadata_for_key` (step pre-1) errors
        // out BEFORE any index removal — i.e. a transient backend error, not an
        // already-free region.
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0xE3, 2);
        // Corrupt the metadata header so reading it back fails (CRC mismatch),
        // turning the delete into a genuine StorageError. The region is still
        // ALLOCATED (we do NOT free it), so this is unambiguously NOT the
        // already-free path.
        let ie = h.index.lookup(&key).unwrap();
        let offset = ie.record_offset;
        let align = h.data_dev.alignment();
        let aligned = offset / align as u64 * align as u64;
        let mut buf = crate::device::AlignedBuf::new(align, align);
        h.data_dev.pread_exact_at(&mut buf, aligned).unwrap();
        // Flip the magic bytes so metadata read rejects it.
        buf[(offset - aligned) as usize] ^= 0xFF;
        h.data_dev.pwrite_all_at(&buf, aligned).unwrap();
        h.data_dev.sync().unwrap();

        let engine = engine_from_harness(h);
        assert!(engine.lookup(&key).is_some());

        let (mut log, mut idx, _dir) = fresh_tombstone_storage();
        seed_tombstone(&mut log, &key, 0, 150);

        let stats = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(
            stats.records_self_purged, 0,
            "a genuine delete error must NOT be counted as a successful purge",
        );
        // The record is still present (the delete failed before index removal),
        // so the next restart will retry — idempotent recovery.
        assert!(
            engine.lookup(&key).is_some(),
            "a genuinely-failed purge must leave the record for retry",
        );
    }

    #[test]
    fn r1_drops_torn_tail_entry() {
        // R1 relies on the log's torn-tail-drop scan: a corrupt final entry
        // is not reconstructed, and is not an error.
        let h = RecoveryTestHarness::new();
        let engine = engine_from_harness(h);
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
        let mut log = TombstoneLog::create(dev.clone(), 0, 8 * 1024 * 1024).unwrap();
        for n in 0..3u8 {
            let mut txid = [0u8; 32];
            txid[0] = n;
            seed_tombstone(&mut log, &TxKey { txid }, n as u32, 100 + n as u32);
        }
        // Corrupt the 3rd (final) entry on device: header block is 4096, then
        // 56-byte entries; entry 2 starts at 4096 + 2*56.
        let header = 4096u64;
        let entry2_off = header + 2 * 56;
        let aligned = entry2_off / 4096 * 4096;
        let intra = (entry2_off - aligned) as usize;
        let mut block = crate::device::AlignedBuf::new(4096, 4096);
        dev.pread_exact_at(&mut block, aligned).unwrap();
        block[intra + 4] ^= 0xFF;
        dev.pwrite_all_at(&block, aligned).unwrap();
        dev.sync().unwrap();
        let log = TombstoneLog::open(dev, 0, 8 * 1024 * 1024).unwrap();

        let (_unused, mut idx, _dir) = fresh_tombstone_storage();
        let _ = _unused;
        let stats = recover_tombstones(&engine, &log, &mut idx).unwrap();
        assert_eq!(
            stats.tombstones_reconstructed, 2,
            "torn final tombstone entry must be dropped, not reconstructed",
        );
        assert_eq!(idx.len(), 2);
    }

    // -----------------------------------------------------------------------
    // BUG3 — record-height floor from replayed live-record (height-bearing)
    // redo entries, independent of tombstones (design §4 height subsystem).
    // -----------------------------------------------------------------------

    #[test]
    fn recovery_folds_max_block_height_across_height_bearing_entries() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0x70, 2);

        // A height-bearing op (SetMined at height 800_123) and a NON-height op
        // (Freeze) — only the former should contribute to the floor.
        let mut redo = h.redo_log();
        redo.append_and_flush(RedoOp::SetMined {
            tx_key: key,
            block_id: 5,
            block_height: 800_123,
            subtree_idx: 0,
            unset: false,
        })
        .unwrap();
        redo.append_and_flush(RedoOp::Freeze {
            tx_key: key,
            offset: 0,
        })
        .unwrap();
        // A lower height-bearing op must not lower the max.
        redo.append_and_flush(RedoOp::SpendV2 {
            tx_key: key,
            offset: 0,
            spending_data: [0xAA; 36],
            new_spent_count: 1,
            current_block_height: 700_000,
            block_height_retention: 288,
            target_generation: 1,
            updated_at: 10,
            utxo_hash: None,
        })
        .unwrap();
        drop(redo);

        let redo = h.redo_log();
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(
            stats.max_observed_block_height, 800_123,
            "floor must be the MAX height across height-bearing entries",
        );
    }

    #[test]
    fn recovery_height_floor_zero_when_no_height_bearing_entry() {
        let mut h = RecoveryTestHarness::new();
        let key = h.create_record(0x71, 1);
        let mut redo = h.redo_log();
        // Only non-height ops → floor stays 0.
        redo.append_and_flush(RedoOp::Freeze {
            tx_key: key,
            offset: 0,
        })
        .unwrap();
        drop(redo);
        let redo = h.redo_log();
        let stats = recover(&*h.data_dev, &redo, &h.index).unwrap();
        assert_eq!(stats.max_observed_block_height, 0);
    }

    /// Sharding task 5 — N=16 replay path.
    ///
    /// A 16-shard index receives 64 records spread across diverse txids. A
    /// `Freeze` redo entry is written for each record, then `recover` is called.
    /// All 64 entries must remain in the index (recovery is idempotent on
    /// already-applied operations), and the stats counter must show the correct
    /// number of entries visited.
    #[test]
    fn replay_into_n16() {
        const N: usize = 64;

        let data_dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let redo_dev = Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();

        // 16-shard in-memory index. Enough capacity to avoid resizes.
        let index = ShardedIndex::new_in_memory(1024, 16).unwrap();

        let record_size = TxMetadata::record_size_for(1);

        // Seed N records. Each txid varies in bytes [0..4] so the SplitMix64
        // routing on [24..32] spreads keys across shards when N >= shard_count.
        let mut keys = Vec::with_capacity(N);
        for i in 0..N {
            let mut txid = [0u8; 32];
            // Vary bytes 0..4 for uniqueness and bytes 24..28 to exercise shard routing.
            let i_u32 = i as u32;
            txid[0] = (i_u32 & 0xFF) as u8;
            txid[1] = ((i_u32 >> 8) & 0xFF) as u8;
            txid[24] = (i_u32 & 0xFF) as u8;
            txid[25] = ((i_u32 >> 8) & 0xFF) as u8;
            let key = TxKey { txid };

            let offset = alloc.allocate(record_size).unwrap();
            let mut meta = TxMetadata::new(1);
            meta.tx_id = txid;
            let slot = UtxoSlot::new_unspent({
                let mut h = [0u8; 32];
                h[0] = i as u8;
                h
            });
            io::write_full_record(&*data_dev, offset, &meta, &[slot]).unwrap();

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
            keys.push(key);
        }

        // Append a Freeze redo entry for every record. Since the slots are
        // UNSPENT and the operation is per-slot, replay will attempt to freeze
        // each one (transitioning UNSPENT → FROZEN on the device). Stats will
        // count each as replayed or skipped depending on current state.
        let mut redo = RedoLog::open(redo_dev.clone(), 0, 4 * 1024 * 1024).unwrap();
        for &key in &keys {
            redo.append_and_flush(RedoOp::Freeze {
                tx_key: key,
                offset: 0,
            })
            .unwrap();
        }
        drop(redo);

        let redo = RedoLog::open(redo_dev, 0, 4 * 1024 * 1024).unwrap();
        let stats = recover(&*data_dev, &redo, &index).unwrap();

        // All N entries must be applied (none were frozen before recovery).
        assert_eq!(
            stats.entries_replayed, N as u64,
            "all {N} Freeze entries must replay across the 16-shard index"
        );
        assert_eq!(stats.failed_io, 0);
        assert_eq!(stats.failed_corrupt, 0);

        // All N keys are still registered (recovery is non-destructive for
        // keys that were not evicted by the BUG-1 alias fix).
        let mut count = 0usize;
        index.for_each(|_key, _entry| {
            count += 1;
        });
        assert_eq!(
            count, N,
            "all {N} records must remain in the 16-shard index after recovery"
        );
    }

    /// Sharding task 5 — BUG-1 offset-alias eviction across shards.
    ///
    /// Two distinct keys (`key_a`, `key_b`) may route to different shards. If
    /// both once pointed to the same `record_offset` (offset aliasing), a
    /// `Create` redo entry for `key_b` must cause `register_unique_offset` to
    /// evict `key_a` (the stale alias) even when the two keys live in different
    /// shards. After recovery `key_a` must be absent and `key_b` present at the
    /// offset.
    #[test]
    fn offset_alias_eviction_across_shards() {
        use crate::record::{METADATA_SIZE, UTXO_SLOT_SIZE};

        let data_dev = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
        let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
        let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();

        // Two-shard index: every shard selection is a single bit flip under the
        // runtime seed, so there are exactly two shards.
        let index = ShardedIndex::new_in_memory(64, 2).unwrap();

        // Find two keys that are DETERMINISTICALLY in different shards by
        // iterating over candidate pairs until we find one. This mirrors the
        // `find_different_shard_keys` helper pattern from the sharded.rs tests
        // and eliminates any reliance on a random seed producing a specific
        // split for hard-coded txid bytes.
        let (key_a, key_b) = {
            let mut found: Option<(TxKey, TxKey)> = None;
            'outer: for a in 0u64..100_000 {
                let mut txid_a = [0u8; 32];
                txid_a[0] = 0xAA;
                txid_a[1..9].copy_from_slice(&a.to_le_bytes());
                txid_a[24..32].copy_from_slice(&a.to_le_bytes());
                let ka = TxKey { txid: txid_a };
                let shard_a = index.index_shard_for_key(&ka);

                for b in (a + 1)..(a + 100).min(100_000) {
                    let mut txid_b = [0u8; 32];
                    txid_b[0] = 0xBB;
                    txid_b[1..9].copy_from_slice(&b.to_le_bytes());
                    txid_b[24..32].copy_from_slice(&b.to_le_bytes());
                    let kb = TxKey { txid: txid_b };
                    if index.index_shard_for_key(&kb) != shard_a {
                        found = Some((ka, kb));
                        break 'outer;
                    }
                }
            }
            found.expect("must find two keys in different shards within 100k candidates")
        };

        let (txid_a, txid_b) = (key_a.txid, key_b.txid);

        // Allocate two adjacent regions. key_a occupies region 1 (offset_a).
        // key_b will claim region 1 via its Create (the alias scenario).
        let utxo_count: u32 = 1;
        let record_size = TxMetadata::record_size_for(utxo_count);

        let offset_a = alloc.allocate(record_size).unwrap();
        // Allocate a second region so the allocator high-water mark advances
        // (key_b's Create will replay at offset_a, claiming it back).
        let _offset_b_unused = alloc.allocate(record_size).unwrap();

        // Write key_a's record bytes at offset_a (the "old" record being aliased).
        let mut meta_a = TxMetadata::new(utxo_count);
        meta_a.tx_id = txid_a;
        let slot_a = UtxoSlot::new_unspent([0xAA; 32]);
        io::write_full_record(&*data_dev, offset_a, &meta_a, &[slot_a]).unwrap();

        // Pre-register key_a → offset_a in the index. This is the stale alias
        // entry that recovery must evict when key_b's Create replays.
        index
            .register(
                key_a,
                TxIndexEntry {
                    device_id: 0,
                    record_offset: offset_a,
                    utxo_count,
                    block_entry_count: 0,
                    tx_flags: 0,
                    spent_utxos: 0,
                    dah_or_preserve: 0,
                    unmined_since: 0,
                    generation: 0,
                },
            )
            .unwrap();

        // Build key_b's record bytes. meta.tx_id must match key_b's txid for
        // the BUG-1 tx_id identity check inside replay_create to pass.
        let mut meta_b = TxMetadata::new(utxo_count);
        meta_b.tx_id = txid_b;
        meta_b.record_size = record_size as u32;
        let slot_b = UtxoSlot::new_unspent([0xBB; 32]);

        let mut record_bytes = Vec::with_capacity(METADATA_SIZE + UTXO_SLOT_SIZE);
        let mut mb = [0u8; METADATA_SIZE];
        meta_b.to_bytes(&mut mb);
        record_bytes.extend_from_slice(&mb);
        let mut sb = [0u8; UTXO_SLOT_SIZE];
        slot_b.to_bytes(&mut sb);
        record_bytes.extend_from_slice(&sb);

        // Append a Create for key_b at offset_a. This is the redo entry that
        // survived the crash; the primary-index update for key_b did not.
        let mut redo = RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap();
        redo.append_and_flush(RedoOp::Create {
            tx_key: key_b,
            device_id: 0,
            record_offset: offset_a,
            utxo_count,
            is_conflicting: false,
            record_bytes,
            parent_txids: Vec::new(),
        })
        .unwrap();
        drop(redo);

        // Run recovery. `build_offset_owners` maps offset_a → key_a.
        // `register_unique_offset` for key_b at offset_a evicts key_a first.
        let redo = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
        let stats = recover(&*data_dev, &redo, &index).unwrap();

        assert_eq!(stats.entries_replayed, 1, "Create for key_b must replay");
        assert_eq!(stats.failed_io, 0);
        assert_eq!(stats.failed_corrupt, 0);

        // key_a must be evicted (no longer in any shard).
        assert!(
            index.lookup(&key_a).is_none(),
            "stale alias key_a must be evicted from the index after key_b's Create replays"
        );

        // key_b must be present at offset_a.
        let entry_b = index
            .lookup(&key_b)
            .expect("key_b must be registered after Create replay");
        assert_eq!(
            entry_b.record_offset, offset_a,
            "key_b must point to offset_a after evicting the alias"
        );
    }

    #[test]
    fn restore_last_durable_height_floors_at_record_height_without_tombstones() {
        // BUG3 core: a node with a LOST `.height` file (persisted = None) and
        // tombstones DISABLED (so the tombstone floor is 0) must still restore
        // its height to the live-record floor H — NOT 0.
        let h = RecoveryTestHarness::new();
        let engine = engine_from_harness(h);

        let big_h = 850_000u32;
        // persisted=None (file lost), record_floor=H (from replayed records).
        // No tombstones involved — the floor comes purely from the record
        // height path, proving independence from `tombstones_enabled`.
        let restored = engine.restore_last_durable_height(None, big_h);
        assert_eq!(
            restored, big_h,
            "must floor at the live-record height, not 0"
        );
        assert_eq!(engine.last_durable_height(), big_h);

        // Monotone: a later restore with a lower floor never regresses.
        let restored2 = engine.restore_last_durable_height(None, 100);
        assert_eq!(restored2, big_h);

        // The persisted file, when present and higher, still wins.
        let restored3 = engine.restore_last_durable_height(Some(900_000), 0);
        assert_eq!(restored3, 900_000);
    }
}
