//! Garbage collection of orphaned external blobs (R-049).
//!
//! TeraSlab stores transaction payloads larger than the inline-tier threshold
//! in an external blob store keyed by txid (`src/storage/blobstore.rs`). The
//! authoritative reference to a blob lives in the primary index entry's
//! `tx_flags`: a record marked [`TxFlags::EXTERNAL`] points at a blob; a
//! record without that flag does not own a blob.
//!
//! Several failure paths leak blobs that are never reclaimed by the foreground
//! mutation pipeline:
//!
//! 1. **Failed creates.** A client uploads the payload (a successful blob
//!    `put`/`finish`), then the create-record dispatch fails between the blob
//!    write and the index registration — the index never points at the blob.
//! 2. **Aborted uploads.** A streaming `OP_STREAM_CHUNK` upload finishes the
//!    blob but the subsequent index registration is rejected (e.g. record
//!    already exists, replication ACK timeout in `reject` mode).
//! 3. **Migration cancellation.** A migration target receives the blob bytes
//!    via the streaming opcodes (`OP_STREAM_CHUNK` / `OP_STREAM_END`, see
//!    `src/protocol/opcodes.rs`) and then the migration is rolled back —
//!    the index references stamped during apply are reverted, but the blob
//!    remains. (F-G9-010: there is no separate `OP_BLOB_PUT` opcode; all
//!    blob ingress uses the streaming path.)
//!
//! Without a periodic reconciliation pass these orphans accumulate on disk
//! forever (audit finding IJK-08). This module implements two reconciliation
//! sweeps:
//!
//! * [`reconcile_orphan_blobs`] — one-shot sweep used by recovery on startup
//!   (after the redo log has been replayed and the primary index reflects the
//!   committed state) and by the periodic background task on each tick.
//! * [`spawn_blob_gc_task`] — long-running thread that calls
//!   [`reconcile_orphan_blobs`] every `interval_secs`.
//!
//! Both sweeps walk [`BlobStore::list`]; what happens to an unreferenced blob
//! depends on the pass (recovery vs. periodic, see `SweepPass`) and the
//! failure class:
//!
//! * entry present WITHOUT [`TxFlags::EXTERNAL`] → **quarantined** (retained
//!   and logged loudly) by BOTH passes. A live record with this exact txid
//!   exists, so the missing flag marks an upstream flag-fidelity defect —
//!   the blob may be that record's only payload copy (the CI scenario-11
//!   data loss, run 31787458246) and must survive for repair.
//! * no primary-index entry → **quarantined** by the recovery pass (boot-time
//!   heals can still re-register the key), **deleted** by the periodic sweep
//!   (genuine debris from failed creates / aborted uploads / cancelled
//!   migrations).
//!
//! The same call also sweeps stale `.tmp` upload artefacts from
//! the file backend (see [`crate::storage::blobstore::FileBlobStore::STALE_TMP_AGE_SECS`]).
//!
//! The periodic sweep additionally synchronizes with in-flight creates via
//! two mechanisms: the F-G9-004 mtime grace filter (skips blobs uploaded
//! moments ago) and the F-IJ-002 pin handshake
//! ([`crate::storage::blobstore::BlobPinSet`]) which protects blobs OLDER
//! than the grace that an in-flight create is about to reference — the
//! create pins the txid before its digest check, and the sweep re-verifies
//! "unpinned AND still unreferenced" under the pin stripe lock immediately
//! before each unlink.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::index::{ShardedIndex, TxKey};
use crate::ops::engine::Engine;
use crate::record::TxFlags;
use crate::storage::blobstore::{BlobError, BlobPinSet, BlobStore, PinSweepOutcome};

/// Minimum age a blob must reach before the periodic [`reconcile_orphan_blobs`]
/// sweep will consider it for deletion (F-G9-004).
///
/// The dispatch path orders `blob_store.put` BEFORE the index `register`. A
/// concurrent sweep that observes the blob between those two operations
/// would mis-classify it as an orphan. The 60-second grace gives the
/// in-flight create enough time to land its index registration even under
/// substantial replication lag.
///
/// The grace only protects blobs whose mtime is FRESH. A blob uploaded long
/// before its create lands (clients may legitimately stream the blob, then
/// send `OP_CREATE_BATCH` minutes later) is past the grace; those are
/// protected by the [`crate::storage::blobstore::BlobPinSet`] handshake
/// instead (F-IJ-002) — see [`reconcile_orphan_blobs`].
///
/// Recovery's reconciliation is race-free (no clients connected) and uses
/// the un-aged [`BlobStore::list`] path; this constant only applies to the
/// runtime [`reconcile_orphan_blobs`] sweep.
pub const PERIODIC_GC_MIN_BLOB_AGE: Duration = Duration::from_secs(60);

/// Counters returned by a single reconciliation sweep.
///
/// `total_blobs` is the number of blobs returned by [`BlobStore::list`] — the
/// upper bound on `kept + deleted_*`. `delete_failed` is non-zero when the
/// underlying store returned an error for a `delete` call (e.g. transient I/O
/// error); the blob will be retried on the next sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BlobGcStats {
    /// Number of blobs enumerated by [`BlobStore::list`] this sweep.
    pub total_blobs: u64,
    /// Blobs whose primary-index entry exists with [`TxFlags::EXTERNAL`] —
    /// kept.
    pub kept: u64,
    /// Blobs whose primary-index entry was absent — deleted as orphan
    /// (periodic sweep only; the recovery pass quarantines instead).
    pub deleted_no_index: u64,
    /// Blobs whose primary-index entry was absent at RECOVERY time —
    /// retained (quarantined), because recovery can transiently miss entries
    /// that a queued reverse-heal (G3 lost-create) or replica resync
    /// re-registers moments later. Genuine orphans are reclaimed by the
    /// periodic sweep once the node is serving.
    pub quarantined_no_index: u64,
    /// Blobs whose primary-index entry was present but **without**
    /// [`TxFlags::EXTERNAL`] — retained (quarantined) and logged loudly.
    /// An existing entry means a live record with this exact txid; a missing
    /// flag on it is a flag-fidelity defect upstream, not proof the blob is
    /// debris. Deleting it destroys the record's only payload copy (the CI
    /// scenario-11 data loss). Never deleted by any pass.
    pub quarantined_not_external: u64,
    /// Blobs that the store refused to delete (logged at warn; retried next
    /// sweep). Counted but not counted as `kept` either.
    pub delete_failed: u64,
    /// Blobs skipped because an in-flight create holds a pin on the txid
    /// (F-IJ-002). Re-examined on the next sweep: by then the create has
    /// either registered the index entry (blob becomes `kept`) or failed and
    /// released the pin (blob becomes an orphan and is deleted).
    pub skipped_pinned: u64,
}

impl BlobGcStats {
    /// Total blobs successfully deleted by this sweep.
    pub fn deleted_total(&self) -> u64 {
        self.deleted_no_index
    }

    /// Total blobs retained (quarantined) by this sweep instead of deleted.
    pub fn quarantined_total(&self) -> u64 {
        self.quarantined_no_index + self.quarantined_not_external
    }
}

/// Which reconciliation pass is running — decides how a blob with NO
/// primary-index entry is handled (an entry present WITHOUT
/// [`TxFlags::EXTERNAL`] is quarantined by BOTH passes; see
/// [`BlobGcStats::quarantined_not_external`]).
///
/// * [`SweepPass::Recovery`] — the one-shot startup pass. Deletes NOTHING:
///   the primary index at this point can transiently miss entries that are
///   re-registered moments later — a buffered-tail-lost `CreateV2` whose key
///   the G3 reverse-heal pull re-fetches from a quorum-current replica, or a
///   `ReplicaRecordAbsent` create the master resyncs on rejoin. Both heals
///   run AFTER this pass, so a recovery-time delete races them and turns a
///   transient gap into permanent payload loss (CI scenario-11,
///   run 31787458246). Blobs are cheap; lost records are not.
/// * [`SweepPass::Periodic`] — the steady-state background sweep. By the
///   time it ticks (default hourly) the boot-time heals have landed, so a
///   blob with no index entry is genuine debris: deleted, guarded by the
///   F-G9-004 age grace and the F-IJ-002 pin handshake it CARRIES — the
///   variant owns the [`BlobPinSet`] reference precisely so an unguarded
///   periodic delete is unrepresentable (a prior `Option<&BlobPinSet>`
///   parameter left a dead `None → unguarded delete` arm a future caller
///   could silently reach).
///
/// Private on purpose: the public entry points
/// ([`reconcile_orphan_blobs_with`] for recovery,
/// [`reconcile_orphan_blobs_with_pins`] / [`reconcile_orphan_blobs`] for the
/// periodic sweep) each hard-code their pass.
enum SweepPass<'a> {
    /// First post-recovery reconciliation — quarantine everything.
    Recovery,
    /// Steady-state periodic sweep — delete no-index orphans (always
    /// pin-guarded via the carried set), quarantine
    /// entry-present-without-EXTERNAL blobs.
    Periodic { pins: &'a BlobPinSet },
}

/// Result of the per-key index lookup performed by the GC sweep.
///
/// Returned by the lookup closure passed to [`reconcile_orphan_blobs_with`]
/// so the caller can plug in either the runtime [`Engine`] (live operation)
/// or a borrowed `PrimaryBackend` (recovery-time, before the engine has
/// been built).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupOutcome {
    /// No primary-index entry exists for this txid — quarantined by the
    /// recovery pass, deleted (pin-guarded) by the periodic sweep.
    NoEntry,
    /// A primary-index entry exists. `external` is read from the record's
    /// on-device [`TxFlags::EXTERNAL`] footer flag (the slim primary index no
    /// longer caches flags); `true` means the record owns this blob and it is
    /// KEPT, `false` means the footer does not reference a blob — the blob is
    /// QUARANTINED (retained + logged loudly), never deleted: a live record
    /// with this exact txid exists, so the missing flag is a flag-fidelity
    /// defect upstream, not proof of debris (CI scenario-11 data loss).
    Found { external: bool },
}

/// Walk every blob in `blob_store` and reconcile it against the primary
/// index under recovery-pass semantics: blobs whose entry exists with
/// [`TxFlags::EXTERNAL`] are kept; EVERYTHING else is QUARANTINED (retained
/// and logged and counted), never deleted — the recovery-time index can
/// transiently miss entries that boot-time heals re-register, and an entry
/// present without the flag marks an upstream flag-fidelity defect whose
/// payload must survive as the potential LAST COPY for repair.
///
/// Generic over the index lookup so the same logic can run against the
/// runtime [`Engine`] (background sweep) or a borrowed `PrimaryBackend`
/// (recovery, before the engine has been constructed).
///
/// Returns aggregate counters for observability. Errors from the underlying
/// `list` call are propagated.
///
/// **Concurrency contract.** This entry point performs NO synchronization
/// against in-flight creates: it is for recovery-time use only, after the
/// redo replay has completed and before any client is connected, so a
/// half-completed create cannot exist. Runtime sweeps must use
/// [`reconcile_orphan_blobs`] (grace filter F-G9-004 + pin handshake
/// F-IJ-002) or [`reconcile_orphan_blobs_with_pins`] instead.
pub fn reconcile_orphan_blobs_with<F>(
    blob_store: &dyn BlobStore,
    mut lookup: F,
) -> Result<BlobGcStats, BlobError>
where
    F: FnMut(&TxKey) -> LookupOutcome,
{
    reconcile_orphan_blobs_with_filter(blob_store, None, SweepPass::Recovery, &mut lookup)
}

/// Pin-aware sweep against an arbitrary index lookup (F-IJ-002).
///
/// Like [`reconcile_orphan_blobs_with`], but every candidate unlink is routed
/// through [`BlobPinSet::delete_orphan_guarded`]: under the pin stripe lock
/// the sweep re-invokes `lookup` and only deletes when the candidate is still
/// unpinned AND still unreferenced. A blob whose index registration landed
/// between candidate classification and the unlink is therefore kept, and a
/// blob pinned by an in-flight create is skipped
/// ([`BlobGcStats::skipped_pinned`]).
///
/// `min_blob_age` applies the F-G9-004 grace filter when `Some`; pass `None`
/// to examine every blob (tests, stores without per-blob mtime).
pub fn reconcile_orphan_blobs_with_pins<F>(
    blob_store: &dyn BlobStore,
    min_blob_age: Option<Duration>,
    pins: &BlobPinSet,
    mut lookup: F,
) -> Result<BlobGcStats, BlobError>
where
    F: FnMut(&TxKey) -> LookupOutcome,
{
    reconcile_orphan_blobs_with_filter(
        blob_store,
        min_blob_age,
        SweepPass::Periodic { pins },
        &mut lookup,
    )
}

/// Internal helper shared by recovery (no min-age filter,
/// [`SweepPass::Recovery`]) and the periodic sweep (min-age filter + the
/// pin handshake carried on [`SweepPass::Periodic`]). See
/// [`reconcile_orphan_blobs_with`], [`reconcile_orphan_blobs_with_pins`]
/// and [`reconcile_orphan_blobs`] for the public entry points.
fn reconcile_orphan_blobs_with_filter<F>(
    blob_store: &dyn BlobStore,
    min_blob_age: Option<Duration>,
    pass: SweepPass<'_>,
    lookup: &mut F,
) -> Result<BlobGcStats, BlobError>
where
    F: FnMut(&TxKey) -> LookupOutcome,
{
    let keys = match min_blob_age {
        Some(age) => blob_store.list_for_gc(age)?,
        None => blob_store.list()?,
    };
    let mut stats = BlobGcStats {
        total_blobs: keys.len() as u64,
        ..Default::default()
    };

    for txid in keys {
        let key = TxKey { txid };
        match lookup(&key) {
            LookupOutcome::Found { external: true } => {
                stats.kept += 1;
                continue;
            }
            LookupOutcome::Found { external: false } => {
                // QUARANTINE, never delete — in EVERY pass. A primary-index
                // entry exists, so a live record with this exact txid is
                // being served; its footer lacking EXTERNAL is a
                // flag-fidelity defect upstream (wire/apply/recovery), not
                // proof the blob is debris. Deleting here destroyed 7
                // externalized records' payloads in CI scenario-11
                // (run 31787458246). Note the retention is NOT for heals to
                // re-reference in place — a heal/re-migration re-ships the
                // payload inline and overwrites via `put` regardless. Its
                // value is LAST-COPY retention: when no peer holds the
                // record (RF=1, or every holder lost it), this blob is the
                // only surviving payload and the raw material for repair.
                // There is no automated flag repair yet (tracked
                // separately); the leak is bounded and observable
                // (`teraslab_blob_gc_quarantined_not_external_total`).
                stats.quarantined_not_external += 1;
                tracing::warn!(
                    txid = %hex_txid(&txid),
                    "blob_gc: QUARANTINE — primary-index entry exists but its record \
                     footer is not flagged EXTERNAL; retaining blob (possible \
                     flag-fidelity defect upstream — never delete a possibly-referenced \
                     payload). Investigate this record's flag integrity.",
                );
                continue;
            }
            LookupOutcome::NoEntry => {}
        }

        // No primary-index entry.
        let pins = match pass {
            SweepPass::Recovery => {
                // First post-recovery pass: the index can transiently miss
                // entries that the G3 reverse-heal pull or a replica resync
                // re-registers AFTER this sweep — deleting now races the heal
                // (the scenario-11 `deleted_no_index` loss). Retain; the
                // periodic sweep reclaims genuine orphans later.
                stats.quarantined_no_index += 1;
                tracing::warn!(
                    txid = %hex_txid(&txid),
                    "blob_gc: QUARANTINE — no primary-index entry at recovery; retaining \
                     blob (a queued reverse-heal / replica resync may re-register the key; \
                     the periodic sweep reclaims genuine orphans once serving)",
                );
                continue;
            }
            SweepPass::Periodic { pins } => pins,
        };

        // Periodic sweep deletion — ALWAYS pin-guarded (the pin set is
        // carried on the Periodic variant, so an unguarded delete is
        // unrepresentable). Re-verify "unpinned AND still without ANY index
        // entry" under the pin stripe lock immediately before the unlink so
        // a create racing between the classification above and this point
        // (F-IJ-002 TOCTOU) cannot lose its blob. The re-check requires
        // `NoEntry` (not merely "not external"): an entry that appeared
        // flag-less in the window is the quarantine class and must not be
        // deleted either.
        let outcome = pins.delete_orphan_guarded(
            &txid,
            || matches!(lookup(&key), LookupOutcome::NoEntry),
            || blob_store.delete(&txid),
        );
        match outcome {
            Ok(PinSweepOutcome::Deleted) => {
                stats.deleted_no_index += 1;
                tracing::info!(
                    txid = %hex_txid(&txid),
                    "blob_gc: deleted orphan blob (no primary-index entry)",
                );
            }
            Ok(PinSweepOutcome::SkippedPinned) => {
                stats.skipped_pinned += 1;
                tracing::info!(
                    txid = %hex_txid(&txid),
                    "blob_gc: skipped blob pinned by in-flight create; will re-examine next sweep",
                );
            }
            Ok(PinSweepOutcome::SkippedReferenced) => {
                // An index registration landed between classification and
                // the unlink — the blob is (or may be) live. Counted as kept;
                // the next sweep re-classifies it (kept or quarantined).
                stats.kept += 1;
            }
            Err(e) => {
                stats.delete_failed += 1;
                tracing::warn!(
                    txid = %hex_txid(&txid),
                    err = %e,
                    "blob_gc: failed to delete orphan blob (no primary-index entry); \
                     will retry next sweep",
                );
            }
        }
    }

    Ok(stats)
}

/// Recovery-time sweep against a borrowed [`ShardedIndex`].
///
/// Called from [`crate::recovery::reconcile_blobs_after_recovery`] after the
/// redo replay has finished. At this point no client is connected to the
/// server, so the concurrency race described on
/// [`reconcile_orphan_blobs_with`] cannot occur. Runs under recovery-pass
/// semantics: nothing is deleted — unreferenced blobs are quarantined
/// (retained and logged) because boot-time heals (G3 reverse-heal pulls,
/// replica resync) run AFTER this sweep and can re-register keys the
/// replayed index transiently lacks; the periodic sweep reclaims genuine
/// orphans once the node is serving. Retention here is last-copy insurance
/// — a heal re-ships payload bytes inline and overwrites via `put`, so it
/// does not need the local blob; but when no peer holds the record, the
/// retained blob is the only surviving payload for repair.
pub fn reconcile_orphan_blobs_against_index(
    blob_store: &dyn BlobStore,
    index: &ShardedIndex,
    devices: &[Arc<dyn crate::device::BlockDevice>],
) -> Result<BlobGcStats, BlobError> {
    reconcile_orphan_blobs_with(blob_store, |key| match index.lookup(key) {
        Some(entry) => {
            // The slim primary index no longer caches `tx_flags`, so read the
            // record's EXTERNAL flag from its on-device footer via the entry's
            // locator. On ANY ambiguity (device out of range, unreadable /
            // CRC-failed footer, or a footer whose `tx_id` does not match this
            // key) keep the blob — never delete a possibly-referenced blob on
            // an uncertain read; the periodic engine sweep re-examines it.
            let external = devices
                .get(entry.device_id as usize)
                .and_then(|dev| crate::io::read_metadata(dev.as_ref(), entry.record_offset).ok())
                .map(|meta| meta.tx_id != key.txid || meta.flags.contains(TxFlags::EXTERNAL))
                .unwrap_or(true);
            LookupOutcome::Found { external }
        }
        None => LookupOutcome::NoEntry,
    })
}

/// Live sweep against the runtime [`Engine`] used by the periodic background
/// task. The engine acquires its own index read lock per lookup.
///
/// Two protections against racing an in-flight create (whose dispatch orders
/// blob `put` BEFORE the index `register`):
///
/// * F-G9-004: the [`PERIODIC_GC_MIN_BLOB_AGE`] grace-period filter excludes
///   blobs whose mtime is fresh — covers creates whose blob was uploaded
///   moments ago.
/// * F-IJ-002: blobs OLDER than the grace (a client may stream the blob and
///   send the create minutes later) are protected by the engine's
///   [`Engine::blob_pins`] handshake — the create pins the txid before its
///   digest check, and this sweep re-verifies "unpinned AND still
///   unreferenced" under the pin stripe lock immediately before each unlink.
pub fn reconcile_orphan_blobs(
    blob_store: &dyn BlobStore,
    engine: &Engine,
) -> Result<BlobGcStats, BlobError> {
    // The slim primary index no longer caches `tx_flags`, so consult the
    // record's on-device footer for the EXTERNAL flag. `engine.read_metadata`
    // resolves the locator AND verifies `meta.tx_id == key.txid` (F-G2-001):
    // a `TxNotFound` (absent or offset aliased to another record) means no
    // live record owns this blob → treat as `NoEntry` (delete the orphan). A
    // transient storage error keeps the blob (conservative — never delete a
    // possibly-referenced blob on an uncertain read).
    let mut lookup = |key: &TxKey| match engine.read_metadata(key) {
        Ok(meta) => LookupOutcome::Found {
            external: meta.flags.contains(TxFlags::EXTERNAL),
        },
        Err(crate::ops::error::SpendError::TxNotFound) => LookupOutcome::NoEntry,
        Err(_) => LookupOutcome::Found { external: true },
    };
    reconcile_orphan_blobs_with_filter(
        blob_store,
        Some(PERIODIC_GC_MIN_BLOB_AGE),
        SweepPass::Periodic {
            pins: engine.blob_pins(),
        },
        &mut lookup,
    )
}

/// Format a 32-byte txid as a lowercase hex string for log lines.
fn hex_txid(txid: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in txid {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Configuration for the background blob-GC task.
#[derive(Debug, Clone)]
pub struct BlobGcConfig {
    /// Wall-clock interval between full reconciliation sweeps. The default in
    /// [`crate::config::ServerConfig`] is one hour; smaller values reclaim
    /// orphans faster at the cost of repeated `BlobStore::list` walks.
    pub interval: Duration,
    /// Granularity of the cooperative shutdown poll. Should be much smaller
    /// than `interval` so an operator-initiated shutdown does not wait a full
    /// sweep cycle to take effect.
    pub poll_interval: Duration,
}

impl BlobGcConfig {
    /// Build a config with the given sweep interval. Uses a 1-second poll
    /// granularity for shutdown checks.
    pub fn new(interval_secs: u64) -> Self {
        Self {
            interval: Duration::from_secs(interval_secs),
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Spawn the background blob-GC task. The task runs until `shutdown` is set.
///
/// Each iteration sleeps `config.interval` (in `poll_interval` chunks so
/// shutdown is responsive) and then runs [`reconcile_orphan_blobs`]. Errors
/// from a single sweep are logged at `error` and do NOT stop the task — a
/// transient enumeration failure should not leave orphans accumulating
/// forever.
pub fn spawn_blob_gc_task(
    config: BlobGcConfig,
    blob_store: Arc<dyn BlobStore>,
    engine: Arc<Engine>,
    shutdown: Arc<AtomicBool>,
    pause: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("teraslab-blob-gc".to_string())
        .spawn(move || {
            tracing::info!(
                interval_secs = config.interval.as_secs(),
                "blob-gc task started",
            );
            // Cooperative sleep: sleep `poll_interval` at a time so a
            // shutdown signal is observed within at most `poll_interval`,
            // even when `interval` is set to many minutes.
            let mut elapsed = Duration::ZERO;
            while !shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(config.poll_interval);
                elapsed += config.poll_interval;
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                if elapsed < config.interval {
                    continue;
                }
                elapsed = Duration::ZERO;

                // An online backup pauses the sweep so no blobs are unlinked
                // while it copies the blob-store tree. GC only ever removes
                // ORPHAN blobs (no primary-index entry at all), so a
                // referenced blob is never at risk, but skipping the sweep
                // entirely avoids racing on the directory listing and
                // needless churn.
                if pause.load(Ordering::Relaxed) {
                    tracing::debug!("blob-gc sweep skipped (paused for backup)");
                    continue;
                }

                let started = std::time::Instant::now();
                match reconcile_orphan_blobs(blob_store.as_ref(), engine.as_ref()) {
                    Ok(stats) => {
                        // Feed per-sweep quarantine counts into the
                        // scrape-visible `teraslab_blob_gc_quarantined_*`
                        // counters (accumulated; re-observations re-count).
                        let bg = crate::metrics::blob_gc_metrics();
                        bg.quarantined_no_index_total
                            .add(stats.quarantined_no_index);
                        bg.quarantined_not_external_total
                            .add(stats.quarantined_not_external);
                        tracing::info!(
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            total_blobs = stats.total_blobs,
                            kept = stats.kept,
                            deleted_no_index = stats.deleted_no_index,
                            quarantined_not_external = stats.quarantined_not_external,
                            delete_failed = stats.delete_failed,
                            "blob-gc sweep complete",
                        );
                    }
                    Err(e) => {
                        tracing::error!(err = %e, "blob-gc sweep failed");
                    }
                }
            }
            tracing::info!("blob-gc task exiting");
        })
        .expect("spawn blob-gc thread")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::MemoryDevice;
    use crate::index::PrimaryBackend;
    use crate::locks::StripedLocks;
    use crate::ops::engine::Engine;
    use crate::record::TxFlags;
    use crate::storage::blobstore::MemoryBlobStore;

    fn make_engine() -> (Arc<Engine>, Arc<MemoryBlobStore>) {
        let device: Arc<dyn crate::device::BlockDevice> =
            Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
        let allocator = SlotAllocator::new(device.clone()).unwrap();
        let index = PrimaryBackend::new_in_memory(1024).unwrap();
        let dah = crate::index::DahBackend::new_in_memory();
        let locks = StripedLocks::new(64);
        let mut engine = Engine::new(device, index, allocator, locks, dah);
        let blob_store = Arc::new(MemoryBlobStore::new());
        engine.set_blob_store(blob_store.clone() as Arc<dyn BlobStore>);
        (Arc::new(engine), blob_store)
    }

    fn txid(n: u8) -> [u8; 32] {
        let mut t = [0u8; 32];
        t[0] = n;
        t
    }

    /// Write a real on-device record carrying `flags` and register its locator
    /// in the primary index. The slim primary index no longer caches
    /// `tx_flags`, so the blob-GC sweep reads the [`TxFlags::EXTERNAL`] bit from
    /// the record's on-device footer via `engine.read_metadata`; the footer
    /// must therefore actually exist (with a matching `tx_id`) for the sweep to
    /// classify the blob as blob-owning vs. orphan debris.
    fn insert_index_entry(engine: &Engine, key: &[u8; 32], flags: TxFlags) {
        use crate::record::{TxMetadata, UtxoSlot};

        let utxo_count = 1u32;
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = *key;
        meta.flags = flags;

        let record_size = TxMetadata::record_size_for(utxo_count);
        let offset = {
            let mut alloc = engine.allocator().lock();
            alloc.allocate(record_size).expect("allocate record")
        };

        let slots = vec![UtxoSlot::new_unspent([0u8; 32]); utxo_count as usize];
        crate::io::write_full_record(engine.device(), offset, &meta, &slots)
            .expect("write record footer");

        let entry = crate::index::TxIndexEntry {
            device_id: 0,
            record_offset: offset,
            mined_slot: crate::index::mined_index::NO_MINED_SLOT,
        };
        engine
            .register(crate::index::TxKey { txid: *key }, entry)
            .expect("register index entry");
    }

    /// Build a bare `ShardedIndex` + device + allocator (no engine) for
    /// driving the RECOVERY entry point
    /// [`reconcile_orphan_blobs_against_index`] the way `bin/server.rs` does.
    fn make_recovery_fixture() -> (
        crate::index::ShardedIndex,
        Arc<dyn crate::device::BlockDevice>,
        SlotAllocator,
        Arc<MemoryBlobStore>,
    ) {
        let device: Arc<dyn crate::device::BlockDevice> =
            Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
        let allocator = SlotAllocator::new(device.clone()).unwrap();
        let index =
            crate::index::ShardedIndex::from_single(PrimaryBackend::new_in_memory(1024).unwrap());
        let blob_store = Arc::new(MemoryBlobStore::new());
        (index, device, allocator, blob_store)
    }

    /// Write a real on-device record carrying `flags` and register its locator
    /// directly in a bare `ShardedIndex` (recovery-time shape, no engine).
    fn insert_bare_index_entry(
        index: &crate::index::ShardedIndex,
        device: &dyn crate::device::BlockDevice,
        alloc: &mut SlotAllocator,
        key: &[u8; 32],
        flags: TxFlags,
    ) {
        use crate::record::{TxMetadata, UtxoSlot};

        let utxo_count = 1u32;
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = *key;
        meta.flags = flags;

        let record_size = TxMetadata::record_size_for(utxo_count);
        let offset = alloc.allocate(record_size).expect("allocate record");
        let slots = vec![UtxoSlot::new_unspent([0u8; 32]); utxo_count as usize];
        crate::io::write_full_record(device, offset, &meta, &slots).expect("write record footer");
        index
            .register(
                TxKey { txid: *key },
                crate::index::TxIndexEntry {
                    device_id: 0,
                    record_offset: offset,
                    mined_slot: crate::index::mined_index::NO_MINED_SLOT,
                },
            )
            .expect("register index entry");
    }

    /// CI 31787458246 scenario 11 (node1 restart): a primary-index entry
    /// EXISTS but its record footer lacks the EXTERNAL flag. That state is a
    /// flag-fidelity defect upstream, NOT proof the blob is debris — deleting
    /// the blob here permanently destroyed 7 externalized records' payloads.
    /// The recovery pass must QUARANTINE (retain + count) instead of delete.
    #[test]
    fn recovery_reconcile_quarantines_blob_when_index_entry_lacks_external_flag() {
        let (index, device, mut alloc, blob_store) = make_recovery_fixture();
        let key = txid(0x66);
        blob_store.put(&key, b"payload-still-referenced").unwrap();
        insert_bare_index_entry(&index, &*device, &mut alloc, &key, TxFlags::empty());

        let stats = reconcile_orphan_blobs_against_index(
            blob_store.as_ref() as &dyn BlobStore,
            &index,
            &[device],
        )
        .unwrap();
        assert!(
            blob_store.exists(&key).unwrap(),
            "recovery must QUARANTINE (retain) a blob whose index entry exists \
             without the EXTERNAL flag — deleting it is unrecoverable data loss",
        );
        assert_eq!(stats.total_blobs, 1);
        assert_eq!(stats.quarantined_not_external, 1);
        assert_eq!(stats.deleted_total(), 0);
        // Still readable — last-copy retention: if no peer holds this
        // record, this blob is the only surviving payload for repair.
        assert_eq!(
            blob_store.get(&key).unwrap().unwrap(),
            b"payload-still-referenced".to_vec(),
        );
    }

    /// CI 31787458246 scenario 11 (node1 restart, deleted_no_index=1): the
    /// recovery pass runs BEFORE the G3 lost-create reverse-heal pulls and the
    /// replica resync, both of which can legitimately re-register a key whose
    /// index entry recovery transiently missed (SkippedMissingCreateBytes /
    /// ReplicaRecordAbsent). The FIRST post-recovery pass must therefore
    /// retain no-index blobs too; the periodic sweep reclaims genuine orphans
    /// once the node is serving and heals have landed.
    #[test]
    fn recovery_reconcile_quarantines_blob_with_no_index_entry() {
        let (index, device, _alloc, blob_store) = make_recovery_fixture();
        let orphan = txid(0x67);
        blob_store.put(&orphan, b"maybe-healed-later").unwrap();
        // No index entry — at recovery time this is NOT proof of debris.

        let stats = reconcile_orphan_blobs_against_index(
            blob_store.as_ref() as &dyn BlobStore,
            &index,
            &[device],
        )
        .unwrap();
        assert!(
            blob_store.exists(&orphan).unwrap(),
            "the first post-recovery pass must retain no-index blobs — a queued \
             reverse-heal / replica resync may still re-register the key",
        );
        assert_eq!(stats.total_blobs, 1);
        assert_eq!(stats.quarantined_no_index, 1);
        assert_eq!(stats.deleted_total(), 0);
    }

    #[test]
    fn reconcile_keeps_external_blobs() {
        let (engine, blob_store) = make_engine();
        let key = txid(1);
        blob_store.put(&key, b"payload").unwrap();
        insert_index_entry(&engine, &key, TxFlags::EXTERNAL);

        let stats =
            reconcile_orphan_blobs(blob_store.as_ref() as &dyn BlobStore, engine.as_ref()).unwrap();
        assert_eq!(stats.total_blobs, 1);
        assert_eq!(stats.kept, 1);
        assert_eq!(stats.deleted_total(), 0);
        assert!(blob_store.exists(&key).unwrap());
    }

    #[test]
    fn reconcile_deletes_blob_with_no_index_entry() {
        let (engine, blob_store) = make_engine();
        let orphan = txid(0xAA);
        blob_store.put(&orphan, b"leaked").unwrap();
        // No index entry registered — the PERIODIC sweep deletes it (the node
        // is serving; boot-time heals have landed, so this is genuine debris).

        let stats =
            reconcile_orphan_blobs(blob_store.as_ref() as &dyn BlobStore, engine.as_ref()).unwrap();
        assert_eq!(stats.total_blobs, 1);
        assert_eq!(stats.kept, 0);
        assert_eq!(stats.deleted_no_index, 1);
        assert_eq!(stats.quarantined_total(), 0);
        assert!(!blob_store.exists(&orphan).unwrap());
    }

    /// The periodic sweep must ALSO quarantine (not delete) a blob whose
    /// index entry exists without the EXTERNAL flag: a live record with this
    /// txid exists, so the missing flag is an upstream flag-fidelity defect
    /// and the blob may be that record's only payload copy. Pre-fix this was
    /// deleted — the same data-loss class as the recovery pass, just an hour
    /// later.
    #[test]
    fn periodic_reconcile_quarantines_blob_when_index_entry_lacks_external_flag() {
        let (engine, blob_store) = make_engine();
        let key = txid(0xBB);
        blob_store.put(&key, b"possibly-referenced").unwrap();
        // Index entry exists but EXTERNAL flag is NOT set.
        insert_index_entry(&engine, &key, TxFlags::IS_COINBASE);

        let stats =
            reconcile_orphan_blobs(blob_store.as_ref() as &dyn BlobStore, engine.as_ref()).unwrap();
        assert_eq!(stats.total_blobs, 1);
        assert_eq!(stats.quarantined_not_external, 1);
        assert_eq!(stats.deleted_total(), 0);
        assert!(
            blob_store.exists(&key).unwrap(),
            "entry-present-without-flag blobs must survive the periodic sweep",
        );
    }

    #[test]
    fn reconcile_mixed_set() {
        let (engine, blob_store) = make_engine();
        // Three categories: kept, no-index orphan (periodic → deleted),
        // entry-without-flag (→ quarantined).
        let keep = txid(1);
        let orphan_no_index = txid(2);
        let quarantine_no_flag = txid(3);
        blob_store.put(&keep, b"k").unwrap();
        blob_store.put(&orphan_no_index, b"o1").unwrap();
        blob_store.put(&quarantine_no_flag, b"o2").unwrap();
        insert_index_entry(&engine, &keep, TxFlags::EXTERNAL);
        insert_index_entry(&engine, &quarantine_no_flag, TxFlags::empty());

        let stats =
            reconcile_orphan_blobs(blob_store.as_ref() as &dyn BlobStore, engine.as_ref()).unwrap();
        assert_eq!(stats.total_blobs, 3);
        assert_eq!(stats.kept, 1);
        assert_eq!(stats.deleted_no_index, 1);
        assert_eq!(stats.quarantined_not_external, 1);
        assert!(blob_store.exists(&keep).unwrap());
        assert!(!blob_store.exists(&orphan_no_index).unwrap());
        assert!(blob_store.exists(&quarantine_no_flag).unwrap());
    }
}
