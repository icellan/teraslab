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
//! Both sweeps walk [`BlobStore::list`] and delete every blob whose primary
//! index entry is absent OR present without the [`TxFlags::EXTERNAL`] flag —
//! both cases signal a blob the foreground pipeline does not (and never will)
//! reference. The same call also sweeps stale `.tmp` upload artefacts from
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
    /// Blobs whose primary-index entry was absent — deleted as orphan.
    pub deleted_no_index: u64,
    /// Blobs whose primary-index entry was present but **without**
    /// [`TxFlags::EXTERNAL`] — deleted as orphan.
    pub deleted_not_external: u64,
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
        self.deleted_no_index + self.deleted_not_external
    }
}

/// Result of the per-key index lookup performed by the GC sweep.
///
/// Returned by the lookup closure passed to [`reconcile_orphan_blobs_with`]
/// so the caller can plug in either the runtime [`Engine`] (live operation)
/// or a borrowed `PrimaryBackend` (recovery-time, before the engine has
/// been built).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupOutcome {
    /// No primary-index entry exists for this txid — orphan, delete.
    NoEntry,
    /// Index entry exists; carries the cached `tx_flags` so the GC can check
    /// for [`TxFlags::EXTERNAL`].
    Found { tx_flags: u8 },
}

/// Walk every blob in `blob_store` and delete every blob whose primary-index
/// entry is absent or present without [`TxFlags::EXTERNAL`].
///
/// Generic over the index lookup so the same logic can run against the
/// runtime [`Engine`] (background sweep) or a borrowed `PrimaryBackend`
/// (recovery, before the engine has been constructed).
///
/// Returns aggregate counters for observability. Errors from the underlying
/// `list` call are propagated; per-blob delete failures are logged and counted
/// in [`BlobGcStats::delete_failed`] so a single stuck blob cannot stop the
/// entire sweep from making progress.
///
/// **Concurrency contract.** This entry point performs NO synchronization
/// against in-flight creates: it is for recovery-time use only, after the
/// redo replay has completed and before any client is connected, so a
/// half-completed create cannot exist. The dispatch path orders blob `put`
/// BEFORE the index `register`, so a sweep that raced a live create would
/// mis-classify its blob as an orphan. Runtime sweeps must use
/// [`reconcile_orphan_blobs`] (grace filter F-G9-004 + pin handshake
/// F-IJ-002) or [`reconcile_orphan_blobs_with_pins`] instead.
pub fn reconcile_orphan_blobs_with<F>(
    blob_store: &dyn BlobStore,
    mut lookup: F,
) -> Result<BlobGcStats, BlobError>
where
    F: FnMut(&TxKey) -> LookupOutcome,
{
    reconcile_orphan_blobs_with_filter(blob_store, None, None, &mut lookup)
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
    reconcile_orphan_blobs_with_filter(blob_store, min_blob_age, Some(pins), &mut lookup)
}

/// Internal helper shared by recovery (no min-age filter, no pins) and the
/// periodic sweep (min-age filter + pin handshake). See
/// [`reconcile_orphan_blobs_with`], [`reconcile_orphan_blobs_with_pins`] and
/// [`reconcile_orphan_blobs`] for the public entry points.
fn reconcile_orphan_blobs_with_filter<F>(
    blob_store: &dyn BlobStore,
    min_blob_age: Option<Duration>,
    pins: Option<&BlobPinSet>,
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
        // Classification. An index entry present without EXTERNAL means the
        // blob is debris from a prior failed create whose registration ended
        // up referring to the inline / separate-tier path instead.
        let classified_found = match lookup(&key) {
            LookupOutcome::Found { tx_flags } => {
                let flags = TxFlags::from_bits_truncate(tx_flags);
                if flags.contains(TxFlags::EXTERNAL) {
                    stats.kept += 1;
                    continue;
                }
                true
            }
            LookupOutcome::NoEntry => false,
        };
        let reason = if classified_found {
            "index entry not flagged EXTERNAL"
        } else {
            "no primary-index entry"
        };

        // Deletion. With a pin set (periodic sweep), re-verify "unpinned AND
        // still unreferenced" under the pin stripe lock immediately before
        // the unlink so a create racing between the classification above and
        // this point (F-IJ-002 TOCTOU) cannot lose its blob. Without a pin
        // set (recovery — single-threaded by contract) delete directly.
        let outcome = match pins {
            Some(p) => p.delete_orphan_guarded(
                &txid,
                || {
                    !matches!(
                        lookup(&key),
                        LookupOutcome::Found { tx_flags }
                            if TxFlags::from_bits_truncate(tx_flags).contains(TxFlags::EXTERNAL)
                    )
                },
                || blob_store.delete(&txid),
            ),
            None => blob_store.delete(&txid).map(|()| PinSweepOutcome::Deleted),
        };
        match outcome {
            Ok(PinSweepOutcome::Deleted) => {
                if classified_found {
                    stats.deleted_not_external += 1;
                } else {
                    stats.deleted_no_index += 1;
                }
                tracing::info!(
                    txid = %hex_txid(&txid),
                    "blob_gc: deleted orphan blob ({reason})",
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
                // The index registration landed between classification and
                // the unlink — the blob is live.
                stats.kept += 1;
            }
            Err(e) => {
                stats.delete_failed += 1;
                tracing::warn!(
                    txid = %hex_txid(&txid),
                    err = %e,
                    "blob_gc: failed to delete orphan blob ({reason}); will retry next sweep",
                );
            }
        }
    }

    Ok(stats)
}

/// Recovery-time sweep against a borrowed [`ShardedIndex`].
///
/// Called from [`crate::recovery::reconcile_blobs_after_recovery`] after the
/// redo replay has finished and the primary index reflects the committed state.
/// At this point no client is connected to the server, so the concurrency
/// race described on [`reconcile_orphan_blobs_with`] cannot occur.
pub fn reconcile_orphan_blobs_against_index(
    blob_store: &dyn BlobStore,
    index: &ShardedIndex,
) -> Result<BlobGcStats, BlobError> {
    reconcile_orphan_blobs_with(blob_store, |key| match index.lookup(key) {
        Some(entry) => LookupOutcome::Found {
            tx_flags: entry.tx_flags,
        },
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
    let mut lookup = |key: &TxKey| match engine.lookup(key) {
        Some(entry) => LookupOutcome::Found {
            tx_flags: entry.tx_flags,
        },
        None => LookupOutcome::NoEntry,
    };
    reconcile_orphan_blobs_with_filter(
        blob_store,
        Some(PERIODIC_GC_MIN_BLOB_AGE),
        Some(engine.blob_pins()),
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

                let started = std::time::Instant::now();
                match reconcile_orphan_blobs(blob_store.as_ref(), engine.as_ref()) {
                    Ok(stats) => {
                        tracing::info!(
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            total_blobs = stats.total_blobs,
                            kept = stats.kept,
                            deleted_no_index = stats.deleted_no_index,
                            deleted_not_external = stats.deleted_not_external,
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
        let unmined = crate::index::UnminedBackend::new_in_memory();
        let locks = StripedLocks::new(64);
        let mut engine = Engine::new(device, index, allocator, locks, dah, unmined);
        let blob_store = Arc::new(MemoryBlobStore::new());
        engine.set_blob_store(blob_store.clone() as Arc<dyn BlobStore>);
        (Arc::new(engine), blob_store)
    }

    fn txid(n: u8) -> [u8; 32] {
        let mut t = [0u8; 32];
        t[0] = n;
        t
    }

    /// Insert a primary-index entry whose `tx_flags` includes the given
    /// flags. Used to simulate "blob-owning" or "non-blob-owning" records
    /// without going through the full create pipeline (which is what the
    /// crash window we are GC'ing tries to skip).
    fn insert_index_entry(engine: &Engine, key: &[u8; 32], flags: TxFlags) {
        let entry = crate::index::TxIndexEntry {
            device_id: 0,
            record_offset: 0,
            utxo_count: 0,
            block_entry_count: 0,
            tx_flags: flags.bits(),
            spent_utxos: 0,
            dah_or_preserve: 0,
            unmined_since: 0,
            generation: 0,
        };
        engine
            .register(crate::index::TxKey { txid: *key }, entry)
            .expect("register index entry");
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
        // No index entry registered — must be deleted.

        let stats =
            reconcile_orphan_blobs(blob_store.as_ref() as &dyn BlobStore, engine.as_ref()).unwrap();
        assert_eq!(stats.total_blobs, 1);
        assert_eq!(stats.kept, 0);
        assert_eq!(stats.deleted_no_index, 1);
        assert_eq!(stats.deleted_not_external, 0);
        assert!(!blob_store.exists(&orphan).unwrap());
    }

    #[test]
    fn reconcile_deletes_blob_when_index_entry_lacks_external_flag() {
        let (engine, blob_store) = make_engine();
        let key = txid(0xBB);
        blob_store.put(&key, b"stale-blob").unwrap();
        // Index entry exists but EXTERNAL flag is NOT set — the record's
        // payload lives inline / on the separate tier; the blob is debris
        // from a prior aborted attempt and must be deleted.
        insert_index_entry(&engine, &key, TxFlags::IS_COINBASE);

        let stats =
            reconcile_orphan_blobs(blob_store.as_ref() as &dyn BlobStore, engine.as_ref()).unwrap();
        assert_eq!(stats.total_blobs, 1);
        assert_eq!(stats.deleted_not_external, 1);
        assert!(!blob_store.exists(&key).unwrap());
    }

    #[test]
    fn reconcile_mixed_set() {
        let (engine, blob_store) = make_engine();
        // Three categories: kept, no-index orphan, non-external orphan.
        let keep = txid(1);
        let orphan_no_index = txid(2);
        let orphan_no_flag = txid(3);
        blob_store.put(&keep, b"k").unwrap();
        blob_store.put(&orphan_no_index, b"o1").unwrap();
        blob_store.put(&orphan_no_flag, b"o2").unwrap();
        insert_index_entry(&engine, &keep, TxFlags::EXTERNAL);
        insert_index_entry(&engine, &orphan_no_flag, TxFlags::empty());

        let stats =
            reconcile_orphan_blobs(blob_store.as_ref() as &dyn BlobStore, engine.as_ref()).unwrap();
        assert_eq!(stats.total_blobs, 3);
        assert_eq!(stats.kept, 1);
        assert_eq!(stats.deleted_no_index, 1);
        assert_eq!(stats.deleted_not_external, 1);
        assert!(blob_store.exists(&keep).unwrap());
        assert!(!blob_store.exists(&orphan_no_index).unwrap());
        assert!(!blob_store.exists(&orphan_no_flag).unwrap());
    }
}
