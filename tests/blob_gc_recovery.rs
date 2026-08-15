//! Integration tests for R-049 — orphan-blob garbage collection at
//! recovery time and from the periodic background sweep.
//!
//! Pre-fix, every failed create / aborted upload / cancelled migration
//! leaked a blob to disk forever (audit IJK-08). The RECOVERY pass, however,
//! must delete NOTHING (CI scenario-11, run 31787458246: it destroyed 8
//! externalized records' payloads): the replayed index can transiently miss
//! entries that boot-time heals re-register, and an entry present without
//! the EXTERNAL flag is an upstream flag-fidelity defect whose blob must
//! survive for repair. These tests assert the recovery pass QUARANTINES
//! (retains + counts) both classes, and that the PERIODIC sweep still
//! reclaims genuine no-index debris.
//!
//! The slim primary index no longer caches `tx_flags`, so the blob GC reads
//! the EXTERNAL flag from each record's on-device footer. These tests
//! therefore write a real record (with the desired flags) via a
//! [`SlotAllocator`] before registering its locator.

use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{PrimaryBackend, ShardedIndex, TxIndexEntry, TxKey};
use teraslab::record::TxFlags;
use teraslab::recovery::reconcile_blobs_after_recovery;
use teraslab::storage::blob_gc::{BlobGcStats, reconcile_orphan_blobs_against_index};
use teraslab::storage::blobstore::{BlobStore, FileBlobStore};

/// Run the PERIODIC-pass reconciler over `store` with an index lookup that
/// resolves each txid's EXTERNAL flag from the on-device footer, mirroring
/// the engine-backed background sweep (no age filter so freshly-written test
/// blobs are visible; an empty pin set).
fn periodic_sweep(
    store: &FileBlobStore,
    index: &ShardedIndex,
    device: &Arc<dyn BlockDevice>,
) -> BlobGcStats {
    use teraslab::storage::blob_gc::{LookupOutcome, reconcile_orphan_blobs_with_pins};
    use teraslab::storage::blobstore::BlobPinSet;

    let pins = BlobPinSet::new();
    reconcile_orphan_blobs_with_pins(store as &dyn BlobStore, None, &pins, |key| {
        match index.lookup(key) {
            Some(entry) => {
                let external = teraslab::io::read_metadata(&**device, entry.record_offset)
                    .map(|meta| meta.tx_id != key.txid || meta.flags.contains(TxFlags::EXTERNAL))
                    .unwrap_or(true);
                LookupOutcome::Found { external }
            }
            None => LookupOutcome::NoEntry,
        }
    })
    .expect("periodic sweep")
}

/// Build a fresh primary index + blob store + data device on a tempdir.
fn fresh() -> (
    ShardedIndex,
    FileBlobStore,
    tempfile::TempDir,
    Arc<dyn BlockDevice>,
    SlotAllocator,
) {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = dir.path().join("blobs");
    std::fs::create_dir_all(&blob_dir).unwrap();
    let store = FileBlobStore::new(&blob_dir, 2);
    let device: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let allocator = SlotAllocator::new(device.clone()).unwrap();
    let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1024).unwrap());
    (index, store, dir, device, allocator)
}

fn txid(seed: u8) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[0] = seed;
    // Spread some entropy across the prefix bytes so the FileBlobStore
    // distributes blobs across distinct prefix subdirectories.
    t[1] = seed.wrapping_mul(31);
    t[2] = seed.wrapping_mul(57);
    t
}

/// Write a real record for `key` carrying `flags` on `device` (via `alloc`),
/// then register its locator in the primary index. The blob GC reads the
/// EXTERNAL flag from this on-device footer.
fn register_entry(
    index: &ShardedIndex,
    device: &dyn BlockDevice,
    alloc: &mut SlotAllocator,
    key: &[u8; 32],
    flags: TxFlags,
) {
    use teraslab::record::{TxMetadata, UtxoSlot};

    let utxo_count = 1u32;
    let mut meta = TxMetadata::new(utxo_count);
    meta.tx_id = *key;
    meta.flags = flags;

    let record_size = TxMetadata::record_size_for(utxo_count);
    let offset = alloc.allocate(record_size).expect("allocate record");
    let slots = vec![UtxoSlot::new_unspent([0u8; 32]); utxo_count as usize];
    teraslab::io::write_full_record(device, offset, &meta, &slots).expect("write record footer");
    index
        .register(
            TxKey { txid: *key },
            TxIndexEntry {
                device_id: 0,
                record_offset: offset,
                mined_slot: teraslab::index::mined_index::NO_MINED_SLOT,
            },
        )
        .expect("register index entry");
}

/// A process crash AFTER the blob has been written but BEFORE the
/// primary-index entry was registered (R-049 leak source #1). The RECOVERY
/// pass must QUARANTINE the blob — recovery cannot distinguish this from a
/// key a queued reverse-heal is about to re-register — and the PERIODIC
/// sweep then reclaims it as genuine debris.
#[test]
fn failed_create_blob_quarantined_on_recovery_then_reclaimed_by_periodic_sweep() {
    let (index, store, _dir, device, _alloc) = fresh();
    let devices = [device.clone()];

    // Simulate a failed create: blob written successfully, but the create
    // dispatch errored out before the index registration could land.
    let leaked = txid(0xAA);
    let payload = b"payload-for-tx-that-never-registered".to_vec();
    let digest = store.put(&leaked, &payload).unwrap();
    assert!(store.exists(&leaked).unwrap());
    assert_eq!(digest.length, payload.len() as u64);

    // Recovery runs against the (empty) primary index — the recovery pass
    // retains (quarantines) the blob rather than racing a possible heal.
    let stats: BlobGcStats =
        reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index, &devices).unwrap();
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.kept, 0);
    assert_eq!(stats.quarantined_no_index, 1);
    assert_eq!(stats.deleted_total(), 0);
    assert_eq!(stats.delete_failed, 0);
    assert!(
        store.exists(&leaked).unwrap(),
        "recovery pass must retain the blob (quarantine, not delete)"
    );

    // The periodic sweep (node serving, heals landed) reclaims the leak so
    // it does not accumulate forever (audit IJK-08).
    let stats = periodic_sweep(&store, &index, &device);
    assert_eq!(stats.deleted_no_index, 1);
    assert!(
        !store.exists(&leaked).unwrap(),
        "periodic sweep must reclaim the genuine no-index leak"
    );
}

/// A blob whose primary-index entry exists AND is flagged EXTERNAL is the
/// committed state — recovery must NOT touch it. Pre-fix this would also
/// have been correct (no GC at all), so the regression to guard against is
/// "GC over-eagerly nukes valid blobs".
#[test]
fn blob_gc_keeps_blobs_referenced_by_external_flagged_records() {
    let (index, store, _dir, device, mut alloc) = fresh();

    let live = txid(0x10);
    store.put(&live, b"live external payload").unwrap();
    register_entry(&index, &*device, &mut alloc, &live, TxFlags::EXTERNAL);
    let devices = [device];

    let stats = reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index, &devices).unwrap();
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.kept, 1);
    assert_eq!(stats.deleted_total(), 0);
    assert_eq!(stats.quarantined_total(), 0);
    assert!(
        store.exists(&live).unwrap(),
        "live external blob must be kept"
    );

    // Round-trip: payload bytes are still readable and digest-verified.
    let read = store.get(&live).unwrap().unwrap();
    assert_eq!(read, b"live external payload");
}

/// Blobs whose txids do not appear in the primary index at all: at RECOVERY
/// time these are quarantined (a queued heal may still re-register the key),
/// not deleted. The periodic sweep is the reclamation path (IJK-08).
#[test]
fn blob_gc_quarantines_blobs_not_in_primary_index_on_recovery() {
    let (index, store, _dir, device, _alloc) = fresh();
    let devices = [device];

    // Three unreferenced blobs, no index entries at all.
    let o1 = txid(1);
    let o2 = txid(2);
    let o3 = txid(3);
    store.put(&o1, b"a").unwrap();
    store.put(&o2, b"bb").unwrap();
    store.put(&o3, b"ccc").unwrap();

    let stats = reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index, &devices).unwrap();
    assert_eq!(stats.total_blobs, 3);
    assert_eq!(stats.kept, 0);
    assert_eq!(stats.quarantined_no_index, 3);
    assert_eq!(stats.deleted_total(), 0);
    assert!(store.exists(&o1).unwrap());
    assert!(store.exists(&o2).unwrap());
    assert!(store.exists(&o3).unwrap());
}

/// CI scenario-11 (run 31787458246) regression: a blob whose primary-index
/// entry is present but does NOT carry the EXTERNAL flag is a live record
/// with a flag-fidelity defect — the blob may be its only payload copy.
/// The recovery pass must retain it (quarantine), never delete.
#[test]
fn blob_gc_quarantines_blobs_when_index_entry_missing_external_flag() {
    let (index, store, _dir, device, mut alloc) = fresh();

    let suspect = txid(0x20);
    store
        .put(&suspect, b"possibly the only payload copy")
        .unwrap();
    register_entry(&index, &*device, &mut alloc, &suspect, TxFlags::IS_COINBASE);
    let devices = [device.clone()];

    // The scrape-visible tripwire counter must accumulate the per-sweep
    // quarantine count (P2: log-only counters are invisible to alerting).
    // The counter is process-global and other tests in this binary also
    // quarantine, so assert a monotonic delta, not an absolute value.
    let metric_before = teraslab::metrics::blob_gc_metrics()
        .quarantined_not_external_total
        .get();

    let stats = reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index, &devices).unwrap();
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.quarantined_not_external, 1);
    assert_eq!(stats.deleted_total(), 0);
    let metric_after = teraslab::metrics::blob_gc_metrics()
        .quarantined_not_external_total
        .get();
    assert!(
        metric_after > metric_before,
        "teraslab_blob_gc_quarantined_not_external_total must accumulate the \
         sweep's quarantine count (before={metric_before}, after={metric_after})"
    );
    assert!(
        store.exists(&suspect).unwrap(),
        "entry-present-without-flag blob must survive recovery"
    );

    // The PERIODIC sweep must not delete it either — same data-loss class,
    // just an hour later.
    let stats = periodic_sweep(&store, &index, &device);
    assert_eq!(stats.quarantined_not_external, 1);
    assert_eq!(stats.deleted_total(), 0);
    assert!(
        store.exists(&suspect).unwrap(),
        "entry-present-without-flag blob must survive the periodic sweep too"
    );
    assert_eq!(
        store.get(&suspect).unwrap().unwrap(),
        b"possibly the only payload copy".to_vec(),
        "payload must remain readable — last-copy retention for repair when \
         no peer holds the record"
    );
}

/// A mixed set covering all three categories at once — kept, no-index,
/// present-but-not-EXTERNAL. Verifies the recovery reconciler does not get
/// confused by interleaving in `BlobStore::list` order, and that the
/// periodic sweep then reclaims ONLY the genuine no-index debris.
#[test]
fn blob_gc_mixed_set_recovery() {
    let (index, store, _dir, device, mut alloc) = fresh();

    let keep_ext = txid(0x30);
    let orphan_no_idx = txid(0x31);
    let quarantine_no_flag = txid(0x32);
    store.put(&keep_ext, b"k").unwrap();
    store.put(&orphan_no_idx, b"o1").unwrap();
    store.put(&quarantine_no_flag, b"o2").unwrap();
    register_entry(&index, &*device, &mut alloc, &keep_ext, TxFlags::EXTERNAL);
    register_entry(
        &index,
        &*device,
        &mut alloc,
        &quarantine_no_flag,
        TxFlags::empty(),
    );
    let devices = [device.clone()];

    // Recovery: nothing deleted; both unreferenced classes quarantined.
    let stats = reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index, &devices).unwrap();
    assert_eq!(stats.total_blobs, 3);
    assert_eq!(stats.kept, 1);
    assert_eq!(stats.quarantined_no_index, 1);
    assert_eq!(stats.quarantined_not_external, 1);
    assert_eq!(stats.deleted_total(), 0);
    assert!(store.exists(&keep_ext).unwrap());
    assert!(store.exists(&orphan_no_idx).unwrap());
    assert!(store.exists(&quarantine_no_flag).unwrap());

    // Periodic: only the genuine no-index debris is reclaimed.
    let stats = periodic_sweep(&store, &index, &device);
    assert_eq!(stats.kept, 1);
    assert_eq!(stats.deleted_no_index, 1);
    assert_eq!(stats.quarantined_not_external, 1);
    assert!(store.exists(&keep_ext).unwrap());
    assert!(!store.exists(&orphan_no_idx).unwrap());
    assert!(store.exists(&quarantine_no_flag).unwrap());
}

/// Pin: stale `.tmp` upload artefacts older than
/// `FileBlobStore::STALE_TMP_AGE_SECS` must be swept on recovery. The
/// reconciler's `BlobStore::list` call drives the sweep as a side effect.
#[test]
fn stale_tmp_files_swept_on_recovery() {
    use std::time::{Duration, SystemTime};

    let (index, store, dir, device, mut alloc) = fresh();

    // Anchor the prefix tree by writing a real blob — its parent dir is
    // where the stale .tmp will live. We register it as EXTERNAL so it is
    // NOT swept by the orphan-blob path (we want this test focused on the
    // .tmp sweep, not on orphan deletion).
    let anchor = txid(0x40);
    store.put(&anchor, b"anchor").unwrap();
    register_entry(&index, &*device, &mut alloc, &anchor, TxFlags::EXTERNAL);
    let devices = [device];

    // Locate the parent prefix dir by walking the tempdir for the only
    // existing file whose name is exactly 64 hex chars (the anchor blob).
    fn walk(p: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let pp = e.path();
                if pp.is_dir() {
                    walk(&pp, out);
                } else {
                    out.push(pp);
                }
            }
        }
    }
    let mut entries = Vec::new();
    walk(dir.path(), &mut entries);
    let blob_root = entries
        .into_iter()
        .find(|p| {
            let n = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            n.len() == 64 && !n.ends_with(".tmp") && !n.ends_with(".meta")
        })
        .expect("anchor blob path")
        .parent()
        .unwrap()
        .to_path_buf();

    // Stale .tmp: backdated mtime past the cutoff — must be deleted.
    let stale_tmp =
        blob_root.join("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100.tmp");
    std::fs::write(&stale_tmp, b"interrupted-upload").unwrap();
    let stale_when =
        SystemTime::now() - Duration::from_secs(FileBlobStore::STALE_TMP_AGE_SECS + 60);
    let ft = filetime::FileTime::from_system_time(stale_when);
    filetime::set_file_mtime(&stale_tmp, ft).unwrap();

    // Fresh .tmp: mtime now — must NOT be swept (an in-flight upload).
    let fresh_tmp =
        blob_root.join("1122334455667788991122334455667788991122334455667788991122334455.tmp");
    std::fs::write(&fresh_tmp, b"in-flight").unwrap();

    // Recovery-time reconciliation: anchor is kept (EXTERNAL, registered),
    // and the .tmp sweep runs as a side effect of `BlobStore::list`.
    let stats = reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index, &devices)
        .expect("reconcile");
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.kept, 1);
    assert_eq!(stats.deleted_total(), 0);
    assert_eq!(stats.quarantined_total(), 0);

    assert!(!stale_tmp.exists(), "stale .tmp must be swept on recovery");
    assert!(fresh_tmp.exists(), "fresh .tmp must survive");
    assert!(store.exists(&anchor).unwrap(), "anchor blob must survive");
}

/// Direct test of the lower-level `reconcile_orphan_blobs_against_index`
/// entry point that recovery wraps. Same semantics, no logging side
/// effects — useful as a regression baseline if the wrapper changes shape.
#[test]
fn reconcile_orphan_blobs_against_index_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileBlobStore::new(dir.path(), 2);
    let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(16).unwrap());
    let device: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let mut alloc = SlotAllocator::new(device.clone()).unwrap();

    let keep = txid(0x50);
    let unreferenced = txid(0x51);
    store.put(&keep, b"keep").unwrap();
    store.put(&unreferenced, b"retain-until-periodic").unwrap();
    register_entry(&index, &*device, &mut alloc, &keep, TxFlags::EXTERNAL);
    let devices = [device];

    let stats = reconcile_orphan_blobs_against_index(&store as &dyn BlobStore, &index, &devices)
        .expect("reconcile");
    assert_eq!(stats.total_blobs, 2);
    assert_eq!(stats.kept, 1);
    assert_eq!(stats.quarantined_no_index, 1);
    assert_eq!(stats.deleted_total(), 0);
    assert!(store.exists(&keep).unwrap());
    assert!(store.exists(&unreferenced).unwrap());
}
