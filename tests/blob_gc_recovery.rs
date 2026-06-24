//! Integration tests for R-049 — orphan-blob garbage collection at
//! recovery time and from the periodic background sweep.
//!
//! Pre-fix, every failed create / aborted upload / cancelled migration
//! leaked a blob to disk forever (audit IJK-08). These tests model each
//! leak source against a real [`FileBlobStore`] and assert that the
//! recovery-time `reconcile_blobs_after_recovery` pass deletes them.

use teraslab::index::{PrimaryBackend, ShardedIndex, TxIndexEntry, TxKey};
use teraslab::record::TxFlags;
use teraslab::recovery::reconcile_blobs_after_recovery;
use teraslab::storage::blob_gc::{BlobGcStats, reconcile_orphan_blobs_against_index};
use teraslab::storage::blobstore::{BlobStore, FileBlobStore};

/// Build a fresh primary index + blob store on a tempdir. The data device
/// is irrelevant for these tests — recovery's blob-reconciliation step
/// only needs the primary index and the blob store.
fn fresh() -> (ShardedIndex, FileBlobStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = dir.path().join("blobs");
    std::fs::create_dir_all(&blob_dir).unwrap();
    let store = FileBlobStore::new(&blob_dir, 2);
    let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(1024).unwrap());
    (index, store, dir)
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

/// Insert a primary-index entry whose `tx_flags` includes the given flags.
fn register_entry(index: &ShardedIndex, key: &[u8; 32], flags: TxFlags) {
    let entry = TxIndexEntry {
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
    index
        .register(TxKey { txid: *key }, entry)
        .expect("register index entry");
}

/// Pin: a process crash AFTER the blob has been written but BEFORE the
/// primary-index entry was registered must reclaim the blob on the next
/// startup. This is the audit-prescribed test name for R-049.
#[test]
fn failed_create_blob_garbage_collected_on_recovery() {
    let (index, store, _dir) = fresh();

    // Simulate a failed create: blob written successfully, but the create
    // dispatch errored out before the index registration could land.
    let leaked = txid(0xAA);
    let payload = b"payload-for-tx-that-never-registered".to_vec();
    let digest = store.put(&leaked, &payload).unwrap();
    assert!(store.exists(&leaked).unwrap());
    assert_eq!(digest.length, payload.len() as u64);

    // Recovery runs against the (empty) primary index — reconciliation
    // must delete the leaked blob.
    let stats: BlobGcStats =
        reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index).unwrap();
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.kept, 0);
    assert_eq!(stats.deleted_no_index, 1);
    assert_eq!(stats.deleted_not_external, 0);
    assert_eq!(stats.delete_failed, 0);
    assert!(
        !store.exists(&leaked).unwrap(),
        "leaked blob must be deleted"
    );
}

/// A blob whose primary-index entry exists AND is flagged EXTERNAL is the
/// committed state — recovery must NOT touch it. Pre-fix this would also
/// have been correct (no GC at all), so the regression to guard against is
/// "GC over-eagerly nukes valid blobs".
#[test]
fn blob_gc_keeps_blobs_referenced_by_external_flagged_records() {
    let (index, store, _dir) = fresh();

    let live = txid(0x10);
    store.put(&live, b"live external payload").unwrap();
    register_entry(&index, &live, TxFlags::EXTERNAL);

    let stats = reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index).unwrap();
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.kept, 1);
    assert_eq!(stats.deleted_no_index, 0);
    assert_eq!(stats.deleted_not_external, 0);
    assert!(
        store.exists(&live).unwrap(),
        "live external blob must be kept"
    );

    // Round-trip: payload bytes are still readable and digest-verified.
    let read = store.get(&live).unwrap().unwrap();
    assert_eq!(read, b"live external payload");
}

/// A blob whose txid does not appear in the primary index at all is an
/// orphan. The audit-prescribed test name for the IJK-08 case.
#[test]
fn blob_gc_skips_blobs_not_in_primary_index() {
    let (index, store, _dir) = fresh();

    // Three orphans, no index entries at all.
    let o1 = txid(1);
    let o2 = txid(2);
    let o3 = txid(3);
    store.put(&o1, b"a").unwrap();
    store.put(&o2, b"bb").unwrap();
    store.put(&o3, b"ccc").unwrap();

    let stats = reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index).unwrap();
    assert_eq!(stats.total_blobs, 3);
    assert_eq!(stats.kept, 0);
    assert_eq!(stats.deleted_no_index, 3);
    assert_eq!(stats.deleted_not_external, 0);
    assert!(!store.exists(&o1).unwrap());
    assert!(!store.exists(&o2).unwrap());
    assert!(!store.exists(&o3).unwrap());
}

/// A blob whose primary-index entry is present but does NOT carry the
/// EXTERNAL flag is debris from an aborted attempt that ended up using the
/// inline / separate-tier path instead. Must be reclaimed.
#[test]
fn blob_gc_deletes_blobs_when_index_entry_missing_external_flag() {
    let (index, store, _dir) = fresh();

    let stale = txid(0x20);
    store.put(&stale, b"stale blob").unwrap();
    register_entry(&index, &stale, TxFlags::IS_COINBASE);

    let stats = reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index).unwrap();
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.deleted_not_external, 1);
    assert!(!store.exists(&stale).unwrap());
}

/// A mixed set covering all three categories at once — kept, no-index
/// orphan, present-but-not-EXTERNAL orphan. Verifies the reconciler does
/// not get confused by interleaving in `BlobStore::list` order.
#[test]
fn blob_gc_mixed_set_recovery() {
    let (index, store, _dir) = fresh();

    let keep_ext = txid(0x30);
    let orphan_no_idx = txid(0x31);
    let orphan_no_flag = txid(0x32);
    store.put(&keep_ext, b"k").unwrap();
    store.put(&orphan_no_idx, b"o1").unwrap();
    store.put(&orphan_no_flag, b"o2").unwrap();
    register_entry(&index, &keep_ext, TxFlags::EXTERNAL);
    register_entry(&index, &orphan_no_flag, TxFlags::empty());

    let stats = reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index).unwrap();
    assert_eq!(stats.total_blobs, 3);
    assert_eq!(stats.kept, 1);
    assert_eq!(stats.deleted_no_index, 1);
    assert_eq!(stats.deleted_not_external, 1);
    assert!(store.exists(&keep_ext).unwrap());
    assert!(!store.exists(&orphan_no_idx).unwrap());
    assert!(!store.exists(&orphan_no_flag).unwrap());
}

/// Pin: stale `.tmp` upload artefacts older than
/// `FileBlobStore::STALE_TMP_AGE_SECS` must be swept on recovery. The
/// reconciler's `BlobStore::list` call drives the sweep as a side effect.
#[test]
fn stale_tmp_files_swept_on_recovery() {
    use std::time::{Duration, SystemTime};

    let (index, store, dir) = fresh();

    // Anchor the prefix tree by writing a real blob — its parent dir is
    // where the stale .tmp will live. We register it as EXTERNAL so it is
    // NOT swept by the orphan-blob path (we want this test focused on the
    // .tmp sweep, not on orphan deletion).
    let anchor = txid(0x40);
    store.put(&anchor, b"anchor").unwrap();
    register_entry(&index, &anchor, TxFlags::EXTERNAL);

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
    let stats =
        reconcile_blobs_after_recovery(&store as &dyn BlobStore, &index).expect("reconcile");
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.kept, 1);
    assert_eq!(stats.deleted_no_index, 0);
    assert_eq!(stats.deleted_not_external, 0);

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

    let keep = txid(0x50);
    let orphan = txid(0x51);
    store.put(&keep, b"keep").unwrap();
    store.put(&orphan, b"drop").unwrap();
    register_entry(&index, &keep, TxFlags::EXTERNAL);

    let stats =
        reconcile_orphan_blobs_against_index(&store as &dyn BlobStore, &index).expect("reconcile");
    assert_eq!(stats.total_blobs, 2);
    assert_eq!(stats.kept, 1);
    assert_eq!(stats.deleted_no_index, 1);
    assert!(store.exists(&keep).unwrap());
    assert!(!store.exists(&orphan).unwrap());
}
