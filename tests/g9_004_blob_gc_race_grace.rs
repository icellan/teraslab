//! F-G9-004 regression test — periodic blob-GC must skip blobs younger than
//! [`PERIODIC_GC_MIN_BLOB_AGE`].
//!
//! Pre-fix the periodic sweep called `BlobStore::list()` and reconciled every
//! returned key against the live primary index. A concurrent create that had
//! just `put` a blob but whose `register` had not landed yet would be
//! mis-classified as an orphan and deleted out from under the in-flight
//! create. Post-fix the periodic sweep calls `list_for_gc(min_age)` so blobs
//! whose payload/sidecar mtime is younger than the grace period are excluded
//! from the candidate set.

use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use teraslab::allocator::SlotAllocator;
use teraslab::device::MemoryDevice;
use teraslab::index::{PrimaryBackend, TxIndexEntry, TxKey};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::record::TxFlags;
use teraslab::storage::blob_gc::{PERIODIC_GC_MIN_BLOB_AGE, reconcile_orphan_blobs};
use teraslab::storage::blobstore::{BlobStore, FileBlobStore};

fn txid(seed: u8) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[0] = seed;
    t[1] = seed.wrapping_mul(31);
    t[2] = seed.wrapping_mul(57);
    t
}

fn build_engine() -> Engine {
    let device: Arc<dyn teraslab::device::BlockDevice> =
        Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
    let allocator = SlotAllocator::new(device.clone()).unwrap();
    let index = PrimaryBackend::new_in_memory(1024).unwrap();
    let dah = teraslab::index::DahBackend::new_in_memory();
    let unmined = teraslab::index::UnminedBackend::new_in_memory();
    let locks = StripedLocks::new(64);
    Engine::new(device, index, allocator, locks, dah, unmined)
}

fn blob_path_for(
    base: &std::path::Path,
    prefix_depth: usize,
    key: &[u8; 32],
) -> std::path::PathBuf {
    let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
    let mut p = base.to_path_buf();
    for i in 0..prefix_depth {
        let start = i * 2;
        p = p.join(&hex[start..start + 2]);
    }
    p.join(&hex)
}

/// Set the mtime of a payload + its sidecar to `mtime`. Used to simulate
/// "blob just landed" (mtime now) vs "blob landed an hour ago" (mtime past).
fn set_file_mtime(path: &std::path::Path, mtime: SystemTime) {
    let f = fs::File::open(path).expect("open blob file");
    f.set_modified(mtime).expect("set blob mtime");
    let meta_path = {
        let mut p = path.as_os_str().to_os_string();
        p.push(".meta");
        std::path::PathBuf::from(p)
    };
    let mf = fs::File::open(&meta_path).expect("open sidecar file");
    mf.set_modified(mtime).expect("set sidecar mtime");
}

#[test]
fn periodic_sweep_skips_freshly_uploaded_blob() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = dir.path().join("blobs");
    fs::create_dir_all(&blob_dir).unwrap();
    let store = FileBlobStore::new(&blob_dir, 2);
    let engine = build_engine();

    // Simulate a freshly-uploaded blob whose index registration has not yet
    // landed (in-flight create). Default mtime is now — well within the
    // grace window.
    let in_flight = txid(0xAA);
    store
        .put(&in_flight, b"in-flight create payload")
        .expect("put blob");
    assert!(store.exists(&in_flight).unwrap());

    let stats = reconcile_orphan_blobs(&store as &dyn BlobStore, &engine).unwrap();

    // The freshly-uploaded blob must not even appear in the candidate set —
    // total_blobs is the size of the eligible-for-GC list, not the on-disk
    // count.
    assert_eq!(
        stats.total_blobs, 0,
        "fresh blob should be filtered out by list_for_gc grace"
    );
    assert_eq!(stats.deleted_total(), 0);
    assert!(
        store.exists(&in_flight).unwrap(),
        "freshly-uploaded blob must survive periodic sweep"
    );
}

#[test]
fn periodic_sweep_deletes_aged_orphan_blob() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = dir.path().join("blobs");
    fs::create_dir_all(&blob_dir).unwrap();
    let store = FileBlobStore::new(&blob_dir, 2);
    let engine = build_engine();

    // Aged orphan: blob exists with mtime in the past, but no index entry.
    // Must be classified as orphan and deleted.
    let aged = txid(0xBB);
    store.put(&aged, b"aged orphan").unwrap();
    let aged_mtime = SystemTime::now() - PERIODIC_GC_MIN_BLOB_AGE - Duration::from_secs(5);
    set_file_mtime(&blob_path_for(&blob_dir, 2, &aged), aged_mtime);

    let stats = reconcile_orphan_blobs(&store as &dyn BlobStore, &engine).unwrap();
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.deleted_no_index, 1);
    assert!(!store.exists(&aged).unwrap(), "aged orphan must be deleted");
}

#[test]
fn periodic_sweep_keeps_aged_blob_with_external_flagged_entry() {
    // Belt-and-braces: a blob older than the grace period with a registered
    // EXTERNAL index entry must STILL be kept — the grace skip is only an
    // optimisation; the live-set check is the source of truth for "delete".
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = dir.path().join("blobs");
    fs::create_dir_all(&blob_dir).unwrap();
    let store = FileBlobStore::new(&blob_dir, 2);
    let engine = build_engine();

    let live = txid(0xCC);
    store.put(&live, b"live external").unwrap();
    let aged_mtime = SystemTime::now() - PERIODIC_GC_MIN_BLOB_AGE - Duration::from_secs(10);
    set_file_mtime(&blob_path_for(&blob_dir, 2, &live), aged_mtime);

    // Register the matching primary-index entry with EXTERNAL.
    let entry = TxIndexEntry {
        device_id: 0,
        record_offset: 0,
        utxo_count: 0,
        block_entry_count: 0,
        tx_flags: TxFlags::EXTERNAL.bits(),
        spent_utxos: 0,
        dah_or_preserve: 0,
        unmined_since: 0,
        generation: 0,
    };
    engine
        .register(TxKey { txid: live }, entry)
        .expect("register");

    let stats = reconcile_orphan_blobs(&store as &dyn BlobStore, &engine).unwrap();
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.kept, 1);
    assert_eq!(stats.deleted_total(), 0);
    assert!(store.exists(&live).unwrap());
}
