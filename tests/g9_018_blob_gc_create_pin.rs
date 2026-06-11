//! F-IJ-002 regression tests — blob-GC TOCTOU on AGED blobs.
//!
//! F-G9-004's grace window only protects blobs whose mtime is fresh. A client
//! may legitimately stream a blob, then send `OP_CREATE_BATCH` minutes later;
//! the blob is then past the 60 s grace. Pre-fix, a periodic GC sweep running
//! between the create dispatch's `digest()` check and the index registration
//! saw an aged blob with no index entry and deleted it — the create then
//! completed and acknowledged an EXTERNAL record whose cold data was
//! permanently gone.
//!
//! Post-fix the create dispatch pins the txid in [`Engine::blob_pins`] BEFORE
//! the digest check and holds the pin until after index registration; the
//! sweep re-verifies "not pinned AND still unreferenced" under the pin stripe
//! lock immediately before each unlink.

use std::fs;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use teraslab::allocator::SlotAllocator;
use teraslab::device::MemoryDevice;
use teraslab::index::{PrimaryBackend, TxIndexEntry, TxKey};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::record::TxFlags;
use teraslab::storage::blob_gc::{
    LookupOutcome, PERIODIC_GC_MIN_BLOB_AGE, reconcile_orphan_blobs,
    reconcile_orphan_blobs_with_pins,
};
use teraslab::storage::blobstore::{BlobPinSet, BlobStore, FileBlobStore, MemoryBlobStore};

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

/// Set the mtime of a payload + its sidecar to `mtime`, simulating a blob
/// uploaded long before the create lands (past the F-G9-004 grace window).
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

fn external_entry() -> TxIndexEntry {
    TxIndexEntry {
        device_id: 0,
        record_offset: 0,
        utxo_count: 0,
        block_entry_count: 0,
        tx_flags: TxFlags::EXTERNAL.bits(),
        spent_utxos: 0,
        dah_or_preserve: 0,
        unmined_since: 0,
        generation: 0,
    }
}

/// Backdate a freshly put blob past the grace window.
fn age_blob(blob_dir: &std::path::Path, key: &[u8; 32]) {
    let aged_mtime = SystemTime::now() - PERIODIC_GC_MIN_BLOB_AGE - Duration::from_secs(30);
    set_file_mtime(&blob_path_for(blob_dir, 2, key), aged_mtime);
}

/// The headline TOCTOU: a blob older than the grace window, with the GC sweep
/// landing deterministically between the create's digest check and the index
/// registration. The sweep is run on the same thread as the simulated create,
/// at exactly the racy interleaving point, so the test is fully deterministic.
///
/// Pre-fix (no pin handshake): the sweep classified the aged blob as an
/// orphan and unlinked it; the create then registered an EXTERNAL entry whose
/// cold data was gone forever. Post-fix: the pin taken before the digest
/// check makes the sweep skip the blob.
#[test]
fn aged_blob_pinned_by_inflight_create_survives_sweep_between_digest_and_register() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = dir.path().join("blobs");
    fs::create_dir_all(&blob_dir).unwrap();
    let store = FileBlobStore::new(&blob_dir, 2);
    let engine = build_engine();

    // Client streamed the blob minutes ago — well past the grace window.
    let key = txid(0xD1);
    let payload = b"aged external payload";
    store.put(&key, payload).expect("put blob");
    age_blob(&blob_dir, &key);

    // --- create dispatch, step 1: pin, then digest check ---
    let pin = engine.blob_pins().pin(&key);
    let digest = store
        .digest(&key)
        .expect("digest read")
        .expect("blob present at digest check");
    assert_eq!(digest.length, payload.len() as u64);

    // --- GC sweep races in between digest check and registration ---
    let stats = reconcile_orphan_blobs(&store as &dyn BlobStore, &engine).unwrap();
    assert_eq!(stats.total_blobs, 1, "aged blob is in the candidate set");
    assert_eq!(
        stats.skipped_pinned, 1,
        "sweep must skip the pinned in-flight blob"
    );
    assert_eq!(stats.deleted_total(), 0);
    assert!(
        store.exists(&key).unwrap(),
        "blob referenced by an in-flight create must survive the sweep"
    );

    // --- create dispatch, step 2: index registration, then pin release ---
    engine
        .register(TxKey { txid: key }, external_entry())
        .expect("register");
    drop(pin);

    // A later sweep keeps the blob via the live-set check.
    let stats2 = reconcile_orphan_blobs(&store as &dyn BlobStore, &engine).unwrap();
    assert_eq!(stats2.total_blobs, 1);
    assert_eq!(stats2.kept, 1);
    assert_eq!(stats2.skipped_pinned, 0);
    assert!(store.exists(&key).unwrap());
}

/// A pin released by a failed create (guard dropped on the error path) must
/// NOT keep protecting the blob: the next sweep reclaims the orphan.
#[test]
fn aged_orphan_with_released_pin_is_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = dir.path().join("blobs");
    fs::create_dir_all(&blob_dir).unwrap();
    let store = FileBlobStore::new(&blob_dir, 2);
    let engine = build_engine();

    let key = txid(0xD2);
    store.put(&key, b"create failed after digest").unwrap();
    age_blob(&blob_dir, &key);

    // Create pinned, digest-checked, then failed before registration — the
    // RAII guard drops on the dispatch error path.
    let pin = engine.blob_pins().pin(&key);
    assert!(store.digest(&key).unwrap().is_some());
    drop(pin);

    let stats = reconcile_orphan_blobs(&store as &dyn BlobStore, &engine).unwrap();
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.skipped_pinned, 0);
    assert_eq!(stats.deleted_no_index, 1);
    assert!(
        !store.exists(&key).unwrap(),
        "orphan must be reclaimed once the pin is released"
    );
}

/// Crash consistency: pins are process-local. A create that crashed while
/// holding a pin (simulated by leaking the guard, then building a fresh
/// engine as a restart would) must not block GC forever — the post-restart
/// pin set is empty and the orphan is reclaimed.
#[test]
fn pins_do_not_survive_restart_so_a_crashed_create_cannot_block_gc() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = dir.path().join("blobs");
    fs::create_dir_all(&blob_dir).unwrap();
    let store = FileBlobStore::new(&blob_dir, 2);

    let key = txid(0xD3);
    store.put(&key, b"pinned then process crashed").unwrap();
    age_blob(&blob_dir, &key);

    // Pre-crash engine: create pins and then the process dies without ever
    // releasing (mem::forget models the crash — no Drop runs).
    let engine_before = build_engine();
    let pin = engine_before.blob_pins().pin(&key);
    assert!(engine_before.blob_pins().is_pinned(&key));
    std::mem::forget(pin);
    // While that process lives, the sweep skips the blob (leak, not loss).
    let stats = reconcile_orphan_blobs(&store as &dyn BlobStore, &engine_before).unwrap();
    assert_eq!(stats.skipped_pinned, 1);
    assert!(store.exists(&key).unwrap());

    // "Restart": a fresh engine has an empty pin set; the orphan is swept.
    let engine_after = build_engine();
    assert!(!engine_after.blob_pins().is_pinned(&key));
    let stats = reconcile_orphan_blobs(&store as &dyn BlobStore, &engine_after).unwrap();
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.deleted_no_index, 1);
    assert!(
        !store.exists(&key).unwrap(),
        "orphaned pin from a crashed create must not block GC after restart"
    );
}

/// The under-lock re-check: a create whose registration lands between the
/// sweep's candidate classification and the unlink must be seen by the
/// re-verification the sweep performs immediately before deleting. Modeled
/// deterministically with a stateful lookup closure: first call (candidate
/// classification) reports no entry; second call (the re-check under the pin
/// stripe lock) reports a registered EXTERNAL entry.
#[test]
fn registration_landing_mid_sweep_is_caught_by_under_lock_recheck() {
    let store = MemoryBlobStore::new();
    let pins = BlobPinSet::new();
    let key = txid(0xD4);
    store.put(&key, b"registered mid-sweep").unwrap();

    let mut calls = 0u32;
    let stats = reconcile_orphan_blobs_with_pins(&store as &dyn BlobStore, None, &pins, |_k| {
        calls += 1;
        if calls == 1 {
            LookupOutcome::NoEntry
        } else {
            LookupOutcome::Found {
                tx_flags: TxFlags::EXTERNAL.bits(),
            }
        }
    })
    .unwrap();

    assert_eq!(calls, 2, "sweep must re-verify the index before unlinking");
    assert_eq!(stats.total_blobs, 1);
    assert_eq!(stats.kept, 1, "re-check outcome counts as kept");
    assert_eq!(stats.deleted_total(), 0);
    assert!(
        store.exists(&key).unwrap(),
        "blob registered between classification and unlink must survive"
    );
}
