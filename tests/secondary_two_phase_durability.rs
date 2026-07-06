//! Integration test for C4 two-phase durability of secondary indexes.
//!
//! Simulates the bug window: a crash happens AFTER the redo log fsync but
//! BEFORE the redb secondary index commit. On the next startup,
//! `recovery::recover_all` must detect the stale on-disk secondary index
//! and reconcile it against the primary's authoritative value.
//!
//! Exercises the on-disk redb backend end-to-end: we intentionally bypass
//! the redb commit, then open a fresh process view and run recovery.

use parking_lot::Mutex;
use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::MemoryDevice;
use teraslab::index::redb_dah::RedbDahIndex;
use teraslab::index::{DahBackend, PrimaryBackend, ShardedIndex, TxIndexEntry, TxKey};
use teraslab::record::TxMetadata;
use teraslab::redo::{RedoLog, RedoOp};

fn make_key(n: u8) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0] = n;
    TxKey { txid }
}

// The slim primary index carries only the locator; the authoritative
// unmined_since / delete_at_height / preserve_until values that recovery's
// secondary reconcile consults live in the on-device footer (written via
// `write_device_metadata`), so this helper takes only the record offset.
fn make_entry(offset: u64) -> TxIndexEntry {
    TxIndexEntry {
        device_id: 0,
        record_offset: offset,
        mined_slot: teraslab::index::mined_index::NO_MINED_SLOT,
    }
}

fn write_device_metadata(
    device: &MemoryDevice,
    key: TxKey,
    offset: u64,
    unmined_since: u32,
    delete_at_height: u32,
) {
    let mut meta = TxMetadata::new(5);
    meta.tx_id = key.txid;
    meta.unmined_since = unmined_since;
    meta.delete_at_height = delete_at_height;
    teraslab::io::write_metadata(device, offset, &meta).unwrap();
}

/// Crash between redo-fsync and redb commit: the DAH intent record is
/// durable, the redb secondary index is still empty. Recovery must repair
/// the secondary from the durable redo intent.
#[test]
fn crash_after_dah_redo_fsync_before_redb_commit() {
    let dir = tempfile::tempdir().unwrap();

    let primary = ShardedIndex::from_single(PrimaryBackend::new_in_memory(100).unwrap());
    let key = make_key(2);
    // Primary's DAH = 900 (no preserve_until).
    let entry = make_entry(8192);
    primary.register(key, entry).unwrap();

    let dah_path = dir.path().join("dah.redb");
    let mut dah_backend =
        DahBackend::OnDisk(RedbDahIndex::open(&dah_path, 16 * 1024 * 1024).unwrap());

    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let redo_log = Arc::new(Mutex::new(
        RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap(),
    ));

    {
        let mut log = redo_log.lock();
        log.append_and_flush(RedoOp::SecondaryDahUpdate {
            tx_key: key,
            old_height: 0,
            new_height: 900,
        })
        .unwrap();
    }

    assert!(dah_backend.is_empty());
    drop(redo_log);

    let redo_log_reopened = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
    let data_dev = MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap();
    write_device_metadata(&data_dev, key, 8192, 0, 900);
    let alloc =
        SlotAllocator::new(Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap())).unwrap();
    let _ = alloc; // keep variable to ensure SlotAllocator is exercised in scope

    let stats =
        teraslab::recovery::recover_all(&data_dev, &redo_log_reopened, &primary, &mut dah_backend)
            .unwrap();

    assert_eq!(stats.entries_replayed, 1);
    assert_eq!(stats.entries_failed, 0);

    let result = dah_backend.range_query(900);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], key);
}

/// Ensure the reconcile path correctly interprets the HAS_PRESERVE_UNTIL
/// flag on the primary index entry. When HAS_PRESERVE_UNTIL is set, the
/// primary's `dah_or_preserve` holds the preserve_until value (not DAH),
/// so the authoritative DAH is 0. A redo DAH intent with new_height != 0
/// must therefore be considered stale.
#[test]
fn recover_dah_respects_has_preserve_until_flag() {
    let dir = tempfile::tempdir().unwrap();

    let primary = ShardedIndex::from_single(PrimaryBackend::new_in_memory(100).unwrap());
    let key = make_key(5);
    // The record is preserved on-device: preserve_until is set and the DAH
    // is cleared, so the AUTHORITATIVE on-device `delete_at_height` is 0
    // (written below). A redo DAH intent with new_height = 900 must therefore
    // be treated as stale and skipped.
    primary.register(key, make_entry(4096)).unwrap();

    let dah_path = dir.path().join("dah.redb");
    let mut dah_backend =
        DahBackend::OnDisk(RedbDahIndex::open(&dah_path, 16 * 1024 * 1024).unwrap());

    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let redo_log = Arc::new(Mutex::new(
        RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap(),
    ));
    {
        let mut log = redo_log.lock();
        // Stale DAH redo: claims DAH should be 900, but primary says 0
        // because HAS_PRESERVE_UNTIL is set.
        log.append_and_flush(RedoOp::SecondaryDahUpdate {
            tx_key: key,
            old_height: 0,
            new_height: 900,
        })
        .unwrap();
    }
    drop(redo_log);

    let redo_log_reopened = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
    let data_dev = MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap();
    // Preserved record on device: preserve_until = 12345, DAH cleared to 0.
    // The reconcile reads the authoritative on-device `delete_at_height` (0),
    // so the redo DAH intent (900) is stale.
    {
        let mut meta = TxMetadata::new(5);
        meta.tx_id = key.txid;
        meta.preserve_until = 12345;
        meta.delete_at_height = 0;
        teraslab::io::write_metadata(&data_dev, 4096, &meta).unwrap();
    }

    let stats =
        teraslab::recovery::recover_all(&data_dev, &redo_log_reopened, &primary, &mut dah_backend)
            .unwrap();

    assert_eq!(stats.entries_skipped, 1);
    assert!(dah_backend.is_empty());
}

/// G-5: a true post-crash restart. Unlike the tests above, this one does
/// NOT reuse a live in-memory primary across the "crash". It rebuilds the
/// primary purely from device bytes via the device-scan path
/// (`PrimaryBackend::rebuild_file_backed`) — exactly what the startup
/// pipeline does when the file-backed index was lost (no clean-shutdown
/// sentinel / corrupt index). Recovery then reconciles the DAH secondary
/// redb index from that rebuilt primary, and we assert it agrees with the
/// authoritative device metadata — including correctly excluding a record
/// whose `delete_at_height` is 0 (only `unmined_since` set).
#[test]
fn restart_rebuilds_primary_from_device_then_reconciles_secondaries() {
    use teraslab::device::BlockDevice;
    use teraslab::record::UtxoSlot;

    let dir = tempfile::tempdir().unwrap();
    let data_dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());

    // Pre-crash: allocate and persist two records on the device, each with
    // a non-zero secondary-index height (one unmined, one DAH). We allocate
    // through a real SlotAllocator so the device-scan rebuild after the
    // crash knows the high-water mark to scan up to.
    let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();

    let key_unmined = make_key(40);
    let key_dah = make_key(41);

    let utxo_count: u32 = 5;
    let record_size = TxMetadata::record_size_for(utxo_count);

    let off_unmined = alloc.allocate(record_size).unwrap();
    let off_dah = alloc.allocate(record_size).unwrap();

    let slots: Vec<UtxoSlot> = (0..utxo_count)
        .map(|_| UtxoSlot::new_unspent([0u8; 32]))
        .collect();

    let mut meta_unmined = TxMetadata::new(utxo_count);
    meta_unmined.tx_id = key_unmined.txid;
    meta_unmined.unmined_since = 700;
    meta_unmined.delete_at_height = 0;
    teraslab::io::write_full_record(&*data_dev, off_unmined, &meta_unmined, &slots).unwrap();

    let mut meta_dah = TxMetadata::new(utxo_count);
    meta_dah.tx_id = key_dah.txid;
    meta_dah.unmined_since = 0;
    meta_dah.delete_at_height = 1234;
    teraslab::io::write_full_record(&*data_dev, off_dah, &meta_dah, &slots).unwrap();

    // *** CRASH ***: the live primary object is gone; only the device bytes
    // and the persisted allocator high-water mark survive. Reconstruct the
    // primary from a device scan, just like startup's rebuild path.
    let idx_path = dir.path().join("primary.idx");
    let primary = PrimaryBackend::rebuild_file_backed(&idx_path, &*data_dev, &alloc).unwrap();

    // The rebuilt primary must contain both records found on the device.
    assert!(
        primary.lookup_checked(&key_unmined).unwrap().is_some(),
        "device-scan rebuild must recover the unmined record"
    );
    assert!(
        primary.lookup_checked(&key_dah).unwrap().is_some(),
        "device-scan rebuild must recover the DAH record"
    );

    // Fresh (empty) secondary redb index — as on a real restart before
    // reconciliation. It must end up reconstructed from the rebuilt primary,
    // NOT carried over from any pre-crash in-memory state.
    let dah_path = dir.path().join("dah.redb");
    let mut dah_backend =
        DahBackend::OnDisk(RedbDahIndex::open(&dah_path, 16 * 1024 * 1024).unwrap());
    assert!(dah_backend.is_empty());

    // Empty redo log (no pending intents): recovery's job here is purely to
    // reconcile the DAH secondary from the freshly-rebuilt primary.
    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let redo_log = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();

    let sharded_primary = ShardedIndex::from_single(primary);
    teraslab::recovery::recover_all(&*data_dev, &redo_log, &sharded_primary, &mut dah_backend)
        .unwrap();

    // The DAH secondary now agrees with the authoritative device metadata.
    // `range_query(cutoff)` returns every key at height <= cutoff. It holds
    // exactly the DAH record (height 1234) and never the unmined-only
    // record (its `delete_at_height` is 0, so it was not inserted).
    let dah_hits = dah_backend.range_query(1234);
    assert_eq!(dah_hits, vec![key_dah]);
    assert!(
        dah_backend.range_query(1233).is_empty(),
        "DAH key (height 1234) must not appear below its height"
    );
}
