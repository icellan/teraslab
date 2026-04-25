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
use teraslab::index::redb_unmined::RedbUnminedIndex;
use teraslab::index::{DahBackend, PrimaryBackend, TxIndexEntry, TxKey, UnminedBackend};
use teraslab::record::TxFlags;
use teraslab::redo::{RedoLog, RedoOp};

fn make_key(n: u8) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0] = n;
    TxKey { txid }
}

fn make_entry(offset: u64, unmined_since: u32, delete_at_height: u32) -> TxIndexEntry {
    TxIndexEntry {
        device_id: 0,
        record_offset: offset,
        utxo_count: 5,
        block_entry_count: 0,
        tx_flags: 0,
        spent_utxos: 0,
        dah_or_preserve: delete_at_height,
        unmined_since,
        generation: 0,
    }
}

/// Crash between redo-fsync and redb commit: the intent record is durable,
/// the redb secondary index is still empty. Recovery must repair the
/// secondary from the durable redo intent.
#[test]
fn crash_after_unmined_redo_fsync_before_redb_commit() {
    let dir = tempfile::tempdir().unwrap();

    // Set up primary (in-memory) with a record whose authoritative
    // unmined_since is 500.
    let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
    let key = make_key(1);
    primary.register(key, make_entry(4096, 500, 0)).unwrap();

    // Open on-disk DAH and unmined indexes.
    let dah_path = dir.path().join("dah.redb");
    let unmined_path = dir.path().join("unmined.redb");
    let mut dah_backend =
        DahBackend::OnDisk(RedbDahIndex::open(&dah_path, 16 * 1024 * 1024).unwrap());
    let mut unmined_backend =
        UnminedBackend::OnDisk(RedbUnminedIndex::open(&unmined_path, 16 * 1024 * 1024).unwrap());

    // Open redo log.
    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let redo_log = Arc::new(Mutex::new(
        RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap(),
    ));

    // *** Simulate the bug window ***
    //
    // Step 1: the intent record is fsynced (using the public RedoLog API
    // directly, to precisely emulate "redo flushed, redb not yet committed").
    {
        let mut log = redo_log.lock();
        log.append_and_flush(RedoOp::SecondaryUnminedUpdate {
            tx_key: key,
            old_height: 0,
            new_height: 500,
        })
        .unwrap();
    }

    // Step 2: the redb commit is SKIPPED (crash).
    assert!(
        unmined_backend.is_empty(),
        "setup invariant: redb has no entry yet"
    );

    drop(redo_log);

    // Reopen the redo log to exercise the post-restart recovery path.
    let redo_log_reopened = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();

    // Use a throwaway data device — recover_all needs &dyn BlockDevice but
    // SecondaryUnminedUpdate replay doesn't touch it.
    let data_dev = MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap();

    let stats = teraslab::recovery::recover_all(
        &data_dev,
        &redo_log_reopened,
        &mut primary,
        &mut dah_backend,
        &mut unmined_backend,
    )
    .unwrap();

    // Recovery should have replayed the secondary intent.
    assert_eq!(
        stats.entries_replayed, 1,
        "recovery should apply the secondary unmined intent"
    );
    assert_eq!(stats.entries_failed, 0);

    // Secondary index is now reconciled with primary.
    let result = unmined_backend.range_query(500);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], key);
}

/// Same scenario for the DAH secondary index.
#[test]
fn crash_after_dah_redo_fsync_before_redb_commit() {
    let dir = tempfile::tempdir().unwrap();

    let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
    let key = make_key(2);
    // Primary's DAH = 900 (no preserve_until).
    let entry = make_entry(8192, 0, 900);
    primary.register(key, entry).unwrap();

    let dah_path = dir.path().join("dah.redb");
    let unmined_path = dir.path().join("unmined.redb");
    let mut dah_backend =
        DahBackend::OnDisk(RedbDahIndex::open(&dah_path, 16 * 1024 * 1024).unwrap());
    let mut unmined_backend =
        UnminedBackend::OnDisk(RedbUnminedIndex::open(&unmined_path, 16 * 1024 * 1024).unwrap());

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
    let alloc =
        SlotAllocator::new(Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap())).unwrap();
    let _ = alloc; // keep variable to ensure SlotAllocator is exercised in scope

    let stats = teraslab::recovery::recover_all(
        &data_dev,
        &redo_log_reopened,
        &mut primary,
        &mut dah_backend,
        &mut unmined_backend,
    )
    .unwrap();

    assert_eq!(stats.entries_replayed, 1);
    assert_eq!(stats.entries_failed, 0);

    let result = dah_backend.range_query(900);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], key);
}

/// Batched redo fsync: a single flush covers BOTH a DAH intent and an
/// unmined intent. Crash happens before either redb commit. Recovery must
/// reconcile both secondary indexes.
#[test]
fn crash_after_batched_redo_fsync_before_both_redb_commits() {
    let dir = tempfile::tempdir().unwrap();

    let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
    let key = make_key(3);
    // Primary: unmined_since = 500, DAH = 900.
    primary.register(key, make_entry(16384, 500, 900)).unwrap();

    let dah_path = dir.path().join("dah.redb");
    let unmined_path = dir.path().join("unmined.redb");
    let mut dah_backend =
        DahBackend::OnDisk(RedbDahIndex::open(&dah_path, 16 * 1024 * 1024).unwrap());
    let mut unmined_backend =
        UnminedBackend::OnDisk(RedbUnminedIndex::open(&unmined_path, 16 * 1024 * 1024).unwrap());

    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let redo_log = Arc::new(Mutex::new(
        RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap(),
    ));

    // Engine would batch both intents into one fsync.
    {
        let mut log = redo_log.lock();
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
        log.append_batch_and_flush(&ops).unwrap();
    }

    // Crash: no redb commits happened.
    assert!(dah_backend.is_empty());
    assert!(unmined_backend.is_empty());

    drop(redo_log);
    let redo_log_reopened = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();

    let data_dev = MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap();

    let stats = teraslab::recovery::recover_all(
        &data_dev,
        &redo_log_reopened,
        &mut primary,
        &mut dah_backend,
        &mut unmined_backend,
    )
    .unwrap();

    assert_eq!(stats.entries_replayed, 2);
    assert_eq!(stats.entries_failed, 0);

    assert_eq!(dah_backend.range_query(900).len(), 1);
    assert_eq!(unmined_backend.range_query(500).len(), 1);
}

/// A stale redo intent (primary moved on to a different value) must NOT be
/// applied. The later-in-time mutation that brought primary to its current
/// state is assumed to have its own redo entry later in the log.
#[test]
fn recover_skips_stale_redo_relative_to_primary() {
    let dir = tempfile::tempdir().unwrap();

    let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
    let key = make_key(4);
    // Primary's authoritative unmined_since is 0 (on-chain).
    primary.register(key, make_entry(4096, 0, 0)).unwrap();

    let unmined_path = dir.path().join("unmined.redb");
    let mut unmined_backend =
        UnminedBackend::OnDisk(RedbUnminedIndex::open(&unmined_path, 16 * 1024 * 1024).unwrap());
    let mut dah_backend = DahBackend::new_in_memory();

    // Stale redo intent: claims unmined_since should become 500. Primary
    // says otherwise (0). Recovery must skip — a later redo superseded this.
    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let redo_log = Arc::new(Mutex::new(
        RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap(),
    ));
    {
        let mut log = redo_log.lock();
        log.append_and_flush(RedoOp::SecondaryUnminedUpdate {
            tx_key: key,
            old_height: 500,
            new_height: 500,
        })
        .unwrap();
    }
    drop(redo_log);

    let redo_log_reopened = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
    let data_dev = MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap();

    let stats = teraslab::recovery::recover_all(
        &data_dev,
        &redo_log_reopened,
        &mut primary,
        &mut dah_backend,
        &mut unmined_backend,
    )
    .unwrap();

    // Stale — skipped, not applied.
    assert_eq!(stats.entries_skipped, 1);
    assert!(unmined_backend.is_empty());
}

/// Ensure the reconcile path correctly interprets the HAS_PRESERVE_UNTIL
/// flag on the primary index entry. When HAS_PRESERVE_UNTIL is set, the
/// primary's `dah_or_preserve` holds the preserve_until value (not DAH),
/// so the authoritative DAH is 0. A redo DAH intent with new_height != 0
/// must therefore be considered stale.
#[test]
fn recover_dah_respects_has_preserve_until_flag() {
    let dir = tempfile::tempdir().unwrap();

    let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
    let key = make_key(5);
    // HAS_PRESERVE_UNTIL flag set; dah_or_preserve = 12345 represents a
    // preserve_until, NOT a DAH — so authoritative DAH = 0.
    let mut entry = make_entry(4096, 0, 12345);
    entry.tx_flags = TxFlags::HAS_PRESERVE_UNTIL.bits();
    primary.register(key, entry).unwrap();

    let dah_path = dir.path().join("dah.redb");
    let mut dah_backend =
        DahBackend::OnDisk(RedbDahIndex::open(&dah_path, 16 * 1024 * 1024).unwrap());
    let mut unmined_backend = UnminedBackend::new_in_memory();

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

    let stats = teraslab::recovery::recover_all(
        &data_dev,
        &redo_log_reopened,
        &mut primary,
        &mut dah_backend,
        &mut unmined_backend,
    )
    .unwrap();

    assert_eq!(stats.entries_skipped, 1);
    assert!(dah_backend.is_empty());
}
