//! Integration tests for the in-process fault-injection harness.
//!
//! Each test arms a [`SyncPoint`] via
//! [`teraslab::fault_injection::arm`], exercises a mutation that
//! touches the armed boundary, catches the induced panic with
//! [`std::panic::catch_unwind`], then tears down in-memory state
//! (RedoLog, index backends) and reconstructs from persistent bytes
//! before asserting post-recovery invariants.
//!
//! The panics are in-process, but they exercise the SAME recovery
//! paths that a real SIGKILL would: nothing placed AFTER the panic
//! runs, so the durability contract "everything before the last
//! successful fsync is persisted" holds by construction (the panic
//! sits precisely at the fsync / commit / rename boundary).
//!
//! These tests require the `fault-injection` feature flag:
//!
//! ```text
//! cargo test --features fault-injection --test fault_injection
//! ```

#![cfg(feature = "fault-injection")]

use parking_lot::Mutex;
use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::MemoryDevice;
use teraslab::fault_injection::{FaultMode, SyncPoint, arm, current, disarm};
use teraslab::index::hashtable::HashTable;
use teraslab::index::redb_dah::RedbDahIndex;
use teraslab::index::redb_unmined::RedbUnminedIndex;
use teraslab::index::{DahBackend, PrimaryBackend, TxIndexEntry, TxKey, UnminedBackend};
use teraslab::redo::{RedoLog, RedoOp};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Catch a panic while ensuring the thread-local [`FaultMode`] is
/// restored even if the closure succeeds without panicking.
///
/// Wraps the closure in [`std::panic::AssertUnwindSafe`] because the
/// types we poke at (`RedoLog`, `SlotAllocator`, `HashTable`, redb
/// `Database`) are not `UnwindSafe` — they contain `RefCell`s,
/// `Mutex`es, or raw pointers. The test code itself is
/// responsible for not observing torn state after the induced panic,
/// and asserts exclusively on post-recovery disk content.
fn armed<R>(mode: FaultMode, f: impl FnOnce() -> R) -> std::thread::Result<R> {
    let _prev = arm(mode);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let _ = disarm();
    // Sanity: mode was correctly cleared.
    assert_eq!(current(), FaultMode::None);
    result
}

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

// ---------------------------------------------------------------------------
// Test 1: crash between redo fsync and data pwrite (spend path)
// ---------------------------------------------------------------------------

/// Kill between `RedoLog::flush`'s fsync and the engine's slot pwrite.
/// We write the redo entry first, arm a panic immediately after the
/// fsync, then verify the panic fires on `flush`. On replay, the redo
/// entry is durable — recovery MUST apply the spend to the on-device
/// slot.
#[test]
fn kill_after_redo_fsync_before_data_pwrite_recovers_slot() {
    // Set up primary (in-memory) and a data device. Seed one record
    // with a valid slot 0 we intend to spend.
    let data_dev: Arc<dyn teraslab::device::BlockDevice> =
        Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
    let key = make_key(7);

    // Allocate a record and write an initial unspent slot + metadata.
    let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
    let utxo_count: u32 = 1;
    let record_size = teraslab::record::TxMetadata::record_size_for(utxo_count);
    let record_offset = alloc.allocate(record_size).unwrap();

    let mut meta = teraslab::record::TxMetadata::new(utxo_count);
    meta.tx_id = key.txid;
    let slot_hash = [0x42u8; 32];
    let slot = teraslab::record::UtxoSlot::new_unspent(slot_hash);
    teraslab::io::write_full_record(&*data_dev, record_offset, &meta, &[slot]).unwrap();

    primary
        .register(key, make_entry(record_offset, 0, 0))
        .unwrap();

    // Redo log on its own device.
    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let redo_log = Arc::new(Mutex::new(
        RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap(),
    ));

    // The spending data we intend to apply.
    let spending_data = {
        let mut sd = [0u8; 36];
        sd[0] = 0xAB;
        sd[32..36].copy_from_slice(&42u32.to_le_bytes());
        sd
    };

    // Arm: panic AFTER redo fsync (meaning the fsync bytes are on the
    // device, then the panic fires before any further work). Execute
    // the redo append-and-flush; it must panic.
    let redo_log_for_panic = redo_log.clone();
    let outcome = armed(FaultMode::PanicAt(SyncPoint::AfterRedoFsync), move || {
        let mut log = redo_log_for_panic.lock();
        let _ = log.append_and_flush(RedoOp::Spend {
            tx_key: key,
            offset: 0,
            spending_data,
            new_spent_count: 1,
        });
    });
    outcome.expect_err("expected induced panic at AfterRedoFsync");

    // Sanity: the slot on the data device is STILL unspent — no
    // post-fsync work ran.
    let pre_recovery_slot = teraslab::io::read_utxo_slot(&*data_dev, record_offset, 0).unwrap();
    assert!(
        pre_recovery_slot.is_unspent(),
        "pre-recovery slot must still be unspent (panic fired before pwrite)"
    );

    // Drop the live redo handle to mimic process restart.
    drop(redo_log);

    // Reopen and run recovery. The durable redo entry must drive the
    // slot to spent.
    let redo_reopened = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
    let mut dah = DahBackend::new_in_memory();
    let mut unmined = UnminedBackend::new_in_memory();
    let stats = teraslab::recovery::recover_all(
        &*data_dev,
        &redo_reopened,
        &mut primary,
        &mut dah,
        &mut unmined,
    )
    .unwrap();
    assert_eq!(
        stats.entries_replayed, 1,
        "exactly one Spend redo entry must be replayed"
    );
    assert_eq!(stats.entries_failed, 0, "no replay must fail");

    let recovered_slot = teraslab::io::read_utxo_slot(&*data_dev, record_offset, 0).unwrap();
    assert!(
        !recovered_slot.is_unspent(),
        "post-recovery slot must be spent"
    );
    assert_eq!(
        recovered_slot.spending_data, spending_data,
        "spending_data must match the journaled intent"
    );
    // Idempotency: a second recovery run must be a no-op (entries_skipped > 0).
    let stats2 = teraslab::recovery::recover_all(
        &*data_dev,
        &redo_reopened,
        &mut primary,
        &mut dah,
        &mut unmined,
    )
    .unwrap();
    assert_eq!(
        stats2.entries_replayed, 0,
        "second recovery must not re-apply (idempotent)"
    );
    assert!(stats2.entries_skipped >= 1);
}

// ---------------------------------------------------------------------------
// Test 2: kill between hashtable tmp rename and parent-dir fsync
// ---------------------------------------------------------------------------

/// Kill between the `rename(2)` of the tmp file and the
/// `fsync_parent_dir` that makes the rename metadata durable. On
/// recovery, the `HashtableResizeBegin` without a matching `Commit`
/// must leave the system in a consistent state — either:
///   (a) the original index file remains usable and the tmp-file
///       orphan is cleaned, OR
///   (b) the renamed-in new table is valid and scanning preserves
///       all entries.
///
/// The production code paths ensure (a) via the recovery orphan
/// cleanup; this test asserts the stronger invariant that all pre-
/// crash entries survive recovery.
#[test]
fn kill_between_rename_and_dir_fsync_recovers_hashtable() {
    let dir = tempfile::tempdir().unwrap();
    let idx_path = dir.path().join("ht.idx");

    // Build a file-backed hashtable, insert a few entries, attach a
    // redo log (so the resize will journal Begin/Commit).
    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let redo_log = Arc::new(Mutex::new(
        RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap(),
    ));

    let entries: Vec<(TxKey, TxIndexEntry)> = (1u8..=8u8)
        .map(|i| (make_key(i), make_entry((i as u64) * 4096, i as u32 * 10, 0)))
        .collect();

    {
        let mut ht = HashTable::open_file_backed(&idx_path, 16).unwrap();
        ht.set_redo_log(redo_log.clone());
        for (k, e) in &entries {
            ht.insert(*k, *e).unwrap();
        }
        drop(ht); // flush & unmap

        // Reopen for the resize (`set_redo_log` must be re-attached).
        let mut ht = HashTable::open_file_backed(&idx_path, 16).unwrap();
        ht.set_redo_log(redo_log.clone());

        // Arm the panic at the post-rename / pre-dir-fsync boundary.
        let panicked = armed(FaultMode::PanicAt(SyncPoint::MidHashtableResize), || {
            let _ = ht.resize(64);
        });
        panicked.expect_err("resize must panic at MidHashtableResize");
        // The in-memory `ht` binding is effectively poisoned by the
        // partially-completed resize — drop it explicitly to release
        // the mmap before we re-open.
        drop(ht);
    }

    // Run recovery: scan the redo log for pending resize intents and
    // clean up any orphan tmp file. The primary index file has either
    // already been renamed over (the rename succeeded pre-crash) or
    // the tmp file is an orphan — recovery handles both.
    let redo_reopened = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
    let data_dev = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let mut alloc_recov = SlotAllocator::new(data_dev.clone()).unwrap();
    let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
    let mut dah = DahBackend::new_in_memory();
    let mut unmined = UnminedBackend::new_in_memory();

    let stats = teraslab::recovery::recover_all_with_allocator(
        &*data_dev,
        &redo_reopened,
        &mut primary,
        &mut dah,
        &mut unmined,
        Some(&mut alloc_recov),
    )
    .unwrap();
    // We expect at least the `HashtableResizeBegin` entry.
    assert!(
        stats.entries_replayed >= 1,
        "recovery must process at least the resize-begin entry"
    );

    // The rename may have succeeded before the panic fired (rename(2)
    // is synchronous at the VFS level; the dir-fsync only makes the
    // directory-entry durable, but on a non-crash in-process panic
    // the rename bytes are already visible to subsequent opens).
    // Either way, the primary index file must exist and contain all
    // pre-crash entries.
    let tmp_path = idx_path.with_extension("tmp");
    assert!(
        !tmp_path.exists(),
        "recovery must remove orphan tmp file (if any)"
    );
    assert!(
        idx_path.exists(),
        "primary index file must survive recovery"
    );

    // Scan the recovered table and assert every pre-crash entry is
    // present and unchanged.
    let recovered = HashTable::open_file_backed(&idx_path, 16).unwrap();
    for (k, expected) in &entries {
        let actual = recovered
            .get_entry(k)
            .unwrap_or_else(|| panic!("missing entry for key {:?} after recovery", k.txid[0]));
        assert_eq!(
            actual.record_offset, expected.record_offset,
            "record_offset mismatch for key {}",
            k.txid[0]
        );
        assert_eq!(
            actual.unmined_since, expected.unmined_since,
            "unmined_since mismatch for key {}",
            k.txid[0]
        );
    }
}

// ---------------------------------------------------------------------------
// Test 3: kill between allocator redo append and freelist mutation
// ---------------------------------------------------------------------------

/// `free()` flushes a `FreeRegion` redo entry BEFORE mutating the
/// in-memory freelist. Kill immediately after the fsync and verify
/// that recovery's allocator replay reconstructs the freelist state.
#[test]
fn kill_after_free_redo_fsync_before_freelist_mutation_reconstructs_freelist() {
    // Data device + redo log.
    let data_dev: Arc<dyn teraslab::device::BlockDevice> =
        Arc::new(MemoryDevice::new(32 * 1024 * 1024, 4096).unwrap());
    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let redo_log = Arc::new(Mutex::new(
        RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap(),
    ));

    // Build an allocator with the redo log attached, then allocate a
    // region. The allocate redo entry goes in first — snapshot the
    // state before we attempt the free-with-panic.
    let mut alloc = SlotAllocator::new(data_dev.clone()).unwrap();
    alloc.set_redo_log(redo_log.clone());
    let off = alloc.allocate(8192).unwrap();
    let pre_free_next_offset = alloc.next_offset();

    // Arm panic at MidAllocatorPersist: `free` appends + flushes the
    // FreeRegion redo, then panics before the freelist mutation.
    let panicked = armed(FaultMode::PanicAt(SyncPoint::MidAllocatorPersist), || {
        let _ = alloc.free(off, 8192);
    });
    panicked.expect_err("free must panic at MidAllocatorPersist");

    // The in-memory alloc state at this point has NOT recorded the
    // free (panic fired before freelist mutation).
    // Drop it to simulate process restart.
    drop(alloc);

    // Reconstruct: a fresh allocator derived from a freshly-persisted
    // snapshot would not yet reflect the free. Recovery with the redo
    // log replays the FreeRegion entry and rebuilds the freelist.
    let mut recovered_alloc = SlotAllocator::new(data_dev.clone()).unwrap();
    // Re-allocate up to pre_free_next_offset so the allocator's
    // high-water mark aligns with the pre-crash state.
    let catchup = pre_free_next_offset - teraslab::allocator::DATA_REGION_OFFSET;
    if catchup > 0 {
        let _ = recovered_alloc.allocate(catchup).unwrap();
    }

    let pre_freelist_largest = recovered_alloc.stats().largest_free_region;
    let redo_reopened = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
    let mut primary = PrimaryBackend::new_in_memory(16).unwrap();
    let mut dah = DahBackend::new_in_memory();
    let mut unmined = UnminedBackend::new_in_memory();
    let stats = teraslab::recovery::recover_all_with_allocator(
        &*data_dev,
        &redo_reopened,
        &mut primary,
        &mut dah,
        &mut unmined,
        Some(&mut recovered_alloc),
    )
    .unwrap();

    // Both an AllocateRegion (from `allocate`) and a FreeRegion (from
    // the panicked `free`) are in the redo log. Replay of the first
    // is idempotent (high-water already reflects it after our catchup);
    // replay of the second inserts the freed region back into the
    // freelist.
    assert!(
        stats.entries_replayed + stats.entries_skipped >= 2,
        "redo must contain at least the AllocateRegion + FreeRegion entries"
    );
    assert_eq!(stats.entries_failed, 0, "no replay must fail");

    let post_freelist_largest = recovered_alloc.stats().largest_free_region;
    assert!(
        post_freelist_largest >= pre_freelist_largest,
        "freelist's largest-free must grow (or stay equal) after replay"
    );
    // The freed region of 8192 bytes must be in the freelist — an
    // 8192-byte allocation must now succeed from the freelist rather
    // than bumping the high-water mark.
    let pre_reallocate_next = recovered_alloc.next_offset();
    let new_off = recovered_alloc.allocate(8192).unwrap();
    assert_eq!(
        new_off, off,
        "allocator must reuse the freed region ({off:#x})"
    );
    assert_eq!(
        recovered_alloc.next_offset(),
        pre_reallocate_next,
        "allocation from freelist must NOT bump the high-water mark"
    );
}

// ---------------------------------------------------------------------------
// Test 4: kill between secondary-redb commit phases (C4 contract)
// ---------------------------------------------------------------------------

/// Kill between `RedbUnminedIndex::insert`'s redo fsync and the redb
/// transaction commit. Recovery must reconcile the secondary index
/// from the durable redo intent.
#[test]
fn kill_before_secondary_redb_commit_reconciles_via_redo() {
    let dir = tempfile::tempdir().unwrap();
    let unmined_path = dir.path().join("unmined.redb");
    let dah_path = dir.path().join("dah.redb");
    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let redo_log = Arc::new(Mutex::new(
        RedoLog::open(redo_dev.clone(), 0, 1024 * 1024).unwrap(),
    ));

    // Primary with an authoritative unmined_since=500 entry.
    let mut primary = PrimaryBackend::new_in_memory(100).unwrap();
    let key = make_key(3);
    primary.register(key, make_entry(4096, 500, 0)).unwrap();

    // Arm the panic at BeforeSecondaryRedbCommit, then call insert on
    // the on-disk unmined backend. The redo flush completes, the
    // panic fires BEFORE `txn.commit()` runs.
    {
        let mut unmined = RedbUnminedIndex::open(&unmined_path, 16 * 1024 * 1024).unwrap();
        let redo_for_panic = redo_log.clone();
        let panicked = armed(
            FaultMode::PanicAt(SyncPoint::BeforeSecondaryRedbCommit),
            move || {
                let _ = unmined.insert(500, key, Some(&*redo_for_panic));
            },
        );
        panicked.expect_err("insert must panic at BeforeSecondaryRedbCommit");
    }

    // Post-panic: reopen the redb file. It must not contain the entry
    // (commit never ran).
    {
        let unmined = RedbUnminedIndex::open(&unmined_path, 16 * 1024 * 1024).unwrap();
        let result = unmined.range_query(500);
        assert!(
            result.is_empty(),
            "redb must be empty before recovery — commit did not run"
        );
    }

    // Drop the live redo handle to mimic process restart.
    drop(redo_log);

    // Reopen and run recovery with both secondary backends.
    let redo_reopened = RedoLog::open(redo_dev, 0, 1024 * 1024).unwrap();
    let mut dah_backend =
        DahBackend::OnDisk(RedbDahIndex::open(&dah_path, 16 * 1024 * 1024).unwrap());
    let mut unmined_backend =
        UnminedBackend::OnDisk(RedbUnminedIndex::open(&unmined_path, 16 * 1024 * 1024).unwrap());

    // Throwaway data device — the SecondaryUnminedUpdate replay doesn't
    // touch it.
    let data_dev = MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap();
    let stats = teraslab::recovery::recover_all(
        &data_dev,
        &redo_reopened,
        &mut primary,
        &mut dah_backend,
        &mut unmined_backend,
    )
    .unwrap();
    assert!(
        stats.entries_replayed >= 1,
        "recovery must replay the SecondaryUnminedUpdate intent"
    );
    assert_eq!(stats.entries_failed, 0);

    // Secondary is reconciled: the txid now appears in the unmined
    // range query at height 500.
    let recovered = unmined_backend.range_query(500);
    assert_eq!(
        recovered.len(),
        1,
        "recovered unmined index must contain the entry"
    );
    assert_eq!(
        recovered[0], key,
        "recovered entry must be the original key"
    );
}

// ---------------------------------------------------------------------------
// Test 5: armed-but-mismatched points are silent
// ---------------------------------------------------------------------------

/// Sanity coverage for the fault-injection harness itself from the
/// integration-test side: an arm at one [`SyncPoint`] must not fire at
/// any other point. Without this a test author could miswrite an
/// arm/assert pair and pass spuriously.
///
/// F-G1-013 removed the previous `NoOpAt` variant (and the
/// corresponding sub-assert here) because it was observationally
/// identical to `None` — a "never panics" check on a variant that
/// never did anything could not catch a broken harness either way.
#[test]
fn unmatched_fault_modes_are_silent() {
    // Unmatched: armed at AfterRedoFsync, but the flush runs without a
    // fsync-triggering check (no redo log in scope) — the arm just
    // stays active and is cleared.
    let prev = arm(FaultMode::PanicAt(SyncPoint::AfterRedoFsync));
    assert_eq!(prev, FaultMode::None);

    // Invoke a `check` for a different SyncPoint — no panic.
    teraslab::fault_injection::check(SyncPoint::MidAllocatorPersist);
    teraslab::fault_injection::check(SyncPoint::BeforeIndexCommit);

    let cleared = disarm();
    assert!(matches!(
        cleared,
        FaultMode::PanicAt(SyncPoint::AfterRedoFsync)
    ));
    assert_eq!(current(), FaultMode::None);
}
