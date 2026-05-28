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

// ---------------------------------------------------------------------------
// Tests 6-8: BeforeRedoFsync — pre-fsync crash window (Theme-4 P0)
// ---------------------------------------------------------------------------
//
// The BeforeRedoFsync SyncPoint sits AFTER `pwrite_all_at` returns but
// BEFORE `device.sync()` is called. In production this models the
// crash window where redo bytes are in the kernel page cache but the
// fsync has not stabilised them on the platter — the highest-risk
// crash window, since a writer that loses power here loses entries
// it has not yet been told are durable.
//
// In this in-process harness the backing device is a `MemoryDevice`
// whose `pwrite_all_at` directly mutates the backing allocation
// (there is no page-cache layer to lose). Tests below therefore
// simulate the "fsync did not happen → batch bytes did not survive"
// contract by EXPLICITLY ZEROING the entries region of the device
// after catching the BeforeRedoFsync panic, BEFORE re-opening the
// redo log. That zero-write models a power-loss-induced page-cache
// drop of the not-yet-fsynced bytes. The pre-fsync prefix that did
// reach an earlier successful `sync()` is preserved.
//
// Why this faithfully exercises production code:
//   1. `RedoLog::flush` is invoked end-to-end (buffer assembly,
//      partial-block read-modify, pwrite, fault-injection check).
//   2. The panic fires at exactly `src/redo.rs:1818`, proving the
//      SyncPoint is present and on the hot path. Removing the
//      sync_point! call would skip the panic and the test would
//      fail at `expect_err(...)`.
//   3. Recovery (`RedoLog::open` + `recover`) is exercised against
//      a device state that matches the post-fsync-loss contract.

/// Overwrite the trailing bytes of the redo log's entries region
/// with zeros, preserving the first `keep_aligned_blocks` aligned
/// blocks. Used to simulate page-cache loss of the un-fsynced tail:
/// the durable prefix that did reach a successful `sync()` is kept,
/// everything past that boundary is dropped.
///
/// Layout reminder (F-G4-001 + F-G4-004): the redo log reserves
/// `device.alignment()` bytes at `log_offset` for the header; the
/// entries region begins at `log_offset + alignment`, and each
/// successful flush writes at the next aligned offset within it.
fn corrupt_trailing_entries_bytes(
    dev: &dyn teraslab::device::BlockDevice,
    log_size: u64,
    keep_aligned_blocks: u64,
) {
    let align = dev.alignment() as u64;
    let entries_off = align; // header occupies block 0 at log_offset=0
    let preserve_bytes = keep_aligned_blocks * align;
    let entries_len = log_size - align;
    assert!(
        preserve_bytes <= entries_len,
        "keep_aligned_blocks={keep_aligned_blocks} aligned blocks exceeds entries region"
    );
    let corrupt_off = entries_off + preserve_bytes;
    let corrupt_len = (entries_len - preserve_bytes) as usize;
    let zeros = vec![0u8; corrupt_len];
    dev.pwrite_all_at(&zeros, corrupt_off)
        .expect("zeroing trailing entries bytes must succeed");
}

/// Test 6 — Theme-4 P0
///
/// Append N entries that are NOT yet fsynced (writer is mid-batch),
/// trigger a simulated crash via `BeforeRedoFsync`, restart, and
/// assert that the recovered set equals the LAST FSYNCED PREFIX —
/// not the partially-written tail. Specifically:
///
///   1. Flush three entries successfully (seq 1, 2, 3) — these are
///      the durable prefix that MUST survive recovery.
///   2. Append two more entries (seq 4, 5) into the buffer.
///   3. Arm `PanicAt(BeforeRedoFsync)`, call `flush()`. The pwrite
///      runs; the SyncPoint fires; flush panics before `sync()`
///      makes the new bytes durable.
///   4. Simulate page-cache loss by zeroing the entries region's
///      not-yet-fsynced tail (block 2 onwards — block 0 is the
///      header, block 1 holds the fsynced seq 1-3 prefix).
///   5. Re-open the redo log and call `recover()`. The result MUST
///      be exactly seq 1-3; seq 4-5 MUST NOT appear.
///
/// If a regression removes the `sync_point!` call at `src/redo.rs:1818`,
/// the `armed(...)` block below would not panic — the
/// `outcome.expect_err(...)` assertion would fire and the test would
/// fail loudly. That is the regression-detection signal this test
/// provides.
#[test]
fn before_redo_fsync_simulated_crash_loses_only_uncommitted_entries() {
    let log_size: u64 = 1024 * 1024;
    let redo_dev: Arc<dyn teraslab::device::BlockDevice> =
        Arc::new(MemoryDevice::new(log_size, 4096).unwrap());

    let mut log = RedoLog::open(redo_dev.clone(), 0, log_size).unwrap();

    // Step 1: three entries committed via successful fsync.
    let durable_ops: Vec<RedoOp> = (1u8..=3u8)
        .map(|i| RedoOp::Freeze {
            tx_key: make_key(i),
            offset: i as u32,
        })
        .collect();
    for op in &durable_ops {
        log.append_and_flush(op.clone()).unwrap();
    }

    // Step 2: two entries appended but NOT yet flushed.
    let pending_ops: Vec<RedoOp> = (10u8..=11u8)
        .map(|i| RedoOp::Freeze {
            tx_key: make_key(i),
            offset: i as u32,
        })
        .collect();
    for op in &pending_ops {
        log.append(op.clone()).unwrap();
    }

    // Step 3: arm BeforeRedoFsync and flush. The flush must panic —
    // if it does not, the SyncPoint check at src/redo.rs:1818 is
    // missing or has been moved out of the flush path.
    let log_cell: Arc<Mutex<Option<RedoLog>>> = Arc::new(Mutex::new(Some(log)));
    let log_for_panic = log_cell.clone();
    let outcome = armed(FaultMode::PanicAt(SyncPoint::BeforeRedoFsync), move || {
        let mut guard = log_for_panic.lock();
        let log = guard.as_mut().expect("log present");
        let _ = log.flush();
    });
    outcome.expect_err(
        "flush must panic at BeforeRedoFsync — if this fails, the \
         sync_point! check is no longer reached on the flush path",
    );

    // Drop the in-memory log so we cannot accidentally read from the
    // entries_cache and mask a recovery bug.
    {
        let mut guard = log_cell.lock();
        let _ = guard.take();
    }

    // Step 4: simulate page-cache loss of the un-fsynced tail.
    // F-G4-004: each successful flush bumps `write_pos` to the next
    // aligned boundary, so the three independent flushes above each
    // occupy a separate aligned block (blocks 0, 1, 2 of the entries
    // region). The panicked flush's pwrite landed at block 3. We
    // preserve the first 3 blocks (the fsynced prefix) and zero the
    // rest — modelling the "fsync never happened" contract for the
    // panicked batch only.
    corrupt_trailing_entries_bytes(&*redo_dev, log_size, durable_ops.len() as u64);

    // Step 5: re-open and recover. The durable prefix must be exactly
    // seq 1-3; nothing from the panicked batch may appear.
    let reopened = RedoLog::open(redo_dev.clone(), 0, log_size).unwrap();
    let recovered = reopened.recover().unwrap();
    assert_eq!(
        recovered.len(),
        durable_ops.len(),
        "recovery must yield exactly the fsynced-prefix entries; \
         got {} entries, expected {} (un-fsynced batch must be lost)",
        recovered.len(),
        durable_ops.len(),
    );
    for (i, entry) in recovered.iter().enumerate() {
        assert_eq!(
            entry.sequence,
            (i as u64) + 1,
            "recovered entry #{i} sequence mismatch"
        );
        assert_eq!(
            entry.op, durable_ops[i],
            "recovered entry #{i} op must match the original fsynced op"
        );
    }
    // Negative assertion: none of the pending ops appear.
    for pending in &pending_ops {
        assert!(
            !recovered.iter().any(|e| &e.op == pending),
            "pending op {pending:?} must NOT appear in recovery — \
             it was never fsynced",
        );
    }
}

/// Test 7 — Theme-4 P0
///
/// Simulate a torn write where the OS persisted SOME bytes of the
/// batch but not all (partial writev). The redo scanner must
/// terminate cleanly at the last valid entry boundary — no infinite
/// loop, no corruption propagated into recovery.
///
/// Approach:
///   1. Flush a durable prefix (seq 1).
///   2. Append seq 2 + seq 3 into the buffer.
///   3. Arm `BeforeRedoFsync`, flush — the pwrite places ALL batch
///      bytes into memory but the panic fires before `sync()`.
///   4. Simulate a torn write by corrupting the trailing portion of
///      what would have been the new flush region: keep the first
///      part (which encodes seq 2 cleanly) and zero the rest, so seq
///      3's bytes are split between "header survived" and "body
///      lost". Concretely, we zero block 2 onwards (preserving block
///      0 = header, block 1 = seq 1, and a portion of block 2 that
///      may hold a complete seq 2 header but a truncated tail).
///   5. Re-open and recover. The call MUST return (no infinite loop)
///      and the result MUST be a prefix of the original sequence —
///      either {seq 1} or {seq 1, seq 2}, but never with seq 3.
///
/// A watchdog channel + 5 s timeout guards against an infinite-loop
/// regression in the scan/carry path.
#[test]
fn before_redo_fsync_crash_after_partial_writev_returns_consistent_prefix() {
    let log_size: u64 = 1024 * 1024;
    let redo_dev: Arc<dyn teraslab::device::BlockDevice> =
        Arc::new(MemoryDevice::new(log_size, 4096).unwrap());

    let mut log = RedoLog::open(redo_dev.clone(), 0, log_size).unwrap();

    // Durable prefix.
    let durable_op = RedoOp::Freeze {
        tx_key: make_key(1),
        offset: 1,
    };
    log.append_and_flush(durable_op.clone()).unwrap();

    // Pending batch of two entries — these will reach the device
    // memory via pwrite but the SyncPoint panic prevents sync().
    let pending_ops: Vec<RedoOp> = vec![
        RedoOp::Freeze {
            tx_key: make_key(2),
            offset: 2,
        },
        RedoOp::Freeze {
            tx_key: make_key(3),
            offset: 3,
        },
    ];
    for op in &pending_ops {
        log.append(op.clone()).unwrap();
    }

    let log_cell: Arc<Mutex<Option<RedoLog>>> = Arc::new(Mutex::new(Some(log)));
    let log_for_panic = log_cell.clone();
    let outcome = armed(FaultMode::PanicAt(SyncPoint::BeforeRedoFsync), move || {
        let mut guard = log_for_panic.lock();
        let log = guard.as_mut().expect("log present");
        let _ = log.flush();
    });
    outcome.expect_err("flush must panic at BeforeRedoFsync");
    {
        let mut guard = log_cell.lock();
        let _ = guard.take();
    }

    // Torn-write simulation: preserve block 0 (header) + block 1 (seq
    // 1, fsynced) and zero everything past that. This drops the body
    // of the not-yet-fsynced batch — any entry whose serialized bytes
    // straddle that boundary is now structurally invalid (its CRC
    // and/or length will not match the zeroed tail).
    //
    // We use the helper `corrupt_trailing_entries_bytes` with
    // keep=1 (only the seq-1 block is preserved) — this models the
    // worst-case torn write where ALL pending bytes are lost.
    corrupt_trailing_entries_bytes(&*redo_dev, log_size, 1);

    // Watchdog: a regression that drops the scanner into an infinite
    // loop on a torn entry would hang CI without this guard. The
    // pad-gap truncation test in src/redo.rs uses the same pattern.
    let dev_for_scan = redo_dev.clone();
    let (tx, rx) = std::sync::mpsc::channel::<Vec<teraslab::redo::RedoEntry>>();
    let handle = std::thread::spawn(move || {
        let reopened = RedoLog::open(dev_for_scan, 0, log_size).unwrap();
        let entries = reopened.recover().unwrap();
        let _ = tx.send(entries);
    });
    let recovered = match rx.recv_timeout(std::time::Duration::from_secs(5)) {
        Ok(entries) => entries,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!(
                "redo scan did not terminate within 5 s on a torn-write \
                 entries region — likely an infinite loop in the \
                 scan/carry path triggered by BeforeRedoFsync"
            );
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("scan worker disconnected without producing a result")
        }
    };
    handle.join().expect("scan worker panicked");

    // The recovered set must be a prefix of the original sequence:
    //   - it MUST contain seq 1 (durably fsynced before the panic).
    //   - it MUST NOT contain seq 3 (the torn-write zone wiped its
    //     tail; recovery rejecting it is the consistent-prefix
    //     property).
    //   - seq 2 is permitted-but-not-required: depending on whether
    //     its bytes happened to fall inside or outside the preserved
    //     block, recovery may accept it or stop at the first gap.
    //     Either outcome satisfies "consistent prefix".
    assert!(
        !recovered.is_empty(),
        "recovery must surface at least the durable seq=1 prefix"
    );
    assert_eq!(
        recovered[0].sequence, 1,
        "the durable prefix must start at sequence 1"
    );
    assert_eq!(
        recovered[0].op, durable_op,
        "the durable prefix entry must match the originally fsynced op"
    );
    // Sequences are strictly monotonic; the recovered slice must be
    // a contiguous prefix 1..=N for some N <= 2 (seq 3's bytes are
    // wholly inside the wiped region).
    for (i, entry) in recovered.iter().enumerate() {
        assert_eq!(
            entry.sequence,
            (i as u64) + 1,
            "recovered entry #{i} broke contiguous-prefix invariant"
        );
    }
    assert!(
        recovered.len() <= 2,
        "recovery returned {} entries; only the fsynced prefix (and at \
         most one bonus entry whose bytes happened to survive in the \
         preserved block) is permitted",
        recovered.len(),
    );
    // Negative assertion: the last pending op (seq 3) must NOT appear.
    let last_pending = &pending_ops[1];
    assert!(
        !recovered.iter().any(|e| &e.op == last_pending),
        "torn-write tail op {last_pending:?} must NOT appear in recovery"
    );
}

/// Test 8 — Theme-4 P0
///
/// Caller-visibility contract: between `append()` (which returns a
/// sequence number) and the next successful `flush()`, the entry is
/// QUEUED but not durable. A second reader that re-opens the log
/// from the device MUST NOT observe the queued entry — exactly the
/// behaviour BeforeRedoFsync exists to defend.
///
/// Approach:
///   1. Flush a durable baseline entry (seq 1).
///   2. `append()` a second entry (seq 2). `append` returns Ok(2),
///      proving the entry is queued.
///   3. Without flushing, simulate a process restart by zeroing the
///      not-yet-fsynced portion of the entries region and re-opening
///      the redo log against the device.
///   4. A reader that calls `recover()` / `read_from_sequence(1)` on
///      the reopened log MUST see only seq 1. seq 2 was queued, not
///      durable, and a crash here loses it.
///   5. Then, for completeness, arm `BeforeRedoFsync` and flush a
///      different entry — confirm the SyncPoint check is on the
///      flush path. This guards against a regression that moves the
///      check elsewhere and silently widens the visibility window.
#[test]
fn before_redo_fsync_caller_sees_durable_only_after_sync() {
    let log_size: u64 = 1024 * 1024;
    let redo_dev: Arc<dyn teraslab::device::BlockDevice> =
        Arc::new(MemoryDevice::new(log_size, 4096).unwrap());

    let mut log = RedoLog::open(redo_dev.clone(), 0, log_size).unwrap();

    // Durable baseline.
    let durable_op = RedoOp::Freeze {
        tx_key: make_key(1),
        offset: 0,
    };
    let durable_seq = log.append_and_flush(durable_op.clone()).unwrap();
    assert_eq!(durable_seq, 1, "first appended sequence must be 1");

    // Queued-but-not-durable entry.
    let queued_op = RedoOp::Freeze {
        tx_key: make_key(99),
        offset: 99,
    };
    let queued_seq = log.append(queued_op.clone()).unwrap();
    assert_eq!(
        queued_seq, 2,
        "append must return a fresh, monotonically-greater sequence"
    );

    // Drop the writer's in-memory log handle WITHOUT calling flush —
    // this is the "process crashed while bytes were still in the
    // buffer" path. The in-memory `pending_entries` and `buffer`
    // vanish; the device retains only what previous successful
    // flushes wrote.
    drop(log);

    // Simulate page-cache loss of any block past the durable prefix.
    // F-G4-004: the single successful flush above placed seq 1 into
    // block 0 of the entries region; the queued seq 2 (never
    // flushed) was only in the in-memory buffer. Preserve block 0,
    // zero the rest — this models the contract "everything before
    // the last successful fsync is durable; anything queued after is
    // lost on crash".
    corrupt_trailing_entries_bytes(&*redo_dev, log_size, 1);

    // Reader path: a fresh open of the redo log against the same
    // device, simulating a separate process / restart. The queued
    // entry MUST NOT be observable.
    let reader_log = RedoLog::open(redo_dev.clone(), 0, log_size).unwrap();
    let recovered = reader_log.recover().unwrap();
    assert_eq!(
        recovered.len(),
        1,
        "reader must see exactly the fsynced entry; got {} (queued \
         entry leaked into the durable view — this is the contract \
         BeforeRedoFsync exists to defend)",
        recovered.len(),
    );
    assert_eq!(recovered[0].sequence, durable_seq);
    assert_eq!(recovered[0].op, durable_op);
    assert!(
        !recovered.iter().any(|e| e.op == queued_op),
        "queued-but-not-flushed op must NOT appear in a reader's view"
    );

    // Also assert read_from_sequence respects the contract.
    let from_one = reader_log.read_from_sequence(1).unwrap();
    assert_eq!(from_one.len(), 1);
    assert_eq!(from_one[0].op, durable_op);

    // Coverage-of-SyncPoint sanity: open a fresh log on the same
    // device and arm BeforeRedoFsync to confirm the check is still
    // wired into the flush path. If a regression removed the check,
    // this `expect_err` would fire and the test would fail.
    let mut sanity_log = RedoLog::open(redo_dev.clone(), 0, log_size).unwrap();
    let sanity_op = RedoOp::Freeze {
        tx_key: make_key(200),
        offset: 200,
    };
    sanity_log.append(sanity_op).unwrap();
    let sanity_cell: Arc<Mutex<Option<RedoLog>>> = Arc::new(Mutex::new(Some(sanity_log)));
    let sanity_for_panic = sanity_cell.clone();
    let sanity_outcome = armed(FaultMode::PanicAt(SyncPoint::BeforeRedoFsync), move || {
        let mut guard = sanity_for_panic.lock();
        let log = guard.as_mut().expect("sanity log present");
        let _ = log.flush();
    });
    sanity_outcome.expect_err(
        "BeforeRedoFsync must remain on the flush path — if this fails, \
         the SyncPoint has regressed and Tests 6/7 above no longer \
         exercise the intended crash window",
    );
    {
        let mut guard = sanity_cell.lock();
        let _ = guard.take();
    }
}
