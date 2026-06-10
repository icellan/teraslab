//! Crash-boundary tests for the WAL-first durability contract (gap #2 part 5).
//!
//! Each test simulates one of the four failure boundaries enumerated in
//! `docs/TERANODE_PRODUCTION_READINESS_GAPS.md` and asserts the production
//! contract by inspecting on-device state, the index, and the
//! replication-intent tracker after recovery.
//!
//! Boundaries exercised:
//!
//! 1. **Before redo fsync** — kill before any WAL flush. Assert: no record
//!    visible, no index entry, no metadata change.
//! 2. **After redo fsync, before record write** — replay reconstructs the
//!    full record byte-for-byte from the new `RedoOp::CreateV2` entry.
//! 3. **After record write, before replication** — replication is
//!    independent of local commit; local state is fully consistent and
//!    survives unchanged.
//! 4. **After replication, before intent clear** — the persistent
//!    [`ReplicationIntentTracker`] shows the pending range, and a fresh
//!    `commit` (the operation startup performs once it has reconciled the
//!    range with replicas) clears it idempotently.
//!
//! These are deterministic state-injection tests, NOT timing-based, so
//! they remain stable in CI.

use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{PrimaryBackend, TxKey};
use teraslab::io;
use teraslab::record::{METADATA_SIZE, TxFlags, TxMetadata, UTXO_SLOT_SIZE, UtxoSlot};
use teraslab::recovery::{recover, recover_all_with_allocator};
use teraslab::redo::{RedoLog, RedoOp};
use teraslab::replication::durable::{ReplicationIntentRange, ReplicationIntentTracker};

// ---------------------------------------------------------------------------
// Shared scaffolding
// ---------------------------------------------------------------------------

/// Build a fresh scaffold of (data device, redo device, allocator,
/// in-memory primary index, redo log).
fn fresh_state() -> (
    Arc<MemoryDevice>,
    Arc<MemoryDevice>,
    SlotAllocator,
    PrimaryBackend,
    RedoLog,
) {
    let data_dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(data_dev.clone()).unwrap();
    let index = PrimaryBackend::new_in_memory(1000).unwrap();
    let redo = RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, 1024 * 1024).unwrap();
    (data_dev, redo_dev, alloc, index, redo)
}

/// Build the exact byte buffer (metadata header + UTXO slots) that a
/// successful create would write to the device. No alignment padding —
/// the device-write path adds that internally.
fn build_record_bytes(meta: &TxMetadata, slots: &[UtxoSlot]) -> Vec<u8> {
    let mut out = Vec::with_capacity(METADATA_SIZE + slots.len() * UTXO_SLOT_SIZE);
    let mut meta_bytes = [0u8; METADATA_SIZE];
    meta.to_bytes(&mut meta_bytes);
    out.extend_from_slice(&meta_bytes);
    for slot in slots {
        let mut sb = [0u8; UTXO_SLOT_SIZE];
        slot.to_bytes(&mut sb);
        out.extend_from_slice(&sb);
    }
    out
}

/// Construct `(metadata, slots)` for a fresh transaction with the given
/// txid byte and utxo_count.
fn make_record(txid_byte: u8, utxo_count: u32) -> (TxKey, TxMetadata, Vec<UtxoSlot>) {
    let mut txid = [0u8; 32];
    txid[0] = txid_byte;
    let mut meta = TxMetadata::new(utxo_count);
    meta.tx_id = txid;
    meta.tx_version = 1;
    meta.fee = 100;
    let base_size = TxMetadata::record_size_for(utxo_count);
    meta.record_size = base_size as u32;
    meta.flags = TxFlags::empty();
    let slots: Vec<UtxoSlot> = (0..utxo_count)
        .map(|i| {
            let mut h = [0u8; 32];
            h[0] = (i + 1) as u8;
            UtxoSlot::new_unspent(h)
        })
        .collect();
    (TxKey { txid }, meta, slots)
}

// ---------------------------------------------------------------------------
// Boundary 1: before redo fsync
// ---------------------------------------------------------------------------

/// Before the WAL flush completes, the operation has no durability
/// guarantees. After a crash + recovery: no record, no index, no
/// metadata change.
#[test]
fn boundary_before_redo_fsync_leaves_no_state() {
    let (data_dev, _redo_dev, alloc, mut index, redo) = fresh_state();
    let (key, meta, slots) = make_record(0xA1, 2);
    let _record_bytes = build_record_bytes(&meta, &slots);

    // The dispatcher would now `redo.append(...)`. Simulate the crash by
    // skipping both the append and the engine apply: simply drop the
    // log. The test asserts that the fresh state is unchanged.
    drop(redo);
    // Pretend space had been pre-allocated (mirrors `pre_allocate_create`)
    // but the freelist mutation hadn't been WAL-flushed either — fresh
    // alloc means no allocation visible.
    let _ = alloc;

    // No replay entries to apply.
    let stats = recover(&*data_dev as &dyn BlockDevice, &fresh_redo(), &mut index).unwrap();
    assert_eq!(stats.entries_replayed, 0, "no replays expected");
    assert_eq!(stats.entries_skipped, 0);
    assert_eq!(stats.entries_failed, 0);

    // Index has nothing for the txid.
    assert!(
        index.lookup(&key).is_none(),
        "no index entry must exist before redo fsync",
    );

    // Device area: the record_offset would have been the next allocator
    // offset. Read the metadata at offset 0 of the data region — it
    // should not have a valid tx_id (fresh devices return zeros which
    // do not match a real metadata magic).
    let read_back = io::read_metadata(&*data_dev as &dyn BlockDevice, 0);
    assert!(
        read_back.is_err() || read_back.map(|m| { m.tx_id } == [0u8; 32]).unwrap_or(false),
        "no record metadata should be visible at offset 0",
    );
}

fn fresh_redo() -> RedoLog {
    let redo_dev = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    RedoLog::open(redo_dev as Arc<dyn BlockDevice>, 0, 1024 * 1024).unwrap()
}

// ---------------------------------------------------------------------------
// Boundary 2: after redo fsync, before record write
// ---------------------------------------------------------------------------

/// After the WAL fsync but BEFORE the engine write, the redo log has the
/// `CreateV2` entry but the device area is still untouched. Recovery
/// must reconstruct the record byte-for-byte from the redo payload.
#[test]
fn boundary_after_redo_fsync_before_record_write_reconstructs_full_record() {
    let (data_dev, _redo_dev, mut alloc, mut index, mut redo) = fresh_state();
    let (key, meta, slots) = make_record(0xB2, 4);
    let record_bytes = build_record_bytes(&meta, &slots);
    let utxo_count: u32 = slots.len() as u32;

    // Pre-allocate the region (mirrors the dispatch path acquiring
    // `record_offset` from `engine.pre_allocate_create`).
    let base_size = TxMetadata::record_size_for(utxo_count);
    let record_offset = alloc.allocate(base_size).unwrap();

    // Append + fsync the WAL entry. CRASH happens HERE — engine apply
    // never runs.
    redo.append_and_flush(RedoOp::CreateV2 {
        tx_key: key,
        record_offset,
        utxo_count,
        is_conflicting: false,
        record_bytes: record_bytes.clone(),
        parent_txids: Vec::new(),
    })
    .unwrap();

    // Recovery rebuilds the full record + registers the index.
    let stats = recover(&*data_dev as &dyn BlockDevice, &redo, &mut index).unwrap();
    assert_eq!(stats.entries_replayed, 1);
    assert_eq!(stats.entries_failed, 0);

    let entry = index.lookup(&key).expect("CreateV2 replay registers index");
    assert_eq!(entry.record_offset, record_offset);
    assert_eq!(entry.utxo_count, utxo_count);

    // On-device bytes must match the original.
    let recovered_meta = io::read_metadata(&*data_dev as &dyn BlockDevice, record_offset).unwrap();
    assert_eq!({ recovered_meta.tx_version }, 1);
    assert_eq!({ recovered_meta.fee }, 100);
    assert_eq!({ recovered_meta.utxo_count }, utxo_count);
    for (i, original) in slots.iter().enumerate() {
        let on_device =
            io::read_utxo_slot(&*data_dev as &dyn BlockDevice, record_offset, i as u32).unwrap();
        assert_eq!(on_device.hash, original.hash, "slot {i} hash matches");
        assert!(on_device.is_unspent(), "slot {i} unspent after replay");
    }
}

// ---------------------------------------------------------------------------
// Boundary 3: after record write, before replication
// ---------------------------------------------------------------------------

/// Replication is independent of local commit. After the record bytes
/// are on the device and the redo entry is fsynced, local state is
/// fully consistent — recovery should observe the steady-state record
/// regardless of whether replication subsequently fired.
#[test]
fn boundary_after_record_write_before_replication_local_state_consistent() {
    let (data_dev, _redo_dev, mut alloc, mut index, mut redo) = fresh_state();
    let (key, meta, slots) = make_record(0xC3, 3);
    let record_bytes = build_record_bytes(&meta, &slots);
    let utxo_count: u32 = slots.len() as u32;
    let base_size = TxMetadata::record_size_for(utxo_count);
    let record_offset = alloc.allocate(base_size).unwrap();

    // Step 1: WAL fsync.
    redo.append_and_flush(RedoOp::CreateV2 {
        tx_key: key,
        record_offset,
        utxo_count,
        is_conflicting: false,
        record_bytes: record_bytes.clone(),
        parent_txids: Vec::new(),
    })
    .unwrap();

    // Step 2: engine write to device. Use the same `pwrite_all_at`
    // discipline the engine uses by writing into an AlignedBuf.
    use teraslab::device::AlignedBuf;
    let align = data_dev.alignment();
    let aligned_len = record_bytes.len().div_ceil(align) * align;
    let mut buf = AlignedBuf::new(aligned_len, align);
    buf[..record_bytes.len()].copy_from_slice(&record_bytes);
    data_dev.pwrite_all_at(&buf, record_offset).unwrap();

    // CRASH happens HERE — replication didn't fire. Recovery sees both
    // the redo entry AND the on-device bytes; replay must observe the
    // record was already there, register the index, and converge to a
    // consistent state.
    let stats = recover(&*data_dev as &dyn BlockDevice, &redo, &mut index).unwrap();
    assert_eq!(
        stats.entries_replayed, 1,
        "CreateV2 still applies (idempotent)"
    );
    let entry = index.lookup(&key).expect("index registered");
    assert_eq!(entry.record_offset, record_offset);

    // The on-device record is byte-equal to what was written before the
    // crash — recovery did not corrupt it.
    let recovered_meta = io::read_metadata(&*data_dev as &dyn BlockDevice, record_offset).unwrap();
    assert_eq!({ recovered_meta.tx_id }, key.txid);
    for (i, original) in slots.iter().enumerate() {
        let on_device =
            io::read_utxo_slot(&*data_dev as &dyn BlockDevice, record_offset, i as u32).unwrap();
        assert_eq!(on_device.hash, original.hash);
    }
}

// ---------------------------------------------------------------------------
// Boundary 4: after replication, before intent clear
// ---------------------------------------------------------------------------

/// The dispatch path persists a `ReplicationIntentTracker` range BEFORE
/// fanning out to replicas and `commit`s the range only after the ACK
/// policy is satisfied. A crash AFTER replication ACKs but BEFORE the
/// intent commit must leave a recoverable record so the next startup
/// can clear the intent idempotently.
#[test]
fn boundary_after_replication_before_intent_clear_is_idempotent() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("repl-intent");

    // Open the persistent tracker and record a pending range — this
    // mirrors what the dispatch path does BEFORE replicating.
    let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();
    let first_seq = 100u64;
    let last_seq = 105u64;
    tracker.begin(first_seq, last_seq).unwrap();

    // Drop the tracker to simulate a process crash WITHOUT calling
    // `commit`. The on-disk file still holds the range.
    drop(tracker);

    // Reload from disk — the gap #2 contract requires the pending range
    // to survive across restart so the next startup can re-do or clear
    // it once the replica responses are reconciled.
    let reopened = ReplicationIntentTracker::load(path.clone()).unwrap();
    let pending = reopened.pending();
    assert_eq!(pending.len(), 1, "one pending range survives crash");
    assert_eq!(pending[0].first_sequence, first_seq);
    assert_eq!(pending[0].last_sequence, last_seq);

    // Idempotent clear: calling `commit` once removes it. Calling it
    // again is a no-op (range already absent), simulating multiple
    // recovery passes.
    reopened.commit(first_seq, last_seq).unwrap();
    assert!(
        reopened.pending().is_empty(),
        "commit clears the pending range",
    );
    reopened.commit(first_seq, last_seq).unwrap();
    assert!(reopened.pending().is_empty(), "second commit is idempotent",);

    // Commit persistence is intentionally coalesced. A crash before the
    // coalesced commit is flushed may reload the stale range, which is safe
    // because startup recovery replays/clears it idempotently.
    let stale_reopen = ReplicationIntentTracker::load(path.clone()).unwrap();
    assert_eq!(
        stale_reopen.pending(),
        vec![ReplicationIntentRange {
            first_sequence: first_seq,
            last_sequence: last_seq,
        }],
        "unflushed commit remains recoverable on disk",
    );
    drop(stale_reopen);

    // Clean shutdown flushes the coalesced clear to disk.
    reopened.flush().unwrap();
    drop(reopened);
    let reopened_again = ReplicationIntentTracker::load(path).unwrap();
    assert!(
        reopened_again.pending().is_empty(),
        "pending range is cleared on disk after flush",
    );
}

// ---------------------------------------------------------------------------
// Sanity: full-pipeline recovery still works with allocator threading.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// SetMined / MarkOnLongestChain crash boundaries (2026-05-29 audit,
// coverage-matrix hole #3). Both ops mutate the DAH/unmined secondary
// state that gates pruning — a non-idempotent replay here resurrects or
// vanishes a UTXO. Each test replays once (crash after WAL fsync,
// before the metadata write) and then runs a SECOND recovery pass over
// the same device + log (restart after a crash mid-recovery) asserting
// the state converges instead of double-applying.
// ---------------------------------------------------------------------------

/// Crash after the SetMined WAL fsync but before the metadata write:
/// replay must reconstruct the block entry; a second recovery pass must
/// observe the duplicate block_id and skip without a second generation
/// bump or a duplicate entry.
#[test]
fn boundary_set_mined_after_wal_replays_and_second_pass_is_idempotent() {
    let (data_dev, _redo_dev, mut alloc, mut index, mut redo) = fresh_state();
    let (key, meta, slots) = make_record(0xD4, 2);
    let record_bytes = build_record_bytes(&meta, &slots);
    let utxo_count: u32 = slots.len() as u32;
    let record_offset = alloc
        .allocate(TxMetadata::record_size_for(utxo_count))
        .unwrap();

    // WAL carries the create AND the set_mined; the device has neither
    // (crash before any device write).
    redo.append_and_flush(RedoOp::CreateV2 {
        tx_key: key,
        record_offset,
        utxo_count,
        is_conflicting: false,
        record_bytes,
        parent_txids: Vec::new(),
    })
    .unwrap();
    redo.append_and_flush(RedoOp::SetMined {
        tx_key: key,
        block_id: 42,
        block_height: 800_000,
        subtree_idx: 7,
        unset: false,
    })
    .unwrap();

    let mut dah = teraslab::index::DahBackend::new_in_memory();
    let mut unmined = teraslab::index::UnminedBackend::new_in_memory();
    let stats = recover_all_with_allocator(
        &*data_dev as &dyn BlockDevice,
        &redo,
        &mut index,
        &mut dah,
        &mut unmined,
        Some(&mut alloc),
    )
    .unwrap();
    assert_eq!(stats.entries_replayed, 2, "create + set_mined both apply");
    assert_eq!(stats.entries_failed, 0);

    let m = io::read_metadata(&*data_dev as &dyn BlockDevice, record_offset).unwrap();
    assert_eq!({ m.block_entry_count }, 1);
    let be = { m.block_entries_inline[0] };
    assert_eq!({ be.block_id }, 42);
    assert_eq!({ be.block_height }, 800_000);
    assert_eq!({ be.subtree_idx }, 7);
    let gen_after_first = { m.generation };

    // Second recovery pass (restart after crash mid-recovery): fresh
    // index/secondaries, same device + WAL. SetMined replay must see
    // the duplicate block_id and skip — same entry count, no further
    // generation bump.
    let mut index2 = PrimaryBackend::new_in_memory(1000).unwrap();
    let mut dah2 = teraslab::index::DahBackend::new_in_memory();
    let mut unmined2 = teraslab::index::UnminedBackend::new_in_memory();
    let stats2 = recover_all_with_allocator(
        &*data_dev as &dyn BlockDevice,
        &redo,
        &mut index2,
        &mut dah2,
        &mut unmined2,
        Some(&mut alloc),
    )
    .unwrap();
    assert_eq!(stats2.entries_failed, 0);

    let m2 = io::read_metadata(&*data_dev as &dyn BlockDevice, record_offset).unwrap();
    assert_eq!(
        { m2.block_entry_count },
        1,
        "duplicate block_id must be skipped, not appended twice"
    );
    assert_eq!(
        { m2.generation },
        gen_after_first,
        "second replay pass must not bump generation again"
    );
}

/// Crash after the MarkOnLongestChain(off) WAL fsync but before the
/// metadata write (a reorg knocked the tx off the longest chain):
/// replay must set `unmined_since` and the reconciled unmined secondary
/// must track the record; the generation token (H7) makes a second
/// pass a no-op.
#[test]
fn boundary_mark_longest_chain_off_after_wal_replays_and_is_idempotent() {
    let (data_dev, _redo_dev, mut alloc, mut index, mut redo) = fresh_state();
    let (key, meta, slots) = make_record(0xD5, 1);
    let record_bytes = build_record_bytes(&meta, &slots);
    let utxo_count: u32 = slots.len() as u32;
    let record_offset = alloc
        .allocate(TxMetadata::record_size_for(utxo_count))
        .unwrap();

    redo.append_and_flush(RedoOp::CreateV2 {
        tx_key: key,
        record_offset,
        utxo_count,
        is_conflicting: false,
        record_bytes,
        parent_txids: Vec::new(),
    })
    .unwrap();
    redo.append_and_flush(RedoOp::MarkOnLongestChain {
        tx_key: key,
        on_longest_chain: false,
        current_block_height: 850_000,
        block_height_retention: 288,
        generation: 1,
    })
    .unwrap();

    let mut dah = teraslab::index::DahBackend::new_in_memory();
    let mut unmined = teraslab::index::UnminedBackend::new_in_memory();
    let stats = recover_all_with_allocator(
        &*data_dev as &dyn BlockDevice,
        &redo,
        &mut index,
        &mut dah,
        &mut unmined,
        Some(&mut alloc),
    )
    .unwrap();
    assert_eq!(stats.entries_failed, 0);

    let m = io::read_metadata(&*data_dev as &dyn BlockDevice, record_offset).unwrap();
    assert_eq!({ m.unmined_since }, 850_000, "off-chain sets unmined_since");
    assert_eq!({ m.generation }, 1, "replay syncs the generation token");
    assert_eq!(
        unmined.len(),
        1,
        "reconciled unmined secondary must track the off-chain record"
    );

    // Second pass: generation 1 is at-or-ahead of the token — replay
    // must skip, leaving unmined_since/generation/secondary unchanged.
    let mut index2 = PrimaryBackend::new_in_memory(1000).unwrap();
    let mut dah2 = teraslab::index::DahBackend::new_in_memory();
    let mut unmined2 = teraslab::index::UnminedBackend::new_in_memory();
    let stats2 = recover_all_with_allocator(
        &*data_dev as &dyn BlockDevice,
        &redo,
        &mut index2,
        &mut dah2,
        &mut unmined2,
        Some(&mut alloc),
    )
    .unwrap();
    assert_eq!(stats2.entries_failed, 0);
    let m2 = io::read_metadata(&*data_dev as &dyn BlockDevice, record_offset).unwrap();
    assert_eq!({ m2.unmined_since }, 850_000);
    assert_eq!({ m2.generation }, 1);
    assert_eq!(unmined2.len(), 1);
}

/// The inverse reorg direction: a record already off the longest chain
/// (unmined_since set in its created bytes) is marked back ON the
/// longest chain. Replay must clear `unmined_since`; the reconciled
/// unmined secondary must drop the record; second pass is a no-op.
#[test]
fn boundary_mark_longest_chain_on_clears_unmined_and_is_idempotent() {
    let (data_dev, _redo_dev, mut alloc, mut index, mut redo) = fresh_state();
    let (key, mut meta, slots) = make_record(0xD6, 1);
    // The record was off-chain at create time.
    meta.unmined_since = 850_000;
    let record_bytes = build_record_bytes(&meta, &slots);
    let utxo_count: u32 = slots.len() as u32;
    let record_offset = alloc
        .allocate(TxMetadata::record_size_for(utxo_count))
        .unwrap();

    redo.append_and_flush(RedoOp::CreateV2 {
        tx_key: key,
        record_offset,
        utxo_count,
        is_conflicting: false,
        record_bytes,
        parent_txids: Vec::new(),
    })
    .unwrap();
    redo.append_and_flush(RedoOp::MarkOnLongestChain {
        tx_key: key,
        on_longest_chain: true,
        current_block_height: 860_000,
        block_height_retention: 288,
        generation: 1,
    })
    .unwrap();

    let mut dah = teraslab::index::DahBackend::new_in_memory();
    let mut unmined = teraslab::index::UnminedBackend::new_in_memory();
    let stats = recover_all_with_allocator(
        &*data_dev as &dyn BlockDevice,
        &redo,
        &mut index,
        &mut dah,
        &mut unmined,
        Some(&mut alloc),
    )
    .unwrap();
    assert_eq!(stats.entries_failed, 0);

    let m = io::read_metadata(&*data_dev as &dyn BlockDevice, record_offset).unwrap();
    assert_eq!({ m.unmined_since }, 0, "back-on-chain clears unmined_since");
    assert_eq!({ m.generation }, 1);
    assert!(
        unmined.is_empty(),
        "reconciled unmined secondary must drop the on-chain record"
    );

    let mut index2 = PrimaryBackend::new_in_memory(1000).unwrap();
    let mut dah2 = teraslab::index::DahBackend::new_in_memory();
    let mut unmined2 = teraslab::index::UnminedBackend::new_in_memory();
    let stats2 = recover_all_with_allocator(
        &*data_dev as &dyn BlockDevice,
        &redo,
        &mut index2,
        &mut dah2,
        &mut unmined2,
        Some(&mut alloc),
    )
    .unwrap();
    assert_eq!(stats2.entries_failed, 0);
    let m2 = io::read_metadata(&*data_dev as &dyn BlockDevice, record_offset).unwrap();
    assert_eq!({ m2.unmined_since }, 0);
    assert_eq!({ m2.generation }, 1);
    assert!(unmined2.is_empty());
}

/// A thin smoke test: drive a CreateV2 entry through
/// `recover_all_with_allocator` to make sure the full pipeline (which is
/// what production startup uses) still reconstructs the record
/// correctly. Belt-and-braces for the boundary 2 reconstruction.
#[test]
fn full_pipeline_recovery_reconstructs_create_v2() {
    let (data_dev, _redo_dev, mut alloc, mut index, mut redo) = fresh_state();
    let (key, meta, slots) = make_record(0xE5, 2);
    let record_bytes = build_record_bytes(&meta, &slots);
    let utxo_count: u32 = slots.len() as u32;
    let base_size = TxMetadata::record_size_for(utxo_count);
    let record_offset = alloc.allocate(base_size).unwrap();

    redo.append_and_flush(RedoOp::CreateV2 {
        tx_key: key,
        record_offset,
        utxo_count,
        is_conflicting: false,
        record_bytes,
        parent_txids: Vec::new(),
    })
    .unwrap();

    let mut dah = teraslab::index::DahBackend::new_in_memory();
    let mut unmined = teraslab::index::UnminedBackend::new_in_memory();
    let stats = recover_all_with_allocator(
        &*data_dev as &dyn BlockDevice,
        &redo,
        &mut index,
        &mut dah,
        &mut unmined,
        Some(&mut alloc),
    )
    .unwrap();
    assert_eq!(stats.entries_replayed, 1);
    assert_eq!(stats.entries_failed, 0);
    assert!(
        index.lookup(&key).is_some(),
        "full pipeline must register the index",
    );
}
