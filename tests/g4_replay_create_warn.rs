//! Tests for F-G4-014: `replay_create` still skips idempotently when
//! the index already contains the key, but surfaces a `tracing::warn!`
//! when the existing entry's `record_offset` or `utxo_count` diverges
//! from the redo entry. The warning helps operators correlate
//! delete+recreate reordering scenarios that crossed the redo log.

use std::sync::Arc;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{PrimaryBackend, ShardedIndex, TxIndexEntry, TxKey};
use teraslab::io;
use teraslab::record::{TxFlags, TxMetadata};
use teraslab::recovery::recover;
use teraslab::redo::{RedoLog, RedoOp};

fn key(b: u8) -> TxKey {
    let mut t = [0u8; 32];
    t[0] = b;
    TxKey { txid: t }
}

#[test]
fn replay_create_skips_when_already_indexed_with_different_offset() {
    let data: Arc<MemoryDevice> = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
    let redo_dev: Arc<MemoryDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());

    // Existing index entry at record_offset 16 KiB; the redo Create
    // entry will reference a different record_offset (4 KiB).
    let index = ShardedIndex::from_single(PrimaryBackend::new_in_memory(64).unwrap());
    let k = key(0x99);
    let utxo_count = 1u32;
    let other_offset = 16 * 1024u64;
    let redo_offset = 4 * 1024u64;

    // Make on-device metadata at redo_offset valid so the redo entry
    // is consistent on its own; it's the existing index entry that
    // points elsewhere.
    let mut meta = TxMetadata::new(utxo_count);
    meta.record_size = TxMetadata::record_size_for(utxo_count) as u32;
    meta.tx_id = k.txid;
    meta.flags = TxFlags::empty();
    io::write_metadata(&*data as &dyn BlockDevice, redo_offset, &meta).unwrap();

    index
        .register(
            k,
            TxIndexEntry {
                device_id: 0,
                record_offset: other_offset,
                utxo_count,
                block_entry_count: 0,
                tx_flags: 0,
                spent_utxos: 0,
                dah_or_preserve: 0,
                unmined_since: 0,
                generation: 0,
            },
        )
        .unwrap();

    let mut log = RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, 1024 * 1024).unwrap();
    log.append_and_flush(RedoOp::ReplicaCreate {
        tx_key: k,
        device_id: 0,
        record_offset: redo_offset,
        utxo_count,
    })
    .unwrap();

    let stats = recover(&*data as &dyn BlockDevice, &log, &index).unwrap();
    assert_eq!(
        stats.entries_replayed, 0,
        "F-G4-014: replay_create with mismatched index must skip, not re-apply",
    );
    assert_eq!(stats.entries_skipped, 1);

    // Index entry must remain pointing at `other_offset` — replay
    // didn't silently rewrite it.
    let e = index.lookup(&k).expect("index entry still present");
    assert_eq!(
        e.record_offset, other_offset,
        "F-G4-014: index entry must not be overwritten by the skipped replay",
    );
}
