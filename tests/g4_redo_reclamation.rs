//! Tests for F-G4-003: redo reclamation goes through
//! `compact_prefix_through` (and / or `reset`). The misleading
//! `advance_checkpoint` method, which only mutated an in-memory
//! `checkpoint_seq` and reclaimed nothing, has been removed entirely.

use std::sync::Arc;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::TxKey;
use teraslab::redo::{RedoLog, RedoOp};

fn key(n: u8) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0] = n;
    TxKey { txid }
}

/// `compact_prefix_through` is the live reclamation path. It must move
/// the on-disk write position so an empty entries region implies no
/// recoverable entries before the fence sequence.
#[test]
fn compact_prefix_through_actually_reclaims() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let mut log = RedoLog::open(dev, 0, 1024 * 1024).unwrap();

    for i in 1..=10u8 {
        log.append_and_flush(RedoOp::Freeze {
            tx_key: key(i),
            offset: 0,
        })
        .unwrap();
    }

    let pos_before = log.write_position();
    assert!(pos_before > 0, "writes must advance position");

    // Compact through the highest sequence — drains all entries.
    let last_seq = log.current_sequence().saturating_sub(1);
    log.compact_prefix_through(last_seq).unwrap();

    // Reclamation reclaims essentially the entire region: the post-
    // compact write_pos is at most one alignment block (the F-G4-004
    // append-only invariant rounds to the next aligned offset), but
    // it must be much less than pos_before.
    assert!(
        log.write_position() < pos_before,
        "F-G4-003: compact_prefix_through reclaims; write_pos must drop \
         (before={pos_before}, after={})",
        log.write_position()
    );
    assert_eq!(
        log.read_from_sequence(1).unwrap().len(),
        0,
        "no entries should remain after compact-to-empty"
    );
}
