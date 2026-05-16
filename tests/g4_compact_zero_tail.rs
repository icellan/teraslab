//! Tests for F-G4-012: after compact_prefix_through writes the retained
//! entries, it also writes one extra aligned block of zeros at the new
//! tail so a subsequent scan does not re-discover stale entries left
//! over from before compaction.

use std::sync::Arc;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::TxKey;
use teraslab::redo::{RedoLog, RedoOp};

fn key(b: u8) -> TxKey {
    let mut t = [0u8; 32];
    t[0] = b;
    TxKey { txid: t }
}

#[test]
fn compaction_does_not_resurrect_old_entries_on_reopen() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());

    let mut log = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
    for i in 1..=10u8 {
        log.append_and_flush(RedoOp::Freeze {
            tx_key: key(i),
            offset: 0,
        })
        .unwrap();
    }
    let total_before = log.current_sequence();

    // Compact through sequence 5 → retain entries 6..=10. The retained
    // serialized bytes are smaller than what was previously on disk;
    // without the zero-tail block the post-write region would still
    // contain the parseable tail of the old entries.
    log.compact_prefix_through(5).unwrap();
    drop(log);

    let reopened = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
    assert_eq!(
        reopened.current_sequence(),
        total_before,
        "F-G4-001 header preserves next_sequence across compaction",
    );

    let entries = reopened.read_from_sequence(1).unwrap();
    // Post-compaction view should only show seq 6..=10. Stale entries
    // 1..=5 must NOT reappear.
    let seqs: Vec<u64> = entries.iter().map(|e| e.sequence).collect();
    assert_eq!(
        seqs,
        vec![6, 7, 8, 9, 10],
        "F-G4-012: stale pre-compaction entries must not resurrect; got {seqs:?}",
    );
}
