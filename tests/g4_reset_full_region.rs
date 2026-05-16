//! Tests for F-G4-013: `RedoLog::reset()` zeroes the entire entries
//! region (not just the first aligned block) so a stale entry left
//! past the first block from a previous run cannot be re-discovered
//! by `scan_entries_region_with_tail` after a reopen.

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
fn reset_then_reopen_sees_no_entries() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());

    let mut log = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
    // Push enough entries that the on-disk content spans multiple
    // aligned blocks (each append_and_flush rounds to an alignment
    // boundary).
    for i in 1..=30u8 {
        log.append_and_flush(RedoOp::Freeze {
            tx_key: key(i),
            offset: 0,
        })
        .unwrap();
    }

    log.reset().unwrap();
    drop(log);

    let reopened = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
    let recovered = reopened.read_from_sequence(1).unwrap();
    assert!(
        recovered.is_empty(),
        "F-G4-013: reset must zero the full entries region; got {} entries",
        recovered.len(),
    );
}
