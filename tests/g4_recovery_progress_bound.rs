//! Tests for F-G4-010: the RecoveryProgress marker's `through_sequence`
//! is bounded against the maximum entry sequence actually seen in the
//! log. A corrupt-but-CRC-valid marker with `through_sequence =
//! u64::MAX` cannot suppress all post-marker entries from replay.

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
fn corrupt_recovery_progress_does_not_mask_post_marker_entries() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let mut log = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();

    // Real entries first.
    for i in 1..=3u8 {
        log.append_and_flush(RedoOp::Freeze {
            tx_key: key(i),
            offset: 0,
        })
        .unwrap();
    }

    // Inject a wildly-large RecoveryProgress marker — the simulated
    // corruption case (a software bug elsewhere writes a bogus value).
    log.append_and_flush(RedoOp::RecoveryProgress {
        through_sequence: u64::MAX,
    })
    .unwrap();

    // More real entries after the marker.
    for i in 4..=6u8 {
        log.append_and_flush(RedoOp::Freeze {
            tx_key: key(i),
            offset: 0,
        })
        .unwrap();
    }

    // recover() must surface the post-marker entries — F-G4-010 caps
    // progress_through at the highest entry sequence seen, so a
    // u64::MAX marker has no suppression power.
    let recovered = log.recover().unwrap();
    let txid_bytes: Vec<u8> = recovered
        .iter()
        .filter_map(|e| match &e.op {
            RedoOp::Freeze { tx_key, .. } => Some(tx_key.txid[0]),
            _ => None,
        })
        .collect();
    assert!(
        txid_bytes.contains(&4) && txid_bytes.contains(&5) && txid_bytes.contains(&6),
        "F-G4-010: post-marker entries must NOT be masked by a u64::MAX through_sequence; got {txid_bytes:?}",
    );
}
