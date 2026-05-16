//! Tests for the F-G4-001 redo-log header (persisted next_sequence).
//!
//! The header was introduced to keep `next_sequence` durable across
//! restarts even after `compact_prefix_through` empties the entries
//! region. Prior to F-G4-001 a restart in this state silently reseeded
//! `next_sequence` to 1, reusing sequence numbers and breaking the
//! replication watermark.

use std::sync::Arc;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::TxKey;
use teraslab::redo::{RedoLog, RedoOp};

fn key(n: u8) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0] = n;
    TxKey { txid }
}

/// After compaction empties the entries region, reopening the log must
/// preserve `next_sequence` — not reseed it from 1.
#[test]
fn next_sequence_survives_compact_to_empty_across_reopen() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());

    let mut log = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
    for i in 1..=5u8 {
        log.append_and_flush(RedoOp::Freeze {
            tx_key: key(i),
            offset: 0,
        })
        .unwrap();
    }
    let seq_before = log.current_sequence();
    assert_eq!(seq_before, 6, "5 entries + 1 = next sequence is 6");

    // Compact through the highest sequence — empties the entries region.
    log.compact_prefix_through(seq_before - 1).unwrap();
    assert_eq!(
        log.read_from_sequence(1).unwrap().len(),
        0,
        "compaction must drain the entries region"
    );

    // Reopen — `next_sequence` must NOT roll back.
    drop(log);
    let reopened = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
    assert_eq!(
        reopened.current_sequence(),
        seq_before,
        "F-G4-001: next_sequence must survive compact-to-empty across reopen"
    );
}

/// Reset() (the existing path that zeroes the entries region) must also
/// persist `next_sequence` so the same restart-reseeding bug cannot
/// happen via the reset path.
#[test]
fn next_sequence_survives_reset_across_reopen() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());

    let mut log = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
    for i in 1..=3u8 {
        log.append_and_flush(RedoOp::Freeze {
            tx_key: key(i),
            offset: 0,
        })
        .unwrap();
    }
    let seq_before = log.current_sequence();
    log.reset().unwrap();
    drop(log);

    let reopened = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
    assert_eq!(
        reopened.current_sequence(),
        seq_before,
        "F-G4-001: reset must not roll back next_sequence"
    );
}

/// The header carries a magic byte string. If the on-disk magic does
/// not match the current binary's expected magic the open must fail
/// with a clear error rather than silently misparsing.
#[test]
fn open_rejects_foreign_header_magic() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());

    // Write a foreign magic into the header block.
    use teraslab::device::AlignedBuf;
    let align = dev.alignment();
    let mut buf = AlignedBuf::new(align, align);
    buf[..8].copy_from_slice(b"NOTSLABx");
    dev.pwrite_all_at(&buf, 0).unwrap();
    dev.sync().unwrap();

    match RedoLog::open(dev, 0, 1024 * 1024) {
        Err(teraslab::redo::RedoError::HeaderMagicMismatch { .. }) => {}
        Ok(_) => panic!("expected HeaderMagicMismatch, got Ok"),
        Err(other) => panic!("expected HeaderMagicMismatch, got {other:?}"),
    }
}
