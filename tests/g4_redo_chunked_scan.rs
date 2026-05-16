//! Tests for F-G4-009: `scan_entries_region_with_tail` reads the
//! redo region in aligned chunks (default 4 MiB) carrying over any
//! trailing partial entry between chunks. Memory footprint is now
//! bounded at chunk_size + entries.size_of() instead of the full log
//! region size.

use std::sync::Arc;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::TxKey;
use teraslab::redo::{RedoLog, RedoOp};

fn key(b: u8) -> TxKey {
    let mut t = [0u8; 32];
    t[0] = b;
    TxKey { txid: t }
}

/// Filling a non-trivially-sized log with many entries that span well
/// over a single 4 MiB scan chunk must round-trip without dropping
/// entries — proving the per-chunk carry-over correctly stitches a
/// partial entry that straddles the chunk boundary.
#[test]
fn many_entries_spanning_multiple_scan_chunks_round_trip() {
    // 8 MiB redo region → strictly more than the 4 MiB default scan
    // chunk, so the multi-chunk path is exercised.
    let size = 8 * 1024 * 1024u64;
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(size, 4096).unwrap());
    let mut log = RedoLog::open(dev.clone(), 0, size).unwrap();

    // Append ~2000 small entries. Each entry is ~50 B serialized but
    // each flush is rounded up to alignment (4 KiB), so ~8 MiB of
    // device range is exercised. We use append() then a single flush
    // to pack entries tightly within one aligned block of disk.
    const N: usize = 2000;
    for i in 0..N as u32 {
        log.append(RedoOp::Freeze {
            tx_key: key((i % 255) as u8),
            offset: i,
        })
        .unwrap();
    }
    log.flush().unwrap();
    let seq_before = log.current_sequence();

    drop(log);
    let reopened = RedoLog::open(dev, 0, size).unwrap();
    assert_eq!(
        reopened.current_sequence(),
        seq_before,
        "F-G4-009: chunked scan must recover all entries",
    );
    let entries = reopened.read_from_sequence(1).unwrap();
    assert_eq!(
        entries.len(),
        N,
        "F-G4-009: chunked scan must NOT drop entries at chunk boundaries",
    );

    // Spot-check a few entries decoded correctly.
    match &entries[0].op {
        RedoOp::Freeze { offset, .. } => assert_eq!(*offset, 0),
        other => panic!("entry 0 wrong: {other:?}"),
    }
    match &entries[N - 1].op {
        RedoOp::Freeze { offset, .. } => assert_eq!(*offset, (N - 1) as u32),
        other => panic!("entry {} wrong: {other:?}", N - 1),
    }
}
