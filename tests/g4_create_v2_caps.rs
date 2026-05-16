//! Tests for F-G4-006: CreateV2 redo entries cap `parents_count` and
//! `record_bytes.len()` at decode so a corrupt-but-CRC-valid entry
//! cannot inflate startup memory with a fabricated parents list or
//! oversized record slab.

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
fn create_v2_with_too_many_parents_is_rejected_on_reopen() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
    let mut log = RedoLog::open(dev.clone(), 0, 8 * 1024 * 1024).unwrap();

    // Append a CreateV2 with parents far above the F-G4-006 cap of 64.
    // The serializer accepts arbitrary lengths (encoder-side trusts
    // callers); the decoder enforces the cap at startup scan.
    let bad_parents: Vec<[u8; 32]> = (0..200u16).map(|i| {
        let mut p = [0u8; 32];
        p[0] = i as u8;
        p[1] = (i >> 8) as u8;
        p
    }).collect();

    log.append_and_flush(RedoOp::CreateV2 {
        tx_key: key(0xAA),
        record_offset: 4096,
        utxo_count: 1,
        is_conflicting: false,
        record_bytes: vec![0u8; 256],
        parent_txids: bad_parents,
    })
    .unwrap();

    // Drop the in-memory cache and reopen — scan-from-disk must drop
    // the offending entry (the decoder returns None for parents over
    // the cap), which terminates the scan at that entry. Critical
    // observation: the entry must NOT be present in `recover()`.
    drop(log);
    let reopened = RedoLog::open(dev, 0, 8 * 1024 * 1024).unwrap();
    let recovered = reopened.recover().unwrap();
    assert!(
        recovered.is_empty(),
        "F-G4-006: a CreateV2 with parents_count over MAX_CREATE_V2_PARENTS \
         must be skipped on reopen; got {} entries",
        recovered.len(),
    );
}

#[test]
fn create_v2_within_caps_round_trips() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
    let mut log = RedoLog::open(dev.clone(), 0, 8 * 1024 * 1024).unwrap();

    // A CreateV2 with parents_count <= cap and small record_bytes
    // must round-trip across reopen.
    let parents: Vec<[u8; 32]> = (0..8u8)
        .map(|i| {
            let mut p = [0u8; 32];
            p[0] = i;
            p
        })
        .collect();

    log.append_and_flush(RedoOp::CreateV2 {
        tx_key: key(0xBB),
        record_offset: 4096,
        utxo_count: 1,
        is_conflicting: false,
        record_bytes: vec![0u8; 512],
        parent_txids: parents.clone(),
    })
    .unwrap();

    drop(log);
    let reopened = RedoLog::open(dev, 0, 8 * 1024 * 1024).unwrap();
    let recovered = reopened.recover().unwrap();
    assert_eq!(recovered.len(), 1, "in-cap CreateV2 must survive reopen");

    match &recovered[0].op {
        RedoOp::CreateV2 { parent_txids, .. } => {
            assert_eq!(parent_txids.len(), 8);
        }
        other => panic!("expected CreateV2, got {other:?}"),
    }
}
