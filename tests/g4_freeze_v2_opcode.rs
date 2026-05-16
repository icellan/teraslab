//! Tests for F-G4-008: V2 freeze/unfreeze entries use distinct opcode
//! bytes (OP_FREEZE_V2 / OP_UNFREEZE_V2) rather than overloading the
//! legacy OP_FREEZE / OP_UNFREEZE tags and disambiguating by length.

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
fn freeze_v2_round_trips_via_distinct_opcode() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let mut log = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();

    let hash = [0xABu8; 32];
    log.append_and_flush(RedoOp::FreezeV2 {
        tx_key: key(0x11),
        offset: 7,
        utxo_hash: hash,
    })
    .unwrap();

    drop(log);
    let reopened = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
    let recovered = reopened.recover().unwrap();
    assert_eq!(recovered.len(), 1);
    match &recovered[0].op {
        RedoOp::FreezeV2 {
            tx_key,
            offset,
            utxo_hash,
        } => {
            assert_eq!(tx_key.txid[0], 0x11);
            assert_eq!(*offset, 7);
            assert_eq!(*utxo_hash, hash);
        }
        other => panic!("F-G4-008: expected FreezeV2, got {other:?}"),
    }
}

#[test]
fn unfreeze_v2_round_trips_via_distinct_opcode() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let mut log = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();

    let hash = [0xCDu8; 32];
    log.append_and_flush(RedoOp::UnfreezeV2 {
        tx_key: key(0x22),
        offset: 3,
        utxo_hash: hash,
    })
    .unwrap();

    drop(log);
    let reopened = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
    let recovered = reopened.recover().unwrap();
    assert_eq!(recovered.len(), 1);
    match &recovered[0].op {
        RedoOp::UnfreezeV2 {
            tx_key,
            offset,
            utxo_hash,
        } => {
            assert_eq!(tx_key.txid[0], 0x22);
            assert_eq!(*offset, 3);
            assert_eq!(*utxo_hash, hash);
        }
        other => panic!("F-G4-008: expected UnfreezeV2, got {other:?}"),
    }
}

#[test]
fn legacy_freeze_remains_distinct_from_v2() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let mut log = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();

    log.append_and_flush(RedoOp::Freeze {
        tx_key: key(0x33),
        offset: 5,
    })
    .unwrap();

    drop(log);
    let reopened = RedoLog::open(dev, 0, 1024 * 1024).unwrap();
    let recovered = reopened.recover().unwrap();
    assert_eq!(recovered.len(), 1);
    match &recovered[0].op {
        RedoOp::Freeze { tx_key, offset } => {
            assert_eq!(tx_key.txid[0], 0x33);
            assert_eq!(*offset, 5);
        }
        other => panic!("F-G4-008: legacy Freeze must round-trip as Freeze, not V2; got {other:?}"),
    }
}
