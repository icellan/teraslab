//! F-G9-002 regression test.
//!
//! When a record's blob is present and the blob store's internal sidecar
//! check passes, [`StorageManager::read_cold_data`] must still cross-check
//! the recomputed SHA-256 against the durable record-anchored
//! `ExternalRef.content_hash`. Only that record-anchored digest catches a
//! coordinated payload+sidecar swap by an attacker (or operator with
//! filesystem access) who replaces both files with internally consistent
//! impostor data.
//!
//! The pre-fix `read_cold_data` trusted only the sidecar — wire-compatible
//! callers (audit tooling, SPV, prune validation) would silently consume the
//! swapped payload. The spend path at `src/ops/engine.rs:2317` did
//! cross-check, so this fix harmonises all read paths to behave consistently.

use std::sync::Arc;
use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::io;
use teraslab::record::{TxFlags, TxMetadata, UtxoSlot};
use teraslab::storage::blobstore::{BlobStore, MemoryBlobStore};
use teraslab::storage::manager::{StorageError, StorageManager};
use teraslab::storage::tiers::ColdData;

#[test]
fn blob_payload_disagreeing_with_record_anchored_digest_fails() {
    let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let blob = Arc::new(MemoryBlobStore::new());
    let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
    let mgr = StorageManager::new(
        dev.clone() as Arc<dyn BlockDevice>,
        SlotAllocator::new(dev.clone()).unwrap(),
        blob.clone(),
    );

    // Realistic scenario: the create path stamped `content_hash` with the
    // SHA-256 of payload A, but a tampering operator replaced both the
    // payload and the sidecar with payload B. The blob store's own digest
    // verifier (which checks sidecar against payload) passes — only the
    // record-anchored digest catches the swap.
    let utxo_count = 1u32;
    let total = TxMetadata::record_size_for(utxo_count);
    let offset = alloc.allocate(total).unwrap();
    let mut tx_id = [0u8; 32];
    tx_id[0] = 0xBE;
    tx_id[1] = 0xEF;

    let expected_payload_hash = {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(b"the legitimate cold data payload that the record was stamped against");
        let out = h.finalize();
        let mut d = [0u8; 32];
        d.copy_from_slice(&out);
        d
    };
    let mut meta = TxMetadata::new(utxo_count);
    meta.tx_id = tx_id;
    meta.flags = TxFlags::EXTERNAL;
    meta.external_ref.content_hash = expected_payload_hash;
    let slots: Vec<UtxoSlot> = (0..utxo_count)
        .map(|_| UtxoSlot::new_unspent([0u8; 32]))
        .collect();
    io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

    // Put a different (but internally consistent) cold payload into the blob
    // store. The MemoryBlobStore re-computes its sidecar from the payload, so
    // its own digest check will pass — only the record-anchored cross-check
    // catches the swap.
    let other = ColdData {
        inputs: vec![1, 2, 3],
        outputs: vec![4, 5, 6],
        inpoints: vec![7],
    };
    blob.put(&tx_id, &other.serialize()).unwrap();

    let err = mgr
        .read_cold_data(offset, utxo_count, &meta)
        .expect_err("payload not matching record-anchored content_hash must error");

    match err {
        StorageError::ContentHashMismatch {
            ref key,
            expected,
            actual,
        } => {
            assert!(key.starts_with("beef"));
            assert_eq!(expected, expected_payload_hash);
            assert_ne!(actual, expected);
        }
        other => panic!("expected ContentHashMismatch, got {other:?}"),
    }
}

#[test]
fn zero_record_anchored_digest_tolerated_for_legacy_records() {
    // R-048 populated `ExternalRef.content_hash` during create, but legacy
    // records written before that audit may still have the field at zero. The
    // fix explicitly tolerates the all-zero placeholder so we do not break
    // upgrade paths — it just emits a warn.
    let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let blob = Arc::new(MemoryBlobStore::new());
    let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
    let mgr = StorageManager::new(
        dev.clone() as Arc<dyn BlockDevice>,
        SlotAllocator::new(dev.clone()).unwrap(),
        blob.clone(),
    );

    let utxo_count = 1u32;
    let total = TxMetadata::record_size_for(utxo_count);
    let offset = alloc.allocate(total).unwrap();
    let mut tx_id = [0u8; 32];
    tx_id[0] = 0x10;
    let mut meta = TxMetadata::new(utxo_count);
    meta.tx_id = tx_id;
    meta.flags = TxFlags::EXTERNAL;
    // content_hash remains all-zero (legacy record).
    let slots: Vec<UtxoSlot> = (0..utxo_count)
        .map(|_| UtxoSlot::new_unspent([0u8; 32]))
        .collect();
    io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

    let cold = ColdData {
        inputs: vec![1, 2, 3],
        outputs: vec![4],
        inpoints: vec![],
    };
    blob.put(&tx_id, &cold.serialize()).unwrap();

    let read = mgr
        .read_cold_data(offset, utxo_count, &meta)
        .expect("legacy zero-digest record must read back without error");
    assert_eq!(read, cold);
}
