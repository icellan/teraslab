//! F-G9-001 regression test.
//!
//! When a record carries [`TxFlags::EXTERNAL`] but the matching blob is absent
//! from the blob store (raced/GC'd/manually removed/upload incomplete),
//! [`StorageManager::read_cold_data`] must surface a
//! [`StorageError::ColdDataNotFound`] error rather than silently returning an
//! empty `ColdData { inputs: [], outputs: [], inpoints: [] }`.
//!
//! Pre-fix the path collapsed "blob missing" and "tx had no cold data" into
//! the same observable behaviour — a hard correctness issue for validation,
//! SPV proof generation, and audit tooling that all assume an `EXTERNAL`
//! record carries cold data.

use std::sync::Arc;
use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::io;
use teraslab::record::{TxFlags, TxMetadata, UtxoSlot};
use teraslab::storage::blobstore::MemoryBlobStore;
use teraslab::storage::manager::{StorageError, StorageManager};

#[test]
fn external_record_with_missing_blob_returns_cold_data_not_found() {
    let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let blob = Arc::new(MemoryBlobStore::new());
    let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
    let mgr = StorageManager::new(
        dev.clone() as Arc<dyn BlockDevice>,
        SlotAllocator::new(dev.clone()).unwrap(),
        blob.clone(),
    );

    // Reserve some space and stamp a record flagged EXTERNAL but never upload
    // the matching blob. Pre-fix `read_cold_data` returned an empty `ColdData`
    // here, silently hiding the integrity violation.
    let utxo_count = 1u32;
    let total = TxMetadata::record_size_for(utxo_count);
    let offset = alloc.allocate(total).unwrap();
    let mut tx_id = [0u8; 32];
    tx_id[0] = 0xDE;
    tx_id[1] = 0xAD;
    let mut meta = TxMetadata::new(utxo_count);
    meta.tx_id = tx_id;
    meta.flags = TxFlags::EXTERNAL;
    let slots: Vec<UtxoSlot> = (0..utxo_count)
        .map(|_| UtxoSlot::new_unspent([0u8; 32]))
        .collect();
    io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

    let err = mgr
        .read_cold_data(offset, utxo_count, &meta)
        .expect_err("missing blob on EXTERNAL record must be an error, not empty ColdData");

    match err {
        StorageError::ColdDataNotFound { ref key } => {
            assert!(
                key.starts_with("dead"),
                "ColdDataNotFound.key should be the lowercase-hex txid, got {key}"
            );
        }
        other => panic!("expected ColdDataNotFound, got {other:?}"),
    }
}
