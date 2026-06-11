//! F-G9-001 / F-IJ-001 regression test.
//!
//! When a record carries [`teraslab::record::TxFlags::EXTERNAL`] but the
//! matching blob is absent from the blob store (raced / GC'd / manually
//! removed / upload incomplete), the PRODUCTION read path
//! [`Engine::read_cold_data`] must surface a typed
//! [`SpendError::BlobNotFound`] error rather than silently returning empty
//! cold data OR a [`SpendError::TxNotFound`] that would tell the caller the
//! transaction never existed.
//!
//! Pre-fix the engine path collapsed "blob missing" into `TxNotFound` (the
//! original F-G9-001 typed error landed only in the now-deleted, production-
//! dead `StorageManager`). That masked a data-integrity violation for an
//! existing, index-registered transaction — a hard correctness issue for
//! validation, SPV proof generation, and audit tooling that all assume an
//! `EXTERNAL` record carries cold data.

use std::sync::Arc;
use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::error::SpendError;
use teraslab::record::ExternalRef;
use teraslab::storage::blobstore::MemoryBlobStore;

fn build_engine_with_blob() -> (Engine, Arc<MemoryBlobStore>) {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(1024).unwrap();
    let mut engine = Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(64),
        DahIndex::new(),
        UnminedIndex::new(),
    );
    let blob = Arc::new(MemoryBlobStore::new());
    engine.set_blob_store(blob.clone());
    (engine, blob)
}

/// Register an EXTERNAL record in the engine via the production create path,
/// stamping the supplied `external_ref`. The blob itself is NOT uploaded — the
/// caller controls the blob store contents.
fn create_external_record(engine: &Engine, tx_id: [u8; 32], external_ref: ExternalRef) {
    let utxo_hashes: Vec<[u8; 32]> = vec![[0u8; 32]];
    let hashes_ref: &[[u8; 32]] = Box::leak(utxo_hashes.into_boxed_slice());
    let req = CreateRequest {
        tx_id,
        tx_version: 1,
        locktime: 0,
        fee: 0,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        utxo_hashes: hashes_ref,
        inputs: None,
        outputs: None,
        inpoints: None,
        is_external: true,
        created_at: 1_710_000_000_000,
        block_height: 1000,
        mined_block_infos: &[],
        frozen: false,
        conflicting: false,
        locked: false,
        external_ref: Some(external_ref),
        parent_txids: &[],
    };
    engine.create(&req).unwrap();
}

#[test]
fn external_record_with_missing_blob_returns_blob_not_found() {
    let (engine, _blob) = build_engine_with_blob();

    let mut tx_id = [0u8; 32];
    tx_id[0] = 0xDE;
    tx_id[1] = 0xAD;
    let external_ref = ExternalRef {
        store_type: 1,
        content_hash: [0x11; 32],
        total_size: 250,
        input_count: 0,
        output_count: 0,
        inputs_offset: 0,
        outputs_offset: 0,
    };
    create_external_record(&engine, tx_id, external_ref);

    let key = TxKey { txid: tx_id };
    // The record exists in the index, but no blob was ever uploaded.
    assert!(engine.lookup(&key).is_some());

    let err = engine
        .read_cold_data(&key)
        .expect_err("missing blob on EXTERNAL record must be an error, not empty cold data");
    match err {
        SpendError::BlobNotFound { txid } => assert_eq!(txid, tx_id),
        other => panic!("expected BlobNotFound, got {other:?}"),
    }
}
