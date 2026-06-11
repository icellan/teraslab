//! F-G9-002 regression test (production read path).
//!
//! When an EXTERNAL record's blob is present and the blob store's internal
//! sidecar check passes, the PRODUCTION read path [`Engine::read_cold_data`]
//! must STILL cross-check the recomputed SHA-256 of the payload against the
//! durable record-anchored `ExternalRef.content_hash`. Only that
//! record-anchored digest catches a coordinated payload+sidecar swap by an
//! attacker (or operator with filesystem access) who replaces both files with
//! internally consistent impostor data.
//!
//! These tests previously pinned `StorageManager::read_cold_data`, which has
//! zero production callers; they now exercise the live engine read path the
//! server actually dispatches through.

use std::sync::Arc;
use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::error::SpendError;
use teraslab::record::ExternalRef;
use teraslab::storage::blobstore::{BlobStore, MemoryBlobStore};
use teraslab::storage::tiers::ColdData;

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

fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(data);
    let mut d = [0u8; 32];
    d.copy_from_slice(&h.finalize());
    d
}

/// Register an EXTERNAL record via the production create path with the given
/// `external_ref` (content hash + total size). The blob is NOT uploaded here.
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
fn blob_payload_disagreeing_with_record_anchored_digest_fails() {
    let (engine, blob) = build_engine_with_blob();

    // The create path stamped `content_hash` with the SHA-256 of payload A,
    // but a tampering operator replaced both the payload and the sidecar with
    // payload B. The blob store's own sidecar verifier passes — only the
    // record-anchored digest catches the swap.
    let mut tx_id = [0u8; 32];
    tx_id[0] = 0xBE;
    tx_id[1] = 0xEF;

    let legitimate =
        b"the legitimate cold data payload that the record was stamped against".to_vec();
    let imposter = ColdData {
        inputs: vec![1, 2, 3],
        outputs: vec![4, 5, 6],
        inpoints: vec![7],
    }
    .serialize();
    // total_size in the record must match the on-store blob length so the
    // length check passes and the digest cross-check is what fires.
    let external_ref = ExternalRef {
        store_type: 1,
        content_hash: sha256(&legitimate),
        total_size: imposter.len() as u64,
        input_count: 0,
        output_count: 0,
        inputs_offset: 0,
        outputs_offset: 0,
    };
    create_external_record(&engine, tx_id, external_ref);

    // Put the internally-consistent impostor payload into the store.
    blob.put(&tx_id, &imposter).unwrap();

    let key = TxKey { txid: tx_id };
    let err = engine
        .read_cold_data(&key)
        .expect_err("payload not matching record-anchored content_hash must error");
    match err {
        SpendError::StorageError { detail } => {
            assert!(
                detail.contains("digest does not match"),
                "expected a digest-mismatch detail, got: {detail}"
            );
        }
        other => panic!("expected StorageError(digest mismatch), got {other:?}"),
    }
}

#[test]
fn correctly_stamped_external_record_reads_back_through_engine() {
    // Positive control: when the on-store payload matches the record-anchored
    // digest and length, the production read path returns the bytes verbatim.
    let (engine, blob) = build_engine_with_blob();

    let mut tx_id = [0u8; 32];
    tx_id[0] = 0x10;
    let payload = ColdData {
        inputs: vec![1, 2, 3],
        outputs: vec![4],
        inpoints: vec![],
    }
    .serialize();
    let external_ref = ExternalRef {
        store_type: 1,
        content_hash: sha256(&payload),
        total_size: payload.len() as u64,
        input_count: 0,
        output_count: 0,
        inputs_offset: 0,
        outputs_offset: 0,
    };
    create_external_record(&engine, tx_id, external_ref);
    blob.put(&tx_id, &payload).unwrap();

    let key = TxKey { txid: tx_id };
    let read = engine
        .read_cold_data(&key)
        .expect("matching payload must read back without error");
    assert_eq!(read, payload);
}
