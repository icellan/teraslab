//! F-G2-006 — defend the size contract between `pre_allocate_create`
//! and `create_at_offset`.
//!
//! Pre-fix both functions independently rebuilt `cold_data` from the
//! same `req` and trusted that the two computations agreed. If a future
//! caller ever mutated `req` between the two calls (or passed a
//! different `req` to `create_at_offset`), the on-device `record_size`
//! and the allocator reservation would silently disagree and writes
//! could spill into the adjacent record.
//!
//! Post-fix `pre_allocate_create` returns the computed `total_size`, and
//! the new `create_at_offset_verified` variant accepts the expected
//! total and surfaces a `CreateError::StorageError` (plus a
//! `debug_assert_eq!` panic in debug builds) when the recomputation
//! disagrees.

use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;

fn build_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(1_024).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(64),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

fn make_req(tx_id: [u8; 32], hashes: &'static [[u8; 32]]) -> CreateRequest<'static> {
    CreateRequest {
        tx_id,
        tx_version: 1,
        locktime: 0,
        fee: 500,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        utxo_hashes: hashes,
        inputs: None,
        outputs: None,
        inpoints: None,
        is_external: false,
        created_at: 1_710_000_000_000,
        block_height: 1000,
        mined_block_infos: &[],
        frozen: false,
        conflicting: false,
        locked: false,
        external_ref: None,
        parent_txids: &[],
    }
}

#[test]
fn pre_allocate_returns_total_size_matching_verified_create() {
    let engine = build_engine();
    let hashes: &'static [[u8; 32]] = Box::leak(vec![[7u8; 32]; 3].into_boxed_slice());
    let mut tx = [0u8; 32];
    tx[0] = 0x10;

    let req = make_req(tx, hashes);
    let (offset, utxo_count, total_size) = engine.pre_allocate_create(&req).unwrap();
    assert_eq!(utxo_count, 3);
    assert!(total_size > 0, "total_size must be the actual reservation");

    // The verified variant accepts the exact size pre_allocate_create
    // reported and proceeds normally.
    let resp = engine
        .create_at_offset_verified(&req, offset, total_size)
        .unwrap();
    assert_eq!(resp.record_offset, offset);
    assert_eq!(resp.utxo_count, 3);
}

#[cfg(not(debug_assertions))]
#[test]
fn create_at_offset_verified_rejects_size_mismatch_in_release_builds() {
    // In release builds the `debug_assert_eq!` is compiled out, so we
    // observe the explicit `CreateError::StorageError` path.
    let engine = build_engine();
    let hashes: &'static [[u8; 32]] = Box::leak(vec![[8u8; 32]; 3].into_boxed_slice());
    let mut tx = [0u8; 32];
    tx[0] = 0x11;

    let req = make_req(tx, hashes);
    let (offset, _utxo_count, total_size) = engine.pre_allocate_create(&req).unwrap();

    let bogus_expected = total_size + 64;
    let err = engine
        .create_at_offset_verified(&req, offset, bogus_expected)
        .expect_err("expected StorageError on size mismatch");
    let detail = format!("{err:?}");
    assert!(
        detail.contains("record_size") && detail.contains("reservation"),
        "expected size-mismatch detail, got {detail}",
    );
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "create_at_offset record_size diverged")]
fn create_at_offset_verified_panics_on_size_mismatch_in_debug_builds() {
    // In debug builds the `debug_assert_eq!` fires before the release
    // `StorageError` return — the panic shape is what we want during
    // development so the divergence is impossible to miss.
    let engine = build_engine();
    let hashes: &'static [[u8; 32]] = Box::leak(vec![[9u8; 32]; 3].into_boxed_slice());
    let mut tx = [0u8; 32];
    tx[0] = 0x12;

    let req = make_req(tx, hashes);
    let (offset, _utxo_count, total_size) = engine.pre_allocate_create(&req).unwrap();
    let bogus_expected = total_size + 64;
    let _ = engine.create_at_offset_verified(&req, offset, bogus_expected);
}
