//! F-G2-002 — reject the reserved all-`0xFF` `spending_data` sentinel.
//!
//! `[0xFF; 36]` is the on-disk marker for a frozen slot. If a client could
//! pass it as `spending_data` and have the engine stamp the slot under
//! `status=UTXO_SPENT`, the slot would be permanently bricked: `unspend`
//! short-circuits with `Frozen` on the sentinel comparison before the
//! data-match check, and `unfreeze` rejects non-`UTXO_FROZEN` status. The
//! 36 bytes are also not a valid BSV `txid + vin` (an all-`0xFF` txid does
//! not exist on the network), so rejecting them loses no real traffic.
//!
//! Both the single-`spend` and the batched `spend_multi` entry points must
//! reject the sentinel BEFORE any device write. The single path returns
//! `Err(ReservedSpendingData)`; the batch path records a per-item error so
//! the rest of the batch can still succeed.

use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::error::SpendError;
use teraslab::ops::spend::{SpendItem, SpendMultiRequest, SpendRequest};

const FROZEN_SENTINEL: [u8; 36] = [0xFFu8; 36];

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

fn leak_hashes(seed: u8, n: usize) -> &'static [[u8; 32]] {
    let v: Vec<[u8; 32]> = (0..n)
        .map(|i| {
            let mut h = [0u8; 32];
            h[0] = seed;
            h[1] = i as u8;
            h
        })
        .collect();
    Box::leak(v.into_boxed_slice())
}

fn seed_tx(engine: &Engine, tx_id: [u8; 32], n_utxos: usize) -> &'static [[u8; 32]] {
    let hashes = leak_hashes(tx_id[0], n_utxos);
    engine
        .create(&CreateRequest {
            tx_id,
            tx_version: 1,
            locktime: 0,
            fee: 100,
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
        })
        .unwrap();
    hashes
}

#[test]
fn spend_rejects_frozen_sentinel_spending_data() {
    let engine = build_engine();
    let mut tx = [0u8; 32];
    tx[0] = 0xA1;
    let hashes = seed_tx(&engine, tx, 4);

    // Before the fix this call would have written the sentinel to the slot
    // and the UTXO would be unrecoverable. The fix rejects it up front.
    let err = engine
        .spend(&SpendRequest {
            tx_key: TxKey::from_bytes(tx),
            offset: 0,
            utxo_hash: hashes[0],
            spending_data: FROZEN_SENTINEL,
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1500,
            block_height_retention: 0,
        })
        .expect_err("spend should reject the reserved sentinel");

    match err {
        SpendError::ReservedSpendingData { offset } => {
            assert_eq!(offset, 0, "error must carry the request offset");
        }
        other => panic!("expected ReservedSpendingData, got {other:?}"),
    }

    // Slot must remain UNSPENT — no on-device side effect of the rejected
    // request. We re-read via the engine's lock-free reader.
    let slot = engine.read_slot(&TxKey::from_bytes(tx), 0).unwrap();
    assert_eq!(
        slot.status,
        teraslab::record::UTXO_UNSPENT,
        "slot must stay UNSPENT after a rejected sentinel spend",
    );
}

#[test]
fn spend_multi_records_per_item_reserved_sentinel_error() {
    let engine = build_engine();
    let mut tx = [0u8; 32];
    tx[0] = 0xA2;
    let hashes = seed_tx(&engine, tx, 4);

    // Build a batch with one legitimate spend and one sentinel spend. The
    // legitimate one must succeed; the sentinel one must surface as a
    // per-item error keyed by its `idx`.
    let mut legit_data = [0u8; 36];
    legit_data[0] = 0xBB;
    legit_data[31] = 0xCC;

    let resp = engine
        .spend_multi(&SpendMultiRequest {
            tx_key: TxKey::from_bytes(tx),
            spends: vec![
                SpendItem {
                    offset: 0,
                    utxo_hash: hashes[0],
                    spending_data: legit_data,
                    idx: 7,
                },
                SpendItem {
                    offset: 1,
                    utxo_hash: hashes[1],
                    spending_data: FROZEN_SENTINEL,
                    idx: 11,
                },
            ],
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1500,
            block_height_retention: 0,
        })
        .unwrap();

    // Item 7 (legitimate) succeeds — no error recorded.
    assert!(
        !resp.errors.contains_key(&7),
        "legitimate spend should not error: errors={:?}",
        resp.errors,
    );
    // Item 11 (sentinel) records ReservedSpendingData.
    match resp.errors.get(&11) {
        Some(SpendError::ReservedSpendingData { offset }) => {
            assert_eq!(*offset, 1);
        }
        other => panic!("expected ReservedSpendingData at idx 11, got {other:?}"),
    }
    // Only the legitimate spend should have counted toward spent_count.
    assert_eq!(resp.spent_count, 1, "only the legitimate spend applied");

    // Slot 1 must remain UNSPENT.
    let slot1 = engine.read_slot(&TxKey::from_bytes(tx), 1).unwrap();
    assert_eq!(
        slot1.status,
        teraslab::record::UTXO_UNSPENT,
        "sentinel-rejected slot stays UNSPENT",
    );
    // Slot 0 must now be SPENT with the legitimate spending data.
    let slot0 = engine.read_slot(&TxKey::from_bytes(tx), 0).unwrap();
    assert_eq!(slot0.status, teraslab::record::UTXO_SPENT);
    assert_eq!(slot0.spending_data, legit_data);
}
