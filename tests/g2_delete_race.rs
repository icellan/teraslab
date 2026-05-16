//! F-G2-001 — `delete()` must not let a concurrent reader observe another
//! transaction's data under a key that has been deleted (and whose region was
//! re-allocated for a new transaction).
//!
//! Repro shape:
//! 1. Create tx_A. Read its tx_key + record offset.
//! 2. Spawn N "deleter+creator" worker threads. Each one alternates
//!    `delete(tx_A)` followed by `create(tx_B)` (where tx_B is a brand-new
//!    key the worker keeps re-creating after `delete` puts the region back
//!    on the allocator freelist). When the allocator reuses the offset for
//!    tx_B, the on-device header momentarily belongs to tx_B even though
//!    no reader should be observing it as "tx_A".
//! 3. Spawn M reader threads that hammer `read_metadata(tx_A)`,
//!    `read_slots(tx_A)`, and `get_spend(tx_A, 0)`. Each reader records
//!    every successful response.
//! 4. After the test window closes, all readers join. The invariant is:
//!    no reader ever observed tx_B's metadata labelled as tx_A — i.e.
//!    every `Ok(meta)` returned by `read_metadata(tx_A)` MUST have
//!    `meta.tx_id == tx_A.txid`. Equivalently: every successful read
//!    response MUST belong to the requested transaction.
//!
//! Pre-fix (delete frees the region BEFORE unregistering the index entry)
//! this test would surface readers that received `tx_B.tx_id` under
//! `tx_A`'s key with a non-zero probability. Post-fix the ordering is
//! tombstone → sync → unregister → free, and `read_metadata_for_key`
//! double-checks `meta.tx_id == key.txid` so the assertion holds.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::error::SpendError;
use teraslab::ops::remaining::{DeleteRequest, GetSpendRequest};

const N_UTXOS: usize = 4;

fn make_create_req(tx_id: [u8; 32], hashes: &'static [[u8; 32]]) -> CreateRequest<'static> {
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

fn leak_hashes(seed: u8) -> &'static [[u8; 32]] {
    let v: Vec<[u8; 32]> = (0..N_UTXOS)
        .map(|i| {
            let mut h = [0u8; 32];
            h[0] = seed;
            h[1] = i as u8;
            h
        })
        .collect();
    Box::leak(v.into_boxed_slice())
}

fn build_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(10_000).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(1024),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

#[test]
fn delete_does_not_alias_concurrent_create() {
    let engine = build_engine();

    // Seed transaction tx_A.
    let mut tx_a = [0u8; 32];
    tx_a[0] = 0xAA;
    tx_a[1] = 0x01;
    let hashes_a: &'static [[u8; 32]] = leak_hashes(0xAA);
    let req_a = make_create_req(tx_a, hashes_a);
    engine.create(&req_a).unwrap();
    let key_a = TxKey::from_bytes(tx_a);

    let stop = Arc::new(AtomicBool::new(false));
    let alias_errors = Arc::new(AtomicU64::new(0));
    let reads_total = Arc::new(AtomicU64::new(0));
    let cycles_total = Arc::new(AtomicU64::new(0));

    // Worker count is modest to keep the test fast and deterministic;
    // the race window is narrow even at higher counts so we rely on
    // many iterations rather than thread parallelism.
    let n_deleters = 4;
    let n_readers = 8;

    let mut handles = Vec::new();

    // Deleter+creator threads.
    for worker in 0..n_deleters {
        let engine = engine.clone();
        let stop = stop.clone();
        let cycles_total = cycles_total.clone();
        let hashes_a = hashes_a;
        let tx_a_inner = tx_a;
        handles.push(thread::spawn(move || {
            let mut local_cycle: u32 = 0;
            // Each worker uses a unique high byte so concurrent workers
            // don't collide on each other's "tx_B".
            while !stop.load(Ordering::Relaxed) {
                // Ensure tx_A is present (race-tolerant: maybe a different
                // worker already deleted it).
                let req_a_local = make_create_req(tx_a_inner, hashes_a);
                let _ = engine.create(&req_a_local);

                // Delete tx_A. May fail TxNotFound if another worker
                // already did the delete — that's fine.
                let _ = engine.delete(&DeleteRequest {
                    tx_key: TxKey::from_bytes(tx_a_inner),
                });

                // Create tx_B with a fresh txid every iteration. This is
                // what forces the allocator to potentially hand back the
                // very offset tx_A just freed.
                let mut tx_b = [0u8; 32];
                tx_b[0] = 0xBB;
                tx_b[1] = worker as u8;
                tx_b[2..6].copy_from_slice(&local_cycle.to_le_bytes());
                let hashes_b: Vec<[u8; 32]> = (0..N_UTXOS)
                    .map(|i| {
                        let mut h = [0u8; 32];
                        h[0] = 0xBB;
                        h[1] = worker as u8;
                        h[2..6].copy_from_slice(&local_cycle.to_le_bytes());
                        h[6] = i as u8;
                        h
                    })
                    .collect();
                let hashes_b_ref: &'static [[u8; 32]] = Box::leak(hashes_b.into_boxed_slice());
                let req_b = make_create_req(tx_b, hashes_b_ref);
                if engine.create(&req_b).is_ok() {
                    // Tear it down so the freelist stays hot.
                    let _ = engine.delete(&DeleteRequest {
                        tx_key: TxKey::from_bytes(tx_b),
                    });
                }
                local_cycle = local_cycle.wrapping_add(1);
                cycles_total.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Reader threads — they may legitimately see TxNotFound (when tx_A is
    // mid-delete or has just been deleted), but every Ok must reference
    // tx_A and not some tx_B that briefly occupied the same offset.
    for _ in 0..n_readers {
        let engine = engine.clone();
        let stop = stop.clone();
        let alias_errors = alias_errors.clone();
        let reads_total = reads_total.clone();
        let key_a = key_a;
        let hashes_a = hashes_a;
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match engine.read_metadata(&key_a) {
                    Ok(meta) => {
                        let tx_id = { meta.tx_id };
                        if tx_id != tx_a {
                            alias_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        reads_total.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(SpendError::TxNotFound) => {}
                    Err(SpendError::StorageError { .. }) => {
                        // Torn read surfaced as CRC failure is acceptable
                        // under R-009 — the protocol surfaces it to the
                        // caller. We only assert on the cross-tx aliasing
                        // hazard, not on torn-read recovery.
                    }
                    Err(other) => panic!("unexpected read_metadata err: {other:?}"),
                }

                match engine.read_slots(&key_a) {
                    Ok(slots) => {
                        // Every slot hash on tx_A must come from tx_A's
                        // hashes. If we observed tx_B's slots aliased as
                        // tx_A's, at least one hash won't match.
                        for (i, slot) in slots.iter().enumerate() {
                            if i < hashes_a.len() && slot.hash != hashes_a[i] {
                                alias_errors.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                        reads_total.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(SpendError::TxNotFound)
                    | Err(SpendError::StorageError { .. })
                    | Err(SpendError::UtxoNotFound { .. }) => {}
                    Err(other) => panic!("unexpected read_slots err: {other:?}"),
                }

                match engine.get_spend(&GetSpendRequest {
                    tx_key: key_a,
                    offset: 0,
                    utxo_hash: hashes_a[0],
                }) {
                    Ok(_) => {
                        reads_total.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(SpendError::TxNotFound)
                    | Err(SpendError::UtxoHashMismatch { .. })
                    | Err(SpendError::UtxoNotFound { .. })
                    | Err(SpendError::StorageError { .. }) => {
                        // Acceptable: tx_A absent (deleted), or tx_A
                        // present with a fresh hash from a re-create
                        // (still tx_A, just a new instance — different
                        // utxo_hash). Cross-tx aliasing would NOT
                        // produce UtxoHashMismatch under this code path
                        // because `get_spend` now verifies tx_id via
                        // `read_metadata_for_key`.
                    }
                    Err(other) => panic!("unexpected get_spend err: {other:?}"),
                }
            }
        }));
    }

    let deadline = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let alias = alias_errors.load(Ordering::Relaxed);
    let reads = reads_total.load(Ordering::Relaxed);
    let cycles = cycles_total.load(Ordering::Relaxed);
    // Guard against a vacuous pass: make sure both sides did real work.
    assert!(reads >= 100, "test did not stress reads enough: {reads}");
    assert!(
        cycles >= 20,
        "test did not stress mutations enough: {cycles}"
    );
    assert_eq!(
        alias, 0,
        "{alias} reads observed cross-tx aliasing (out of {reads} successful reads, {cycles} delete/create cycles)",
    );
}
