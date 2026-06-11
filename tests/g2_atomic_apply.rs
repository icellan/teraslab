//! F-G5-022 / A-4 — engine-side atomic apply for `spend()`.
//!
//! Hypothesis (F-G5-022 / A-4, and the TODO at
//! `src/server/dispatch.rs:3242` referring to F-G5-022): a concurrent
//! same-key spend could observe a stale before-image and write its
//! own SPENT outcome on top of another thread's already-applied SPENT
//! state — i.e. one thread sees `UTXO_UNSPENT`, returns `Ok` to the
//! client, while another thread also sees `UTXO_UNSPENT` (because it
//! read before the first thread's write landed) and ALSO returns
//! `Ok`. Two competing `Ok` returns for the same UTXO would be a
//! correctness bug: the on-device slot is single-valued, so at most
//! one spend can be authoritative, and the client population would
//! diverge from the on-device state.
//!
//! This test reproduces the race the hard way: N threads spend the
//! same `(tx_key, offset, utxo_hash)` concurrently, each with a
//! distinct `spending_data` (so the idempotent re-spend short-circuit
//! at `src/ops/engine.rs:1232` cannot mask the race — different
//! `spending_data` means at most one thread is the "first writer",
//! and the rest MUST see `AlreadySpent`).
//!
//! Acceptance:
//!   - Exactly one thread returns `Ok`.
//!   - Every other thread returns `Err(AlreadySpent { .. })`.
//!   - The slot's final `spending_data` on disk matches the winner's
//!     request.
//!
//! Outcome: with the current code, `Engine::spend` takes the
//! per-tx-key stripe mutex (`self.locks.lock(&req.tx_key)` at
//! `engine.rs:1176`) for the FULL validate-and-mutate sequence —
//! read metadata, read slot, write slot, write metadata, sync index.
//! No race window exists between the UNSPENT observation and the
//! write because both happen under the same mutex.
//!
//! The test therefore passes today as a regression guard: any future
//! refactor that splits validation from application across the lock
//! boundary (or weakens the lock to a read/write split that allows
//! two readers to both observe UNSPENT) will surface as ≥2 `Ok`
//! returns and a panic here.
//!
//! Per the original plan (P1.3): if the race
//! is not reachable, this becomes documentation only and A-4 /
//! F-G5-022 is resolved as NOT-APPLICABLE (existing stripe-lock
//! already provides the atomic-apply invariant).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::error::SpendError;
use teraslab::ops::spend::SpendRequest;
use teraslab::record::UTXO_SPENT;

const N_UTXOS: usize = 1;
const N_THREADS: usize = 16;
const N_ITERATIONS: usize = 200;

fn build_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(1_024).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        // Single stripe is irrelevant — the engine hashes tx_key into
        // 64 stripes; a 16-thread fight on one key always serializes
        // through one mutex. Using 64 stripes matches production sizing.
        StripedLocks::new(64),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

fn make_hashes(seed: u8) -> &'static [[u8; 32]] {
    let v: Vec<[u8; 32]> = (0..N_UTXOS)
        .map(|i| {
            let mut h = [0u8; 32];
            h[0] = seed;
            h[1] = i as u8;
            h[2] = 0xCC; // distinguish from other test hashes
            h
        })
        .collect();
    Box::leak(v.into_boxed_slice())
}

fn seed_tx(engine: &Engine, tx_id: [u8; 32]) -> &'static [[u8; 32]] {
    let hashes = make_hashes(tx_id[0]);
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

/// One iteration: create the tx, fire N threads at the same UTXO,
/// verify exactly one winner.
///
/// Returning the (winners, losers) counts lets the outer loop sanity-
/// check that we actually exercised the race on every iteration —
/// not just degenerated into "all threads see TxNotFound".
fn run_one_race_round(engine: &Arc<Engine>, tx_id: [u8; 32], iteration: u32) -> (u32, u32) {
    let hashes = seed_tx(engine, tx_id);

    let start_gate = Arc::new(AtomicBool::new(false));
    let winners = Arc::new(AtomicU32::new(0));
    let losers = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::with_capacity(N_THREADS);
    for t in 0..N_THREADS {
        let engine = engine.clone();
        let start_gate = start_gate.clone();
        let winners = winners.clone();
        let losers = losers.clone();
        handles.push(thread::spawn(move || {
            // Build a unique spending_data per thread so the idempotent
            // re-spend short-circuit (same spending_data, no-op success)
            // cannot mask a real race. If two threads both observed
            // UTXO_UNSPENT and both wrote, the slot would end up with
            // ONE of these `spending_data` values, and the OTHER thread's
            // `Ok` would be wrong.
            let mut spending = [0u8; 36];
            spending[0] = 0xEE;
            spending[1] = t as u8;
            spending[2..6].copy_from_slice(&iteration.to_le_bytes());

            // Busy-wait on the gate so all threads enter `spend()` at
            // roughly the same instant — maximises the chance of
            // observing the race window if one exists.
            while !start_gate.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            let result = engine.spend(&SpendRequest {
                tx_key: TxKey::from_bytes(tx_id),
                offset: 0,
                utxo_hash: hashes[0],
                spending_data: spending,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 1500,
                block_height_retention: 0,
            });

            match result {
                Ok(_) => {
                    winners.fetch_add(1, Ordering::Relaxed);
                    Some(spending)
                }
                Err(SpendError::AlreadySpent { offset, .. }) => {
                    assert_eq!(offset, 0, "AlreadySpent must carry the requested offset");
                    losers.fetch_add(1, Ordering::Relaxed);
                    None
                }
                Err(other) => panic!(
                    "iteration={iteration} thread={t}: unexpected spend error: {other:?} \
                     (expected Ok or AlreadySpent)"
                ),
            }
        }));
    }

    // Release all threads.
    start_gate.store(true, Ordering::Release);

    let mut winner_data: Option<[u8; 36]> = None;
    for h in handles {
        if let Some(d) = h.join().unwrap() {
            assert!(
                winner_data.is_none(),
                "iteration={iteration}: two threads both returned Ok — atomic-apply invariant violated. \
                 first winner spending_data={:?}, second={d:?}",
                winner_data.unwrap(),
            );
            winner_data = Some(d);
        }
    }

    let w = winners.load(Ordering::Relaxed);
    let l = losers.load(Ordering::Relaxed);
    assert_eq!(
        w, 1,
        "iteration={iteration}: expected exactly one winner, got {w} (losers={l})"
    );
    assert_eq!(
        w + l,
        N_THREADS as u32,
        "iteration={iteration}: not all threads accounted for (w={w}, l={l}, expected total {N_THREADS})"
    );

    // Verify the on-device slot reflects the unique winner. Reading
    // through the engine's lock-free path is fine here — all writes
    // are quiescent post-join.
    let slot = engine
        .read_slot(&TxKey::from_bytes(tx_id), 0)
        .expect("read_slot must succeed post-race");
    assert_eq!(
        slot.status, UTXO_SPENT,
        "iteration={iteration}: slot must be SPENT after the race"
    );
    let expected = winner_data.expect("a winner must exist");
    assert_eq!(
        slot.spending_data, expected,
        "iteration={iteration}: on-device spending_data must equal the winner's request \
         (a torn or two-writer race would leave a different value or a mix)"
    );

    // Tear down so the next iteration can re-seed under the same tx_id.
    engine
        .delete(&teraslab::ops::remaining::DeleteRequest {
            tx_key: TxKey::from_bytes(tx_id),
        })
        .expect("delete must succeed");

    (w, l)
}

#[test]
fn concurrent_spend_same_utxo_yields_exactly_one_winner() {
    let engine = build_engine();

    // Each iteration uses a distinct txid so the allocator handing back
    // the same offset between rounds cannot mask a regression that only
    // shows on a "warm" record. We rotate the high byte to keep the keys
    // visually distinguishable in failure messages.
    for iteration in 0..N_ITERATIONS as u32 {
        let mut tx_id = [0u8; 32];
        tx_id[0] = 0xA0;
        tx_id[1..5].copy_from_slice(&iteration.to_le_bytes());

        let (w, l) = run_one_race_round(&engine, tx_id, iteration);

        // Liveness sanity: across all iterations we want at least some
        // contention to be real. Asserting per-iteration is the strong
        // claim; this is only here so a future regression that turns
        // every iteration into "1 winner, 15 TxNotFound" would surface
        // as a test failure rather than a green-but-vacuous pass.
        debug_assert!(w == 1 && l == N_THREADS as u32 - 1);
    }
}
