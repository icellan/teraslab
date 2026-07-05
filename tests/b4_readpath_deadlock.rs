//! B4 — `read_block_entry` must not deadlock against a concurrent
//! `write_overflow_entries` on the same record.
//!
//! `Engine::read_block_entry` acquires the per-record striped read guard
//! (`io::record_read_guard(record_offset)`) and — while holding it — reads
//! the record metadata. The F-G2 torn-read fix later made `io::read_metadata`
//! itself acquire the SAME striped read guard, so the read path re-entered
//! the same non-reentrant `parking_lot::RwLock`. parking_lot readers park
//! behind a queued writer, so if a `write_overflow_entries` writer (which
//! takes the write side of the same stripe via `set_mined`) queues between
//! the outer read-acquire and the inner re-acquire, the reader deadlocks
//! permanently — and the queued writer never proceeds either.
//!
//! This test drives Thread A (repeated `read_block_entry`) and Thread B
//! (repeated `set_mined`, which calls `write_overflow_entries`) against ONE
//! record that has overflow block entries (block_entry_count >
//! INLINE_BLOCK_ENTRIES so the read path reads the overflow region). A
//! watchdog fails the test if both threads do not finish their iterations
//! within a generous timeout. Without the fix the reader (and writer) hang
//! and the watchdog fires.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::set_mined::SetMinedRequest;

fn build_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(32 * 1024 * 1024, 512).unwrap());
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

fn seed_tx(engine: &Engine, tx_id: [u8; 32]) {
    let hashes: &'static [[u8; 32]] = Box::leak(vec![[0u8; 32]; 1].into_boxed_slice());
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
}

#[test]
fn read_block_entry_does_not_deadlock_with_concurrent_overflow_write() {
    let engine = build_engine();
    let mut tx = [0u8; 32];
    tx[0] = 0xB4;
    seed_tx(&engine, tx);
    let key = TxKey::from_bytes(tx);

    // Push the record well into overflow: INLINE_BLOCK_ENTRIES == 3, so 40
    // mined-block entries force the overflow region to exist, meaning
    // `read_block_entry` reads both the metadata AND the overflow block under
    // the single held guard.
    const SEED_ENTRIES: u32 = 40;
    for i in 0..SEED_ENTRIES {
        engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 1000 + i,
                block_height: 1000 + i,
                subtree_idx: i,
                current_block_height: 2000,
                block_height_retention: 0,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();
    }

    const ITERS: u32 = 2000;
    let stop = Arc::new(AtomicBool::new(false));

    // Thread A: hammer read_block_entry (the guarded metadata+overflow read).
    let reader = {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            for i in 0..ITERS {
                // Look up an overflow-resident block_id so the overflow read
                // path (under the held guard) is exercised too.
                let block_id = 1000 + (i % SEED_ENTRIES);
                let res = engine.read_block_entry(&key, block_id);
                assert!(
                    res.is_ok(),
                    "read_block_entry returned an error: {:?}",
                    res.err()
                );
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
        })
    };

    // Thread B: hammer set_mined toggles, which internally calls
    // write_overflow_entries and takes the record write guard on the same
    // stripe key as the reader's outer guard.
    let writer = {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            for i in 0..ITERS {
                let block_id = 2000 + (i % SEED_ENTRIES);
                // Add then remove the entry so overflow grows/shrinks and the
                // write guard is taken on every iteration.
                engine
                    .set_mined(&SetMinedRequest {
                        tx_key: key,
                        block_id,
                        block_height: block_id,
                        subtree_idx: i,
                        current_block_height: 2000,
                        block_height_retention: 0,
                        on_longest_chain: true,
                        unset_mined: false,
                    })
                    .unwrap();
                engine
                    .set_mined(&SetMinedRequest {
                        tx_key: key,
                        block_id,
                        block_height: block_id,
                        subtree_idx: i,
                        current_block_height: 2000,
                        block_height_retention: 0,
                        on_longest_chain: true,
                        unset_mined: true,
                    })
                    .unwrap();
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
        })
    };

    // Watchdog: a separate joiner thread signals completion over a channel.
    // The main thread waits with a timeout; if it fires, the workers are
    // deadlocked (there is no way to un-park them, so we fail the test rather
    // than block CI forever).
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let a = reader.join();
        let b = writer.join();
        let _ = done_tx.send((a, b));
    });

    match done_rx.recv_timeout(Duration::from_secs(30)) {
        Ok((a, b)) => {
            a.expect("reader thread panicked");
            b.expect("writer thread panicked");
        }
        Err(_) => {
            stop.store(true, Ordering::Relaxed);
            panic!(
                "DEADLOCK: read_block_entry / write_overflow_entries did not \
                 complete within the watchdog timeout — the read path re-entered \
                 the per-record striped RwLock behind a queued writer"
            );
        }
    }
}
