//! F-G2-003 — `write_overflow_entries` must free the *full* allocated
//! overflow size, not one alignment unit.
//!
//! Pre-fix the free path passed exactly `device.alignment()` bytes to
//! `allocator.free` regardless of how many alignment units the overflow
//! block actually occupied. On a 512-byte-aligned device this leaks the
//! tail of every multi-page overflow block (`align_up(252 * 12, 512) =
//! 3072` allocated, 512 freed → 2560 bytes lost per drop).
//!
//! The test seeds enough mined-block entries to push overflow over one
//! alignment unit, then drains them and asserts that `total_free_bytes`
//! returns to its baseline (modulo a small fixed delta for the data
//! record itself). Pre-fix `total_free_bytes` is strictly smaller after
//! the cycle than before.

use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::set_mined::SetMinedRequest;

/// Build an engine backed by a 512-byte-aligned in-memory device.
/// The smaller alignment magnifies the F-G2-003 leak (each 4096-byte
/// allocation rounded from 252-entry overflow → 3072 alignable bytes
/// instead of one 4096 block, so the under-free is 2560 bytes per cycle).
fn build_engine_512_aligned() -> Arc<Engine> {
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
fn overflow_drain_returns_all_allocated_bytes_to_freelist() {
    let engine = build_engine_512_aligned();
    let mut tx = [0u8; 32];
    tx[0] = 0xD1;
    seed_tx(&engine, tx);
    let key = TxKey::from_bytes(tx);

    // Capture the post-create baseline of `used_bytes`. After grow +
    // full drain, `used_bytes` must return to exactly this value —
    // every overflow byte we allocated must end up back on the
    // freelist. The pre-fix free path leaked `(units - 1) * alignment`
    // bytes per shrink-to-zero, so `used_bytes` would be permanently
    // inflated after the cycle.
    let baseline_used = engine.allocator_stats().used_bytes;

    // Add enough block entries to push overflow well past one 512-byte
    // alignment unit. INLINE_BLOCK_ENTRIES = 3, each overflow entry is
    // 12 bytes → 80 overflow entries = 960 bytes → 2 alignment units
    // (1024 bytes). The pre-fix free path would return only 512.
    const ADDS: u32 = 83; // 80 overflow + 3 inline
    for i in 0..ADDS {
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

    // Now drain all of them. After the last unset_mined, the overflow
    // block must be fully returned to the allocator: every byte we drew
    // for the overflow region is back on the freelist.
    for i in 0..ADDS {
        engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 1000 + i,
                block_height: 1000 + i,
                subtree_idx: i,
                current_block_height: 2000,
                block_height_retention: 0,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();
    }

    let final_used = engine.allocator_stats().used_bytes;
    assert_eq!(
        final_used, baseline_used,
        "overflow drain leaked allocator space: baseline_used={baseline_used}, \
         final_used={final_used} (pre-fix the partial-free path leaked \
         (alignment_units - 1) * 512 bytes per drop-to-empty on \
         sub-4K-aligned devices)",
    );
}

#[test]
fn overflow_grow_then_shrink_does_not_leak_when_crossing_alignment_boundary() {
    // A subtler shape than the drain test: grow up to two alignment
    // units, then shrink back to zero. The grow path must `free(old)` +
    // `allocate(new)` rather than overwriting in place (which would
    // either leak the smaller old allocation or write past the live
    // allocation depending on direction). The shrink path stays
    // in-place (smaller-or-equal new size).
    let engine = build_engine_512_aligned();
    let mut tx = [0u8; 32];
    tx[0] = 0xD2;
    seed_tx(&engine, tx);
    let key = TxKey::from_bytes(tx);

    let baseline_used = engine.allocator_stats().used_bytes;

    // Grow: 3 inline + 50 overflow (600 bytes → 2 alignment units).
    for i in 0..53 {
        engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 1_000 + i,
                block_height: 1_000 + i,
                subtree_idx: i,
                current_block_height: 2000,
                block_height_retention: 0,
                on_longest_chain: true,
                unset_mined: false,
            })
            .unwrap();
    }

    // Now drain back to zero.
    for i in 0..53 {
        engine
            .set_mined(&SetMinedRequest {
                tx_key: key,
                block_id: 1_000 + i,
                block_height: 1_000 + i,
                subtree_idx: i,
                current_block_height: 2000,
                block_height_retention: 0,
                on_longest_chain: true,
                unset_mined: true,
            })
            .unwrap();
    }

    let final_used = engine.allocator_stats().used_bytes;
    assert_eq!(
        final_used, baseline_used,
        "grow-then-drain leaked allocator space: baseline_used={baseline_used}, \
         final_used={final_used}",
    );
}
