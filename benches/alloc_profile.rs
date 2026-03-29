//! Allocation profiler — counts heap allocations per operation.
//!
//! Uses a global counting allocator to measure exactly how many allocations
//! and total bytes each engine operation, codec function, and index operation
//! triggers. This is NOT a criterion benchmark — it outputs a report to stdout.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Counting allocator
// ---------------------------------------------------------------------------

struct CountingAlloc;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static TRACKING: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACKING.load(Ordering::Relaxed) != 0 {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if TRACKING.load(Ordering::Relaxed) != 0 {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn start_tracking() {
    ALLOC_COUNT.store(0, Ordering::SeqCst);
    ALLOC_BYTES.store(0, Ordering::SeqCst);
    TRACKING.store(1, Ordering::SeqCst);
}

fn stop_tracking() -> (u64, u64) {
    TRACKING.store(0, Ordering::SeqCst);
    (
        ALLOC_COUNT.load(Ordering::SeqCst),
        ALLOC_BYTES.load(Ordering::SeqCst),
    )
}

/// Run `f` N times, return (allocs_per_op, bytes_per_op).
fn measure<F: FnMut()>(name: &str, n: u32, mut f: F) -> (f64, f64) {
    // Warm up.
    for _ in 0..10 {
        f();
    }

    start_tracking();
    for _ in 0..n {
        f();
    }
    let (allocs, bytes) = stop_tracking();

    let per_alloc = allocs as f64 / n as f64;
    let per_bytes = bytes as f64 / n as f64;
    println!("{name:50} {per_alloc:10.1} allocs/op  {per_bytes:10.0} bytes/op");
    (per_alloc, per_bytes)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::*;
use teraslab::ops::engine::Engine;
use teraslab::ops::mark_longest_chain::*;
use teraslab::ops::remaining::*;
use teraslab::ops::set_mined::*;
use teraslab::ops::spend::*;
use teraslab::ops::unspend::*;
use teraslab::protocol::codec::*;

fn make_tx_id(n: u32) -> [u8; 32] {
    let mut txid = [0u8; 32];
    txid[0..4].copy_from_slice(&n.to_le_bytes());
    txid[8..12].copy_from_slice(&(n.wrapping_mul(0x9E37)).to_le_bytes());
    txid[16..18].copy_from_slice(&(n as u16).to_le_bytes());
    txid
}

fn make_utxo_hash(tx_n: u32, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0] = (vout & 0xFF) as u8;
    h[1] = ((vout >> 8) & 0xFF) as u8;
    h[4..8].copy_from_slice(&tx_n.to_le_bytes());
    h
}

fn create_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(512 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(200_000).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(65536),
        DahIndex::new(),
        UnminedIndex::new(),
    ))
}

fn create_tx(engine: &Engine, tx_idx: u32, utxo_count: u32) {
    let tx_id = make_tx_id(tx_idx);
    let utxo_hashes: Vec<[u8; 32]> =
        (0..utxo_count).map(|v| make_utxo_hash(tx_idx, v)).collect();
    let req = CreateRequest {
        tx_id,
        tx_version: 1,
        locktime: 0,
        fee: 500,
        size_in_bytes: 250,
        extended_size: 0,
        is_coinbase: false,
        spending_height: 0,
        utxo_hashes: &utxo_hashes,
        inputs: None,
        outputs: None,
        inpoints: None,
        is_external: false,
        created_at: 1710000000000,
        block_height: 1000,
        mined_block_infos: &[],
        frozen: false,
        conflicting: false,
        locked: false,
        parent_txids: &[],
    };
    engine.create(&req).unwrap();
}

fn setup_engine(count: u32, utxos_per_tx: u32) -> Arc<Engine> {
    let engine = create_engine();
    for i in 0..count {
        create_tx(&engine, i, utxos_per_tx);
    }
    engine
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    println!("=== TeraSlab Allocation Profile ===");
    println!("Each operation measured over N iterations. Results are per-operation averages.");
    println!();

    // -----------------------------------------------------------------------
    // Engine operations
    // -----------------------------------------------------------------------
    println!("--- Engine Operations (50k pre-populated txs, 5 UTXOs each) ---");
    let engine = setup_engine(50_000, 5);
    let n = 10_000u32;

    let mut tx_i = 0u32;
    measure("engine::create (5 UTXOs)", n, || {
        let tx_id = make_tx_id(tx_i + 1_000_000);
        let utxo_hashes: Vec<[u8; 32]> =
            (0..5u32).map(|v| make_utxo_hash(tx_i + 1_000_000, v)).collect();
        let req = CreateRequest {
            tx_id,
            tx_version: 1, locktime: 0, fee: 500, size_in_bytes: 250,
            extended_size: 0, is_coinbase: false, spending_height: 0,
            utxo_hashes: &utxo_hashes, inputs: None, outputs: None, inpoints: None,
            is_external: false, created_at: 1710000000000,
            block_height: 1000, mined_block_infos: &[],
            frozen: false, conflicting: false, locked: false,
            parent_txids: &[],
        };
        let _ = engine.create(&req);
        tx_i += 1;
    });

    let mut tx_i = 0u32;
    measure("engine::spend", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let mut sd = [0u8; 36];
        sd[0..4].copy_from_slice(&(tx_i + 10000).to_le_bytes());
        let _ = engine.spend(&SpendRequest {
            tx_key: key, offset: (tx_i % 5),
            utxo_hash: make_utxo_hash(tx_i, tx_i % 5),
            spending_data: sd,
            ignore_conflicting: false, ignore_locked: false,
            current_block_height: 2000, block_height_retention: 288,
        });
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    let mut tx_i = 0u32;
    measure("engine::unspend", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.unspend(&UnspendRequest {
            tx_key: key, offset: (tx_i % 5),
            utxo_hash: make_utxo_hash(tx_i, tx_i % 5),
            current_block_height: 2000, block_height_retention: 288,
        });
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    let mut tx_i = 0u32;
    measure("engine::set_mined", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.set_mined(&SetMinedRequest {
            tx_key: key, block_id: tx_i + 100, block_height: 2000,
            subtree_idx: 0, current_block_height: 2000,
            block_height_retention: 288, on_longest_chain: true,
            unset_mined: false,
        });
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    let mut tx_i = 0u32;
    measure("engine::read_metadata", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.read_metadata(&key);
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    let mut tx_i = 0u32;
    measure("engine::get_spend", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.get_spend(&GetSpendRequest {
            tx_key: key, offset: 0,
            utxo_hash: make_utxo_hash(tx_i, 0),
        });
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    let mut tx_i = 0u32;
    measure("engine::freeze", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.freeze(&FreezeRequest {
            tx_key: key, offset: 1,
            utxo_hash: make_utxo_hash(tx_i, 1),
        });
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    let mut tx_i = 0u32;
    measure("engine::unfreeze", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.unfreeze(&UnfreezeRequest {
            tx_key: key, offset: 1,
            utxo_hash: make_utxo_hash(tx_i, 1),
        });
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    let mut tx_i = 0u32;
    measure("engine::set_conflicting", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.set_conflicting(&SetConflictingRequest {
            tx_key: key, value: true,
            current_block_height: 2000, block_height_retention: 288,
        });
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    let mut tx_i = 0u32;
    measure("engine::set_locked", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.set_locked(&SetLockedRequest {
            tx_key: key, value: true,
        });
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    let mut tx_i = 0u32;
    measure("engine::preserve_until", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.preserve_until(&PreserveUntilRequest {
            tx_key: key, block_height: 5000,
        });
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    let mut tx_i = 0u32;
    measure("engine::mark_on_longest_chain", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.mark_on_longest_chain(&MarkOnLongestChainRequest {
            tx_key: key, on_longest_chain: true,
            current_block_height: 2000, block_height_retention: 288,
        });
        tx_i += 1;
        if tx_i >= 50_000 { tx_i = 0; }
    });

    drop(engine);

    // Delete needs a fresh engine since it's destructive.
    println!();
    println!("--- Engine Delete ---");
    let engine = setup_engine(20_000, 5);
    let mut tx_i = 0u32;
    measure("engine::delete", 5_000, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = engine.delete(&DeleteRequest { tx_key: key });
        tx_i += 1;
    });
    drop(engine);

    // -----------------------------------------------------------------------
    // Index operations
    // -----------------------------------------------------------------------
    println!();
    println!("--- Index Operations (100k entries) ---");

    let mut index = Index::new(200_000).unwrap();
    for i in 0..100_000u32 {
        let key = TxKey { txid: make_tx_id(i) };
        let entry = teraslab::index::TxIndexEntry {
            device_id: 0, record_offset: (i as u64) * 4096,
            utxo_count: 5, block_entry_count: 1, tx_flags: 0,
            spent_utxos: 0, dah_or_preserve: 0, unmined_since: 0,
            generation: 0,
        };
        index.register(key, entry).unwrap();
    }

    let mut tx_i = 0u32;
    measure("index::lookup (hit)", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = index.lookup(&key);
        tx_i += 1;
        if tx_i >= 100_000 { tx_i = 0; }
    });

    let mut tx_i = 500_000u32;
    measure("index::lookup (miss)", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let _ = index.lookup(&key);
        tx_i += 1;
    });

    let mut tx_i = 200_000u32;
    measure("index::register", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        let entry = teraslab::index::TxIndexEntry {
            device_id: 0, record_offset: (tx_i as u64) * 4096,
            utxo_count: 5, block_entry_count: 1, tx_flags: 0,
            spent_utxos: 0, dah_or_preserve: 0, unmined_since: 0,
            generation: 0,
        };
        let _ = index.register(key, entry);
        tx_i += 1;
    });

    let mut tx_i = 0u32;
    measure("index::update_cached_fields", n, || {
        let key = TxKey { txid: make_tx_id(tx_i) };
        index.update_cached_fields(&key, 0x01, 2, tx_i, 0, 0, tx_i + 1);
        tx_i += 1;
        if tx_i >= 100_000 { tx_i = 0; }
    });

    drop(index);

    // -----------------------------------------------------------------------
    // Allocator operations
    // -----------------------------------------------------------------------
    println!();
    println!("--- Allocator Operations ---");

    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(512 * 1024 * 1024, 4096).unwrap());
    let mut alloc = SlotAllocator::new(dev).unwrap();
    measure("allocator::allocate (4096)", n, || {
        let _ = alloc.allocate(4096);
    });

    // Re-create with fragmentation.
    let dev: Arc<dyn BlockDevice> =
        Arc::new(MemoryDevice::new(512 * 1024 * 1024, 4096).unwrap());
    let mut alloc = SlotAllocator::new(dev).unwrap();
    let offsets: Vec<u64> = (0..20_000).map(|_| alloc.allocate(4096).unwrap()).collect();
    for i in (0..20_000).step_by(2) {
        alloc.free(offsets[i], 4096).unwrap();
    }
    measure("allocator::allocate (fragmented)", n, || {
        let off = alloc.allocate(4096).unwrap();
        alloc.free(off, 4096).unwrap();
    });

    // -----------------------------------------------------------------------
    // Wire codec operations
    // -----------------------------------------------------------------------
    println!();
    println!("--- Wire Codec (encode+decode round-trips, 1024 items) ---");

    let spend_params = SpendBatchParams {
        ignore_conflicting: false, ignore_locked: false,
        current_block_height: 1000, block_height_retention: 288,
    };
    let spend_items: Vec<WireSpendItem> = (0..1024u32)
        .map(|i| WireSpendItem {
            txid: make_tx_id(i), vout: i,
            utxo_hash: make_utxo_hash(i, 0),
            spending_data: [0xAB; 36],
        })
        .collect();
    measure("codec::spend_batch (encode 1024)", n, || {
        let _ = encode_spend_batch(&spend_params, &spend_items);
    });
    let encoded = encode_spend_batch(&spend_params, &spend_items);
    measure("codec::spend_batch (decode 1024)", n, || {
        let _ = decode_spend_batch(&encoded);
    });

    let mined_params = SetMinedBatchParams {
        block_id: 42, block_height: 800_000, subtree_idx: 7,
        on_longest_chain: true, unset_mined: false,
        current_block_height: 800_000, block_height_retention: 288,
    };
    let txids: Vec<[u8; 32]> = (0..1024u32).map(make_tx_id).collect();
    measure("codec::set_mined_batch (encode 1024)", n, || {
        let _ = encode_set_mined_batch(&mined_params, &txids);
    });
    let encoded = encode_set_mined_batch(&mined_params, &txids);
    measure("codec::set_mined_batch (decode 1024)", n, || {
        let _ = decode_set_mined_batch(&encoded);
    });

    let create_items: Vec<WireCreateItem> = (0..100u32)
        .map(|i| WireCreateItem {
            txid: make_tx_id(i), tx_version: 2, locktime: 0, fee: 500,
            size_in_bytes: 250, extended_size: 0, is_coinbase: false,
            spending_height: 0, created_at: 1710000000000, flags: 0,
            utxo_hashes: (0..5).map(|v| make_utxo_hash(i, v)).collect(),
            cold_data: vec![], block_height: 1000,
            mined_block_id: None, mined_block_height: None,
            mined_subtree_idx: None, parent_txids: vec![],
        })
        .collect();
    measure("codec::create_batch (encode 100)", n, || {
        let _ = encode_create_batch(&create_items);
    });
    let encoded = encode_create_batch(&create_items);
    measure("codec::create_batch (decode 100)", n, || {
        let _ = decode_create_batch(&encoded);
    });

    let slot_items: Vec<WireSlotItem> = (0..256u32)
        .map(|i| WireSlotItem {
            txid: make_tx_id(i), vout: i,
            utxo_hash: make_utxo_hash(i, 0),
        })
        .collect();
    measure("codec::slot_item_batch (encode 256)", n, || {
        let _ = encode_slot_item_batch(&slot_items);
    });
    let encoded = encode_slot_item_batch(&slot_items);
    measure("codec::slot_item_batch (decode 256)", n, || {
        let _ = decode_slot_item_batch(&encoded);
    });

    let get_items: Vec<WireGetResult> = (0..100)
        .map(|_| WireGetResult { status: 0, data: vec![0u8; 100] })
        .collect();
    measure("codec::get_response (encode 100)", n, || {
        let _ = encode_get_response(&get_items);
    });
    let encoded = encode_get_response(&get_items);
    measure("codec::get_response (decode 100)", n, || {
        let _ = decode_get_response(&encoded);
    });

    let errors: Vec<BatchItemError> = (0..50u32)
        .map(|i| BatchItemError {
            item_index: i, error_code: 1, error_data: vec![],
        })
        .collect();
    measure("codec::sparse_errors (encode 50)", n, || {
        let _ = encode_sparse_errors(&errors);
    });
    let encoded = encode_sparse_errors(&errors);
    measure("codec::sparse_errors (decode 50)", n, || {
        let _ = decode_sparse_errors(&encoded);
    });

    println!();
    println!("=== Profile Complete ===");
}
