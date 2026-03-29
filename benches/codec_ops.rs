//! Criterion benchmarks for the Rust-side wire protocol codec.
//!
//! Covers encode and decode for every batch operation type, plus response
//! codecs (sparse errors, partial-with-signals, get response, get-spend
//! response, stream chunk/end).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use teraslab::protocol::codec::*;

fn test_txid(n: u32) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[0..4].copy_from_slice(&n.to_le_bytes());
    t
}

fn test_utxo_hash(n: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[4..8].copy_from_slice(&n.to_le_bytes());
    h
}

// ---------------------------------------------------------------------------
// SpendBatch encode/decode
// ---------------------------------------------------------------------------

fn bench_spend_batch_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_spend_batch");

    for &count in &[1usize, 100, 1024] {
        let params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 1000,
            block_height_retention: 288,
        };
        let items: Vec<WireSpendItem> = (0..count as u32)
            .map(|i| WireSpendItem {
                txid: test_txid(i),
                vout: i,
                utxo_hash: test_utxo_hash(i),
                spending_data: [0xAB; 36],
            })
            .collect();

        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(
            BenchmarkId::new("encode", count),
            &count,
            |b, _| {
                b.iter(|| encode_spend_batch(&params, &items))
            },
        );

        let encoded = encode_spend_batch(&params, &items);
        group.bench_with_input(
            BenchmarkId::new("decode", count),
            &count,
            |b, _| {
                b.iter(|| decode_spend_batch(&encoded))
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// SetMinedBatch encode/decode
// ---------------------------------------------------------------------------

fn bench_set_mined_batch_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_set_mined_batch");

    for &count in &[1usize, 100, 1024] {
        let params = SetMinedBatchParams {
            block_id: 42,
            block_height: 800_000,
            subtree_idx: 7,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 800_000,
            block_height_retention: 288,
        };
        let txids: Vec<[u8; 32]> = (0..count as u32).map(test_txid).collect();

        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(
            BenchmarkId::new("encode", count),
            &count,
            |b, _| b.iter(|| encode_set_mined_batch(&params, &txids)),
        );

        let encoded = encode_set_mined_batch(&params, &txids);
        group.bench_with_input(
            BenchmarkId::new("decode", count),
            &count,
            |b, _| b.iter(|| decode_set_mined_batch(&encoded)),
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// TxidBatch (Delete, SetLocked, etc.) encode/decode
// ---------------------------------------------------------------------------

fn bench_txid_batch_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_txid_batch");

    let txids: Vec<[u8; 32]> = (0..1024u32).map(test_txid).collect();
    let shared = vec![0x01, 0x00, 0x00, 0x03, 0xE8, 0x00, 0x00, 0x01, 0x20]; // 9 bytes

    group.throughput(Throughput::Elements(1024));

    group.bench_function("encode_1024", |b| {
        b.iter(|| encode_txid_batch(&txids, &shared))
    });

    let encoded = encode_txid_batch(&txids, &shared);
    group.bench_function("decode_1024", |b| {
        b.iter(|| decode_txid_batch(&encoded, shared.len()))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// SlotItemBatch (Freeze/Unfreeze/GetSpend) encode/decode
// ---------------------------------------------------------------------------

fn bench_slot_item_batch_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_slot_item_batch");

    for &count in &[1usize, 50, 256] {
        let items: Vec<WireSlotItem> = (0..count as u32)
            .map(|i| WireSlotItem {
                txid: test_txid(i),
                vout: i,
                utxo_hash: test_utxo_hash(i),
            })
            .collect();

        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(
            BenchmarkId::new("encode", count),
            &count,
            |b, _| b.iter(|| encode_slot_item_batch(&items)),
        );

        let encoded = encode_slot_item_batch(&items);
        group.bench_with_input(
            BenchmarkId::new("decode", count),
            &count,
            |b, _| b.iter(|| decode_slot_item_batch(&encoded)),
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// ReassignBatch encode/decode
// ---------------------------------------------------------------------------

fn bench_reassign_batch_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_reassign_batch");

    let count = 100;
    let params = ReassignBatchParams {
        block_height: 2000,
        spendable_after: 10,
    };
    let items: Vec<WireReassignItem> = (0..count)
        .map(|i: u32| WireReassignItem {
            txid: test_txid(i),
            vout: i,
            utxo_hash: test_utxo_hash(i),
            new_utxo_hash: test_utxo_hash(i + 1_000_000),
        })
        .collect();

    group.throughput(Throughput::Elements(count as u64));

    group.bench_function("encode_100", |b| {
        b.iter(|| encode_reassign_batch(&params, &items))
    });

    let encoded = encode_reassign_batch(&params, &items);
    group.bench_function("decode_100", |b| {
        b.iter(|| decode_reassign_batch(&encoded))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// UnspendBatch encode/decode
// ---------------------------------------------------------------------------

fn bench_unspend_batch_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_unspend_batch");

    let count = 256;
    let params = UnspendBatchParams {
        current_block_height: 2000,
        block_height_retention: 288,
    };
    let items: Vec<WireSlotItem> = (0..count)
        .map(|i: u32| WireSlotItem {
            txid: test_txid(i),
            vout: i,
            utxo_hash: test_utxo_hash(i),
        })
        .collect();

    group.throughput(Throughput::Elements(count as u64));

    group.bench_function("encode_256", |b| {
        b.iter(|| encode_unspend_batch(&params, &items))
    });

    let encoded = encode_unspend_batch(&params, &items);
    group.bench_function("decode_256", |b| {
        b.iter(|| decode_unspend_batch(&encoded))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// CreateBatch encode/decode
// ---------------------------------------------------------------------------

fn bench_create_batch_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_create_batch");

    for &count in &[1usize, 10, 100] {
        let items: Vec<WireCreateItem> = (0..count as u32)
            .map(|i| WireCreateItem {
                txid: test_txid(i),
                tx_version: 2,
                locktime: 0,
                fee: 500,
                size_in_bytes: 250,
                extended_size: 0,
                is_coinbase: false,
                spending_height: 0,
                created_at: 1710000000000,
                flags: 0,
                utxo_hashes: (0..5).map(|v| test_utxo_hash(i * 100 + v)).collect(),
                cold_data: vec![],
                block_height: 1000,
                mined_block_id: None,
                mined_block_height: None,
                mined_subtree_idx: None,
                parent_txids: vec![],
            })
            .collect();

        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(
            BenchmarkId::new("encode", count),
            &count,
            |b, _| b.iter(|| encode_create_batch(&items)),
        );

        let encoded = encode_create_batch(&items);
        group.bench_with_input(
            BenchmarkId::new("decode", count),
            &count,
            |b, _| b.iter(|| decode_create_batch(&encoded)),
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// GetBatch encode/decode
// ---------------------------------------------------------------------------

fn bench_get_batch_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_get_batch");

    let count = 1024;
    let txids: Vec<[u8; 32]> = (0..count).map(test_txid).collect();

    group.throughput(Throughput::Elements(count as u64));

    group.bench_function("encode_1024", |b| {
        b.iter(|| encode_get_batch(FieldMask::ALL, &txids))
    });

    let encoded = encode_get_batch(FieldMask::ALL, &txids);
    group.bench_function("decode_1024", |b| {
        b.iter(|| decode_get_batch(&encoded))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// GetResponse encode/decode
// ---------------------------------------------------------------------------

fn bench_get_response_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_get_response");

    let count = 100;
    let items: Vec<WireGetResult> = (0..count)
        .map(|_| WireGetResult {
            status: 0,
            data: vec![0u8; 100],
        })
        .collect();

    group.throughput(Throughput::Elements(count as u64));

    group.bench_function("encode_100", |b| {
        b.iter(|| encode_get_response(&items))
    });

    let encoded = encode_get_response(&items);
    group.bench_function("decode_100", |b| {
        b.iter(|| decode_get_response(&encoded))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// GetSpendBatch encode/decode
// ---------------------------------------------------------------------------

fn bench_get_spend_batch_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_get_spend_batch");

    let count = 1024;
    let items: Vec<WireGetSpendItem> = (0..count)
        .map(|i: u32| WireGetSpendItem {
            txid: test_txid(i),
            vout: i,
        })
        .collect();

    group.throughput(Throughput::Elements(count as u64));

    group.bench_function("encode_1024", |b| {
        b.iter(|| encode_get_spend_batch(&items))
    });

    let encoded = encode_get_spend_batch(&items);
    group.bench_function("decode_1024", |b| {
        b.iter(|| decode_get_spend_batch(&encoded))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// GetSpendResponse encode/decode
// ---------------------------------------------------------------------------

fn bench_get_spend_response_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_get_spend_response");

    let count = 1024;
    let items: Vec<WireGetSpendResult> = (0..count)
        .map(|_| WireGetSpendResult {
            status: 0,
            error_code: 0,
            slot_status: 0,
            spending_data: [0u8; 36],
        })
        .collect();

    group.throughput(Throughput::Elements(count as u64));

    group.bench_function("encode_1024", |b| {
        b.iter(|| encode_get_spend_response(&items))
    });

    let encoded = encode_get_spend_response(&items);
    group.bench_function("decode_1024", |b| {
        b.iter(|| decode_get_spend_response(&encoded))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// SparseErrors encode/decode
// ---------------------------------------------------------------------------

fn bench_sparse_errors_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_sparse_errors");

    let errors: Vec<BatchItemError> = (0..50u32)
        .map(|i| BatchItemError {
            item_index: i,
            error_code: 0x0001,
            error_data: vec![],
        })
        .collect();

    group.throughput(Throughput::Elements(50));

    group.bench_function("encode_50", |b| {
        b.iter(|| encode_sparse_errors(&errors))
    });

    let encoded = encode_sparse_errors(&errors);
    group.bench_function("decode_50", |b| {
        b.iter(|| decode_sparse_errors(&encoded))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// PartialWithSignals encode/decode
// ---------------------------------------------------------------------------

fn bench_partial_with_signals_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_partial_signals");

    let successes: Vec<BatchItemSuccess> = (0..100u32)
        .map(|i| BatchItemSuccess {
            item_index: i,
            signal: 1,
            block_ids: vec![42, 43],
        })
        .collect();

    let errors: Vec<BatchItemError> = (0..5u32)
        .map(|i| BatchItemError {
            item_index: i + 100,
            error_code: 0x0001,
            error_data: vec![],
        })
        .collect();

    group.throughput(Throughput::Elements(105));

    group.bench_function("encode_100s_5e", |b| {
        b.iter(|| encode_partial_with_signals(&successes, &errors))
    });

    let encoded = encode_partial_with_signals(&successes, &errors);
    group.bench_function("decode_100s_5e", |b| {
        b.iter(|| decode_partial_with_signals(&encoded))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// StreamChunk encode/decode
// ---------------------------------------------------------------------------

fn bench_stream_chunk_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_stream_chunk");

    let txid = test_txid(1);
    let data = vec![0xABu8; 65536]; // 64 KiB chunk

    group.throughput(Throughput::Bytes(65536));

    group.bench_function("encode_64k", |b| {
        b.iter(|| encode_stream_chunk(&txid, 0, &data))
    });

    let encoded = encode_stream_chunk(&txid, 0, &data);
    group.bench_function("decode_64k", |b| {
        b.iter(|| decode_stream_chunk(&encoded))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// StreamEnd encode/decode
// ---------------------------------------------------------------------------

fn bench_stream_end_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec_stream_end");

    let txid = test_txid(1);

    group.throughput(Throughput::Elements(1));

    group.bench_function("encode", |b| {
        b.iter(|| encode_stream_end(&txid, 1_048_576))
    });

    let encoded = encode_stream_end(&txid, 1_048_576);
    group.bench_function("decode", |b| {
        b.iter(|| decode_stream_end(&encoded))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_spend_batch_codec,
    bench_set_mined_batch_codec,
    bench_txid_batch_codec,
    bench_slot_item_batch_codec,
    bench_reassign_batch_codec,
    bench_unspend_batch_codec,
    bench_create_batch_codec,
    bench_get_batch_codec,
    bench_get_response_codec,
    bench_get_spend_batch_codec,
    bench_get_spend_response_codec,
    bench_sparse_errors_codec,
    bench_partial_with_signals_codec,
    bench_stream_chunk_codec,
    bench_stream_end_codec,
);
criterion_main!(benches);
