//! Criterion benchmarks for standalone Index operations.
//!
//! Covers lookup, register, unregister, and update_cached_fields on the
//! primary hash table index — the critical path for every engine operation.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use teraslab::index::{Index, TxIndexEntry, TxKey};

fn make_tx_key(n: u32) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0..4].copy_from_slice(&n.to_le_bytes());
    txid[8..12].copy_from_slice(&(n.wrapping_mul(0x9E37)).to_le_bytes());
    txid[16..18].copy_from_slice(&(n as u16).to_le_bytes());
    TxKey { txid }
}

fn make_entry(n: u32) -> TxIndexEntry {
    TxIndexEntry {
        device_id: 0,
        record_offset: (n as u64) * 4096,
        utxo_count: 5,
        block_entry_count: 1,
        tx_flags: 0,
        spent_utxos: 0,
        dah_or_preserve: 0,
        unmined_since: 0,
        generation: 0,
    }
}

fn populated_index(count: usize) -> Index {
    let mut index = Index::new(count + 1000).unwrap();
    for i in 0..count as u32 {
        index.register(make_tx_key(i), make_entry(i)).unwrap();
    }
    index
}

fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_lookup");

    for &count in &[10_000usize, 100_000, 500_000] {
        let index = populated_index(count);
        let mut i = 0u32;

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("hit", count),
            &count,
            |b, _| {
                b.iter(|| {
                    let key = make_tx_key(i);
                    let _ = index.lookup(&key);
                    i += 1;
                    if i >= count as u32 {
                        i = 0;
                    }
                })
            },
        );
    }

    // Benchmark lookup misses (keys that don't exist).
    let index = populated_index(100_000);
    let mut i = 1_000_000u32;
    group.throughput(Throughput::Elements(1));
    group.bench_function("miss_100k", |b| {
        b.iter(|| {
            let key = make_tx_key(i);
            let _ = index.lookup(&key);
            i += 1;
        })
    });

    group.finish();
}

fn bench_register(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_register");
    group.throughput(Throughput::Elements(1));

    group.bench_function("into_empty", |b| {
        let mut index = Index::new(200_000).unwrap();
        let mut i = 0u32;
        b.iter(|| {
            index.register(make_tx_key(i), make_entry(i)).unwrap();
            i += 1;
        })
    });

    // Register into a pre-populated index (higher load factor).
    group.bench_function("into_loaded_100k", |b| {
        let mut index = populated_index(100_000);
        let mut i = 500_000u32;
        b.iter(|| {
            index.register(make_tx_key(i), make_entry(i)).unwrap();
            i += 1;
        })
    });

    group.finish();
}

fn bench_unregister(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_unregister");
    group.throughput(Throughput::Elements(1));

    let mut index = populated_index(100_000);
    let mut i = 0u32;

    group.bench_function("remove_100k", |b| {
        b.iter(|| {
            let key = make_tx_key(i);
            let _ = index.unregister(&key);
            i += 1;
            if i >= 100_000 {
                // Re-populate so we don't run out of keys.
                for j in 0..100_000u32 {
                    let _ = index.register(make_tx_key(j), make_entry(j));
                }
                i = 0;
            }
        })
    });

    group.finish();
}

fn bench_update_cached_fields(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_update_cached");
    group.throughput(Throughput::Elements(1));

    let mut index = populated_index(100_000);
    let mut i = 0u32;

    group.bench_function("update_100k", |b| {
        b.iter(|| {
            let key = make_tx_key(i);
            index.update_cached_fields(&key, 0x01, 2, i, 0, 0, i + 1);
            i += 1;
            if i >= 100_000 {
                i = 0;
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_lookup,
    bench_register,
    bench_unregister,
    bench_update_cached_fields,
);
criterion_main!(benches);
