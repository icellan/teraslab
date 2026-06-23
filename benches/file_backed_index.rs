//! Criterion benchmarks comparing in-memory vs redb on-disk index backends.
//!
//! Each benchmark group creates ONE backend at a time to avoid OOM in
//! memory-constrained environments. The backend is selected by the benchmark
//! parameter, not pre-allocated in bulk.
//!
//! Index size controlled by BENCH_INDEX_SIZE env var: `2` (default) or `8`.
//!
//! Run with: `cargo bench --bench file_backed_index`
//! Docker:   `./benches/docker/run-constrained-bench.sh`

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::path::PathBuf;

use teraslab::config::{IndexBackendMode, IndexConfig};
use teraslab::index::backend::PrimaryBackend;
use teraslab::index::hashtable::{TxIndexEntry, TxKey};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_key(n: u64) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0..8].copy_from_slice(&n.to_le_bytes());
    txid[8..16].copy_from_slice(&(n.wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes());
    txid[16..24].copy_from_slice(&(n.wrapping_mul(0x517CC1B727220A95)).to_le_bytes());
    txid[24..32].copy_from_slice(&(n.wrapping_mul(0x6C62272E07BB0142)).to_le_bytes());
    TxKey { txid }
}

fn make_entry(n: u64) -> TxIndexEntry {
    TxIndexEntry {
        device_id: 0,
        record_offset: n * 4096,
        utxo_count: 2,
        block_entry_count: 0,
        tx_flags: 0,
        spent_utxos: 0,
        dah_or_preserve: 0,
        unmined_since: 0,
        generation: 0,
    }
}

struct Xorshift64 {
    state: u64,
}

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    #[inline(always)]
    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    #[inline(always)]
    fn next_bounded(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

fn selected_entries() -> u64 {
    match std::env::var("BENCH_INDEX_SIZE").as_deref() {
        Ok("8") => 94_000_000, // ~8GB hash table equivalent
        Ok("2") => 23_000_000, // ~2GB hash table equivalent
        Ok("1") => 10_000_000, // ~640MB — good for constrained testing
        _ => 5_000_000,        // ~320MB — fast default, still meaningful
    }
}

fn size_label() -> String {
    let e = selected_entries();
    format!("{}m", e / 1_000_000)
}

// ---------------------------------------------------------------------------
// Backend setup — creates ONE backend at a time
// ---------------------------------------------------------------------------

const POPULATE_BATCH_SIZE: usize = 10_000;

struct BenchBackend {
    backend: PrimaryBackend,
    _tmpdir: Option<tempfile::TempDir>,
}

fn create_backend(label: &str, entries: u64) -> BenchBackend {
    match label {
        "in_memory" => {
            eprintln!("  [{label}] Allocating for {entries} entries...");
            let mut backend = PrimaryBackend::new_in_memory(entries as usize).unwrap();
            eprintln!("  [{label}] Populating...");
            for i in 0..entries {
                backend.register(make_key(i), make_entry(i)).unwrap();
            }
            eprintln!("  [{label}] Done. {} entries", backend.len());
            BenchBackend {
                backend,
                _tmpdir: None,
            }
        }
        _ => {
            let cache_size: usize = match label {
                "redb_256mb" => 256 * 1024 * 1024,
                "redb_64mb" => 64 * 1024 * 1024,
                "redb_16mb" => 16 * 1024 * 1024,
                _ => 256 * 1024 * 1024,
            };
            let cache_mb = cache_size / (1024 * 1024);
            let tmpdir = tempfile::tempdir().unwrap();
            let config = IndexConfig {
                backend: IndexBackendMode::Redb,
                redb_path: tmpdir.path().join("bench-primary.redb"),
                redb_dah_path: PathBuf::from("/dev/null"),
                redb_unmined_path: PathBuf::from("/dev/null"),
                redb_cache_size: cache_size,
                ..IndexConfig::default()
            };
            eprintln!("  [{label}] Opening redb (cache: {cache_mb}MB)...");
            let mut backend = PrimaryBackend::new_on_disk(&config).unwrap();
            eprintln!(
                "  [{label}] Populating {entries} entries (batch size {POPULATE_BATCH_SIZE})..."
            );
            if let PrimaryBackend::OnDisk(redb) = &mut backend {
                let mut batch = Vec::with_capacity(POPULATE_BATCH_SIZE);
                for i in 0..entries {
                    batch.push((make_key(i), make_entry(i)));
                    if batch.len() >= POPULATE_BATCH_SIZE {
                        redb.register_batch(&batch).unwrap();
                        batch.clear();
                    }
                }
                if !batch.is_empty() {
                    redb.register_batch(&batch).unwrap();
                }
            }
            let file_mb = std::fs::metadata(tmpdir.path().join("bench-primary.redb"))
                .map(|m| m.len() / (1024 * 1024))
                .unwrap_or(0);
            eprintln!(
                "  [{label}] Done. {} entries, file={file_mb}MB",
                backend.len()
            );
            BenchBackend {
                backend,
                _tmpdir: Some(tmpdir),
            }
        }
    }
}

/// Which backends to test. In-memory is skipped for 8GB (would OOM).
fn backend_labels() -> Vec<&'static str> {
    let entries = selected_entries();
    // Skip in-memory for large indexes (>30M entries ≈ 2GB mmap).
    if entries <= 30_000_000 {
        vec!["in_memory", "redb_256mb", "redb_64mb"]
    } else {
        vec!["redb_256mb", "redb_64mb"]
    }
}

// ---------------------------------------------------------------------------
// Benchmark: Sequential lookup
// ---------------------------------------------------------------------------

fn bench_sequential_lookup(c: &mut Criterion) {
    let entries = selected_entries();
    let sz = size_label();
    let mut group = c.benchmark_group(format!("{sz}_sequential_lookup"));
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);
    group.warm_up_time(std::time::Duration::from_secs(3));
    group.measurement_time(std::time::Duration::from_secs(10));

    for label in backend_labels() {
        let setup = create_backend(label, entries);
        let backend = &setup.backend;
        let mut idx = 0u64;

        group.bench_with_input(BenchmarkId::from_parameter(label), &(), |b, _| {
            b.iter(|| {
                let result = backend.lookup(&make_key(idx));
                debug_assert!(result.is_some());
                idx += 1;
                if idx >= entries {
                    idx = 0;
                }
                result
            })
        });
        // Backend is dropped here — frees memory before next setup.
        drop(setup);
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: Random lookup (cold access)
// ---------------------------------------------------------------------------

fn bench_random_lookup(c: &mut Criterion) {
    let entries = selected_entries();
    let sz = size_label();
    let mut group = c.benchmark_group(format!("{sz}_random_lookup"));
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);
    group.warm_up_time(std::time::Duration::from_secs(3));
    group.measurement_time(std::time::Duration::from_secs(10));

    for label in backend_labels() {
        let setup = create_backend(label, entries);
        let backend = &setup.backend;
        let mut rng = Xorshift64::new(0xDEAD_BEEF_CAFE_BABE);

        group.bench_with_input(BenchmarkId::from_parameter(label), &(), |b, _| {
            b.iter(|| {
                let key_idx = rng.next_bounded(entries);
                let result = backend.lookup(&make_key(key_idx));
                debug_assert!(result.is_some());
                result
            })
        });
        drop(setup);
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: Hot/cold 90/10 lookup
// ---------------------------------------------------------------------------

fn bench_hot_cold_lookup(c: &mut Criterion) {
    let entries = selected_entries();
    let sz = size_label();
    let mut group = c.benchmark_group(format!("{sz}_hot_cold_90_10_lookup"));
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);
    group.warm_up_time(std::time::Duration::from_secs(3));
    group.measurement_time(std::time::Duration::from_secs(10));

    let hot_start = entries - entries / 10;
    let cold_end = hot_start;

    for label in backend_labels() {
        let setup = create_backend(label, entries);
        let backend = &setup.backend;
        let mut rng = Xorshift64::new(0xCAFE_BABE_DEAD_BEEF);

        group.bench_with_input(BenchmarkId::from_parameter(label), &(), |b, _| {
            b.iter(|| {
                let roll = rng.next() % 100;
                let key_idx = if roll < 90 {
                    hot_start + rng.next_bounded(entries - hot_start)
                } else {
                    rng.next_bounded(cold_end)
                };
                let result = backend.lookup(&make_key(key_idx));
                debug_assert!(result.is_some());
                result
            })
        });
        drop(setup);
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: Random mixed spend (lookup + update)
// ---------------------------------------------------------------------------

fn bench_random_mixed_spend(c: &mut Criterion) {
    let entries = selected_entries();
    let sz = size_label();
    let mut group = c.benchmark_group(format!("{sz}_random_mixed_spend"));
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);
    group.warm_up_time(std::time::Duration::from_secs(3));
    group.measurement_time(std::time::Duration::from_secs(10));

    for label in backend_labels() {
        let mut setup = create_backend(label, entries);
        let mut rng = Xorshift64::new(0x1234_5678_9ABC_DEF0);

        group.bench_with_input(BenchmarkId::from_parameter(label), &(), |b, _| {
            b.iter(|| {
                let key_idx = rng.next_bounded(entries);
                let key = make_key(key_idx);
                let entry = setup.backend.lookup(&key);
                debug_assert!(entry.is_some());
                setup
                    .backend
                    .update_cached_fields(&key, 0x01, 1, 1, 0, 0, key_idx as u32);
            })
        });
        drop(setup);
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: Cold burst — simulate block arrival
// ---------------------------------------------------------------------------

fn bench_block_burst(c: &mut Criterion) {
    let entries = selected_entries();
    let sz = size_label();
    let mut group = c.benchmark_group(format!("{sz}_block_burst_2000"));
    group.throughput(Throughput::Elements(4000));
    group.sample_size(20);
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.measurement_time(std::time::Duration::from_secs(15));

    // Redb only — in-memory doesn't need this test.
    let redb_labels: Vec<&str> = backend_labels()
        .into_iter()
        .filter(|l| l.starts_with("redb"))
        .collect();

    for label in redb_labels {
        let mut setup = create_backend(label, entries);
        let mut rng = Xorshift64::new(0xAAAA_BBBB_CCCC_DDDD);
        let mut next_key = entries;

        group.bench_with_input(BenchmarkId::from_parameter(label), &(), |b, _| {
            b.iter(|| {
                let batch_start = next_key;
                for i in 0..2000u64 {
                    let k = make_key(batch_start + i);
                    setup
                        .backend
                        .register(k, make_entry(batch_start + i))
                        .unwrap();
                }
                next_key += 2000;

                for _ in 0..2000 {
                    let key_idx = rng.next_bounded(entries);
                    let key = make_key(key_idx);
                    let _ = setup.backend.lookup(&key);
                    setup
                        .backend
                        .update_cached_fields(&key, 0x01, 1, 1, 0, 0, key_idx as u32);
                }

                for i in 0..2000u64 {
                    setup.backend.unregister(&make_key(batch_start + i));
                }
            })
        });
        drop(setup);
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: Sustained throughput
// ---------------------------------------------------------------------------

fn bench_sustained_throughput(c: &mut Criterion) {
    let entries = selected_entries();
    let sz = size_label();
    let mut group = c.benchmark_group(format!("{sz}_sustained_random_ops"));
    group.throughput(Throughput::Elements(100));
    group.sample_size(30);
    group.warm_up_time(std::time::Duration::from_secs(3));
    group.measurement_time(std::time::Duration::from_secs(15));

    let redb_labels: Vec<&str> = backend_labels()
        .into_iter()
        .filter(|l| l.starts_with("redb"))
        .collect();

    for label in redb_labels {
        let mut setup = create_backend(label, entries);
        let mut rng = Xorshift64::new(0xFEED_FACE_DEAD_C0DE);

        group.bench_with_input(BenchmarkId::from_parameter(label), &(), |b, _| {
            b.iter(|| {
                for _ in 0..50 {
                    let key_idx = rng.next_bounded(entries);
                    let _ = setup.backend.lookup(&make_key(key_idx));
                }
                for _ in 0..30 {
                    let key_idx = rng.next_bounded(entries);
                    let key = make_key(key_idx);
                    setup
                        .backend
                        .update_cached_fields(&key, 0x01, 1, 1, 0, 0, key_idx as u32);
                }
                for i in 0..20u64 {
                    let miss_key = make_key(entries + rng.next_bounded(1_000_000) + i);
                    let result = setup.backend.lookup(&miss_key);
                    debug_assert!(result.is_none());
                }
            })
        });
        drop(setup);
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_lookup,
    bench_random_lookup,
    bench_hot_cold_lookup,
    bench_random_mixed_spend,
    bench_block_burst,
    bench_sustained_throughput,
);
criterion_main!(benches);
