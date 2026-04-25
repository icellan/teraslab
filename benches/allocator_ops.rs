//! Criterion benchmarks for the SlotAllocator.
//!
//! Covers allocation throughput, free (with coalescing), and allocation under
//! fragmented freelist conditions.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};

fn make_device(mb: u64) -> Arc<dyn BlockDevice> {
    Arc::new(MemoryDevice::new(mb * 1024 * 1024, 4096).unwrap())
}

fn bench_allocate_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocator_allocate");
    group.throughput(Throughput::Elements(1));

    for &size in &[4096u64, 16384, 65536] {
        group.bench_with_input(
            BenchmarkId::new("sequential", size),
            &size,
            |b, &alloc_size| {
                let dev = make_device(1024);
                let mut alloc = SlotAllocator::new(dev).unwrap();
                b.iter(|| {
                    let _ = alloc.allocate(alloc_size);
                })
            },
        );
    }

    group.finish();
}

fn bench_free(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocator_free");
    group.throughput(Throughput::Elements(1));

    // Pre-allocate a bunch of slots, then benchmark freeing them.
    // After each full pass, re-allocate to keep the benchmark running.
    group.bench_function("free_4096", |b| {
        let dev = make_device(512);
        let mut alloc = SlotAllocator::new(dev).unwrap();
        let mut offsets: Vec<u64> = (0..10_000).map(|_| alloc.allocate(4096).unwrap()).collect();
        let mut idx = 0;

        b.iter(|| {
            alloc.free(offsets[idx], 4096).unwrap();
            idx += 1;
            if idx >= offsets.len() {
                // Re-allocate everything from the freelist.
                offsets.clear();
                for _ in 0..10_000 {
                    offsets.push(alloc.allocate(4096).unwrap());
                }
                idx = 0;
            }
        })
    });

    group.finish();
}

fn bench_allocate_fragmented(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocator_fragmented");
    group.throughput(Throughput::Elements(1));

    // Create fragmentation: allocate many slots, free every other one.
    group.bench_function("alloc_after_frag", |b| {
        let dev = make_device(512);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        let offsets: Vec<u64> = (0..20_000).map(|_| alloc.allocate(4096).unwrap()).collect();

        // Free every other slot to create fragmentation.
        for i in (0..20_000).step_by(2) {
            alloc.free(offsets[i], 4096).unwrap();
        }

        b.iter(|| {
            // Allocate from freelist (best-fit search).
            let off = alloc.allocate(4096).unwrap();
            // Return it so we don't exhaust the freelist.
            alloc.free(off, 4096).unwrap();
        })
    });

    // Fragmented with varying sizes — stresses best-fit search.
    group.bench_function("alloc_varied_sizes", |b| {
        let dev = make_device(512);
        let mut alloc = SlotAllocator::new(dev).unwrap();

        let mut offsets = Vec::new();
        for i in 0..5_000u64 {
            let size = 4096 * (1 + (i % 4)); // 4K, 8K, 12K, 16K
            offsets.push((alloc.allocate(size).unwrap(), size));
        }

        // Free every other slot.
        for i in (0..5_000).step_by(2) {
            alloc.free(offsets[i].0, offsets[i].1).unwrap();
        }

        let mut alloc_size_idx = 0u64;
        b.iter(|| {
            let size = 4096 * (1 + (alloc_size_idx % 4));
            let off = alloc.allocate(size).unwrap();
            alloc.free(off, size).unwrap();
            alloc_size_idx += 1;
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_allocate_sequential,
    bench_free,
    bench_allocate_fragmented,
);
criterion_main!(benches);
