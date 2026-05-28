//! F-G9-005 regression: a reader concurrent with `put` AND/OR
//! `begin_stream → finish` to the same key must never observe a transient
//! `DigestMismatch` between payload and sidecar.
//!
//! Pre-fix behavior: `FileStreamWriter::finish` renamed the payload before
//! writing the sidecar; a reader landing in that window saw the new payload
//! against the previous put's stale sidecar — transient DigestMismatch.
//!
//! Fix: `get` and `stream_to` take the per-key lock briefly so the
//! payload+sidecar pair they observe is consistent with the writer that
//! most recently held the lock.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use teraslab::storage::blobstore::{BlobError, BlobStore, FileBlobStore};

fn key_for(byte: u8) -> [u8; 32] {
    [byte; 32]
}

#[test]
fn put_stream_and_readers_never_observe_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(FileBlobStore::new(dir.path(), 2));
    let key = key_for(0xA5);

    // Seed an initial valid value so readers always have something to read.
    store.put(&key, &vec![0u8; 4096]).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let mismatches = Arc::new(AtomicU64::new(0));
    let put_iterations = Arc::new(AtomicU64::new(0));
    let stream_iterations = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    // Writer 1 — `put` loop on the same key.
    {
        let s = store.clone();
        let stop = stop.clone();
        let put_iterations = put_iterations.clone();
        handles.push(thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let payload = vec![(i & 0xFF) as u8; 2048 + (i as usize % 1024)];
                s.put(&key, &payload).unwrap();
                i = i.wrapping_add(1);
                put_iterations.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Writer 2 — `begin_stream` -> chunks -> `finish` loop on the same key.
    {
        let s = store.clone();
        let stop = stop.clone();
        let stream_iterations = stream_iterations.clone();
        handles.push(thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let mut writer = s.begin_stream(&key).unwrap();
                let chunk = vec![((i + 128) & 0xFF) as u8; 1024];
                writer.write_chunk(&chunk).unwrap();
                writer.write_chunk(&chunk).unwrap();
                writer.finish().unwrap();
                i = i.wrapping_add(1);
                stream_iterations.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Four readers: half use `get`, half use `stream_to`.
    for r in 0..4 {
        let s = store.clone();
        let stop = stop.clone();
        let mismatches = mismatches.clone();
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let res = if r % 2 == 0 {
                    s.get(&key).map(|_| ())
                } else {
                    let mut sink: Vec<u8> = Vec::new();
                    s.stream_to(&key, &mut sink).map(|_| ())
                };
                if let Err(BlobError::DigestMismatch { .. }) = res {
                    mismatches.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // Let the contention run a bit, then quiesce. On loaded CI runners
    // the 400 ms soak can complete zero writer iterations and the
    // mismatch == 0 check passes vacuously — defend against that by
    // extending the soak until both writers have completed at least
    // `MIN_ITERATIONS` cycles, capped at a hard ceiling so a genuinely
    // wedged writer eventually fails the test rather than hanging CI.
    const MIN_ITERATIONS: u64 = 10;
    const HARD_CEILING: Duration = Duration::from_secs(10);
    let start = std::time::Instant::now();
    loop {
        thread::sleep(Duration::from_millis(50));
        let p = put_iterations.load(Ordering::Relaxed);
        let s = stream_iterations.load(Ordering::Relaxed);
        if p >= MIN_ITERATIONS
            && s >= MIN_ITERATIONS
            && start.elapsed() >= Duration::from_millis(400)
        {
            break;
        }
        if start.elapsed() >= HARD_CEILING {
            break;
        }
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let put = put_iterations.load(Ordering::Relaxed);
    let stream = stream_iterations.load(Ordering::Relaxed);
    assert!(
        put >= MIN_ITERATIONS && stream >= MIN_ITERATIONS,
        "vacuous-pass guard: each writer must complete at least \
         {MIN_ITERATIONS} cycles; got put={put}, stream={stream}. \
         A wedged writer would otherwise let the mismatch=0 assertion \
         pass without actually exercising the race."
    );

    let observed = mismatches.load(Ordering::Relaxed);
    assert_eq!(
        observed, 0,
        "readers observed {observed} transient DigestMismatch errors — \
         F-G9-005 regression: payload/sidecar pair not atomic to readers"
    );
}
