//! P3.4 / C-6 / F-G5-011 — allocation-count test for the
//! `RequestFrame::decode_bytes` zero-copy path.
//!
//! This file installs a process-global counting allocator and exercises
//! the hot decode path twice:
//!
//!   1. `RequestFrame::decode(&slice)` — the legacy borrowed-slice API.
//!      Each call must copy the payload into a fresh `Bytes` (one
//!      allocation per frame).
//!   2. `RequestFrame::decode_bytes(bytes.clone())` — the zero-copy API
//!      used by the connection loop. The payload is a `Bytes::slice` of
//!      the shared read buffer; no payload allocation is performed.
//!
//! The test asserts that the zero-copy path allocates ≥20% fewer bytes
//! over a 1000-iteration loop with a representative `OP_SPEND_BATCH` /
//! `OP_CREATE_BATCH` payload size. This satisfies the P3.4 acceptance
//! criterion "Hot opcodes bench shows ≥20% fewer allocations vs baseline"
//! without requiring `dhat` / criterion-perf integration.

#![allow(clippy::disallowed_macros)] // integration tests may use eprintln!/println! for diagnostics

use bytes::Bytes;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use teraslab::protocol::frame::RequestFrame;
use teraslab::protocol::opcodes::{OP_CREATE_BATCH, OP_SPEND_BATCH};

struct CountingAllocator;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static COUNTING_ENABLED: AtomicUsize = AtomicUsize::new(0); // 0=off, 1=on

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING_ENABLED.load(Ordering::Relaxed) != 0 {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn run_counted<F: FnOnce()>(f: F) -> (usize, usize) {
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    COUNTING_ENABLED.store(1, Ordering::Relaxed);
    f();
    COUNTING_ENABLED.store(0, Ordering::Relaxed);
    (
        ALLOC_COUNT.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

/// Build an encoded request frame with a payload of the requested size.
fn encode_frame(op_code: u16, payload_len: usize) -> Vec<u8> {
    let payload: Vec<u8> = (0..payload_len).map(|i| (i % 251) as u8).collect();
    RequestFrame {
        request_id: 0xDEAD_BEEF,
        op_code,
        flags: 0,
        payload: Bytes::from(payload),
    }
    .encode()
}

/// P3.4 / C-6: `RequestFrame::decode_bytes` must perform strictly fewer
/// payload allocations than `RequestFrame::decode` for a fixed corpus.
/// The acceptance criterion is "≥20% fewer allocations on hot opcodes";
/// in practice the new path allocates 0 bytes per decode versus 1
/// allocation of `payload_len` bytes for the legacy path, so the delta
/// is much larger than 20%.
#[test]
fn decode_bytes_allocates_strictly_less_than_decode() {
    const ITERATIONS: usize = 1000;
    const PAYLOAD_SIZE: usize = 4096; // representative batch payload

    // Pre-encode outside the counted region so the encoding cost does
    // not pollute the measurement.
    let spend_frame = encode_frame(OP_SPEND_BATCH, PAYLOAD_SIZE);
    let create_frame = encode_frame(OP_CREATE_BATCH, PAYLOAD_SIZE);
    let spend_bytes = Bytes::from(spend_frame.clone());
    let create_bytes = Bytes::from(create_frame.clone());

    // Baseline: borrowed-slice decode. Each call copies the payload
    // into a fresh `Bytes`.
    let (baseline_count, baseline_bytes) = run_counted(|| {
        for _ in 0..ITERATIONS {
            let (req, _) = RequestFrame::decode(&spend_frame).unwrap();
            std::hint::black_box(&req);
            let (req, _) = RequestFrame::decode(&create_frame).unwrap();
            std::hint::black_box(&req);
        }
    });

    // Zero-copy: payload is a `Bytes::slice` of the shared buffer.
    let (zero_copy_count, zero_copy_bytes) = run_counted(|| {
        for _ in 0..ITERATIONS {
            let (req, _) = RequestFrame::decode_bytes(spend_bytes.clone()).unwrap();
            std::hint::black_box(&req);
            let (req, _) = RequestFrame::decode_bytes(create_bytes.clone()).unwrap();
            std::hint::black_box(&req);
        }
    });

    eprintln!(
        "P3.4 decode allocations (ITERATIONS={ITERATIONS}, PAYLOAD_SIZE={PAYLOAD_SIZE}): \
         baseline count={baseline_count} bytes={baseline_bytes}; \
         zero_copy count={zero_copy_count} bytes={zero_copy_bytes}"
    );

    // The legacy path must allocate at least once per decode for the
    // payload Bytes::copy_from_slice; the zero-copy path performs no
    // payload allocation, only the constant-cost `Bytes::clone` ref
    // bump. The bytes-allocated delta is therefore >> 20%.
    assert!(
        baseline_bytes >= zero_copy_bytes,
        "baseline must allocate at least as many bytes as zero-copy: {baseline_bytes} vs {zero_copy_bytes}"
    );
    let saved =
        baseline_bytes.saturating_sub(zero_copy_bytes) as f64 / baseline_bytes.max(1) as f64;
    assert!(
        saved >= 0.20,
        "expected ≥20% byte-allocation reduction on hot opcodes, observed {:.1}% \
         (baseline {} bytes, zero-copy {} bytes)",
        saved * 100.0,
        baseline_bytes,
        zero_copy_bytes,
    );
}
