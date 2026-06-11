//! Deterministic fuzz-smoke test for the wire-protocol parsers (N-04 /
//! LMNH-17, CI-enforced half).
//!
//! The binary protocol decoders are the untrusted network boundary. This
//! test feeds two classes of hostile input through EVERY decoder at that
//! boundary and asserts the contract "Ok or typed error, never panic":
//!
//! 1. fully random byte strings (seeded xorshift64 RNG, no time/seed
//!    flakiness — every run executes the identical byte sequence);
//! 2. structure-aware mutations of VALID encoded payloads and frames:
//!    bit flips, length/count-field corruption (including `u32::MAX`
//!    inflation), and truncation.
//!
//! Any panic inside a decoder fails the test outright (the harness runs
//! decoders directly, not under `catch_unwind`). Errors are typed by
//! construction — the tally closures only accept `Result<_, FrameError>` /
//! `Result<_, CodecError>` — and each error's `Display` impl is exercised
//! so formatting code is covered too.
//!
//! Detection-power assertion: per-decoder invocation counters are tracked
//! and the test asserts every decoder was invoked thousands of times AND
//! produced both Ok and Err outcomes (proving the harness reaches both
//! the accept and reject paths of every parser). The counts are printed
//! at the end of the run.
//!
//! Deep-exploration counterpart: the cargo-fuzz target in
//! `fuzz/fuzz_targets/decode_request.rs` (see `fuzz/README.md`).

#![allow(clippy::disallowed_macros)] // integration tests may eprintln! diagnostics

use std::collections::BTreeMap;

use bytes::Bytes;

use teraslab::protocol::codec::*;
use teraslab::protocol::frame::{RequestFrame, ResponseFrame, try_decode_frames};
use teraslab::protocol::opcodes::OP_SPEND_BATCH;

// ---------------------------------------------------------------------------
// Deterministic RNG (xorshift64, same algorithm as tests/workload/generator.rs)
// ---------------------------------------------------------------------------

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
    fn next_u32(&mut self) -> u32 {
        (self.next_u64() & 0xFFFF_FFFF) as u32
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
    fn byte(&mut self) -> u8 {
        (self.next_u64() & 0xFF) as u8
    }
    fn fill(&mut self, buf: &mut [u8]) {
        for b in buf.iter_mut() {
            *b = self.byte();
        }
    }
    fn array<const N: usize>(&mut self) -> [u8; N] {
        let mut a = [0u8; N];
        self.fill(&mut a);
        a
    }
}

// ---------------------------------------------------------------------------
// Decoder harness: every untrusted-boundary decoder, with ok/err tallies
// ---------------------------------------------------------------------------

/// Per-call batch cap. Small enough that count-field inflation reliably
/// trips `BatchTooLarge`, large enough that valid corpus payloads decode Ok.
const MAX_BATCH: u32 = 1024;

/// Shared-params length used for `decode_txid_batch_checked` (matches the
/// set_conflicting wire shape: value(1) + cbh(4) + bhr(4)).
const TXID_SHARED_LEN: usize = 9;

#[derive(Default, Clone, Copy)]
struct Tally {
    ok: u64,
    err: u64,
}

struct Counts(BTreeMap<&'static str, Tally>);

impl Counts {
    fn new() -> Self {
        Counts(BTreeMap::new())
    }
    fn record<T, E: std::fmt::Display>(&mut self, name: &'static str, r: Result<T, E>) {
        let t = self.0.entry(name).or_default();
        match r {
            Ok(_) => t.ok += 1,
            Err(e) => {
                // Exercise the error's Display impl too — a panic in
                // formatting is also a parser-boundary bug.
                let _ = e.to_string();
                t.err += 1;
            }
        }
    }
}

/// Feed one byte buffer through every decoder at the untrusted boundary.
/// Panics propagate and fail the test.
fn feed_all(data: &[u8], c: &mut Counts) {
    // Frame-level decoders.
    c.record("frame::RequestFrame::decode", RequestFrame::decode(data));
    c.record(
        "frame::RequestFrame::decode_bytes",
        RequestFrame::decode_bytes(Bytes::copy_from_slice(data)),
    );
    c.record("frame::ResponseFrame::decode", ResponseFrame::decode(data));
    c.record("frame::try_decode_frames", try_decode_frames(data));

    // Per-opcode checked payload decoders (the complete set in
    // src/protocol/codec.rs — keep in sync with the cargo-fuzz target).
    c.record(
        "codec::decode_spend_batch_checked",
        decode_spend_batch_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_set_mined_batch_checked",
        decode_set_mined_batch_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_txid_batch_checked",
        decode_txid_batch_checked(data, TXID_SHARED_LEN, MAX_BATCH),
    );
    c.record(
        "codec::decode_slot_item_batch_checked",
        decode_slot_item_batch_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_reassign_batch_checked",
        decode_reassign_batch_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_unspend_batch_checked",
        decode_unspend_batch_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_create_batch_checked",
        decode_create_batch_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_get_batch_checked",
        decode_get_batch_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_get_response_checked",
        decode_get_response_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_partial_with_signals_checked",
        decode_partial_with_signals_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_sparse_errors_checked",
        decode_sparse_errors_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_get_spend_batch_checked",
        decode_get_spend_batch_checked(data, MAX_BATCH),
    );
    c.record(
        "codec::decode_get_spend_response_checked",
        decode_get_spend_response_checked(data, MAX_BATCH),
    );
}

// ---------------------------------------------------------------------------
// Valid corpus: one well-formed encoding per wire shape
// ---------------------------------------------------------------------------

/// Build a fresh set of valid payloads with rng-chosen small item counts.
fn valid_corpus(rng: &mut Rng) -> Vec<Vec<u8>> {
    let n = 1 + rng.below(3); // 1..=3 items per batch

    let spend_items: Vec<WireSpendItem> = (0..n)
        .map(|_| WireSpendItem {
            txid: rng.array(),
            vout: rng.next_u32() % 16,
            utxo_hash: rng.array(),
            spending_data: rng.array(),
        })
        .collect();
    let spend_params = SpendBatchParams {
        ignore_conflicting: false,
        ignore_locked: true,
        current_block_height: 2000,
        block_height_retention: 288,
    };

    let set_mined_params = SetMinedBatchParams {
        block_id: rng.next_u32(),
        block_height: 2000,
        subtree_idx: 3,
        on_longest_chain: true,
        unset_mined: false,
        current_block_height: 2000,
        block_height_retention: 288,
    };
    let txids: Vec<[u8; 32]> = (0..n).map(|_| rng.array()).collect();

    let slot_items: Vec<WireSlotItem> = (0..n)
        .map(|_| WireSlotItem {
            txid: rng.array(),
            vout: rng.next_u32() % 16,
            utxo_hash: rng.array(),
        })
        .collect();

    let reassign_params = ReassignBatchParams {
        block_height: 2000,
        spendable_after: 1000,
    };
    let reassign_items: Vec<WireReassignItem> = (0..n)
        .map(|_| WireReassignItem {
            txid: rng.array(),
            vout: rng.next_u32() % 16,
            utxo_hash: rng.array(),
            new_utxo_hash: rng.array(),
        })
        .collect();

    let unspend_params = UnspendBatchParams {
        current_block_height: 2000,
        block_height_retention: 288,
    };
    let unspend_items: Vec<WireUnspendItem> = (0..n)
        .map(|_| WireUnspendItem {
            txid: rng.array(),
            vout: rng.next_u32() % 16,
            utxo_hash: rng.array(),
            spending_data: rng.array(),
        })
        .collect();

    let create_items: Vec<WireCreateItem> = (0..n)
        .map(|i| WireCreateItem {
            txid: rng.array(),
            tx_version: 1,
            locktime: 0,
            fee: 500,
            size_in_bytes: 250,
            extended_size: 0,
            is_coinbase: i == 0,
            spending_height: 0,
            created_at: 1710000000000,
            flags: 0,
            utxo_hashes: (0..1 + rng.below(3)).map(|_| rng.array()).collect(),
            cold_data: {
                let mut v = vec![0u8; rng.below(48)];
                rng.fill(&mut v);
                v
            },
            block_height: 1000,
            mined_block_id: (i % 2 == 0).then(|| rng.next_u32()),
            mined_block_height: (i % 2 == 0).then_some(2000),
            mined_subtree_idx: (i % 2 == 0).then_some(1),
            parent_txids: (0..rng.below(3)).map(|_| rng.array()).collect(),
        })
        .collect();

    let get_results: Vec<WireGetResult> = (0..n)
        .map(|_| WireGetResult {
            status: 0,
            data: {
                let mut v = vec![0u8; rng.below(32)];
                rng.fill(&mut v);
                v
            },
        })
        .collect();

    let successes: Vec<BatchItemSuccess> = (0..n)
        .map(|i| BatchItemSuccess {
            item_index: i as u32,
            signal: 1,
            block_ids: vec![rng.next_u32(), rng.next_u32()],
        })
        .collect();
    let errors: Vec<BatchItemError> = (0..n)
        .map(|i| BatchItemError {
            item_index: i as u32,
            error_code: 2,
            error_data: rng.array::<36>().to_vec(),
        })
        .collect();

    let get_spend_items: Vec<WireGetSpendItem> = (0..n)
        .map(|_| WireGetSpendItem {
            txid: rng.array(),
            vout: rng.next_u32() % 16,
            utxo_hash: rng.array(),
        })
        .collect();
    let get_spend_results: Vec<WireGetSpendResult> = (0..n)
        .map(|_| WireGetSpendResult {
            status: 0,
            error_code: 0,
            slot_status: 1,
            spending_data: rng.array(),
        })
        .collect();

    let mut shared = [0u8; TXID_SHARED_LEN];
    rng.fill(&mut shared);

    let payloads = vec![
        encode_spend_batch(&spend_params, &spend_items),
        encode_set_mined_batch(&set_mined_params, &txids),
        encode_txid_batch(&txids, &shared),
        encode_slot_item_batch(&slot_items),
        encode_reassign_batch(&reassign_params, &reassign_items),
        encode_unspend_batch(&unspend_params, &unspend_items),
        encode_create_batch(&create_items),
        encode_get_batch(FieldMask::ALL, &txids),
        encode_get_response(&get_results),
        encode_partial_with_signals(&successes, &errors),
        encode_sparse_errors(&errors),
        encode_get_spend_batch(&get_spend_items),
        encode_get_spend_response(&get_spend_results),
    ];

    // Plus a fully framed request wrapping one of the payloads.
    let frame = RequestFrame {
        request_id: rng.next_u64(),
        op_code: OP_SPEND_BATCH,
        flags: 0,
        payload: Bytes::from(payloads[rng.below(payloads.len())].clone()),
    };
    let mut out = payloads;
    out.push(frame.encode());
    out
}

// ---------------------------------------------------------------------------
// Mutators
// ---------------------------------------------------------------------------

/// Apply one structure-aware mutation in place. Returns a description tag
/// (unused except to keep the match exhaustive and readable).
fn mutate(rng: &mut Rng, buf: &mut Vec<u8>) {
    if buf.is_empty() {
        buf.push(rng.byte());
        return;
    }
    match rng.below(4) {
        // Bit flip at a random position.
        0 => {
            let i = rng.below(buf.len());
            buf[i] ^= 1 << rng.below(8);
        }
        // Length/count-field corruption: overwrite a 4-byte LE word at a
        // random aligned-ish offset with an extreme value (u32::MAX, huge,
        // off-by-one, zero). The first word of every codec payload is the
        // count field, so offset 0 is weighted in.
        1 => {
            let val: u32 = match rng.below(4) {
                0 => u32::MAX,
                1 => 0x7FFF_FFFF,
                2 => buf.len() as u32 + 1,
                _ => 0,
            };
            let off = if rng.below(2) == 0 || buf.len() < 8 {
                0
            } else {
                rng.below(buf.len().saturating_sub(4).max(1))
            };
            let end = (off + 4).min(buf.len());
            buf[off..end].copy_from_slice(&val.to_le_bytes()[..end - off]);
        }
        // Truncation at a random point.
        2 => {
            let keep = rng.below(buf.len());
            buf.truncate(keep);
        }
        // Splice random garbage over a random span.
        _ => {
            let start = rng.below(buf.len());
            let len = rng.below(buf.len() - start + 1).min(16);
            for b in &mut buf[start..start + len] {
                *b = rng.byte();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The smoke test
// ---------------------------------------------------------------------------

/// Iterations per generator. Sized so the whole test runs in ~1-2 s in
/// debug CI (measured ~1.3 s on an M-series laptop; see N-04 notes).
const RANDOM_ITERS: usize = 1500;
const MUTATION_ITERS: usize = 1500;

#[test]
fn wire_parsers_survive_random_and_mutated_input() {
    let mut rng = Rng::new(0x7e7a_51ab_f022_5eed);
    let mut counts = Counts::new();

    // Generator 1: fully random byte strings, length 0..512.
    for i in 0..RANDOM_ITERS {
        let len = rng.below(512);
        let mut buf = vec![0u8; len];
        rng.fill(&mut buf);
        // Every 8th buffer: force a plausible length prefix so the frame
        // decoders get past the TooLarge/BelowMinimum gate more often.
        if i % 8 == 0 && len >= 4 {
            let body = (len as u32).saturating_sub(4 + rng.next_u32() % 8);
            buf[0..4].copy_from_slice(&body.to_le_bytes());
        }
        feed_all(&buf, &mut counts);
    }

    // Generator 2: structure-aware mutations of valid encodings. The
    // pristine encoding is fed first (guarantees every decoder sees Ok
    // input), then 1..=3 stacked mutations.
    let mut produced = 0usize;
    'outer: loop {
        let corpus = valid_corpus(&mut rng);
        for valid in corpus {
            feed_all(&valid, &mut counts);
            let mut mutated = valid.clone();
            for _ in 0..1 + rng.below(3) {
                mutate(&mut rng, &mut mutated);
            }
            feed_all(&mutated, &mut counts);
            produced += 2;
            if produced >= MUTATION_ITERS {
                break 'outer;
            }
        }
    }

    // Detection-power assertions: every decoder was exercised on both its
    // accept and reject paths, many times over.
    let expected_decoders = [
        "frame::RequestFrame::decode",
        "frame::RequestFrame::decode_bytes",
        "frame::ResponseFrame::decode",
        "frame::try_decode_frames",
        "codec::decode_spend_batch_checked",
        "codec::decode_set_mined_batch_checked",
        "codec::decode_txid_batch_checked",
        "codec::decode_slot_item_batch_checked",
        "codec::decode_reassign_batch_checked",
        "codec::decode_unspend_batch_checked",
        "codec::decode_create_batch_checked",
        "codec::decode_get_batch_checked",
        "codec::decode_get_response_checked",
        "codec::decode_partial_with_signals_checked",
        "codec::decode_sparse_errors_checked",
        "codec::decode_get_spend_batch_checked",
        "codec::decode_get_spend_response_checked",
    ];
    assert_eq!(
        counts.0.len(),
        expected_decoders.len(),
        "harness decoder set drifted from the expected list"
    );

    let total_inputs = (RANDOM_ITERS + produced) as u64;
    eprintln!("wire_fuzz_smoke: {total_inputs} inputs through {} decoders", counts.0.len());
    for name in expected_decoders {
        let t = counts.0.get(name).copied().unwrap_or_default();
        eprintln!("  {name}: ok={} err={}", t.ok, t.err);
        assert_eq!(
            t.ok + t.err,
            total_inputs,
            "{name}: was not invoked on every input"
        );
        assert!(t.ok > 0, "{name}: never returned Ok — harness lacks valid input for it");
        assert!(t.err > 0, "{name}: never returned Err — harness lacks hostile input for it");
    }
}

/// `try_decode_frames` must report corrupt (non-truncated) trailing bytes
/// as a typed error, and a clean multi-frame stream of valid frames must
/// round-trip — checked here on mutated streams so the smoke suite also
/// covers the stream-level (multi-frame) entry point with exact values.
#[test]
fn frame_stream_mutations_yield_typed_results() {
    let mut rng = Rng::new(0xdead_beef_0042_cafe);
    for _ in 0..200 {
        // Build a 3-frame valid stream.
        let frames: Vec<RequestFrame> = (0..3)
            .map(|i| RequestFrame {
                request_id: i,
                op_code: OP_SPEND_BATCH,
                flags: 0,
                payload: {
                    let mut p = vec![0u8; rng.below(64)];
                    rng.fill(&mut p);
                    Bytes::from(p)
                },
            })
            .collect();
        let mut stream = Vec::new();
        for f in &frames {
            stream.extend_from_slice(&f.encode());
        }

        // Valid stream: decodes to exactly the 3 frames, consuming all bytes.
        let (decoded, consumed) = try_decode_frames(&stream).unwrap();
        assert_eq!(decoded, frames);
        assert_eq!(consumed, stream.len());

        // Mutated stream: must not panic; Err must be a typed FrameError
        // (Display exercised), Ok must consume no more than the buffer.
        let mut mutated = stream.clone();
        for _ in 0..1 + rng.below(3) {
            mutate(&mut rng, &mut mutated);
        }
        match try_decode_frames(&mutated) {
            Ok((frames, consumed)) => {
                assert!(consumed <= mutated.len());
                // Every request frame consumes >= 16 wire bytes
                // (length prefix 4 + minimum body 12), so the frame
                // count is hard-bounded by the buffer size.
                assert!(
                    frames.len() <= mutated.len() / 16,
                    "decoded {} frames from {} bytes",
                    frames.len(),
                    mutated.len()
                );
            }
            Err(e) => {
                let _ = e.to_string();
            }
        }
    }
}
