//! Fuzz target for the wire-protocol untrusted boundary (N-04 / LMNH-17).
//!
//! Feeds arbitrary bytes through frame header parsing and EVERY per-opcode
//! `decode_*_checked` payload decoder in `src/protocol/codec.rs`. The
//! contract under fuzz is "Ok or typed error, never panic" — libFuzzer
//! flags any panic, abort, or sanitizer fault.
//!
//! Each decoder is invoked twice: once with a small batch cap (64) so the
//! `BatchTooLarge` boundary is constantly probed, and once with the
//! absolute hard cap [`MAX_DECODE_BATCH`] so count-validation paths that
//! only trigger near the payload-fit limit are reachable too.
//!
//! Keep the decoder list in sync with `tests/wire_fuzz_smoke.rs` (the
//! CI-enforced deterministic half of this coverage).

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use teraslab::protocol::codec::{
    MAX_DECODE_BATCH, decode_create_batch_checked, decode_get_batch_checked,
    decode_get_response_checked, decode_get_spend_batch_checked,
    decode_get_spend_response_checked, decode_partial_with_signals_checked,
    decode_reassign_batch_checked, decode_set_mined_batch_checked, decode_slot_item_batch_checked,
    decode_sparse_errors_checked, decode_spend_batch_checked, decode_txid_batch_checked,
    decode_unspend_batch_checked,
};
use teraslab::protocol::frame::{RequestFrame, ResponseFrame, try_decode_frames};

fuzz_target!(|data: &[u8]| {
    // Frame-level parsers.
    let _ = RequestFrame::decode(data);
    let _ = RequestFrame::decode_bytes(Bytes::copy_from_slice(data));
    let _ = ResponseFrame::decode(data);
    let _ = try_decode_frames(data);

    for max_batch in [64u32, MAX_DECODE_BATCH] {
        let _ = decode_spend_batch_checked(data, max_batch);
        let _ = decode_set_mined_batch_checked(data, max_batch);
        // shared_len variants: 0 (delete/mark-longest-chain shape),
        // 9 (set_conflicting shape: value(1)+cbh(4)+bhr(4)).
        let _ = decode_txid_batch_checked(data, 0, max_batch);
        let _ = decode_txid_batch_checked(data, 9, max_batch);
        let _ = decode_slot_item_batch_checked(data, max_batch);
        let _ = decode_reassign_batch_checked(data, max_batch);
        let _ = decode_unspend_batch_checked(data, max_batch);
        let _ = decode_create_batch_checked(data, max_batch);
        let _ = decode_get_batch_checked(data, max_batch);
        let _ = decode_get_response_checked(data, max_batch);
        let _ = decode_partial_with_signals_checked(data, max_batch);
        let _ = decode_sparse_errors_checked(data, max_batch);
        let _ = decode_get_spend_batch_checked(data, max_batch);
        let _ = decode_get_spend_response_checked(data, max_batch);
    }
});
