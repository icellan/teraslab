//! Issue #34 — the INLINE cold-data blob must round-trip byte-exact.
//!
//! Regression for a deterministic 1-byte-short inline cold blob observed on a
//! specific transaction template: 1 input, 2 outputs, whose serialized inline
//! cold data decodes to `inputs=190`, `outputs=74`, `inpoints=44` — total
//! inner blob length `4+190 + 4+74 + 4+44 = 320` bytes. When any later full
//! `FieldColdData` read fetched the record as a spent parent, the blob came
//! back one byte short (319), so the trailing `inpoints` field decoded as
//! truncated and wedged legacy catch-up sync.
//!
//! These tests store the exact-template record inline, then read the full cold
//! data back and assert byte-equality with what was written — first straight
//! through create→read, then through create→spend(relocate)→read on a
//! log-structured (segment) store where the record is physically moved and its
//! cold tail carried verbatim.

use std::sync::Arc;

use teraslab::allocator::SlotAllocator;
use teraslab::device::{AlignedBuf, BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey};
use teraslab::locks::StripedLocks;
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::{Engine, build_cold_data};
use teraslab::ops::error::SpendError;
use teraslab::ops::spend::SpendRequest;
use teraslab::record::{METADATA_SIZE, UTXO_SLOT_SIZE};
use teraslab::segment_allocator::SegmentAllocator;

/// Build the exact-template inline cold data from issue #34.
///
/// Returns `(inputs, outputs, inpoints, cold_blob)` where `cold_blob` is the
/// canonical serialized form the store must persist byte-for-byte. Field sizes
/// are the ones from the byte-level evidence: 190 / 74 / 44.
fn issue_34_template() -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
    // Distinct, non-zero fill patterns per section so a dropped byte from ANY
    // section (not just the trailing one) is caught by the byte-equality check.
    let inputs: Vec<u8> = (0..190u32).map(|n| (n % 251) as u8 + 1).collect();
    let outputs: Vec<u8> = (0..74u32).map(|n| (n % 241) as u8 + 3).collect();
    // The trailing inpoints section is where the observed truncation landed.
    // Its last byte (index 43) is deliberately non-zero and distinctive.
    let inpoints: Vec<u8> = (0..44u32).map(|n| (n % 229) as u8 + 7).collect();

    let cold = build_cold_data(Some(&inputs), Some(&outputs), Some(&inpoints));
    // The serialized blob must be exactly 320 bytes: 4+190 + 4+74 + 4+44.
    assert_eq!(
        cold.len(),
        320,
        "template must serialize to the 320-byte inner blob from the issue"
    );
    (inputs, outputs, inpoints, cold)
}

fn slot_engine() -> Arc<Engine> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(1_024).unwrap();
    Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(64),
        DahIndex::new(),
    ))
}

fn make_req<'a>(
    tx_id: [u8; 32],
    hashes: &'a [[u8; 32]],
    inputs: &'a [u8],
    outputs: &'a [u8],
    inpoints: &'a [u8],
) -> CreateRequest<'a> {
    CreateRequest {
        tx_id,
        tx_version: 1,
        locktime: 0,
        fee: 500,
        size_in_bytes: 219,
        extended_size: 259,
        is_coinbase: false,
        spending_height: 0,
        utxo_hashes: hashes,
        inputs: Some(inputs),
        outputs: Some(outputs),
        inpoints: Some(inpoints),
        is_external: false,
        created_at: 1_710_000_000_000,
        block_height: 1000,
        mined_block_infos: &[],
        frozen: false,
        conflicting: false,
        locked: false,
        external_ref: None,
        parent_txids: &[],
    }
}

/// create → full-cold read must be byte-exact for the issue-34 template.
#[test]
fn inline_cold_data_round_trips_byte_exact_on_create() {
    let engine = slot_engine();
    let (inputs, outputs, inpoints, cold) = issue_34_template();

    // 1 input, 2 outputs → 2 UTXO slots, matching the record metadata
    // (UtxoCount:2) in the issue.
    let hashes = [[7u8; 32], [9u8; 32]];
    let mut tx = [0u8; 32];
    tx[0] = 0x34;

    let req = make_req(tx, &hashes, &inputs, &outputs, &inpoints);
    let resp = engine.create(&req).unwrap();
    assert_eq!(resp.utxo_count, 2);

    let key = TxKey { txid: tx };
    let read_back = engine.read_cold_data(&key).unwrap();

    assert_eq!(
        read_back.len(),
        cold.len(),
        "stored inline cold blob truncated: got {} bytes, wrote {}",
        read_back.len(),
        cold.len(),
    );
    assert_eq!(
        read_back, cold,
        "inline cold data must round-trip byte-exact"
    );
}

/// create → spend(relocate) → full-cold read must still be byte-exact.
///
/// On a log-structured segment store a spend physically relocates the record
/// to a fresh append offset, carrying the cold tail verbatim. This is exactly
/// the "read in full when fetched as a parent, i.e. after it's been spent"
/// path the issue describes.
#[test]
fn inline_cold_data_round_trips_byte_exact_after_spend_relocate() {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let seg = SegmentAllocator::new(dev.clone(), 8 * 1024 * 1024).unwrap();
    let engine = Arc::new(Engine::new(
        dev,
        Index::new(64).unwrap(),
        seg,
        StripedLocks::new(64),
        DahIndex::new(),
    ));

    let (inputs, outputs, inpoints, cold) = issue_34_template();
    let hashes = [[7u8; 32], [9u8; 32]];
    let mut tx = [0u8; 32];
    tx[0] = 0x35;

    let req = make_req(tx, &hashes, &inputs, &outputs, &inpoints);
    engine.create(&req).unwrap();
    let key = TxKey { txid: tx };

    let pre_offset = engine.lookup(&key).unwrap().record_offset;

    // Spend vout 0 — on a segment store this relocates the whole record.
    let spend = SpendRequest {
        tx_key: key,
        offset: 0,
        utxo_hash: hashes[0],
        spending_data: [0x11u8; 36],
        ignore_conflicting: true,
        ignore_locked: true,
        current_block_height: 100,
        block_height_retention: 0,
    };
    engine.spend(&spend).unwrap();

    let post_offset = engine.lookup(&key).unwrap().record_offset;
    assert_ne!(
        pre_offset, post_offset,
        "segment spend must relocate the record (precondition for the test)"
    );

    let read_back = engine.read_cold_data(&key).unwrap();
    assert_eq!(
        read_back.len(),
        cold.len(),
        "relocated inline cold blob truncated: got {} bytes, wrote {}",
        read_back.len(),
        cold.len(),
    );
    assert_eq!(
        read_back, cold,
        "inline cold data must survive spend→relocate byte-exact"
    );
}

/// Defense-in-depth (issue #34, fix 2): a record whose stored inline cold blob
/// declares (via its own length prefixes) more bytes than the record's cold
/// region actually holds must surface a typed `ColdDataInconsistent` error on
/// read — NOT a silently-short buffer that downstream decodes as "truncated".
#[test]
fn inconsistent_inline_cold_blob_surfaces_typed_error() {
    // Build a real, valid record inline first, then corrupt ONLY the on-device
    // inpoints length prefix so it overstates the trailing bytes by 1 — the
    // exact shape of the issue-34 corruption (`inpoints_len` claims 45, only 44
    // are stored), reproduced deterministically on the device.
    let engine = slot_engine();
    let dev = engine.device();

    let (inputs, outputs, inpoints, _cold) = issue_34_template();
    let hashes = [[7u8; 32], [9u8; 32]];
    let mut tx = [0u8; 32];
    tx[0] = 0x36;

    let req = make_req(tx, &hashes, &inputs, &outputs, &inpoints);
    let resp = engine.create(&req).unwrap();
    let record_offset = resp.record_offset;

    // Offset of the inpoints length prefix within the cold blob:
    //   cold_start = METADATA_SIZE + utxo_count * UTXO_SLOT_SIZE
    //   inpoints_len prefix = cold_start + (4 + inputs_len) + (4 + outputs_len)
    let cold_start = METADATA_SIZE as u64 + 2 * UTXO_SLOT_SIZE as u64;
    let inpoints_len_prefix = cold_start + (4 + 190) + (4 + 74);
    let device_prefix_off = record_offset + inpoints_len_prefix;

    // Read the covering block, bump the u32 inpoints_len from 44 to 45, write
    // it back. Now the blob declares 321 cold bytes while only 320 are stored.
    let align = dev.alignment();
    let aligned_base = device_prefix_off / align as u64 * align as u64;
    let intra = (device_prefix_off - aligned_base) as usize;
    let mut block = AlignedBuf::new(align, align);
    dev.pread_exact_at(&mut block, aligned_base).unwrap();

    let mut cur = [0u8; 4];
    cur.copy_from_slice(&block[intra..intra + 4]);
    assert_eq!(
        u32::from_le_bytes(cur),
        44,
        "precondition: inpoints_len is 44"
    );
    block[intra..intra + 4].copy_from_slice(&45u32.to_le_bytes());
    dev.pwrite_all_at(&block, aligned_base).unwrap();

    // The read must now fail closed with the typed integrity error, reporting
    // declared (321) > available (320).
    let key = TxKey { txid: tx };
    let err = engine.read_cold_data(&key).unwrap_err();
    match err {
        SpendError::ColdDataInconsistent {
            declared,
            available,
        } => {
            assert_eq!(available, 320, "available cold region is the stored 320");
            assert_eq!(
                declared, 321,
                "declared length is the inflated 12+190+74+45"
            );
        }
        other => panic!("expected ColdDataInconsistent, got {other:?}"),
    }
}
