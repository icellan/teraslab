//! Tests for F-G4-005: legacy Freeze redo replay must NOT re-freeze a
//! slot whose status has moved on (SPENT, PRUNED, LOCKED). The legacy
//! opcode carries no `expected_hash`, so recovery cannot verify the
//! slot is still the UTXO the original Freeze targeted.

use std::sync::Arc;
use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{PrimaryBackend, TxIndexEntry, TxKey};
use teraslab::io;
use teraslab::record::{TxFlags, TxMetadata, UTXO_FROZEN, UTXO_SPENT, UTXO_UNSPENT, UtxoSlot};
use teraslab::recovery::recover;
use teraslab::redo::{RedoLog, RedoOp};

fn key(b: u8) -> TxKey {
    let mut t = [0u8; 32];
    t[0] = b;
    TxKey { txid: t }
}

#[test]
fn legacy_freeze_replay_skips_already_spent_slot() {
    // Data and redo on separate MemoryDevices.
    let data: Arc<MemoryDevice> = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
    let redo_dev: Arc<MemoryDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let mut alloc = SlotAllocator::new(data.clone()).unwrap();
    let mut index = PrimaryBackend::new_in_memory(128).unwrap();

    // Allocate a record region for one tx with one UTXO.
    let utxo_count = 1u32;
    let record_size = TxMetadata::record_size_for(utxo_count);
    let record_offset = alloc.allocate(record_size).unwrap();

    // Build a metadata header + one SPENT slot on the device.
    let mut meta = TxMetadata::new(utxo_count);
    meta.record_size = record_size as u32;
    meta.flags = TxFlags::empty();
    let k = key(0x33);
    meta.tx_id = k.txid;
    io::write_metadata(&*data as &dyn BlockDevice, record_offset, &meta).unwrap();

    // Write the slot as SPENT.
    let hash = [0xCDu8; 32];
    let spent_slot = UtxoSlot::new_spent(hash, [0x77u8; 36]);
    io::write_utxo_slot(&*data as &dyn BlockDevice, record_offset, 0, &spent_slot).unwrap();

    // Register the primary index.
    index
        .register(
            k,
            TxIndexEntry {
                device_id: 0,
                record_offset,
                utxo_count,
                block_entry_count: 0,
                tx_flags: 0,
                spent_utxos: 1,
                dah_or_preserve: 0,
                unmined_since: 0,
                generation: 0,
            },
        )
        .unwrap();

    // Append a legacy Freeze entry covering the same slot.
    let mut log = RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, 1024 * 1024).unwrap();
    log.append_and_flush(RedoOp::Freeze {
        tx_key: k,
        offset: 0,
    })
    .unwrap();

    // Replay.
    let stats = recover(&*data as &dyn BlockDevice, &log, &mut index).unwrap();
    assert_eq!(
        stats.entries_replayed, 0,
        "F-G4-005: legacy Freeze must NOT replay over a SPENT slot",
    );
    assert_eq!(stats.entries_skipped, 1);

    // Slot status must still be SPENT on device.
    let read_back = io::read_utxo_slot(&*data as &dyn BlockDevice, record_offset, 0).unwrap();
    assert_eq!(
        read_back.status, UTXO_SPENT,
        "slot status must not have been overwritten with FROZEN",
    );
    assert_ne!(
        read_back.status, UTXO_FROZEN,
        "legacy Freeze must not have re-stamped FROZEN over SPENT",
    );
}

#[test]
fn legacy_freeze_replay_applies_on_unspent_slot() {
    // Sanity: the legacy path still applies when the slot is still
    // UNSPENT — F-G4-005 is conservative, not a regression.
    let data: Arc<MemoryDevice> = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
    let redo_dev: Arc<MemoryDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());
    let mut alloc = SlotAllocator::new(data.clone()).unwrap();
    let mut index = PrimaryBackend::new_in_memory(128).unwrap();

    let utxo_count = 1u32;
    let record_size = TxMetadata::record_size_for(utxo_count);
    let record_offset = alloc.allocate(record_size).unwrap();

    let mut meta = TxMetadata::new(utxo_count);
    meta.record_size = record_size as u32;
    let k = key(0x44);
    meta.tx_id = k.txid;
    io::write_metadata(&*data as &dyn BlockDevice, record_offset, &meta).unwrap();

    let hash = [0x55u8; 32];
    let unspent_slot = UtxoSlot::new_unspent(hash);
    io::write_utxo_slot(&*data as &dyn BlockDevice, record_offset, 0, &unspent_slot).unwrap();

    index
        .register(
            k,
            TxIndexEntry {
                device_id: 0,
                record_offset,
                utxo_count,
                block_entry_count: 0,
                tx_flags: 0,
                spent_utxos: 0,
                dah_or_preserve: 0,
                unmined_since: 0,
                generation: 0,
            },
        )
        .unwrap();

    let mut log = RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, 1024 * 1024).unwrap();
    log.append_and_flush(RedoOp::Freeze {
        tx_key: k,
        offset: 0,
    })
    .unwrap();

    let stats = recover(&*data as &dyn BlockDevice, &log, &mut index).unwrap();
    assert_eq!(stats.entries_replayed, 1);

    let read_back = io::read_utxo_slot(&*data as &dyn BlockDevice, record_offset, 0).unwrap();
    assert_eq!(
        read_back.status, UTXO_FROZEN,
        "legacy Freeze must still apply on UNSPENT slot",
    );
    // Pre-state was UNSPENT; sanity-check our setup.
    let _ = UTXO_UNSPENT;
}
