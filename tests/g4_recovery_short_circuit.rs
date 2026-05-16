//! Tests for F-G4-007: recovery replay short-circuits on the first
//! non-tolerable failure (anything other than `MissingPrimary`).
//! Continuing past a fatal failure risked partially-applying state
//! that no rollback path could undo.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use teraslab::device::{BlockDevice, DeviceError, MemoryDevice};
use teraslab::index::{PrimaryBackend, TxIndexEntry, TxKey};
use teraslab::io;
use teraslab::record::{TxFlags, TxMetadata, UtxoSlot};
use teraslab::recovery::recover;
use teraslab::redo::{RedoLog, RedoOp};

fn key(b: u8) -> TxKey {
    let mut t = [0u8; 32];
    t[0] = b;
    TxKey { txid: t }
}

/// Wraps a MemoryDevice and rejects pread/pwrite to a specific record
/// offset once the test arms it. Used to inject an IoError partway
/// through a multi-entry replay.
struct FailRangeDevice {
    inner: Arc<MemoryDevice>,
    fail_offset: AtomicU64,
    fail_range_size: AtomicU64,
    armed: AtomicBool,
}

impl FailRangeDevice {
    fn new(size: u64) -> Self {
        Self {
            inner: Arc::new(MemoryDevice::new(size, 4096).unwrap()),
            fail_offset: AtomicU64::new(u64::MAX),
            fail_range_size: AtomicU64::new(0),
            armed: AtomicBool::new(false),
        }
    }

    fn arm_fail_range(&self, offset: u64, size: u64) {
        self.fail_offset.store(offset, Ordering::SeqCst);
        self.fail_range_size.store(size, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    fn in_fail_range(&self, offset: u64, len: u64) -> bool {
        if !self.armed.load(Ordering::SeqCst) {
            return false;
        }
        let fail_off = self.fail_offset.load(Ordering::SeqCst);
        let fail_size = self.fail_range_size.load(Ordering::SeqCst);
        offset < fail_off.saturating_add(fail_size) && offset.saturating_add(len) > fail_off
    }
}

impl BlockDevice for FailRangeDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> teraslab::device::Result<usize> {
        if self.in_fail_range(offset, buf.len() as u64) {
            return Err(DeviceError::Io(std::io::Error::other(
                "F-G4-007 simulated pread failure",
            )));
        }
        self.inner.pread(buf, offset)
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> teraslab::device::Result<usize> {
        if self.in_fail_range(offset, buf.len() as u64) {
            return Err(DeviceError::Io(std::io::Error::other(
                "F-G4-007 simulated pwrite failure",
            )));
        }
        self.inner.pwrite(buf, offset)
    }

    fn alignment(&self) -> usize {
        self.inner.alignment()
    }

    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn sync(&self) -> teraslab::device::Result<()> {
        self.inner.sync()
    }
}

#[test]
fn replay_stops_on_first_fatal_io_error() {
    let data: Arc<FailRangeDevice> = Arc::new(FailRangeDevice::new(8 * 1024 * 1024));
    let redo_dev: Arc<MemoryDevice> = Arc::new(MemoryDevice::new(1024 * 1024, 4096).unwrap());

    let mut index = PrimaryBackend::new_in_memory(128).unwrap();

    // Create two on-device records (A and B) at known offsets. Register
    // their primary index entries. Then write a redo log with two
    // Freeze entries; arm a pread failure inside record A's slot
    // region so replay of entry A fails with IoError. F-G4-007 says
    // entry B must NOT be touched after the fatal failure.
    let record_a = 8 * 1024u64;
    let record_b = 16 * 1024u64;

    for (record_offset, txkey) in &[(record_a, key(0xA1)), (record_b, key(0xB2))] {
        let mut meta = TxMetadata::new(1);
        meta.record_size = TxMetadata::record_size_for(1) as u32;
        meta.tx_id = txkey.txid;
        meta.flags = TxFlags::empty();
        io::write_metadata(&*data as &dyn BlockDevice, *record_offset, &meta).unwrap();
        io::write_utxo_slot(
            &*data as &dyn BlockDevice,
            *record_offset,
            0,
            &UtxoSlot::new_unspent([0x42u8; 32]),
        )
        .unwrap();

        index
            .register(
                *txkey,
                TxIndexEntry {
                    device_id: 0,
                    record_offset: *record_offset,
                    utxo_count: 1,
                    block_entry_count: 0,
                    tx_flags: 0,
                    spent_utxos: 0,
                    dah_or_preserve: 0,
                    unmined_since: 0,
                    generation: 0,
                },
            )
            .unwrap();
    }

    // Two Freeze entries (V2 — has expected_hash to skip the F-G4-005
    // guard so the failure mode we test is pure I/O, not freeze policy).
    let mut log = RedoLog::open(redo_dev.clone() as Arc<dyn BlockDevice>, 0, 1024 * 1024).unwrap();
    log.append_and_flush(RedoOp::FreezeV2 {
        tx_key: key(0xA1),
        offset: 0,
        utxo_hash: [0x42u8; 32],
    })
    .unwrap();
    log.append_and_flush(RedoOp::FreezeV2 {
        tx_key: key(0xB2),
        offset: 0,
        utxo_hash: [0x42u8; 32],
    })
    .unwrap();

    // Arm the failure to cover record A only.
    data.arm_fail_range(record_a, 8 * 1024);

    let stats = recover(&*data as &dyn BlockDevice, &log, &mut index).unwrap();
    assert_eq!(
        stats.entries_failed, 1,
        "F-G4-007: one fatal failure must be observed",
    );
    assert_eq!(stats.failed_io, 1, "fatal cause must be IoError");
    assert_eq!(
        stats.entries_replayed, 0,
        "no entries should have been applied after the fatal failure",
    );

    // Record B must still be UNSPENT — the loop must have stopped
    // before touching it.
    let slot_b = io::read_utxo_slot(&*data as &dyn BlockDevice, record_b, 0).unwrap();
    assert_ne!(
        slot_b.status,
        teraslab::record::UTXO_FROZEN,
        "F-G4-007: replay must short-circuit; record B must not have been frozen",
    );
}
