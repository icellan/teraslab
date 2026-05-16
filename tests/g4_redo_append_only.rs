//! Tests for F-G4-004: flush() is append-only at aligned offsets — no
//! read-modify-write on every flush. The buffer is padded to the next
//! aligned offset in-memory; subsequent flushes start at the next
//! aligned offset.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::TxKey;
use teraslab::redo::{RedoLog, RedoOp};

fn key(n: u8) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0] = n;
    TxKey { txid }
}

/// A device wrapper that counts pread calls and can refuse them — so a
/// test can prove the redo flush hot path does NOT issue an unexpected
/// pread (header + entries readback during open is expected; further
/// flushes must NOT pread).
struct PreadCountingDevice {
    inner: Arc<MemoryDevice>,
    pread_count: AtomicU64,
    refuse_preads: AtomicBool,
}

impl PreadCountingDevice {
    fn new(size: u64) -> Self {
        Self {
            inner: Arc::new(MemoryDevice::new(size, 4096).unwrap()),
            pread_count: AtomicU64::new(0),
            refuse_preads: AtomicBool::new(false),
        }
    }

    fn pread_count(&self) -> u64 {
        self.pread_count.load(Ordering::SeqCst)
    }

    fn refuse_preads(&self) {
        self.refuse_preads.store(true, Ordering::SeqCst);
    }
}

impl BlockDevice for PreadCountingDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> teraslab::device::Result<usize> {
        self.pread_count.fetch_add(1, Ordering::SeqCst);
        if self.refuse_preads.load(Ordering::SeqCst) {
            return Err(teraslab::device::DeviceError::Io(std::io::Error::other(
                "F-G4-004: hot path must not pread on flush",
            )));
        }
        self.inner.pread(buf, offset)
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> teraslab::device::Result<usize> {
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

/// After open, subsequent `append_and_flush()` calls must not perform a
/// pread on the device — the buffer is padded to the next aligned
/// offset in-memory and the next flush starts at a fresh aligned
/// offset. Prior to F-G4-004 every flush read the trailing aligned
/// block back, spliced new bytes in, and rewrote the whole thing.
#[test]
fn flush_after_open_does_not_pread() {
    let dev: Arc<PreadCountingDevice> = Arc::new(PreadCountingDevice::new(1024 * 1024));
    let mut log = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();
    let preads_after_open = dev.pread_count();

    // Now refuse any future pread. If the hot path were still RMW, the
    // next flush would fail; with append-only writes it must succeed.
    dev.refuse_preads();

    for i in 1..=20u8 {
        log.append_and_flush(RedoOp::Freeze {
            tx_key: key(i),
            offset: 0,
        })
        .unwrap();
    }

    // No extra preads beyond open() should have happened.
    assert_eq!(
        dev.pread_count(),
        preads_after_open,
        "F-G4-004: flush hot path must not pread"
    );
}
