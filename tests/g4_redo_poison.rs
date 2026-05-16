//! Tests for the F-G4-002 redo-log poisoning on flush failure.
//!
//! Prior to F-G4-002, a failing `flush()` left `self.buffer` populated.
//! Another thread could subsequently re-flush the same buffer, making
//! the first caller's "failed" ops durable. The fix poisons the log on
//! flush error: the buffer is dropped, and future calls return
//! `RedoError::Poisoned` until the process restarts and recovery
//! reconstructs from the on-disk state.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use teraslab::device::{BlockDevice, DeviceError, MemoryDevice};
use teraslab::index::TxKey;
use teraslab::redo::{RedoError, RedoLog, RedoOp};

fn key(n: u8) -> TxKey {
    let mut txid = [0u8; 32];
    txid[0] = n;
    TxKey { txid }
}

/// A MemoryDevice wrapper that lets a test toggle pwrite failures.
struct FailWriteDevice {
    inner: Arc<MemoryDevice>,
    fail: AtomicBool,
}

impl FailWriteDevice {
    fn new(size: u64) -> Self {
        Self {
            inner: Arc::new(MemoryDevice::new(size, 4096).unwrap()),
            fail: AtomicBool::new(false),
        }
    }

    fn fail_writes(&self) {
        self.fail.store(true, Ordering::SeqCst);
    }
}

impl BlockDevice for FailWriteDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> teraslab::device::Result<usize> {
        self.inner.pread(buf, offset)
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> teraslab::device::Result<usize> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(DeviceError::Io(std::io::Error::other(
                "F-G4-002 simulated pwrite failure",
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
fn flush_failure_poisons_log_and_drops_buffer() {
    let dev: Arc<FailWriteDevice> = Arc::new(FailWriteDevice::new(1024 * 1024));
    let mut log = RedoLog::open(dev.clone(), 0, 1024 * 1024).unwrap();

    // Append a successful entry first so the log has known good state.
    log.append_and_flush(RedoOp::Freeze {
        tx_key: key(1),
        offset: 0,
    })
    .unwrap();

    // Append into the buffer, then force the next pwrite to fail.
    log.append(RedoOp::Freeze {
        tx_key: key(2),
        offset: 0,
    })
    .unwrap();
    dev.fail_writes();

    match log.flush() {
        Err(RedoError::Io(_)) => {}
        Ok(()) => panic!("flush should have failed when pwrites error"),
        Err(other) => panic!("expected RedoError::Io, got {other:?}"),
    }

    // Subsequent append / flush must report Poisoned, not silently
    // persist the supposedly-failed ops.
    match log.append(RedoOp::Freeze {
        tx_key: key(3),
        offset: 0,
    }) {
        Err(RedoError::Poisoned) => {}
        Ok(_) => panic!("append on poisoned log must fail"),
        Err(other) => panic!("expected Poisoned, got {other:?}"),
    }

    match log.flush() {
        Err(RedoError::Poisoned) => {}
        Ok(()) => panic!("flush on poisoned log must fail"),
        Err(other) => panic!("expected Poisoned, got {other:?}"),
    }
}
