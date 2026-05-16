//! F-G9-008 regression: when the uploader's pwrite of `ExternalRef`
//! fails after the blob has already been uploaded, the just-uploaded
//! blob must be deleted so the record does not enter a permanent
//! half-state (blob present, content_hash still zero).
//!
//! Pre-fix: the upload returned `Err` but left the blob on disk.
//! Subsequent reads cross-checking content_hash (F-G9-002) would fail
//! forever, with no foreground signal to the original CREATE caller.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, DeviceError, MemoryDevice};
use teraslab::io;
use teraslab::record::{TxFlags, TxMetadata, UtxoSlot};
use teraslab::storage::blobstore::{BlobStore, MemoryBlobStore};
use teraslab::storage::uploader::BlobUploader;

/// Device wrapper that fails `pwrite` after an arming flag is set.
///
/// All reads, the initial seed writes (while disarmed), and `alignment`
/// queries pass through to the inner `MemoryDevice`. Once armed, every
/// subsequent `pwrite` returns `DeviceError::Io` — simulating a
/// transient device failure during the uploader's metadata RMW.
struct ArmedFailDevice {
    inner: Arc<MemoryDevice>,
    armed: AtomicBool,
    pwrite_attempts: AtomicU64,
}

impl ArmedFailDevice {
    fn new(inner: Arc<MemoryDevice>) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(false),
            pwrite_attempts: AtomicU64::new(0),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

impl BlockDevice for ArmedFailDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize, DeviceError> {
        self.inner.pread(buf, offset)
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize, DeviceError> {
        self.pwrite_attempts.fetch_add(1, Ordering::Relaxed);
        if self.armed.load(Ordering::SeqCst) {
            return Err(DeviceError::Io(std::io::Error::other(
                "simulated pwrite failure",
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

    fn sync(&self) -> Result<(), DeviceError> {
        self.inner.sync()
    }
}

#[test]
fn uploader_rolls_back_blob_when_external_ref_write_fails() {
    let mem = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let dev: Arc<ArmedFailDevice> = Arc::new(ArmedFailDevice::new(Arc::clone(&mem)));
    let blob: Arc<MemoryBlobStore> = Arc::new(MemoryBlobStore::new());

    // Seed: a hot record with EXTERNAL flag and zero ExternalRef. Done while
    // the wrapper is still disarmed so it passes through to MemoryDevice.
    let mut alloc = SlotAllocator::new(Arc::clone(&mem) as Arc<dyn BlockDevice>).unwrap();
    let utxo_count = 2u32;
    let record_offset = alloc
        .allocate(TxMetadata::record_size_for(utxo_count))
        .unwrap();

    let mut tx_id = [0u8; 32];
    tx_id[0] = 0xC8;
    tx_id[1] = 0x08;
    let mut meta = TxMetadata::new(utxo_count);
    meta.tx_id = tx_id;
    meta.flags = TxFlags::EXTERNAL;
    let slots: Vec<UtxoSlot> = (0..utxo_count)
        .map(|_| UtxoSlot::new_unspent([0; 32]))
        .collect();
    io::write_full_record(&*mem, record_offset, &meta, &slots).unwrap();

    // Arm the failure injector.
    dev.arm();

    let uploader = BlobUploader::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::clone(&dev) as Arc<dyn BlockDevice>,
    );

    let payload = vec![0xAB; 4096];
    let handle = uploader
        .submit(tx_id, record_offset, payload.clone(), 1, 1)
        .expect("submit accepts task");

    // The upload thread will fail when it tries the device pwrite RMW; the
    // handle surfaces that error.
    let err = handle.wait().expect_err("upload must fail when device pwrite fails");
    let msg = format!("{err}");
    assert!(
        msg.contains("device write failed") || msg.contains("simulated pwrite"),
        "unexpected error message: {msg}"
    );

    // F-G9-008: the blob must have been rolled back. Without the rollback,
    // the orphan blob would persist on disk with the record's
    // ExternalRef.content_hash still zero — a permanent half-state.
    assert!(
        !blob.exists(&tx_id).unwrap(),
        "F-G9-008 regression: uploader left an orphan blob after metadata-write failure"
    );
    assert!(
        blob.get(&tx_id).unwrap().is_none(),
        "blob payload must be deleted on rollback"
    );
}
