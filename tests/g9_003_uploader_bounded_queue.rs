//! F-G9-003 regression test.
//!
//! The original [`BlobUploader`] used an unbounded `std::sync::mpsc::channel`.
//! A fast producer could enqueue an unlimited number of multi-MiB upload
//! tasks before the background thread drained any of them — a memory
//! exhaustion DoS waiting to happen under bursty external-tier load.
//!
//! Post-fix the uploader uses a bounded `sync_channel` and `submit` returns
//! [`BlobError::UploaderQueueFull`] immediately when the queue is saturated.
//! This test wedges the background thread by configuring a tiny capacity and
//! filling it without ever calling `.wait()` (so the thread is stuck on the
//! first task's processing while subsequent submits hit the full channel).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use teraslab::allocator::SlotAllocator;
use teraslab::device::MemoryDevice;
use teraslab::io;
use teraslab::record::{TxFlags, TxMetadata};
use teraslab::storage::blobstore::{BlobDigest, BlobError, BlobStore, BlobStreamWriter, Result};
use teraslab::storage::uploader::{BlobUploader, DEFAULT_UPLOADER_QUEUE_CAPACITY};

/// A blob store wrapper whose `put` blocks until `release_one` is called.
/// Lets us pin the background upload thread on a single task so the channel
/// fills up with subsequent submits.
struct WedgedBlobStore {
    release: Arc<AtomicU32>,
    seen_puts: Arc<AtomicU32>,
}

impl WedgedBlobStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            release: Arc::new(AtomicU32::new(0)),
            seen_puts: Arc::new(AtomicU32::new(0)),
        })
    }
}

impl BlobStore for WedgedBlobStore {
    fn put(&self, _key: &[u8; 32], _data: &[u8]) -> Result<BlobDigest> {
        let my_id = self.seen_puts.fetch_add(1, Ordering::SeqCst);
        // Spin until release counter passes `my_id`. Bounded retry budget so
        // a buggy test never hangs the suite indefinitely.
        let started = std::time::Instant::now();
        loop {
            if self.release.load(Ordering::SeqCst) > my_id {
                break;
            }
            if started.elapsed() > Duration::from_secs(30) {
                return Err(BlobError::Io(std::io::Error::other(
                    "wedged blob store: release never arrived (test timeout)",
                )));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(BlobDigest {
            sha256: [0u8; 32],
            length: 0,
        })
    }

    fn get(&self, _key: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn get_range(&self, _key: &[u8; 32], _offset: u64, _length: u64) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn delete(&self, _key: &[u8; 32]) -> Result<()> {
        Ok(())
    }

    fn exists(&self, _key: &[u8; 32]) -> Result<bool> {
        Ok(false)
    }

    fn digest(&self, _key: &[u8; 32]) -> Result<Option<BlobDigest>> {
        Ok(None)
    }

    fn stream_to(&self, _key: &[u8; 32], _writer: &mut dyn std::io::Write) -> Result<u64> {
        Err(BlobError::NotFound {
            key: "wedged".into(),
        })
    }

    fn begin_stream(&self, _key: &[u8; 32]) -> Result<Box<dyn BlobStreamWriter>> {
        Err(BlobError::Io(std::io::Error::other("unsupported in test")))
    }

    fn list(&self) -> Result<Vec<[u8; 32]>> {
        Ok(vec![])
    }
}

fn pre_stamped_record_offsets(
    dev: &Arc<MemoryDevice>,
    alloc: &mut SlotAllocator,
    n: usize,
) -> Vec<(u64, [u8; 32])> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let utxo_count = 1u32;
        let offset = alloc
            .allocate(TxMetadata::record_size_for(utxo_count))
            .unwrap();
        let mut tx_id = [0u8; 32];
        tx_id[0] = i as u8;
        tx_id[1] = 0xAA;
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = tx_id;
        meta.flags = TxFlags::EXTERNAL;
        let slots = vec![teraslab::record::UtxoSlot::new_unspent([0; 32])];
        io::write_full_record(&**dev, offset, &meta, &slots).unwrap();
        out.push((offset, tx_id));
    }
    out
}

#[test]
fn submit_returns_uploader_queue_full_when_saturated() {
    // 2-slot bound so we can saturate quickly. The first `submit` is consumed
    // by the upload thread (which immediately wedges in WedgedBlobStore::put),
    // the next 2 `submit`s fill the bounded channel, and the 4th must be
    // rejected with UploaderQueueFull.
    let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
    let mut alloc = SlotAllocator::new(dev.clone()).unwrap();
    let blob = WedgedBlobStore::new();
    let uploader = BlobUploader::with_capacity(blob.clone(), dev.clone(), 2);

    let records = pre_stamped_record_offsets(&dev, &mut alloc, 16);

    // First submit may or may not be consumed by the worker immediately; loop
    // a few sends until we observe a Full rejection, with an upper bound so a
    // regression (back to unbounded channel) fails the test deterministically
    // instead of hanging.
    let mut got_queue_full = false;
    let mut _handles = Vec::new();
    let started = std::time::Instant::now();
    for (offset, tx_id) in records.iter() {
        if started.elapsed() > Duration::from_secs(10) {
            break;
        }
        match uploader.submit(*tx_id, *offset, vec![0u8; 16 * 1024], 1, 1) {
            Ok(h) => _handles.push(h),
            Err(BlobError::UploaderQueueFull { capacity, .. }) => {
                assert_eq!(capacity, 2);
                got_queue_full = true;
                break;
            }
            Err(other) => panic!("unexpected error from submit: {other:?}"),
        }
    }

    assert!(
        got_queue_full,
        "bounded uploader queue must reject submit with UploaderQueueFull when saturated"
    );
    // The queue_full_count metric must have ticked up at least once.
    assert!(
        uploader.queue_full_count() >= 1,
        "queue_full_count must increment on each rejection (got {})",
        uploader.queue_full_count()
    );

    // Release the wedge so the thread can finish (otherwise tests in this
    // process leak the spinning thread until the test executable exits).
    blob.release.store(u32::MAX, Ordering::SeqCst);
}

#[test]
fn default_capacity_matches_documented_constant() {
    let dev = Arc::new(MemoryDevice::new(8 * 1024 * 1024, 4096).unwrap());
    let blob = Arc::new(teraslab::storage::blobstore::MemoryBlobStore::new());
    let uploader = BlobUploader::new(blob, dev);
    assert_eq!(uploader.capacity(), DEFAULT_UPLOADER_QUEUE_CAPACITY);
    assert_eq!(uploader.queue_full_count(), 0);

    // Convenience: silence unused-mut clippy on `_uploader` by referencing the
    // shutdown sentinel below in case the test gets extended later.
    let _ = AtomicBool::new(false);
}
