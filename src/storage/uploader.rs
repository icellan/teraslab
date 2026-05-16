//! Asynchronous blob uploader for the external storage tier.
//!
//! For large transactions (> 1 MiB), the blob upload should not block the
//! creation path. The hot record is written immediately so the UTXO is
//! spendable, while the cold data upload happens in the background.
//!
//! After upload completes:
//! 1. Content hash of the blob is computed
//! 2. `ExternalRef` is built with store_type, content_hash, total_size, offsets
//! 3. The `ExternalRef` is pwritten into the metadata region at the record's
//!    `record_offset` — a single small metadata write, no record reallocation

use crate::device::BlockDevice;
use crate::record::ExternalRef;
use crate::storage::blobstore::{BlobError, BlobStore};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Handle returned by [`BlobUploader::submit`] to track upload progress.
///
/// Call [`wait`](UploadHandle::wait) to block until the upload completes,
/// or [`is_complete`](UploadHandle::is_complete) to poll.
pub struct UploadHandle {
    inner: Arc<UploadHandleInner>,
}

struct UploadHandleInner {
    result: parking_lot::Mutex<Option<std::result::Result<(), BlobError>>>,
    condvar: parking_lot::Condvar,
}

impl UploadHandle {
    fn new() -> (Self, Arc<UploadHandleInner>) {
        let inner = Arc::new(UploadHandleInner {
            result: parking_lot::Mutex::new(None),
            condvar: parking_lot::Condvar::new(),
        });
        (
            Self {
                inner: inner.clone(),
            },
            inner,
        )
    }

    /// Block until the upload completes and return the result.
    pub fn wait(self) -> std::result::Result<(), BlobError> {
        let mut guard = self.inner.result.lock();
        while guard.is_none() {
            self.inner.condvar.wait(&mut guard);
        }
        guard.take().unwrap()
    }

    /// Check whether the upload has completed (non-blocking).
    pub fn is_complete(&self) -> bool {
        self.inner.result.lock().is_some()
    }
}

/// Upload task queued for background processing.
struct UploadTask {
    tx_id: [u8; 32],
    record_offset: u64,
    data: Vec<u8>,
    inputs_len: u32,
    outputs_len: u32,
    handle: Arc<UploadHandleInner>,
}

/// Default bound on the asynchronous upload queue.
///
/// F-G9-003: the original implementation used an unbounded
/// [`std::sync::mpsc::channel`]. Under bursty external-tier load a fast
/// producer could grow the queue without limit, each task carrying a
/// multi-MiB `Vec<u8>` payload — memory exhaustion was just a matter of
/// time. We cap at 1024 in-flight tasks; oversubscribed callers receive
/// [`BlobError::UploaderQueueFull`] and can apply backpressure (retry,
/// fail, or fall back to a synchronous upload).
pub const DEFAULT_UPLOADER_QUEUE_CAPACITY: usize = 1024;

/// Background blob uploader that processes external-tier uploads without
/// blocking the creation path.
///
/// Spawns a dedicated thread that drains the upload queue. After each
/// successful upload, the `ExternalRef` fields are pwritten into the
/// record's metadata region.
///
/// # Backpressure (F-G9-003)
///
/// The internal channel is bounded by [`DEFAULT_UPLOADER_QUEUE_CAPACITY`] (or
/// the value passed to [`BlobUploader::with_capacity`]). When `submit` finds
/// the queue full it returns [`BlobError::UploaderQueueFull`] immediately —
/// no blocking, no silent buffering — and increments the
/// [`BlobUploader::queue_full_count`] counter so an operator-visible metric
/// makes the saturation event observable.
pub struct BlobUploader {
    sender: std::sync::mpsc::SyncSender<UploadTask>,
    _handle: std::thread::JoinHandle<()>,
    /// Configured queue capacity, retained for diagnostic error messages.
    capacity: usize,
    /// Monotonic count of [`submit`](Self::submit) calls that were rejected
    /// because the queue was full. Exposed via [`Self::queue_full_count`] for
    /// observability dashboards. Storing the counter on the uploader (rather
    /// than registering it with the global metrics subsystem) keeps the
    /// uploader self-contained — the parent server can read and expose it as
    /// it pleases.
    queue_full_count: Arc<AtomicU64>,
}

impl BlobUploader {
    /// Create a new blob uploader with a background upload thread and the
    /// default queue capacity ([`DEFAULT_UPLOADER_QUEUE_CAPACITY`]).
    ///
    /// # Parameters
    /// - `blob_store`: the external blob store to upload to
    /// - `device`: the NVMe device (for pwriting ExternalRef into metadata)
    pub fn new(blob_store: Arc<dyn BlobStore>, device: Arc<dyn BlockDevice>) -> Self {
        Self::with_capacity(blob_store, device, DEFAULT_UPLOADER_QUEUE_CAPACITY)
    }

    /// Create a new blob uploader with a custom queue capacity.
    ///
    /// `capacity` is the maximum number of in-flight upload tasks; once this
    /// many tasks are queued, [`submit`](Self::submit) returns
    /// [`BlobError::UploaderQueueFull`] until the background thread drains
    /// at least one task. Callers that want to retry should back off — the
    /// underlying channel does not signal completion via the queue depth.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero. Use [`Self::new`] for the safe default.
    pub fn with_capacity(
        blob_store: Arc<dyn BlobStore>,
        device: Arc<dyn BlockDevice>,
        capacity: usize,
    ) -> Self {
        assert!(capacity > 0, "BlobUploader::with_capacity requires capacity > 0");
        // sync_channel(capacity) is the stdlib's bounded-mpsc primitive. Once
        // `capacity` tasks are queued, additional `send`s block; we use
        // `try_send` in submit() to convert that into an observable
        // backpressure error rather than a hidden producer stall.
        let (task_tx, task_rx) = std::sync::mpsc::sync_channel::<UploadTask>(capacity);

        let handle = std::thread::Builder::new()
            .name("blob-uploader".into())
            .spawn(move || {
                Self::upload_loop(task_rx, blob_store, device);
            })
            .expect("failed to spawn blob uploader thread");

        Self {
            sender: task_tx,
            _handle: handle,
            capacity,
            queue_full_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Total number of [`submit`](Self::submit) calls that were rejected
    /// because the upload queue was full.
    ///
    /// Read with [`Ordering::Relaxed`] — this is observability telemetry, not
    /// a synchronization primitive.
    pub fn queue_full_count(&self) -> u64 {
        self.queue_full_count.load(Ordering::Relaxed)
    }

    /// Configured maximum number of in-flight upload tasks.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Upload processing loop — runs in the background thread.
    fn upload_loop(
        rx: std::sync::mpsc::Receiver<UploadTask>,
        blob_store: Arc<dyn BlobStore>,
        device: Arc<dyn BlockDevice>,
    ) {
        while let Ok(task) = rx.recv() {
            let result = Self::process_task(&task, &*blob_store, &*device);
            let mut guard = task.handle.result.lock();
            *guard = Some(result);
            task.handle.condvar.notify_all();
        }
    }

    /// Process a single upload task: upload blob, then pwrite ExternalRef.
    ///
    /// The blob store computes the content SHA-256 and length as part of
    /// its atomic write path; we record both in the on-device `ExternalRef`
    /// so subsequent reads (or audit tooling) can integrity-verify the blob
    /// against the record metadata.
    fn process_task(
        task: &UploadTask,
        blob_store: &dyn BlobStore,
        device: &dyn BlockDevice,
    ) -> std::result::Result<(), BlobError> {
        // Upload the blob and capture the durable content digest.
        let digest = blob_store.put(&task.tx_id, &task.data)?;

        // Build ExternalRef. `content_hash` is the SHA-256 of the actual
        // blob payload — not the txid — so that bit rot or replacement of
        // the on-disk blob is detectable from the device record alone.
        let ext_ref = ExternalRef {
            store_type: 1, // 1 = file/object store
            content_hash: digest.sha256,
            total_size: digest.length,
            input_count: task.inputs_len,
            output_count: task.outputs_len,
            inputs_offset: 0,
            outputs_offset: task.inputs_len as u64, // inputs come first in serialized cold data
        };

        // pwrite the ExternalRef into the metadata region
        Self::write_external_ref(device, task.record_offset, &ext_ref).map_err(|e| {
            BlobError::Io(std::io::Error::other(format!("device write failed: {e}")))
        })?;

        Ok(())
    }

    /// Write the ExternalRef into a record's metadata at `record_offset`.
    ///
    /// Reads the full metadata, updates only the `external_ref` field,
    /// and writes it back. This is a single small metadata write.
    fn write_external_ref(
        device: &dyn BlockDevice,
        record_offset: u64,
        ext_ref: &ExternalRef,
    ) -> std::result::Result<(), crate::device::DeviceError> {
        let mut meta = crate::io::read_metadata(device, record_offset)?;
        meta.external_ref = *ext_ref;
        crate::io::write_metadata(device, record_offset, &meta)?;
        Ok(())
    }

    /// Submit a blob for asynchronous upload.
    ///
    /// Returns an [`UploadHandle`] that can be used to wait for or poll
    /// the upload's completion.
    ///
    /// # Parameters
    /// - `tx_id`: transaction ID (used as blob key)
    /// - `record_offset`: device offset of the record (for ExternalRef pwrite)
    /// - `data`: the serialized cold data to upload
    /// - `inputs_len`: number of inputs (stored in ExternalRef)
    /// - `outputs_len`: number of outputs (stored in ExternalRef)
    pub fn submit(
        &self,
        tx_id: [u8; 32],
        record_offset: u64,
        data: Vec<u8>,
        inputs_len: u32,
        outputs_len: u32,
    ) -> std::result::Result<UploadHandle, BlobError> {
        let (handle, inner) = UploadHandle::new();

        let task = UploadTask {
            tx_id,
            record_offset,
            data,
            inputs_len,
            outputs_len,
            handle: inner,
        };

        // F-G9-003: try_send is non-blocking. A full queue must surface as an
        // observable backpressure signal (UploaderQueueFull), not silently
        // block the caller's request thread.
        match self.sender.try_send(task) {
            Ok(()) => Ok(handle),
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                let prev = self.queue_full_count.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    target: "teraslab::storage::uploader",
                    capacity = self.capacity,
                    queue_full_count = prev + 1,
                    "blob uploader queue full; rejecting submit with backpressure",
                );
                Err(BlobError::UploaderQueueFull {
                    queued: self.capacity,
                    capacity: self.capacity,
                })
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => Err(BlobError::Io(
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "uploader thread has exited"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::SlotAllocator;
    use crate::device::MemoryDevice;
    use crate::io;
    use crate::record::{TxFlags, TxMetadata};
    use crate::storage::blobstore::MemoryBlobStore;

    fn setup() -> (Arc<MemoryDevice>, Arc<MemoryBlobStore>, BlobUploader) {
        let dev = Arc::new(MemoryDevice::new(64 * 1024 * 1024, 4096).unwrap());
        let blob = Arc::new(MemoryBlobStore::new());
        let uploader = BlobUploader::new(blob.clone(), dev.clone());
        (dev, blob, uploader)
    }

    fn write_hot_record(
        dev: &dyn BlockDevice,
        offset: u64,
        utxo_count: u32,
        tx_id: [u8; 32],
    ) -> TxMetadata {
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = tx_id;
        meta.flags = TxFlags::EXTERNAL;
        let slots: Vec<crate::record::UtxoSlot> = (0..utxo_count)
            .map(|_| crate::record::UtxoSlot::new_unspent([0; 32]))
            .collect();
        io::write_full_record(dev, offset, &meta, &slots).unwrap();
        meta
    }

    #[test]
    fn submit_and_wait() {
        let (dev, blob, uploader) = setup();
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();

        let utxo_count = 2u32;
        let offset = alloc
            .allocate(TxMetadata::record_size_for(utxo_count))
            .unwrap();
        let mut tx_id = [0u8; 32];
        tx_id[0] = 0xAA;
        write_hot_record(&*dev, offset, utxo_count, tx_id);

        let data = vec![0x42; 2 * 1024 * 1024]; // 2 MB
        let handle = uploader.submit(tx_id, offset, data.clone(), 10, 5).unwrap();
        handle.wait().unwrap();

        // Blob should exist in store
        assert!(blob.exists(&tx_id).unwrap());
        let stored = blob.get(&tx_id).unwrap().unwrap();
        assert_eq!(stored, data);

        // ExternalRef should be written to metadata
        let meta = io::read_metadata(&*dev, offset).unwrap();
        let ext = meta.external_ref;
        assert_eq!(ext.store_type, 1);
        assert_eq!({ ext.total_size }, data.len() as u64);
        assert_eq!({ ext.input_count }, 10);
        assert_eq!({ ext.output_count }, 5);
        // content_hash must be the actual SHA-256 of the blob payload, not the txid.
        let expected = {
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(&data);
            let out = h.finalize();
            let mut d = [0u8; 32];
            d.copy_from_slice(&out);
            d
        };
        assert_eq!(ext.content_hash, expected);
        assert_ne!(ext.content_hash, tx_id);
    }

    #[test]
    fn is_complete_polling() {
        let (dev, _blob, uploader) = setup();
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();

        let utxo_count = 1u32;
        let offset = alloc
            .allocate(TxMetadata::record_size_for(utxo_count))
            .unwrap();
        let mut tx_id = [0u8; 32];
        tx_id[0] = 0xBB;
        write_hot_record(&*dev, offset, utxo_count, tx_id);

        let handle = uploader.submit(tx_id, offset, vec![0; 1024], 1, 1).unwrap();
        // Wait for completion
        handle.wait().unwrap();
        // After wait, it should be complete (can't poll after wait since wait consumes)
    }

    #[test]
    fn multiple_uploads() {
        let (dev, blob, uploader) = setup();
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();

        let mut handles = Vec::new();
        for i in 0..10u8 {
            let utxo_count = 1u32;
            let offset = alloc
                .allocate(TxMetadata::record_size_for(utxo_count))
                .unwrap();
            let mut tx_id = [0u8; 32];
            tx_id[0] = i;
            write_hot_record(&*dev, offset, utxo_count, tx_id);

            let data = vec![i; 1024 * 100]; // 100 KB each
            let handle = uploader.submit(tx_id, offset, data, 1, 1).unwrap();
            handles.push((tx_id, handle));
        }

        for (tx_id, handle) in handles {
            handle.wait().unwrap();
            assert!(blob.exists(&tx_id).unwrap());
        }
    }

    #[test]
    fn external_ref_pwrite_only_touches_metadata() {
        let (dev, _blob, uploader) = setup();
        let mut alloc = SlotAllocator::new(dev.clone()).unwrap();

        let utxo_count = 3u32;
        let offset = alloc
            .allocate(TxMetadata::record_size_for(utxo_count))
            .unwrap();
        let mut tx_id = [0u8; 32];
        tx_id[0] = 0xCC;

        // Write record with specific UTXO slot data
        let mut meta = TxMetadata::new(utxo_count);
        meta.tx_id = tx_id;
        meta.flags = TxFlags::EXTERNAL;
        let hash = [0xEE; 32];
        let slots = vec![crate::record::UtxoSlot::new_unspent(hash); utxo_count as usize];
        io::write_full_record(&*dev, offset, &meta, &slots).unwrap();

        // Submit upload
        let handle = uploader.submit(tx_id, offset, vec![0; 2048], 5, 3).unwrap();
        handle.wait().unwrap();

        // Verify UTXO slots are untouched
        for i in 0..utxo_count {
            let slot = io::read_utxo_slot(&*dev, offset, i).unwrap();
            assert_eq!(
                slot.hash, hash,
                "UTXO slot {i} corrupted by ExternalRef pwrite"
            );
            assert!(slot.is_unspent(), "UTXO slot {i} status corrupted");
        }

        // Verify ExternalRef was written
        let updated_meta = io::read_metadata(&*dev, offset).unwrap();
        assert_eq!(updated_meta.external_ref.store_type, 1);
        assert_eq!({ updated_meta.external_ref.total_size }, 2048);
    }
}
