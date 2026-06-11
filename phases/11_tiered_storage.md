# Phase 11: Tiered storage

**Status:** partial — `src/storage/` hot tier + external blob store shipped, including R-049 orphan-blob GC at recovery + periodic sweep. The separate-NVMe middle tier described in this phase is intentionally not enabled (see the implementation note below) because the fixed `TxMetadata` layout has no durable offset/length fields for it; reintroducing the tier requires a metadata schema migration.

## Goal

Implement the tiered storage system for transaction inputs/outputs. Small txs have data inline on NVMe, medium txs in a separate NVMe region, and large txs in an external blob store. This completes the storage architecture.

**Current implementation note (R-108):** the separate-NVMe middle tier described in this phase is not enabled in production. The fixed `TxMetadata` layout has no durable `separate_cold_offset` / `separate_cold_size` fields, so medium cold data now routes to the external blob tier instead of creating an unreferencable device allocation. Reintroducing this tier requires a metadata schema migration.

## Dependencies

Phases 1-10 must be complete with all tests passing.

## Reference

- `specs/BSV_UTXO_STORE_SPEC.md` §2.3 (Record Layout on NVMe)
- `specs/BSV_UTXO_STORE_SPEC.md` §2.6 (External Reference Structure)
- `specs/BSV_UTXO_STORE_SPEC.md` §4.3-4.4 (Tiered Storage and Creation Pipeline)
- The original implementation uses an `external` boolean flag and `externalStore=file://` path

## Record layout recap

The on-disk record uses a **metadata-first** layout (see spec §2.3). All offsets are deterministic:

```
record_offset + 0                                       → Metadata (fixed METADATA_SIZE)
record_offset + METADATA_SIZE                           → UTXO slots (utxo_count × 69 bytes)
record_offset + METADATA_SIZE + utxo_count * 69         → Cold data (inline tier only)
```

Key points for tiered storage:

- **Cold data offset is deterministic**: `METADATA_SIZE + utxo_count * 69`. No pointer or offset field is needed in metadata or the index entry to locate inline cold data — it is computed from `utxo_count` (which is in metadata).
- **UTXO slots are 69 bytes each** (32B hash + 1B status + 36B spending_data).
- **ExternalRef is part of the fixed metadata region** (~73 bytes, see spec §2.6). For large txs whose blob upload completes asynchronously, the `external_ref` fields are populated via a `pwrite` to the metadata region — no separate region or pointer indirection.
- **Reassignments use extension blocks** referenced by `reassignment_offset` in metadata (a device offset to a separately allocated block). They are NOT stored inline in the record. The cold data region contains only `inputs`, `outputs`, and `inpoints` — all write-once.
- **No "Region D" or inline variable data region exists**. The record has exactly three contiguous sections: metadata, UTXO slots, cold data.

## What to build

### 11.1 Storage tier definitions — `src/storage/tiers.rs`

```rust
pub const INLINE_THRESHOLD: usize = 8 * 1024;      // 8 KiB — same NVMe write as hot record
pub const SEPARATE_THRESHOLD: usize = 1024 * 1024;  // 1 MiB — separate NVMe write
// Above SEPARATE_THRESHOLD → external blob store

pub enum StorageTier {
    Inline,             // Cold data in same allocation as hot record
    SeparateNvme,       // Cold data on same device, separate allocation
    External(BlobRef),  // Cold data in external blob store
}
```

### 11.2 Blob store trait — `src/storage/blobstore.rs`

```rust
pub trait BlobStore: Send + Sync {
    /// Write a blob. Key is the txid.
    fn put(&self, key: &[u8; 32], data: &[u8]) -> Result<()>;

    /// Read a blob. Returns None if not found.
    fn get(&self, key: &[u8; 32]) -> Result<Option<Vec<u8>>>;

    /// Read a range of bytes from a blob (for partial reads).
    fn get_range(&self, key: &[u8; 32], offset: u64, length: u64) -> Result<Option<Vec<u8>>>;

    /// Delete a blob.
    fn delete(&self, key: &[u8; 32]) -> Result<()>;

    /// Check if a blob exists.
    fn exists(&self, key: &[u8; 32]) -> Result<bool>;

    /// Stream a blob to a writer (for large blobs).
    fn stream_to(&self, key: &[u8; 32], writer: &mut dyn std::io::Write) -> Result<u64>;
}
```

### 11.3 File-based blob store — `src/storage/file_blobstore.rs`

Store blobs as files in a directory, organized by hash prefix:

```
base_dir/
  ab/
    cd/
      abcd1234...5678  (full txid hex as filename)
```

This matches the current Teranode `externalStore=file://` pattern.

```rust
pub struct FileBlobStore {
    base_dir: PathBuf,
    hash_prefix_depth: usize,  // number of hex character pairs for subdirectories (default 2)
}
```

### 11.4 S3-compatible blob store — `src/storage/s3_blobstore.rs`

Optional S3/MinIO backend for cloud deployments.

```rust
pub struct S3BlobStore {
    bucket: String,
    prefix: String,
    client: S3Client,  // from aws-sdk-s3 or rusoto
}
```

Implement the same BlobStore trait. Use async internally.

### 11.5 Tiered storage manager — `src/storage/manager.rs`

Coordinates the three tiers:

```rust
pub struct StorageManager {
    device: Arc<dyn BlockDevice>,
    allocator: Arc<Mutex<SlotAllocator>>,
    blob_store: Arc<dyn BlobStore>,
    inline_threshold: usize,
    separate_threshold: usize,
}

impl StorageManager {
    /// Determine which tier to use for the given data size.
    pub fn tier_for_size(&self, data_size: usize) -> StorageTier;

    /// Write cold data during record creation.
    ///
    /// For inline tier: cold data is appended to the record at the deterministic
    /// offset `METADATA_SIZE + utxo_count * 69`. The caller includes it in the
    /// same io_uring SQE as the hot record. Returns `ColdDataRef::Inline`.
    ///
    /// For separate NVMe tier: allocates a separate device block, writes cold
    /// data there. Returns `ColdDataRef::SeparateNvme` with the device offset.
    ///
    /// For external tier: returns `ColdDataRef::External` immediately. The actual
    /// blob upload is handled by `BlobUploader` (§11.8). Once the upload completes,
    /// the `external_ref` fields in the record's metadata region are populated
    /// via a metadata pwrite.
    pub fn write_cold_data(
        &self,
        tx_id: &[u8; 32],
        inputs: &[u8],
        outputs: &[u8],
        inpoints: &[u8],
        utxo_count: u32,
        record_offset: u64,
    ) -> Result<ColdDataRef>;

    /// Read cold data for a record.
    ///
    /// For inline tier: computes cold data offset as
    /// `record_offset + METADATA_SIZE + utxo_count * 69` and issues a pread.
    ///
    /// For separate NVMe tier: reads from the separate device allocation
    /// (offset stored in the separate_nvme_offset field).
    ///
    /// For external tier: reads the ExternalRef from metadata and fetches
    /// from the blob store.
    pub fn read_cold_data(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &Metadata,
    ) -> Result<ColdData>;

    /// Stream cold data to a writer (for large external blobs).
    pub fn stream_cold_data(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &Metadata,
        writer: &mut dyn std::io::Write,
    ) -> Result<u64>;

    /// Delete cold data when a record is pruned.
    ///
    /// For inline tier: no-op (freed with the record allocation).
    /// For separate NVMe tier: returns the separate allocation to the freelist.
    /// For external tier: deletes the blob from the blob store.
    pub fn delete_cold_data(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &Metadata,
        tx_id: &[u8; 32],
    ) -> Result<()>;

    /// Read only inputs data (for validation without full read).
    pub fn read_inputs(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &Metadata,
    ) -> Result<Option<Vec<u8>>>;

    /// Read only a specific output (for SPV proof generation).
    pub fn read_output_at(
        &self,
        record_offset: u64,
        utxo_count: u32,
        metadata: &Metadata,
        output_index: u32,
    ) -> Result<Option<Vec<u8>>>;
}

/// Result of a cold data write. The tier determines how to locate the data
/// on subsequent reads.
pub enum ColdDataRef {
    /// Cold data written inline at deterministic offset. No extra state needed —
    /// offset is always `METADATA_SIZE + utxo_count * 69`.
    Inline { cold_size: u32 },

    /// Cold data written to a separate NVMe allocation.
    SeparateNvme { device_offset: u64, cold_size: u32 },

    /// Cold data will be uploaded to external blob store asynchronously.
    /// The ExternalRef in metadata is populated once the upload completes.
    External,
}

pub struct ColdData {
    pub inputs: Vec<u8>,
    pub outputs: Vec<u8>,
    pub inpoints: Vec<u8>,
}
```

### 11.6 Input reference storage — `src/storage/input_refs.rs`

For large transactions, store compact outpoint references on NVMe for fast validation:

```rust
pub struct InputRef {
    pub prev_txid: [u8; 32],
    pub prev_vout: u32,
}
// 36 bytes per input

pub fn write_input_refs(
    device: &dyn BlockDevice,
    offset: u64,
    refs: &[InputRef],
) -> Result<()>;

pub fn read_input_refs(
    device: &dyn BlockDevice,
    offset: u64,
    count: u32,
) -> Result<Vec<InputRef>>;
```

### 11.7 Integration with creation path

> **As-implemented (IJ-3):** The shipped creation path does **not** consult
> `tier_for_size` / `INLINE_THRESHOLD` to choose a tier. Tiering is
> **client-driven**: the client sets the `FLAG_EXTERNAL_BLOB` request flag to
> route cold data to the external blob store (the payload having been
> pre-uploaded via the streaming chunk protocol); without the flag, cold data
> is written inline in the same NVMe allocation as the hot record. The server
> does not re-derive placement from size — by the time the frame arrives the
> client has already committed to inline-vs-streamed. The `SeparateNvme` tier
> below was never enabled (no durable offset/length metadata fields exist for
> it), and the `uploader.rs` / storage-manager components described in 11.8
> were removed. `tier_for_size` / `INLINE_THRESHOLD` / `StorageTier` remain in
> `src/storage/tiers.rs` as an **advisory client-side size guideline**, not a
> server-enforced threshold. Only `inline_cold_offset` is on a live path (the
> read path's inline-region anchor). Steps 2–5 below are the original design,
> retained for historical context.

Modify `create` (Phase 5) to use the storage manager:

1. Compute total cold data size: `len(inputs) + len(outputs) + len(inpoints)` plus length-prefix overhead
2. Determine tier via `tier_for_size(cold_data_size)`
3. **Inline**: allocate a single contiguous record of size `METADATA_SIZE + utxo_count * 69 + cold_data_size`. Write metadata + UTXO slots + cold data in one io_uring SQE
4. **Separate NVMe**: allocate hot record of size `METADATA_SIZE + utxo_count * 69` (no cold region). Write hot record first (tx is immediately spendable). Allocate a separate device block and write cold data asynchronously
5. **External**: allocate hot record of size `METADATA_SIZE + utxo_count * 69` (no cold region). Write hot record with `EXTERNAL` flag set. Kick off async blob upload via `BlobUploader`. Once upload completes, `pwrite` the `external_ref` fields in the metadata region
6. For large txs: also write input_refs on NVMe for fast spend validation

### 11.8 Async blob upload — `src/storage/uploader.rs`

For external blobs, the upload should not block the creation path:

```rust
pub struct BlobUploader {
    queue: mpsc::Sender<UploadTask>,
    blob_store: Arc<dyn BlobStore>,
}

struct UploadTask {
    tx_id: [u8; 32],
    record_offset: u64,   // needed to pwrite external_ref into metadata
    data: Vec<u8>,        // or a path to read from
    completion: oneshot::Sender<Result<()>>,
}
```

The uploader runs as a background task. After upload completes:
1. Compute content hash of the uploaded blob
2. Build `ExternalRef` struct with `store_type`, `content_hash`, `total_size`, offsets and lengths for inputs/outputs within the blob
3. `pwrite` the `ExternalRef` into the metadata region at the record's `record_offset` (the `external_ref` field is at a fixed compile-time offset within `METADATA_SIZE`)

This is a single small metadata write — no record reallocation needed.

## Acceptance criteria

### Tier classification tests

```
- [ ] 100 byte payload → Inline
- [ ] 8000 byte payload → Inline (just under threshold)
- [ ] 8193 byte payload → SeparateNvme (just over threshold)
- [ ] 500 KiB payload → SeparateNvme
- [ ] 1 MiB + 1 byte payload → External
- [ ] 320 MB payload → External
```

### File blob store tests

```
- [ ] Put and get: data matches
- [ ] Put, delete, get: returns None
- [ ] exists: true after put, false after delete
- [ ] get_range: returns correct byte range
- [ ] stream_to: writes correct data to writer
- [ ] Two blobs with same first 2 prefix bytes: both stored correctly (different files)
- [ ] Large blob (10 MB): put and get work correctly
- [ ] Concurrent puts: no corruption (different keys)
- [ ] Put with non-writable directory: returns error
```

### Inline cold data tests

```
- [ ] Create tx with 1 KiB inputs/outputs: data stored inline at METADATA_SIZE + utxo_count * 69
- [ ] Read back cold data: matches original (inputs, outputs, inpoints)
- [ ] Cold data offset matches deterministic formula
- [ ] Cold data survives record creation (same device write)
- [ ] Spend operation does NOT corrupt cold data (spends write to UTXO slots region only)
- [ ] SetMined does NOT corrupt cold data (setMined writes to metadata region only)
- [ ] Delete record: entire allocation (metadata + slots + cold data) freed
```

### Separate NVMe cold data tests

```
- [ ] Create tx with 100 KiB inputs/outputs: separate allocation
- [ ] Hot record size is exactly METADATA_SIZE + utxo_count * 69 (no cold region)
- [ ] Read back: matches original (inputs, outputs, inpoints)
- [ ] Hot record committed before cold data write
- [ ] UTXO is spendable before cold data write completes (if async)
- [ ] Delete: both hot record and separate cold allocation freed
```

### External blob tests

```
- [ ] Create tx with 2 MB data: blob uploaded to store
- [ ] Hot record size is exactly METADATA_SIZE + utxo_count * 69 (no cold region)
- [ ] Read metadata: external_ref populated in metadata after upload completes
- [ ] ExternalRef fields (store_type, content_hash, total_size, offsets, lengths) are correct
- [ ] Read cold data via storage manager: fetched from blob store using ExternalRef
- [ ] Stream cold data: streamed in chunks
- [ ] Delete: blob removed from store
- [ ] Get during upload (before completion): returns appropriate status
- [ ] After upload, external_ref pwrite only touches metadata region (69-byte UTXO slots untouched)
```

### Input refs tests

```
- [ ] Write 100 input refs, read back: all match
- [ ] Read individual input ref by index: correct
- [ ] Input refs survive independently of cold data
```

### End-to-end tiered storage tests

```
- [ ] Create small tx (200B), medium tx (50KiB), large tx (5MB):
      each uses correct tier, all data retrievable
- [ ] Pruning each tier: all data cleaned up correctly
- [ ] Mixed workload: 1000 small, 100 medium, 10 large: all correct
- [ ] Verify inline cold data offset = METADATA_SIZE + utxo_count * 69 for all inline records
```

## NOT in this phase

- No data migration between tiers (once placed, data stays in its tier)
- No automatic blob store cleanup for orphaned blobs (would need a GC pass)
