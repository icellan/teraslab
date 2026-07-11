//! Carving one physical device or file into multiple virtual sub-devices.
//!
//! A [`SubDevice`] is a [`BlockDevice`] mapped onto a disjoint byte range
//! `[base, base + len)` of a larger physical [`BlockDevice`]. Splitting a
//! device into K sub-devices yields K independent storage domains — each gets
//! its own allocator, redo log, and index in the layer above — that share one
//! physical device. This is the reference-style "virtual device" model: it
//! buys lock/WAL parallelism even on a single physical device. Physical I/O
//! bandwidth and the fsync barrier are still shared by co-located sub-devices.
//!
//! Works identically for raw block devices and regular files: both are
//! presented through the same [`BlockDevice`] trait (file-backed
//! [`DirectDevice`](crate::device::DirectDevice) included), so a sub-device
//! only ever translates an offset and never cares what the backing store is.
//!
//! ## Coalesced fsync barrier
//!
//! All sub-devices of one physical device share a single fsync barrier domain:
//! one `sync()` on the underlying fd flushes the device's write cache for
//! every prior write, regardless of which sub-range it targeted. So the
//! sub-devices share a [`PhysicalBarrier`] that COALESCES concurrent `sync()`
//! calls into a single underlying sync — group commit at the physical-device
//! level. Without this, K co-located redo logs would each issue a full-device
//! barrier and contend pointlessly.

use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::device::{BlockDevice, DeviceError, Result};

/// Coalesces fsync barriers across all [`SubDevice`]s carved from one physical
/// device. Owns the underlying device and serializes + coalesces its `sync()`.
pub struct PhysicalBarrier {
    inner: Arc<dyn BlockDevice>,
    state: Mutex<BarrierState>,
    cond: Condvar,
}

struct BarrierState {
    /// Number of underlying syncs that have COMPLETED. Monotonic.
    epoch: u64,
    /// Whether a sync syscall is currently in flight (at most one — syncs are
    /// serialized so a single underlying barrier can cover many callers).
    leader_busy: bool,
    /// `Display` of the error from the most recently completed sync, or `None`
    /// if it succeeded. Followers report the outcome of the barrier they
    /// coalesced onto via this field (the error object itself is not `Clone`).
    last_err: Option<String>,
    /// Highest [`epoch`](Self::epoch) whose sync FAILED. Monotonic: only ever
    /// raised, when a sync returns `Err`. A later successful sync clears
    /// [`last_err`](Self::last_err) but NOT this, so a follower whose qualifying
    /// generation failed can never be masked by a subsequent success (P0-10).
    last_failed_epoch: u64,
}

impl BarrierState {
    /// Outcome to report to a follower that has reached its wait target (i.e.
    /// `epoch >= target`), given `earliest_covering` — the earliest generation
    /// whose sync could have flushed (and therefore, on failure, DROPPED) the
    /// follower's already-issued write.
    ///
    /// A follower WAITS for `target` — the first sync guaranteed to begin after
    /// its `barrier()` call, so a success there proves durability. But when it
    /// coalesced behind a sync already in flight, the earliest sync that could
    /// have COVERED its write is one generation earlier (`target - 1`).
    /// Post-fsyncgate, a failed fsync can mark those dirty pages clean-and-
    /// errored so a LATER successful sync never re-flushes them; so the follower
    /// must report failure if ANY generation at or after `earliest_covering`
    /// failed — not merely at or after `target`, which would let a failed
    /// in-flight sync be masked by the next success (a co-located sub-device
    /// would then ack durability it never achieved).
    ///
    /// Returns `Err` iff `last_failed_epoch >= earliest_covering`. Conservative
    /// in the safe direction: a strictly-later failure also trips it, and it
    /// only ever fires while the device is actively failing syncs (it heals once
    /// the epoch advances past the failure).
    fn follower_outcome(&self, earliest_covering: u64) -> Result<()> {
        if self.last_failed_epoch >= earliest_covering {
            return Err(coalesced_barrier_error(
                self.last_err
                    .as_deref()
                    .unwrap_or("qualifying sync generation failed"),
            ));
        }
        Ok(())
    }
}

impl PhysicalBarrier {
    /// Wrap a physical device in a coalescing barrier.
    pub fn new(inner: Arc<dyn BlockDevice>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            state: Mutex::new(BarrierState {
                epoch: 0,
                leader_busy: false,
                last_err: None,
                last_failed_epoch: 0,
            }),
            cond: Condvar::new(),
        })
    }

    /// The underlying physical device (shared by all co-located sub-devices).
    #[inline]
    pub fn device(&self) -> &Arc<dyn BlockDevice> {
        &self.inner
    }

    /// Durably flush every write that completed before this call, coalescing
    /// concurrent callers onto a single underlying `sync()`.
    ///
    /// # Durability contract
    /// On return, all writes issued (to any co-located sub-device) before this
    /// call are on stable storage. Correctness rests on: the first underlying
    /// sync that *begins after* this call returns flushes the whole device's
    /// pending writes, including ours. A sync already in flight when we arrive
    /// began before our writes and may not cover them, so we wait for the one
    /// after it.
    ///
    /// # Errors
    /// Returns the underlying [`DeviceError`] for the leader that ran the
    /// failing sync; coalesced followers receive a
    /// [`DeviceError::Io`]-wrapped message (the source error is not `Clone`).
    fn barrier(&self) -> Result<()> {
        let mut st = self.state.lock();
        // Generation that must SUCCEED for our prior writes to be durable: the
        // first sync guaranteed to begin after this call. If a sync is already
        // in flight we wait for the one after it (it may have begun before our
        // writes and so may not cover them).
        let target = st.epoch + if st.leader_busy { 2 } else { 1 };
        // Earliest generation whose sync could have COVERED our already-issued
        // writes — and thus, on failure, dropped them: the in-flight sync when we
        // coalesce behind one (`epoch + 1`), else our own `target`. A failure at
        // or after this must surface even though we only WAIT for `target`.
        let earliest_covering = st.epoch + 1;
        loop {
            if st.epoch >= target {
                // A qualifying sync completed. Report from `last_failed_epoch`
                // (NOT the volatile `last_err`): a failure at or after the
                // earliest generation that could have covered our writes must
                // surface even if a later success has since cleared `last_err`,
                // or a co-located sub-device would ack durability it never got.
                return st.follower_outcome(earliest_covering);
            }
            if !st.leader_busy {
                // Become the leader for the next generation.
                st.leader_busy = true;
                drop(st);
                let res = self.inner.sync();
                let mut st2 = self.state.lock();
                st2.epoch += 1;
                st2.last_err = res.as_ref().err().map(|e| e.to_string());
                if res.is_err() {
                    // Remember the failed generation so a later success cannot
                    // mask it for a follower still coalesced onto this one.
                    st2.last_failed_epoch = st2.epoch;
                }
                st2.leader_busy = false;
                self.cond.notify_all();
                // The sync we just ran is, by construction, the qualifying one
                // for us — return its precise outcome directly.
                return res;
            }
            self.cond.wait(&mut st);
        }
    }
}

fn coalesced_barrier_error(msg: &str) -> DeviceError {
    DeviceError::Io(std::io::Error::other(format!(
        "coalesced device barrier failed: {msg}"
    )))
}

/// A virtual device mapped onto `[base, base + len)` of a physical device.
///
/// All I/O offsets are sub-device-relative (the sub-device presents an address
/// space starting at 0); they are translated by `base` and bounds-checked
/// against `len` before reaching the physical device. `sync()` delegates to the
/// shared [`PhysicalBarrier`] so co-located sub-devices coalesce their fsyncs.
pub struct SubDevice {
    barrier: Arc<PhysicalBarrier>,
    base: u64,
    len: u64,
}

impl SubDevice {
    /// Create a sub-device over `[base, base + len)` of `barrier`'s device.
    ///
    /// # Errors
    /// - [`DeviceError::OutOfBounds`] if `base + len` exceeds the physical
    ///   device size or overflows.
    /// - [`DeviceError::AlignmentViolation`] if `base` or `len` is not a
    ///   multiple of the device alignment (required for `O_DIRECT`).
    /// - [`DeviceError::ZeroSize`] if `len` is 0.
    pub fn new(barrier: Arc<PhysicalBarrier>, base: u64, len: u64) -> Result<Arc<Self>> {
        if len == 0 {
            return Err(DeviceError::ZeroSize);
        }
        let align = barrier.device().alignment() as u64;
        if !base.is_multiple_of(align) || !len.is_multiple_of(align) {
            return Err(DeviceError::AlignmentViolation {
                detail: format!("base {base} / len {len} not a multiple of alignment {align}"),
            });
        }
        let dev_size = barrier.device().size();
        let end = base.checked_add(len).ok_or(DeviceError::OutOfBounds {
            offset: base,
            len,
            device_size: dev_size,
        })?;
        if end > dev_size {
            return Err(DeviceError::OutOfBounds {
                offset: base,
                len,
                device_size: dev_size,
            });
        }
        Ok(Arc::new(Self { barrier, base, len }))
    }

    #[inline]
    fn translate(&self, offset: u64, n: usize) -> Result<u64> {
        let end = offset
            .checked_add(n as u64)
            .ok_or(DeviceError::OutOfBounds {
                offset,
                len: n as u64,
                device_size: self.len,
            })?;
        if end > self.len {
            return Err(DeviceError::OutOfBounds {
                offset,
                len: n as u64,
                device_size: self.len,
            });
        }
        Ok(self.base + offset)
    }
}

impl BlockDevice for SubDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let phys = self.translate(offset, buf.len())?;
        self.barrier.device().pread(buf, phys)
    }

    fn pread_nocache(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let phys = self.translate(offset, buf.len())?;
        self.barrier.device().pread_nocache(buf, phys)
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
        let phys = self.translate(offset, buf.len())?;
        self.barrier.device().pwrite(buf, phys)
    }

    fn alignment(&self) -> usize {
        self.barrier.device().alignment()
    }

    fn size(&self) -> u64 {
        self.len
    }

    fn sync(&self) -> Result<()> {
        self.barrier.barrier()
    }

    fn as_raw_ptr(&self) -> Option<*mut u8> {
        // Memory-backed physical devices expose a pointer; offset it to this
        // sub-device's region. File/raw O_DIRECT devices return None and the
        // engine falls back to pread/pwrite — which already work via translate.
        self.barrier
            .device()
            .as_raw_ptr()
            .map(|p| unsafe { p.add(self.base as usize) })
    }

    fn is_block_device(&self) -> bool {
        self.barrier.device().is_block_device()
    }

    fn physical_device_id(&self) -> Option<crate::device::PhysicalDeviceId> {
        // A sub-device's writes land on the shared physical device, and its
        // `sync()` delegates to that device's barrier — so its physical
        // identity IS the underlying device's. Two sub-devices carved from ONE
        // physical device (a `device_split` store's data + redo) therefore
        // report the SAME id and classify as co-located (FU#8): an fsync of
        // either flushes the other's writes on the shared device.
        self.barrier.device().physical_device_id()
    }
}

/// Split a physical device into `k` equal-sized virtual sub-devices that share
/// one coalescing fsync barrier.
///
/// Each region is the device size divided by `k`, rounded DOWN to the device
/// alignment so every sub-device is `O_DIRECT`-aligned; any remainder past the
/// last region is left unused. Region *i* owns `[i*region, i*region + region)`.
/// The mapping is deterministic, so recovery re-derives identical regions from
/// `(device size, k)`.
///
/// # Errors
/// - [`DeviceError::ZeroSize`] if `k` is 0 or the device is too small to give
///   every region at least one alignment block.
pub fn split_device(inner: Arc<dyn BlockDevice>, k: usize) -> Result<Vec<Arc<SubDevice>>> {
    if k == 0 {
        return Err(DeviceError::ZeroSize);
    }
    let align = inner.alignment() as u64;
    let total = inner.size();
    // Largest alignment-multiple region that fits k times.
    let region = (total / k as u64) / align * align;
    if region == 0 {
        return Err(DeviceError::ZeroSize);
    }
    let barrier = PhysicalBarrier::new(inner);
    let mut subs = Vec::with_capacity(k);
    for i in 0..k as u64 {
        subs.push(SubDevice::new(barrier.clone(), i * region, region)?);
    }
    Ok(subs)
}

/// Maximum number of stores (virtual devices) a node may run.
///
/// Bounded by [`crate::index::TxIndexEntry`]'s `device_id`, a `u8`: a store
/// index recorded in the index must fit in `0..=255`.
pub const MAX_STORES: usize = u8::MAX as usize + 1;

/// Error returned when a configured store count is unusable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreCountError {
    /// No stores (empty `device_paths` or zero `device_split`).
    #[error("at least one store must be configured")]
    Zero,
    /// More stores than the `u8` `device_id` index can represent.
    #[error("store count {count} exceeds the maximum of {MAX_STORES}")]
    TooMany {
        /// The rejected store count.
        count: usize,
    },
}

/// Validate a node's total store count (`num_physical_devices × device_split`):
/// must be `1..=MAX_STORES`. Called once at startup so the per-create placement
/// fast path can assume a valid count.
pub fn validate_store_count(num_stores: usize) -> std::result::Result<(), StoreCountError> {
    match num_stores {
        0 => Err(StoreCountError::Zero),
        n if n > MAX_STORES => Err(StoreCountError::TooMany { count: n }),
        _ => Ok(()),
    }
}

/// How a new record is assigned to a store at create time.
///
/// Placement is a free LOCAL choice: the chosen store is recorded in the index
/// entry's `device_id`, and every later access (read, spend, setMined, delete)
/// routes by that recorded `device_id`, never by re-deriving placement. So the
/// strategy only affects WHICH store a *new* record lands on; switching modes
/// on an existing store is safe — already-written records keep whatever
/// `device_id` was recorded for them and remain readable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PlacementStrategy {
    /// Even fill across stores via a rotating counter (default). Placement is
    /// independent of the txid — record N goes to store `N % num_stores`.
    #[default]
    RoundRobin,
    /// Deterministic function of the txid: store = `last8(txid) as u64 (LE) %
    /// num_stores`. Because a txid is a double-SHA256 (uniformly random), this
    /// is uniform across stores, and a record's store is computable from its
    /// txid for EVERY op — the foundation for per-store dispatch routing.
    ///
    /// The cluster already shards BETWEEN nodes on the FIRST bytes of the txid,
    /// so this uses the LAST 8 bytes: independent of the inter-node shard, and
    /// uniform within a single node.
    Txid,
}

/// Store placement for new records.
///
/// Wraps a [`PlacementStrategy`] plus the round-robin counter. Placement at
/// create time is a free local choice — the chosen store is recorded in the
/// index entry's `device_id`, so reads and later mutations follow the index,
/// not any function of the key. See [`PlacementStrategy`] for why switching
/// modes is safe for already-written records.
#[derive(Debug)]
pub struct StorePlacer {
    strategy: PlacementStrategy,
    num_stores: usize,
    next: std::sync::atomic::AtomicUsize,
}

impl StorePlacer {
    /// Create a placer over `num_stores` stores (must be >= 1) using `strategy`.
    pub fn new(strategy: PlacementStrategy, num_stores: usize) -> Self {
        debug_assert!(num_stores >= 1);
        Self {
            strategy,
            num_stores,
            next: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Convenience constructor for the default round-robin strategy.
    pub fn round_robin(num_stores: usize) -> Self {
        Self::new(PlacementStrategy::RoundRobin, num_stores)
    }

    /// The placement strategy in effect.
    #[inline]
    pub fn strategy(&self) -> PlacementStrategy {
        self.strategy
    }

    /// Choose the store index in `0..num_stores` for a record with `txid`.
    ///
    /// In [`PlacementStrategy::RoundRobin`] the txid is ignored and a rotating
    /// counter is used. In [`PlacementStrategy::Txid`] the store is the
    /// little-endian `u64` formed from the LAST 8 bytes of `txid`, modulo
    /// `num_stores` — deterministic and uniform for random txids.
    #[inline]
    pub fn place(&self, txid: &[u8; 32]) -> usize {
        if self.num_stores == 1 {
            return 0;
        }
        match self.strategy {
            PlacementStrategy::RoundRobin => {
                self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.num_stores
            }
            PlacementStrategy::Txid => {
                let last8: [u8; 8] = txid[24..32]
                    .try_into()
                    .expect("a 32-byte txid always has 8 trailing bytes");
                (u64::from_le_bytes(last8) % self.num_stores as u64) as usize
            }
        }
    }

    /// Number of stores this placer distributes over.
    #[inline]
    pub fn num_stores(&self) -> usize {
        self.num_stores
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemoryDevice;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn mem(size: u64) -> Arc<dyn BlockDevice> {
        Arc::new(MemoryDevice::new(size, 4096).unwrap())
    }

    #[test]
    fn split_produces_k_aligned_disjoint_regions() {
        let dev = mem(8 * 4096);
        let subs = split_device(dev, 4).unwrap();
        assert_eq!(subs.len(), 4);
        for s in &subs {
            assert_eq!(s.size(), 2 * 4096); // 8 blocks / 4 = 2 blocks each
            assert_eq!(s.base % 4096, 0);
        }
        // Disjoint, contiguous bases.
        assert_eq!(subs[0].base, 0);
        assert_eq!(subs[1].base, 2 * 4096);
        assert_eq!(subs[2].base, 4 * 4096);
        assert_eq!(subs[3].base, 6 * 4096);
    }

    #[test]
    fn split_rounds_region_down_to_alignment() {
        // 10 blocks / 4 = 2.5 -> 2 blocks per region; last 2 blocks unused.
        let dev = mem(10 * 4096);
        let subs = split_device(dev, 4).unwrap();
        for s in &subs {
            assert_eq!(s.size(), 2 * 4096);
        }
        assert_eq!(subs[3].base + subs[3].size(), 8 * 4096);
    }

    #[test]
    fn split_rejects_device_too_small() {
        let dev = mem(2 * 4096);
        assert!(matches!(split_device(dev, 4), Err(DeviceError::ZeroSize)));
    }

    #[test]
    fn write_lands_in_own_region_only() {
        let dev = mem(8 * 4096);
        let subs = split_device(dev.clone(), 4).unwrap();
        let mut wbuf = crate::device::AlignedBuf::new(4096, 4096);
        wbuf[..4].copy_from_slice(&[1, 2, 3, 4]);
        subs[2].pwrite(&wbuf, 0).unwrap();

        // Read it back through the same sub-device.
        let mut rbuf = crate::device::AlignedBuf::new(4096, 4096);
        subs[2].pread(&mut rbuf, 0).unwrap();
        assert_eq!(&rbuf[..4], &[1, 2, 3, 4]);

        // Other sub-devices see zeros at the same relative offset.
        for i in [0usize, 1, 3] {
            let mut other = crate::device::AlignedBuf::new(4096, 4096);
            subs[i].pread(&mut other, 0).unwrap();
            assert_eq!(&other[..4], &[0, 0, 0, 0], "sub {i} leaked sub 2's write");
        }

        // And the physical address is base_2 + 0.
        let mut phys = crate::device::AlignedBuf::new(4096, 4096);
        dev.pread(&mut phys, 4 * 4096).unwrap();
        assert_eq!(&phys[..4], &[1, 2, 3, 4]);
    }

    #[test]
    fn out_of_bounds_read_write_rejected() {
        let dev = mem(8 * 4096);
        let subs = split_device(dev, 4).unwrap();
        let mut buf = crate::device::AlignedBuf::new(4096, 4096);
        // Region is 2 blocks; offset at the last block is fine.
        assert!(subs[0].pread(&mut buf, 4096).is_ok());
        // One block past the end is out of bounds.
        assert!(matches!(
            subs[0].pread(&mut buf, 2 * 4096),
            Err(DeviceError::OutOfBounds { .. })
        ));
        assert!(matches!(
            subs[0].pwrite(&buf, 2 * 4096),
            Err(DeviceError::OutOfBounds { .. })
        ));
    }

    #[test]
    fn raw_ptr_is_offset_for_memory_backed() {
        let dev = mem(8 * 4096);
        let base_ptr = dev.as_raw_ptr().unwrap();
        let subs = split_device(dev, 4).unwrap();
        let p2 = subs[2].as_raw_ptr().unwrap();
        assert_eq!(p2 as usize, base_ptr as usize + 4 * 4096);
    }

    /// FU#8: every sub-device carved from ONE physical device reports the SAME
    /// physical id (delegated through the shared barrier), while sub-devices of
    /// a DIFFERENT physical device differ — so a `device_split` store's data +
    /// redo sub-devices classify as co-located.
    #[test]
    fn subdevices_of_one_physical_share_id() {
        use crate::device::BlockDevice;
        let phys = mem(8 * 4096);
        let phys_id = phys.physical_device_id();
        assert!(phys_id.is_some(), "backing MemoryDevice has an identity");
        let subs = split_device(phys, 4).unwrap();
        for s in &subs {
            assert_eq!(
                s.physical_device_id(),
                phys_id,
                "a sub-device inherits its physical device's identity"
            );
        }
        // A sub-device of an unrelated physical device does NOT match.
        let other_subs = split_device(mem(8 * 4096), 4).unwrap();
        assert_ne!(other_subs[0].physical_device_id(), phys_id);
    }

    #[test]
    fn new_rejects_misaligned_and_oversized() {
        let barrier = PhysicalBarrier::new(mem(8 * 4096));
        assert!(matches!(
            SubDevice::new(barrier.clone(), 100, 4096),
            Err(DeviceError::AlignmentViolation { .. })
        ));
        assert!(matches!(
            SubDevice::new(barrier.clone(), 0, 100),
            Err(DeviceError::AlignmentViolation { .. })
        ));
        assert!(matches!(
            SubDevice::new(barrier.clone(), 4 * 4096, 8 * 4096),
            Err(DeviceError::OutOfBounds { .. })
        ));
        assert!(matches!(
            SubDevice::new(barrier, 0, 0),
            Err(DeviceError::ZeroSize)
        ));
    }

    // A device wrapper that counts sync() calls (and sleeps briefly inside
    // sync to widen the coalescing window) so we can prove barrier coalescing.
    struct CountingSync {
        inner: Arc<dyn BlockDevice>,
        syncs: AtomicU64,
        fail: bool,
    }
    impl BlockDevice for CountingSync {
        fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            self.inner.pread(buf, offset)
        }
        fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
            self.inner.pwrite(buf, offset)
        }
        fn alignment(&self) -> usize {
            self.inner.alignment()
        }
        fn size(&self) -> u64 {
            self.inner.size()
        }
        fn sync(&self) -> Result<()> {
            self.syncs.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(5));
            if self.fail {
                Err(DeviceError::WriteStalled {
                    offset: 0,
                    remaining: 0,
                })
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn validate_store_count_bounds() {
        assert_eq!(validate_store_count(0), Err(StoreCountError::Zero));
        assert_eq!(validate_store_count(1), Ok(()));
        assert_eq!(validate_store_count(MAX_STORES), Ok(()));
        assert_eq!(
            validate_store_count(MAX_STORES + 1),
            Err(StoreCountError::TooMany {
                count: MAX_STORES + 1
            })
        );
    }

    /// Build a txid whose last 8 bytes encode `tail` (little-endian); the rest
    /// is `lead` so we can prove the first bytes are IGNORED by txid placement.
    fn txid_with_tail(lead: u8, tail: u64) -> [u8; 32] {
        let mut t = [lead; 32];
        t[24..32].copy_from_slice(&tail.to_le_bytes());
        t
    }

    #[test]
    fn round_robin_is_the_default_strategy() {
        let p = StorePlacer::round_robin(3);
        assert_eq!(p.strategy(), PlacementStrategy::RoundRobin);
        assert_eq!(PlacementStrategy::default(), PlacementStrategy::RoundRobin);
    }

    #[test]
    fn round_robin_cycles_and_stays_in_range() {
        let p = StorePlacer::round_robin(3);
        let zero = [0u8; 32];
        // Round-robin ignores the txid: identical txid still rotates.
        let picks: Vec<usize> = (0..7).map(|_| p.place(&zero)).collect();
        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2, 0]);
        assert!(picks.iter().all(|&s| s < 3));
    }

    #[test]
    fn round_robin_single_store_always_zero() {
        let p = StorePlacer::round_robin(1);
        let zero = [0u8; 32];
        for _ in 0..10 {
            assert_eq!(p.place(&zero), 0);
        }
    }

    #[test]
    fn txid_placement_is_deterministic_for_the_same_txid() {
        let p = StorePlacer::new(PlacementStrategy::Txid, 4);
        let txid = txid_with_tail(0xAB, 0x0102_0304_0506_0707);
        let first = p.place(&txid);
        // Same txid → same store across many calls (no hidden counter state).
        for _ in 0..100 {
            assert_eq!(p.place(&txid), first);
        }
        // It is exactly last8(txid) LE % num_stores.
        assert_eq!(first, (0x0102_0304_0506_0707u64 % 4) as usize);
    }

    #[test]
    fn txid_placement_uses_last_bytes_not_first() {
        let p = StorePlacer::new(PlacementStrategy::Txid, 7);
        // Same trailing 8 bytes, different leading bytes → same store.
        let a = txid_with_tail(0x00, 12345);
        let b = txid_with_tail(0xFF, 12345);
        assert_eq!(p.place(&a), p.place(&b));
        assert_eq!(p.place(&a), (12345u64 % 7) as usize);
        // Different trailing bytes generally route differently.
        let c = txid_with_tail(0x00, 12346);
        assert_ne!(p.place(&a), p.place(&c));
    }

    #[test]
    fn txid_placement_single_store_always_zero() {
        let p = StorePlacer::new(PlacementStrategy::Txid, 1);
        for tail in 0..50u64 {
            assert_eq!(p.place(&txid_with_tail(1, tail)), 0);
        }
    }

    #[test]
    fn txid_placement_distributes_random_txids_roughly_uniformly() {
        const NUM_STORES: usize = 8;
        const SAMPLES: usize = 80_000;
        let p = StorePlacer::new(PlacementStrategy::Txid, NUM_STORES);
        let mut counts = [0usize; NUM_STORES];
        // Deterministic PRNG (splitmix64) standing in for random double-SHA256
        // txids — no external dependency, reproducible.
        let mut state: u64 = 0x1234_5678_9abc_def0;
        for _ in 0..SAMPLES {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let mut txid = [0u8; 32];
            txid[24..32].copy_from_slice(&z.to_le_bytes());
            counts[p.place(&txid)] += 1;
        }
        // Every store gets a reasonable share (expected ~12.5%); allow ±20%.
        let expected = SAMPLES / NUM_STORES;
        for (store, &c) in counts.iter().enumerate() {
            assert!(
                c > expected * 4 / 5 && c < expected * 6 / 5,
                "store {store} got {c} of {SAMPLES} (expected ~{expected}); skewed"
            );
        }
    }

    #[test]
    fn concurrent_syncs_coalesce_into_fewer_underlying_syncs() {
        let counting = Arc::new(CountingSync {
            inner: mem(8 * 4096),
            syncs: AtomicU64::new(0),
            fail: false,
        });
        let barrier = PhysicalBarrier::new(counting.clone());
        let n = 32;
        std::thread::scope(|scope| {
            for _ in 0..n {
                let b = barrier.clone();
                scope.spawn(move || {
                    b.barrier().unwrap();
                });
            }
        });
        let count = counting.syncs.load(Ordering::SeqCst);
        // All 32 callers got durability, but far fewer than 32 underlying
        // syncs ran — they coalesced. (Conservative bound: well under n.)
        assert!(count >= 1, "at least one underlying sync must run");
        assert!(
            count < n,
            "expected coalescing: {count} underlying syncs for {n} callers"
        );
    }

    #[test]
    fn barrier_propagates_sync_failure_to_all_callers() {
        let counting = Arc::new(CountingSync {
            inner: mem(8 * 4096),
            syncs: AtomicU64::new(0),
            fail: true,
        });
        let barrier = PhysicalBarrier::new(counting);
        // Single caller: leader gets the precise error variant.
        let err = barrier.barrier().unwrap_err();
        assert!(matches!(err, DeviceError::WriteStalled { .. }));

        // Concurrent callers: every one observes a failure (leader precise,
        // followers Io-wrapped) — none silently sees success.
        let counting2 = Arc::new(CountingSync {
            inner: mem(8 * 4096),
            syncs: AtomicU64::new(0),
            fail: true,
        });
        let barrier2 = PhysicalBarrier::new(counting2);
        let results: Vec<bool> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let b = barrier2.clone();
                    scope.spawn(move || b.barrier().is_err())
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert!(
            results.iter().all(|&is_err| is_err),
            "a caller saw success on a failing device"
        );
    }

    // REGRESSION (P0-10): a follower whose covering sync generation FAILED must
    // observe failure even after a LATER successful sync has cleared `last_err`.
    // The masking window is a lock-starved follower that reads the barrier state
    // only after a subsequent generation succeeded; a live-thread repro is
    // impractical (parking_lot's eventual fairness bounds starvation), but the
    // durability hole is real, so we pin the invariant at the exact decision
    // point the barrier uses. `follower_outcome` takes `earliest_covering` — the
    // earliest generation whose sync could have covered (and on failure dropped)
    // the follower's write.
    //
    // The buggy logic reported purely from `last_err`, so the masked state
    // (`last_err == None`, `last_failed_epoch == 2`) returned Ok — silently
    // acking durability for a write that was never flushed. The fix consults
    // `last_failed_epoch`, so a failed covering generation can never be masked.
    #[test]
    fn follower_outcome_reports_a_failed_covering_generation_even_after_a_later_success() {
        // State after: gen 2 FAILED, then gen 3 SUCCEEDED (cleared last_err).
        let masked = BarrierState {
            epoch: 3,
            leader_busy: false,
            last_err: None,
            last_failed_epoch: 2,
        };
        // A follower whose earliest-covering generation is 2 (or earlier, ≤ 2)
        // coalesced onto the failed sync — it MUST see failure despite
        // last_err == None.
        assert!(
            masked.follower_outcome(2).is_err(),
            "earliest_covering=2 follower must observe the gen-2 failure (masked by gen-3 success)"
        );
        assert!(
            masked.follower_outcome(1).is_err(),
            "earliest_covering=1 follower is also covered by the ≤2 failure"
        );
        // A follower whose earliest-covering generation is 3+ depends only on the
        // successful generations — it is durable → Ok (post-heal).
        assert!(
            masked.follower_outcome(3).is_ok(),
            "earliest_covering=3 follower's covering sync succeeded"
        );
        assert!(
            masked.follower_outcome(4).is_ok(),
            "post-heal follower is durable"
        );
    }

    // REGRESSION (P0-10 follow-up): a follower that coalesced BEHIND an in-flight
    // sync waits for `target = epoch+2` but its earliest-covering generation is
    // the in-flight one, `epoch+1`. If that in-flight sync FAILS (dropping the
    // follower's pages under fsyncgate) and `target` then SUCCEEDS, reporting
    // from `target` (last_failed_epoch >= target) would return Ok — masking the
    // failure and letting a co-located sub-device ack un-flushed data. Reporting
    // from `earliest_covering` (= target-1) catches it.
    #[test]
    fn follower_outcome_catches_in_flight_sync_failure_masked_by_later_success() {
        // Leader_busy follower arrived at epoch 0 → target 2, earliest_covering 1.
        // gen 1 (the in-flight sync it coalesced behind) FAILED; gen 2 SUCCEEDED.
        let masked = BarrierState {
            epoch: 2,
            leader_busy: false,
            last_err: None, // gen-2 success cleared it
            last_failed_epoch: 1,
        };
        // Reporting from `target` (2) would wrongly say Ok (1 >= 2 is false);
        // reporting from `earliest_covering` (1) correctly says Err.
        assert!(
            masked.follower_outcome(1).is_err(),
            "in-flight sync (gen 1) failure must surface even though the follower waited for gen 2"
        );
    }

    // Conservative reporting: a follower is failed by ANY generation at or after
    // its earliest-covering generation. A strictly-later failure still trips it —
    // the safe direction, and it only fires while the device is actively failing
    // syncs. A clean device (no failures) reports Ok for every follower.
    #[test]
    fn follower_outcome_is_conservative_at_or_after_earliest_covering() {
        let failing = BarrierState {
            epoch: 5,
            leader_busy: false,
            last_err: Some("gen-5 stalled".to_string()),
            last_failed_epoch: 5,
        };
        assert!(
            failing.follower_outcome(5).is_err(),
            "earliest_covering=5 follower doomed by gen-5"
        );
        assert!(
            failing.follower_outcome(3).is_err(),
            "earliest_covering=3 follower conservatively failed while device is failing (never masks)"
        );

        // A device that has never failed a sync reports Ok for every follower.
        let clean = BarrierState {
            epoch: 9,
            leader_busy: false,
            last_err: None,
            last_failed_epoch: 0,
        };
        assert!(clean.follower_outcome(1).is_ok());
        assert!(clean.follower_outcome(9).is_ok());
    }
}
