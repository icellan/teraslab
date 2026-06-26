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
        // Generation that must complete for our prior writes to be durable.
        let target = st.epoch + if st.leader_busy { 2 } else { 1 };
        loop {
            if st.epoch >= target {
                // A qualifying sync completed. `last_err` reflects the latest
                // completed sync: if it succeeded, everything pending (incl.
                // ours) was flushed; if it failed, report failure conservatively.
                return match &st.last_err {
                    None => Ok(()),
                    Some(msg) => Err(coalesced_barrier_error(msg)),
                };
            }
            if !st.leader_busy {
                // Become the leader for the next generation.
                st.leader_busy = true;
                drop(st);
                let res = self.inner.sync();
                let mut st2 = self.state.lock();
                st2.epoch += 1;
                st2.last_err = res.as_ref().err().map(|e| e.to_string());
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

/// Round-robin store placement for new records.
///
/// Placement at create time is a free local choice — the chosen store is
/// recorded in the index entry's `device_id`, so reads and later mutations
/// follow the index, not any function of the key. Round-robin gives an even
/// fill across equal-sized stores with a single atomic and no per-store query.
/// (A least-loaded policy is a drop-in replacement for [`Self::pick`] if churn
/// ever skews fill.)
#[derive(Debug)]
pub struct RoundRobinPlacer {
    num_stores: usize,
    next: std::sync::atomic::AtomicUsize,
}

impl RoundRobinPlacer {
    /// Create a placer over `num_stores` stores (must be >= 1).
    pub fn new(num_stores: usize) -> Self {
        debug_assert!(num_stores >= 1);
        Self {
            num_stores,
            next: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Pick the next store index in `0..num_stores`.
    #[inline]
    pub fn pick(&self) -> usize {
        if self.num_stores == 1 {
            return 0;
        }
        self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.num_stores
    }

    /// Number of stores this placer rotates over.
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

    #[test]
    fn round_robin_cycles_and_stays_in_range() {
        let p = RoundRobinPlacer::new(3);
        let picks: Vec<usize> = (0..7).map(|_| p.pick()).collect();
        assert_eq!(picks, vec![0, 1, 2, 0, 1, 2, 0]);
        assert!(picks.iter().all(|&s| s < 3));
    }

    #[test]
    fn round_robin_single_store_always_zero() {
        let p = RoundRobinPlacer::new(1);
        for _ in 0..10 {
            assert_eq!(p.pick(), 0);
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
}
