//! Optional in-RAM block cache layered over the data device.
//!
//! `O_DIRECT` deliberately bypasses the OS page cache, so every read-modify-write
//! op (`spend`, `set_mined`) re-reads its record from the device. On low-latency
//! NVMe that is cheap; on slower disks it dominates batch latency. [`CachingDevice`]
//! is an optional [`BlockDevice`] wrapper that caches aligned device blocks in
//! RAM, configurable down to zero (zero = the device is never wrapped, i.e.
//! today's exact behavior).
//!
//! See `docs/WRITE_CACHE_SPEC.md` for the full design. Two modes:
//!
//! * **write-through** (`writeback = false`): every `pwrite` reaches the inner
//!   device immediately AND populates the cache; reads are served from RAM on a
//!   hit. Pure read acceleration — durability is byte-for-byte identical to the
//!   raw device.
//! * **write-back** (`writeback = true`): a `pwrite` updates only the cached
//!   block and marks it dirty; [`CachingDevice::sync`] flushes dirty blocks to
//!   the inner device before its `inner.sync()`. This is safe under TeraSlab's
//!   WAL-first contract because the checkpoint issues its data-device sync
//!   barrier via `BlockDevice::sync` (`recovery.rs` durability contract), so
//!   dirty blocks are flushed before any redo entry that could replay them is
//!   compacted; a dirty block lost on crash is replayed from the fsynced redo.
//!
//! The cache is keyed by physical block offset, so it is record-format agnostic
//! and needs no allocator-free invalidation: a freed-then-reused offset is
//! simply overwritten by the next `Create`'s `pwrite`.

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::device::{AlignedBuf, BlockDevice, Result};

/// Shutdown coordination for the background writeback thread: a flag plus a
/// condition variable so [`CachingDevice::stop`] can wake the thread out of its
/// inter-tick wait *immediately*, instead of blocking the join until the next
/// tick. The thread waits on the condvar with the configured interval as a
/// timeout (the tick cadence); `stop()` sets the flag and notifies so the wait
/// returns at once and the thread exits promptly regardless of how long the
/// interval is.
struct WritebackShutdown {
    stop: Mutex<bool>,
    cv: Condvar,
}

impl WritebackShutdown {
    fn new() -> Self {
        Self {
            stop: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    /// Request shutdown and wake the writeback thread immediately.
    fn signal(&self) {
        *self.stop.lock() = true;
        self.cv.notify_all();
    }

    /// Wait up to `interval` for the next tick, returning early if shutdown was
    /// requested. Returns `true` if the thread should stop.
    fn wait_tick(&self, interval: Duration) -> bool {
        let mut stop = self.stop.lock();
        if *stop {
            return true;
        }
        // `wait_for` may wake spuriously; re-check the flag after waking.
        self.cv.wait_for(&mut stop, interval);
        *stop
    }
}

/// A single cached device block.
struct Block {
    /// Block contents. Length is the block size (or the clamped tail length for
    /// a final block on a device whose size is not a block multiple).
    data: Box<[u8]>,
    /// `true` in write-back mode when the block holds writes not yet flushed to
    /// the inner device.
    dirty: bool,
    /// Monotonic per-shard tick of last access, for LRU eviction.
    last_used: u64,
}

/// One lock-striped partition of the cache.
struct Shard {
    blocks: std::collections::HashMap<u64, Block>,
    tick: u64,
    /// Max blocks this shard may hold (>= 1).
    cap: usize,
}

impl Shard {
    fn bump(&mut self) -> u64 {
        self.tick = self.tick.wrapping_add(1);
        self.tick
    }
}

/// Shared cache state: the inner device, the lock-striped shards, and the flush
/// helpers. Held behind an [`Arc`] so the background writeback thread can share
/// it with the [`CachingDevice`] without copying.
struct CacheState {
    inner: Arc<dyn BlockDevice>,
    block_size: usize,
    writeback: bool,
    shards: Box<[Mutex<Shard>]>,
    shard_count: u64,
}

/// In-RAM block cache over an inner [`BlockDevice`].
///
/// Construct with [`CachingDevice::new`]; a `bytes` budget of 0 is rejected —
/// callers should simply not wrap the device when caching is disabled.
///
/// In write-back mode the device owns a background writeback thread (see
/// [`CachingDevice::new`]) that continuously drains dirty blocks to the inner
/// device so the dirty footprint stays bounded and the checkpoint's `sync()`
/// barrier stays cheap. The thread is a pure performance optimization: it only
/// flushes dirty blocks *earlier* than `sync()` would, never changing what
/// `sync()`/recovery guarantee. It is joined on [`CachingDevice::stop`] and on
/// drop, so it is never leaked.
pub struct CachingDevice {
    state: Arc<CacheState>,
    /// Shutdown coordination for the background writeback thread (flag +
    /// condvar). `None` in write-through mode (no thread is ever spawned).
    writeback_shutdown: Option<Arc<WritebackShutdown>>,
    /// Join handle for the background writeback thread, taken on `stop()`/drop.
    writeback_handle: Mutex<Option<JoinHandle<()>>>,
}

impl CachingDevice {
    /// Wrap `inner` with a block cache of at most `bytes` RAM (rounded down to a
    /// whole number of blocks, minimum one block per shard).
    ///
    /// In write-back mode (`writeback == true`) this also spawns a background
    /// writeback thread that flushes dirty blocks toward the inner device every
    /// `writeback_interval_ms` milliseconds, keeping the dirty footprint
    /// bounded. In write-through mode no thread is spawned and behavior is
    /// byte-for-byte identical to wrapping with no background activity.
    ///
    /// `writeback_interval_ms` is clamped to a minimum of 1 ms.
    ///
    /// # Panics
    ///
    /// Panics if `bytes == 0`; a zero budget means "no cache", which the caller
    /// expresses by not wrapping the device at all.
    pub fn new(
        inner: Arc<dyn BlockDevice>,
        bytes: usize,
        writeback: bool,
        writeback_interval_ms: u64,
    ) -> Self {
        assert!(bytes > 0, "CachingDevice requires a non-zero byte budget");
        let block_size = inner.alignment().max(1);
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let shard_count = (cores * 2).clamp(1, 64) as u64;
        let total_blocks = (bytes / block_size).max(1);
        let per_shard_cap = (total_blocks / shard_count as usize).max(1);
        let shards = (0..shard_count)
            .map(|_| {
                Mutex::new(Shard {
                    blocks: std::collections::HashMap::new(),
                    tick: 0,
                    cap: per_shard_cap,
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let state = Arc::new(CacheState {
            inner,
            block_size,
            writeback,
            shards,
            shard_count,
        });

        // Write-back: spawn the background writeback thread. Write-through never
        // dirties a block, so a drain thread would have nothing to do — skip it
        // entirely so that mode is completely unaffected (no thread, no flag).
        let (writeback_shutdown, writeback_handle) = if writeback {
            let shutdown = Arc::new(WritebackShutdown::new());
            let interval = Duration::from_millis(writeback_interval_ms.max(1));
            let thread_state = state.clone();
            let thread_shutdown = shutdown.clone();
            match std::thread::Builder::new()
                .name("cache-writeback".to_string())
                .spawn(move || {
                    // Wait one interval (interruptible by `stop()`), then drain.
                    // Waiting first means a quickly-dropped cache exits without a
                    // pointless flush. The condvar wait returns immediately on
                    // shutdown, so a long interval never delays the join.
                    while !thread_shutdown.wait_tick(interval) {
                        // Drain the current dirty snapshot toward the device.
                        // Errors are logged, not fatal: durability is owned by
                        // the WAL + the checkpoint `sync()` barrier, so a failed
                        // background flush just leaves the block dirty for the
                        // next tick / for `sync()` to retry and surface.
                        if let Err(e) = thread_state.flush_all_dirty() {
                            tracing::error!(err = %e, "background cache writeback flush failed");
                        }
                    }
                }) {
                Ok(handle) => (Some(shutdown), Mutex::new(Some(handle))),
                Err(e) => {
                    // Spawn failure is non-fatal and durability-neutral: without
                    // the drain thread the dirty set is bounded only by eviction
                    // and `sync()` still flushes everything, so correctness is
                    // unchanged — only the proactive bounding is lost.
                    tracing::error!(
                        err = %e,
                        "failed to spawn cache-writeback thread; falling back to \
                         sync/eviction-only flushing"
                    );
                    (None, Mutex::new(None))
                }
            }
        } else {
            (None, Mutex::new(None))
        };

        Self {
            state,
            writeback_shutdown,
            writeback_handle,
        }
    }

    /// Signal the background writeback thread to stop and join it. Idempotent:
    /// safe to call multiple times (subsequent calls are no-ops). A no-op in
    /// write-through mode, where no thread was spawned.
    ///
    /// This does NOT flush dirty blocks — durability is the caller's `sync()`
    /// barrier's job. Callers that want a clean, fully-flushed stop should call
    /// [`BlockDevice::sync`] before or after `stop()` as the shutdown path does.
    pub fn stop(&self) {
        if let Some(shutdown) = &self.writeback_shutdown {
            // Set the flag AND wake the thread out of its inter-tick wait so the
            // join returns at once, even with a long configured interval.
            shutdown.signal();
        }
        if let Some(handle) = self.writeback_handle.lock().take() {
            // The thread is either waiting on the (now-signalled) condvar or
            // briefly holding a shard lock for a flush; it never holds a lock
            // across device I/O, so the join completes promptly.
            if handle.join().is_err() {
                tracing::error!("cache-writeback thread panicked during join");
            }
        }
    }
}

impl Drop for CachingDevice {
    fn drop(&mut self) {
        // Robust lifecycle: never leak the thread even if `stop()` was not
        // called explicitly. `stop()` is idempotent.
        self.stop();
    }
}

impl CacheState {
    fn shard_of(&self, block_idx: u64) -> &Mutex<Shard> {
        &self.shards[(block_idx % self.shard_count) as usize]
    }

    /// Byte length of the block starting at `block_start` (clamped at EOF for a
    /// device whose size is not a block multiple).
    fn block_len(&self, block_start: u64) -> usize {
        let remaining = self.inner.size().saturating_sub(block_start);
        (self.block_size as u64).min(remaining) as usize
    }

    /// Read block `block_idx` from the inner device into an owned buffer, using
    /// an aligned bounce buffer so the inner `O_DIRECT` read is legal.
    fn load_from_inner(&self, block_idx: u64) -> Result<Box<[u8]>> {
        let block_start = block_idx * self.block_size as u64;
        let len = self.block_len(block_start);
        let mut buf = AlignedBuf::new(len, self.block_size);
        self.inner.pread_exact_at(&mut buf[..len], block_start)?;
        Ok(buf[..len].to_vec().into_boxed_slice())
    }

    /// Evict the least-recently-used block from a full shard, flushing it first
    /// if it is dirty (write-back). Caller holds the shard lock.
    fn evict_if_full(&self, shard: &mut Shard) -> Result<()> {
        while shard.blocks.len() >= shard.cap {
            let victim = shard
                .blocks
                .iter()
                .min_by_key(|(_, b)| b.last_used)
                .map(|(idx, _)| *idx);
            let Some(idx) = victim else { break };
            // Remove first; if the flush fails, re-insert so we don't lose the
            // dirty bytes (the caller surfaces the error).
            let block = shard.blocks.remove(&idx).expect("victim present");
            if block.dirty
                && let Err(e) = self.flush_block(idx, &block.data)
            {
                shard.blocks.insert(idx, block);
                return Err(e);
            }
        }
        Ok(())
    }

    /// Write one block's bytes back to the inner device (aligned bounce buffer).
    fn flush_block(&self, block_idx: u64, data: &[u8]) -> Result<()> {
        let block_start = block_idx * self.block_size as u64;
        let mut buf = AlignedBuf::new(data.len(), self.block_size);
        buf[..data.len()].copy_from_slice(data);
        self.inner.pwrite_all_at(&buf[..data.len()], block_start)
    }

    /// Copy the intersection of the requested byte range with a single block.
    fn block_span(&self, block_idx: u64, offset: u64, len: usize) -> (usize, usize, usize) {
        let block_start = block_idx * self.block_size as u64;
        let req_start = offset.max(block_start);
        let req_end = (offset + len as u64).min(block_start + self.block_size as u64);
        let in_block = (req_start - block_start) as usize;
        let in_buf = (req_start - offset) as usize;
        let n = (req_end - req_start) as usize;
        (in_block, in_buf, n)
    }
}

impl CacheState {
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let bs = self.block_size as u64;
        let first = offset / bs;
        let last = (offset + buf.len() as u64 - 1) / bs;
        for block_idx in first..=last {
            let (in_block, in_buf, n) = self.block_span(block_idx, offset, buf.len());
            // Fast path: hit.
            {
                let mut shard = self.shard_of(block_idx).lock();
                let t = shard.bump();
                if let Some(b) = shard.blocks.get_mut(&block_idx) {
                    b.last_used = t;
                    buf[in_buf..in_buf + n].copy_from_slice(&b.data[in_block..in_block + n]);
                    continue;
                }
            }
            // Miss: load outside the lock so a slow inner read does not serialize
            // the shard, then insert (keeping any block that appeared meanwhile —
            // it may be a fresh dirty write we must not clobber).
            let data = self.load_from_inner(block_idx)?;
            let mut shard = self.shard_of(block_idx).lock();
            if !shard.blocks.contains_key(&block_idx) {
                self.evict_if_full(&mut shard)?;
                let t = shard.bump();
                shard.blocks.insert(
                    block_idx,
                    Block {
                        data,
                        dirty: false,
                        last_used: t,
                    },
                );
            }
            let t = shard.bump();
            let b = shard.blocks.get_mut(&block_idx).expect("just inserted");
            b.last_used = t;
            buf[in_buf..in_buf + n].copy_from_slice(&b.data[in_block..in_block + n]);
        }
        Ok(buf.len())
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // Write-through: the inner device gets the exact bytes immediately, so
        // durability is unchanged. (The engine always passes aligned buf/offset
        // on the O_DIRECT path.)
        if !self.writeback {
            self.inner.pwrite_all_at(buf, offset)?;
        }
        let bs = self.block_size as u64;
        let first = offset / bs;
        let last = (offset + buf.len() as u64 - 1) / bs;
        for block_idx in first..=last {
            let (in_block, in_buf, n) = self.block_span(block_idx, offset, buf.len());
            let whole_block = in_block == 0 && n == self.block_len(block_idx * bs);
            // For a partial-block write-back we must preserve untouched bytes, so
            // load the block first (outside the lock) if it is not resident.
            let preload = if self.writeback && !whole_block {
                let resident = self
                    .shard_of(block_idx)
                    .lock()
                    .blocks
                    .contains_key(&block_idx);
                if resident {
                    None
                } else {
                    Some(self.load_from_inner(block_idx)?)
                }
            } else {
                None
            };
            let mut shard = self.shard_of(block_idx).lock();
            let t = shard.bump();
            if let Some(b) = shard.blocks.get_mut(&block_idx) {
                b.data[in_block..in_block + n].copy_from_slice(&buf[in_buf..in_buf + n]);
                b.last_used = t;
                if self.writeback {
                    b.dirty = true;
                }
            } else {
                self.evict_if_full(&mut shard)?;
                let mut data = match preload {
                    Some(d) => d,
                    None => vec![0u8; self.block_len(block_idx * bs)].into_boxed_slice(),
                };
                data[in_block..in_block + n].copy_from_slice(&buf[in_buf..in_buf + n]);
                let t = shard.bump();
                shard.blocks.insert(
                    block_idx,
                    Block {
                        data,
                        dirty: self.writeback,
                        last_used: t,
                    },
                );
            }
        }
        Ok(buf.len())
    }

    fn sync(&self) -> Result<()> {
        self.flush_all_dirty()?;
        self.inner.sync()
    }

    fn sync_data(&self) -> Result<()> {
        self.flush_all_dirty()?;
        self.inner.sync_data()
    }

    /// Flush every dirty block to the inner device (write-back). A no-op in
    /// write-through mode (no block is ever dirty).
    ///
    /// Locking discipline (identical to the eviction path so it can never lose
    /// or clobber a concurrent write): collect the dirty `(idx, bytes)` snapshot
    /// under each shard lock, perform the device `pwrite` OUTSIDE the lock, then
    /// re-acquire the lock and clear the dirty flag ONLY IF the cached bytes are
    /// unchanged since the snapshot. A block re-written concurrently keeps its
    /// dirty flag set and is flushed again on the next tick / `sync()`. The
    /// shard lock is never held across a device `pwrite`, so this can never
    /// deadlock against `pread`/`pwrite`/`sync`/eviction.
    fn flush_all_dirty(&self) -> Result<()> {
        if !self.writeback {
            return Ok(());
        }
        for shard in self.shards.iter() {
            // Collect dirty (idx, bytes) under the lock, flush outside it, then
            // clear the dirty flag if the bytes are unchanged.
            let dirty: Vec<(u64, Box<[u8]>)> = {
                let shard = shard.lock();
                shard
                    .blocks
                    .iter()
                    .filter(|(_, b)| b.dirty)
                    .map(|(idx, b)| (*idx, b.data.clone()))
                    .collect()
            };
            for (idx, data) in dirty {
                self.flush_block(idx, &data)?;
                let mut shard = shard.lock();
                if let Some(b) = shard.blocks.get_mut(&idx) {
                    // Only clear if untouched since the snapshot (length is a
                    // cheap, sufficient proxy: blocks never change length).
                    if b.data.as_ref() == data.as_ref() {
                        b.dirty = false;
                    }
                }
            }
        }
        Ok(())
    }
}

impl BlockDevice for CachingDevice {
    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.state.pread(buf, offset)
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
        self.state.pwrite(buf, offset)
    }

    fn alignment(&self) -> usize {
        self.state.inner.alignment()
    }

    fn size(&self) -> u64 {
        self.state.inner.size()
    }

    fn is_block_device(&self) -> bool {
        self.state.inner.is_block_device()
    }

    fn sync(&self) -> Result<()> {
        self.state.sync()
    }

    fn sync_data(&self) -> Result<()> {
        self.state.sync_data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemoryDevice;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Inner device that counts pread/pwrite/sync calls so tests can assert the
    /// cache actually elides inner I/O.
    struct CountingDev {
        inner: MemoryDevice,
        reads: AtomicUsize,
        writes: AtomicUsize,
        syncs: AtomicUsize,
    }

    impl CountingDev {
        fn new(size: usize, align: usize) -> Arc<Self> {
            Arc::new(Self {
                inner: MemoryDevice::new(size as u64, align).unwrap(),
                reads: AtomicUsize::new(0),
                writes: AtomicUsize::new(0),
                syncs: AtomicUsize::new(0),
            })
        }
    }

    impl BlockDevice for CountingDev {
        fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.pread(buf, offset)
        }
        fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.inner.pwrite(buf, offset)
        }
        fn alignment(&self) -> usize {
            self.inner.alignment()
        }
        fn size(&self) -> u64 {
            self.inner.size()
        }
        fn sync(&self) -> Result<()> {
            self.syncs.fetch_add(1, Ordering::Relaxed);
            self.inner.sync()
        }
        // as_raw_ptr stays None (trait default) so the cache is exercised via
        // pread/pwrite rather than the mmap fast path.
    }

    const BS: usize = 4096;

    /// Writeback interval used by tests that want the background thread
    /// effectively disabled (so they can assert sync-only / pre-sync behavior
    /// deterministically). One hour: the thread sleeps this long and never fires
    /// during a test, but is still spawned and joined on drop.
    const NEVER_MS: u64 = 3_600_000;

    /// Fast writeback interval for tests that exercise the background drain.
    const FAST_MS: u64 = 5;

    /// Aligned, byte-filled block buffer (MemoryDevice/O_DIRECT require the
    /// buffer address itself to be alignment-aligned).
    fn ab(byte: u8, len: usize) -> AlignedBuf {
        let mut b = AlignedBuf::new(len, BS);
        b[..].fill(byte);
        b
    }

    /// Read `len` bytes directly off the inner device (bypassing the cache),
    /// using an aligned buffer.
    fn read_inner(dev: &CountingDev, off: u64, len: usize) -> Vec<u8> {
        let mut b = AlignedBuf::new(len, BS);
        dev.inner.pread(&mut b[..], off).unwrap();
        b[..].to_vec()
    }

    /// Read `len` bytes through the cache into an aligned buffer.
    fn read_cache(cache: &CachingDevice, off: u64, len: usize) -> Vec<u8> {
        let mut b = AlignedBuf::new(len, BS);
        cache.pread(&mut b[..], off).unwrap();
        b[..].to_vec()
    }

    #[test]
    fn read_through_caches_and_elides_second_inner_read() {
        let dev = CountingDev::new(64 * BS, BS);
        dev.inner.pwrite(&ab(0xAB, BS), 0).unwrap();
        let cache = CachingDevice::new(dev.clone(), 16 * BS, false, NEVER_MS);

        assert_eq!(
            read_cache(&cache, 0, BS),
            vec![0xAB; BS],
            "first read returns device bytes"
        );
        assert_eq!(
            dev.reads.load(Ordering::Relaxed),
            1,
            "first read hits inner once"
        );

        assert_eq!(
            read_cache(&cache, 0, BS),
            vec![0xAB; BS],
            "second read returns same bytes"
        );
        assert_eq!(
            dev.reads.load(Ordering::Relaxed),
            1,
            "second read served from cache, no inner read"
        );
    }

    #[test]
    fn write_through_reaches_inner_immediately() {
        let dev = CountingDev::new(64 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 16 * BS, false, NEVER_MS);

        cache.pwrite(&ab(0x11, BS), BS as u64).unwrap();
        assert!(
            dev.writes.load(Ordering::Relaxed) >= 1,
            "write-through must reach the inner device"
        );
        assert_eq!(
            read_inner(&dev, BS as u64, BS),
            vec![0x11; BS],
            "inner has the written bytes immediately"
        );

        // The cache serves the same bytes without re-reading inner.
        let reads_before = dev.reads.load(Ordering::Relaxed);
        assert_eq!(read_cache(&cache, BS as u64, BS), vec![0x11; BS]);
        assert_eq!(
            dev.reads.load(Ordering::Relaxed),
            reads_before,
            "write-through populated the cache; read needs no inner I/O"
        );
    }

    #[test]
    fn write_back_defers_inner_write_until_sync() {
        let dev = CountingDev::new(64 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 16 * BS, true, NEVER_MS);

        cache.pwrite(&ab(0x22, BS), 2 * BS as u64).unwrap();
        assert_eq!(
            dev.writes.load(Ordering::Relaxed),
            0,
            "write-back must NOT touch the inner device before sync"
        );
        assert_eq!(
            read_inner(&dev, 2 * BS as u64, BS),
            vec![0x00; BS],
            "inner unchanged before sync (a crash here loses it; the WAL replays it)"
        );
        assert_eq!(
            read_cache(&cache, 2 * BS as u64, BS),
            vec![0x22; BS],
            "cache serves the dirty write coherently"
        );

        cache.sync().unwrap();
        assert_eq!(
            read_inner(&dev, 2 * BS as u64, BS),
            vec![0x22; BS],
            "sync flushed the dirty block to inner"
        );
        assert!(dev.syncs.load(Ordering::Relaxed) >= 1, "inner sync issued");
    }

    #[test]
    fn write_back_eviction_flushes_dirty_block() {
        // Whole-cache budget of one block forces per-shard cap 1, so the second
        // write into the same shard evicts the first.
        let dev = CountingDev::new(1024 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), BS, true, NEVER_MS);

        let sc = cache.state.shard_count;
        let idx_a = 0u64;
        let idx_b = sc; // same shard as 0 (idx % sc == 0)
        cache.pwrite(&ab(0xA1, BS), idx_a * BS as u64).unwrap();
        cache.pwrite(&ab(0xB2, BS), idx_b * BS as u64).unwrap();
        assert_eq!(
            read_inner(&dev, idx_a * BS as u64, BS),
            vec![0xA1; BS],
            "evicting a dirty write-back block must flush it to the inner device"
        );
    }

    #[test]
    fn overwrite_is_coherent_on_hit() {
        let dev = CountingDev::new(64 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 16 * BS, false, NEVER_MS);
        cache.pwrite(&ab(0x01, BS), 0).unwrap();
        cache.pwrite(&ab(0x02, BS), 0).unwrap();
        assert_eq!(
            read_cache(&cache, 0, BS),
            vec![0x02; BS],
            "cache reflects the latest write"
        );
    }

    #[test]
    fn partial_block_write_back_preserves_untouched_bytes() {
        let dev = CountingDev::new(64 * BS, BS);
        dev.inner.pwrite(&ab(0x55, BS), 0).unwrap();
        let cache = CachingDevice::new(dev.clone(), 16 * BS, true, NEVER_MS);

        // Sub-block write of 8 bytes of 0x99 at offset 16 (aligned caller buffer).
        cache.pwrite(&ab(0x99, BS)[..8], 16).unwrap();
        cache.sync().unwrap();

        let got = read_inner(&dev, 0, BS);
        assert_eq!(&got[16..24], &[0x99; 8], "written bytes present");
        assert_eq!(&got[0..16], &[0x55; 16], "bytes before the write preserved");
        assert_eq!(
            &got[24..],
            &vec![0x55; BS - 24][..],
            "bytes after preserved"
        );
    }

    #[test]
    fn multi_block_read_spans_blocks() {
        let dev = CountingDev::new(64 * BS, BS);
        dev.inner.pwrite(&ab(0x07, BS), 0).unwrap();
        dev.inner.pwrite(&ab(0x08, BS), BS as u64).unwrap();
        let cache = CachingDevice::new(dev.clone(), 16 * BS, false, NEVER_MS);

        let got = read_cache(&cache, 0, 2 * BS);
        assert_eq!(&got[..BS], &vec![0x07; BS][..]);
        assert_eq!(&got[BS..], &vec![0x08; BS][..]);
    }

    /// Total number of blocks currently marked dirty across all shards.
    fn dirty_count(cache: &CachingDevice) -> usize {
        cache
            .state
            .shards
            .iter()
            .map(|s| s.lock().blocks.values().filter(|b| b.dirty).count())
            .sum()
    }

    /// Poll `cond` until it returns true or `timeout` elapses; returns whether
    /// it became true. Used instead of a fixed sleep so the background-thread
    /// tests are not flaky under load.
    fn poll_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if cond() {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn background_writeback_flushes_dirty_without_explicit_sync() {
        let dev = CountingDev::new(1024 * BS, BS);
        // Generous budget so nothing is evicted: the ONLY way bytes reach inner
        // is the background drain (no sync(), no eviction).
        let cache = CachingDevice::new(dev.clone(), 512 * BS, true, FAST_MS);

        // Write several distinct blocks across (likely) several shards.
        for i in 0..8u64 {
            cache
                .pwrite(&ab(0xC0 + i as u8, BS), i * BS as u64)
                .unwrap();
        }
        assert_eq!(
            dirty_count(&cache),
            8,
            "all writes start dirty in write-back"
        );

        // Without ever calling sync(), the background thread must drain them.
        let drained = poll_until(Duration::from_secs(5), || dirty_count(&cache) == 0);
        assert!(
            drained,
            "background writeback must clear the dirty set without an explicit sync()"
        );
        assert!(
            dev.writes.load(Ordering::Relaxed) >= 8,
            "background writeback issued the inner writes (got {})",
            dev.writes.load(Ordering::Relaxed)
        );

        // The bytes that landed on the inner device are exactly what was written.
        for i in 0..8u64 {
            assert_eq!(
                read_inner(&dev, i * BS as u64, BS),
                vec![0xC0 + i as u8; BS],
                "block {i} flushed with the correct bytes"
            );
        }
    }

    #[test]
    fn background_writeback_preserves_read_coherency() {
        let dev = CountingDev::new(1024 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 512 * BS, true, FAST_MS);

        cache.pwrite(&ab(0x10, BS), 0).unwrap();
        // Let the background thread flush block 0 at least once.
        assert!(
            poll_until(Duration::from_secs(5), || dirty_count(&cache) == 0),
            "initial write drained"
        );
        // Reading still serves the latest bytes from the (now-clean) cache.
        assert_eq!(
            read_cache(&cache, 0, BS),
            vec![0x10; BS],
            "clean cached block still serves the latest bytes"
        );

        // Re-dirty the SAME block right after it was flushed; the new bytes must
        // not be lost and the block must end up dirty again (then re-drained).
        cache.pwrite(&ab(0x20, BS), 0).unwrap();
        assert_eq!(
            read_cache(&cache, 0, BS),
            vec![0x20; BS],
            "re-write is immediately visible through the cache"
        );
        assert!(
            poll_until(Duration::from_secs(5), || {
                dirty_count(&cache) == 0 && read_inner(&dev, 0, BS) == vec![0x20; BS]
            }),
            "re-dirtied block is re-flushed with the newest bytes (not lost)"
        );
        assert_eq!(
            read_cache(&cache, 0, BS),
            vec![0x20; BS],
            "cache remains coherent after the re-drain"
        );
    }

    #[test]
    fn clean_shutdown_joins_thread_and_sync_stays_consistent() {
        let dev = CountingDev::new(1024 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 512 * BS, true, FAST_MS);

        cache.pwrite(&ab(0x33, BS), 3 * BS as u64).unwrap();
        // Stop joins the background thread without panic; idempotent.
        cache.stop();
        cache.stop();

        // A final sync() after stop must still flush remaining dirty bytes and
        // leave the inner device consistent (durability does not depend on the
        // thread having run).
        cache.sync().unwrap();
        assert_eq!(
            dirty_count(&cache),
            0,
            "sync() after stop clears the dirty set"
        );
        assert_eq!(
            read_inner(&dev, 3 * BS as u64, BS),
            vec![0x33; BS],
            "sync() after stop left the inner device consistent"
        );
        assert!(
            dev.syncs.load(Ordering::Relaxed) >= 1,
            "inner sync issued by the final sync()"
        );
        // Dropping after an explicit stop must not panic / double-join.
        drop(cache);
    }

    #[test]
    fn write_through_spawns_no_background_thread() {
        let dev = CountingDev::new(64 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 16 * BS, false, FAST_MS);

        // Write-through never spawns the thread and never marks a block dirty.
        assert!(
            cache.writeback_shutdown.is_none(),
            "write-through must not arm a shutdown flag (no thread spawned)"
        );
        assert!(
            cache.writeback_handle.lock().is_none(),
            "write-through must not hold a join handle"
        );

        cache.pwrite(&ab(0x44, BS), BS as u64).unwrap();
        assert_eq!(
            dirty_count(&cache),
            0,
            "write-through never dirties a cached block"
        );
        let writes_after = dev.writes.load(Ordering::Relaxed);
        assert!(writes_after >= 1, "write-through reached inner immediately");

        // Even after waiting longer than several FAST_MS intervals, no further
        // inner writes appear — confirming no background activity.
        assert!(
            !poll_until(Duration::from_millis(100), || {
                dev.writes.load(Ordering::Relaxed) > writes_after
            }),
            "no background thread should issue extra inner writes in write-through"
        );

        // stop() is a no-op and must not panic.
        cache.stop();
    }
}
