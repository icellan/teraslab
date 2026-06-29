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

/// Upper bound on the size of a single coalesced write-back flush, in bytes.
///
/// Contiguous dirty blocks are merged into one sequential `pwrite` (see
/// [`CacheState::flush_all_dirty_coalesced`]) to turn the scattered 4 KiB
/// per-block writes into large sequential I/O. This cap bounds the size of the
/// aligned bounce buffer a single merged write allocates and keeps each device
/// write to a sequential-friendly size; a contiguous run longer than this is
/// split into successive capped writes. 256 KiB is large enough to collapse
/// ~64 contiguous 4 KiB blocks into one syscall while keeping the transient
/// bounce-buffer allocation modest. Must be a multiple of every supported block
/// size (it is a power of two >= any device alignment).
const MAX_COALESCE_BYTES: usize = 256 * 1024;

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
    ///
    /// Held behind an [`Arc`] so the per-tick dirty snapshot taken under the
    /// shard lock is a cheap refcount bump instead of a multi-KiB `memcpy`. A
    /// write that mutates a block currently being flushed uses [`Arc::make_mut`]
    /// (copy-on-write): if the flusher still holds a clone of the old `Arc`, the
    /// writer transparently gets a fresh buffer, so the in-flight snapshot is
    /// never corrupted.
    data: Arc<[u8]>,
    /// `true` in write-back mode when the block holds writes not yet flushed to
    /// the inner device.
    dirty: bool,
    /// Monotonic per-shard tick of last access, for LRU eviction.
    last_used: u64,
}

/// One lock-striped partition of the cache.
struct Shard {
    blocks: std::collections::HashMap<u64, Block>,
    /// Fast worklist of dirty block indices. Invariant (held under the shard
    /// lock): this set contains EXACTLY the indices whose `Block.dirty == true`.
    /// It is a worklist, not the authority — [`Block::dirty`] is the authority —
    /// so the flush sweep re-checks `b.dirty` under the lock and uses
    /// [`Arc::ptr_eq`] to detect a concurrent re-dirty. Maintaining it lets the
    /// flush path iterate O(dirty) instead of O(all cached blocks) per tick.
    dirty: std::collections::HashSet<u64>,
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
    /// Dedicated work-stealing pool used to flush shards concurrently in
    /// [`CacheState::flush_all_dirty`]. Built once at construction (persistent —
    /// no per-tick spawn cost) and sized to `min(shard_count, cores)`. Isolated
    /// from the global rayon pool and the dispatch read-pool so writeback never
    /// contends with request-serving fan-out. `None` if the pool failed to build
    /// or only one worker is warranted, in which case shards flush serially on
    /// the calling thread (correct, just single-core for that path).
    flush_pool: Option<rayon::ThreadPool>,
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
        // Finer sharding lowers contention on the per-shard mutex, which under the
        // high-concurrency chain workload is contended between the write-back flush
        // threads (flush_block) and the serving read/write threads (pread/insert).
        // Each shard is a small HashMap+Mutex, so a higher count is cheap; profiled
        // as a remaining bottleneck after the fallocate fix.
        let shard_count = (cores * 16).clamp(1, 1024) as u64;
        let total_blocks = (bytes / block_size).max(1);
        let per_shard_cap = (total_blocks / shard_count as usize).max(1);
        let shards = (0..shard_count)
            .map(|_| {
                Mutex::new(Shard {
                    blocks: std::collections::HashMap::new(),
                    dirty: std::collections::HashSet::new(),
                    tick: 0,
                    cap: per_shard_cap,
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        // Dedicated writeback flush pool: only write-back ever flushes dirty
        // blocks, so write-through skips it entirely. Cap workers at the shard
        // count (no point in more workers than shards) and at the host's
        // parallelism. With <= 1 worker there is nothing to parallelize, so we
        // keep `None` and flush serially on the calling thread.
        let flush_pool = if writeback {
            let workers = (shard_count as usize).min(cores);
            if workers <= 1 {
                None
            } else {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(workers)
                    .thread_name(|i| format!("cache-flush-{i}"))
                    .build()
                    .map_err(|e| {
                        tracing::error!(
                            err = %e,
                            "cache writeback flush pool build failed; shards flush serially"
                        );
                    })
                    .ok()
            }
        } else {
            None
        };

        let state = Arc::new(CacheState {
            inner,
            block_size,
            writeback,
            shards,
            shard_count,
            flush_pool,
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
    fn load_from_inner(&self, block_idx: u64) -> Result<Arc<[u8]>> {
        let block_start = block_idx * self.block_size as u64;
        let len = self.block_len(block_start);
        let mut buf = AlignedBuf::new(len, self.block_size);
        self.inner.pread_exact_at(&mut buf[..len], block_start)?;
        Ok(Arc::from(&buf[..len]))
    }

    /// Evict the least-recently-used block from a full shard. Caller holds the
    /// shard lock.
    ///
    /// PREFER-CLEAN: a clean block can be dropped with no device I/O, so the LRU
    /// **clean** block is evicted first. Only when every resident block is dirty
    /// (the background writeback flusher has not yet drained any block in this
    /// shard) does eviction fall back to flushing the LRU dirty victim — a
    /// synchronous `pwrite` under the shard lock. This keeps the common eviction
    /// off the device-write path, so a read or write inserting into a full shard
    /// no longer stalls every other op on that shard behind a flush (the per-op
    /// tail source profiled in bench/results/20260629-local-profile). Eviction is
    /// a pure performance heuristic, so preferring clean over strictly-oldest is
    /// always safe.
    fn evict_if_full(&self, shard: &mut Shard) -> Result<()> {
        while shard.blocks.len() >= shard.cap {
            // Fast path: drop the LRU CLEAN block — no flush, no device I/O.
            let clean_victim = shard
                .blocks
                .iter()
                .filter(|(_, b)| !b.dirty)
                .min_by_key(|(_, b)| b.last_used)
                .map(|(idx, _)| *idx);
            if let Some(idx) = clean_victim {
                // Clean block: not in the dirty worklist, nothing to flush.
                shard.blocks.remove(&idx);
                continue;
            }
            // Every resident block is dirty — flush the LRU dirty victim under
            // the lock. Remove first; if the flush fails, re-insert so we don't
            // lose the dirty bytes (the caller surfaces the error). Keep the
            // dirty set in lockstep.
            let victim = shard
                .blocks
                .iter()
                .min_by_key(|(_, b)| b.last_used)
                .map(|(idx, _)| *idx);
            let Some(idx) = victim else { break };
            let block = shard.blocks.remove(&idx).expect("victim present");
            shard.dirty.remove(&idx);
            if let Err(e) = self.flush_block(idx, &block.data) {
                shard.dirty.insert(idx);
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
                // Copy-on-write: `make_mut` mutates in place when this block is
                // uniquely owned, but clones into a fresh buffer if a flusher is
                // still holding a snapshot `Arc` of the old bytes — so a write
                // concurrent with an in-flight flush never corrupts the snapshot.
                Arc::make_mut(&mut b.data)[in_block..in_block + n]
                    .copy_from_slice(&buf[in_buf..in_buf + n]);
                b.last_used = t;
                if self.writeback {
                    b.dirty = true;
                    // Keep the dirty worklist in lockstep with `Block::dirty`.
                    // (`b`'s borrow of `shard.blocks` ends here; `shard.dirty`
                    // is a disjoint field so this is a fresh borrow.)
                    shard.dirty.insert(block_idx);
                }
            } else {
                self.evict_if_full(&mut shard)?;
                let mut data: Vec<u8> = match preload {
                    // Partial write into a non-resident block: start from the
                    // device bytes (loaded outside the lock) so untouched bytes
                    // are preserved.
                    Some(d) => d.to_vec(),
                    None => vec![0u8; self.block_len(block_idx * bs)],
                };
                data[in_block..in_block + n].copy_from_slice(&buf[in_buf..in_buf + n]);
                let t = shard.bump();
                shard.blocks.insert(
                    block_idx,
                    Block {
                        data: Arc::from(data),
                        dirty: self.writeback,
                        last_used: t,
                    },
                );
                // Inserting a dirty block (write-back) adds it to the worklist.
                if self.writeback {
                    shard.dirty.insert(block_idx);
                }
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

    /// Flush every dirty block across all shards to the inner device (write-back),
    /// COALESCING contiguous dirty blocks into single large sequential writes.
    /// A no-op in write-through mode (no block is ever dirty).
    ///
    /// Why a global sweep, not the old per-shard flush: `shard_of(block_idx) =
    /// block_idx % shard_count`, so contiguous device blocks land in DIFFERENT
    /// shards. A per-shard flush therefore can never see two adjacent blocks
    /// together and degrades into one scattered 4 KiB `pwrite` per dirty block —
    /// the measured 104k-IOPS / 5.6 KB-avg pathology. This sweep instead gathers
    /// the dirty `(block_idx, Arc<bytes>)` snapshot from ALL shards, sorts by
    /// `block_idx`, merges adjacent indices into runs, and issues ONE `pwrite`
    /// per run (capped at [`MAX_COALESCE_BYTES`], so a long run splits into
    /// bounded sequential writes), turning scattered small writes into large
    /// sequential ones.
    ///
    /// Locking discipline (PRESERVED — same snapshot-under-lock then
    /// pwrite-outside-lock then ptr_eq-clear pattern the eviction path uses):
    /// the snapshot for each shard is taken under that shard's lock — a cheap
    /// refcount bump, NOT a `memcpy` — and the lock is released before any device
    /// I/O. Every `pwrite` happens OUTSIDE all shard locks. Dirty-bit clearing
    /// re-acquires the owning shard's lock per block and clears ONLY if the
    /// cached `Arc` is still byte-for-byte the snapshot we flushed
    /// ([`Arc::ptr_eq`]) — a concurrent write swaps the `Arc` (CoW), so a
    /// mismatch means "re-dirtied since snapshot": the block keeps its dirty flag
    /// and worklist entry for the next flush. The first run error is returned;
    /// remaining runs are still attempted so one bad block does not strand the
    /// rest dirty.
    fn flush_all_dirty(&self) -> Result<()> {
        if !self.writeback {
            return Ok(());
        }
        // 1) Snapshot dirty (idx, Arc) from every shard under its own lock, then
        //    release. Sorting all of them globally is what lets contiguous blocks
        //    living in different shards merge into one run.
        let mut snapshot: Vec<(u64, Arc<[u8]>)> = Vec::new();
        for shard in self.shards.iter() {
            let shard = shard.lock();
            snapshot.reserve(shard.dirty.len());
            for idx in shard.dirty.iter() {
                if let Some(b) = shard.blocks.get(idx).filter(|b| b.dirty) {
                    snapshot.push((*idx, b.data.clone()));
                }
            }
        }
        if snapshot.is_empty() {
            return Ok(());
        }
        // 2) Sort by block index and merge adjacent indices into contiguous runs,
        //    splitting a run once it would exceed MAX_COALESCE_BYTES. A run is
        //    contiguous iff each block immediately follows the previous one
        //    (idx == prev_idx + 1). Only the device's final block may be a short
        //    tail (< block_size); since it is the last block, it can only ever
        //    end a run, so a run is always block_size-aligned in length except
        //    possibly its tail — always legal for an aligned `pwrite`.
        snapshot.sort_unstable_by_key(|(idx, _)| *idx);
        let runs = self.merge_into_runs(snapshot);

        // 3) Write each run as one sequential pwrite (outside all locks), then
        //    clear the dirty bits per block under the owning shard lock with the
        //    Arc::ptr_eq re-check. Independent runs touch disjoint device ranges
        //    and (mostly) disjoint shards, so they fan out across the flush pool
        //    when present; the per-block dirty-clear re-locks each shard.
        match &self.flush_pool {
            Some(pool) => {
                use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
                pool.install(|| {
                    runs.par_iter()
                        .map(|run| self.flush_run(run))
                        .reduce(|| Ok(()), |a, b| a.and(b))
                })
            }
            None => {
                let mut first_err = Ok(());
                for run in &runs {
                    if let Err(e) = self.flush_run(run)
                        && first_err.is_ok()
                    {
                        first_err = Err(e);
                    }
                }
                first_err
            }
        }
    }

    /// Merge a block-index-sorted dirty snapshot into contiguous runs, each
    /// capped at [`MAX_COALESCE_BYTES`]. Adjacent device blocks (`idx ==
    /// prev + 1`) join one run; a gap, or reaching the byte cap, starts a new
    /// run. The input MUST be sorted ascending by block index.
    fn merge_into_runs(&self, snapshot: Vec<(u64, Arc<[u8]>)>) -> Vec<FlushRun> {
        let mut runs: Vec<FlushRun> = Vec::new();
        for (idx, data) in snapshot {
            let len = data.len();
            match runs.last_mut() {
                // Extend the current run iff this block immediately follows the
                // last one AND adding it stays within the coalesce cap.
                Some(run)
                    if idx == run.start + run.blocks.len() as u64
                        && run.bytes + len <= MAX_COALESCE_BYTES =>
                {
                    run.bytes += len;
                    run.blocks.push((idx, data));
                }
                _ => runs.push(FlushRun {
                    start: idx,
                    bytes: len,
                    blocks: vec![(idx, data)],
                }),
            }
        }
        runs
    }

    /// Write one contiguous run as a single sequential `pwrite`, then clear the
    /// dirty bit of each block it covered (under the owning shard lock, with the
    /// [`Arc::ptr_eq`] concurrent-redirty re-check). No shard lock is held across
    /// the device write.
    fn flush_run(&self, run: &FlushRun) -> Result<()> {
        // Concatenate the run's block bytes into one aligned bounce buffer and
        // issue a single sequential pwrite at the run's device offset. The base
        // offset is block-aligned (start * block_size) and the buffer base is
        // alignment-aligned, so the inner O_DIRECT write is legal.
        let run_base = run.start * self.block_size as u64;
        let mut buf = AlignedBuf::new(run.bytes, self.block_size);
        let mut off = 0usize;
        for (_, data) in &run.blocks {
            buf[off..off + data.len()].copy_from_slice(data);
            off += data.len();
        }
        self.inner.pwrite_all_at(&buf[..run.bytes], run_base)?;

        // Clear the dirty bit per block under its shard lock, only if the cached
        // Arc is still the exact snapshot we just flushed (CoW re-dirty check).
        for (idx, data) in &run.blocks {
            let mut shard = self.shard_of(*idx).lock();
            if let Some(b) = shard.blocks.get_mut(idx)
                && Arc::ptr_eq(&b.data, data)
            {
                b.dirty = false;
                shard.dirty.remove(idx);
            }
        }
        Ok(())
    }
}

/// One contiguous run of dirty blocks to be flushed as a single sequential
/// `pwrite`. `start` is the device block index of the first block; `blocks`
/// holds `(block_idx, Arc<bytes>)` in ascending, contiguous order; `bytes` is
/// the total payload length (sum of the block lengths, `<= MAX_COALESCE_BYTES`).
struct FlushRun {
    start: u64,
    bytes: usize,
    blocks: Vec<(u64, Arc<[u8]>)>,
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

    /// Build a bare single-shard `CacheState` (no background flusher, no flush
    /// pool) so eviction policy can be exercised deterministically — the
    /// production `shard_count` is `cores*16`, which makes per-shard placement
    /// non-deterministic in a test.
    fn one_shard_state(inner: Arc<dyn BlockDevice>, cap: usize, writeback: bool) -> CacheState {
        CacheState {
            inner,
            block_size: BS,
            writeback,
            shards: vec![Mutex::new(Shard {
                blocks: std::collections::HashMap::new(),
                dirty: std::collections::HashSet::new(),
                tick: 0,
                cap,
            })]
            .into_boxed_slice(),
            shard_count: 1,
            flush_pool: None,
        }
    }

    fn put_block(shard: &mut Shard, idx: u64, byte: u8, dirty: bool, last_used: u64) {
        let data: Arc<[u8]> = vec![byte; BS].into();
        shard.blocks.insert(
            idx,
            Block {
                data,
                dirty,
                last_used,
            },
        );
        if dirty {
            shard.dirty.insert(idx);
        }
    }

    #[test]
    fn evict_prefers_clean_victim_over_older_dirty_no_flush_under_lock() {
        // A full shard holding one dirty (LRU/oldest) + one clean (newer) block.
        // Pure-LRU would evict+FLUSH the dirty block under the lock; the fix must
        // instead evict the CLEAN block with NO device write.
        let dev = CountingDev::new(64 * BS, BS);
        let state = one_shard_state(dev.clone(), 2, true);
        let mut shard = state.shards[0].lock();
        put_block(&mut shard, 0, 0xAA, true, 1); // dirty, oldest
        put_block(&mut shard, 1, 0xBB, false, 2); // clean, newer
        state.evict_if_full(&mut shard).unwrap();
        assert!(
            shard.blocks.contains_key(&0),
            "dirty block must be retained (not flushed/evicted)"
        );
        assert!(
            !shard.blocks.contains_key(&1),
            "clean block must be the eviction victim"
        );
        assert_eq!(
            dev.writes.load(Ordering::Relaxed),
            0,
            "no device write under the shard lock when a clean victim exists"
        );
        assert!(
            shard.dirty.contains(&0),
            "dirty worklist still tracks the retained dirty block"
        );
    }

    #[test]
    fn evict_flushes_lru_dirty_victim_when_no_clean_block_exists() {
        // Fallback: every resident block is dirty → eviction MUST flush the LRU
        // dirty victim under the lock (correctness preserved), exactly once.
        let dev = CountingDev::new(64 * BS, BS);
        let state = one_shard_state(dev.clone(), 2, true);
        let mut shard = state.shards[0].lock();
        put_block(&mut shard, 0, 0xAA, true, 1); // dirty, oldest
        put_block(&mut shard, 1, 0xBB, true, 2); // dirty, newer
        state.evict_if_full(&mut shard).unwrap();
        assert!(
            !shard.blocks.contains_key(&0),
            "LRU dirty victim is evicted when no clean block is available"
        );
        assert!(
            shard.blocks.contains_key(&1),
            "newer dirty block is retained"
        );
        assert_eq!(
            dev.writes.load(Ordering::Relaxed),
            1,
            "exactly one device write to flush the dirty victim"
        );
        assert_eq!(
            read_inner(&dev, 0, BS),
            vec![0xAA; BS],
            "the victim's dirty bytes were flushed to the device (no data loss)"
        );
        assert!(
            !shard.dirty.contains(&0),
            "flushed victim is removed from the dirty worklist"
        );
    }

    #[test]
    fn clean_eviction_then_reread_reloads_correct_bytes() {
        // cap=1 single shard: reading a second block evicts the first (clean,
        // read-only) block with no flush; re-reading it reloads correct bytes.
        let dev = CountingDev::new(64 * BS, BS);
        dev.inner.pwrite(&ab(0x11, BS), 0).unwrap();
        dev.inner.pwrite(&ab(0x22, BS), BS as u64).unwrap();
        let state = one_shard_state(dev.clone(), 1, true);
        let mut b = AlignedBuf::new(BS, BS);
        state.pread(&mut b[..], 0).unwrap();
        assert_eq!(b[..].to_vec(), vec![0x11; BS]);
        state.pread(&mut b[..], BS as u64).unwrap(); // evicts clean block 0
        assert_eq!(b[..].to_vec(), vec![0x22; BS]);
        assert_eq!(
            dev.writes.load(Ordering::Relaxed),
            0,
            "evicting clean read-only blocks needs no device write"
        );
        state.pread(&mut b[..], 0).unwrap(); // block 0 was evicted → reload
        assert_eq!(
            b[..].to_vec(),
            vec![0x11; BS],
            "evicted clean block reloads correct bytes from the device"
        );
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

    /// Assert the lockstep invariant across every shard: the `dirty` index set
    /// contains EXACTLY the indices whose `Block.dirty == true`. Returns the
    /// total size of the dirty index sets for convenience.
    fn assert_dirty_index_consistent(cache: &CachingDevice) -> usize {
        let mut total_set = 0usize;
        for s in cache.state.shards.iter() {
            let s = s.lock();
            // Every index in the set is a resident, genuinely-dirty block.
            for idx in s.dirty.iter() {
                let b = s
                    .blocks
                    .get(idx)
                    .unwrap_or_else(|| panic!("dirty index {idx} not resident in blocks map"));
                assert!(b.dirty, "index {idx} in dirty set but Block.dirty == false");
            }
            // Every genuinely-dirty block is in the set.
            for (idx, b) in s.blocks.iter() {
                if b.dirty {
                    assert!(
                        s.dirty.contains(idx),
                        "dirty block {idx} missing from the dirty index set"
                    );
                }
            }
            total_set += s.dirty.len();
        }
        total_set
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

        // Write several distinct blocks. Indices 0..8 are CONTIGUOUS, so the
        // coalescing flush merges them into a single sequential pwrite — the
        // background drain therefore issues far fewer than 8 inner writes (1 with
        // full coalescing), while still landing every block's bytes. We assert
        // the drain happened (>= 1 write) and verify every block byte-for-byte
        // below; the exact coalesced count is asserted by the dedicated
        // coalescing tests.
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
            dev.writes.load(Ordering::Relaxed) >= 1,
            "background writeback issued the (coalesced) inner write(s) (got {})",
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
    fn concurrent_write_during_flush_does_not_corrupt_snapshot() {
        // CoW correctness: a write that mutates a block while that block's bytes
        // are being flushed must not corrupt the in-flight snapshot. With the
        // Arc<[u8]> payload, the dirty snapshot is a refcount bump and a writer
        // that mutates the shared block replaces the Arc (copy-on-write) rather
        // than scribbling over the buffer the flusher is reading.
        //
        // Inner device whose pwrite blocks on a barrier so a concurrent writer
        // is guaranteed to interleave with an in-flight flush.
        use std::sync::mpsc;

        struct GatedDev {
            inner: MemoryDevice,
            // Sends the bytes observed by the flusher at pwrite time.
            observed: Mutex<Option<mpsc::Sender<Vec<u8>>>>,
            // The flusher blocks here until the test releases it.
            release: Arc<(Mutex<bool>, Condvar)>,
        }

        impl BlockDevice for GatedDev {
            fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
                self.inner.pread(buf, offset)
            }
            fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
                // Report the bytes this flush is about to persist, then block so
                // the test can mutate the same block before we return.
                if let Some(tx) = self.observed.lock().take() {
                    let _ = tx.send(buf.to_vec());
                    let (m, cv) = &*self.release;
                    let mut released = m.lock();
                    while !*released {
                        cv.wait(&mut released);
                    }
                }
                self.inner.pwrite(buf, offset)
            }
            fn alignment(&self) -> usize {
                self.inner.alignment()
            }
            fn size(&self) -> u64 {
                self.inner.size()
            }
            fn sync(&self) -> Result<()> {
                self.inner.sync()
            }
        }

        let (tx, rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let dev = Arc::new(GatedDev {
            inner: MemoryDevice::new((64 * BS) as u64, BS).unwrap(),
            observed: Mutex::new(Some(tx)),
            release: release.clone(),
        });
        // NEVER_MS so the only flush is the explicit sync() we drive from a
        // helper thread; the test owns the interleaving.
        let cache = Arc::new(CachingDevice::new(dev.clone(), 16 * BS, true, NEVER_MS));

        // Dirty block 0 with 0xAA.
        cache.pwrite(&ab(0xAA, BS), 0).unwrap();

        // Flush in a helper thread; it will block inside GatedDev::pwrite.
        let flusher = {
            let cache = cache.clone();
            std::thread::spawn(move || cache.sync().unwrap())
        };

        // Wait until the flusher is mid-pwrite holding the 0xAA snapshot.
        let observed = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            observed,
            vec![0xAA; BS],
            "flush observed the snapshot bytes at the moment of the write"
        );

        // Now mutate the SAME block while the flush is in flight. CoW must keep
        // the flusher's snapshot intact (the assertion above already captured
        // the bytes, but the cache must also not panic / alias).
        cache.pwrite(&ab(0xBB, BS), 0).unwrap();
        assert_eq!(
            read_cache(&cache, 0, BS),
            vec![0xBB; BS],
            "the concurrent write is immediately visible through the cache"
        );

        // Release the flusher.
        {
            let (m, cv) = &*release;
            *m.lock() = true;
            cv.notify_all();
        }
        flusher.join().unwrap();

        // The block was re-dirtied during the flush, so it must remain dirty
        // (its bytes changed since the snapshot) and a subsequent sync persists
        // the newest bytes.
        assert_eq!(
            dirty_count(&cache),
            1,
            "re-dirtied-during-flush block keeps its dirty flag (bytes changed)"
        );
        cache.sync().unwrap();
        let mut got = AlignedBuf::new(BS, BS);
        dev.inner.pread(&mut got[..], 0).unwrap();
        assert_eq!(
            got[..].to_vec(),
            vec![0xBB; BS],
            "final sync persists the newest bytes, not the stale snapshot"
        );
    }

    #[test]
    fn parallel_flush_drains_many_shards_correctly() {
        // Exercise the parallel multi-shard flush path: write one block into
        // every shard, then a single sync() must flush them all with correct
        // bytes regardless of how the shard work is fanned out across workers.
        let dev = CountingDev::new(4096 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 2048 * BS, true, NEVER_MS);
        let sc = cache.state.shard_count;

        // Block index `s` lands in shard `s % sc`; writing 0..sc covers them all.
        for s in 0..sc {
            let byte = (0x40 + (s & 0x3f)) as u8;
            cache.pwrite(&ab(byte, BS), s * BS as u64).unwrap();
        }
        assert_eq!(
            dirty_count(&cache),
            sc as usize,
            "every shard holds one dirty block before sync"
        );

        cache.sync().unwrap();
        assert_eq!(dirty_count(&cache), 0, "sync cleared every shard");
        for s in 0..sc {
            let byte = (0x40 + (s & 0x3f)) as u8;
            assert_eq!(
                read_inner(&dev, s * BS as u64, BS),
                vec![byte; BS],
                "shard block {s} flushed with the correct bytes"
            );
        }
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

    #[test]
    fn flush_only_touches_dirty_blocks_and_index_matches() {
        // (a) After writes, ONLY the dirty blocks are flushed and reach the
        // inner device, and the dirty index exactly matches the dirty blocks.
        let dev = CountingDev::new(1024 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 512 * BS, true, NEVER_MS);

        // Make several blocks resident-but-CLEAN via reads (so flush would scan
        // them under the old O(all-blocks) path but must NOT flush them now).
        dev.inner.pwrite(&ab(0x00, BS), 10 * BS as u64).unwrap();
        dev.inner.pwrite(&ab(0x00, BS), 11 * BS as u64).unwrap();
        dev.inner.pwrite(&ab(0x00, BS), 12 * BS as u64).unwrap();
        for i in 10..13u64 {
            let _ = read_cache(&cache, i * BS as u64, BS);
        }
        let clean_reads = dev.reads.load(Ordering::Relaxed);

        // Dirty exactly two blocks. Use NON-contiguous indices (0 and 2) so the
        // coalescing flush keeps them as two separate runs → two pwrites, making
        // the "exactly the two dirty blocks were flushed" count deterministic and
        // unaffected by run-merging (contiguous coalescing is covered by its own
        // tests).
        cache.pwrite(&ab(0xD1, BS), 0).unwrap();
        cache.pwrite(&ab(0xD2, BS), 2 * BS as u64).unwrap();

        // Index invariant + count == exactly the dirty blocks (the 3 clean
        // resident blocks are NOT in the set).
        assert_eq!(assert_dirty_index_consistent(&cache), 2);
        assert_eq!(dirty_count(&cache), 2);

        let writes_before = dev.writes.load(Ordering::Relaxed);
        cache.sync().unwrap();
        let flushed = dev.writes.load(Ordering::Relaxed) - writes_before;
        assert_eq!(
            flushed, 2,
            "exactly the two (non-contiguous) dirty blocks were flushed (clean resident blocks were not)"
        );
        // No spurious inner reads from the flush path.
        assert_eq!(
            dev.reads.load(Ordering::Relaxed),
            clean_reads,
            "flush issues no inner reads"
        );
        // The dirty bytes reached the device.
        assert_eq!(read_inner(&dev, 0, BS), vec![0xD1; BS]);
        assert_eq!(read_inner(&dev, 2 * BS as u64, BS), vec![0xD2; BS]);
        // Index now empty and still consistent.
        assert_eq!(assert_dirty_index_consistent(&cache), 0);
        assert_eq!(dirty_count(&cache), 0);
    }

    #[test]
    fn write_during_inflight_flush_keeps_block_in_index() {
        // (b) A write during an in-flight flush keeps the block dirty AND in the
        // index (CoW re-dirty); a later sync persists the newest bytes. Drives
        // the interleave with a gated inner device, asserting the index state.
        use std::sync::mpsc;

        struct GatedDev {
            inner: MemoryDevice,
            observed: Mutex<Option<mpsc::Sender<Vec<u8>>>>,
            release: Arc<(Mutex<bool>, Condvar)>,
        }

        impl BlockDevice for GatedDev {
            fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
                self.inner.pread(buf, offset)
            }
            fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
                if let Some(tx) = self.observed.lock().take() {
                    let _ = tx.send(buf.to_vec());
                    let (m, cv) = &*self.release;
                    let mut released = m.lock();
                    while !*released {
                        cv.wait(&mut released);
                    }
                }
                self.inner.pwrite(buf, offset)
            }
            fn alignment(&self) -> usize {
                self.inner.alignment()
            }
            fn size(&self) -> u64 {
                self.inner.size()
            }
            fn sync(&self) -> Result<()> {
                self.inner.sync()
            }
        }

        let (tx, rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let dev = Arc::new(GatedDev {
            inner: MemoryDevice::new((64 * BS) as u64, BS).unwrap(),
            observed: Mutex::new(Some(tx)),
            release: release.clone(),
        });
        let cache = Arc::new(CachingDevice::new(dev.clone(), 16 * BS, true, NEVER_MS));

        cache.pwrite(&ab(0xAA, BS), 0).unwrap();
        assert_eq!(assert_dirty_index_consistent(&cache), 1);

        let flusher = {
            let cache = cache.clone();
            std::thread::spawn(move || cache.sync().unwrap())
        };

        // Flusher is mid-pwrite holding the 0xAA snapshot.
        let observed = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(observed, vec![0xAA; BS]);

        // Re-dirty the SAME block during the in-flight flush (CoW swaps the Arc).
        cache.pwrite(&ab(0xBB, BS), 0).unwrap();
        // Index must still contain the block (it was re-dirtied).
        assert_eq!(
            assert_dirty_index_consistent(&cache),
            1,
            "re-dirtied-during-flush block stays in the index"
        );

        // Release the flusher; ptr_eq fails (Arc was swapped) so it must NOT
        // clear dirty and must NOT remove from the index.
        {
            let (m, cv) = &*release;
            *m.lock() = true;
            cv.notify_all();
        }
        flusher.join().unwrap();

        assert_eq!(
            assert_dirty_index_consistent(&cache),
            1,
            "block re-dirtied during flush remains dirty and in the index"
        );
        assert_eq!(dirty_count(&cache), 1);

        // A later sync persists the NEWEST bytes via the index.
        cache.sync().unwrap();
        assert_eq!(assert_dirty_index_consistent(&cache), 0);
        let mut got = AlignedBuf::new(BS, BS);
        dev.inner.pread(&mut got[..], 0).unwrap();
        assert_eq!(
            got[..].to_vec(),
            vec![0xBB; BS],
            "later sync persists the newest bytes via the dirty index"
        );
    }

    #[test]
    fn eviction_of_dirty_victim_flushes_and_clears_index() {
        // (c) Eviction of a dirty victim flushes it AND removes it from the
        // index. Per-shard cap 1 forces eviction within a shard.
        let dev = CountingDev::new(1024 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), BS, true, NEVER_MS);
        let sc = cache.state.shard_count;

        let idx_a = 0u64;
        let idx_b = sc; // same shard as 0
        cache.pwrite(&ab(0xA1, BS), idx_a * BS as u64).unwrap();
        // The victim is dirty and in the index.
        assert_eq!(assert_dirty_index_consistent(&cache), 1);

        // Writing idx_b evicts idx_a (flushing it), then idx_b is the new dirty.
        cache.pwrite(&ab(0xB2, BS), idx_b * BS as u64).unwrap();
        assert_eq!(
            read_inner(&dev, idx_a * BS as u64, BS),
            vec![0xA1; BS],
            "evicting a dirty victim flushes it to the inner device"
        );
        // idx_a is gone from the cache (and thus the index); only idx_b remains.
        let total = assert_dirty_index_consistent(&cache);
        assert_eq!(total, 1, "only the surviving dirty block is in the index");
        // Confirm the surviving entry is idx_b, not the evicted idx_a.
        let shard = cache.state.shard_of(idx_b).lock();
        assert!(shard.dirty.contains(&idx_b), "surviving block in index");
        assert!(
            !shard.dirty.contains(&idx_a),
            "evicted victim removed from index"
        );
    }

    #[test]
    fn sync_flushes_everything_via_index() {
        // (d) sync() flushes all dirty blocks via the index, across many shards.
        let dev = CountingDev::new(4096 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 2048 * BS, true, NEVER_MS);
        let sc = cache.state.shard_count;

        for s in 0..sc {
            let byte = (0x40 + (s & 0x3f)) as u8;
            cache.pwrite(&ab(byte, BS), s * BS as u64).unwrap();
        }
        assert_eq!(
            assert_dirty_index_consistent(&cache),
            sc as usize,
            "every shard's index holds its one dirty block"
        );

        cache.sync().unwrap();
        assert_eq!(
            assert_dirty_index_consistent(&cache),
            0,
            "sync cleared every shard's index"
        );
        for s in 0..sc {
            let byte = (0x40 + (s & 0x3f)) as u8;
            assert_eq!(read_inner(&dev, s * BS as u64, BS), vec![byte; BS]);
        }
    }

    #[test]
    fn contiguous_dirty_blocks_flush_as_one_coalesced_pwrite() {
        // N contiguous dirty blocks (block indices 0..N) live in N DIFFERENT
        // shards (shard_of = idx % shard_count), yet a single sync() must
        // coalesce them into ONE sequential pwrite — not N scattered 4 KiB
        // writes — because the flush gathers dirty blocks ACROSS shards, sorts
        // by block_idx, and merges adjacent runs. With N*BS <= MAX_COALESCE the
        // whole run is one pwrite.
        let n: u64 = 16;
        assert!(
            (n as usize) * BS <= MAX_COALESCE_BYTES,
            "test run must fit one coalesced write"
        );
        let dev = CountingDev::new(1024 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 512 * BS, true, NEVER_MS);

        for i in 0..n {
            cache
                .pwrite(&ab(0x10 + i as u8, BS), i * BS as u64)
                .unwrap();
        }
        assert_eq!(
            dirty_count(&cache),
            n as usize,
            "every contiguous block starts dirty"
        );

        let writes_before = dev.writes.load(Ordering::Relaxed);
        cache.sync().unwrap();
        let pwrites = dev.writes.load(Ordering::Relaxed) - writes_before;
        assert_eq!(
            pwrites, 1,
            "N={n} contiguous dirty blocks must coalesce into ONE pwrite, got {pwrites}"
        );
        // Integrity: every block's exact bytes reached the device.
        for i in 0..n {
            assert_eq!(
                read_inner(&dev, i * BS as u64, BS),
                vec![0x10 + i as u8; BS],
                "coalesced block {i} has the correct bytes on the device"
            );
        }
        assert_eq!(dirty_count(&cache), 0, "all blocks clean after flush");
        assert_eq!(assert_dirty_index_consistent(&cache), 0);
    }

    #[test]
    fn noncontiguous_dirty_blocks_flush_as_separate_runs() {
        // Three dirty blocks at indices 0, 5, 10 (gaps between them) form three
        // separate runs → exactly three pwrites, one per run.
        let dev = CountingDev::new(1024 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 512 * BS, true, NEVER_MS);

        for &i in &[0u64, 5, 10] {
            cache
                .pwrite(&ab(0x20 + i as u8, BS), i * BS as u64)
                .unwrap();
        }
        assert_eq!(dirty_count(&cache), 3);

        let writes_before = dev.writes.load(Ordering::Relaxed);
        cache.sync().unwrap();
        let pwrites = dev.writes.load(Ordering::Relaxed) - writes_before;
        assert_eq!(
            pwrites, 3,
            "three non-contiguous dirty blocks are three runs → three pwrites, got {pwrites}"
        );
        for &i in &[0u64, 5, 10] {
            assert_eq!(
                read_inner(&dev, i * BS as u64, BS),
                vec![0x20 + i as u8; BS],
                "non-contiguous block {i} flushed correctly"
            );
        }
        assert_eq!(dirty_count(&cache), 0);
    }

    #[test]
    fn mixed_runs_coalesce_per_contiguous_group() {
        // Blocks {0,1,2} contiguous, {7,8} contiguous, {15} alone → 3 runs.
        let dev = CountingDev::new(1024 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 512 * BS, true, NEVER_MS);

        let idxs = [0u64, 1, 2, 7, 8, 15];
        for &i in &idxs {
            cache
                .pwrite(&ab(0x30 + i as u8, BS), i * BS as u64)
                .unwrap();
        }
        let writes_before = dev.writes.load(Ordering::Relaxed);
        cache.sync().unwrap();
        let pwrites = dev.writes.load(Ordering::Relaxed) - writes_before;
        assert_eq!(
            pwrites, 3,
            "runs {{0,1,2}},{{7,8}},{{15}} → 3 coalesced pwrites, got {pwrites}"
        );
        for &i in &idxs {
            assert_eq!(
                read_inner(&dev, i * BS as u64, BS),
                vec![0x30 + i as u8; BS],
                "block {i} flushed correctly across coalesced runs"
            );
        }
    }

    #[test]
    fn long_contiguous_run_is_split_at_max_coalesce_cap() {
        // A contiguous run longer than MAX_COALESCE_BYTES must be split into
        // bounded pwrites of at most MAX_COALESCE_BYTES each — never one giant
        // unbounded write. Choose N so the run spans > 1 cap-sized chunk.
        let cap_blocks = MAX_COALESCE_BYTES / BS;
        let n = (cap_blocks as u64) * 2 + 3; // > 2 full caps → 3 pwrites
        let expected = n.div_ceil(cap_blocks as u64);
        // The budget must hold the whole contiguous run resident so NO block is
        // evicted (and flushed individually) during the write loop — an early
        // eviction would fragment the run and inflate the pwrite count. Blocks i
        // and i+shard_count collide in one shard, so a generous total budget
        // (large per-shard cap) keeps the run resident regardless of the
        // production `cores*16` shard count.
        let dev = CountingDev::new(4096 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 2048 * BS, true, NEVER_MS);

        for i in 0..n {
            cache
                .pwrite(&ab((i & 0xff) as u8, BS), i * BS as u64)
                .unwrap();
        }
        let writes_before = dev.writes.load(Ordering::Relaxed);
        cache.sync().unwrap();
        let pwrites = (dev.writes.load(Ordering::Relaxed) - writes_before) as u64;
        assert_eq!(
            pwrites, expected,
            "a {n}-block contiguous run must split into {expected} capped pwrites \
             (cap = {cap_blocks} blocks), got {pwrites}"
        );
        // Integrity across the split boundaries.
        for i in 0..n {
            assert_eq!(
                read_inner(&dev, i * BS as u64, BS),
                vec![(i & 0xff) as u8; BS],
                "block {i} correct across a split coalesced write"
            );
        }
        assert_eq!(dirty_count(&cache), 0);
    }

    #[test]
    fn coalesced_flush_integrity_reading_every_block_back() {
        // Data integrity at scale: a dense run of contiguous dirty blocks, each
        // with distinct bytes, all read back bypassing the cache after a single
        // coalesced sync().
        let n: u64 = 64;
        let dev = CountingDev::new(1024 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 512 * BS, true, NEVER_MS);
        for i in 0..n {
            cache
                .pwrite(&ab((i & 0xff) as u8, BS), i * BS as u64)
                .unwrap();
        }
        cache.sync().unwrap();
        for i in 0..n {
            assert_eq!(
                read_inner(&dev, i * BS as u64, BS),
                vec![(i & 0xff) as u8; BS],
                "every coalesced block reads back correctly bypassing the cache"
            );
        }
    }

    #[test]
    fn write_through_never_populates_dirty_index() {
        // (e) Write-through is unchanged: no block is ever dirty and the index
        // stays empty.
        let dev = CountingDev::new(64 * BS, BS);
        let cache = CachingDevice::new(dev.clone(), 16 * BS, false, NEVER_MS);

        cache.pwrite(&ab(0x44, BS), 0).unwrap();
        cache.pwrite(&ab(0x55, BS), BS as u64).unwrap();
        // Reads to make blocks resident must not add to the index either.
        let _ = read_cache(&cache, 0, BS);

        assert_eq!(
            assert_dirty_index_consistent(&cache),
            0,
            "write-through never adds to the dirty index"
        );
        assert_eq!(dirty_count(&cache), 0);
        // Bytes reached inner immediately (unchanged write-through semantics).
        assert_eq!(read_inner(&dev, 0, BS), vec![0x44; BS]);
        assert_eq!(read_inner(&dev, BS as u64, BS), vec![0x55; BS]);
        // sync() over an empty index issues no flush writes, only the inner sync.
        let writes_before = dev.writes.load(Ordering::Relaxed);
        cache.sync().unwrap();
        assert_eq!(
            dev.writes.load(Ordering::Relaxed),
            writes_before,
            "write-through sync flushes nothing via the index"
        );
    }
}
