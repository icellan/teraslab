//! Streaming write buffer for the log-structured (segment) engine.
//!
//! The segment engine turns creates and spends into sequential appends at the
//! allocator cursor (see [`crate::segment_allocator`] and the relocate-on-spend
//! path). On its own that only makes the *logical* write pattern sequential —
//! the EC2 measurement (`bench/results/20260630-ec2-segment`) showed the writes
//! still hit the device as ~6 KB scattered pwrites, because the random-access
//! write-back cache shards contiguous blocks across shards and its eviction path
//! flushes single blocks. Post-hoc coalescing is fragile under that pressure.
//!
//! [`StreamingWriteDevice`] fixes the *physical* pattern by construction. It is a
//! [`BlockDevice`] wrapper (same integration shape as
//! [`crate::cache::CachingDevice`]) that buffers the contiguous tail of the
//! append stream in RAM and flushes it to the inner device as large sequential
//! pwrites (the reference datastore's streaming-buffer model). Reads of
//! not-yet-flushed offsets are served from the buffer; everything else falls
//! through to the inner device. Because the engine's writes reach the device via
//! [`crate::io::write_record_bytes`] → `write_image_rmw`, every write this
//! wrapper sees is already device-block aligned in both offset and length, so the
//! buffer always holds whole blocks — no sub-block RMW or padding is needed here.
//!
//! # Write cases
//! For a block-aligned write of `bytes` at `offset` (buffer covers
//! `[base, base+buf.len())`):
//! - **append / extend** — `offset == base + buf.len()`, or the buffer is empty:
//!   the common hot path; appended to the tail and the head flushed once the
//!   buffer crosses [`StreamingWriteDevice::flush_threshold`].
//! - **in-place overwrite** — `[offset, offset+len) ⊆ [base, base+buf.len())`:
//!   a re-write of a still-buffered block (e.g. a packed record's RMW updating a
//!   block other records share, or a setMined footer write before the block has
//!   flushed); copied into the buffer in place.
//! - **already-flushed write** — `offset + len <= base`: a mutation of a record
//!   that has left the buffer (in-place setMined/freeze on an older segment
//!   record); written through to the inner device.
//! - **discontiguous** — a gap above the tail (segment-boundary cursor jump) or a
//!   straddle: the buffer is flushed and a fresh buffer is started at `offset`.
//!
//! # Durability
//! [`StreamingWriteDevice::sync`] flushes the whole buffer to the inner device
//! and then syncs it, so the segment engine's buffered-durability contract is
//! unchanged: the checkpoint barrier's `sync()` makes every buffered append
//! durable before any covering redo prefix is reclaimed. A crash before that
//! barrier loses the unflushed tail together with its (buffered) redo — exactly
//! the segment engine's existing buffered-tail-loss semantics.

use crate::device::{AlignedBuf, BlockDevice, DeviceError, Result};
use parking_lot::Mutex;
use std::sync::Arc;

/// Default flush threshold: accumulate this many buffered bytes before flushing
/// the head, so each flush is one large sequential write rather than many small
/// ones. 1 MiB comfortably amortizes a pwrite while bounding the RAM footprint
/// and the crash-loss window of a single store's tail.
pub const DEFAULT_FLUSH_THRESHOLD: usize = 1024 * 1024;

/// Default cap on a single flush pwrite. The flushable head is written in pieces
/// no larger than this, matching the reference datastore's ~128 KiB streaming
/// flush granularity.
pub const DEFAULT_FLUSH_CHUNK: usize = 128 * 1024;

/// A [`BlockDevice`] wrapper that buffers the sequential append tail and flushes
/// it to the inner device as large sequential writes. See the module docs.
pub struct StreamingWriteDevice {
    inner: Arc<dyn BlockDevice>,
    block_size: usize,
    flush_threshold: usize,
    flush_chunk: usize,
    state: Mutex<Stream>,
}

/// The buffered append tail. `buf` covers the device range
/// `[base, base + buf.len())`; it is empty iff nothing is buffered. `buf.len()`
/// is always a whole multiple of `block_size` (every write this wrapper accepts
/// is block-aligned in offset and length).
struct Stream {
    base: u64,
    buf: Vec<u8>,
}

impl StreamingWriteDevice {
    /// Wrap `inner`, buffering the append tail. `flush_threshold` is the buffered
    /// byte count that triggers a head flush; `flush_chunk` caps a single flush
    /// pwrite. Both are rounded up to a whole number of device blocks (and to at
    /// least one block) so every flush write stays block-aligned.
    pub fn new(inner: Arc<dyn BlockDevice>, flush_threshold: usize, flush_chunk: usize) -> Self {
        let block_size = inner.alignment().max(1);
        let round = |n: usize| n.div_ceil(block_size).max(1) * block_size;
        Self {
            block_size,
            flush_threshold: round(flush_threshold),
            flush_chunk: round(flush_chunk),
            inner,
            state: Mutex::new(Stream {
                base: 0,
                buf: Vec::new(),
            }),
        }
    }

    /// Wrap `inner` with the default thresholds.
    pub fn with_defaults(inner: Arc<dyn BlockDevice>) -> Self {
        Self::new(inner, DEFAULT_FLUSH_THRESHOLD, DEFAULT_FLUSH_CHUNK)
    }

    /// Write the buffered head (everything except the still-active last block) to
    /// the inner device in `flush_chunk`-sized sequential pwrites, then advance
    /// `base` past the flushed bytes and drop them from `buf`.
    ///
    /// The last block is RETAINED: under packed placement the cursor's current
    /// block keeps receiving in-place record overwrites until the cursor advances
    /// past it, so flushing it early would force those overwrites down the
    /// already-flushed (scattered) path. `force` flushes the whole buffer,
    /// including the last block — used by [`Self::sync`].
    ///
    /// I/O happens while the lock is held: a reader of a just-flushed offset must
    /// not observe `base` advanced before the inner write lands (it would read a
    /// stale inner block). Holding the lock across the pwrite keeps `base` and the
    /// inner device consistent. Writers to a store already serialize on the
    /// engine's allocator/stripe locks, and each flush is one large sequential
    /// write, so the cost is bounded.
    fn flush_head(&self, st: &mut Stream, force: bool) -> Result<()> {
        let flushable = if force {
            st.buf.len()
        } else {
            // Keep the trailing (active) block buffered.
            st.buf.len().saturating_sub(self.block_size)
        };
        if flushable == 0 {
            return Ok(());
        }
        let mut written = 0usize;
        while written < flushable {
            let take = self.flush_chunk.min(flushable - written);
            let off = st.base + written as u64;
            // O_DIRECT requires a block-aligned buffer ADDRESS; the buffer is a
            // plain `Vec`, so bounce the chunk through an `AlignedBuf` (mirrors
            // the cache's `flush_run`). `take` is a whole number of blocks.
            let mut bounce = AlignedBuf::new(take, self.block_size);
            bounce[..take].copy_from_slice(&st.buf[written..written + take]);
            self.inner.pwrite_all_at(&bounce, off)?;
            written += take;
        }
        st.buf.drain(0..flushable);
        st.base += flushable as u64;
        Ok(())
    }
}

impl BlockDevice for StreamingWriteDevice {
    fn alignment(&self) -> usize {
        self.inner.alignment()
    }

    fn size(&self) -> u64 {
        self.inner.size()
    }

    fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
        let len = buf.len();
        if len == 0 {
            return Ok(0);
        }
        // Every engine write reaches the device block-aligned (write_image_rmw).
        // A non-block-aligned write would break the whole-block buffer invariant;
        // surface it loudly rather than silently corrupt the stream.
        if !(offset as usize).is_multiple_of(self.block_size)
            || !len.is_multiple_of(self.block_size)
        {
            return Err(DeviceError::AlignmentViolation {
                detail: format!(
                    "StreamingWriteDevice requires block-aligned writes: offset={offset} len={len} block={}",
                    self.block_size,
                ),
            });
        }

        let mut st = self.state.lock();
        let end = st.base + st.buf.len() as u64;

        if st.buf.is_empty() {
            // Start a fresh buffer at this offset.
            st.base = offset;
            st.buf.extend_from_slice(buf);
        } else if offset == end {
            // Contiguous append — the hot path.
            st.buf.extend_from_slice(buf);
        } else if offset >= st.base && offset + len as u64 <= end {
            // In-place overwrite of a still-buffered region.
            let at = (offset - st.base) as usize;
            st.buf[at..at + len].copy_from_slice(buf);
        } else if offset + len as u64 <= st.base {
            // Mutation of an already-flushed record — write through.
            self.inner.pwrite_all_at(buf, offset)?;
        } else {
            // Discontiguous (segment-boundary jump or straddle): flush what we
            // have, then start a fresh buffer at `offset`.
            self.flush_head(&mut st, true)?;
            st.base = offset;
            st.buf.clear();
            st.buf.extend_from_slice(buf);
        }

        if st.buf.len() >= self.flush_threshold {
            self.flush_head(&mut st, false)?;
        }
        Ok(len)
    }

    fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let len = buf.len();
        if len == 0 {
            return Ok(0);
        }
        // Snapshot the overlap with the buffered region under the lock, copy it
        // out, and remember which sub-ranges still need the inner device. Inner
        // reads then run OUTSIDE the lock.
        let read_end = offset + len as u64;
        let mut below: Option<(u64, usize, usize)> = None; // (off, buf_start, copy_len)
        let mut above: Option<(u64, usize, usize)> = None;
        {
            let st = self.state.lock();
            let bend = st.base + st.buf.len() as u64;
            if st.buf.is_empty() || read_end <= st.base || offset >= bend {
                // No overlap — entirely on the inner device.
            } else {
                // Overlap [lo, hi) with the buffer.
                let lo = offset.max(st.base);
                let hi = read_end.min(bend);
                let dst_start = (lo - offset) as usize;
                let src_start = (lo - st.base) as usize;
                let n = (hi - lo) as usize;
                buf[dst_start..dst_start + n].copy_from_slice(&st.buf[src_start..src_start + n]);
                if offset < lo {
                    below = Some((offset, 0, (lo - offset) as usize));
                }
                if read_end > hi {
                    above = Some((hi, (hi - offset) as usize, (read_end - hi) as usize));
                }
            }
        }

        // Whole read served from the inner device (no buffer overlap).
        if below.is_none() && above.is_none() {
            let st_empty = {
                let st = self.state.lock();
                st.buf.is_empty() || read_end <= st.base || offset >= st.base + st.buf.len() as u64
            };
            if st_empty {
                return self.inner.pread_exact_at(buf, offset).map(|()| len);
            }
        }
        // Fill the below/above gaps from the inner device.
        if let Some((off, start, n)) = below {
            self.inner.pread_exact_at(&mut buf[start..start + n], off)?;
        }
        if let Some((off, start, n)) = above {
            self.inner.pread_exact_at(&mut buf[start..start + n], off)?;
        }
        Ok(len)
    }

    fn sync(&self) -> Result<()> {
        {
            let mut st = self.state.lock();
            self.flush_head(&mut st, true)?;
        }
        self.inner.sync()
    }

    fn sync_data(&self) -> Result<()> {
        {
            let mut st = self.state.lock();
            self.flush_head(&mut st, true)?;
        }
        self.inner.sync_data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::MemoryDevice;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Inner device that counts pwrite calls and records each write's length, so
    /// tests can prove coalescing (few large writes vs many small ones).
    struct CountingDevice {
        inner: MemoryDevice,
        writes: AtomicUsize,
        write_bytes: AtomicUsize,
        max_write: AtomicUsize,
    }

    impl CountingDevice {
        fn new(size: usize, align: usize) -> Self {
            Self {
                inner: MemoryDevice::new(size as u64, align).unwrap(),
                writes: AtomicUsize::new(0),
                write_bytes: AtomicUsize::new(0),
                max_write: AtomicUsize::new(0),
            }
        }
    }

    impl BlockDevice for CountingDevice {
        fn alignment(&self) -> usize {
            self.inner.alignment()
        }
        fn size(&self) -> u64 {
            self.inner.size()
        }
        fn pwrite(&self, buf: &[u8], offset: u64) -> Result<usize> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.write_bytes.fetch_add(buf.len(), Ordering::Relaxed);
            self.max_write.fetch_max(buf.len(), Ordering::Relaxed);
            self.inner.pwrite(buf, offset)
        }
        fn pread(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
            self.inner.pread(buf, offset)
        }
        fn sync(&self) -> Result<()> {
            self.inner.sync()
        }
    }

    const BS: usize = 4096;

    fn blk(byte: u8) -> Vec<u8> {
        vec![byte; BS]
    }

    fn write(dev: &StreamingWriteDevice, offset: u64, data: &[u8]) {
        let n = dev.pwrite(data, offset).unwrap();
        assert_eq!(n, data.len());
    }

    fn read(dev: &StreamingWriteDevice, offset: u64, len: usize) -> Vec<u8> {
        let mut buf = AlignedBuf::new(len, BS);
        let n = dev.pread(&mut buf, offset).unwrap();
        assert_eq!(n, len);
        buf[..len].to_vec()
    }

    #[test]
    fn sequential_appends_coalesce_into_few_large_writes() {
        // 64 single-block appends with a 1 MiB threshold (256 blocks) should hit
        // the inner device only at sync time — as ONE large sequential write —
        // not 64 scattered 4 KiB writes.
        let counting = Arc::new(CountingDevice::new(8 * 1024 * 1024, BS));
        let dev = StreamingWriteDevice::new(counting.clone(), 1024 * 1024, 1024 * 1024);
        for i in 0..64u64 {
            write(&dev, i * BS as u64, &blk(i as u8 + 1));
        }
        // Nothing flushed yet (well under threshold).
        assert_eq!(counting.writes.load(Ordering::Relaxed), 0, "no early flush");
        dev.sync().unwrap();
        assert_eq!(
            counting.writes.load(Ordering::Relaxed),
            1,
            "64 contiguous appends must flush as ONE sequential write"
        );
        assert_eq!(counting.write_bytes.load(Ordering::Relaxed), 64 * BS);
        // Data is intact end to end.
        for i in 0..64u64 {
            assert_eq!(read(&dev, i * BS as u64, BS), blk(i as u8 + 1), "block {i}");
        }
    }

    #[test]
    fn threshold_flushes_head_keeping_active_block() {
        // Threshold = 4 blocks. Writing the 4th block crosses the threshold and
        // flushes the head as ONE coalesced write, KEEPING the active (last)
        // block buffered — so 3 of the 4 blocks flush, not 4. The 5th block then
        // appends onto the retained tail. Reads stay correct across the boundary.
        let counting = Arc::new(CountingDevice::new(1024 * 1024, BS));
        let dev = StreamingWriteDevice::new(counting.clone(), 4 * BS, 4 * BS);
        for i in 0..5u64 {
            write(&dev, i * BS as u64, &blk(i as u8 + 10));
        }
        assert_eq!(
            counting.writes.load(Ordering::Relaxed),
            1,
            "one coalesced head flush"
        );
        assert_eq!(
            counting.write_bytes.load(Ordering::Relaxed),
            3 * BS,
            "flushed 3 of the first 4 blocks, kept the active block buffered"
        );
        for i in 0..5u64 {
            assert_eq!(
                read(&dev, i * BS as u64, BS),
                blk(i as u8 + 10),
                "block {i}"
            );
        }
    }

    #[test]
    fn in_place_overwrite_of_buffered_block_updates_buffer() {
        // Packed-style RMW: re-writing a still-buffered block must update the
        // buffer (not write through), and read back the new bytes.
        let counting = Arc::new(CountingDevice::new(1024 * 1024, BS));
        let dev = StreamingWriteDevice::with_defaults(counting.clone());
        write(&dev, 0, &blk(1));
        write(&dev, BS as u64, &blk(2));
        // Overwrite block 0 in place.
        write(&dev, 0, &blk(9));
        assert_eq!(
            counting.writes.load(Ordering::Relaxed),
            0,
            "buffered overwrite must not write through"
        );
        assert_eq!(read(&dev, 0, BS), blk(9));
        assert_eq!(read(&dev, BS as u64, BS), blk(2));
    }

    #[test]
    fn read_spans_buffer_and_inner_boundary() {
        // Flush blocks 0..4 (on the inner device), keep block 4 buffered, then
        // read a range crossing the flushed/buffered boundary.
        let counting = Arc::new(CountingDevice::new(1024 * 1024, BS));
        let dev = StreamingWriteDevice::new(counting.clone(), 4 * BS, 4 * BS);
        for i in 0..5u64 {
            write(&dev, i * BS as u64, &blk(i as u8 + 1));
        }
        // read blocks 3..5 (3 on inner, 4 buffered)
        let got = read(&dev, 3 * BS as u64, 2 * BS);
        assert_eq!(&got[..BS], &blk(4)[..]);
        assert_eq!(&got[BS..], &blk(5)[..]);
    }

    #[test]
    fn write_to_already_flushed_region_passes_through() {
        // After flushing block 0 to the inner device, an in-place mutation of it
        // (setMined on an older record) writes through and reads back correctly.
        let counting = Arc::new(CountingDevice::new(1024 * 1024, BS));
        let dev = StreamingWriteDevice::new(counting.clone(), 2 * BS, 2 * BS);
        for i in 0..3u64 {
            write(&dev, i * BS as u64, &blk(i as u8 + 1));
        }
        // blocks 0..2 flushed, block 2 buffered. Mutate flushed block 0.
        let writes_before = counting.writes.load(Ordering::Relaxed);
        write(&dev, 0, &blk(99));
        assert!(
            counting.writes.load(Ordering::Relaxed) > writes_before,
            "flushed-region write must pass through to inner"
        );
        assert_eq!(read(&dev, 0, BS), blk(99));
    }

    #[test]
    fn discontiguous_jump_flushes_and_restarts() {
        // A cursor jump to a far offset (new segment) flushes the prior buffer and
        // starts fresh; both regions read back correctly.
        let counting = Arc::new(CountingDevice::new(8 * 1024 * 1024, BS));
        let dev = StreamingWriteDevice::with_defaults(counting.clone());
        write(&dev, 0, &blk(1));
        write(&dev, BS as u64, &blk(2));
        // Jump to block 1000.
        write(&dev, 1000 * BS as u64, &blk(7));
        // Prior buffer (blocks 0,1) was flushed by the jump.
        assert!(counting.writes.load(Ordering::Relaxed) >= 1);
        assert_eq!(read(&dev, 0, BS), blk(1));
        assert_eq!(read(&dev, BS as u64, BS), blk(2));
        assert_eq!(read(&dev, 1000 * BS as u64, BS), blk(7));
    }

    #[test]
    fn sync_makes_everything_durable_on_inner() {
        // After sync the inner device holds every byte (buffer drained).
        let counting = Arc::new(CountingDevice::new(1024 * 1024, BS));
        let dev = StreamingWriteDevice::with_defaults(counting.clone());
        for i in 0..10u64 {
            write(&dev, i * BS as u64, &blk(i as u8 + 1));
        }
        dev.sync().unwrap();
        // Read directly from the inner device, bypassing the buffer.
        for i in 0..10u64 {
            let mut b = AlignedBuf::new(BS, BS);
            counting
                .inner
                .pread_exact_at(&mut b, i * BS as u64)
                .unwrap();
            assert_eq!(&b[..], &blk(i as u8 + 1)[..], "inner block {i}");
        }
    }

    #[test]
    fn rejects_non_block_aligned_write() {
        let counting = Arc::new(CountingDevice::new(1024 * 1024, BS));
        let dev = StreamingWriteDevice::with_defaults(counting);
        // Offset not block-aligned.
        let err = dev.pwrite(&blk(1), 100).unwrap_err();
        assert!(matches!(err, DeviceError::AlignmentViolation { .. }));
        // Length not a block multiple.
        let err = dev.pwrite(&vec![0u8; 100], 0).unwrap_err();
        assert!(matches!(err, DeviceError::AlignmentViolation { .. }));
    }

    #[test]
    fn write_record_bytes_round_trips_through_wrapper() {
        // The engine writes records via `io::write_record_bytes` (RMW → block
        // aligned). Drive a multi-block record image through the wrapper as a
        // `dyn BlockDevice` and read it back via the same I/O path the engine uses.
        let counting = Arc::new(CountingDevice::new(1024 * 1024, BS));
        let dev: Arc<dyn BlockDevice> = Arc::new(StreamingWriteDevice::with_defaults(counting));
        let mut image = AlignedBuf::new(2 * BS, BS);
        for (i, b) in image.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        crate::io::write_record_bytes(&*dev, BS as u64, &image).unwrap();
        // Read back the whole image (served from the buffer, pre-sync).
        let mut back = AlignedBuf::new(2 * BS, BS);
        dev.pread_exact_at(&mut back, BS as u64).unwrap();
        assert_eq!(&back[..], &image[..], "round trip before sync");
        // And after sync (served from the inner device).
        dev.sync().unwrap();
        let mut back2 = AlignedBuf::new(2 * BS, BS);
        dev.pread_exact_at(&mut back2, BS as u64).unwrap();
        assert_eq!(&back2[..], &image[..], "round trip after sync");
    }
}
