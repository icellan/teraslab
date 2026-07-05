//! The throttled, torn-read-safe device copier.
//!
//! Reads device ranges through the engine's store handle with
//! [`crate::device::BlockDevice::pread_nocache`] (so the copy never evicts the
//! hot write-back cache), each chunk guarded by the same striped block locks a
//! writer holds across its RMW ([`crate::io::read_span_blocks`]), and paced by a
//! token bucket. Every range is written to a backup image file and SHA-256'd.

use std::io::Write;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use crate::backup::BackupError;
use crate::backup::manifest::RangeEntry;
use crate::ops::engine::Engine;

/// The chunk size for a single guarded read (128 KiB = 32 blocks at 4 KiB).
pub const CHUNK_BYTES: u64 = 128 * 1024;

/// A simple token-bucket rate limiter (bytes/sec) with a one-second burst.
///
/// Pure/deterministic: [`Self::refill`] and [`Self::consume`] do no I/O or
/// clock reads, so the pacing math is unit-testable; the copier drives them
/// with a real [`Instant`] clock in [`Self::throttle`].
#[derive(Debug)]
pub struct TokenBucket {
    rate: f64,
    capacity: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    /// A bucket refilling at `rate_bytes_per_sec`, starting full (a one-second
    /// burst). A rate of 0 means unthrottled.
    pub fn new(rate_bytes_per_sec: u64) -> Self {
        let rate = rate_bytes_per_sec as f64;
        Self {
            rate,
            capacity: rate,
            tokens: rate,
            last: Instant::now(),
        }
    }

    /// Whether this bucket is unthrottled (rate 0).
    pub fn is_unlimited(&self) -> bool {
        self.rate <= 0.0
    }

    /// Add tokens for `elapsed`, capped at capacity.
    pub fn refill(&mut self, elapsed: Duration) {
        self.tokens = (self.tokens + elapsed.as_secs_f64() * self.rate).min(self.capacity);
    }

    /// Consume `bytes`. Returns `None` if enough tokens were available (and
    /// deducts them), else the [`Duration`] the caller must wait before the
    /// bucket will have accrued enough (tokens are NOT deducted in that case).
    pub fn consume(&mut self, bytes: u64) -> Option<Duration> {
        if self.is_unlimited() {
            return None;
        }
        let need = bytes as f64;
        if self.tokens >= need {
            self.tokens -= need;
            None
        } else {
            let deficit = need - self.tokens;
            Some(Duration::from_secs_f64(deficit / self.rate))
        }
    }

    /// Block until `bytes` can be sent, pacing to the configured rate.
    pub fn throttle(&mut self, bytes: u64) {
        if self.is_unlimited() {
            return;
        }
        loop {
            let now = Instant::now();
            let elapsed = now.saturating_duration_since(self.last);
            self.last = now;
            self.refill(elapsed);
            match self.consume(bytes) {
                None => return,
                Some(wait) => std::thread::sleep(wait),
            }
        }
    }
}

/// Copy a store-relative device range `[device_offset, device_offset + len)`
/// from store `device_id` into `out`, guarded per-chunk and throttled, hashing
/// the bytes both into `whole` (the running image hash) and a per-range hash.
///
/// Reads use `pread_nocache` under [`crate::io::read_span_blocks`] read guards,
/// so a chunk is never torn against an in-flight in-place RMW. `len` must be a
/// multiple of the device alignment (segment sizes and the 1 MiB header block
/// both are). Returns the range's manifest entry.
#[allow(clippy::too_many_arguments)]
pub fn copy_range<W: Write>(
    engine: &Engine,
    device_id: u8,
    device_offset: u64,
    len: u64,
    alignment: usize,
    throttle: &mut TokenBucket,
    out: &mut W,
    whole: &mut Sha256,
) -> Result<RangeEntry, BackupError> {
    let device = engine.device_for(device_id).clone();
    let mut range_hasher = Sha256::new();
    let mut copied = 0u64;
    let mut buf = crate::device::AlignedBuf::new(CHUNK_BYTES as usize, alignment);
    while copied < len {
        let this = CHUNK_BYTES.min(len - copied);
        let at = device_offset + copied;
        throttle.throttle(this);
        {
            // Hold the SAME striped block locks a writer holds across its RMW,
            // so this chunk read reflects a committed pre- or post-state, never
            // a torn mix.
            let _guards = crate::io::read_span_blocks(at, this);
            device
                .pread_nocache(&mut buf[..this as usize], at)
                .map_err(BackupError::Device)?;
        }
        let slice = &buf[..this as usize];
        out.write_all(slice).map_err(BackupError::Io)?;
        range_hasher.update(slice);
        whole.update(slice);
        copied += this;
    }
    Ok(RangeEntry {
        device_offset,
        len,
        sha256: hex_digest(range_hasher),
    })
}

/// Finalize a SHA-256 hasher into lowercase hex.
pub fn hex_digest(h: Sha256) -> String {
    let digest = h.finalize();
    let mut s = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::manifest::sha256_hex;
    use crate::device::{BlockDevice, MemoryDevice};
    use crate::index::Index;
    use crate::index::{DahIndex, UnminedIndex};
    use crate::locks::StripedLocks;
    use crate::ops::engine::Engine;
    use crate::segment_allocator::SegmentAllocator;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn engine_over(dev: Arc<dyn BlockDevice>) -> Engine {
        let seg = SegmentAllocator::new(dev.clone(), 2 * 4096).unwrap();
        Engine::new(
            dev,
            Index::new(256).unwrap(),
            seg,
            StripedLocks::new(256),
            DahIndex::new(),
            UnminedIndex::new(),
        )
    }

    #[test]
    fn token_bucket_math_is_deterministic() {
        let mut tb = TokenBucket::new(1000); // 1000 bytes/s, burst 1000
        // Start full: exactly the burst is immediate.
        assert_eq!(tb.consume(1000), None);
        // Now empty: 1 more byte needs 1/1000 s.
        let wait = tb.consume(1).expect("should need to wait");
        assert!(
            (wait.as_secs_f64() - 0.001).abs() < 1e-9,
            "1 byte at 1000 B/s = 1ms"
        );
        // Refill a full second → back to capacity, capped.
        tb.refill(Duration::from_secs(5));
        assert_eq!(tb.consume(1000), None);
    }

    #[test]
    fn token_bucket_zero_rate_is_unlimited() {
        let mut tb = TokenBucket::new(0);
        assert!(tb.is_unlimited());
        assert_eq!(tb.consume(u64::MAX), None);
        tb.throttle(u64::MAX); // returns immediately, no panic
    }

    #[test]
    fn sha256_stream_matches_direct_hash() {
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        // Seed a 3-block region with a recognizable pattern.
        let mut src = crate::device::AlignedBuf::new(3 * 4096, 4096);
        for (i, b) in src[..].iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        dev.pwrite(&src[..], 4096).unwrap();
        let engine = engine_over(dev);

        let mut out: Vec<u8> = Vec::new();
        let mut whole = Sha256::new();
        let mut tb = TokenBucket::new(0);
        let entry = copy_range(
            &engine,
            0,
            4096,
            3 * 4096,
            4096,
            &mut tb,
            &mut out,
            &mut whole,
        )
        .unwrap();

        assert_eq!(entry.device_offset, 4096);
        assert_eq!(entry.len, 3 * 4096);
        assert_eq!(&out[..], &src[..], "image bytes equal the source range");
        assert_eq!(
            entry.sha256,
            sha256_hex(&src[..]),
            "range hash matches source"
        );
        assert_eq!(
            hex_digest(whole),
            sha256_hex(&src[..]),
            "whole hash matches"
        );
    }

    #[test]
    fn chunk_copy_is_block_atomic_under_concurrent_inplace_rmw() {
        // A writer flips a block between all-0xAA and all-0xBB under the write
        // guard; the copier reads it under the read guard. Every copied block
        // must be uniform (never a torn 0xAA/0xBB mix).
        let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(4 * 1024 * 1024, 4096).unwrap());
        let block_off = 64 * 1024u64; // one block within the device
        // Seed 0xAA.
        let seed = crate::device::AlignedBuf::new(4096, 4096);
        dev.pwrite(
            &{
                let mut b = seed;
                b[..].fill(0xAA);
                b
            }[..],
            block_off,
        )
        .unwrap();
        let engine = Arc::new(engine_over(dev.clone()));

        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let dev = dev.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut val = 0xAAu8;
                while !stop.load(Ordering::Relaxed) {
                    val = if val == 0xAA { 0xBB } else { 0xAA };
                    let mut b = crate::device::AlignedBuf::new(4096, 4096);
                    b[..].fill(val);
                    let _g = crate::io::lock_span_blocks(block_off, 4096);
                    dev.pwrite(&b[..], block_off).unwrap();
                }
            })
        };

        for _ in 0..2000 {
            let mut out: Vec<u8> = Vec::new();
            let mut whole = Sha256::new();
            let mut tb = TokenBucket::new(0);
            copy_range(
                &engine, 0, block_off, 4096, 4096, &mut tb, &mut out, &mut whole,
            )
            .unwrap();
            let first = out[0];
            assert!(first == 0xAA || first == 0xBB);
            assert!(
                out.iter().all(|&b| b == first),
                "copied block torn: not uniform"
            );
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    }
}
