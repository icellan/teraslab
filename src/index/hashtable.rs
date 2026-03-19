//! Robin Hood open-addressing hash table backed by mmap.
//!
//! Uses the txid's first 8 bytes as the bucket index (the txid is already
//! a cryptographic hash with excellent distribution) and bytes 8–16 as a
//! fingerprint for fast rejection during probing.
//!
//! # Memory
//!
//! Backing memory is allocated via `mmap(MAP_ANONYMOUS | MAP_PRIVATE)`.
//! On Linux, the allocator first attempts 2 MB hugepages (`MAP_HUGETLB`)
//! for better TLB performance at scale, falling back to regular pages.
//! On macOS (development), regular pages are used directly.

use thiserror::Error;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors from hash table operations.
#[derive(Error, Debug)]
pub enum HashTableError {
    /// The table is full (should not happen if resize works).
    #[error("hash table full: {count}/{capacity}")]
    Full { count: usize, capacity: usize },

    /// Memory allocation failed.
    #[error("mmap allocation failed: {0}")]
    AllocFailed(String),
}

pub type Result<T> = std::result::Result<T, HashTableError>;

// ---------------------------------------------------------------------------
// TxKey
// ---------------------------------------------------------------------------

/// Primary key for index lookups — the 32-byte transaction ID.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxKey {
    /// Transaction hash (double-SHA256), 32 bytes.
    pub txid: [u8; 32],
}

impl TxKey {
    /// Create a key from a 32-byte txid.
    pub fn from_bytes(txid: [u8; 32]) -> Self {
        Self { txid }
    }
}

impl std::fmt::Debug for TxKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hex: String = self.txid[..8]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        write!(f, "TxKey({hex}...)")
    }
}

// ---------------------------------------------------------------------------
// TxIndexEntry
// ---------------------------------------------------------------------------

/// What the primary index stores for each transaction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TxIndexEntry {
    /// Which device this record lives on (for multi-device setups).
    pub device_id: u16,
    /// Byte offset on that device to the start of TxMetadata.
    pub record_offset: u64,
    /// Number of UTXO slots in this record.
    pub utxo_count: u32,
    /// Byte offset to inline cold data (0 if none/external).
    pub cold_offset: u64,
    /// Size of inline cold data in bytes (0 if none/external).
    pub cold_size: u32,
    /// Bit flags (has_external_ref, etc.).
    pub flags: u8,
}

// ---------------------------------------------------------------------------
// Bucket
// ---------------------------------------------------------------------------

const BUCKET_EMPTY: u8 = 0;
const BUCKET_OCCUPIED: u8 = 1;
#[allow(dead_code)]
const BUCKET_TOMBSTONE: u8 = 2;

/// Entry size: 1 + 2 + 32 + 8 + 2 + 8 + 4 + 8 + 4 + 1 = 70, pad to 8-byte boundary.
#[repr(C)]
#[derive(Clone, Copy)]
struct Bucket {
    occupied: u8,
    probe_distance: u16,
    txid: [u8; 32],
    fingerprint: u64,
    device_id: u16,
    record_offset: u64,
    utxo_count: u32,
    cold_offset: u64,
    cold_size: u32,
    flags: u8,
    _pad: [u8; BUCKET_PAD],
}

const BUCKET_RAW_SIZE: usize = 1 + 2 + 32 + 8 + 2 + 8 + 4 + 8 + 4 + 1;
const BUCKET_PAD: usize = if BUCKET_RAW_SIZE.is_multiple_of(8) {
    0
} else {
    8 - (BUCKET_RAW_SIZE % 8)
};

/// Actual size of one bucket in bytes.
pub const BUCKET_SIZE: usize = std::mem::size_of::<Bucket>();

impl Bucket {
    fn empty() -> Self {
        Self {
            occupied: BUCKET_EMPTY,
            probe_distance: 0,
            txid: [0; 32],
            fingerprint: 0,
            device_id: 0,
            record_offset: 0,
            utxo_count: 0,
            cold_offset: 0,
            cold_size: 0,
            flags: 0,
            _pad: [0; BUCKET_PAD],
        }
    }

    fn is_empty(&self) -> bool {
        self.occupied == BUCKET_EMPTY
    }

    fn is_occupied(&self) -> bool {
        self.occupied == BUCKET_OCCUPIED
    }

    fn entry(&self) -> TxIndexEntry {
        TxIndexEntry {
            device_id: self.device_id,
            record_offset: self.record_offset,
            utxo_count: self.utxo_count,
            cold_offset: self.cold_offset,
            cold_size: self.cold_size,
            flags: self.flags,
        }
    }

    fn set_entry(&mut self, key: &TxKey, entry: &TxIndexEntry, probe_dist: u16) {
        self.occupied = BUCKET_OCCUPIED;
        self.probe_distance = probe_dist;
        self.txid = key.txid;
        self.fingerprint = fingerprint(key);
        self.device_id = entry.device_id;
        self.record_offset = entry.record_offset;
        self.utxo_count = entry.utxo_count;
        self.cold_offset = entry.cold_offset;
        self.cold_size = entry.cold_size;
        self.flags = entry.flags;
    }
}

// ---------------------------------------------------------------------------
// Hash functions
// ---------------------------------------------------------------------------

/// Compute the bucket index from a TxKey. Uses bytes 0–7 of txid.
fn bucket_index(key: &TxKey, mask: usize) -> usize {
    let h = u64::from_le_bytes(key.txid[0..8].try_into().unwrap());
    (h as usize) & mask
}

/// Compute the fingerprint from a TxKey. Uses bytes 8–16 of txid.
fn fingerprint(key: &TxKey) -> u64 {
    u64::from_le_bytes(key.txid[8..16].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// mmap helpers
// ---------------------------------------------------------------------------

/// Allocate a zeroed mmap region for `capacity` buckets.
///
/// On Linux, attempts 2 MB hugepages first. Falls back to regular pages.
/// On macOS, uses regular pages directly.
///
/// Returns `(pointer, mmap_byte_length, hugepage_used)`.
fn alloc_mmap_buckets(
    capacity: usize,
) -> Result<(*mut Bucket, usize, bool)> {
    let byte_len = capacity
        .checked_mul(std::mem::size_of::<Bucket>())
        .ok_or_else(|| HashTableError::AllocFailed("capacity overflow".into()))?;

    if byte_len == 0 {
        return Err(HashTableError::AllocFailed(
            "zero-size allocation".into(),
        ));
    }

    // On Linux, try hugepages first.
    #[cfg(target_os = "linux")]
    {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                byte_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB,
                -1,
                0,
            )
        };
        if ptr != libc::MAP_FAILED {
            return Ok((ptr.cast::<Bucket>(), byte_len, true));
        }
        // Hugepage allocation failed — fall through to regular mmap.
    }

    // Regular mmap (works on macOS and Linux).
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            byte_len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        return Err(HashTableError::AllocFailed(format!(
            "mmap({byte_len} bytes) failed: {}",
            std::io::Error::last_os_error()
        )));
    }

    // MAP_ANONYMOUS returns zeroed memory, so all buckets start as BUCKET_EMPTY.
    Ok((ptr.cast::<Bucket>(), byte_len, false))
}

/// Release a previously mmap'd region.
///
/// # Safety
///
/// `ptr` must point to a region of `byte_len` bytes allocated by `alloc_mmap_buckets`.
unsafe fn dealloc_mmap_buckets(ptr: *mut Bucket, byte_len: usize) {
    if byte_len > 0 && !ptr.is_null() {
        unsafe { libc::munmap(ptr.cast(), byte_len) };
    }
}

// ---------------------------------------------------------------------------
// HashTable
// ---------------------------------------------------------------------------

/// Robin Hood open-addressing hash table backed by an mmap'd flat array.
///
/// Capacity is always a power of two for fast modulo via bitmask.
///
/// # Memory
///
/// Attempts to use 2 MB hugepages on Linux (`MAP_HUGETLB`). Falls back to
/// regular pages on macOS or when hugepages are unavailable. On drop, the
/// mmap region is released via `munmap`.
pub struct HashTable {
    /// Pointer to the mmap'd bucket array.
    ptr: *mut Bucket,
    /// Total number of buckets (always a power of 2).
    capacity: usize,
    /// Number of occupied entries.
    count: usize,
    /// `capacity - 1`, for fast modulo via bitmask.
    mask: usize,
    /// Size of the mmap'd region in bytes.
    mmap_len: usize,
    /// Whether 2 MB hugepages are backing this table.
    hugepage: bool,
    /// Maximum probe distance observed so far.
    max_probe: usize,
}

impl std::fmt::Debug for HashTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashTable")
            .field("capacity", &self.capacity)
            .field("count", &self.count)
            .field("hugepage", &self.hugepage)
            .field("max_probe", &self.max_probe)
            .finish()
    }
}

// Safety: HashTable owns its mmap allocation exclusively. The contents are
// plain Copy data with no interior mutability or thread-local state.
unsafe impl Send for HashTable {}
unsafe impl Sync for HashTable {}

impl HashTable {
    /// Create a new hash table with at least `initial_capacity` buckets.
    ///
    /// The capacity is rounded up to the next power of two.
    /// Memory is allocated via `mmap` with `MAP_ANONYMOUS | MAP_PRIVATE`.
    /// On Linux, 2 MB hugepages are attempted first.
    pub fn new(initial_capacity: usize) -> Result<Self> {
        let capacity = initial_capacity.next_power_of_two().max(16);
        let (ptr, mmap_len, hugepage) = alloc_mmap_buckets(capacity)?;
        Ok(Self {
            ptr,
            capacity,
            count: 0,
            mask: capacity - 1,
            mmap_len,
            hugepage,
            max_probe: 0,
        })
    }

    /// Safe accessor for bucket at `idx` (immutable).
    #[inline]
    fn bucket(&self, idx: usize) -> &Bucket {
        debug_assert!(idx < self.capacity);
        // Safety: idx is within the mmap'd region (checked by caller or mask).
        unsafe { &*self.ptr.add(idx) }
    }

    /// Safe accessor for bucket at `idx` (mutable).
    #[inline]
    fn bucket_mut(&mut self, idx: usize) -> &mut Bucket {
        debug_assert!(idx < self.capacity);
        // Safety: idx is within the mmap'd region, &mut self guarantees exclusivity.
        unsafe { &mut *self.ptr.add(idx) }
    }

    /// Look up a transaction by key, returning the entry by value. O(1) expected.
    pub fn get_entry(&self, key: &TxKey) -> Option<TxIndexEntry> {
        let fp = fingerprint(key);
        let mut idx = bucket_index(key, self.mask);
        let mut dist: u16 = 0;

        loop {
            let bucket = self.bucket(idx);
            if bucket.is_empty() {
                return None;
            }
            if bucket.is_occupied() {
                if dist > bucket.probe_distance {
                    // Robin Hood invariant: the key cannot be further.
                    return None;
                }
                if bucket.fingerprint == fp && bucket.txid == key.txid {
                    return Some(bucket.entry());
                }
            }
            idx = (idx + 1) & self.mask;
            dist += 1;
            if dist as usize >= self.capacity {
                return None;
            }
        }
    }

    /// Insert or update an entry. Returns the previous entry if the key existed.
    pub fn insert(
        &mut self,
        key: TxKey,
        entry: TxIndexEntry,
    ) -> Result<Option<TxIndexEntry>> {
        // Check for update of existing key first.
        let fp = fingerprint(&key);
        {
            let mut idx = bucket_index(&key, self.mask);
            let mut dist: u16 = 0;
            loop {
                let bucket = self.bucket(idx);
                if bucket.is_empty() {
                    break;
                }
                if bucket.is_occupied() {
                    if dist > bucket.probe_distance {
                        break;
                    }
                    if bucket.fingerprint == fp && bucket.txid == key.txid {
                        let old = bucket.entry();
                        self.bucket_mut(idx).set_entry(&key, &entry, dist);
                        return Ok(Some(old));
                    }
                }
                idx = (idx + 1) & self.mask;
                dist += 1;
                if dist as usize >= self.capacity {
                    break;
                }
            }
        }

        // New insert — Robin Hood insertion.
        let mut idx = bucket_index(&key, self.mask);
        let mut dist: u16 = 0;
        let mut cur_key = key;
        let mut cur_entry = entry;

        loop {
            let bucket = self.bucket(idx);
            if bucket.is_empty() {
                self.bucket_mut(idx)
                    .set_entry(&cur_key, &cur_entry, dist);
                self.count += 1;
                if dist as usize > self.max_probe {
                    self.max_probe = dist as usize;
                }
                return Ok(None);
            }

            // Robin Hood: if our displacement is greater, swap.
            if bucket.is_occupied() && dist > bucket.probe_distance {
                let displaced_key = TxKey {
                    txid: bucket.txid,
                };
                let displaced_entry = bucket.entry();
                let displaced_dist = bucket.probe_distance;

                self.bucket_mut(idx)
                    .set_entry(&cur_key, &cur_entry, dist);

                cur_key = displaced_key;
                cur_entry = displaced_entry;
                dist = displaced_dist;
            }

            idx = (idx + 1) & self.mask;
            dist += 1;

            if dist as usize >= self.capacity {
                return Err(HashTableError::Full {
                    count: self.count,
                    capacity: self.capacity,
                });
            }
        }
    }

    /// Remove an entry by key. Returns the removed entry if it existed.
    ///
    /// Uses backward-shift deletion for better probe-chain performance.
    pub fn remove(&mut self, key: &TxKey) -> Option<TxIndexEntry> {
        let fp = fingerprint(key);
        let mut idx = bucket_index(key, self.mask);
        let mut dist: u16 = 0;

        // Find the entry.
        loop {
            let bucket = self.bucket(idx);
            if bucket.is_empty() {
                return None;
            }
            if bucket.is_occupied() {
                if dist > bucket.probe_distance {
                    return None;
                }
                if bucket.fingerprint == fp && bucket.txid == key.txid {
                    break; // Found at idx
                }
            }
            idx = (idx + 1) & self.mask;
            dist += 1;
            if dist as usize >= self.capacity {
                return None;
            }
        }

        let removed = self.bucket(idx).entry();
        self.count -= 1;

        // Backward-shift: move subsequent entries back to fill the gap.
        let mut empty_idx = idx;
        loop {
            let next_idx = (empty_idx + 1) & self.mask;
            let next = self.bucket(next_idx);
            if next.is_empty()
                || (next.is_occupied() && next.probe_distance == 0)
            {
                break;
            }
            // Shift this entry back.
            let shifted = *self.bucket(next_idx);
            let b = self.bucket_mut(empty_idx);
            *b = shifted;
            b.probe_distance -= 1;
            empty_idx = next_idx;
        }
        *self.bucket_mut(empty_idx) = Bucket::empty();

        Some(removed)
    }

    /// Number of occupied entries.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Total bucket capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Load factor: count / capacity.
    pub fn load_factor(&self) -> f64 {
        self.count as f64 / self.capacity as f64
    }

    /// Maximum probe distance observed.
    pub fn max_probe_distance(&self) -> usize {
        self.max_probe
    }

    /// Approximate memory usage in bytes (mmap region size).
    pub fn memory_bytes(&self) -> usize {
        self.mmap_len
    }

    /// Whether hugepages are backing this table.
    pub fn hugepage_enabled(&self) -> bool {
        self.hugepage
    }

    /// Resize the table to at least `new_capacity` buckets.
    ///
    /// Allocates a new mmap region, rehashes all occupied entries, and
    /// releases the old mmap region.
    pub fn resize(&mut self, new_capacity: usize) -> Result<()> {
        let new_cap = new_capacity.next_power_of_two().max(16);
        let mut new_table = HashTable::new(new_cap)?;

        for i in 0..self.capacity {
            let bucket = self.bucket(i);
            if bucket.is_occupied() {
                let key = TxKey { txid: bucket.txid };
                new_table.insert(key, bucket.entry())?;
            }
        }

        // Replace self with new_table.
        // The old self's Drop will run (munmapping the old region).
        *self = new_table;
        Ok(())
    }

    /// Iterate over all occupied `(TxKey, TxIndexEntry)` pairs.
    pub fn iter(&self) -> HashTableIter<'_> {
        HashTableIter {
            table: self,
            pos: 0,
        }
    }
}

impl Drop for HashTable {
    fn drop(&mut self) {
        if self.mmap_len > 0 {
            // Safety: ptr was allocated by alloc_mmap_buckets with mmap_len bytes.
            unsafe { dealloc_mmap_buckets(self.ptr, self.mmap_len) };
        }
    }
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

/// Iterator over occupied hash table entries.
pub struct HashTableIter<'a> {
    table: &'a HashTable,
    pos: usize,
}

impl<'a> Iterator for HashTableIter<'a> {
    type Item = (TxKey, TxIndexEntry);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.table.capacity {
            let bucket = self.table.bucket(self.pos);
            self.pos += 1;
            if bucket.is_occupied() {
                return Some((
                    TxKey { txid: bucket.txid },
                    bucket.entry(),
                ));
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(n: u64) -> TxKey {
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&n.to_le_bytes());
        // Put something in bytes 8-16 for fingerprint variation
        txid[8..16].copy_from_slice(
            &(n.wrapping_mul(0x9E3779B97F4A7C15)).to_le_bytes(),
        );
        TxKey { txid }
    }

    fn make_entry(offset: u64) -> TxIndexEntry {
        TxIndexEntry {
            device_id: 0,
            record_offset: offset,
            utxo_count: 10,
            cold_offset: 0,
            cold_size: 0,
            flags: 0,
        }
    }

    /// Create a key that hashes to a specific bucket (for collision testing).
    fn make_colliding_key(
        bucket_target: usize,
        sequence: u64,
        mask: usize,
    ) -> TxKey {
        let mut txid = [0u8; 32];
        // Set bytes 0-7 so bucket_index == bucket_target
        let base = (bucket_target & mask) as u64;
        txid[0..8].copy_from_slice(&base.to_le_bytes());
        // Set bytes 8-16 uniquely per sequence for different fingerprints
        txid[8..16].copy_from_slice(&sequence.to_le_bytes());
        // Set bytes 16+ for additional uniqueness
        txid[16..24]
            .copy_from_slice(&(sequence.wrapping_mul(7)).to_le_bytes());
        TxKey { txid }
    }

    // -- Correctness tests --

    #[test]
    fn insert_one_get() {
        let mut t = HashTable::new(16).unwrap();
        let key = make_key(1);
        let entry = make_entry(4096);
        t.insert(key, entry).unwrap();

        let got = t.get_entry(&key).unwrap();
        assert_eq!(got, entry);
    }

    #[test]
    fn insert_100_get_each() {
        let mut t = HashTable::new(256).unwrap();
        let items: Vec<_> = (0..100)
            .map(|i| (make_key(i), make_entry(i * 4096)))
            .collect();

        for (k, e) in &items {
            t.insert(*k, *e).unwrap();
        }

        for (k, e) in &items {
            let got = t.get_entry(k).expect("key should exist");
            assert_eq!(got, *e);
        }
    }

    #[test]
    fn get_nonexistent() {
        let t = HashTable::new(16).unwrap();
        assert!(t.get_entry(&make_key(42)).is_none());
    }

    #[test]
    fn insert_same_key_twice() {
        let mut t = HashTable::new(16).unwrap();
        let key = make_key(1);
        let e1 = make_entry(1000);
        let e2 = make_entry(2000);

        let prev = t.insert(key, e1).unwrap();
        assert!(prev.is_none());

        let prev = t.insert(key, e2).unwrap();
        assert_eq!(prev, Some(e1));

        let got = t.get_entry(&key).unwrap();
        assert_eq!(got, e2);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn remove_existing() {
        let mut t = HashTable::new(16).unwrap();
        let key = make_key(1);
        let entry = make_entry(4096);
        t.insert(key, entry).unwrap();

        let removed = t.remove(&key);
        assert_eq!(removed, Some(entry));
        assert!(t.get_entry(&key).is_none());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn remove_nonexistent() {
        let mut t = HashTable::new(16).unwrap();
        assert!(t.remove(&make_key(99)).is_none());
    }

    #[test]
    fn insert_remove_insert() {
        let mut t = HashTable::new(16).unwrap();
        let key = make_key(1);
        let e1 = make_entry(1000);
        let e2 = make_entry(2000);

        t.insert(key, e1).unwrap();
        t.remove(&key);
        t.insert(key, e2).unwrap();

        let got = t.get_entry(&key).unwrap();
        assert_eq!(got, e2);
    }

    #[test]
    fn different_fingerprints_same_bucket() {
        let mut t = HashTable::new(1024).unwrap();
        // Two keys that map to the same bucket but have different fingerprints
        let k1 = make_colliding_key(42, 1, 1023);
        let k2 = make_colliding_key(42, 2, 1023);

        assert_eq!(
            bucket_index(&k1, 1023),
            bucket_index(&k2, 1023),
            "keys should collide"
        );
        assert_ne!(fingerprint(&k1), fingerprint(&k2));

        let e1 = make_entry(1000);
        let e2 = make_entry(2000);
        t.insert(k1, e1).unwrap();
        t.insert(k2, e2).unwrap();

        assert_eq!(t.get_entry(&k1), Some(e1));
        assert_eq!(t.get_entry(&k2), Some(e2));
        assert_eq!(t.len(), 2);
    }

    // -- Capacity and resize tests --

    #[test]
    fn fill_to_100_percent() {
        let mut t = HashTable::new(16).unwrap();
        assert_eq!(t.capacity(), 16);
        for i in 0..16u64 {
            t.insert(make_key(i), make_entry(i * 4096)).unwrap();
        }
        assert_eq!(t.len(), 16);
        for i in 0..16u64 {
            assert!(t.get_entry(&make_key(i)).is_some());
        }
    }

    #[test]
    fn resize_preserves_entries() {
        let mut t = HashTable::new(16).unwrap();
        for i in 0..12u64 {
            t.insert(make_key(i), make_entry(i * 100)).unwrap();
        }
        let old_cap = t.capacity();
        t.resize(old_cap * 2).unwrap();

        assert!(t.capacity() >= old_cap * 2);
        for i in 0..12u64 {
            let e = t
                .get_entry(&make_key(i))
                .expect("entry should survive resize");
            assert_eq!(e.record_offset, i * 100);
        }
    }

    #[test]
    fn resize_doubles_capacity() {
        let mut t = HashTable::new(16).unwrap();
        for i in 0..12u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        t.resize(32).unwrap();
        assert_eq!(t.capacity(), 32);
    }

    #[test]
    fn new_inserts_after_resize() {
        let mut t = HashTable::new(16).unwrap();
        for i in 0..12u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        t.resize(32).unwrap();
        t.insert(make_key(100), make_entry(100)).unwrap();
        assert_eq!(
            t.get_entry(&make_key(100)).unwrap().record_offset,
            100
        );
    }

    #[test]
    fn fill_70_percent() {
        let mut t = HashTable::new(1024).unwrap();
        for i in 0..716u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        for i in 0..716u64 {
            assert!(
                t.get_entry(&make_key(i)).is_some(),
                "missing key {i}"
            );
        }
    }

    #[test]
    fn fill_90_percent() {
        let mut t = HashTable::new(1024).unwrap();
        for i in 0..921u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        for i in 0..921u64 {
            assert!(
                t.get_entry(&make_key(i)).is_some(),
                "missing key {i}"
            );
        }
    }

    // -- Robin Hood invariant tests --

    #[test]
    fn collisions_all_found() {
        let mut t = HashTable::new(64).unwrap();
        let mask = t.capacity() - 1;
        let keys: Vec<_> = (0..10)
            .map(|i| make_colliding_key(0, i, mask))
            .collect();

        for (i, k) in keys.iter().enumerate() {
            t.insert(*k, make_entry(i as u64 * 100)).unwrap();
        }

        for (i, k) in keys.iter().enumerate() {
            let e =
                t.get_entry(k).expect("colliding key should be found");
            assert_eq!(e.record_offset, i as u64 * 100);
        }
    }

    #[test]
    fn adversarial_1000_all_same_bucket() {
        let mut t = HashTable::new(2048).unwrap();
        let mask = t.capacity() - 1;
        let keys: Vec<_> = (0..1000)
            .map(|i| make_colliding_key(0, i, mask))
            .collect();

        for (i, k) in keys.iter().enumerate() {
            t.insert(*k, make_entry(i as u64)).unwrap();
        }

        for (i, k) in keys.iter().enumerate() {
            let e = t.get_entry(k).unwrap_or_else(|| {
                panic!("adversarial key {i} not found")
            });
            assert_eq!(e.record_offset, i as u64);
        }
    }

    #[test]
    fn max_probe_distance_reasonable() {
        let mut t = HashTable::new(2048).unwrap();
        for i in 0..1800u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        // At ~88% load, max probe should be bounded.
        // Robin Hood at <90% load typically has max probe < 50.
        assert!(
            t.max_probe_distance() < 100,
            "probe distance {} too high",
            t.max_probe_distance()
        );
    }

    // -- Tombstone / delete tests --

    #[test]
    fn delete_middle_of_chain() {
        let mut t = HashTable::new(64).unwrap();
        let mask = t.capacity() - 1;
        let ka = make_colliding_key(5, 1, mask);
        let kb = make_colliding_key(5, 2, mask);
        let kc = make_colliding_key(5, 3, mask);

        t.insert(ka, make_entry(1)).unwrap();
        t.insert(kb, make_entry(2)).unwrap();
        t.insert(kc, make_entry(3)).unwrap();

        t.remove(&kb);

        assert_eq!(t.get_entry(&ka).unwrap().record_offset, 1);
        assert!(t.get_entry(&kb).is_none());
        assert_eq!(t.get_entry(&kc).unwrap().record_offset, 3);
    }

    #[test]
    fn delete_head_of_chain() {
        let mut t = HashTable::new(64).unwrap();
        let mask = t.capacity() - 1;
        let ka = make_colliding_key(5, 1, mask);
        let kb = make_colliding_key(5, 2, mask);
        let kc = make_colliding_key(5, 3, mask);

        t.insert(ka, make_entry(1)).unwrap();
        t.insert(kb, make_entry(2)).unwrap();
        t.insert(kc, make_entry(3)).unwrap();

        t.remove(&ka);

        assert!(t.get_entry(&ka).is_none());
        assert_eq!(t.get_entry(&kb).unwrap().record_offset, 2);
        assert_eq!(t.get_entry(&kc).unwrap().record_offset, 3);
    }

    #[test]
    fn insert_delete_reinsert() {
        let mut t = HashTable::new(16).unwrap();
        let key = make_key(42);
        t.insert(key, make_entry(100)).unwrap();
        t.remove(&key);
        t.insert(key, make_entry(200)).unwrap();
        assert_eq!(t.get_entry(&key).unwrap().record_offset, 200);
    }

    #[test]
    fn many_insert_delete_cycles() {
        let mut t = HashTable::new(8192).unwrap();
        for cycle in 0..50u64 {
            for i in 0..100u64 {
                let n = cycle * 1000 + i;
                t.insert(make_key(n), make_entry(n)).unwrap();
            }
            for i in 0..50u64 {
                let n = cycle * 1000 + i;
                t.remove(&make_key(n));
            }
        }
        // Table should still function correctly
        let key = make_key(999_999);
        t.insert(key, make_entry(42)).unwrap();
        assert_eq!(t.get_entry(&key).unwrap().record_offset, 42);
    }

    // -- Memory mapping tests --

    #[test]
    fn hashtable_uses_mmap() {
        // Verify that the hash table's memory is allocated via mmap
        // by checking that memory_bytes() matches expected bucket * capacity.
        let t = HashTable::new(1024).unwrap();
        let expected = t.capacity() * BUCKET_SIZE;
        assert_eq!(t.memory_bytes(), expected);
        // The pointer should be page-aligned (mmap guarantees this).
        assert_eq!(t.ptr as usize % 4096, 0);
    }

    #[test]
    fn hugepage_fallback_works() {
        // Even if hugepages aren't available (typical on macOS and unprivileged
        // Linux), the table should still be created successfully with regular pages.
        let t = HashTable::new(4096).unwrap();
        assert_eq!(t.capacity(), 4096);
        assert_eq!(t.len(), 0);
        // On macOS, hugepage should always be false.
        #[cfg(target_os = "macos")]
        assert!(!t.hugepage_enabled());
    }

    #[test]
    fn drop_releases_memory() {
        // Allocate a reasonably sized table, then drop it.
        // We can't directly verify munmap, but we can verify the table
        // was created and dropped without error or leak.
        let t = HashTable::new(1 << 16).unwrap(); // 64K buckets
        let bytes = t.memory_bytes();
        assert!(bytes > 0);
        drop(t);
        // If munmap failed, the process would continue but with a memory leak.
        // Under valgrind or miri this would be caught.
    }

    // -- Scale tests --

    #[test]
    fn one_million_entries() {
        let mut t = HashTable::new(1 << 21).unwrap(); // ~2M buckets
        for i in 0..1_000_000u64 {
            t.insert(make_key(i), make_entry(i * 8)).unwrap();
        }
        assert_eq!(t.len(), 1_000_000);

        for i in 0..1_000_000u64 {
            let e = t.get_entry(&make_key(i)).unwrap_or_else(|| {
                panic!("key {i} not found at 1M scale")
            });
            assert_eq!(e.record_offset, i * 8);
        }
    }

    #[test]
    fn memory_usage_proportional() {
        let t = HashTable::new(1 << 20).unwrap(); // 1M buckets
        let expected = (1 << 20) * BUCKET_SIZE;
        let actual = t.memory_bytes();
        // Should be exactly proportional
        assert_eq!(actual, expected);
    }

    #[test]
    fn ten_million_entries() {
        let mut t = HashTable::new(1 << 24).unwrap(); // 16M buckets
        for i in 0..10_000_000u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        assert_eq!(t.len(), 10_000_000);

        // Spot check a sample of entries
        for i in (0..10_000_000u64).step_by(1000) {
            let e = t.get_entry(&make_key(i)).unwrap_or_else(|| {
                panic!("key {i} not found at 10M scale")
            });
            assert_eq!(e.record_offset, i);
        }
    }

    // -- Performance benchmarks (measured, informational) --

    #[test]
    fn bench_lookup_1m() {
        let mut t = HashTable::new(1 << 21).unwrap();
        for i in 0..1_000_000u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }

        let start = std::time::Instant::now();
        let iters = 1_000_000;
        for i in 0..iters {
            let _ = t.get_entry(&make_key(i));
        }
        let elapsed = start.elapsed();
        let ns_per_lookup = elapsed.as_nanos() / iters as u128;
        eprintln!(
            "[bench] 1M entries: avg lookup = {ns_per_lookup} ns ({} lookups/sec)",
            1_000_000_000u128 / ns_per_lookup.max(1)
        );
    }

    #[test]
    fn bench_insert_throughput() {
        let start = std::time::Instant::now();
        let count = 1_000_000u64;
        let mut t = HashTable::new((count as usize).next_power_of_two() * 2).unwrap();
        for i in 0..count {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }
        let elapsed = start.elapsed();
        let ops_per_sec = count as f64 / elapsed.as_secs_f64();
        eprintln!(
            "[bench] insert throughput: {ops_per_sec:.0} ops/sec ({count} inserts in {elapsed:?})"
        );
    }

    // -- Iterator tests --

    #[test]
    fn iter_all_entries() {
        let mut t = HashTable::new(64).unwrap();
        for i in 0..20u64 {
            t.insert(make_key(i), make_entry(i)).unwrap();
        }

        let mut collected: Vec<_> = t.iter().collect();
        collected.sort_by_key(|(_, e)| e.record_offset);
        assert_eq!(collected.len(), 20);
        for (i, (_, e)) in collected.iter().enumerate() {
            assert_eq!(e.record_offset, i as u64);
        }
    }
}
