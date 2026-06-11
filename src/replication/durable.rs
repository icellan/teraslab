//! Persistent replication ACK tracking.
//!
//! Tracks per-replica ACKed positions durably to disk so that after a
//! master restart, the master knows where each replica left off and can
//! stream the missing redo entries instead of requiring a full resync.
//!
//! R-D1/D-3 sequence-space note: the [`AckTracker`] records positions in
//! the master's **redo-log space** (the highest redo sequence whose ops
//! were covered by a batch this replica ACKed) — this is the cursor that
//! catch-up and lag monitoring need. The **dense per-replica stream
//! sequence** used for wire-level ordering/dedup is NOT persisted here;
//! the master re-adopts it from the receiver's authoritative applied
//! watermark via an empty-batch probe on first contact (see
//! `server::dispatch::send_replica_ops_to`), which keeps both sides
//! consistent across restarts by construction.

use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Instant;

use parking_lot::Mutex;

fn ensure_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn durable_tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

#[cfg(unix)]
fn fsync_parent_dir(path: &Path) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let dir = std::fs::File::open(parent)?;
    dir.sync_all()
}

#[cfg(not(unix))]
fn fsync_parent_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

fn write_durable_file(path: &Path, data: &[u8]) -> std::io::Result<()> {
    ensure_parent_dir(path)?;
    let tmp = durable_tmp_path(path);
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(data)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    fsync_parent_dir(path)?;
    Ok(())
}

/// Manages persistent per-replica ACK tracking.
///
/// The `last_acked` map records, per replica, the highest **redo-log**
/// sequence whose ops were covered by a batch that replica durably
/// acknowledged (conservative: a replica may additionally hold later
/// ranges it ACKed out of redo order — re-replaying those during
/// catch-up is idempotent). This is written to disk periodically (at
/// most once per second) to amortize I/O while ensuring reasonable
/// recovery bounds.
pub struct AckTracker {
    path: PathBuf,
    inner: Mutex<AckTrackerInner>,
}

struct AckTrackerInner {
    /// Per-replica last-ACKed replication sequence.
    last_acked: HashMap<SocketAddr, u64>,
    /// Whether the in-memory state has changed since the last flush.
    dirty: bool,
    /// Timestamp of the last flush to disk.
    last_flush: Instant,
    /// R-067 (D-03): number of ACK record_ack calls accumulated since
    /// the last flush. Reset to 0 by `flush_locked`. Allows the
    /// flush trigger to fire on EITHER the time threshold OR the
    /// burst-count threshold, so a master that takes ~1000 ACKs in
    /// 100 ms before crashing does not lose every one of them.
    dirty_count: u32,
}

/// Minimum interval between flushes to disk (1 second).
const FLUSH_INTERVAL_MS: u128 = 1000;

/// R-067 (D-03): maximum number of ACK records that may accumulate
/// in the dirty buffer before a flush is forced regardless of the
/// time-based threshold. Pre-fix the tracker only flushed on the
/// 1-second timer, so a master crashing 999 ms after the last
/// flush could lose ~1000+ ACKs at peak throughput. 100 keeps the
/// per-flush amortization useful while bounding the at-risk
/// window to a small number of operations.
const FLUSH_DIRTY_COUNT_THRESHOLD: u32 = 100;

/// Minimum interval between deferred replication-intent commit flushes.
const INTENT_COMMIT_FLUSH_INTERVAL_MS: u128 = 1000;

/// Maximum number of committed intent removals that may stay dirty before
/// forcing a disk flush. `begin()` remains immediately durable; deferred
/// commit flushes can only leave stale ranges that recovery replays
/// idempotently.
///
/// F-G7-004 contract (revised by R-D1/D-3): the deferred `commit()`
/// durability is safe because recovery replay of a stale range is
/// absorbed at the **op level** — the receiver's per-record generation
/// guard plus the create-payload dedup make re-application a no-op.
/// (Pre-fix this comment claimed the receiver's sequence-dedup tracker
/// as the safety net; that no longer holds, since recovery re-sends are
/// assigned fresh per-replica stream labels and are re-applied, not
/// sequence-skipped.) If a future change weakens op-level idempotency,
/// this constant must be set to 1 to make every commit immediately
/// durable so stale ranges never reach recovery replay.
const INTENT_COMMIT_FLUSH_DIRTY_COUNT_THRESHOLD: u32 = 100;

impl AckTracker {
    /// Create a new tracker with the given persistence path.
    ///
    /// If the file exists, loads the persisted state. Otherwise starts empty.
    pub fn new(path: PathBuf) -> Self {
        let last_acked = Self::load_from_disk(&path).unwrap_or_default();
        Self {
            path,
            inner: Mutex::new(AckTrackerInner {
                last_acked,
                dirty: false,
                last_flush: Instant::now(),
                dirty_count: 0,
            }),
        }
    }

    /// Record a successful ACK from a replica. Flushes to disk on
    /// EITHER the 1-second time threshold OR an accumulated burst of
    /// `FLUSH_DIRTY_COUNT_THRESHOLD` ACKs since the last flush
    /// (R-067 / D-03). The 1-second window alone could lose a
    /// thousand-ACK burst on a master that crashes ~999 ms after the
    /// previous flush; the burst-count threshold caps the at-risk
    /// window.
    pub fn record_ack(&self, addr: SocketAddr, through_sequence: u64) {
        let mut inner = self.inner.lock();
        let entry = inner.last_acked.entry(addr).or_insert(0);
        if through_sequence > *entry {
            *entry = through_sequence;
            inner.dirty = true;
            inner.dirty_count = inner.dirty_count.saturating_add(1);
        }

        // Amortize: flush when either threshold is met. Time-based
        // flush bounds latency; count-based flush bounds the number
        // of at-risk ACKs in a burst.
        let time_due = inner.last_flush.elapsed().as_millis() >= FLUSH_INTERVAL_MS;
        let count_due = inner.dirty_count >= FLUSH_DIRTY_COUNT_THRESHOLD;
        if inner.dirty && (time_due || count_due) {
            self.flush_locked(&mut inner);
        }
    }

    /// Get the last-ACKed sequence for a replica, or 0 if unknown.
    pub fn last_acked(&self, addr: &SocketAddr) -> u64 {
        let inner = self.inner.lock();
        inner.last_acked.get(addr).copied().unwrap_or(0)
    }

    /// Get all tracked replicas and their ACK sequences.
    pub fn all_acked(&self) -> HashMap<SocketAddr, u64> {
        let inner = self.inner.lock();
        inner.last_acked.clone()
    }

    /// Force a flush of any dirty state to disk.
    pub fn flush(&self) {
        let mut inner = self.inner.lock();
        if inner.dirty {
            self.flush_locked(&mut inner);
        }
    }

    fn flush_locked(&self, inner: &mut AckTrackerInner) {
        if let Err(e) = Self::write_to_disk(&self.path, &inner.last_acked) {
            // F-G7-008: surface the failure on the observability
            // pipeline. The on-disk state stays behind in-memory
            // truth until the next successful flush; without a
            // counter, operators would have to tail logs to notice.
            if let Some(m) = crate::metrics::replication_metrics() {
                m.ack_tracker_flush_failures.inc();
            }
            tracing::warn!(err = %e, "ack_tracker: flush failed");
            return;
        }
        inner.dirty = false;
        inner.dirty_count = 0;
        inner.last_flush = Instant::now();
    }

    /// Serialize and write the ACK state to disk.
    ///
    /// Format: `[entry_count:4 LE]([addr_len:2 LE][addr_bytes][last_acked:8 LE])*`
    fn write_to_disk(path: &Path, state: &HashMap<SocketAddr, u64>) -> std::io::Result<()> {
        ensure_parent_dir(path)?;
        let mut buf = Vec::with_capacity(4 + state.len() * 30);
        buf.extend_from_slice(&(state.len() as u32).to_le_bytes());
        for (addr, &seq) in state {
            let addr_str = addr.to_string();
            let addr_bytes = addr_str.as_bytes();
            buf.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(addr_bytes);
            buf.extend_from_slice(&seq.to_le_bytes());
        }
        write_durable_file(path, &buf)
    }

    /// Load ACK state from disk.
    fn load_from_disk(path: &Path) -> std::io::Result<HashMap<SocketAddr, u64>> {
        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(HashMap::new());
            }
            Err(e) => return Err(e),
        };

        if data.len() < 4 {
            return Ok(HashMap::new());
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut result = HashMap::with_capacity(count);
        let mut pos = 4;

        for _ in 0..count {
            if pos + 2 > data.len() {
                break;
            }
            let addr_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if pos + addr_len + 8 > data.len() {
                break;
            }
            let addr_str = std::str::from_utf8(&data[pos..pos + addr_len]).unwrap_or("");
            pos += addr_len;
            let seq = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                result.insert(addr, seq);
            }
        }

        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Master-side pending replication intent tracker
// ---------------------------------------------------------------------------

/// A durable redo sequence range that has been applied locally but has not
/// yet been proven replicated to the required holders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicationIntentRange {
    pub first_sequence: u64,
    pub last_sequence: u64,
}

/// Errors emitted by [`ReplicationIntentTracker`] persistence operations.
#[derive(thiserror::Error, Debug)]
pub enum ReplicationIntentError {
    #[error("replication intent tracker io: {0}")]
    Io(#[from] std::io::Error),
    #[error("replication intent tracker state corrupt: {0}")]
    Corrupt(String),
}

/// Persistent master-side journal of pending replication ranges.
///
/// The dispatcher records a range before attempting replica fan-out and removes
/// it only after the configured ACK policy is satisfied (or after a failed
/// client mutation has been durably compensated). On restart, any range left in
/// this file must be replicated to current holders before the node serves.
#[derive(Debug)]
pub struct ReplicationIntentTracker {
    path: PathBuf,
    inner: Mutex<ReplicationIntentInner>,
}

#[derive(Debug)]
struct ReplicationIntentInner {
    pending: BTreeSet<ReplicationIntentRange>,
    commit_dirty: bool,
    last_flush: Instant,
    dirty_commit_count: u32,
}

impl ReplicationIntentTracker {
    pub fn load(path: PathBuf) -> std::result::Result<Self, ReplicationIntentError> {
        ensure_parent_dir(&path).map_err(ReplicationIntentError::Io)?;
        let pending = Self::read_from_disk(&path)?;
        Ok(Self {
            path,
            inner: Mutex::new(ReplicationIntentInner {
                pending,
                commit_dirty: false,
                last_flush: Instant::now(),
                dirty_commit_count: 0,
            }),
        })
    }

    pub fn in_memory() -> Self {
        Self {
            path: PathBuf::new(),
            inner: Mutex::new(ReplicationIntentInner {
                pending: BTreeSet::new(),
                commit_dirty: false,
                last_flush: Instant::now(),
                dirty_commit_count: 0,
            }),
        }
    }

    pub fn begin(
        &self,
        first_sequence: u64,
        last_sequence: u64,
    ) -> std::result::Result<(), ReplicationIntentError> {
        if first_sequence == 0 || last_sequence < first_sequence {
            return Ok(());
        }
        let mut inner = self.inner.lock();
        let changed = inner.pending.insert(ReplicationIntentRange {
            first_sequence,
            last_sequence,
        });
        if changed {
            self.write_locked(&inner.pending)?;
            inner.commit_dirty = false;
            inner.dirty_commit_count = 0;
            inner.last_flush = Instant::now();
        }
        Ok(())
    }

    pub fn commit(
        &self,
        first_sequence: u64,
        last_sequence: u64,
    ) -> std::result::Result<(), ReplicationIntentError> {
        if first_sequence == 0 || last_sequence < first_sequence {
            return Ok(());
        }
        let mut inner = self.inner.lock();
        let changed = inner.pending.remove(&ReplicationIntentRange {
            first_sequence,
            last_sequence,
        });
        if changed {
            if self.path.as_os_str().is_empty() {
                return Ok(());
            }
            inner.commit_dirty = true;
            inner.dirty_commit_count = inner.dirty_commit_count.saturating_add(1);
            let time_due =
                inner.last_flush.elapsed().as_millis() >= INTENT_COMMIT_FLUSH_INTERVAL_MS;
            let count_due = inner.dirty_commit_count >= INTENT_COMMIT_FLUSH_DIRTY_COUNT_THRESHOLD;
            if time_due || count_due {
                self.flush_locked(&mut inner)?;
            }
        }
        Ok(())
    }

    pub fn pending(&self) -> Vec<ReplicationIntentRange> {
        let inner = self.inner.lock();
        inner.pending.iter().copied().collect()
    }

    pub fn flush(&self) -> std::result::Result<(), ReplicationIntentError> {
        let mut inner = self.inner.lock();
        if inner.commit_dirty {
            self.flush_locked(&mut inner)?;
        }
        Ok(())
    }

    fn write_locked(
        &self,
        pending: &BTreeSet<ReplicationIntentRange>,
    ) -> std::result::Result<(), ReplicationIntentError> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }
        Self::write_to_disk(&self.path, pending)
    }

    fn flush_locked(
        &self,
        inner: &mut ReplicationIntentInner,
    ) -> std::result::Result<(), ReplicationIntentError> {
        self.write_locked(&inner.pending)?;
        inner.commit_dirty = false;
        inner.dirty_commit_count = 0;
        inner.last_flush = Instant::now();
        Ok(())
    }

    fn write_to_disk(
        path: &Path,
        pending: &BTreeSet<ReplicationIntentRange>,
    ) -> std::result::Result<(), ReplicationIntentError> {
        let mut buf = Vec::with_capacity(4 + pending.len() * 16);
        buf.extend_from_slice(&(pending.len() as u32).to_le_bytes());
        for range in pending {
            buf.extend_from_slice(&range.first_sequence.to_le_bytes());
            buf.extend_from_slice(&range.last_sequence.to_le_bytes());
        }
        write_durable_file(path, &buf).map_err(ReplicationIntentError::Io)
    }

    fn read_from_disk(
        path: &Path,
    ) -> std::result::Result<BTreeSet<ReplicationIntentRange>, ReplicationIntentError> {
        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(BTreeSet::new());
            }
            Err(e) => return Err(ReplicationIntentError::Io(e)),
        };
        if data.is_empty() {
            return Ok(BTreeSet::new());
        }
        if data.len() < 4 {
            return Err(ReplicationIntentError::Corrupt("truncated header".into()));
        }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let expected = 4 + count * 16;
        if data.len() < expected {
            return Err(ReplicationIntentError::Corrupt("truncated ranges".into()));
        }
        let mut pending = BTreeSet::new();
        let mut pos = 4;
        for _ in 0..count {
            let first_sequence = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let last_sequence = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            if first_sequence == 0 || last_sequence < first_sequence {
                return Err(ReplicationIntentError::Corrupt(format!(
                    "invalid range {first_sequence}..{last_sequence}",
                )));
            }
            pending.insert(ReplicationIntentRange {
                first_sequence,
                last_sequence,
            });
        }
        Ok(pending)
    }
}

// ---------------------------------------------------------------------------
// Replica-side applied-sequence tracker
// ---------------------------------------------------------------------------

/// Errors emitted by [`ReplicaAppliedTracker`] persistence operations.
#[derive(thiserror::Error, Debug)]
pub enum ReplicaAppliedError {
    /// I/O error when reading or writing the on-disk state file.
    #[error("replica applied tracker io: {0}")]
    Io(#[from] std::io::Error),
    /// On-disk state file failed structural validation.
    #[error("replica applied tracker state corrupt: {0}")]
    Corrupt(String),
}

/// Per-shard `(shard_or_stream_id, last_applied_seq)` journal used by
/// the replication receiver to guarantee batch-level idempotency.
///
/// The receiver consults this tracker before dispatching an incoming
/// batch: if the batch's first sequence is less-than-or-equal to
/// `get(stream)`, the batch has already been applied and is skipped.
/// On successful apply the tracker is updated and — subject to a
/// configurable batch / time budget — flushed to disk so that a
/// receiver restart resumes from the correct point.
///
/// The file format mirrors [`AckTracker`]:
/// `[entry_count:4 LE]([id_len:2 LE][id_bytes][last_applied:8 LE])*`
#[derive(Debug)]
pub struct ReplicaAppliedTracker {
    path: PathBuf,
    inner: Mutex<ReplicaAppliedInner>,
}

#[derive(Debug)]
struct ReplicaAppliedInner {
    /// Per-stream / per-shard highest applied sequence.
    last_applied: HashMap<String, u64>,
    /// Unflushed updates accumulated since the last `flush`.
    dirty: bool,
}

impl ReplicaAppliedTracker {
    /// Open (or create) a tracker backed by the given path.
    ///
    /// If the file exists but is malformed, returns
    /// [`ReplicaAppliedError::Corrupt`]. A missing file is NOT an
    /// error; the tracker starts empty.
    pub fn load(path: PathBuf) -> std::result::Result<Self, ReplicaAppliedError> {
        ensure_parent_dir(&path).map_err(ReplicaAppliedError::Io)?;
        let last_applied = Self::read_from_disk(&path)?;
        Ok(Self {
            path,
            inner: Mutex::new(ReplicaAppliedInner {
                last_applied,
                dirty: false,
            }),
        })
    }

    /// Construct a tracker without touching disk — used for tests and
    /// for receivers running without durable idempotency.
    pub fn in_memory() -> Self {
        Self {
            path: PathBuf::new(),
            inner: Mutex::new(ReplicaAppliedInner {
                last_applied: HashMap::new(),
                dirty: false,
            }),
        }
    }

    /// Highest sequence applied for `stream`. Returns `0` if the
    /// stream has no record yet.
    pub fn get(&self, stream: &str) -> u64 {
        let inner = self.inner.lock();
        inner.last_applied.get(stream).copied().unwrap_or(0)
    }

    /// Record that `stream` has durably applied through `seq`.
    ///
    /// Only advances; a lower `seq` is ignored so concurrent callers
    /// cannot rewind the journal. Marks the tracker dirty for the
    /// next [`flush`](Self::flush) call.
    pub fn set(&self, stream: &str, seq: u64) {
        let mut inner = self.inner.lock();
        let entry = inner.last_applied.entry(stream.to_string()).or_insert(0);
        if seq > *entry {
            *entry = seq;
            inner.dirty = true;
        }
    }

    /// Force the in-memory state to disk if it has been modified.
    ///
    /// Returns `Ok(())` when the file already reflects the state
    /// (either clean or the flush succeeded) and `Err` if writing the
    /// temp file or the rename failed. The tracker is left dirty if
    /// the flush failed so a later retry can persist the update.
    pub fn flush(&self) -> std::result::Result<(), ReplicaAppliedError> {
        let mut inner = self.inner.lock();
        if !inner.dirty {
            return Ok(());
        }
        if self.path.as_os_str().is_empty() {
            // Memory-only tracker: clearing the dirty flag is legal
            // because there is no backing file to keep in sync.
            inner.dirty = false;
            return Ok(());
        }
        Self::write_to_disk(&self.path, &inner.last_applied)?;
        inner.dirty = false;
        Ok(())
    }

    /// Snapshot of all tracked streams and their last-applied
    /// sequences — useful for diagnostics and tests.
    pub fn snapshot(&self) -> HashMap<String, u64> {
        let inner = self.inner.lock();
        inner.last_applied.clone()
    }

    /// Serialize the state map to disk atomically.
    fn write_to_disk(
        path: &Path,
        state: &HashMap<String, u64>,
    ) -> std::result::Result<(), ReplicaAppliedError> {
        let mut buf = Vec::with_capacity(4 + state.len() * 24);
        buf.extend_from_slice(&(state.len() as u32).to_le_bytes());
        for (id, &seq) in state {
            let bytes = id.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(bytes);
            buf.extend_from_slice(&seq.to_le_bytes());
        }
        write_durable_file(path, &buf).map_err(ReplicaAppliedError::Io)
    }

    /// Load and parse the state map from disk.
    fn read_from_disk(
        path: &Path,
    ) -> std::result::Result<HashMap<String, u64>, ReplicaAppliedError> {
        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(HashMap::new());
            }
            Err(e) => return Err(ReplicaAppliedError::Io(e)),
        };
        if data.is_empty() {
            return Ok(HashMap::new());
        }
        if data.len() < 4 {
            return Err(ReplicaAppliedError::Corrupt("truncated header".into()));
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4])) as usize;
        let mut result = HashMap::with_capacity(count);
        let mut pos = 4;
        for _ in 0..count {
            if pos + 2 > data.len() {
                return Err(ReplicaAppliedError::Corrupt(
                    "truncated entry length".into(),
                ));
            }
            let id_len =
                u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap_or([0; 2])) as usize;
            pos += 2;
            if pos + id_len + 8 > data.len() {
                return Err(ReplicaAppliedError::Corrupt("truncated entry body".into()));
            }
            let id = std::str::from_utf8(&data[pos..pos + id_len])
                .map_err(|e| ReplicaAppliedError::Corrupt(format!("invalid utf8: {e}")))?
                .to_string();
            pos += id_len;
            let seq = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap_or([0; 8]));
            pos += 8;
            result.insert(id, seq);
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Catch-up runner
// ---------------------------------------------------------------------------

use crate::replication::protocol::ReplicaOp;

/// Structured failure modes for [`run_catchup_for_replica`].
///
/// Replaces the previous `Result<u64, String>` contract that forced callers
/// to substring-match on error messages to recover the "redo log wrapped past
/// the replica" signal that triggers a full-shard resync. With the typed
/// variant, the bin-side dispatch becomes an exhaustive `match` and a future
/// refactor of an error message can no longer silently disable resync
/// requests.
///
/// Per the project convention (`CLAUDE.md` — "All error types must be enums
/// with descriptive variants"). The `RedoReclaimed` variant is the only
/// load-bearing variant for control flow today; the rest preserve the
/// fidelity of the underlying transport / replica failure for logging and
/// future programmatic handling.
#[derive(Debug, thiserror::Error)]
pub enum CatchupError {
    /// The circular redo log has wrapped past `from`, so the entries needed
    /// to bring the replica up to date are no longer available. The caller
    /// must fall back to a full-shard resync.
    ///
    /// `from` is the first sequence number the catch-up needed to stream.
    /// `available` is the earliest sequence still present in the redo log
    /// (when known — `None` indicates the wrap was detected because the
    /// `ops_from_seq` callback returned an empty vec without the log
    /// reporting its earliest sequence separately).
    #[error(
        "redo log wrapped past replica position: requested from sequence {from}, \
         earliest available {available:?}; full resync required"
    )]
    RedoReclaimed { from: u64, available: Option<u64> },

    /// Sending a catch-up chunk to the replica failed (transport error,
    /// replica-side error ack, or sequence renegotiation failure — the
    /// `send_chunk` callback flattens these into one detail string).
    #[error("transport to {addr}: {detail}")]
    Transport { addr: SocketAddr, detail: String },
}

/// Check whether the redo log has been truncated past a requested sequence.
///
/// Returns `Ok(())` if the entries start at or before `requested_seq`,
/// meaning no gap exists. Returns `Err(msg)` if the earliest available
/// entry is beyond the requested sequence — the circular redo log has
/// wrapped and the caller must fall back to a full resync.
///
/// Used by both the replication catch-up path and migration delta streaming
/// to detect log truncation consistently.
pub fn check_redo_truncation(
    first_entry_seq: Option<u64>,
    requested_seq: u64,
) -> std::result::Result<(), String> {
    if let Some(first_seq) = first_entry_seq
        && first_seq > requested_seq
    {
        return Err(format!(
            "redo log truncated: need seq {requested_seq}, earliest available {first_seq}; full resync required"
        ));
    }
    Ok(())
}

/// Run catch-up replication for a single replica, streaming redo-derived
/// ops from `from_seq` (a real redo-log sequence) to the current master
/// sequence in chunks of `batch_size` ops.
///
/// R-D2/D-3: this runner deals exclusively in **redo space** (which ops
/// the replica is missing). Wire labeling on the **per-replica dense
/// sequence stream** is the `send_chunk` callback's job — production
/// wires it to `server::dispatch::send_replica_ops_to`, which assigns
/// contiguous labels under the same per-address cursor the steady-state
/// fan-out uses (so catch-up chunks and concurrent live batches share
/// one densely numbered stream, and the pre-fix off-by-one that dropped
/// the first op of every chunk after the first cannot recur — labels no
/// longer derive from ACK arithmetic in this loop).
///
/// `send_chunk(chunk)` must return `Ok(())` only once the replica has
/// durably applied (or provably already applied) every op in `chunk`.
///
/// Returns `Ok(through_redo_seq)` on success: the highest redo sequence
/// covered by the pass, computed conservatively as
/// `from_seq + ops_sent - 1` (redo entries that produce no `ReplicaOp`
/// are under-counted; re-replaying them later is idempotent). Callers
/// record this against the [`AckTracker`]. Failure modes are the typed
/// [`CatchupError`] variants; callers that dispatch on "redo wrapped —
/// request a full resync" MUST `match` on `RedoReclaimed` rather than
/// substring-matching the rendered message — see `bin/server.rs` for the
/// canonical pattern.
///
/// The `ops_from_seq` callback should read redo entries starting at the
/// given sequence and convert them to `ReplicaOp`s. It returns an empty
/// vec when the entries have been reclaimed (circular redo log wrapped).
///
/// The `first_available_seq` callback returns the sequence number of the
/// earliest available redo entry, or `None` if the log is empty. Used to
/// detect redo log truncation: if the earliest entry is beyond `from_seq`,
/// the log has wrapped and a full resync is required instead.
#[allow(clippy::too_many_arguments)]
pub fn run_catchup_for_replica(
    addr: &std::net::SocketAddr,
    from_seq: u64,
    current_seq: u64,
    batch_size: usize,
    max_ops_per_pass: usize,
    ops_from_seq: &dyn Fn(u64) -> Vec<ReplicaOp>,
    first_available_seq: Option<u64>,
    send_chunk: &dyn Fn(&[ReplicaOp]) -> std::result::Result<(), String>,
) -> std::result::Result<u64, CatchupError> {
    if from_seq >= current_seq {
        return Ok(from_seq); // already caught up
    }

    // Detect redo log truncation before attempting to stream.
    // If the circular redo log has wrapped past `from_seq`, the entries
    // we need are gone and the replica needs a full resync. We use
    // `check_redo_truncation` for the comparison but discard its
    // string-typed error and reconstruct the structured variant from
    // the inputs we already have — the underlying helper is shared with
    // the migration delta path which still consumes a string-typed
    // contract.
    if check_redo_truncation(first_available_seq, from_seq).is_err() {
        return Err(CatchupError::RedoReclaimed {
            from: from_seq,
            available: first_available_seq,
        });
    }

    let mut ops = ops_from_seq(from_seq);
    if ops.is_empty() {
        return Err(CatchupError::RedoReclaimed {
            from: from_seq,
            available: first_available_seq,
        });
    }
    let max_ops_per_pass = max_ops_per_pass.max(1);
    if ops.len() > max_ops_per_pass {
        ops.truncate(max_ops_per_pass);
    }

    let batch_size = batch_size.max(1);
    let mut sent: u64 = 0;
    for chunk in ops.chunks(batch_size) {
        send_chunk(chunk).map_err(|detail| CatchupError::Transport {
            addr: *addr,
            detail,
        })?;
        sent += chunk.len() as u64;
    }

    Ok(from_seq + sent - 1)
}

// ---------------------------------------------------------------------------
// Background lag monitor
// ---------------------------------------------------------------------------

/// Callback invoked by the lag monitor for a replica whose lag exceeds the
/// catch-up threshold. Receives the replica address, its last-acked redo
/// sequence, and the master's current redo sequence. Implementations should
/// run one bounded catch-up pass for the replica (e.g. via
/// [`run_catchup_for_replica`]) and persist the new ACK position; the lag
/// monitor will re-invoke it on subsequent ticks until the replica converges.
pub type OnLaggingReplica = std::sync::Arc<dyn Fn(SocketAddr, u64, u64) + Send + Sync>;

/// Spawn a background thread that periodically checks replica lag.
///
/// Every `interval` seconds, reads the per-replica `last_acked` from the
/// tracker and compares against the current master sequence. Logs a
/// warning when lag exceeds `warn_threshold` ops.
///
/// D-7/D-8 runtime catch-up: when `on_lagging` is `Some` and a replica's lag
/// exceeds `catchup_threshold` ops, the callback is invoked for that replica
/// on this tick. The callback runs one bounded catch-up pass; because the
/// monitor re-evaluates lag every interval, a replica that fell behind while
/// the master stayed up converges over successive ticks without any spinning
/// loop or master restart. Passing `None` preserves the warn-only behavior.
///
/// Returns a join handle. The thread runs until `shutdown` is set to true.
#[allow(clippy::too_many_arguments)]
pub fn spawn_lag_monitor(
    tracker: &'static AckTracker,
    current_seq_fn: std::sync::Arc<dyn Fn() -> u64 + Send + Sync>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    interval_secs: u64,
    warn_threshold: u64,
    catchup_threshold: u64,
    on_lagging: Option<OnLaggingReplica>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let interval = std::time::Duration::from_secs(interval_secs);
        loop {
            std::thread::sleep(interval);
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            let master_seq = current_seq_fn();
            let all = tracker.all_acked();
            for (addr, last_acked) in &all {
                let lag = master_seq.saturating_sub(*last_acked);
                if lag > warn_threshold {
                    tracing::warn!(
                        %addr,
                        lag,
                        last_acked,
                        master_seq,
                        "replication: replica lag exceeds threshold",
                    );
                }
                // D-7/D-8: drive runtime catch-up for replicas that have
                // fallen behind the catch-up threshold. One bounded pass per
                // tick; the monitor re-checks lag next interval, so the
                // replica converges across ticks. Re-check `shutdown` so we
                // do not start a fresh pass while tearing down.
                if let Some(cb) = on_lagging.as_ref()
                    && lag > catchup_threshold
                    && !shutdown.load(std::sync::atomic::Ordering::Relaxed)
                {
                    cb(*addr, *last_acked, master_seq);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[test]
    fn record_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        let tracker = AckTracker::new(path);

        let addr = test_addr(5000);
        assert_eq!(tracker.last_acked(&addr), 0);

        tracker.record_ack(addr, 42);
        assert_eq!(tracker.last_acked(&addr), 42);

        // Higher sequence wins.
        tracker.record_ack(addr, 100);
        assert_eq!(tracker.last_acked(&addr), 100);

        // Lower sequence is ignored.
        tracker.record_ack(addr, 50);
        assert_eq!(tracker.last_acked(&addr), 100);
    }

    /// R-067 (D-03) regression: a burst of ACKs MUST trigger a flush
    /// to disk on the count-based threshold, not just the time-based
    /// 1-second window. Pre-fix only the time threshold existed, so a
    /// master crashing within the 1-second window after the previous
    /// flush could lose every ACK that arrived since.
    #[test]
    fn ack_burst_flushes_to_disk_before_time_window_elapses() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        let tracker = AckTracker::new(path.clone());

        // Send FLUSH_DIRTY_COUNT_THRESHOLD distinct ACKs in rapid
        // succession (well under the 1-second time threshold). The
        // count-based threshold must trigger a flush.
        let burst = FLUSH_DIRTY_COUNT_THRESHOLD as u16;
        for i in 0..burst {
            tracker.record_ack(test_addr(7000 + i), 1);
        }

        // The on-disk state must include all burst entries — no
        // explicit `flush()` call. Reopen the tracker from disk to
        // observe what was actually persisted.
        drop(tracker);
        let reopened = AckTracker::new(path);
        for i in 0..burst {
            assert_eq!(
                reopened.last_acked(&test_addr(7000 + i)),
                1,
                "burst entry {i} must be durable on count-threshold flush",
            );
        }
    }

    #[test]
    fn persistence_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");

        {
            let tracker = AckTracker::new(path.clone());
            tracker.record_ack(test_addr(5000), 42);
            tracker.record_ack(test_addr(5001), 99);
            tracker.flush();
        }

        // Load from disk in a new instance.
        let tracker = AckTracker::new(path);
        assert_eq!(tracker.last_acked(&test_addr(5000)), 42);
        assert_eq!(tracker.last_acked(&test_addr(5001)), 99);
    }

    #[test]
    fn multiple_replicas() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        let tracker = AckTracker::new(path);

        tracker.record_ack(test_addr(5000), 10);
        tracker.record_ack(test_addr(5001), 20);
        tracker.record_ack(test_addr(5002), 30);

        let all = tracker.all_acked();
        assert_eq!(all.len(), 3);
        assert_eq!(all[&test_addr(5000)], 10);
        assert_eq!(all[&test_addr(5001)], 20);
        assert_eq!(all[&test_addr(5002)], 30);
    }

    /// R-038 (D-01) regression: `spawn_lag_monitor` spawns a thread
    /// that runs the lag-check loop, calls `current_seq_fn` at least
    /// once per interval, and exits promptly when the shutdown flag is
    /// set. Pre-fix `replica_lag_check_interval_secs` was a dead
    /// config field — `spawn_lag_monitor` existed but was never called
    /// from `bin/server.rs`. This test pins the contract so a future
    /// refactor that breaks the spawn-and-shutdown handshake is
    /// caught immediately.
    #[test]
    fn spawn_lag_monitor_polls_and_shuts_down() {
        // Leak a tracker so the spawn_lag_monitor's `&'static` requirement
        // is satisfied for the duration of the test. Cheap because we
        // run a single-iteration loop and join the thread immediately.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        let tracker_box: Box<AckTracker> = Box::new(AckTracker::new(path));
        let tracker_static: &'static AckTracker = Box::leak(tracker_box);
        // Seed one replica well behind the master so the lag-warn branch
        // would fire if our threshold were 0. We use a large warn
        // threshold to avoid emitting anything from the test (we are
        // not asserting on logs here, only on the polling contract).
        tracker_static.record_ack(test_addr(6000), 5);

        let poll_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let poll_count_for_fn = poll_count.clone();
        let current_seq_fn: std::sync::Arc<dyn Fn() -> u64 + Send + Sync> =
            std::sync::Arc::new(move || {
                poll_count_for_fn.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                1_000_000 // simulate a master far ahead of the seeded replica
            });
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let handle = spawn_lag_monitor(
            tracker_static,
            current_seq_fn,
            shutdown.clone(),
            // 1-second interval: short enough to observe at least one
            // poll within the test's max wait (5 s) but long enough
            // that the test does not hammer.
            1,
            u64::MAX, // suppress any warn lines — we test polling, not logs
            u64::MAX, // no catch-up: this test pins only the polling contract
            None,
        );

        // Wait up to 5 seconds for at least one poll, then trigger
        // shutdown. If polling never happened, the thread is stuck and
        // the assertion below will fail.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while poll_count.load(std::sync::atomic::Ordering::Relaxed) == 0 {
            if std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        // Give the loop one extra interval to observe shutdown.
        let join_result = handle.join();
        assert!(
            join_result.is_ok(),
            "lag monitor thread must exit cleanly on shutdown",
        );
        assert!(
            poll_count.load(std::sync::atomic::Ordering::Relaxed) >= 1,
            "lag monitor must call current_seq_fn at least once before shutdown",
        );
    }

    /// D-7/D-8 regression: the lag monitor must drive runtime catch-up for a
    /// replica that fell behind while the master stayed up. Pre-fix the
    /// monitor was warn-only, so a lagging replica was never repaired until
    /// the master restarted. This test seeds a replica far behind a static
    /// master sequence and asserts that (1) the `on_lagging` callback is
    /// invoked (proving the trigger fires), and (2) when the callback advances
    /// the replica's ACK toward the master, the monitor converges and stops
    /// invoking the callback. With the old (warn-only) signature this test
    /// would not compile, and a monitor that ignored the callback would never
    /// converge.
    #[test]
    fn lag_monitor_drives_catchup_to_convergence() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        let tracker_box: Box<AckTracker> = Box::new(AckTracker::new(path));
        let tracker_static: &'static AckTracker = Box::leak(tracker_box);
        let replica = test_addr(6100);
        // Replica starts 95 ops behind the master (master = 100).
        tracker_static.record_ack(replica, 5);

        const MASTER_SEQ: u64 = 100;
        let current_seq_fn: std::sync::Arc<dyn Fn() -> u64 + Send + Sync> =
            std::sync::Arc::new(|| MASTER_SEQ);
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Count callback invocations. The callback simulates a successful
        // catch-up pass: production `run_catchup_for_replica` streams up to
        // `max_ops_per_pass` (10k) entries per pass — far more than this
        // test's 95-op gap — so a single triggered pass closes the gap and
        // records the master sequence back into the tracker. We clamp the
        // step at `master` to model the bounded read.
        let invocations = std::sync::Arc::new(AtomicU64::new(0));
        let invocations_cb = invocations.clone();
        let on_lagging: OnLaggingReplica =
            std::sync::Arc::new(move |addr: SocketAddr, last_acked: u64, master: u64| {
                invocations_cb.fetch_add(1, Ordering::Relaxed);
                // A single bounded pass closes the gap up to the master.
                let next = (last_acked + 10_000).min(master);
                tracker_static.record_ack(addr, next);
            });

        let handle = spawn_lag_monitor(
            tracker_static,
            current_seq_fn,
            shutdown.clone(),
            1,         // 1s interval
            u64::MAX,  // suppress warn lines
            10,        // catch-up threshold: 10 ops
            Some(on_lagging),
        );

        // Wait until the replica converges (last_acked reaches master) or the
        // deadline. One pass closes the gap; allow generous slack for the
        // first tick under CI noise.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            if tracker_static.last_acked(&replica) >= MASTER_SEQ {
                break;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        let converged_acked = tracker_static.last_acked(&replica);
        let converged_invocations = invocations.load(Ordering::Relaxed);

        shutdown.store(true, Ordering::Relaxed);
        handle.join().expect("lag monitor must exit cleanly");

        assert_eq!(
            converged_acked, MASTER_SEQ,
            "lag monitor must drive the replica to full convergence via catch-up",
        );
        assert!(
            converged_invocations >= 1,
            "catch-up callback must have been invoked at least once",
        );
        // After convergence the lag (0) is below the threshold, so no further
        // invocations occur — bound the total to a couple of ticks of slack,
        // proving the loop drives catch-up but does not spin.
        assert!(
            converged_invocations <= 4,
            "catch-up must converge in a bounded number of passes, not spin (got {converged_invocations})",
        );
    }

    /// D-7/D-8 counter-case: the pre-fix behavior (warn-only monitor, i.e.
    /// `on_lagging = None`) must NOT repair a lagging replica. This pins the
    /// regression so a future change that accidentally drops the callback is
    /// caught: with no callback the seeded replica stays exactly where it was
    /// even after several monitor ticks against a far-ahead master.
    #[test]
    fn lag_monitor_without_callback_leaves_replica_behind() {
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        let tracker_box: Box<AckTracker> = Box::new(AckTracker::new(path));
        let tracker_static: &'static AckTracker = Box::leak(tracker_box);
        let replica = test_addr(6200);
        tracker_static.record_ack(replica, 5);

        let current_seq_fn: std::sync::Arc<dyn Fn() -> u64 + Send + Sync> =
            std::sync::Arc::new(|| 100);
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let handle = spawn_lag_monitor(
            tracker_static,
            current_seq_fn,
            shutdown.clone(),
            1,
            u64::MAX,
            10,
            None, // warn-only: the pre-fix behavior
        );

        // Let the monitor run a few ticks.
        std::thread::sleep(std::time::Duration::from_millis(2500));
        let acked = tracker_static.last_acked(&replica);

        shutdown.store(true, Ordering::Relaxed);
        handle.join().expect("lag monitor must exit cleanly");

        assert_eq!(
            acked, 5,
            "warn-only monitor must leave the lagging replica unrepaired (got {acked})",
        );
    }

    #[test]
    fn empty_file_loads_ok() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        // No file exists — should load empty.
        let tracker = AckTracker::new(path);
        assert_eq!(tracker.all_acked().len(), 0);
    }

    #[test]
    fn durable_tmp_path_appends_instead_of_replacing_suffix() {
        let base = PathBuf::from("/tmp/cluster.state.repl-applied");
        assert_eq!(
            durable_tmp_path(&base),
            PathBuf::from("/tmp/cluster.state.repl-applied.tmp")
        );

        let ack = PathBuf::from("/tmp/cluster.state.repl-ack");
        let intent = PathBuf::from("/tmp/cluster.state.repl-intent");
        assert_ne!(durable_tmp_path(&base), durable_tmp_path(&ack));
        assert_ne!(durable_tmp_path(&base), durable_tmp_path(&intent));
        assert_ne!(durable_tmp_path(&ack), durable_tmp_path(&intent));
    }

    #[test]
    fn ack_tracker_flush_creates_missing_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("ack.dat");
        let tracker = AckTracker::new(path.clone());

        tracker.record_ack(test_addr(5000), 42);
        tracker.flush();

        let reopened = AckTracker::new(path);
        assert_eq!(reopened.last_acked(&test_addr(5000)), 42);
    }

    /// F-G7-008: when `flush_locked` cannot persist the per-replica
    /// ACK map (disk full / permission denied / EIO) the failure was
    /// only visible in the trace log. Operators have no way to alert
    /// on it without scraping logs. The receiver-side metric
    /// `ack_tracker_flush_failures` must increment so the failure is
    /// observable on the standard metrics pipeline.
    #[test]
    fn ack_tracker_flush_failure_bumps_metric() {
        // Install the metric subsystem so the counter has somewhere
        // to live (idempotent — any prior test wins).
        static TEST_METRICS: std::sync::OnceLock<&'static crate::metrics::ReplicationMetrics> =
            std::sync::OnceLock::new();
        let metrics_ref = *TEST_METRICS
            .get_or_init(|| Box::leak(Box::new(crate::metrics::ReplicationMetrics::new())));
        crate::metrics::init_replication_metrics(metrics_ref);
        let metrics =
            crate::metrics::replication_metrics().expect("replication metrics installed for test");
        let before = metrics.ack_tracker_flush_failures.get();

        // Make the path point to a parent that is a regular file rather
        // than a directory — `write_to_disk` then fails inside
        // `ensure_parent_dir`/`create_dir_all` with NotADirectory.
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"not a dir").unwrap();
        let path = blocker.join("inside").join("ack.dat");
        let tracker = AckTracker::new(path);

        tracker.record_ack(test_addr(7777), 99);
        tracker.flush();

        let after = metrics.ack_tracker_flush_failures.get();
        assert!(
            after > before,
            "ack_tracker_flush_failures must bump on persist error \
             (was {before}, now {after})",
        );
    }

    #[test]
    fn ack_tracker_flush_leaves_no_tmp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        let tracker = AckTracker::new(path.clone());

        tracker.record_ack(test_addr(5000), 42);
        tracker.flush();

        assert!(path.exists());
        assert!(!durable_tmp_path(&path).exists());
    }

    // -------------------------------------------------------------------
    // ReplicationIntentTracker
    // -------------------------------------------------------------------

    #[test]
    fn replication_intent_tracker_persistence_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");

        {
            let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();
            tracker.begin(10, 12).unwrap();
            tracker.begin(20, 20).unwrap();
            tracker.begin(0, 2).unwrap();
            assert_eq!(
                tracker.pending(),
                vec![
                    ReplicationIntentRange {
                        first_sequence: 10,
                        last_sequence: 12
                    },
                    ReplicationIntentRange {
                        first_sequence: 20,
                        last_sequence: 20
                    },
                ],
            );
        }

        let reopened = ReplicationIntentTracker::load(path.clone()).unwrap();
        assert_eq!(
            reopened.pending(),
            vec![
                ReplicationIntentRange {
                    first_sequence: 10,
                    last_sequence: 12
                },
                ReplicationIntentRange {
                    first_sequence: 20,
                    last_sequence: 20
                },
            ],
        );

        reopened.commit(10, 12).unwrap();
        assert_eq!(
            reopened.pending(),
            vec![ReplicationIntentRange {
                first_sequence: 20,
                last_sequence: 20
            }],
        );

        let stale_reopen = ReplicationIntentTracker::load(path.clone()).unwrap();
        assert_eq!(
            stale_reopen.pending(),
            vec![
                ReplicationIntentRange {
                    first_sequence: 10,
                    last_sequence: 12
                },
                ReplicationIntentRange {
                    first_sequence: 20,
                    last_sequence: 20
                },
            ],
            "commit persistence is intentionally coalesced; stale ranges \
             cause idempotent re-replication after a crash"
        );

        reopened.flush().unwrap();
        let reopened_again = ReplicationIntentTracker::load(path).unwrap();
        assert_eq!(
            reopened_again.pending(),
            vec![ReplicationIntentRange {
                first_sequence: 20,
                last_sequence: 20
            }],
        );
    }

    #[test]
    fn replication_intent_commit_flush_coalesces_until_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();

        for i in 1..=INTENT_COMMIT_FLUSH_DIRTY_COUNT_THRESHOLD {
            let seq = u64::from(i);
            tracker.begin(seq, seq).unwrap();
        }

        for i in 1..INTENT_COMMIT_FLUSH_DIRTY_COUNT_THRESHOLD {
            let seq = u64::from(i);
            tracker.commit(seq, seq).unwrap();
        }

        let stale_reopen = ReplicationIntentTracker::load(path.clone()).unwrap();
        assert_eq!(
            stale_reopen.pending().len(),
            INTENT_COMMIT_FLUSH_DIRTY_COUNT_THRESHOLD as usize,
            "commit removals before the threshold should remain coalesced on disk"
        );

        let seq = u64::from(INTENT_COMMIT_FLUSH_DIRTY_COUNT_THRESHOLD);
        tracker.commit(seq, seq).unwrap();

        let flushed_reopen = ReplicationIntentTracker::load(path).unwrap();
        assert!(
            flushed_reopen.pending().is_empty(),
            "threshold commit must flush the coalesced removals"
        );
    }

    #[test]
    fn replication_intent_tracker_begin_is_idempotent_and_commit_removes_range() {
        let tracker = ReplicationIntentTracker::in_memory();

        tracker.begin(5, 7).unwrap();
        tracker.begin(5, 7).unwrap();
        tracker.begin(8, 7).unwrap();
        assert_eq!(
            tracker.pending(),
            vec![ReplicationIntentRange {
                first_sequence: 5,
                last_sequence: 7
            }],
        );

        tracker.commit(5, 7).unwrap();
        tracker.commit(5, 7).unwrap();
        assert!(tracker.pending().is_empty());
    }

    #[test]
    fn replication_intent_tracker_creates_missing_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("intent.dat");
        let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();

        tracker.begin(5, 7).unwrap();

        let reopened = ReplicationIntentTracker::load(path).unwrap();
        assert_eq!(
            reopened.pending(),
            vec![ReplicationIntentRange {
                first_sequence: 5,
                last_sequence: 7
            }],
        );
    }

    #[test]
    fn replication_intent_tracker_write_leaves_no_tmp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();

        tracker.begin(5, 7).unwrap();

        assert!(path.exists());
        assert!(!durable_tmp_path(&path).exists());
    }

    #[test]
    fn replication_intent_tracker_corrupt_range_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&9u64.to_le_bytes());
        data.extend_from_slice(&8u64.to_le_bytes());
        std::fs::write(&path, data).unwrap();

        let err = ReplicationIntentTracker::load(path).expect_err("invalid range should reject");
        match err {
            ReplicationIntentError::Corrupt(msg) => assert!(msg.contains("invalid range")),
            other => panic!("expected Corrupt, got {other:?}"),
        }
    }

    // -------------------------------------------------------------------
    // ReplicaAppliedTracker
    // -------------------------------------------------------------------

    #[test]
    fn applied_tracker_set_and_get_monotonic() {
        let t = ReplicaAppliedTracker::in_memory();
        assert_eq!(t.get("shard-0"), 0);

        t.set("shard-0", 50);
        assert_eq!(t.get("shard-0"), 50);

        // Lower sequence must not rewind.
        t.set("shard-0", 10);
        assert_eq!(t.get("shard-0"), 50);

        // Higher advances.
        t.set("shard-0", 100);
        assert_eq!(t.get("shard-0"), 100);

        // Independent streams are separate.
        t.set("shard-1", 7);
        assert_eq!(t.get("shard-1"), 7);
        assert_eq!(t.get("shard-0"), 100);
    }

    #[test]
    fn applied_tracker_persistence_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("applied.dat");

        {
            let t = ReplicaAppliedTracker::load(path.clone()).unwrap();
            t.set("alpha", 42);
            t.set("beta", 100);
            t.flush().unwrap();
        }

        let t2 = ReplicaAppliedTracker::load(path).unwrap();
        assert_eq!(t2.get("alpha"), 42);
        assert_eq!(t2.get("beta"), 100);
        assert_eq!(t2.get("unknown"), 0);
    }

    #[test]
    fn applied_tracker_flush_clears_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("applied.dat");
        let t = ReplicaAppliedTracker::load(path.clone()).unwrap();
        t.set("s", 5);
        t.flush().unwrap();
        // Second flush is a no-op (not dirty) and must still succeed.
        t.flush().unwrap();
        // Reload verifies the flush actually persisted the value.
        let t2 = ReplicaAppliedTracker::load(path).unwrap();
        assert_eq!(t2.get("s"), 5);
    }

    #[test]
    fn applied_tracker_flush_creates_missing_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("applied.dat");
        let tracker = ReplicaAppliedTracker::load(path.clone()).unwrap();

        tracker.set("source", 9);
        tracker.flush().unwrap();

        let reopened = ReplicaAppliedTracker::load(path).unwrap();
        assert_eq!(reopened.get("source"), 9);
    }

    #[test]
    fn applied_tracker_flush_leaves_no_tmp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("applied.dat");
        let tracker = ReplicaAppliedTracker::load(path.clone()).unwrap();

        tracker.set("source", 9);
        tracker.flush().unwrap();

        assert!(path.exists());
        assert!(!durable_tmp_path(&path).exists());
    }

    #[test]
    fn applied_tracker_corrupt_file_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("applied.dat");
        // Write a truncated header (only 2 bytes instead of the required 4).
        std::fs::write(&path, [0xFFu8; 2]).unwrap();
        let err = ReplicaAppliedTracker::load(path).expect_err("should reject");
        match err {
            ReplicaAppliedError::Corrupt(_) => {}
            other => panic!("expected Corrupt, got {other:?}"),
        }
    }

    // -------------------------------------------------------------------
    // Catch-up runner
    // -------------------------------------------------------------------

    /// R-D2 regression (unit level): the catch-up runner must deliver
    /// every op exactly once across chunk boundaries and report redo
    /// coverage as `from_seq + ops_sent - 1`. Pre-fix the runner
    /// labeled chunk N+1 with the last ACKED sequence instead of
    /// acked+1, so the receiver's dedup dropped the first op of every
    /// chunk after the first. Labeling now lives in the `send_chunk`
    /// callback (the dispatch-side dense stream cursor); this test pins
    /// that the runner itself hands over contiguous, complete,
    /// non-overlapping chunks.
    #[test]
    fn run_catchup_chunks_cover_all_ops_without_skips_or_overlap() {
        use crate::index::TxKey;

        let addr: SocketAddr = "127.0.0.1:65533".parse().unwrap();
        // 10 distinguishable ops: Delete on tx_key marked by index.
        let make_ops = |n: u8| -> Vec<ReplicaOp> {
            (0..n)
                .map(|i| ReplicaOp::Delete {
                    tx_key: TxKey::from_bytes([i + 1; 32]),
                })
                .collect()
        };
        let all_ops = make_ops(10);
        let ops_for_cb = all_ops.clone();

        let delivered: std::sync::Mutex<Vec<ReplicaOp>> = std::sync::Mutex::new(Vec::new());
        let chunk_sizes: std::sync::Mutex<Vec<usize>> = std::sync::Mutex::new(Vec::new());

        let result = run_catchup_for_replica(
            &addr,
            5,  // from_seq (redo space)
            15, // current_seq
            3,  // batch_size → chunks of 3,3,3,1
            10_000,
            &move |_from| ops_for_cb.clone(),
            Some(5),
            &|chunk| {
                chunk_sizes.lock().unwrap().push(chunk.len());
                delivered.lock().unwrap().extend_from_slice(chunk);
                Ok(())
            },
        );

        assert_eq!(
            result.unwrap(),
            14,
            "redo coverage must be from_seq + ops_sent - 1 = 5 + 10 - 1",
        );
        assert_eq!(*chunk_sizes.lock().unwrap(), vec![3, 3, 3, 1]);
        assert_eq!(
            *delivered.lock().unwrap(),
            all_ops,
            "every op must be delivered exactly once, in order, across chunk boundaries",
        );
    }

    /// A chunk-send failure must abort the pass with a typed
    /// `CatchupError::Transport` carrying the callback's detail string,
    /// and no further chunks may be sent.
    #[test]
    fn run_catchup_send_failure_aborts_with_transport_error() {
        use crate::index::TxKey;

        let addr: SocketAddr = "127.0.0.1:65532".parse().unwrap();
        let ops: Vec<ReplicaOp> = (0..6u8)
            .map(|i| ReplicaOp::Delete {
                tx_key: TxKey::from_bytes([i + 1; 32]),
            })
            .collect();
        let calls = std::sync::atomic::AtomicU64::new(0);

        let err = run_catchup_for_replica(
            &addr,
            1,
            7,
            2,
            10_000,
            &move |_from| ops.clone(),
            Some(1),
            &|_chunk| {
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if n == 1 {
                    Err("replica error: boom".to_string())
                } else {
                    assert!(n < 2, "no chunk may be sent after a failure");
                    Ok(())
                }
            },
        )
        .expect_err("second chunk fails — pass must abort");

        match err {
            CatchupError::Transport { addr: a, detail } => {
                assert_eq!(a, addr);
                assert_eq!(detail, "replica error: boom");
            }
            other => panic!("expected CatchupError::Transport, got {other:?}"),
        }
    }

    /// F-G10-017 / B-4 — `run_catchup_for_replica` MUST surface a typed
    /// `CatchupError::RedoReclaimed { from, available }` when the circular
    /// redo log has wrapped past the replica's resume position, so the
    /// bin-side dispatch can match on the variant instead of a fragile
    /// `String::contains("redo entries reclaimed")` substring check.
    ///
    /// Two wrap-detection paths exist in the function and both must lower
    /// to the same variant:
    ///
    /// 1. `check_redo_truncation` sees `first_available_seq > from_seq` —
    ///    detectable WITHOUT reading any entries.
    /// 2. `ops_from_seq` returns an empty vec — happens when the redo
    ///    helper cannot reify the requested sequence for any reason.
    #[test]
    fn run_catchup_returns_typed_redo_reclaimed_when_log_wrapped() {
        let addr: SocketAddr = "127.0.0.1:65535".parse().unwrap();
        let no_ops: &dyn Fn(u64) -> Vec<ReplicaOp> = &|_| Vec::new();
        let no_send: &dyn Fn(&[ReplicaOp]) -> std::result::Result<(), String> =
            &|_| panic!("send_chunk must not be called when redo wrapped");

        // Path 1: explicit truncation signal — `first_available_seq` is
        // ahead of `from_seq` so `check_redo_truncation` short-circuits
        // before any transport work happens. `from = 10`, `available = 50`.
        let err1 = run_catchup_for_replica(&addr, 10, 100, 16, 100, no_ops, Some(50), no_send)
            .expect_err("must error when redo wrapped past from_seq");
        match err1 {
            CatchupError::RedoReclaimed { from, available } => {
                assert_eq!(from, 10, "RedoReclaimed.from must echo the requested seq");
                assert_eq!(
                    available,
                    Some(50),
                    "RedoReclaimed.available must echo the earliest available seq",
                );
            }
            other => panic!("expected CatchupError::RedoReclaimed, got {other:?}"),
        }

        // Path 2: `first_available_seq = None` — log reports it has no
        // earliest entry yet `ops_from_seq` returns nothing. This is the
        // wrap-without-witness case the original string error covered.
        let err2 = run_catchup_for_replica(&addr, 7, 42, 16, 100, no_ops, None, no_send)
            .expect_err("must error when ops_from_seq returns empty");
        match err2 {
            CatchupError::RedoReclaimed { from, available } => {
                assert_eq!(from, 7);
                assert_eq!(available, None);
            }
            other => panic!("expected CatchupError::RedoReclaimed, got {other:?}"),
        }

        // Sanity: the rendered Display message still mentions the
        // replica position — but consumers MUST NOT depend on substring
        // matching on it. This assertion is purely an operator-log
        // sanity check.
        let display = format!(
            "{}",
            CatchupError::RedoReclaimed {
                from: 7,
                available: Some(3),
            }
        );
        assert!(
            display.contains("redo log wrapped"),
            "Display impl should describe the wrap condition: {display}",
        );
    }

    /// Companion to the test above: when `from_seq >= current_seq` the
    /// catch-up is a no-op and returns `Ok(from_seq)`. This pins the
    /// happy-path early-return so a future refactor cannot accidentally
    /// fall through into the redo-reclaimed branch.
    #[test]
    fn run_catchup_already_caught_up_returns_ok() {
        let addr: SocketAddr = "127.0.0.1:65534".parse().unwrap();
        let no_ops: &dyn Fn(u64) -> Vec<ReplicaOp> = &|_| Vec::new();
        let no_send: &dyn Fn(&[ReplicaOp]) -> std::result::Result<(), String> =
            &|_| panic!("send_chunk must not be called when already caught up");

        let result = run_catchup_for_replica(&addr, 100, 100, 16, 100, no_ops, Some(50), no_send);
        assert_eq!(result.unwrap(), 100);

        let result = run_catchup_for_replica(&addr, 200, 100, 16, 100, no_ops, Some(50), no_send);
        assert_eq!(result.unwrap(), 200);
    }
}
