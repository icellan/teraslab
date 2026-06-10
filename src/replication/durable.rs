//! Persistent replication ACK tracking.
//!
//! Tracks per-replica `last_acked` sequences durably to disk so that after
//! a master restart, the master knows where each replica left off and can
//! stream the missing redo entries instead of requiring a full resync.

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
/// The `last_acked` map records the highest replication sequence number
/// that each replica has durably acknowledged. This is written to disk
/// periodically (at most once per second) to amortize I/O while ensuring
/// reasonable recovery bounds.
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
/// F-G7-004 contract: the deferred `commit()` durability is safe ONLY
/// when the master-side recovery replay path consults the receiver's
/// `ReplicaAppliedTracker` before re-applying each range. Recovery
/// MUST NOT bypass the dedup tracker (including for batches flagged
/// `FLAG_MIGRATION_BATCH`; F-G7-005 enforces a non-zero cluster_key
/// gate on those paths in clustered mode). If a future change to the
/// recovery loop skips dedup, this constant must be set to 1 to make
/// every commit immediately durable so stale ranges never reach
/// recovery replay.
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

use crate::replication::manager::ReplicaTransport;
use crate::replication::protocol::{ReplicaBatch, ReplicaOp};
use crate::replication::tcp_transport::TcpReplicaTransport;

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

    /// The TCP transport to the replica failed at connect / send / recv time.
    #[error("transport to {addr}: {detail}")]
    Transport { addr: SocketAddr, detail: String },

    /// The replica returned an `ERR` ack instead of `OK`.
    #[error("replica {addr} returned error: {message}")]
    ReplicaError { addr: SocketAddr, message: String },
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

/// Build a single catch-up `ReplicaBatch` stamped with the caller-supplied
/// `local_cluster_key`.
///
/// Extracted from [`run_catchup_for_replica`] so the cluster-epoch tagging
/// can be unit-tested without spinning up a TCP transport. The receiver-side
/// gate added in Phase B2 (see [`ERR_STALE_EPOCH`]) rejects any batch whose
/// `cluster_key` neither matches the local epoch nor is the V1-compat
/// sentinel `0`, so catch-up batches must carry the master's live epoch
/// just like the steady-state path.
///
/// `first_sequence` is the sequence number of the first op in `chunk`;
/// `chunk` is non-empty (an empty chunk would still produce a valid batch
/// but the catch-up loop never builds one).
///
/// [`ERR_STALE_EPOCH`]: crate::protocol::opcodes::ERR_STALE_EPOCH
pub fn build_catchup_batch(
    first_sequence: u64,
    chunk: &[ReplicaOp],
    local_cluster_key: u64,
) -> ReplicaBatch {
    ReplicaBatch {
        first_sequence,
        ops: chunk.to_vec(),
        trace_ctx: crate::observability::WireTraceContext::from_current_span(),
        source_node_id: None,
        cluster_key: local_cluster_key,
    }
}

/// Run catch-up replication for a single replica, streaming redo-derived
/// ops from `from_seq` to the current master sequence.
///
/// Returns `Ok(through_seq)` on success with the final ACKed sequence, or
/// a [`CatchupError`] variant identifying the failure mode. Callers that
/// need to dispatch on the failure (e.g. "redo wrapped — request a full
/// resync") MUST `match` on the variant rather than substring-matching on
/// the rendered message — see `bin/server.rs` for the canonical pattern.
///
/// `local_cluster_key` is the master's current topology epoch (snapshot of
/// [`RunningCluster::local_cluster_key`]); every batch is stamped with it so
/// the receiver's epoch gate (Phase B2) accepts the catch-up the same way
/// it accepts steady-state batches. Pass `0` only from test fixtures where
/// the receiver-side gate is intentionally bypassed via the V1-compat
/// sentinel.
///
/// The `ops_from_seq` callback should read redo entries starting at the
/// given sequence and convert them to `ReplicaOp`s. It returns an empty
/// vec when the entries have been reclaimed (circular redo log wrapped).
///
/// The `first_available_seq` callback returns the sequence number of the
/// earliest available redo entry, or `None` if the log is empty. Used to
/// detect redo log truncation: if the earliest entry is beyond `from_seq`,
/// the log has wrapped and a full resync is required instead.
///
/// [`RunningCluster::local_cluster_key`]: crate::cluster::coordinator::RunningCluster::local_cluster_key
#[allow(clippy::too_many_arguments)]
pub fn run_catchup_for_replica(
    addr: &std::net::SocketAddr,
    from_seq: u64,
    current_seq: u64,
    batch_size: usize,
    max_ops_per_pass: usize,
    ops_from_seq: &dyn Fn(u64) -> Vec<ReplicaOp>,
    first_available_seq: Option<u64>,
    local_cluster_key: u64,
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

    let mut transport =
        TcpReplicaTransport::connect(&addr.to_string(), std::time::Duration::from_secs(5))
            .map_err(|e| CatchupError::Transport {
                addr: *addr,
                detail: format!("connect: {e}"),
            })?;

    let mut last_acked = from_seq;
    for chunk in ops.chunks(batch_size) {
        let batch = build_catchup_batch(last_acked, chunk, local_cluster_key);
        transport
            .send_batch(&batch)
            .map_err(|e| CatchupError::Transport {
                addr: *addr,
                detail: format!("send_batch: {e}"),
            })?;
        match transport.recv_ack(std::time::Duration::from_secs(5)) {
            Ok(crate::replication::protocol::ReplicaAck::Ok { through_sequence }) => {
                last_acked = through_sequence;
            }
            Ok(crate::replication::protocol::ReplicaAck::Error { message, .. }) => {
                return Err(CatchupError::ReplicaError {
                    addr: *addr,
                    message,
                });
            }
            Err(e) => {
                return Err(CatchupError::Transport {
                    addr: *addr,
                    detail: format!("recv_ack: {e}"),
                });
            }
        }
    }

    Ok(last_acked)
}

// ---------------------------------------------------------------------------
// Background lag monitor
// ---------------------------------------------------------------------------

/// Spawn a background thread that periodically checks replica lag.
///
/// Every `interval` seconds, reads the per-replica `last_acked` from the
/// tracker and compares against the current master sequence. Logs a
/// warning when lag exceeds `warn_threshold` ops.
///
/// Returns a join handle. The thread runs until `shutdown` is set to true.
pub fn spawn_lag_monitor(
    tracker: &'static AckTracker,
    current_seq_fn: std::sync::Arc<dyn Fn() -> u64 + Send + Sync>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    interval_secs: u64,
    warn_threshold: u64,
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
    // Catch-up batch construction — Phase B3 cluster_key wiring
    // -------------------------------------------------------------------

    #[test]
    fn catchup_batch_attaches_caller_supplied_cluster_key() {
        // Phase B3 fixup: catch-up batches must carry the master's live
        // topology epoch so the receiver-side ERR_STALE_EPOCH gate accepts
        // them. Previously the catch-up path hard-coded cluster_key: 0,
        // which only worked while the receiver still treated 0 as the
        // V1-compat "unknown" sentinel.
        use crate::index::TxKey;

        let tx_key = TxKey::from_bytes([7u8; 32]);
        let ops = vec![ReplicaOp::Delete { tx_key }];

        let batch = build_catchup_batch(123, &ops, 42);

        assert_eq!(
            batch.cluster_key, 42,
            "catch-up batch must propagate caller-supplied local_cluster_key",
        );
        assert_eq!(batch.first_sequence, 123);
        assert_eq!(batch.ops.len(), 1);
        assert!(
            batch.source_node_id.is_none(),
            "catch-up batches do not stamp a source node id",
        );
    }

    #[test]
    fn catchup_batch_zero_cluster_key_is_v1_compat_path() {
        // Tests are still permitted to construct cluster_key: 0 batches —
        // the receiver-side gate treats 0 as the V1-compat sentinel. This
        // test pins that the helper does not silently rewrite 0 to some
        // other value (e.g. a default epoch).
        use crate::index::TxKey;

        let tx_key = TxKey::from_bytes([0u8; 32]);
        let ops = vec![ReplicaOp::Delete { tx_key }];

        let batch = build_catchup_batch(1, &ops, 0);

        assert_eq!(batch.cluster_key, 0);
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

        // Path 1: explicit truncation signal — `first_available_seq` is
        // ahead of `from_seq` so `check_redo_truncation` short-circuits
        // before any transport work happens. `from = 10`, `available = 50`.
        let err1 = run_catchup_for_replica(&addr, 10, 100, 16, 100, no_ops, Some(50), 0)
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
        let err2 = run_catchup_for_replica(&addr, 7, 42, 16, 100, no_ops, None, 0)
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

        let result = run_catchup_for_replica(&addr, 100, 100, 16, 100, no_ops, Some(50), 0);
        assert_eq!(result.unwrap(), 100);

        let result = run_catchup_for_replica(&addr, 200, 100, 16, 100, no_ops, Some(50), 0);
        assert_eq!(result.unwrap(), 200);
    }
}
