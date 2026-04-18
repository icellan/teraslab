//! Persistent replication ACK tracking.
//!
//! Tracks per-replica `last_acked` sequences durably to disk so that after
//! a master restart, the master knows where each replica left off and can
//! stream the missing redo entries instead of requiring a full resync.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

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
}

/// Minimum interval between flushes to disk (1 second).
const FLUSH_INTERVAL_MS: u128 = 1000;

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
            }),
        }
    }

    /// Record a successful ACK from a replica. Flushes to disk if enough
    /// time has passed since the last flush.
    pub fn record_ack(&self, addr: SocketAddr, through_sequence: u64) {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.last_acked.entry(addr).or_insert(0);
        if through_sequence > *entry {
            *entry = through_sequence;
            inner.dirty = true;
        }

        // Amortize: flush at most once per second.
        if inner.dirty && inner.last_flush.elapsed().as_millis() >= FLUSH_INTERVAL_MS {
            self.flush_locked(&mut inner);
        }
    }

    /// Get the last-ACKed sequence for a replica, or 0 if unknown.
    pub fn last_acked(&self, addr: &SocketAddr) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.last_acked.get(addr).copied().unwrap_or(0)
    }

    /// Get all tracked replicas and their ACK sequences.
    pub fn all_acked(&self) -> HashMap<SocketAddr, u64> {
        let inner = self.inner.lock().unwrap();
        inner.last_acked.clone()
    }

    /// Force a flush of any dirty state to disk.
    pub fn flush(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.dirty {
            self.flush_locked(&mut inner);
        }
    }

    fn flush_locked(&self, inner: &mut AckTrackerInner) {
        if let Err(e) = Self::write_to_disk(&self.path, &inner.last_acked) {
            eprintln!("ack_tracker: flush failed: {e}");
            return;
        }
        inner.dirty = false;
        inner.last_flush = Instant::now();
    }

    /// Serialize and write the ACK state to disk.
    ///
    /// Format: `[entry_count:4 LE]([addr_len:2 LE][addr_bytes][last_acked:8 LE])*`
    fn write_to_disk(
        path: &Path,
        state: &HashMap<SocketAddr, u64>,
    ) -> std::io::Result<()> {
        let mut buf = Vec::with_capacity(4 + state.len() * 30);
        buf.extend_from_slice(&(state.len() as u32).to_le_bytes());
        for (addr, &seq) in state {
            let addr_str = addr.to_string();
            let addr_bytes = addr_str.as_bytes();
            buf.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(addr_bytes);
            buf.extend_from_slice(&seq.to_le_bytes());
        }
        // Atomic write: write to temp, then rename.
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &buf)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
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
            let addr_str = std::str::from_utf8(&data[pos..pos + addr_len])
                .unwrap_or("");
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
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.last_applied.get(stream).copied().unwrap_or(0)
    }

    /// Record that `stream` has durably applied through `seq`.
    ///
    /// Only advances; a lower `seq` is ignored so concurrent callers
    /// cannot rewind the journal. Marks the tracker dirty for the
    /// next [`flush`](Self::flush) call.
    pub fn set(&self, stream: &str, seq: u64) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &buf)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
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
            let id_len = u16::from_le_bytes(
                data[pos..pos + 2]
                    .try_into()
                    .unwrap_or([0; 2]),
            ) as usize;
            pos += 2;
            if pos + id_len + 8 > data.len() {
                return Err(ReplicaAppliedError::Corrupt("truncated entry body".into()));
            }
            let id = std::str::from_utf8(&data[pos..pos + id_len])
                .map_err(|e| ReplicaAppliedError::Corrupt(format!("invalid utf8: {e}")))?
                .to_string();
            pos += id_len;
            let seq = u64::from_le_bytes(
                data[pos..pos + 8]
                    .try_into()
                    .unwrap_or([0; 8]),
            );
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
/// ops from `from_seq` to the current master sequence.
///
/// Returns `Ok(through_seq)` on success with the final ACKed sequence,
/// `Err(msg)` if the catch-up fails (transport error, reclaimed redo, etc.).
///
/// The `ops_from_seq` callback should read redo entries starting at the
/// given sequence and convert them to `ReplicaOp`s. It returns an empty
/// vec when the entries have been reclaimed (circular redo log wrapped).
///
/// The `first_available_seq` callback returns the sequence number of the
/// earliest available redo entry, or `None` if the log is empty. Used to
/// detect redo log truncation: if the earliest entry is beyond `from_seq`,
/// the log has wrapped and a full resync is required instead.
pub fn run_catchup_for_replica(
    addr: &std::net::SocketAddr,
    from_seq: u64,
    current_seq: u64,
    batch_size: usize,
    ops_from_seq: &dyn Fn(u64) -> Vec<ReplicaOp>,
    first_available_seq: Option<u64>,
) -> std::result::Result<u64, String> {
    if from_seq >= current_seq {
        return Ok(from_seq); // already caught up
    }

    // Detect redo log truncation before attempting to stream.
    // If the circular redo log has wrapped past `from_seq`, the entries
    // we need are gone and the replica needs a full resync.
    check_redo_truncation(first_available_seq, from_seq)?;

    let ops = ops_from_seq(from_seq);
    if ops.is_empty() {
        return Err("redo entries reclaimed; full resync required".to_string());
    }

    let mut transport = TcpReplicaTransport::connect(
        &addr.to_string(),
        std::time::Duration::from_secs(5),
    ).map_err(|e| format!("catchup connect to {addr}: {e}"))?;

    let mut last_acked = from_seq;
    for chunk in ops.chunks(batch_size) {
        let batch = ReplicaBatch {
            first_sequence: last_acked,
            ops: chunk.to_vec(),
        };
        transport.send_batch(&batch)
            .map_err(|e| format!("catchup send to {addr}: {e}"))?;
        match transport.recv_ack(std::time::Duration::from_secs(5)) {
            Ok(crate::replication::protocol::ReplicaAck::Ok { through_sequence }) => {
                last_acked = through_sequence;
            }
            Ok(crate::replication::protocol::ReplicaAck::Error { message, .. }) => {
                return Err(format!("catchup: replica error: {message}"));
            }
            Err(e) => {
                return Err(format!("catchup recv_ack from {addr}: {e}"));
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
                    eprintln!(
                        "replication: replica {addr} lag={lag} ops (last_acked={last_acked}, master={master_seq})"
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

    #[test]
    fn empty_file_loads_ok() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        // No file exists — should load empty.
        let tracker = AckTracker::new(path);
        assert_eq!(tracker.all_acked().len(), 0);
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
}
