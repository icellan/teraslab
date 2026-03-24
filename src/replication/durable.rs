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
// Catch-up runner
// ---------------------------------------------------------------------------

use crate::replication::manager::ReplicaTransport;
use crate::replication::protocol::{ReplicaBatch, ReplicaOp};
use crate::replication::tcp_transport::TcpReplicaTransport;

/// Run catch-up replication for a single replica, streaming redo-derived
/// ops from `from_seq` to the current master sequence.
///
/// Returns `Ok(through_seq)` on success with the final ACKed sequence,
/// `Err(msg)` if the catch-up fails (transport error, reclaimed redo, etc.).
///
/// The `ops_from_seq` callback should read redo entries starting at the
/// given sequence and convert them to `ReplicaOp`s. It returns an empty
/// vec when the entries have been reclaimed (circular redo log wrapped).
pub fn run_catchup_for_replica(
    addr: &std::net::SocketAddr,
    from_seq: u64,
    current_seq: u64,
    batch_size: usize,
    ops_from_seq: &dyn Fn(u64) -> Vec<ReplicaOp>,
) -> std::result::Result<u64, String> {
    if from_seq >= current_seq {
        return Ok(from_seq); // already caught up
    }

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
}
