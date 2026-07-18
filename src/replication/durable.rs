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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Instant;

use parking_lot::Mutex;

use crate::index::TxKey;

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
    // `parent()` is `Some("")` for a bare relative name, not `None` (issue #13).
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
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
        // F-D1: the ACK file is a re-derivable master-side hint — the
        // empty-batch ACK probe re-establishes per-replica watermarks at
        // runtime. A corrupt/truncated file must NOT be parsed into a PARTIAL
        // map: a partial map can carry stale-high watermarks that MASK replica
        // lag (the opposite of safe). Fail closed by discarding the whole file
        // and starting from empty (which forces a full, idempotent catch-up),
        // and surface the loss loudly via ERROR + a metric — never silently
        // trust a half-parsed map. (Contrast the previous `unwrap_or_default`
        // which silently kept a partial parse.)
        let last_acked = match Self::load_from_disk(&path) {
            Ok(m) => m,
            Err(e) => {
                if let Some(m) = crate::metrics::replication_metrics() {
                    m.ack_tracker_load_failures.inc();
                }
                tracing::error!(
                    err = %e,
                    path = %path.display(),
                    "ack_tracker: load failed (corrupt/truncated ACK file); discarding it and \
                     starting from empty — replica progress will be re-verified via catch-up",
                );
                HashMap::new()
            }
        };
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

    /// Reverse-heal Tier-1 fast-path (finding C1): the replicas whose durably
    /// persisted last-ACK sequence is at-or-beyond `floor`.
    ///
    /// `floor` is this node's recovered `shared_sequence_floor` = the master's
    /// `next_sequence` = highest-durable redo sequence + 1. It is the EXCLUSIVE
    /// next-to-assign sequence, so the highest sequence this node can still
    /// prove it holds after recovery is `floor - 1`. The tracker stores the
    /// INCLUSIVE `redo_high` each replica ACKed (the highest redo seq whose ops
    /// that replica confirmed durable). Both live in this master's own global
    /// redo-sequence space, so the comparison is valid.
    ///
    /// Because the ACK is inclusive and the floor is exclusive, a lost acked
    /// tail exists iff `acked >= floor`: an ACK of exactly `floor` means the
    /// master returned `STATUS_OK` for op `floor` yet recovered only through
    /// `floor - 1` — the depth-1 lost tail that is the modal crash. Using `>`
    /// here (pre-fix) missed that boundary and silently accepted the loss. A
    /// NON-EMPTY result therefore means this node acked at least one write it
    /// can no longer prove it holds — a lost acked tail. Empty means Tier-1
    /// sees no gap.
    ///
    /// This is a *fast-path* filter, not the sole authority: the tracker
    /// flushes on a <=1s / 100-ACK cadence, so it can be stale-low and MISS a
    /// gap in the sub-second window. The per-shard generation-manifest confirm
    /// (Tier-2) is the authoritative backstop.
    pub fn acked_beyond(&self, floor: u64) -> Vec<(SocketAddr, u64)> {
        let inner = self.inner.lock();
        inner
            .last_acked
            .iter()
            .filter(|&(_, &seq)| seq >= floor)
            .map(|(addr, &seq)| (*addr, seq))
            .collect()
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

        if data.is_empty() {
            return Ok(HashMap::new());
        }
        // F-D1: fail closed on a corrupt/truncated file (matches
        // `ReplicaAppliedTracker::read_from_disk`). The caller (`AckTracker::new`)
        // turns an `Err` into "discard + start empty", never a partial map.
        fn corrupt(msg: &str) -> std::io::Error {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("ack_tracker file corrupt: {msg}"),
            )
        }
        if data.len() < 4 {
            return Err(corrupt("truncated header"));
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut result = HashMap::with_capacity(count);
        let mut pos = 4;

        for _ in 0..count {
            if pos + 2 > data.len() {
                return Err(corrupt("truncated entry length"));
            }
            let addr_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if pos + addr_len + 8 > data.len() {
                return Err(corrupt("truncated entry body"));
            }
            let addr_str = std::str::from_utf8(&data[pos..pos + addr_len])
                .map_err(|e| corrupt(&format!("invalid utf8 addr: {e}")))?;
            pos += addr_len;
            let seq = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            let addr = addr_str
                .parse::<SocketAddr>()
                .map_err(|e| corrupt(&format!("invalid socket addr {addr_str:?}: {e}")))?;
            result.insert(addr, seq);
        }

        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Master-side pending replication intent tracker
// ---------------------------------------------------------------------------

/// Append-only intent log record-type tags (frame format documented on
/// [`ReplicationIntentTracker`]). No version field: the on-disk layout may
/// change freely across releases — a node resets its intent file on
/// upgrade.
const INTENT_RECORD_SNAPSHOT: u8 = 0;
const INTENT_RECORD_BEGIN: u8 = 1;
const INTENT_RECORD_COMMIT: u8 = 2;

/// Guards [`intent_log_parse_frames`] against an implausible/garbage length
/// field (a torn write can leave any 4 bytes at a length-field offset). No
/// real intent record — even a `SNAPSHOT` of a very large pending set —
/// comes close to this; anything bigger is corruption or a torn tail, and
/// either way parsing must stop rather than attempt to read gigabytes.
const INTENT_LOG_MAX_FRAME_PAYLOAD_LEN: u32 = 256 * 1024 * 1024;

/// Number of records the append-only intent log may accumulate since its
/// last `SNAPSHOT` before compaction rewrites it back down to one. Chosen
/// generously above realistic per-restart record volume (each `begin`/
/// `commit` is one record) so compaction stays a rare, amortized event —
/// not a per-op cost — while still bounding on-disk log size and worst-case
/// recovery replay length.
const INTENT_LOG_COMPACT_RECORD_THRESHOLD: u32 = 512;

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
    /// R12 review (Critical, fail-closed fix): a prior compaction's
    /// post-rename reopen of the append handle failed, and every subsequent
    /// write/sync is refused rather than risk silently persisting to the
    /// stale, now-unlinked handle. See [`AppendState::Poisoned`].
    #[error("replication intent tracker poisoned (durability barrier unavailable): {0}")]
    Poisoned(String),
}

/// Persistent master-side journal of pending replication ranges.
///
/// The dispatcher records a range before attempting replica fan-out and removes
/// it only after the configured ACK policy is satisfied (or after a failed
/// client mutation has been durably compensated). On restart, any range left in
/// this file must be replicated to current holders before the node serves.
///
/// ## On-disk format (R12/C32): append-only, CRC-framed record log
///
/// No version field — nodes reset their intent file on upgrade, so this
/// format may change freely. Each record is framed as:
///
/// ```text
/// [payload_len:4 LE][crc32:4 LE][type:1][payload]
/// ```
///
/// `payload_len` covers `type` + `payload` (i.e. `1 + payload.len()`);
/// `crc32 = crc32fast::hash(&[type] ++ payload)`. Three record types:
///
/// - `SNAPSHOT` (0): payload is the full pending set — `[range_count:4]
///   ( [first:8][last:8][key_count:4][txid:32]{key_count} )*`. Written only
///   by compaction; on replay it RESETS the reconstructed map to exactly
///   its contents.
/// - `BEGIN` (1): payload is one `[first:8][last:8][key_count:4]
///   [txid:32]{key_count}`. On replay, inserts/overwrites that range.
/// - `COMMIT` (2): payload is `[first:8][last:8]`. On replay, removes that
///   range.
///
/// `begin` appends exactly one `BEGIN` record and calls `sync_data`
/// (fdatasync) before returning — the append-only replacement for the old
/// per-`begin` full-snapshot-rewrite + dual-fsync (temp-write + rename +
/// parent-dir fsync). `commit` stays amortized: it appends a `COMMIT`
/// record immediately but only fsyncs on the existing time/count-threshold
/// cadence, so a lost `COMMIT` just leaves a stale range that recovery
/// replays idempotently (unchanged contract).
///
/// Recovery ([`read_from_disk`](Self::read_from_disk)) applies records in
/// order and is torn-tail-safe: it stops — without erroring — at the first
/// frame that is truncated (EOF before a full frame), has an implausible
/// length, or fails its CRC check; everything before that frame is valid
/// and is applied. Only a frame whose CRC is otherwise valid but whose
/// *decoded contents* are structurally invalid (e.g. a key section shorter
/// than its declared count, or an unrecognized record type) is a hard
/// [`ReplicationIntentError::Corrupt`] error, since a valid CRC over
/// consistent bytes cannot be explained by a crash mid-append.
///
/// When the log grows past [`INTENT_LOG_COMPACT_RECORD_THRESHOLD`] records
/// since its last snapshot, it is compacted: rewritten as a single fresh
/// `SNAPSHOT` via the same atomic temp+`sync_all`+rename+parent-fsync
/// pattern used elsewhere in this module (a rare, amortized event — unlike
/// per-`begin` durability), and the append handle is reopened on the new
/// file (`rename` does not redirect an already-open file descriptor to the
/// new inode).
#[derive(Debug)]
pub struct ReplicationIntentTracker {
    path: PathBuf,
    inner: Mutex<ReplicationIntentInner>,
}

/// Durability state of the on-disk log's append handle.
///
/// R12 review (Critical, fail-closed fix): `compact_locked` writes a fresh
/// `SNAPSHOT` (atomic temp-write + rename), then reopens the append handle
/// on the new file (`rename` does not redirect an already-open fd to the
/// new inode). If that reopen fails AFTER the rename already succeeded, the
/// PREVIOUS handle is left pointing at an unlinked, orphaned inode — POSIX
/// permits `write()`/`fsync()` on an unlinked fd to succeed, so leaving that
/// stale handle in place would make every subsequent `begin`/`commit`
/// silently "succeed" while writing to a file no path-based recovery can
/// ever see. `Poisoned` makes that failure fail CLOSED instead: every write
/// or sync then hard-errors until the process restarts and reloads from
/// disk (poison-until-restart; no automatic self-heal — a fresh compaction
/// is never attempted again once poisoned, since `begin`/`commit`/`flush`
/// all error out before reaching `maybe_compact_locked`).
#[derive(Debug)]
enum AppendState {
    /// The in-memory / empty-path tracker — every write/sync is a no-op.
    InMemory,
    /// A real tracker with a healthy, open `O_APPEND|O_WRONLY|O_CREAT`
    /// handle, opened once in `load` (and again after each successful
    /// compaction) so `begin`/`commit` never pay a fresh `open()` on the
    /// hot path.
    Active(std::fs::File),
    /// A compaction's post-rename reopen failed; the string is the
    /// captured cause. See the type-level doc above.
    Poisoned(String),
}

#[derive(Debug)]
struct ReplicationIntentInner {
    /// Each pending range carries the EXACT key set of the RPC that recorded
    /// it. Recovery replays only those keys from the merged redo window, so a
    /// foreign op whose sequence interleaved into the range is never re-shipped
    /// (the latent wrong-apply vector when a `ReplicaOp` is non-idempotent).
    pending: BTreeMap<ReplicationIntentRange, Vec<TxKey>>,
    commit_dirty: bool,
    last_flush: Instant,
    dirty_commit_count: u32,
    /// Durability state of the append handle — see [`AppendState`].
    append_state: AppendState,
    /// Records appended to the log since the last `SNAPSHOT` (the record
    /// count found at load time, or the count reset by the last
    /// compaction). Drives the compaction trigger.
    records_since_snapshot: u32,
    /// Encoded `COMMIT` frames not yet written to the append handle,
    /// preserving the amortized-commit contract: a `write()` (not just its
    /// `fdatasync`) stays off-disk until the existing time/count threshold
    /// trips (or the next `begin`), so a plain reload without an
    /// intervening flush still observes the pre-commit (stale) state — a
    /// lost buffered commit on an actual crash just leaves a stale range
    /// that recovery replays idempotently, unchanged from the pre-R12
    /// contract.
    unflushed_commit_frames: Vec<u8>,
    /// Test-only fault injection (R12 review): when `true`, the NEXT
    /// `compact_locked` call fails its post-rename reopen with a synthetic
    /// error instead of actually reopening the file — exercising the
    /// fail-closed poisoning path without needing real fd exhaustion.
    /// One-shot: consumed (reset to `false`) on use.
    #[cfg(test)]
    force_reopen_failure: bool,
}

/// Encode one record frame: `[payload_len:4 LE][crc32:4 LE][type:1][payload]`.
/// `payload_len` = `1 + payload.len()` (covers `type` + `payload`); the CRC
/// covers the same `type ++ payload` bytes.
fn intent_log_encode_frame(record_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(1 + payload.len());
    body.push(record_type);
    body.extend_from_slice(payload);
    let crc = crc32fast::hash(&body);
    let payload_len = body.len() as u32;
    let mut frame = Vec::with_capacity(8 + body.len());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// Parse `data` into `(record_type, payload)` frames, stopping — without
/// erroring — at the first frame that is torn (EOF before a full frame),
/// has an implausible length, or fails its CRC check. This is the standard
/// WAL torn-tail contract: a crash mid-append leaves at most one partial
/// trailing record, and everything before it remains valid and ordered.
fn intent_log_parse_frames(data: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut records = Vec::new();
    let mut pos = 0usize;
    loop {
        if pos + 8 > data.len() {
            break;
        }
        let payload_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap_or([0; 4]));
        let crc_stored = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap_or([0; 4]));
        if payload_len == 0 || payload_len > INTENT_LOG_MAX_FRAME_PAYLOAD_LEN {
            break;
        }
        let body_start = pos + 8;
        let body_end = body_start + payload_len as usize;
        if body_end > data.len() {
            break;
        }
        let body = &data[body_start..body_end];
        if crc32fast::hash(body) != crc_stored {
            break;
        }
        records.push((body[0], body[1..].to_vec()));
        pos = body_end;
    }
    records
}

/// Encode a `[first:8][last:8][key_count:4][txid:32]{key_count}` body —
/// shared by the `BEGIN` record payload and each entry of a `SNAPSHOT`
/// payload.
fn intent_log_encode_range_and_keys(range: &ReplicationIntentRange, keys: &[TxKey]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(20 + keys.len() * 32);
    buf.extend_from_slice(&range.first_sequence.to_le_bytes());
    buf.extend_from_slice(&range.last_sequence.to_le_bytes());
    buf.extend_from_slice(&(keys.len() as u32).to_le_bytes());
    for key in keys {
        buf.extend_from_slice(&key.txid);
    }
    buf
}

/// Encode a `SNAPSHOT` record payload: `[range_count:4]
/// ( [first:8][last:8][key_count:4][txid:32]{key_count} )*`.
fn intent_log_encode_snapshot_payload(
    pending: &BTreeMap<ReplicationIntentRange, Vec<TxKey>>,
) -> Vec<u8> {
    let total_keys: usize = pending.values().map(Vec::len).sum();
    let mut buf = Vec::with_capacity(4 + pending.len() * 20 + total_keys * 32);
    buf.extend_from_slice(&(pending.len() as u32).to_le_bytes());
    for (range, keys) in pending {
        buf.extend_from_slice(&intent_log_encode_range_and_keys(range, keys));
    }
    buf
}

/// Decode a `BEGIN` (or one `SNAPSHOT` entry's) `[first:8][last:8]
/// [key_count:4][txid:32]{key_count}` body. A structurally invalid decode
/// (bad range, or a key section whose length disagrees with its declared
/// count) is a hard error — the frame's CRC already validated these exact
/// bytes, so this cannot be a torn-tail write.
fn intent_log_decode_range_and_keys(
    payload: &[u8],
) -> std::result::Result<(ReplicationIntentRange, Vec<TxKey>), ReplicationIntentError> {
    if payload.len() < 20 {
        return Err(ReplicationIntentError::Corrupt(
            "truncated begin record".into(),
        ));
    }
    let first_sequence = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let last_sequence = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    if first_sequence == 0 || last_sequence < first_sequence {
        return Err(ReplicationIntentError::Corrupt(format!(
            "invalid range {first_sequence}..{last_sequence}",
        )));
    }
    let key_count = u32::from_le_bytes(payload[16..20].try_into().unwrap()) as usize;
    let keys_bytes = key_count
        .checked_mul(32)
        .ok_or_else(|| ReplicationIntentError::Corrupt("key count overflow".into()))?;
    if 20 + keys_bytes != payload.len() {
        return Err(ReplicationIntentError::Corrupt(
            "truncated intent keys".into(),
        ));
    }
    let mut keys = Vec::with_capacity(key_count);
    let mut pos = 20;
    for _ in 0..key_count {
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&payload[pos..pos + 32]);
        pos += 32;
        keys.push(TxKey { txid });
    }
    Ok((
        ReplicationIntentRange {
            first_sequence,
            last_sequence,
        },
        keys,
    ))
}

/// Decode a `SNAPSHOT` record payload. On replay the caller REPLACES its
/// whole reconstructed map with the result (reset semantics).
fn intent_log_decode_snapshot_payload(
    payload: &[u8],
) -> std::result::Result<BTreeMap<ReplicationIntentRange, Vec<TxKey>>, ReplicationIntentError> {
    if payload.len() < 4 {
        return Err(ReplicationIntentError::Corrupt(
            "truncated snapshot header".into(),
        ));
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let mut pending = BTreeMap::new();
    let mut pos = 4;
    for _ in 0..count {
        if pos + 20 > payload.len() {
            return Err(ReplicationIntentError::Corrupt(
                "truncated snapshot ranges".into(),
            ));
        }
        let first_sequence = u64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
        let last_sequence = u64::from_le_bytes(payload[pos + 8..pos + 16].try_into().unwrap());
        if first_sequence == 0 || last_sequence < first_sequence {
            return Err(ReplicationIntentError::Corrupt(format!(
                "invalid range {first_sequence}..{last_sequence}",
            )));
        }
        let key_count =
            u32::from_le_bytes(payload[pos + 16..pos + 20].try_into().unwrap()) as usize;
        pos += 20;
        let keys_bytes = key_count
            .checked_mul(32)
            .ok_or_else(|| ReplicationIntentError::Corrupt("key count overflow".into()))?;
        if pos + keys_bytes > payload.len() {
            return Err(ReplicationIntentError::Corrupt(
                "truncated snapshot keys".into(),
            ));
        }
        let mut keys = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            let mut txid = [0u8; 32];
            txid.copy_from_slice(&payload[pos..pos + 32]);
            pos += 32;
            keys.push(TxKey { txid });
        }
        pending.insert(
            ReplicationIntentRange {
                first_sequence,
                last_sequence,
            },
            keys,
        );
    }
    Ok(pending)
}

/// Decode a `COMMIT` record payload: `[first:8][last:8]`.
fn intent_log_decode_commit_payload(
    payload: &[u8],
) -> std::result::Result<ReplicationIntentRange, ReplicationIntentError> {
    if payload.len() != 16 {
        return Err(ReplicationIntentError::Corrupt(
            "malformed commit record".into(),
        ));
    }
    let first_sequence = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let last_sequence = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    Ok(ReplicationIntentRange {
        first_sequence,
        last_sequence,
    })
}

/// Apply one decoded record to the in-progress reconstructed map:
/// `SNAPSHOT` resets it, `BEGIN` inserts/overwrites, `COMMIT` removes.
fn intent_log_apply_record(
    pending: &mut BTreeMap<ReplicationIntentRange, Vec<TxKey>>,
    record_type: u8,
    payload: &[u8],
) -> std::result::Result<(), ReplicationIntentError> {
    match record_type {
        INTENT_RECORD_SNAPSHOT => {
            *pending = intent_log_decode_snapshot_payload(payload)?;
        }
        INTENT_RECORD_BEGIN => {
            let (range, keys) = intent_log_decode_range_and_keys(payload)?;
            pending.insert(range, keys);
        }
        INTENT_RECORD_COMMIT => {
            let range = intent_log_decode_commit_payload(payload)?;
            pending.remove(&range);
        }
        other => {
            return Err(ReplicationIntentError::Corrupt(format!(
                "unknown intent record type {other}",
            )));
        }
    }
    Ok(())
}

impl ReplicationIntentTracker {
    pub fn load(path: PathBuf) -> std::result::Result<Self, ReplicationIntentError> {
        ensure_parent_dir(&path).map_err(ReplicationIntentError::Io)?;
        let (pending, record_count) = Self::read_from_disk(&path)?;
        let append_file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .map_err(ReplicationIntentError::Io)?;
        Ok(Self {
            path,
            inner: Mutex::new(ReplicationIntentInner {
                pending,
                commit_dirty: false,
                last_flush: Instant::now(),
                dirty_commit_count: 0,
                append_state: AppendState::Active(append_file),
                records_since_snapshot: record_count,
                unflushed_commit_frames: Vec::new(),
                #[cfg(test)]
                force_reopen_failure: false,
            }),
        })
    }

    pub fn in_memory() -> Self {
        Self {
            path: PathBuf::new(),
            inner: Mutex::new(ReplicationIntentInner {
                pending: BTreeMap::new(),
                commit_dirty: false,
                last_flush: Instant::now(),
                dirty_commit_count: 0,
                append_state: AppendState::InMemory,
                records_since_snapshot: 0,
                unflushed_commit_frames: Vec::new(),
                #[cfg(test)]
                force_reopen_failure: false,
            }),
        }
    }

    /// Record a pending replication intent for the redo range
    /// `[first_sequence, last_sequence]` together with the EXACT key set
    /// (`keys`) of the RPC that produced it.
    ///
    /// Duplicate keys are removed before storage. An empty `keys` slice is
    /// still recorded (the range is preserved); recovery commits it as a no-op
    /// since there is nothing keyed to replay. Invalid ranges
    /// (`first_sequence == 0` or `last_sequence < first_sequence`) are ignored,
    /// matching the prior contract.
    ///
    /// On a real (non-empty path) tracker, a single `BEGIN` record is
    /// appended and `fdatasync`'d before returning — this is the durability
    /// barrier: a crash immediately after `begin` returns `Ok` MUST recover
    /// this range. Errors surface I/O failures from that append/sync.
    pub fn begin(
        &self,
        first_sequence: u64,
        last_sequence: u64,
        keys: &[TxKey],
    ) -> std::result::Result<(), ReplicationIntentError> {
        if first_sequence == 0 || last_sequence < first_sequence {
            return Ok(());
        }
        let range = ReplicationIntentRange {
            first_sequence,
            last_sequence,
        };
        let mut deduped: Vec<TxKey> = Vec::with_capacity(keys.len());
        let mut seen: BTreeSet<[u8; 32]> = BTreeSet::new();
        for key in keys {
            if seen.insert(key.txid) {
                deduped.push(*key);
            }
        }
        let mut inner = self.inner.lock();
        // Re-recording an identical range overwrites its key set: a begin for a
        // range already pending is idempotent in identity but the caller's key
        // set is authoritative. (Ranges are globally-unique per RPC in
        // practice; this keeps semantics defined if a range ever repeats.)
        let changed = match inner.pending.get(&range) {
            Some(existing) => existing != &deduped,
            None => true,
        };
        if changed {
            let payload = intent_log_encode_range_and_keys(&range, &deduped);
            inner.pending.insert(range, deduped);
            // Drain any buffered (not-yet-written) COMMIT frames first: the
            // old full-rewrite always incorporated committed-away ranges on
            // every begin, so the append-only equivalent is to write those
            // out before this BEGIN record, then fdatasync everything
            // together as one durability barrier.
            Self::drain_unflushed_commits_locked(&mut inner)?;
            self.append_record_locked(&mut inner, INTENT_RECORD_BEGIN, &payload)?;
            Self::sync_locked(&mut inner)?;
            inner.commit_dirty = false;
            inner.dirty_commit_count = 0;
            inner.last_flush = Instant::now();
            self.maybe_compact_locked(&mut inner)?;
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
        let changed = inner
            .pending
            .remove(&ReplicationIntentRange {
                first_sequence,
                last_sequence,
            })
            .is_some();
        if changed {
            if self.path.as_os_str().is_empty() {
                return Ok(());
            }
            // Buffer the COMMIT record's frame rather than writing it now:
            // preserves the amortized contract that an unflushed commit
            // stays entirely invisible on disk (not merely unsynced) until
            // the existing time/count threshold — or a subsequent `begin` —
            // forces it out.
            let mut payload = Vec::with_capacity(16);
            payload.extend_from_slice(&first_sequence.to_le_bytes());
            payload.extend_from_slice(&last_sequence.to_le_bytes());
            let frame = intent_log_encode_frame(INTENT_RECORD_COMMIT, &payload);
            inner.unflushed_commit_frames.extend_from_slice(&frame);
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

    /// Snapshot of the pending ranges (without their key sets) in ascending
    /// order. Retained for back-compat / diagnostics; recovery uses
    /// [`pending_with_keys`](Self::pending_with_keys).
    pub fn pending(&self) -> Vec<ReplicationIntentRange> {
        let inner = self.inner.lock();
        inner.pending.keys().copied().collect()
    }

    /// Snapshot of every pending range paired with the exact key set recorded
    /// for it, in ascending range order. Recovery replays each range's redo
    /// window filtered to these keys.
    pub fn pending_with_keys(&self) -> Vec<(ReplicationIntentRange, Vec<TxKey>)> {
        let inner = self.inner.lock();
        inner
            .pending
            .iter()
            .map(|(range, keys)| (*range, keys.clone()))
            .collect()
    }

    pub fn flush(&self) -> std::result::Result<(), ReplicationIntentError> {
        let mut inner = self.inner.lock();
        if inner.commit_dirty {
            self.flush_locked(&mut inner)?;
        }
        Ok(())
    }

    /// Append one record's frame to the log via the held handle. No-op for
    /// the in-memory tracker. Hard `Err(Poisoned)` — WITHOUT touching the
    /// filesystem — if a prior compaction reopen failed (see
    /// [`AppendState::Poisoned`]): the stale pre-compaction handle must
    /// never be written to again.
    fn append_record_locked(
        &self,
        inner: &mut ReplicationIntentInner,
        record_type: u8,
        payload: &[u8],
    ) -> std::result::Result<(), ReplicationIntentError> {
        let file = match &mut inner.append_state {
            AppendState::InMemory => return Ok(()),
            AppendState::Poisoned(cause) => {
                return Err(ReplicationIntentError::Poisoned(cause.clone()));
            }
            AppendState::Active(file) => file,
        };
        let frame = intent_log_encode_frame(record_type, payload);
        file.write_all(&frame).map_err(ReplicationIntentError::Io)?;
        inner.records_since_snapshot = inner.records_since_snapshot.saturating_add(1);
        Ok(())
    }

    /// `fdatasync` the append handle. No-op for the in-memory tracker.
    /// Hard `Err(Poisoned)` if a prior compaction reopen failed — see
    /// [`append_record_locked`](Self::append_record_locked).
    fn sync_locked(
        inner: &mut ReplicationIntentInner,
    ) -> std::result::Result<(), ReplicationIntentError> {
        match &inner.append_state {
            AppendState::InMemory => Ok(()),
            AppendState::Poisoned(cause) => Err(ReplicationIntentError::Poisoned(cause.clone())),
            AppendState::Active(file) => file.sync_data().map_err(ReplicationIntentError::Io),
        }
    }

    /// Write out any buffered `COMMIT` frames accumulated by `commit()`
    /// (does not sync — callers fsync afterward). No-op if nothing is
    /// buffered or the tracker is in-memory. Hard `Err(Poisoned)` if a
    /// prior compaction reopen failed — see
    /// [`append_record_locked`](Self::append_record_locked).
    fn drain_unflushed_commits_locked(
        inner: &mut ReplicationIntentInner,
    ) -> std::result::Result<(), ReplicationIntentError> {
        if inner.unflushed_commit_frames.is_empty() {
            return Ok(());
        }
        match &mut inner.append_state {
            AppendState::InMemory => {}
            AppendState::Poisoned(cause) => {
                return Err(ReplicationIntentError::Poisoned(cause.clone()));
            }
            AppendState::Active(file) => {
                // Invariant this accounting relies on: `dirty_commit_count`
                // at this point equals exactly the number of buffered
                // frames we are about to write (i.e. "frames just
                // drained"). That holds only because every call site that
                // resets `dirty_commit_count` to 0 (`begin`, `flush_locked`)
                // does so AFTER calling this drain in the same invocation —
                // never before, and never without draining first. A future
                // refactor that reset `dirty_commit_count` (or repopulated
                // `unflushed_commit_frames`) out of that order would
                // silently corrupt the `records_since_snapshot` count that
                // drives the compaction trigger.
                debug_assert!(
                    inner.dirty_commit_count > 0,
                    "unflushed_commit_frames is non-empty but dirty_commit_count == 0 — \
                     every buffered COMMIT frame push increments dirty_commit_count in \
                     lockstep (see `commit()`), so this violates the invariant the \
                     records_since_snapshot accounting below depends on",
                );
                file.write_all(&inner.unflushed_commit_frames)
                    .map_err(ReplicationIntentError::Io)?;
                inner.records_since_snapshot = inner
                    .records_since_snapshot
                    .saturating_add(inner.dirty_commit_count);
            }
        }
        inner.unflushed_commit_frames.clear();
        Ok(())
    }

    fn flush_locked(
        &self,
        inner: &mut ReplicationIntentInner,
    ) -> std::result::Result<(), ReplicationIntentError> {
        Self::drain_unflushed_commits_locked(inner)?;
        Self::sync_locked(inner)?;
        inner.commit_dirty = false;
        inner.dirty_commit_count = 0;
        inner.last_flush = Instant::now();
        self.maybe_compact_locked(inner)?;
        Ok(())
    }

    /// Rewrite the log as a single fresh `SNAPSHOT` once it has grown past
    /// [`INTENT_LOG_COMPACT_RECORD_THRESHOLD`] records since the last one.
    /// No-op for the in-memory tracker.
    fn maybe_compact_locked(
        &self,
        inner: &mut ReplicationIntentInner,
    ) -> std::result::Result<(), ReplicationIntentError> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }
        if inner.records_since_snapshot < INTENT_LOG_COMPACT_RECORD_THRESHOLD {
            return Ok(());
        }
        self.compact_locked(inner)
    }

    /// Atomically rewrite the log file as one `SNAPSHOT` record of the
    /// current in-memory `pending` state (temp-write + `sync_all` + rename +
    /// parent-dir fsync — the same pattern [`write_durable_file`] uses
    /// elsewhere), then reopen the append handle on the fresh file.
    ///
    /// R12 review (Critical, fail-closed fix): by the time the reopen below
    /// runs, the `SNAPSHOT` has already been durably renamed into place. If
    /// the reopen itself then fails, the OLD handle in `inner.append_state`
    /// is left pointing at that renamed-away file's now-unlinked inode —
    /// POSIX permits `write()`/`fsync()` on an unlinked fd to succeed, so
    /// leaving it in place would make every subsequent `begin`/`commit`
    /// silently "succeed" while writing to a file no recovery can ever see.
    /// Poison the tracker instead (dropping the stale handle) so every
    /// later write/sync hard-errors — see [`AppendState`].
    fn compact_locked(
        &self,
        inner: &mut ReplicationIntentInner,
    ) -> std::result::Result<(), ReplicationIntentError> {
        let payload = intent_log_encode_snapshot_payload(&inner.pending);
        let frame = intent_log_encode_frame(INTENT_RECORD_SNAPSHOT, &payload);
        write_durable_file(&self.path, &frame).map_err(ReplicationIntentError::Io)?;
        // `rename` (inside `write_durable_file`) does not redirect an
        // already-open append fd to the new inode — reopen on `self.path` so
        // subsequent appends land in the file that now exists there.
        #[cfg(test)]
        let reopened = if inner.force_reopen_failure {
            inner.force_reopen_failure = false; // one-shot
            Err(std::io::Error::other(
                "injected compaction reopen failure (test)",
            ))
        } else {
            std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.path)
        };
        #[cfg(not(test))]
        let reopened = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path);
        let file = match reopened {
            Ok(file) => file,
            Err(e) => {
                let cause = format!("intent log compaction reopen failed: {e}");
                // Replace (dropping/closing) the stale pre-compaction
                // handle rather than leaving it as the live append target —
                // fail CLOSED, not silently no-op (which `None` would mean
                // to `append_record_locked`/`sync_locked`).
                inner.append_state = AppendState::Poisoned(cause.clone());
                // Observability follow-up: this transition previously had
                // no dedicated signal — the poison only surfaced later,
                // indirectly, the next time a caller's begin/commit
                // returned Err(Poisoned). Emit a loud ERROR at the
                // transition itself and bump a counter so operators don't
                // have to wait for (or scrape logs for) the next write.
                tracing::error!(
                    cause = %cause,
                    "replication intent log POISONED — durability barrier lost (compaction \
                     reopen failed); begin/commit will fail until restart",
                );
                if let Some(m) = crate::metrics::replication_metrics() {
                    m.intent_log_poisoned.inc();
                }
                return Err(ReplicationIntentError::Io(e));
            }
        };
        inner.append_state = AppendState::Active(file);
        inner.records_since_snapshot = 1; // the SNAPSHOT record just written
        inner.commit_dirty = false;
        inner.dirty_commit_count = 0;
        inner.last_flush = Instant::now();
        Ok(())
    }

    /// Read + parse the on-disk log, applying records in order to
    /// reconstruct the pending map. Returns the map together with the
    /// number of valid records applied, which seeds `records_since_snapshot`
    /// for the freshly loaded tracker (compaction is not triggered here —
    /// the next `begin`/`commit` will trip it if the loaded file was
    /// already over threshold). An absent file yields an empty map.
    fn read_from_disk(
        path: &Path,
    ) -> std::result::Result<
        (BTreeMap<ReplicationIntentRange, Vec<TxKey>>, u32),
        ReplicationIntentError,
    > {
        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok((BTreeMap::new(), 0));
            }
            Err(e) => return Err(ReplicationIntentError::Io(e)),
        };
        if data.is_empty() {
            return Ok((BTreeMap::new(), 0));
        }
        let frames = intent_log_parse_frames(&data);
        let mut pending = BTreeMap::new();
        for (record_type, payload) in &frames {
            intent_log_apply_record(&mut pending, *record_type, payload)?;
        }
        Ok((pending, frames.len() as u32))
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
/// Returns `Ok(through_redo_seq)` on success: the redo sequence of the last
/// **fully-included** entry in this pass. The per-pass op budget
/// (`max_ops_per_pass`) is applied at redo-ENTRY granularity, never at the
/// level of the flattened `ReplicaOp` list: whole entries are accumulated
/// in order, and the pass stops before an entry that would push the
/// running op count over budget — but only once at least one entry has
/// already been accumulated. A single entry whose own expansion alone
/// exceeds the budget still ships whole (a multi-txid `SetMinedBatch`
/// expansion is atomic and must never be split across passes). This
/// matters because one redo sequence can expand to N `ReplicaOp`s (e.g. a
/// multi-txid `SetMinedBatch`): a prior version derived the watermark as
/// `from_seq + ops_sent - 1` and truncated the flattened op list, which
/// could both split a batch's expansion mid-entry and over-report how many
/// redo sequences were actually fully sent — silently advancing the
/// replica's ACK past ops that were never delivered. Callers record the
/// returned watermark against the [`AckTracker`] and resume the next pass
/// at `watermark + 1`. Failure modes are the typed [`CatchupError`]
/// variants; callers that dispatch on "redo wrapped — request a full
/// resync" MUST `match` on `RedoReclaimed` rather than substring-matching
/// the rendered message — see `bin/server.rs` for the canonical pattern.
///
/// The `ops_from_seq` callback should read redo entries starting at the
/// given sequence and return one `(sequence, ReplicaOps)` group per entry,
/// in ascending sequence order — the group's ops is the entry's full
/// `ReplicaOp` expansion (possibly empty, e.g. for entries that carry no
/// replicated mutation). It returns an empty vec when the entries have
/// been reclaimed (circular redo log wrapped).
///
/// The `first_available_seq` callback returns the sequence number of the
/// earliest available redo entry, or `None` if the log is empty. Used to
/// detect redo log truncation: if the earliest entry is beyond `from_seq`,
/// the log has wrapped and a full resync is required instead.
// Catch-up driver: the sequence-range/batching scalars and the two distinct
// closures (read-ops-from-seq and send-chunk) are independent inputs the caller
// supplies separately; bundling them into a struct would not reduce the
// genuine parameter count, so the allow stands.
#[allow(clippy::too_many_arguments)]
pub fn run_catchup_for_replica(
    addr: &std::net::SocketAddr,
    from_seq: u64,
    current_seq: u64,
    batch_size: usize,
    max_ops_per_pass: usize,
    ops_from_seq: &dyn Fn(u64) -> Vec<(u64, Vec<ReplicaOp>)>,
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

    let entries = ops_from_seq(from_seq);
    if entries.is_empty() {
        return Err(CatchupError::RedoReclaimed {
            from: from_seq,
            available: first_available_seq,
        });
    }

    // Accumulate whole entries (each entry = one redo sequence's full
    // `ReplicaOp` expansion) up to the `max_ops_per_pass` budget. Never
    // split a single entry's expansion across passes: the first entry
    // accumulated is always included in full — even if it alone exceeds
    // the budget — because a batch must ship atomically and the cap is
    // only a soft budget once at least one entry has already been taken.
    let max_ops_per_pass = max_ops_per_pass.max(1);
    let mut ops: Vec<ReplicaOp> = Vec::new();
    let mut op_count: usize = 0;
    let mut through_seq: Option<u64> = None;
    for (seq, entry_ops) in entries {
        let would_be = op_count + entry_ops.len();
        if through_seq.is_some() && would_be > max_ops_per_pass {
            break;
        }
        op_count = would_be;
        ops.extend(entry_ops);
        through_seq = Some(seq);
    }
    // `entries` was checked non-empty above, and the budget check above
    // only ever triggers `break` after `through_seq` is already `Some`
    // (the first entry is always accumulated unconditionally), so
    // `through_seq` is always set here. `unwrap_or` (rather than
    // `unwrap`/`expect`, banned in library code) keeps this branch total
    // without ever actually being reached.
    let through_seq = through_seq.unwrap_or(from_seq);

    let batch_size = batch_size.max(1);
    for chunk in ops.chunks(batch_size) {
        send_chunk(chunk).map_err(|detail| CatchupError::Transport {
            addr: *addr,
            detail,
        })?;
    }

    Ok(through_seq)
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
// Thread-spawn entry point: arguments are independent runtime knobs (tracker,
// current-seq fn, shutdown flag, interval, two thresholds, optional callback)
// configured separately by the caller; they have no cohesive grouping, so the
// count is warranted.
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

    /// Reverse-heal Phase 1 Tier-1 (finding C1): a downstream replica's
    /// durably-persisted ACK that survives recovery ABOVE the recovered
    /// `shared_sequence_floor` proves this node acked a write it no longer
    /// holds — the detector must fire. An ACK at or below the floor is covered
    /// by the recovered log and must NOT fire.
    #[test]
    fn recovery_detects_lost_acked_tail_when_acktracker_ahead_of_fence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");

        // A master durably ACKs a downstream replica through master-global
        // sequence 100 (INCLUSIVE — 100 is the highest redo seq it told the
        // client was durable), then "crashes".
        {
            let tracker = AckTracker::new(path.clone());
            tracker.record_ack(test_addr(5000), 100);
            tracker.flush();
        }

        // Recovery reloads the persisted tracker. `floor` is the recovered
        // `shared_sequence_floor` = `next_sequence` = highest-durable + 1: the
        // EXCLUSIVE next-to-assign sequence. Because the ACK is inclusive and
        // the floor is exclusive, a lost acked tail exists iff `acked >= floor`.
        let recovered = AckTracker::new(path);

        // floor 50 well below the ACK → lost acked tail (fires).
        let lost = recovered.acked_beyond(50);
        assert_eq!(lost.len(), 1, "replica acked 100 >= floor 50 → lost tail");
        assert_eq!(lost[0], (test_addr(5000), 100));

        // floor == acked → the DEPTH-1 lost tail (the modal crash): the master
        // acked 100 but recovered only through 99 (next_sequence = 100). It
        // returned STATUS_OK for op 100 yet can no longer prove it holds it, so
        // this MUST fire — inclusive ACK vs exclusive floor.
        let depth_one = recovered.acked_beyond(100);
        assert_eq!(
            depth_one.len(),
            1,
            "acked == floor is a depth-1 lost acked tail, not covered",
        );
        assert_eq!(depth_one[0], (test_addr(5000), 100));

        // floor == acked + 1 → op 100 was recovered (next_sequence = 101) →
        // nothing lost. This is the no-loss case; it must NOT false-positive.
        assert!(
            recovered.acked_beyond(101).is_empty(),
            "floor one past the ACK → op 100 durable, nothing lost",
        );
    }

    /// Reverse-heal Tier-1 depth-1 regression (P1-1): the ACK stored is
    /// INCLUSIVE (highest redo seq covered) while boot passes an EXCLUSIVE floor
    /// (`shared_sequence_floor` = `next_sequence` = highest-durable + 1). The
    /// modal crash loses exactly the last acked op: master acks seq N, crashes
    /// before N is durable, recovers `next_sequence = N`, so `floor == acked ==
    /// N`. Pre-fix `acked_beyond` used `seq > floor` and MISSED this (`N > N` is
    /// false), silently accepting the loss of an op the client was told was
    /// durable. The detector must fire at `acked == floor`.
    #[test]
    fn acked_beyond_flags_depth_one_lost_tail() {
        let dir = tempfile::tempdir().unwrap();
        let tracker = AckTracker::new(dir.path().join("ack.dat"));
        let replica = test_addr(5100);

        // Master acked op 100; recovery restored next_sequence = 100.
        tracker.record_ack(replica, 100);
        let floor = 100u64;

        assert_eq!(
            tracker.acked_beyond(floor),
            vec![(replica, 100)],
            "acked == exclusive floor (depth-1 lost tail) must fire",
        );

        // One deeper: op 100 was recovered (next_sequence = 101) → nothing lost.
        assert!(
            tracker.acked_beyond(101).is_empty(),
            "acked strictly below the floor → covered, must not fire",
        );
    }

    /// Reverse-heal Phase 1 NEGATIVE: a node whose every replica ACK is at or
    /// below the recovered floor (and a fresh/empty tracker) must not fire the
    /// Tier-1 detector — no false positive.
    #[test]
    fn recovery_no_lost_tail_does_not_fire_detector() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        let tracker = AckTracker::new(path);
        tracker.record_ack(test_addr(5000), 40);
        tracker.record_ack(test_addr(5001), 50);
        // Exclusive floor 51: every replica ACK is strictly below it, so every
        // acked op was recovered → the detector must not fire.
        assert!(
            tracker.acked_beyond(51).is_empty(),
            "no replica acked at or beyond the floor → detector must not fire",
        );

        let empty = AckTracker::new(dir.path().join("empty.dat"));
        assert!(
            empty.acked_beyond(0).is_empty(),
            "empty tracker never fires the detector",
        );
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
            1,        // 1s interval
            u64::MAX, // suppress warn lines
            10,       // catch-up threshold: 10 ops
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

    /// F-D1: a truncated/corrupt ACK file must NOT be parsed into a partial
    /// map (which could carry stale-high watermarks that mask replica lag). The
    /// tracker fails closed: it discards the file, starts empty (forcing full
    /// idempotent catch-up), and bumps `ack_tracker_load_failures`.
    #[test]
    fn corrupt_ack_file_is_discarded_and_starts_empty() {
        static TEST_METRICS: std::sync::OnceLock<&'static crate::metrics::ReplicationMetrics> =
            std::sync::OnceLock::new();
        let metrics_ref = *TEST_METRICS
            .get_or_init(|| Box::leak(Box::new(crate::metrics::ReplicationMetrics::new())));
        crate::metrics::init_replication_metrics(metrics_ref);
        let metrics =
            crate::metrics::replication_metrics().expect("replication metrics installed for test");

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");

        // Write a well-formed file first (2 entries), then truncate it mid-entry
        // so it claims a count it cannot satisfy.
        let good = AckTracker::new(path.clone());
        good.record_ack(test_addr(5000), 42);
        good.record_ack(test_addr(5001), 99);
        good.flush();
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() > 6);
        std::fs::write(&path, &bytes[..bytes.len() - 4]).unwrap(); // chop a tail entry

        let before = metrics.ack_tracker_load_failures.get();
        let reopened = AckTracker::new(path);
        let after = metrics.ack_tracker_load_failures.get();

        // Fail-closed: empty map (NOT a partial parse of the surviving entry).
        assert_eq!(
            reopened.all_acked().len(),
            0,
            "corrupt ACK file must yield an EMPTY tracker, not a partial map",
        );
        assert!(
            after > before,
            "ack_tracker_load_failures must bump on a corrupt load (was {before}, now {after})",
        );
    }

    #[test]
    fn truncated_ack_header_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ack.dat");
        std::fs::write(&path, [0x01, 0x02]).unwrap(); // < 4 bytes: truncated header
        let tracker = AckTracker::new(path);
        assert_eq!(tracker.all_acked().len(), 0);
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
            tracker.begin(10, 12, &[]).unwrap();
            tracker.begin(20, 20, &[]).unwrap();
            tracker.begin(0, 2, &[]).unwrap();
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
            tracker.begin(seq, seq, &[]).unwrap();
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

        tracker.begin(5, 7, &[]).unwrap();
        tracker.begin(5, 7, &[]).unwrap();
        tracker.begin(8, 7, &[]).unwrap();
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

        tracker.begin(5, 7, &[]).unwrap();

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

        tracker.begin(5, 7, &[]).unwrap();

        assert!(path.exists());
        assert!(!durable_tmp_path(&path).exists());
    }

    #[test]
    fn replication_intent_tracker_corrupt_range_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        let mut payload = Vec::new();
        payload.extend_from_slice(&9u64.to_le_bytes()); // first
        payload.extend_from_slice(&8u64.to_le_bytes()); // last < first → invalid
        payload.extend_from_slice(&0u32.to_le_bytes()); // key_count
        let frame = intent_log_encode_frame(INTENT_RECORD_BEGIN, &payload);
        std::fs::write(&path, frame).unwrap();

        let err = ReplicationIntentTracker::load(path).expect_err("invalid range should reject");
        match err {
            ReplicationIntentError::Corrupt(msg) => assert!(msg.contains("invalid range")),
            other => panic!("expected Corrupt, got {other:?}"),
        }
    }

    fn intent_key(b: u8) -> TxKey {
        TxKey { txid: [b; 32] }
    }

    #[test]
    fn replication_intent_tracker_key_set_round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        let k_a = intent_key(0xAA);
        let k_b = intent_key(0xBB);

        {
            let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();
            // Range 10..12 owns two keys (with a duplicate to exercise dedup);
            // range 20..20 owns one key.
            tracker.begin(10, 12, &[k_a, k_b, k_a]).unwrap();
            tracker.begin(20, 20, &[k_b]).unwrap();
        }

        let reopened = ReplicationIntentTracker::load(path.clone()).unwrap();
        let with_keys = reopened.pending_with_keys();
        assert_eq!(
            with_keys,
            vec![
                (
                    ReplicationIntentRange {
                        first_sequence: 10,
                        last_sequence: 12,
                    },
                    vec![k_a, k_b],
                ),
                (
                    ReplicationIntentRange {
                        first_sequence: 20,
                        last_sequence: 20,
                    },
                    vec![k_b],
                ),
            ],
            "begin → write_to_disk → read_from_disk must round-trip the exact \
             (range, deduped key set) pairs"
        );

        // `pending()` (back-compat) returns the same ranges without keys.
        assert_eq!(
            reopened.pending(),
            vec![
                ReplicationIntentRange {
                    first_sequence: 10,
                    last_sequence: 12,
                },
                ReplicationIntentRange {
                    first_sequence: 20,
                    last_sequence: 20,
                },
            ],
        );

        // commit removes by range identity and the removal persists across a
        // flush + reopen.
        reopened.commit(10, 12).unwrap();
        reopened.flush().unwrap();
        let after_commit = ReplicationIntentTracker::load(path).unwrap();
        assert_eq!(
            after_commit.pending_with_keys(),
            vec![(
                ReplicationIntentRange {
                    first_sequence: 20,
                    last_sequence: 20,
                },
                vec![k_b],
            )],
        );
    }

    #[test]
    fn replication_intent_tracker_empty_key_set_is_recorded() {
        // An intent with no keys must still be recorded so its range is not lost
        // (recovery commits it as a no-op). Round-trips with an empty key set.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        {
            let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();
            tracker.begin(5, 7, &[]).unwrap();
        }
        let reopened = ReplicationIntentTracker::load(path).unwrap();
        assert_eq!(
            reopened.pending_with_keys(),
            vec![(
                ReplicationIntentRange {
                    first_sequence: 5,
                    last_sequence: 7,
                },
                vec![],
            )],
        );
    }

    #[test]
    fn replication_intent_tracker_truncated_key_section_rejected() {
        // A valid header + range + key_count=2 but only ONE txid worth of bytes
        // must surface a Corrupt error, not a silent partial read. Note this is
        // NOT the torn-tail case: the frame's own CRC validates exactly these
        // bytes (nothing is missing at the file level), so the mismatch between
        // the declared key_count and the actual payload length is a genuine
        // structural corruption, not crash residue — a hard error is correct.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        let mut payload = Vec::new();
        payload.extend_from_slice(&10u64.to_le_bytes()); // first
        payload.extend_from_slice(&12u64.to_le_bytes()); // last
        payload.extend_from_slice(&2u32.to_le_bytes()); // key_count = 2
        payload.extend_from_slice(&[0xCC; 32]); // only 1 of the 2 promised keys
        let frame = intent_log_encode_frame(INTENT_RECORD_BEGIN, &payload);
        std::fs::write(&path, frame).unwrap();

        let err = ReplicationIntentTracker::load(path)
            .expect_err("truncated key section must be rejected");
        match err {
            ReplicationIntentError::Corrupt(msg) => assert!(
                msg.contains("truncated intent keys"),
                "unexpected corrupt message: {msg}"
            ),
            other => panic!("expected Corrupt, got {other:?}"),
        }
    }

    #[test]
    fn intent_log_begin_is_durable_after_restart() {
        // R12/C32: `begin` must fdatasync a single APPENDED record, not
        // rewrite the whole file. Proven two ways: (1) the bytes written by
        // the first begin remain an untouched PREFIX after the second begin
        // (a full-rewrite implementation would re-serialize the whole map
        // and the prefix would not match byte-for-byte), and (2) both
        // ranges survive a reload with NO explicit `flush()` call.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();

        tracker.begin(10, 12, &[]).unwrap();
        let after_first = std::fs::read(&path).unwrap();

        tracker.begin(20, 20, &[]).unwrap();
        let after_second = std::fs::read(&path).unwrap();

        assert!(
            after_second.starts_with(&after_first),
            "begin must APPEND a new record — the first begin's bytes must survive \
             unmodified as a prefix, not be rewritten",
        );
        assert!(
            after_second.len() > after_first.len(),
            "second begin must grow the file",
        );

        drop(tracker); // no flush() — begin's own fdatasync must already be durable

        let reopened = ReplicationIntentTracker::load(path).unwrap();
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
            "both begins must be durable across restart with no flush() call",
        );
    }

    #[test]
    fn intent_log_torn_tail_record_is_discarded_prefix_recovered() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        {
            let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();
            tracker.begin(10, 12, &[]).unwrap();
            tracker.begin(20, 20, &[]).unwrap();
        }
        let valid_bytes = std::fs::read(&path).unwrap();
        let expected_prefix = vec![
            ReplicationIntentRange {
                first_sequence: 10,
                last_sequence: 12,
            },
            ReplicationIntentRange {
                first_sequence: 20,
                last_sequence: 20,
            },
        ];

        // Case 1: EOF mid-frame — a crash mid-`write_all` of a third BEGIN's
        // frame leaves only part of its bytes on disk.
        let extra_frame = intent_log_encode_frame(
            INTENT_RECORD_BEGIN,
            &intent_log_encode_range_and_keys(
                &ReplicationIntentRange {
                    first_sequence: 30,
                    last_sequence: 30,
                },
                &[],
            ),
        );
        let mut torn = valid_bytes.clone();
        torn.extend_from_slice(&extra_frame[..extra_frame.len() - 3]);
        std::fs::write(&path, &torn).unwrap();

        let recovered = ReplicationIntentTracker::load(path.clone())
            .expect("a torn trailing record must NOT be a hard error");
        assert_eq!(
            recovered.pending(),
            expected_prefix,
            "only the valid prefix must be recovered; the torn record is discarded",
        );

        // Case 2: full-length frame but a corrupted byte inside it — CRC no
        // longer matches. Same outcome: discarded, valid prefix kept, no error.
        let mut crc_corrupt = valid_bytes.clone();
        crc_corrupt.extend_from_slice(&extra_frame);
        let last = crc_corrupt.len() - 1;
        crc_corrupt[last] ^= 0xFF;
        std::fs::write(&path, &crc_corrupt).unwrap();

        let recovered2 = ReplicationIntentTracker::load(path)
            .expect("a CRC-corrupted trailing record must NOT be a hard error");
        assert_eq!(
            recovered2.pending(),
            expected_prefix,
            "CRC mismatch on the trailing record discards it, keeping the valid prefix",
        );
    }

    #[test]
    fn intent_log_commit_removes_range_and_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        {
            let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();
            tracker.begin(5, 7, &[]).unwrap();
            tracker.commit(5, 7).unwrap();
            tracker.flush().unwrap(); // force durability of the COMMIT record
        }
        let reopened = ReplicationIntentTracker::load(path).unwrap();
        assert!(
            reopened.pending().is_empty(),
            "a flushed commit must be durable across restart",
        );

        // Deferred (unflushed) commit: the amortized-commit contract means a
        // lost COMMIT just leaves a stale range that recovery replays
        // idempotently — must NOT be lost silently either way.
        let dir2 = tempfile::tempdir().unwrap();
        let path2 = dir2.path().join("intent.dat");
        {
            let tracker = ReplicationIntentTracker::load(path2.clone()).unwrap();
            tracker.begin(5, 7, &[]).unwrap();
            tracker.commit(5, 7).unwrap(); // below the coalescing threshold — not flushed
        }
        let stale_reopen = ReplicationIntentTracker::load(path2).unwrap();
        assert_eq!(
            stale_reopen.pending(),
            vec![ReplicationIntentRange {
                first_sequence: 5,
                last_sequence: 7
            }],
            "an unflushed commit must leave the range durable (stale, idempotent replay)",
        );
    }

    #[test]
    fn intent_log_compaction_bounds_file_and_preserves_pending() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();

        // Drive well past the compaction threshold with begin/commit pairs:
        // an uncompacted log would accumulate ~1.5x this many records.
        let total = u64::from(INTENT_LOG_COMPACT_RECORD_THRESHOLD) + 50;
        for i in 1..=total {
            tracker.begin(i, i, &[]).unwrap();
            if i % 2 == 0 {
                tracker.commit(i, i).unwrap();
            }
        }
        tracker.flush().unwrap();

        let bytes_after = std::fs::read(&path).unwrap();
        let frames_after = intent_log_parse_frames(&bytes_after);
        assert!(
            frames_after.len() < INTENT_LOG_COMPACT_RECORD_THRESHOLD as usize,
            "compaction must bound the log well under the threshold — got {} records",
            frames_after.len(),
        );
        assert_eq!(
            frames_after[0].0, INTENT_RECORD_SNAPSHOT,
            "the compacted file's base record must be a SNAPSHOT",
        );

        // Correctness: exactly the odd sequences remain pending.
        let expected: Vec<ReplicationIntentRange> = (1..=total)
            .filter(|i| i % 2 != 0)
            .map(|i| ReplicationIntentRange {
                first_sequence: i,
                last_sequence: i,
            })
            .collect();
        assert_eq!(
            tracker.pending(),
            expected,
            "in-memory pending must be exact after compaction",
        );

        let reopened = ReplicationIntentTracker::load(path).unwrap();
        assert_eq!(
            reopened.pending(),
            expected,
            "reload after compaction must reconstruct pending exactly",
        );
    }

    /// R12 review (Critical, silent durability loss): `compact_locked`
    /// writes a fresh `SNAPSHOT` via atomic temp-write+rename, then reopens
    /// the append handle on the renamed-into-place file. Pre-fix, if that
    /// reopen failed, `inner.append_file` stayed the OLD handle — now an fd
    /// on an unlinked, orphaned inode. POSIX permits `write()`+`fsync()` on
    /// an unlinked fd to succeed, so every subsequent `begin` would
    /// silently return `Ok` while durably writing to a file no recovery
    /// could ever see: a crash after that point loses every begin/commit
    /// since the failed reopen with NO error ever surfaced. This forces
    /// that reopen to fail via the `force_reopen_failure` test-only seam
    /// (mirroring the engine's `WriteFailingDevice` fault-injection
    /// pattern) — exercising the real `compact_locked` code path, not a
    /// hand-rolled substitute — and asserts the tracker fails CLOSED.
    #[test]
    fn intent_log_poisoned_on_compaction_reopen_failure_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        let tracker = ReplicationIntentTracker::load(path.clone()).unwrap();

        // Establish pending state before poisoning, to prove `pending()`
        // (an in-memory read) keeps working afterward.
        tracker.begin(1, 1, &[]).unwrap();

        // Arm the seam: the NEXT compact_locked's reopen fails.
        tracker.inner.lock().force_reopen_failure = true;

        // Drive compact_locked directly — the record-count trigger that
        // normally invokes it is covered by
        // `intent_log_compaction_bounds_file_and_preserves_pending`; this
        // test targets compact_locked's failure handling specifically. The
        // preceding `write_durable_file` inside it still runs for real
        // (the SNAPSHOT is genuinely written and renamed into place) —
        // only the reopen that follows is faked to fail, exactly
        // reproducing the pre-fix bug scenario.
        let compact_result = {
            let mut inner = tracker.inner.lock();
            tracker.compact_locked(&mut inner)
        };
        assert!(
            compact_result.is_err(),
            "the forced reopen failure must surface as an Err from compact_locked",
        );

        // Fail-closed assertion: pre-fix, `append_file` still held the
        // stale (now-unlinked) handle and this `begin` returned `Ok`,
        // silently writing/fsyncing to a file recovery could never see.
        // Post-fix, the tracker is poisoned and this MUST return `Err`.
        let begin_after_poison = tracker.begin(2, 2, &[]);
        assert!(
            begin_after_poison.is_err(),
            "begin() after a poisoned compaction reopen must fail loudly, not silently \
             succeed as it did pre-fix",
        );
        assert!(
            matches!(
                begin_after_poison.unwrap_err(),
                ReplicationIntentError::Poisoned(_)
            ),
            "the error must be the dedicated Poisoned variant",
        );

        // A second call must ALSO fail — poisoning is not one-shot.
        let commit_after_poison = tracker.commit(1, 1);
        // commit() only touches the append handle once its buffered frame
        // is actually flushed (time/count threshold or a subsequent
        // begin/flush); force that here via an explicit flush() so the
        // poisoned state is exercised on the commit path too.
        assert!(
            commit_after_poison.is_ok(),
            "commit() itself only buffers in memory and does not touch the append handle",
        );
        assert!(
            tracker.flush().is_err(),
            "flush() must fail loudly once poisoned, refusing to write the buffered commit \
             frame to the stale handle",
        );

        // In-memory reads must still work — poisoning blocks writes/
        // durability, not pure reads of already-recorded pending state.
        // `begin()` updates `pending` before attempting the (now-poisoned)
        // write, so range 2 is present despite `begin(2, 2, ..)` returning
        // Err above (unchanged, pre-existing in-memory-vs-disk divergence
        // on a failed write — not part of this fix); range 1 was removed
        // by the `commit(1, 1)` call. Either way, `pending()` must simply
        // not panic and reflect exactly that in-memory truth.
        assert_eq!(
            tracker.pending(),
            vec![ReplicationIntentRange {
                first_sequence: 2,
                last_sequence: 2,
            }],
            "pending() (in-memory read) must still work while poisoned",
        );
    }

    /// Follow-up to `intent_log_poisoned_on_compaction_reopen_failure_fails_closed`:
    /// the Active->Poisoned transition in `compact_locked` previously emitted
    /// no dedicated signal at all — the poison only surfaced indirectly, the
    /// next time a caller's `begin`/`commit` happened to return
    /// `Err(Poisoned)`. An operator whose intent log has silently gone
    /// non-durable needs a signal AT the transition, not just on next use.
    /// Assert the `intent_log_poisoned` metric bumps 0->1 exactly at
    /// `compact_locked`'s failure point.
    #[test]
    fn intent_log_poison_transition_increments_metric() {
        static TEST_METRICS: std::sync::OnceLock<&'static crate::metrics::ReplicationMetrics> =
            std::sync::OnceLock::new();
        let metrics_ref = *TEST_METRICS
            .get_or_init(|| Box::leak(Box::new(crate::metrics::ReplicationMetrics::new())));
        crate::metrics::init_replication_metrics(metrics_ref);
        let metrics =
            crate::metrics::replication_metrics().expect("replication metrics installed for test");
        let before = metrics.intent_log_poisoned.get();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");
        let tracker = ReplicationIntentTracker::load(path).unwrap();
        tracker.begin(1, 1, &[]).unwrap();

        // Arm the seam: the NEXT compact_locked's reopen fails, exactly as
        // in `intent_log_poisoned_on_compaction_reopen_failure_fails_closed`.
        tracker.inner.lock().force_reopen_failure = true;
        let compact_result = {
            let mut inner = tracker.inner.lock();
            tracker.compact_locked(&mut inner)
        };
        assert!(
            compact_result.is_err(),
            "the forced reopen failure must surface as an Err from compact_locked",
        );

        let after = metrics.intent_log_poisoned.get();
        assert_eq!(
            after,
            before + 1,
            "intent_log_poisoned must bump by exactly 1 at the Active->Poisoned \
             transition (was {before}, now {after})",
        );
    }

    #[test]
    fn intent_log_snapshot_reset_semantics() {
        // A hand-crafted file: SNAPSHOT{A(key_a), B(key_b)} then BEGIN{C(key_c)}
        // then COMMIT{A} → reload must yield pending == {B, C}: the SNAPSHOT
        // resets the base, and later records replay on top of it in order.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intent.dat");

        let range_a = ReplicationIntentRange {
            first_sequence: 1,
            last_sequence: 1,
        };
        let range_b = ReplicationIntentRange {
            first_sequence: 2,
            last_sequence: 2,
        };
        let range_c = ReplicationIntentRange {
            first_sequence: 3,
            last_sequence: 3,
        };
        let key_a = intent_key(0xAA);
        let key_b = intent_key(0xBB);
        let key_c = intent_key(0xCC);

        let mut snapshot_pending = BTreeMap::new();
        snapshot_pending.insert(range_a, vec![key_a]);
        snapshot_pending.insert(range_b, vec![key_b]);

        let mut data = Vec::new();
        data.extend_from_slice(&intent_log_encode_frame(
            INTENT_RECORD_SNAPSHOT,
            &intent_log_encode_snapshot_payload(&snapshot_pending),
        ));
        data.extend_from_slice(&intent_log_encode_frame(
            INTENT_RECORD_BEGIN,
            &intent_log_encode_range_and_keys(&range_c, &[key_c]),
        ));
        let mut commit_payload = Vec::new();
        commit_payload.extend_from_slice(&range_a.first_sequence.to_le_bytes());
        commit_payload.extend_from_slice(&range_a.last_sequence.to_le_bytes());
        data.extend_from_slice(&intent_log_encode_frame(
            INTENT_RECORD_COMMIT,
            &commit_payload,
        ));
        std::fs::write(&path, &data).unwrap();

        let tracker = ReplicationIntentTracker::load(path).unwrap();
        assert_eq!(
            tracker.pending_with_keys(),
            vec![(range_b, vec![key_b]), (range_c, vec![key_c])],
            "SNAPSHOT resets the base; subsequent BEGIN/COMMIT replay on top of it",
        );
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
    /// coverage as the last fully-included entry's sequence. Pre-fix the
    /// runner labeled chunk N+1 with the last ACKED sequence instead of
    /// acked+1, so the receiver's dedup dropped the first op of every
    /// chunk after the first. Labeling now lives in the `send_chunk`
    /// callback (the dispatch-side dense stream cursor); this test pins
    /// that the runner itself hands over contiguous, complete,
    /// non-overlapping chunks. One op per entry here (sequences 5..=14) so
    /// the expected watermark (14) matches the pre-entry-boundary-fix
    /// value exactly — the entry-boundary accounting introduced by the P0
    /// fix is exercised separately by
    /// `catchup_truncates_at_entry_boundary_not_mid_batch`.
    #[test]
    fn run_catchup_chunks_cover_all_ops_without_skips_or_overlap() {
        use crate::index::TxKey;

        let addr: SocketAddr = "127.0.0.1:65533".parse().unwrap();
        // 10 distinguishable ops, one per entry, at sequences 5..=14.
        let make_ops = |n: u8| -> Vec<ReplicaOp> {
            (0..n)
                .map(|i| ReplicaOp::Delete {
                    tx_key: TxKey::from_bytes([i + 1; 32]),
                })
                .collect()
        };
        let all_ops = make_ops(10);
        let entries_for_cb: Vec<(u64, Vec<ReplicaOp>)> = all_ops
            .iter()
            .enumerate()
            .map(|(i, op)| (5 + i as u64, vec![op.clone()]))
            .collect();

        let delivered: std::sync::Mutex<Vec<ReplicaOp>> = std::sync::Mutex::new(Vec::new());
        let chunk_sizes: std::sync::Mutex<Vec<usize>> = std::sync::Mutex::new(Vec::new());

        let result = run_catchup_for_replica(
            &addr,
            5,  // from_seq (redo space)
            15, // current_seq
            3,  // batch_size → chunks of 3,3,3,1
            10_000,
            &move |_from| entries_for_cb.clone(),
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
            "watermark must be the last fully-included entry's sequence (14)",
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
            &move |_from| vec![(1u64, ops.clone())],
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
        let no_ops: &dyn Fn(u64) -> Vec<(u64, Vec<ReplicaOp>)> = &|_| Vec::new();
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
        let no_ops: &dyn Fn(u64) -> Vec<(u64, Vec<ReplicaOp>)> = &|_| Vec::new();
        let no_send: &dyn Fn(&[ReplicaOp]) -> std::result::Result<(), String> =
            &|_| panic!("send_chunk must not be called when already caught up");

        let result = run_catchup_for_replica(&addr, 100, 100, 16, 100, no_ops, Some(50), no_send);
        assert_eq!(result.unwrap(), 100);

        let result = run_catchup_for_replica(&addr, 200, 100, 16, 100, no_ops, Some(50), no_send);
        assert_eq!(result.unwrap(), 200);
    }

    /// P0 regression: the pre-fix runner flattened every redo entry's
    /// `ReplicaOp` expansion into one `Vec<ReplicaOp>` before applying
    /// `max_ops_per_pass`, so `truncate` could cut a single multi-txid
    /// `SetMinedBatch` expansion in half, and the returned watermark
    /// (`from_seq + ops_sent - 1`) over-reported how many redo SEQUENCES
    /// had actually been fully sent — advancing the replica's ACK past ops
    /// that were silently dropped (permanent divergence with a false
    /// "caught up" signal).
    ///
    /// This models the exact repro: 3 redo entries expand to
    /// `[1 op, 5 ops (a 5-txid batch), 1 op]` = 7 ops total, with
    /// `max_ops_per_pass = 4`. The fix applies the budget at entry
    /// (sequence) granularity: whole entries are accumulated, and the pass
    /// stops BEFORE an entry that would push the running total over budget
    /// once at least one entry has already been accumulated. The returned
    /// watermark is the sequence of the last FULLY-INCLUDED entry, so a
    /// second pass resuming at `watermark + 1` delivers exactly the
    /// remaining entries with nothing dropped and nothing re-sent.
    #[test]
    fn catchup_truncates_at_entry_boundary_not_mid_batch() {
        use crate::index::TxKey;

        let addr: SocketAddr = "127.0.0.1:65531".parse().unwrap();
        let op = |b: u8| ReplicaOp::Delete {
            tx_key: TxKey::from_bytes([b; 32]),
        };

        // Entry 10: 1 op. Entry 11: 5 ops (the multi-txid batch). Entry 12:
        // 1 op. Total 7 ops across 3 entries.
        let entry_10_ops = vec![op(1)];
        let entry_11_ops: Vec<ReplicaOp> = (2..=6).map(op).collect();
        let entry_12_ops = vec![op(7)];
        let all_entries = vec![
            (10u64, entry_10_ops.clone()),
            (11u64, entry_11_ops.clone()),
            (12u64, entry_12_ops.clone()),
        ];

        let ops_from_seq = {
            let all_entries = all_entries.clone();
            move |from: u64| -> Vec<(u64, Vec<ReplicaOp>)> {
                all_entries
                    .iter()
                    .filter(|(seq, _)| *seq >= from)
                    .cloned()
                    .collect()
            }
        };

        let delivered: std::sync::Mutex<Vec<ReplicaOp>> = std::sync::Mutex::new(Vec::new());
        let send = |chunk: &[ReplicaOp]| -> std::result::Result<(), String> {
            delivered.lock().unwrap().extend_from_slice(chunk);
            Ok(())
        };

        // First pass: from_seq = 10, max_ops_per_pass = 4. Entry 10 (1 op)
        // fits; entry 11 would bring the running total to 6 > 4, and an
        // entry is already accumulated, so entry 11 must NOT be included.
        let result = run_catchup_for_replica(&addr, 10, 13, 10, 4, &ops_from_seq, Some(10), &send);

        assert_eq!(
            result.unwrap(),
            10,
            "watermark must be the last FULLY-INCLUDED entry's sequence (10), not \
             from_seq + ops_sent - 1",
        );
        assert_eq!(
            *delivered.lock().unwrap(),
            entry_10_ops,
            "pass must ship only whole entries within budget: entry 10's single op, \
             stopping before the 5-op batch at entry 11",
        );

        // Second pass resumes at watermark + 1 = 11 with a budget generous
        // enough to take both remaining entries whole.
        delivered.lock().unwrap().clear();
        let result2 =
            run_catchup_for_replica(&addr, 11, 13, 10, 10_000, &ops_from_seq, Some(10), &send);

        assert_eq!(
            result2.unwrap(),
            12,
            "second pass must cover through the last remaining entry (12)",
        );
        let mut expected_remaining = entry_11_ops;
        expected_remaining.extend(entry_12_ops);
        assert_eq!(
            *delivered.lock().unwrap(),
            expected_remaining,
            "second pass must deliver the remaining entries with nothing dropped and \
             nothing re-sent",
        );
    }

    /// A single redo entry whose `ReplicaOp` expansion alone exceeds
    /// `max_ops_per_pass` must still ship whole in one pass — a
    /// `SetMinedBatch` expansion is atomic, and splitting it would
    /// resurrect the mid-batch truncation bug. The returned watermark is
    /// that entry's own sequence, and the next entry must NOT be pulled
    /// into the same pass.
    #[test]
    fn catchup_single_oversized_batch_ships_whole() {
        use crate::index::TxKey;

        let addr: SocketAddr = "127.0.0.1:65530".parse().unwrap();
        let op = |b: u8| ReplicaOp::Delete {
            tx_key: TxKey::from_bytes([b; 32]),
        };

        // Entry 20 alone expands to 10 ops -- far over the budget of 3.
        // Entry 21 (2 more ops) must not be pulled in alongside it.
        let big_entry_ops: Vec<ReplicaOp> = (1..=10).map(op).collect();
        let entries = [
            (20u64, big_entry_ops.clone()),
            (21u64, vec![op(11), op(12)]),
        ];

        let delivered: std::sync::Mutex<Vec<ReplicaOp>> = std::sync::Mutex::new(Vec::new());

        let result = run_catchup_for_replica(
            &addr,
            20,
            22,
            100, // batch_size: large enough to send in one chunk
            3,   // max_ops_per_pass: smaller than the single entry's 10 ops
            &move |from: u64| {
                entries
                    .iter()
                    .filter(|(seq, _)| *seq >= from)
                    .cloned()
                    .collect()
            },
            Some(20),
            &|chunk| {
                delivered.lock().unwrap().extend_from_slice(chunk);
                Ok(())
            },
        );

        assert_eq!(
            result.unwrap(),
            20,
            "watermark must be the oversized entry's own sequence",
        );
        assert_eq!(
            *delivered.lock().unwrap(),
            big_entry_ops,
            "the oversized single entry must ship whole (not split), and the next entry \
             must not be pulled into the same pass",
        );
    }
}
