//! Server configuration.

use crate::observability::ObservabilityConfig;
use serde::Deserialize;
use std::fmt;
use std::net::IpAddr;
use std::path::PathBuf;
use thiserror::Error;

/// A bearer string (admin token, cluster secret) whose `Debug` impl never
/// prints the raw bytes — only `"<redacted, len=N>"`.
///
/// Prevents accidental leaks via `tracing::debug!(?config, ...)` or panic
/// messages that format the whole struct. See F-G10-007 in the audit.
///
/// Equality follows the inner string for config tests; the `Debug` impl is
/// the only piece that diverges from `String`. Note: equality is *not*
/// constant-time — that property only matters for HTTP/wire-side compares
/// (see `subtle` crate usage in the HTTP middleware), not for config
/// validation, which runs once at startup.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wrap a raw secret string. Empty strings are allowed at this layer —
    /// emptiness is enforced by [`ServerConfig::validate_safe_defaults`].
    pub fn new<S: Into<String>>(s: S) -> Self {
        Self(s.into())
    }

    /// Whether the wrapped secret is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Length of the wrapped secret in bytes (`String::len` semantics).
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Borrow the inner secret as `&str`. Used by the HMAC/auth wiring; the
    /// `Debug` redaction is the only protection — callers must not log this.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Borrow the inner secret bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted, len={}>", self.0.len())
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Secret)
    }
}

impl From<String> for Secret {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Secret {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Errors produced when validating a [`ServerConfig`] before startup.
///
/// Each variant carries the operator-actionable detail required to fix the
/// configuration. Errors are returned by [`ServerConfig::validate_safe_defaults`]
/// and surface as fatal startup failures from the binary entry point.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// `listen_addr` does not parse as `host:port`.
    #[error(
        "listen_addr {addr:?} is not a valid host:port (parse error: {source}); use \
         e.g. \"127.0.0.1:3300\" or \"0.0.0.0:3300\""
    )]
    InvalidListenAddr {
        /// The raw address string that failed to parse.
        addr: String,
        /// Underlying parse error from the address parser.
        source: std::net::AddrParseError,
    },

    /// `http_listen_addr` does not parse as `host:port`.
    #[error(
        "http_listen_addr {addr:?} is not a valid host:port (parse error: {source}); use \
         e.g. \"127.0.0.1:9100\" or \"0.0.0.0:9100\""
    )]
    InvalidHttpListenAddr {
        /// The raw address string that failed to parse.
        addr: String,
        /// Underlying parse error from the address parser.
        source: std::net::AddrParseError,
    },

    /// A non-loopback bind was configured without `enable_remote_bind = true`.
    ///
    /// Until the mTLS wave lands, exposing `listen_addr` or `http_listen_addr`
    /// on a non-loopback interface gives any network actor that can reach the
    /// port the ability to mutate state or read sensitive debug data. See
    /// gap #1 in `docs/TERANODE_PRODUCTION_READINESS_GAPS.md`.
    #[error(
        "{field} is bound to non-loopback address {addr:?} but enable_remote_bind = false; \
         either set enable_remote_bind = true (only safe on a private network) or change \
         the bind to a loopback address. Authenticated remote access (mTLS) is tracked as \
         a follow-up to the safe-defaults slice of gap #1."
    )]
    RemoteBindRefused {
        /// Which config field failed the check (e.g. `"listen_addr"`).
        field: &'static str,
        /// The non-loopback address the operator tried to bind.
        addr: String,
    },

    /// Cluster mode was enabled without a `cluster_secret`.
    ///
    /// With clustering enabled, every SWIM/topology/migration frame is
    /// authority-bearing inter-node traffic; without an HMAC secret any peer
    /// that can connect can inject those frames. RF > 1 is also rejected
    /// because replication traffic is authority-bearing even if `node_id`
    /// was misconfigured as 0.
    #[error(
        "cluster mode or replication_factor = {rf} requires a non-empty cluster_secret to \
         authenticate inter-node SWIM messages and cluster control frames; either set \
         cluster_secret in config or run true single-node mode (node_id = 0, \
         replication_factor = 1)"
    )]
    ClusterSecretRequired {
        /// The configured replication factor.
        rf: u8,
    },

    /// `enable_admin_endpoints = true` was set without a non-empty
    /// `admin_token` (or `TERASLAB_ADMIN_TOKEN` env override).
    ///
    /// When the mutating `/admin/*` and `/debug/*` surface is registered it
    /// must be guarded by a bearer token so a network actor with TCP reach
    /// cannot quiesce, drain, rebalance, or read sensitive debug data without
    /// proving knowledge of an operator-issued secret. Opting into the surface
    /// without a token is treated as a configuration mistake, not a deployment
    /// choice — the server refuses to start so the misconfiguration is
    /// surfaced before any port binds.
    #[error(
        "enable_admin_endpoints = true requires a non-empty admin_token (or the \
         TERASLAB_ADMIN_TOKEN environment override) to gate the mutating /admin/* and \
         /debug/* HTTP routes with bearer-token auth; either set admin_token in config / \
         export TERASLAB_ADMIN_TOKEN, or set enable_admin_endpoints = false"
    )]
    AdminTokenRequired,

    /// `device_paths` was empty. The startup path indexes `device_paths[0]`
    /// to derive the redo log path and cluster state path; an empty vec
    /// would panic. See F-G10-004.
    #[error(
        "device_paths must contain at least one path; the default \
         \"teraslab-data.dat\" is used when no TOML override is provided"
    )]
    NoDevicePaths,

    /// `device_split` is zero, or `device_paths.len() * device_split` exceeds
    /// 256 (the index entry's `device_id` is a `u8`).
    #[error(
        "invalid store layout: device_paths.len()={paths} * device_split={split} \
         must be in 1..=256 (device_id is a u8)"
    )]
    InvalidStoreLayout {
        /// Number of configured device paths.
        paths: usize,
        /// Configured per-device split factor.
        split: usize,
    },

    /// Multi-store (`device_paths.len() * device_split > 1`) is configured with a
    /// non in-memory index backend. Only the in-memory backend's device-scan
    /// rebuild scans every store; the redb / file-backed rebuilds scan store 0
    /// only and would silently lose every record on stores 1..N after a snapshot
    /// loss. Fail closed rather than risk silent data loss.
    #[error(
        "multi-store layout ({stores} stores) requires the in-memory index backend; \
         backend={backend:?} only rebuilds store 0 from a device scan and would lose \
         records on the other stores after a snapshot loss"
    )]
    MultiStoreRequiresMemoryBackend {
        /// Total configured stores (`device_paths.len() * device_split`).
        stores: usize,
        /// The configured (unsupported-for-multi-store) backend.
        backend: IndexBackendMode,
    },

    /// `cache.writeback = true` with `cache.bytes = 0`. Write-back needs a
    /// buffer to defer writes into; a zero budget means "no cache" and can only
    /// be write-through (i.e. the device is not wrapped at all).
    #[error(
        "cache.writeback = true requires cache.bytes > 0 (write-back needs a buffer); \
         set cache.bytes to a non-zero budget or leave cache.writeback = false"
    )]
    WriteBackRequiresCacheBytes,

    /// `storage.packed = true` with `device_alignment > 4096`. Packed mode's
    /// block-granular `io_locks` hardcode a 4096-byte lock block; a larger
    /// device block could map two packed records in the same physical block to
    /// different lock stripes and under-lock a shared block (torn writes). See
    /// `docs/PACKED_RECORD_STORAGE_DESIGN.md` §3.2.
    #[error(
        "storage.packed = true requires device_alignment <= 4096 (the packed io_locks lock \
         block is 4096 bytes), found device_alignment = {device_alignment}; either set \
         device_alignment <= 4096 or disable packing (storage.packed = false)"
    )]
    PackedAlignmentTooLarge {
        /// The configured device alignment that exceeds the packed lock block.
        device_alignment: usize,
    },

    /// `advertise_addr` does not parse as `host:port` (only checked when set).
    /// See F-G10-013.
    #[error(
        "advertise_addr {addr:?} is not a valid host:port (parse error: {source}); \
         use e.g. \"192.168.1.10:3300\""
    )]
    InvalidAdvertiseAddr {
        /// The raw address string that failed to parse.
        addr: String,
        /// Underlying parse error.
        source: std::net::AddrParseError,
    },

    /// `--strict-auth` was set (or `strict_auth = true` in TOML) and the
    /// multi-node configuration is missing a `cluster_secret`. See F-X-001
    /// for the full threat-model rationale.
    #[error(
        "strict_auth = true (or --strict-auth) requires a non-empty cluster_secret in \
         multi-node configurations (node_id > 0 OR replication_factor > 1), found none. \
         Either drop --strict-auth to fall back to trusted-overlay defaults (a security \
         warning will be logged) or provide cluster_secret"
    )]
    StrictAuthRequiresSecret,

    /// F-E2: `--strict-auth` was set (or `strict_auth = true`) on a clustered
    /// node (`node_id > 0`) but no `cluster_id` is configured. Without a
    /// persisted `cluster_id`, two independently-bootstrapped clusters that
    /// share a `cluster_secret` can silently merge (the cross-cluster
    /// membership reject only fires when BOTH sides advertise a real id), risking
    /// split-brain. Strict mode requires the merge guard to be armed.
    #[error(
        "strict_auth = true (or --strict-auth) requires a persisted cluster_id on clustered \
         nodes (node_id > 0), found none. A missing cluster_id lets two clusters sharing a \
         cluster_secret merge (split-brain risk). Set a 32-hex-char cluster_id, or drop \
         --strict-auth to fall back to trusted-overlay defaults (a warning will be logged)"
    )]
    StrictAuthRequiresClusterId,

    /// E-2: the `cluster_secret` seen by the TCP server (from
    /// [`ServerConfig`]) does not match the one the attached
    /// [`crate::cluster::coordinator::RunningCluster`] uses for inter-node
    /// HMAC.
    ///
    /// These are two independently-populated copies of the same shared
    /// secret. Production TOML loading derives both from the same field, but
    /// programmatic construction (notably tests) can set one and leave the
    /// other `None`/different. When that happens the server signs client and
    /// inter-node *responses* with one secret while topology / replication
    /// proposals expect the other — every HMAC verification fails forever
    /// with no surfaced error, manifesting as a silent cluster-formation
    /// hang. A money store must fail closed at startup instead, so this is a
    /// hard refuse whenever clustering is active (the cluster is attached and
    /// the deployment is multi-node).
    #[error(
        "cluster_secret mismatch: the TCP server's secret (ServerConfig, {server_state}) \
         differs from the attached cluster coordinator's secret (ClusterConfig, \
         {cluster_state}); both must carry the identical shared secret or inter-node \
         HMAC verification fails silently forever. Set cluster_secret once and derive \
         both copies from it"
    )]
    ClusterSecretMismatch {
        /// Human-readable state of the server-side secret (`"set"` / `"unset"`).
        server_state: &'static str,
        /// Human-readable state of the cluster-side secret (`"set"` / `"unset"`).
        cluster_state: &'static str,
    },

    /// One of the sizing knobs is zero, not a power of 2 where required, or
    /// otherwise out of range. See F-G10-005.
    #[error("invalid sizing config: {0}")]
    InvalidSizing(String),

    /// `cluster_id` was set but is not exactly 32 hex characters (16 bytes).
    /// See P1.1.
    #[error("invalid cluster_id: {reason}")]
    InvalidClusterId {
        /// Human-readable reason describing the malformed input.
        reason: String,
    },

    /// `cluster_secret` was set but is shorter than the minimum required
    /// entropy (16 bytes / 128 bits). See F-G10-011.
    #[error(
        "cluster_secret is {actual} bytes; minimum required is {min} bytes to give \
         the HMAC enough entropy against an attacker who can speak SWIM/replication. \
         Use `openssl rand -base64 24` or similar to generate one"
    )]
    ClusterSecretTooShort {
        /// Actual length of the configured secret in bytes.
        actual: usize,
        /// Minimum required length in bytes.
        min: usize,
    },

    /// `admin_token` was set but is shorter than the minimum required length
    /// when both `enable_admin_endpoints` and `enable_remote_bind` are on.
    /// See F-G10-010.
    #[error(
        "admin_token is {actual} bytes; minimum required is {min} bytes when both \
         enable_admin_endpoints and enable_remote_bind are true (remote-reachable \
         admin surface). Use `openssl rand -base64 24` or similar to generate one"
    )]
    AdminTokenTooShort {
        /// Actual length of the configured token in bytes.
        actual: usize,
        /// Minimum required length in bytes.
        min: usize,
    },
}

/// Parse the host portion of an `addr` string of the form `host:port`.
///
/// Returns the parsed [`IpAddr`] on success. Used to gate non-loopback binds.
/// Decode a single hex digit (`0-9`, `a-f`, `A-F`) into its 0-15 value
/// or return a typed [`ConfigError::InvalidClusterId`] otherwise.
fn hex_nibble(c: u8) -> Result<u8, ConfigError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(ConfigError::InvalidClusterId {
            reason: format!("non-hex byte 0x{c:02x}"),
        }),
    }
}

fn parse_bind_host(addr: &str) -> Result<IpAddr, std::net::AddrParseError> {
    // SocketAddr accepts both IPv4 and bracketed IPv6 forms.
    addr.parse::<std::net::SocketAddr>().map(|sa| sa.ip())
}

fn parse_usize_env(name: &str) -> std::result::Result<Option<usize>, String> {
    match std::env::var(name) {
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => raw
            .parse::<usize>()
            .map(Some)
            .map_err(|e| format!("{name} must be a non-negative integer: {e}")),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(format!("{name} could not be read: {e}")),
    }
}

fn parse_u64_env(name: &str) -> std::result::Result<Option<u64>, String> {
    match std::env::var(name) {
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|e| format!("{name} must be a non-negative integer: {e}")),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(format!("{name} could not be read: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Index backend configuration
// ---------------------------------------------------------------------------

/// Index backend mode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum IndexBackendMode {
    /// In-memory Robin Hood hash table (default, fastest).
    #[default]
    Memory,
    /// On-disk B+ tree via redb (low-RAM deployments).
    Redb,
    /// File-backed mmap (persistent, relies on redo log for crash recovery).
    FileBacked,
}

impl<'de> Deserialize<'de> for IndexBackendMode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "memory" | "" => Ok(Self::Memory),
            "redb" => Ok(Self::Redb),
            "file_backed" => Ok(Self::FileBacked),
            other => Err(serde::de::Error::custom(format!(
                "unknown index backend: {other:?} (expected \"memory\", \"redb\", or \"file_backed\")"
            ))),
        }
    }
}

/// Configuration for the index subsystem.
///
/// Controls which backend is used for the primary and secondary indexes.
/// When `backend` is `"memory"` (default), the existing in-memory Robin Hood
/// hash table is used. When `"redb"`, a crash-durable B+ tree backed by redb
/// is used instead, trading throughput for dramatically lower RAM requirements.
/// When `"file_backed"`, a memory-mapped file is used for the primary index
/// (persistent across restarts, relying on the redo log for crash recovery);
/// secondary indexes remain in-memory.
///
/// The redb backend uses three separate database files: one for the primary
/// index (`redb_path`), one for the DAH secondary index (`redb_dah_path`),
/// and one for the unmined secondary index (`redb_unmined_path`).
///
/// # Example (TOML)
///
/// ```toml
/// [index]
/// backend = "redb"
/// redb_path = "/data/teraslab-index.redb"
/// redb_dah_path = "/data/teraslab-dah.redb"
/// redb_unmined_path = "/data/teraslab-unmined.redb"
/// redb_cache_size = 268435456
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IndexConfig {
    /// Backend mode: `"memory"` (default), `"redb"`, or `"file_backed"`.
    pub backend: IndexBackendMode,

    /// Path for the redb primary index database file.
    /// Only used when `backend = "redb"`.
    pub redb_path: PathBuf,

    /// Path for the redb DAH secondary index database.
    /// Only used when `backend = "redb"`.
    pub redb_dah_path: PathBuf,

    /// Path for the redb unmined secondary index database.
    /// Only used when `backend = "redb"`.
    pub redb_unmined_path: PathBuf,

    /// redb page cache size in bytes. Default: 256 MiB.
    /// Only applies to the redb backend.
    pub redb_cache_size: usize,

    /// Path for the file-backed mmap primary index.
    /// Only used when `backend = "file_backed"`.
    pub file_backed_path: PathBuf,

    /// Number of independent index shards. Each shard is a complete
    /// `PrimaryBackend` behind its own `RwLock`. Rounded up to the next
    /// power of two, clamped to `[1, 256]`. Default `256`: N must be
    /// much greater than the number of concurrent writers for good read
    /// isolation; write-lock collision probability ≈ writers / N.
    pub index_shards: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            backend: IndexBackendMode::Memory,
            redb_path: PathBuf::from("teraslab-index.redb"),
            redb_dah_path: PathBuf::from("teraslab-dah.redb"),
            redb_unmined_path: PathBuf::from("teraslab-unmined.redb"),
            redb_cache_size: 256 * 1024 * 1024, // 256 MiB
            file_backed_path: PathBuf::from("teraslab-index.dat"),
            index_shards: 256,
        }
    }
}

impl IndexConfig {
    /// Whether the in-memory backend is selected.
    pub fn is_memory(&self) -> bool {
        self.backend == IndexBackendMode::Memory
    }

    /// Whether the redb on-disk backend is selected.
    pub fn is_redb(&self) -> bool {
        self.backend == IndexBackendMode::Redb
    }

    /// Whether the file-backed mmap backend is selected.
    pub fn is_file_backed(&self) -> bool {
        self.backend == IndexBackendMode::FileBacked
    }
}

/// Optional in-RAM data-device block cache (see `docs/WRITE_CACHE_SPEC.md`).
///
/// `O_DIRECT` bypasses the OS page cache, so read-modify-write ops re-read each
/// record from the device. This optional cache absorbs those reads (and, in
/// write-back mode, defers the data writes to the next `sync()` barrier).
///
/// # Example (TOML)
///
/// ```toml
/// [cache]
/// bytes = 2147483648         # 2 GiB per-store; 0 (default) = no cache
/// writeback = false          # false = write-through (no durability change)
/// writeback_interval_ms = 50 # write-back only: background drain cadence
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    /// Per-store cache budget in bytes. `0` (default) disables the cache
    /// entirely — the device is not wrapped and behavior is byte-for-byte the
    /// raw `O_DIRECT` path (maximum safety).
    pub bytes: usize,

    /// `false` (default) = write-through (every write reaches the device
    /// immediately; pure read acceleration, durability unchanged). `true` =
    /// write-back (writes are buffered in RAM and flushed on the `sync()`
    /// barrier the checkpoint already issues; still WAL-safe). Requires
    /// `bytes > 0`.
    pub writeback: bool,

    /// Write-back only: cadence in milliseconds of the background writeback
    /// thread that continuously drains dirty blocks to the device so the dirty
    /// footprint stays bounded (keeping writes RAM-fast and the checkpoint's
    /// `sync()` cheap). Default 50 ms. Purely a performance knob — it changes
    /// only *when* dirty blocks reach the device ahead of `sync()`, never what
    /// `sync()`/recovery guarantee. Ignored in write-through mode (no thread is
    /// spawned). Clamped to a minimum of 1 ms.
    pub writeback_interval_ms: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            bytes: 0,
            writeback: false,
            writeback_interval_ms: default_cache_writeback_interval_ms(),
        }
    }
}

/// Default background writeback cadence (ms) for [`CacheConfig`].
const fn default_cache_writeback_interval_ms() -> u64 {
    50
}

impl CacheConfig {
    /// Whether the cache is enabled (non-zero budget).
    pub fn is_enabled(&self) -> bool {
        self.bytes > 0
    }
}

/// On-device storage layout configuration (`[storage]` TOML section).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StorageConfig {
    /// Pack multiple sub-block records contiguously within a single device
    /// block instead of giving each record its own 4 KiB block. `false`
    /// (default) is the unchanged block-per-record layout — byte-for-byte
    /// identical behavior and no extra I/O.
    ///
    /// Packing kills create write amplification (≈7 records per device write)
    /// but changes the on-disk format: a packed device stamps allocator
    /// header version 2 and MUST always be reopened packed (reopening it
    /// non-packed corrupts it via `free()`'s block-rounding). Packing therefore
    /// requires a FRESH device — there is no in-place migration. See
    /// `docs/PACKED_RECORD_STORAGE_DESIGN.md`.
    ///
    /// When enabled, `device_alignment` must be `<= 4096`: the block-granular
    /// `io_locks` hardcode a 4096-byte lock block, so a larger device block
    /// could map two packed records to different lock stripes and under-lock a
    /// shared block. Validation rejects `packed = true` with
    /// `device_alignment > 4096`.
    pub packed: bool,

    /// How a new record is assigned to a store at create time:
    /// `"round_robin"` (default) or `"txid"`.
    ///
    /// `"round_robin"` (default) is the unchanged behavior — even fill across
    /// stores via a rotating counter, independent of the txid. `"txid"` makes
    /// placement a deterministic function of the txid's last 8 bytes
    /// (`store = last8(txid) LE % num_stores`), so a record's store is
    /// computable from its txid for every op — the foundation for per-store
    /// dispatch routing.
    ///
    /// Reads always route by the index entry's recorded `device_id`, never by
    /// re-deriving placement, so switching this key on an already-populated
    /// store is safe: existing records keep their recorded store and stay
    /// readable; only NEW records follow the new strategy.
    #[serde(deserialize_with = "deserialize_placement")]
    pub placement: crate::subdevice::PlacementStrategy,

    /// Append-only allocation: never reuse freed regions; every new record
    /// extends the high-water mark (`false`, default, is the unchanged best-fit
    /// freelist behavior).
    ///
    /// This is the Phase 1 log-structured write lever (see
    /// `bench/results/LOG_STRUCTURED_DATA_LAYER_DESIGN.md`): under the UTXO
    /// recipe the create-then-delete churn fills the freelist, and best-fit
    /// reuse then scatters new records into the freed holes — defeating the
    /// write-back cache's sequential-flush coalescing. With `append_only`, frees
    /// are still journaled and tracked (for recovery + accounting) but never
    /// handed back out, so creates stay strictly sequential and coalesce into
    /// large sequential write-backs like a log-structured store.
    ///
    /// Trade-off: NO space reclamation — the device grows unbounded (the
    /// freelist accumulates and `persist` will eventually hit
    /// `FreelistOverflow`). Intended for bounded benchmark runs and as the
    /// precursor to the full segment engine (defrag-based reclaim, Phase 3), not
    /// for unbounded production use. Unlike `packed`, this is a pure runtime
    /// placement policy: it does not change the on-disk format and is NOT
    /// persisted, so a device can be reopened in either mode safely.
    pub append_only: bool,

    /// Storage engine: `"in_place"` (default) uses the best-fit `SlotAllocator`;
    /// `"segment"` uses the log-structured append-cursor `SegmentAllocator`
    /// (creates append to a moving cursor; sequential writes; relocate + defrag in
    /// later phases). See `bench/results/LOG_STRUCTURED_DATA_LAYER_DESIGN.md`.
    ///
    /// The engine determines the on-disk allocator header format (distinct
    /// magics), so a device formatted by one engine cannot be opened by the
    /// other — switching engines requires a fresh device.
    #[serde(deserialize_with = "deserialize_storage_engine")]
    pub engine: StorageEngine,

    /// Segment size in bytes for the `"segment"` engine (ignored by `"in_place"`).
    /// Default 8 MiB. Must be a positive multiple of `device_alignment` and fit
    /// the per-store data region.
    #[serde(default = "default_segment_size")]
    pub segment_size: u64,
}

/// Default segment size (8 MiB) for the segment storage engine.
const fn default_segment_size() -> u64 {
    8 * 1024 * 1024
}

/// On-disk storage engine selection (`[storage] engine`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StorageEngine {
    /// Best-fit freelist allocator (records placed at home offsets, updated in
    /// place). The historical default.
    #[default]
    InPlace,
    /// Log-structured append-cursor allocator (creates append sequentially).
    Segment,
}

/// Deserialize the `[storage] engine` key into a [`StorageEngine`].
/// Accepts `"in_place"` (or empty) and `"segment"`; rejects anything else loudly.
fn deserialize_storage_engine<'de, D>(
    deserializer: D,
) -> std::result::Result<StorageEngine, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.as_str() {
        "in_place" | "" => Ok(StorageEngine::InPlace),
        "segment" => Ok(StorageEngine::Segment),
        other => Err(serde::de::Error::custom(format!(
            "unknown storage engine: {other:?} (expected \"in_place\" or \"segment\")"
        ))),
    }
}

/// Deserialize the `[storage] placement` key into a [`PlacementStrategy`].
/// Accepts `"round_robin"` (or empty) and `"txid"`; rejects anything else with
/// a typed serde error so a typo fails startup loudly instead of silently
/// defaulting.
fn deserialize_placement<'de, D>(
    deserializer: D,
) -> std::result::Result<crate::subdevice::PlacementStrategy, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use crate::subdevice::PlacementStrategy;
    let s = String::deserialize(deserializer)?;
    match s.as_str() {
        "round_robin" | "" => Ok(PlacementStrategy::RoundRobin),
        "txid" => Ok(PlacementStrategy::Txid),
        other => Err(serde::de::Error::custom(format!(
            "unknown placement strategy: {other:?} (expected \"round_robin\" or \"txid\")"
        ))),
    }
}

/// Maximum `device_alignment` (bytes) compatible with packed mode. The
/// block-granular `io_locks` / `lock_span_blocks` hardcode a 4096-byte lock
/// block (`docs/PACKED_RECORD_STORAGE_DESIGN.md` §3.2); a larger device block
/// could under-lock a shared block, so packing is refused above this.
const PACKED_MAX_DEVICE_ALIGNMENT: usize = 4096;

/// TeraSlab server configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// TCP listen address for the binary wire protocol.
    pub listen_addr: String,

    /// Address to advertise to other cluster nodes. If not set, defaults to
    /// `listen_addr`. Set this when `listen_addr` uses `0.0.0.0` (e.g. in
    /// Docker containers) so other nodes can reach this node by its actual IP.
    pub advertise_addr: Option<String>,

    /// Device file paths for data storage.
    /// Each path is a file that will be created if it doesn't exist.
    pub device_paths: Vec<PathBuf>,

    /// Size of each device file in bytes (only used when creating new files).
    pub device_size: u64,

    /// Device I/O alignment in bytes (4096 for most NVMe/SSDs).
    pub device_alignment: usize,

    /// Number of virtual stores to carve each physical device into
    /// (reference-style virtual devices). `1` = one store per device. Splitting
    /// a device into K stores gives K independent allocators + index lock
    /// domains (lock/contention parallelism) sharing one physical device's I/O
    /// bandwidth and fsync barrier. Total stores = `device_paths.len() *
    /// device_split`, bounded by 256 (the index entry's `device_id` is a `u8`).
    /// Records are placed across all stores at create time per the
    /// `[storage] placement` strategy (round-robin by default, or deterministic
    /// txid→store); reads always route by the index entry's recorded
    /// `device_id`.
    pub device_split: usize,

    /// Size of the redo log region in bytes.
    pub redo_log_size: u64,

    /// Path for the redo log file. If not set, derived from the first device
    /// path by appending `.redo`.
    pub redo_log_path: Option<PathBuf>,

    /// Path for the tiny durable node-height file that persists the engine's
    /// `last_durable_height` across restarts (deletion-tombstone design §4,
    /// height subsystem). If not set, derived from the index snapshot path by
    /// appending `.height`.
    ///
    /// The file holds a single fsynced, CRC-protected `u32` written
    /// atomically (temp + rename) by the checkpoint task and on graceful
    /// shutdown, sibling to the allocator persist. It is ALWAYS maintained; on
    /// recovery the value is restored and then bounded below by a
    /// record-derived floor so the height can never regress (monotonicity).
    /// A missing or corrupt file simply falls back to the record-derived
    /// floor — never a hard failure.
    pub last_durable_height_path: Option<PathBuf>,

    /// Path for the index snapshot file.
    pub index_snapshot_path: PathBuf,

    /// Expected number of records (for initial index sizing).
    pub expected_records: usize,

    /// Lock stripe count (power of 2, default 65536).
    pub lock_stripes: usize,

    /// Maximum batch size accepted from clients.
    pub max_batch_size: u32,

    /// Maximum concurrent client connections.
    pub max_connections: usize,

    /// Maximum concurrent client connections from a single source IP.
    ///
    /// Before this cap existed, a single attacker IP could pin all
    /// `max_connections` slots with slow-loris reads and starve every
    /// other client. The accept loop now tracks connection counts per
    /// source IP and rejects new connections from an IP that already
    /// holds `max_connections_per_ip` connections — the reject closes
    /// the socket without spawning a thread or reading any bytes.
    ///
    /// The default (64) is comfortably above what a legitimate client
    /// with connection pooling will open (Teranode-side pools sit at 8
    /// to 32 connections) but well below the global cap, so a single
    /// hostile peer can never drown out the rest of the fleet. Operators
    /// who run all clients behind a single egress NAT may need to raise
    /// this; setting it to `0` disables per-IP enforcement entirely (not
    /// recommended outside trusted overlay networks).
    pub max_connections_per_ip: usize,

    /// Maximum cumulative payload bytes accepted for one streaming blob
    /// upload on a single connection before the stream is aborted.
    pub max_stream_total_bytes: u64,

    /// Maximum number of in-progress streaming blob uploads that a single
    /// connection may hold open simultaneously.
    ///
    /// Each in-progress stream (keyed by txid) holds an OS file descriptor,
    /// a temp file, and a hasher in `ConnectionState.streams`. Without this
    /// cap a single connection could open one `OP_STREAM_CHUNK` (offset 0,
    /// one byte) for millions of distinct txids — never finishing any of
    /// them — and exhaust the process file-descriptor table and blob tmp
    /// directory. The per-stream byte cap ([`Self::max_stream_total_bytes`])
    /// does not help: each abandoned stream needs only one chunk to stay
    /// resident. Opening a new stream past this cap is rejected with
    /// `ERR_RATE_LIMITED`; existing streams are unaffected.
    ///
    /// The default (64) comfortably exceeds the fan-out of any legitimate
    /// client (Teranode uploads cold data one transaction at a time) while
    /// bounding the fd/tmp footprint of a hostile connection. Setting it to
    /// `0` disables the per-connection stream-count cap entirely (not
    /// recommended outside trusted overlay networks).
    pub max_active_streams_per_connection: usize,

    /// Idle timeout, in seconds, after which an in-progress streaming blob
    /// upload that has received no further chunk is reaped — its file
    /// descriptor, temp file, hasher, and map entry freed — independently of
    /// connection close.
    ///
    /// The frame-assembly deadline bounds a single frame's assembly, not the
    /// gap *between* `OP_STREAM_CHUNK` frames. Without an idle-stream reaper a
    /// client could open a stream, send one chunk, then keep the connection
    /// cheaply alive (periodic `OP_PING`) and pin the stream's resources
    /// forever. The reaper runs per connection on each request and aborts any
    /// stream older than this timeout (removing the tmp file via the writer's
    /// `abort`). A subsequent op on a reaped stream id is treated as an
    /// unknown stream and returns a clean error.
    ///
    /// The default (60 s) is generous for a legitimate slow uploader (a 4 GiB
    /// blob streamed at a few hundred KiB/s sends chunks far more often than
    /// once a minute). Setting it to `0` disables the idle reaper entirely
    /// (not recommended).
    pub stream_idle_timeout_secs: u64,

    /// Maximum aggregate request-frame bytes allowed in flight across all
    /// TCP connection threads. A value of 0 disables the aggregate cap.
    pub max_inflight_request_bytes: usize,

    /// Per-connection request pipelining depth: the number of requests on a
    /// single connection that may be dispatched concurrently.
    ///
    /// `1` (the default) preserves the strictly serial per-connection model —
    /// each request is fully handled (including its redo fsync) before the next
    /// on that connection is read. Values `> 1` let the connection dispatch up
    /// to `pipeline_depth` requests at once on a bounded worker pool, with
    /// responses written back as each completes (matched by `request_id`, so
    /// they may return out of order). This raises the number of mutations
    /// reaching the redo group-commit concurrently — the throughput lever for
    /// clients that keep several requests in flight on one connection — without
    /// needing one OS connection/thread per in-flight request.
    ///
    /// Ordering caveat: with `> 1`, two requests issued on the same connection
    /// without waiting for the first's response may be applied in either order.
    /// Stateful blob-streaming ops (`OP_STREAM_CHUNK` / `OP_STREAM_END`) and
    /// authenticated inter-node frames always take a drain barrier and run
    /// serially, so their semantics are unchanged.
    pub pipeline_depth: usize,

    /// Buffered (relaxed) redo durability. `false` (default) = strict: every
    /// mutation is fsynced to the redo WAL before it is acked (no acked write is
    /// ever lost on crash). `true` = buffered: a mutation is acked after its
    /// in-memory redo append, and a background flusher (every
    /// [`Self::redo_flush_interval_ms`]) plus the checkpoint barrier make it
    /// durable. This removes the fsync from the ack path — the main write-
    /// throughput lever — at the cost of a bounded crash-loss window: on an
    /// unclean shutdown, mutations acked since the last background flush are
    /// lost. The store stays internally consistent (the redo is the source of
    /// truth, so a lost entry's mutation vanishes atomically). Use only where
    /// the client tolerates re-submitting a small recent tail after a crash.
    pub redo_buffered: bool,

    /// Background redo-flush interval in milliseconds when
    /// [`Self::redo_buffered`] is `true`. Smaller = narrower crash-loss window
    /// but more fsyncs; larger = better coalescing but more at-risk data.
    /// Ignored under strict durability.
    pub redo_flush_interval_ms: u64,

    /// Open the redo log through the OS page cache (buffered I/O) instead of
    /// `O_DIRECT` (Linux) / `F_NOCACHE` (macOS), AND make the background
    /// flusher pwrite WITHOUT a per-flush fsync. `false` (default) keeps the
    /// existing behavior byte-for-byte: the redo device is opened `O_DIRECT`
    /// and every background flush fsyncs.
    ///
    /// When `true`, redo writes go through the page cache (smooth, kernel-
    /// coalesced writeback) and the background flusher skips the per-flush
    /// fsync — durability for the redo then comes from (a) OS writeback and
    /// (b) the checkpoint barrier, which still fsyncs the redo BEFORE it
    /// fences/reclaims the log, so reclamation safety is unchanged. The DATA
    /// device(s) are UNAFFECTED — they always stay `O_DIRECT`. Only the redo
    /// WAL is buffered. This is a relaxed-durability lever (matching a
    /// no-commit-to-device posture): on an unclean shutdown the un-fsynced
    /// redo tail is lost, but the store stays internally consistent because
    /// the data writes for that tail are equally relaxed.
    ///
    /// This implies buffered redo durability: it is only meaningful together
    /// with [`Self::redo_buffered`] (the ack path must already be off the
    /// fsync), and the server enables buffered durability automatically when
    /// this is set.
    pub redo_buffered_io: bool,

    /// Lever 7: use the in-device segment-ring redo layout
    /// (`docs/REDO_SEGMENT_RING_DESIGN.md`) instead of the linear-with-reset log.
    /// `false` (default) keeps the linear layout. A FRESH redo region adopts this
    /// setting; an existing on-disk ring is always used as a ring regardless
    /// (the device format wins), and an existing non-empty LINEAR log stays
    /// linear until it is drained (clean shutdown) + the region reset — the node
    /// warns and runs on linear that session rather than discarding live redo.
    pub redo_segment_ring: bool,

    /// Segment size in bytes for the ring layout when [`Self::redo_segment_ring`]
    /// is enabled. `0` (default) auto-derives ~8 segments from
    /// [`Self::redo_log_size`]. When set, must be a non-zero multiple of
    /// [`Self::device_alignment`] and leave at least 3 whole segments in the
    /// region. Ignored when the ring is disabled.
    pub redo_segment_size: u64,

    /// HTTP listen address for observability endpoints (metrics, health, debug).
    pub http_listen_addr: String,

    /// Whether to allow `listen_addr` / `http_listen_addr` to bind a
    /// non-loopback interface.
    ///
    /// Defaults to `false` so a fresh install only exposes ports on the
    /// loopback interface. Operators that need remote access must explicitly
    /// opt in by setting this to `true` *and* understand that until the mTLS
    /// wave lands the binary protocol and HTTP server have no transport-level
    /// authentication beyond `cluster_secret` for SWIM. See gap #1 in
    /// `docs/TERANODE_PRODUCTION_READINESS_GAPS.md`.
    pub enable_remote_bind: bool,

    /// Whether to register the `/admin/*` and mutating `/debug/*` HTTP routes.
    ///
    /// Defaults to `false` so a fresh install never exposes
    /// `/admin/quiesce`, `/admin/rebalance`, `/admin/drain/{node_id}`,
    /// `/debug/log-level` (PUT), `/debug/records/{txid}`, `/debug/index`,
    /// or `/debug/redo`. Operators that need these endpoints must explicitly
    /// opt in *and* configure [`Self::admin_token`] — see
    /// [`ConfigError::AdminTokenRequired`].
    pub enable_admin_endpoints: bool,

    /// Bearer token required on `Authorization: Bearer <token>` for every
    /// gated admin / debug request when [`Self::enable_admin_endpoints`] is
    /// `true`.
    ///
    /// Set via the TOML field `admin_token = "..."` or the
    /// [`Self::ENV_ADMIN_TOKEN`] environment variable (env wins on conflict;
    /// an empty env value clears the TOML value).
    ///
    /// When `enable_admin_endpoints = true` and this field is `None` or
    /// `Some("")`, [`Self::validate_safe_defaults`] returns
    /// [`ConfigError::AdminTokenRequired`] and the server refuses to start.
    /// When `enable_admin_endpoints = false`, this field is ignored: the
    /// gated sub-router is never built so there is nothing to authenticate.
    ///
    /// Wrapped in [`Secret`] so debug-formatting the whole config does not
    /// leak the token to logs / OTLP traces. See F-G10-007.
    pub admin_token: Option<Secret>,

    /// Block height retention for DAH evaluation.
    pub block_height_retention: u32,

    // -- Cluster settings --
    /// Unique node ID. Must be different for each node in the cluster.
    /// 0 = single-node mode (no clustering).
    pub node_id: u64,

    /// UDP port for SWIM membership protocol.
    pub swim_port: u16,

    /// Seed node addresses for cluster discovery (host:swim_port).
    pub seed_nodes: Vec<String>,

    /// Replication factor (1 = no replication, 2 = master + 1 replica).
    pub replication_factor: u8,

    /// SWIM probe interval in milliseconds.
    pub swim_probe_interval_ms: u64,

    /// SWIM suspicion timeout in milliseconds.
    pub swim_suspicion_timeout_ms: u64,

    /// Topology proposal timeout in milliseconds. `0` means derive from
    /// `max(swim_probe_interval_ms * 3, 500)`.
    pub topology_propose_timeout_ms: u64,

    /// Debounce window in milliseconds for coalescing SWIM membership
    /// changes before proposing a new topology term (W3.3 / audit F5).
    ///
    /// A burst of membership changes — a staggered N-node boot, or a node
    /// flap (dead→alive within a short window) — would otherwise fire one
    /// topology proposal per change, each carrying a full ~(1-1/n)·4096
    /// shard migration round. With round-robin placement that churn is
    /// enormous (a 5-node staggered boot worst-cases into 4 sequential
    /// terms). This window makes the proposer wait until the observed
    /// membership has been *stable* for the debounce period (trailing-edge
    /// debounce) before proposing, so the burst collapses into ONE proposal
    /// against the settled membership.
    ///
    /// `0` (the default) means derive from
    /// `max(swim_probe_interval_ms * 2, swim_suspicion_timeout_ms / 2)`:
    ///   * `2 × probe_interval` lets straggling JOINs in a staggered boot
    ///     arrive before the first proposal.
    ///   * `suspicion_timeout / 2` ensures a flapping node's re-join is
    ///     absorbed before a shrink is proposed on its (transient) LEAVE.
    ///
    /// A single window covers both join and leave; the larger of the two
    /// terms dominates. With the shipped SWIM defaults (200/5000) this is
    /// 2500 ms; with the docker kill-scenario timing (150/1000) it is
    /// 500 ms — small enough that suspicion (1000) + debounce (500) +
    /// exchange + migration still fits the scenarios' 30 s ready / 60 s
    /// migration post-kill bounds.
    ///
    /// To bound deferral when a cluster flaps continuously, the proposer
    /// also force-proposes once the *first* un-proposed change in a burst
    /// is older than `4 ×` this window (the max-wait cap), so churn can
    /// never starve topology progress indefinitely.
    pub topology_debounce_ms: u64,

    /// 16-byte cluster instance UUID, encoded as 32 lowercase hex
    /// characters (no dashes, no `0x` prefix). All nodes in the same
    /// cluster must use the same value; mismatched ids reject
    /// cross-cluster topology proposals at the
    /// [`crate::cluster::topology::TopologyAuthority`] level (P1.1).
    ///
    /// `None` (TOML omits the key) maps to
    /// [`crate::cluster::topology::ClusterId::UNSET`] and falls back to
    /// the F-G8-001 ever-seen heuristic — only single-node demos and
    /// pre-orchestrator deployments should leave it unset in a
    /// multi-node setup.
    pub cluster_id: Option<String>,

    /// Directory for external blob storage (large transaction cold data).
    ///
    /// Default `./teraslab-blobstore` (per F-G10-006). Previously defaulted
    /// to `/blobstore`, which is unwritable for any non-root process and
    /// caused first-create failures on a fresh deploy.
    pub blobstore_path: PathBuf,

    /// Interval in seconds between periodic orphan-blob garbage-collection
    /// sweeps (R-049). Each tick walks the blob store and deletes any blob
    /// whose primary-index entry is absent or not flagged EXTERNAL — debris
    /// from failed creates, aborted uploads, and cancelled migrations.
    /// Default: 3600 seconds (1 hour). Set to 0 to disable the periodic
    /// sweep (recovery-time reconciliation still runs on every startup).
    pub blob_gc_interval_secs: u64,

    /// Redo-log usage fraction (0.0..1.0) at or above which the
    /// background checkpoint task triggers a checkpoint (BC-01).
    /// Default: 0.75. Must satisfy
    /// `0.0 < checkpoint_low_water < checkpoint_high_water < 1.0`.
    pub checkpoint_high_water: f64,

    /// Redo-log usage fraction (0.0..1.0) at or below which the
    /// background checkpoint trigger re-arms after a previous
    /// checkpoint (BC-01, hysteresis). Default: 0.25.
    pub checkpoint_low_water: f64,

    /// Redo-log usage fraction (0.0..1.0) at or above which the background
    /// checkpoint task forces a **blocking** (stop-the-world) checkpoint that
    /// fully drains the redo log, instead of the normal non-blocking (fuzzy)
    /// one. Default: 0.90. Must satisfy
    /// `checkpoint_high_water < checkpoint_emergency_water < 1.0`.
    ///
    /// A fuzzy checkpoint only reclaims the prefix durable before its snapshot
    /// began, so under sustained write load it cannot keep up; this mark is the
    /// hard fallback that prevents the redo from filling to `LogFull`. Set it
    /// comfortably above `checkpoint_high_water` so the fuzzy (non-blocking)
    /// path is the common case — if it collapses onto `high_water` every
    /// checkpoint blocks, defeating the non-blocking design.
    pub checkpoint_emergency_water: f64,

    /// Cadence in milliseconds at which the background checkpoint
    /// task samples redo-log usage (BC-01). Default: 1000 ms. The
    /// sample itself is a single mutex acquire + atomic load on the
    /// redo log, so this can be lowered for tests without measurable
    /// production cost.
    pub checkpoint_poll_interval_ms: u64,

    /// Path for persisted cluster state (peak cluster size for quorum safety).
    /// If not set, derived from the first device path by appending `.cluster`.
    pub cluster_state_path: Option<PathBuf>,

    /// Shared secret for cluster authentication (HMAC-SHA256).
    ///
    /// When set, all SWIM messages and inter-node TCP connections are
    /// authenticated. Peers that cannot produce a valid HMAC are rejected.
    /// All nodes in the cluster must use the same secret.
    ///
    /// Wrapped in [`Secret`] so debug-formatting the whole config does not
    /// leak the secret to logs / OTLP traces. See F-G10-007.
    pub cluster_secret: Option<Secret>,

    /// When `true`, refuse to start in multi-node configurations
    /// (`node_id > 0` OR `replication_factor > 1`) without a `cluster_secret`.
    ///
    /// Default is `true` (production-safe, per F-X-002): a clustered config
    /// with a missing secret is a hard refuse at startup, not a warning.
    /// Operators who want the older trusted-overlay behavior (a missing
    /// secret triggers a boot-time `tracing::warn!` instead of refusing, so
    /// demo / single-host clusters spin up without ceremony) can opt out by
    /// setting `strict_auth = false` in TOML. See F-X-001 / F-X-002 and
    /// `docs/DEPLOYMENT_ASSUMPTIONS.md`.
    pub strict_auth: bool,

    /// Maximum concurrent migration threads per topology change.
    /// Prevents resource exhaustion during rapid churn. Default: 16.
    pub max_migration_threads: usize,

    // -- Replication durability settings --
    /// Replication acknowledgment policy.
    ///
    /// - `"auto"` (default): WriteAll for RF=2, WriteMajority for RF>=3,
    ///   best_effort for RF=1.
    /// - `"write_all"`: Wait for ALL replicas to ACK before client success.
    /// - `"write_majority"`: Wait for floor(RF/2)+1 copies (including master).
    /// - `"best_effort"`: Log replication failures but don't fail the client.
    pub ack_policy: String,

    /// Timeout in milliseconds for each replication batch ACK. Default: 3000.
    pub replication_timeout_ms: u64,

    /// Timeout floor in milliseconds for foreground replication ACKs while
    /// local migration pressure is active. Default: 30000.
    ///
    /// Migration traffic can temporarily contend with live replication on the
    /// same target links; this knob makes the longer pressure-window timeout
    /// explicit instead of silently stretching `replication_timeout_ms`.
    pub replication_timeout_during_migration_ms: u64,

    /// Behavior when the replication ACK policy cannot be satisfied.
    ///
    /// - `"reject"` (default): Fail the mutation with ERR_REPLICATION_FAILED.
    /// - `"best_effort"`: Log the failure but succeed the client request.
    ///
    /// # WARNING — DATA LOSS RISK
    ///
    /// Setting this to `"best_effort"` means that acknowledged writes can be
    /// **permanently lost** if the master node crashes before replicas catch up.
    /// In best-effort mode, the client receives STATUS_OK even when zero
    /// replicas have confirmed the write. If the master then dies, those writes
    /// exist only on the dead master's device and are irrecoverable.
    ///
    /// Only use `"best_effort"` when availability is more important than
    /// durability — e.g., for idempotent workloads where the client can
    /// safely retry, or during planned maintenance windows. For production
    /// deployments where every acknowledged write must survive a single node
    /// failure, keep the default `"reject"`.
    pub replication_degraded_mode: String,

    // -- Migration performance settings --
    /// Number of parallel TCP connections per migration target.
    /// More connections = higher throughput for large migrations, up to the
    /// point where socket/file-descriptor pressure dominates. Override with
    /// TOML or `TERASLAB_MIGRATION_POOL_SIZE` for environment-specific limits.
    /// Default: 128.
    pub migration_pool_size: usize,

    /// Number of records per baseline streaming batch during migration.
    /// Larger batches reduce round-trip overhead but increase memory per batch.
    /// Default: 500.
    pub migration_batch_size: usize,

    /// Interval in seconds between replica lag checks. Default: 30.
    /// Set to 0 to disable lag monitoring.
    pub replica_lag_check_interval_secs: u64,

    /// Replica lag threshold, in redo sequences, for warn logs and HTTP
    /// readiness degradation. Default: 10,000. Set to 0 to log/report lag
    /// without making `/health/ready` fail.
    pub replica_lag_warn_threshold_ops: u64,

    /// Maximum MissingPrimary replay failures tolerated during startup
    /// recovery. MissingPrimary is benign for redo entries superseded by a
    /// later delete, but a very high count can indicate a wrong device/index
    /// pairing. Default preserves the historical cap: 65,536.
    pub recovery_missing_primary_tolerance: u64,

    // -- Index backend settings --
    /// Index backend configuration. Controls whether the primary and secondary
    /// indexes use in-memory hash tables or on-disk redb B+ trees.
    pub index: IndexConfig,

    /// Optional in-RAM data-device block cache. Disabled by default
    /// (`bytes = 0`). See [`CacheConfig`].
    pub cache: CacheConfig,

    /// On-device storage layout (`[storage]`). `packed` is off by default. See
    /// [`StorageConfig`].
    pub storage: StorageConfig,

    /// Expected device identity (hex string). If set, the server refuses to
    /// start if the on-disk identity does not match. Use this to prevent
    /// accidentally pointing at the wrong device.
    ///
    /// The expected value is a 32-character lowercase hex string, as printed
    /// by `device_id_hex()` and logged on first startup.
    pub device_id: Option<String>,

    /// Observability configuration (Phase 4: OTLP tracing).
    ///
    /// Populated from the `[observability]` TOML section. Every field can
    /// be individually overridden via `TERASLAB_*` environment variables —
    /// call [`ServerConfig::apply_env_overrides`] after loading to apply them.
    pub observability: ObservabilityConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:3300".to_string(),
            advertise_addr: None,
            device_paths: vec![PathBuf::from("teraslab-data.dat")],
            device_size: 1024 * 1024 * 1024, // 1 GiB
            device_alignment: 4096,
            device_split: 1,
            redo_log_size: 64 * 1024 * 1024, // 64 MiB
            redo_log_path: None,
            last_durable_height_path: None,
            index_snapshot_path: PathBuf::from("teraslab-index.snap"),
            expected_records: 100_000,
            lock_stripes: 65536,
            max_batch_size: 8192,
            max_connections: 1024,
            max_connections_per_ip: 64,
            max_stream_total_bytes: Self::DEFAULT_MAX_STREAM_TOTAL_BYTES,
            max_active_streams_per_connection: Self::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION,
            stream_idle_timeout_secs: Self::DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
            max_inflight_request_bytes: 256 * 1024 * 1024,
            pipeline_depth: 1,
            redo_buffered: false,
            redo_flush_interval_ms: 5,
            redo_buffered_io: false,
            redo_segment_ring: false,
            redo_segment_size: 0,
            http_listen_addr: "127.0.0.1:9100".to_string(),
            enable_remote_bind: false,
            enable_admin_endpoints: false,
            admin_token: None,
            block_height_retention: 288,
            node_id: 0,
            swim_port: 3301,
            seed_nodes: vec![],
            replication_factor: 1,
            swim_probe_interval_ms: 200,
            swim_suspicion_timeout_ms: 5000,
            topology_propose_timeout_ms: 0,
            topology_debounce_ms: 0,
            cluster_id: None,
            blobstore_path: PathBuf::from("./teraslab-blobstore"),
            blob_gc_interval_secs: 3600,
            checkpoint_high_water: 0.75,
            checkpoint_low_water: 0.25,
            checkpoint_emergency_water: 0.90,
            checkpoint_poll_interval_ms: 1000,
            cluster_state_path: None,
            cluster_secret: None,
            // F-X-002 (production default): strict_auth defaults to `true`
            // so a fresh install rejects unsigned inter-node frames on the
            // data port whenever a `cluster_secret` is configured, and
            // refuses to start a clustered config (`node_id > 0` OR
            // `replication_factor > 1`) without a secret. Trusted-overlay
            // / single-host-demo deployments that intentionally run
            // without a secret can opt back out by setting
            // `strict_auth = false` in TOML (see
            // `docs/DEPLOYMENT_ASSUMPTIONS.md`).
            strict_auth: true,
            max_migration_threads: 16,
            ack_policy: "auto".to_string(),
            replication_timeout_ms: 3000,
            replication_timeout_during_migration_ms: 30000,
            replication_degraded_mode: "reject".to_string(),
            migration_pool_size: 128,
            migration_batch_size: 500,
            replica_lag_check_interval_secs: 30,
            replica_lag_warn_threshold_ops: 10_000,
            recovery_missing_primary_tolerance: 65_536,
            index: IndexConfig::default(),
            cache: CacheConfig::default(),
            storage: StorageConfig::default(),
            device_id: None,
            observability: ObservabilityConfig::default(),
        }
    }
}

impl ServerConfig {
    pub const ENV_MIGRATION_POOL_SIZE: &'static str = "TERASLAB_MIGRATION_POOL_SIZE";
    pub const ENV_MIGRATION_BATCH_SIZE: &'static str = "TERASLAB_MIGRATION_BATCH_SIZE";
    pub const ENV_MAX_STREAM_TOTAL_BYTES: &'static str = "TERASLAB_MAX_STREAM_TOTAL_BYTES";
    pub const DEFAULT_MAX_STREAM_TOTAL_BYTES: u64 = 4 * 1024 * 1024 * 1024;

    /// Default for [`Self::max_active_streams_per_connection`]. Mirrors the
    /// style of [`Self::max_connections_per_ip`] (64): well above any
    /// legitimate per-connection upload fan-out, low enough to bound the
    /// fd/tmp footprint of a hostile connection.
    pub const DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION: usize = 64;

    /// Default for [`Self::stream_idle_timeout_secs`] (60 s). Long enough
    /// that a legitimate slow uploader never trips it, short enough that an
    /// abandoned half-open stream's resources are reclaimed promptly.
    pub const DEFAULT_STREAM_IDLE_TIMEOUT_SECS: u64 = 60;

    /// Environment variable that overrides [`Self::admin_token`]. When the
    /// env var is set to a non-empty value it replaces any TOML-configured
    /// token; when set to an empty value it explicitly clears the TOML
    /// token (so an operator can disable a baked-in token without editing
    /// the file). When the env var is absent the TOML value is preserved.
    pub const ENV_ADMIN_TOKEN: &'static str = "TERASLAB_ADMIN_TOKEN";

    /// Whether clustering is enabled (node_id > 0).
    pub fn is_clustered(&self) -> bool {
        self.node_id > 0
    }

    /// Resolve the topology proposal timeout used by non-proposer nodes
    /// waiting for the deterministic proposer to broadcast a term.
    pub fn resolved_topology_propose_timeout_ms(&self) -> u64 {
        if self.topology_propose_timeout_ms == 0 {
            self.swim_probe_interval_ms.saturating_mul(3).max(500)
        } else {
            self.topology_propose_timeout_ms
        }
    }

    /// Resolve the topology-proposal debounce window (W3.3). `0` derives
    /// `max(swim_probe_interval_ms * 2, swim_suspicion_timeout_ms / 2)`; an
    /// explicit non-zero value is used verbatim. See
    /// [`Self::topology_debounce_ms`] for the derivation rationale.
    pub fn resolved_topology_debounce_ms(&self) -> u64 {
        if self.topology_debounce_ms == 0 {
            self.swim_probe_interval_ms
                .saturating_mul(2)
                .max(self.swim_suspicion_timeout_ms / 2)
        } else {
            self.topology_debounce_ms
        }
    }

    /// Parse [`Self::cluster_id`] into a 16-byte
    /// [`crate::cluster::topology::ClusterId`].
    ///
    /// Accepts exactly 32 lowercase or uppercase hex digits (no dashes,
    /// no `0x` prefix). Returns
    /// [`crate::cluster::topology::ClusterId::UNSET`] when the field is
    /// absent. Any malformed value yields a typed error so startup
    /// refuses rather than silently degrading to UNSET.
    pub fn resolved_cluster_id(&self) -> Result<crate::cluster::topology::ClusterId, ConfigError> {
        let s = match &self.cluster_id {
            None => return Ok(crate::cluster::topology::ClusterId::UNSET),
            Some(s) => s.trim(),
        };
        if s.is_empty() {
            return Ok(crate::cluster::topology::ClusterId::UNSET);
        }
        if s.len() != 32 {
            return Err(ConfigError::InvalidClusterId {
                reason: format!(
                    "cluster_id must be 32 hex chars (16 bytes); got {} chars",
                    s.len()
                ),
            });
        }
        let mut bytes = [0u8; 16];
        for (i, byte) in bytes.iter_mut().enumerate() {
            let hi = hex_nibble(s.as_bytes()[2 * i])?;
            let lo = hex_nibble(s.as_bytes()[2 * i + 1])?;
            *byte = (hi << 4) | lo;
        }
        Ok(crate::cluster::topology::ClusterId(bytes))
    }

    /// Resolve the redo log file path. Uses `redo_log_path` if explicitly set,
    /// otherwise derives it from the first device path by appending `.redo`.
    ///
    /// When `redo_log_path` is `None` and `device_paths` is empty (a
    /// Whether buffered (relaxed) redo durability is in effect.
    ///
    /// `true` when [`Self::redo_buffered`] is set OR [`Self::redo_buffered_io`]
    /// is set: the page-cache redo open + no-per-flush-fsync flusher only make
    /// sense once the ack path is already off the fsync, so `redo_buffered_io`
    /// implies buffered durability. This single source of truth gates both the
    /// engine's `set_buffered_durability` and the background flusher's spawn.
    pub fn redo_buffered_effective(&self) -> bool {
        self.redo_buffered || self.redo_buffered_io
    }

    /// misconfiguration that `validate_safe_defaults` rejects with
    /// `ConfigError::NoDevicePaths`), this falls back to the built-in
    /// default `teraslab-data.dat.redo` rather than panicking. The
    /// validation gate is the source of truth — see F-G10-004.
    pub fn resolved_redo_log_path(&self) -> PathBuf {
        match &self.redo_log_path {
            Some(p) => p.clone(),
            None => {
                let base = self
                    .device_paths
                    .first()
                    .cloned()
                    .unwrap_or_else(|| PathBuf::from("teraslab-data.dat"));
                let mut p = base.into_os_string();
                p.push(".redo");
                PathBuf::from(p)
            }
        }
    }

    /// Resolve the index snapshot file path.
    ///
    /// Uses [`Self::index_snapshot_path`] verbatim when it carries a directory
    /// component (absolute, or relative with a parent like `data/snap`). When
    /// it is a **bare relative name** (no directory — e.g. the default
    /// `teraslab-index.snap`), co-locate it with the first device's directory
    /// so the checkpoint writes the snapshot into the (persisted, writable)
    /// data directory rather than the process's current working directory.
    ///
    /// Issue #13: a bare relative name also tripped an ENOENT in the parent-dir
    /// fsync (fixed in `fsync_parent_dir`), but even with that fixed, writing
    /// the snapshot to the container cwd (`/`, not a mounted volume) would lose
    /// it on every restart and force a full device rebuild — so the default is
    /// co-located with the data device here.
    ///
    /// Falls back to the configured path unchanged when no device directory can
    /// be derived (`validate_safe_defaults` gates an empty device list).
    pub fn resolved_index_snapshot_path(&self) -> PathBuf {
        if let Some(parent) = self.index_snapshot_path.parent()
            && !parent.as_os_str().is_empty()
        {
            return self.index_snapshot_path.clone();
        }
        let file_name = self
            .index_snapshot_path
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("teraslab-index.snap"));
        match self.device_paths.first().and_then(|d| d.parent()) {
            Some(dir) if !dir.as_os_str().is_empty() => dir.join(file_name),
            _ => self.index_snapshot_path.clone(),
        }
    }

    /// Resolve the durable node-height file path. Uses
    /// `last_durable_height_path` if explicitly set, otherwise derives it from
    /// the resolved index snapshot path by appending `.height`.
    ///
    /// Deriving from the snapshot path (rather than a device path) co-locates
    /// the height with the other engine-derived durable artifacts the
    /// checkpoint task writes (index snapshot, allocator header) and works
    /// even for in-memory device configurations used in tests.
    pub fn resolved_last_durable_height_path(&self) -> PathBuf {
        match &self.last_durable_height_path {
            Some(p) => p.clone(),
            None => {
                let mut p = self.resolved_index_snapshot_path().into_os_string();
                p.push(".height");
                PathBuf::from(p)
            }
        }
    }

    /// Resolve the cluster state file path. Uses `cluster_state_path` if set,
    /// otherwise derives from the first device path by appending `.cluster`.
    ///
    /// Same fallback story as [`Self::resolved_redo_log_path`] when
    /// `device_paths` is empty — `validate_safe_defaults` is the gate.
    pub fn resolved_cluster_state_path(&self) -> PathBuf {
        match &self.cluster_state_path {
            Some(p) => p.clone(),
            None => {
                let base = self
                    .device_paths
                    .first()
                    .cloned()
                    .unwrap_or_else(|| PathBuf::from("teraslab-data.dat"));
                let mut p = base.into_os_string();
                p.push(".cluster");
                PathBuf::from(p)
            }
        }
    }

    /// Resolve the replication ACK policy based on config and replication factor.
    ///
    /// Returns `None` when replication is best-effort (RF=1 or explicit "best_effort").
    /// Returns the appropriate `AckPolicy` otherwise.
    pub fn resolved_ack_policy(&self) -> Option<crate::replication::manager::AckPolicy> {
        use crate::replication::manager::AckPolicy;
        match self.ack_policy.as_str() {
            "write_all" => Some(AckPolicy::WriteAll),
            "write_majority" => Some(AckPolicy::WriteMajority),
            "best_effort" => None,
            "auto" => match self.replication_factor {
                0 | 1 => None,
                2 => Some(AckPolicy::WriteAll),
                _ => Some(AckPolicy::WriteMajority),
            },
            // `validate_cluster_safety` rejects this before startup. Keep the
            // runtime fallback conservative for callers that resolve before
            // validating.
            _ => Some(AckPolicy::WriteAll),
        }
    }

    /// Whether replication failures should be tolerated (best_effort mode).
    pub fn is_replication_best_effort(&self) -> bool {
        self.replication_degraded_mode == "best_effort"
    }

    /// Validate cluster durability settings against the server safety contract.
    ///
    /// Rejects `replication_degraded_mode = "best_effort"` when the configured
    /// replication factor (RF) is greater than 1. With RF > 1 the cluster has
    /// replicas whose ACKs define durability; allowing best-effort mode would
    /// silently degrade the contract to single-node durability without any
    /// operator-visible signal. If RF = 1 (or 0) there are no replicas to
    /// ACK, so the flag is a no-op and the combination is allowed.
    ///
    /// See also: [`STATUS_DEGRADED_DURABILITY`](crate::protocol::opcodes::STATUS_DEGRADED_DURABILITY)
    /// — the runtime signal emitted when RF > 1 best-effort is *not* in use
    /// but individual best-effort paths fall back because replicas ACK-failed.
    pub fn validate_cluster_safety(&self) -> std::result::Result<(), String> {
        match self.ack_policy.as_str() {
            "auto" | "write_all" | "write_majority" | "best_effort" => {}
            other => {
                return Err(format!(
                    "unknown ack_policy {other:?}: expected \"auto\", \"write_all\", \
                     \"write_majority\", or \"best_effort\"",
                ));
            }
        }
        match self.replication_degraded_mode.as_str() {
            "reject" | "best_effort" => {}
            other => {
                return Err(format!(
                    "unknown replication_degraded_mode {other:?}: expected \"reject\" or \"best_effort\"",
                ));
            }
        }
        if self.replication_factor > 1 && self.replication_degraded_mode == "best_effort" {
            return Err(format!(
                "replication_degraded_mode = \"best_effort\" is not allowed with \
                 replication_factor = {} (> 1): acknowledged writes could be lost \
                 if the master crashes before replicas catch up. Either set \
                 replication_degraded_mode = \"reject\" or lower replication_factor to 1.",
                self.replication_factor,
            ));
        }
        if self.replication_factor > 1 && self.ack_policy == "best_effort" {
            return Err(format!(
                "ack_policy = \"best_effort\" is not allowed with replication_factor = {} (> 1): \
                 it disables replica ACK enforcement and can acknowledge writes with zero durable \
                 replicas. Use \"auto\", \"write_all\", or \"write_majority\", or lower \
                 replication_factor to 1.",
                self.replication_factor,
            ));
        }
        Ok(())
    }

    /// Validate the `device_id` config value, if set.
    ///
    /// Returns `Ok(())` if absent or a valid 32-char lowercase hex string.
    /// Returns `Err` with a descriptive message otherwise.
    pub fn validate_device_id(&self) -> std::result::Result<(), String> {
        if let Some(ref id) = self.device_id {
            if id.len() != 32 {
                return Err(format!(
                    "device_id must be exactly 32 hex characters, got {} characters",
                    id.len()
                ));
            }
            if !id
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
            {
                return Err(
                    "device_id must contain only lowercase hex digits (0-9, a-f)".to_string(),
                );
            }
        }
        Ok(())
    }

    /// Maximum allowed `block_height_retention` value.
    ///
    /// At BSV's 10-minute target block time, 10,000,000 blocks is roughly
    /// 190 years — well beyond any realistic retention policy. Capping here
    /// ensures `current_block_height + block_height_retention` cannot
    /// overflow `u32` for any remotely plausible current height, turning
    /// the defense-in-depth `checked_add` in `evaluate_delete_at_height`
    /// into an impossibility guard in practice. The runtime path still
    /// returns `SpendError::DahOverflow` if overflow ever does occur.
    pub const MAX_BLOCK_HEIGHT_RETENTION: u32 = 10_000_000;

    /// Apply all `TERASLAB_*` environment overrides to runtime config.
    pub fn apply_env_overrides(&mut self) -> std::result::Result<(), String> {
        self.apply_migration_env_overrides()?;
        self.apply_stream_env_overrides()?;
        self.apply_observability_env_overrides()?;
        self.apply_admin_token_env_override();
        Ok(())
    }

    /// Apply the [`Self::ENV_ADMIN_TOKEN`] override.
    ///
    /// Semantics (matches the rest of the env-override surface):
    ///
    /// - Env absent → leave TOML value unchanged.
    /// - Env present and non-empty → replace the TOML value with the env
    ///   value (env wins).
    /// - Env present and empty → clear the TOML value (`None`).
    ///
    /// Empty / missing token is *not* an error here: the actual gate lives in
    /// [`Self::validate_safe_defaults`], which only refuses startup when the
    /// admin endpoints are also enabled. That keeps a deployment that opts
    /// out of the admin surface entirely from needing a vestigial token.
    pub fn apply_admin_token_env_override(&mut self) {
        match std::env::var(Self::ENV_ADMIN_TOKEN) {
            Ok(raw) if raw.is_empty() => {
                // Explicit empty env value clears the TOML default.
                self.admin_token = None;
            }
            Ok(raw) => {
                self.admin_token = Some(Secret::new(raw));
            }
            Err(_) => {
                // Env var not set / not unicode — preserve TOML value.
            }
        }
    }

    /// Apply `TERASLAB_*` environment overrides to migration tuning.
    ///
    /// These knobs are intentionally independent from the TOML defaults so
    /// constrained Docker runs can lower fan-out while production can keep or
    /// raise it without baking environment-specific limits into code.
    pub fn apply_migration_env_overrides(&mut self) -> std::result::Result<(), String> {
        if let Some(value) = parse_usize_env(Self::ENV_MIGRATION_POOL_SIZE)? {
            self.migration_pool_size = value;
        }
        if let Some(value) = parse_usize_env(Self::ENV_MIGRATION_BATCH_SIZE)? {
            self.migration_batch_size = value;
        }
        Ok(())
    }

    /// Apply `TERASLAB_MAX_STREAM_TOTAL_BYTES` to the per-connection streaming
    /// upload cap.
    pub fn apply_stream_env_overrides(&mut self) -> std::result::Result<(), String> {
        if let Some(value) = parse_u64_env(Self::ENV_MAX_STREAM_TOTAL_BYTES)? {
            self.max_stream_total_bytes = value;
        }
        Ok(())
    }

    /// Apply `TERASLAB_*` environment overrides to the observability
    /// subsection. Call this once after [`Self::load`] so config-file
    /// values are visible to validation before env vars take precedence.
    ///
    /// Returns an error if `TERASLAB_TRACE_SAMPLING_RATIO` is present but
    /// does not parse as `f64`.
    pub fn apply_observability_env_overrides(&mut self) -> std::result::Result<(), String> {
        self.observability
            .apply_env_overrides()
            .map_err(|e| e.to_string())
    }

    /// Validate observability settings against the startup contract.
    ///
    /// Currently only checks that `trace_sampling_ratio` is in `[0.0, 1.0]`.
    /// OTLP endpoint shape is left to the exporter build step — a malformed
    /// endpoint surfaces as an exporter construction error at init time, not
    /// a config-time error, because DNS/TCP reachability is runtime-only.
    pub fn validate_observability(&self) -> std::result::Result<(), String> {
        self.observability.validate().map_err(|e| e.to_string())
    }

    /// Validate the bind/auth safe defaults that gate gap #1 of the
    /// production-readiness review.
    ///
    /// Currently enforces:
    ///
    /// 1. `listen_addr` and `http_listen_addr` parse as `host:port`.
    /// 2. Non-loopback bind requires `enable_remote_bind = true`. Until the
    ///    mTLS wave lands, the binary protocol and HTTP admin endpoints have
    ///    no per-connection authentication, so binding a routable interface
    ///    is only safe on a private/audited network with the explicit opt-in.
    /// 3. Cluster mode (`node_id > 0`) or `replication_factor > 1` requires a
    ///    non-empty `cluster_secret`. Cluster mode keys SWIM and inter-node
    ///    TCP frames on the shared secret; an empty secret means anyone
    ///    reachable on those ports can spoof membership, topology,
    ///    replication, or migration messages.
    /// 4. `enable_admin_endpoints = true` requires a non-empty `admin_token`
    ///    (TOML field or `TERASLAB_ADMIN_TOKEN` env override). Without a
    ///    token the mutating `/admin/*` and `/debug/*` surface would be
    ///    reachable by anyone with TCP access to the HTTP port.
    ///
    /// Errors are returned as a [`ConfigError`] enum so callers can map them to
    /// startup-fatal codes.
    pub fn validate_safe_defaults(&self) -> std::result::Result<(), ConfigError> {
        // (0a) device_paths must be non-empty (resolve_redo_log_path / cluster
        // state path index `[0]` unconditionally). See F-G10-004.
        if self.device_paths.is_empty() {
            return Err(ConfigError::NoDevicePaths);
        }

        // Store layout: total stores = device_paths.len() * device_split, and
        // a store index must fit the index entry's u8 device_id.
        let num_stores = self.device_paths.len().saturating_mul(self.device_split);
        if self.device_split == 0 || num_stores == 0 || num_stores > crate::subdevice::MAX_STORES {
            return Err(ConfigError::InvalidStoreLayout {
                paths: self.device_paths.len(),
                split: self.device_split,
            });
        }
        // Multi-store device-scan rebuild is only implemented for the in-memory
        // backend (`load_sharded_index_in_memory_multi` scans every store). The
        // redb / file-backed primary rebuilds scan store 0 only, so a multi-store
        // node on those backends would silently lose records on stores 1..N after
        // a snapshot/index-file loss. Fail closed.
        if num_stores > 1 && self.index.backend != IndexBackendMode::Memory {
            return Err(ConfigError::MultiStoreRequiresMemoryBackend {
                stores: num_stores,
                backend: self.index.backend.clone(),
            });
        }

        // Write-back caching needs a non-zero buffer to defer writes into.
        if self.cache.writeback && self.cache.bytes == 0 {
            return Err(ConfigError::WriteBackRequiresCacheBytes);
        }

        // Packed mode is only safe when the device block (the RMW unit) is no
        // larger than the 4096-byte io_locks lock block; otherwise two packed
        // records in one physical block could map to different lock stripes and
        // a shared block could be under-locked. Off by default → no-op.
        if self.storage.packed && self.device_alignment > PACKED_MAX_DEVICE_ALIGNMENT {
            return Err(ConfigError::PackedAlignmentTooLarge {
                device_alignment: self.device_alignment,
            });
        }

        // (0b) Size sanity gates. Pre-fix `device_alignment = 0` or
        // non-power-of-2 `lock_stripes` produced cryptic runtime panics.
        self.validate_sizes()?;

        // (1) + (2): listen_addr.
        let listen_ip =
            parse_bind_host(&self.listen_addr).map_err(|e| ConfigError::InvalidListenAddr {
                addr: self.listen_addr.clone(),
                source: e,
            })?;
        if !listen_ip.is_loopback() && !self.enable_remote_bind {
            return Err(ConfigError::RemoteBindRefused {
                field: "listen_addr",
                addr: self.listen_addr.clone(),
            });
        }

        // (1) + (2): http_listen_addr.
        let http_ip = parse_bind_host(&self.http_listen_addr).map_err(|e| {
            ConfigError::InvalidHttpListenAddr {
                addr: self.http_listen_addr.clone(),
                source: e,
            }
        })?;
        if !http_ip.is_loopback() && !self.enable_remote_bind {
            return Err(ConfigError::RemoteBindRefused {
                field: "http_listen_addr",
                addr: self.http_listen_addr.clone(),
            });
        }

        // (2b) advertise_addr (when set) must parse — pre-fix the daemon
        // bin called `.expect("invalid advertise_addr")` post-validation,
        // turning misconfig into a cryptic panic. See F-G10-013.
        if let Some(ref adv) = self.advertise_addr {
            adv.parse::<std::net::SocketAddr>()
                .map_err(|e| ConfigError::InvalidAdvertiseAddr {
                    addr: adv.clone(),
                    source: e,
                })?;
        }

        // (3) Cluster/SWIM mode or RF>1 requires a cluster_secret.
        //
        // Per the trusted-overlay deployment model (see
        // `docs/DEPLOYMENT_ASSUMPTIONS.md`) the default behaviour is
        // fail-open with a startup warning logged from the daemon binary —
        // hard rejection only happens when the operator opts into
        // `strict_auth = true` (or `--strict-auth`). See F-X-001.
        let multi_node = self.is_clustered() || self.replication_factor > 1;
        let cluster_secret_missing = self
            .cluster_secret
            .as_ref()
            .map(|s| s.is_empty())
            .unwrap_or(true);
        if multi_node && cluster_secret_missing && self.strict_auth {
            return Err(ConfigError::StrictAuthRequiresSecret);
        }

        // (3b) cluster_secret entropy. Even in non-strict mode an explicit
        // short secret is rejected — a 1-byte secret is a typo, not a
        // deployment choice, and the HMAC offers no protection at that
        // length. See F-G10-011.
        if let Some(ref s) = self.cluster_secret
            && !s.is_empty()
            && s.len() < Self::MIN_CLUSTER_SECRET_LEN
        {
            return Err(ConfigError::ClusterSecretTooShort {
                actual: s.len(),
                min: Self::MIN_CLUSTER_SECRET_LEN,
            });
        }

        // (3c) F-E2 — split-brain safety. A clustered node with a shared
        // cluster_secret but an UNSET cluster_id cannot reject a foreign
        // cluster's membership merge (the cross-cluster guard needs BOTH sides
        // to advertise a real id), so two independently-bootstrapped clusters
        // can merge. Under strict_auth, require a persisted, well-formed
        // cluster_id on clustered nodes so the guard is always armed. Mirrors
        // the cluster_secret requirement above (trusted-overlay model: warn by
        // default, hard-reject only under strict_auth). Checked after the
        // cluster_secret presence/length gates so those errors take precedence.
        // `resolved_cluster_id` also surfaces a malformed id here.
        if self.is_clustered() && self.strict_auth && self.resolved_cluster_id()?.is_unset() {
            return Err(ConfigError::StrictAuthRequiresClusterId);
        }

        // (4) enable_admin_endpoints requires a non-empty admin_token.
        // We treat both `None` and `Some("")` as "no token configured" so a
        // degenerate TOML entry (`admin_token = ""`) is rejected on the same
        // code path as omitting the field entirely.
        if self.enable_admin_endpoints
            && self
                .admin_token
                .as_ref()
                .map(|s| s.is_empty())
                .unwrap_or(true)
        {
            return Err(ConfigError::AdminTokenRequired);
        }

        // (4b) When the admin surface is reachable over a network
        // (`enable_admin_endpoints && enable_remote_bind`), the token must
        // carry enough entropy. A 1-char token over the public internet is
        // brute-forceable in milliseconds. See F-G10-010.
        if self.enable_admin_endpoints
            && self.enable_remote_bind
            && let Some(ref t) = self.admin_token
            && !t.is_empty()
            && t.len() < Self::MIN_REMOTE_ADMIN_TOKEN_LEN
        {
            return Err(ConfigError::AdminTokenTooShort {
                actual: t.len(),
                min: Self::MIN_REMOTE_ADMIN_TOKEN_LEN,
            });
        }

        Ok(())
    }

    /// Minimum length (in bytes) for `cluster_secret` when set. 16 bytes ≈
    /// 128 bits of entropy from a properly random source — the same lower
    /// bound the audit recommends in F-G10-011.
    pub const MIN_CLUSTER_SECRET_LEN: usize = 16;

    /// Minimum length (in bytes) for `admin_token` when both
    /// `enable_admin_endpoints` and `enable_remote_bind` are on. See
    /// F-G10-010.
    pub const MIN_REMOTE_ADMIN_TOKEN_LEN: usize = 16;

    /// Validate the size / cardinality knobs. Pre-fix these passed through
    /// with `0` / `usize::MAX` / non-power-of-2 values and produced runtime
    /// panics or cryptic errors (divide-by-zero in alignment math, overflow
    /// in hashtable capacity). See F-G10-005.
    pub fn validate_sizes(&self) -> std::result::Result<(), ConfigError> {
        fn pow2(name: &str, v: usize) -> std::result::Result<(), ConfigError> {
            if v == 0 || !v.is_power_of_two() {
                return Err(ConfigError::InvalidSizing(format!(
                    "{name} = {v} must be a non-zero power of two"
                )));
            }
            Ok(())
        }
        fn nonzero_usize(name: &str, v: usize) -> std::result::Result<(), ConfigError> {
            if v == 0 {
                return Err(ConfigError::InvalidSizing(format!(
                    "{name} must be non-zero"
                )));
            }
            Ok(())
        }
        fn nonzero_u32(name: &str, v: u32) -> std::result::Result<(), ConfigError> {
            if v == 0 {
                return Err(ConfigError::InvalidSizing(format!(
                    "{name} must be non-zero"
                )));
            }
            Ok(())
        }
        fn nonzero_u64(name: &str, v: u64) -> std::result::Result<(), ConfigError> {
            if v == 0 {
                return Err(ConfigError::InvalidSizing(format!(
                    "{name} must be non-zero"
                )));
            }
            Ok(())
        }

        pow2("device_alignment", self.device_alignment)?;
        pow2("lock_stripes", self.lock_stripes)?;
        // index_shards is silently clamped by ShardedIndex constructors but a
        // non-power-of-two or out-of-range value in the config file is almost
        // certainly an operator mistake; reject it here so the error surfaces at
        // startup rather than silently running with a different shard count.
        {
            let s = self.index.index_shards;
            if s == 0 || !s.is_power_of_two() || s > 256 {
                return Err(ConfigError::InvalidSizing(format!(
                    "index.index_shards = {s} must be a non-zero power of two in [1, 256]"
                )));
            }
        }
        nonzero_u64("device_size", self.device_size)?;
        nonzero_u64("redo_log_size", self.redo_log_size)?;
        nonzero_usize("expected_records", self.expected_records)?;
        nonzero_u32("max_batch_size", self.max_batch_size)?;
        nonzero_usize("max_connections", self.max_connections)?;

        // Lever 7: validate an explicit ring segment size (0 = auto-derive). The
        // redo region (minus its one-block header) must hold at least 3 whole
        // segments, each independently O_DIRECT-writable (alignment-multiple).
        if self.redo_segment_ring && self.redo_segment_size != 0 {
            let align = self.device_alignment as u64;
            if !self.redo_segment_size.is_multiple_of(align) {
                return Err(ConfigError::InvalidSizing(format!(
                    "redo_segment_size = {} must be a multiple of device_alignment {}",
                    self.redo_segment_size, self.device_alignment
                )));
            }
            let entries = self.redo_log_size.saturating_sub(align);
            if entries / self.redo_segment_size < 3 {
                return Err(ConfigError::InvalidSizing(format!(
                    "redo_segment_size = {} leaves fewer than 3 segments in a {}-byte redo region",
                    self.redo_segment_size, self.redo_log_size
                )));
            }
        }

        // BC-01: checkpoint watermarks must form a valid hysteresis
        // band (0 < low < high < 1) so the background trigger has
        // somewhere to fall back to between consecutive checkpoints.
        if !(0.0..1.0).contains(&self.checkpoint_high_water) {
            return Err(ConfigError::InvalidSizing(format!(
                "checkpoint_high_water = {} must be in [0.0, 1.0)",
                self.checkpoint_high_water
            )));
        }
        if !(0.0..1.0).contains(&self.checkpoint_low_water) {
            return Err(ConfigError::InvalidSizing(format!(
                "checkpoint_low_water = {} must be in [0.0, 1.0)",
                self.checkpoint_low_water
            )));
        }
        if self.checkpoint_low_water >= self.checkpoint_high_water {
            return Err(ConfigError::InvalidSizing(format!(
                "checkpoint_low_water ({}) must be strictly less than checkpoint_high_water ({})",
                self.checkpoint_low_water, self.checkpoint_high_water
            )));
        }
        if !(0.0..1.0).contains(&self.checkpoint_emergency_water) {
            return Err(ConfigError::InvalidSizing(format!(
                "checkpoint_emergency_water = {} must be in [0.0, 1.0)",
                self.checkpoint_emergency_water
            )));
        }
        // The emergency (blocking-checkpoint) mark must sit strictly above
        // high_water, else every checkpoint takes the blocking stop-the-world
        // path and the non-blocking fuzzy checkpoint is never used.
        if self.checkpoint_emergency_water <= self.checkpoint_high_water {
            return Err(ConfigError::InvalidSizing(format!(
                "checkpoint_emergency_water ({}) must be strictly greater than \
                 checkpoint_high_water ({}) so the non-blocking checkpoint path is used",
                self.checkpoint_emergency_water, self.checkpoint_high_water
            )));
        }
        nonzero_u64(
            "checkpoint_poll_interval_ms",
            self.checkpoint_poll_interval_ms,
        )?;

        // device_size must be large enough to hold at least one record's
        // worth of data; the runtime allocator otherwise hits a divide-by-
        // zero. Use UTXO_SLOT_SIZE+METADATA_SIZE as the lower bound here.
        const MIN_DEVICE_SIZE: u64 =
            (crate::record::METADATA_SIZE + crate::record::UTXO_SLOT_SIZE) as u64;
        if self.device_size < MIN_DEVICE_SIZE {
            return Err(ConfigError::InvalidSizing(format!(
                "device_size = {} is below the minimum {} bytes (one record header + slot)",
                self.device_size, MIN_DEVICE_SIZE
            )));
        }

        Ok(())
    }

    /// Validate `block_height_retention` against the sanity bound.
    ///
    /// Returns `Err` if `block_height_retention` exceeds
    /// [`Self::MAX_BLOCK_HEIGHT_RETENTION`] — a value so large that
    /// configuring it is almost certainly an operator mistake and would
    /// leave no headroom for `current_block_height` before `u32` overflow.
    pub fn validate_block_height_retention(&self) -> std::result::Result<(), String> {
        if self.block_height_retention > Self::MAX_BLOCK_HEIGHT_RETENTION {
            return Err(format!(
                "block_height_retention = {} exceeds maximum allowed value {} \
                 (roughly 190 years at 10-minute target); configuring this \
                 large a retention would risk u32 overflow on \
                 current_block_height + retention",
                self.block_height_retention,
                Self::MAX_BLOCK_HEIGHT_RETENTION,
            ));
        }
        Ok(())
    }
}

impl ServerConfig {
    /// Load configuration from a TOML file, falling back to defaults.
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file: {e}"))?;
        toml::from_str(&content).map_err(|e| format!("failed to parse config: {e}"))
    }
}

/// E-2: cross-check that the `cluster_secret` the TCP server signs with
/// (from [`ServerConfig`]) is byte-identical to the one the attached
/// [`crate::cluster::coordinator::RunningCluster`] uses for inter-node HMAC.
///
/// The two secrets are independently-populated copies. `ServerConfig::validate`
/// cannot perform this check because it has no view of the running cluster —
/// the coordinator is constructed separately and attached via
/// `Server::with_cluster`. This guard runs at the boundary where both are
/// finally known (server startup) and fails closed with a typed error so a
/// split secret surfaces loudly at startup instead of as a silent
/// cluster-formation hang.
///
/// The check only fires when clustering is actually active: a cluster is
/// attached (`cluster_attached`) AND the deployment is multi-node
/// (`multi_node`, i.e. `node_id > 0` OR `replication_factor > 1`). A true
/// single-node deployment never exchanges authenticated inter-node frames, so
/// a stray cluster handle with no secret is harmless there and must not block
/// startup.
///
/// Empty secrets are normalized to "unset" (`None`-equivalent) so a degenerate
/// `Some(b"")` on one side and `None` on the other are treated as a match, not
/// a mismatch — both mean "no HMAC". Returns [`ConfigError::ClusterSecretMismatch`]
/// when the effective secrets differ.
pub(crate) fn check_cluster_secret_agreement(
    server_secret: Option<&[u8]>,
    cluster_secret: Option<&[u8]>,
    cluster_attached: bool,
    multi_node: bool,
) -> std::result::Result<(), ConfigError> {
    if !(cluster_attached && multi_node) {
        return Ok(());
    }
    // Normalize empty -> None so "unset" and "explicitly empty" compare equal.
    fn normalize(s: Option<&[u8]>) -> Option<&[u8]> {
        s.filter(|bytes| !bytes.is_empty())
    }
    let server = normalize(server_secret);
    let cluster = normalize(cluster_secret);
    if server != cluster {
        let describe = |s: Option<&[u8]>| if s.is_some() { "set" } else { "unset" };
        return Err(ConfigError::ClusterSecretMismatch {
            server_state: describe(server),
            cluster_state: describe(cluster),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_index_config_is_memory() {
        let cfg = IndexConfig::default();
        assert_eq!(cfg.backend, IndexBackendMode::Memory);
        assert!(cfg.is_memory());
        assert!(!cfg.is_redb());
    }

    #[test]
    fn parse_index_backend_memory() {
        let toml_str = r#"
[index]
backend = "memory"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.index.is_memory());
    }

    #[test]
    fn parse_index_backend_redb() {
        let toml_str = r#"
[index]
backend = "redb"
redb_path = "/data/primary.redb"
redb_dah_path = "/data/dah.redb"
redb_unmined_path = "/data/unmined.redb"
redb_cache_size = 536870912
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.index.is_redb());
        assert_eq!(cfg.index.redb_path, PathBuf::from("/data/primary.redb"));
        assert_eq!(cfg.index.redb_dah_path, PathBuf::from("/data/dah.redb"));
        assert_eq!(
            cfg.index.redb_unmined_path,
            PathBuf::from("/data/unmined.redb")
        );
        assert_eq!(cfg.index.redb_cache_size, 536870912);
    }

    #[test]
    fn parse_no_index_section_defaults_to_memory() {
        let toml_str = r#"
listen_addr = "0.0.0.0:3300"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.index.is_memory());
        assert_eq!(cfg.index.redb_cache_size, 256 * 1024 * 1024);
    }

    #[test]
    fn parse_unknown_backend_is_error() {
        let toml_str = r#"
[index]
backend = "rocksdb"
"#;
        let result: std::result::Result<ServerConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown index backend"), "error was: {err}");
    }

    #[test]
    fn parse_empty_backend_defaults_to_memory() {
        let toml_str = r#"
[index]
backend = ""
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.index.is_memory());
    }

    #[test]
    fn file_backed_config() {
        let cfg = IndexConfig {
            backend: IndexBackendMode::FileBacked,
            ..IndexConfig::default()
        };
        assert!(cfg.is_file_backed());
        assert!(!cfg.is_memory());
        assert!(!cfg.is_redb());
        assert_eq!(cfg.file_backed_path, PathBuf::from("teraslab-index.dat"));
    }

    #[test]
    fn deserialize_file_backed_backend() {
        let toml_str = r#"backend = "file_backed""#;
        let cfg: IndexConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.backend, IndexBackendMode::FileBacked);
    }

    #[test]
    fn best_effort_with_rf_3_is_rejected() {
        let cfg = ServerConfig {
            node_id: 7,
            replication_factor: 3,
            replication_degraded_mode: "best_effort".to_string(),
            ..ServerConfig::default()
        };

        let err = cfg.validate_cluster_safety().unwrap_err();
        assert!(err.contains("replication_degraded_mode"));
        assert!(err.contains("best_effort"));
        assert!(err.contains("replication_factor = 3"));
    }

    #[test]
    fn best_effort_with_rf_2_is_rejected() {
        let cfg = ServerConfig {
            node_id: 2,
            replication_factor: 2,
            replication_degraded_mode: "best_effort".to_string(),
            ..ServerConfig::default()
        };

        let err = cfg.validate_cluster_safety().unwrap_err();
        assert!(err.contains("replication_factor = 2"));
    }

    #[test]
    fn ack_policy_best_effort_requires_degraded_mode_best_effort() {
        let cfg = ServerConfig {
            node_id: 7,
            replication_factor: 3,
            ack_policy: "best_effort".to_string(),
            replication_degraded_mode: "reject".to_string(),
            ..ServerConfig::default()
        };

        let err = cfg.validate_cluster_safety().unwrap_err();
        assert!(err.contains("ack_policy"), "error was: {err}");
        assert!(err.contains("best_effort"), "error was: {err}");
        assert!(err.contains("replication_factor = 3"), "error was: {err}");
    }

    #[test]
    fn best_effort_with_rf_1_is_accepted() {
        // RF=1 means no replicas — best_effort is a no-op and permitted.
        let cfg = ServerConfig {
            node_id: 7,
            replication_factor: 1,
            replication_degraded_mode: "best_effort".to_string(),
            ..ServerConfig::default()
        };

        cfg.validate_cluster_safety()
            .expect("RF=1 with best_effort must validate successfully");
    }

    #[test]
    fn multi_store_with_redb_backend_is_rejected() {
        // device_split>1 → 2 stores; only the in-memory backend's device-scan
        // rebuild scans every store, so redb must be rejected (fail closed
        // rather than silently lose records on stores 1..N after a snapshot loss).
        let cfg = ServerConfig {
            device_paths: vec![std::path::PathBuf::from(
                "/tmp/teraslab-multistore-test.dat",
            )],
            device_split: 2,
            index: IndexConfig {
                backend: IndexBackendMode::Redb,
                ..IndexConfig::default()
            },
            ..ServerConfig::default()
        };

        match cfg.validate_safe_defaults() {
            Err(ConfigError::MultiStoreRequiresMemoryBackend { stores, backend }) => {
                assert_eq!(stores, 2);
                assert_eq!(backend, IndexBackendMode::Redb);
            }
            other => panic!("expected MultiStoreRequiresMemoryBackend, got {other:?}"),
        }
    }

    #[test]
    fn writeback_cache_requires_nonzero_bytes() {
        let cfg = ServerConfig {
            cache: CacheConfig {
                bytes: 0,
                writeback: true,
                ..CacheConfig::default()
            },
            ..ServerConfig::default()
        };
        match cfg.validate_safe_defaults() {
            Err(ConfigError::WriteBackRequiresCacheBytes) => {}
            other => panic!("expected WriteBackRequiresCacheBytes, got {other:?}"),
        }
    }

    #[test]
    fn cache_defaults_to_disabled_and_passes_validation() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.cache.bytes, 0, "cache is off by default");
        assert!(!cfg.cache.writeback);
        assert_eq!(
            cfg.cache.writeback_interval_ms, 50,
            "background writeback cadence defaults to 50 ms"
        );
        assert!(!cfg.cache.is_enabled());
        // Default config (cache off) must validate.
        cfg.validate_safe_defaults()
            .expect("default config (cache disabled) must validate");
    }

    #[test]
    fn storage_packed_defaults_off_and_validates() {
        let cfg = ServerConfig::default();
        assert!(!cfg.storage.packed, "packed must default to OFF");
        cfg.validate_safe_defaults()
            .expect("default config (packed off) must validate");
    }

    #[test]
    fn packed_with_4096_alignment_validates() {
        let cfg = ServerConfig {
            storage: StorageConfig {
                packed: true,
                ..StorageConfig::default()
            },
            device_alignment: 4096,
            ..ServerConfig::default()
        };
        cfg.validate_safe_defaults()
            .expect("packed with device_alignment = 4096 must validate");
    }

    #[test]
    fn placement_defaults_to_round_robin() {
        let cfg = ServerConfig::default();
        assert_eq!(
            cfg.storage.placement,
            crate::subdevice::PlacementStrategy::RoundRobin,
            "placement must default to round_robin (unchanged behavior)",
        );
        // A config with no [storage] section at all also defaults to round_robin.
        let cfg2: ServerConfig = toml::from_str("").unwrap();
        assert_eq!(
            cfg2.storage.placement,
            crate::subdevice::PlacementStrategy::RoundRobin,
        );
    }

    #[test]
    fn placement_txid_parses_from_toml() {
        let cfg: ServerConfig = toml::from_str("[storage]\nplacement = \"txid\"\n").unwrap();
        assert_eq!(
            cfg.storage.placement,
            crate::subdevice::PlacementStrategy::Txid,
        );
    }

    #[test]
    fn placement_round_robin_parses_from_toml() {
        let cfg: ServerConfig = toml::from_str("[storage]\nplacement = \"round_robin\"\n").unwrap();
        assert_eq!(
            cfg.storage.placement,
            crate::subdevice::PlacementStrategy::RoundRobin,
        );
    }

    #[test]
    fn placement_unknown_value_is_rejected() {
        let result: std::result::Result<ServerConfig, _> =
            toml::from_str("[storage]\nplacement = \"by_size\"\n");
        let err = result.expect_err("unknown placement strategy must fail to parse");
        assert!(
            err.to_string().contains("unknown placement strategy"),
            "error must name the bad key: {err}",
        );
    }

    #[test]
    fn storage_engine_defaults_to_in_place() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.storage.engine, StorageEngine::InPlace);
    }

    #[test]
    fn storage_engine_segment_parses_with_segment_size_default() {
        let cfg: ServerConfig = toml::from_str("[storage]\nengine = \"segment\"\n").unwrap();
        assert_eq!(cfg.storage.engine, StorageEngine::Segment);
        assert_eq!(cfg.storage.segment_size, 8 * 1024 * 1024, "default 8 MiB");
    }

    #[test]
    fn storage_engine_in_place_and_custom_segment_size_parse() {
        let cfg: ServerConfig =
            toml::from_str("[storage]\nengine = \"in_place\"\nsegment_size = 16777216\n").unwrap();
        assert_eq!(cfg.storage.engine, StorageEngine::InPlace);
        assert_eq!(cfg.storage.segment_size, 16 * 1024 * 1024);
    }

    #[test]
    fn storage_engine_unknown_value_is_rejected() {
        let result: std::result::Result<ServerConfig, _> =
            toml::from_str("[storage]\nengine = \"lsm\"\n");
        let err = result.expect_err("unknown storage engine must fail to parse");
        assert!(
            err.to_string().contains("unknown storage engine"),
            "error must name the bad key: {err}",
        );
    }

    #[test]
    fn redo_segment_ring_defaults_off_and_validates() {
        let cfg = ServerConfig::default();
        assert!(!cfg.redo_segment_ring, "segment ring must default OFF");
        assert_eq!(cfg.redo_segment_size, 0, "segment size defaults to auto");
        cfg.validate_sizes()
            .expect("default config (ring off) must validate");
    }

    #[test]
    fn redo_buffered_io_defaults_off_and_implies_buffered_durability() {
        // Default: both off → strict durability, no buffered effect.
        let cfg = ServerConfig::default();
        assert!(!cfg.redo_buffered_io, "redo_buffered_io must default OFF");
        assert!(!cfg.redo_buffered);
        assert!(
            !cfg.redo_buffered_effective(),
            "neither flag set → not buffered"
        );

        // redo_buffered alone → buffered.
        let buffered = ServerConfig {
            redo_buffered: true,
            ..ServerConfig::default()
        };
        assert!(buffered.redo_buffered_effective());

        // redo_buffered_io alone → implies buffered durability.
        let io = ServerConfig {
            redo_buffered_io: true,
            redo_buffered: false,
            ..ServerConfig::default()
        };
        assert!(
            io.redo_buffered_effective(),
            "redo_buffered_io must imply buffered durability"
        );
    }

    #[test]
    fn redo_buffered_io_parses_from_toml_top_level_scalar() {
        // Top-level scalar before any [section], as required by the async config.
        let toml_str = "redo_buffered = true\nredo_buffered_io = true\n";
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(
            cfg.redo_buffered_io,
            "redo_buffered_io must parse from TOML"
        );
        assert!(cfg.redo_buffered_effective());

        // Absent key → default false (backward compatibility with old configs).
        let cfg2: ServerConfig = toml::from_str("redo_buffered = true\n").unwrap();
        assert!(
            !cfg2.redo_buffered_io,
            "absent redo_buffered_io defaults to false"
        );
    }

    #[test]
    fn redo_segment_ring_auto_and_valid_explicit_size_validate() {
        // Auto (0) is always valid.
        let auto = ServerConfig {
            redo_segment_ring: true,
            redo_segment_size: 0,
            device_alignment: 4096,
            redo_log_size: 64 * 1024 * 1024,
            ..ServerConfig::default()
        };
        auto.validate_sizes()
            .expect("ring with auto segment size validates");

        // Explicit, alignment-multiple, >= 3 segments.
        let explicit = ServerConfig {
            redo_segment_size: 8 * 1024 * 1024,
            ..auto
        };
        explicit
            .validate_sizes()
            .expect("ring with a valid explicit segment size validates");
    }

    #[test]
    fn redo_segment_ring_rejects_bad_segment_size() {
        // Not a multiple of device_alignment.
        let misaligned = ServerConfig {
            redo_segment_ring: true,
            redo_segment_size: 5000,
            device_alignment: 4096,
            redo_log_size: 64 * 1024 * 1024,
            ..ServerConfig::default()
        };
        assert!(
            misaligned.validate_sizes().is_err(),
            "misaligned segment size rejected"
        );

        // Too large → fewer than 3 segments.
        let too_big = ServerConfig {
            redo_segment_ring: true,
            redo_segment_size: 32 * 1024 * 1024,
            device_alignment: 4096,
            redo_log_size: 64 * 1024 * 1024,
            ..ServerConfig::default()
        };
        assert!(
            too_big.validate_sizes().is_err(),
            "segment size leaving < 3 segments rejected"
        );
    }

    #[test]
    fn packed_with_alignment_above_4096_is_rejected() {
        let cfg = ServerConfig {
            storage: StorageConfig {
                packed: true,
                ..StorageConfig::default()
            },
            device_alignment: 8192,
            ..ServerConfig::default()
        };
        match cfg.validate_safe_defaults() {
            Err(ConfigError::PackedAlignmentTooLarge { device_alignment }) => {
                assert_eq!(device_alignment, 8192);
            }
            other => panic!("expected PackedAlignmentTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn nonpacked_with_alignment_above_4096_is_allowed() {
        // The packed-only alignment gate must NOT fire when packing is off:
        // a non-packed device may legitimately use a larger block.
        let cfg = ServerConfig {
            storage: StorageConfig {
                packed: false,
                ..StorageConfig::default()
            },
            device_alignment: 8192,
            ..ServerConfig::default()
        };
        if let Err(e) = cfg.validate_safe_defaults() {
            assert!(
                !matches!(e, ConfigError::PackedAlignmentTooLarge { .. }),
                "non-packed config must not trip the packed alignment gate, got {e:?}"
            );
        }
    }

    #[test]
    fn writethrough_cache_with_bytes_passes_validation() {
        let cfg = ServerConfig {
            cache: CacheConfig {
                bytes: 64 * 1024 * 1024,
                writeback: false,
                ..CacheConfig::default()
            },
            ..ServerConfig::default()
        };
        assert!(cfg.cache.is_enabled());
        cfg.validate_safe_defaults()
            .expect("write-through cache with a budget must validate");
    }

    #[test]
    fn multi_store_with_memory_backend_does_not_trip_backend_guard() {
        // The in-memory backend supports multi-store device-scan rebuild, so the
        // backend guard must NOT fire (later unrelated checks may still fail).
        let cfg = ServerConfig {
            device_paths: vec![std::path::PathBuf::from(
                "/tmp/teraslab-multistore-test.dat",
            )],
            device_split: 4,
            index: IndexConfig {
                backend: IndexBackendMode::Memory,
                ..IndexConfig::default()
            },
            ..ServerConfig::default()
        };

        if let Err(e) = cfg.validate_safe_defaults() {
            assert!(
                !matches!(e, ConfigError::MultiStoreRequiresMemoryBackend { .. }),
                "in-memory backend must not trip the multi-store backend guard, got {e:?}"
            );
        }
    }

    #[test]
    fn reject_mode_with_rf_3_is_accepted() {
        let cfg = ServerConfig {
            node_id: 7,
            replication_factor: 3,
            replication_degraded_mode: "reject".to_string(),
            ..ServerConfig::default()
        };

        cfg.validate_cluster_safety()
            .expect("reject mode must always validate");
    }

    #[test]
    fn unknown_ack_policy_is_rejected() {
        let cfg = ServerConfig {
            ack_policy: "write_quorumish".to_string(),
            ..ServerConfig::default()
        };

        let err = cfg.validate_cluster_safety().unwrap_err();
        assert!(err.contains("unknown ack_policy"), "error was: {err}");
    }

    #[test]
    fn unknown_replication_degraded_mode_is_rejected() {
        let cfg = ServerConfig {
            replication_degraded_mode: "maybe".to_string(),
            ..ServerConfig::default()
        };

        let err = cfg.validate_cluster_safety().unwrap_err();
        assert!(
            err.contains("unknown replication_degraded_mode"),
            "error was: {err}"
        );
    }

    /// H-1/LM-1 + H-2: the streaming-DoS caps default to the documented
    /// values (64 concurrent streams per connection, 60 s idle timeout).
    #[test]
    fn stream_dos_caps_have_expected_defaults() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.max_active_streams_per_connection, 64);
        assert_eq!(
            cfg.max_active_streams_per_connection,
            ServerConfig::DEFAULT_MAX_ACTIVE_STREAMS_PER_CONNECTION
        );
        assert_eq!(cfg.stream_idle_timeout_secs, 60);
        assert_eq!(
            cfg.stream_idle_timeout_secs,
            ServerConfig::DEFAULT_STREAM_IDLE_TIMEOUT_SECS
        );
    }

    #[test]
    fn default_config_validates_cluster_safety() {
        let cfg = ServerConfig::default();
        cfg.validate_cluster_safety()
            .expect("default config must validate");
    }

    #[test]
    fn default_migration_pool_prioritizes_fast_rebalancing() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.migration_pool_size, 128);
        assert_eq!(cfg.migration_batch_size, 500);
    }

    #[test]
    fn default_replica_lag_threshold_is_configured() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.replica_lag_check_interval_secs, 30);
        assert_eq!(cfg.replica_lag_warn_threshold_ops, 10_000);
        assert_eq!(cfg.recovery_missing_primary_tolerance, 65_536);
        assert_eq!(cfg.max_inflight_request_bytes, 256 * 1024 * 1024);
        assert_eq!(cfg.max_connections_per_ip, 64);
    }

    #[test]
    fn topology_propose_timeout_can_be_decoupled_from_probe_interval() {
        let auto_fast_probe = ServerConfig {
            swim_probe_interval_ms: 10,
            ..ServerConfig::default()
        };
        assert_eq!(auto_fast_probe.resolved_topology_propose_timeout_ms(), 500);

        let explicit = ServerConfig {
            swim_probe_interval_ms: 10,
            topology_propose_timeout_ms: 2_500,
            ..ServerConfig::default()
        };
        assert_eq!(explicit.resolved_topology_propose_timeout_ms(), 2_500);
    }

    #[test]
    fn topology_debounce_default_derives_from_swim_timing() {
        // Shipped SWIM defaults (200/5000): suspicion/2 = 2500 dominates
        // 2×probe = 400.
        let shipped = ServerConfig::default();
        assert_eq!(shipped.resolved_topology_debounce_ms(), 2_500);

        // Docker kill-scenario timing (150/1000): 2×probe = 300 vs
        // suspicion/2 = 500 → 500 wins, still well inside the 30s/60s
        // post-kill wait bounds.
        let kill_scenario = ServerConfig {
            swim_probe_interval_ms: 150,
            swim_suspicion_timeout_ms: 1_000,
            ..ServerConfig::default()
        };
        assert_eq!(kill_scenario.resolved_topology_debounce_ms(), 500);

        // Aggressive probe with no suspicion: 2×probe dominates.
        let fast_probe = ServerConfig {
            swim_probe_interval_ms: 400,
            swim_suspicion_timeout_ms: 100,
            ..ServerConfig::default()
        };
        assert_eq!(fast_probe.resolved_topology_debounce_ms(), 800);

        // Explicit value is used verbatim, ignoring the derivation.
        let explicit = ServerConfig {
            swim_probe_interval_ms: 200,
            swim_suspicion_timeout_ms: 5_000,
            topology_debounce_ms: 750,
            ..ServerConfig::default()
        };
        assert_eq!(explicit.resolved_topology_debounce_ms(), 750);
    }

    #[test]
    fn default_block_height_retention_passes_validation() {
        let cfg = ServerConfig::default();
        cfg.validate_block_height_retention()
            .expect("default retention (288) must be well under the bound");
    }

    #[test]
    fn block_height_retention_at_bound_is_accepted() {
        let cfg = ServerConfig {
            block_height_retention: ServerConfig::MAX_BLOCK_HEIGHT_RETENTION,
            ..ServerConfig::default()
        };
        cfg.validate_block_height_retention()
            .expect("exactly at the bound is allowed");
    }

    #[test]
    fn block_height_retention_u32_max_is_rejected() {
        let cfg = ServerConfig {
            block_height_retention: u32::MAX,
            ..ServerConfig::default()
        };
        let err = cfg
            .validate_block_height_retention()
            .expect_err("u32::MAX retention must be rejected");
        assert!(err.contains("block_height_retention"));
        assert!(err.contains("exceeds maximum"));
    }

    #[test]
    fn block_height_retention_one_past_bound_is_rejected() {
        let cfg = ServerConfig {
            block_height_retention: ServerConfig::MAX_BLOCK_HEIGHT_RETENTION + 1,
            ..ServerConfig::default()
        };
        let err = cfg.validate_block_height_retention().unwrap_err();
        assert!(err.contains("block_height_retention"));
    }

    // ------------------------------------------------------------------
    // Observability (Phase 4): TOML + env override wiring
    // ------------------------------------------------------------------

    /// Guards the `TERASLAB_*` env vars across tests so two parallel test
    /// threads don't clobber each other.
    fn obs_env_guard() -> std::sync::MutexGuard<'static, ()> {
        use std::sync::Mutex;
        use std::sync::OnceLock;
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let m = LOCK.get_or_init(|| Mutex::new(()));
        // Poisoned locks are safe here — the inner () has no invariants
        // to break, so we just recover and move on.
        m.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    fn clear_migration_env() {
        unsafe {
            std::env::remove_var(ServerConfig::ENV_MIGRATION_POOL_SIZE);
            std::env::remove_var(ServerConfig::ENV_MIGRATION_BATCH_SIZE);
        }
    }

    fn clear_stream_env() {
        unsafe {
            std::env::remove_var(ServerConfig::ENV_MAX_STREAM_TOTAL_BYTES);
        }
    }

    #[test]
    fn migration_env_overrides_replace_toml_values() {
        let _guard = obs_env_guard();
        clear_migration_env();

        let mut cfg = ServerConfig {
            migration_pool_size: 8,
            migration_batch_size: 64,
            ..ServerConfig::default()
        };
        unsafe {
            std::env::set_var(ServerConfig::ENV_MIGRATION_POOL_SIZE, "256");
            std::env::set_var(ServerConfig::ENV_MIGRATION_BATCH_SIZE, "2048");
        }

        cfg.apply_migration_env_overrides()
            .expect("migration env overrides apply cleanly");

        assert_eq!(cfg.migration_pool_size, 256);
        assert_eq!(cfg.migration_batch_size, 2048);
        clear_migration_env();
    }

    #[test]
    fn max_stream_total_bytes_env_override_respected() {
        let _guard = obs_env_guard();
        clear_stream_env();

        let mut cfg = ServerConfig {
            max_stream_total_bytes: 4096,
            ..ServerConfig::default()
        };
        unsafe {
            std::env::set_var(ServerConfig::ENV_MAX_STREAM_TOTAL_BYTES, "2048");
        }

        cfg.apply_stream_env_overrides()
            .expect("stream env override applies cleanly");

        assert_eq!(cfg.max_stream_total_bytes, 2048);
        clear_stream_env();
    }

    #[test]
    fn max_stream_total_bytes_empty_env_leaves_config_unchanged() {
        let _guard = obs_env_guard();
        clear_stream_env();

        let mut cfg = ServerConfig {
            max_stream_total_bytes: 4096,
            ..ServerConfig::default()
        };
        unsafe {
            std::env::set_var(ServerConfig::ENV_MAX_STREAM_TOTAL_BYTES, " ");
        }

        cfg.apply_stream_env_overrides()
            .expect("empty stream env override is ignored");

        assert_eq!(cfg.max_stream_total_bytes, 4096);
        clear_stream_env();
    }

    #[test]
    fn migration_env_empty_values_leave_config_unchanged() {
        let _guard = obs_env_guard();
        clear_migration_env();

        let mut cfg = ServerConfig {
            migration_pool_size: 24,
            migration_batch_size: 300,
            ..ServerConfig::default()
        };
        unsafe {
            std::env::set_var(ServerConfig::ENV_MIGRATION_POOL_SIZE, "");
            std::env::set_var(ServerConfig::ENV_MIGRATION_BATCH_SIZE, "  ");
        }

        cfg.apply_migration_env_overrides()
            .expect("empty migration env overrides are ignored");

        assert_eq!(cfg.migration_pool_size, 24);
        assert_eq!(cfg.migration_batch_size, 300);
        clear_migration_env();
    }

    #[test]
    fn migration_env_malformed_pool_is_error() {
        let _guard = obs_env_guard();
        clear_migration_env();

        let mut cfg = ServerConfig::default();
        unsafe {
            std::env::set_var(ServerConfig::ENV_MIGRATION_POOL_SIZE, "many");
        }

        let err = cfg.apply_migration_env_overrides().unwrap_err();
        assert!(
            err.contains(ServerConfig::ENV_MIGRATION_POOL_SIZE),
            "err was: {err}",
        );
        clear_migration_env();
    }

    #[test]
    fn apply_env_overrides_applies_migration_and_observability() {
        let _guard = obs_env_guard();
        clear_migration_env();
        unsafe {
            std::env::remove_var(ObservabilityConfig::ENV_OTLP_ENDPOINT);
            std::env::remove_var(ObservabilityConfig::ENV_SAMPLING_RATIO);
            std::env::remove_var(ObservabilityConfig::ENV_SERVICE_NAME);
        }

        let mut cfg = ServerConfig::default();
        unsafe {
            std::env::set_var(ServerConfig::ENV_MIGRATION_POOL_SIZE, "192");
            std::env::set_var(ObservabilityConfig::ENV_SAMPLING_RATIO, "0.75");
        }

        cfg.apply_env_overrides()
            .expect("combined env overrides apply cleanly");

        assert_eq!(cfg.migration_pool_size, 192);
        assert_eq!(cfg.observability.trace_sampling_ratio, 0.75);

        clear_migration_env();
        unsafe {
            std::env::remove_var(ObservabilityConfig::ENV_SAMPLING_RATIO);
        }
    }

    #[test]
    fn observability_config_parses_toml_and_env_override() {
        let _guard = obs_env_guard();
        unsafe {
            std::env::remove_var(ObservabilityConfig::ENV_OTLP_ENDPOINT);
            std::env::remove_var(ObservabilityConfig::ENV_SAMPLING_RATIO);
            std::env::remove_var(ObservabilityConfig::ENV_SERVICE_NAME);
        }

        let toml_str = r#"
[observability]
otlp_endpoint = "http://jaeger.local:4317"
trace_sampling_ratio = 0.25
service_name = "teraslab-test"
"#;
        let mut cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            cfg.observability.otlp_endpoint.as_deref(),
            Some("http://jaeger.local:4317")
        );
        assert_eq!(cfg.observability.trace_sampling_ratio, 0.25);
        assert_eq!(
            cfg.observability.service_name.as_deref(),
            Some("teraslab-test")
        );

        // Env vars override TOML values.
        unsafe {
            std::env::set_var(
                ObservabilityConfig::ENV_OTLP_ENDPOINT,
                "http://otel-collector:4318",
            );
            std::env::set_var(ObservabilityConfig::ENV_SAMPLING_RATIO, "0.5");
            std::env::set_var(ObservabilityConfig::ENV_SERVICE_NAME, "teraslab-env");
        }
        cfg.apply_observability_env_overrides()
            .expect("env overrides apply cleanly");
        assert_eq!(
            cfg.observability.otlp_endpoint.as_deref(),
            Some("http://otel-collector:4318"),
        );
        assert_eq!(cfg.observability.trace_sampling_ratio, 0.5);
        assert_eq!(
            cfg.observability.service_name.as_deref(),
            Some("teraslab-env"),
        );

        // Ratios outside [0.0, 1.0] fail validation.
        cfg.observability.trace_sampling_ratio = 2.0;
        let err = cfg.validate_observability().unwrap_err();
        assert!(
            err.contains("trace_sampling_ratio"),
            "validation error was: {err}",
        );
        cfg.observability.trace_sampling_ratio = -0.01;
        assert!(cfg.validate_observability().is_err());

        unsafe {
            std::env::remove_var(ObservabilityConfig::ENV_OTLP_ENDPOINT);
            std::env::remove_var(ObservabilityConfig::ENV_SAMPLING_RATIO);
            std::env::remove_var(ObservabilityConfig::ENV_SERVICE_NAME);
        }
    }

    #[test]
    fn observability_absent_toml_section_defaults_to_otlp_disabled() {
        let _guard = obs_env_guard();
        unsafe {
            std::env::remove_var(ObservabilityConfig::ENV_OTLP_ENDPOINT);
        }
        let toml_str = r#"
listen_addr = "0.0.0.0:3300"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.observability.otlp_endpoint.is_none());
        assert_eq!(cfg.observability.trace_sampling_ratio, 0.01);
    }

    #[test]
    fn observability_empty_env_endpoint_clears_toml_value() {
        let _guard = obs_env_guard();
        let toml_str = r#"
[observability]
otlp_endpoint = "http://set-via-toml:4317"
"#;
        let mut cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.observability.otlp_endpoint.is_some());

        unsafe {
            std::env::set_var(ObservabilityConfig::ENV_OTLP_ENDPOINT, "");
        }
        cfg.apply_observability_env_overrides().unwrap();
        assert!(
            cfg.observability.otlp_endpoint.is_none(),
            "empty env var must clear the endpoint (disable OTLP)",
        );
        unsafe {
            std::env::remove_var(ObservabilityConfig::ENV_OTLP_ENDPOINT);
        }
    }

    #[test]
    fn observability_env_malformed_ratio_is_error() {
        let _guard = obs_env_guard();
        let mut cfg = ServerConfig::default();
        unsafe {
            std::env::set_var(ObservabilityConfig::ENV_SAMPLING_RATIO, "not-a-float");
        }
        let err = cfg.apply_observability_env_overrides().unwrap_err();
        assert!(
            err.contains("TERASLAB_TRACE_SAMPLING_RATIO"),
            "err was: {err}"
        );
        unsafe {
            std::env::remove_var(ObservabilityConfig::ENV_SAMPLING_RATIO);
        }
    }

    // ------------------------------------------------------------------
    // Safe defaults (gap #1): localhost bind + admin gating + RF>1 secret
    // ------------------------------------------------------------------

    #[test]
    fn default_listen_addrs_are_loopback() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.listen_addr, "127.0.0.1:3300");
        assert_eq!(cfg.http_listen_addr, "127.0.0.1:9100");
        assert!(!cfg.enable_remote_bind);
        assert!(!cfg.enable_admin_endpoints);
    }

    #[test]
    fn default_config_passes_safe_defaults() {
        let cfg = ServerConfig::default();
        cfg.validate_safe_defaults()
            .expect("default config must pass safe-defaults validation");
    }

    #[test]
    fn rf_gt_one_without_cluster_secret_under_strict_auth_is_rejected() {
        // F-X-001: trusted-overlay deployment model — multi-node mode
        // without a cluster_secret is fail-open by default (a warn is
        // logged at boot from the daemon binary). The hard rejection
        // only fires when the operator opts in with `strict_auth = true`
        // (or the `--strict-auth` CLI flag).
        let toml_str = r#"
node_id = 1
replication_factor = 3
strict_auth = true
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        let err = cfg
            .validate_safe_defaults()
            .expect_err("strict_auth + RF>1 + no cluster_secret must be rejected");
        match err {
            ConfigError::StrictAuthRequiresSecret => {}
            other => panic!("expected StrictAuthRequiresSecret, got {other:?}"),
        }
    }

    #[test]
    fn rf_gt_one_with_empty_cluster_secret_under_strict_auth_is_rejected() {
        // F-X-001: an explicit empty cluster_secret is treated as
        // "missing" by the strict-auth check.
        let toml_str = r#"
node_id = 1
replication_factor = 2
cluster_secret = ""
strict_auth = true
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        let err = cfg
            .validate_safe_defaults()
            .expect_err("strict_auth + RF>1 + empty cluster_secret must be rejected");
        match err {
            ConfigError::StrictAuthRequiresSecret => {}
            other => panic!("expected StrictAuthRequiresSecret, got {other:?}"),
        }
    }

    #[test]
    fn rf_gt_one_without_cluster_secret_under_default_auth_is_rejected() {
        // F-X-002: production default flipped strict_auth to `true`, so a
        // clustered config (RF>1 or node_id>0) without a cluster_secret
        // now refuses to start out of the box. The pre-F-X-002 trusted-
        // overlay behaviour is preserved as an explicit
        // `strict_auth = false` opt-out (see
        // `strict_auth_false_opts_back_into_trusted_overlay`).
        let toml_str = r#"
node_id = 1
replication_factor = 3
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(
            cfg.strict_auth,
            "F-X-002: default config must have strict_auth=true",
        );
        let err = cfg.validate_safe_defaults().expect_err(
            "F-X-002: default-auth multi-node config without cluster_secret must be rejected",
        );
        match err {
            ConfigError::StrictAuthRequiresSecret => {}
            other => panic!("expected StrictAuthRequiresSecret, got {other:?}"),
        }
    }

    #[test]
    fn strict_auth_false_opts_back_into_trusted_overlay() {
        // F-X-002 opt-out: operators that deliberately run a trusted-
        // overlay cluster without a cluster_secret can set
        // `strict_auth = false` in TOML. The hard reject is gone, the
        // boot-time warn in `src/bin/server.rs` still fires (covered
        // by separate daemon integration coverage).
        let toml_str = r#"
node_id = 1
replication_factor = 3
strict_auth = false
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(
            !cfg.strict_auth,
            "explicit strict_auth = false must round-trip",
        );
        cfg.validate_safe_defaults().expect(
            "strict_auth = false multi-node config without cluster_secret must validate \
             (legacy trusted-overlay opt-out)",
        );
    }

    #[test]
    fn rf_gt_one_with_cluster_secret_is_accepted() {
        // Clustered (node_id>0) config under the strict-auth default now also
        // requires a cluster_id (F-E2 split-brain guard), so a valid one is set.
        let toml_str = r#"
node_id = 1
replication_factor = 3
cluster_secret = "0123456789abcdef"
cluster_id = "00112233445566778899aabbccddeeff"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        cfg.validate_safe_defaults()
            .expect("RF>1 with cluster_secret + cluster_id must pass");
    }

    #[test]
    fn clustered_without_cluster_id_under_strict_auth_is_rejected() {
        // F-E2: a clustered node (node_id>0) with a valid cluster_secret but no
        // cluster_id cannot arm the cross-cluster merge guard. Under the
        // strict-auth default this must be rejected.
        let toml_str = r#"
node_id = 1
replication_factor = 3
cluster_secret = "0123456789abcdef"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.strict_auth, "default config must have strict_auth=true");
        let err = cfg
            .validate_safe_defaults()
            .expect_err("strict_auth + clustered + no cluster_id must be rejected");
        match err {
            ConfigError::StrictAuthRequiresClusterId => {}
            other => panic!("expected StrictAuthRequiresClusterId, got {other:?}"),
        }
    }

    #[test]
    fn clustered_without_cluster_id_under_trusted_overlay_is_accepted() {
        // F-E2 opt-out: with `strict_auth = false` the missing-cluster_id check
        // is downgraded to a boot-time warning (emitted from src/bin/server.rs),
        // matching the cluster_secret trusted-overlay model.
        let toml_str = r#"
node_id = 1
replication_factor = 3
cluster_secret = "0123456789abcdef"
strict_auth = false
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        cfg.validate_safe_defaults()
            .expect("strict_auth = false clustered config without cluster_id must validate");
    }

    #[test]
    fn empty_cluster_id_under_strict_auth_is_rejected() {
        // An explicit empty cluster_id resolves to UNSET and is treated as
        // "missing" by the strict-auth split-brain check.
        let toml_str = r#"
node_id = 1
replication_factor = 3
cluster_secret = "0123456789abcdef"
cluster_id = ""
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        let err = cfg
            .validate_safe_defaults()
            .expect_err("strict_auth + clustered + empty cluster_id must be rejected");
        match err {
            ConfigError::StrictAuthRequiresClusterId => {}
            other => panic!("expected StrictAuthRequiresClusterId, got {other:?}"),
        }
    }

    #[test]
    fn cluster_mode_requires_secret_under_strict_auth_regardless_of_rf() {
        // F-X-001: even at RF=1, `is_clustered()` (node_id > 0) makes
        // the config "multi-node" for the purposes of the strict-auth
        // check — SWIM frames still flow between members. Rejection
        // only fires when strict_auth opts into the hard check.
        let toml_str = r#"
node_id = 1
replication_factor = 1
strict_auth = true
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        let err = cfg.validate_safe_defaults().expect_err(
            "strict_auth + node_id>0 + no cluster_secret must be rejected even at RF=1",
        );
        match err {
            ConfigError::StrictAuthRequiresSecret => {}
            other => panic!("expected StrictAuthRequiresSecret, got {other:?}"),
        }
    }

    #[test]
    fn single_node_rf_one_without_cluster_secret_is_accepted() {
        let toml_str = r#"
node_id = 0
replication_factor = 1
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        cfg.validate_safe_defaults()
            .expect("true single-node mode needs no cluster_secret");
    }

    #[test]
    fn resolved_index_snapshot_path_colocates_bare_name_with_device_dir() {
        // Issue #13: the default bare-relative snapshot name must resolve into
        // the first device's directory (a persisted volume) rather than the
        // process cwd.
        let cfg = ServerConfig {
            index_snapshot_path: PathBuf::from("teraslab-index.snap"),
            device_paths: vec![PathBuf::from("/data/teraslab.dat")],
            ..ServerConfig::default()
        };
        assert_eq!(
            cfg.resolved_index_snapshot_path(),
            PathBuf::from("/data/teraslab-index.snap"),
        );
        // The derived height path co-locates too.
        assert_eq!(
            cfg.resolved_last_durable_height_path(),
            PathBuf::from("/data/teraslab-index.snap.height"),
        );
    }

    #[test]
    fn resolved_index_snapshot_path_honors_explicit_directory() {
        // An absolute path (or any path with a directory component) is used
        // verbatim — co-location only kicks in for a bare name.
        let cfg = ServerConfig {
            index_snapshot_path: PathBuf::from("/snapshots/idx.snap"),
            device_paths: vec![PathBuf::from("/data/teraslab.dat")],
            ..ServerConfig::default()
        };
        assert_eq!(
            cfg.resolved_index_snapshot_path(),
            PathBuf::from("/snapshots/idx.snap"),
        );

        let cfg_rel = ServerConfig {
            index_snapshot_path: PathBuf::from("sub/idx.snap"),
            device_paths: vec![PathBuf::from("/data/teraslab.dat")],
            ..ServerConfig::default()
        };
        assert_eq!(
            cfg_rel.resolved_index_snapshot_path(),
            PathBuf::from("sub/idx.snap"),
        );
    }

    // E-2: cross-check between the server's secret (ServerConfig) and the
    // attached cluster's secret (ClusterConfig). A split — set in one, absent
    // or different in the other — must fail closed at startup instead of
    // hanging cluster formation forever on silent HMAC failures.

    #[test]
    fn split_cluster_secret_server_only_is_rejected() {
        // Server signs with a secret; cluster has none. Inter-node responses
        // would be signed while proposals expect no signature -> silent hang.
        let err = check_cluster_secret_agreement(
            Some(b"0123456789abcdef"),
            None,
            /* cluster_attached */ true,
            /* multi_node */ true,
        )
        .expect_err("server secret set but cluster secret unset must be rejected");
        match err {
            ConfigError::ClusterSecretMismatch {
                server_state,
                cluster_state,
            } => {
                assert_eq!(server_state, "set");
                assert_eq!(cluster_state, "unset");
            }
            other => panic!("expected ClusterSecretMismatch, got {other:?}"),
        }
    }

    #[test]
    fn split_cluster_secret_cluster_only_is_rejected() {
        let err = check_cluster_secret_agreement(None, Some(b"0123456789abcdef"), true, true)
            .expect_err("cluster secret set but server secret unset must be rejected");
        match err {
            ConfigError::ClusterSecretMismatch {
                server_state,
                cluster_state,
            } => {
                assert_eq!(server_state, "unset");
                assert_eq!(cluster_state, "set");
            }
            other => panic!("expected ClusterSecretMismatch, got {other:?}"),
        }
    }

    #[test]
    fn differing_cluster_secrets_are_rejected() {
        let err = check_cluster_secret_agreement(
            Some(b"secret-aaaaaaaaa"),
            Some(b"secret-bbbbbbbbb"),
            true,
            true,
        )
        .expect_err("two different secrets must be rejected");
        match err {
            ConfigError::ClusterSecretMismatch {
                server_state,
                cluster_state,
            } => {
                assert_eq!(server_state, "set");
                assert_eq!(cluster_state, "set");
            }
            other => panic!("expected ClusterSecretMismatch, got {other:?}"),
        }
    }

    #[test]
    fn matching_cluster_secrets_are_accepted() {
        check_cluster_secret_agreement(
            Some(b"0123456789abcdef"),
            Some(b"0123456789abcdef"),
            true,
            true,
        )
        .expect("identical secrets must agree");
    }

    #[test]
    fn empty_and_unset_secrets_are_treated_as_equal() {
        // Some(b"") on one side and None on the other both mean "no HMAC"
        // and must not be flagged as a mismatch.
        check_cluster_secret_agreement(Some(b""), None, true, true)
            .expect("empty server secret and unset cluster secret both mean no HMAC");
        check_cluster_secret_agreement(None, Some(b""), true, true)
            .expect("unset server secret and empty cluster secret both mean no HMAC");
    }

    #[test]
    fn single_node_skips_cluster_secret_cross_check() {
        // Not multi-node: a stray cluster handle with a mismatched secret is
        // harmless because no authenticated inter-node frames flow. Must not
        // block startup.
        check_cluster_secret_agreement(
            Some(b"server-secret-aaa"),
            None,
            /* cluster_attached */ true,
            /* multi_node */ false,
        )
        .expect("single-node deployment must not run the cross-check");
    }

    #[test]
    fn no_cluster_attached_skips_cross_check() {
        // No cluster coordinator attached: the server's secret is only used
        // for client-facing signing; there is no second copy to disagree.
        check_cluster_secret_agreement(
            Some(b"server-secret-aaa"),
            None,
            /* cluster_attached */ false,
            /* multi_node */ true,
        )
        .expect("with no cluster attached there is nothing to cross-check");
    }

    #[test]
    fn non_loopback_listen_without_remote_bind_is_rejected() {
        let toml_str = r#"
listen_addr = "0.0.0.0:3300"
http_listen_addr = "127.0.0.1:9100"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        let err = cfg
            .validate_safe_defaults()
            .expect_err("0.0.0.0 without enable_remote_bind must be rejected");
        match err {
            ConfigError::RemoteBindRefused { field, addr } => {
                assert_eq!(field, "listen_addr");
                assert_eq!(addr, "0.0.0.0:3300");
            }
            other => panic!("expected RemoteBindRefused, got {other:?}"),
        }
    }

    #[test]
    fn non_loopback_http_listen_without_remote_bind_is_rejected() {
        let toml_str = r#"
listen_addr = "127.0.0.1:3300"
http_listen_addr = "0.0.0.0:9100"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        let err = cfg
            .validate_safe_defaults()
            .expect_err("0.0.0.0 http without enable_remote_bind must be rejected");
        match err {
            ConfigError::RemoteBindRefused { field, addr } => {
                assert_eq!(field, "http_listen_addr");
                assert_eq!(addr, "0.0.0.0:9100");
            }
            other => panic!("expected RemoteBindRefused, got {other:?}"),
        }
    }

    #[test]
    fn non_loopback_listen_with_remote_bind_is_accepted() {
        let toml_str = r#"
listen_addr = "192.168.1.10:3300"
http_listen_addr = "192.168.1.10:9100"
enable_remote_bind = true
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        cfg.validate_safe_defaults()
            .expect("non-loopback bind with explicit opt-in must validate");
    }

    #[test]
    fn ipv6_loopback_is_treated_as_loopback() {
        let toml_str = r#"
listen_addr = "[::1]:3300"
http_listen_addr = "[::1]:9100"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        cfg.validate_safe_defaults()
            .expect("[::1] (IPv6 loopback) must be treated as loopback");
    }

    #[test]
    fn malformed_listen_addr_is_rejected() {
        let toml_str = r#"
listen_addr = "not-a-socket-addr"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        let err = cfg.validate_safe_defaults().unwrap_err();
        match err {
            ConfigError::InvalidListenAddr { addr, .. } => {
                assert_eq!(addr, "not-a-socket-addr");
            }
            other => panic!("expected InvalidListenAddr, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // R-056 (gap LMNH-08 / F14): admin-token requirement when admin
    // endpoints are enabled, plus env override semantics.
    // ------------------------------------------------------------------

    /// The regression: pre-fix, `enable_admin_endpoints = true` with no
    /// token returned `Ok(())` and let an unauthenticated mutation surface
    /// up. This must now fail with `AdminTokenRequired`.
    #[test]
    fn startup_refuses_when_admin_endpoints_enabled_without_token() {
        let cfg = ServerConfig {
            enable_admin_endpoints: true,
            admin_token: None,
            ..ServerConfig::default()
        };
        let err = cfg
            .validate_safe_defaults()
            .expect_err("admin endpoints with no token must be rejected");
        match err {
            ConfigError::AdminTokenRequired => {}
            other => panic!("expected AdminTokenRequired, got {other:?}"),
        }
    }

    /// A degenerate empty token (`admin_token = ""` or `TERASLAB_ADMIN_TOKEN=`
    /// after the override) is treated identically to no token. This guards
    /// against a typo / misconfig sneaking past the gate.
    #[test]
    fn startup_refuses_when_admin_endpoints_enabled_with_empty_token() {
        let cfg = ServerConfig {
            enable_admin_endpoints: true,
            admin_token: Some(Secret::new(String::new())),
            ..ServerConfig::default()
        };
        let err = cfg.validate_safe_defaults().unwrap_err();
        match err {
            ConfigError::AdminTokenRequired => {}
            other => panic!("expected AdminTokenRequired, got {other:?}"),
        }
    }

    /// The happy path: a non-empty token plus `enable_admin_endpoints = true`
    /// validates cleanly. The token is otherwise opaque to config validation.
    #[test]
    fn admin_endpoints_with_token_validates() {
        let cfg = ServerConfig {
            enable_admin_endpoints: true,
            admin_token: Some(Secret::new("operator-issued-secret-1234")),
            ..ServerConfig::default()
        };
        cfg.validate_safe_defaults()
            .expect("admin endpoints with token must validate");
    }

    /// When admin endpoints are off the token requirement is not enforced —
    /// a deployment that opts out of the mutating surface entirely should
    /// not need to provision a vestigial secret.
    #[test]
    fn missing_admin_token_is_fine_when_admin_endpoints_disabled() {
        let cfg = ServerConfig {
            enable_admin_endpoints: false,
            admin_token: None,
            ..ServerConfig::default()
        };
        cfg.validate_safe_defaults()
            .expect("no token is fine when admin surface is off");
    }

    /// Guards env var so two parallel admin-token tests don't collide.
    fn admin_token_env_guard() -> std::sync::MutexGuard<'static, ()> {
        use std::sync::Mutex;
        use std::sync::OnceLock;
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let m = LOCK.get_or_init(|| Mutex::new(()));
        m.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    #[test]
    fn admin_token_env_override_replaces_toml_value() {
        let _guard = admin_token_env_guard();
        // SAFETY: env access is single-threaded under `_guard`.
        unsafe {
            std::env::remove_var(ServerConfig::ENV_ADMIN_TOKEN);
        }

        let mut cfg = ServerConfig {
            admin_token: Some(Secret::new("from-toml")),
            ..ServerConfig::default()
        };
        unsafe {
            std::env::set_var(ServerConfig::ENV_ADMIN_TOKEN, "from-env");
        }
        cfg.apply_admin_token_env_override();
        assert_eq!(
            cfg.admin_token.as_ref().map(|s| s.as_str()),
            Some("from-env"),
        );

        unsafe {
            std::env::remove_var(ServerConfig::ENV_ADMIN_TOKEN);
        }
    }

    #[test]
    fn empty_admin_token_env_clears_toml_value() {
        let _guard = admin_token_env_guard();
        unsafe {
            std::env::remove_var(ServerConfig::ENV_ADMIN_TOKEN);
        }

        let mut cfg = ServerConfig {
            admin_token: Some(Secret::new("from-toml")),
            ..ServerConfig::default()
        };
        unsafe {
            std::env::set_var(ServerConfig::ENV_ADMIN_TOKEN, "");
        }
        cfg.apply_admin_token_env_override();
        assert!(
            cfg.admin_token.is_none(),
            "explicit empty env must clear the TOML value (matches the OTLP \
             endpoint override semantics)",
        );

        unsafe {
            std::env::remove_var(ServerConfig::ENV_ADMIN_TOKEN);
        }
    }

    #[test]
    fn absent_admin_token_env_preserves_toml_value() {
        let _guard = admin_token_env_guard();
        unsafe {
            std::env::remove_var(ServerConfig::ENV_ADMIN_TOKEN);
        }

        let mut cfg = ServerConfig {
            admin_token: Some(Secret::new("from-toml")),
            ..ServerConfig::default()
        };
        cfg.apply_admin_token_env_override();
        assert_eq!(
            cfg.admin_token.as_ref().map(|s| s.as_str()),
            Some("from-toml"),
            "missing env var must leave the TOML value untouched",
        );
    }

    /// `apply_env_overrides` plumbs through to the admin-token override so
    /// callers do not have to remember which knobs are pre-validated.
    #[test]
    fn apply_env_overrides_pulls_in_admin_token() {
        let _guard = admin_token_env_guard();
        let _obs_guard = obs_env_guard();
        clear_migration_env();
        unsafe {
            std::env::remove_var(ServerConfig::ENV_ADMIN_TOKEN);
            std::env::remove_var(ObservabilityConfig::ENV_OTLP_ENDPOINT);
            std::env::remove_var(ObservabilityConfig::ENV_SAMPLING_RATIO);
            std::env::remove_var(ObservabilityConfig::ENV_SERVICE_NAME);
        }

        let mut cfg = ServerConfig::default();
        unsafe {
            std::env::set_var(ServerConfig::ENV_ADMIN_TOKEN, "set-via-env");
        }
        cfg.apply_env_overrides()
            .expect("admin-token env override must apply cleanly");
        assert_eq!(
            cfg.admin_token.as_ref().map(|s| s.as_str()),
            Some("set-via-env"),
        );

        unsafe {
            std::env::remove_var(ServerConfig::ENV_ADMIN_TOKEN);
        }
    }

    #[test]
    fn index_shards_non_power_of_two_rejected() {
        let cfg = ServerConfig {
            index: crate::config::IndexConfig {
                index_shards: 100,
                ..Default::default()
            },
            ..ServerConfig::default()
        };
        let err = cfg.validate_sizes().unwrap_err();
        assert!(err.to_string().contains("index.index_shards"));
    }

    #[test]
    fn index_shards_zero_rejected() {
        let cfg = ServerConfig {
            index: crate::config::IndexConfig {
                index_shards: 0,
                ..Default::default()
            },
            ..ServerConfig::default()
        };
        let err = cfg.validate_sizes().unwrap_err();
        assert!(err.to_string().contains("index.index_shards"));
    }

    #[test]
    fn index_shards_too_large_rejected() {
        let cfg = ServerConfig {
            index: crate::config::IndexConfig {
                index_shards: 512,
                ..Default::default()
            },
            ..ServerConfig::default()
        };
        let err = cfg.validate_sizes().unwrap_err();
        assert!(err.to_string().contains("index.index_shards"));
    }

    #[test]
    fn index_shards_valid_values_pass() {
        for &n in &[1usize, 2, 4, 16, 128, 256] {
            let cfg = ServerConfig {
                index: crate::config::IndexConfig {
                    index_shards: n,
                    ..Default::default()
                },
                ..ServerConfig::default()
            };
            cfg.validate_sizes()
                .unwrap_or_else(|e| panic!("index_shards={n} should be valid: {e}"));
        }
    }
}
