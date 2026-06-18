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

    /// Size of the redo log region in bytes.
    pub redo_log_size: u64,

    /// Path for the redo log file. If not set, derived from the first device
    /// path by appending `.redo`.
    pub redo_log_path: Option<PathBuf>,

    /// Size of the on-device deletion-tombstone log region in bytes.
    ///
    /// The tombstone log ([`crate::tombstone::TombstoneLog`]) is append-only
    /// and — unlike the redo log — is NOT reset on checkpoint; it is bounded
    /// only by GC compaction below the safe-rejoin horizon (a later phase).
    /// The region must hold roughly `deletion_rate × horizon_window × 56 B`
    /// worth of tombstones; the default matches the redo region until the GC
    /// horizon tuning (deletion-tombstone design §3.3/§4.5) lands.
    pub tombstone_region_size: u64,

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
            redo_log_size: 64 * 1024 * 1024, // 64 MiB
            redo_log_path: None,
            tombstone_region_size: 64 * 1024 * 1024, // 64 MiB
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
        nonzero_u64("device_size", self.device_size)?;
        nonzero_u64("redo_log_size", self.redo_log_size)?;
        nonzero_u64("tombstone_region_size", self.tombstone_region_size)?;
        nonzero_usize("expected_records", self.expected_records)?;
        nonzero_u32("max_batch_size", self.max_batch_size)?;
        nonzero_usize("max_connections", self.max_connections)?;

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
        let toml_str = r#"
node_id = 1
replication_factor = 3
cluster_secret = "0123456789abcdef"
"#;
        let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
        cfg.validate_safe_defaults()
            .expect("RF>1 with non-empty cluster_secret must pass");
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
}
