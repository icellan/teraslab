//! TeraSlab server binary.
//!
//! Startup sequence:
//! 1. Load configuration
//! 2. Open/create device files
//! 3. Create or recover allocator
//! 4. Load index from snapshot or rebuild from device scan
//! 5. Open redo log and replay entries
//! 6. Create Engine
//! 7. Start TCP server
//! 8. On shutdown: snapshot index, sync device

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8};

use parking_lot::Mutex;

use teraslab::allocator::AllocatorError;
use teraslab::config::IndexBackendMode;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, DirectDevice};
use teraslab::index::{DahBackend, ShardedIndex};
use teraslab::locks::StripedLocks;
use teraslab::metrics::{
    AllocatorMetrics, ClusterAuthMetrics, MigrationMetrics, RedoMetrics, ReplicationMetrics,
    SwimMetrics, ThreadHistograms, ThreadMetrics,
};
use teraslab::ops::engine::Engine;
use teraslab::redo::RedoLog;
use teraslab::server::Server;
use teraslab::server::dispatch::{SecondaryStatus, set_secondary_status};
use teraslab::server::http::{HttpState, start_http_server};
use teraslab::server::startup::{
    AllocatorOrigin, SecondaryLoadOutcome, apply_packed_mode, check_replay_tolerance_with_cap,
    fallback_dah_index, load_primary_index_file_backed, load_primary_index_redb,
    load_sharded_index_in_memory, load_sharded_index_in_memory_multi, open_mandatory_redo_log,
    rebuild_in_memory_secondaries, recover_or_create_boxed_allocator, secondaries_from_pair,
};
use teraslab::storage::blobstore::{BlobStore, FileBlobStore};

/// Classify a flattened (`String`) error from the replication
/// intent-recovery path as transient redo backpressure.
///
/// `recover_pending_replication_intents` returns `Result<(), String>`, so
/// the typed [`teraslab::redo::RedoError::LogFull`] variant has already
/// been flattened by the time the startup barrier sees it. A full redo log
/// on rejoin is transient self-healing backpressure — the inbound
/// migration applies that fill it are idempotently re-drivable from their
/// source under the persisted inbound fence, and the checkpointer/catch-up
/// will free space. It must therefore route to the retry path, never to
/// the terminal `process::exit(1)` arm (which would leave the cluster
/// permanently stuck at 0/N ready, scenario_09). Genuine device/IO faults
/// (`Poisoned`, I/O errors) do NOT match here and stay terminal.
///
/// The match is against [`teraslab::redo::LOG_FULL_MESSAGE_PREFIX`] so the
/// `Display` format and this discriminator cannot drift apart.
fn is_redo_pressure(err: &str) -> bool {
    err.contains(teraslab::redo::LOG_FULL_MESSAGE_PREFIX)
}

/// Whether the RF=1 buffered-durability "up to one flush interval of acked
/// writes may be lost on an unclean shutdown" loss window applies to this
/// node's boot warning.
///
/// Only true for single-node (`replication_factor <= 1`) deployments. Under
/// `replication_factor > 1`, `ensure_local_write_durable`
/// (`src/server/dispatch.rs`, C-1 / G3) forces the master's redo tail and
/// data devices durable before every ack, concurrently with the replica
/// round-trip — an acked write is already locally fsync-durable there, so
/// the flush-interval loss window this warning describes does not exist.
/// See `rf_gt_1_mutation_is_locally_fsync_durable_before_ack`
/// (`src/server/dispatch.rs`).
fn buffered_loss_window_applies(replication_factor: u8) -> bool {
    replication_factor <= 1
}

/// Walk local interfaces via `getifaddrs(3)` and return the first
/// non-loopback IPv4 address. Used as a best-effort fallback when
/// `listen_addr = 0.0.0.0` and the operator did not configure
/// `advertise_addr`; if no usable interface is found (or the call fails) the
/// caller refuses to start rather than guessing.
///
/// Pre-fix this function connected a UDP socket to `8.8.8.8:53` to discover
/// the default-route interface. Two problems:
///
/// 1. The kernel route lookup touches Google's public IP, which trips egress
///    monitoring / DLP in audited / air-gapped environments — surprising in
///    a self-hosted UTXO database.
/// 2. In clusters where `8.8.8.8` is unroutable, the function returned
///    `None`, the binary then fell back to `bind_addr.ip()` (= `0.0.0.0`),
///    and `0.0.0.0` was advertised to other nodes — silently breaking SWIM
///    convergence in a non-obvious way.
///
/// The new behaviour iterates the interface list directly
/// (`libc::getifaddrs`) and returns only IPv4 addresses that are not
/// loopback. The caller logs and exits when this returns `None`, so the
/// operator sees a clear "set advertise_addr" message at startup instead of
/// silent misconfiguration.
///
/// See F-G10-008 in the audit.
fn detect_local_ip() -> Option<std::net::IpAddr> {
    use std::net::{IpAddr, Ipv4Addr};
    // SAFETY: libc::getifaddrs returns a heap-allocated linked list that we
    // own; we walk the list reading scalar fields out of repr-C structs and
    // free it via libc::freeifaddrs when done. No raw pointers escape this
    // function. The `unsafe` is isolated to this helper.
    unsafe {
        let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut ifap) != 0 || ifap.is_null() {
            return None;
        }
        let mut chosen: Option<IpAddr> = None;
        let mut cursor = ifap;
        while !cursor.is_null() {
            let entry = &*cursor;
            if !entry.ifa_addr.is_null() {
                let family = (*entry.ifa_addr).sa_family as i32;
                if family == libc::AF_INET {
                    let sin = &*(entry.ifa_addr as *const libc::sockaddr_in);
                    // `s_addr` is in network byte order on every supported
                    // platform; from_be lifts that into the host order
                    // `Ipv4Addr::from(u32)` expects.
                    let addr = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
                    if !addr.is_loopback() && !addr.is_unspecified() {
                        chosen = Some(IpAddr::V4(addr));
                        break;
                    }
                }
            }
            cursor = entry.ifa_next;
        }
        libc::freeifaddrs(ifap);
        chosen
    }
}

/// Global metrics counters for the server binary.
static SERVER_METRICS: ThreadMetrics = ThreadMetrics::new();

/// Global latency histograms for the server binary.
static SERVER_HISTOGRAMS: ThreadHistograms = ThreadHistograms::new();

/// Replication subsystem metrics (Phase 5).
static REPLICATION_METRICS: ReplicationMetrics = ReplicationMetrics::new();

/// Redo log metrics (Phase 5).
static REDO_METRICS: RedoMetrics = RedoMetrics::new();

/// Shard migration metrics (Phase 5).
static MIGRATION_METRICS: MigrationMetrics = MigrationMetrics::new();

/// SWIM failure-detector metrics (Phase 5).
static SWIM_METRICS: SwimMetrics = SwimMetrics::new();

/// Device-space allocator metrics (Phase 5).
static ALLOCATOR_METRICS: AllocatorMetrics = AllocatorMetrics::new();

/// Shared handles needed to run a replication catch-up pass against a single
/// replica. Captured once at startup and reused by both the one-shot startup
/// pass and the runtime lag-monitor converge loop (D-7/D-8), so the two paths
/// can never diverge in how they build, authenticate, and send catch-up
/// batches.
#[derive(Clone)]
struct CatchupContext {
    redo_log: Option<Arc<Mutex<RedoLog>>>,
    engine: Arc<Engine>,
    /// Live cluster topology epoch (shared atomic; re-read per chunk).
    cluster_key_handle: Arc<std::sync::atomic::AtomicU64>,
    /// P1 §4.2/I12: read handle onto the committed regime state so every
    /// catch-up chunk is stamped (V3) while regime enforcement is active.
    /// Captured per chunk at that chunk's fan-out entry, alongside the
    /// `cluster_key_handle` read.
    topology_authority: Arc<teraslab::cluster::topology::TopologyAuthority>,
    /// Channel handle for posting a full-shard resync when the redo log has
    /// wrapped past the replica's last-acked position.
    resync_handle: teraslab::cluster::coordinator::ResyncSenderHandle,
    /// Cluster HMAC secret (None in unsecured clusters).
    auth_secret: Option<Vec<u8>>,
    source_node_id: u64,
    /// Per-chunk ACK timeout for catch-up sends. Threaded from the configured
    /// `replication_timeout_ms` so the catch-up path matches the foreground
    /// fan-out (`cluster.replication_timeout()`) instead of using a hardcoded
    /// value (REL-112).
    replication_timeout: std::time::Duration,
    /// Outbound-bytes admission control SHARED with shard migrations (the
    /// coordinator's throttle), so concurrent catch-up streams and
    /// migrations respect one combined byte cap instead of stacking.
    migration_throttle: Arc<teraslab::cluster::migration::MigrationThrottle>,
    /// "At most one convergence loop per replica" registry shared by the
    /// startup pass and the lag monitor, so ticks never stack loops on a
    /// slow replica.
    catchup_in_flight: Arc<teraslab::replication::durable::CatchupInFlight>,
    /// Process shutdown flag — convergence loops abort promptly on shutdown.
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

/// Ops per catch-up pass — the CHUNK size of the convergence loop. This is
/// the historical `catchup_max_ops_per_pass` value (10_000): it used to be
/// the hard cap per 30 s lag-monitor tick; under the convergence loop it is
/// the unit streamed-and-ACKed per pass while the loop runs passes
/// back-to-back until the replica converges.
const CATCHUP_CHUNK_OPS: usize = 10_000;

/// Nominal per-op byte estimate used to size a chunk's admission request
/// against the shared migration throttle (10_000 ops ≈ 2.5 MiB against the
/// default 32 MiB cap). This is admission-control pacing, not exact
/// accounting: ops range from 32-byte deletes to multi-KiB creates, and the
/// synchronous per-chunk ACK already bounds how much data is in flight.
const CATCHUP_EST_BYTES_PER_OP: u64 = 256;

/// Run one bounded catch-up pass for a single replica and persist the new ACK
/// position on success.
///
/// Streams redo entries in `[from_seq, current_seq)` (capped at
/// `max_ops_per_pass`) to `addr` through the same authenticated, dense-stream
/// fan-out used by steady-state replication, then records the achieved
/// `through_sequence` into the process ACK tracker. On `RedoReclaimed` (the
/// linear redo log reclaimed its prefix past `from_seq`) it posts a full-shard
/// resync request via the coordinator instead, retrying the signal with bounded
/// backoff so a transiently-unknown address does not silently drop the resync.
/// Other transport errors are logged and left for the next pass to retry — the
/// function is idempotent because the replica applies ops idempotently and the
/// ACK tracker only advances on success.
///
/// Returns the pass outcome for the convergence loop
/// ([`teraslab::replication::durable::run_catchup_to_convergence`]):
/// `Advanced` when the replica's ACK moved, `NeedsResync` when the redo
/// prefix was reclaimed (the resync request has been posted), `NoAdvance`
/// on any other failure.
fn run_one_catchup_pass(
    ctx: &CatchupContext,
    tracker: &teraslab::replication::durable::AckTracker,
    addr: std::net::SocketAddr,
    from_seq: u64,
    current_seq: u64,
    max_ops_per_pass: usize,
) -> teraslab::replication::durable::CatchupPassOutcome {
    use std::sync::atomic::Ordering;
    use teraslab::replication::durable::CatchupPassOutcome;

    if from_seq >= current_seq {
        return CatchupPassOutcome::NoAdvance; // already caught up
    }

    let eng_ref = ctx.engine.clone();
    // Per-store redo splits the one logical stream across N store logs; the
    // earliest recoverable sequence is the min across them (== global fence + 1),
    // and catch-up must read the merged, sequence-ordered view — not a single log.
    let first_avail_seq = eng_ref.earliest_redo_sequence_merged().ok().flatten();
    let cluster_key_handle = ctx.cluster_key_handle.clone();
    let regime_authority = ctx.topology_authority.clone();
    let auth_secret = ctx.auth_secret.clone();

    let result = teraslab::replication::durable::run_catchup_for_replica(
        &addr,
        from_seq,
        current_seq,
        1000,
        max_ops_per_pass,
        &|seq| {
            let mut entries = match eng_ref.read_redo_from_sequence_merged(seq) {
                Ok(e) => e,
                Err(_) => return Vec::new(),
            };
            // Cap the CONVERSION work at one pass's worth of entries. The
            // pass budget (`max_ops_per_pass`, applied at entry granularity
            // by `run_catchup_for_replica`) can never fully include more
            // than budget + 1 entries, but the converter below does real
            // per-entry work (engine record reads for creates). Without
            // this cap a convergence loop over a deep backlog would convert
            // the ENTIRE remaining tail on every pass and then discard all
            // but the first chunk — O(backlog) engine reads per pass.
            // Trade-off: runs of >1 zero-op entries beyond the cap no
            // longer ride along "for free" in one pass; they are swept by
            // the following passes instead (correctness unchanged — the
            // watermark advances pass by pass).
            entries.truncate(max_ops_per_pass.saturating_add(1));
            // CRITICAL FIX: `RedoOp::tx_key()` only represents a
            // `SetMinedBatch` of exactly one txid (`None` for 0 or 2+), so a
            // genuine multi-txid batch must be expanded across every shard its
            // txids belong to — there is no single "the" shard to derive via
            // `tx_key()` the way single-key ops have. Without this, a live
            // setMined RPC touching 2+ txids was silently dropped from replica
            // catch-up with no error or retry (permanent divergence).
            //
            // Each entry keeps its own sequence paired with its full `ReplicaOp`
            // expansion (rather than flattening into one list) so
            // `run_catchup_for_replica` can apply the per-pass op budget at
            // entry granularity — never splitting one entry's (possibly
            // multi-op) expansion across two passes, and never over-reporting
            // the watermark past ops that were not actually sent.
            //
            // The converter is fail-closed: a create whose current record state
            // cannot be read back (unreadable slot/mined-state, or a
            // missing/unreadable EXTERNAL blob) returns `Err` rather than
            // shipping a partial/corrupt record. On any such error we return an
            // empty batch (same as the redo-read error above), which forces a
            // full-shard resync instead of silently advancing the catch-up
            // watermark past the unshippable entry.
            let built: Result<
                Vec<(u64, Vec<teraslab::replication::protocol::ReplicaOp>)>,
                teraslab::cluster::coordinator::ReplicaConvertError,
            > = entries
                .iter()
                .map(|e| {
                    let ops: Vec<teraslab::replication::protocol::ReplicaOp> =
                        if let teraslab::redo::RedoOp::SetMinedBatch { txids, .. } = &e.op {
                            let mut ops = Vec::new();
                            let mut seen_shards = std::collections::HashSet::new();
                            for tx_key in txids {
                                let shard =
                                    teraslab::cluster::shards::ShardTable::shard_for_key(tx_key);
                                if seen_shards.insert(shard) {
                                    ops.extend(
                                        teraslab::cluster::coordinator::redo_entry_to_replica_ops(
                                            e, shard, &eng_ref,
                                        )?,
                                    );
                                }
                            }
                            ops
                        } else if let Some(tx_key) = e.op.tx_key() {
                            let shard =
                                teraslab::cluster::shards::ShardTable::shard_for_key(tx_key);
                            teraslab::cluster::coordinator::redo_entry_to_replica_op(
                                e, shard, &eng_ref,
                            )?
                            .into_iter()
                            .collect()
                        } else {
                            Vec::new()
                        };
                    Ok((e.sequence, ops))
                })
                .collect();
            match built {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(
                        %addr,
                        err = %err,
                        "catchup: converter failed to read record state — forcing resync",
                    );
                    Vec::new()
                }
            }
        },
        first_avail_seq,
        &move |chunk| {
            // P1 §4.2/I12: capture the regime stamps at THIS chunk's
            // fan-out entry, alongside the cluster_key read; the same
            // table then rides every resend inside the send loop.
            // `None` (emit V2) while enforcement is not active.
            let regime_table: Option<Vec<(u16, u64)>> =
                if regime_authority.regime_enforcement_active() {
                    Some(
                        teraslab::replication::protocol::touched_shards(chunk.iter())
                            .into_iter()
                            .map(|s| (s, regime_authority.committed_regime(s)))
                            .collect(),
                    )
                } else {
                    None
                };
            teraslab::server::dispatch::send_replica_ops_to(
                addr,
                chunk,
                ctx.replication_timeout,
                auth_secret.as_deref(),
                cluster_key_handle.load(Ordering::Acquire),
                regime_table.as_deref(),
                ctx.source_node_id,
                0,
            )
        },
    );

    match result {
        Ok(through) => {
            if through >= from_seq {
                tracing::info!(%addr, through, "catchup: replica advanced");
            }
            tracker.record_ack(addr, through);
            tracker.flush();
            CatchupPassOutcome::Advanced { through }
        }
        Err(e) => {
            tracing::warn!(%addr, err = %e, "catchup: replica catch-up failed");
            if let teraslab::replication::durable::CatchupError::RedoReclaimed { .. } = e {
                // The redo prefix the replica still needs has been reclaimed,
                // so the only safe repair is a full-shard resync. A dropped
                // resync request must not be silently lost (REL-113): the
                // common cause of `signal_for_addr` returning false is that the
                // node-address map has not yet learned `addr` (transient on
                // join), so retry with bounded backoff. If it still fails, the
                // receiver is gone (shutdown) or the address is genuinely
                // unknown; escalate to error-level so the gap is observable.
                // Either way the ACK tracker has NOT advanced, so the next
                // lag-monitor tick re-detects the lag and re-runs this pass.
                const RESYNC_SIGNAL_ATTEMPTS: u32 = 3;
                let mut queued = false;
                for attempt in 0..RESYNC_SIGNAL_ATTEMPTS {
                    if ctx.resync_handle.signal_for_addr(&addr, Vec::new()) {
                        queued = true;
                        break;
                    }
                    if attempt + 1 < RESYNC_SIGNAL_ATTEMPTS {
                        std::thread::sleep(std::time::Duration::from_millis(
                            50 * (attempt as u64 + 1),
                        ));
                    }
                }
                if queued {
                    tracing::info!(%addr, "catchup: posted full-shard resync request");
                } else {
                    tracing::error!(
                        %addr,
                        attempts = RESYNC_SIGNAL_ATTEMPTS,
                        "catchup: resync request could not be queued (unknown addr or \
                         coordinator stopped); replica remains behind and will be \
                         retried on the next lag-monitor tick",
                    );
                }
                // Terminal for the convergence loop either way: streaming
                // deltas cannot repair a reclaimed prefix. If the queueing
                // failed, the ACK tracker has not advanced, so the next
                // lag-monitor tick re-detects the lag and retries the signal.
                CatchupPassOutcome::NeedsResync
            } else {
                CatchupPassOutcome::NoAdvance
            }
        }
    }
}

/// Drive one replica all the way to convergence (blocking).
///
/// Claims the per-replica in-flight slot (a second caller for the same
/// address is a no-op — the running loop already covers it), then runs the
/// shared stream-until-converged loop: chunks of [`CATCHUP_CHUNK_OPS`] ops,
/// each pass paced by the coordinator's migration throttle and acknowledged
/// synchronously by the replica, re-reading the live redo head and the
/// persisted ACK watermark between passes. Exits on convergence, on a
/// sub-chunk tail (handed to steady-state replication), on two consecutive
/// non-advancing passes, on redo reclaim (resync posted), or on shutdown —
/// see `run_catchup_to_convergence` for the loop contract.
fn run_replica_convergence(
    ctx: &CatchupContext,
    tracker: &teraslab::replication::durable::AckTracker,
    addr: std::net::SocketAddr,
) {
    use teraslab::replication::durable::{
        CatchupConvergence, ConvergenceControls, run_catchup_to_convergence,
    };

    let Some(_guard) = ctx.catchup_in_flight.try_begin(addr) else {
        tracing::debug!(
            %addr,
            "catchup: convergence loop already in flight for this replica; skipping",
        );
        return;
    };

    let controls = ConvergenceControls {
        chunk_ops: CATCHUP_CHUNK_OPS,
        ..ConvergenceControls::default()
    };
    let chunk_est_bytes = (CATCHUP_CHUNK_OPS as u64).saturating_mul(CATCHUP_EST_BYTES_PER_OP);
    let redo = ctx.redo_log.clone();

    let outcome = run_catchup_to_convergence(
        &addr,
        &controls,
        &move || {
            redo.as_ref()
                .map(|rl| rl.lock().current_sequence())
                .unwrap_or(0)
        },
        &|| tracker.last_acked(&addr),
        &|| ctx.shutdown.load(std::sync::atomic::Ordering::Relaxed),
        &|| ctx.migration_throttle.try_admit(chunk_est_bytes),
        &|from_seq, current_seq| {
            run_one_catchup_pass(ctx, tracker, addr, from_seq, current_seq, CATCHUP_CHUNK_OPS)
        },
    );

    match outcome {
        CatchupConvergence::Converged { last_acked } => {
            tracing::info!(%addr, last_acked, "catchup: replica converged");
        }
        CatchupConvergence::HandedToSteadyState {
            last_acked,
            remaining,
        } => {
            tracing::info!(
                %addr,
                last_acked,
                remaining,
                "catchup: sub-chunk tail handed back to steady-state replication",
            );
        }
        CatchupConvergence::NoProgress { last_acked } => {
            tracing::warn!(
                %addr,
                last_acked,
                "catchup: convergence made no progress; will retry on the next \
                 lag-monitor tick",
            );
        }
        CatchupConvergence::NeedsResync { last_acked } => {
            tracing::info!(
                %addr,
                last_acked,
                "catchup: redo reclaimed past replica position — resync path engaged",
            );
        }
        CatchupConvergence::Aborted { last_acked } => {
            tracing::info!(%addr, last_acked, "catchup: convergence aborted by shutdown");
        }
    }
}

/// Lag-monitor entry point: run the convergence loop for `addr` on a
/// detached background thread so the monitor keeps ticking (and keeps
/// serving OTHER lagging replicas) while a slow replica converges.
///
/// Stacking is prevented inside `run_replica_convergence` by the
/// per-replica in-flight slot: a tick that fires while a loop is already
/// running spawns a thread that exits immediately (at most one redundant
/// spawn per tick per replica — negligible at the 30 s default interval).
fn spawn_replica_convergence(ctx: CatchupContext, addr: std::net::SocketAddr) {
    let spawned = std::thread::Builder::new()
        .name(format!("catchup-{addr}"))
        .spawn(move || {
            // The process-wide tracker is installed before the lag monitor
            // exists; `None` here means we are tearing down.
            if let Some(tracker) = teraslab::server::dispatch::ack_tracker_handle() {
                run_replica_convergence(&ctx, tracker, addr);
            }
        });
    if let Err(e) = spawned {
        tracing::warn!(
            %addr,
            err = %e,
            "catchup: failed to spawn convergence thread; will retry on the next \
             lag-monitor tick",
        );
    }
}

/// Inter-node authentication metrics (E-4 / E-5): distinct HMAC-failure
/// vs clock-skew rejection counters.
static CLUSTER_AUTH_METRICS: ClusterAuthMetrics = ClusterAuthMetrics::new();

fn main() {
    // Parse config first so the observability section can drive the
    // subscriber (OTLP endpoint, sampling ratio, service name).
    let args: Vec<String> = std::env::args().collect();

    // F-X-001 / F-G10 strict_auth: presence of `--strict-auth` anywhere in
    // the args promotes the multi-node-without-secret WARN to a hard
    // refuse. This is the hard-mode toggle for the trusted-overlay
    // deployment model documented in `docs/DEPLOYMENT_ASSUMPTIONS.md`.
    let strict_auth_cli = args.iter().any(|a| a == "--strict-auth");

    // P1.1: `--cluster-id <hex>` overrides TOML / unset. Parsed later
    // alongside the TOML value via `ServerConfig::resolved_cluster_id()`.
    let cluster_id_cli: Option<String> = args
        .windows(2)
        .find(|w| w[0] == "--cluster-id")
        .map(|w| w[1].clone());

    // P1 stage 1 (§4.1 "Persistence") — CLI-ONLY (deliberately never a
    // TOML field: a config file would make the tolerant legacy decoder a
    // standing setting; the flag is a one-shot migration verb),
    // SELF-CONSUMING upgrade switch for a legacy topology state file. The
    // first legacy decode rewrites the file in the new checksummed
    // envelope, and passing the flag when the file is already new-format
    // is a hard startup error.
    let allow_legacy_topology_state = args.iter().any(|a| a == "--allow-legacy-topology-state");

    let mut config = if args.len() > 1 && args[1] == "--config" {
        if args.len() < 3 {
            // CLI usage message goes to stderr before the subscriber is
            // effectively useful — keep it as a direct stderr write so
            // operators always see it on bad invocation.
            #[allow(clippy::disallowed_macros)]
            {
                eprintln!(
                    "Usage: teraslab-server --config <path.toml> [--strict-auth] \
                     [--allow-legacy-topology-state]"
                );
            }
            std::process::exit(1);
        }
        match ServerConfig::load(std::path::Path::new(&args[2])) {
            Ok(c) => c,
            Err(e) => {
                init_tracing_subscriber_fallback();
                tracing::error!(err = %e, "failed to load config");
                std::process::exit(1);
            }
        }
    } else {
        // Subscriber is not yet installed — defer the "using defaults" log
        // line until we have a real subscriber a few lines below.
        ServerConfig::default()
    };
    // CLI flag wins over TOML (matches the rest of the env/CLI override
    // chain). TOML `strict_auth = true` still applies when `--strict-auth`
    // is absent.
    if strict_auth_cli {
        config.strict_auth = true;
    }
    if let Some(s) = cluster_id_cli {
        config.cluster_id = Some(s);
    }
    let used_defaults = !(args.len() > 1 && args[1] == "--config");

    // Apply TERASLAB_* env overrides on top of TOML values. If env vars
    // contain malformed values (e.g. a non-numeric tuning value) fail fast
    // with a plain-stderr message so operators see the root cause even before
    // the subscriber is installed.
    if let Err(e) = config.apply_env_overrides() {
        init_tracing_subscriber_fallback();
        tracing::error!(err = %e, "FATAL: invalid TERASLAB_* env override");
        std::process::exit(1);
    }
    if let Err(e) = config.validate_observability() {
        init_tracing_subscriber_fallback();
        tracing::error!(err = %e, "FATAL: invalid [observability] config");
        std::process::exit(1);
    }
    // Gate gap #1 safe defaults (localhost binds, RF>1 needs cluster_secret)
    // here too — these are config-only errors that should refuse startup
    // before any device I/O.
    if let Err(e) = config.validate_safe_defaults() {
        init_tracing_subscriber_fallback();
        tracing::error!(err = %e, "FATAL: unsafe bind/auth defaults (gap #1)");
        std::process::exit(1);
    }

    // Install the subscriber now that observability config is validated.
    let otlp_provider = match teraslab::observability::init_subscriber(
        &config.observability,
        config.node_id,
        // Shard count is fixed at compile time (cluster::shards::NUM_SHARDS).
        teraslab::cluster::shards::NUM_SHARDS as u32,
    ) {
        Ok(p) => p,
        Err(e) => {
            init_tracing_subscriber_fallback();
            tracing::error!(err = %e, "FATAL: observability init failed");
            std::process::exit(1);
        }
    };

    if used_defaults {
        tracing::warn!("no config file specified, using defaults");
    }
    if otlp_provider.is_some() {
        tracing::info!(
            endpoint = %config.observability.otlp_endpoint.as_deref().unwrap_or(""),
            sampling_ratio = config.observability.trace_sampling_ratio,
            "OTLP tracing enabled",
        );
    }

    teraslab::server::dispatch::init_dispatch_metrics(&SERVER_METRICS);
    teraslab::server::dispatch::init_dispatch_histograms(&SERVER_HISTOGRAMS);

    // Phase 5: wire up subsystem metrics. Each `init_*_metrics` uses a
    // process-wide `OnceLock`, so subsequent calls (from tests) are no-ops.
    teraslab::metrics::init_replication_metrics(&REPLICATION_METRICS);
    teraslab::metrics::init_redo_metrics(&REDO_METRICS);
    teraslab::metrics::init_migration_metrics(&MIGRATION_METRICS);
    teraslab::metrics::init_swim_metrics(&SWIM_METRICS);
    teraslab::metrics::init_allocator_metrics(&ALLOCATOR_METRICS);
    teraslab::metrics::init_cluster_auth_metrics(&CLUSTER_AUTH_METRICS);

    tracing::info!(
        service = "teraslab",
        version = env!("CARGO_PKG_VERSION"),
        node_id = config.node_id,
        target_throughput = "10M+ ops/sec",
        listen = %config.listen_addr,
        devices = ?config.device_paths,
        device_size_mib = config.device_size / (1024 * 1024),
        "TeraSlab server starting",
    );

    // F-X-002: with `strict_auth = true` as the production default,
    // `validate_safe_defaults` already refused this combination above —
    // the only way to reach this branch is the explicit opt-out
    // `strict_auth = false` in TOML (trusted-overlay legacy mode). Emit a
    // prominent boot-time warning under the `teraslab::security` target
    // so operators always see the missing-secret state in the audit
    // trail. See `docs/DEPLOYMENT_ASSUMPTIONS.md` for the full rationale.
    let multi_node = config.node_id > 0 || config.replication_factor > 1;
    let cluster_secret_missing = config
        .cluster_secret
        .as_ref()
        .map(|s| s.is_empty())
        .unwrap_or(true);
    if multi_node && cluster_secret_missing && !config.strict_auth {
        tracing::warn!(
            target: "teraslab::security",
            node_id = config.node_id,
            replication_factor = config.replication_factor,
            "cluster is multi-node but no cluster_secret is configured: inter-node SWIM, \
             topology, replication, and migration frames will be ACCEPTED UNAUTHENTICATED. \
             You explicitly opted out of the F-X-002 production default by setting \
             `strict_auth = false`. This is the legacy trusted-overlay mode (see \
             docs/DEPLOYMENT_ASSUMPTIONS.md); only safe on a fully audited private \
             network. Remove `strict_auth = false` from your TOML AND configure \
             `cluster_secret` to restore the production-safe default. This INCLUDES \
             topology mutations such as `PUT /admin/shrink`: without a cluster_secret, \
             an unauthenticated peer on the data port can forge a committed topology \
             (including a low committed_peak) and drive a split-brain — the quorum \
             gates are a structural, not cryptographic, defense in this mode.",
        );
    }

    // F-E2 — split-brain safety. A clustered node without a persisted
    // `cluster_id` cannot reject a foreign cluster's membership merge, so two
    // independently-bootstrapped clusters sharing a `cluster_secret` can merge.
    // `validate_safe_defaults` hard-rejects this under `strict_auth`; in the
    // trusted-overlay default mode, warn prominently so operators arm the guard.
    let cluster_id_unset = config
        .resolved_cluster_id()
        .map(|id| id.is_unset())
        .unwrap_or(false);
    if config.is_clustered() && cluster_id_unset && !config.strict_auth {
        tracing::warn!(
            target: "teraslab::security",
            node_id = config.node_id,
            "clustered node has no cluster_id configured: the cross-cluster merge guard is \
             DISARMED. Two independently-bootstrapped clusters that share a cluster_secret can \
             silently merge into one (split-brain / data divergence). Set a stable 32-hex-char \
             `cluster_id` (identical on every node of THIS cluster, distinct from any other \
             cluster) to arm the guard. See docs/DEPLOYMENT_ASSUMPTIONS.md.",
        );
    }

    // 1. Take the single-instance advisory lock before touching any device, so
    // a second server (or an offline `teraslab-cli restore`) refuses to race
    // this instance's data files. Held for the whole process lifetime.
    let _instance_lock =
        match teraslab::instance_lock::InstanceLock::acquire(&config.device_paths[0]) {
            Ok(lock) => {
                tracing::info!(path = %lock.path().display(), "instance lock acquired");
                lock
            }
            Err(e) => {
                tracing::error!(err = %e, "failed to acquire single-instance lock");
                std::process::exit(1);
            }
        };

    // 1a. Open device
    let device_path = &config.device_paths[0];
    let device: Arc<dyn BlockDevice> =
        match DirectDevice::open(device_path, config.device_size, config.device_alignment) {
            Ok(d) => {
                tracing::info!(path = %device_path.display(), "device opened");
                Arc::new(d)
            }
            Err(e) => {
                tracing::error!(path = %device_path.display(), err = %e, "failed to open device");
                std::process::exit(1);
            }
        };

    // 1b. Build the per-store devices. Each configured device_path is carved
    // into `device_split` virtual stores (SubDevices). `device_split == 1` with
    // a single path keeps the raw device untouched (byte-identical single
    // store). Total store count was validated to 1..=256 by
    // `validate_safe_defaults` (the index entry's device_id is a u8).
    let num_stores = config.device_paths.len() * config.device_split;
    let store_devices: Vec<Arc<dyn BlockDevice>> = if num_stores == 1 {
        vec![device.clone()]
    } else {
        let mut devs: Vec<Arc<dyn BlockDevice>> = Vec::with_capacity(num_stores);
        for (i, path) in config.device_paths.iter().enumerate() {
            // device_paths[0] is already open as `device`; open the rest.
            let phys: Arc<dyn BlockDevice> = if i == 0 {
                device.clone()
            } else {
                match DirectDevice::open(path, config.device_size, config.device_alignment) {
                    Ok(d) => Arc::new(d),
                    Err(e) => {
                        tracing::error!(path = %path.display(), err = %e, "failed to open device");
                        std::process::exit(1);
                    }
                }
            };
            if config.device_split == 1 {
                devs.push(phys);
            } else {
                match teraslab::subdevice::split_device(phys, config.device_split) {
                    Ok(subs) => devs.extend(subs.into_iter().map(|s| s as Arc<dyn BlockDevice>)),
                    Err(e) => {
                        tracing::error!(path = %path.display(), err = %e, "failed to split device into virtual stores");
                        std::process::exit(1);
                    }
                }
            }
        }
        devs
    };

    // 1c. Optionally interpose the in-RAM data-device block cache
    // (docs/WRITE_CACHE_SPEC.md). `cache.bytes == 0` (default) leaves the raw
    // O_DIRECT devices untouched — byte-for-byte today's behavior, maximum
    // safety. When enabled, every store gets its own cache; the engine uses it
    // transparently as a `BlockDevice`, and write-back stays WAL-safe because
    // the checkpoint's data-device barrier flushes dirty blocks via `sync()`.
    // The streaming write buffer owns write coalescing for the segment engine, so
    // the underlying data cache must be WRITE-THROUGH under it — a write-back cache
    // would re-scatter the streaming flushes through its own eviction path,
    // defeating the point. Streaming is segment-only (in_place writes are in-place
    // RMW, not appends).
    let use_streaming = config.storage.engine == teraslab::config::StorageEngine::Segment
        && config.storage.streaming;
    let cache_writeback = config.cache.writeback && !use_streaming;
    let store_devices: Vec<Arc<dyn BlockDevice>> = if config.cache.is_enabled() {
        tracing::info!(
            bytes = config.cache.bytes,
            mode = if cache_writeback {
                "write-back"
            } else {
                "write-through"
            },
            forced_write_through = use_streaming && config.cache.writeback,
            stores = store_devices.len(),
            "interposing in-RAM data-device cache"
        );
        store_devices
            .into_iter()
            .map(|d| {
                Arc::new(teraslab::cache::CachingDevice::new(
                    d,
                    config.cache.bytes,
                    cache_writeback,
                    config.cache.writeback_interval_ms,
                )) as Arc<dyn BlockDevice>
            })
            .collect()
    } else {
        store_devices
    };

    // Interpose the per-store streaming write buffer for the segment engine. It
    // buffers the append tail and flushes it as large sequential writes; the
    // checkpoint barrier's `sync()` flushes it before any redo prefix is reclaimed,
    // so buffered durability is unchanged. The allocator header (offset 0) and any
    // in-place mutation of an already-flushed record pass straight through to the
    // cache/device — see `StreamingWriteDevice`.
    let store_devices: Vec<Arc<dyn BlockDevice>> = if use_streaming {
        tracing::info!(
            stores = store_devices.len(),
            flush_threshold = teraslab::streaming::DEFAULT_FLUSH_THRESHOLD,
            flush_chunk = teraslab::streaming::DEFAULT_FLUSH_CHUNK,
            "interposing per-store streaming write buffer (segment engine)"
        );
        store_devices
            .into_iter()
            .map(|d| {
                Arc::new(teraslab::streaming::StreamingWriteDevice::with_defaults(d))
                    as Arc<dyn BlockDevice>
            })
            .collect()
    } else {
        store_devices
    };

    // Store 0 backs the existing single-store boot code (snapshot load, header
    // verify); stores 1..N are wired in below for allocators/recovery/engine.
    let device = store_devices[0].clone();
    if num_stores > 1 {
        tracing::info!(
            stores = num_stores,
            paths = config.device_paths.len(),
            split = config.device_split,
            "multi-store layout: records placed round-robin across stores",
        );
    }

    // Validate device_id config format before using it.
    if let Err(e) = config.validate_device_id() {
        tracing::error!(err = %e, "FATAL: invalid config");
        std::process::exit(1);
    }
    // Durability-framed signpost for a best-effort replication posture at
    // RF > 1. The same combination is a hard error in validate_cluster_safety
    // below; emitting this first gives the operator the "why it is unsafe"
    // context ahead of the fatal (and covers programmatic embedders that skip
    // this validation chain).
    if let Some(warning) = config.durability_warning() {
        tracing::warn!(warning = %warning, "replication durability degraded");
    }
    if let Err(e) = config.validate_cluster_safety() {
        tracing::error!(err = %e, "FATAL: unsafe cluster config");
        std::process::exit(1);
    }
    if let Err(e) = config.validate_block_height_retention() {
        tracing::error!(err = %e, "FATAL: invalid block_height_retention");
        std::process::exit(1);
    }
    if let Err(e) = config.validate_safe_defaults() {
        tracing::error!(err = %e, "FATAL: unsafe bind / cluster configuration");
        std::process::exit(1);
    }
    if config.enable_remote_bind {
        tracing::warn!(
            listen_addr = %config.listen_addr,
            http_listen_addr = %config.http_listen_addr,
            "enable_remote_bind = true: binding non-loopback addresses without mTLS — \
             ensure network-level authentication/authorization is in place \
             (see TERANODE_PRODUCTION_READINESS_GAPS.md gap #1)",
        );
    }
    if config.enable_admin_endpoints {
        tracing::warn!(
            "enable_admin_endpoints = true: /admin/* and mutating /debug/* HTTP routes are \
             registered behind bearer-token auth (Authorization: Bearer <admin_token>). \
             Network access is still required to reach the port — pair with mTLS / a \
             private interface for defence in depth (gap #1)",
        );
    }

    // 2. Recover or create allocator.
    //
    // Audit B-2: fail closed on a torn/corrupt header. Only a genuinely
    // fresh device (all-zero header region) may start with a fresh
    // allocator — a fresh allocator over a device with persisted state
    // restarts allocation at the data-region start and its next creates
    // overwrite live records.
    let (allocator, allocator_origin) = match recover_or_create_boxed_allocator(
        device.clone(),
        config.storage.engine,
        config.storage.segment_size,
    ) {
        Ok(pair) => pair,
        Err(e) => {
            if matches!(e, AllocatorError::InvalidSegmentSize { .. }) {
                tracing::error!(
                    err = %e,
                    "FATAL: [storage] segment_size is invalid for this device — \
                     refusing to start. This is a CONFIG error, not device \
                     corruption: fix [storage] segment_size and restart.",
                );
            } else {
                tracing::error!(
                    err = %e,
                    "FATAL: allocator header unusable — refusing to start with a \
                     fresh allocator over a device that may hold live records \
                     (creates would overwrite them). Inspect the device header, \
                     restore from a replica, or wipe the device explicitly before \
                     restarting",
                );
            }
            std::process::exit(1);
        }
    };
    match allocator_origin {
        AllocatorOrigin::Recovered => {
            tracing::info!("allocator recovered from device header");
            let device_id_hex = allocator.device_id_hex();
            tracing::info!(device_id = %device_id_hex, "device identity");

            if let Some(ref expected) = config.device_id {
                if expected != &device_id_hex {
                    tracing::error!(
                        expected = %expected,
                        found = %device_id_hex,
                        "FATAL: device identity mismatch — the device path points to the wrong device",
                    );
                    std::process::exit(1);
                }
                tracing::info!("device identity verified");
            }
        }
        AllocatorOrigin::Fresh => {
            let device_id_hex = allocator.device_id_hex();
            if let Some(ref expected) = config.device_id {
                // The config expects an existing device identity, but the
                // device is blank — the path points at the wrong device.
                tracing::error!(
                    expected = %expected,
                    found = %device_id_hex,
                    "FATAL: config device_id is set but the device has no \
                     persisted allocator state (blank device) — the device \
                     path points to the wrong device",
                );
                std::process::exit(1);
            }
            tracing::info!("allocator: fresh (header region all zeros — new device)");
            tracing::info!(device_id = %device_id_hex, "device identity (copy to config device_id to enable verification)");
        }
    }

    // 2b. Per-store allocators. Store 0 is the `allocator` just recovered;
    // stores 1..N recover their own header. Each is tagged with its store
    // index so its AllocateRegion/FreeRegion redo entries carry that store's
    // device_id (recovery routes region ops to the right store's allocator).
    let mut store_allocators: Vec<teraslab::allocator::BoxedAllocator> =
        Vec::with_capacity(num_stores);
    let mut allocator = allocator;
    allocator.set_redo_device_id(0);
    // Packed mode: a FRESH device adopts config.storage.packed (before any
    // allocation); a RECOVERED device keeps its on-disk format (device wins,
    // mismatch logged). Done per store so every store is consistent.
    apply_packed_mode(&mut *allocator, allocator_origin, config.storage.packed, 0);
    // Append-only placement (Phase 1 log-structured write lever): pure runtime
    // policy, not an on-disk format, so config always wins regardless of origin.
    allocator.set_append_only(config.storage.append_only);
    if config.storage.append_only {
        tracing::warn!(
            "storage.append_only = true: freed regions are never reused (records stay \
             sequential for log-structured write-back coalescing). Space is NOT reclaimed — \
             the device grows unbounded. Intended for benchmarks, not unbounded production."
        );
    }
    store_allocators.push(allocator);
    for (i, sdev) in store_devices.iter().enumerate().skip(1) {
        let (mut alloc, origin) = match recover_or_create_boxed_allocator(
            sdev.clone(),
            config.storage.engine,
            config.storage.segment_size,
        ) {
            Ok(pair) => pair,
            Err(e) => {
                if matches!(e, AllocatorError::InvalidSegmentSize { .. }) {
                    tracing::error!(
                        store = i,
                        err = %e,
                        "FATAL: [storage] segment_size is invalid for this store's \
                         device — refusing to start. This is a CONFIG error, not \
                         device corruption: fix [storage] segment_size and restart.",
                    );
                } else {
                    tracing::error!(
                        store = i,
                        err = %e,
                        "FATAL: store allocator header unusable — refusing to start (creates \
                         could overwrite live records). Inspect/restore the store device.",
                    );
                }
                std::process::exit(1);
            }
        };
        alloc.set_redo_device_id(i as u8);
        apply_packed_mode(&mut *alloc, origin, config.storage.packed, i);
        alloc.set_append_only(config.storage.append_only);
        store_allocators.push(alloc);
    }

    // 3. Load or rebuild index (backend selected by config)
    let index_backend_name = match &config.index.backend {
        IndexBackendMode::Memory => "memory",
        IndexBackendMode::Redb => "redb",
        IndexBackendMode::FileBacked => "file_backed",
    };
    tracing::info!(backend = %index_backend_name, "index backend");

    // Gap #5 (TERANODE_PRODUCTION_READINESS_GAPS.md): rebuild paths must
    // fail closed on primary index errors and surface secondary index
    // failures as degraded readiness rather than silent empty-index starts.
    // The on-disk redb / file-backed primary file is preserved untouched on
    // rebuild failure so the operator can capture diagnostics and run an
    // explicit rescan before restart.
    // The configured number of index shards (rounded up to a power of two and
    // clamped to `[1, 256]` internally by `ShardedIndex`). The in-memory backend
    // builds a `ShardedIndex` at this count directly; the redb and file_backed
    // backends are not yet sharded (deferred follow-up) and run at one shard,
    // warning once if a multi-shard count is configured.
    let index_shards = config.index.index_shards;
    // Device-scan primary rebuild (snapshot lost/corrupt). Records are round-robin
    // placed across all stores and routed by `entry.device_id`, so the rebuild
    // MUST scan every store — a single-store (store-0-only) scan silently loses
    // every record on stores 1..N. `load_sharded_index_in_memory_multi` scans all
    // stores and stamps each entry's `device_id`. N=1 keeps the byte-identical
    // single-device path.
    // Pre-size the rebuilt in-memory index to the configured steady-state
    // record count: on a fresh/empty device the scan finds 0 records, so without
    // this each shard would start tiny and rehash-under-write-guard repeatedly as
    // creates arrive — a resize storm that serializes the create path. Captured
    // into a local so the closure borrows only the scalar, not all of `config`.
    let expected_records = config.expected_records;
    let rebuild_primary_from_scan = |shard_count: usize| -> Result<ShardedIndex, _> {
        if num_stores > 1 {
            load_sharded_index_in_memory_multi(
                &store_devices,
                &store_allocators,
                shard_count,
                expected_records,
            )
        } else {
            load_sharded_index_in_memory(
                &*device,
                &*store_allocators[0],
                shard_count,
                expected_records,
            )
        }
    };
    let load_outcome: (ShardedIndex, SecondaryLoadOutcome) =
        if config.index.backend == IndexBackendMode::Redb {
            // ReDB on-disk backend
            if index_shards > 1 {
                tracing::warn!(
                    index_shards,
                    "index_shards={index_shards} not yet supported for the redb backend; \
                 using 1 shard (sharding is implemented for the in-memory backend)",
                );
            }
            let primary =
                match load_primary_index_redb(&config.index, &*device, &*store_allocators[0]) {
                    Ok(idx) => {
                        tracing::info!(entries = idx.len(), "redb primary index opened");
                        idx
                    }
                    Err(e) => {
                        tracing::error!(err = %e, "FATAL: primary index rebuild failed");
                        std::process::exit(1);
                    }
                };
            // Open the redb DAH index. Failure is degraded readiness, NOT an
            // empty start: the dispatch readiness gate rejects DAH-dependent
            // endpoints with ERR_INDEX_DEGRADED.
            let (dah, dah_ok) = match teraslab::index::redb_dah::RedbDahIndex::open(
                &config.index.redb_dah_path,
                config.index.redb_cache_size,
            ) {
                Ok(idx) => (DahBackend::OnDisk(idx), true),
                Err(e) => (fallback_dah_index("DAH", e), false),
            };
            (
                ShardedIndex::from_single(primary),
                SecondaryLoadOutcome {
                    dah,
                    status: SecondaryStatus {
                        dah_ok,
                        unmined_ok: true,
                    },
                },
            )
        } else if config.index.backend == IndexBackendMode::FileBacked {
            // File-backed mmap backend
            if index_shards > 1 {
                tracing::warn!(
                    index_shards,
                    "index_shards={index_shards} not yet supported for the file_backed backend; \
                 using 1 shard (sharding is implemented for the in-memory backend)",
                );
            }
            let fb_path = &config.index.file_backed_path;
            let primary = match load_primary_index_file_backed(
                fb_path,
                config.expected_records,
                &*device,
                &*store_allocators[0],
            ) {
                Ok(idx) => {
                    tracing::info!(entries = idx.len(), "file-backed index opened");
                    idx
                }
                Err(e) => {
                    tracing::error!(err = %e, "FATAL: primary index rebuild failed");
                    std::process::exit(1);
                }
            };
            // File-backed mode: secondary indexes stay in-memory.
            let secondaries = rebuild_in_memory_secondaries(&*device, &*store_allocators[0]);
            (ShardedIndex::from_single(primary), secondaries)
        } else {
            // In-memory backend (default). Builds a `ShardedIndex` at
            // `index_shards` directly: the snapshot restore re-shards on a
            // shard-count mismatch (and loads v1 single-table snapshots), and the
            // device-scan rebuild routes every scanned entry into its target shard.
            let snap_path = config.resolved_index_snapshot_path();
            let snap_path = snap_path.as_path();
            if snap_path.exists() {
                match ShardedIndex::restore_all(snap_path, index_shards) {
                    Ok((idx, dah, flags)) => {
                        tracing::info!(
                            entries = idx.len(),
                            shards = idx.shard_count(),
                            "index restored from snapshot",
                        );
                        let secondaries = if flags.dah_needs_rebuild {
                            tracing::warn!("DAH index needs rebuild (snapshot corrupt)");
                            rebuild_in_memory_secondaries(&*device, &*store_allocators[0])
                        } else {
                            secondaries_from_pair(dah)
                        };
                        (idx, secondaries)
                    }
                    Err(e) => {
                        tracing::warn!(err = %e, "index snapshot corrupt, rebuilding from device");
                        let index = match rebuild_primary_from_scan(index_shards) {
                            Ok(idx) => idx,
                            Err(e) => {
                                tracing::error!(err = %e, "FATAL: primary index rebuild failed");
                                std::process::exit(1);
                            }
                        };
                        let secondaries =
                            rebuild_in_memory_secondaries(&*device, &*store_allocators[0]);
                        (index, secondaries)
                    }
                }
            } else {
                tracing::info!("no index snapshot found, rebuilding from device");
                let index = match rebuild_primary_from_scan(index_shards) {
                    Ok(idx) => idx,
                    Err(e) => {
                        tracing::error!(err = %e, "FATAL: primary index rebuild failed");
                        std::process::exit(1);
                    }
                };
                let secondaries = rebuild_in_memory_secondaries(&*device, &*store_allocators[0]);
                (index, secondaries)
            }
        };
    // The in-memory backend yields a `ShardedIndex` at the configured shard
    // count directly; the redb / file_backed backends yield a single-shard
    // `ShardedIndex` (sharding for those backends is a deferred follow-up). Both
    // paths converge here so recovery and the engine share the same index.
    let (index, secondary_outcome) = load_outcome;
    let SecondaryLoadOutcome {
        dah: mut dah_index,
        status: secondary_status,
    } = secondary_outcome;
    // Install the global readiness flags BEFORE the server begins
    // accepting client requests. Dispatch then gates handlers that
    // depend on a missing secondary with ERR_INDEX_DEGRADED.
    set_secondary_status(secondary_status);
    if !secondary_status.dah_ok {
        tracing::warn!(
            "secondary readiness: DAH index unavailable — dependent endpoints \
             will reject with ERR_INDEX_DEGRADED",
        );
    }

    tracing::info!(
        entries = index.len(),
        // F-G10-012: the previous field name `load_factor` was a labelling
        // bug — the value already multiplies the unitless 0..1 ratio by
        // 100, so it's a percentage. Renamed to `load_factor_pct` so
        // dashboards / alerts read the right unit.
        load_factor_pct = index.stats().load_factor * 100.0,
        "index loaded",
    );
    tracing::info!(entries = dah_index.len(), "DAH index loaded");

    // 3b. Open redo log device (separate file) and run recovery.
    //
    // Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): the redo log is
    // mandatory. We MUST NOT fall back to an in-memory device when the
    // configured path cannot be opened — that would make every WAL-fsync
    // ack a lie (the bytes are in volatile memory and disappear at
    // shutdown). On open or create failure we fail closed so the operator
    // can fix permissions / disk / path and try again.
    // Per-store redo: one redo log per store so writes get N parallel fsync
    // streams instead of serializing on a single redo mutex. Store 0 uses the
    // configured path; store i (i >= 1) uses a `.<i>` suffix sibling. All logs
    // share ONE global sequence counter so the redo sequence — the replication
    // contract — stays globally ordered across stores.
    let redo_log_path = config.resolved_redo_log_path();
    let mut redo_log_devices: Vec<Arc<dyn BlockDevice>> = Vec::with_capacity(num_stores);
    let mut redo_logs_owned: Vec<RedoLog> = Vec::with_capacity(num_stores);
    for store_idx in 0..num_stores {
        let path = if store_idx == 0 {
            redo_log_path.clone()
        } else {
            let mut os = redo_log_path.clone().into_os_string();
            os.push(format!(".{store_idx}"));
            std::path::PathBuf::from(os)
        };
        let segment_ring = config.redo_segment_ring.then_some(config.redo_segment_size);
        match open_mandatory_redo_log(
            &path,
            config.redo_log_size,
            config.device_alignment,
            segment_ring,
            config.redo_buffered_io,
        ) {
            Ok((dev, log)) => {
                tracing::info!(
                    path = %path.display(),
                    store = store_idx,
                    size_mib = config.redo_log_size / (1024 * 1024),
                    segment_ring = log.is_segment_ring(),
                    "redo log opened (mandatory, per-store)",
                );
                redo_log_devices.push(dev);
                redo_logs_owned.push(log);
            }
            Err(e) => {
                tracing::error!(
                    path = %path.display(),
                    store = store_idx,
                    err = %e,
                    "FATAL: redo log unavailable — cannot start with mandatory WAL disabled",
                );
                std::process::exit(1);
            }
        }
    }

    // Seed and attach the shared global sequence counter from the max
    // local high-water mark across all stores' logs, so a restart never reuses
    // a sequence number that landed in any store's log before the crash.
    let shared_seq_floor =
        teraslab::redo::RedoLog::shared_sequence_floor(&redo_logs_owned.iter().collect::<Vec<_>>());
    let shared_seq = Arc::new(std::sync::atomic::AtomicU64::new(shared_seq_floor));
    for log in &mut redo_logs_owned {
        log.attach_shared_sequence(shared_seq.clone());
    }

    // Keep the device handles alive for the lifetime of the process so any
    // future redo-log replay/extension paths share the same fds.
    let _redo_log_devices: Vec<Arc<dyn BlockDevice>> = redo_log_devices;
    let mut redo_logs: Option<Vec<RedoLog>> = Some(redo_logs_owned);

    // Construct the blob store up front so recovery can reconcile orphan
    // blobs against the freshly-replayed primary index (R-049). The store is
    // a thin path handle — initialising it does not touch any blob until
    // recovery's `reconcile_blobs_after_recovery` call below.
    let blob_store: Arc<dyn BlobStore> = Arc::new(FileBlobStore::new(&config.blobstore_path, 2));
    tracing::info!(path = %config.blobstore_path.display(), "blobstore configured");

    // Run recovery if we have a redo log, while indexes are still mutable.
    // Uses `recover_all_with_allocator` so the two-phase secondary
    // durability intent records (RedoOp::SecondaryUnminedUpdate /
    // SecondaryDahUpdate) reconcile the on-disk redb secondary indexes AND
    // RedoOp::AllocateRegion / FreeRegion entries replay into the rebuilt
    // per-store allocators so freelist mutations between snapshots are not lost.
    let mut pending_conflicting_children = Vec::new();
    let mut pending_deleted_children = Vec::new();
    // Reverse-heal G3: co-located `CreateV2` keys whose bytes were lost on the
    // buffered tail (populated by `recover_all_multi_store` below). Folded into
    // the reverse-heal stale-suspect set after the Tier-1 detector runs.
    let mut g3_resync_create_keys: Vec<teraslab::index::TxKey> = Vec::new();
    // Height subsystem (deletion-tombstone design §4; BUG3): the max block
    // height the replayed redo entries prove this node has durably seen. Folded
    // into the last-durable-height floor below so a lost/corrupt `.height` file
    // cannot regress the node to height 0. Independent of `tombstones_enabled`.
    let mut recovery_height_floor: u32 = 0;
    // FU#7 Option B: carry the redo-touched key set + the clean-secondary
    // predicate OUT of the recovery block so the DAH reconcile (further below,
    // after `recover_mined_index`) can drive the O(redo) Touched-scope pass.
    let mut recovery_touched_keys: std::collections::HashSet<teraslab::index::TxKey> =
        std::collections::HashSet::new();
    // `true` iff the redb backend loaded BOTH secondaries cleanly (== the
    // `!full_secondary_rebuild` condition). A necessary (not sufficient)
    // precondition for the Touched-scope fast boot.
    let mut clean_secondaries_for_fast_boot = false;
    if let Some(ref mut redo) = redo_logs {
        // B-7: only the redb backend has crash-durable secondaries that
        // already reflect committed state; when it loaded both cleanly,
        // reconcile just the redo-touched keys (O(redo)) instead of
        // re-deriving from a full primary-index scan (O(store)) every
        // boot. Every other case (in-memory snapshot, file-backed, or a
        // secondary that needed a device-scan rebuild) is treated as an
        // unclean secondary that requires the full rebuild — matching the
        // pre-B-7 behavior exactly, so no correctness regression.
        let full_secondary_rebuild =
            !(config.index.backend == IndexBackendMode::Redb && secondary_status.dah_ok);
        clean_secondaries_for_fast_boot = !full_secondary_rebuild;
        match teraslab::recovery::recover_all_multi_store(
            &store_devices,
            &mut store_allocators,
            redo.as_mut_slice(),
            &index,
            &mut dah_index,
            full_secondary_rebuild,
            // Task 16d: DEFER the device-metadata secondary reconcile. The
            // device `block_entry_count` / `unmined_since` / `delete_at_height`
            // fields are no longer kept current by `set_mined`, so reconciling
            // from them here would resurrect a stale DAH entry for a
            // reorg-unmined (retained) record. The DAH secondary is instead
            // rebuilt store-authoritatively from the recovered MinedIndex below,
            // AFTER `engine.recover_mined_index`.
            true,
        ) {
            Ok((stats, pending, deleted, touched, resync_creates)) => {
                pending_conflicting_children = pending;
                pending_deleted_children = deleted;
                recovery_touched_keys = touched;
                // Reverse-heal G3: keys of co-located `CreateV2` entries whose
                // record bytes were lost on the buffered tail (dropped by
                // recovery). Consumed below (after the engine is up and the
                // Tier-1 detector has run) to mark their shards stale-suspect
                // under RF>1 + reverse-heal, so the lost creates are pulled from
                // a quorum-current replica instead of vanishing silently.
                g3_resync_create_keys = resync_creates;
                recovery_height_floor = stats.max_observed_block_height;
                tracing::info!(
                    replayed = stats.entries_replayed,
                    skipped = stats.entries_skipped,
                    failed = stats.entries_failed,
                    failed_missing_primary = stats.failed_missing_primary,
                    failed_io = stats.failed_io,
                    failed_corrupt = stats.failed_corrupt,
                    failed_logic = stats.failed_logic,
                    failed_missing_record_bytes = stats.failed_missing_record_bytes,
                    failed_replica_record_absent = stats.failed_replica_record_absent,
                    "recovery complete",
                );
                // Gap #5 (TERANODE_PRODUCTION_READINESS_GAPS.md): replace
                // the previous blanket `MAX_TOLERATED_FAILURES = 32` with
                // per-cause classification. `MissingPrimary` is benign
                // during idempotent replay and tolerated up to a high cap.
                // Any I/O / corrupt-entry / logic-error failure is fatal
                // regardless of count: those are storage-level corruption
                // signals that must not be papered over.
                if let Err(msg) = check_replay_tolerance_with_cap(
                    &stats,
                    config.recovery_missing_primary_tolerance,
                ) {
                    tracing::error!(
                        failed_missing_primary = stats.failed_missing_primary,
                        failed_io = stats.failed_io,
                        failed_corrupt = stats.failed_corrupt,
                        failed_logic = stats.failed_logic,
                        failed_missing_record_bytes = stats.failed_missing_record_bytes,
                        failed_replica_record_absent = stats.failed_replica_record_absent,
                        "recovery: aborting startup — {msg}",
                    );
                    std::process::exit(1);
                }
            }
            Err(e) => {
                // Top-level recovery errors (e.g. corrupt redo log, index
                // error) are fatal — we cannot proceed without a consistent
                // on-disk state. Exit immediately so the operator can
                // investigate rather than serving stale or corrupt data.
                tracing::error!(err = %e, "recovery failed — aborting startup");
                std::process::exit(1);
            }
        }

        // R-049: reconcile orphan external blobs against the freshly-replayed
        // primary index. Failed creates / aborted uploads / cancelled
        // migrations leave blobs on disk that the foreground pipeline will
        // never reference; without this sweep they accumulate forever
        // (audit IJK-08). Errors during reconciliation are non-fatal — a
        // transient blob-store issue must not block the server from coming
        // up; the periodic background sweep retries on its next tick.
        match teraslab::recovery::reconcile_blobs_after_recovery(
            blob_store.as_ref(),
            &index,
            &store_devices,
        ) {
            Ok(stats) => {
                tracing::info!(
                    total_blobs = stats.total_blobs,
                    kept = stats.kept,
                    deleted_no_index = stats.deleted_no_index,
                    deleted_not_external = stats.deleted_not_external,
                    delete_failed = stats.delete_failed,
                    "recovery: blob reconciliation summary",
                );
            }
            Err(e) => {
                tracing::warn!(err = %e, "recovery: blob reconciliation failed (will retry from background sweep)");
            }
        }
    }

    // Wrap each store's redo log in Arc<Mutex> for shared access from dispatch
    // threads. `redo_logs_arc[i]` is store i's log; `redo_log` is store 0's,
    // kept as the single representative handle the index / engine
    // representative slot / replication receiver fall back to.
    let redo_logs_arc: Option<Vec<Arc<Mutex<RedoLog>>>> =
        redo_logs.map(|logs| logs.into_iter().map(|l| Arc::new(Mutex::new(l))).collect());
    let redo_log: Option<Arc<Mutex<RedoLog>>> =
        redo_logs_arc.as_ref().and_then(|v| v.first().cloned());

    // Attach EACH store's redo log to ITS OWN allocator BEFORE moving them into
    // the engine, so every allocate/free is journaled and fsynced to that
    // store's log before the caller observes its effect. This closes the crash
    // window between `persist()` snapshots. Each allocator already carries its
    // store's `redo_device_id`.
    if let Some(ref logs) = redo_logs_arc {
        for (alloc, log) in store_allocators.iter_mut().zip(logs.iter()) {
            alloc.set_redo_log(log.clone());
        }
    }

    // Attach the representative (store 0) redo log to the primary index so
    // file-backed hash table resizes are crash-atomic (Begin/Commit journaling
    // + parent-dir fsync). The FileBacked variant actually uses the redo log;
    // InMemory / OnDisk accept the attachment but treat it as a no-op.
    if let Some(ref log) = redo_log {
        index.set_redo_log(log.clone());
    }

    // 4. Create engine — index is already a ShardedIndex (from the recovery
    // path above), so pass it directly to avoid re-wrapping in N=1.
    let locks = StripedLocks::new(config.lock_stripes);
    // Split the store allocators into store 0 (primary) + aux stores 1..N,
    // pairing each aux allocator with its device, and construct the multi-store
    // engine. With one store, aux is empty and this is exactly the prior
    // single-store engine.
    // Segment engine: recompute each store's append frontier from the rebuilt
    // index BEFORE the allocators are moved into the engine, so a post-checkpoint
    // record (beyond the stale header cursor) is never overwritten by the first
    // fresh allocation. No-op for the in-place engine (skipped here to avoid the
    // per-store metadata read).
    if config.storage.engine == teraslab::config::StorageEngine::Segment
        && let Err(e) = teraslab::recovery::recover_allocator_frontiers(
            &index,
            &store_devices,
            &mut store_allocators,
        )
    {
        tracing::error!(
            err = %e,
            "FATAL: could not recompute the segment allocator frontier on recovery \
             (a corrupt highest-offset record); refusing to start rather than risk \
             overwriting live data",
        );
        std::process::exit(1);
    }
    let mut alloc_iter = store_allocators.into_iter();
    let primary_allocator = alloc_iter
        .next()
        .expect("at least one store allocator (validated >= 1 store)");
    // store_allocators already holds boxed `dyn RecordAllocator` (in-place or
    // log-structured per storage.engine), so pair each aux allocator with its
    // device directly.
    let aux_stores: Vec<(Arc<dyn BlockDevice>, teraslab::allocator::BoxedAllocator)> =
        store_devices[1..].iter().cloned().zip(alloc_iter).collect();
    let mut engine = Engine::new_multi_store(
        store_devices[0].clone(),
        primary_allocator,
        aux_stores,
        index,
        locks,
        dah_index,
    );

    // Honor the configured create-time store placement strategy (round-robin by
    // default, or deterministic txid→store). Reads always route by the recorded
    // device_id, so this only changes where NEW records land — safe to set on an
    // already-populated store.
    engine.set_placement_strategy(config.storage.placement);
    tracing::info!(
        placement = ?config.storage.placement,
        stores = engine.store_count(),
        "store placement strategy set",
    );

    // Fail closed if the recovered/loaded index references a store that does not
    // exist in the current layout (a `device_id >= store_count`). That means the
    // node was previously run with MORE stores than are configured now, so the
    // data placed on the removed stores is unreachable; routing such an entry
    // would index out of bounds in `device_for`/`allocator_for` and panic the
    // first request that touches it. Surface a clear operator error at boot.
    if let Err(device_id) = engine.validate_device_ids() {
        tracing::error!(
            device_id,
            store_count = engine.store_count(),
            "FATAL: index references device_id {device_id} but only {} store(s) are \
             configured — this node was previously run with more stores. Restore the \
             original device layout (device_paths × device_split) or reset the node.",
            engine.store_count(),
        );
        std::process::exit(1);
    }

    // Drain R-221 engine-level append intents after constructing the engine
    // but before attaching the engine redo handle. The allocator already has
    // redo attached above, so replacement child-list block allocations remain
    // journaled; the original AppendConflictingChild intent remains in the
    // log until checkpoint, so writing a duplicate high-level intent here is
    // unnecessary.
    //
    // F-G10-015: `Engine::append_conflicting_child` is idempotent for the
    // (parent, child) pair — recovery may surface a draining-intent for a
    // child the redo replay already applied to the index. The engine
    // tolerates the redundant call by short-circuiting when the child is
    // already present in the parent's list. Until G2 surfaces a public
    // `has_conflicting_child` accessor we rely on that engine-side check;
    // the orchestrator should follow up to expose the accessor so this
    // loop can pre-filter (audit follow-up FUP-G10-015).
    if !pending_conflicting_children.is_empty() {
        for pending in &pending_conflicting_children {
            // Drain in redo-log order: append vs remove per the recorded intent.
            // Both engine ops are idempotent, so re-draining is safe.
            let result = if pending.is_remove {
                engine.remove_conflicting_child(&pending.parent_key, pending.child_txid)
            } else {
                engine.append_conflicting_child(&pending.parent_key, pending.child_txid)
            };
            if let Err(e) = result {
                tracing::error!(
                    parent_key = ?pending.parent_key,
                    child_txid = ?pending.child_txid,
                    is_remove = pending.is_remove,
                    err = %e,
                    "recovery: failed to drain conflicting-child intent; aborting startup",
                );
                std::process::exit(1);
            }
        }
        tracing::info!(
            drained = pending_conflicting_children.len(),
            "recovery: drained pending conflicting-child append intents",
        );
    }

    // AUDIT M2.6 — drain deleted-child append intents the same way. A crash
    // between the prune and the deleted-child append left this list short;
    // `Engine::append_deleted_child` is idempotent so re-draining is safe and
    // restores the idempotent-respend-defense / audit trail.
    if !pending_deleted_children.is_empty() {
        for pending in &pending_deleted_children {
            if let Err(e) = engine.append_deleted_child(&pending.parent_key, pending.child_txid) {
                tracing::error!(
                    parent_key = ?pending.parent_key,
                    child_txid = ?pending.child_txid,
                    err = %e,
                    "recovery: failed to drain deleted-child intent; aborting startup",
                );
                std::process::exit(1);
            }
        }
        tracing::info!(
            drained = pending_deleted_children.len(),
            "recovery: drained pending deleted-child append intents",
        );
    }

    // Rebuild the in-memory conflicting index from the recovered primary
    // index. It carries no on-device durability of its own, so it is
    // re-derived here from each record's authoritative CONFLICTING flag.
    engine.rebuild_conflicting_index();

    // Rebuild the in-memory preserve index the same way (#25): it is not
    // journaled, so it is re-derived from each record's authoritative on-device
    // `preserve_until` before any client traffic. From here the per-mutation
    // `update_preserve_index` calls keep it current. This is the SOLE
    // O(store) preserve scan — the expiry sweep is now O(expired).
    if let Err(e) = engine.rebuild_preserve_index_from_device() {
        tracing::error!(
            err = %e,
            "FATAL: failed to rebuild the preserve index from the recovered \
             primary index; aborting startup",
        );
        std::process::exit(1);
    }

    // Recover the in-memory ShardedMinedIndex the same way: it lives only in
    // RAM (unlike the primary index), so it boots empty on every restart —
    // both the snapshot-restore and device-scan-rebuild paths above converge
    // on `index` by this point, and either one leaves `mined_slot` stale or
    // sentinel. Task 16d: pure store-auth recovery — the checkpoint's
    // TXID-keyed MinedIndex snapshot + post-checkpoint redo-tail replay is
    // the ONLY path now (the device-scan fallback was removed: `set_mined`
    // performs zero device writes, so the device is no longer a trustworthy
    // reconstruction source). A genuinely fresh boot (no checkpoint has ever
    // run) instead does a full redo-tail replay from genesis. Either
    // outcome overwrites every primary entry's `mined_slot` with a
    // freshly-allocated slot before any client traffic (GET/spend/
    // delete_eval all read mined-state from this index, not the device); an
    // absent/corrupt/stale-fenced `.mined` section despite an EXISTING
    // checkpoint is now FATAL (aborts startup below) rather than silently
    // rebuilding stale mined-state from the device.
    let primary_snapshot_path = config.resolved_index_snapshot_path();
    let mined_snapshot_path =
        teraslab::checkpoint::mined_index_snapshot_path(&primary_snapshot_path);
    let mined_redo_logs: &[Arc<Mutex<RedoLog>>] = redo_logs_arc.as_deref().unwrap_or(&[]);
    match engine.recover_mined_index(
        &primary_snapshot_path,
        &mined_snapshot_path,
        mined_redo_logs,
    ) {
        Ok(used_snapshot) => {
            tracing::info!(
                used_snapshot,
                "mined-index recovery complete (true = checkpoint snapshot + redo tail, \
                 false = fresh boot, full redo-tail replay from genesis)",
            );
        }
        Err(e) => {
            tracing::error!(
                err = %e,
                "FATAL: failed to recover the mined index from the recovered \
                 primary index; aborting startup",
            );
            std::process::exit(1);
        }
    }

    // Task 16d recovery reorder: with the MinedIndex now recovered, rebuild the
    // DAH secondary index store-authoritatively from it (the device-metadata
    // reconcile in `recover_all_multi_store` was deferred above; Task 16e
    // removed the former sibling `unmined_index` secondary this step used to
    // also rebuild — `unmined_since` membership now lives solely in the
    // recovered MinedIndex, nothing left here to reconcile for it). Post-16d
    // the device mined-state fields are stale, so this is the ONLY correct
    // source: it EXCLUDES a reorg-unmined record's stale DAH (no premature
    // delete of a retained record on crash recovery) and re-derives a
    // setMined-planted DAH the device never recorded.
    //
    // P0 (premature-sweep): the "current height" for re-deriving a device-stale
    // DAH must be the node's real chain height, NOT the bare
    // `recovery_height_floor`. That floor is the max height in the REPLAYED REDO
    // TAIL only, and is 0 for a height-free tail — routine, since
    // Create/Relocate/Delete/V1-spend carry no height and a clean restart (or a
    // crash right after a checkpoint) replays no height-bearing entry. Passing 0
    // here would re-derive every setMined-planted DAH as `0 + retention`, already
    // past at real chain height → the acked record is swept a full retention
    // window early and a later reorg-unspend returns TxNotFound. Fold the durable
    // `.height` file (a pure atomic + CRC read, no side effects) into the floor
    // BEFORE reconcile via `reconcile_height_floor` = `max(persisted, tail)`;
    // `persisted_height` / `height_path` are reused below for the engine-side
    // last-durable-height restore, keeping the two floors identical.
    let height_path = config.resolved_last_durable_height_path();
    let persisted_height = teraslab::ops::engine::read_durable_height_file(&height_path);
    let reconcile_floor =
        teraslab::ops::engine::reconcile_height_floor(persisted_height, recovery_height_floor);
    // FU#7 Option B: pick the DAH-reconcile scope. The O(redo) Touched fast path
    // is eligible ONLY when ALL of:
    //   * `fast_boot_touched_secondaries` is enabled (default off), AND
    //   * the redb backend loaded BOTH secondaries cleanly (== the
    //     `!full_secondary_rebuild` predicate captured during recovery), AND
    //   * the restored `.mined` snapshot was v3 (persisted the DE/LSA cache).
    // Any miss falls back to the Full whole-store self-healing rebuild — the
    // unchanged, always-correct path (a first-boot v2 snapshot takes it once,
    // then the next checkpoint writes v3). The v3 FORMAT is always written, so
    // enabling the flag later needs no migration.
    let fast_boot_eligible = config.index.fast_boot_touched_secondaries
        && clean_secondaries_for_fast_boot
        && engine.mined_snapshot_restored_v3();
    let reconcile_scope = if fast_boot_eligible {
        teraslab::ops::engine::DahReconcileScope::Touched(&recovery_touched_keys)
    } else {
        teraslab::ops::engine::DahReconcileScope::Full
    };
    tracing::info!(
        fast_boot_eligible,
        touched_keys = recovery_touched_keys.len(),
        clean_secondaries = clean_secondaries_for_fast_boot,
        snapshot_v3 = engine.mined_snapshot_restored_v3(),
        flag = config.index.fast_boot_touched_secondaries,
        "FU#7 Option B: selecting DAH reconcile scope (Touched = O(redo), Full = O(store))",
    );
    if let Err(e) = engine.reconcile_secondaries_scoped(
        reconcile_scope,
        reconcile_floor,
        config.block_height_retention,
    ) {
        tracing::error!(
            err = %e,
            "FATAL: failed to rebuild the DAH secondary index from the \
             recovered mined index; aborting startup",
        );
        std::process::exit(1);
    }
    tracing::info!(
        dah_entries = engine.dah_index().len(),
        "DAH secondary index rebuilt store-authoritatively from the recovered mined index",
    );

    // Attach the per-store redo logs so the engine performs two-phase durability
    // for secondary index updates (redo fsync BEFORE redb commit), routing each
    // intent to the owning store's log. This is the SOLE attach point: store 0's
    // log is `logs[0]` (the representative handle the replication receiver and
    // migration suppression read via `engine.redo_log()`).
    if let Some(ref logs) = redo_logs_arc {
        engine.set_redo_logs(logs.clone());
        // As of the P0 double-spend fix the segment spend redo is uniform across node
        // roles: EVERY segment spend — standalone and clustered — emits the
        // convertible per-vout SpendV2 and the relocate move journals nothing, so the
        // engine no longer needs a clustered/standalone flag
        // (specs/SEGMENT_CLUSTERING_DESIGN.md).
        let clustered = config.is_clustered() || config.replication_factor > 1;
        // Surface the segment-on-cluster path explicitly: `Segment` is the default
        // engine (and an empty `[storage] engine` parses to it), so a clustered
        // node with the engine unset now boots the clustered-segment path where a
        // pre-cluster build would have refused. Make that non-silent for operators.
        if clustered && config.storage.engine == teraslab::config::StorageEngine::Segment {
            tracing::info!(
                node_id = config.node_id,
                replication_factor = config.replication_factor,
                "clustered node running the SEGMENT storage engine: spend replication \
                 and recovery are carried by the convertible SpendV2 redo; durability \
                 is replication-quorum + failover + rejoin-resync under the required \
                 buffered mode (specs/SEGMENT_CLUSTERING_DESIGN.md)"
            );
        }
        // Apply buffered (relaxed) redo durability if configured. Must follow
        // set_redo_logs so the per-store group-commit coordinators exist.
        // `redo_buffered_io` implies buffered durability (see
        // `redo_buffered_effective`).
        if config.redo_buffered_effective() {
            engine.set_buffered_durability(true);
            if buffered_loss_window_applies(config.replication_factor) {
                tracing::warn!(
                    flush_interval_ms = config.redo_flush_interval_ms,
                    buffered_io = config.redo_buffered_io,
                    "BUFFERED redo durability enabled — mutations are acked before \
                     fsync; up to one flush interval of acked writes may be lost on \
                     an unclean shutdown (relaxed-durability mode)"
                );
            } else {
                tracing::info!(
                    flush_interval_ms = config.redo_flush_interval_ms,
                    buffered_io = config.redo_buffered_io,
                    replication_factor = config.replication_factor,
                    "BUFFERED redo durability enabled — under replication_factor > 1 \
                     the redo tail and data devices are fsync-forced before every ack \
                     (C1), so no acked-write loss window applies"
                );
            }
        }
    }

    // 4b. Attach the (already-constructed) blobstore to the engine. The
    // store was built ahead of recovery so the orphan-blob reconciliation
    // could run against the freshly-replayed primary index — see R-049.
    engine.set_blob_store(blob_store.clone());

    let engine = Arc::new(engine);

    // Deletion tombstones removed: deletes are independent-node prune GC (no
    // tombstone log/index, no replication, no migration reconciliation).

    // Height subsystem (deletion-tombstone design §4): attach the durable
    // height file and restore the node's last-durable height. ALWAYS-ON and
    // additive — independent of `tombstones_enabled` / `tombstone_gc_enabled`.
    //
    // The restored value is `max(persisted_file, record_floor)`: the persisted
    // file is the primary source (atomic + CRC), and `record_floor` is a free
    // safety net that keeps the height from regressing below what the node's own
    // durable state proves it has seen even if the file is lost or corrupt.
    //
    // BUG3: `record_floor` is the MAX of two independent lower bounds, so it is
    // correct even with tombstones DISABLED:
    //   - the max tombstone `deletion_height` (only non-zero when tombstones are
    //     enabled — a tombstone's height ≤ the tip the node saw when deleting);
    //   - the max block height across replayed height-bearing redo entries
    //     (`recovery_height_floor`, always-on, independent of tombstones). This
    //     is what stops a tombstones-off node with a lost `.height` file from
    //     reporting 0 (which would freeze the cluster GC horizon and force
    //     unnecessary full resyncs — design §4 height subsystem).
    // Persistence keeps the value monotone across restarts.
    //
    // `height_path` + `persisted_height` were read above (folded into the
    // boot-reconcile floor); reuse them here for the engine-side height restore
    // so the restored height and the reconcile floor stay identical.
    let record_floor = recovery_height_floor;
    engine.set_last_durable_height_path(height_path.clone());
    let restored_height = engine.restore_last_durable_height(persisted_height, record_floor);
    tracing::info!(
        path = %height_path.display(),
        persisted = ?persisted_height,
        record_floor,
        restored = restored_height,
        "node last-durable height restored (height subsystem)",
    );

    // Resolve the reverse-heal enable ONCE: an explicit `reverse_heal.tombstones`
    // wins; unset defaults to ON for a clustered node (RF>1) and OFF for
    // single-node / RF=1 (a heal has no replica source there, so tombstones would
    // add cost with no benefit).
    let reverse_heal_enabled = config
        .reverse_heal
        .tombstones_enabled(config.replication_factor);

    // Reverse-heal Phase 2a: attach the deletion-tombstone log ONLY when
    // enabled. Runs AFTER the height restore so the boot floor-GC sees the
    // restored last-durable height, and AFTER recovery has rebuilt + reconciled
    // the primary index so the live-reconcile (Invariant TS-1) can drop any
    // tombstone whose record came back live.
    if reverse_heal_enabled {
        let tombstone_path = config.resolved_tombstone_log_path();
        let retention = config.resolved_tombstone_retention_blocks();
        match teraslab::ops::tombstone::TombstoneLog::load(
            tombstone_path.clone(),
            engine.index_seed(),
            engine.index_shard_count(),
            retention,
        ) {
            Ok(log) => {
                engine.set_tombstone_log(log);
                let dropped_live = engine.reconcile_tombstones_against_live_index();
                let dropped_gc = engine.gc_tombstones();
                tracing::info!(
                    path = %tombstone_path.display(),
                    retention_blocks = retention,
                    dropped_live,
                    dropped_gc,
                    "deletion tombstones enabled (reverse-heal Phase 2a): log replayed",
                );
            }
            Err(e) => {
                tracing::error!(
                    path = %tombstone_path.display(),
                    err = %e,
                    "failed to load deletion-tombstone log; refusing to start with a \
                     corrupt tombstone set (a lost tombstone risks resurrecting a deleted \
                     record on a future heal)",
                );
                std::process::exit(1);
            }
        }
    }

    // 5. Start cluster if configured
    //
    // F-G10-002: the bin's `shutdown_flag` only drives the background tasks
    // (checkpoint / blob_gc / lag_monitor / catch-up convergence). The
    // `Server::run` accept loop polls its own private flag — we flip that one
    // via the public `Server::shutdown()` method from the signal handler
    // below, AFTER we wrap `Server` in `Arc` so the handler closure can hold
    // a reference. Created here (before the cluster block) because the
    // catch-up context captures it so convergence loops abort promptly on
    // shutdown.
    let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    //
    // D-7/D-8: the catch-up handles built inside the cluster-start block are
    // captured here so the runtime lag monitor (spawned further down) can run
    // the same authenticated catch-up passes the startup pass uses. `None`
    // when single-node / RF=1 (no replicas to repair).
    let mut catchup_ctx: Option<CatchupContext> = None;
    let cluster = if config.is_clustered() {
        use teraslab::cluster::coordinator::{
            ClusterConfig, ClusterCoordinator, ReplicationRuntimeConfig,
        };
        use teraslab::cluster::shards::NodeId;

        // `validate_safe_defaults` already parsed both `listen_addr` and
        // `advertise_addr` (when set) — F-G10-013 made `advertise_addr` a
        // typed config error. The parses here are defensive: if they ever
        // fail, that's a logic bug between validation and use, not an
        // operator-fixable issue, so we log and exit rather than panicking.
        let bind_addr: std::net::SocketAddr = match config.listen_addr.parse() {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(addr = %config.listen_addr, err = %e, "FATAL: listen_addr unparseable post-validation");
                std::process::exit(1);
            }
        };
        // Determine the address to advertise to other nodes.
        // If advertise_addr is set, use it. Otherwise, if listen_addr uses
        // 0.0.0.0 (common in Docker), detect a non-loopback IP via getifaddrs.
        // If no advertise address is available we refuse to start: silently
        // advertising 0.0.0.0 (or guessing 8.8.8.8's route) broke SWIM
        // convergence in non-obvious ways. See F-G10-008.
        let self_addr: std::net::SocketAddr = if let Some(ref adv) = config.advertise_addr {
            match adv.parse() {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!(addr = %adv, err = %e, "FATAL: advertise_addr unparseable post-validation");
                    std::process::exit(1);
                }
            }
        } else if bind_addr.ip().is_unspecified() {
            match detect_local_ip() {
                Some(ip) => std::net::SocketAddr::new(ip, bind_addr.port()),
                None => {
                    tracing::error!(
                        listen_addr = %config.listen_addr,
                        "FATAL: listen_addr is 0.0.0.0 (or ::) but no non-loopback interface was found \
                         and `advertise_addr` is unset; set `advertise_addr` explicitly so peers can \
                         reach this node",
                    );
                    std::process::exit(1);
                }
            }
        } else {
            bind_addr
        };
        // SWIM must bind to the same stable IP as `self_addr` (advertised identity),
        // not `0.0.0.0` from a wildcard `listen_addr`. On multi-interface containers
        // (several Docker bridges), binding UDP to 0.0.0.0 can produce probes whose
        // source address does not match membership gossip, breaking convergence.
        let swim_bind = std::net::SocketAddr::new(self_addr.ip(), config.swim_port);
        let seed_addrs: Vec<std::net::SocketAddr> = config
            .seed_nodes
            .iter()
            .filter_map(|s| {
                // Try direct parse first (IP:port), then fall back to DNS resolution.
                s.parse().ok().or_else(|| {
                    use std::net::ToSocketAddrs;
                    s.to_socket_addrs().ok().and_then(|mut addrs| addrs.next())
                })
            })
            .collect();

        let probe_interval = std::time::Duration::from_millis(config.swim_probe_interval_ms);

        let cluster_state_path = config.resolved_cluster_state_path();
        // P1 stage 1 — load the topology state FAIL-CLOSED: a corrupt or
        // integrity-failed new-format file, an unreadable existing file,
        // or a legacy-format file without the one-shot
        // `--allow-legacy-topology-state` upgrade flag all refuse startup
        // (the StrictAuthRequiresSecret posture — a clustered node must
        // never boot on silently-defaulted topology safety state).
        let legacy_mode = if allow_legacy_topology_state {
            teraslab::cluster::coordinator::LegacyTopologyDecode::AllowAndUpgradeOnce
        } else {
            teraslab::cluster::coordinator::LegacyTopologyDecode::Refuse
        };
        let topo_state = match teraslab::cluster::coordinator::load_startup_topology_state(
            &cluster_state_path,
            legacy_mode,
        ) {
            Ok(state) => state,
            Err(e) => {
                tracing::error!(err = %e, "FATAL: topology state file failed to load (fail-closed)");
                std::process::exit(1);
            }
        };
        // P1 stage 1 (§4.1 "Armed marker") — refuse to start when the
        // `.regime-armed` marker is present but the persisted regime state
        // is absent/zeroed (lost state while enforcement was armed); a
        // marker with no committed term ever observed is a loud warning
        // only (a stray file must not be a boot-DoS).
        {
            let topology_path =
                teraslab::cluster::coordinator::topology_state_path_for_cluster_state(
                    &cluster_state_path,
                );
            if let Err(e) = teraslab::cluster::coordinator::validate_regime_armed_marker_at_boot(
                &topology_path,
                &topo_state,
            ) {
                tracing::error!(err = %e, "FATAL: .regime-armed marker check failed (fail-closed)");
                std::process::exit(1);
            }
        }
        // G8 final review (finding 1) — seed from the durable ANCHOR
        // (`committed_peak`/`committed_members.len()`), not the vestigial
        // `peak_cluster_size` field. `persisted_state_for_commit`
        // (topology.rs) computes `peak_cluster_size` PRE-apply from the
        // OLD peak, so on a committed shrink it is stale-HIGH (still the
        // pre-shrink peak) while `committed_peak` is already correctly
        // lowered. Seeding `initial_peak` from the stale field re-inflated
        // a shrunk floor straight back to the old peak on every restart.
        // Stale-LOW is unreachable here: persist is raise-only and
        // `committed_peak` is floored at `committed_members.len()`.
        let initial_peak = topo_state
            .committed_peak
            .max(topo_state.committed_members.len() as u64) as usize;
        let initial_epoch = topo_state.committed_term;

        let resolved_cluster_id = match config.resolved_cluster_id() {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(err = %e, "FATAL: invalid cluster_id config");
                std::process::exit(1);
            }
        };
        let cluster_config = ClusterConfig {
            self_id: NodeId(config.node_id),
            self_addr,
            swim_bind,
            swim_advertise_addr: None,
            seed_nodes: seed_addrs,
            replication_factor: config.replication_factor,
            probe_interval,
            suspicion_timeout: std::time::Duration::from_millis(config.swim_suspicion_timeout_ms),
            cluster_secret: config
                .cluster_secret
                .as_ref()
                .map(|s| s.as_bytes().to_vec()),
            max_migration_threads: config.max_migration_threads,
            topology_propose_timeout: std::time::Duration::from_millis(
                config.resolved_topology_propose_timeout_ms(),
            ),
            topology_debounce: std::time::Duration::from_millis(
                config.resolved_topology_debounce_ms(),
            ),
            migration_pool_size: config.migration_pool_size,
            migration_batch_size: config.migration_batch_size,
            persisted_incarnation: topo_state.incarnation,
            cluster_id: resolved_cluster_id,
            // Reverse-heal Phase 3b — RUNTIME online re-heal rides the reverse-heal
            // enable (`reverse_heal.tombstones`): RULE-DS, the delete-safe apply the
            // pull relies on, is a no-op without it. Default OFF.
            reverse_heal_online: reverse_heal_enabled,
            // Reverse-heal Phase 3c — fenced-heal deadline + fallback (design §E3).
            // Only consulted when online re-heal is enabled; a heal stuck past the
            // deadline escalates to a fresher master (default) or alert-and-holds.
            heal_deadline: config.reverse_heal.resolved_heal_deadline(),
            heal_deadline_action: config.reverse_heal.heal_deadline_action,
        };
        if initial_peak > 1 {
            tracing::info!(
                peak = initial_peak,
                term = initial_epoch,
                quorum = (initial_peak / 2) + 1,
                "cluster: restored peak/term from persisted state",
            );
        }
        let coordinator = ClusterCoordinator::new(cluster_config, initial_peak);
        // Restore topology state so new terms/epochs are strictly higher.
        coordinator
            .topology_epoch
            .store(initial_epoch, std::sync::atomic::Ordering::Relaxed);
        coordinator.topology_authority.restore(&topo_state);
        // P1 stage 1 (I4) — promotion-PROPOSAL opt-in
        // (`enable_automatic_promotion`, default false; already validated
        // against the resolved ack policy by `validate_cluster_safety`).
        // Gates only whether this node will ever PROPOSE
        // `promotion_enabled = true` — never what it applies (I0).
        coordinator
            .topology_authority
            .set_promotion_proposal_opt_in(config.enable_automatic_promotion);
        // C6 — reconstruct and install the committed topology's shard table so
        // a restarting node masters ONLY the shards it actually owns. The
        // bootstrap table is the single-member `[self]` table (self masters
        // every shard); stamping its `version` to the committed term would make
        // the stale-table gate treat it as CURRENT while it still claimed
        // all-master — an all-master split-brain write window until reactivation
        // converged. Installing the real committed assignment (from the
        // membership restored just above) scopes the node correctly at boot,
        // with no reliance on a later activation. When no committed membership
        // is recoverable, the bootstrap table is left as-is and the stale-table
        // gate withholds authority until the first activation.
        coordinator.install_restored_shard_table();
        let running = coordinator.start(
            engine.clone(),
            Some(cluster_state_path),
            redo_log.clone(),
            ReplicationRuntimeConfig {
                ack_policy: config.resolved_ack_policy(),
                best_effort: config.is_replication_best_effort(),
                timeout: std::time::Duration::from_millis(config.replication_timeout_ms.max(1)),
                timeout_during_migration: std::time::Duration::from_millis(
                    config.replication_timeout_during_migration_ms.max(1),
                ),
            },
        );
        // Restore migration state from a previous run so shards that were
        // mid-migration remain blocked (inbound) or tracked (outbound).
        //
        // A corrupt/truncated/unreadable inbound-fence file is fail-closed: it
        // is the only record of which shards were still receiving data, so we
        // abort startup rather than come up serving those (incomplete) shards
        // as complete authority. The node will NOT boot to receive a re-sync
        // while the condition persists — an operator must remove (or repair)
        // the corrupt fence file so the node restarts with no pending inbound
        // state and the source node re-initiates the migration.
        if let Err(e) = running.restore_inbound_state() {
            tracing::error!(err = %e, "cluster: inbound-fence state unrecoverable — refusing to start (incomplete shards would be served as complete); remove the corrupt inbound-state file to boot");
            std::process::exit(1);
        }
        running.restore_outbound_state();
        // Reverse-heal Phase 2d: hand the engine's checkpoint retention GC a
        // shared handle to the inbound-fence bitmap so a reverse-heal that spans
        // a checkpoint cannot GC a tombstone it still needs to gate an incoming
        // image (RULE-DS). Only meaningful when tombstones are enabled (the GC is
        // a no-op otherwise); every heal raises the inbound fence, so this covers
        // every heal_pending shard. See `Engine::set_tombstone_gc_guard`.
        if reverse_heal_enabled {
            engine.set_tombstone_gc_guard(running.inbound_fence_bitmap());
        }
        // Initialize persistent ACK tracker alongside the cluster state file.
        let ack_path = {
            let mut p = config.resolved_cluster_state_path().into_os_string();
            p.push(".repl-ack");
            std::path::PathBuf::from(p)
        };
        teraslab::server::dispatch::init_ack_tracker(ack_path);
        let applied_path = {
            let mut p = config.resolved_cluster_state_path().into_os_string();
            p.push(".repl-applied");
            std::path::PathBuf::from(p)
        };
        if let Err(e) = teraslab::server::dispatch::init_replica_applied_tracker(applied_path) {
            tracing::error!(err = %e, "replication receiver applied tracker init failed — aborting startup");
            std::process::exit(1);
        }
        let intent_path = {
            let mut p = config.resolved_cluster_state_path().into_os_string();
            p.push(".repl-intent");
            std::path::PathBuf::from(p)
        };
        if let Err(e) = teraslab::server::dispatch::init_replication_intent_tracker(intent_path) {
            tracing::error!(err = %e, "replication intent tracker init failed — aborting startup");
            std::process::exit(1);
        }

        // Reverse-heal Phase 1 (finding C1): DETECT a lost acked tail at boot.
        // Tier-1 fast-path — if the persistent AckTracker proves this node
        // durably ACKed a downstream replica BEYOND the redo tail recovery could
        // restore (`shared_seq_floor`), it returned STATUS_OK for writes it no
        // longer holds. This build is DETECTION-ONLY: LOG loudly + METER the
        // suspicion via the `stale_suspect_shards` gauge (scoped to the shards
        // this node masters). No fence, no pull — those land in later phases.
        if config.replication_factor > 1
            && let Some(tracker) = teraslab::server::dispatch::ack_tracker_handle()
        {
            let lost = tracker.acked_beyond(shared_seq_floor);
            if lost.is_empty() {
                tracing::debug!(
                    floor = shared_seq_floor,
                    "reverse-heal: no lost acked tail at boot (Tier-1 clean)",
                );
            } else {
                // P2 — scope the suspicion to the shards this node masters that
                // REPLICATE to a lost-tail replica, not ALL mastered shards. The
                // lost tail was acked by specific downstream replicas
                // (`lost[..].0`), and a replica only holds the shards it replicates
                // for, so `{ mastered } ∩ { replicating to a lost replica }` is the
                // tightest correct bound (the AckTracker's redo sequences are
                // node-coarse and carry no shard). Reduces the wedge blast radius
                // and boot cost. If the mapping is empty (a lost replica no longer
                // resolves to a known address), fall back to all mastered shards —
                // fail-closed: better to over-suspect than to miss a real gap.
                let lost_addrs: Vec<std::net::SocketAddr> =
                    lost.iter().map(|(addr, _)| *addr).collect();
                let scoped = running.mastered_shards_replicating_to(&lost_addrs);
                let suspects = if scoped.is_empty() {
                    running.mastered_shards()
                } else {
                    scoped
                };
                tracing::error!(
                    floor = shared_seq_floor,
                    lost_replicas = lost.len(),
                    suspect_shards = suspects.len(),
                    ?lost,
                    "reverse-heal: LOST ACKED TAIL detected at boot (Tier-1) — this \
                     node acked writes beyond its recovered redo floor; marking the \
                     shards it masters that replicate to a lost-tail replica \
                     stale-suspect (detection-only: no fence, no pull)",
                );
                running.record_stale_suspect_shards(suspects);
            }
        }

        // Reverse-heal G3 (finding G3): a co-located `CreateV2` whose record
        // bytes were lost on the buffered tail is DROPPED by recovery — a silent
        // loss of an acked create that, under active replication (RF>1), a
        // quorum-current replica still holds. The Tier-1 AckTracker detector
        // above can MISS this (it flags only when the persistent AckTracker
        // proves a lost tail, which lags on a ≤1 s / 100-ACK cadence and is
        // node-coarse), so surface the precise per-shard signal recovery
        // collected and fold it into the SAME `stale_suspect_shards` set the
        // Phase-2c pull below consumes. Gated on RF>1 + reverse-heal enabled: a
        // single-device / RF=1 node has no replica to pull the lost create from,
        // so the skip stays a logged drop (no false stale-suspect). Runs AFTER
        // Tier-1 (which replaces the set) and BEFORE Phase 2c (which reads it),
        // and UNIONs with any Tier-1 suspects.
        {
            let g3_shards = teraslab::cluster::coordinator::colocated_create_stale_shards(
                &g3_resync_create_keys,
                config.replication_factor,
                reverse_heal_enabled,
            );
            if !g3_shards.is_empty() {
                let mut suspects: std::collections::BTreeSet<u16> =
                    running.stale_suspect_shards().into_iter().collect();
                let before = suspects.len();
                suspects.extend(g3_shards.iter().copied());
                let added = suspects.len() - before;
                tracing::error!(
                    lost_creates = g3_resync_create_keys.len(),
                    g3_stale_shards = g3_shards.len(),
                    newly_marked = added,
                    "reverse-heal G3: co-located CreateV2 record bytes lost on the \
                     buffered tail under RF>1 — marking their shards stale-suspect \
                     so the reverse-pull recovers the acked creates from a \
                     quorum-current replica (instead of a silent drop)",
                );
                running.record_stale_suspect_shards(suspects);
            } else if !g3_resync_create_keys.is_empty() {
                // RF=1 / single-device, or reverse-heal disabled: no replica to
                // pull from → the lost creates stay a logged drop (unchanged).
                tracing::warn!(
                    lost_creates = g3_resync_create_keys.len(),
                    replication_factor = config.replication_factor,
                    reverse_heal = reverse_heal_enabled,
                    "reverse-heal G3: co-located CreateV2 record bytes lost on the \
                     buffered tail, but no replica to heal from (RF=1 / single-device \
                     or reverse-heal disabled) — dropping the creates (unchanged)",
                );
            }
        }

        // Reverse-heal Phase 2c (finding C1): the detected stale-suspect shards
        // now drive a DELETE-SAFE reverse-PULL — gated on
        // `reverse_heal.tombstones` (the same enable that attached the tombstone
        // log above; RULE-DS is a no-op without it; defaults ON for a clustered
        // node, RF>1). SAFETY — NO-SERVE-BEFORE-HEAL: this runs BEFORE the
        // readiness transition below, so every stale shard is fenced (its
        // `is_master` answers `Transitioning`, never `Yes`) until its heal
        // completes or is proven impossible. A source that never converges
        // leaves the shard fenced fail-closed rather than timing out + giving
        // up, bounded by `heal_deadline_secs` (design §E3, Phase 3c) which
        // surfaces the stuck shard loudly without un-fencing it (auto-escalation
        // was rejected as unsafe — see `HealDeadlineAction`). This boot-time
        // pull is now complemented by Phase 3b RUNTIME online re-heal
        // (`RunningCluster::run_online_reheal`) — the pull is NOT boot-triggered
        // only anymore.
        //
        // P1 — ACCEPTED RESIDUAL (consensus, design-acked E5, do NOT treat as
        // fixed): RULE-DS drops a `ClientDelete`-tombstoned key's baseline
        // Create unconditionally (`TombstoneLog::blocks_heal_apply`), so a
        // legitimately RE-CREATED UTXO is LOST when a BOOT heal is its SOLE
        // carrier (client-delete k → node down → reorg re-creates k while down
        // → boot heal drops k, tombstone stays live → node masters missing a
        // live UTXO until the tombstone GCs at `tombstone_retention_blocks`).
        // Phase 3b online re-heal closes the common case — a re-create
        // delivered via the normal replica `Create` path is admitted
        // immediately, since RULE-DS here only ever gates an ABSENT-key
        // BOOT-heal baseline — but not this specific gap: there is nothing for
        // online re-heal to re-detect until the boot heal itself runs. This is
        // the CONSERVATIVE direction (a LOSS, not a double-spend). A
        // HEIGHT-AWARE ClientDelete gate (admit a re-org re-create at height >
        // deletion_height) was proposed and REJECTED in review — there is no
        // immutable create-height to gate on, so the gate would itself open a
        // latent double-spend window. The accepted mitigation is sizing
        // `tombstone_retention_blocks` at or above the reorg/finality horizon.
        if reverse_heal_enabled {
            let stale = running.stale_suspect_shards();
            if !stale.is_empty() {
                // No membership exchange has converged a partition view at boot,
                // so source selection falls back to the shard's committed replica
                // set (a data holder by assignment; the source re-validates
                // ownership before streaming, and RULE-DS + generation
                // idempotency gate every applied image). Shards with a source are
                // fenced + queued for the pull; shards with NONE are fenced
                // FAIL-CLOSED (never served un-healed).
                let empty_view = std::collections::HashMap::new();
                let sources = running.select_reverse_heal_sources(&stale, &empty_view);
                let queued = running.begin_reverse_heal(&sources);
                let sourced: std::collections::HashSet<u16> =
                    sources.iter().map(|(s, _)| *s).collect();
                let mut fenced_fail_closed = 0usize;
                for &shard in &stale {
                    if !sourced.contains(&shard) {
                        // P0 — `mark_inbound_heal_fence` (not `mark_inbound_active`)
                        // raises the `heal_pending` marker so this fail-closed
                        // fence SURVIVES a concurrent runtime topology commit's
                        // `clear_inbound` — the un-healed shard is never served.
                        running.mark_inbound_heal_fence(shard);
                        fenced_fail_closed += 1;
                    }
                }
                tracing::warn!(
                    stale_shards = stale.len(),
                    pull_queued = queued,
                    fenced_fail_closed,
                    "reverse-heal Phase 2c: fenced stale shards (no-serve-before-heal) \
                     and queued delete-safe reverse-pull from committed sources",
                );
            }
        }

        if config.replication_factor > 1 {
            // Startup barrier: durable pending replication intents must be
            // resolved before any HTTP or TCP listener is started below. If a
            // restarted master accepted new writes first, an old local-only
            // mutation could remain neither replicated nor compensated while
            // new sequence ranges advance past it.
            let start = std::time::Instant::now();
            loop {
                match teraslab::server::dispatch::recover_pending_replication_intents(
                    &running,
                    redo_log.as_deref(),
                    &engine,
                ) {
                    Ok(()) => break,
                    // Redo-pressure (`RedoError::LogFull`) on rejoin is
                    // transient self-healing backpressure, NOT a terminal
                    // fault: the inbound migration applies that are filling
                    // the redo log are idempotently re-drivable from their
                    // source under the persisted inbound fence, so the
                    // checkpointer/catch-up will drain the log and free space.
                    // Aborting here would leave the cluster permanently stuck
                    // at 0/N ready (rejoin-after-quiesce, scenario_09). Keep
                    // retrying past the 60s window — only the marker re-drive
                    // is deferred, no client is served yet.
                    Err(e) if is_redo_pressure(&e) => {
                        tracing::warn!(
                            err = %e,
                            "replication intent recovery deferred by redo backpressure; \
                             retrying (transient, re-drivable from source)",
                        );
                        std::thread::sleep(std::time::Duration::from_millis(500));
                    }
                    Err(e) if start.elapsed() < std::time::Duration::from_secs(60) => {
                        tracing::warn!(
                            err = %e,
                            "replication intent recovery pending; retrying before serving",
                        );
                        std::thread::sleep(std::time::Duration::from_millis(500));
                    }
                    Err(e) => {
                        tracing::error!(
                            err = %e,
                            "replication intent recovery failed — aborting startup",
                        );
                        std::process::exit(1);
                    }
                }
            }
        }

        // Spawn background catch-up for replicas that are behind.
        // Reads persisted last_acked per replica and streams missing redo
        // entries. This runs asynchronously so it doesn't block startup.
        //
        // D-7/D-8: this one-shot startup pass and the runtime lag-monitor
        // converge loop now share a single `CatchupContext` and the
        // `run_one_catchup_pass` helper, so a replica that falls behind while
        // the master stays up is repaired during normal operation — not only
        // at master restart.
        if config.replication_factor > 1 {
            // Phase B3: each catch-up batch must carry the master's live
            // topology epoch so the receiver-side ERR_STALE_EPOCH gate
            // accepts it; the shared `Arc<AtomicU64>` is re-read per chunk.
            // Phase H: the resync handle posts a full-shard backfill when the
            // redo log has wrapped past a replica's last-acked position.
            // R-D2/D-3 + catch-up auth: chunks go through
            // `send_replica_ops_to`, sharing the per-address dense stream
            // cursor and pooled HMAC-authenticated connection with the
            // steady-state fan-out.
            let ctx = CatchupContext {
                redo_log: redo_log.clone(),
                engine: engine.clone(),
                cluster_key_handle: running.cluster_key_handle(),
                topology_authority: running.topology_authority_handle(),
                resync_handle: running.resync_sender_handle(),
                auth_secret: running.cluster_secret().map(|s| s.to_vec()),
                source_node_id: config.node_id,
                replication_timeout: std::time::Duration::from_millis(
                    config.replication_timeout_ms.max(1),
                ),
                migration_throttle: running.migration_throttle().clone(),
                catchup_in_flight: Arc::new(teraslab::replication::durable::CatchupInFlight::new()),
                shutdown: shutdown_flag.clone(),
            };
            // Stash for the runtime lag monitor (spawned after this block).
            catchup_ctx = Some(ctx.clone());

            // G12: snapshot the topology's EXPECTED replica set so the startup
            // pass catches up a replica the master has never ACKed (no tracker
            // entry) rather than no-opping on an empty tracker.
            let expected_replicas = running.expected_replica_addrs();

            std::thread::spawn(move || {
                // Use the process-wide ACK tracker installed by
                // `init_ack_tracker` above — constructing a second
                // instance on the same path would race its flushes.
                let tracker = match teraslab::server::dispatch::ack_tracker_handle() {
                    Some(t) => t,
                    None => return,
                };
                let all_acked = tracker.all_acked();

                let current_seq = ctx
                    .redo_log
                    .as_ref()
                    .map(|rl| rl.lock().current_sequence())
                    .unwrap_or(0);

                // G12: drive the UNION of tracker-known and expected replicas.
                // An expected-but-absent replica gets from_seq = 0 + 1 = 1.
                let targets = teraslab::server::dispatch::startup_catchup_targets(
                    &all_acked,
                    &expected_replicas,
                    current_seq,
                );
                // Sequential per replica, deliberately: the convergence loop
                // self-terminates, and the runtime lag monitor (which shares
                // the in-flight registry) picks up any replica still behind,
                // so a slow first replica cannot starve the others for more
                // than one monitor interval.
                for (addr, last_acked) in targets {
                    tracing::info!(
                        %addr,
                        lag = current_seq - last_acked,
                        from_seq = last_acked + 1,
                        "catchup: replica behind, starting catch-up",
                    );
                    run_replica_convergence(&ctx, tracker, addr);
                }
            });
        }

        tracing::info!(
            node_id = config.node_id,
            rf = config.replication_factor,
            "cluster: node started",
        );
        Some(Arc::new(running))
    } else {
        tracing::info!("cluster: single-node mode (node_id=0)");
        None
    };

    // 6. Start HTTP observability server
    let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // F-G10-021: parse the validated `http_listen_addr` and take its port
    // directly. Pre-fix this fell back to `9100` on parse failure, which
    // silently misreported the bound port when validation was weakened.
    let http_port: u16 = match config.http_listen_addr.parse::<std::net::SocketAddr>() {
        Ok(sa) => sa.port(),
        Err(e) => {
            tracing::error!(
                addr = %config.http_listen_addr,
                err = %e,
                "FATAL: http_listen_addr unparseable post-validation",
            );
            std::process::exit(1);
        }
    };
    // The online-backup blob-GC pause flag is created HERE (ahead of the HTTP
    // state and the blob-GC spawn below) so the BackupManager and the blob-GC
    // sweep share ONE Arc: a running backup toggles this flag to pause GC, and
    // the sweep observes it. Defaults to unpaused (GC runs normally).
    let blob_gc_pause = Arc::new(AtomicBool::new(false));
    // Online-backup coordinator (single-flight over a background job). Its root
    // is `config.backup.backup_dir`; when unset (`None`) `start()` rejects every
    // request, so backups are inert unless explicitly configured.
    let backup_manager = teraslab::backup::BackupManager::new(
        engine.clone(),
        blob_gc_pause.clone(),
        config.backup.to_params(),
        config.clone(),
        config.backup.backup_dir.clone(),
    );
    let http_state = Arc::new(HttpState {
        backup: backup_manager,
        engine: engine.clone(),
        metrics: &SERVER_METRICS,
        histograms: &SERVER_HISTOGRAMS,
        // M-02: constructed not-ready; flipped to `true` below once
        // recovery + engine attach are known-complete. Constructing the
        // flag `true` made readiness correctness depend solely on the
        // HTTP thread spawning after synchronous recovery — a future
        // refactor that starts HTTP earlier would silently re-introduce
        // the "ready before recovery" bug (F-G6-001).
        ready: Arc::new(AtomicBool::new(false)),
        log_level: Arc::new(AtomicU8::new(2)), // INFO
        cluster: cluster.clone(),
        redo_log: redo_log.clone(),
        redo_atomics: redo_log.as_ref().map(|r| r.lock().atomics()),
        active_connections: active_connections.clone(),
        http_port,
        replica_lag_warn_threshold_ops: config.replica_lag_warn_threshold_ops,
        replica_lag_cache: std::sync::atomic::AtomicU64::new(0),
    });
    // M-02: recovery (step 4) and engine construction completed
    // synchronously above, so this node can serve traffic — mark ready
    // before the HTTP listener starts answering probes.
    http_state
        .ready
        .store(true, std::sync::atomic::Ordering::Release);
    let http_addr = config.http_listen_addr.clone();
    let admin_endpoints_enabled = config.enable_admin_endpoints;
    // R-056: when admin endpoints are on, the bearer token has been validated
    // non-empty by `validate_safe_defaults`. We pass an owned clone into the
    // dedicated HTTP thread; cloning a small `String` is cheap and avoids
    // sharing mutable state with the rest of the server. The unwrap of the
    // `Secret` newtype is benign: the inner `String` is what
    // `start_http_server` already consumes, and `Secret` only wraps the
    // `Debug` impl, not the runtime API.
    let admin_token = config.admin_token.as_ref().map(|s| s.as_str().to_string());
    std::thread::spawn(move || {
        start_http_server(http_addr, http_state, admin_endpoints_enabled, admin_token);
    });

    // 7. Setup TCP server
    let mut server =
        Server::new(engine.clone(), config.clone()).with_active_connections(active_connections);
    if let Some(ref c) = cluster {
        server = server.with_cluster(c.clone());
    }
    if let Some(ref rl) = redo_log {
        server = server.with_redo_log(rl.clone());
    }
    server = server.with_blob_store(blob_store.clone());
    // (`shutdown_flag` is created above the cluster block — see F-G10-002
    // note there — so the catch-up context can capture it.)
    let server = Arc::new(server);

    // R-003: spawn the redo-log checkpoint task. Without a periodic
    // snapshot+reset, the redo log fills (~750k mutations at the 64 MiB
    // default + ~85 B/entry) and the master bricks: every subsequent
    // mutation returns ERR_INTERNAL once `RedoLog::append` returns
    // `LogFull`. The task wakes every 100 ms; when usage_fraction
    // crosses 0.5 it takes a snapshot, persists the allocator, writes a
    // checkpoint marker, and resets the log so future appends start
    // from offset 0. In replicated mode, the reset is skipped while any
    // known replica's durable ACK is below the redo floor that reset
    // would erase.
    let checkpoint_handle = redo_log.as_ref().map(|log| {
        let mut cfg =
            teraslab::checkpoint::CheckpointConfig::new(config.resolved_index_snapshot_path());
        // BC-01: honour operator-configured hysteresis band and poll
        // cadence rather than the library defaults. Config validation
        // guarantees `0 < low_water < high_water < 1` and
        // `poll_interval_ms > 0`, so the values below are safe to plug
        // in unchecked.
        cfg.high_water = config.checkpoint_high_water;
        cfg.low_water = config.checkpoint_low_water;
        // Without this the emergency mark stays at the library default (0.90);
        // an operator `checkpoint_high_water >= 0.90` would then collapse it
        // onto high_water and make every checkpoint blocking. Config validation
        // guarantees `high_water < emergency_water < 1`.
        cfg.emergency_high_water = config.checkpoint_emergency_water;
        cfg.poll_interval = std::time::Duration::from_millis(config.checkpoint_poll_interval_ms);
        // Pressure-aware segment-defrag tuning from [storage.defrag]. Config
        // validation (validate_sizes) already checked the ranges/ordering.
        cfg.defrag = config.storage.defrag.clone();
        if let Some(tracker) = teraslab::server::dispatch::ack_tracker_handle() {
            let cluster_for_reset = cluster.clone();
            // R13: capture the replication factor by value (u8 is Copy) so the
            // reset-guard closure stays 'static without borrowing `config`.
            let replication_factor = config.replication_factor;
            let reset_guard: std::sync::Arc<dyn Fn(u64) -> bool + Send + Sync + 'static> =
                std::sync::Arc::new(move |floor_sequence| {
                    let acked = tracker.all_acked();
                    // C3: seed the reclaim denominator from the topology's
                    // EXPECTED replica set, not just the replicas already
                    // present in the ACK tracker. An empty/partial tracker with
                    // an expected-but-unacked replica yields min_acked = 0 and
                    // blocks the reclaim, so we never erase a redo prefix a
                    // replica still needs.
                    let expected = cluster_for_reset
                        .as_ref()
                        .map(|c| c.expected_replica_addrs())
                        .unwrap_or_default();
                    let min_acked = teraslab::server::dispatch::min_acked_over_expected(
                        &acked,
                        &expected,
                        floor_sequence,
                        replication_factor,
                    );
                    let can_reset = min_acked >= floor_sequence;
                    if !can_reset {
                        tracing::warn!(
                            floor_sequence,
                            min_acked,
                            expected_replicas = expected.len(),
                            acked_replicas = acked.len(),
                            "checkpoint reset deferred until replicas catch up",
                        );
                    }
                    can_reset
                });
            teraslab::checkpoint::spawn_checkpoint_task_with_reset_guard(
                cfg,
                engine.clone(),
                log.clone(),
                shutdown_flag.clone(),
                reset_guard,
            )
        } else {
            teraslab::checkpoint::spawn_checkpoint_task(
                cfg,
                engine.clone(),
                log.clone(),
                shutdown_flag.clone(),
            )
        }
    });

    // Background redo flusher for buffered durability: periodically push every
    // store's redo log to the device so acked-but-unflushed mutations become
    // durable, bounding the crash-loss window to ~one interval. Strict
    // durability skips this (each commit already fsyncs). Observes the shutdown
    // flag and exits promptly.
    //
    // Under `redo_buffered_io` the periodic flush pwrites WITHOUT a per-flush
    // fsync (`flush_all_redo_no_sync`): the redo device is opened buffered, so
    // the bytes go to the OS page cache and durability is provided by kernel
    // writeback plus the checkpoint barrier's redo fsync before it reclaims.
    // This removes the periodic device fsync that, on some virtualized hosts,
    // stalls the VM for tens of milliseconds. Without `redo_buffered_io` the
    // periodic flush keeps its per-flush fsync (`flush_all_redo`), unchanged.
    //
    // The FINAL flush on a clean shutdown ALWAYS fsyncs (`flush_all_redo`),
    // regardless of `redo_buffered_io`, so a graceful stop loses nothing.
    let redo_flush_handle: Option<std::thread::JoinHandle<()>> = if config.redo_buffered_effective()
    {
        let engine = engine.clone();
        let shutdown_flag = shutdown_flag.clone();
        let interval = std::time::Duration::from_millis(config.redo_flush_interval_ms.max(1));
        let buffered_io = config.redo_buffered_io;
        Some(std::thread::spawn(move || {
            while !shutdown_flag.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(interval);
                let res = if buffered_io {
                    engine.flush_all_redo_no_sync()
                } else {
                    engine.flush_all_redo()
                };
                if let Err(e) = res {
                    tracing::error!(err = %e, "background redo flush failed");
                }
            }
            // Final flush on shutdown so a clean stop loses nothing — always a
            // real fsync, even under `redo_buffered_io`.
            if let Err(e) = engine.flush_all_redo() {
                tracing::error!(err = %e, "final redo flush on shutdown failed");
            }
        }))
    } else {
        None
    };

    // R-049: spawn the periodic orphan-blob GC sweep. Recovery already
    // reconciled the blob store against the freshly-replayed primary index
    // on startup; this task takes care of orphans that accumulate during
    // normal operation (failed creates whose registration was rejected,
    // aborted streaming uploads, migrations cancelled mid-flight). The
    // tick interval defaults to one hour and can be set to 0 to disable
    // the periodic sweep entirely (recovery-time reconciliation still runs).
    // The online backup pauses this sweep for the duration of a copy via
    // `blob_gc_pause` — created above (before the HTTP state) and shared with
    // the BackupManager. The clone below hands the sweep its read handle.
    let blob_gc_handle: Option<std::thread::JoinHandle<()>> = if config.blob_gc_interval_secs > 0 {
        let cfg = teraslab::storage::blob_gc::BlobGcConfig::new(config.blob_gc_interval_secs);
        Some(teraslab::storage::blob_gc::spawn_blob_gc_task(
            cfg,
            blob_store.clone(),
            engine.clone(),
            shutdown_flag.clone(),
            blob_gc_pause.clone(),
        ))
    } else {
        tracing::info!("blob-gc periodic sweep disabled (blob_gc_interval_secs = 0)",);
        None
    };

    // R-038 (D-01): spawn the replica-lag monitor when:
    //   (a) we are clustered (RF > 1, so `init_ack_tracker` has been
    //       called and the static is populated), AND
    //   (b) the operator has not explicitly disabled it via
    //       `replica_lag_check_interval_secs = 0`.
    // Pre-fix `replica_lag_check_interval_secs` was a dead config field —
    // `spawn_lag_monitor` existed and was tested in isolation but no
    // production code path ever called it. The lag monitor periodically
    // compares the master's current redo sequence against each replica's
    // last-acked sequence and emits `tracing::warn!` when the gap exceeds
    // `replica_lag_warn_threshold_ops`. `/metrics` exposes the same lag as
    // a bounded-cardinality gauge, and `/health/ready` uses the threshold to
    // let load balancers drain lagging leaders.
    let lag_monitor_handle: Option<std::thread::JoinHandle<()>> =
        if config.replication_factor > 1 && config.replica_lag_check_interval_secs > 0 {
            match (
                teraslab::server::dispatch::ack_tracker_handle(),
                redo_log.clone(),
            ) {
                (Some(tracker), Some(redo)) => {
                    let current_seq_fn: std::sync::Arc<dyn Fn() -> u64 + Send + Sync> = {
                        let redo = redo.clone();
                        std::sync::Arc::new(move || redo.lock().current_sequence())
                    };
                    // D-7/D-8: drive runtime catch-up from the lag monitor.
                    // When a replica's lag exceeds the warn threshold the
                    // monitor spawns the stream-until-converged loop for it
                    // on a background thread (so the monitor keeps ticking
                    // for other replicas); the per-replica in-flight slot
                    // makes a tick that fires mid-loop a no-op. The loop
                    // re-reads the live head and ACK watermark itself, so
                    // the tick's `last_acked`/`master_seq` snapshot is not
                    // forwarded. The callback is `None` when we have no
                    // catch-up context (e.g. RF=1), preserving warn-only
                    // behavior.
                    let on_lagging: Option<teraslab::replication::durable::OnLaggingReplica> =
                        catchup_ctx.clone().map(|ctx| {
                            let cb: teraslab::replication::durable::OnLaggingReplica =
                                std::sync::Arc::new(
                                    move |addr: std::net::SocketAddr,
                                          _last_acked: u64,
                                          _master_seq: u64| {
                                        spawn_replica_convergence(ctx.clone(), addr);
                                    },
                                );
                            cb
                        });
                    Some(teraslab::replication::durable::spawn_lag_monitor(
                        tracker,
                        current_seq_fn,
                        shutdown_flag.clone(),
                        config.replica_lag_check_interval_secs,
                        config.replica_lag_warn_threshold_ops,
                        config.replica_lag_warn_threshold_ops,
                        on_lagging,
                    ))
                }
                _ => {
                    tracing::warn!(
                        rf = config.replication_factor,
                        interval_secs = config.replica_lag_check_interval_secs,
                        "replica-lag monitor not spawned: ACK_TRACKER or redo_log unavailable",
                    );
                    None
                }
            }
        } else {
            None
        };

    let app = ServerWithShutdown {
        inner: server.clone(),
        shutdown: shutdown_flag.clone(),
        engine,
        snap_path: config.resolved_index_snapshot_path(),
        device,
        cluster,
        otlp_provider,
        // F-G10-022: take ownership of background-thread join handles so
        // `run()` can join them after the shutdown flag is set but before
        // `device.sync()`. Pre-fix these were `_`-prefixed bindings that
        // dropped at end-of-scope, leaving threads potentially mid-fsync
        // while the foreground unwind raced ahead.
        checkpoint_handle: Mutex::new(checkpoint_handle),
        blob_gc_handle: Mutex::new(blob_gc_handle),
        redo_flush_handle: Mutex::new(redo_flush_handle),
        lag_monitor_handle: Mutex::new(lag_monitor_handle),
    };

    // F-G10-001 + F-G10-002: install the SIGINT/SIGTERM handler now. The
    // handler closure flips BOTH atomics: the bin's `shutdown_flag` drives
    // the background tasks (checkpoint / blob_gc / lag_monitor), and the
    // public `Server::shutdown()` flips the accept-loop flag that
    // `Server::run` polls. Pre-fix only the former was wired and the
    // latter atomic was internal to `Server::new`, so no signal could ever
    // exit the accept loop.
    {
        let shutdown_clone = shutdown_flag.clone();
        let server_inner = server.clone();
        ctrlc_handler(move || {
            tracing::info!("shutdown signal received");
            shutdown_clone.store(true, std::sync::atomic::Ordering::Relaxed);
            server_inner.shutdown();
        });
    }

    // 7. Start serving
    if let Err(e) = app.run() {
        tracing::error!(err = %e, "server error");
        std::process::exit(1);
    }

    tracing::info!("server stopped");
}

struct ServerWithShutdown {
    inner: Arc<Server>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    engine: Arc<Engine>,
    snap_path: PathBuf,
    device: Arc<dyn BlockDevice>,
    cluster: Option<Arc<teraslab::cluster::coordinator::RunningCluster>>,
    /// OTLP provider, present when `[observability].otlp_endpoint` was
    /// configured. Flushed with a 5 s timeout on graceful shutdown.
    otlp_provider: Option<teraslab::observability::OtelTracerProvider>,
    /// Join handle for the redo-log checkpoint thread. See F-G10-022.
    checkpoint_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Join handle for the periodic blob-GC sweep thread. See F-G10-022.
    blob_gc_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Join handle for the background redo flusher (buffered durability only).
    redo_flush_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Join handle for the replica-lag monitor thread. See F-G10-022.
    lag_monitor_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl ServerWithShutdown {
    fn run(&self) -> Result<(), String> {
        let result = self.inner.run();

        // Mirror the signal-handler's flag flip in case `Server::run`
        // returned for another reason (a bind error, a test that called
        // `shutdown()` directly). Background threads exit on their next
        // poll once the flag is true.
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // F-G10-022: join background tasks before persistence. Each
        // observes the shutdown flag on its poll loop (typically ≤100 ms)
        // and exits. We bound the wait so a stuck thread cannot pin the
        // daemon forever — falling through and running persistence is
        // safer than blocking forever on shutdown.
        Self::join_with_timeout(
            "checkpoint",
            self.checkpoint_handle.lock().take(),
            std::time::Duration::from_secs(5),
        );
        Self::join_with_timeout(
            "blob_gc",
            self.blob_gc_handle.lock().take(),
            std::time::Duration::from_secs(5),
        );
        Self::join_with_timeout(
            "redo_flush",
            self.redo_flush_handle.lock().take(),
            std::time::Duration::from_secs(5),
        );
        Self::join_with_timeout(
            "lag_monitor",
            self.lag_monitor_handle.lock().take(),
            std::time::Duration::from_secs(5),
        );

        // On shutdown: stop cluster, sync device
        if let Some(ref cluster) = self.cluster {
            cluster.shutdown();
            tracing::info!("cluster stopped");
        }

        tracing::info!("persisting state");

        // Snapshot index to disk for fast restart
        match self.engine.snapshot_index(&self.snap_path) {
            Ok(()) => tracing::info!(path = %self.snap_path.display(), "index snapshot written"),
            Err(e) => tracing::warn!(err = %e, "index snapshot failed"),
        }

        // Persist allocator freelist
        match self.engine.persist_allocator() {
            Ok(()) => tracing::info!("allocator state persisted"),
            Err(e) => tracing::warn!(err = %e, "allocator persist failed"),
        }

        // Persist the node's last-durable height (deletion-tombstone design
        // §4, height subsystem). No-op when no path is attached.
        match self.engine.persist_last_durable_height() {
            Ok(()) => tracing::info!(
                height = self.engine.last_durable_height(),
                "last-durable height persisted"
            ),
            Err(e) => tracing::warn!(err = %e, "last-durable height persist failed"),
        }

        match teraslab::server::dispatch::flush_replication_intent_tracker() {
            Ok(()) => tracing::info!("replication intent tracker flushed"),
            Err(e) => tracing::warn!(err = %e, "replication intent tracker flush failed"),
        }

        // F-G10-003 / P1-22: flush EVERY store's redo log before syncing the
        // data device — not just store 0. This runs post-drain (after
        // `self.inner.run()` returned and the background flusher joined above),
        // so any buffered mutation acked during the connection-drain window is
        // captured here. `self.redo_log` is only store 0's handle
        // (server.rs:1190-1195); flushing it alone silently dropped drain-window
        // acks routed to stores 1..N on a clean shutdown, contradicting the
        // "graceful stop loses nothing" contract. `flush_all_redo` fsyncs every
        // per-store committer.
        match self.engine.flush_all_redo() {
            Ok(()) => tracing::info!("all redo logs flushed"),
            Err(e) => tracing::warn!(err = %e, "redo log flush failed"),
        }

        // Sync device
        if let Err(e) = self.device.sync() {
            tracing::warn!(err = %e, "device sync error");
        } else {
            tracing::info!("device synced");
        }
        tracing::info!("state persisted");

        // Flush the OTLP span pipeline last. Any later span would arrive
        // after the provider shuts down and be silently dropped.
        if let Some(ref provider) = self.otlp_provider {
            teraslab::observability::shutdown(provider, std::time::Duration::from_secs(5));
        }

        result
    }

    /// Join a background thread with a wall-clock timeout. If the thread
    /// has not exited by the deadline, log a warning and leak the handle.
    /// Used by the shutdown path so a stuck task does not hold up the
    /// rest of persistence forever. See F-G10-022.
    fn join_with_timeout(
        name: &'static str,
        handle: Option<std::thread::JoinHandle<()>>,
        timeout: std::time::Duration,
    ) {
        let Some(handle) = handle else {
            return;
        };
        // `JoinHandle` has no built-in timeout, so delegate to a helper
        // thread that signals on completion. The helper joins the real
        // task; we wait on a channel with the deadline.
        let (tx, rx) = std::sync::mpsc::channel();
        let joiner = std::thread::spawn(move || {
            let _ = handle.join();
            let _ = tx.send(());
        });
        match rx.recv_timeout(timeout) {
            Ok(()) => {
                let _ = joiner.join();
                tracing::info!(task = name, "background task joined");
            }
            Err(_) => {
                tracing::warn!(
                    task = name,
                    timeout_ms = timeout.as_millis() as u64,
                    "background task did not exit within timeout — leaving handle to be \
                     reaped on process exit",
                );
            }
        }
    }
}

/// Register a SIGINT + SIGTERM handler that fires the given closure on the
/// first signal observed.
///
/// Pre-fix this function was a stub that immediately dropped `handler`, so
/// the daemon had no graceful-shutdown signal path at all: `kill -TERM` /
/// Ctrl-C hard-killed the process and the cleanup chain (cluster stop,
/// snapshot, allocator persist, replication-intent flush, device.sync,
/// OTLP flush) never ran. The `ctrlc` crate registers a single forwarding
/// handler on both SIGINT and SIGTERM (with the `termination` feature) and
/// runs the closure on a dedicated handler thread; calling it twice in the
/// same process is a programmer error and panics, so the binary may only
/// register one handler.
///
/// See F-G10-001 in the audit.
fn ctrlc_handler<F: Fn() + Send + 'static>(handler: F) {
    if let Err(e) = ctrlc::set_handler(handler) {
        // A duplicate registration (`ctrlc::Error::MultipleHandlers`) is
        // the only realistic failure mode at this point. Log and continue
        // — failing the daemon over a signal-handler diagnostic would be
        // worse than not having graceful shutdown.
        tracing::error!(err = %e, "failed to install SIGINT/SIGTERM handler — graceful shutdown disabled");
    }
}

/// Fallback `tracing` subscriber used ONLY on the early error paths before
/// the observability config has been validated.
///
/// This is a no-frills JSON fmt-layer registry — identical behavior to the
/// Phase 3 default. Normal startup installs the Phase 4 subscriber via
/// [`teraslab::observability::init_subscriber`] which composes the same
/// fmt layer with an optional OTLP exporter.
fn init_tracing_subscriber_fallback() {
    use tracing_subscriber::{EnvFilter, Registry, fmt, prelude::*};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = fmt::Layer::new()
        .json()
        .with_current_span(true)
        .with_span_list(false);
    let subscriber = Registry::default().with(filter).with(fmt_layer);
    // Best-effort: if a subscriber was already installed (e.g. by a test
    // harness in the same process), we simply keep the existing one.
    let _ = tracing::subscriber::set_global_default(subscriber);
}

// F-G10-016: the previous in-module test asserted startup ordering by
// grepping the source file at compile time, which silently broke any time
// the recovery block was refactored. Runtime coverage of the same
// invariant ("recovery completes before any listener accepts") lives in
// `tests/g10_lifecycle.rs`, where a slow-recovery fault-injection point
// proves no TCP/HTTP socket can answer during the recovery window.

#[cfg(test)]
mod tests {
    use super::{buffered_loss_window_applies, is_redo_pressure};
    use teraslab::redo::RedoError;

    /// The intent-recovery startup barrier downgrades `RedoError::LogFull`
    /// (transient redo backpressure, re-drivable from source) to the retry
    /// path. The discriminator must recognise the flattened `Display`
    /// string of the `LogFull` variant.
    #[test]
    fn log_full_is_redo_pressure_routes_to_retry() {
        // The recovery path flattens RedoError -> String, so feed the
        // canonical Display the same way the barrier would observe it.
        let log_full = RedoError::LogFull {
            used: 64 * 1024 * 1024,
            capacity: 64 * 1024 * 1024,
        }
        .to_string();
        assert!(
            is_redo_pressure(&log_full),
            "LogFull must route to the retry path, not terminal exit; got {log_full:?}",
        );

        // A wrapped form (intent-recovery sometimes prefixes context) must
        // still be detected via substring.
        let wrapped = format!("replication intent re-replication: {log_full}");
        assert!(
            is_redo_pressure(&wrapped),
            "wrapped LogFull must still route to retry; got {wrapped:?}",
        );
    }

    /// Genuine device/IO faults must NOT be downgraded — they stay on the
    /// terminal abort arm so a real fault is not retried forever.
    #[test]
    fn device_and_poison_faults_stay_terminal() {
        let poisoned = RedoError::Poisoned.to_string();
        assert!(
            !is_redo_pressure(&poisoned),
            "Poisoned is a terminal fault, must not route to retry; got {poisoned:?}",
        );

        let checksum = RedoError::ChecksumMismatch { offset: 4096 }.to_string();
        assert!(
            !is_redo_pressure(&checksum),
            "checksum mismatch is terminal, must not route to retry; got {checksum:?}",
        );

        // A bare device I/O message (no "redo log full" substring) is terminal.
        let io = "read redo for pending replication intent failed".to_string();
        assert!(
            !is_redo_pressure(&io),
            "device I/O failure is terminal, must not route to retry; got {io:?}",
        );
    }

    /// Single-node (RF=1) buffered durability still has a flush-interval
    /// loss window — the boot warning must fire.
    #[test]
    fn buffered_loss_window_applies_rf1_warns() {
        assert!(
            buffered_loss_window_applies(1),
            "RF=1 buffered mode must still warn about the flush-interval loss window",
        );
    }

    /// RF>1 buffered durability is protected by C-1's concurrent
    /// local-fsync-before-ack — the flush-interval loss window does not
    /// exist there, so the boot warning must NOT fire.
    #[test]
    fn buffered_loss_window_applies_rf_gt_1_no_warn() {
        assert!(
            !buffered_loss_window_applies(2),
            "RF>1 buffered mode must not warn — C1 removes the loss window",
        );
        assert!(
            !buffered_loss_window_applies(5),
            "RF>1 buffered mode must not warn — C1 removes the loss window",
        );
    }
}
