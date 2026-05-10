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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8};

use parking_lot::Mutex;
use teraslab::allocator::SlotAllocator;
use teraslab::config::IndexBackendMode;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, DirectDevice};
use teraslab::index::{DahBackend, PrimaryBackend, UnminedBackend};
use teraslab::locks::StripedLocks;
use teraslab::metrics::{
    AllocatorMetrics, IoUringMetrics, MigrationMetrics, RedoMetrics, ReplicationMetrics,
    SwimMetrics, ThreadHistograms, ThreadMetrics,
};
use teraslab::ops::engine::Engine;
use teraslab::redo::RedoLog;
use teraslab::server::Server;
use teraslab::server::dispatch::{SecondaryStatus, set_secondary_status};
use teraslab::server::http::{HttpState, start_http_server};
use teraslab::server::startup::{
    SecondaryLoadOutcome, check_replay_tolerance, fallback_dah_index, fallback_unmined_index,
    load_primary_index_file_backed, load_primary_index_in_memory, load_primary_index_redb,
    open_mandatory_redo_log, rebuild_in_memory_secondaries, secondaries_from_pair,
};
use teraslab::storage::blobstore::{BlobStore, FileBlobStore};

/// Detect the first non-loopback IPv4 address on this host.
/// Used when `listen_addr` is `0.0.0.0` and no `advertise_addr` is configured,
/// so the node can advertise a reachable IP to cluster peers.
fn detect_local_ip() -> Option<std::net::IpAddr> {
    // Connect a UDP socket to a public IP to discover our default route address.
    // No traffic is sent — this just causes the OS to select the outgoing interface.
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:53").ok()?;
    socket.local_addr().ok().map(|a| a.ip())
}

/// Global metrics counters for the server binary.
static SERVER_METRICS: ThreadMetrics = ThreadMetrics::new();

/// Global latency histograms for the server binary.
static SERVER_HISTOGRAMS: ThreadHistograms = ThreadHistograms::new();

/// Replication subsystem metrics (Phase 5).
static REPLICATION_METRICS: ReplicationMetrics = ReplicationMetrics::new();

/// io_uring backend metrics (Phase 5).
static IO_URING_METRICS: IoUringMetrics = IoUringMetrics::new();

/// Redo log metrics (Phase 5).
static REDO_METRICS: RedoMetrics = RedoMetrics::new();

/// Shard migration metrics (Phase 5).
static MIGRATION_METRICS: MigrationMetrics = MigrationMetrics::new();

/// SWIM failure-detector metrics (Phase 5).
static SWIM_METRICS: SwimMetrics = SwimMetrics::new();

/// Device-space allocator metrics (Phase 5).
static ALLOCATOR_METRICS: AllocatorMetrics = AllocatorMetrics::new();

fn main() {
    // Parse config first so the observability section can drive the
    // subscriber (OTLP endpoint, sampling ratio, service name).
    let args: Vec<String> = std::env::args().collect();

    let mut config = if args.len() > 1 && args[1] == "--config" {
        if args.len() < 3 {
            // CLI usage message goes to stderr before the subscriber is
            // effectively useful — keep it as a direct stderr write so
            // operators always see it on bad invocation.
            #[allow(clippy::disallowed_macros)]
            {
                eprintln!("Usage: teraslab-server --config <path.toml>");
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
    teraslab::metrics::init_io_uring_metrics(&IO_URING_METRICS);
    teraslab::metrics::init_redo_metrics(&REDO_METRICS);
    teraslab::metrics::init_migration_metrics(&MIGRATION_METRICS);
    teraslab::metrics::init_swim_metrics(&SWIM_METRICS);
    teraslab::metrics::init_allocator_metrics(&ALLOCATOR_METRICS);

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

    // 1. Open device
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

    // Validate device_id config format before using it.
    if let Err(e) = config.validate_device_id() {
        tracing::error!(err = %e, "FATAL: invalid config");
        std::process::exit(1);
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

    // 2. Recover or create allocator
    let allocator = match SlotAllocator::recover(device.clone()) {
        Ok(alloc) => {
            tracing::info!("allocator recovered from device header");
            let device_id_hex = alloc.device_id_hex();
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

            alloc
        }
        Err(_) => match SlotAllocator::new(device.clone()) {
            Ok(fresh) => {
                let device_id_hex = fresh.device_id_hex();
                tracing::info!("allocator: fresh (no persisted state found)");
                tracing::info!(device_id = %device_id_hex, "device identity (copy to config device_id to enable verification)");
                fresh
            }
            Err(e) => {
                tracing::error!(err = %e, "failed to create allocator");
                std::process::exit(1);
            }
        },
    };

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
    let load_outcome = if config.index.backend == IndexBackendMode::Redb {
        // ReDB on-disk backend
        let primary = match load_primary_index_redb(&config.index, &*device, &allocator) {
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
        let (unmined, unmined_ok) = match teraslab::index::redb_unmined::RedbUnminedIndex::open(
            &config.index.redb_unmined_path,
            config.index.redb_cache_size,
        ) {
            Ok(idx) => (UnminedBackend::OnDisk(idx), true),
            Err(e) => (fallback_unmined_index("unmined", e), false),
        };
        (
            primary,
            SecondaryLoadOutcome {
                dah,
                unmined,
                status: SecondaryStatus { dah_ok, unmined_ok },
            },
        )
    } else if config.index.backend == IndexBackendMode::FileBacked {
        // File-backed mmap backend
        let fb_path = &config.index.file_backed_path;
        let primary = match load_primary_index_file_backed(
            fb_path,
            config.expected_records,
            &*device,
            &allocator,
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
        let secondaries = rebuild_in_memory_secondaries(&*device, &allocator);
        (primary, secondaries)
    } else {
        // In-memory backend (default)
        let snap_path = &config.index_snapshot_path;
        if snap_path.exists() {
            match PrimaryBackend::restore_all(snap_path) {
                Ok((idx, dah, unmined, flags)) => {
                    tracing::info!(entries = idx.len(), "index restored from snapshot");
                    let secondaries = if flags.dah_needs_rebuild && flags.unmined_needs_rebuild {
                        tracing::warn!("both secondary indexes need rebuild (snapshot corrupt)");
                        rebuild_in_memory_secondaries(&*device, &allocator)
                    } else if flags.dah_needs_rebuild {
                        tracing::warn!("DAH index needs rebuild (snapshot corrupt)");
                        // Preserve the intact unmined from the snapshot;
                        // rebuild only DAH from the device scan. Failure
                        // marks DAH as degraded but keeps unmined healthy.
                        match teraslab::index::PrimaryBackend::rebuild_secondary(
                            &*device, &allocator,
                        ) {
                            Ok((rebuilt_dah, _)) => SecondaryLoadOutcome {
                                dah: DahBackend::from(rebuilt_dah),
                                unmined: UnminedBackend::from(unmined),
                                status: SecondaryStatus {
                                    dah_ok: true,
                                    unmined_ok: true,
                                },
                            },
                            Err(e) => {
                                tracing::error!(
                                    err = %e,
                                    "DAH rebuild failed — degraded readiness",
                                );
                                SecondaryLoadOutcome {
                                    dah: DahBackend::from(
                                        teraslab::index::DahIndex::new(),
                                    ),
                                    unmined: UnminedBackend::from(unmined),
                                    status: SecondaryStatus {
                                        dah_ok: false,
                                        unmined_ok: true,
                                    },
                                }
                            }
                        }
                    } else if flags.unmined_needs_rebuild {
                        tracing::warn!("unmined index needs rebuild (snapshot corrupt)");
                        match teraslab::index::PrimaryBackend::rebuild_secondary(
                            &*device, &allocator,
                        ) {
                            Ok((_, rebuilt_unmined)) => SecondaryLoadOutcome {
                                dah: DahBackend::from(dah),
                                unmined: UnminedBackend::from(rebuilt_unmined),
                                status: SecondaryStatus {
                                    dah_ok: true,
                                    unmined_ok: true,
                                },
                            },
                            Err(e) => {
                                tracing::error!(
                                    err = %e,
                                    "unmined rebuild failed — degraded readiness",
                                );
                                SecondaryLoadOutcome {
                                    dah: DahBackend::from(dah),
                                    unmined: UnminedBackend::from(
                                        teraslab::index::UnminedIndex::new(),
                                    ),
                                    status: SecondaryStatus {
                                        dah_ok: true,
                                        unmined_ok: false,
                                    },
                                }
                            }
                        }
                    } else {
                        secondaries_from_pair(dah, unmined)
                    };
                    (idx, secondaries)
                }
                Err(e) => {
                    tracing::warn!(err = %e, "index snapshot corrupt, rebuilding from device");
                    let primary = match load_primary_index_in_memory(&*device, &allocator) {
                        Ok(idx) => idx,
                        Err(e) => {
                            tracing::error!(err = %e, "FATAL: primary index rebuild failed");
                            std::process::exit(1);
                        }
                    };
                    let secondaries = rebuild_in_memory_secondaries(&*device, &allocator);
                    (primary, secondaries)
                }
            }
        } else {
            tracing::info!("no index snapshot found, rebuilding from device");
            let primary = match load_primary_index_in_memory(&*device, &allocator) {
                Ok(idx) => idx,
                Err(e) => {
                    tracing::error!(err = %e, "FATAL: primary index rebuild failed");
                    std::process::exit(1);
                }
            };
            let secondaries = rebuild_in_memory_secondaries(&*device, &allocator);
            (primary, secondaries)
        }
    };
    let (mut index, secondary_outcome) = load_outcome;
    let SecondaryLoadOutcome {
        dah: mut dah_index,
        unmined: mut unmined_index,
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
    if !secondary_status.unmined_ok {
        tracing::warn!(
            "secondary readiness: unmined index unavailable — dependent endpoints \
             will reject with ERR_INDEX_DEGRADED",
        );
    }

    tracing::info!(
        entries = index.len(),
        load_factor = index.stats().load_factor * 100.0,
        "index loaded",
    );
    tracing::info!(entries = dah_index.len(), "DAH index loaded");
    tracing::info!(entries = unmined_index.len(), "unmined index loaded");

    // 3b. Open redo log device (separate file) and run recovery.
    //
    // Gap #2 (TERANODE_PRODUCTION_READINESS_GAPS.md): the redo log is
    // mandatory. We MUST NOT fall back to an in-memory device when the
    // configured path cannot be opened — that would make every WAL-fsync
    // ack a lie (the bytes are in volatile memory and disappear at
    // shutdown). On open or create failure we fail closed so the operator
    // can fix permissions / disk / path and try again.
    let redo_log_path = config.resolved_redo_log_path();
    let (redo_log_device, redo_log) = match open_mandatory_redo_log(
        &redo_log_path,
        config.redo_log_size,
        config.device_alignment,
    ) {
        Ok(parts) => parts,
        Err(e) => {
            tracing::error!(
                path = %redo_log_path.display(),
                err = %e,
                "FATAL: redo log unavailable — cannot start with mandatory WAL disabled",
            );
            std::process::exit(1);
        }
    };
    tracing::info!(
        path = %redo_log_path.display(),
        size_mib = config.redo_log_size / (1024 * 1024),
        "redo log opened (mandatory)",
    );
    // Keep the device handle alive for the lifetime of the process so
    // any future redo-log replay/extension paths share the same fd.
    let _redo_log_device: Arc<dyn BlockDevice> = redo_log_device;
    let redo_log: Option<RedoLog> = Some(redo_log);

    // Run recovery if we have a redo log, while indexes are still mutable.
    // Uses `recover_all_with_allocator` so the two-phase secondary
    // durability intent records (RedoOp::SecondaryUnminedUpdate /
    // SecondaryDahUpdate) reconcile the on-disk redb secondary indexes AND
    // RedoOp::AllocateRegion / FreeRegion entries replay into the rebuilt
    // allocator so freelist mutations between snapshots are not lost.
    let mut allocator = allocator;
    if let Some(ref redo) = redo_log {
        match teraslab::recovery::recover_all_with_allocator(
            &*device,
            redo,
            &mut index,
            &mut dah_index,
            &mut unmined_index,
            Some(&mut allocator),
        ) {
            Ok(stats) => {
                tracing::info!(
                    replayed = stats.entries_replayed,
                    skipped = stats.entries_skipped,
                    failed = stats.entries_failed,
                    failed_missing_primary = stats.failed_missing_primary,
                    failed_io = stats.failed_io,
                    failed_corrupt = stats.failed_corrupt,
                    failed_logic = stats.failed_logic,
                    failed_missing_record_bytes = stats.failed_missing_record_bytes,
                    "recovery complete",
                );
                // Gap #5 (TERANODE_PRODUCTION_READINESS_GAPS.md): replace
                // the previous blanket `MAX_TOLERATED_FAILURES = 32` with
                // per-cause classification. `MissingPrimary` is benign
                // during idempotent replay and tolerated up to a high cap.
                // Any I/O / corrupt-entry / logic-error failure is fatal
                // regardless of count: those are storage-level corruption
                // signals that must not be papered over.
                if let Err(msg) = check_replay_tolerance(&stats) {
                    tracing::error!(
                        failed_missing_primary = stats.failed_missing_primary,
                        failed_io = stats.failed_io,
                        failed_corrupt = stats.failed_corrupt,
                        failed_logic = stats.failed_logic,
                        failed_missing_record_bytes = stats.failed_missing_record_bytes,
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
    }

    // Wrap redo log in Arc<Mutex> for shared access from dispatch threads
    let redo_log: Option<Arc<Mutex<RedoLog>>> = redo_log.map(|log| Arc::new(Mutex::new(log)));

    // Attach the redo log to the allocator BEFORE moving it into the engine,
    // so all subsequent allocate/free operations are journaled and fsynced
    // before the caller observes their effect. This closes the crash window
    // between `persist()` snapshots.
    if let Some(ref log) = redo_log {
        allocator.set_redo_log(log.clone());
    }

    // Attach the redo log to the primary index so file-backed hash table
    // resizes are crash-atomic (Begin/Commit journaling + parent-dir fsync).
    // The FileBacked variant actually uses the redo log; InMemory / OnDisk
    // accept the attachment but treat it as a no-op.
    if let Some(ref log) = redo_log {
        index.set_redo_log(log.clone());
    }

    // 4. Create engine
    let locks = StripedLocks::new(config.lock_stripes);
    let mut engine = Engine::new(
        device.clone(),
        index,
        allocator,
        locks,
        dah_index,
        unmined_index,
    );

    // Attach the redo log so the engine performs two-phase durability for
    // secondary index updates (redo fsync BEFORE redb commit).
    if let Some(ref log) = redo_log {
        engine.set_redo_log(log.clone());
    }

    // 4b. Initialize blobstore from config and attach to engine
    let blob_store: Arc<dyn BlobStore> =
        Arc::new(FileBlobStore::new(Path::new(&config.blobstore_path), 2));
    engine.set_blob_store(blob_store.clone());
    tracing::info!(path = %config.blobstore_path, "blobstore configured");

    let engine = Arc::new(engine);

    // 5. Start cluster if configured
    let cluster = if config.is_clustered() {
        use teraslab::cluster::coordinator::{ClusterConfig, ClusterCoordinator};
        use teraslab::cluster::shards::NodeId;

        let bind_addr: std::net::SocketAddr =
            config.listen_addr.parse().expect("invalid listen_addr");
        // Determine the address to advertise to other nodes.
        // If advertise_addr is set, use it. Otherwise, if listen_addr uses
        // 0.0.0.0 (common in Docker), auto-detect a non-loopback IP.
        let self_addr: std::net::SocketAddr = if let Some(ref adv) = config.advertise_addr {
            adv.parse().expect("invalid advertise_addr")
        } else if bind_addr.ip().is_unspecified() {
            let ip = detect_local_ip().unwrap_or(bind_addr.ip());
            std::net::SocketAddr::new(ip, bind_addr.port())
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
        // Load topology state (backward-compatible with old format).
        let topo_state = teraslab::cluster::coordinator::load_topology_state(&cluster_state_path);
        let initial_peak = topo_state.peak_cluster_size as usize;
        let initial_epoch = topo_state.committed_term;

        let cluster_config = ClusterConfig {
            self_id: NodeId(config.node_id),
            self_addr,
            swim_bind,
            seed_nodes: seed_addrs,
            replication_factor: config.replication_factor,
            probe_interval,
            suspicion_timeout: std::time::Duration::from_millis(config.swim_suspicion_timeout_ms),
            cluster_secret: config
                .cluster_secret
                .as_ref()
                .map(|s| s.as_bytes().to_vec()),
            max_migration_threads: config.max_migration_threads,
            topology_propose_timeout: probe_interval * 3,
            migration_pool_size: config.migration_pool_size,
            migration_batch_size: config.migration_batch_size,
            persisted_incarnation: topo_state.incarnation,
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
        if initial_epoch > 0 {
            coordinator.shard_table.write().version = initial_epoch;
        }
        let running = coordinator.start(
            engine.clone(),
            Some(cluster_state_path),
            redo_log.clone(),
            config.resolved_ack_policy(),
            config.is_replication_best_effort(),
            std::time::Duration::from_millis(config.replication_timeout_ms.max(1)),
        );
        // Restore migration state from a previous run so shards that were
        // mid-migration remain blocked (inbound) or tracked (outbound).
        running.restore_inbound_state();
        running.restore_outbound_state();
        // Initialize persistent ACK tracker alongside the cluster state file.
        let ack_path = {
            let mut p = config.resolved_cluster_state_path().into_os_string();
            p.push(".repl-ack");
            std::path::PathBuf::from(p)
        };
        teraslab::server::dispatch::init_ack_tracker(ack_path.clone());
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

        if config.replication_factor > 1 {
            let start = std::time::Instant::now();
            loop {
                match teraslab::server::dispatch::recover_pending_replication_intents(
                    &running,
                    redo_log.as_deref(),
                    &engine,
                ) {
                    Ok(()) => break,
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
        if config.replication_factor > 1 {
            let redo_for_catchup = redo_log.clone();
            let engine_for_catchup = engine.clone();
            // Phase B3: each catch-up batch must carry the master's live
            // topology epoch so the receiver-side ERR_STALE_EPOCH gate
            // accepts it. We clone the shared `Arc<AtomicU64>` so the
            // background thread always reads the current epoch (and not a
            // start-time snapshot that could go stale before the first
            // batch is sent).
            let cluster_key_handle = running.cluster_key_handle();
            // Phase H — `Send`-able handle for posting `ResyncRequest`
            // whenever catchup returns the truncation sentinel ("redo
            // entries reclaimed; full resync required"). The handle
            // wraps a clone of the coordinator's resync channel
            // sender + the node-address map for addr → NodeId
            // resolution; `RunningCluster` itself is not `Clone`.
            let resync_handle = running.resync_sender_handle();
            std::thread::spawn(move || {
                let tracker = teraslab::replication::durable::AckTracker::new(ack_path);
                let all_acked = tracker.all_acked();
                if all_acked.is_empty() {
                    return; // No known replicas yet
                }

                let current_seq = redo_for_catchup
                    .as_ref()
                    .map(|rl| rl.lock().current_sequence())
                    .unwrap_or(0);

                for (addr, last_acked) in &all_acked {
                    if *last_acked >= current_seq {
                        continue; // Already caught up
                    }
                    let lag = current_seq - last_acked;
                    tracing::info!(
                        %addr,
                        lag,
                        from_seq = last_acked + 1,
                        "catchup: replica behind, starting catch-up",
                    );

                    let redo_ref = redo_for_catchup.clone();
                    let eng_ref = engine_for_catchup.clone();

                    // Determine the earliest available redo sequence for
                    // truncation detection. If the redo log has wrapped past
                    // the replica's last-acked position, catch-up is impossible
                    // and the replica needs a full resync.
                    let first_avail_seq = redo_ref
                        .as_ref()
                        .and_then(|rl| rl.lock().earliest_sequence().ok().flatten());

                    let local_cluster_key =
                        cluster_key_handle.load(std::sync::atomic::Ordering::Acquire);

                    let result = teraslab::replication::durable::run_catchup_for_replica(
                        addr,
                        last_acked + 1,
                        current_seq,
                        1000,
                        &|from_seq| {
                            let rl = match redo_ref.as_ref() {
                                Some(rl) => rl,
                                None => return Vec::new(),
                            };
                            let entries = match rl.lock().read_from_sequence(from_seq) {
                                Ok(e) => e,
                                Err(_) => return Vec::new(),
                            };
                            // Convert redo entries to ReplicaOps. Each entry's shard
                            // is derived from its tx_key. The replica applies all
                            // ops idempotently, gracefully skipping unknown records.
                            entries
                                .iter()
                                .filter_map(|e| {
                                    let tx_key = e.op.tx_key()?;
                                    let shard =
                                        teraslab::cluster::shards::ShardTable::shard_for_key(
                                            tx_key,
                                        );
                                    teraslab::cluster::coordinator::redo_entry_to_replica_op(
                                        e, shard, &eng_ref,
                                    )
                                })
                                .collect()
                        },
                        first_avail_seq,
                        local_cluster_key,
                    );

                    match result {
                        Ok(through) => {
                            tracing::info!(%addr, through, "catchup: replica caught up");
                            tracker.record_ack(*addr, through);
                            tracker.flush();
                        }
                        Err(e) => {
                            tracing::warn!(%addr, err = %e, "catchup: replica catch-up failed");
                            // Phase H — when catchup returns the
                            // truncation sentinel, post a resync
                            // request so the coordinator synthesizes a
                            // full-shard backfill. The error string
                            // contract is fixed by `run_catchup_for_replica`.
                            if e.contains("redo entries reclaimed") {
                                let queued = resync_handle.signal_for_addr(addr, Vec::new());
                                if queued {
                                    tracing::info!(
                                        %addr,
                                        "catchup: posted full-shard resync request",
                                    );
                                } else {
                                    tracing::warn!(
                                        %addr,
                                        "catchup: resync request dropped (unknown addr or coordinator stopped)",
                                    );
                                }
                            }
                        }
                    }
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
    let http_port: u16 = config
        .http_listen_addr
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse().ok())
        .unwrap_or(9100);
    let http_state = Arc::new(HttpState {
        engine: engine.clone(),
        metrics: &SERVER_METRICS,
        histograms: &SERVER_HISTOGRAMS,
        ready: Arc::new(AtomicBool::new(true)),
        log_level: Arc::new(AtomicU8::new(2)), // INFO
        cluster: cluster.clone(),
        redo_log: redo_log.clone(),
        active_connections: active_connections.clone(),
        http_port,
    });
    let http_addr = config.http_listen_addr.clone();
    let admin_endpoints_enabled = config.enable_admin_endpoints;
    // R-056: when admin endpoints are on, the bearer token has been validated
    // non-empty by `validate_safe_defaults`. We pass an owned clone into the
    // dedicated HTTP thread; cloning a small `String` is cheap and avoids
    // sharing mutable state with the rest of the server.
    let admin_token = config.admin_token.clone();
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
    server = server.with_blob_store(blob_store);
    let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shutdown_clone = shutdown_flag.clone();
    ctrlc_handler(move || {
        tracing::info!("shutdown signal received");
        shutdown_clone.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    let server = ServerWithShutdown {
        inner: server,
        shutdown: shutdown_flag,
        engine,
        snap_path: config.index_snapshot_path.clone(),
        device,
        cluster,
        otlp_provider,
    };

    // 7. Start serving
    if let Err(e) = server.run() {
        tracing::error!(err = %e, "server error");
        std::process::exit(1);
    }

    tracing::info!("server stopped");
}

struct ServerWithShutdown {
    inner: Server,
    #[allow(dead_code)]
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    engine: Arc<Engine>,
    snap_path: PathBuf,
    device: Arc<dyn BlockDevice>,
    cluster: Option<Arc<teraslab::cluster::coordinator::RunningCluster>>,
    /// OTLP provider, present when `[observability].otlp_endpoint` was
    /// configured. Flushed with a 5 s timeout on graceful shutdown.
    otlp_provider: Option<teraslab::observability::OtelTracerProvider>,
}

impl ServerWithShutdown {
    fn run(&self) -> Result<(), String> {
        let result = self.inner.run();

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
}

fn ctrlc_handler<F: Fn() + Send + 'static>(handler: F) {
    // Unfortunately without a signal crate, we can't easily catch SIGINT.
    // The server's read timeout + shutdown flag handle graceful shutdown.
    // For production, add the `ctrlc` or `signal-hook` crate.
    drop(handler);
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
