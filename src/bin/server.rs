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
use teraslab::allocator::SlotAllocator;
use teraslab::config::IndexBackendMode;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, DirectDevice};
use teraslab::index::{DahBackend, PrimaryBackend, UnminedBackend};
use teraslab::locks::StripedLocks;
use teraslab::metrics::{
    AllocatorMetrics, MigrationMetrics, RedoMetrics, ReplicationMetrics,
    SwimMetrics, ThreadHistograms, ThreadMetrics,
};
use teraslab::ops::engine::Engine;
use teraslab::redo::RedoLog;
use teraslab::server::Server;
use teraslab::server::dispatch::{SecondaryStatus, set_secondary_status};
use teraslab::server::http::{HttpState, start_http_server};
use teraslab::server::startup::{
    SecondaryLoadOutcome, check_replay_tolerance_with_cap, fallback_dah_index,
    fallback_unmined_index, load_primary_index_file_backed, load_primary_index_in_memory,
    load_primary_index_redb, open_mandatory_redo_log, rebuild_in_memory_secondaries,
    secondaries_from_pair,
};
use teraslab::storage::blobstore::{BlobStore, FileBlobStore};

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

    let mut config = if args.len() > 1 && args[1] == "--config" {
        if args.len() < 3 {
            // CLI usage message goes to stderr before the subscriber is
            // effectively useful — keep it as a direct stderr write so
            // operators always see it on bad invocation.
            #[allow(clippy::disallowed_macros)]
            {
                eprintln!("Usage: teraslab-server --config <path.toml> [--strict-auth]");
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
             `cluster_secret` to restore the production-safe default.",
        );
    }

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
                                    dah: DahBackend::from(teraslab::index::DahIndex::new()),
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
        // F-G10-012: the previous field name `load_factor` was a labelling
        // bug — the value already multiplies the unitless 0..1 ratio by
        // 100, so it's a percentage. Renamed to `load_factor_pct` so
        // dashboards / alerts read the right unit.
        load_factor_pct = index.stats().load_factor * 100.0,
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
    let mut redo_log: Option<RedoLog> = Some(redo_log);

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
    // allocator so freelist mutations between snapshots are not lost.
    let mut allocator = allocator;
    let mut pending_conflicting_children = Vec::new();
    if let Some(ref mut redo) = redo_log {
        match teraslab::recovery::recover_all_with_allocator_collecting_pending_conflicts_progress(
            &*device,
            redo,
            &mut index,
            &mut dah_index,
            &mut unmined_index,
            Some(&mut allocator),
        ) {
            Ok((stats, pending)) => {
                pending_conflicting_children = pending;
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
        match teraslab::recovery::reconcile_blobs_after_recovery(blob_store.as_ref(), &index) {
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
            if let Err(e) = engine.append_conflicting_child(&pending.parent_key, pending.child_txid)
            {
                tracing::error!(
                    parent_key = ?pending.parent_key,
                    child_txid = ?pending.child_txid,
                    err = %e,
                    "recovery: failed to drain conflicting-child append intent; aborting startup",
                );
                std::process::exit(1);
            }
        }
        tracing::info!(
            drained = pending_conflicting_children.len(),
            "recovery: drained pending conflicting-child append intents",
        );
    }

    // Attach the redo log so the engine performs two-phase durability for
    // secondary index updates (redo fsync BEFORE redb commit).
    if let Some(ref log) = redo_log {
        engine.set_redo_log(log.clone());
    }

    // 4b. Attach the (already-constructed) blobstore to the engine. The
    // store was built ahead of recovery so the orphan-blob reconciliation
    // could run against the freshly-replayed primary index — see R-049.
    engine.set_blob_store(blob_store.clone());

    let engine = Arc::new(engine);

    // 5. Start cluster if configured
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
        // Load topology state (backward-compatible with old format).
        let topo_state =
            teraslab::cluster::coordinator::load_startup_topology_state(&cluster_state_path);
        let initial_peak = topo_state.peak_cluster_size as usize;
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
            migration_pool_size: config.migration_pool_size,
            migration_batch_size: config.migration_batch_size,
            persisted_incarnation: topo_state.incarnation,
            cluster_id: resolved_cluster_id,
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
                        10_000,
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
                            // full-shard backfill. The match is on the
                            // typed `CatchupError::RedoReclaimed` variant
                            // (was previously a `String::contains` on the
                            // rendered message — F-G10-017 / B-4).
                            if let teraslab::replication::durable::CatchupError::RedoReclaimed {
                                ..
                            } = e
                            {
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
        replica_lag_warn_threshold_ops: config.replica_lag_warn_threshold_ops,
    });
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
    let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // F-G10-002: the bin's `shutdown_flag` only drives the background
    // tasks (checkpoint / blob_gc / lag_monitor). The `Server::run` accept
    // loop polls its own private flag — we flip that one via the public
    // `Server::shutdown()` method from the signal handler below, AFTER we
    // wrap `Server` in `Arc` so the handler closure can hold a reference.
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
            teraslab::checkpoint::CheckpointConfig::new(config.index_snapshot_path.clone());
        // BC-01: honour operator-configured hysteresis band and poll
        // cadence rather than the library defaults. Config validation
        // guarantees `0 < low_water < high_water < 1` and
        // `poll_interval_ms > 0`, so the values below are safe to plug
        // in unchecked.
        cfg.high_water = config.checkpoint_high_water;
        cfg.low_water = config.checkpoint_low_water;
        cfg.poll_interval =
            std::time::Duration::from_millis(config.checkpoint_poll_interval_ms);
        if let Some(tracker) = teraslab::server::dispatch::ack_tracker_handle() {
            let reset_guard: std::sync::Arc<dyn Fn(u64) -> bool + Send + Sync + 'static> =
                std::sync::Arc::new(move |floor_sequence| {
                    let all = tracker.all_acked();
                    let min_acked = all.values().copied().min().unwrap_or(floor_sequence);
                    let can_reset = min_acked >= floor_sequence;
                    if !can_reset {
                        tracing::warn!(
                            floor_sequence,
                            min_acked,
                            replicas = all.len(),
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

    // R-049: spawn the periodic orphan-blob GC sweep. Recovery already
    // reconciled the blob store against the freshly-replayed primary index
    // on startup; this task takes care of orphans that accumulate during
    // normal operation (failed creates whose registration was rejected,
    // aborted streaming uploads, migrations cancelled mid-flight). The
    // tick interval defaults to one hour and can be set to 0 to disable
    // the periodic sweep entirely (recovery-time reconciliation still runs).
    let blob_gc_handle: Option<std::thread::JoinHandle<()>> = if config.blob_gc_interval_secs > 0 {
        let cfg = teraslab::storage::blob_gc::BlobGcConfig::new(config.blob_gc_interval_secs);
        Some(teraslab::storage::blob_gc::spawn_blob_gc_task(
            cfg,
            blob_store.clone(),
            engine.clone(),
            shutdown_flag.clone(),
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
                    Some(teraslab::replication::durable::spawn_lag_monitor(
                        tracker,
                        current_seq_fn,
                        shutdown_flag.clone(),
                        config.replica_lag_check_interval_secs,
                        config.replica_lag_warn_threshold_ops,
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
        snap_path: config.index_snapshot_path.clone(),
        device,
        cluster,
        otlp_provider,
        // F-G10-003: hold the redo log so we can flush it on shutdown
        // before `device.sync()`. Defense-in-depth: per-op fsync is the
        // primary durability guarantee; this just ensures any tail buffer
        // is on disk before we tear down.
        redo_log: redo_log.clone(),
        // F-G10-022: take ownership of background-thread join handles so
        // `run()` can join them after the shutdown flag is set but before
        // `device.sync()`. Pre-fix these were `_`-prefixed bindings that
        // dropped at end-of-scope, leaving threads potentially mid-fsync
        // while the foreground unwind raced ahead.
        checkpoint_handle: Mutex::new(checkpoint_handle),
        blob_gc_handle: Mutex::new(blob_gc_handle),
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
    /// Redo log handle, held so `run()` can flush it on shutdown ahead of
    /// `device.sync()`. See F-G10-003.
    redo_log: Option<Arc<Mutex<RedoLog>>>,
    /// Join handle for the redo-log checkpoint thread. See F-G10-022.
    checkpoint_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Join handle for the periodic blob-GC sweep thread. See F-G10-022.
    blob_gc_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
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

        match teraslab::server::dispatch::flush_replication_intent_tracker() {
            Ok(()) => tracing::info!("replication intent tracker flushed"),
            Err(e) => tracing::warn!(err = %e, "replication intent tracker flush failed"),
        }

        // F-G10-003: flush the redo log before syncing the data device.
        // Per-op fsync in the hot path is the primary durability guarantee;
        // this just makes sure the tail buffer (if any) is on disk before
        // the next-restart redo scan reads it.
        if let Some(ref log) = self.redo_log {
            match log.lock().flush() {
                Ok(()) => tracing::info!("redo log flushed"),
                Err(e) => tracing::warn!(err = %e, "redo log flush failed"),
            }
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
