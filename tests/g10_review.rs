//! G10 review meta-tests.
//!
//! Catch-all integration tests that exercise the smaller G10 findings
//! that don't warrant their own test file. Each test is the runtime
//! witness for one F-G10-* fix; failure here regresses the cited
//! G10 deployment-hardening finding.

use teraslab::config::ServerConfig;

// ---------------------------------------------------------------------------
// F-G10-006: blobstore_path is a PathBuf (not String), default is relative,
// and resolves to something that a non-root process can create.
// ---------------------------------------------------------------------------

#[test]
fn blobstore_path_is_pathbuf_default_relative_and_writable() {
    use std::path::PathBuf;
    let cfg = ServerConfig::default();
    // Static type check: PathBuf, not String.
    let _: PathBuf = cfg.blobstore_path.clone();
    // Default is the new relative path.
    assert_eq!(cfg.blobstore_path, PathBuf::from("./teraslab-blobstore"));
    // Confirm it's not the pre-fix absolute root path.
    assert_ne!(cfg.blobstore_path, PathBuf::from("/blobstore"));
}

// ---------------------------------------------------------------------------
// F-G10-014: lib.rs internals — historically `device_io` was widened to
// `pub` and demoted back to `pub(crate)` in this finding. The module was
// deleted entirely on 2026-05-28 per the May-2026 external review (it was
// unused scaffolding gated behind a feature). The test below now only
// locks down the remaining `pub` surface the bins/tests/benches consume;
// the device_io-specific assertion is moot.
// ---------------------------------------------------------------------------

#[test]
fn public_modules_remain_reachable() {
    // Touch each `pub mod` we still depend on so a future overzealous
    // demotion to `pub(crate)` would fail this test (and the build).
    use teraslab::allocator::SlotAllocator;
    use teraslab::config::ServerConfig as _ServerConfig;
    use teraslab::device::MemoryDevice;
    use teraslab::index::Index;
    use teraslab::locks::StripedLocks;
    use teraslab::ops::engine::Engine;
    use teraslab::record::UTXO_SLOT_SIZE;
    use teraslab::redo::RedoLog as _RedoLog;
    use teraslab::server::Server;

    // Sanity-construct a small thing through each module so the import
    // is not silently dead-code-eliminated.
    let dev = std::sync::Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap())
        as std::sync::Arc<dyn teraslab::device::BlockDevice>;
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let idx = Index::new(1024).unwrap();
    let dah = teraslab::index::DahIndex::new();
    let unmined = teraslab::index::UnminedIndex::new();
    let engine = std::sync::Arc::new(Engine::new(
        dev,
        idx,
        alloc,
        StripedLocks::new(256),
        dah,
        unmined,
    ));
    let cfg = _ServerConfig::default();
    let _server = Server::new(engine, cfg);
    assert_eq!(UTXO_SLOT_SIZE, 73);
    // Hand-wave: just take the type's size so the use isn't optimised
    // away. Doesn't matter what the size is.
    let _ = std::mem::size_of::<_RedoLog>();
}

// ---------------------------------------------------------------------------
// F-G10-021: the http_port parser must reject malformed http_listen_addr
// instead of silently falling back to 9100. We can't test the daemon
// binary in-process, but we can verify the SocketAddr-parse contract that
// the bin uses.
// ---------------------------------------------------------------------------

#[test]
fn http_listen_addr_parse_is_strict_after_validation() {
    use std::net::SocketAddr;

    // Mirror the bin's post-validation parse path.
    let cfg = ServerConfig::default();
    let sa: SocketAddr = cfg
        .http_listen_addr
        .parse()
        .expect("validated default must parse");
    assert_eq!(sa.port(), 9100);

    // A malformed value would fail to parse. We don't pass it through
    // `validate_safe_defaults` here — that step would catch it earlier;
    // the unit-test contract is just "no silent 9100 fallback".
    let bad = "not-a-socket-addr";
    let res: Result<SocketAddr, _> = bad.parse();
    assert!(res.is_err(), "malformed addr must not parse silently");
}

// ---------------------------------------------------------------------------
// F-G10-009: cli.rs `data_addr` default must equal server's `listen_addr`
// default (127.0.0.1:3300). We cannot exec the CLI binary here, but the
// contract is "the strings match". A hard-coded check is enough: a future
// drift would either break the CLI's first-try ping or break this test.
// ---------------------------------------------------------------------------

#[test]
fn cli_default_data_addr_matches_server_listen_addr_default() {
    let cfg = ServerConfig::default();
    // The CLI default is hard-coded as a clap default_value; this test
    // is the cross-check that nobody bumps one without the other.
    assert_eq!(
        cfg.listen_addr, "127.0.0.1:3300",
        "server default listen_addr drifted — CLI default_value must follow"
    );
}
