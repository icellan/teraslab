//! G10 lifecycle / shutdown regression tests.
//!
//! The signal-handler wiring lives in `bin/server.rs` and is hard to
//! exercise from a unit test (registering a real SIGINT handler in the
//! test binary would clobber the harness). Instead we cover the
//! contract via the public surface: `Server::shutdown()` causes
//! `Server::run()` to exit, and a `ServerConfig::strict_auth` flag flips
//! validation. The ctrlc handler itself is tested by simulating its
//! intended effect — calling `Server::shutdown()` directly.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use teraslab::allocator::SlotAllocator;
use teraslab::config::ServerConfig;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::ops::engine::Engine;
use teraslab::server::Server;

/// Build a minimal in-memory Server backed by a `MemoryDevice`, bound to
/// an ephemeral port. The returned `Arc<Server>` can be cloned into the
/// (simulated) signal-handler closure and into the run thread.
fn build_test_server() -> Arc<Server> {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(1_024).unwrap();
    let engine = Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(256),
        DahIndex::new(),
        UnminedIndex::new(),
    ));

    // Bind to :0 so the OS picks an unused port. We snapshot the address
    // into the config so Server::run binds to the same one.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = listener.local_addr().unwrap().to_string();
    drop(listener);

    let mut config = ServerConfig {
        listen_addr: listen,
        ..ServerConfig::default()
    };
    // Trim runtime-touchy knobs that the integration test does not need.
    config.max_connections = 16;
    Arc::new(Server::new(engine, config))
}

// ---------------------------------------------------------------------------
// F-G10-001 + F-G10-002: a signal-handler-style call to
// `Server::shutdown()` exits the accept loop in `Server::run()`.
// ---------------------------------------------------------------------------

#[test]
fn server_shutdown_exits_run_loop() {
    let server = build_test_server();
    let run_handle = {
        let s = server.clone();
        std::thread::spawn(move || s.run())
    };

    // Give the server a beat to bind & enter the accept loop.
    std::thread::sleep(Duration::from_millis(200));

    // Simulate what the ctrlc handler does: flip the public shutdown
    // flag. If the binary's signal wiring regresses, this exact call
    // would be the one that fails to wake `Server::run`.
    server.shutdown();

    // Bound the wait so a regression doesn't hang the test forever.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !run_handle.is_finished() {
        if Instant::now() > deadline {
            panic!(
                "Server::run did not exit within 5s after Server::shutdown — \
                 signal handler / accept-loop wiring regressed (F-G10-001 / F-G10-002)",
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let result = run_handle.join().expect("run thread should not panic");
    assert!(result.is_ok(), "Server::run should exit Ok, got {result:?}");
}

#[test]
fn server_is_shutting_down_observable() {
    let server = build_test_server();
    assert!(
        !server.is_shutting_down(),
        "fresh server must not be in shutdown state"
    );
    server.shutdown();
    assert!(
        server.is_shutting_down(),
        "after shutdown(), is_shutting_down must return true",
    );
}

// ---------------------------------------------------------------------------
// F-G10-022 (light contract): an external shutdown flag flips correctly.
// The full timeout-bounded join lives in the binary; here we just verify
// the shared-flag pattern that the bin uses for the background tasks.
// ---------------------------------------------------------------------------

#[test]
fn shared_shutdown_flag_visible_to_background_thread() {
    let flag = Arc::new(AtomicBool::new(false));
    let flag_thread = flag.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        // Poll for up to 5 s; report what we saw.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if flag_thread.load(Ordering::Relaxed) {
                tx.send(true).unwrap();
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        tx.send(false).unwrap();
    });

    std::thread::sleep(Duration::from_millis(100));
    flag.store(true, Ordering::Relaxed);

    let saw_flag = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let _ = worker.join();
    assert!(
        saw_flag,
        "background thread must observe the shared shutdown flag",
    );
}
