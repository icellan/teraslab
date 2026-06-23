//! Latency-isolation: GET /admin/top must stay responsive WHILE a sustained
//! write burst saturates the data path. Proves the observability read no
//! longer contends with the redo writer lock (plan Phase 1).
//!
//! # Why relative, not absolute?
//!
//! An absolute wall-clock bound (e.g. "< 50 ms") measures debug-build JSON
//! serialisation cost and OS scheduling noise, not lock-isolation. On a quiet
//! debug machine we observed p99 = 38–82 ms with *zero* write load, so a 50 ms
//! ceiling fails 3 of 4 runs before the burst even starts. The variable we
//! actually care about is *delta* caused by write-path lock contention: if the
//! snapshot still holds or waits for the redo writer lock, the 6-writer burst
//! queues it behind many acquisitions and burst_p99 balloons far past control.
//! A generous additive margin (150 ms) covers debug JSON cost and scheduling
//! jitter — both present equally in control and burst — while still catching
//! gross re-coupling. On a real fsync device the burst-vs-control gap would be
//! hundreds of milliseconds to seconds, so this bound remains meaningful there.
#![allow(clippy::disallowed_macros)] // integration tests may use println! for diagnostics

mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[test]
fn admin_top_responsive_under_write_burst() {
    let srv = spawn_write_server();

    // ------------------------------------------------------------------
    // CONTROL phase: poll /admin/top with no write load to establish
    // the baseline JSON-build + round-trip cost on this machine/build.
    // Must happen BEFORE any writers start so the server is truly idle.
    // ------------------------------------------------------------------
    let mut control_samples = Vec::new();
    for _ in 0..30 {
        let (status, body, dt) = http_get_timed(srv.http_port, "/admin/top", ADMIN_TOKEN);
        assert_eq!(status, 200, "admin/top must return 200 (control phase)");
        assert!(
            serde_json::from_str::<serde_json::Value>(&body).is_ok(),
            "admin/top body must be valid JSON (control phase)"
        );
        control_samples.push(dt);
        std::thread::sleep(Duration::from_millis(10));
    }
    control_samples.sort();
    let control_p99 = control_samples[(control_samples.len() as f64 * 0.99) as usize - 1];

    // ------------------------------------------------------------------
    // TREATMENT phase: start 6 sustained writers, let them ramp, then
    // poll /admin/top 50 times while the burst is running.
    // ------------------------------------------------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..6u32)
        .map(|c| {
            let port = srv.tcp_port;
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut n = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    // 500 creates per call so the data path stays busy.
                    let _ = drive_creates(port, c.wrapping_add(n.wrapping_mul(6)), 500);
                    n = n.wrapping_add(1);
                }
            })
        })
        .collect();

    // Let the burst ramp up before we start measuring.
    std::thread::sleep(Duration::from_millis(300));

    let mut burst_samples = Vec::new();
    for _ in 0..50 {
        let (status, body, dt) = http_get_timed(srv.http_port, "/admin/top", ADMIN_TOKEN);
        assert_eq!(status, 200, "admin/top must return 200 under burst");
        assert!(
            serde_json::from_str::<serde_json::Value>(&body).is_ok(),
            "admin/top body must be valid JSON (burst phase)"
        );
        burst_samples.push(dt);
        std::thread::sleep(Duration::from_millis(5));
    }

    stop.store(true, Ordering::Relaxed);
    for w in writers {
        let _ = w.join();
    }

    burst_samples.sort();
    let burst_p99 = burst_samples[(burst_samples.len() as f64 * 0.99) as usize - 1];

    println!("admin/top p99: control={control_p99:?} burst={burst_p99:?}");

    // The write burst must not materially inflate /admin/top latency. If the
    // snapshot still took a write-path lock, a sustained 6-writer burst would
    // queue it behind many lock acquisitions and burst_p99 would balloon far
    // past control. A generous additive margin tolerates debug-build JSON cost
    // and scheduling jitter (both present in control AND burst) while still
    // catching gross re-coupling.
    assert!(
        burst_p99 < control_p99 + Duration::from_millis(150),
        "write burst inflated /admin/top p99 beyond margin: control={control_p99:?} burst={burst_p99:?} \
         (the observability read is contending with the write path)"
    );
    // Catastrophic-block backstop: a snapshot serialized behind a real fsync
    // burst would be hundreds of ms to seconds.
    assert!(
        burst_p99 < Duration::from_millis(500),
        "/admin/top p99 under burst is catastrophically high: {burst_p99:?}"
    );
}
