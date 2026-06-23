//! Latency-isolation: GET /admin/top must stay responsive WHILE a sustained
//! write burst saturates the data path. Proves the observability read no
//! longer contends with the redo writer lock (plan Phase 1).

mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[test]
fn admin_top_responsive_under_write_burst() {
    let srv = spawn_write_server();
    let stop = Arc::new(AtomicBool::new(false));

    // Sustained write burst: 6 clients hammering creates until told to stop.
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

    // Let the burst ramp up.
    std::thread::sleep(Duration::from_millis(300));

    // Poll /admin/top 50 times during the burst; collect latencies.
    let mut samples = Vec::new();
    for _ in 0..50 {
        let (status, body, dt) = http_get_timed(srv.http_port, "/admin/top", ADMIN_TOKEN);
        assert_eq!(status, 200, "admin/top must return 200 under load");
        assert!(
            serde_json::from_str::<serde_json::Value>(&body).is_ok(),
            "admin/top body must be valid JSON"
        );
        samples.push(dt);
        std::thread::sleep(Duration::from_millis(5));
    }

    stop.store(true, Ordering::Relaxed);
    for w in writers {
        let _ = w.join();
    }

    samples.sort();
    let p99 = samples[(samples.len() as f64 * 0.99) as usize - 1];
    println!("admin/top p99 under burst = {p99:?}");
    assert!(
        p99 < Duration::from_millis(50),
        "admin/top p99 ({p99:?}) must stay < 50ms during a write burst"
    );
}
