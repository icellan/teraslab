//! Real-TCP write-path scaling + observability-isolation harness.
//!
//! Phase 0 captures baseline numbers (no scaling assertion). The scaling
//! assertion (8 clients >= N x 1 client) is enabled once the write path is
//! parallelized (plan Task 2c). The heavy multi-client run is gated behind
//! the `slow-tests` feature; a small smoke runs per-PR.

mod common;
use common::*;

// ---- per-PR smoke: cheap, no scaling bar; just proves the harness drives the
// real server and the gauge is reachable. ----
#[test]
fn write_scaling_smoke() {
    let srv = spawn_write_server();
    let (acked, el) = run_clients(srv.tcp_port, 4, 200);
    assert_eq!(acked, 800, "every create must be acked");
    println!(
        "[smoke] 4 clients x 200 = {acked} acked in {el:?} -> {:.0} ops/s, gauge_max={}",
        ops_per_sec(acked, el),
        teraslab::metrics::writers_in_flight_max()
    );
}

// ---- heavy baseline/scaling run: slow-tests only. Phase 0 = measurement,
// no assertion. Task 2c converts the printed ratio into an assertion. ----
#[cfg(feature = "slow-tests")]
#[test]
fn write_scaling_baseline_1_vs_8() {
    let per_client = 50_000u32;
    let srv = spawn_write_server();

    let (a1, e1) = run_clients(srv.tcp_port, 1, per_client);
    let one = ops_per_sec(a1, e1);
    let max1 = teraslab::metrics::writers_in_flight_max();

    let cpu0 = process_cpu_time();
    let (a8, e8) = run_clients(srv.tcp_port, 8, per_client);
    let cpu_used = process_cpu_time() - cpu0;
    let cores = cpu_used.as_secs_f64() / e8.as_secs_f64();
    let eight = ops_per_sec(a8, e8);
    let max8 = teraslab::metrics::writers_in_flight_max();

    println!("[baseline] 1 client : {a1} acked, {one:.0} ops/s, gauge_max={max1}");
    println!("[baseline] 8 clients: {a8} acked, {eight:.0} ops/s, gauge_max={max8}");
    println!("[baseline] scaling ratio (8/1) = {:.2}x", eight / one);
    println!("[baseline] 8-client CPU/wall ratio = {cores:.2} cores (reported, not asserted)");
    // PHASE 0: no assertion. See plan Task 2c.
}
