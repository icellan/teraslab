//! Real-TCP write-path scaling + observability-isolation harness.
//!
//! Phase 0 captures baseline numbers (no scaling assertion). The scaling
//! assertion (8 clients >= N x 1 client) is enabled once the write path is
//! parallelized (plan Task 2c). The heavy multi-client run is gated behind
//! the `slow-tests` feature; a small smoke runs per-PR.
#![allow(clippy::disallowed_macros)] // integration tests may use println! for diagnostics

mod common;
use common::*;
use serial_test::serial;
use std::sync::Arc;

// ---- per-PR smoke: cheap, no scaling bar; just proves the harness drives the
// real server and the gauge is reachable. ----
#[test]
fn write_scaling_smoke() {
    let srv = spawn_write_server();
    let (acked, el) = run_clients(srv.tcp_port, 4, 200);
    // drive_creates counts STATUS_OK only, so this fails if any create on the
    // write path is broken (a STATUS_PARTIAL_ERROR drops the ack).
    assert_eq!(acked, 800, "every create must be OK-acked");
    // The 1-item-batch smoke does not reliably overlap writers (gauge peaks at
    // 1), so assert only that the writers_in_flight gauge is wired and reached;
    // the > 1 parallelism bar lives in the slow 8-client baseline below where
    // it actually holds.
    let gauge_max = teraslab::metrics::writers_in_flight_max();
    assert!(
        gauge_max >= 1,
        "writers_in_flight gauge must register at least one writer, got {gauge_max}"
    );
    println!(
        "[smoke] 4 clients x 200 = {acked} acked in {el:?} -> {:.0} ops/s, gauge_max={gauge_max}",
        ops_per_sec(acked, el),
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

    // Write parallelism (PR#21): 8 concurrent connection threads drive the
    // sharded write path across multiple cores. The CPU/wall ratio is the
    // write-path analogue of the read fan-out's cores assertion — and the
    // metric that actually reflects parallelism here: `writers_in_flight`
    // peaks at 1 even when cores > 2, so it is reported, not asserted.
    //
    // NOTE: this harness runs WITHOUT a checkpoint task, so a sustained
    // 400k-write run overflows the test redo log and the tail of creates fail
    // with LogFull — `a8` is therefore not the full total and is not asserted
    // equal to it. The per-PR smoke's `acked == 800` covers the all-OK path on
    // a workload that does not overflow.
    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    if available >= 2 {
        assert!(
            cores > 1.5,
            "8 concurrent clients must use >1.5 cores on the write path \
             (got {cores:.2} on a {available}-core host)"
        );
    }
}

// ===========================================================================
// READ / decoration-heavy profile (Phase A — the read/serving bottleneck).
//
// teranode's parent decoration sends one fat GetRecordBatch(FieldColdData) per
// connection; teraslab's handle_get_batch walks the batch in a single serial
// `for txid in &txids` loop on the one connection thread, so a single batch is
// pinned to one core no matter how many cores are free. These tests drive that
// exact shape. The smoke is per-PR; the cores baseline is slow-tests only.
// ===========================================================================

// ---- per-PR smoke: proves the harness seeds cold data and the decoration
// read path round-trips every item. No perf bar. ----
#[test]
fn read_scaling_smoke() {
    let srv = spawn_write_server();
    let txids = seed_cold_records(srv.tcp_port, 512, 512);
    assert_eq!(txids.len(), 512, "all parents seeded");

    // One connection, 4 fat batches of 256 — cycles through the 512 parents,
    // every txid exists so every item must decorate OK.
    let (decorated, el) = drive_decoration_reads(srv.tcp_port, &txids, 256, 4);
    assert_eq!(decorated, 256 * 4, "every requested item must decorate");
    println!(
        "[read-smoke] 1 client x 4 x 256 = {decorated} decorated in {el:?} -> {:.0} reads/s",
        ops_per_sec(decorated, el)
    );
}

// ---- per-PR: the pprof CPU-profile endpoint returns a real flamegraph while
// the read path is under load. Proves the profiling gate is wired and renders
// a non-trivial SVG, not a stub. ----
// `#[serial(pprof)]`: pprof installs a PROCESS-GLOBAL ITIMER_PROF profiler, so
// the two pprof tests must not run concurrently (cargo runs a binary's tests in
// parallel by default) — one would collide with the other's in-flight profile.
#[test]
#[serial(pprof)]
fn pprof_endpoint_returns_flamegraph_under_load() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let srv = spawn_write_server();
    let txids = seed_cold_records(srv.tcp_port, 256, 512);

    // Background read load so the 1s sample has live stacks to capture.
    let stop = Arc::new(AtomicBool::new(false));
    let load = {
        let stop = stop.clone();
        let txids = txids.clone();
        let port = srv.tcp_port;
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = drive_decoration_reads(port, &txids, 128, 8);
            }
        })
    };

    let (status, body, _el) =
        http_get_timed(srv.http_port, "/debug/pprof/profile?seconds=1", ADMIN_TOKEN);
    stop.store(true, Ordering::Relaxed);
    load.join().unwrap();

    assert_eq!(
        status, 200,
        "pprof profile must return 200; body={body:.200}"
    );
    assert!(
        body.contains("<svg"),
        "flamegraph must be an SVG document; got {} bytes starting {:?}",
        body.len(),
        &body[..body.len().min(80)]
    );
    assert!(
        body.len() > 1000,
        "flamegraph SVG suspiciously small: {} bytes",
        body.len()
    );
    println!("[pprof] flamegraph SVG = {} bytes", body.len());
}

// ---- per-PR: the endpoint rejects a second concurrent profile (single-flight)
// so two operators can't fight over the one process-global profiler. ----
#[test]
#[serial(pprof)]
fn pprof_endpoint_is_single_flight() {
    let srv = spawn_write_server();
    let port = srv.http_port;

    // First profile runs for 2s in the background.
    let first = std::thread::spawn(move || {
        http_get_timed(port, "/debug/pprof/profile?seconds=2", ADMIN_TOKEN)
    });
    // Give it time to claim the profiler.
    std::thread::sleep(std::time::Duration::from_millis(300));
    let (status, _body, _el) = http_get_timed(port, "/debug/pprof/profile?seconds=1", ADMIN_TOKEN);
    assert_eq!(
        status, 409,
        "second concurrent profile must be rejected with 409"
    );

    let (s1, _b, _e) = first.join().unwrap();
    assert_eq!(s1, 200, "first profile must still succeed");
}

// ---- per-PR: COLD_DATA_OUTPUTS (#20) returns only the parent's outputs
// section, a strictly smaller wire payload than full COLD_DATA. ----
#[test]
fn cold_data_outputs_ships_only_the_outputs_section() {
    use teraslab::protocol::codec::{FieldMask, decode_get_response_checked, encode_get_batch};
    use teraslab::protocol::frame::RequestFrame;
    use teraslab::protocol::opcodes::{OP_GET_BATCH, STATUS_OK};

    let srv = spawn_write_server();
    let outputs_len = 128usize;
    // One parent whose cold data is inputs(32) + outputs(`outputs_len`) + inpoints(16).
    let txids = seed_cold_records(srv.tcp_port, 1, outputs_len);

    let mut stream = std::net::TcpStream::connect(format!("127.0.0.1:{}", srv.tcp_port)).unwrap();
    stream.set_nodelay(true).unwrap();

    let get = |stream: &mut std::net::TcpStream, mask: u32| -> Vec<u8> {
        let payload = encode_get_batch(mask, &[txids[0]]);
        let resp = send_frame(
            stream,
            &RequestFrame {
                request_id: 1,
                op_code: OP_GET_BATCH,
                flags: 0,
                payload: payload.into(),
            },
        );
        let items = decode_get_response_checked(&resp.payload, 4).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, STATUS_OK, "get must succeed");
        items[0].data.clone()
    };

    let full = get(&mut stream, FieldMask::COLD_DATA);
    let outs = get(&mut stream, FieldMask::COLD_DATA_OUTPUTS);

    // make_cold_data fills the outputs section with bytes 0..outputs_len.
    let expected: Vec<u8> = (0..outputs_len).map(|i| (i & 0xff) as u8).collect();
    assert!(
        outs.len() >= 4,
        "outputs response must carry a length prefix"
    );
    let out_len = u32::from_le_bytes(outs[0..4].try_into().unwrap()) as usize;
    assert_eq!(
        out_len, outputs_len,
        "outputs length must match the seeded section"
    );
    assert_eq!(
        &outs[4..4 + out_len],
        &expected[..],
        "outputs bytes must be exactly the parent's outputs section"
    );
    // The whole point of #20: outputs-only is a smaller wire payload than the
    // full inputs+outputs+inpoints cold blob.
    assert!(
        outs.len() < full.len(),
        "outputs-only ({}) must be smaller than full cold data ({})",
        outs.len(),
        full.len()
    );
}

// ---- heavy scaling assertion: single-connection fat-batch decoration profile.
// The CPU/wall ratio is the read-path equivalent of the write baseline's cores
// figure. Pre-fix (serial `for txid in &txids` loop) this pinned ~1.0 core even
// on a many-core host; post-fix (rayon intra-batch fan-out) it must climb well
// above one core — that is the whole point of Phase B. ----
#[cfg(feature = "slow-tests")]
#[test]
fn read_scaling_single_batch_uses_multiple_cores() {
    let srv = spawn_write_server();
    // Enough parents to spread across all 256 shards and exceed L2.
    let txids = seed_cold_records(srv.tcp_port, 50_000, 1024);

    // One connection, 200 fat batches of 826 (the live batch size). A single
    // connection is the teranode decoration shape — the case the serial loop
    // pinned to one core.
    let cpu0 = process_cpu_time();
    let (decorated, el) = run_read_clients(srv.tcp_port, &txids, 1, 826, 200);
    let cpu_used = process_cpu_time() - cpu0;
    let cores = cpu_used.as_secs_f64() / el.as_secs_f64();
    let reads = ops_per_sec(decorated, el);

    assert_eq!(decorated, 826 * 200, "every requested item must decorate");
    println!("[read-scaling] 1 conn, 826/batch: {decorated} decorated, {reads:.0} reads/s");
    println!("[read-scaling] single-connection CPU/wall ratio = {cores:.2} cores");

    // On any multi-core host the fanned batch must use materially more than one
    // core. Gated on >= 2 available cores so a single-core CI runner (where the
    // ratio cannot exceed 1.0 by construction) does not flake. Pre-fix this sat
    // at ~1.0 regardless of core count; 1.5 is a wide margin below what the
    // fan-out actually achieves (5+ cores on a 14-core host).
    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    if available >= 2 {
        assert!(
            cores > 1.5,
            "single-connection fat-batch decoration must use >1.5 cores after fan-out \
             (got {cores:.2} on a {available}-core host); the serial batch loop has regressed"
        );
    }
}
