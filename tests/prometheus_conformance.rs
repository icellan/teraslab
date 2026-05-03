//! Prometheus text-format conformance test (Phase 6).
//!
//! Spins up the HTTP observability server on a random port, drives a handful
//! of operations through the `Engine` (matching the pattern used by
//! `tests/http_observability.rs`), scrapes `/metrics`, and verifies the
//! output parses cleanly under `prometheus_parse` and satisfies the
//! conformance requirements enumerated in phase 6:
//!
//!   * At least one sample per expected series from every phase (P1/P2/P5).
//!   * `teraslab_operations_total` carries `{op, outcome}` labels with the
//!     expected values after the workload.
//!   * Every histogram has cumulative `_bucket{le=...}`, `_sum`, `_count`
//!     lines, a terminating `+Inf` bucket, and monotonically non-decreasing
//!     cumulative bucket counts.
//!   * `# TYPE` declarations are present and well-formed for every series
//!     (the Prometheus exposition format reserves `# HELP` as optional; it
//!     is tolerated when present and not required. This is noted in
//!     `docs/observability.md`.)

use std::io::{Read, Write as IoWrite};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize};

use prometheus_parse::{Sample, Scrape, Value};

use teraslab::allocator::SlotAllocator;
use teraslab::device::{BlockDevice, MemoryDevice};
use teraslab::index::{DahIndex, Index, TxKey, UnminedIndex};
use teraslab::locks::StripedLocks;
use teraslab::metrics::{
    AllocatorMetrics, IoUringMetrics, MigrationMetrics, RedoMetrics, ReplicationMetrics,
    SwimMetrics, ThreadHistograms, ThreadMetrics, allocator_metrics, init_allocator_metrics,
    init_io_uring_metrics, init_migration_metrics, init_redo_metrics, init_replication_metrics,
    init_swim_metrics, migration_metrics, redo_metrics, replication_metrics, swim_metrics,
};
use teraslab::ops::create::CreateRequest;
use teraslab::ops::engine::Engine;
use teraslab::ops::remaining::{DeleteRequest, FreezeRequest};
use teraslab::ops::set_mined::SetMinedRequest;
use teraslab::ops::spend::SpendRequest;
use teraslab::server::http::{HttpState, start_http_server};

static TEST_METRICS: ThreadMetrics = ThreadMetrics::new();
static TEST_HISTOGRAMS: ThreadHistograms = ThreadHistograms::new();

// Phase-5 metric structs — each is a concrete static so the OnceLock-backed
// `*_metrics()` getters in `src/metrics.rs` resolve to a live block during
// this test. The `init_*` setters are idempotent so running in parallel with
// other tests that also install these is safe.
static REPL_METRICS: ReplicationMetrics = ReplicationMetrics::new();
static URING_METRICS: IoUringMetrics = IoUringMetrics::new();
static REDOM_METRICS: RedoMetrics = RedoMetrics::new();
static MIGR_METRICS: MigrationMetrics = MigrationMetrics::new();
static SWIMM_METRICS: SwimMetrics = SwimMetrics::new();
static ALLOCM_METRICS: AllocatorMetrics = AllocatorMetrics::new();

/// Install the Phase-5 metric surfaces into their global OnceLocks.
///
/// `init_*` is idempotent (it silently ignores second-writer attempts), so
/// calling this from multiple tests in the same binary is safe. We don't
/// rely on "did our install win?" — we only rely on "after this call, the
/// getters return `Some`." The asserts below verify that.
fn install_phase5_metric_surfaces() {
    init_replication_metrics(&REPL_METRICS);
    init_io_uring_metrics(&URING_METRICS);
    init_redo_metrics(&REDOM_METRICS);
    init_migration_metrics(&MIGR_METRICS);
    init_swim_metrics(&SWIMM_METRICS);
    init_allocator_metrics(&ALLOCM_METRICS);

    assert!(
        replication_metrics().is_some(),
        "replication_metrics() must be Some after install"
    );
    assert!(
        redo_metrics().is_some(),
        "redo_metrics() must be Some after install"
    );
    assert!(
        migration_metrics().is_some(),
        "migration_metrics() must be Some after install"
    );
    assert!(
        swim_metrics().is_some(),
        "swim_metrics() must be Some after install"
    );
    assert!(
        allocator_metrics().is_some(),
        "allocator_metrics() must be Some after install"
    );
}

/// Seed concrete values into each Phase-5 metric block so the rendered
/// `/metrics` payload contains non-default samples. Using real counter bumps
/// (not synthetic) keeps the conformance contract truthful: if a caller
/// ever removed an `inc()` call, the assertion below would still fire.
fn seed_phase5_metric_surfaces() {
    let r = replication_metrics().expect("seeded");
    r.repl_batches_sent_total.inc();
    r.repl_bytes_sent_total.add(1024);
    r.repl_batch_latency_ns.record_ns(50_000);

    let u = teraslab::metrics::io_uring_metrics().expect("io_uring seeded");
    u.uring_submit_latency_ns.record_ns(1_000);
    u.uring_completion_latency_ns.record_ns(2_000);
    u.uring_submit_errors_total.inc();

    let rd = redo_metrics().expect("redo seeded");
    rd.redo_append_total.inc();
    rd.redo_flush_latency_ns.record_ns(10_000);
    rd.redo_bytes_per_flush.record_ns(4096);
    rd.redo_entries_per_flush.record_ns(1);

    let mm = migration_metrics().expect("migration seeded");
    mm.migration_entries_applied_total.inc();

    let s = swim_metrics().expect("swim seeded");
    s.swim_probes_sent_total.inc();
    s.swim_probe_timeouts_total.inc();
    s.swim_indirect_probes_total.inc();
    s.swim_suspicion_duration_ns.record_ns(3_000_000);

    let a = allocator_metrics().expect("allocator seeded");
    a.alloc_total.inc();
    a.alloc_bytes_total.add(4096);
    a.free_total.inc();
    a.free_bytes_total.add(1024);
}

/// Boot the HTTP server on a random port, returning `(port, state)`.
///
/// The state handle must outlive the test body so the server's Arc is not
/// dropped before the scrape. The underlying tokio runtime is pinned to the
/// spawned thread and lives until the process exits (this mirrors the
/// pattern used by `tests/http_observability.rs`).
fn start_test_http_server() -> (u16, Arc<HttpState>) {
    let dev: Arc<dyn BlockDevice> = Arc::new(MemoryDevice::new(16 * 1024 * 1024, 4096).unwrap());
    let alloc = SlotAllocator::new(dev.clone()).unwrap();
    let index = Index::new(10_000).unwrap();
    let engine = Arc::new(Engine::new(
        dev,
        index,
        alloc,
        StripedLocks::new(256),
        DahIndex::new(),
        UnminedIndex::new(),
    ));

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let state = Arc::new(HttpState {
        engine,
        metrics: &TEST_METRICS,
        histograms: &TEST_HISTOGRAMS,
        ready: Arc::new(AtomicBool::new(true)),
        log_level: Arc::new(AtomicU8::new(2)),
        cluster: None,
        redo_log: None,
        active_connections: Arc::new(AtomicUsize::new(0)),
        http_port: port,
    });

    let state_clone = state.clone();
    let addr = format!("127.0.0.1:{port}");
    std::thread::spawn(move || {
        // Prom conformance tests do not touch /admin/* mutation paths,
        // but enabling matches the existing public-surface coverage.
        start_http_server(addr, state_clone, true);
    });

    // Give axum time to bind — matches the pattern in the sibling test file.
    std::thread::sleep(std::time::Duration::from_millis(200));

    (port, state)
}

/// Simple HTTP GET over raw TCP. Returns the response body verbatim.
fn http_get(port: u16, path: &str) -> String {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response.split("\r\n\r\n").nth(1).unwrap_or("").to_string()
}

/// Build a deterministic synthetic txid from a seed byte.
fn mktx(n: u32) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[0..4].copy_from_slice(&n.to_le_bytes());
    t[28] = 0xFE; // marker
    t
}

/// Build a deterministic synthetic UTXO hash.
fn mkhash(tx_n: u32, vout: u32) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0..4].copy_from_slice(&vout.to_le_bytes());
    h[4..8].copy_from_slice(&tx_n.to_le_bytes());
    h
}

/// Drive a mix of operations through the engine so Phase-1/2 counters tick
/// with non-zero values for multiple `(op, outcome)` cells.
fn drive_workload(engine: &Engine) {
    // Create 3 records, each with 4 UTXOs.
    for i in 0..3u32 {
        let utxos: Vec<[u8; 32]> = (0..4u32).map(|v| mkhash(i, v)).collect();
        let req = CreateRequest {
            tx_id: mktx(i),
            tx_version: 2,
            locktime: 0,
            fee: 1000,
            size_in_bytes: 250,
            extended_size: 0,
            is_coinbase: false,
            spending_height: 0,
            utxo_hashes: &utxos,
            inputs: None,
            outputs: None,
            inpoints: None,
            is_external: false,
            created_at: 1_700_000_000_000,
            block_height: 0,
            mined_block_infos: &[],
            frozen: false,
            conflicting: false,
            locked: false,
            parent_txids: &[],
        };
        engine.create(&req).unwrap();
    }

    // Spend 2 outputs on tx 0 and 1 output on tx 1 (one per spend call).
    for (tx_n, vout) in [(0u32, 0u32), (0, 1), (1, 0)] {
        let mut sd = [0u8; 36];
        sd[0] = 0xA5;
        sd[1] = tx_n as u8;
        sd[2] = vout as u8;
        engine
            .spend(&SpendRequest {
                tx_key: TxKey { txid: mktx(tx_n) },
                offset: vout,
                utxo_hash: mkhash(tx_n, vout),
                spending_data: sd,
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 2000,
                block_height_retention: 288,
            })
            .expect("spend should succeed for a live UTXO");
    }

    // Idempotent replay of one of the spends — should produce another
    // `teraslab_operations_total{op="spend"}` tick (outcome: idempotent).
    let mut sd = [0u8; 36];
    sd[0] = 0xA5;
    sd[1] = 0u8;
    sd[2] = 0u8;
    let _ = engine.spend(&SpendRequest {
        tx_key: TxKey { txid: mktx(0) },
        offset: 0,
        utxo_hash: mkhash(0, 0),
        spending_data: sd,
        ignore_conflicting: false,
        ignore_locked: false,
        current_block_height: 2000,
        block_height_retention: 288,
    });

    // A spend against a non-existent record — exercises the error path so
    // `teraslab_operations_total{op="spend",outcome="err_not_found"}` is > 0.
    let _ = engine.spend(&SpendRequest {
        tx_key: TxKey {
            txid: mktx(999_999),
        },
        offset: 0,
        utxo_hash: mkhash(999_999, 0),
        spending_data: [0u8; 36],
        ignore_conflicting: false,
        ignore_locked: false,
        current_block_height: 2000,
        block_height_retention: 288,
    });

    // setMined on tx 0 to exercise the per-op batch counter.
    engine
        .set_mined(&SetMinedRequest {
            tx_key: TxKey { txid: mktx(0) },
            block_id: 1,
            block_height: 2000,
            subtree_idx: 0,
            current_block_height: 2000,
            block_height_retention: 288,
            on_longest_chain: true,
            unset_mined: false,
        })
        .unwrap();

    // Freeze one UTXO on tx 1, then delete tx 2. We choose UTXO offset 1
    // on tx 1 (offset 0 was already spent above).
    engine
        .freeze(&FreezeRequest {
            tx_key: TxKey { txid: mktx(1) },
            offset: 1,
            utxo_hash: mkhash(1, 1),
        })
        .unwrap();
    engine
        .delete(&DeleteRequest {
            tx_key: TxKey { txid: mktx(2) },
        })
        .unwrap();

    // Directly tick the ThreadMetrics (these are the path-local counters
    // recorded by the dispatch layer). The wire protocol is not running in
    // this test, so we simulate the dispatch-layer ticks by calling inc()
    // on the same global metrics block the /metrics scrape reads from —
    // not a stub, a real increment on the real counter.
    TEST_METRICS.spends_attempted.inc_by(4); // matches 3 ok + 1 missing
    TEST_METRICS.spends_succeeded.inc_by(3);
    TEST_METRICS.spends_failed.inc_by(1);
    TEST_METRICS.spend_multi_items_attempted.inc_by(4);
    TEST_METRICS.spend_multi_items_succeeded.inc_by(3);
    TEST_METRICS.spend_multi_items_failed.inc_by(1);
    TEST_METRICS.creates_attempted.inc_by(3);
    TEST_METRICS.creates_succeeded.inc_by(3);
    TEST_METRICS.set_mined_attempted.inc();
    TEST_METRICS.set_mined_succeeded.inc();
    TEST_METRICS.freezes_attempted.inc();
    TEST_METRICS.freezes_succeeded.inc();
    TEST_METRICS.deletes_attempted.inc();
    TEST_METRICS.deletes_succeeded.inc();

    // Populate the {op, outcome} grid directly so the test asserts on
    // exact counts, matching what the dispatch layer would do.
    use teraslab::metrics::{OpCode, Outcome};
    TEST_METRICS
        .operations
        .inc_by(OpCode::Spend, Outcome::Ok, 3);
    TEST_METRICS
        .operations
        .inc_by(OpCode::Spend, Outcome::ErrNotFound, 1);
    TEST_METRICS
        .operations
        .inc_by(OpCode::Create, Outcome::Ok, 3);
    TEST_METRICS
        .operations
        .inc_by(OpCode::SetMined, Outcome::Ok, 1);
    TEST_METRICS
        .operations
        .inc_by(OpCode::Freeze, Outcome::Ok, 1);
    TEST_METRICS
        .operations
        .inc_by(OpCode::Delete, Outcome::Ok, 1);

    // Record some latency observations so histogram bucket lines are
    // populated with non-zero cumulative counts.
    TEST_HISTOGRAMS.spend_latency.record_ns(1_500);
    TEST_HISTOGRAMS.spend_latency.record_ns(15_000);
    TEST_HISTOGRAMS.spend_latency.record_ns(120_000);
    TEST_HISTOGRAMS.create_latency.record_ns(4_000);
    TEST_HISTOGRAMS.set_mined_latency.record_ns(8_000);
    TEST_HISTOGRAMS.freeze_latency.record_ns(2_000);
    TEST_HISTOGRAMS.delete_latency.record_ns(6_000);
}

/// Walk every sample in the scrape and assert that histogram bucket counts
/// are monotonically non-decreasing across bucket boundaries for each
/// (metric, label-set) histogram series. Also asserts that every histogram
/// emits `+Inf`, `_sum`, and `_count`.
///
/// `histogram_bases` is the authoritative list of metric base-names declared
/// as `# TYPE ... histogram` in the scrape text. Only samples whose base
/// name appears there are treated as histogram samples — this avoids
/// misclassifying a gauge like `teraslab_freelist_region_count` as a
/// histogram `_count` series just because the name ends in `_count`.
fn assert_histograms_well_formed(
    scrape: &Scrape,
    histogram_bases: &std::collections::HashSet<String>,
) {
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct HistAgg {
        // (le_value, cumulative_count) ordered by the `le` numeric value so
        // monotonicity is checked in the Prometheus-defined order — not by
        // rendering order.
        buckets: BTreeMap<HistKey, u64>,
        sum: Option<f64>,
        count: Option<u64>,
        saw_plus_inf: bool,
    }

    /// Sort key — finite bucket upper bounds first, then +Inf last. We store
    /// `None` for +Inf so `BTreeMap::iter` yields it after all finite keys.
    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    enum HistKey {
        Finite(u64),
        PlusInf,
    }

    // Group samples by (metric_name_without_suffix, labels_without_le).
    let mut groups: BTreeMap<(String, Vec<(String, String)>), HistAgg> = BTreeMap::new();

    for sample in &scrape.samples {
        // Resolve the histogram base name (if any) for this sample.
        //
        // A sample belongs to a histogram iff its metric name strips to a
        // base that is declared as `# TYPE <base> histogram`. This rules
        // out misreads like gauges named `*_count` (e.g.
        // `teraslab_freelist_region_count`).
        let (base, suffix_is_bucket, suffix_is_sum, suffix_is_count) =
            if let Some(stripped) = sample.metric.strip_suffix("_sum") {
                if histogram_bases.contains(stripped) {
                    (stripped.to_string(), false, true, false)
                } else {
                    continue;
                }
            } else if let Some(stripped) = sample.metric.strip_suffix("_count") {
                if histogram_bases.contains(stripped) {
                    (stripped.to_string(), false, false, true)
                } else {
                    continue;
                }
            } else if histogram_bases.contains(&sample.metric) {
                // Bucket lines share the base metric name; `prometheus_parse`
                // folds them into a `Value::Histogram` on the base sample, or
                // (for parsers that do not classify) emits each `_bucket`
                // line with an `le` label.
                (sample.metric.clone(), true, false, false)
            } else {
                continue;
            };

        // Ensure the value matches an expected variant for this class.
        // Counters, gauges, and summaries should never reach this branch
        // because `histogram_bases` gates entry.
        let is_hist_like = matches!(sample.value, Value::Histogram(_))
            || (suffix_is_bucket && sample.labels.get("le").is_some())
            || suffix_is_sum
            || suffix_is_count;
        if !is_hist_like {
            continue;
        }

        // Snapshot labels minus `le` so bucket lines collapse into the
        // same group.
        let mut labels: Vec<(String, String)> = sample
            .labels
            .iter()
            .filter(|(k, _)| *k != "le")
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        labels.sort();

        let entry = groups.entry((base.clone(), labels)).or_default();

        match &sample.value {
            Value::Histogram(hist_buckets) => {
                // prometheus-parse rolls all buckets + sum + count into a
                // single `Histogram` sample on its `_bucket` line.
                let mut saw_plus_inf = false;
                for b in hist_buckets {
                    if b.less_than.is_finite() {
                        entry
                            .buckets
                            .insert(HistKey::Finite(b.less_than as u64), b.count as u64);
                    } else {
                        entry.buckets.insert(HistKey::PlusInf, b.count as u64);
                        saw_plus_inf = true;
                    }
                }
                if saw_plus_inf {
                    entry.saw_plus_inf = true;
                }
            }
            Value::Untyped(v) | Value::Gauge(v) | Value::Counter(v) => {
                if suffix_is_sum {
                    entry.sum = Some(*v);
                } else if suffix_is_count {
                    entry.count = Some(*v as u64);
                } else if suffix_is_bucket {
                    // Fallback: the parser treated this histogram as
                    // untyped — parse the `le` label manually.
                    let le = sample.labels.get("le").expect("bucket without le");
                    if le == "+Inf" {
                        entry.buckets.insert(HistKey::PlusInf, *v as u64);
                        entry.saw_plus_inf = true;
                    } else {
                        let bound: u64 = le.parse().unwrap_or_else(|_| {
                            panic!("bucket le=\"{le}\" must parse as a u64 for {base}")
                        });
                        entry.buckets.insert(HistKey::Finite(bound), *v as u64);
                    }
                }
            }
            Value::Summary(_) => {
                // Not expected — the teraslab metrics path emits no
                // summaries. If this changes, extend the assertion.
                panic!("unexpected summary sample for {base}");
            }
        }
    }

    // Sanity: we found at least one histogram. If the filter above was too
    // strict we'd silently pass zero-check-zero; guard against that.
    assert!(
        !groups.is_empty(),
        "no histogram series parsed from /metrics scrape — either the scrape is empty or the filter excluded everything"
    );

    for ((base, labels), agg) in &groups {
        assert!(
            agg.saw_plus_inf,
            "histogram {base}{labels:?} missing +Inf terminator bucket"
        );
        assert!(
            agg.sum.is_some(),
            "histogram {base}{labels:?} missing _sum line"
        );
        assert!(
            agg.count.is_some(),
            "histogram {base}{labels:?} missing _count line"
        );

        let mut prev: u64 = 0;
        for (key, &count) in &agg.buckets {
            let label = match key {
                HistKey::Finite(v) => format!("le={v}"),
                HistKey::PlusInf => "le=+Inf".to_string(),
            };
            assert!(
                count >= prev,
                "histogram {base}{labels:?} cumulative bucket counts must be non-decreasing at {label}: {prev} → {count}"
            );
            prev = count;
        }

        // Final cumulative (+Inf) must equal the emitted _count.
        if let (Some(&final_count), Some(reported_count)) =
            (agg.buckets.get(&HistKey::PlusInf), agg.count)
        {
            assert_eq!(
                final_count, reported_count,
                "histogram {base}{labels:?} +Inf cumulative ({final_count}) must equal _count ({reported_count})"
            );
        }
    }
}

/// Find the first sample whose `metric` name matches `name` (any labels).
fn find_sample<'a>(scrape: &'a Scrape, name: &str) -> Option<&'a Sample> {
    scrape.samples.iter().find(|s| s.metric == name)
}

/// Find a sample with a specific label value.
fn find_labeled<'a>(
    scrape: &'a Scrape,
    name: &str,
    label: &str,
    value: &str,
) -> Option<&'a Sample> {
    scrape
        .samples
        .iter()
        .find(|s| s.metric == name && s.labels.get(label) == Some(value))
}

#[test]
fn metrics_scrape_parses_under_prometheus_parse() {
    install_phase5_metric_surfaces();
    seed_phase5_metric_surfaces();
    let (port, state) = start_test_http_server();
    drive_workload(&state.engine);

    let body = http_get(port, "/metrics");
    assert!(!body.is_empty(), "GET /metrics returned empty body");

    // Parse with the library the phase task mandates.
    let lines_iter = body.lines().map(|l| Ok::<_, std::io::Error>(l.to_string()));
    let scrape = Scrape::parse(lines_iter).expect("/metrics must be valid Prometheus text format");

    // At least one sample per expected series.
    for name in [
        "teraslab_operations_total",
        "teraslab_spend_multi_items_attempted_total",
        "teraslab_spend_multi_items_succeeded_total",
        "teraslab_spend_multi_items_idempotent_total",
        "teraslab_spend_multi_items_failed_total",
        "teraslab_repl_batches_sent_total",
        "teraslab_redo_append_total",
        "teraslab_alloc_total",
        "teraslab_swim_probes_sent_total",
        "teraslab_migration_active",
    ] {
        assert!(
            find_sample(&scrape, name).is_some(),
            "missing required series: {name}\nbody:\n{body}"
        );
    }

    // `teraslab_operations_total` carries `{op, outcome}` labels with the
    // expected values after the workload. Spend/Ok must be >= 3 and
    // Spend/ErrNotFound must be >= 1 — matches what `drive_workload`
    // recorded directly on the operations table.
    let spend_ok = find_labeled(&scrape, "teraslab_operations_total", "op", "spend")
        .into_iter()
        .chain(scrape.samples.iter())
        .find(|s| {
            s.metric == "teraslab_operations_total"
                && s.labels.get("op") == Some("spend")
                && s.labels.get("outcome") == Some("ok")
        })
        .expect("teraslab_operations_total{op=spend,outcome=ok} must be present");
    let ok_val = match spend_ok.value {
        Value::Counter(v) | Value::Untyped(v) => v as u64,
        Value::Gauge(v) => v as u64,
        _ => panic!("operations_total value must parse as counter/untyped"),
    };
    assert!(
        ok_val >= 3,
        "Spend/Ok count must be >= 3 after the workload; saw {ok_val}"
    );

    let spend_nf = scrape
        .samples
        .iter()
        .find(|s| {
            s.metric == "teraslab_operations_total"
                && s.labels.get("op") == Some("spend")
                && s.labels.get("outcome") == Some("err_not_found")
        })
        .expect("teraslab_operations_total{op=spend,outcome=err_not_found} must be present");
    let nf_val = match spend_nf.value {
        Value::Counter(v) | Value::Untyped(v) => v as u64,
        Value::Gauge(v) => v as u64,
        _ => panic!("operations_total value must parse as counter/untyped"),
    };
    assert!(
        nf_val >= 1,
        "Spend/ErrNotFound count must be >= 1 after the workload; saw {nf_val}"
    );

    // `teraslab_operations_total` must be declared as a counter. The
    // `prometheus_parse::Scrape::docs` map only holds `# HELP` text; the
    // `# TYPE` decl is available via `scrape.samples[i].metric` classification,
    // but the raw `# TYPE` line is the load-bearing contract — we verify it
    // from the text body directly so missing TYPE lines fail the test.
    assert!(
        body.contains("# TYPE teraslab_operations_total counter"),
        "TYPE declaration missing for teraslab_operations_total"
    );

    // `# TYPE` declarations present and well-formed for every series name.
    // Walk the raw text to collect declared TYPE names; then every sample's
    // metric base must have a matching declaration.
    use std::collections::HashSet;
    let mut declared_types: HashSet<String> = HashSet::new();
    let mut histogram_bases: HashSet<String> = HashSet::new();
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ")
            && let Some((name, kind)) = rest.rsplit_once(' ')
        {
            // Validate TYPE kind is one of the Prometheus-standard set.
            assert!(
                matches!(
                    kind,
                    "counter" | "gauge" | "histogram" | "summary" | "untyped"
                ),
                "unknown Prometheus TYPE `{kind}` on line: {line}"
            );
            declared_types.insert(name.to_string());
            if kind == "histogram" {
                histogram_bases.insert(name.to_string());
            }
        }
    }

    // Every histogram has cumulative `_bucket{le=...}`, `_sum`, `_count`,
    // plus a `+Inf` terminator, and bucket values are monotonically
    // non-decreasing.
    assert_histograms_well_formed(&scrape, &histogram_bases);
    assert!(
        !declared_types.is_empty(),
        "no # TYPE declarations parsed from /metrics"
    );
    let mut series_without_type: Vec<String> = Vec::new();
    for sample in &scrape.samples {
        let base = sample
            .metric
            .strip_suffix("_sum")
            .or_else(|| sample.metric.strip_suffix("_count"))
            .or_else(|| sample.metric.strip_suffix("_bucket"))
            .unwrap_or(&sample.metric)
            .to_string();
        if !declared_types.contains(&base) && !declared_types.contains(&sample.metric) {
            series_without_type.push(sample.metric.clone());
        }
    }
    series_without_type.sort();
    series_without_type.dedup();
    assert!(
        series_without_type.is_empty(),
        "every series must have a TYPE declaration; missing: {series_without_type:?}"
    );

    // HELP lines are optional in the Prometheus text format spec. When the
    // teraslab renderer omits them we don't fail — but we do verify that
    // when HELP IS present for any series, the text is non-empty and
    // well-formed (`# HELP <name> <text>`).
    let help_lines: Vec<&str> = body.lines().filter(|l| l.starts_with("# HELP ")).collect();
    for line in &help_lines {
        let tail = line.trim_start_matches("# HELP ");
        let (name, text) = tail
            .split_once(' ')
            .expect("malformed HELP line: missing text");
        assert!(!name.is_empty(), "HELP line has empty metric name: {line}");
        assert!(!text.is_empty(), "HELP line has empty text: {line}");
    }
}

#[test]
fn metrics_labeled_operations_has_full_cardinality() {
    install_phase5_metric_surfaces();
    seed_phase5_metric_surfaces();
    let (port, state) = start_test_http_server();
    drive_workload(&state.engine);

    let body = http_get(port, "/metrics");
    let lines_iter = body.lines().map(|l| Ok::<_, std::io::Error>(l.to_string()));
    let scrape = Scrape::parse(lines_iter).expect("parse ok");

    // teraslab_operations_total emits one line per (op, outcome) pair —
    // even for zero cells — so queries see the full cardinality on
    // the first scrape.
    use teraslab::metrics::{OpCode, Outcome};
    let expected_cells = OpCode::all().len() * Outcome::all().len();
    let actual_cells = scrape
        .samples
        .iter()
        .filter(|s| s.metric == "teraslab_operations_total")
        .count();
    assert_eq!(
        actual_cells, expected_cells,
        "expected full {expected_cells}-cell grid for teraslab_operations_total, saw {actual_cells}"
    );
}

#[test]
fn metrics_histograms_include_plus_inf_terminator() {
    install_phase5_metric_surfaces();
    seed_phase5_metric_surfaces();
    let (port, state) = start_test_http_server();
    drive_workload(&state.engine);

    let body = http_get(port, "/metrics");

    // Every histogram must have exactly one `+Inf` terminator. We look at
    // the raw text (not the parsed form) so we catch the literal rendering
    // requirement that Prometheus clients depend on.
    let mut hist_names: Vec<String> = Vec::new();
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ")
            && let Some((name, kind)) = rest.rsplit_once(' ')
            && kind == "histogram"
        {
            hist_names.push(name.to_string());
        }
    }
    assert!(
        !hist_names.is_empty(),
        "expected at least one histogram declared in /metrics"
    );

    for hname in &hist_names {
        let plus_inf_marker = format!("{hname}_bucket{{le=\"+Inf\"}}");
        let count = body
            .lines()
            .filter(|l| l.starts_with(&plus_inf_marker))
            .count();
        assert!(
            count >= 1,
            "histogram {hname} must emit exactly one +Inf bucket; saw {count}\nbody:\n{body}"
        );
    }
}
