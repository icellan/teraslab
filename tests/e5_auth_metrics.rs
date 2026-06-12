//! E-5 — distinct clock-skew vs HMAC-failure rejection metrics, and the
//! configurable clock-skew window.
//!
//! A clock-skew partition (every node reachable and sharing the secret,
//! but wall-clocks disagreeing) must be diagnosable separately from a
//! wrong/rotated secret. `cluster::auth` therefore bumps two DISTINCT
//! counters — `auth_skew_rejections_total` and `auth_hmac_rejections_total`
//! — and emits a distinct `warn` log on the skew path.
//!
//! These tests live in their own integration binary so the ONLY code that
//! touches the process-wide `ClusterAuthMetrics` is the auth `verify*`
//! paths exercised here. `#[serial(cluster_auth)]` keeps them from
//! racing each other, so the before/after counter deltas are exact.

use std::io::Cursor;
use std::sync::OnceLock;
use std::time::Duration;

use serial_test::serial;
use teraslab::cluster::auth::{
    DEFAULT_MAX_CLOCK_SKEW, set_max_clock_skew, sign_with_timestamp,
    verify_frame_streaming_with_now, verify_with_now,
};
use teraslab::metrics::{ClusterAuthMetrics, cluster_auth_metrics, init_cluster_auth_metrics};

/// Install the process-wide `ClusterAuthMetrics` exactly once and return
/// the installed handle. `OnceLock` means whichever test runs first wins
/// the install; every later call observes the same handle.
fn metrics() -> &'static ClusterAuthMetrics {
    static M: OnceLock<ClusterAuthMetrics> = OnceLock::new();
    let m = M.get_or_init(ClusterAuthMetrics::new);
    init_cluster_auth_metrics(m);
    cluster_auth_metrics().unwrap_or(m)
}

const NOW_MS: u64 = 1_700_000_000_000;
const SIX_MIN_MS: u64 = 6 * 60 * 1000;

#[serial(cluster_auth)]
#[test]
fn skew_rejection_bumps_skew_metric_not_hmac() {
    let m = metrics();
    let skew_before = m.auth_skew_rejections_total.get();
    let hmac_before = m.auth_hmac_rejections_total.get();

    // Valid tag, timestamp 6 minutes in the past → skew path.
    let key = b"cluster-secret";
    let signed = sign_with_timestamp(key, b"gossip", NOW_MS - SIX_MIN_MS);
    let err = verify_with_now(key, &signed, NOW_MS).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("stale timestamp"));

    assert_eq!(
        m.auth_skew_rejections_total.get(),
        skew_before + 1,
        "skew rejection must bump the skew counter"
    );
    assert_eq!(
        m.auth_hmac_rejections_total.get(),
        hmac_before,
        "skew rejection must NOT bump the HMAC counter"
    );
}

#[serial(cluster_auth)]
#[test]
fn hmac_rejection_bumps_hmac_metric_not_skew() {
    let m = metrics();
    let skew_before = m.auth_skew_rejections_total.get();
    let hmac_before = m.auth_hmac_rejections_total.get();

    // Wrong key → HMAC path (timestamp is fresh, skew never reached).
    let signed = sign_with_timestamp(b"key1", b"gossip", NOW_MS);
    let err = verify_with_now(b"key2", &signed, NOW_MS).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);

    assert_eq!(
        m.auth_hmac_rejections_total.get(),
        hmac_before + 1,
        "HMAC failure must bump the HMAC counter"
    );
    assert_eq!(
        m.auth_skew_rejections_total.get(),
        skew_before,
        "HMAC failure must NOT bump the skew counter"
    );
}

#[serial(cluster_auth)]
#[test]
fn streaming_skew_rejection_bumps_skew_metric() {
    let m = metrics();
    let skew_before = m.auth_skew_rejections_total.get();
    let hmac_before = m.auth_hmac_rejections_total.get();

    let key = b"k";
    let body = sign_with_timestamp(key, b"payload", NOW_MS - SIX_MIN_MS);
    let mut framed = Vec::with_capacity(4 + body.len());
    framed.extend_from_slice(&(body.len() as u32).to_le_bytes());
    framed.extend_from_slice(&body);

    let mut reader = Cursor::new(&framed[..]);
    let mut sink = Vec::<u8>::new();
    let err = verify_frame_streaming_with_now(key, &mut reader, &mut sink, NOW_MS).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    assert_eq!(m.auth_skew_rejections_total.get(), skew_before + 1);
    assert_eq!(m.auth_hmac_rejections_total.get(), hmac_before);
}

#[serial(cluster_auth)]
#[test]
fn configurable_skew_window_widens_acceptance() {
    let key = b"k";
    let signed = sign_with_timestamp(key, b"x", NOW_MS - SIX_MIN_MS);

    // Rejected at the 5-minute default…
    assert!(verify_with_now(key, &signed, NOW_MS).is_err());

    // …accepted once widened to 10 minutes.
    set_max_clock_skew(Duration::from_secs(10 * 60));
    let payload = verify_with_now(key, &signed, NOW_MS)
        .expect("widened window must accept a 6-minute-old frame");
    assert_eq!(payload, b"x");

    // Restore the default so other serialized tests see the 5-minute window.
    set_max_clock_skew(DEFAULT_MAX_CLOCK_SKEW);
    assert!(verify_with_now(key, &signed, NOW_MS).is_err());
}
