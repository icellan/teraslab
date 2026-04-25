//! Metrics collection and reporting for the TeraSlab test client.
//!
//! [`MetricsReporter`] collects per-operation-type latency samples and computes
//! percentile statistics (p50, p95, p99, max). The reporter is thread-safe via
//! a `parking_lot::Mutex` and is designed to be shared across async tasks.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Latency tracker (internal, per-operation type)
// ---------------------------------------------------------------------------

/// Accumulates raw latency samples for a single operation type.
///
/// Samples are stored in a `Vec` and sorted on-demand when statistics are
/// requested. This avoids the overhead of maintaining a sorted structure on
/// every insert.
pub struct LatencyTracker {
    /// Raw, unsorted latency samples.
    samples: Vec<Duration>,
    /// Total number of samples recorded (always equals `samples.len()`).
    count: u64,
}

impl LatencyTracker {
    /// Create a new, empty tracker.
    fn new() -> Self {
        Self {
            samples: Vec::new(),
            count: 0,
        }
    }

    /// Record a single latency sample.
    ///
    /// # Parameters
    /// - `duration`: The observed request latency.
    fn record(&mut self, duration: Duration) {
        self.samples.push(duration);
        self.count += 1;
    }

    /// Compute percentile statistics over all recorded samples.
    ///
    /// Returns `None` if no samples have been recorded.
    ///
    /// The percentile computation sorts the samples (if not already sorted)
    /// and selects the value at index `floor(percentile * count)`, clamped
    /// to the valid range.
    fn stats(&mut self) -> Option<LatencyStats> {
        if self.samples.is_empty() {
            return None;
        }

        self.samples.sort_unstable();

        let count = self.count;
        let p50 = percentile_value(&self.samples, 0.50);
        let p90 = percentile_value(&self.samples, 0.90);
        let p95 = percentile_value(&self.samples, 0.95);
        let p99 = percentile_value(&self.samples, 0.99);
        let p999 = percentile_value(&self.samples, 0.999);
        let max = self.samples[self.samples.len() - 1];

        Some(LatencyStats {
            p50,
            p90,
            p95,
            p99,
            p999,
            max,
            count,
        })
    }
}

/// Pick the sample at the given percentile (0.0 – 1.0).
///
/// Index is `floor(percentile * count)`, clamped to `[0, len-1]`.
fn percentile_value(sorted: &[Duration], pct: f64) -> Duration {
    let idx = ((pct * sorted.len() as f64) as usize).min(sorted.len() - 1);
    sorted[idx]
}

// ---------------------------------------------------------------------------
// Latency stats (public snapshot)
// ---------------------------------------------------------------------------

/// Computed percentile statistics for a single operation type.
#[derive(Debug, Clone)]
pub struct LatencyStats {
    /// 50th percentile (median) latency.
    pub p50: Duration,
    /// 90th percentile latency.
    pub p90: Duration,
    /// 95th percentile latency.
    pub p95: Duration,
    /// 99th percentile latency.
    pub p99: Duration,
    /// 99.9th percentile latency.
    pub p999: Duration,
    /// Maximum observed latency.
    pub max: Duration,
    /// Total number of samples.
    pub count: u64,
}

// ---------------------------------------------------------------------------
// MetricsReporter
// ---------------------------------------------------------------------------

/// Thread-safe latency collector and reporter.
///
/// Tracks latency samples grouped by operation type (e.g. "create", "spend",
/// "read"). Provides methods to compute per-type percentile statistics and
/// render a human-readable summary.
pub struct MetricsReporter {
    latencies: Mutex<HashMap<String, LatencyTracker>>,
}

impl Default for MetricsReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsReporter {
    /// Create a new, empty reporter.
    pub fn new() -> Self {
        Self {
            latencies: Mutex::new(HashMap::new()),
        }
    }

    /// Record a latency sample for the given operation type.
    ///
    /// If this is the first sample for `op_type`, a new tracker is created
    /// automatically.
    ///
    /// # Parameters
    /// - `op_type`: Operation name (e.g. "create", "spend", "read").
    /// - `duration`: The observed request latency.
    pub fn record(&self, op_type: &str, duration: Duration) {
        let mut map = self.latencies.lock();
        map.entry(op_type.to_owned())
            .or_insert_with(LatencyTracker::new)
            .record(duration);
    }

    /// Compute percentile statistics for a single operation type.
    ///
    /// Returns `None` if no samples have been recorded for `op_type`.
    ///
    /// # Parameters
    /// - `op_type`: Operation name to query.
    pub fn stats(&self, op_type: &str) -> Option<LatencyStats> {
        let mut map = self.latencies.lock();
        map.get_mut(op_type).and_then(|t| t.stats())
    }

    /// Compute percentile statistics for all tracked operation types.
    ///
    /// Returns a map from operation type name to its [`LatencyStats`]. Types
    /// with zero samples are omitted.
    pub fn all_stats(&self) -> HashMap<String, LatencyStats> {
        let mut map = self.latencies.lock();
        let mut result = HashMap::new();
        for (name, tracker) in map.iter_mut() {
            if let Some(stats) = tracker.stats() {
                result.insert(name.clone(), stats);
            }
        }
        result
    }

    /// Remove all recorded samples for all operation types.
    pub fn reset(&self) {
        let mut map = self.latencies.lock();
        map.clear();
    }

    /// Render a human-readable summary of all latency statistics.
    ///
    /// The output contains one section per operation type, sorted
    /// alphabetically, with p50/p95/p99/max and sample count.
    ///
    /// Example output:
    /// ```text
    /// === Latency Summary ===
    /// create (1500 samples):
    ///   p50:  1.20ms  p95:  3.45ms  p99:  8.90ms  max: 15.30ms
    /// read (3200 samples):
    ///   p50:  0.50ms  p95:  1.10ms  p99:  2.80ms  max:  7.20ms
    /// ```
    pub fn format_summary(&self) -> String {
        let stats = self.all_stats();
        if stats.is_empty() {
            return "=== Latency Summary ===\n(no samples recorded)\n".to_owned();
        }

        let mut names: Vec<&String> = stats.keys().collect();
        names.sort();

        let mut out = String::new();
        let _ = writeln!(out, "=== Latency Summary ===");
        for name in names {
            let s = &stats[name];
            let _ = writeln!(out, "{name} ({} samples):", s.count);
            let _ = writeln!(
                out,
                "  p50: {:>8.2}ms  p90: {:>8.2}ms  p95: {:>8.2}ms  p99: {:>8.2}ms  p99.9: {:>8.2}ms  max: {:>8.2}ms",
                duration_ms(s.p50),
                duration_ms(s.p90),
                duration_ms(s.p95),
                duration_ms(s.p99),
                duration_ms(s.p999),
                duration_ms(s.max),
            );
        }
        out
    }
}

/// Convert a `Duration` to fractional milliseconds for display.
fn duration_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_reporter_stats_returns_none() {
        let r = MetricsReporter::new();
        assert!(r.stats("create").is_none());
    }

    #[test]
    fn empty_reporter_all_stats_is_empty() {
        let r = MetricsReporter::new();
        assert!(r.all_stats().is_empty());
    }

    #[test]
    fn single_sample_stats() {
        let r = MetricsReporter::new();
        r.record("create", Duration::from_millis(5));

        let s = r.stats("create").expect("should have stats");
        assert_eq!(s.count, 1);
        assert_eq!(s.p50, Duration::from_millis(5));
        assert_eq!(s.p95, Duration::from_millis(5));
        assert_eq!(s.p99, Duration::from_millis(5));
        assert_eq!(s.max, Duration::from_millis(5));
    }

    #[test]
    fn multiple_samples_percentiles() {
        let r = MetricsReporter::new();
        // Record 100 samples: 1ms, 2ms, ..., 100ms
        for i in 1..=100 {
            r.record("read", Duration::from_millis(i));
        }

        let s = r.stats("read").expect("should have stats");
        assert_eq!(s.count, 100);
        // p50 = index floor(0.50 * 100) = 50 -> 51ms (1-indexed value at 0-indexed 50)
        assert_eq!(s.p50, Duration::from_millis(51));
        // p95 = index floor(0.95 * 100) = 95 -> 96ms
        assert_eq!(s.p95, Duration::from_millis(96));
        // p99 = index floor(0.99 * 100) = 99 -> 100ms
        assert_eq!(s.p99, Duration::from_millis(100));
        assert_eq!(s.max, Duration::from_millis(100));
    }

    #[test]
    fn multiple_operation_types() {
        let r = MetricsReporter::new();
        r.record("create", Duration::from_millis(10));
        r.record("spend", Duration::from_millis(20));
        r.record("read", Duration::from_millis(5));

        let all = r.all_stats();
        assert_eq!(all.len(), 3);
        assert!(all.contains_key("create"));
        assert!(all.contains_key("spend"));
        assert!(all.contains_key("read"));
        assert_eq!(all["create"].count, 1);
        assert_eq!(all["spend"].count, 1);
        assert_eq!(all["read"].count, 1);
    }

    #[test]
    fn reset_clears_all_samples() {
        let r = MetricsReporter::new();
        r.record("create", Duration::from_millis(10));
        r.record("spend", Duration::from_millis(20));
        assert_eq!(r.all_stats().len(), 2);

        r.reset();
        assert!(r.all_stats().is_empty());
        assert!(r.stats("create").is_none());
        assert!(r.stats("spend").is_none());
    }

    #[test]
    fn format_summary_empty() {
        let r = MetricsReporter::new();
        let summary = r.format_summary();
        assert!(summary.contains("no samples recorded"));
    }

    #[test]
    fn format_summary_with_data() {
        let r = MetricsReporter::new();
        r.record("create", Duration::from_millis(5));
        r.record("create", Duration::from_millis(10));
        r.record("read", Duration::from_millis(1));

        let summary = r.format_summary();
        assert!(summary.contains("=== Latency Summary ==="));
        assert!(summary.contains("create (2 samples):"));
        assert!(summary.contains("read (1 samples):"));
        assert!(summary.contains("p50:"));
        assert!(summary.contains("p95:"));
        assert!(summary.contains("p99:"));
        assert!(summary.contains("max:"));
    }

    #[test]
    fn format_summary_sorted_alphabetically() {
        let r = MetricsReporter::new();
        r.record("spend", Duration::from_millis(1));
        r.record("create", Duration::from_millis(1));
        r.record("read", Duration::from_millis(1));

        let summary = r.format_summary();
        let create_pos = summary.find("create").expect("should contain create");
        let read_pos = summary.find("read").expect("should contain read");
        let spend_pos = summary.find("spend").expect("should contain spend");
        assert!(create_pos < read_pos);
        assert!(read_pos < spend_pos);
    }

    #[test]
    fn percentile_value_edge_cases() {
        // Two samples
        let samples = vec![Duration::from_millis(1), Duration::from_millis(100)];
        // p50 at index floor(0.5 * 2) = 1 -> 100ms
        assert_eq!(percentile_value(&samples, 0.50), Duration::from_millis(100));
        // p0 at index floor(0.0 * 2) = 0 -> 1ms
        assert_eq!(percentile_value(&samples, 0.0), Duration::from_millis(1));
        // p99 at index floor(0.99 * 2) = 1 -> 100ms (clamped)
        assert_eq!(percentile_value(&samples, 0.99), Duration::from_millis(100));
    }

    #[test]
    fn stats_after_many_records_same_type() {
        let r = MetricsReporter::new();
        for _ in 0..1000 {
            r.record("create", Duration::from_micros(500));
        }
        let s = r.stats("create").expect("should have stats");
        assert_eq!(s.count, 1000);
        // All samples are the same, so all percentiles equal that value.
        assert_eq!(s.p50, Duration::from_micros(500));
        assert_eq!(s.p95, Duration::from_micros(500));
        assert_eq!(s.p99, Duration::from_micros(500));
        assert_eq!(s.max, Duration::from_micros(500));
    }

    #[test]
    fn duration_ms_conversion() {
        assert!((duration_ms(Duration::from_millis(1)) - 1.0).abs() < 0.001);
        assert!((duration_ms(Duration::from_micros(500)) - 0.5).abs() < 0.001);
        assert!((duration_ms(Duration::from_secs(1)) - 1000.0).abs() < 0.001);
    }

    #[test]
    fn ten_samples_percentiles() {
        let r = MetricsReporter::new();
        // 10 samples: 1, 2, 3, ..., 10
        for i in 1..=10 {
            r.record("op", Duration::from_millis(i));
        }
        let s = r.stats("op").expect("should have stats");
        assert_eq!(s.count, 10);
        // p50 = index floor(0.50 * 10) = 5 -> 6ms
        assert_eq!(s.p50, Duration::from_millis(6));
        // p95 = index floor(0.95 * 10) = 9 -> 10ms
        assert_eq!(s.p95, Duration::from_millis(10));
        // p99 = index floor(0.99 * 10) = 9 -> 10ms
        assert_eq!(s.p99, Duration::from_millis(10));
        assert_eq!(s.max, Duration::from_millis(10));
    }

    #[test]
    fn record_after_reset_works() {
        let r = MetricsReporter::new();
        r.record("create", Duration::from_millis(5));
        r.reset();
        r.record("create", Duration::from_millis(15));

        let s = r
            .stats("create")
            .expect("should have stats after reset+record");
        assert_eq!(s.count, 1);
        assert_eq!(s.p50, Duration::from_millis(15));
    }
}
