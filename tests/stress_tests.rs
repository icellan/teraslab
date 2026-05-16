//! Stress test entry point for Phase 12.
//!
//! These tests exercise the system under sustained load to surface rare bugs.

mod stress;

/// Run random operations with 8 threads, verify consistency periodically.
#[test]
fn stress_random_operations_8_threads() {
    stress::stress_random_operations();
}

/// Fill device to high capacity, then churn (create + delete),
/// verify no fragmentation death spiral.
#[test]
fn stress_device_fill_and_churn() {
    stress::stress_device_fill_and_churn();
}

#[test]
fn stress_set_mined_reorg_churn() {
    stress::stress_set_mined_reorg_churn();
}

#[test]
fn stress_mark_longest_chain_reorg_churn() {
    stress::stress_mark_longest_chain_reorg_churn();
}

#[test]
fn stress_reassign_churn() {
    stress::stress_reassign_churn();
}

#[test]
fn stress_set_conflicting_churn() {
    stress::stress_set_conflicting_churn();
}

#[test]
fn stress_preserve_until_churn() {
    stress::stress_preserve_until_churn();
}
