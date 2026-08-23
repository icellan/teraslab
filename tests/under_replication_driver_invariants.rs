//! #95 review P2-4 — source-level invariants of the holder-driven
//! under-replication repair driver.
//!
//! Three of the driver's load-bearing properties are STRUCTURAL — statements
//! about which code exists at which site, not about what a function returns —
//! so no runtime test can establish them:
//!
//!  1. the exchange-completion CALL SITE actually arms the driver (a unit test
//!     of `arm_exchange_repair` proves the function works, not that anything
//!     calls it);
//!  2. the probe's collected view NEVER reaches `retained_exchange_view` (the
//!     single-writer input to the election assignment provider) or
//!     `exchange_complete_tx` (which activates a topology);
//!  3. the probe never sources its members from anything but the committed
//!     set.
//!
//! (2) is the most important property in the design. It is what makes the
//! probe repair-only: `retained_exchange_view`'s documented single writer is
//! the exchange-completion arm, and a second writer would let a background
//! repair query silently redirect master election. Until this file existed,
//! it rested entirely on code reading.
//!
//! Same enforcement shape as `tracing_lint.rs`: read the production source and
//! assert on it. These tests are deliberately brittle to refactors — a rename
//! that trips them is a prompt to re-establish the invariant at the new site,
//! not to delete the test.

use std::fs;
use std::path::PathBuf;

/// The coordinator source, read from the crate root at test time.
fn coordinator_src() -> String {
    let path: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "src",
        "cluster",
        "coordinator.rs",
    ]
    .iter()
    .collect();
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Strip `#[cfg(test)]` content by cutting at the coordinator's test module.
///
/// Every invariant here is about PRODUCTION code; the test module legitimately
/// mentions the same identifiers (that is what the unit tests do), so scanning
/// the whole file would make each assertion vacuous.
fn production_only(src: &str) -> &str {
    // The file has exactly one top-level `#[cfg(test)]\nmod tests {`.
    match src.find("#[cfg(test)]\nmod tests {") {
        Some(idx) => &src[..idx],
        None => panic!(
            "coordinator.rs no longer has the expected top-level `#[cfg(test)] mod tests` \
             marker — these source invariants can no longer separate production code from \
             tests and must be re-anchored"
        ),
    }
}

/// #95 review P2-4 (1) — the exchange-completion site must ARM the driver.
///
/// The arm lives immediately after the single assignment to
/// `retained_exchange_view`. Reverting it to the bare
/// `event_repair_trigger.observe(...)` — which self-gates on
/// `under_replication_sweep_enabled` — silently removes the only
/// event-driven driver a default cluster has, and every other test stays
/// green. That is exactly the #95 defect.
#[test]
fn the_exchange_completion_site_arms_the_repair_driver() {
    let src = coordinator_src();
    let prod = production_only(&src);

    let writer = "*retained_exchange_view_event.lock() = partition_view.clone();";
    let idx = prod.find(writer).unwrap_or_else(|| {
        panic!(
            "the single writer of `retained_exchange_view` was not found; the #95 \
             exchange arm is anchored to it and must be re-anchored"
        )
    });
    // The arm is within the same block as the writer — a generous window that
    // still excludes unrelated code hundreds of lines away.
    let window = &prod[idx..prod.len().min(idx + 4000)];
    assert!(
        window.contains("arm_exchange_repair("),
        "the exchange-completion site must call `arm_exchange_repair` — without \
         it the arm self-gates on `under_replication_sweep_enabled` (default \
         OFF) and a default cluster has no holder-driven repair driver at all \
         (#95, CI 32084447959 / 32630545533)"
    );
}

/// #95 review P2-4 (2) — THE central safety property: the probe's view is
/// repair-only.
///
/// `retained_exchange_view` feeds the election assignment provider and is
/// documented as having exactly ONE writer. `exchange_complete_tx` activates
/// a topology. A probe view reaching either would turn a read-only background
/// repair query into an input to master election or an activation trigger.
#[test]
fn the_probe_view_never_reaches_the_retained_view_or_an_activation() {
    let src = coordinator_src();
    let prod = production_only(&src);

    // (a) `retained_exchange_view` still has exactly one writer.
    let writes = prod
        .matches("*retained_exchange_view_event.lock() =")
        .count()
        + prod.matches("*retained_exchange_view.lock() =").count();
    assert_eq!(
        writes, 1,
        "`retained_exchange_view` must keep exactly ONE writer (found {writes}). \
         It is the election assignment provider's input; a second writer — \
         especially the #95 repair probe — would let a background repair query \
         redirect master election."
    );

    // (b) the probe's view is never the value assigned to it.
    for forbidden in [
        "*retained_exchange_view_event.lock() = probe_view",
        "*retained_exchange_view.lock() = probe_view",
    ] {
        assert!(
            !prod.contains(forbidden),
            "the probe view must never be stored as the retained exchange view \
             (found `{forbidden}`): the probe is REPAIR-ONLY"
        );
    }

    // (c) the probe's collection never routes into the activation channel.
    // Its launch thread sends on `probe_tx` and nothing else; a send to
    // `exchange_tx`/`exchange_complete_tx` from that thread would activate a
    // topology off a repair query.
    let launch = "PartitionReportOrigin::RepairProbe,";
    let idx = prod.find(launch).unwrap_or_else(|| {
        panic!("the probe's `run_exchange_phase` call site was not found; re-anchor this test")
    });
    let window = &prod[idx..prod.len().min(idx + 600)];
    assert!(
        window.contains("probe_tx.send("),
        "the probe collection must report on its PRIVATE channel"
    );
    assert!(
        !window.contains("exchange_tx.send(") && !window.contains("exchange_complete_tx.send("),
        "the probe collection must NEVER report into the exchange-completion \
         channel — that channel ACTIVATES a topology, and a repair probe has no \
         business activating anything"
    );
}

/// #95 review P2-4 (3) — the probe's member set comes from the COMMITTED
/// topology, filtered to SWIM-alive, and from nowhere else.
///
/// Sourcing it from the address book instead is a mistake this codebase has
/// already shipped once: W11's `catch_up_fallback_proposal` widened membership
/// from the address book and resurrected a quiesced node into a live term.
#[test]
fn the_probe_queries_only_alive_committed_members() {
    let src = coordinator_src();
    let prod = production_only(&src);

    let idx = prod
        .find("let probe_members: Vec<NodeId> = ")
        .unwrap_or_else(|| panic!("the probe member set construction was not found"));
    let window = &prod[idx..prod.len().min(idx + 400)];
    assert!(
        window.contains("committed_members"),
        "the probe's member set must come from the COMMITTED topology, never \
         from the address book (the W11 membership-widening defect)"
    );
    assert!(
        window.contains("alive.contains"),
        "and it must be filtered to SWIM-alive members: querying a dead peer \
         burns the exchange timeout to learn nothing the dead-peer guard does \
         not already enforce"
    );
}
