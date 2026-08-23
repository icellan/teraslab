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
//!     topology authority.
//!
//! (2) is the most important property in the design. It is what makes the
//! probe repair-only: `retained_exchange_view`'s documented single writer is
//! the exchange-completion arm, and a second writer would let a background
//! repair query silently redirect master election.
//!
//! Same enforcement shape as `tracing_lint.rs`: read the production source and
//! assert on it.
//!
//! # Why these scan BLOCKS, not fixed-length windows
//!
//! The first cut of this file used fixed forward windows (`the next 400 chars
//! after this anchor`). A later, unrelated hoist moved a definition 1002 chars
//! ahead of its anchor, and the assertion silently degraded from "the probe
//! sources its members from the topology authority" to "the string
//! `committed_members` appears somewhere nearby" — which the exact W11
//! address-book-widening defect this file names in its own failure message
//! satisfies. **The test did not fail. It stopped checking.**
//!
//! That is the failure mode source-scanning tests are prone to, so the scan is
//! now bounded by the enclosing BLOCK (via indentation, which rustfmt makes
//! reliable and which no brace inside a string or comment can fool), and every
//! lookup goes through [`require`]/[`refuse`], which panic with a re-anchor
//! message when an anchor no longer resolves. A drift must be LOUD.

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
    match src.find("#[cfg(test)]\nmod tests {") {
        Some(idx) => &src[..idx],
        None => panic!(
            "coordinator.rs no longer has the expected top-level `#[cfg(test)] mod tests` \
             marker — these source invariants can no longer separate production code from \
             tests and must be re-anchored"
        ),
    }
}

/// Indentation width of `line` (spaces only; the file is rustfmt-normalised).
fn indent_of(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// The block introduced by the line containing `anchor`: that line plus every
/// following line indented deeper, plus the line that closes it.
///
/// Bounded by INDENTATION rather than a character count, so the scan follows
/// the code when it moves instead of sliding off the end of it, and rather
/// than by brace matching, so a `{` inside a tracing message or a comment
/// cannot unbalance it.
///
/// Panics — loudly, with a re-anchor instruction — when `anchor` is missing or
/// not unique. A structural test whose anchor has dissolved must fail, never
/// quietly pass over nothing.
fn block_at(prod: &str, anchor: &str) -> String {
    let occurrences = prod.matches(anchor).count();
    assert_eq!(
        occurrences, 1,
        "source-invariant anchor {anchor:?} matched {occurrences} times in production code \
         (expected exactly 1). The code it pins has moved or been duplicated — RE-ANCHOR this \
         test against the new site. Do not delete it: it is the only check on a property no \
         runtime test can reach."
    );
    let lines: Vec<&str> = prod.lines().collect();
    let start = lines
        .iter()
        .position(|l| l.contains(anchor))
        .unwrap_or_else(|| panic!("anchor {anchor:?} vanished between count and scan"));
    let base = indent_of(lines[start]);

    // The construct's header may span several lines — a multi-line call in an
    // `if`, and that call may itself take a multi-line CLOSURE whose `|| {`
    // ends in a brace at a DEEPER indent. The opener we want is the header's
    // last line: the first line ending in `{` at the anchor's OWN
    // indentation.
    //
    // (The first cut matched any line ending in `{` and latched onto exactly
    // such a closure the moment one was introduced. It failed loudly and
    // named the cause, which is the point — but the heuristic was wrong.)
    let open = (start..lines.len())
        .find(|i| {
            let l = lines[*i];
            l.trim_end().ends_with('{') && indent_of(l) == base
        })
        .unwrap_or_else(|| {
            panic!(
                "anchor {anchor:?} is not followed by a block opener at its own indentation \
                 — RE-ANCHOR this test"
            )
        });

    let mut out = String::new();
    for line in &lines[start..=open] {
        out.push_str(line);
        out.push('\n');
    }
    let mut closed = false;
    for line in &lines[open + 1..] {
        out.push_str(line);
        out.push('\n');
        if !line.trim().is_empty() && indent_of(line) <= base {
            // The line that closes the block — included, then stop.
            closed = true;
            break;
        }
    }
    assert!(
        closed,
        "the block opened at anchor {anchor:?} never closed at or below its own indentation \
         — the indentation-bounded scan cannot delimit it, so RE-ANCHOR this test rather \
         than let it assert over the rest of the file"
    );
    out
}

/// Assert `block` contains `needle`, attributing the failure to `why`.
fn require(block: &str, needle: &str, why: &str) {
    assert!(
        block.contains(needle),
        "{why}\n\nexpected to find {needle:?} in:\n{block}"
    );
}

/// Assert `block` does NOT contain `needle`, attributing the failure to `why`.
fn refuse(block: &str, needle: &str, why: &str) {
    assert!(
        !block.contains(needle),
        "{why}\n\nfound forbidden {needle:?} in:\n{block}"
    );
}

/// The probe's launch block, from its gate to its closing brace.
fn probe_launch_block(prod: &str) -> String {
    block_at(prod, "if probe_launch_admissible(")
}

/// The probe's drain block, where a collected view is turned into a pass.
fn probe_drain_block(prod: &str) -> String {
    block_at(
        prod,
        "while let Ok(probe_view) = probe_view_rx.try_recv() {",
    )
}

/// #95 review P2-4 (1) — the exchange-completion site must ARM the driver.
///
/// The arm lives just after the single assignment to `retained_exchange_view`.
/// Reverting it to the bare `event_repair_trigger.observe(...)` — which
/// self-gates on `under_replication_sweep_enabled` — silently removes the
/// driver's exchange arm ENTIRELY, so arming
/// `under_replication_repair_enabled` (default OFF since W15) would do
/// nothing, and every other test stays green. The flag is a qualification
/// switch, so a mechanism it can no longer reach is the #95 defect back.
#[test]
fn the_exchange_completion_site_arms_the_repair_driver() {
    let src = coordinator_src();
    let prod = production_only(&src);

    let block = block_at(prod, "*retained_exchange_view_event.lock() =");
    // Landmark: proves the scan still covers the repair-arming region. If the
    // arming moved elsewhere entirely, this fires with a re-anchor message
    // rather than the (misleading) defect message below.
    require(
        &block,
        "event_repair_trigger",
        "the retained-view writer's block no longer mentions `event_repair_trigger` — the \
         #95 exchange arm has MOVED. Re-anchor this test against its new site rather than \
         letting the scan pass over code that no longer contains the property.",
    );
    require(
        &block,
        "arm_exchange_repair(",
        "the exchange-completion site must call `arm_exchange_repair` — without it the arm \
         self-gates on `under_replication_sweep_enabled` (default OFF), so arming \
         `under_replication_repair_enabled` reaches nothing and the driver is unqualifiable \
         (#95, CI 32084447959 / 32630545533)",
    );
}

/// #95 review P2-4 (2) — THE central safety property: the probe's view is
/// repair-only.
///
/// `retained_exchange_view` feeds the election assignment provider and is
/// documented as having exactly ONE writer. `exchange_complete_tx` activates a
/// topology. A probe view reaching either would turn a read-only background
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
        "`retained_exchange_view` must keep exactly ONE writer (found {writes}). It is the \
         election assignment provider's input; a second writer — especially the #95 repair \
         probe — would let a background repair query redirect master election."
    );

    // (b) the probe's view is never the value assigned to it.
    for forbidden in [
        "*retained_exchange_view_event.lock() = probe_view",
        "*retained_exchange_view.lock() = probe_view",
    ] {
        assert!(
            !prod.contains(forbidden),
            "the probe view must never be stored as the retained exchange view (found \
             `{forbidden}`): the probe is REPAIR-ONLY"
        );
    }

    // (c) neither the LAUNCH nor the DRAIN routes into activation or the
    // retained view. Round-2 P3: the drain was previously unscanned, so a
    // write added there would have gone unnoticed by (b)'s exact-string check
    // if it were spelled any other way.
    for (name, block) in [
        ("launch", probe_launch_block(prod)),
        ("drain", probe_drain_block(prod)),
    ] {
        require(
            &block,
            "probe_",
            "the probe {name} block no longer mentions the probe — RE-ANCHOR this test",
        );
        for forbidden in [
            "exchange_tx.send(",
            "exchange_complete_tx.send(",
            "retained_exchange_view",
        ] {
            refuse(
                &block,
                forbidden,
                &format!(
                    "the probe {name} block must never touch `{forbidden}`: sending into the \
                     exchange-completion channel ACTIVATES a topology, and writing the \
                     retained view feeds master election. A repair probe does neither."
                ),
            );
        }
    }

    // The launch does send — on its own private channel.
    require(
        &probe_launch_block(prod),
        "probe_tx.send(",
        "the probe collection must report on its PRIVATE channel",
    );
}

/// #95 review P2-4 (3) — the probe's member set comes from the COMMITTED
/// topology authority, filtered to SWIM-alive, and from nowhere else.
///
/// Sourcing it from the address book instead is a mistake this codebase has
/// already shipped once: W11's `catch_up_fallback_proposal` widened membership
/// from the address book and resurrected a quiesced node into a live term.
///
/// ROUND-2 FINDING: the first cut of this test asserted `contains
/// ("committed_members")` inside a 400-char window after the `probe_members`
/// definition. A P2-2 hoist then moved the real source 1002 chars ahead of
/// that window, leaving the assertion matching a local variable NAME — and
/// replacing its definition with `node_addrs.read().keys().copied().collect()`
/// passed silently. It now pins the AUTHORITY CALL, inside the block, and
/// refuses the address book by name.
#[test]
fn the_probe_queries_only_alive_committed_members() {
    let src = coordinator_src();
    let prod = production_only(&src);
    let block = probe_launch_block(prod);

    require(
        &block,
        "let probe_members: Vec<NodeId> = ",
        "the probe's member set construction is no longer inside the launch block — \
         RE-ANCHOR this test",
    );
    require(
        &block,
        "topo_authority_event.committed_members()",
        "the probe's member set must come from the COMMITTED TOPOLOGY AUTHORITY. Matching \
         the local variable name is not enough — that is exactly how this assertion was \
         defanged once already, while the W11 address-book-widening defect it names passed \
         straight through it.",
    );
    require(
        &block,
        "alive.contains",
        "the probe's member set must be filtered to SWIM-alive members: querying a dead peer \
         burns the exchange timeout to learn nothing the dead-peer guard does not already \
         enforce",
    );
    refuse(
        &block,
        "node_addrs.read()",
        "the probe must never source members from the ADDRESS BOOK — W11's \
         `catch_up_fallback_proposal` did exactly that and resurrected a quiesced node into \
         a live term",
    );
}
