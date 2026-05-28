# TeraSlab module layout convention

External review (re-review P2, May-2026) flagged that
`src/server/dispatch.rs` (~13.5k lines), `src/ops/engine.rs` (~11.3k
lines), and `src/cluster/coordinator.rs` (~10.4k lines) are
god-files that absorb new feature work instead of being split into
per-opcode / per-concern shells. Per-op shells exist but new code
keeps landing in the monoliths.

This document is the project's convention for where new code goes,
and the rationale for keeping the existing per-op shells alive.

## Where new feature code MUST land

When you add an opcode, a new mutation, a new replication
concern, or a new admin endpoint, route it through the per-concern
shell file. Do NOT add to the god-files directly.

### Op-handler convention

For an op `OP_FOO_BATCH`:

- **Engine state mutation** belongs in `src/ops/foo.rs`. New
  `pub fn foo(req: &FooRequest) -> Result<FooResponse, SpendError>`
  goes here. Cross-cutting helpers like
  `read_metadata_fast`, `write_slot_fast` stay in `engine.rs`.

- **Wire codec** belongs in `src/protocol/codec.rs` (or a new
  `src/protocol/codec_foo.rs` for very large per-op encoders).

- **TCP dispatch arm** in `src/server/dispatch.rs` is the THINNEST
  possible adapter: decode payload → call the engine method →
  encode response. No business logic. If the arm grows past
  ~30 lines, factor the body into a `handle_foo(...)` function in
  a new module under `src/server/dispatch/foo.rs` and add a
  `mod foo;` line to `dispatch.rs`. Anchor opcode → handler
  mapping at the top of `dispatch.rs`.

- **HTTP admin endpoint** in `src/server/http.rs`: handler bodies
  live in `src/server/http/<area>.rs` once the file grows past
  ~30 lines. Same shell pattern.

- **Cluster control plane**: new coordinator concerns
  (topology, migration, swim) live in per-concern files under
  `src/cluster/`. New code in `coordinator.rs` is reserved for
  cross-concern orchestration.

### Why the convention exists

1. **Review cost.** A 13.5k-line file is a review blocker — diffs
   land in the wrong place and the wrong reviewer is paged.
2. **Build incrementality.** Changes inside a small file rebuild
   the small file; changes inside `dispatch.rs` rebuild a 13.5k-
   line crate-internal module.
3. **Discoverability.** New contributors grep for `OP_FOO` and
   expect to find `dispatch::foo::handle_foo`. Instead they find
   it three hundred lines into a switch arm.
4. **Test isolation.** Each shell module's `#[cfg(test)] mod tests`
   exercises only that shell, not the whole god-file's worth of
   side imports.

## What's already split

The convention is already partially observed:

- `src/ops/{create,spend,unspend,delete_eval,set_mined,remaining,engine,error}.rs` —
  per-op (or per-op-group) shells.
- `src/protocol/{codec,frame,opcodes}.rs` — clean split.
- `src/replication/{durable,manager,protocol,receiver,tcp_transport}.rs` —
  clean split.
- `src/cluster/{auth,coordinator,membership,migration,shards,swim,topology}.rs` —
  per-concern shells (though `coordinator.rs` is itself
  oversized — see below).

## What's still pending

The three god-files have NOT been split because the existing
contents are tightly coupled and the split is multi-day
refactor work. As of 2026-05-28 the convention applies to NEW
code only:

| File | Current size | Action |
|---|---|---|
| `src/server/dispatch.rs` | ~13.7k | NEW per-op handlers MUST go into a sibling `src/server/dispatch/<op>.rs` shell. Existing arms stay until a coherent group is ready to move. |
| `src/ops/engine.rs` | ~11.3k | NEW pub methods MUST go into a sibling `src/ops/<concern>.rs` shell. Existing impl block stays. |
| `src/cluster/coordinator.rs` | ~10.4k | NEW orchestration MUST go into a sibling `src/cluster/<concern>.rs` shell. |

## Enforcement

The CI workflow (`/.github/workflows/ci.yml`) includes a per-PR
check that any file exceeding 1000 lines NEW (post-PR) MUST
either (a) shrink, (b) be a known god-file with a documented
exemption (the three above), or (c) include a `// MODULE_LAYOUT:
god-file exemption` comment block explaining the justification.

The check is intentionally advisory — sometimes a single large
data table or a single state machine is genuinely indivisible.
But the reviewer must explicitly acknowledge the exemption in
the PR description.

## When the god-files are split

Each split lands as its own commit (no mixed feature + split
work). The split commit body MUST include:

- File-by-file LOC delta.
- The new module's public surface.
- Confirmation that `cargo clippy --lib -- -D warnings` and
  `cargo test --lib` pass.

After the split, this document is updated to remove the
exemption.
