# TeraSlab — Purpose-Built UTXO Store for BSV Teranode

## Project overview

TeraSlab is a purpose-built Rust database server that serves as the UTXO store backend for BSV Teranode. It exploits fixed, known workload patterns to target 10M+ ops/sec sustained throughput, 10-50x less SSD wear, and dramatically better tail latency than general-purpose databases.

Read `specs/SPEC_BRIEFING.md` for the full architecture analysis and rationale. Read `specs/teranode.lua` for the current Lua UDF implementation being replaced.

## Parallel-agent worktrees — required cleanup

Parallel fix/review work dispatches Agent-tool sub-agents in isolated `git worktree` checkouts under `.claude/worktrees/agent-<id>/`. Each carries its own `target/` directory (~2-4 GB). They accumulate fast — a single full review+fix campaign can leave 25+ stale worktrees totalling 60-80 GB.

**The harness does not clean them up.** Run `scripts/cleanup-worktrees.sh` after every multi-agent dispatch:

```bash
# Remove all agent worktrees:
scripts/cleanup-worktrees.sh

# Keep specific in-flight agents (pass their IDs from the dispatch result):
scripts/cleanup-worktrees.sh <agent_id_1> <agent_id_2>

# Also delete orphan agent branches (refs are kept by default for inspection):
scripts/cleanup-worktrees.sh --branches

# Also cargo-clean the main target/ directory (~50 GB recovery):
scripts/cleanup-worktrees.sh --target
```

Anything that orchestrates parallel agents must end its turn by calling this script. The script is idempotent and safe — it only touches `.claude/worktrees/agent-*` and never the main checkout.

## Project structure

```
teraslab/
├── CLAUDE.md                          ← You are here
├── specs/
│   ├── SPEC_BRIEFING.md               ← Architecture analysis (read-only)
│   ├── BSV_UTXO_STORE_SPEC.md         ← Formal specification (exists, refined)
│   ├── BSV_UTXO_STORE_RUST_CRATES.md  ← Crate recommendations (exists, refined)
│   └── teranode.lua                   ← Current Lua UDF (reference)
├── phases/
│   ├── 00_analysis_and_spec.md
│   ├── 01_storage_layout.md
│   ├── 02_index.md
│   ├── 03_spend_path.md
│   ├── 04_setmined_path.md
│   ├── 05_creation_path.md
│   ├── 06_remaining_ops.md
│   ├── 07_crash_safety.md
│   ├── 08_replication.md
│   ├── 09_clustering.md
│   ├── 10_wire_protocol.md
│   ├── 11_tiered_storage.md
│   ├── 12_integration.md
│   ├── 13_admin_tooling.md
│   └── 14_backup.md
└── src/                               ← Rust source (you build this)
```

## Build phases

Complete phases IN ORDER. Each phase is defined in `phases/NN_name.md`. Do not start a phase until all previous phases are complete with all tests passing.

## Absolute rules — violations are never acceptable

### No stubs, no placeholders, no skipping

```
BANNED PATTERNS — do not write any of these under any circumstances:

  todo!()
  unimplemented!()
  panic!("not yet")
  panic!("not implemented")
  panic!("TODO")
  #[ignore] on any test
  assert!(true)  (vacuous assertion)
  Empty test function bodies
  Tests that only assert .is_ok() without checking the returned value
  Tests that only assert .is_err() without checking the error variant
  Functions that return Ok(()) without doing real work
  Comments saying "// simplified for now" or "// stub"
```

If you catch yourself wanting to write any of these, STOP. Implement the real logic. If the real logic depends on a later phase, design an interface/trait boundary now and implement it fully against a test double (not a stub — a real in-memory implementation).

### Test-first development

Within each phase, follow this order strictly:

1. Write the types and structs
2. Write ALL the tests specified in the phase (they will fail — this is correct)
3. Implement the logic until all tests pass
4. Run the full test suite (`cargo test --all`) to confirm zero regressions

### Checkpoint protocol

Before starting any phase:

```bash
cargo test --all 2>&1 | tail -30
```

Paste the output. If any test fails, fix it before proceeding.

After completing any phase:

```bash
cargo test --all 2>&1 | tail -30
cargo test --all 2>&1 | grep -E "test result|FAILED"
```

Confirm: zero failures, zero ignored tests.

### Code quality

- All public functions must have doc comments explaining behavior, parameters, and error conditions
- All error types must be enums with descriptive variants — no string errors
- Use `thiserror` for error derivation
- No `unwrap()` or `expect()` in library code (only in tests)
- All byte layout structs must be `#[repr(C, packed)]` with compile-time size assertions
- Use `unsafe` only for raw device I/O and memory-mapped index — isolate it behind safe APIs
- Run `cargo clippy --all -- -D warnings` after each phase — zero warnings allowed

### Naming conventions

- Crate name: `teraslab`
- Module names match the phase topics: `device`, `record`, `allocator`, `index`, `ops`, `locks`, `redo`, `replication`, `cluster`, `protocol`, `storage` (the `uring` module was removed 2026-05-28 — I/O is synchronous O_DIRECT via `device`)
- Test modules inside each source file: `#[cfg(test)] mod tests { ... }`
- Integration tests in `tests/` directory for cross-module tests

## Phase execution

### Phase 0 (validation — no code)

Start here. Read `phases/00_analysis_and_spec.md`. The formal specification and crate recommendations already exist:

- `BSV_UTXO_STORE_SPEC.md` — the formal specification (already refined)
- `BSV_UTXO_STORE_RUST_CRATES.md` — recommended Rust crates (already refined)

Phase 0 validates these existing documents against the actual Teranode source repos. Clone the repos, trace the Go code, and produce a `SPEC_VALIDATION_REPORT.md` flagging any gaps, discrepancies, or missing operations. If amendments to the spec or phase files are needed, list them explicitly in the report for review before implementation begins.

### Phase 1+ (implementation)

After Phase 0 is complete and reviewed, proceed to Phase 1. Read `phases/01_storage_layout.md` and implement it. Continue through phases in order.
