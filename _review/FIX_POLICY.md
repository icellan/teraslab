# Fix policy — read before applying any review-derived fix

Operator constraints (from the user, 2026-05-16):

1. **Scope** — fix all 216 findings. INFOs are not bugs; for an INFO that is a deployment assumption or a positive verification, the "fix" is to either (a) record it in `docs/DEPLOYMENT_ASSUMPTIONS.md` (create if missing), or (b) mark it as `# Verified` in a code comment at the cited site, or (c) leave it untouched if it is already a positive verification of correct code.

2. **Threat model for the TCP data port (3300)** — **TRUSTED OVERLAY.** Fail-open default is intentional for single-node demos. Do NOT change behaviour to reject startup when `cluster_secret = None`. Instead:
   - When the daemon starts and detects multi-node configuration (RF>1 OR cluster membership >1) WITHOUT a `cluster_secret`, emit a prominent `tracing::warn!` at boot with `target = "teraslab::security"` saying inter-node frames will be accepted unauthenticated.
   - Add an opt-in `--strict-auth` CLI flag / `strict_auth = true` config field that, when set, makes `cluster_secret = None` a startup error in multi-node configurations.
   - Add a `docs/DEPLOYMENT_ASSUMPTIONS.md` page documenting the trusted-overlay assumption. The HTTP observability port (9100) has the same assumption: it must be private or operator-authenticated.

3. **Wire and on-disk formats** — FREE TO BUMP. Pre-prod. No deployed clusters to migrate. Add header bytes / version fields / new opcodes as needed for cleanest fix. Bump on-disk magic / version numbers when the layout changes; update recovery code to reject prior magics with a clear "version not supported" error rather than silently misparsing.

4. **Cargo dependencies** — small well-known crates are OK. Add `ctrlc` or `signal-hook` (signal handling), `hmac` (replace hand-rolled HMAC over SHA-256), `validator` or hand-rolled config-range checks. Each new dep gets a one-line justification comment in `Cargo.toml` alongside the existing comments (see `subtle` for the precedent).

## Discipline rules (apply to every fix)

- **CLAUDE.md absolute rules still apply**: no `todo!()`, no `unimplemented!()`, no `#[ignore]` on tests, no `unwrap()` / `expect()` in lib code, no stub Ok(()) returns. Real implementations only.
- **Tests gate every fix**. For each finding you fix: (a) write a failing test that reproduces the issue (or extend an existing test to cover it), (b) verify it fails on `main`, (c) apply the fix, (d) confirm the test passes, (e) confirm `cargo test --all` still passes. If you cannot write a test (e.g., race condition that only manifests under specific timing), document the rationale in the commit message.
- **Each fix is its own commit**. Conventional Commits format. Message starts `fix(<scope>): <F-G?-NNN> short title`. Body explains the *why*.
- **No commit attribution to AI**. Per the user's CLAUDE.md, never add Co-Authored-By, "Generated with Claude", or any AI attribution to commit messages. Author is Siggi only.
- **No drive-by fixes**. Stick to the finding. If you spot something unrelated, file a follow-up note in `_review/follow_ups.md`; do not bundle it.
- **Surgical**. Match existing style, error patterns, test patterns. Don't refactor adjacent code.
- **No silent error swallowing in your fix**. If a fix to one finding introduces a new `let _ =` or bare `tracing::warn` over a Result, you have just opened a new copy of F-X-002.
- **Style checks**: after each fix, run `cargo fmt --all` and `cargo clippy --all-targets -- -D warnings`. Both must be clean before you commit.

## Ownership matrix (parallel-agent rule)

Each fixer agent owns ONE group's files exclusively. The "shared touch points" column lists files where two groups might both have findings; those go to the **primary** owner; other groups must coordinate via this orchestrator (do NOT touch them).

| Group | Owned files | Primary cross-cutting touch points it must coordinate |
|-------|-------------|--------------------------------------------------------|
| G1 | `src/device.rs`, `src/io.rs`, `src/record.rs`, `src/allocator.rs`, `src/device_io/*`, `src/locks.rs`, `src/fault_injection.rs` | — |
| G2 | `src/ops/*` (engine.rs + sub-paths) | F-X-007 stripe-lock contract — coordinate with G1 (the read APIs live in io.rs) |
| G3 | `src/index/*` | — |
| G4 | `src/recovery.rs`, `src/redo.rs`, `src/checkpoint.rs` | F-X-006 (replay ordering) — coordinate with G7 |
| G5 | `src/protocol/*`, `src/server/dispatch.rs`, **`src/server/mod.rs`** (auth gate lives here — G5 owns the file; G7 and G8 must coordinate) | F-X-001 cluster_secret → see policy item 2 above (docs + WARN + `--strict-auth`) |
| G6 | `src/server/http.rs`, `src/server/startup.rs`, `src/observability/*`, `src/metrics.rs` | F-X-004 admin auth — gate `/admin/top`, `/ws/top` behind same bearer middleware as writes; `/health/ready` becomes real readiness |
| G7 | `src/replication/*` | F-X-006 redo-append vs engine-apply ordering — coordinate with G4 |
| G8 | `src/cluster/*` | F-X-009 cluster auth — coordinate with G5 (HMAC verifier in server/mod.rs) |
| G9 | `src/storage/*` | — |
| G10 | `src/bin/server.rs`, `src/bin/cli.rs`, `src/config.rs`, `src/lib.rs`, `Cargo.toml` | Lifecycle (signal handler), config validation, deps, docs page (`docs/DEPLOYMENT_ASSUMPTIONS.md`) |

## Output protocol

Each fix agent writes a per-group progress log at `_review/04_fixes_G<N>.md` with one entry per finding:

```
### F-G<N>-NNN — <FIXED | DEFERRED | NOT-APPLICABLE | NEEDS-ORCHESTRATOR>
- Commit: <sha or "pending">
- Files changed: list
- Test added/extended: `tests/<path>::<test_name>` (or "covered by existing X")
- Notes: 1–3 lines, what was changed and why this matches the finding's recommendation.
```

States:
- **FIXED** — code change committed, test added, cargo build + clippy + test clean.
- **DEFERRED** — finding is touched by a file outside agent's ownership; left for orchestrator (list the file).
- **NOT-APPLICABLE** — INFO/positive verification — no code change needed; documented in `docs/DEPLOYMENT_ASSUMPTIONS.md` if a deployment assumption.
- **NEEDS-ORCHESTRATOR** — fix needs a design decision the agent can't make alone (e.g. new opcode); orchestrator picks up.

After all per-group agents return, the orchestrator: handles all NEEDS-ORCHESTRATOR and DEFERRED items; runs `cargo test --all`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`; commits any cross-cutting changes; writes `_review/05_summary.md` with final status of every finding (FIXED | DEFERRED-FOLLOWUP | NOT-APPLICABLE).
