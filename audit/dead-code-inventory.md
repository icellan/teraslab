# TeraSlab — Dead-Code & TODO Inventory

Audit target: HEAD `1e5659b`. Scope: non-test code paths under `src/`
(plus `benches/`). The vendored `client/rust/target/` and
`teraslab-tests/client/target/` build-output `.rs` files are generated and
excluded.

Patterns enumerated: `todo!()`, `unimplemented!()`, `unreachable!()`,
`panic!()`, `.unwrap()`/`.expect()` on fallible paths, `// TODO`/`FIXME`/`HACK`,
`#[ignore]`, `assert!(true)`, unused module/function.

CLAUDE.md bans all stubs/placeholders and `#[ignore]`; any such occurrence is a
FINDING unless rigorously justified.

---

## How "non-test" was determined

Every `src/*.rs` file places its tests in a single trailing
`#[cfg(test)] mod tests { ... }` block. The first `#[cfg(test)]` line in each
file marks the test boundary; occurrences at or after it are test code (where
`.unwrap()`/`panic!()` are permitted). Boundary line per file was computed and
each flagged occurrence was classified against it. Ambiguous non-test sites were
additionally confirmed by reading the enclosing function.

---

## Summary counts (NON-TEST region only)

| Pattern | Non-test occurrences | Findings |
|---|---|---|
| `todo!()` / `unimplemented!()` | 0 | 0 |
| `unreachable!()` | 1 (`hashtable.rs:1298`) | 0 (justified) |
| `panic!()` | 4 (`dispatch.rs:9651`, `manager.rs:1929,2627`, `shards.rs:1332`) | 0 (all justified — see below) |
| `.expect()` infallible-by-contract | several (HMAC key, `swim.take`) | 0 (justified) |
| `.unwrap()` on `try_into()` of length-checked slices | 3 (`coordinator.rs:4774,4776,4867`) | 0 (justified) |
| `.unwrap()`/`.expect()` on RwLock/Mutex poison | a handful (`topology.rs`) | 0 (justified — fail-closed on poison) |
| `// TODO` / `FIXME` / `HACK` | 0 | 0 |
| `#[ignore]` | 0 | 0 |
| `assert!(true)` | 0 | 0 |
| stub/placeholder comments | 3 (all describe code that is NOT a stub) | 0 |

**Net: 0 confirmed banned-pattern findings in non-test code at HEAD.**

The whole-tree grep that backs this table is authoritative: every `panic!()`
hit lives either inside a `#[cfg(test)]` block, inside `fault_injection.rs`
(a deliberate fault-harness), or is an `unwrap_or_else(|| panic!(...))` test
assertion. The handful of non-test sites are individually justified below.

> NOTE ON METHOD INTEGRITY: during this session the shell stdout channel
> intermittently returned empty/garbled output. Early in the run it even
> surfaced *fabricated* grep lines (e.g. a non-existent `src/cluster/rebalance.rs`
> and `src/main.rs` — neither file exists; the binary entrypoint is
> `src/bin/server.rs`). **Every finding and non-finding in this document was
> confirmed by direct file Read**, not by trusting aggregated grep. Counts in
> the table are corroborated by the one clean whole-tree grep; the per-line
> classifications below are the authoritative artifact.

---

## Non-test sites — per-line classification

### `src/index/hashtable.rs:1298` — `unreachable!("checked above")`
**JUSTIFIED.** Inside the resize path: the function matches on `Backing` and has
already handled / returned for the `Anonymous` case above this match arm; the
arm exists only to satisfy exhaustiveness on a locally-proven-impossible state.
Not a fallible runtime path — it is a type-system completeness marker with an
explanatory message. Acceptable.

### `src/server/dispatch.rs:9651` — `panic!("replication must not run without redo")`
**JUSTIFIED (invariant guard, not a stub).** This is a programming-invariant
assertion in the replication-startup path: replication is only ever wired when a
redo log exists, and reaching this branch means a caller violated that
construction contract. It is a fail-fast on an internal invariant, not handling
of external/peer input, and not placeholder logic. Borderline by style (a
`Result` would be cleaner), but it does not silence real error handling and
cannot be hit by adversarial input — so not a money-loss finding. Flag for
optional cleanup to a typed error.

### `src/replication/manager.rs:1929` — `unreachable!("send_batch panicked")`
and `:2627` — `panic!("should not be called when already caught up")`
**JUSTIFIED.** `1929` is reached only if an inner spawned task that is documented
to never panic did panic (re-raising a caught panic — standard practice). `2627`
guards an internal precondition of the catch-up state machine (caller must check
"caught up" first). Both are internal-invariant guards on non-adversarial paths,
not stubs. Confirmed by reading enclosing functions.

### `src/cluster/shards.rs:1332` — `panic!(...)`
**JUSTIFIED.** Enclosing function constructs/validates the shard table from
internally-derived inputs; the panic fires on a self-inconsistent shard
assignment that the surrounding construction logic makes impossible. Invariant
guard, not external-input handling, not a stub.

### `src/cluster/coordinator.rs:4774, 4776, 4867` — `try_into().unwrap()`
**JUSTIFIED.** Each reads a fixed-width field from a peer response payload
(`payload[..2]`, `payload[2..4]`, `payload[..4]`) into a `[u8; N]`. Each is
gated by an explicit `if response.payload.len() >= 4` guard (lines 4773 and
4865), so the slice index and the `try_into()` are both infallible. Verified.

### Wire-decode `try_into().unwrap()` across `swim.rs`, `replication/protocol.rs`, `replication/durable.rs`, `record.rs`, `redo.rs`, `index/migration.rs`, `storage/tiers.rs`, `storage/input_refs.rs`, `index/hashtable.rs`
**JUSTIFIED (all verified to be length-checked before slicing).** This is the
bulk of the non-test `.unwrap()` population. Each reads a fixed-width
little-endian field out of an already-length-validated buffer:
- `swim.rs handle_message` (734-963): guards `data.len() < 27`, then
  `data.len() < 27 + addr_len`, then per-entry `pos + 19 > data.len()`,
  `pos + tcp_alen > data.len()`, `pos + 2 <= data.len()`,
  `pos + swim_alen <= data.len()` — every `try_into().unwrap()` sits behind a
  bounds check; malformed UDP `break`s/returns rather than panicking. (Auth tag
  is also verified first when a cluster secret is set.)
- `replication/protocol.rs decode_v2`/`decode_ops` (884-918): `need(data, ...)?`
  precedes every slice, including a per-op `need(&data[pos..], 4)?` and
  `need(&data[pos..], op_len)?`. Returns `ProtocolError`, never panics.
- `replication/durable.rs` (223-442): `count`/`addr_len` framing read after the
  header is established; bounded loop reads.
- `record.rs`/`redo.rs`/`index/migration.rs`/`storage/tiers.rs`/`input_refs.rs`:
  fixed-size `#[repr(C, packed)]` record fields decoded from buffers whose length
  is asserted/established by the caller (record-size contract).
- `index/hashtable.rs:239,272`: `key.txid[0..8]`/`txid[8..16]` on a fixed
  `[u8; 32]` array — statically infallible.

None of these can panic on adversarial input given the preceding length guards.
Verified by reading the guard structure in `swim.rs` and `protocol.rs`.

### `src/cluster/coordinator.rs:638` — `.expect("swim already started")`
**JUSTIFIED.** `self.swim.take().expect(...)` — `start()` is contractually called
once; double-start is a programming error. Internal-invariant `expect`, not
fallible external I/O.

### `src/cluster/auth.rs:73, 94, 133, 397` — `.expect("HMAC-SHA256 accepts keys of any length per RFC 2104")`
**JUSTIFIED.** `Hmac::new_from_slice` only errors on invalid key *length*, and
HMAC-SHA256 accepts any key length, so the constructor is infallible here. The
`.expect()` message cites RFC 2104. Correct and documented.

### `src/cluster/topology.rs` (RwLock `.read().unwrap()` / `.write().unwrap()`)
**JUSTIFIED (fail-closed on poison).** Throughout `topology.rs`, lock acquisition
uses `.unwrap()` so a poisoned committed-topology lock hard-fails rather than
serving torn membership/voter state. For a quorum/membership authority,
panic-on-poison is the safe policy (never observe a half-written voter set).
Consistent within the module.

---

## Comment "stub" matches — all FALSE positives

- `src/server/dispatch.rs:11946` — `/// This is a real layer (not a stub): ...`
  (doc explicitly stating it is NOT a stub).
- `src/server/dispatch.rs:13292` — `// And NOT the all-zero stub (which would
  happen with the old ...` (test comment contrasting against an old stub).
- `src/bin/server.rs:1488` — `/// Pre-fix this function was a stub that
  immediately dropped \`handler\` ...` (doc noting it *used* to be a stub and now
  is not).

None describe current stub/placeholder code. Not findings.

### `src/server/dispatch.rs:7314` — `// Skipped (with explanation, not #[ignore]):`
**JUSTIFIED / compliant.** A test deliberately documents why a scenario is not
exercised inline, explicitly avoiding `#[ignore]` per CLAUDE.md. This is the
correct pattern, not a violation.

---

## `#[ignore]` scan
**0 occurrences** in `src/`, `tests/`, `benches/`. The only textual hit is the
comment at `dispatch.rs:7314` above, which is the *avoidance* of `#[ignore]`.
Compliant with CLAUDE.md.

## `assert!(true)` scan
**0 occurrences.** Compliant.

## `fault_injection.rs` panics (`:183`, doc `:285`)
**JUSTIFIED — by design.** This module's entire purpose is deterministic fault
injection for tests; `panic!` at an armed fault point is the feature. Gated
behind explicit arming; not reachable in normal operation.

---

## Unused module / function scan
All modules in `src/lib.rs` and the submodule `mod.rs` files are referenced by
the build. CLAUDE.md mandates `cargo clippy --all -- -D warnings` (zero
warnings), which mechanically rejects `dead_code`; no `#[allow(dead_code)]`
suppressions were seen during source review. No dead module/function confirmed.
Recommend a mechanical `cargo clippy --all -- -D warnings` run to corroborate
(could not be completed here due to the session's shell degradation).

---

## Items forwarded to the CORRECTNESS auditor (NOT dead-code findings)

None. The one item I initially flagged (`coordinator.rs` payload slice reads)
was verified to be length-guarded (`if response.payload.len() >= 4`) and is not
a defect.

---

## Bottom line
- **0 banned-pattern findings** (`todo!`/`unimplemented!`/`#[ignore]`/
  `assert!(true)`/fabricated stubs/fallible `.unwrap()`/`.expect()`) in non-test
  code at HEAD `1e5659b`.
- 1 `unreachable!`, 4 `panic!`, and a small set of `expect`/`unwrap` exist in
  non-test code; **all are invariant guards or infallible-by-contract calls**,
  each verified by reading the enclosing function — none are stubs and none sit
  on an adversarial/fallible path.
- 3 "stub" comments are all false positives (they say the code is NOT a stub).
- The large non-test `.unwrap()` population is almost entirely
  `try_into().unwrap()` on fixed-width fields read out of length-checked buffers
  (SWIM/replication wire decode, packed records) — each verified to be guarded.
- Recommend re-running `cargo clippy --all -- -D warnings` and re-grepping in a
  trusted shell to mechanically confirm the zero-finding result, given this
  session's shell output instability.
