# Pattern D — scenario 10 verifier/cluster disagreement (findings)

## Resolution

**Fixed** in the client (and scenario 10 workload + reconciliation
cleanup). See "Recommended fix" section — the recommendation has been
implemented; the TDD regression test
`spend_batch_populates_successes_on_full_success` lives in
`client/rust/src/lib.rs`, and scenario 10 now passes without the
reconciliation block. See the commit
`client: synthesize spend_batch successes on STATUS_OK (pattern D fix)`
for the full change set.

## Verdict

**(a) — the spends are actually succeeding on the cluster, but the client's
successful-spend signal is empty by construction, and the scenario-10
workload interprets "no successes reported" as "spend was not applied" and
skips updating its verifier.**

The verifier is correct. The cluster is correct. The reconciliation at the
end of scenario 10 (`teraslab-tests/client/tests/scenario_10_sustained_load.rs:586`)
papers over a straightforward client-API / test-usage bug. The recommended
fix is **client-side** (details below), and the reconciliation step should
then be removed — the scenario should pass without it.

## Minimal repro

Any successful `Client::spend_batch` call where the full batch succeeds.
That is the common case in scenario 10, so you see it on every successful
spend:

```rust
match client.spend_batch(&params, &[spend_item]).await {
    Ok(resp) => {
        // resp.successes is empty, resp.errors is empty.
        if !resp.successes.is_empty() { spends_ok += 1; ... }
        //  ^^^ always false → spends_ok is never incremented, and the
        //  verifier never hears about the spend.
    }
    ...
}
```

In scenario 10's final metrics this appears as `spends: 0 ok, 27546 err`
— the `err` count is legitimate (real failures + redirects + partials),
but the `ok` count is always zero because every fully-successful spend
lands in the `Ok(resp)` arm with an empty `successes` vector, and the
scenario's check `!resp.successes.is_empty()` drops it silently.

## Why `successes` is empty on a fully successful spend

### Server side: `src/server/dispatch.rs:1505-1515`

```rust
if errors.is_empty() {
    let status = if repl_outcome.is_degraded() { STATUS_DEGRADED_DURABILITY }
                 else { STATUS_OK };
    ResponseFrame { request_id, status, payload: vec![] }  // empty body
} else {
    ResponseFrame { request_id, status: STATUS_PARTIAL_ERROR,
                     payload: encode_sparse_errors(&errors) }
}
```

Fully successful spend → `STATUS_OK` with an empty payload. Per-item
success indices are **not** encoded. They were never on the wire.

### Client side: `client/rust/src/lib.rs:262-313` (`handle_signal_response`)

```rust
STATUS_OK => {
    if !resp.payload.is_empty() {
        // decode signals (set_mined etc.)
    } else {
        Ok(SpendBatchResponse { successes: Vec::new(), errors: Vec::new() })
    }
}
STATUS_PARTIAL_ERROR => {
    let errs = decode_sparse_errors(&resp.payload)?;
    Err(ClientError::Partial(PartialError {
        successes: Vec::new(),   // <-- also empty
        errors: errs,
    }))
}
```

Two separate paths end up with empty `successes`:

- `Ok(SpendBatchResponse { successes: [], errors: [] })` on a totally
  successful batch.
- `Err(ClientError::Partial { successes: [], errors })` on any partial
  failure. The implicit "the items not listed in `errors` succeeded"
  information is only reconstructable by the caller.

### The test-side confusion

`tests/scenario_10_sustained_load.rs:248-277` handles both paths. The
`Err(ClientError::Partial)` arm correctly infers success from the absence
of the item's index in `pe.errors`:

```rust
Err(ClientError::Partial(ref pe)) => {
    let item_failed = pe.errors.iter().any(|e| e.item_index == 0);
    if !item_failed { spends_ok += 1; verifier.record_spend(..); }
    else            { spends_err += 1; }
}
```

But the `Ok(resp)` arm guards on `!resp.successes.is_empty()`, which is
**always false** for spend (because the server returns empty payload on
full success). So every fully-successful spend silently drops from both
`spends_ok` and the verifier's spent-utxos tracking.

## ALREADY_SPENT as a non-error

While investigating I also confirmed the idempotent-retry path in
`src/ops/engine.rs:820-823`:

```rust
UTXO_SPENT => {
    if slot.spending_data == item.spending_data {
        continue;   // idempotent: skipped, not added to errors
    }
    if slot.spending_data == [FROZEN_BYTE; 36] { ... Frozen ... }
    errors.insert(item.idx, SpendError::AlreadySpent { ... });
}
```

So `ALREADY_SPENT` means "already spent **by a different transaction**"
(spending_data mismatch) — a real race / conflict, not an idempotent
retry. The idempotent case silently skips (no error, no counter
increment). This is correct engine behaviour.

The `"retry after all-items-failed partial error [ALREADY_SPENT=1]"`
log line in the scenario-10 transcript is the generic retry in
`client/rust/src/lib.rs:877` firing when *every* item in a single-item
batch came back as `ALREADY_SPENT`. Given how scenario-10 picks vouts at
random from the workload's accumulated `created_snapshot`, this is
expected: by the time the workload is running at 2000 spends/sec, a good
chunk of selected `(txid, vout)` pairs have already been spent by a
previous iteration. That error is a real race, not a mistaken retry.

## Why the reconciliation papers this over

`scenario_10_sustained_load.rs:586-625` walks every tracked txid in the
verifier after the workload stops, reads actual state from the cluster,
and pushes the cluster's observed `spent_utxos` back into the verifier.
Since the cluster has the *true* spend state (it did apply them), this
works — but it also erases any real divergence that would indicate a
bug. The scenario then asserts `post_recon_mismatches == 0`, which is a
tautology at that point.

## Recommended fix (client-side, one place)

In `client/rust/src/lib.rs`'s `handle_signal_response` and/or the
`spend_batch_cluster` / `spend_batch` wrappers, populate the `successes`
vector on `STATUS_OK` so callers get a complete per-item picture. The
request items are known at the call site, so the wrapper can synthesise
`(0..N)` success indices when the server's payload is empty.

With that in place:

- Scenario 10's `Ok(resp)` arm becomes: `spends_ok += resp.successes.len()`
  (or simply always-increment — the only way to reach `Ok` is with no
  errors). Remove the `!resp.successes.is_empty()` guard.
- The reconciliation block at `scenario_10_sustained_load.rs:586-625`
  can be deleted. The scenario should then pass with genuine zero
  mismatches at the final check.
- Every other caller of `spend_batch` that currently checks
  `resp.successes.is_empty()` benefits automatically.

Alternative fixes (not recommended):

- **Server-side**: add success signals to the `STATUS_OK` spend_batch
  payload. Increases wire size for the common all-success case; the
  client already has the information it needs.
- **Test-side**: change the `Ok(resp)` arm to count success
  unconditionally without fixing the API. Every future caller hits the
  same trap.

## Is the cluster spend idempotent?

Yes, for same `spending_data`. A client retrying a previously-successful
spend with identical `spending_data` gets the idempotent `continue`
branch — no error, no double-apply, no duplicate counting. The
`spent_count` in the response reflects only newly-applied spends. This
means the client-side "retry after all-items-failed" loop is safe on
spend: it will not double-count, regardless of whether the first attempt
actually applied the operation.

## Files referenced

- `teraslab-tests/client/tests/scenario_10_sustained_load.rs:248-277`
  (workload spend dispatch / counter increments)
- `teraslab-tests/client/tests/scenario_10_sustained_load.rs:586-625`
  (the reconciliation block to remove after the client fix)
- `client/rust/src/lib.rs:262-313` (`handle_signal_response` — where the
  client fix lands)
- `client/rust/src/lib.rs:523-720` (`spend_batch_cluster` — alternative
  location for `successes` synthesis)
- `client/rust/src/lib.rs:877` (the `retry after all-items-failed` log
  line that mystified the investigation; it is firing on a real race,
  not a misclassified success)
- `src/ops/engine.rs:820-823` (idempotent `UTXO_SPENT` path)
- `src/server/dispatch.rs:1356-1524` (`handle_spend_batch` — where the
  empty-payload `STATUS_OK` originates)

## Not fixing yet

Per the task brief: diagnosis only for Pattern D. Flagging the client-side
fix above as the recommended path; happy to implement it in a follow-up
commit once you want me to.
