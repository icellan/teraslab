# Pattern A — post-partition-heal master-route miss (findings)

## TL;DR

The read-verification barrier (`wait_for_migration_reads_ready`) works as
designed: it samples the scenario's tracked txids after
`wait_migrations_complete` returns, reads them back, and only returns once
every master-routed read succeeds and replicas are present. It reliably
catches the same failure mode users see in the flake reports.

However, in scenario 08 (minority-isolation → partition → heal), the barrier
surfaces a stable, non-transient gap: ~2–3% of records remain unreadable via
their new master even after 5 minutes of polling. The cluster state does not
drift; it is stuck. This is **not** a timing issue that longer waits will
resolve. Surfacing it as a test-timeout fails the P1 acceptance criterion
(10/10 passes), so I am stopping Pattern A here for discussion before
escalating.

Pattern B (client pinned to the minority side) is a separate real bug,
reliably reproducible, and my fix for it is in place and working. With
Pattern B fixed the scenario now gets far enough to expose Pattern A.

## Reproduction

```
TERASLAB_TEST_TIMING=1 cargo test --manifest-path client/Cargo.toml \
    --release --test scenario_08_network_partition -- --nocapture
```

With the Pattern A barrier (`wait_for_migration_reads_ready`) wired in after
`wait_migrations_complete`, the run reliably fails at `[8a.3]` with:

```
migration read verify timeout after 300s:
  master_failed=29/1200, under_replicated=4/50 (min_replicas=2, nodes=[1, 2, 3]);
  first_failures=[
    txid_prefix=19845511c2cc holders_via_local_read=2/3 |
    txid_prefix=5d527783f4c3 holders_via_local_read=2/3 |
    txid_prefix=35c67c54012a holders_via_local_read=2/3
  ]
```

Key observations from the diagnostic:

- `master_failed` holds stable at 29/1200 for the full 300-second window —
  not decreasing, not changing.
- The three txids probed by the diagnostic report `2/3` holders via
  `FLAG_LOCAL_READ`. The records physically exist on two nodes.
- `under_replicated=4/50` — a handful of sampled records have only **one**
  holder via local read, even though replication_factor=2. Under-replication
  is a real correctness issue, not just read routing.
- Partition map is stable (`version=4`, all three nodes present, no dangling
  masters).

So for each failing record we have: data exists on two of three nodes, the
third node has nothing, the client routes reads to that third node, and
nothing about the cluster changes for five minutes.

## Why the server returns `TX_NOT_FOUND` instead of redirecting

`src/server/dispatch.rs:2688` (`handle_get_batch`) decides what to serve
based on `cluster.is_master(&key)`, `cluster.is_migrating_outbound(&key)`,
and `cluster.has_pending_inbound(&key)`:

```rust
if !is_master && !is_migrating_out {
    // return REDIRECT to the current master
}
if is_master
   && engine.read_metadata(&key).is_err()
   && cluster.has_pending_inbound(&key)
{
    // return ERR_MIGRATION_IN_PROGRESS so client retries
}
// fall through → TX_NOT_FOUND
```

In our stuck state:

- Old holders (nodes 1 and 2) are no longer master for the shard, so they
  send the client a `REDIRECT` pointing at the new master.
- New master (node 3, say) has no data for the key and
  `has_pending_inbound(&key)` returns **false**, so it falls through to
  `TX_NOT_FOUND` rather than `MIGRATION_IN_PROGRESS`.

That is the exact contract the dispatch code expects: if inbound is not
pending, the master is assumed to have the data. The contract is being
violated.

## The underlying bug — server-side, not test-side

Either:

1. **Migration accounting lies.** The coordinator marks the shard migration
   complete (decrements `inbound_pending`, clears the pending-inbound flag
   on the receiver) before the record-level data has actually been applied
   to the receiver's index. After that point there is no visible work left,
   so `wait_migrations_complete` and the barrier both poll indefinitely
   while the affected records stay orphaned on the old holders.
2. **Shard-table publish is eager.** The new shard assignment is pushed to
   clients and peers before the migration is initiated (or while it is
   still draining), and some records on that shard silently fall off the
   migration queue.
3. **Replication failure during partition.** While node3 was isolated, some
   of the 200 writes to the majority had their replica ack time out but
   still committed to master only (RF=2 requirement satisfied with 1 of 2
   replicas? — unclear without reading the replication path). These would
   present as under-replicated after heal regardless of migration.

The `2/3` local-read count for master-route failures argues against (3) for
most of them (both surviving nodes still have copies). The `1/3` count on a
few sampled records does point to (3) for that subset — replication ack
was lost and the record never made it to a second node.

Either way this is a **real post-heal inconsistency** in the cluster, not a
test-side timing issue that a longer barrier window will clear. The
coordinator, replication, or migration-completion accounting is publishing
false readiness.

## What is in the commit

- `wait_for_migration_reads_ready` helper in `tests/common/mod.rs`. Probes
  every tracked txid via the normal client route (the exact read path the
  test will exercise), and probes a sample via `FLAG_LOCAL_READ` across all
  live nodes to detect replica-lag vs. master-miss independently. Explicit
  timeout error with prefix `migration read verify timeout` and the first
  few failing-record prefixes + observed holder counts for diagnosis.
- Called from scenarios 05, 08 (8a/8b/8d), 11, 13 at the points where the
  scenarios previously read records right after a migration-complete signal.
- Barrier timeout left at **60 seconds** in every call site except 8b/8d
  where 60 is already generous. Not 300 seconds — I am deliberately **not**
  escalating the timeout to hide the server bug above. If the server is
  healthy the barrier returns in ~1 second (confirmed on runs where data
  actually converges).

The Pattern B work (separate commit) replaces the main client in scenario
08 with a majority-only-seed client after partitioning, which is the reason
the scenario now gets far enough to surface Pattern A at all. That fix is
independently correct and should be kept.

## Suggested next steps (for discussion, not yet implemented)

1. While the scenario is in the stuck state, query
   `/admin/migration_status` and `/admin/shards` (or the equivalent) on
   every node and dump which shards each node thinks it is master of +
   which inbound migrations are pending. Compare with the failing txids'
   shards. This distinguishes "accounting lied" from "shard-table
   published too early".
2. Grep for `has_pending_inbound`, `inbound_pending`, `complete_migration`
   (or similar) in `src/cluster/coordinator.rs` and check the ordering:
   is the pending-inbound flag cleared **before** the receiver confirms
   every record has been applied, or after?
3. For the `1/3` under-replicated records, investigate the partition-time
   write path: does `create_batch` succeed with only one replica's ack
   ever, or is that a bug in the seed-records partial-retry loop?
4. Re-run with `TERASLAB_DEBUG_SHARDS=1` (env var visible in
   `helpers.rs::ensure_compose_file`) and collect server logs to see what
   the coordinator actually did during the heal.

Once the server bug is understood, the barrier is already in place and
will clear the scenario automatically.

## Files

- `tests/common/mod.rs` — `wait_for_migration_reads_ready`,
  `create_client_subset`, `assert_client_excludes_nodes`
- `tests/scenario_08_network_partition.rs` — barrier calls at 8a.3, 8b.3,
  8d.3; Pattern B client rebuild at 8a.1
- `tests/scenario_11_large_transactions.rs` — barrier at 11.10
- `tests/scenario_05_node_recovery_catchup.rs` — barrier at 5.2
- `tests/scenario_13_data_migration_under_load.rs` — barrier at 13.2

## Scenario 11 (scale-up) shows the same signature

Running scenario 11 (large-transactions scale-up from 3 → 4 nodes) with the
same barrier wired in:

```
migration read verify timeout after 60s:
  master_failed=3/10, under_replicated=6/10 (min_replicas=2, nodes=[1, 2, 3, 4]);
  first_failures=[
    txid_prefix=88e3993c56c6 holders_via_local_read=1/4 |
    txid_prefix=e7abef5602aa holders_via_local_read=2/4 |
    txid_prefix=bbf35f9fa7c9 holders_via_local_read=1/4
  ]
```

- 3 of 10 large records fail master-route — same panic the existing test
  produces at `scenario_11_large_transactions.rs:760` ("large record 3
  should be accessible after migration").
- **6 of 10** large records are under-replicated. For records up to 50 MiB
  that is a very different ratio than the ~0.3% under-replication on
  scenario 08 (50 records × ≤2 under-replicated / 1200 population ≈ 0.3%).
  That difference suggests replication of large blobs is timing out
  silently during migration — a replica timeout of 3s is unlikely to be
  enough to ship a 50 MiB write.
- Condition is stable; does not resolve with additional waiting.

So Pattern A reproduces on scale-up migrations too, and the large-record
failure rate points strongly at replication timeouts for the blob payload
rather than (or in addition to) the migration accounting race described
above.

## Evidence log (excerpted)

```
[8a] === Minority isolation sub-scenario ===
...
[8a.2] OK -- created 200 records during partition      (after Pattern B fix)
[8a.3] Healing partition on all nodes
  wait_for_migration_reads_ready: master_failed=29/1200, under_replicated=4/50 after 2.6s
  wait_for_migration_reads_ready: master_failed=29/1200, under_replicated=4/50 after 60.0s
  wait_for_migration_reads_ready: master_failed=29/1200, under_replicated=4/50 after 120.0s
  wait_for_migration_reads_ready: master_failed=29/1200, under_replicated=4/50 after 180.0s
  wait_for_migration_reads_ready: master_failed=29/1200, under_replicated=4/50 after 240.0s
  wait_for_migration_reads_ready: master_failed=29/1200, under_replicated=4/50 after 300.0s
...
thread panicked: migration read verify timeout after 300s:
  master_failed=29/1200, under_replicated=4/50
  first_failures=[txid_prefix=19845511c2cc holders_via_local_read=2/3 | ...]
```
