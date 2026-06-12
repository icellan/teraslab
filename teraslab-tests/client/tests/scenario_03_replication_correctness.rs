#[allow(dead_code)]
mod common;

use std::time::Duration;
use teraslab_test_client::ClientError;
use teraslab_test_client::types::*;
use teraslab_test_client::verifier::StateVerifier;

macro_rules! tlog {
    ($t0:expr, $($arg:tt)*) => {
        if common::timing_enabled() {
            eprintln!("[{:6.1}s] {}", $t0.elapsed().as_secs_f64(), format!($($arg)*));
        }
    };
}

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 3;

/// Format a txid as a short hex prefix for assertion messages.
fn txid_hex(txid: &[u8; 32]) -> String {
    txid.iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

// Use batch helpers from common
use common::{batch_verify_replication, direct_get, find_holders};

#[tokio::test(flavor = "multi_thread")]
async fn scenario_03_replication_correctness() {
    let result = tokio::time::timeout(Duration::from_secs(300), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            common::collect_failure_diagnostics(SID).await;
            common::teardown_all(SID).await;
            panic!("scenario failed: {e}");
        }
        Err(_) => {
            common::collect_failure_diagnostics(SID).await;
            common::teardown_all(SID).await;
            panic!("scenario timed out after 300s");
        }
    }
}

async fn run_scenario() -> Result<(), ClientError> {
    let t0 = std::time::Instant::now();
    // Ensure clean state
    tlog!(t0, "teardown_all (pre-clean)...");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    let (docker, client) = common::start_3node_cluster(SID).await?;

    // The 3 host-mapped client protocol addresses for this scenario.
    let node_addrs = docker.host_client_addrs(3);

    // Wait for migrations to settle before seeding data
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(120)).await?;

    // Refresh routing so the client has the latest partition map
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    // -- Setup: seed 2000 records with 10 UTXOs each --
    let txids = common::seed_records(&client, &verifier, 2000, 10).await?;
    assert_eq!(txids.len(), 2000, "expected 2000 seeded records");

    // Wait for redo log sequences to converge (replication settled).
    common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

    // ==========================================================================
    // Test 3.1: Post-seed replication verification -- check ALL 2000 records
    // ==========================================================================
    tlog!(t0, "test 3.1 start");
    eprintln!("[3.1] Verifying replication for ALL 2000 records");
    {
        let (mismatches, holder_errors) =
            batch_verify_replication(&client, &node_addrs, &txids, true).await?;
        assert_eq!(
            holder_errors, 0,
            "Test 3.1: {holder_errors} records not held by exactly 2 nodes (RF=2)"
        );
        assert_eq!(
            mismatches, 0,
            "Test 3.1: {mismatches} replication mismatches found in ALL 2000 records"
        );
    }
    eprintln!("[3.1] OK -- all 2000 records replicated correctly");
    tlog!(t0, "test 3.1 done");

    // ==========================================================================
    // Test 3.2: 500 spends replicated -- check ALL 500 affected records
    // ==========================================================================
    tlog!(t0, "test 3.2 start");
    eprintln!("[3.2] Spending 500 UTXOs and verifying replication on ALL affected records");
    {
        let spend_count = 500;
        let mut spent_txids = Vec::with_capacity(spend_count);

        for &txid in &txids[..spend_count] {
            let rec = verifier
                .get_record(&txid)
                .expect("Test 3.2: seeded record should exist in verifier");
            let utxo_hash = rec.utxo_hashes[0];

            let params = SpendBatchParams {
                ignore_conflicting: false,
                ignore_locked: false,
                current_block_height: 100,
                block_height_retention: 1000,
            };
            let items = vec![SpendItem {
                txid,
                vout: 0,
                utxo_hash,
                spending_data: [0u8; 36],
            }];

            // Assert spend succeeds
            let result = client.spend_batch(&params, &items).await;
            match &result {
                Ok(resp) => {
                    assert!(
                        resp.errors.is_empty(),
                        "Test 3.2: spend on txid {} returned errors",
                        txid_hex(&txid),
                    );
                }
                Err(ClientError::Partial(pe)) => {
                    panic!(
                        "Test 3.2: spend on txid {} partial: {} errors",
                        txid_hex(&txid),
                        pe.errors.len(),
                    );
                }
                Err(e) => {
                    panic!("Test 3.2: spend on txid {} failed: {e}", txid_hex(&txid));
                }
            }

            verifier.record_spend(txid, 0);
            spent_txids.push(txid);
        }

        // Wait for redo log sequences to converge (replication settled).
        common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

        // Check ALL 500 affected records
        let (mismatches, holder_errors) =
            batch_verify_replication(&client, &node_addrs, &spent_txids, true).await?;
        assert_eq!(
            holder_errors, 0,
            "Test 3.2: {holder_errors} post-spend records not held by exactly 2 nodes"
        );
        assert_eq!(
            mismatches, 0,
            "Test 3.2: {mismatches} spend replication mismatches found in ALL {spend_count} records"
        );
    }
    eprintln!("[3.2] OK -- all 500 spend records replicated correctly");
    tlog!(t0, "test 3.2 done");

    // ==========================================================================
    // Test 3.3: 300 SetMined replicated -- check ALL 300 affected records
    // ==========================================================================
    tlog!(t0, "test 3.3 start");
    eprintln!("[3.3] SetMined on 300 records and verifying replication on ALL");
    {
        let mined_count = 300;
        let mined_start = 500;
        let mut mined_txids = Vec::with_capacity(mined_count);

        for i in 0..mined_count {
            let txid = txids[mined_start + i];
            let params = SetMinedBatchParams {
                block_id: 42,
                block_height: 800_000,
                subtree_idx: 0,
                on_longest_chain: true,
                unset_mined: false,
                current_block_height: 800_000,
                block_height_retention: 1000,
            };

            client.set_mined_batch(&params, &[txid]).await?;

            verifier.record_set_mined(txid);
            mined_txids.push(txid);
        }

        // Wait for redo log sequences to converge (replication settled).
        common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

        // Check ALL 300 affected records
        let (mismatches, holder_errors) =
            batch_verify_replication(&client, &node_addrs, &mined_txids, true).await?;
        assert_eq!(
            holder_errors, 0,
            "Test 3.3: {holder_errors} post-set_mined records not held by exactly 2 nodes"
        );
        assert_eq!(
            mismatches, 0,
            "Test 3.3: {mismatches} set_mined replication mismatches in ALL {mined_count} records"
        );
    }
    eprintln!("[3.3] OK -- all 300 set_mined records replicated correctly");
    tlog!(t0, "test 3.3 done");

    // ==========================================================================
    // Test 3.4: 50 freeze, 20 unfreeze, 10 reassign -- re-verify replication
    // ==========================================================================
    tlog!(t0, "test 3.4 start");
    eprintln!("[3.4] Freeze 50, unfreeze 20, reassign 10 -- verify replication");
    {
        let freeze_start = 1000;
        let mut affected_txids = Vec::new();

        // Freeze vout=0 on 50 records
        for i in 0..50 {
            let txid = txids[freeze_start + i];
            let rec = verifier
                .get_record(&txid)
                .expect("Test 3.4: record should exist in verifier");
            let freeze_item = FreezeItem {
                txid,
                vout: 0,
                utxo_hash: rec.utxo_hashes[0],
            };
            client
                .freeze_batch(&[freeze_item])
                .await
                .unwrap_or_else(|e| panic!("Test 3.4: freeze failed on index {i}: {e}"));
            verifier.record_freeze(txid, 0);
            affected_txids.push(txid);
        }

        // Unfreeze vout=0 on the first 20 of those frozen records
        for i in 0..20 {
            let txid = txids[freeze_start + i];
            let rec = verifier
                .get_record(&txid)
                .expect("Test 3.4: record should exist in verifier");
            let freeze_item = FreezeItem {
                txid,
                vout: 0,
                utxo_hash: rec.utxo_hashes[0],
            };
            client
                .unfreeze_batch(&[freeze_item])
                .await
                .unwrap_or_else(|e| panic!("Test 3.4: unfreeze failed on index {i}: {e}"));
            verifier.record_unfreeze(txid, 0);
        }

        // Reassign vout=0 on 10 still-frozen records (indices 20-29)
        for i in 20..30 {
            let txid = txids[freeze_start + i];
            let rec = verifier
                .get_record(&txid)
                .expect("Test 3.4: record should exist in verifier");
            let mut new_hash = [0u8; 32];
            new_hash[0] = 0xDE;
            new_hash[1] = i as u8;

            let reassign_params = ReassignBatchParams {
                block_height: 800_000,
                spendable_after: 0,
            };
            let reassign_item = ReassignItem {
                txid,
                vout: 0,
                utxo_hash: rec.utxo_hashes[0],
                new_utxo_hash: new_hash,
            };
            client
                .reassign_batch(&reassign_params, &[reassign_item])
                .await
                .unwrap_or_else(|e| panic!("Test 3.4: reassign failed on index {i}: {e}"));
            verifier.record_reassign(txid, 0, new_hash);
            verifier.record_unfreeze(txid, 0);
        }

        // Wait for redo log sequences to converge (replication settled).
        common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

        // Verify replication on all affected records
        let (mismatches, _) =
            batch_verify_replication(&client, &node_addrs, &affected_txids, true).await?;
        assert_eq!(
            mismatches, 0,
            "Test 3.4: {mismatches} replication mismatches after freeze/unfreeze/reassign"
        );
    }
    eprintln!("[3.4] OK -- freeze/unfreeze/reassign replicated correctly");
    tlog!(t0, "test 3.4 done");

    // ==========================================================================
    // Test 3.5: 100 deletes replicated -- verified deleted from both master AND replica
    // ==========================================================================
    tlog!(t0, "test 3.5 start");
    eprintln!("[3.5] Deleting 100 records and verifying deleted from both nodes");
    {
        let delete_count = 100;
        let delete_start = 1800;
        let mut deleted_txids = Vec::with_capacity(delete_count);

        for i in 0..delete_count {
            let txid = txids[delete_start + i];

            client.delete_batch(&[txid]).await?;

            verifier.record_delete(txid);
            deleted_txids.push(txid);
        }

        // Wait for redo log sequences to converge (replication settled).
        common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

        let (_, replication_failures) =
            batch_verify_replication(&client, &node_addrs, &deleted_txids, false).await?;
        assert_eq!(
            replication_failures, 0,
            "Test 3.5: {replication_failures} deleted records still present on at least one node"
        );
    }
    eprintln!("[3.5] OK -- all 100 deletes replicated correctly");
    tlog!(t0, "test 3.5 done");

    // ==========================================================================
    // Test 3.6: Per-shard key listing -- verify keys exist on master + replica
    //           but NOT on the third (non-holder) node
    // ==========================================================================
    tlog!(t0, "test 3.6 start");
    eprintln!("[3.6] Per-shard key listing: verify keys on master+replica, not third node");
    {
        // Sample 200 records to check key placement (skip the deleted range 1800-1899)
        let sample_txids: Vec<[u8; 32]> = (1100..1300)
            .filter(|&idx| idx < 1800)
            .filter_map(|idx| {
                let txid = &txids[idx];
                let rec = verifier.get_record(txid);
                if rec.is_none() || rec.as_ref().is_some_and(|r| r.is_deleted) {
                    None
                } else {
                    Some(*txid)
                }
            })
            .collect();

        let (_, placement_errors) =
            batch_verify_replication(&client, &node_addrs, &sample_txids, true).await?;
        assert_eq!(
            placement_errors, 0,
            "Test 3.6: {placement_errors} key placement errors (not on exactly 2 of 3 nodes)"
        );
    }
    eprintln!("[3.6] OK -- keys exist on exactly master+replica, not third node");
    tlog!(t0, "test 3.6 done");

    // ==========================================================================
    // Test 3.7: Idempotent spend -- send same spend twice, counter increments once
    //           on both nodes
    // ==========================================================================
    tlog!(t0, "test 3.7 start");
    eprintln!("[3.7] Idempotent spend: same spend twice, counter increments once");
    {
        // Pick a record that hasn't been spent yet
        let idempotent_txid = txids[1500];
        let rec = verifier
            .get_record(&idempotent_txid)
            .expect("Test 3.7: record should exist");

        // Read initial counter
        let pre_results = client
            .get_batch(FIELD_ALL_METADATA, &[idempotent_txid])
            .await?;
        let (pre_meta, _) = TxMetadata::decode(FIELD_ALL_METADATA, &pre_results.item(0).data)?;
        let pre_spent = pre_meta.spent_utxos;

        let spend_params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 100,
            block_height_retention: 1000,
        };
        let spend_item = SpendItem {
            txid: idempotent_txid,
            vout: 0,
            utxo_hash: rec.utxo_hashes[0],
            spending_data: [0u8; 36],
        };

        // First spend -- should succeed
        client
            .spend_batch(&spend_params, std::slice::from_ref(&spend_item))
            .await
            .unwrap_or_else(|e| panic!("Test 3.7: first spend failed: {e}"));
        verifier.record_spend(idempotent_txid, 0);

        // Second spend of the same UTXO -- should fail (ALREADY_SPENT) or be a no-op
        let second_result = client.spend_batch(&spend_params, &[spend_item]).await;
        match &second_result {
            Err(ClientError::Partial(pe)) => {
                assert!(
                    pe.errors
                        .iter()
                        .any(|e| e.code == teraslab::protocol::opcodes::ERR_ALREADY_SPENT),
                    "Test 3.7: second spend should return ERR_ALREADY_SPENT"
                );
            }
            Err(ClientError::Server { code, .. }) => {
                assert_eq!(*code, teraslab::protocol::opcodes::ERR_ALREADY_SPENT);
            }
            Ok(_) => {
                // Some implementations silently succeed idempotently -- that's OK
            }
            Err(e) => {
                panic!("Test 3.7: unexpected error on second spend: {e}");
            }
        }

        // Wait for redo log sequences to converge (replication settled).
        common::wait_replication_settled(&docker, 3, Duration::from_secs(5)).await?;

        // Verify counter incremented exactly once on both nodes
        let (holders, _) = find_holders(&client, &node_addrs, &idempotent_txid).await?;
        assert_eq!(holders.len(), 2, "Test 3.7: record should be on 2 nodes");

        for &h in &holders {
            let (_, payload) = direct_get(&client, &node_addrs[h], &[idempotent_txid]).await?;
            // Parse the per-item response to get metadata.
            // Response layout: [count:4][status:1][data_len:4][data...]
            // The metadata is in the data portion.
            if payload.len() >= 9 {
                let item_status = payload[4];
                assert_eq!(item_status, 0, "Test 3.7: item should be found on node {h}");
                let data_len = u32::from_le_bytes(payload[5..9].try_into().unwrap()) as usize;
                if payload.len() >= 9 + data_len && data_len >= ALL_METADATA_SIZE {
                    let (meta, _) = TxMetadata::decode(FIELD_ALL, &payload[9..9 + data_len])?;
                    assert_eq!(
                        meta.spent_utxos,
                        pre_spent + 1,
                        "Test 3.7: node {h} counter should be {} (incremented once), got {}",
                        pre_spent + 1,
                        meta.spent_utxos,
                    );
                }
            }
        }
    }
    eprintln!("[3.7] OK -- idempotent spend: counter incremented once on both nodes");
    tlog!(t0, "test 3.7 done");

    // ==========================================================================
    // Test 3.8: Full replication check using verify_consistency()
    //
    // LIMITATION: verify_consistency only performs routed reads (through the
    // cluster's normal shard-routing layer), so it validates logical
    // correctness but does NOT directly compare master vs. replica data on
    // each node. A proper replication verification would need a
    // `verify_replication` function that does FLAG_LOCAL_READ to each
    // node and compares payloads (similar to what tests 3.1-3.5 do for
    // individual operations). That is a larger change left for future work.
    // ==========================================================================
    tlog!(t0, "test 3.8 start");
    eprintln!("[3.8] Full consistency check using verify_consistency()");
    {
        let mismatches = common::verify_consistency(&client, &verifier).await?;

        if !mismatches.is_empty() {
            for mm in mismatches.iter().take(10) {
                eprintln!(
                    "Test 3.8 MISMATCH: txid {} field={} expected={} actual={}",
                    txid_hex(&mm.txid),
                    mm.field,
                    mm.expected,
                    mm.actual,
                );
            }
        }

        assert!(
            mismatches.is_empty(),
            "Test 3.8: verify_consistency found {} mismatches",
            mismatches.len(),
        );
    }
    eprintln!("[3.8] OK -- full consistency check passed: zero mismatches");
    tlog!(t0, "test 3.8 done");

    // Teardown
    tlog!(t0, "teardown_all (final)...");
    common::teardown_all(SID).await;
    tlog!(t0, "teardown_all done");

    tlog!(t0, "=== SCENARIO COMPLETE ===");
    Ok(())
}
