#[allow(dead_code)]
mod common;

use std::time::Duration;
use teraslab_test_client::{Client, ClientError};
use teraslab_test_client::verifier::StateVerifier;
use teraslab_test_client::types::*;

use teraslab::protocol::codec::encode_get_batch;
use teraslab::protocol::opcodes::{FLAG_LOCAL_READ, OP_GET_BATCH, STATUS_OK};

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
    txid.iter().take(8).map(|b| format!("{b:02x}")).collect::<String>()
}

/// Read a batch of txids from a specific node, bypassing shard routing via
/// FLAG_LOCAL_READ. Returns `(status, raw_payload)`.
async fn direct_get(
    client: &Client,
    node_addr: &str,
    txids: &[[u8; 32]],
) -> Result<(u8, Vec<u8>), ClientError> {
    let payload = encode_get_batch(FIELD_ALL, txids);
    client.send_to_addr(node_addr, OP_GET_BATCH, FLAG_LOCAL_READ, payload).await
}

/// Compare two get_batch payloads ignoring the `updated_at` timestamp field.
///
/// The `updated_at` field (8 bytes at response offset 70) differs between
/// master and replica because each node sets it to local time when the
/// operation is applied. All other fields should be byte-identical.
fn payloads_match(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_copy = a.to_vec();
    let mut b_copy = b.to_vec();
    // Zero out updated_at (bytes 70..78) for comparison
    if a_copy.len() >= 78 {
        a_copy[70..78].fill(0);
        b_copy[70..78].fill(0);
    }
    a_copy == b_copy
}

/// For a given txid with 3 nodes and RF=2, determine which nodes hold the record.
/// Checks the per-item status inside the response payload, not the frame status.
async fn find_holders(
    client: &Client,
    node_addrs: &[String],
    txid: &[u8; 32],
) -> Result<(Vec<usize>, Vec<usize>), ClientError> {
    let mut holders = Vec::new();
    let mut non_holders = Vec::new();
    for (i, addr) in node_addrs.iter().enumerate() {
        let (frame_status, payload) = direct_get(client, addr, &[*txid]).await?;
        if frame_status == STATUS_OK && !payload.is_empty() {
            // Decode per-item results: [count:4][items: status(1)+data_len(4)+data...]
            // If count >= 1 and the first item's status is 0 (success), this node holds the record.
            if payload.len() >= 4 {
                let count = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                if count >= 1 && payload.len() >= 5 {
                    let item_status = payload[4];
                    if item_status == 0 {
                        holders.push(i);
                        continue;
                    }
                }
            }
        }
        non_holders.push(i);
    }
    Ok((holders, non_holders))
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_03_replication_correctness() {
    let result =
        tokio::time::timeout(Duration::from_secs(300), run_scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            common::teardown_all(SID).await;
            panic!("scenario failed: {e}");
        }
        Err(_) => {
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
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(180)).await?;

    // Refresh routing so the client has the latest partition map
    client.refresh_routing().await?;

    let verifier = StateVerifier::new();

    // -- Setup: seed 2000 records with 10 UTXOs each --
    let txids = common::seed_records(&client, &verifier, 2000, 10).await?;
    assert_eq!(txids.len(), 2000, "expected 2000 seeded records");

    // Wait for redo log sequences to converge (replication settled).
    common::wait_replication_settled(&docker, 3, Duration::from_secs(30)).await?;

    // ==========================================================================
    // Test 3.1: Post-seed replication verification -- check ALL 2000 records
    // ==========================================================================
    tlog!(t0, "test 3.1 start");
    eprintln!("[3.1] Verifying replication for ALL 2000 records");
    {
        let mut mismatches = 0u32;

        for (idx, txid) in txids.iter().enumerate() {
            let (holders, _non_holders) = find_holders(&client, &node_addrs, txid).await?;

            assert!(
                holders.len() == 2,
                "Test 3.1: txid at index {} is held by {} nodes, expected 2 (RF=2)",
                idx,
                holders.len()
            );

            if holders.len() == 2 {
                let (status_a, payload_a) =
                    direct_get(&client, &node_addrs[holders[0]], &[*txid]).await?;
                let (status_b, payload_b) =
                    direct_get(&client, &node_addrs[holders[1]], &[*txid]).await?;

                assert_eq!(status_a, STATUS_OK);
                assert_eq!(status_b, STATUS_OK);

                if !payloads_match(&payload_a, &payload_b) {
                    mismatches += 1;
                    if mismatches <= 3 {
                        let first_diff = payload_a.iter().zip(payload_b.iter())
                            .position(|(a, b)| a != b)
                            .unwrap_or(payload_a.len().min(payload_b.len()));
                        eprintln!("[3.1] MISMATCH on txid {}: len_a={}, len_b={}, first diff at byte {}",
                            txid_hex(txid), payload_a.len(), payload_b.len(), first_diff);
                    }
                }
            }
        }

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

        for i in 0..spend_count {
            let txid = txids[i];
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
                        txid_hex(&txid), pe.errors.len(),
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
        common::wait_replication_settled(&docker, 3, Duration::from_secs(30)).await?;

        // Check ALL 500 affected records
        let mut mismatches = 0u32;

        for (idx, txid) in spent_txids.iter().enumerate() {
            let (holders, _) = find_holders(&client, &node_addrs, txid).await?;

            assert!(
                holders.len() == 2,
                "Test 3.2: post-spend txid at index {} held by {} nodes, expected 2",
                idx,
                holders.len()
            );

            if holders.len() == 2 {
                let (_, payload_a) =
                    direct_get(&client, &node_addrs[holders[0]], &[*txid]).await?;
                let (_, payload_b) =
                    direct_get(&client, &node_addrs[holders[1]], &[*txid]).await?;

                if !payloads_match(&payload_a, &payload_b) {
                    mismatches += 1;
                    if mismatches <= 3 {
                        let first_diff = payload_a.iter().zip(payload_b.iter())
                            .position(|(a, b)| a != b)
                            .unwrap_or(payload_a.len().min(payload_b.len()));
                        eprintln!("[3.2] MISMATCH at index {}: first diff at byte {first_diff}", idx);
                    }
                }
            }
        }

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
        common::wait_replication_settled(&docker, 3, Duration::from_secs(30)).await?;

        // Check ALL 300 affected records
        let mut mismatches = 0u32;

        for (idx, txid) in mined_txids.iter().enumerate() {
            let (holders, _) = find_holders(&client, &node_addrs, txid).await?;

            assert!(
                holders.len() == 2,
                "Test 3.3: post-set_mined txid at index {idx} held by {} nodes, expected 2",
                holders.len()
            );

            if holders.len() == 2 {
                let (_, payload_a) =
                    direct_get(&client, &node_addrs[holders[0]], &[*txid]).await?;
                let (_, payload_b) =
                    direct_get(&client, &node_addrs[holders[1]], &[*txid]).await?;

                if !payloads_match(&payload_a, &payload_b) {
                    mismatches += 1;
                }
            }
        }

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
            let rec = verifier.get_record(&txid)
                .expect("Test 3.4: record should exist in verifier");
            let freeze_item = FreezeItem {
                txid,
                vout: 0,
                utxo_hash: rec.utxo_hashes[0],
            };
            client.freeze_batch(&[freeze_item]).await
                .unwrap_or_else(|e| panic!("Test 3.4: freeze failed on index {i}: {e}"));
            verifier.record_freeze(txid, 0);
            affected_txids.push(txid);
        }

        // Unfreeze vout=0 on the first 20 of those frozen records
        for i in 0..20 {
            let txid = txids[freeze_start + i];
            let rec = verifier.get_record(&txid)
                .expect("Test 3.4: record should exist in verifier");
            let freeze_item = FreezeItem {
                txid,
                vout: 0,
                utxo_hash: rec.utxo_hashes[0],
            };
            client.unfreeze_batch(&[freeze_item]).await
                .unwrap_or_else(|e| panic!("Test 3.4: unfreeze failed on index {i}: {e}"));
            verifier.record_unfreeze(txid, 0);
        }

        // Reassign vout=0 on 10 still-frozen records (indices 20-29)
        for i in 20..30 {
            let txid = txids[freeze_start + i];
            let rec = verifier.get_record(&txid)
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
            client.reassign_batch(&reassign_params, &[reassign_item]).await
                .unwrap_or_else(|e| panic!("Test 3.4: reassign failed on index {i}: {e}"));
            verifier.record_reassign(txid, 0, new_hash);
            verifier.record_unfreeze(txid, 0);
        }

        // Wait for redo log sequences to converge (replication settled).
        common::wait_replication_settled(&docker, 3, Duration::from_secs(30)).await?;

        // Verify replication on all affected records
        let mut mismatches = 0u32;
        for txid in &affected_txids {
            let (holders, _) = find_holders(&client, &node_addrs, txid).await?;
            if holders.len() == 2 {
                let (_, payload_a) =
                    direct_get(&client, &node_addrs[holders[0]], &[*txid]).await?;
                let (_, payload_b) =
                    direct_get(&client, &node_addrs[holders[1]], &[*txid]).await?;
                if !payloads_match(&payload_a, &payload_b) {
                    mismatches += 1;
                }
            }
        }

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
        common::wait_replication_settled(&docker, 3, Duration::from_secs(30)).await?;

        let mut replication_failures = 0u32;

        for (i, txid) in deleted_txids.iter().enumerate() {
            let (holders, _) = find_holders(&client, &node_addrs, txid).await?;

            if !holders.is_empty() {
                replication_failures += 1;
                eprintln!(
                    "Test 3.5: deleted txid at index {} still found on {} node(s)",
                    delete_start + i,
                    holders.len()
                );
            }
        }

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
        // Sample 200 records to check key placement
        let sample_count = 200;
        let sample_start = 1100;
        let mut placement_errors = 0u32;

        for i in 0..sample_count {
            let idx = sample_start + i;
            if idx >= 1800 {
                break; // Skip the deleted range
            }
            let txid = &txids[idx];
            let rec = verifier.get_record(txid);
            if rec.is_none() || rec.as_ref().is_some_and(|r| r.is_deleted) {
                continue;
            }

            let (holders, non_holders) = find_holders(&client, &node_addrs, txid).await?;

            // With RF=2, exactly 2 nodes should hold the record
            if holders.len() != 2 {
                placement_errors += 1;
                if placement_errors <= 3 {
                    eprintln!(
                        "[3.6] txid at index {idx} held by {} nodes, expected exactly 2",
                        holders.len()
                    );
                }
                continue;
            }

            // The third node should NOT hold the record
            if non_holders.len() != 1 {
                placement_errors += 1;
                if placement_errors <= 3 {
                    eprintln!(
                        "[3.6] txid at index {idx} has {} non-holders, expected exactly 1",
                        non_holders.len()
                    );
                }
            }
        }

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
        let rec = verifier.get_record(&idempotent_txid)
            .expect("Test 3.7: record should exist");

        // Read initial counter
        let pre_results = client.get_batch(FIELD_ALL_METADATA, &[idempotent_txid]).await?;
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
        client.spend_batch(&spend_params, &[spend_item.clone()]).await
            .unwrap_or_else(|e| panic!("Test 3.7: first spend failed: {e}"));
        verifier.record_spend(idempotent_txid, 0);

        // Second spend of the same UTXO -- should fail (ALREADY_SPENT) or be a no-op
        let second_result = client.spend_batch(&spend_params, &[spend_item]).await;
        match &second_result {
            Err(ClientError::Partial(pe)) => {
                assert!(
                    pe.errors.iter().any(|e| e.code == teraslab::protocol::opcodes::ERR_ALREADY_SPENT),
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
        common::wait_replication_settled(&docker, 3, Duration::from_secs(10)).await?;

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
                        meta.spent_utxos, pre_spent + 1,
                        "Test 3.7: node {h} counter should be {} (incremented once), got {}",
                        pre_spent + 1, meta.spent_utxos,
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
                    txid_hex(&mm.txid), mm.field, mm.expected, mm.actual,
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
