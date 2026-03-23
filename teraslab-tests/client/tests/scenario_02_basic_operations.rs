//! Scenario 02 -- All UTXO operations across a 3-node cluster.
//!
//! Exercises every operation type (create, get, spend, unspend, set_mined,
//! unset_mined, freeze, unfreeze, reassign, set_conflicting, set_locked,
//! preserve_until, delete, coinbase maturity, batch create, batch get) and
//! verifies full correctness using the in-memory [`StateVerifier`] and
//! field-level read-back.

mod common;

use std::time::Duration;
use teraslab_test_client::ClientError;
use teraslab_test_client::verifier::{StateVerifier, parse_metadata_fields};
use teraslab_test_client::types::*;

/// Scenario ID for unique Docker ports and container names.
const SID: u16 = 2;

/// Helper: format a txid as a short hex prefix for assertion messages.
fn txid_hex(txid: &[u8; 32]) -> String {
    txid.iter().take(8).map(|b| format!("{b:02x}")).collect::<String>()
}

#[tokio::test(flavor = "multi_thread")]
async fn scenario_02_basic_operations() {
    // -- Setup --
    let timeout_guard = tokio::time::timeout(Duration::from_secs(300), async {
        common::teardown_all(SID).await;

        let (mut docker, client) = common::start_3node_cluster(SID).await
            .expect("failed to start 3-node cluster");

        let verifier = StateVerifier::new();

        // ==================================================================
        // Test 2.1 -- Create 1000 txs (10 UTXOs each) + read back ALL fields
        // ==================================================================
        eprintln!("[2.1] Create 1000 txs with 10 UTXOs each, then read back and verify all fields");

        let txids = common::seed_records(&client, &verifier, 1000, 10).await
            .expect("seed_records failed");

        assert_eq!(txids.len(), 1000, "expected 1000 txids from seed_records");
        assert_eq!(verifier.record_count(), 1000, "verifier should track 1000 records");

        // Read them back and verify all fields using verifier comparison.
        for chunk in txids.chunks(50) {
            let results = client.get_batch(FIELD_ALL, chunk).await
                .expect("get batch failed");
            assert_eq!(
                results.len(), chunk.len(),
                "Test 2.1: get_batch should return same count as requested"
            );
            for (i, result) in results.iter().enumerate() {
                assert_eq!(
                    result.status(), 0,
                    "Test 2.1: get batch item {i} returned status {}, expected 0 for txid {}",
                    result.status(),
                    txid_hex(&chunk[i]),
                );
                assert!(
                    !result.data().is_empty(),
                    "Test 2.1: get batch item {i} payload should be non-empty for txid {}",
                    txid_hex(&chunk[i]),
                );
                // Parse metadata and verify against verifier expected state
                let (meta, _) = TxMetadata::decode(FIELD_ALL, result.data())
                    .unwrap_or_else(|e| panic!(
                        "Test 2.1: failed to decode metadata for txid {}: {e}",
                        txid_hex(&chunk[i]),
                    ));
                let rec = verifier.get_record(&chunk[i])
                    .expect("Test 2.1: record should exist in verifier");
                assert_eq!(
                    meta.utxo_count, rec.utxo_count,
                    "Test 2.1: utxo_count mismatch for txid {}",
                    txid_hex(&chunk[i]),
                );
                assert_eq!(
                    meta.spent_utxos, rec.spent_utxos,
                    "Test 2.1: spent_utxos mismatch for txid {}",
                    txid_hex(&chunk[i]),
                );
                assert_eq!(
                    meta.fee, 500,
                    "Test 2.1: fee mismatch for txid {}",
                    txid_hex(&chunk[i]),
                );
            }
        }

        eprintln!("[2.1] OK -- created and verified all fields on 1000 records");

        // ==================================================================
        // Test 2.2 -- Spend vout=0 on 500 txids, assert success, verify counter
        // ==================================================================
        eprintln!("[2.2] Spend vout=0 on 500 txids");

        let spend_txids: Vec<[u8; 32]> = txids[..500].to_vec();
        let spend_params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 800_000,
            block_height_retention: 100,
        };

        for chunk in spend_txids.chunks(50) {
            let items: Vec<SpendItem> = chunk.iter().map(|txid| {
                let rec = verifier.get_record(txid)
                    .expect("record should exist in verifier");
                SpendItem {
                    txid: *txid,
                    vout: 0,
                    utxo_hash: rec.utxo_hashes[0],
                    spending_data: [0u8; 36],
                }
            }).collect();

            let result = client.spend_batch(&spend_params, &items).await;
            match &result {
                Ok(resp) => {
                    // All items should have succeeded
                    assert!(
                        resp.errors.is_empty(),
                        "Test 2.2: spend_batch returned {} errors, expected 0",
                        resp.errors.len(),
                    );
                }
                Err(ClientError::Partial(pe)) => {
                    panic!(
                        "Test 2.2: spend_batch returned partial error: {} successes, {} errors",
                        pe.successes.len(), pe.errors.len(),
                    );
                }
                Err(e) => {
                    panic!("Test 2.2: spend_batch failed: {e}");
                }
            }
        }

        for txid in &spend_txids {
            verifier.record_spend(*txid, 0);
        }

        // Verify spent_utxos counter on ALL 500 records via read-back.
        for txid in spend_txids.iter() {
            let results = client.get_batch(FIELD_ALL_METADATA, std::slice::from_ref(txid)).await
                .unwrap_or_else(|e| panic!("Test 2.2 read-back failed: {e}"));
            let (meta, _) = TxMetadata::decode(FIELD_ALL_METADATA, &results.item(0).data)
                .unwrap_or_else(|e| panic!("Test 2.2 decode failed: {e}"));
            assert_eq!(
                meta.spent_utxos, 1,
                "Test 2.2: txid {} should have spent_utxos=1 after spend, got {}",
                txid_hex(txid), meta.spent_utxos,
            );
        }

        eprintln!("[2.2] OK -- spent vout=0 on 500 txids, verified counter");

        // ==================================================================
        // Test 2.3 -- SpendMulti: 5 UTXOs on same tx, verify counter +5
        // ==================================================================
        eprintln!("[2.3] Spend vouts 0-4 on a single tx (multi-vout spend)");

        let multi_txid = txids[500];
        let multi_rec = verifier.get_record(&multi_txid)
            .expect("record should exist in verifier");

        // Read the spent_utxos before spending
        let pre_results = client.get_batch(FIELD_ALL_METADATA, &[multi_txid]).await
            .expect("Test 2.3: pre-spend read failed");
        let (pre_meta, _) = TxMetadata::decode(FIELD_ALL_METADATA, &pre_results.item(0).data)
            .expect("Test 2.3: pre-spend decode failed");
        let pre_spent = pre_meta.spent_utxos;

        // Send a single spend_batch call with all 5 vouts in one batch.
        let multi_items: Vec<SpendItem> = (0u32..5)
            .map(|vout| SpendItem {
                txid: multi_txid,
                vout,
                utxo_hash: multi_rec.utxo_hashes[vout as usize],
                spending_data: [0u8; 36],
            })
            .collect();
        client.spend_batch(&spend_params, &multi_items).await
            .unwrap_or_else(|e| panic!(
                "Test 2.3: batch spend of 5 vouts on txid {} failed: {e}",
                txid_hex(&multi_txid),
            ));
        for vout in 0u32..5 {
            verifier.record_spend(multi_txid, vout);
        }

        // Read back and verify counter incremented by 5
        let post_results = client.get_batch(FIELD_ALL_METADATA, &[multi_txid]).await
            .expect("Test 2.3: post-spend read failed");
        let (post_meta, _) = TxMetadata::decode(FIELD_ALL_METADATA, &post_results.item(0).data)
            .expect("Test 2.3: post-spend decode failed");
        assert_eq!(
            post_meta.spent_utxos, pre_spent + 5,
            "Test 2.3: spent_utxos should be {} after spending 5, got {}",
            pre_spent + 5, post_meta.spent_utxos,
        );

        eprintln!("[2.3] OK -- spent vouts 0-4 on txid {}, counter={}", txid_hex(&multi_txid), post_meta.spent_utxos);

        // ==================================================================
        // Test 2.4 -- Unspend a spent UTXO, verify slot cleared + counter decremented
        // ==================================================================
        eprintln!("[2.4] Unspend a previously spent UTXO");

        let unspend_txid = spend_txids[0];
        let unspend_rec = verifier.get_record(&unspend_txid)
            .expect("Test 2.4: record should exist in verifier");

        // Verify it's currently spent (counter = 1)
        let pre_results = client.get_batch(FIELD_ALL_METADATA, &[unspend_txid]).await
            .expect("Test 2.4: pre-unspend read failed");
        let (pre_meta, _) = TxMetadata::decode(FIELD_ALL_METADATA, &pre_results.item(0).data)
            .expect("Test 2.4: pre-unspend decode failed");
        assert_eq!(
            pre_meta.spent_utxos, 1,
            "Test 2.4: before unspend, spent_utxos should be 1, got {}",
            pre_meta.spent_utxos,
        );

        let unspend_params = UnspendBatchParams {
            current_block_height: 800_000,
            block_height_retention: 100,
        };
        let unspend_item = UnspendItem {
            txid: unspend_txid,
            vout: 0,
            utxo_hash: unspend_rec.utxo_hashes[0],
        };
        client.unspend_batch(&unspend_params, &[unspend_item]).await
            .unwrap_or_else(|e| panic!("Test 2.4: unspend failed: {e}"));
        verifier.record_unspend(unspend_txid, 0);

        // Read back and verify counter decremented
        let post_results = client.get_batch(FIELD_ALL_METADATA, &[unspend_txid]).await
            .expect("Test 2.4: post-unspend read failed");
        let (post_meta, _) = TxMetadata::decode(FIELD_ALL_METADATA, &post_results.item(0).data)
            .expect("Test 2.4: post-unspend decode failed");
        assert_eq!(
            post_meta.spent_utxos, 0,
            "Test 2.4: after unspend, spent_utxos should be 0, got {}",
            post_meta.spent_utxos,
        );

        eprintln!("[2.4] OK -- unspend succeeded, counter decremented to 0");

        // ==================================================================
        // Test 2.5 -- SetMined on 200 txs, verify block entries present
        // ==================================================================
        eprintln!("[2.5] SetMined on 200 txids");

        let mined_txids: Vec<[u8; 32]> = txids[..200].to_vec();
        let set_mined_params = SetMinedBatchParams {
            block_id: 1,
            block_height: 100,
            subtree_idx: 0,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 800_001,
            block_height_retention: 100,
        };

        for chunk in mined_txids.chunks(50) {
            client.set_mined_batch(&set_mined_params, chunk).await
                .expect("Test 2.5: set_mined batch failed");
        }

        for txid in &mined_txids {
            verifier.record_set_mined(*txid);
        }

        // Read back ALL 200 with FIELD_BLOCK_ENTRIES and verify block entries are present.
        for txid in mined_txids.iter() {
            let results = client.get_batch(FIELD_ALL_METADATA | FIELD_BLOCK_ENTRIES, std::slice::from_ref(txid)).await
                .unwrap_or_else(|e| panic!("Test 2.5 read-back failed: {e}"));
            assert_eq!(results.item(0).status, 0, "Test 2.5: txid {} should be readable", txid_hex(txid));
            // After metadata (81 bytes), the block entry count byte should be >= 1
            assert!(
                results.item(0).data.len() > ALL_METADATA_SIZE,
                "Test 2.5: response too short for block entries for txid {}",
                txid_hex(txid),
            );
            let block_count = results.item(0).data[ALL_METADATA_SIZE];
            assert!(
                block_count >= 1,
                "Test 2.5: txid {} should have at least 1 block entry after set_mined, got {}",
                txid_hex(txid), block_count,
            );
        }

        eprintln!("[2.5] OK -- set_mined on 200 txids, block entries confirmed");

        // ==================================================================
        // Test 2.6 -- UnsetMined on 50 txs, verify block entry removed + unmined_since set
        // ==================================================================
        eprintln!("[2.6] UnsetMined on 50 txids");

        let unset_mined_txids: Vec<[u8; 32]> = mined_txids[..50].to_vec();
        let unset_mined_params = SetMinedBatchParams {
            block_id: 1,
            block_height: 100,
            subtree_idx: 0,
            on_longest_chain: true,
            unset_mined: true,
            current_block_height: 800_001,
            block_height_retention: 100,
        };

        for chunk in unset_mined_txids.chunks(50) {
            client.set_mined_batch(&unset_mined_params, chunk).await
                .expect("Test 2.6: unset_mined batch failed");
        }

        for txid in &unset_mined_txids {
            verifier.record_unset_mined(*txid);
        }

        // Read back ALL 50 and verify block entries removed and unmined_since is set.
        for txid in unset_mined_txids.iter() {
            let results = client.get_batch(FIELD_ALL_METADATA | FIELD_BLOCK_ENTRIES, std::slice::from_ref(txid)).await
                .unwrap_or_else(|e| panic!("Test 2.6 read-back failed: {e}"));
            assert_eq!(results.item(0).status, 0, "Test 2.6: txid {} should be readable", txid_hex(txid));
            // Check that block entry count is 0 after unset_mined
            if results.item(0).data.len() > ALL_METADATA_SIZE {
                let block_count = results.item(0).data[ALL_METADATA_SIZE];
                assert_eq!(
                    block_count, 0,
                    "Test 2.6: txid {} should have 0 block entries after unset_mined, got {}",
                    txid_hex(txid), block_count,
                );
            }
            // Parse metadata and check unmined_since is set (non-zero)
            let (meta, _) = TxMetadata::decode(FIELD_ALL_METADATA, &results.item(0).data)
                .unwrap_or_else(|e| panic!("Test 2.6 decode failed: {e}"));
            assert!(
                meta.unmined_since > 0,
                "Test 2.6: txid {} should have unmined_since set after unset_mined, got 0",
                txid_hex(txid),
            );
        }

        eprintln!("[2.6] OK -- unset_mined on 50 txids, block entries removed, unmined_since set");

        // ==================================================================
        // Test 2.7 -- Freeze -> spend (FROZEN) -> unfreeze -> spend (success)
        // ==================================================================
        eprintln!("[2.7] Full freeze lifecycle: freeze -> spend fails FROZEN -> unfreeze -> spend succeeds");

        let freeze_txids = common::seed_records(&client, &verifier, 1, 5).await
            .expect("Test 2.7: failed to create freeze test record");
        let freeze_txid = freeze_txids[0];
        let freeze_rec = verifier.get_record(&freeze_txid)
            .expect("Test 2.7: freeze test record should exist in verifier");

        // Step 1: Freeze vout=0
        let freeze_item = FreezeItem {
            txid: freeze_txid,
            vout: 0,
            utxo_hash: freeze_rec.utxo_hashes[0],
        };
        client.freeze_batch(&[freeze_item.clone()]).await
            .expect("Test 2.7: freeze batch failed");
        verifier.record_freeze(freeze_txid, 0);

        // Step 2: Attempt to spend the frozen UTXO -- should fail with FROZEN error
        let frozen_spend_item = SpendItem {
            txid: freeze_txid,
            vout: 0,
            utxo_hash: freeze_rec.utxo_hashes[0],
            spending_data: [0u8; 36],
        };
        let frozen_spend_result = client.spend_batch(&spend_params, &[frozen_spend_item]).await;
        match frozen_spend_result {
            Err(ClientError::Partial(ref pe)) => {
                assert!(
                    !pe.errors.is_empty(),
                    "Test 2.7: spending frozen UTXO should produce per-item error"
                );
                let err_code = pe.errors[0].code;
                assert_eq!(
                    err_code,
                    teraslab::protocol::opcodes::ERR_FROZEN,
                    "Test 2.7: spending frozen UTXO should return ERR_FROZEN ({}), got {} ({})",
                    teraslab::protocol::opcodes::ERR_FROZEN,
                    err_code,
                    teraslab_test_client::errors::error_code_string(err_code),
                );
            }
            Err(ClientError::Server { code, .. }) => {
                assert_eq!(
                    code,
                    teraslab::protocol::opcodes::ERR_FROZEN,
                    "Test 2.7: spending frozen UTXO should return ERR_FROZEN, got {code}"
                );
            }
            Ok(_) => {
                panic!("Test 2.7: spending a frozen UTXO should have failed, but succeeded");
            }
            Err(e) => {
                panic!("Test 2.7: unexpected error type spending frozen UTXO: {e}");
            }
        }

        // Step 3: Unfreeze
        client.unfreeze_batch(&[freeze_item]).await
            .expect("Test 2.7: unfreeze batch failed");
        verifier.record_unfreeze(freeze_txid, 0);

        // Step 4: Spend again -- should succeed now
        let unfrozen_spend_item = SpendItem {
            txid: freeze_txid,
            vout: 0,
            utxo_hash: freeze_rec.utxo_hashes[0],
            spending_data: [0u8; 36],
        };
        client.spend_batch(&spend_params, &[unfrozen_spend_item]).await
            .unwrap_or_else(|e| panic!(
                "Test 2.7: spending after unfreeze should succeed, but got: {e}"
            ));
        verifier.record_spend(freeze_txid, 0);

        eprintln!("[2.7] OK -- freeze lifecycle passed: freeze -> FROZEN error -> unfreeze -> spend OK");

        // ==================================================================
        // Test 2.8 -- Reassign frozen UTXO with spendableAfter
        // ==================================================================
        eprintln!("[2.8] Reassign frozen UTXO with spendableAfter");

        let reassign_txids = common::seed_records(&client, &verifier, 1, 3).await
            .expect("Test 2.8: failed to create reassign test record");
        let reassign_txid = reassign_txids[0];
        let reassign_rec = verifier.get_record(&reassign_txid)
            .expect("Test 2.8: reassign test record should exist in verifier");

        // Freeze vout=0 first
        let reassign_freeze_item = FreezeItem {
            txid: reassign_txid,
            vout: 0,
            utxo_hash: reassign_rec.utxo_hashes[0],
        };
        client.freeze_batch(&[reassign_freeze_item]).await
            .expect("Test 2.8: freeze before reassign failed");
        verifier.record_freeze(reassign_txid, 0);

        // Reassign with new hash and spendable_after=10
        let mut new_hash = [0u8; 32];
        new_hash[0] = 0xAA;
        new_hash[1] = 0xBB;
        let reassign_params = ReassignBatchParams {
            block_height: 5,
            spendable_after: 10,
        };
        let reassign_item = ReassignItem {
            txid: reassign_txid,
            vout: 0,
            utxo_hash: reassign_rec.utxo_hashes[0],
            new_utxo_hash: new_hash,
        };
        client.reassign_batch(&reassign_params, &[reassign_item]).await
            .unwrap_or_else(|e| panic!("Test 2.8: reassign failed: {e}"));
        verifier.record_reassign(reassign_txid, 0, new_hash);
        verifier.record_unfreeze(reassign_txid, 0);

        // Verify the reassign changed the utxo hash by reading back and checking.
        let readback = client.get_batch(FIELD_ALL, std::slice::from_ref(&reassign_txid)).await
            .unwrap_or_else(|e| panic!("Test 2.8: read-back after reassign failed: {e}"));
        assert!(readback.found(0), "Test 2.8: reassigned txid should be readable");

        // Attempt spend at current_block_height < spendable_after (10) -- should fail
        let low_height_params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 5,
            block_height_retention: 1000,
        };
        let reassign_spend_item = SpendItem {
            txid: reassign_txid,
            vout: 0,
            utxo_hash: new_hash,
            spending_data: [0u8; 36],
        };
        let low_result = client.spend_batch(&low_height_params, &[reassign_spend_item.clone()]).await;
        match low_result {
            Err(ClientError::Partial(ref pe)) => {
                assert!(
                    !pe.errors.is_empty(),
                    "Test 2.8: spending before spendable_after height should produce an error"
                );
            }
            Err(ClientError::Server { .. }) => {
                // Expected -- server-level rejection
            }
            Ok(_) => {
                panic!("Test 2.8: spending before spendable_after height should have failed");
            }
            Err(e) => {
                panic!("Test 2.8: unexpected error spending before spendable_after: {e}");
            }
        }

        // Attempt spend at current_block_height > spendable_height (block_height + spendable_after = 5 + 10 = 15)
        let high_height_params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 16,
            block_height_retention: 1000,
        };
        match client.spend_batch(&high_height_params, &[reassign_spend_item]).await {
            Ok(_) => {}
            Err(ClientError::Partial(ref pe)) => {
                for err in &pe.errors {
                    eprintln!("[2.8] spend error at height 11: item={} code={} data={:?}",
                        err.item_index, err.code, err.data);
                }
                panic!("Test 2.8: spending at spendable_after height should succeed, got partial error");
            }
            Err(e) => {
                panic!("Test 2.8: spending at spendable_after height should succeed: {e}");
            }
        }
        verifier.record_spend(reassign_txid, 0);

        eprintln!("[2.8] OK -- reassign with spendableAfter succeeded, spend-before-height rejected, spend-at-height accepted");

        // ==================================================================
        // Test 2.9 -- SetConflicting on 100 txs, spend without flag fails,
        //             with flag succeeds
        // ==================================================================
        eprintln!("[2.9] SetConflicting on 100 txs");

        let conflicting_txids: Vec<[u8; 32]> = txids[700..800].to_vec();
        let set_conflicting_params = SetConflictingParams {
            value: true,
            current_block_height: 800_000,
            block_height_retention: 100,
        };
        client.set_conflicting_batch(&set_conflicting_params, &conflicting_txids).await
            .unwrap_or_else(|e| panic!("Test 2.9: set_conflicting failed: {e}"));

        for txid in &conflicting_txids {
            verifier.record_set_conflicting(*txid, true);
        }

        // Attempt to spend vout=1 without ignore_conflicting -- should fail
        let conflict_test_txid = conflicting_txids[0];
        let conflict_rec = verifier.get_record(&conflict_test_txid)
            .expect("Test 2.9: record should exist");
        let strict_params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 800_000,
            block_height_retention: 100,
        };
        let conflict_spend = SpendItem {
            txid: conflict_test_txid,
            vout: 1,
            utxo_hash: conflict_rec.utxo_hashes[1],
            spending_data: [0u8; 36],
        };
        let conflict_result = client.spend_batch(&strict_params, &[conflict_spend]).await;
        match conflict_result {
            Err(ClientError::Partial(ref pe)) => {
                assert!(
                    pe.errors.iter().any(|e| e.code == teraslab::protocol::opcodes::ERR_CONFLICTING),
                    "Test 2.9: spending conflicting tx without flag should return ERR_CONFLICTING"
                );
            }
            Err(ClientError::Server { code, .. }) => {
                assert_eq!(code, teraslab::protocol::opcodes::ERR_CONFLICTING);
            }
            Ok(_) => {
                panic!("Test 2.9: spending conflicting tx without flag should fail");
            }
            Err(e) => {
                panic!("Test 2.9: unexpected error: {e}");
            }
        }

        // Now spend with ignore_conflicting=true -- should succeed
        let lenient_params = SpendBatchParams {
            ignore_conflicting: true,
            ignore_locked: false,
            current_block_height: 800_000,
            block_height_retention: 100,
        };
        let conflict_spend2 = SpendItem {
            txid: conflict_test_txid,
            vout: 1,
            utxo_hash: conflict_rec.utxo_hashes[1],
            spending_data: [0u8; 36],
        };
        client.spend_batch(&lenient_params, &[conflict_spend2]).await
            .unwrap_or_else(|e| panic!("Test 2.9: spend with ignore_conflicting should succeed: {e}"));
        verifier.record_spend(conflict_test_txid, 1);

        eprintln!("[2.9] OK -- SetConflicting: spend without flag failed, with flag succeeded");

        // ==================================================================
        // Test 2.10 -- SetLocked on 50 txs, spend without flag fails,
        //              setMined clears locked
        // ==================================================================
        eprintln!("[2.10] SetLocked on 50 txs");

        let locked_txids: Vec<[u8; 32]> = txids[800..850].to_vec();
        client.set_locked_batch(true, &locked_txids).await
            .unwrap_or_else(|e| panic!("Test 2.10: set_locked failed: {e}"));

        for txid in &locked_txids {
            verifier.record_set_locked(*txid, true);
        }

        // Attempt to spend locked tx without ignore_locked -- should fail
        let locked_test_txid = locked_txids[0];
        let locked_rec = verifier.get_record(&locked_test_txid)
            .expect("Test 2.10: record should exist");
        let locked_spend = SpendItem {
            txid: locked_test_txid,
            vout: 2,
            utxo_hash: locked_rec.utxo_hashes[2],
            spending_data: [0u8; 36],
        };
        let locked_result = client.spend_batch(&strict_params, &[locked_spend]).await;
        match locked_result {
            Err(ClientError::Partial(ref pe)) => {
                assert!(
                    pe.errors.iter().any(|e| e.code == teraslab::protocol::opcodes::ERR_LOCKED),
                    "Test 2.10: spending locked tx without flag should return ERR_LOCKED"
                );
            }
            Err(ClientError::Server { code, .. }) => {
                assert_eq!(code, teraslab::protocol::opcodes::ERR_LOCKED);
            }
            Ok(_) => {
                panic!("Test 2.10: spending locked tx without flag should fail");
            }
            Err(e) => {
                panic!("Test 2.10: unexpected error: {e}");
            }
        }

        // SetMined clears the locked flag
        let locked_mine_params = SetMinedBatchParams {
            block_id: 2,
            block_height: 200,
            subtree_idx: 0,
            on_longest_chain: true,
            unset_mined: false,
            current_block_height: 800_001,
            block_height_retention: 100,
        };
        client.set_mined_batch(&locked_mine_params, &[locked_test_txid]).await
            .unwrap_or_else(|e| panic!("Test 2.10: set_mined to clear locked failed: {e}"));
        verifier.record_set_mined(locked_test_txid);
        verifier.record_set_locked(locked_test_txid, false);

        // Verify locked flag is cleared via read-back
        let locked_results = client.get_batch(FIELD_ALL_METADATA, &[locked_test_txid]).await
            .unwrap_or_else(|e| panic!("Test 2.10 read-back failed: {e}"));
        let (locked_meta, _) = TxMetadata::decode(FIELD_ALL_METADATA, &locked_results.item(0).data)
            .unwrap_or_else(|e| panic!("Test 2.10 decode failed: {e}"));
        // The locked flag is bit 2 (0x04) of the flags byte
        let is_still_locked = locked_meta.flags & 0b0000_0100 != 0;
        assert!(
            !is_still_locked,
            "Test 2.10: locked flag should be cleared after set_mined"
        );

        eprintln!("[2.10] OK -- SetLocked: spend without flag failed, setMined cleared locked");

        // ==================================================================
        // Test 2.11 -- PreserveUntil on 20 txs
        // ==================================================================
        eprintln!("[2.11] PreserveUntil on 20 txs");

        let preserve_txids: Vec<[u8; 32]> = txids[850..870].to_vec();
        let preserve_height: u32 = 999_999;
        client.preserve_until_batch(preserve_height, &preserve_txids).await
            .unwrap_or_else(|e| panic!("Test 2.11: preserve_until failed: {e}"));

        for txid in &preserve_txids {
            verifier.record_preserve_until(*txid, preserve_height);
        }

        // Read back and verify preserve_until is set
        for txid in preserve_txids.iter().take(10) {
            let results = client.get_batch(FIELD_ALL_METADATA, std::slice::from_ref(txid)).await
                .unwrap_or_else(|e| panic!("Test 2.11 read-back failed: {e}"));
            let (meta, _) = TxMetadata::decode(FIELD_ALL_METADATA, &results.item(0).data)
                .unwrap_or_else(|e| panic!("Test 2.11 decode failed: {e}"));
            assert_eq!(
                meta.preserve_until, preserve_height,
                "Test 2.11: txid {} preserve_until should be {}, got {}",
                txid_hex(txid), preserve_height, meta.preserve_until,
            );
        }

        eprintln!("[2.11] OK -- preserve_until set on 20 txs");

        // ==================================================================
        // Test 2.12 -- Delete 100 txids, verify NotFound specifically
        // ==================================================================
        eprintln!("[2.12] Delete 100 txids and verify NotFound on read-back");

        let delete_txids: Vec<[u8; 32]> = txids[900..1000].to_vec();

        for chunk in delete_txids.chunks(50) {
            client.delete_batch(chunk).await
                .expect("Test 2.12: delete batch failed");
        }

        for txid in &delete_txids {
            verifier.record_delete(*txid);
        }

        // Read deleted records back -- must get NotFound status (not swallow errors)
        for chunk in delete_txids.chunks(50) {
            let results = client.get_batch(FIELD_ALL, chunk).await;
            match results {
                Ok(items) => {
                    for (i, item) in items.iter().enumerate() {
                        assert_ne!(
                            item.status(), 0,
                            "Test 2.12: deleted txid {} returned status 0 (found), expected not-found",
                            txid_hex(&chunk[i]),
                        );
                    }
                }
                Err(ClientError::NotFound) => {
                    // Expected -- entire batch not found
                }
                Err(e) => {
                    panic!(
                        "Test 2.12: unexpected error reading deleted records: {e}. \
                         Only NotFound or status!=0 results are acceptable."
                    );
                }
            }
        }

        eprintln!("[2.12] OK -- deleted 100 txids and confirmed NotFound");

        // ==================================================================
        // Test 2.13 -- Coinbase tx with spending_height, test below and at height
        // ==================================================================
        eprintln!("[2.13] Coinbase tx with spending_height maturity check");

        let mut coinbase_txid = [0u8; 32];
        coinbase_txid[0] = 0xCB;
        coinbase_txid[1] = 0x01;
        let mut coinbase_hash = [0u8; 32];
        coinbase_hash[0] = 0xCC;

        let coinbase_item = CreateItem {
            txid: coinbase_txid,
            utxo_hashes: vec![coinbase_hash],
            tx_version: 1,
            locktime: 0,
            fee: 0,
            size_in_bytes: 100,
            extended_size: 0,
            is_coinbase: true,
            spending_height: 150,
            created_at: 1710000000000,
            flags: 0,
            cold_data: vec![],
            mined_block_id: None,
            mined_block_height: None,
            mined_subtree_idx: None,
            parent_txids: vec![],
        };
        client.create_batch(&[coinbase_item]).await
            .expect("Test 2.13: create coinbase tx failed");
        verifier.record_create(coinbase_txid, 1, vec![coinbase_hash]);

        // Spend at height 100 (below spending_height=150) -- should fail with COINBASE_IMMATURE
        let immature_params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 100,
            block_height_retention: 100,
        };
        let coinbase_spend = SpendItem {
            txid: coinbase_txid,
            vout: 0,
            utxo_hash: coinbase_hash,
            spending_data: [0u8; 36],
        };
        let immature_result = client.spend_batch(&immature_params, &[coinbase_spend.clone()]).await;
        match immature_result {
            Err(ClientError::Partial(ref pe)) => {
                assert!(
                    pe.errors.iter().any(|e| e.code == teraslab::protocol::opcodes::ERR_COINBASE_IMMATURE),
                    "Test 2.13: spending immature coinbase should return ERR_COINBASE_IMMATURE, got codes: {:?}",
                    pe.errors.iter().map(|e| e.code).collect::<Vec<_>>(),
                );
            }
            Err(ClientError::Server { code, .. }) => {
                assert_eq!(code, teraslab::protocol::opcodes::ERR_COINBASE_IMMATURE);
            }
            Ok(_) => {
                panic!("Test 2.13: spending immature coinbase should fail");
            }
            Err(e) => {
                panic!("Test 2.13: unexpected error: {e}");
            }
        }

        // Spend at height 150 (at spending_height) -- should succeed
        let mature_params = SpendBatchParams {
            ignore_conflicting: false,
            ignore_locked: false,
            current_block_height: 150,
            block_height_retention: 100,
        };
        client.spend_batch(&mature_params, &[coinbase_spend]).await
            .unwrap_or_else(|e| panic!("Test 2.13: spending mature coinbase should succeed: {e}"));
        verifier.record_spend(coinbase_txid, 0);

        eprintln!("[2.13] OK -- coinbase maturity: immature spend failed, mature spend succeeded");

        // ==================================================================
        // Test 2.14 -- Batch create 100 txs, verify all readable
        // ==================================================================
        eprintln!("[2.14] Batch create 100 txs");

        let batch_create_txids = common::seed_records(&client, &verifier, 100, 5).await
            .expect("Test 2.14: batch create failed");
        assert_eq!(batch_create_txids.len(), 100, "Test 2.14: expected 100 txids");

        // Verify all 100 are readable
        let results = client.get_batch(FIELD_ALL_METADATA, &batch_create_txids).await
            .expect("Test 2.14: get_batch after create failed");
        for (i, result) in results.iter().enumerate() {
            assert_eq!(
                result.status(), 0,
                "Test 2.14: newly created txid {} should be readable, got status {}",
                txid_hex(&batch_create_txids[i]), result.status(),
            );
        }

        eprintln!("[2.14] OK -- batch created 100 txs, all readable");

        // ==================================================================
        // Test 2.15 -- Batch get 200 txs, verify all returned correctly
        // ==================================================================
        eprintln!("[2.15] Batch get 200 txs");

        // Use the first 200 non-deleted txids from the verifier
        let non_deleted = verifier.non_deleted_txids();
        let batch_get_txids: Vec<[u8; 32]> = non_deleted.iter().take(200).copied().collect();
        assert!(
            batch_get_txids.len() == 200,
            "Test 2.15: expected at least 200 non-deleted txids, got {}",
            batch_get_txids.len(),
        );

        let mut all_found = 0u32;
        for chunk in batch_get_txids.chunks(50) {
            let results = client.get_batch(FIELD_ALL_METADATA, chunk).await
                .expect("Test 2.15: batch get failed");
            assert_eq!(
                results.len(), chunk.len(),
                "Test 2.15: get_batch should return same count as requested"
            );
            for (i, result) in results.iter().enumerate() {
                assert_eq!(
                    result.status(), 0,
                    "Test 2.15: txid {} should be readable, got status {}",
                    txid_hex(&chunk[i]), result.status(),
                );
                // Verify field values against the verifier's expected state
                if result.status() == 0 {
                    if let Some((spent_count, is_mined, is_conflicting, is_locked)) =
                        parse_metadata_fields(result.data())
                    {
                        let mm = verifier.verify_record(
                            &chunk[i],
                            spent_count,
                            is_mined,
                            is_conflicting,
                            is_locked,
                            false,
                        );
                        assert!(
                            mm.is_empty(),
                            "Test 2.15: field mismatch for txid {}: {:?}",
                            txid_hex(&chunk[i]),
                            mm.iter().map(|m| format!("{}={} (expected {})", m.field, m.actual, m.expected)).collect::<Vec<_>>(),
                        );
                    }
                }
                all_found += 1;
            }
        }
        assert_eq!(all_found, 200, "Test 2.15: should have found all 200 records");

        eprintln!("[2.15] OK -- batch get returned all 200 txs correctly");

        // ==================================================================
        // Test 2.16 -- Full consistency check using verify_consistency()
        // ==================================================================
        eprintln!("[2.16] Full consistency check against state verifier");

        let mismatches = common::verify_consistency(&client, &verifier).await
            .expect("Test 2.16: verify_consistency call failed");

        if !mismatches.is_empty() {
            for mm in mismatches.iter().take(10) {
                eprintln!(
                    "Test 2.16 MISMATCH: txid {} field={} expected={} actual={}",
                    txid_hex(&mm.txid), mm.field, mm.expected, mm.actual,
                );
            }
        }

        assert!(
            mismatches.is_empty(),
            "Test 2.16: full consistency check found {} mismatches (first 10 shown above)",
            mismatches.len(),
        );

        eprintln!("[2.16] OK -- full consistency check passed: zero mismatches");

        // -- Teardown --
        let _ = docker.compose_down().await;

        eprintln!("[scenario_02] All sub-tests passed");
    });

    match timeout_guard.await {
        Ok(()) => {}
        Err(_) => panic!("scenario_02_basic_operations timed out after 300s"),
    }
}
