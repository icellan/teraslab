#[allow(dead_code)]
mod common;

use std::time::Duration;
use teraslab_test_client::verifier::StateVerifier;

#[tokio::test(flavor = "multi_thread")]
async fn debug_bootstrap_hold() {
    const SID: u16 = 15;

    common::teardown_all(SID).await;
    let (docker, client) = common::start_3node_cluster(SID)
        .await
        .expect("bootstrap cluster");

    for node_num in 1..=3u32 {
        let status = common::http_status(&docker, node_num)
            .await
            .expect("status");
        let migration = common::http_migration_status(&docker, node_num)
            .await
            .expect("migration status");
        eprintln!("node{node_num} status={status}");
        eprintln!("node{node_num} migration={migration}");
    }

    let verifier = StateVerifier::new();
    let seed = common::seed_records(&client, &verifier, 10, 1).await;
    eprintln!("seed result={seed:?}");

    tokio::time::sleep(Duration::from_secs(120)).await;
    common::teardown_all(SID).await;
}
