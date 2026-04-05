#[allow(dead_code)]
mod common;

use std::time::Duration;
use teraslab_test_client::verifier::StateVerifier;

#[tokio::test(flavor = "multi_thread")]
async fn debug_term3_kill_hold() {
    const SID: u16 = 99;

    common::teardown_all(SID).await;

    let (docker, client) = common::start_3node_cluster(SID)
        .await
        .expect("bootstrap cluster");
    common::wait_migrations_complete(&docker, 3, Duration::from_secs(15))
        .await
        .expect("initial migrations");
    client.refresh_routing().await.expect("refresh routing");

    let verifier = StateVerifier::new();
    let seeded = common::seed_records(&client, &verifier, 5000, 10).await;
    eprintln!("seeded={}", seeded.as_ref().map(|v| v.len()).unwrap_or(0));
    seeded.expect("seed records");

    common::wait_replication_settled(&docker, 3, Duration::from_secs(5))
        .await
        .expect("replication settled");

    docker.kill_node("node2").await.expect("kill node2");
    common::wait_specific_nodes_ready(&docker, &[1, 3], 2, Duration::from_secs(15))
        .await
        .expect("survivors size=2");
    common::wait_specific_migrations_complete(&docker, &[1, 3], Duration::from_secs(30))
        .await
        .expect("survivor migrations");

    for node_num in [1u32, 3u32] {
        let status = common::http_status(&docker, node_num)
            .await
            .expect("status");
        let migration = common::http_migration_status(&docker, node_num)
            .await
            .expect("migration status");
        eprintln!("node{node_num} status={status}");
        eprintln!("node{node_num} migration={migration}");
    }

    tokio::time::sleep(Duration::from_secs(120)).await;

    let post_kill_seed = common::seed_records(&client, &verifier, 10, 1).await;
    eprintln!("post_kill_seed={post_kill_seed:?}");

    docker
        .collect_logs("debug-logs/ts99-term3")
        .await
        .expect("collect logs");

    common::teardown_all(SID).await;
}
