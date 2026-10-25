//! Scope retirement must fence actual resource reads, not only a preflight.
use super::tests::{limits, native_fixture, request, scope};
use super::*;
use orch8_storage::{
    ClaimedRead, InstanceStore, OutputStore, ResourceStore, sqlite::SqliteStorage,
};
use orch8_types::{
    ids::{BlockId, InstanceId},
    output::BlockOutput,
};
use std::sync::Arc;

fn output(instance_id: InstanceId, block_id: BlockId) -> BlockOutput {
    BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id,
        block_id,
        output: serde_json::json!({"secret":"output"}),
        output_ref: None,
        output_size: 19,
        attempt: 0,
        created_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn independent_pool_retirement_and_reopen_fence_kv_and_output_reads() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reads.sqlite");
    let sqlite = Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let storage = MobileStorage::new(sqlite.clone());
    let other = Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let independent = MobileStorage::new(other.clone());
    let owner = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&owner, request(Some("old"), "{}"), limits())
        .await
        .unwrap();
    let instance = native_fixture(&sqlite, &run).await;
    let token = sqlite
        .claim_due_instances(chrono::Utc::now(), 8, 0)
        .await
        .unwrap()
        .pop()
        .unwrap()
        .claimed_at
        .unwrap();
    let block = BlockId::new("work");
    sqlite
        .set_instance_kv(instance.id, "private", &serde_json::json!({"secret":"kv"}))
        .await
        .unwrap();
    sqlite
        .save_block_output(&output(instance.id, block.clone()))
        .await
        .unwrap();
    let before = sqlite
        .get_instance(instance.id)
        .await
        .unwrap()
        .unwrap()
        .updated_at;
    assert!(matches!(
        other
            .get_instance_kv_claimed(instance.id, &instance.tenant_id, "private", token)
            .await
            .unwrap(),
        ClaimedRead::Value(Some(_))
    ));
    assert!(matches!(
        other
            .get_block_output_claimed(instance.id, &instance.tenant_id, &block, token)
            .await
            .unwrap(),
        ClaimedRead::Value(Some(_))
    ));
    assert_eq!(
        sqlite
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .updated_at,
        before,
        "read must not renew ownership"
    );
    independent
        .retire_account_scope(&owner.account)
        .await
        .unwrap();
    for reopened in [false, true] {
        if reopened {
            scope(&storage, "alice", "one").await;
        }
        assert!(matches!(
            sqlite
                .get_instance_kv_claimed(instance.id, &instance.tenant_id, "private", token)
                .await
                .unwrap(),
            ClaimedRead::Lost
        ));
        assert!(matches!(
            sqlite
                .get_block_output_claimed(instance.id, &instance.tenant_id, &block, token)
                .await
                .unwrap(),
            ClaimedRead::Lost
        ));
    }
    assert!(
        sqlite
            .get_instance_kv(instance.id, "private")
            .await
            .unwrap()
            .is_some(),
        "retirement preserves evidence; trusted raw reads remain explicit"
    );
}
