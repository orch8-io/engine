//! Durable decision format and `SQLite` in-place upgrade/reopen behavior.
use chrono::Utc;
use orch8_storage::{
    InstanceStore, SequenceStore, WorkerStore,
    sqlite::SqliteStorage,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
    worker_claim::WorkerClaimPrincipal,
    worker_outcome::WorkerCompletion,
    worker_reconciliation::WorkerReconciliationApproval,
};
use orch8_types::worker::{WorkerClaim, WorkerTask};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn sqlite_legacy_schema_upgrade_and_reopen_retain_the_explicit_decision() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("history.sqlite");
    let path = path.to_str().unwrap();
    let storage = SqliteStorage::file(path).await.unwrap();
    // Exact preceding codec guard and schema marker, before any decision exists.
    sqlx::raw_sql("DROP TRIGGER worker_outcome_encoding_insert; DROP INDEX uq_worker_reconciliation_approval;")
        .execute(storage.pool())
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../src/sqlite/worker_outcome_encoding.sql"))
        .execute(storage.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM schema_versions WHERE version=71")
        .execute(storage.pool())
        .await
        .unwrap();
    storage.pool().close().await;
    drop(storage);
    let storage = SqliteStorage::file(path).await.unwrap();
    let seq = SequenceBuilder::new("file-reconciliation")
        .step("work", "noop")
        .build();
    storage.create_sequence(&seq).await.unwrap();
    let native = InstanceBuilder::for_sequence(&seq).build();
    storage.create_instance(&native).await.unwrap();
    let task:WorkerTask=serde_json::from_value(json!({"id":Uuid::now_v7(),"instance_id":native.id,"block_id":"work","handler_name":"noop","params":{},"context":{},"attempt":0,"state":"pending","created_at":Utc::now()})).unwrap();
    storage.create_worker_task(&task).await.unwrap();
    let held = storage
        .claim_worker_tasks("noop", "old", 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let claim = WorkerClaim::new("old", held.claim_epoch);
    let approval = WorkerReconciliationApproval::new(
        WorkerClaimPrincipal::Root {},
        native.tenant_id.clone(),
        held.id,
        claim.clone(),
        Uuid::now_v7(),
        "backend-ack-immutable".into(),
    )
    .unwrap();
    let completion = WorkerCompletion::for_legacy_reconciliation(
        json!({"value":"nul\0λ","large":1e18,"negative_zero":-0.0}),
        approval,
    )
    .unwrap();
    assert!(
        storage
            .accept_worker_completion(held.id, &claim, &completion)
            .await
            .unwrap()
    );
    let expected = storage
        .get_worker_accepted_outcome(Some(&native.tenant_id), held.id, &claim)
        .await
        .unwrap()
        .unwrap();
    assert!(completion.matches_reconciled_outcome(&expected));
    storage.delete_worker_task(held.id).await.unwrap();
    storage.pool().close().await;
    drop(storage);
    let storage = SqliteStorage::file(path).await.unwrap();
    let actual = storage
        .get_worker_accepted_outcome(Some(&native.tenant_id), held.id, &claim)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual, expected);
    assert!(completion.matches_reconciled_outcome(&actual));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT MAX(version) FROM schema_versions")
            .fetch_one(storage.pool())
            .await
            .unwrap(),
        71
    );
    assert!(
        sqlx::query("DELETE FROM worker_accepted_outcomes WHERE task_id=?1")
            .bind(held.id.to_string())
            .execute(storage.pool())
            .await
            .is_err()
    );
    storage.pool().close().await;
}
