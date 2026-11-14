use super::*;
use crate::storage::scopes::definitions::tests::{bundle, catalog};
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::delegation_definition::{
    input::EncodedDelegationInput, protocol::DelegationResourceUsage,
};
use std::sync::Arc;
const SERVER: &str = "https://engine.example/";
const REQUEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
async fn fixture() -> (
    MobileStorage,
    CatalogScope,
    DelegationClaimContext,
    DelegationInputEnvelope,
) {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    fixture_storage(storage).await
}
async fn fixture_storage(
    storage: MobileStorage,
) -> (
    MobileStorage,
    CatalogScope,
    DelegationClaimContext,
    DelegationInputEnvelope,
) {
    let scope = catalog(&storage, "alice", "work").await;
    let (mut wire, mut claim, reference) = bundle("private");
    claim.claim_epoch = 1;
    wire.scope = claim.clone();
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    let input = EncodedDelegationInput::from_value(
        &serde_json::json!({"wide":u64::MAX,"private":"é\u{0000}"}),
        InputLimits::default(),
    )
    .unwrap();
    let context = DelegationClaimContext {
        version: 1,
        scope: claim,
        request_sha256: REQUEST.into(),
        expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
        definition: reference,
        input: input.reference().clone(),
        usage: DelegationResourceUsage {
            definition_expanded_bytes: 4096,
            input_wire_version: 1,
            input_payload_bytes: input.reference().payload_bytes,
            input_expanded_bytes: input.reference().expanded_bytes,
        },
    };
    context.validate().unwrap();
    let envelope =
        DelegationInputEnvelope::new(context.scope.clone(), REQUEST.into(), &input).unwrap();
    (storage, scope, context, envelope)
}
#[tokio::test]
async fn one_exact_input_survives_claim_replacement_and_signin_epochs() {
    let (storage, scope, mut context, envelope) = fixture().await;
    assert!(
        storage
            .load_claim_input(&scope, SERVER, &context)
            .await
            .unwrap()
            .is_none()
    );
    for _ in 0..2 {
        assert_eq!(
            storage
                .install_claim_input(&scope, SERVER, &context, &envelope)
                .await
                .unwrap()
                .value()["wide"]
                .as_u64(),
            Some(u64::MAX)
        );
    }
    let old = context.clone();
    context.scope.claim_epoch = u64::MAX;
    context.scope.worker_id = "replacement".into();
    storage
        .bind_claim_run(&scope, SERVER, &context.scope, &context.definition, REQUEST)
        .await
        .unwrap();
    assert!(
        storage
            .load_claim_input(&scope, SERVER, &old)
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .load_claim_input(&scope, SERVER, &context)
            .await
            .unwrap()
            .unwrap()
            .value()["wide"]
            .as_u64(),
        Some(u64::MAX)
    );
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    storage.retire_account_scope(&account).await.unwrap();
    assert!(
        storage
            .load_claim_input(&scope, SERVER, &context)
            .await
            .is_err()
    );
    let fresh = catalog(&storage, "alice", "work").await;
    storage
        .bind_claim_run(&fresh, SERVER, &context.scope, &context.definition, REQUEST)
        .await
        .unwrap();
    assert!(
        storage
            .load_claim_input(&fresh, SERVER, &context)
            .await
            .unwrap()
            .is_some()
    );
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_remote_run_inputs")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(n, 1);
}
#[tokio::test]
async fn foreign_scope_and_changed_input_cannot_replace_retained_content() {
    let (storage, scope, context, mut envelope) = fixture().await;
    storage
        .install_claim_input(&scope, SERVER, &context, &envelope)
        .await
        .unwrap();
    let foreign = catalog(&storage, "bob", "work").await;
    assert!(
        storage
            .load_claim_input(&foreign, SERVER, &context)
            .await
            .is_err()
    );
    assert!(
        storage
            .load_claim_input(&scope, "https://other.example/", &context)
            .await
            .is_err()
    );
    envelope.payload_base64 = "PRIVATE_CORRUPTION".into();
    assert!(
        storage
            .install_claim_input(&scope, SERVER, &context, &envelope)
            .await
            .is_err()
    );
    assert!(
        storage
            .load_claim_input(&scope, SERVER, &context)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        sqlx::query("UPDATE mobile_remote_run_inputs SET version=2")
            .execute(storage.pool())
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM mobile_remote_run_inputs")
            .execute(storage.pool())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn corrupt_or_future_input_is_never_repaired_by_a_fresh_download() {
    for update in [
        "UPDATE mobile_remote_run_inputs SET version=2",
        "UPDATE mobile_remote_run_inputs SET payload_base64='AAAA'",
        "UPDATE mobile_remote_run_inputs SET reference_json='{}'",
        "UPDATE mobile_remote_run_inputs SET expanded_bytes=1",
    ] {
        let (storage, scope, context, envelope) = fixture().await;
        storage
            .install_claim_input(&scope, SERVER, &context, &envelope)
            .await
            .unwrap();
        sqlx::query("DROP TRIGGER mobile_remote_input_update")
            .execute(storage.pool())
            .await
            .unwrap();
        sqlx::query(update).execute(storage.pool()).await.unwrap();
        assert!(
            storage
                .load_claim_input(&scope, SERVER, &context)
                .await
                .is_err()
        );
        assert!(
            storage
                .install_claim_input(&scope, SERVER, &context, &envelope)
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn suppressed_insert_and_capacity_failure_leave_no_published_input() {
    let (storage, scope, context, envelope) = fixture().await;
    sqlx::query("CREATE TRIGGER suppress_input BEFORE INSERT ON mobile_remote_run_inputs BEGIN SELECT RAISE(IGNORE); END").execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .install_claim_input(&scope, SERVER, &context, &envelope)
            .await
            .is_err()
    );
    assert!(
        storage
            .load_claim_input(&scope, SERVER, &context)
            .await
            .unwrap()
            .is_none()
    );
    let mut tx = storage.pool().begin().await.unwrap();
    assert!(capacity(&mut tx, &scope, 32 * 1024 * 1024).await.is_ok());
    assert!(
        capacity(&mut tx, &scope, 32 * 1024 * 1024 + 1)
            .await
            .is_err()
    );
    assert!(capacity(&mut tx, &scope, i64::MAX).await.is_err());
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn database_reopen_retains_exact_input_without_network_or_native_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("inputs.db");
    let raw = SqliteStorage::file_mobile(path.to_str().unwrap())
        .await
        .unwrap();
    let (storage, scope, context, envelope) =
        fixture_storage(MobileStorage::new(Arc::new(raw))).await;
    storage
        .install_claim_input(&scope, SERVER, &context, &envelope)
        .await
        .unwrap();
    storage.pool().close().await;
    drop(storage);
    drop(envelope);
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let value = storage
        .load_claim_input(&scope, SERVER, &context)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value.value()["private"].as_str(), Some("é\u{0000}"));
    assert_eq!(value.value()["wide"].as_u64(), Some(u64::MAX));
    let native: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(native, 0);
}

#[tokio::test]
async fn catalog_count_limit_charges_retained_runs_but_allows_exact_retries() {
    let (storage, scope, context, envelope) = fixture().await;
    storage
        .install_claim_input(&scope, SERVER, &context, &envelope)
        .await
        .unwrap();
    let mut tx = storage.pool().begin().await.unwrap();
    // Synthetic independent receipts/bindings exercise quota accounting only.
    // They are not used as provenance or native execution fixtures.
    for _ in 1..128 {
        let receipt = uuid::Uuid::new_v4().to_string();
        let run = uuid::Uuid::new_v4().to_string();
        let task = uuid::Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO mobile_claim_definition_receipts SELECT ?1,account_id,catalog_id,owner_epoch,server,remote_tenant,?2,worker_id,claim_epoch,reference_json,?1,root_id,definition_bytes FROM mobile_claim_definition_receipts LIMIT 1").bind(&receipt).bind(&task).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO mobile_remote_run_bindings SELECT ?1,account_id,catalog_id,server,remote_tenant,?2,owner_epoch,worker_id,claim_epoch,request_sha256,?3,?3 FROM mobile_remote_run_bindings LIMIT 1").bind(&run).bind(task).bind(receipt).execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO mobile_remote_run_inputs VALUES(?1,1,?2,?3,?4)")
            .bind(run)
            .bind(serde_json::to_string(&context.input).unwrap())
            .bind(&envelope.payload_base64)
            .bind(i64::from(context.input.expanded_bytes))
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    assert!(capacity(&mut tx, &scope, 0).await.is_err());
    tx.commit().await.unwrap();
    assert!(
        storage
            .install_claim_input(&scope, SERVER, &context, &envelope)
            .await
            .is_ok()
    );
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_remote_run_inputs")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(n, 128);
}
