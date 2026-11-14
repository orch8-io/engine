use crate::worker::claims::{ClaimKey, ClaimStore, Outcome};
use orch8_engine::remote_worker::RemoteTask;
use orch8_types::delegation_definition::{
    DefinitionGraphLimits,
    claim::DelegationClaimContext,
    input::{EncodedDelegationInput, InputLimits},
    protocol::DelegationResourceUsage,
    wire::DefinitionWireEnvelope,
};
use serde_json::{Value, json};

fn task() -> RemoteTask {
    let vector: Value = serde_json::from_str(include_str!(
        "../../../../../contracts/vectors/delegation-definition-v1.json"
    ))
    .unwrap();
    let mut definition: DefinitionWireEnvelope =
        serde_json::from_value(vector["envelope"].clone()).unwrap();
    definition.scope.worker_id = "worker".into();
    definition.scope.claim_epoch = 9_007_199_254_740_993;
    let graph = definition
        .verify(
            &definition.scope,
            &definition.reference,
            DefinitionGraphLimits::default(),
        )
        .unwrap();
    let input = EncodedDelegationInput::from_value(&json!(null), InputLimits::default()).unwrap();
    let context = DelegationClaimContext {
        version: 1,
        scope: definition.scope.clone(),
        request_sha256: "a".repeat(64),
        expires_at: chrono::Utc::now() + chrono::Duration::minutes(1),
        definition: definition.reference.clone(),
        input: input.reference().clone(),
        usage: DelegationResourceUsage {
            definition_expanded_bytes: graph.expanded_bytes(),
            input_wire_version: 1,
            input_payload_bytes: 4,
            input_expanded_bytes: 64,
        },
    };
    serde_json::from_value(json!({"id":context.scope.task_id,"instance_id":uuid::Uuid::new_v4(),"block_id":"mailbox","handler_name":"orch8.delegation","params":{},"context":{},"claim_epoch":context.scope.claim_epoch,"delegation":context})).unwrap()
}
#[tokio::test]
async fn context_is_atomic_immutable_and_survives_file_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("claims.db");
    let storage = orch8_storage::sqlite::SqliteStorage::file_mobile(path.to_str().unwrap())
        .await
        .unwrap();
    let store = ClaimStore::new(storage.pool().clone());
    store.init_tables().await.unwrap();
    let task = task();
    let key = ClaimKey::for_task(&task, "worker", "https://control.example/api/v1");
    assert!(store.insert(&task, "worker", &key.api_base).await.unwrap());
    assert!(store.mark_started(&key).await.unwrap());
    assert!(
        store
            .record_outcome(
                &key,
                &Outcome::Complete {
                    output: json!({"receipt":true})
                }
            )
            .await
            .unwrap()
    );
    let mut changed = task.clone();
    changed.delegation.as_mut().unwrap().input.payload_sha256 = "b".repeat(64);
    assert!(
        store
            .insert(&changed, "worker", &key.api_base)
            .await
            .is_err()
    );
    assert!(!store.insert(&task, "worker", &key.api_base).await.unwrap());
    assert!(
        sqlx::query("UPDATE mobile_worker_claims SET delegation_context=NULL")
            .execute(storage.pool())
            .await
            .is_err()
    );
    drop(store);
    storage.close().await;
    let storage = orch8_storage::sqlite::SqliteStorage::file_mobile(path.to_str().unwrap())
        .await
        .unwrap();
    let reopened = ClaimStore::new(storage.pool().clone());
    reopened.init_tables().await.unwrap();
    let row = reopened.get(&key).await.unwrap().unwrap();
    assert_eq!(row.delegation, task.delegation);
    assert!(row.started);
    assert!(row.outcome.is_some());
}
#[tokio::test]
async fn absent_legacy_context_and_corrupt_records_cannot_be_restamped() {
    let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
        .await
        .unwrap();
    let store = ClaimStore::new(storage.pool().clone());
    store.init_tables().await.unwrap();
    let task = task();
    let key = ClaimKey::for_task(&task, "worker", "https://control.example/api/v1");
    sqlx::query("INSERT INTO mobile_worker_claims(task_id,claim_epoch,worker_id,handler_name,api_base,claimed_at) VALUES(?1,?2,'worker','orch8.delegation',?3,?4)").bind(task.id.to_string()).bind(i64::try_from(task.claim_epoch).unwrap()).bind(&key.api_base).bind(chrono::Utc::now().to_rfc3339()).execute(storage.pool()).await.unwrap();
    assert!(store.insert(&task, "worker", &key.api_base).await.is_err());
    assert!(store.get(&key).await.unwrap().unwrap().delegation.is_none());
    sqlx::query("DROP TRIGGER mobile_claim_context_immutable")
        .execute(storage.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE mobile_worker_claims SET delegation_context=?1")
        .bind("{\"private\":\"JOURNAL_PRIVATE_CANARY\"}")
        .execute(storage.pool())
        .await
        .unwrap();
    let error = store.get(&key).await.unwrap_err();
    assert!(!format!("{error:?}").contains("JOURNAL_PRIVATE_CANARY"));
    assert!(store.insert(&task, "worker", &key.api_base).await.is_err());
}
