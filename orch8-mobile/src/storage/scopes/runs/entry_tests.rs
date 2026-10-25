//! Generic core entry must honor durable mobile retirement as well as claims.
use super::tests::{limits, native_fixture, request, scope};
use super::*;
use orch8_engine::{
    error::EngineError,
    handlers::{
        HandlerRegistry,
        step::{StepExecParams, execute_step_dry},
    },
};
use orch8_storage::{InstanceStore, StorageBackend, sqlite::SqliteStorage};
use orch8_types::{context::ExecutionContext, ids::BlockId};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn core_entry_refuses_retired_owner_even_with_a_retained_running_token() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("entry.sqlite");
    let sqlite = Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let storage = MobileStorage::new(sqlite.clone());
    let independent = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let owner = scope(&storage, "alice", "one").await;
    let run = storage
        .prepare_catalog_run(&owner, request(Some("entry"), "{}"), limits())
        .await
        .unwrap();
    let instance = native_fixture(&sqlite, &run).await;
    let token = sqlite
        .claim_due_instances(chrono::Utc::now(), 8, 0)
        .await
        .unwrap()
        .pop()
        .unwrap()
        .claimed_at;
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut registry = HandlerRegistry::new();
    registry.register("http_request", move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        async { Ok(serde_json::json!({})) }
    });
    independent
        .retire_account_scope(&owner.account)
        .await
        .unwrap();
    let backend: Arc<dyn StorageBackend> = sqlite.clone();
    let exec = StepExecParams {
        claim_token: token,
        instance_id: instance.id,
        tenant_id: instance.tenant_id.clone(),
        block_id: BlockId::new("work"),
        handler_name: "http_request".into(),
        params: serde_json::json!({"url":"https://provider.example/action"}),
        context: ExecutionContext::default(),
        attempt: 0,
        timeout: None,
        externalize_threshold: 0,
        wait_for_input: None,
        cache_key: None,
        output_schema: None,
    };
    assert!(matches!(
        execute_step_dry(&backend, &registry, exec).await,
        Err(EngineError::ClaimLost { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        backend
            .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
            .await
            .unwrap()
            .is_none()
    );
}
