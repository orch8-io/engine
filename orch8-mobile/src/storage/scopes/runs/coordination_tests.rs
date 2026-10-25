//! Actual core coordination handlers over durable mobile owner proofs.
//! Mobile's public builtin allow-list remains unchanged.
use super::tests::{fixture, limits, native_fixture, request, scope};
use super::*;
use orch8_engine::handlers::{HandlerRegistry, StepContext};
use orch8_storage::{InstanceStore, SignalStore, sqlite::SqliteStorage};
use orch8_types::{
    error::{StepError, StorageError},
    ids::BlockId,
    instance::TaskInstance,
    signal::{Signal, SignalType},
};
use serde_json::{Value, json};
use std::sync::Arc;

async fn native(
    storage: &MobileStorage,
    sqlite: &SqliteStorage,
    owner: &CatalogScope,
    key: &str,
) -> TaskInstance {
    let request = storage
        .prepare_catalog_run(owner, request(Some(key), "{}"), limits())
        .await
        .unwrap();
    native_fixture(sqlite, &request).await
}
fn context(
    storage: Arc<SqliteStorage>,
    caller: &TaskInstance,
    target: &TaskInstance,
    handler: &str,
) -> StepContext {
    StepContext {
        effect_admission: None,
        claim_token: None,
        instance_id: caller.id,
        tenant_id: caller.tenant_id.clone(),
        block_id: BlockId::new(handler),
        params: json!({"instance_id":target.id,"signal_type":"resume","payload":{"test":"owned"}}),
        context: Arc::new(caller.context.clone()),
        attempt: 0,
        storage,
        wait_for_input: None,
    }
}
fn signal(target: &TaskInstance) -> Signal {
    Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: target.id,
        signal_type: SignalType::Resume,
        payload: Value::Null,
        delivered: false,
        created_at: chrono::Utc::now(),
        delivered_at: None,
    }
}
async fn denied(sqlite: &SqliteStorage, caller: &TaskInstance, target: &TaskInstance) {
    assert!(
        sqlite
            .get_instance_for_caller(caller.id, &caller.tenant_id, target.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        sqlite
            .enqueue_signal_for_caller(caller.id, &caller.tenant_id, &signal(target), None)
            .await,
        Err(StorageError::NotFound { .. })
    ));
}

#[tokio::test]
async fn real_coordination_handlers_isolate_four_durable_scopes() {
    let (storage, sqlite) = fixture().await;
    let mut owners = Vec::new();
    for account in ["alice", "bob"] {
        for catalog in ["one", "two"] {
            let owner = scope(&storage, account, catalog).await;
            let caller = native(&storage, &sqlite, &owner, "caller").await;
            let target = native(&storage, &sqlite, &owner, "target").await;
            owners.push((owner, caller, target));
        }
    }
    let mut registry = HandlerRegistry::new();
    orch8_engine::handlers::builtin::register_builtins(&mut registry);
    for (source, (_, caller, _)) in owners.iter().enumerate() {
        for (destination, (_, _, target)) in owners.iter().enumerate() {
            let result = registry.get("query_instance").unwrap()(context(
                sqlite.clone(),
                caller,
                target,
                "query_instance",
            ))
            .await
            .unwrap();
            assert_eq!(result["found"], source == destination);
            let sent = registry.get("send_signal").unwrap()(context(
                sqlite.clone(),
                caller,
                target,
                "send_signal",
            ))
            .await;
            if source == destination {
                assert!(sent.is_ok());
            } else {
                assert!(
                    matches!(sent,Err(StepError::Permanent{message,..}) if message=="target instance not found")
                );
            }
        }
    }
    for (_, _, target) in owners {
        assert_eq!(
            sqlite.get_pending_signals(target.id).await.unwrap().len(),
            1
        );
    }
}

#[tokio::test]
async fn scoped_coordination_refuses_relabelled_proofs_legacy_fallback_and_old_epochs() {
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let caller = native(&storage, &sqlite, &owner, "caller").await;
    let target = native(&storage, &sqlite, &owner, "target").await;
    let mut forged = caller.clone();
    forged.id = orch8_types::ids::InstanceId::new();
    sqlite.create_instance(&forged).await.unwrap();
    denied(&sqlite, &forged, &target).await;
    denied(&sqlite, &caller, &forged).await;
    sqlx::query("UPDATE task_instances SET namespace = 'foreign-catalog' WHERE id = ?1")
        .bind(target.id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    denied(&sqlite, &caller, &target).await;
    sqlx::query("UPDATE task_instances SET namespace = ?2 WHERE id = ?1")
        .bind(target.id.to_string())
        .bind(target.namespace.as_str())
        .execute(storage.pool())
        .await
        .unwrap();
    storage.retire_account_scope(&owner.account).await.unwrap();
    denied(&sqlite, &caller, &target).await;
    let fresh = scope(&storage, "alice", "one").await;
    let new_caller = native(&storage, &sqlite, &fresh, "fresh-caller").await;
    let new_target = native(&storage, &sqlite, &fresh, "fresh-target").await;
    denied(&sqlite, &caller, &new_target).await;
    denied(&sqlite, &new_caller, &target).await;
    assert!(
        sqlite
            .get_instance_for_caller(new_caller.id, &new_caller.tenant_id, new_target.id)
            .await
            .unwrap()
            .is_some()
    );
    sqlite
        .enqueue_signal_for_caller(
            new_caller.id,
            &new_caller.tenant_id,
            &signal(&new_target),
            None,
        )
        .await
        .unwrap();
    assert!(
        sqlite
            .get_pending_signals(target.id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn signal_waiting_for_writer_rechecks_retirement_in_an_independent_pool() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("coordination.sqlite");
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
    let owner = scope(&storage, "alice", "one").await;
    let caller = native(&storage, &sqlite, &owner, "caller").await;
    let target = native(&storage, &sqlite, &owner, "target").await;
    assert!(
        other
            .get_instance_for_caller(caller.id, &caller.tenant_id, target.id)
            .await
            .unwrap()
            .is_some()
    );
    let mut retirement = storage.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE mobile_accounts SET active=0 WHERE id=?1")
        .bind(owner.account_id())
        .execute(&mut *retirement)
        .await
        .unwrap();
    let message = signal(&target);
    let mut pending =
        Box::pin(other.enqueue_signal_for_caller(caller.id, &caller.tenant_id, &message, None));
    assert!(
        std::future::Future::poll(
            pending.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending(),
        "enqueue waits for the writer instead of trusting its earlier read"
    );
    retirement.commit().await.unwrap();
    assert!(matches!(pending.await, Err(StorageError::NotFound { .. })));
    assert!(
        sqlite
            .get_pending_signals(target.id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn real_signal_handler_refuses_another_catalog_and_cancelled_request_proof() {
    let (storage, sqlite) = fixture().await;
    let one = scope(&storage, "alice", "one").await;
    let two = scope(&storage, "alice", "two").await;
    let caller = native(&storage, &sqlite, &one, "caller").await;
    let foreign = native(&storage, &sqlite, &two, "target").await;
    let own = native(&storage, &sqlite, &one, "target").await;
    let mut registry = HandlerRegistry::new();
    orch8_engine::handlers::builtin::register_builtins(&mut registry);
    let sent = registry.get("send_signal").unwrap()(context(
        sqlite.clone(),
        &caller,
        &foreign,
        "send_signal",
    ))
    .await;
    assert!(
        matches!(sent,Err(StepError::Permanent{message,..}) if message=="target instance not found")
    );
    assert!(
        sqlite
            .get_pending_signals(foreign.id)
            .await
            .unwrap()
            .is_empty()
    );
    // Admission cancellation is intentionally limited to never-dispatched
    // requests. A forged/stale native row cannot bypass a cancelled proof.
    assert!(!storage.cancel_catalog_run(&one, caller.id).await.unwrap());
    let cancelled = storage
        .prepare_catalog_run(&one, request(Some("cancelled"), "{}"), limits())
        .await
        .unwrap();
    assert!(
        storage
            .cancel_catalog_run(&one, cancelled.id)
            .await
            .unwrap()
    );
    let cancelled_caller = native_fixture(&sqlite, &cancelled).await;
    denied(&sqlite, &cancelled_caller, &own).await;
    assert!(sqlite.get_pending_signals(own.id).await.unwrap().is_empty());
}

#[tokio::test]
async fn real_signal_handler_returns_claim_lost_after_preflight_owner_remains_valid() {
    let (storage, sqlite) = fixture().await;
    let owner = scope(&storage, "alice", "one").await;
    let caller = native(&storage, &sqlite, &owner, "caller").await;
    let target = native(&storage, &sqlite, &owner, "target").await;
    let claim = sqlite
        .claim_due_instances(chrono::Utc::now() + chrono::Duration::seconds(1), 8, 0)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == caller.id)
        .unwrap();
    let mut ctx = context(sqlite.clone(), &caller, &target, "send_signal");
    ctx.claim_token = claim.claimed_at;
    sqlite
        .update_instance_state(
            caller.id,
            orch8_types::instance::InstanceState::Waiting,
            None,
        )
        .await
        .unwrap();
    let mut registry = HandlerRegistry::new();
    orch8_engine::handlers::builtin::register_builtins(&mut registry);
    assert!(matches!(
        registry.get("send_signal").unwrap()(ctx).await,
        Err(StepError::ClaimLost)
    ));
    assert!(
        sqlite
            .get_pending_signals(target.id)
            .await
            .unwrap()
            .is_empty()
    );
}
