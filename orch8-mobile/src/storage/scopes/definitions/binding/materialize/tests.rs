use super::*;
use crate::storage::scopes::{
    definitions::tests::{bundle, catalog},
    peers::WorkerPeerDescriptor,
};
use orch8_storage::{AdminStore, ClaimedRead, InstanceStore, MobileInstanceAccess, WorkerStore};
use orch8_types::{
    continuity::RuntimeId,
    delegation_definition::{
        input::{DelegationInputEnvelope, EncodedDelegationInput, InputLimits},
        protocol::DelegationResourceUsage,
    },
    worker::WorkerTask,
};
use std::sync::Arc;
const SERVER: &str = "https://engine.example/";
const REQUEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
async fn initialize(
    storage: &MobileStorage,
    peer: &CatalogWorkerPeer,
    context: &DelegationClaimContext,
    limits: InstanceLimits,
    stop: &CancellationToken,
) -> Result<TaskInstance, StorageError> {
    let expected = storage
        .bind_claim_run(
            peer.scope(),
            SERVER,
            &context.scope,
            &context.definition,
            &context.request_sha256,
        )
        .await?;
    storage
        .initialize_remote_instance_for_run(peer, context, &expected, limits, stop)
        .await
}
fn limits() -> InstanceLimits {
    InstanceLimits {
        global: 16,
        account: 8,
        catalog: 4,
    }
}
async fn fixture() -> (MobileStorage, CatalogWorkerPeer, DelegationClaimContext) {
    let storage = MobileStorage::new(Arc::new(SqliteStorage::in_memory().await.unwrap()));
    setup(storage).await
}
async fn setup(
    storage: MobileStorage,
) -> (MobileStorage, CatalogWorkerPeer, DelegationClaimContext) {
    let scope = catalog(&storage, "alice", "work").await;
    let runtime = RuntimeId::new();
    let (mut wire, mut claim, reference) = bundle("private");
    claim.worker_id = runtime.to_string();
    claim.claim_epoch = 1;
    wire.scope = claim.clone();
    let expanded = wire
        .verify(
            &claim,
            &reference,
            orch8_types::delegation_definition::DefinitionGraphLimits::default(),
        )
        .unwrap()
        .expanded_bytes();
    let peer = storage
        .bind_worker_peer(
            &scope,
            WorkerPeerDescriptor {
                peer_key: "backend",
                api_base: SERVER,
                remote_tenant: claim.tenant_id.as_str(),
                runtime_id: runtime,
            },
        )
        .await
        .unwrap();
    storage
        .install_claim_definitions(&scope, SERVER, &claim, &reference, &wire)
        .await
        .unwrap();
    storage
        .bind_claim_run(&scope, SERVER, &claim, &reference, REQUEST)
        .await
        .unwrap();
    let input = EncodedDelegationInput::from_value(
        &serde_json::json!({"wide":u64::MAX,"private":"PRIVATE_INIT"}),
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
            definition_expanded_bytes: expanded,
            input_wire_version: 1,
            input_payload_bytes: input.reference().payload_bytes,
            input_expanded_bytes: input.reference().expanded_bytes,
        },
    };
    let envelope =
        DelegationInputEnvelope::new(context.scope.clone(), REQUEST.into(), &input).unwrap();
    storage
        .install_claim_input(&scope, SERVER, &context, &envelope)
        .await
        .unwrap();
    (storage, peer, context)
}
#[tokio::test]
async fn materialization_is_paused_atomic_and_idempotent_across_claims() {
    let (storage, peer, mut context) = fixture().await;
    let stop = CancellationToken::new();
    let first = initialize(&storage, &peer, &context, limits(), &stop)
        .await
        .unwrap();
    assert_eq!(first.state, InstanceState::Paused);
    assert!(first.next_fire_at.is_none());
    assert!(first.claimed_at.is_none());
    assert_eq!(first.context.data["wide"].as_u64(), Some(u64::MAX));
    context.scope.claim_epoch = u64::MAX;
    storage
        .bind_claim_run(
            peer.scope(),
            SERVER,
            &context.scope,
            &context.definition,
            REQUEST,
        )
        .await
        .unwrap();
    let again = initialize(
        &storage,
        &peer,
        &context,
        InstanceLimits {
            global: 0,
            account: 0,
            catalog: 0,
        },
        &stop,
    )
    .await
    .unwrap();
    assert_eq!(again.id, first.id);
    assert_eq!(again.sequence_id, first.sequence_id);
    assert_eq!(again.created_at, first.created_at);
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(n, 1);
}
#[tokio::test]
async fn mutable_native_rows_cannot_downgrade_into_legacy_authority() {
    let (storage, peer, context) = fixture().await;
    let instance = initialize(
        &storage,
        &peer,
        &context,
        limits(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let now = chrono::Utc::now();
    sqlx::query("UPDATE task_instances SET tenant_id='mobile',namespace='default',state='scheduled',next_fire_at=?2 WHERE id=?1").bind(instance.id.to_string()).bind(now.to_rfc3339()).execute(storage.pool()).await.unwrap();
    assert!(
        storage
            .inner
            .claim_due_instances(now + chrono::Duration::seconds(1), 16, 16)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        storage
            .inner
            .get_mobile_instance(&MobileInstanceAccess::Legacy, instance.id)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("UPDATE task_instances SET state='running',claimed_at=?2 WHERE id=?1")
        .bind(instance.id.to_string())
        .bind(now.to_rfc3339())
        .execute(storage.pool())
        .await
        .unwrap();
    assert!(matches!(
        storage
            .inner
            .get_credential_for_claim(
                instance.id,
                &TenantId::new("mobile").unwrap(),
                now,
                "global-secret"
            )
            .await
            .unwrap(),
        ClaimedRead::Lost
    ));
    let task:WorkerTask=serde_json::from_value(serde_json::json!({"id":uuid::Uuid::new_v4(),"instance_id":instance.id,"block_id":"a","handler_name":"noop","params":{},"context":{},"attempt":1,"state":"pending","created_at":now})).unwrap();
    assert!(storage.inner.create_worker_task(&task).await.is_err());
    assert!(
        initialize(
            &storage,
            &peer,
            &context,
            limits(),
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
}
#[tokio::test]
async fn missing_or_future_receipts_are_not_adopted_or_recreated() {
    for action in [
        "DELETE FROM mobile_remote_run_materializations",
        "UPDATE mobile_remote_run_materializations SET record=json_set(record,'$.version',2)",
    ] {
        let (storage, peer, context) = fixture().await;
        initialize(
            &storage,
            &peer,
            &context,
            limits(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(
            sqlx::query("DELETE FROM mobile_remote_run_materializations")
                .execute(storage.pool())
                .await
                .is_err()
        );
        sqlx::query("DROP TRIGGER mobile_remote_materialization_delete")
            .execute(storage.pool())
            .await
            .unwrap();
        sqlx::query("DROP TRIGGER mobile_remote_materialization_update")
            .execute(storage.pool())
            .await
            .unwrap();
        if action.starts_with("DELETE") {
            sqlx::query("DROP TRIGGER mobile_remote_physical_delete")
                .execute(storage.pool())
                .await
                .unwrap();
            sqlx::query("DELETE FROM mobile_remote_physical_roots")
                .execute(storage.pool())
                .await
                .unwrap();
        }
        sqlx::query(action).execute(storage.pool()).await.unwrap();
        sqlx::query(
            "UPDATE task_instances SET tenant_id='mobile',state='scheduled',next_fire_at=?1",
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(storage.pool())
        .await
        .unwrap();
        assert!(
            storage
                .inner
                .claim_due_instances(chrono::Utc::now() + chrono::Duration::seconds(1), 16, 16)
                .await
                .unwrap()
                .is_empty()
        );
        let id = storage
            .bind_claim_run(
                peer.scope(),
                SERVER,
                &context.scope,
                &context.definition,
                REQUEST,
            )
            .await
            .unwrap()
            .local_run_id;
        assert!(
            storage
                .inner
                .get_mobile_instance(&MobileInstanceAccess::Legacy, id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            initialize(
                &storage,
                &peer,
                &context,
                limits(),
                &CancellationToken::new()
            )
            .await
            .is_err()
        );
    }
}
#[tokio::test]
async fn suppressed_receipt_and_initial_body_mutation_roll_back_instance() {
    for trigger in [
        "CREATE TRIGGER sabotage BEFORE INSERT ON mobile_remote_run_materializations BEGIN SELECT RAISE(IGNORE); END",
        "CREATE TRIGGER sabotage AFTER INSERT ON task_instances BEGIN UPDATE task_instances SET context='{}' WHERE id=NEW.id; END",
    ] {
        let (storage, peer, context) = fixture().await;
        sqlx::query(trigger).execute(storage.pool()).await.unwrap();
        assert!(
            initialize(
                &storage,
                &peer,
                &context,
                limits(),
                &CancellationToken::new()
            )
            .await
            .is_err()
        );
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(n, 0);
    }
}
#[tokio::test]
async fn cancelled_expired_retired_or_over_capacity_initialization_creates_no_instance() {
    let (storage, peer, mut context) = fixture().await;
    let stop = CancellationToken::new();
    stop.cancel();
    assert!(
        initialize(&storage, &peer, &context, limits(), &stop)
            .await
            .is_err()
    );
    assert!(
        initialize(
            &storage,
            &peer,
            &context,
            InstanceLimits {
                global: 0,
                account: 0,
                catalog: 0
            },
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
    context.expires_at = chrono::Utc::now() - chrono::Duration::seconds(1);
    assert!(
        initialize(
            &storage,
            &peer,
            &context,
            limits(),
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
    context.expires_at = chrono::Utc::now() + chrono::Duration::hours(1);
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    storage.retire_account_scope(&account).await.unwrap();
    assert!(
        initialize(
            &storage,
            &peer,
            &context,
            limits(),
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(n, 0);
}
#[tokio::test]
async fn root_survives_database_reopen_with_original_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("remote.db");
    let (storage, peer, context) = setup(MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    )))
    .await;
    let first = initialize(
        &storage,
        &peer,
        &context,
        limits(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    storage.pool().close().await;
    drop(storage);
    let storage = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let reopened = initialize(
        &storage,
        &peer,
        &context,
        limits(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(first.id, reopened.id);
    assert_eq!(first.created_at, reopened.created_at);
    assert_eq!(reopened.state, InstanceState::Paused);
}

#[tokio::test]
async fn quota_uses_durable_owner_even_if_native_tenant_and_namespace_change() {
    let (storage, peer, context) = fixture().await;
    initialize(
        &storage,
        &peer,
        &context,
        limits(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE task_instances SET tenant_id='mobile',namespace='default'")
        .execute(storage.pool())
        .await
        .unwrap();
    let mut tx = storage.pool().begin().await.unwrap();
    for limits in [
        InstanceLimits {
            global: 1,
            account: 8,
            catalog: 4,
        },
        InstanceLimits {
            global: 16,
            account: 1,
            catalog: 4,
        },
        InstanceLimits {
            global: 16,
            account: 8,
            catalog: 1,
        },
    ] {
        assert!(
            !crate::storage::scopes::instance_capacity::available(&mut tx, peer.scope(), limits)
                .await
                .unwrap()
        );
    }
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn another_account_cannot_create_or_adopt_the_same_remote_physical_root() {
    let (storage, peer, mut context) = fixture().await;
    let first = initialize(
        &storage,
        &peer,
        &context,
        limits(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let other = catalog(&storage, "bob", "work").await;
    let runtime = RuntimeId::new();
    context.scope.worker_id = runtime.to_string();
    context.scope.claim_epoch = 2;
    let next = storage
        .bind_worker_peer(
            &other,
            WorkerPeerDescriptor {
                peer_key: "backend",
                api_base: SERVER,
                remote_tenant: context.scope.tenant_id.as_str(),
                runtime_id: runtime,
            },
        )
        .await
        .unwrap();
    let (mut wire, _, reference) = bundle("private");
    assert_eq!(reference, context.definition);
    wire.scope = context.scope.clone();
    storage
        .install_claim_definitions(&other, SERVER, &context.scope, &context.definition, &wire)
        .await
        .unwrap();
    let logical = storage
        .bind_claim_run(&other, SERVER, &context.scope, &context.definition, REQUEST)
        .await
        .unwrap();
    assert_ne!(logical.local_run_id, first.id);
    let input = EncodedDelegationInput::from_value(
        &serde_json::json!({"wide":u64::MAX,"private":"PRIVATE_INIT"}),
        InputLimits::default(),
    )
    .unwrap();
    let envelope =
        DelegationInputEnvelope::new(context.scope.clone(), REQUEST.into(), &input).unwrap();
    storage
        .install_claim_input(&other, SERVER, &context, &envelope)
        .await
        .unwrap();
    let error = initialize(
        &storage,
        &next,
        &context,
        limits(),
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(!format!("{error:?}").contains("PRIVATE_INIT"));
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(n, 1);
}
#[tokio::test]
async fn independent_pools_initialize_one_native_root() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("race.db");
    let (storage, peer, context) = setup(MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    )))
    .await;
    let second = MobileStorage::new(Arc::new(
        SqliteStorage::file_mobile(path.to_str().unwrap())
            .await
            .unwrap(),
    ));
    let stop = CancellationToken::new();
    let (a, b) = tokio::join!(
        initialize(&storage, &peer, &context, limits(), &stop),
        initialize(&second, &peer, &context, limits(), &stop)
    );
    assert_eq!(a.unwrap().id, b.unwrap().id);
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_remote_physical_roots")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(n, 1);
}
#[tokio::test]
async fn a_new_signin_reuses_the_original_native_root_without_restamping_provenance() {
    let (storage, peer, mut context) = fixture().await;
    let first = initialize(
        &storage,
        &peer,
        &context,
        limits(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    storage.retire_account_scope(&account).await.unwrap();
    assert!(
        initialize(
            &storage,
            &peer,
            &context,
            limits(),
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
    let scope = catalog(&storage, "alice", "work").await;
    let fresh = storage
        .bind_worker_peer(
            &scope,
            WorkerPeerDescriptor {
                peer_key: "backend",
                api_base: SERVER,
                remote_tenant: context.scope.tenant_id.as_str(),
                runtime_id: peer.runtime_id(),
            },
        )
        .await
        .unwrap();
    context.scope.claim_epoch = 2;
    storage
        .bind_claim_run(&scope, SERVER, &context.scope, &context.definition, REQUEST)
        .await
        .unwrap();
    let reopened = initialize(
        &storage,
        &fresh,
        &context,
        limits(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(first.id, reopened.id);
    assert_eq!(first.created_at, reopened.created_at);
}

#[tokio::test]
async fn changed_prepared_identity_cannot_allocate_or_pin_a_different_root() {
    let (storage, peer, context) = fixture().await;
    for mismatch in ["id", "graph", "worker", "epoch"] {
        let mut expected = storage
            .bind_claim_run(
                peer.scope(),
                SERVER,
                &context.scope,
                &context.definition,
                REQUEST,
            )
            .await
            .unwrap();
        match mismatch {
            "id" => expected.local_run_id = InstanceId::new(),
            "graph" => expected.definitions.root_id = SequenceId::new(),
            "worker" => expected.worker_id = "other".into(),
            "epoch" => expected.claim_epoch += 1,
            _ => unreachable!(),
        }
        assert!(
            storage
                .initialize_remote_instance_for_run(
                    &peer,
                    &context,
                    &expected,
                    limits(),
                    &CancellationToken::new()
                )
                .await
                .is_err(),
            "{mismatch}"
        );
    }
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM mobile_remote_physical_roots")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(n, 0);
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(n, 0);
}
