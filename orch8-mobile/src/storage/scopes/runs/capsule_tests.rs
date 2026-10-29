//! Capture and publication authority must survive concurrent scope retirement.
use super::{
    materialize_tests::owner_with_work,
    tests::{limits, native_fixture, request},
    *,
};
use chrono::{Duration, Utc};
use orch8_engine::capsule::{
    CapsuleExportRequest, CapsuleServiceError, export_paused_capsule_manifest,
    export_paused_capsule_manifest_with_mobile_access,
};
use orch8_storage::{
    ContinuityStore, InstanceStore, MobileInstanceAccess, ResourceStore, StorageBackend,
    artifacts::ObjectArtifactStore, sqlite::SqliteStorage, testing::faulty::FaultStorage,
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{
        CapsuleManifest, CapsuleRequirements, ContinuityExecution, ContinuityId, ExecutionEpoch,
        OwnershipState, RuntimeId,
    },
    encryption::FieldEncryptor,
    instance::InstanceState,
};
use std::sync::Arc;

struct Fixture {
    mobile: Arc<MobileStorage>,
    sqlite: Arc<SqliteStorage>,
    scope: CatalogScope,
    request: CapsuleExportRequest,
    access: MobileInstanceAccess,
}
async fn fixture() -> Fixture {
    let sqlite = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
    );
    let mobile = Arc::new(MobileStorage::new(sqlite.clone()));
    fixture_in(mobile, sqlite, "alice", "one").await
}
async fn fixture_in(
    mobile: Arc<MobileStorage>,
    sqlite: Arc<SqliteStorage>,
    account: &str,
    catalog: &str,
) -> Fixture {
    let scope = owner_with_work(&mobile, account, catalog).await;
    let run = mobile
        .prepare_catalog_run(&scope, request(Some("capsule"), "{}"), limits())
        .await
        .unwrap();
    let instance = native_fixture(&sqlite, &run).await;
    sqlite
        .update_instance_state(instance.id, InstanceState::Paused, None)
        .await
        .unwrap();
    sqlite
        .save_checkpoint(&Checkpoint {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            checkpoint_data: serde_json::json!({"completed_blocks":[]}),
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id,
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    sqlite.create_continuity_execution(&owner).await.unwrap();
    let request = CapsuleExportRequest {
        continuity: owner,
        destination_runtime_id: Some(RuntimeId::new()),
        requirements: CapsuleRequirements::default(),
        expires_at: Utc::now() + Duration::minutes(5),
        signing_key_id: "scoped-signer".into(),
        encryption_key_id: "transfer".into(),
    };
    let access = MobileInstanceAccess::Catalog {
        account_id: scope.account_id().into(),
        catalog_id: scope.catalog_id().into(),
        session_epoch: scope.epoch(),
    };
    Fixture {
        mobile,
        sqlite,
        scope,
        request,
        access,
    }
}
async fn export(
    storage: &dyn StorageBackend,
    request: CapsuleExportRequest,
    access: &MobileInstanceAccess,
) -> Result<CapsuleManifest, CapsuleServiceError> {
    export_paused_capsule_manifest_with_mobile_access(
        storage,
        request,
        &FieldEncryptor::from_bytes(&[3; 32]),
        access,
    )
    .await
}
#[tokio::test]
async fn scoped_capture_requires_the_exact_current_account_and_catalog() {
    let f = fixture().await;
    assert!(
        export_paused_capsule_manifest(
            f.sqlite.as_ref(),
            f.request.clone(),
            &FieldEncryptor::from_bytes(&[3; 32])
        )
        .await
        .is_err()
    );
    for access in [
        MobileInstanceAccess::Legacy,
        MobileInstanceAccess::Catalog {
            account_id: "foreign".into(),
            catalog_id: f.scope.catalog_id().into(),
            session_epoch: f.scope.epoch(),
        },
        MobileInstanceAccess::Catalog {
            account_id: f.scope.account_id().into(),
            catalog_id: "foreign".into(),
            session_epoch: f.scope.epoch(),
        },
        MobileInstanceAccess::Catalog {
            account_id: f.scope.account_id().into(),
            catalog_id: f.scope.catalog_id().into(),
            session_epoch: f.scope.epoch() + 1,
        },
    ] {
        assert!(
            export(f.sqlite.as_ref(), f.request.clone(), &access)
                .await
                .is_err()
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capsule_source_cuts")
        .fetch_one(f.sqlite.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(
        f.sqlite
            .get_continuity_execution(
                &f.request.continuity.tenant_id,
                f.request.continuity.continuity_id
            )
            .await
            .unwrap()
            .unwrap(),
        f.request.continuity
    );
    let manifest = export(f.sqlite.as_ref(), f.request.clone(), &f.access)
        .await
        .unwrap();
    let body: String =
        sqlx::query_scalar("SELECT request FROM capsule_source_cuts WHERE capsule_id=?1")
            .bind(manifest.capsule_id.to_string())
            .fetch_one(f.sqlite.pool())
            .await
            .unwrap();
    let stored: orch8_storage::capsule_export::CapsuleCutRequest =
        serde_json::from_str(&body).unwrap();
    assert_eq!(stored.mobile_access, Some(f.access));
}
#[tokio::test]
async fn retirement_winning_before_capture_creates_no_cut_or_artifact() {
    let f = fixture().await;
    let (wrapped, faults) = FaultStorage::wrap(f.sqlite.clone());
    let held = faults.on("capture_capsule_cut").times(1).pause();
    let request = f.request.clone();
    let access = f.access.clone();
    let pending = tokio::spawn(async move { export(wrapped.as_ref(), request, &access).await });
    tokio::time::timeout(std::time::Duration::from_secs(10), held.entered())
        .await
        .unwrap();
    f.mobile
        .retire_account_scope(&f.scope.account)
        .await
        .unwrap();
    held.release();
    assert!(pending.await.unwrap().is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capsule_source_cuts")
        .fetch_one(f.sqlite.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        f.sqlite
            .list_artifacts(f.request.continuity.current_instance_id)
            .await
            .unwrap()
            .is_empty()
    );
}
async fn retire_during_publication(cancel: bool) {
    let f = fixture().await;
    let (wrapped, faults) = FaultStorage::wrap(f.sqlite.clone());
    let held = faults.on("put_artifact_with_id").times(1).pause();
    let request = f.request.clone();
    let access = f.access.clone();
    let pending = tokio::spawn(async move { export(wrapped.as_ref(), request, &access).await });
    tokio::time::timeout(std::time::Duration::from_secs(10), held.entered())
        .await
        .unwrap();
    if cancel {
        let backend: Arc<dyn StorageBackend> = f.sqlite.clone();
        assert_eq!(
            backend
                .cancel_mobile_instance(&f.access, f.request.continuity.current_instance_id)
                .await
                .unwrap(),
            Some(InstanceState::Cancelled)
        );
    } else {
        f.mobile
            .retire_account_scope(&f.scope.account)
            .await
            .unwrap();
    }
    let retired: bool = sqlx::query_scalar("SELECT state='retired' AND snapshot IS NULL AND sealed_payload IS NULL FROM capsule_source_cuts")
        .fetch_one(f.sqlite.pool()).await.unwrap();
    assert!(retired);
    held.release();
    assert!(matches!(
        pending.await.unwrap(),
        Err(CapsuleServiceError::OwnershipMismatch)
    ));
    assert!(
        f.sqlite
            .list_artifacts(f.request.continuity.current_instance_id)
            .await
            .unwrap()
            .is_empty()
    );
    let cleanup: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capsule_artifact_retirements")
        .fetch_one(f.sqlite.pool())
        .await
        .unwrap();
    assert_eq!(cleanup, 1);
    assert_eq!(
        f.sqlite
            .get_continuity_execution(
                &f.request.continuity.tenant_id,
                f.request.continuity.continuity_id
            )
            .await
            .unwrap()
            .unwrap()
            .state,
        OwnershipState::Transferring
    );
}
#[tokio::test]
async fn account_retirement_during_upload_retires_bytes_and_blocks_publication() {
    retire_during_publication(false).await;
}
#[tokio::test]
async fn run_cancellation_during_upload_retires_bytes_without_unfencing_source() {
    retire_during_publication(true).await;
}

#[tokio::test]
async fn retiring_one_account_keeps_another_accounts_transport_and_authority() {
    let alice = fixture().await;
    let bob = fixture_in(alice.mobile.clone(), alice.sqlite.clone(), "bob", "one").await;
    let a = export(alice.sqlite.as_ref(), alice.request.clone(), &alice.access)
        .await
        .unwrap();
    let b = export(bob.sqlite.as_ref(), bob.request.clone(), &bob.access)
        .await
        .unwrap();
    alice
        .mobile
        .retire_account_scope(&alice.scope.account)
        .await
        .unwrap();
    assert!(
        !alice
            .sqlite
            .capsule_cut_publishable(&a.tenant_id, a.capsule_id)
            .await
            .unwrap()
    );
    assert!(
        bob.sqlite
            .capsule_cut_publishable(&b.tenant_id, b.capsule_id)
            .await
            .unwrap()
    );
    orch8_engine::gc::collect_retired_capsule_artifacts(
        alice.sqlite.as_ref(),
        Utc::now() + Duration::seconds(1),
        32,
    )
    .await
    .unwrap();
    assert!(
        alice
            .sqlite
            .get_artifact(&a.payload_artifact.key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        bob.sqlite
            .get_artifact(&b.payload_artifact.key)
            .await
            .unwrap()
            .is_some()
    );
    let retry = export(bob.sqlite.as_ref(), bob.request, &bob.access)
        .await
        .unwrap();
    assert_eq!(retry, b);
}

#[tokio::test]
async fn later_verified_acceptance_reconciles_retired_transport_without_resuming_source() {
    let f = fixture().await;
    let manifest = export(f.sqlite.as_ref(), f.request.clone(), &f.access)
        .await
        .unwrap();
    f.mobile
        .retire_account_scope(&f.scope.account)
        .await
        .unwrap();
    let accepted = ContinuityExecution {
        epoch: f.request.continuity.epoch.checked_next().unwrap(),
        owner_runtime_id: f.request.destination_runtime_id.unwrap(),
        current_instance_id: InstanceId::new(),
        state: OwnershipState::Owned,
        ..f.request.continuity.clone()
    };
    let receipt = f
        .sqlite
        .acknowledge_capsule_cut(&accepted.tenant_id, manifest.capsule_id, &accepted)
        .await
        .unwrap();
    assert_eq!(receipt.accepted_owner, accepted);
    assert_eq!(receipt.manifest, manifest);
    assert_eq!(
        f.sqlite
            .get_instance(f.request.continuity.current_instance_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Cancelled
    );
    assert_eq!(
        f.sqlite
            .get_continuity_execution(&accepted.tenant_id, accepted.continuity_id)
            .await
            .unwrap()
            .unwrap(),
        accepted
    );
    assert_eq!(
        f.sqlite
            .acknowledge_capsule_cut(&accepted.tenant_id, manifest.capsule_id, &accepted)
            .await
            .unwrap(),
        receipt
    );
}
