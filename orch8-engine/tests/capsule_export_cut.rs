//! Source export must revoke captured admission before publishing its snapshot.
use chrono::{Duration, Utc};
use ed25519_dalek::SigningKey;
use orch8_engine::{
    capsule::{
        CapsuleExportRequest, CapsuleImportRequest, activate_imported_capsule,
        export_paused_capsule, verify_and_import_paused_capsule_bytes,
    },
    handlers::{
        HandlerRegistry,
        step::{StepExecParams, execute_step},
    },
};
use orch8_storage::{
    ResourceStore, StorageBackend,
    artifacts::ObjectArtifactStore,
    sqlite::SqliteStorage,
    testing::{
        faulty::FaultStorage,
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{
        CapsuleRequirements, ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState,
        RuntimeId,
    },
    encryption::FieldEncryptor,
    ids::BlockId,
    instance::InstanceState,
};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn params(instance: &orch8_types::instance::TaskInstance) -> StepExecParams {
    StepExecParams {
        claim_token: instance.claimed_at,
        instance_id: instance.id,
        tenant_id: instance.tenant_id.clone(),
        block_id: BlockId::new("charge"),
        handler_name: "native-charge".into(),
        params: json!({}),
        context: instance.context.clone(),
        attempt: 0,
        timeout: None,
        externalize_threshold: 0,
        wait_for_input: None,
        cache_key: None,
        output_schema: None,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one held-callback scenario crosses the complete signed transfer boundary
async fn source_resume_during_publication_cannot_double_dispatch_a_prepared_effect() {
    let source: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
    );
    let destination: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let sequence = SequenceBuilder::new("export-cut")
        .step("charge", "native-charge")
        .build();
    source.create_sequence(&sequence).await.unwrap();
    destination.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Running)
        .build();
    source.create_instance(&instance).await.unwrap();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    source.create_continuity_execution(&owner).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let (ready, prepared) = tokio::sync::oneshot::channel();
    let ready = Arc::new(Mutex::new(Some(ready)));
    let mut handlers = HandlerRegistry::new();
    let count = calls.clone();
    let released = release.clone();
    handlers.register_with_deferred_effect_admission("native-charge", move |ctx| {
        let ready = ready.clone();
        let release = released.clone();
        let calls = count.clone();
        async move {
            ready.lock().unwrap().take().unwrap().send(()).unwrap();
            release.notified().await;
            ctx.effect_admission.as_ref().unwrap().admit().await?;
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!({"provider":"charged"}))
        }
    });
    let src = source.clone();
    let exec = params(&instance);
    let source_task = tokio::spawn(async move { execute_step(&src, &handlers, exec).await });
    tokio::time::timeout(std::time::Duration::from_secs(10), prepared)
        .await
        .unwrap()
        .unwrap();
    source
        .update_instance_state(instance.id, InstanceState::Paused, None)
        .await
        .unwrap();
    source
        .save_checkpoint(&Checkpoint {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            checkpoint_data: json!({"completed_blocks":[]}),
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let (export_storage, faults) = FaultStorage::wrap(source.clone());
    let publication = faults.on("put_artifact_with_id").times(1).pause();
    let runtime = RuntimeId::new();
    let encryption = FieldEncryptor::from_bytes(&[3; 32]);
    let encryptor = encryption.clone();
    let expected = owner.clone();
    let export = tokio::spawn(async move {
        export_paused_capsule(
            export_storage.as_ref(),
            CapsuleExportRequest {
                continuity: expected,
                destination_runtime_id: Some(runtime),
                requirements: CapsuleRequirements::default(),
                expires_at: Utc::now() + Duration::minutes(5),
                signing_key_id: "signer".into(),
                encryption_key_id: "payload".into(),
            },
            &SigningKey::from_bytes(&[7; 32]),
            &encryptor,
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), publication.entered())
        .await
        .unwrap();
    // A real app resume races the publication of the already assembled capsule.
    source
        .update_instance_state(instance.id, InstanceState::Running, None)
        .await
        .unwrap();
    release.notify_one();
    let source_outcome = tokio::time::timeout(std::time::Duration::from_secs(10), source_task)
        .await
        .unwrap()
        .unwrap();
    publication.release();
    let signed = tokio::time::timeout(std::time::Duration::from_secs(10), export)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let bytes = source
        .get_artifact(&signed.manifest.payload_artifact.key)
        .await
        .unwrap()
        .unwrap();
    // Even a coordinator which fences the source immediately after export
    // cannot repair an outcome admitted between snapshot and publication.
    let current = source
        .get_continuity_execution(&owner.tenant_id, owner.continuity_id)
        .await
        .unwrap()
        .unwrap();
    if current.state == OwnershipState::Owned {
        let fenced = ContinuityExecution {
            state: OwnershipState::Transferring,
            ..current.clone()
        };
        assert!(
            source
                .cas_continuity_owner(
                    &owner.tenant_id,
                    owner.continuity_id,
                    current.epoch,
                    current.owner_runtime_id,
                    &fenced
                )
                .await
                .unwrap()
        );
    }
    let (imported, payload) = verify_and_import_paused_capsule_bytes(
        destination.as_ref(),
        &signed,
        &bytes,
        CapsuleImportRequest {
            tenant_id: &owner.tenant_id,
            destination_runtime_id: runtime,
            destination_instance_id: None,
            expected_epoch: owner.epoch,
            trusted_public_keys: std::slice::from_ref(&signed.public_key),
            now: Utc::now(),
        },
        &encryption,
    )
    .await
    .unwrap();
    assert!(payload.outputs.is_empty());
    activate_imported_capsule(
        destination.as_ref(),
        &owner.tenant_id,
        signed.manifest.capsule_id,
        runtime,
        imported.id,
        Utc::now(),
    )
    .await
    .unwrap();
    let claimed = destination
        .claim_due_instances(Utc::now() + Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut handlers = HandlerRegistry::new();
    let count = calls.clone();
    handlers.register_with_deferred_effect_admission("native-charge", move |ctx| {
        let calls = count.clone();
        async move {
            ctx.effect_admission.as_ref().unwrap().admit().await?;
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!({"provider":"charged"}))
        }
    });
    execute_step(&destination, &handlers, params(&claimed))
        .await
        .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "provider entered on source after export cut (source result: {source_outcome:?}) and again on destination"
    );
    assert!(
        source_outcome.is_err(),
        "captured source admission survived export"
    );
}

async fn retirement_fixture() -> (Arc<dyn StorageBackend>, ContinuityExecution, RuntimeId) {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
    );
    retirement_fixture_on(storage).await
}

async fn retirement_fixture_on(
    storage: Arc<dyn StorageBackend>,
) -> (Arc<dyn StorageBackend>, ContinuityExecution, RuntimeId) {
    let sequence = SequenceBuilder::new("retired-export")
        .step("done", "noop")
        .build();
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Paused)
        .build();
    storage.create_instance(&instance).await.unwrap();
    storage
        .save_checkpoint(&Checkpoint {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            checkpoint_data: json!({"completed_blocks":[]}),
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
    storage.create_continuity_execution(&owner).await.unwrap();
    (storage, owner, RuntimeId::new())
}
async fn retirement_export(
    storage: &dyn StorageBackend,
    owner: &ContinuityExecution,
    destination: RuntimeId,
) -> Result<
    orch8_publisher::capsule::SignedCapsuleManifest,
    orch8_engine::capsule::CapsuleServiceError,
> {
    export_paused_capsule(
        storage,
        CapsuleExportRequest {
            continuity: owner.clone(),
            destination_runtime_id: Some(destination),
            requirements: CapsuleRequirements::default(),
            expires_at: Utc::now() + Duration::minutes(10),
            signing_key_id: "test-signer".into(),
            encryption_key_id: "transfer".into(),
        },
        &SigningKey::from_bytes(&[8; 32]),
        &FieldEncryptor::from_bytes(&[3; 32]),
    )
    .await
}
fn accepted_owner(owner: &ContinuityExecution, destination: RuntimeId) -> ContinuityExecution {
    ContinuityExecution {
        epoch: owner.epoch.checked_next().unwrap(),
        owner_runtime_id: destination,
        current_instance_id: orch8_types::ids::InstanceId::new(),
        state: OwnershipState::Owned,
        ..owner.clone()
    }
}

#[tokio::test]
async fn acknowledgement_cannot_reuse_the_sources_physical_instance_identity() {
    use orch8_engine::ownership::{LocalOwnership, local_ownership};
    let (storage, owner, destination) = retirement_fixture().await;
    let capsule = retirement_export(storage.as_ref(), &owner, destination)
        .await
        .unwrap();
    let mut accepted = accepted_owner(&owner, destination);
    accepted.current_instance_id = owner.current_instance_id;
    assert!(
        storage
            .acknowledge_capsule_cut(&owner.tenant_id, capsule.manifest.capsule_id, &accepted)
            .await
            .is_err(),
        "reusing the source UUID must not make the old source locally owned"
    );
    assert_eq!(
        local_ownership(
            storage.as_ref(),
            &owner.tenant_id,
            owner.current_instance_id
        )
        .await
        .unwrap(),
        LocalOwnership::Transferring
    );
    accepted.current_instance_id = orch8_types::ids::InstanceId::new();
    storage
        .acknowledge_capsule_cut(&owner.tenant_id, capsule.manifest.capsule_id, &accepted)
        .await
        .unwrap();
    assert_eq!(
        local_ownership(
            storage.as_ref(),
            &owner.tenant_id,
            owner.current_instance_id
        )
        .await
        .unwrap(),
        LocalOwnership::Superseded
    );
}

#[tokio::test]
async fn retired_transport_gc_retries_failure_and_removes_late_upload_without_touching_outputs() {
    let (storage, owner, destination) = retirement_fixture().await;
    let capsule = retirement_export(storage.as_ref(), &owner, destination)
        .await
        .unwrap();
    let key = &capsule.manifest.payload_artifact.key;
    let bytes = storage.get_artifact(key).await.unwrap().unwrap();
    let sibling = storage
        .put_artifact(
            owner.current_instance_id,
            "text/plain",
            bytes::Bytes::from_static(b"user output"),
        )
        .await
        .unwrap();
    let (wrapped, faults) = FaultStorage::wrap(storage.clone());
    faults.on("delete_artifact").times(1).fail_transient();
    assert!(
        orch8_engine::capsule::acknowledge_exported_capsule(
            wrapped.as_ref(),
            &owner.tenant_id,
            capsule.manifest.capsule_id,
            &accepted_owner(&owner, destination)
        )
        .await
        .is_err()
    );
    assert!(storage.get_artifact(key).await.unwrap().is_some());
    let now = Utc::now() + Duration::seconds(1);
    orch8_engine::gc::gc_tick(
        storage.as_ref(),
        &orch8_engine::gc::GcOptions::default(),
        now,
    )
    .await;
    assert!(storage.get_artifact(key).await.unwrap().is_none());
    // Simulate an upload completing after its source acknowledged and GC ran.
    storage
        .put_artifact_with_id(
            owner.current_instance_id,
            capsule.manifest.capsule_id.into_uuid(),
            "application/octet-stream",
            bytes.into(),
        )
        .await
        .unwrap();
    assert!(storage.get_artifact(key).await.unwrap().is_some());
    orch8_engine::gc::gc_tick(
        storage.as_ref(),
        &orch8_engine::gc::GcOptions::default(),
        now + Duration::minutes(6),
    )
    .await;
    assert!(storage.get_artifact(key).await.unwrap().is_none());
    assert_eq!(
        storage.get_artifact(&sibling.key).await.unwrap().unwrap(),
        b"user output"
    );
}

#[tokio::test]
async fn upload_finishing_after_acknowledgement_is_deleted_and_cannot_return_a_capsule() {
    let (storage, owner, destination) = retirement_fixture().await;
    let capsule = retirement_export(storage.as_ref(), &owner, destination)
        .await
        .unwrap();
    let (wrapped, faults) = FaultStorage::wrap(storage.clone());
    let publication = faults.on("put_artifact_with_id").times(1).pause();
    let source = owner.clone();
    let retry =
        tokio::spawn(
            async move { retirement_export(wrapped.as_ref(), &source, destination).await },
        );
    tokio::time::timeout(std::time::Duration::from_secs(10), publication.entered())
        .await
        .unwrap();
    orch8_engine::capsule::acknowledge_exported_capsule(
        storage.as_ref(),
        &owner.tenant_id,
        capsule.manifest.capsule_id,
        &accepted_owner(&owner, destination),
    )
    .await
    .unwrap();
    publication.release();
    assert!(matches!(
        retry.await.unwrap(),
        Err(orch8_engine::capsule::CapsuleServiceError::OwnershipMismatch)
    ));
    assert!(
        storage
            .get_artifact(&capsule.manifest.payload_artifact.key)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn retired_transport_cleanup_survives_file_reopen_and_source_cut_purge() {
    let scratch = tempfile::tempdir().unwrap();
    let database = scratch.path().join("engine.sqlite");
    let artifacts = scratch.path().join("artifacts");
    let concrete = Arc::new(
        SqliteStorage::file(database.to_str().unwrap())
            .await
            .unwrap()
            .with_artifact_store(Arc::new(
                ObjectArtifactStore::local(artifacts.to_str().unwrap()).unwrap(),
            )),
    );
    let (storage, owner, destination) = retirement_fixture_on(concrete.clone()).await;
    let capsule = retirement_export(storage.as_ref(), &owner, destination)
        .await
        .unwrap();
    let key = &capsule.manifest.payload_artifact.key;
    let bytes = storage.get_artifact(key).await.unwrap().unwrap();
    // Commit acknowledgement without running the external-delete phase, then
    // remove source-cut history before closing the physical database.
    storage
        .acknowledge_capsule_cut(
            &owner.tenant_id,
            capsule.manifest.capsule_id,
            &accepted_owner(&owner, destination),
        )
        .await
        .unwrap();
    sqlx::query("DELETE FROM capsule_source_cuts WHERE capsule_id=?1")
        .bind(capsule.manifest.capsule_id.to_string())
        .execute(concrete.pool())
        .await
        .unwrap();
    concrete.close().await;
    drop(storage);
    drop(concrete);
    let reopened = SqliteStorage::file(database.to_str().unwrap())
        .await
        .unwrap()
        .with_artifact_store(Arc::new(
            ObjectArtifactStore::local(artifacts.to_str().unwrap()).unwrap(),
        ));
    assert!(reopened.get_artifact(key).await.unwrap().is_some());
    let now = Utc::now() + Duration::seconds(1);
    orch8_engine::gc::gc_tick(&reopened, &orch8_engine::gc::GcOptions::default(), now).await;
    assert!(reopened.get_artifact(key).await.unwrap().is_none());
    // A late writer cannot make the blob permanent even after history purge.
    reopened
        .put_artifact_with_id(
            owner.current_instance_id,
            capsule.manifest.capsule_id.into_uuid(),
            "application/octet-stream",
            bytes.into(),
        )
        .await
        .unwrap();
    orch8_engine::gc::gc_tick(
        &reopened,
        &orch8_engine::gc::GcOptions::default(),
        now + Duration::minutes(6),
    )
    .await;
    assert!(reopened.get_artifact(key).await.unwrap().is_none());
    reopened.close().await;
}
