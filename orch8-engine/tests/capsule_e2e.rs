use std::sync::Arc;

use chrono::{Duration, Utc};
use ed25519_dalek::SigningKey;
use orch8_engine::capsule::{
    CapsuleExportRequest, CapsuleImportRequest, CapsuleServiceError, export_paused_capsule,
    verify_and_import_paused_capsule,
};
use orch8_storage::artifacts::ObjectArtifactStore;
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::checkpoint::Checkpoint;
use orch8_types::continuity::{
    CapsuleRequirements, ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState,
    RuntimeId,
};
use orch8_types::encryption::FieldEncryptor;
use orch8_types::ids::BlockId;
use orch8_types::instance::InstanceState;
use serde_json::json;

mod common;
use common::{mk_instance_with_ctx, mk_sequence, mk_step};

#[tokio::test]
#[allow(clippy::too_many_lines)] // one scenario verifies the complete encrypted transfer boundary
async fn encrypted_capsule_roundtrips_between_backends_into_paused_quarantine() {
    let artifacts = Arc::new(ObjectArtifactStore::memory());
    let source: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(artifacts.clone()),
    );
    let destination: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(artifacts),
    );
    let sequence = mk_sequence(vec![mk_step("work", "noop")]);
    source.create_sequence(&sequence).await.unwrap();
    destination.create_sequence(&sequence).await.unwrap();
    let mut instance = mk_instance_with_ctx(sequence.id, json!({"portable": true}));
    instance.state = InstanceState::Paused;
    instance.context.runtime.current_step = Some(BlockId::new("work"));
    source.create_instance(&instance).await.unwrap();
    source
        .save_checkpoint(&Checkpoint {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            checkpoint_data: json!({"completed_blocks": [], "safe_boundary": "work"}),
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let source_runtime = RuntimeId::new();
    let destination_runtime = RuntimeId::new();
    let continuity = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: source_runtime,
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    source
        .create_continuity_execution(&continuity)
        .await
        .unwrap();
    let signing_key = SigningKey::from_bytes(&[7; 32]);
    let encryptor = FieldEncryptor::from_bytes(&[9; 32]);
    let signed = export_paused_capsule(
        source.as_ref(),
        CapsuleExportRequest {
            continuity: continuity.clone(),
            destination_runtime_id: Some(destination_runtime),
            requirements: CapsuleRequirements::default(),
            expires_at: Utc::now() + Duration::minutes(5),
            signing_key_id: "signing-v1".into(),
            encryption_key_id: "destination-v1".into(),
        },
        &signing_key,
        &encryptor,
    )
    .await
    .unwrap();

    let trusted = [signed.public_key.clone()];
    let (imported, payload) = verify_and_import_paused_capsule(
        destination.as_ref(),
        &signed,
        CapsuleImportRequest {
            tenant_id: &instance.tenant_id,
            destination_runtime_id: destination_runtime,
            expected_epoch: ExecutionEpoch::initial(),
            trusted_public_keys: &trusted,
            now: Utc::now(),
        },
        &encryptor,
    )
    .await
    .unwrap();

    assert_ne!(imported.id, instance.id);
    assert_eq!(imported.state, InstanceState::Paused);
    assert_eq!(imported.context.data, json!({"portable": true}));
    assert_eq!(payload.checkpoint.instance_id, instance.id);
    assert!(
        destination
            .get_latest_checkpoint(imported.id)
            .await
            .unwrap()
            .is_some()
    );

    let wrong_destination = verify_and_import_paused_capsule(
        destination.as_ref(),
        &signed,
        CapsuleImportRequest {
            tenant_id: &instance.tenant_id,
            destination_runtime_id: RuntimeId::new(),
            expected_epoch: ExecutionEpoch::initial(),
            trusted_public_keys: &trusted,
            now: Utc::now(),
        },
        &encryptor,
    )
    .await;
    assert!(matches!(
        wrong_destination,
        Err(CapsuleServiceError::Protocol(_))
    ));
}

#[tokio::test]
async fn export_rejects_a_running_instance_without_mutating_it() {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
    );
    let sequence = mk_sequence(vec![mk_step("work", "noop")]);
    storage.create_sequence(&sequence).await.unwrap();
    let mut instance = mk_instance_with_ctx(sequence.id, json!({}));
    instance.state = InstanceState::Running;
    storage.create_instance(&instance).await.unwrap();
    let continuity = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };

    let result = export_paused_capsule(
        storage.as_ref(),
        CapsuleExportRequest {
            continuity,
            destination_runtime_id: Some(RuntimeId::new()),
            requirements: CapsuleRequirements::default(),
            expires_at: Utc::now() + Duration::minutes(5),
            signing_key_id: "signing-v1".into(),
            encryption_key_id: "destination-v1".into(),
        },
        &SigningKey::from_bytes(&[7; 32]),
        &FieldEncryptor::from_bytes(&[9; 32]),
    )
    .await;
    assert!(matches!(result, Err(CapsuleServiceError::UnsafeBoundary)));
    assert_eq!(
        storage
            .get_instance(instance.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Running
    );
}
