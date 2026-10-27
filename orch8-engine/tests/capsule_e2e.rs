use std::sync::Arc;

use chrono::{Duration, Utc};
use ed25519_dalek::SigningKey;
use orch8_engine::capsule::{
    CapsuleExportRequest, CapsuleImportRequest, CapsuleServiceError, export_paused_capsule,
    verify_and_import_paused_capsule, verify_and_import_paused_capsule_bytes,
};
use orch8_engine::continuity::ContinuityServiceError;
use orch8_storage::artifacts::ObjectArtifactStore;
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::checkpoint::Checkpoint;
use orch8_types::continuity::{
    CapsuleRequirements, ContinuityExecution, ContinuityId, ExecutionEpoch, ExecutionHandoff,
    HandoffId, HandoffState, OwnershipState, RuntimeId,
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
    let sequence = mk_sequence(vec![
        mk_step("completed", "must-not-replay"),
        mk_step("work", "noop"),
    ]);
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
    // Completed output evidence must survive quarantine import; the scheduler
    // uses these rows to avoid replaying completed work.
    let completed = orch8_types::output::BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: instance.id,
        block_id: BlockId::new("completed"),
        output: json!({"receipt": "already-applied"}),
        output_ref: None,
        output_size: 29,
        attempt: 1,
        created_at: Utc::now(),
    };
    // Deliberately disagree with insertion order at the same timestamp.
    let mut completed = completed;
    completed.id = uuid::Uuid::from_u128(3);
    let mut older = completed.clone();
    older.id = uuid::Uuid::from_u128(1);
    older.output = json!({"receipt":"old"});
    let mut middle = older.clone();
    middle.id = uuid::Uuid::from_u128(2);
    source.save_block_output(&completed).await.unwrap();
    source.save_block_output(&older).await.unwrap();
    source.save_block_output(&middle).await.unwrap();
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
    let isolated: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
    );
    isolated.create_sequence(&sequence).await.unwrap();
    let sealed = source
        .get_artifact(&signed.manifest.payload_artifact.key)
        .await
        .unwrap()
        .unwrap();
    // A trusted signer cannot bind this tenant's imported instance to a
    // sequence owned by another tenant, even with a matching sequence hash.
    let foreign_store: Arc<dyn StorageBackend> =
        Arc::new(SqliteStorage::in_memory().await.unwrap());
    let mut foreign_sequence = sequence.clone();
    foreign_sequence.tenant_id = orch8_types::ids::TenantId::new("other-account").unwrap();
    foreign_store
        .create_sequence(&foreign_sequence)
        .await
        .unwrap();
    let mut foreign_manifest = signed.manifest.clone();
    foreign_manifest.sequence.content_sha256 =
        orch8_engine::capsule::sequence_content_hash(&foreign_sequence, foreign_manifest.schema)
            .unwrap();
    let foreign_signed =
        orch8_publisher::capsule::sign_capsule_manifest(foreign_manifest, &signing_key).unwrap();
    let foreign_import_id = orch8_types::ids::InstanceId::new();
    let foreign_import = verify_and_import_paused_capsule_bytes(
        foreign_store.as_ref(),
        &foreign_signed,
        &sealed,
        CapsuleImportRequest {
            tenant_id: &instance.tenant_id,
            destination_runtime_id: destination_runtime,
            destination_instance_id: Some(foreign_import_id),
            expected_epoch: ExecutionEpoch::initial(),
            trusted_public_keys: &trusted,
            now: Utc::now(),
        },
        &encryptor,
    )
    .await;
    assert!(
        matches!(foreign_import, Err(CapsuleServiceError::OwnershipMismatch)),
        "cross-tenant sequence import must be refused: {foreign_import:?}"
    );
    assert!(
        foreign_store
            .get_instance(foreign_import_id)
            .await
            .unwrap()
            .is_none()
    );
    let mut wrong_version = signed.manifest.clone();
    wrong_version.sequence.version += 1;
    let wrong_version =
        orch8_publisher::capsule::sign_capsule_manifest(wrong_version, &signing_key).unwrap();
    assert!(matches!(
        verify_and_import_paused_capsule_bytes(
            isolated.as_ref(),
            &wrong_version,
            &sealed,
            CapsuleImportRequest {
                tenant_id: &instance.tenant_id,
                destination_runtime_id: destination_runtime,
                destination_instance_id: None,
                expected_epoch: ExecutionEpoch::initial(),
                trusted_public_keys: &trusted,
                now: Utc::now()
            },
            &encryptor
        )
        .await,
        Err(CapsuleServiceError::PayloadIdentityMismatch)
    ));
    let destination_instance_id = orch8_types::ids::InstanceId::new();
    let (portable_import, _) = verify_and_import_paused_capsule_bytes(
        isolated.as_ref(),
        &signed,
        &sealed,
        CapsuleImportRequest {
            tenant_id: &instance.tenant_id,
            destination_runtime_id: destination_runtime,
            destination_instance_id: Some(destination_instance_id),
            expected_epoch: ExecutionEpoch::initial(),
            trusted_public_keys: &trusted,
            now: Utc::now(),
        },
        &encryptor,
    )
    .await
    .unwrap();
    assert_eq!(portable_import.id, destination_instance_id);
    let restored = isolated.get_all_outputs(portable_import.id).await.unwrap();
    assert_eq!(
        restored.len(),
        3,
        "import discarded completed output evidence"
    );
    assert_eq!(
        isolated
            .get_block_output(portable_import.id, &completed.block_id)
            .await
            .unwrap()
            .unwrap()
            .output,
        completed.output
    );
    assert_eq!(restored[0].instance_id, portable_import.id);
    assert_ne!(restored[0].id, completed.id);
    common::accept_capsule_import(
        isolated.as_ref(),
        &signed.manifest,
        portable_import.id,
        destination_runtime,
    )
    .await;
    isolated
        .update_instance_state(
            portable_import.id,
            InstanceState::Scheduled,
            Some(Utc::now()),
        )
        .await
        .unwrap();
    let handlers = Arc::new(common::registry());
    let semaphore = common::semaphore(4);
    let config = common::default_config();
    let cache = common::cache();
    let cancel = tokio_util::sync::CancellationToken::new();
    for _ in 0..10 {
        orch8_engine::scheduler::tick_once(
            &isolated, &handlers, &semaphore, &config, &cache, &cancel,
        )
        .await
        .unwrap();
        if isolated
            .get_instance(portable_import.id)
            .await
            .unwrap()
            .unwrap()
            .state
            == InstanceState::Completed
        {
            break;
        }
    }
    assert_eq!(
        isolated
            .get_instance(portable_import.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed,
        "completed step was replayed instead of continuing at work"
    );
    assert_eq!(
        isolated
            .get_all_outputs(portable_import.id)
            .await
            .unwrap()
            .iter()
            .filter(|row| row.block_id == completed.block_id)
            .count(),
        3
    );

    assert!(
        isolated
            .get_artifact(&signed.manifest.payload_artifact.key)
            .await
            .unwrap()
            .is_none()
    );

    let (same_store, _) = verify_and_import_paused_capsule(
        source.as_ref(),
        &signed,
        CapsuleImportRequest {
            tenant_id: &instance.tenant_id,
            destination_runtime_id: destination_runtime,
            destination_instance_id: None,
            expected_epoch: ExecutionEpoch::initial(),
            trusted_public_keys: &trusted,
            now: Utc::now(),
        },
        &encryptor,
    )
    .await
    .unwrap();
    assert_eq!(
        source.get_all_outputs(same_store.id).await.unwrap().len(),
        3
    );
    assert_eq!(
        source
            .get_block_output(same_store.id, &completed.block_id)
            .await
            .unwrap()
            .unwrap()
            .output,
        completed.output
    );

    let (imported, payload) = verify_and_import_paused_capsule(
        destination.as_ref(),
        &signed,
        CapsuleImportRequest {
            tenant_id: &instance.tenant_id,
            destination_runtime_id: destination_runtime,
            destination_instance_id: None,
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

    let (redelivered, redelivered_payload) = verify_and_import_paused_capsule(
        destination.as_ref(),
        &signed,
        CapsuleImportRequest {
            tenant_id: &instance.tenant_id,
            destination_runtime_id: destination_runtime,
            destination_instance_id: None,
            expected_epoch: ExecutionEpoch::initial(),
            trusted_public_keys: &trusted,
            now: Utc::now(),
        },
        &encryptor,
    )
    .await
    .unwrap();
    assert_eq!(redelivered.id, imported.id);
    assert_eq!(
        redelivered_payload.checkpoint.checkpoint_data,
        payload.checkpoint.checkpoint_data
    );
    assert_eq!(
        redelivered_payload.instance.sequence_id,
        payload.instance.sequence_id
    );

    let transferring = destination
        .get_continuity_execution(&instance.tenant_id, continuity.continuity_id)
        .await
        .unwrap()
        .expect("capsule import atomically creates pending ownership");
    assert_eq!(transferring.state, OwnershipState::Transferring);
    let exported = ExecutionHandoff {
        id: HandoffId::new(),
        continuity_id: continuity.continuity_id,
        tenant_id: instance.tenant_id.clone(),
        source_runtime_id: source_runtime,
        destination_runtime_id: destination_runtime,
        expected_epoch: ExecutionEpoch::initial(),
        state: HandoffState::Exported,
        capsule_id: Some(signed.manifest.capsule_id),
        placement_decision_id: None,
        preview_sha256: "a".repeat(64),
        version: 2,
        failure_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    destination.create_handoff(&exported).await.unwrap();
    let mut accepted_handoff = exported.clone();
    accepted_handoff.state = HandoffState::Accepted;
    accepted_handoff.version += 1;
    let mut unrelated = mk_instance_with_ctx(sequence.id, json!({"unrelated": true}));
    unrelated.state = InstanceState::Paused;
    destination.create_instance(&unrelated).await.unwrap();
    let mut substituted_execution = transferring.clone();
    substituted_execution.current_instance_id = unrelated.id;
    substituted_execution.owner_runtime_id = destination_runtime;
    substituted_execution.epoch = ExecutionEpoch::initial().checked_next().unwrap();
    substituted_execution.state = OwnershipState::Owned;
    let substitution = orch8_engine::continuity::accept_handoff(
        destination.as_ref(),
        &exported,
        &accepted_handoff,
        &transferring,
        &substituted_execution,
    )
    .await;
    assert!(matches!(
        substitution,
        Err(ContinuityServiceError::UnboundDestinationInstance)
    ));

    let mut accepted_execution = substituted_execution;
    accepted_execution.current_instance_id = imported.id;
    orch8_engine::continuity::accept_handoff(
        destination.as_ref(),
        &exported,
        &accepted_handoff,
        &transferring,
        &accepted_execution,
    )
    .await
    .unwrap();

    let wrong_destination = verify_and_import_paused_capsule(
        destination.as_ref(),
        &signed,
        CapsuleImportRequest {
            tenant_id: &instance.tenant_id,
            destination_runtime_id: RuntimeId::new(),
            destination_instance_id: None,
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
