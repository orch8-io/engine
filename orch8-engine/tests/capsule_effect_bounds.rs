//! A successful capsule must not silently omit long-running effect history.
use chrono::{Duration, Utc};
use orch8_engine::capsule::{
    CapsuleExportRequest, CapsuleServiceError, export_paused_capsule_manifest,
};
use orch8_storage::{
    ContinuityStore, ResourceStore, StorageBackend,
    artifacts::ObjectArtifactStore,
    sqlite::SqliteStorage,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    checkpoint::Checkpoint,
    continuity::{
        CapsulePayload, CapsuleRequirements, ContinuityExecution, ContinuityId, EffectId,
        EffectKind, EffectReceipt, EffectState, ExecutionEpoch, OwnershipState, RuntimeId,
    },
    encryption::FieldEncryptor,
    ids::BlockId,
    instance::InstanceState,
};
use std::sync::Arc;

async fn fixture(storage: &dyn StorageBackend) -> (ContinuityExecution, EffectReceipt) {
    let sequence = SequenceBuilder::new("capsule-effect-bounds")
        .step("work", "noop")
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
            checkpoint_data: serde_json::json!({}),
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let owner = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::new(),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    storage.create_continuity_execution(&owner).await.unwrap();
    let receipt = EffectReceipt {
        id: EffectId::new(),
        tenant_id: instance.tenant_id,
        continuity_id: owner.continuity_id,
        epoch: owner.epoch,
        instance_id: instance.id,
        block_id: BlockId::new("work"),
        kind: EffectKind::Worker,
        state: EffectState::Committed,
        destination_fingerprint: "provider".into(),
        idempotency_key: None,
        request_sha256: "request".into(),
        provider_receipt_id: None,
        attempt: 0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    (owner, receipt)
}
fn request(owner: &ContinuityExecution) -> CapsuleExportRequest {
    CapsuleExportRequest {
        continuity: owner.clone(),
        destination_runtime_id: None,
        requirements: CapsuleRequirements::default(),
        expires_at: Utc::now() + Duration::minutes(5),
        signing_key_id: "signer".into(),
        encryption_key_id: "payload".into(),
    }
}

async fn decode_transferred_payload(
    storage: &dyn StorageBackend,
    manifest: orch8_types::continuity::CapsuleManifest,
    encryptor: &FieldEncryptor,
) -> CapsulePayload {
    let instance = storage
        .get_instance(manifest.source_instance_id)
        .await
        .unwrap()
        .unwrap();
    let sequence = storage
        .get_sequence(instance.sequence_id)
        .await
        .unwrap()
        .unwrap();
    let destination = SqliteStorage::in_memory().await.unwrap();
    orch8_storage::SequenceStore::create_sequence(&destination, &sequence)
        .await
        .unwrap();
    let signed = orch8_publisher::capsule::sign_capsule_manifest(
        manifest,
        &ed25519_dalek::SigningKey::from_bytes(&[9; 32]),
    )
    .unwrap();
    let bytes = storage
        .get_artifact(&signed.manifest.payload_artifact.key)
        .await
        .unwrap()
        .unwrap();
    let (_, payload) = orch8_engine::capsule::verify_and_import_paused_capsule_bytes(
        &destination,
        &signed,
        &bytes,
        orch8_engine::capsule::CapsuleImportRequest {
            tenant_id: &signed.manifest.tenant_id,
            destination_runtime_id: RuntimeId::new(),
            destination_instance_id: None,
            expected_epoch: signed.manifest.epoch,
            trusted_public_keys: std::slice::from_ref(&signed.public_key),
            now: Utc::now(),
        },
        encryptor,
    )
    .await
    .unwrap();
    payload
}

#[tokio::test]
async fn exact_effect_limit_export_retries_its_original_frozen_history() {
    let storage = SqliteStorage::in_memory()
        .await
        .unwrap()
        .with_artifact_store(Arc::new(ObjectArtifactStore::memory()));
    let (owner, mut receipt) = fixture(&storage).await;
    let mut expected = std::collections::HashSet::new();
    for _ in 0..CapsulePayload::MAX_PENDING_ITEMS {
        receipt.id = EffectId::new();
        expected.insert(receipt.id);
        storage.create_effect_receipt(&receipt).await.unwrap();
    }
    let encryptor = FieldEncryptor::from_bytes(&[23; 32]);
    let first = export_paused_capsule_manifest(&storage, request(&owner), &encryptor)
        .await
        .unwrap();
    let payload = decode_transferred_payload(&storage, first.clone(), &encryptor).await;
    assert_eq!(payload.effect_ids.len(), CapsulePayload::MAX_PENDING_ITEMS);
    assert_eq!(
        payload
            .effect_ids
            .into_iter()
            .collect::<std::collections::HashSet<_>>(),
        expected
    );

    let artifacts = storage
        .list_artifacts(owner.current_instance_id)
        .await
        .unwrap()
        .len();
    receipt.id = EffectId::new();
    storage.create_effect_receipt(&receipt).await.unwrap();
    let retry = export_paused_capsule_manifest(&storage, request(&owner), &encryptor)
        .await
        .unwrap();
    assert_eq!(
        retry, first,
        "retry must preserve the original frozen manifest"
    );
    let replayed = decode_transferred_payload(&storage, retry, &encryptor).await;
    assert_eq!(
        replayed
            .effect_ids
            .into_iter()
            .collect::<std::collections::HashSet<_>>(),
        expected
    );
    assert_eq!(
        storage
            .list_artifacts(owner.current_instance_id)
            .await
            .unwrap()
            .len(),
        artifacts,
        "a retry must not create a second artifact"
    );
}

#[tokio::test]
async fn first_export_over_effect_limit_refuses_before_fencing_or_publication() {
    let storage = SqliteStorage::in_memory()
        .await
        .unwrap()
        .with_artifact_store(Arc::new(ObjectArtifactStore::memory()));
    let (owner, mut receipt) = fixture(&storage).await;
    for _ in 0..=CapsulePayload::MAX_PENDING_ITEMS {
        receipt.id = EffectId::new();
        storage.create_effect_receipt(&receipt).await.unwrap();
    }
    let overflow = export_paused_capsule_manifest(
        &storage,
        request(&owner),
        &FieldEncryptor::from_bytes(&[23; 32]),
    )
    .await;
    assert!(
        matches!(overflow, Err(CapsuleServiceError::PayloadTooLarge)),
        "{overflow:?}"
    );
    assert!(
        storage
            .list_artifacts(owner.current_instance_id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        storage
            .get_continuity_execution(&owner.tenant_id, owner.continuity_id)
            .await
            .unwrap()
            .unwrap(),
        owner
    );
    let cuts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capsule_source_cuts")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    assert_eq!(cuts, 0, "overflow must not retain a frozen cut");
}
