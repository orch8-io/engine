//! Offline portable-continuity import and activation for mobile runtimes.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::Utc;
use orch8_publisher::capsule::SignedCapsuleManifest;
use orch8_storage::StorageBackend;
use orch8_types::continuity::{CapsuleId, ContinuityExecution, OwnershipState, RuntimeId};
use orch8_types::encryption::FieldEncryptor;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::error::MobileError;

#[derive(Debug, Clone, uniffi::Record)]
pub struct ContinuityImportResult {
    pub capsule_id: String,
    pub continuity_id: String,
    pub instance_id: String,
    pub source_epoch: u64,
    pub state: String,
}

fn invalid(message: impl Into<String>) -> MobileError {
    MobileError::InvalidInput {
        message: message.into(),
    }
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid, MobileError> {
    Uuid::parse_str(value).map_err(|error| invalid(format!("invalid {field}: {error}")))
}

fn transfer_encryptor(encoded: &str) -> Result<FieldEncryptor, MobileError> {
    let decoded = Zeroizing::new(
        BASE64
            .decode(encoded)
            .map_err(|_| invalid("payload key is not valid base64"))?,
    );
    let key: &[u8; 32] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| invalid("payload key must decode to exactly 32 bytes"))?;
    Ok(FieldEncryptor::from_bytes(key))
}

#[allow(clippy::too_many_arguments)]
pub async fn import_capsule(
    storage: &dyn StorageBackend,
    trusted_root_key: &str,
    capsule_json: &str,
    payload_base64: &str,
    payload_key_base64: &str,
    destination_runtime_id: &str,
    destination_instance_id: &str,
) -> Result<ContinuityImportResult, MobileError> {
    if trusted_root_key.is_empty() {
        return Err(invalid(
            "root_public_key is required for continuity capsule verification",
        ));
    }
    let signed: SignedCapsuleManifest = serde_json::from_str(capsule_json)?;
    let declared_bytes = usize::try_from(signed.manifest.payload_artifact.bytes)
        .map_err(|_| invalid("capsule payload size is unsupported"))?;
    let max_sealed_bytes = orch8_types::continuity::CapsulePayload::MAX_ENCODED_BYTES + 64;
    let max_base64_bytes = declared_bytes.saturating_add(2) / 3 * 4;
    if declared_bytes > max_sealed_bytes || payload_base64.len() > max_base64_bytes {
        return Err(MobileError::ResourceLimit {
            message: "transported capsule payload exceeds protocol bounds".into(),
        });
    }
    let sealed = BASE64
        .decode(payload_base64)
        .map_err(|_| invalid("capsule payload is not valid base64"))?;
    let encryptor = transfer_encryptor(payload_key_base64)?;
    let runtime_id = RuntimeId::from_uuid(parse_uuid(
        destination_runtime_id,
        "destination runtime id",
    )?);
    let instance_id = InstanceId::from_uuid(parse_uuid(
        destination_instance_id,
        "destination instance id",
    )?);
    let trusted_keys = [trusted_root_key.to_owned()];
    let tenant_id = signed.manifest.tenant_id.clone();
    let (instance, _) = orch8_engine::capsule::verify_and_import_paused_capsule_bytes(
        storage,
        &signed,
        &sealed,
        orch8_engine::capsule::CapsuleImportRequest {
            tenant_id: &tenant_id,
            destination_runtime_id: runtime_id,
            destination_instance_id: Some(instance_id),
            expected_epoch: signed.manifest.epoch,
            trusted_public_keys: &trusted_keys,
            now: Utc::now(),
        },
        &encryptor,
    )
    .await
    .map_err(|error| invalid(format!("capsule verification failed: {error}")))?;

    let pending = ContinuityExecution {
        continuity_id: signed.manifest.continuity_id,
        tenant_id: tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: signed.manifest.source_runtime_id,
        epoch: signed.manifest.epoch,
        state: OwnershipState::Transferring,
        updated_at: Utc::now(),
    };
    if let Err(error) = storage.create_continuity_execution(&pending).await {
        let existing = storage
            .get_continuity_execution(&tenant_id, pending.continuity_id)
            .await?;
        if !existing.is_some_and(|existing| {
            existing.continuity_id == pending.continuity_id
                && existing.tenant_id == pending.tenant_id
                && existing.current_instance_id == pending.current_instance_id
                && existing.owner_runtime_id == pending.owner_runtime_id
                && existing.epoch == pending.epoch
                && existing.state == pending.state
        }) {
            return Err(MobileError::Storage {
                message: format!("cannot claim imported continuity identity: {error}"),
            });
        }
    }
    storage.save_capsule_manifest(&signed.manifest).await?;

    Ok(ContinuityImportResult {
        capsule_id: signed.manifest.capsule_id.to_string(),
        continuity_id: signed.manifest.continuity_id.to_string(),
        instance_id: instance.id.to_string(),
        source_epoch: signed.manifest.epoch.get(),
        state: "paused".into(),
    })
}

pub async fn activate_capsule(
    storage: &dyn StorageBackend,
    capsule_id: &str,
    destination_runtime_id: &str,
    destination_instance_id: &str,
) -> Result<(), MobileError> {
    let capsule_id = CapsuleId::from_uuid(parse_uuid(capsule_id, "capsule id")?);
    let runtime_id = RuntimeId::from_uuid(parse_uuid(
        destination_runtime_id,
        "destination runtime id",
    )?);
    let instance_id = InstanceId::from_uuid(parse_uuid(
        destination_instance_id,
        "destination instance id",
    )?);
    let instance = storage
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| invalid("imported destination instance does not exist"))?;
    let manifest = storage
        .get_capsule_manifest(&instance.tenant_id, capsule_id)
        .await?
        .ok_or_else(|| invalid("imported capsule manifest does not exist"))?;
    if manifest.allowed_destination_runtime_id != Some(runtime_id)
        || !storage
            .is_capsule_import_instance(&instance.tenant_id, capsule_id, runtime_id, instance_id)
            .await?
    {
        return Err(invalid(
            "capsule is not bound to this runtime-local instance",
        ));
    }
    let current = storage
        .get_continuity_execution(&instance.tenant_id, manifest.continuity_id)
        .await?
        .ok_or_else(|| invalid("continuity import ownership record does not exist"))?;
    let accepted_epoch = manifest
        .epoch
        .checked_next()
        .map_err(|error| invalid(error.to_string()))?;
    if current.state == OwnershipState::Owned
        && current.owner_runtime_id == runtime_id
        && current.epoch == accepted_epoch
        && current.current_instance_id == instance_id
    {
        storage
            .update_instance_state(instance_id, InstanceState::Scheduled, Some(Utc::now()))
            .await?;
        return Ok(());
    }
    if current.state != OwnershipState::Transferring
        || current.epoch != manifest.epoch
        || current.owner_runtime_id != manifest.source_runtime_id
    {
        return Err(invalid("continuity ownership is not ready for activation"));
    }
    let accepted = ContinuityExecution {
        continuity_id: current.continuity_id,
        tenant_id: current.tenant_id.clone(),
        current_instance_id: instance_id,
        owner_runtime_id: runtime_id,
        epoch: accepted_epoch,
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    if !storage
        .cas_continuity_owner(
            &current.tenant_id,
            current.continuity_id,
            current.epoch,
            current.owner_runtime_id,
            &accepted,
        )
        .await?
    {
        return Err(invalid("continuity ownership changed concurrently"));
    }
    storage
        .update_instance_state(instance_id, InstanceState::Scheduled, Some(Utc::now()))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use chrono::{Duration, Utc};
    use ed25519_dalek::SigningKey;
    use orch8_engine::capsule::{CapsuleExportRequest, export_paused_capsule};
    use orch8_storage::artifacts::ObjectArtifactStore;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::checkpoint::Checkpoint;
    use orch8_types::context::ExecutionContext;
    use orch8_types::continuity::{
        CapsuleRequirements, ContinuityId, ExecutionEpoch, OwnershipState,
    };
    use orch8_types::ids::{Namespace, SequenceId, TenantId};
    use orch8_types::instance::{Priority, TaskInstance};
    use orch8_types::sequence::SequenceDefinition;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn portable_import_survives_redelivery_and_activation() {
        let source = SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory()));
        let destination = SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory()));
        let tenant_id = TenantId::new("mobile-continuity").unwrap();
        let sequence: SequenceDefinition = serde_json::from_value(json!({
            "id": SequenceId::new(),
            "tenant_id": tenant_id,
            "namespace": "default",
            "name": "offline-flow",
            "version": 1,
            "blocks": [],
            "created_at": Utc::now(),
        }))
        .unwrap();
        source.create_sequence(&sequence).await.unwrap();
        destination.create_sequence(&sequence).await.unwrap();
        let now = Utc::now();
        let source_instance = TaskInstance {
            id: InstanceId::new(),
            sequence_id: sequence.id,
            tenant_id: sequence.tenant_id.clone(),
            namespace: Namespace::new("default"),
            state: InstanceState::Paused,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        };
        source.create_instance(&source_instance).await.unwrap();
        source
            .save_checkpoint(&Checkpoint {
                id: Uuid::now_v7(),
                instance_id: source_instance.id,
                checkpoint_data: json!({"safe_boundary":"offline"}),
                created_at: now,
            })
            .await
            .unwrap();
        let source_runtime = RuntimeId::new();
        let destination_runtime = RuntimeId::new();
        let continuity = ContinuityExecution {
            continuity_id: ContinuityId::new(),
            tenant_id: sequence.tenant_id.clone(),
            current_instance_id: source_instance.id,
            owner_runtime_id: source_runtime,
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: now,
        };
        source
            .create_continuity_execution(&continuity)
            .await
            .unwrap();
        let signing_key = SigningKey::from_bytes(&[17; 32]);
        let payload_key = [19; 32];
        let signed = export_paused_capsule(
            &source,
            CapsuleExportRequest {
                continuity,
                destination_runtime_id: Some(destination_runtime),
                requirements: CapsuleRequirements::default(),
                expires_at: now + Duration::minutes(5),
                signing_key_id: "mobile-test".into(),
                encryption_key_id: "transfer-test".into(),
            },
            &signing_key,
            &FieldEncryptor::from_bytes(&payload_key),
        )
        .await
        .unwrap();
        let sealed = source
            .get_artifact(&signed.manifest.payload_artifact.key)
            .await
            .unwrap()
            .unwrap();
        let capsule_json = serde_json::to_string(&signed).unwrap();
        let payload_base64 = BASE64.encode(sealed);
        let key_base64 = BASE64.encode(payload_key);
        let destination_instance = InstanceId::new();

        let untrusted = import_capsule(
            &destination,
            &BASE64.encode([1; 32]),
            &capsule_json,
            &payload_base64,
            &key_base64,
            &destination_runtime.to_string(),
            &destination_instance.to_string(),
        )
        .await;
        assert!(matches!(untrusted, Err(MobileError::InvalidInput { .. })));

        let first = import_capsule(
            &destination,
            &signed.public_key,
            &capsule_json,
            &payload_base64,
            &key_base64,
            &destination_runtime.to_string(),
            &destination_instance.to_string(),
        )
        .await
        .unwrap();
        let redelivered = import_capsule(
            &destination,
            &signed.public_key,
            &capsule_json,
            &payload_base64,
            &key_base64,
            &destination_runtime.to_string(),
            &destination_instance.to_string(),
        )
        .await
        .unwrap();
        assert_eq!(redelivered.instance_id, first.instance_id);

        activate_capsule(
            &destination,
            &first.capsule_id,
            &destination_runtime.to_string(),
            &destination_instance.to_string(),
        )
        .await
        .unwrap();
        // Activation is idempotent after a kill/restart boundary.
        activate_capsule(
            &destination,
            &first.capsule_id,
            &destination_runtime.to_string(),
            &destination_instance.to_string(),
        )
        .await
        .unwrap();
        let active = destination
            .get_instance(destination_instance)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.state, InstanceState::Scheduled);
        let owned = destination
            .get_continuity_execution(&sequence.tenant_id, signed.manifest.continuity_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(owned.owner_runtime_id, destination_runtime);
        assert_eq!(owned.epoch.get(), 1);
        assert_eq!(owned.state, OwnershipState::Owned);
    }
}
