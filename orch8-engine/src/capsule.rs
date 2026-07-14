//! Safe-boundary execution capsule export and paused import.

use std::fmt::Write as _;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey;
use orch8_publisher::capsule::{
    CapsuleSignatureError, SignedCapsuleManifest, check_capsule_key_trust, sign_capsule_manifest,
    verify_signed_capsule,
};
use orch8_publisher::manifest::canonical_json;
use orch8_storage::StorageBackend;
use orch8_types::continuity::{
    ArtifactReference, CapsuleId, CapsuleInstanceState, CapsuleManifest, CapsulePayload,
    CapsuleRequirements, CapsuleSchemaVersion, CheckpointIdentity, ContinuityExecution,
    ExecutionEpoch, RuntimeId, SequenceIdentity,
};
use orch8_types::encryption::FieldEncryptor;
use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

const CAPSULE_CONTENT_TYPE: &str = "application/vnd.orch8.execution-capsule+aes256gcm";

#[derive(Debug, Clone)]
pub struct CapsuleExportRequest {
    pub continuity: ContinuityExecution,
    pub destination_runtime_id: Option<RuntimeId>,
    pub requirements: CapsuleRequirements,
    pub expires_at: DateTime<Utc>,
    pub signing_key_id: String,
    pub encryption_key_id: String,
}

#[derive(Debug, Clone)]
pub struct CapsuleImportRequest<'a> {
    pub tenant_id: &'a TenantId,
    pub destination_runtime_id: RuntimeId,
    pub expected_epoch: ExecutionEpoch,
    pub trusted_public_keys: &'a [String],
    pub now: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum CapsuleServiceError {
    #[error("continuity execution is not owned by the source instance")]
    OwnershipMismatch,
    #[error("instance must be paused before capsule export")]
    UnsafeBoundary,
    #[error("capsule expiry must be in the future")]
    InvalidExpiry,
    #[error("no persisted checkpoint exists for the paused instance")]
    MissingCheckpoint,
    #[error("sequence referenced by the capsule is unavailable")]
    MissingSequence,
    #[error("capsule import record references an unavailable instance")]
    MissingImportedInstance,
    #[error("capsule payload exceeds the protocol byte limit")]
    PayloadTooLarge,
    #[error("capsule artifact is missing")]
    MissingArtifact,
    #[error("capsule artifact hash mismatch")]
    ArtifactSubstitution,
    #[error("capsule epoch is stale or unexpected")]
    StaleEpoch,
    #[error("capsule payload identity does not match its manifest")]
    PayloadIdentityMismatch,
    #[error("capsule serialization failed: {0}")]
    Serialization(String),
    #[error("capsule encryption failed: {0}")]
    Encryption(String),
    #[error(transparent)]
    Signature(#[from] CapsuleSignatureError),
    #[error(transparent)]
    Protocol(#[from] orch8_types::continuity::ContinuityError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn capsule_aad(
    tenant_id: &TenantId,
    continuity_id: orch8_types::continuity::ContinuityId,
    epoch: ExecutionEpoch,
    destination: Option<RuntimeId>,
) -> Vec<u8> {
    format!(
        "orch8-capsule-v1\0{}\0{}\0{}\0{}",
        tenant_id,
        continuity_id,
        epoch.get(),
        destination.map_or_else(|| "any".into(), |id| id.to_string())
    )
    .into_bytes()
}

#[allow(clippy::too_many_lines)] // protocol assembly is intentionally linear and auditable
pub async fn export_paused_capsule(
    storage: &dyn StorageBackend,
    request: CapsuleExportRequest,
    signing_key: &SigningKey,
    payload_encryptor: &FieldEncryptor,
) -> Result<SignedCapsuleManifest, CapsuleServiceError> {
    let instance = storage
        .get_instance(request.continuity.current_instance_id)
        .await?
        .ok_or(CapsuleServiceError::OwnershipMismatch)?;
    if instance.tenant_id != request.continuity.tenant_id
        || request.continuity.current_instance_id != instance.id
        || request.continuity.state != orch8_types::continuity::OwnershipState::Owned
    {
        return Err(CapsuleServiceError::OwnershipMismatch);
    }
    if !matches!(
        instance.state,
        InstanceState::Paused | InstanceState::Waiting
    ) {
        return Err(CapsuleServiceError::UnsafeBoundary);
    }
    let now = Utc::now();
    if request.expires_at <= now {
        return Err(CapsuleServiceError::InvalidExpiry);
    }
    let checkpoint = storage
        .get_latest_checkpoint(instance.id)
        .await?
        .ok_or(CapsuleServiceError::MissingCheckpoint)?;
    let sequence = storage
        .get_sequence(instance.sequence_id)
        .await?
        .ok_or(CapsuleServiceError::MissingSequence)?;
    if sequence.tenant_id != instance.tenant_id {
        return Err(CapsuleServiceError::OwnershipMismatch);
    }
    let outputs = storage.get_all_outputs(instance.id).await?;
    let payload = CapsulePayload {
        instance: CapsuleInstanceState {
            sequence_id: instance.sequence_id,
            namespace: instance.namespace.clone(),
            priority: instance.priority,
            timezone: instance.timezone.clone(),
            metadata: instance.metadata.clone(),
            context: instance.context.clone(),
            budget: instance.budget.clone(),
            parent_instance_id: instance.parent_instance_id,
        },
        checkpoint: checkpoint.clone(),
        outputs,
        pending_waits: Vec::new(),
        pending_signals: Vec::new(),
        effect_ids: storage
            .list_effect_receipts(
                &request.continuity.tenant_id,
                request.continuity.continuity_id,
                10_000,
            )
            .await?
            .into_iter()
            .map(|receipt| receipt.id)
            .collect(),
        artifacts: Vec::new(),
        stream_cursors: Vec::new(),
        redacted_audit_context: serde_json::Value::Null,
    };
    payload.validate_bounds()?;
    let encoded = canonical_json(&payload)
        .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?
        .into_bytes();
    if encoded.len() > CapsulePayload::MAX_ENCODED_BYTES {
        return Err(CapsuleServiceError::PayloadTooLarge);
    }
    let aad = capsule_aad(
        &request.continuity.tenant_id,
        request.continuity.continuity_id,
        request.continuity.epoch,
        request.destination_runtime_id,
    );
    let sealed = payload_encryptor
        .encrypt_bytes_with_aad(&encoded, &aad)
        .map_err(|error| CapsuleServiceError::Encryption(error.to_string()))?;
    let payload_sha256 = sha256_hex(&sealed);
    let artifact = storage
        .put_artifact(instance.id, CAPSULE_CONTENT_TYPE, Bytes::from(sealed))
        .await?;
    let sequence_json = canonical_json(&sequence)
        .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
    let checkpoint_json = canonical_json(&checkpoint)
        .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
    let requirements_json = canonical_json(&request.requirements)
        .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
    let block_id = instance
        .context
        .runtime
        .current_step
        .clone()
        .unwrap_or_else(|| BlockId::new("checkpoint"));
    let manifest = CapsuleManifest {
        schema: CapsuleSchemaVersion::V1,
        capsule_id: CapsuleId::new(),
        continuity_id: request.continuity.continuity_id,
        source_instance_id: instance.id,
        epoch: request.continuity.epoch,
        tenant_id: request.continuity.tenant_id,
        source_runtime_id: request.continuity.owner_runtime_id,
        allowed_destination_runtime_id: request.destination_runtime_id,
        sequence: SequenceIdentity {
            id: sequence.id,
            version: sequence.version,
            content_sha256: sha256_hex(sequence_json.as_bytes()),
        },
        checkpoint: CheckpointIdentity {
            block_id,
            sha256: sha256_hex(checkpoint_json.as_bytes()),
        },
        requirements_sha256: sha256_hex(requirements_json.as_bytes()),
        payload_artifact: ArtifactReference {
            key: artifact.key,
            sha256: payload_sha256,
            bytes: artifact.size,
        },
        provenance_head: storage
            .list_provenance(
                &instance.tenant_id,
                request.continuity.continuity_id,
                10_000,
            )
            .await?
            .last()
            .map(|entry| entry.entry_sha256.clone()),
        issued_at: now,
        expires_at: request.expires_at,
        signing_key_id: request.signing_key_id,
        encryption_key_id: request.encryption_key_id,
    };
    storage.save_capsule_manifest(&manifest).await?;
    sign_capsule_manifest(manifest, signing_key).map_err(Into::into)
}

pub async fn verify_and_import_paused_capsule(
    storage: &dyn StorageBackend,
    signed: &SignedCapsuleManifest,
    request: CapsuleImportRequest<'_>,
    payload_encryptor: &FieldEncryptor,
) -> Result<(TaskInstance, CapsulePayload), CapsuleServiceError> {
    verify_signed_capsule(signed)?;
    check_capsule_key_trust(signed, request.trusted_public_keys)?;
    signed.manifest.validate_for_import(
        request.now,
        request.tenant_id,
        request.destination_runtime_id,
        CapsuleSchemaVersion::V1,
    )?;
    if signed.manifest.epoch != request.expected_epoch {
        return Err(CapsuleServiceError::StaleEpoch);
    }
    let sealed = storage
        .get_artifact(&signed.manifest.payload_artifact.key)
        .await?
        .ok_or(CapsuleServiceError::MissingArtifact)?;
    let actual_hash = sha256_hex(&sealed);
    let equal: bool = actual_hash
        .as_bytes()
        .ct_eq(signed.manifest.payload_artifact.sha256.as_bytes())
        .into();
    if !equal {
        return Err(CapsuleServiceError::ArtifactSubstitution);
    }
    let aad = capsule_aad(
        request.tenant_id,
        signed.manifest.continuity_id,
        signed.manifest.epoch,
        signed.manifest.allowed_destination_runtime_id,
    );
    let plaintext = payload_encryptor
        .decrypt_bytes_with_aad(&sealed, &aad)
        .map_err(|error| CapsuleServiceError::Encryption(error.to_string()))?;
    if plaintext.len() > CapsulePayload::MAX_ENCODED_BYTES {
        return Err(CapsuleServiceError::PayloadTooLarge);
    }
    let payload: CapsulePayload = serde_json::from_slice(&plaintext)
        .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
    payload.validate_bounds()?;
    if payload.instance.sequence_id != signed.manifest.sequence.id
        || payload.checkpoint.instance_id != signed.manifest.source_instance_id
    {
        return Err(CapsuleServiceError::PayloadIdentityMismatch);
    }
    let sequence = storage
        .get_sequence(payload.instance.sequence_id)
        .await?
        .ok_or(CapsuleServiceError::MissingSequence)?;
    let sequence_json = canonical_json(&sequence)
        .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
    if sha256_hex(sequence_json.as_bytes()) != signed.manifest.sequence.content_sha256 {
        return Err(CapsuleServiceError::PayloadIdentityMismatch);
    }
    let now = request.now;
    let candidate = TaskInstance {
        id: InstanceId::new(),
        sequence_id: payload.instance.sequence_id,
        tenant_id: request.tenant_id.clone(),
        namespace: payload.instance.namespace.clone(),
        state: InstanceState::Paused,
        next_fire_at: None,
        priority: payload.instance.priority,
        timezone: payload.instance.timezone.clone(),
        metadata: payload.instance.metadata.clone(),
        context: payload.instance.context.clone(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: payload.instance.parent_instance_id,
        budget: payload.instance.budget.clone(),
        created_at: now,
        updated_at: now,
    };
    let checkpoint = orch8_types::checkpoint::Checkpoint {
        id: uuid::Uuid::now_v7(),
        instance_id: candidate.id,
        checkpoint_data: payload.checkpoint.checkpoint_data.clone(),
        created_at: now,
    };
    let imported_id = storage
        .import_capsule_instance(
            signed.manifest.capsule_id,
            request.destination_runtime_id,
            &candidate,
            &checkpoint,
        )
        .await?;
    let imported = if imported_id == candidate.id {
        candidate
    } else {
        storage
            .get_instance(imported_id)
            .await?
            .ok_or(CapsuleServiceError::MissingImportedInstance)?
    };
    Ok((imported, payload))
}
