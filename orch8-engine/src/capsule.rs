//! Safe-boundary execution capsule export and paused import.

mod state;

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
use orch8_types::instance::TaskInstance;
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
    /// Runtime-local identity chosen by an isolated destination. Supplying it
    /// lets the control plane and the offline runtime record the same global
    /// location instead of inventing unrelated shadow identities. It must be
    /// fresh for this physical execution, distinct from the source instance.
    pub destination_instance_id: Option<InstanceId>,
    pub expected_epoch: ExecutionEpoch,
    pub trusted_public_keys: &'a [String],
    pub now: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum CapsuleServiceError {
    #[error("capsule is missing required portable execution state")]
    MissingExecutionState,

    #[error("capsule is missing complete portable effect recovery evidence")]
    MissingEffectState,

    #[error("continuity execution is not owned by the source instance")]
    OwnershipMismatch,
    #[error("instance must be paused before capsule export")]
    UnsafeBoundary,
    #[error(
        "instance has {0} pending or claimed worker task(s); wait for them to settle \
         (or cancel them) before exporting"
    )]
    WorkerTasksInFlight(usize),
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

/// Sequence fingerprint for the selected capsule wire schema. In 1.1 the
/// executable document excludes only backend-managed `created_at`: SQL stores
/// have different timestamp precision. Legacy 1.0 retains its exact full JSON.
pub fn sequence_content_hash(
    sequence: &orch8_types::sequence::SequenceDefinition,
    schema: CapsuleSchemaVersion,
) -> Result<String, CapsuleServiceError> {
    if !CapsuleSchemaVersion::CURRENT.can_read(schema) {
        return Err(
            orch8_types::continuity::ContinuityError::UnsupportedSchema {
                offered_major: schema.major,
                offered_minor: schema.minor,
            }
            .into(),
        );
    }
    let encoded = if schema == CapsuleSchemaVersion::V1 {
        canonical_json(sequence)
    } else {
        let mut document = serde_json::to_value(sequence)
            .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
        document
            .as_object_mut()
            .ok_or_else(|| {
                CapsuleServiceError::Serialization("sequence must serialize as an object".into())
            })?
            .remove("created_at");
        canonical_json(&document)
    }
    .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
    Ok(sha256_hex(encoded.as_bytes()))
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

/// Number of `pending` or `claimed` worker tasks of an instance.
pub async fn open_worker_task_count(
    storage: &dyn StorageBackend,
    instance_id: orch8_types::ids::InstanceId,
) -> Result<usize, orch8_types::error::StorageError> {
    Ok(storage
        .list_worker_tasks(
            &orch8_types::worker_filter::WorkerTaskFilter {
                instance_id: Some(instance_id),
                states: Some(vec![
                    orch8_types::worker::WorkerTaskState::Pending,
                    orch8_types::worker::WorkerTaskState::Claimed,
                ]),
                ..orch8_types::worker_filter::WorkerTaskFilter::default()
            },
            &orch8_types::filter::Pagination {
                offset: 0,
                limit: 1_000,
                sort_ascending: true,
            },
        )
        .await?
        .len())
}

#[allow(clippy::too_many_lines)] // protocol assembly is intentionally linear and auditable
pub async fn export_paused_capsule(
    storage: &dyn StorageBackend,
    request: CapsuleExportRequest,
    signing_key: &SigningKey,
    payload_encryptor: &FieldEncryptor,
) -> Result<SignedCapsuleManifest, CapsuleServiceError> {
    let manifest = export_paused_capsule_manifest(storage, request, payload_encryptor).await?;
    sign_capsule_manifest(manifest, signing_key).map_err(Into::into)
}

/// Assemble, encrypt, and persist a capsule without choosing how it is
/// signed. Embedded runtimes use this boundary with host-managed signing keys,
/// then verify the returned external signature before transport.
#[allow(clippy::too_many_lines)] // protocol assembly is intentionally linear and auditable
pub async fn export_paused_capsule_manifest(
    storage: &dyn StorageBackend,
    request: CapsuleExportRequest,
    payload_encryptor: &FieldEncryptor,
) -> Result<CapsuleManifest, CapsuleServiceError> {
    export_capsule_manifest(storage, request, payload_encryptor, None).await
}

/// Export under a trusted mobile account/catalog session. Capture and publish
/// checks enforce the descriptor in storage, including retries after revocation.
pub async fn export_paused_capsule_manifest_with_mobile_access(
    storage: &dyn StorageBackend,
    request: CapsuleExportRequest,
    payload_encryptor: &FieldEncryptor,
    access: &orch8_storage::MobileInstanceAccess,
) -> Result<CapsuleManifest, CapsuleServiceError> {
    export_capsule_manifest(storage, request, payload_encryptor, Some(access.clone())).await
}

#[allow(clippy::too_many_lines)] // linear durable capture/seal/publication protocol
async fn export_capsule_manifest(
    storage: &dyn StorageBackend,
    request: CapsuleExportRequest,
    payload_encryptor: &FieldEncryptor,
    mobile_access: Option<orch8_storage::MobileInstanceAccess>,
) -> Result<CapsuleManifest, CapsuleServiceError> {
    use orch8_storage::capsule_export::{CapsuleCutCapture, CapsuleCutRefusal, CapsuleCutRequest};
    let now = Utc::now();
    if request.expires_at <= now {
        return Err(CapsuleServiceError::InvalidExpiry);
    }
    if !storage.artifacts_enabled() {
        return Err(StorageError::Unsupported("artifact storage is not configured".into()).into());
    }
    let options = canonical_json(&serde_json::json!({
        "requirements":request.requirements,
        "signing_key_id":request.signing_key_id,
        "encryption_key_id":request.encryption_key_id,
    }))
    .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
    let cut_request = CapsuleCutRequest {
        owner: request.continuity.clone(),
        destination_runtime_id: request.destination_runtime_id,
        options_sha256: sha256_hex(options.as_bytes()),
        mobile_access,
        now,
        expires_at: request.expires_at,
    };
    let cut = match storage.capture_capsule_cut(&cut_request).await? {
        CapsuleCutCapture::Ready(cut) => *cut,
        CapsuleCutCapture::Refused(reason) => {
            return Err(match reason {
                CapsuleCutRefusal::Ownership => CapsuleServiceError::OwnershipMismatch,
                CapsuleCutRefusal::UnsafeBoundary => CapsuleServiceError::UnsafeBoundary,
                CapsuleCutRefusal::MissingCheckpoint => CapsuleServiceError::MissingCheckpoint,
                CapsuleCutRefusal::MissingSequence => CapsuleServiceError::MissingSequence,
                CapsuleCutRefusal::PayloadTooLarge => CapsuleServiceError::PayloadTooLarge,
                CapsuleCutRefusal::WorkerTasksInFlight(n) => {
                    CapsuleServiceError::WorkerTasksInFlight(n)
                }
            });
        }
    };
    if cut.request.expires_at <= now {
        return Err(CapsuleServiceError::InvalidExpiry);
    }
    let id = cut.id;
    let cut = if cut.manifest.is_some() {
        cut
    } else {
        let prepared = prepare_cut_payload(cut, &request, payload_encryptor);
        let (manifest, sealed) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                storage
                    .abort_preparing_capsule_cut(&request.continuity.tenant_id, id)
                    .await?;
                return Err(error);
            }
        };
        storage
            .seal_capsule_cut(&request.continuity.tenant_id, id, &manifest, &sealed)
            .await?
    };
    let manifest = cut
        .manifest
        .ok_or(CapsuleServiceError::PayloadIdentityMismatch)?;
    let sealed = cut
        .sealed_payload
        .ok_or(CapsuleServiceError::MissingArtifact)?;
    if !storage
        .capsule_cut_publishable(&manifest.tenant_id, id)
        .await?
    {
        return Err(CapsuleServiceError::OwnershipMismatch);
    }
    let artifact_result = storage
        .put_artifact_with_id(
            manifest.source_instance_id,
            id.into_uuid(),
            CAPSULE_CONTENT_TYPE,
            Bytes::from(sealed),
        )
        .await;
    if !storage
        .capsule_cut_publishable(&manifest.tenant_id, id)
        .await?
    {
        // The upload may have landed even if its acknowledgement was lost.
        // A durable tombstone retries this deletion if we crash or it fails.
        storage
            .delete_artifact(&manifest.payload_artifact.key)
            .await?;
        return Err(CapsuleServiceError::OwnershipMismatch);
    }
    let artifact = artifact_result?;
    if artifact.key != manifest.payload_artifact.key
        || artifact.size != manifest.payload_artifact.bytes
    {
        return Err(CapsuleServiceError::ArtifactSubstitution);
    }
    Ok(manifest)
}

pub async fn verify_and_import_paused_capsule(
    storage: &dyn StorageBackend,
    signed: &SignedCapsuleManifest,
    request: CapsuleImportRequest<'_>,
    payload_encryptor: &FieldEncryptor,
) -> Result<(TaskInstance, CapsulePayload), CapsuleServiceError> {
    let sealed = storage
        .get_artifact(&signed.manifest.payload_artifact.key)
        .await?
        .ok_or(CapsuleServiceError::MissingArtifact)?;
    verify_and_import_paused_capsule_bytes(storage, signed, &sealed, request, payload_encryptor)
        .await
}

/// No SQL or external I/O occurs while encoding the already frozen snapshot.
fn prepare_cut_payload(
    cut: orch8_storage::capsule_export::CapsuleSourceCut,
    request: &CapsuleExportRequest,
    encryptor: &FieldEncryptor,
) -> Result<(CapsuleManifest, Vec<u8>), CapsuleServiceError> {
    let source = cut
        .snapshot
        .ok_or(CapsuleServiceError::PayloadIdentityMismatch)?;
    let instance = source.instance;
    let checkpoint = source.checkpoint;
    let mut effects = source.effects;
    state::complete_legacy_bindings(&cut.request.owner, &mut effects)?;
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
        execution_state: Some(source.execution_state),
        effect_ids: effects.receipts.iter().map(|receipt| receipt.id).collect(),
        effect_state: Some(effects),
        outputs: source.outputs,
        pending_waits: Vec::new(),
        pending_signals: Vec::new(),
        artifacts: Vec::new(),
        stream_cursors: Vec::new(),
        redacted_audit_context: serde_json::Value::Null,
    };
    payload.validate_bounds()?;
    // The same recovery validation used by import runs before sealing. A bad
    // frozen reference can therefore be safely abandoned without publication.
    state::restore(
        &payload,
        instance.id,
        instance.clone(),
        payload.outputs.clone(),
    )?;
    let encoded = canonical_json(&payload)
        .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?
        .into_bytes();
    if encoded.len() > CapsulePayload::MAX_ENCODED_BYTES {
        return Err(CapsuleServiceError::PayloadTooLarge);
    }
    let aad = capsule_aad(
        &cut.request.owner.tenant_id,
        cut.request.owner.continuity_id,
        cut.request.owner.epoch,
        cut.request.destination_runtime_id,
    );
    let sealed = encryptor
        .encrypt_bytes_with_aad(&encoded, &aad)
        .map_err(|error| CapsuleServiceError::Encryption(error.to_string()))?;
    let checkpoint_json = canonical_json(&checkpoint)
        .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
    let requirements_json = canonical_json(&request.requirements)
        .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
    let manifest = CapsuleManifest {
        schema: CapsuleSchemaVersion::CURRENT,
        capsule_id: cut.id,
        continuity_id: cut.request.owner.continuity_id,
        source_instance_id: instance.id,
        epoch: cut.request.owner.epoch,
        tenant_id: instance.tenant_id,
        source_runtime_id: cut.request.owner.owner_runtime_id,
        allowed_destination_runtime_id: cut.request.destination_runtime_id,
        sequence: SequenceIdentity {
            id: source.sequence.id,
            version: source.sequence.version,
            content_sha256: sequence_content_hash(&source.sequence, CapsuleSchemaVersion::CURRENT)?,
        },
        checkpoint: CheckpointIdentity {
            block_id: instance
                .context
                .runtime
                .current_step
                .unwrap_or_else(|| BlockId::new("checkpoint")),
            sha256: sha256_hex(checkpoint_json.as_bytes()),
        },
        requirements_sha256: sha256_hex(requirements_json.as_bytes()),
        payload_artifact: ArtifactReference {
            key: format!("{}/{}", instance.id, cut.id),
            sha256: sha256_hex(&sealed),
            bytes: sealed.len() as u64,
        },
        provenance_head: source.provenance_head,
        issued_at: cut.request.now,
        expires_at: cut.request.expires_at,
        signing_key_id: request.signing_key_id.clone(),
        encryption_key_id: request.encryption_key_id.clone(),
    };
    Ok((manifest, sealed))
}

/// Verify and import a capsule whose encrypted payload traveled with its
/// signed manifest. This is the portable boundary used by isolated runtimes
/// that cannot read the source runtime's artifact store.
pub async fn verify_and_import_paused_capsule_bytes(
    storage: &dyn StorageBackend,
    signed: &SignedCapsuleManifest,
    sealed: &[u8],
    request: CapsuleImportRequest<'_>,
    payload_encryptor: &FieldEncryptor,
) -> Result<(TaskInstance, CapsulePayload), CapsuleServiceError> {
    verify_signed_capsule(signed)?;
    check_capsule_key_trust(signed, request.trusted_public_keys)?;
    signed.manifest.validate_for_import(
        request.now,
        request.tenant_id,
        request.destination_runtime_id,
        CapsuleSchemaVersion::CURRENT,
    )?;
    if signed.manifest.epoch != request.expected_epoch {
        return Err(CapsuleServiceError::StaleEpoch);
    }
    if u64::try_from(sealed.len()).ok() != Some(signed.manifest.payload_artifact.bytes) {
        return Err(CapsuleServiceError::ArtifactSubstitution);
    }
    let actual_hash = sha256_hex(sealed);
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
        .decrypt_bytes_with_aad(sealed, &aad)
        .map_err(|error| CapsuleServiceError::Encryption(error.to_string()))?;
    if plaintext.len() > CapsulePayload::MAX_ENCODED_BYTES {
        return Err(CapsuleServiceError::PayloadTooLarge);
    }
    let payload: CapsulePayload = serde_json::from_slice(&plaintext)
        .map_err(|error| CapsuleServiceError::Serialization(error.to_string()))?;
    payload.validate_bounds()?;
    state::validate_version(&payload, signed.manifest.schema)?;
    let destination_id = request.destination_instance_id.unwrap_or_default();
    // ContinuityId survives a move; InstanceId identifies a physical execution.
    // Reusing the source ID would let its scheduler mistake remote ownership
    // for local ownership once the accepted receipt is reconciled.
    if destination_id == signed.manifest.source_instance_id {
        return Err(CapsuleServiceError::PayloadIdentityMismatch);
    }
    let outputs = remap_capsule_outputs(&payload, &signed.manifest, destination_id)?;
    let sequence = storage
        .get_sequence(payload.instance.sequence_id)
        .await?
        .ok_or(CapsuleServiceError::MissingSequence)?;
    if &sequence.tenant_id != request.tenant_id {
        return Err(CapsuleServiceError::OwnershipMismatch);
    }
    if sequence.version != signed.manifest.sequence.version
        || sequence_content_hash(&sequence, signed.manifest.schema)?
            != signed.manifest.sequence.content_sha256
    {
        return Err(CapsuleServiceError::PayloadIdentityMismatch);
    }
    if payload.execution_state.is_none()
        && sequence
            .blocks
            .iter()
            .any(|block| !matches!(block, orch8_types::sequence::BlockDefinition::Step(_)))
    {
        return Err(CapsuleServiceError::MissingExecutionState);
    }
    let now = request.now;
    let candidate = state::paused_instance(&payload, request.tenant_id, destination_id, now);
    let checkpoint = orch8_types::checkpoint::Checkpoint {
        id: uuid::Uuid::now_v7(),
        instance_id: candidate.id,
        checkpoint_data: payload.checkpoint.checkpoint_data.clone(),
        created_at: now,
    };
    let snapshot = state::restore(
        &payload,
        signed.manifest.source_instance_id,
        candidate,
        outputs,
    )?;
    let effects = state::effect_import(&payload, signed)?;
    let imported_id = storage
        .import_capsule_recovery(
            signed.manifest.capsule_id,
            request.destination_runtime_id,
            &snapshot,
            &checkpoint,
            Some(&effects),
        )
        .await?;
    // An explicit destination ID can also be an idempotent redelivery. Always
    // return persisted state, which may have advanced since the first import.
    let imported = storage
        .get_instance(imported_id)
        .await?
        .ok_or(CapsuleServiceError::MissingImportedInstance)?;
    Ok((imported, payload))
}

/// Activate only after the trusted host confirms exclusive ownership with its
/// app/backend coordinator. Acceptance and first scheduling commit together;
/// retries preserve the persisted state, including waits and completion.
pub async fn activate_imported_capsule(
    storage: &dyn StorageBackend,
    tenant: &TenantId,
    capsule: CapsuleId,
    runtime: RuntimeId,
    instance: InstanceId,
    now: DateTime<Utc>,
) -> Result<TaskInstance, CapsuleServiceError> {
    storage
        .activate_capsule_import(tenant, capsule, runtime, instance, now)
        .await?;
    let imported = storage
        .get_instance(instance)
        .await?
        .ok_or(CapsuleServiceError::MissingImportedInstance)?;
    if &imported.tenant_id != tenant {
        return Err(CapsuleServiceError::OwnershipMismatch);
    }
    Ok(imported)
}

/// Validate signed source identity before remapping recovery evidence.
fn remap_capsule_outputs(
    payload: &CapsulePayload,
    manifest: &CapsuleManifest,
    destination: InstanceId,
) -> Result<Vec<orch8_types::output::BlockOutput>, CapsuleServiceError> {
    if payload.instance.sequence_id != manifest.sequence.id
        || payload.checkpoint.instance_id != manifest.source_instance_id
    {
        return Err(CapsuleServiceError::PayloadIdentityMismatch);
    }
    let mut output_ids = std::collections::HashSet::with_capacity(payload.outputs.len());
    if payload.outputs.iter().any(|output| {
        output.instance_id != manifest.source_instance_id || !output_ids.insert(output.id)
    }) {
        return Err(CapsuleServiceError::PayloadIdentityMismatch);
    }
    // SQLite output IDs are globally unique. Allocate fresh IDs while preserving
    // the source (created_at, id) order, including same-timestamp retries.
    let mut outputs = payload.outputs.clone();
    outputs.sort_unstable_by_key(|output| (output.created_at, output.id));
    let mut ids = (0..outputs.len())
        .map(|_| uuid::Uuid::now_v7())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    for (output, id) in outputs.iter_mut().zip(ids) {
        output.id = id;
        output.instance_id = destination;
    }
    Ok(outputs)
}

/// Record trusted app/backend destination acceptance, then delete its transport
/// artifact. The receipt and cleanup tombstone commit before external I/O; a
/// deletion error can be retried and background GC also retries it durably.
pub async fn acknowledge_exported_capsule(
    storage: &dyn StorageBackend,
    tenant: &TenantId,
    id: orch8_types::continuity::CapsuleId,
    accepted: &ContinuityExecution,
) -> Result<orch8_storage::capsule_export::CapsuleCutReceipt, CapsuleServiceError> {
    let receipt = storage
        .acknowledge_capsule_cut(tenant, id, accepted)
        .await?;
    storage
        .delete_artifact(&receipt.manifest.payload_artifact.key)
        .await?;
    Ok(receipt)
}

#[cfg(test)]
mod sequence_hash_tests {
    use super::*;
    use chrono::Timelike;
    use orch8_storage::testing::fixtures::SequenceBuilder;

    #[test]
    fn portable_sequence_hash_ignores_only_creation_time_and_keeps_legacy_contract() {
        let mut sequence = SequenceBuilder::new("portable-identity")
            .step("work", "noop")
            .build();
        sequence.created_at = sequence.created_at.with_nanosecond(123_456_789).unwrap();
        let mut rounded = sequence.clone();
        rounded.created_at = sequence.created_at.with_nanosecond(123_456_000).unwrap();
        let current = sequence_content_hash(&sequence, CapsuleSchemaVersion::CURRENT).unwrap();
        assert_eq!(
            current,
            sequence_content_hash(&rounded, CapsuleSchemaVersion::CURRENT).unwrap()
        );
        assert_eq!(
            sequence_content_hash(&sequence, CapsuleSchemaVersion::V1).unwrap(),
            sha256_hex(canonical_json(&sequence).unwrap().as_bytes())
        );
        assert_ne!(
            sequence_content_hash(&sequence, CapsuleSchemaVersion::V1).unwrap(),
            sequence_content_hash(&rounded, CapsuleSchemaVersion::V1).unwrap()
        );
        rounded.version += 1;
        assert_ne!(
            current,
            sequence_content_hash(&rounded, CapsuleSchemaVersion::CURRENT).unwrap()
        );
        rounded = sequence.clone();
        rounded.tenant_id = TenantId::new("different-tenant").unwrap();
        assert_ne!(
            current,
            sequence_content_hash(&rounded, CapsuleSchemaVersion::CURRENT).unwrap()
        );
        rounded = sequence.clone();
        if let orch8_types::sequence::BlockDefinition::Step(step) = &mut rounded.blocks[0] {
            step.params = serde_json::json!({"changed":true});
        }
        assert_ne!(
            current,
            sequence_content_hash(&rounded, CapsuleSchemaVersion::CURRENT).unwrap()
        );
    }
}
