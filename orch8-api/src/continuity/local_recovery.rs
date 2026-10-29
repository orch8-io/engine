//! Signed local rollback snapshots are deliberately not portable transfer grants.
use super::{ApiError, AppState, hex_sha256};
use crate::ContinuityCrypto;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer};
use orch8_storage::{
    capsule_export::CapsuleSourceSnapshot,
    local_migration::{
        LocalMigrationSnapshot, LocalMigrationSnapshotCapture, LocalMigrationSnapshotRequest,
    },
};
use orch8_types::{
    continuity::{
        ArtifactReference, CapsuleId, ContinuityExecution, ContinuityId, EffectState,
        ExecutionEpoch,
    },
    continuity_advanced::{LiveMigrationRecord, MigrationPlanId, MigrationRollbackCapsule},
    ids::{InstanceId, SequenceId, TenantId},
    instance::TaskInstance,
};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

const PURPOSE: &str = "orch8.local-migration-recovery/v1";
const MAX_PLAIN: usize = 64 * 1024 * 1024;
const MAX_ENVELOPE: usize = 96 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    purpose: String,
    migration_id: MigrationPlanId,
    tenant_id: TenantId,
    continuity_id: ContinuityId,
    instance_id: InstanceId,
    epoch: ExecutionEpoch,
    sequence_id: SequenceId,
    sequence_version: i32,
    checkpoint_id: uuid::Uuid,
    source_sha256: String,
    expires_at: DateTime<Utc>,
    signing_key_id: String,
    encryption_key_id: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    identity: Identity,
    payload_sha256: String,
    payload_bytes: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    manifest: Manifest,
    payload_base64: String,
    public_key: String,
    signature: String,
}

fn conflict() -> ApiError {
    ApiError::Conflict("local migration recovery identity or integrity mismatch".into())
}
fn canonical(value: &impl Serialize) -> Result<Vec<u8>, ApiError> {
    orch8_publisher::manifest::canonical_json(value)
        .map(String::into_bytes)
        .map_err(|e| ApiError::Internal(e.to_string()))
}
fn identity(
    record: &LiveMigrationRecord,
    crypto: &ContinuityCrypto,
    source_sha256: String,
) -> Identity {
    Identity {
        purpose: PURPOSE.into(),
        migration_id: record.plan.id,
        tenant_id: record.plan.tenant_id.clone(),
        continuity_id: record.plan.continuity_id,
        instance_id: record.source_checkpoint.instance_id,
        epoch: record.expected_epoch,
        sequence_id: record.plan.from_sequence_id,
        sequence_version: record.plan.from_version,
        checkpoint_id: record.source_checkpoint.id,
        source_sha256,
        expires_at: record.rollback_expires_at,
        signing_key_id: crypto.signing_key_id.clone(),
        encryption_key_id: crypto.encryption_key_id.clone(),
    }
}
fn aad(identity: &Identity) -> Result<Vec<u8>, ApiError> {
    let mut bytes = PURPOSE.as_bytes().to_vec();
    bytes.push(0);
    bytes.extend(canonical(identity)?);
    Ok(bytes)
}
fn validate_source(
    record: &LiveMigrationRecord,
    source: &CapsuleSourceSnapshot,
) -> Result<(), ApiError> {
    if source.instance.id != record.source_checkpoint.instance_id
        || source.instance.tenant_id != record.plan.tenant_id
        || source.instance.sequence_id != record.plan.from_sequence_id
        || source.sequence.id != record.plan.from_sequence_id
        || source.sequence.version != record.plan.from_version
        || serde_json::to_value(&source.instance.context).map_err(|_| conflict())?
            != record.source_context
        || serde_json::to_value(&source.checkpoint).map_err(|_| conflict())?
            != serde_json::to_value(&record.source_checkpoint).map_err(|_| conflict())?
    {
        return Err(conflict());
    }
    Ok(())
}
fn reference(
    record: &LiveMigrationRecord,
    key: &str,
    bytes: &[u8],
    envelope: &Envelope,
) -> Result<MigrationRollbackCapsule, ApiError> {
    Ok(MigrationRollbackCapsule {
        capsule_id: CapsuleId::from_uuid(record.plan.id.into_uuid()),
        payload_artifact: ArtifactReference {
            key: key.into(),
            sha256: hex_sha256(bytes),
            bytes: bytes.len() as u64,
        },
        manifest_sha256: hex_sha256(&canonical(&envelope.manifest)?),
        public_key: envelope.public_key.clone(),
        signature: envelope.signature.clone(),
    })
}
fn open(
    record: &LiveMigrationRecord,
    crypto: &ContinuityCrypto,
    bytes: &[u8],
    expected_source: Option<&str>,
) -> Result<(Envelope, CapsuleSourceSnapshot), ApiError> {
    if bytes.len() > MAX_ENVELOPE || record.rollback_expires_at <= Utc::now() {
        return Err(conflict());
    }
    let envelope: Envelope = serde_json::from_slice(bytes).map_err(|_| conflict())?;
    let current = &envelope.manifest.identity;
    if *current != identity(record, crypto, current.source_sha256.clone())
        || expected_source.is_some_and(|sha| sha != current.source_sha256)
        || envelope.public_key != BASE64.encode(crypto.signing_key.verifying_key().as_bytes())
        || envelope.manifest.payload_bytes > MAX_PLAIN as u64 + 64
    {
        return Err(conflict());
    }
    let signed = canonical(&envelope.manifest)?;
    let signature =
        Signature::from_slice(&BASE64.decode(&envelope.signature).map_err(|_| conflict())?)
            .map_err(|_| conflict())?;
    crypto
        .signing_key
        .verifying_key()
        .verify_strict(&signed, &signature)
        .map_err(|_| conflict())?;
    let sealed = BASE64
        .decode(&envelope.payload_base64)
        .map_err(|_| conflict())?;
    if sealed.len() as u64 != envelope.manifest.payload_bytes
        || hex_sha256(&sealed) != envelope.manifest.payload_sha256
    {
        return Err(conflict());
    }
    let plain = Zeroizing::new(
        crypto
            .payload_encryptor
            .decrypt_bytes_with_aad(&sealed, &aad(current)?)
            .map_err(|_| conflict())?,
    );
    if plain.len() > MAX_PLAIN || hex_sha256(&plain) != current.source_sha256 {
        return Err(conflict());
    }
    let source: CapsuleSourceSnapshot = serde_json::from_slice(&plain).map_err(|_| conflict())?;
    validate_source(record, &source)?;
    Ok((envelope, source))
}

pub(super) async fn capture(
    state: &AppState,
    record: &LiveMigrationRecord,
    execution: &ContinuityExecution,
    expected_instance_updated_at: DateTime<Utc>,
) -> Result<LocalMigrationSnapshot, ApiError> {
    let captured = state
        .storage
        .prepare_local_migration_snapshot(&LocalMigrationSnapshotRequest {
            execution: execution.clone(),
            migration_id: record.plan.id,
            expected_instance_updated_at,
            mobile_access: None,
        })
        .await
        .map_err(|e| ApiError::from_storage(e, "local migration snapshot"))?;
    let LocalMigrationSnapshotCapture::Ready(snapshot) = captured else {
        return Err(ApiError::Conflict(
            "migration source is no longer at a safe unchanged local boundary".into(),
        ));
    };
    Ok(*snapshot)
}

pub(super) async fn prepare(
    state: &AppState,
    record: &LiveMigrationRecord,
    execution: &ContinuityExecution,
    instance: &TaskInstance,
) -> Result<(MigrationRollbackCapsule, LocalMigrationSnapshot), ApiError> {
    let crypto = state.continuity_crypto.as_ref().ok_or_else(|| {
        ApiError::Unavailable("local recovery signing/encryption keys are not configured".into())
    })?;
    let snapshot = capture(state, record, execution, instance.updated_at).await?;
    if serde_json::to_value(&snapshot.record).map_err(|_| conflict())?
        != serde_json::to_value(record).map_err(|_| conflict())?
    {
        return Err(conflict());
    }
    validate_source(record, &snapshot.source)?;
    if snapshot
        .source
        .effects
        .receipts
        .iter()
        .any(|r| matches!(r.state, EffectState::Dispatched | EffectState::Unknown))
    {
        return Err(ApiError::Conflict(
            "local migration requires reconciliation of in-flight or unknown effects".into(),
        ));
    }
    let plain = Zeroizing::new(canonical(&snapshot.source)?);
    if plain.len() > MAX_PLAIN {
        return Err(ApiError::PayloadTooLarge(
            "local recovery snapshot exceeds 64 MiB".into(),
        ));
    }
    let source_sha = hex_sha256(&plain);
    if let Some(bytes) = state
        .storage
        .get_artifact(&snapshot.artifact_key)
        .await
        .map_err(|e| ApiError::from_storage(e, "local recovery retry"))?
    {
        let (envelope, _) = open(record, crypto, &bytes, Some(&source_sha))?;
        return Ok((
            reference(record, &snapshot.artifact_key, &bytes, &envelope)?,
            snapshot,
        ));
    }
    let identity = identity(record, crypto, source_sha.clone());
    let sealed = crypto
        .payload_encryptor
        .encrypt_bytes_with_aad(&plain, &aad(&identity)?)
        .map_err(|_| ApiError::Internal("local recovery encryption failed".into()))?;
    let manifest = Manifest {
        identity,
        payload_sha256: hex_sha256(&sealed),
        payload_bytes: sealed.len() as u64,
    };
    let signature = BASE64.encode(crypto.signing_key.sign(&canonical(&manifest)?).to_bytes());
    let envelope = Envelope {
        manifest,
        payload_base64: BASE64.encode(&sealed),
        public_key: BASE64.encode(crypto.signing_key.verifying_key().as_bytes()),
        signature,
    };
    let encoded = canonical(&envelope)?;
    if encoded.len() > MAX_ENVELOPE {
        return Err(ApiError::PayloadTooLarge(
            "local recovery envelope exceeds 96 MiB".into(),
        ));
    }
    let result = state
        .storage
        .put_artifact_with_id(
            snapshot.source.instance.id,
            record.plan.id.into_uuid(),
            "application/vnd.orch8.local-migration-recovery+json",
            encoded.clone().into(),
        )
        .await;
    let bytes = match result {
        Ok(artifact) => {
            if artifact.key != snapshot.artifact_key || artifact.size != encoded.len() as u64 {
                return Err(conflict());
            }
            encoded
        }
        Err(error) => {
            // A Create conflict or lost upload acknowledgement can recover the
            // prior signed bytes. Never overwrite the original plan's artifact.
            match state.storage.get_artifact(&snapshot.artifact_key).await {
                Ok(Some(existing)) => existing,
                _ => return Err(ApiError::from_storage(error, "local recovery artifact")),
            }
        }
    };
    let (envelope, _) = open(record, crypto, &bytes, Some(&source_sha))?;
    Ok((
        reference(record, &snapshot.artifact_key, &bytes, &envelope)?,
        snapshot,
    ))
}

fn verify(
    state: &AppState,
    record: &LiveMigrationRecord,
    bytes: &[u8],
) -> Result<CapsuleSourceSnapshot, ApiError> {
    let crypto = state
        .continuity_crypto
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("local recovery keys are not configured".into()))?;
    let expected = record.rollback_capsule.as_ref().ok_or_else(conflict)?;
    let key = format!(
        "{}/{}",
        record.source_checkpoint.instance_id, record.plan.id
    );
    let (envelope, source) = open(record, crypto, bytes, None)?;
    if reference(record, &key, bytes, &envelope)? != *expected {
        return Err(conflict());
    }
    Ok(source)
}

/// Authenticate the retained reference and envelope before any rollback write.
pub(super) async fn load(
    state: &AppState,
    record: &LiveMigrationRecord,
) -> Result<CapsuleSourceSnapshot, ApiError> {
    let rollback_capsule = record.rollback_capsule.as_ref().ok_or_else(|| {
        ApiError::Conflict("migration has no retained pre-migration capsule".into())
    })?;
    let capsule_payload = state
        .storage
        .get_artifact(&rollback_capsule.payload_artifact.key)
        .await
        .map_err(|error| ApiError::from_storage(error, "rollback capsule"))?
        .ok_or_else(|| ApiError::Conflict("rollback capsule artifact is unavailable".into()))?;
    if capsule_payload.len() as u64 != rollback_capsule.payload_artifact.bytes
        || hex_sha256(&capsule_payload) != rollback_capsule.payload_artifact.sha256
    {
        return Err(ApiError::Conflict(
            "rollback capsule artifact failed integrity verification".into(),
        ));
    }
    verify(state, record, &capsule_payload)
}
