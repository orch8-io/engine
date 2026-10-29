//! Offline portable-continuity import and activation for mobile runtimes.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::Utc;
use orch8_publisher::capsule::SignedCapsuleManifest;
use orch8_storage::StorageBackend;
use orch8_types::continuity::{CapsuleId, OwnershipState, RuntimeId};
use orch8_types::encryption::FieldEncryptor;
use orch8_types::ids::InstanceId;
use sha2::Digest as _;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::error::MobileError;

#[uniffi::export(with_foreign)]
pub trait CapsuleSigner: Send + Sync {
    /// Stable identifier of the host-managed signing key.
    fn key_id(&self) -> String;
    /// Base64 raw 32-byte Ed25519 public key registered for this runtime.
    fn public_key_base64(&self) -> String;
    /// Sign the ASCII SHA-256 digest with a non-exportable host key.
    fn sign_manifest_sha256(&self, digest: String) -> Result<String, MobileError>;
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct ContinuityImportResult {
    pub capsule_id: String,
    pub continuity_id: String,
    pub instance_id: String,
    pub source_epoch: u64,
    pub state: String,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct ContinuityExportResult {
    pub capsule_id: String,
    pub continuity_id: String,
    pub source_epoch: u64,
    pub capsule_json: String,
    pub payload_base64: String,
}

fn invalid(message: impl Into<String>) -> MobileError {
    MobileError::InvalidInput {
        detail: message.into(),
    }
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid, MobileError> {
    Uuid::parse_str(value).map_err(|error| invalid(format!("invalid {field}: {error}")))
}

fn transfer_encryptor(encoded: &str) -> Result<(FieldEncryptor, String), MobileError> {
    let decoded = Zeroizing::new(
        BASE64
            .decode(encoded)
            .map_err(|_| invalid("payload key is not valid base64"))?,
    );
    let key: &[u8; 32] = decoded
        .as_slice()
        .try_into()
        .map_err(|_| invalid("payload key must decode to exactly 32 bytes"))?;
    let key_id = format!(
        "destination-transfer-{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(key))
    );
    Ok((FieldEncryptor::from_bytes(key), key_id))
}

#[allow(clippy::too_many_arguments)]
pub async fn export_capsule(
    storage: &dyn StorageBackend,
    capsule_signer: &dyn CapsuleSigner,
    instance_id: &str,
    destination_runtime_id: &str,
    payload_key_base64: &str,
    expires_in_seconds: u32,
) -> Result<ContinuityExportResult, MobileError> {
    if !(1..=3_600).contains(&expires_in_seconds) {
        return Err(invalid("capsule expiry must be between 1 and 3600 seconds"));
    }
    let instance_id = InstanceId::from_uuid(parse_uuid(instance_id, "source instance id")?);
    let destination_runtime_id = RuntimeId::from_uuid(parse_uuid(
        destination_runtime_id,
        "destination runtime id",
    )?);
    let instance = storage
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| invalid("source instance does not exist"))?;
    if instance.tenant_id.as_str() != "mobile" {
        return Err(invalid(
            "legacy capsule export cannot access an account-scoped run",
        ));
    }
    let continuity = storage
        .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
        .await?
        .ok_or_else(|| invalid("source instance has no continuity identity"))?;
    if !matches!(
        continuity.state,
        OwnershipState::Owned | OwnershipState::Transferring
    ) || continuity.current_instance_id != instance.id
    {
        return Err(invalid("source runtime does not own this execution"));
    }
    let key_id = capsule_signer.key_id();
    if key_id.is_empty() || key_id.len() > 128 {
        return Err(invalid(
            "capsule signing key id must contain 1 to 128 bytes",
        ));
    }
    let public_key = capsule_signer.public_key_base64();
    let public_key_bytes: [u8; 32] = BASE64
        .decode(&public_key)
        .map_err(|_| invalid("capsule signing public key is not valid base64"))?
        .try_into()
        .map_err(|_| invalid("capsule signing public key must decode to 32 bytes"))?;
    ed25519_dalek::VerifyingKey::from_bytes(&public_key_bytes)
        .map_err(|_| invalid("capsule signing public key is invalid"))?;
    let (encryptor, encryption_key_id) = transfer_encryptor(payload_key_base64)?;
    let manifest = orch8_engine::capsule::export_paused_capsule_manifest(
        storage,
        orch8_engine::capsule::CapsuleExportRequest {
            continuity,
            destination_runtime_id: Some(destination_runtime_id),
            requirements: orch8_types::continuity::CapsuleRequirements::default(),
            expires_at: Utc::now() + chrono::Duration::seconds(i64::from(expires_in_seconds)),
            signing_key_id: key_id,
            encryption_key_id,
        },
        &encryptor,
    )
    .await
    .map_err(|error| invalid(format!("capsule export failed: {error}")))?;
    let manifest_sha256 = orch8_publisher::capsule::manifest_sha256(&manifest)
        .map_err(|error| invalid(error.to_string()))?;
    let signature = capsule_signer.sign_manifest_sha256(manifest_sha256.clone())?;
    let signed_capsule = SignedCapsuleManifest {
        manifest,
        manifest_sha256,
        signature,
        public_key,
    };
    orch8_publisher::capsule::verify_signed_capsule(&signed_capsule)
        .map_err(|error| invalid(format!("host capsule signature is invalid: {error}")))?;
    let payload = storage
        .get_artifact(&signed_capsule.manifest.payload_artifact.key)
        .await?
        .ok_or_else(|| invalid("exported capsule payload is unavailable"))?;
    if u64::try_from(payload.len()).ok() != Some(signed_capsule.manifest.payload_artifact.bytes) {
        return Err(invalid(
            "exported capsule payload size does not match its manifest",
        ));
    }
    Ok(ContinuityExportResult {
        capsule_id: signed_capsule.manifest.capsule_id.to_string(),
        continuity_id: signed_capsule.manifest.continuity_id.to_string(),
        source_epoch: signed_capsule.manifest.epoch.get(),
        capsule_json: serde_json::to_string(&signed_capsule)?,
        payload_base64: BASE64.encode(payload),
    })
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
    if !transport_size_is_valid(declared_bytes, payload_base64.len()) {
        return Err(MobileError::ResourceLimit {
            detail: "transported capsule payload exceeds protocol bounds".into(),
        });
    }
    let sealed = BASE64
        .decode(payload_base64)
        .map_err(|_| invalid("capsule payload is not valid base64"))?;
    let (encryptor, _) = transfer_encryptor(payload_key_base64)?;
    let runtime_id = RuntimeId::from_uuid(parse_uuid(
        destination_runtime_id,
        "destination runtime id",
    )?);
    let instance_id = InstanceId::from_uuid(parse_uuid(
        destination_instance_id,
        "destination instance id",
    )?);
    let trusted_keys = [trusted_root_key.to_owned()];
    // Validate against this runtime's own identity, not the capsule's claims:
    // passing `signed.manifest.tenant_id` / `.epoch` back as the expected
    // values made both checks tautological.
    let tenant_id = crate::mobile_tenant_id();
    if signed.manifest.tenant_id != tenant_id {
        return Err(invalid("capsule belongs to a different tenant"));
    }
    // Epoch: a capsule older than the locally recorded ownership epoch for
    // this continuity is a stale replay. (Equal epochs are the idempotent
    // redelivery path, verified in full below.)
    let expected_epoch = match storage
        .get_continuity_execution(&tenant_id, signed.manifest.continuity_id)
        .await?
    {
        Some(existing) if signed.manifest.epoch.get() < existing.epoch.get() => {
            let accepted = signed.manifest.epoch.checked_next().ok() == Some(existing.epoch)
                && existing.owner_runtime_id == runtime_id
                && existing.current_instance_id == instance_id
                && matches!(
                    existing.state,
                    OwnershipState::Owned | OwnershipState::Completed
                )
                && storage
                    .is_capsule_import_instance(
                        &tenant_id,
                        signed.manifest.capsule_id,
                        runtime_id,
                        instance_id,
                    )
                    .await?;
            if !accepted {
                return Err(invalid(
                    "capsule epoch is older than the local ownership epoch",
                ));
            }
            signed.manifest.epoch
        }
        Some(existing) if signed.manifest.epoch.get() == existing.epoch.get() => existing.epoch,
        _ => signed.manifest.epoch,
    };
    let (instance, _) = orch8_engine::capsule::verify_and_import_paused_capsule_bytes(
        storage,
        &signed,
        &sealed,
        orch8_engine::capsule::CapsuleImportRequest {
            tenant_id: &tenant_id,
            destination_runtime_id: runtime_id,
            destination_instance_id: Some(instance_id),
            expected_epoch,
            trusted_public_keys: &trusted_keys,
            now: Utc::now(),
        },
        &encryptor,
    )
    .await
    .map_err(|error| invalid(format!("capsule verification failed: {error}")))?;

    Ok(ContinuityImportResult {
        capsule_id: signed.manifest.capsule_id.to_string(),
        continuity_id: signed.manifest.continuity_id.to_string(),
        instance_id: instance.id.to_string(),
        source_epoch: signed.manifest.epoch.get(),
        state: instance.state.to_string(),
    })
}

fn transport_size_is_valid(declared_bytes: usize, encoded_bytes: usize) -> bool {
    let max_sealed_bytes = orch8_types::continuity::CapsulePayload::MAX_ENCODED_BYTES + 64;
    if declared_bytes > max_sealed_bytes {
        return false;
    }
    let max_base64_bytes = declared_bytes.div_ceil(3) * 4;
    encoded_bytes <= max_base64_bytes
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
    storage
        .activate_capsule_import(
            &crate::mobile_tenant_id(),
            capsule_id,
            runtime_id,
            instance_id,
            Utc::now(),
        )
        .await
        .map_err(|error| match error {
            orch8_types::error::StorageError::Constraint(detail) => invalid(detail),
            error => error.into(),
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use base64::engine::general_purpose::STANDARD as BASE64;
    use chrono::{Duration, Utc};
    use ed25519_dalek::{Signer as _, SigningKey};
    use orch8_engine::capsule::{CapsuleExportRequest, export_paused_capsule};
    use orch8_storage::artifacts::ObjectArtifactStore;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::{
        ContinuityStore as _, InstanceStore as _, ResourceStore as _, SequenceStore as _,
    };
    use orch8_types::checkpoint::Checkpoint;
    use orch8_types::context::ExecutionContext;
    use orch8_types::continuity::{
        CapsuleRequirements, ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState,
    };
    use orch8_types::ids::{Namespace, SequenceId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use orch8_types::sequence::SequenceDefinition;
    use serde_json::json;

    use super::*;

    #[test]
    fn transport_size_rejects_huge_declaration_without_overflow() {
        assert!(!transport_size_is_valid(usize::MAX, 0));
    }

    #[test]
    fn transport_size_accepts_exact_base64_bound_and_rejects_excess() {
        assert!(transport_size_is_valid(4, 8));
        assert!(!transport_size_is_valid(4, 9));
    }

    #[tokio::test]
    async fn activation_keeps_storage_failures_retryable_and_identity_errors_invalid() {
        use orch8_storage::testing::faulty::FaultStorage;
        let store = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let (storage, faults) = FaultStorage::wrap(store);
        let capsule = CapsuleId::new().to_string();
        let runtime = RuntimeId::new().to_string();
        let instance = InstanceId::new().to_string();
        faults
            .on("activate_capsule_import")
            .times(1)
            .fail_transient();
        assert!(matches!(
            activate_capsule(storage.as_ref(), &capsule, &runtime, &instance).await,
            Err(MobileError::Storage { .. })
        ));
        assert!(matches!(
            activate_capsule(storage.as_ref(), &capsule, &runtime, &instance).await,
            Err(MobileError::InvalidInput { .. })
        ));
    }

    struct TestSigner(SigningKey);

    impl CapsuleSigner for TestSigner {
        fn key_id(&self) -> String {
            "device-key".into()
        }

        fn public_key_base64(&self) -> String {
            BASE64.encode(self.0.verifying_key().to_bytes())
        }

        fn sign_manifest_sha256(&self, digest: String) -> Result<String, MobileError> {
            Ok(BASE64.encode(self.0.sign(digest.as_bytes()).to_bytes()))
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one round trip exercises every durable mobile boundary
    async fn portable_roundtrip_survives_redelivery_and_activation() {
        let source = SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory()));
        let destination = SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory()));
        let tenant_id = crate::mobile_tenant_id();
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
            claimed_at: None,
            sub_tenant: None,
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

        destination
            .update_instance_state(destination_instance, InstanceState::Waiting, None)
            .await
            .unwrap();
        // A delayed acknowledgement retry must not resume a legitimate wait.
        let waiting = destination
            .get_instance(destination_instance)
            .await
            .unwrap()
            .unwrap();
        activate_capsule(
            &destination,
            &first.capsule_id,
            &destination_runtime.to_string(),
            &destination_instance.to_string(),
        )
        .await
        .unwrap();
        let retried = destination
            .get_instance(destination_instance)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried.state, InstanceState::Waiting);
        assert_eq!(retried.next_fire_at, waiting.next_fire_at);
        assert_eq!(retried.updated_at, waiting.updated_at);
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
        assert_eq!(redelivered.state, "waiting");
        let return_key = [23; 32];
        let exported = export_capsule(
            &destination,
            &TestSigner(SigningKey::from_bytes(&[29; 32])),
            &destination_instance.to_string(),
            &RuntimeId::new().to_string(),
            &BASE64.encode(return_key),
            300,
        )
        .await
        .unwrap();
        let signed_return: SignedCapsuleManifest =
            serde_json::from_str(&exported.capsule_json).unwrap();
        orch8_publisher::capsule::verify_signed_capsule(&signed_return).unwrap();
        assert_eq!(
            signed_return.manifest.source_runtime_id,
            destination_runtime
        );
        assert_eq!(signed_return.manifest.epoch.get(), 1);
        assert!(!exported.payload_base64.is_empty());
    }

    #[test]
    #[allow(clippy::too_many_lines)] // one real constructor/reopen/background/foreground lifecycle
    fn mobile_constructor_exports_and_reopens_durable_artifacts() {
        use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
        let scratch = tempfile::tempdir().unwrap();
        let path = scratch
            .path()
            .join("device.sqlite")
            .to_string_lossy()
            .into_owned();
        let engine =
            crate::MobileEngine::new(path.clone(), crate::MobileEngineConfig::default()).unwrap();
        let sequence = SequenceBuilder::new("mobile").step("done", "noop").build();
        let instance = InstanceBuilder::for_sequence(&sequence)
            .state(InstanceState::Paused)
            .build();
        engine.runtime.block_on(async {
            engine.storage.create_sequence(&sequence).await.unwrap();
            engine.storage.create_instance(&instance).await.unwrap();
            engine
                .storage
                .save_checkpoint(&Checkpoint {
                    id: Uuid::now_v7(),
                    instance_id: instance.id,
                    checkpoint_data: json!({"completed_blocks":[]}),
                    created_at: Utc::now(),
                })
                .await
                .unwrap();
            engine
                .storage
                .create_continuity_execution(&ContinuityExecution {
                    continuity_id: ContinuityId::new(),
                    tenant_id: instance.tenant_id.clone(),
                    current_instance_id: instance.id,
                    owner_runtime_id: RuntimeId::new(),
                    epoch: ExecutionEpoch::initial(),
                    state: OwnershipState::Owned,
                    updated_at: Utc::now(),
                })
                .await
                .unwrap();
        });
        let destination = RuntimeId::new().to_string();
        let key = BASE64.encode([13; 32]);
        let exported = engine
            .export_continuity_capsule(
                instance.id.to_string(),
                destination.clone(),
                key.clone(),
                300,
                Arc::new(TestSigner(SigningKey::from_bytes(&[17; 32]))),
            )
            .unwrap();
        let sibling = engine
            .runtime
            .block_on(engine.storage.put_artifact(
                instance.id,
                "text/plain",
                b"persistent output".as_slice().to_vec().into(),
            ))
            .unwrap();
        engine.shutdown();
        engine.runtime.block_on(engine.sqlite.close());
        drop(engine);
        let reopen_path = {
            #[cfg(unix)]
            {
                let alias = scratch.path().join("device-alias.sqlite");
                std::os::unix::fs::symlink(&path, &alias).unwrap();
                alias.to_string_lossy().into_owned()
            }
            #[cfg(not(unix))]
            {
                path.clone()
            }
        };
        let reopened =
            crate::MobileEngine::new(reopen_path.clone(), crate::MobileEngineConfig::default())
                .unwrap();
        assert_eq!(
            reopened
                .runtime
                .block_on(reopened.storage.get_artifact(&sibling.key))
                .unwrap()
                .unwrap(),
            b"persistent output"
        );
        let retry = reopened
            .export_continuity_capsule(
                instance.id.to_string(),
                destination.clone(),
                key,
                600,
                Arc::new(TestSigner(SigningKey::from_bytes(&[17; 32]))),
            )
            .unwrap();
        assert_eq!(retry.capsule_json, exported.capsule_json);
        assert_eq!(retry.payload_base64, exported.payload_base64);
        assert!(
            reopened
                .export_continuity_capsule(
                    instance.id.to_string(),
                    destination.clone(),
                    BASE64.encode([14; 32]),
                    300,
                    Arc::new(TestSigner(SigningKey::from_bytes(&[17; 32])))
                )
                .is_err()
        );
        let signed: SignedCapsuleManifest = serde_json::from_str(&exported.capsule_json).unwrap();
        let accepted = reopened.runtime.block_on(async {
            let mut owner = reopened
                .storage
                .get_continuity_execution(&instance.tenant_id, signed.manifest.continuity_id)
                .await
                .unwrap()
                .unwrap();
            owner.epoch = owner.epoch.checked_next().unwrap();
            owner.owner_runtime_id = RuntimeId::from_uuid(destination.parse().unwrap());
            owner.current_instance_id = InstanceId::new();
            owner.state = OwnershipState::Owned;
            reopened
                .storage
                .acknowledge_capsule_cut(&instance.tenant_id, signed.manifest.capsule_id, &owner)
                .await
                .unwrap();
            owner
        });
        // The real OS-background API reaches retirement maintenance even though
        // the source remains paused and cannot execute another workflow step.
        reopened.run_until_idle(1, 1000).unwrap();
        assert!(
            reopened
                .runtime
                .block_on(
                    reopened
                        .storage
                        .get_artifact(&signed.manifest.payload_artifact.key)
                )
                .unwrap()
                .is_none()
        );
        reopened.runtime.block_on(async {
            reopened
                .storage
                .put_artifact_with_id(
                    instance.id,
                    signed.manifest.capsule_id.into_uuid(),
                    "application/octet-stream",
                    BASE64.decode(&exported.payload_base64).unwrap().into(),
                )
                .await
                .unwrap();
            sqlx::query(
                "UPDATE capsule_artifact_retirements SET next_sweep_at=?1 WHERE artifact_key=?2",
            )
            .bind((Utc::now() - Duration::minutes(1)).to_rfc3339())
            .bind(&signed.manifest.payload_artifact.key)
            .execute(reopened.sqlite.pool())
            .await
            .unwrap();
        });
        reopened.shutdown();
        reopened.runtime.block_on(reopened.sqlite.close());
        drop(reopened);
        let foreground =
            crate::MobileEngine::new(reopen_path, crate::MobileEngineConfig::default()).unwrap();
        foreground.resume();
        let started = std::time::Instant::now();
        while foreground
            .runtime
            .block_on(
                foreground
                    .storage
                    .get_artifact(&signed.manifest.payload_artifact.key),
            )
            .unwrap()
            .is_some()
        {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(5),
                "foreground maintenance did not retire transport"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(
            foreground
                .runtime
                .block_on(
                    foreground
                        .storage
                        .get_continuity_execution(&instance.tenant_id, accepted.continuity_id)
                )
                .unwrap()
                .unwrap(),
            accepted
        );
        foreground.pause();
        foreground.shutdown();
    }

    #[tokio::test]
    async fn legacy_export_cannot_freeze_an_account_scoped_execution() {
        use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
        let storage = SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory()));
        let sequence = SequenceBuilder::new("private-account")
            .step("done", "noop")
            .build();
        let instance = InstanceBuilder::for_sequence(&sequence)
            .state(InstanceState::Paused)
            .build();
        storage.create_sequence(&sequence).await.unwrap();
        storage.create_instance(&instance).await.unwrap();
        storage
            .save_checkpoint(&Checkpoint {
                id: Uuid::now_v7(),
                instance_id: instance.id,
                checkpoint_data: json!({"completed_blocks":[]}),
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
        let error = export_capsule(
            &storage,
            &TestSigner(SigningKey::from_bytes(&[17; 32])),
            &instance.id.to_string(),
            &RuntimeId::new().to_string(),
            &BASE64.encode([13; 32]),
            300,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, MobileError::InvalidInput { detail } if detail.contains("account-scoped"))
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
        assert_eq!(cuts, 0);
    }
}
