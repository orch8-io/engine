use chrono::{Duration, Utc};
use ed25519_dalek::SigningKey;
use orch8_engine::capsule::{CapsuleExportRequest, CapsuleImportRequest};
use orch8_publisher::capsule::SignedCapsuleManifest;
use orch8_types::checkpoint::Checkpoint;
use orch8_types::continuity::{
    CapsuleRequirements, ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState,
    RuntimeId,
};
use orch8_types::encryption::FieldEncryptor;
use orch8_types::ids::InstanceId;
use orch8_types::instance::{InstanceState, TaskInstance};
use uuid::Uuid;

use crate::{Engine, Error};

const MAX_CHECKPOINT_DATA_BYTES: usize = 1024 * 1024;

/// Host-selected settings for a signed, encrypted execution capsule.
#[derive(Debug, Clone)]
pub struct CapsuleExportOptions {
    pub source_runtime_id: RuntimeId,
    pub destination_runtime_id: Option<RuntimeId>,
    pub requirements: CapsuleRequirements,
    /// Bounded to 1–3,600 seconds to keep intercepted capsules short-lived.
    pub expires_in_seconds: u32,
    pub signing_key_id: String,
    pub encryption_key_id: String,
}

/// Transport-ready capsule. The encrypted payload is carried separately from
/// the signed manifest so hosts can use their own network or storage channel.
#[derive(Clone)]
pub struct PortableCapsule {
    pub signed_manifest: SignedCapsuleManifest,
    pub encrypted_payload: Vec<u8>,
}

impl std::fmt::Debug for PortableCapsule {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PortableCapsule")
            .field("capsule_id", &self.signed_manifest.manifest.capsule_id)
            .field("encrypted_payload_bytes", &self.encrypted_payload.len())
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// Persist a bounded safe-boundary checkpoint for later capsule export.
    ///
    /// The instance must already be paused or waiting. Orch8 refuses to
    /// snapshot a running instance because its external effects may still be
    /// in flight.
    pub async fn portable_checkpoint(
        &self,
        instance_id: InstanceId,
        checkpoint_data: serde_json::Value,
    ) -> Result<Checkpoint, Error> {
        let instance = self.get_instance(instance_id).await?;
        self.require_local_tenant(&instance)?;
        if !matches!(
            instance.state,
            InstanceState::Paused | InstanceState::Waiting
        ) {
            return Err(Error::Config(
                "portable checkpoints require a paused or waiting instance".into(),
            ));
        }
        let encoded = serde_json::to_vec(&checkpoint_data)
            .map_err(|error| Error::Config(format!("invalid checkpoint data: {error}")))?;
        if encoded.len() > MAX_CHECKPOINT_DATA_BYTES {
            return Err(Error::Config(format!(
                "checkpoint data exceeds the {MAX_CHECKPOINT_DATA_BYTES}-byte facade limit"
            )));
        }
        let checkpoint = Checkpoint {
            id: Uuid::now_v7(),
            instance_id,
            checkpoint_data,
            created_at: Utc::now(),
        };
        self.storage_backend().save_checkpoint(&checkpoint).await?;
        Ok(checkpoint)
    }

    /// Export a paused instance as a signed, encrypted, transport-ready
    /// capsule. The destination must already have the referenced immutable
    /// sequence definition before import.
    pub async fn export_portable_capsule(
        &self,
        instance_id: InstanceId,
        options: CapsuleExportOptions,
        signing_key: &SigningKey,
        payload_encryptor: &FieldEncryptor,
    ) -> Result<PortableCapsule, Error> {
        validate_export_options(&options)?;
        let instance = self.get_instance(instance_id).await?;
        self.require_local_tenant(&instance)?;
        let candidate = ContinuityExecution {
            continuity_id: ContinuityId::default(),
            tenant_id: instance.tenant_id.clone(),
            current_instance_id: instance.id,
            owner_runtime_id: options.source_runtime_id,
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: Utc::now(),
        };
        let continuity = self
            .storage_backend()
            .ensure_continuity_execution(&candidate)
            .await?;
        if continuity.owner_runtime_id != options.source_runtime_id
            || continuity.current_instance_id != instance.id
            || continuity.state != OwnershipState::Owned
        {
            return Err(Error::Config(
                "source runtime does not own this continuity execution".into(),
            ));
        }
        let signed_manifest = orch8_engine::capsule::export_paused_capsule(
            self.storage_backend().as_ref(),
            CapsuleExportRequest {
                continuity,
                destination_runtime_id: options.destination_runtime_id,
                requirements: options.requirements,
                expires_at: Utc::now() + Duration::seconds(i64::from(options.expires_in_seconds)),
                signing_key_id: options.signing_key_id,
                encryption_key_id: options.encryption_key_id,
            },
            signing_key,
            payload_encryptor,
        )
        .await?;
        let encrypted_payload = self
            .storage_backend()
            .get_artifact(&signed_manifest.manifest.payload_artifact.key)
            .await?
            .ok_or(orch8_engine::capsule::CapsuleServiceError::MissingArtifact)?
            .clone();
        Ok(PortableCapsule {
            signed_manifest,
            encrypted_payload,
        })
    }

    /// Verify and idempotently import a transported capsule in paused state.
    /// Signature trust, tenant, destination runtime, epoch, payload size, and
    /// payload digest are all checked before any instance is created.
    pub async fn import_portable_capsule(
        &self,
        capsule: &PortableCapsule,
        destination_runtime_id: RuntimeId,
        destination_instance_id: Option<InstanceId>,
        trusted_public_keys: &[String],
        payload_encryptor: &FieldEncryptor,
    ) -> Result<TaskInstance, Error> {
        if &capsule.signed_manifest.manifest.tenant_id != self.tenant_id() {
            return Err(Error::NotFound("capsule tenant".into()));
        }
        let manifest = &capsule.signed_manifest.manifest;
        let (instance, _) = orch8_engine::capsule::verify_and_import_paused_capsule_bytes(
            self.storage_backend().as_ref(),
            &capsule.signed_manifest,
            &capsule.encrypted_payload,
            CapsuleImportRequest {
                tenant_id: self.tenant_id(),
                destination_runtime_id,
                destination_instance_id,
                expected_epoch: manifest.epoch,
                trusted_public_keys,
                now: Utc::now(),
            },
            payload_encryptor,
        )
        .await?;
        let pending = ContinuityExecution {
            continuity_id: manifest.continuity_id,
            tenant_id: self.tenant_id().clone(),
            current_instance_id: instance.id,
            owner_runtime_id: manifest.source_runtime_id,
            epoch: manifest.epoch,
            state: OwnershipState::Transferring,
            updated_at: Utc::now(),
        };
        let durable = self
            .storage_backend()
            .ensure_continuity_execution(&pending)
            .await?;
        if !same_continuity_identity(&durable, &pending) {
            return Err(Error::Config(
                "imported capsule conflicts with an existing continuity identity".into(),
            ));
        }
        self.storage_backend()
            .save_capsule_manifest(&capsule.signed_manifest.manifest)
            .await?;
        Ok(instance)
    }

    fn require_local_tenant(&self, instance: &TaskInstance) -> Result<(), Error> {
        if &instance.tenant_id == self.tenant_id() {
            Ok(())
        } else {
            Err(Error::NotFound(format!("instance {}", instance.id)))
        }
    }
}

fn same_continuity_identity(left: &ContinuityExecution, right: &ContinuityExecution) -> bool {
    left.continuity_id == right.continuity_id
        && left.tenant_id == right.tenant_id
        && left.current_instance_id == right.current_instance_id
        && left.owner_runtime_id == right.owner_runtime_id
        && left.epoch == right.epoch
        && left.state == right.state
}

fn validate_export_options(options: &CapsuleExportOptions) -> Result<(), Error> {
    if !(1..=3_600).contains(&options.expires_in_seconds) {
        return Err(Error::Config(
            "capsule expiry must be between 1 and 3600 seconds".into(),
        ));
    }
    for (label, value) in [
        ("signing key id", &options.signing_key_id),
        ("encryption key id", &options.encryption_key_id),
    ] {
        if value.is_empty() || value.len() > 128 {
            return Err(Error::Config(format!(
                "capsule {label} must contain 1 to 128 bytes"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> CapsuleExportOptions {
        CapsuleExportOptions {
            source_runtime_id: RuntimeId::new(),
            destination_runtime_id: Some(RuntimeId::new()),
            requirements: CapsuleRequirements::default(),
            expires_in_seconds: 300,
            signing_key_id: "signing".into(),
            encryption_key_id: "encryption".into(),
        }
    }

    #[test]
    fn export_options_enforce_short_lived_capsules() {
        let mut value = options();
        value.expires_in_seconds = 0;
        assert!(validate_export_options(&value).is_err());
        value.expires_in_seconds = 3_601;
        assert!(validate_export_options(&value).is_err());
        value.expires_in_seconds = 3_600;
        assert!(validate_export_options(&value).is_ok());
    }

    #[test]
    fn export_options_bound_key_identifiers() {
        let mut value = options();
        value.signing_key_id.clear();
        assert!(validate_export_options(&value).is_err());
        value.signing_key_id = "s".repeat(129);
        assert!(validate_export_options(&value).is_err());
    }
}

#[cfg(test)]
#[path = "portable_coverage_tests.rs"]
mod portable_coverage_tests;
