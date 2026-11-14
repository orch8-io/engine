//! Immutable publication-owned input; reading content never authorizes execution.
use orch8_types::{
    delegation_definition::{
        input::{
            DelegationInputEnvelope, DelegationInputReference, EncodedDelegationInput, InputLimits,
        },
        protocol::DelegationResourceUsage,
        wire::DefinitionReadScope,
    },
    error::StorageError,
    ids::TenantId,
};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone)]
pub struct PublishedDelegationInput {
    tenant: TenantId,
    task: Uuid,
    request_sha256: String,
    expires_at: chrono::DateTime<chrono::Utc>,
    reference: DelegationInputReference,
    usage: DelegationResourceUsage,
    pub(crate) stored_input: Value,
}
impl std::fmt::Debug for PublishedDelegationInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishedDelegationInput")
            .field("task", &self.task)
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}
impl PublishedDelegationInput {
    pub(crate) fn from_record(
        tenant: &TenantId,
        task: Uuid,
        publication: &str,
        request_sha256: String,
        reference: DelegationInputReference,
        definition_expanded_bytes: u32,
        stored_input: &str,
    ) -> Result<Self, StorageError> {
        let receipt: crate::delegation_publication::DelegationPublication =
            serde_json::from_str(publication).map_err(|_| invalid())?;
        receipt.validate().map_err(|_| invalid())?;
        reference.validate(InputLimits::default())?;
        if receipt.task_id != task
            || receipt.delegation.tenant_id != *tenant
            || receipt.request_sha256 != request_sha256
            || request_sha256.len() != 64
            || !request_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || definition_expanded_bytes == 0
            || definition_expanded_bytes > 16 * 1024 * 1024
            || stored_input.len() > 8 * 1024 * 1024
        {
            return Err(invalid());
        }
        let usage = DelegationResourceUsage {
            definition_expanded_bytes,
            input_wire_version: reference.version,
            input_payload_bytes: reference.payload_bytes,
            input_expanded_bytes: reference.expanded_bytes,
        };
        Ok(Self {
            tenant: tenant.clone(),
            task,
            request_sha256,
            expires_at: receipt.delegation.expires_at,
            reference,
            usage,
            stored_input: Value::String(
                serde_json::from_str::<String>(stored_input).map_err(|_| invalid())?,
            ),
        })
    }
    pub fn expires_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.expires_at
    }
    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }
    pub fn reference(&self) -> &DelegationInputReference {
        &self.reference
    }
    pub fn usage(&self) -> DelegationResourceUsage {
        self.usage
    }
    pub fn envelope(
        &self,
        scope: DefinitionReadScope,
    ) -> Result<DelegationInputEnvelope, StorageError> {
        if scope.tenant_id != self.tenant || scope.task_id != self.task {
            return Err(invalid());
        }
        let input = EncodedDelegationInput::from_retained_base64(
            self.reference.clone(),
            self.stored_input.as_str().ok_or_else(invalid)?,
            InputLimits::default(),
        )?;
        DelegationInputEnvelope::new(scope, self.request_sha256.clone(), &input)
    }
}
pub(crate) fn invalid() -> StorageError {
    StorageError::Constraint("invalid, missing or oversized retained delegation input".into())
}
