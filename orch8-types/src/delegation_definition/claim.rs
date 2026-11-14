//! Immutable publication references bound to one issued lease, not provider authority.
use super::{
    DefinitionGraphLimits,
    input::{DelegationInputReference, InputLimits},
    protocol::{DelegationProtocolCapabilities, DelegationResourceUsage},
    wire::{DefinitionBundleReference, DefinitionReadScope},
};
use crate::error::StorageError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DelegationClaimContext {
    pub version: u32,
    pub scope: DefinitionReadScope,
    pub request_sha256: String,
    pub expires_at: DateTime<Utc>,
    pub definition: DefinitionBundleReference,
    pub input: DelegationInputReference,
    pub usage: DelegationResourceUsage,
}
impl DelegationClaimContext {
    pub fn validate(&self) -> Result<(), StorageError> {
        self.scope.validate()?;
        self.input.validate(InputLimits::default())?;
        let limits = DelegationProtocolCapabilities::v1(
            DefinitionGraphLimits::default(),
            super::input::MAX_INPUT_PAYLOAD_BYTES,
            super::input::MAX_INPUT_EXPANDED_BYTES,
        )?;
        if self.version != 1
            || self.request_sha256.len() != 64
            || !self
                .request_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.usage.input_wire_version != self.input.version
            || self.usage.input_payload_bytes != self.input.payload_bytes
            || self.usage.input_expanded_bytes != self.input.expanded_bytes
            || !limits.accepts(&self.definition, self.usage)
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn validate_scope(&self, expected: &DefinitionReadScope) -> Result<(), StorageError> {
        self.validate()?;
        if &self.scope != expected {
            return Err(invalid());
        }
        Ok(())
    }
}
fn invalid() -> StorageError {
    StorageError::Constraint("invalid issued delegation references".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn context() -> DelegationClaimContext {
        DelegationClaimContext {
            version: 1,
            scope: DefinitionReadScope {
                tenant_id: crate::ids::TenantId::unchecked("tenant"),
                task_id: uuid::Uuid::from_u128(1),
                worker_id: "worker".into(),
                claim_epoch: u64::MAX,
            },
            request_sha256: "a".repeat(64),
            expires_at: Utc::now(),
            definition: DefinitionBundleReference {
                version: 1,
                sequence_sha256: "b".repeat(64),
                bundle_sha256: "c".repeat(64),
                payload_sha256: "d".repeat(64),
                payload_bytes: 128,
                definition_count: 1,
                selector_count: 0,
            },
            input: DelegationInputReference {
                version: 1,
                payload_sha256: "e".repeat(64),
                payload_bytes: 4,
                expanded_bytes: 64,
            },
            usage: DelegationResourceUsage {
                definition_expanded_bytes: 512,
                input_wire_version: 1,
                input_payload_bytes: 4,
                input_expanded_bytes: 64,
            },
        }
    }
    #[test]
    fn full_width_scope_round_trips_and_metadata_is_consistent() {
        let value = context();
        value.validate().unwrap();
        let wire = serde_json::to_value(&value).unwrap();
        assert_eq!(
            wire["scope"]["claim_epoch"],
            serde_json::json!(u64::MAX.to_string())
        );
        let restored: DelegationClaimContext = serde_json::from_value(wire).unwrap();
        assert_eq!(restored, value);
        let mut wrong = value.scope.clone();
        wrong.worker_id = "other".into();
        assert!(value.validate_scope(&wrong).is_err());
        let mut wrong = value.scope.clone();
        wrong.claim_epoch -= 1;
        assert!(value.validate_scope(&wrong).is_err());
    }
    #[test]
    fn unknown_versions_mismatched_usage_and_injected_payload_are_rejected() {
        let mut variants = Vec::new();
        let mut v = context();
        v.version = 2;
        variants.push(v);
        let mut v = context();
        v.request_sha256 = "PRIVATE_UNSUPPORTED".into();
        variants.push(v);
        let mut v = context();
        v.usage.input_payload_bytes += 1;
        variants.push(v);
        let mut v = context();
        v.usage.input_expanded_bytes += 1;
        variants.push(v);
        let mut v = context();
        v.usage.input_wire_version = 2;
        variants.push(v);
        let mut v = context();
        v.usage.definition_expanded_bytes = 0;
        variants.push(v);
        for value in variants {
            let error = value.validate().unwrap_err();
            assert!(!format!("{error:?}").contains("PRIVATE_UNSUPPORTED"));
        }
        let mut wire = serde_json::to_value(context()).unwrap();
        wire["payload_base64"] = serde_json::json!("private");
        assert!(serde_json::from_value::<DelegationClaimContext>(wire).is_err());
    }
}
