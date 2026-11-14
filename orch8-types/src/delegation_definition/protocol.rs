//! Explicit decoder/interpretation support, never execution or credential authority.
use super::DefinitionGraphLimits;
use super::wire::DefinitionBundleReference;
use crate::error::StorageError;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub use super::input::{MAX_INPUT_EXPANDED_BYTES, MAX_INPUT_PAYLOAD_BYTES};

/// Opt-in support for the complete delegated definition/input protocol.
/// Missing support must never be inferred from handlers, platform or build version.
/// Existing clients must not advertise this until their execution bridge is ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DelegationProtocolCapabilities {
    pub version: u32,
    pub definition_wire_version: u32,
    pub max_definition_payload_bytes: u32,
    pub max_definitions: u32,
    pub max_selectors: u32,
    pub max_definition_expanded_bytes: u32,
    pub input_wire_version: u32,
    pub max_input_payload_bytes: u32,
    pub max_input_expanded_bytes: u32,
}

/// Immutable publication metadata must supply usage; it is not a claimant assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DelegationResourceUsage {
    pub definition_expanded_bytes: u32,
    pub input_wire_version: u32,
    pub input_payload_bytes: u32,
    pub input_expanded_bytes: u32,
}

impl DelegationResourceUsage {
    /// Measure retained definition bytes before typed defaults, and logical input
    /// before storage encryption. Publication must commit this immutable metadata.
    pub fn from_encoded(
        definitions: &super::wire::EncodedDefinitionBundle,
        input: &super::input::EncodedDelegationInput,
    ) -> Self {
        Self {
            definition_expanded_bytes: definitions.expanded_bytes(),
            input_wire_version: input.reference().version,
            input_payload_bytes: input.reference().payload_bytes,
            input_expanded_bytes: input.reference().expanded_bytes,
        }
    }
}

impl DelegationProtocolCapabilities {
    pub fn v1(
        definitions: DefinitionGraphLimits,
        input_payload_bytes: u32,
        input_expanded_bytes: u32,
    ) -> Result<Self, StorageError> {
        definitions.validate()?;
        let narrow = |value| u32::try_from(value).map_err(|_| invalid());
        let value = Self {
            version: 1,
            definition_wire_version: 1,
            max_definition_payload_bytes: narrow(definitions.serialized_bytes)?,
            max_definitions: narrow(definitions.definitions)?,
            max_selectors: narrow(definitions.selectors)?,
            max_definition_expanded_bytes: narrow(definitions.expanded_bytes)?,
            input_wire_version: 1,
            max_input_payload_bytes: input_payload_bytes,
            max_input_expanded_bytes: input_expanded_bytes,
        };
        value.validate()?;
        Ok(value)
    }
    pub fn definition_limits(self) -> Result<DefinitionGraphLimits, StorageError> {
        let size = |value| usize::try_from(value).map_err(|_| invalid());
        Ok(DefinitionGraphLimits {
            definitions: size(self.max_definitions)?,
            selectors: size(self.max_selectors)?,
            serialized_bytes: size(self.max_definition_payload_bytes)?,
            expanded_bytes: size(self.max_definition_expanded_bytes)?,
        })
    }
    pub fn validate(self) -> Result<(), StorageError> {
        if self.version != 1
            || self.definition_wire_version != 1
            || self.input_wire_version != 1
            || self.max_input_payload_bytes == 0
            || self.max_input_payload_bytes > MAX_INPUT_PAYLOAD_BYTES
            || self.max_input_expanded_bytes == 0
            || self.max_input_expanded_bytes > MAX_INPUT_EXPANDED_BYTES
        {
            return Err(invalid());
        }
        self.definition_limits()?.validate().map_err(|_| invalid())
    }
    /// Compatibility only. Issuer, lease, scope, handler requirements and physical
    /// capacity must be enforced independently in the atomic admission path.
    #[must_use]
    pub fn accepts(
        self,
        reference: &DefinitionBundleReference,
        usage: DelegationResourceUsage,
    ) -> bool {
        if self.validate().is_err() {
            return false;
        }
        let Ok(limits) = self.definition_limits() else {
            return false;
        };
        reference.version == self.definition_wire_version
            && reference.validate(limits).is_ok()
            && usage.definition_expanded_bytes > 0
            && usize::try_from(usage.definition_expanded_bytes)
                .is_ok_and(|bytes| bytes <= limits.expanded_bytes)
            && usage.input_wire_version == self.input_wire_version
            && usage.input_payload_bytes > 0
            && usage.input_payload_bytes <= self.max_input_payload_bytes
            && usage.input_expanded_bytes > 0
            && usage.input_expanded_bytes <= self.max_input_expanded_bytes
    }
}
fn invalid() -> StorageError {
    StorageError::Constraint("invalid delegation protocol capability contract".into())
}

#[cfg(test)]
mod tests;
