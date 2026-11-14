//! Exact-byte delegated input, separately bounded from workflow definitions.
use super::{bytes_sha256, value_budget};
use crate::{delegation_definition::wire::DefinitionReadScope, error::StorageError};
use serde::{Deserialize, Serialize};
use std::{
    cell::Cell,
    io::{self, Write},
};
use utoipa::ToSchema;

mod wire;
pub use wire::DelegationInputEnvelope;

pub const MAX_INPUT_PAYLOAD_BYTES: u32 = 4 * 1024 * 1024;
pub const MAX_INPUT_EXPANDED_BYTES: u32 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct InputLimits {
    pub serialized_bytes: usize,
    pub expanded_bytes: usize,
}
impl Default for InputLimits {
    fn default() -> Self {
        Self {
            serialized_bytes: 4 * 1024 * 1024,
            expanded_bytes: 16 * 1024 * 1024,
        }
    }
}
impl InputLimits {
    pub fn validate(self) -> Result<(), StorageError> {
        let hard = Self::default();
        if self.serialized_bytes == 0
            || self.serialized_bytes > hard.serialized_bytes
            || self.expanded_bytes == 0
            || self.expanded_bytes > hard.expanded_bytes
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DelegationInputReference {
    pub version: u32,
    pub payload_sha256: String,
    pub payload_bytes: u32,
    pub expanded_bytes: u32,
}
impl DelegationInputReference {
    pub fn validate(&self, limits: InputLimits) -> Result<(), StorageError> {
        limits.validate()?;
        if self.version != 1
            || !valid_hash(&self.payload_sha256)
            || self.payload_bytes == 0
            || self.expanded_bytes == 0
            || !usize::try_from(self.payload_bytes).is_ok_and(|n| n <= limits.serialized_bytes)
            || !usize::try_from(self.expanded_bytes).is_ok_and(|n| n <= limits.expanded_bytes)
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct EncodedDelegationInput {
    reference: DelegationInputReference,
    payload: Vec<u8>,
}
impl std::fmt::Debug for EncodedDelegationInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedDelegationInput")
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}
impl EncodedDelegationInput {
    /// Validate before callers clone input or build a request hash. Counts wire
    /// bytes without allocating a second copy of the serialized value.
    pub fn validate_value(
        value: &serde_json::Value,
        limits: InputLimits,
    ) -> Result<(), StorageError> {
        limits.validate()?;
        value_budget::check(value, &Cell::new(limits.expanded_bytes), 0).map_err(|_| invalid())?;
        let mut writer = CountingWriter {
            written: 0,
            limit: limits.serialized_bytes,
        };
        serde_json::to_writer(&mut writer, value).map_err(|_| invalid())
    }
    /// Storage encoding preserves the exact JSON bytes through field encryption.
    pub fn payload_base64(&self) -> String {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        STANDARD.encode(&self.payload)
    }
    pub fn from_value(
        value: &serde_json::Value,
        limits: InputLimits,
    ) -> Result<Self, StorageError> {
        limits.validate()?;
        let remaining = Cell::new(limits.expanded_bytes);
        value_budget::check(value, &remaining, 0).map_err(|_| invalid())?;
        let expanded = limits.expanded_bytes - remaining.get();
        let mut writer = LimitedWriter {
            bytes: Vec::new(),
            limit: limits.serialized_bytes,
        };
        serde_json::to_writer(&mut writer, value).map_err(|_| invalid())?;
        let payload = writer.bytes;
        let reference = DelegationInputReference {
            version: 1,
            payload_sha256: bytes_sha256(&payload),
            payload_bytes: u32::try_from(payload.len()).map_err(|_| invalid())?,
            expanded_bytes: u32::try_from(expanded).map_err(|_| invalid())?,
        };
        reference.validate(limits)?;
        Ok(Self { reference, payload })
    }
    /// Check retained raw JSON without reserializing it through today's defaults.
    pub fn from_retained_base64(
        reference: DelegationInputReference,
        payload_base64: &str,
        limits: InputLimits,
    ) -> Result<Self, StorageError> {
        let (payload, _) = wire::decode_payload(&reference, payload_base64, limits)?;
        Ok(Self { reference, payload })
    }
    pub fn reference(&self) -> &DelegationInputReference {
        &self.reference
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Verified content/provenance equality, never permission to start provider work.
pub struct VerifiedDelegationInput {
    scope: DefinitionReadScope,
    request_sha256: String,
    reference: DelegationInputReference,
    value: serde_json::Value,
}
impl std::fmt::Debug for VerifiedDelegationInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedDelegationInput")
            .field("scope", &self.scope)
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}
impl VerifiedDelegationInput {
    pub fn scope(&self) -> &DefinitionReadScope {
        &self.scope
    }
    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }
    pub fn reference(&self) -> &DelegationInputReference {
        &self.reference
    }
    pub fn value(&self) -> &serde_json::Value {
        &self.value
    }
    pub fn into_value(self) -> serde_json::Value {
        self.value
    }
}

struct CountingWriter {
    written: usize,
    limit: usize,
}
impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.written = self
            .written
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| io::Error::other("delegated input exceeds wire budget"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Stops before extending the encoded buffer beyond its limit, including JSON
/// escaping expansion. Capacity growth also stays within the configured bound.
struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let needed = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| io::Error::other("delegated input exceeds wire budget"))?;
        if needed > self.bytes.capacity() {
            let capacity = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(needed)
                .max(4096)
                .min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| io::Error::other("delegated input allocation failed"))?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn invalid() -> StorageError {
    StorageError::Constraint("invalid, unavailable or oversized delegated input".into())
}

#[cfg(test)]
mod tests;
