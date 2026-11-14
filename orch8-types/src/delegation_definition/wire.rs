//! Versioned exact-byte transport. A trusted claim supplies the expected
//! reference and scope; a digest copied from an untrusted response proves neither.
use super::{
    DefinitionGraphLimits, DefinitionManifest, FrozenDefinitionGraph, FrozenDelegationDefinition,
    bytes_sha256, definition_from_value, invalid,
};
use crate::{error::StorageError, ids::TenantId};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::de::DeserializeSeed as _;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

mod decode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DefinitionBundleReference {
    pub version: u32,
    pub sequence_sha256: String,
    pub bundle_sha256: String,
    pub payload_sha256: String,
    pub payload_bytes: u32,
    pub definition_count: u32,
    pub selector_count: u32,
}
impl DefinitionBundleReference {
    pub fn validate(&self, limits: DefinitionGraphLimits) -> Result<(), StorageError> {
        limits.validate()?;
        if self.version != 1
            || self.payload_bytes == 0
            || u64::from(self.payload_bytes) > limits.serialized_bytes as u64
            || self.definition_count == 0
            || u64::from(self.definition_count) > limits.definitions as u64
            || u64::from(self.selector_count) > limits.selectors as u64
            || [
                &self.sequence_sha256,
                &self.bundle_sha256,
                &self.payload_sha256,
            ]
            .iter()
            .any(|h| {
                h.len() != 64
                    || !h
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DefinitionReadScope {
    pub tenant_id: TenantId,
    pub task_id: Uuid,
    pub worker_id: String,
    /// Decimal text on the wire: JSON numbers cannot preserve every u64 in JS.
    #[serde(with = "epoch_text")]
    #[schema(value_type = String, example = "1")]
    pub claim_epoch: u64,
}
impl DefinitionReadScope {
    pub fn validate(&self) -> Result<(), StorageError> {
        if TenantId::new(self.tenant_id.as_str()).is_err()
            || self.task_id.is_nil()
            || self.worker_id.trim().is_empty()
            || self.worker_id.len() > 256
            || self.claim_epoch == 0
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionPayloadEncoding {
    JsonUtf8Base64,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DefinitionWireEnvelope {
    pub version: u32,
    pub scope: DefinitionReadScope,
    pub reference: DefinitionBundleReference,
    pub payload_encoding: DefinitionPayloadEncoding,
    pub payload_base64: String,
}
impl std::fmt::Debug for DefinitionWireEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefinitionWireEnvelope")
            .field("version", &self.version)
            .field("scope", &self.scope)
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}
impl DefinitionWireEnvelope {
    pub fn new(
        scope: DefinitionReadScope,
        bundle: &EncodedDefinitionBundle,
    ) -> Result<Self, StorageError> {
        scope.validate()?;
        Ok(Self {
            version: 1,
            scope,
            reference: bundle.reference.clone(),
            payload_encoding: DefinitionPayloadEncoding::JsonUtf8Base64,
            payload_base64: STANDARD.encode(&bundle.payload),
        })
    }

    pub fn verify(
        &self,
        expected_scope: &DefinitionReadScope,
        expected: &DefinitionBundleReference,
        limits: DefinitionGraphLimits,
    ) -> Result<VerifiedDefinitionGraph, StorageError> {
        expected_scope.validate()?;
        expected.validate(limits)?;
        let bytes = usize::try_from(expected.payload_bytes).map_err(|_| invalid())?;
        if self.version != 1
            || self.payload_encoding != DefinitionPayloadEncoding::JsonUtf8Base64
            || &self.scope != expected_scope
            || &self.reference != expected
            || self.payload_base64.len() != bytes.div_ceil(3) * 4
        {
            return Err(invalid());
        }
        // Length/resource checks precede decoding/allocation. Identity precedes JSON parsing.
        let payload = STANDARD
            .decode(&self.payload_base64)
            .map_err(|_| invalid())?;
        if payload.len() != bytes || bytes_sha256(&payload) != expected.payload_sha256 {
            return Err(invalid());
        }
        // Serde diagnostics can include raw field values. The public boundary
        // must not turn a malformed retained secret into display/debug output.
        let verified =
            decode_payload(&payload, &expected.bundle_sha256, limits).map_err(|_| invalid())?;
        let graph = verified.graph();
        if graph.manifest.root_sha256 != expected.sequence_sha256
            || graph.definitions.len() as u64 != u64::from(expected.definition_count)
            || graph.manifest.bindings.len() as u64 != u64::from(expected.selector_count)
            || verified.root().definition.tenant_id != expected_scope.tenant_id
        {
            return Err(invalid());
        }
        Ok(verified)
    }
}

/// Exact retained row bytes, not a language's parse/stringify round trip.
pub struct RetainedDefinitionDocument {
    pub sequence_sha256: String,
    pub definition_json: String,
}

#[derive(Clone)]
pub struct EncodedDefinitionBundle {
    reference: DefinitionBundleReference,
    payload: Vec<u8>,
    expanded_bytes: u32,
}
impl std::fmt::Debug for EncodedDefinitionBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedDefinitionBundle")
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}
impl EncodedDefinitionBundle {
    /// Codec 1 uses this exact wrapper/order around retained JSON. Its bytes must
    /// remain stable across coordinator upgrades and are pinned by the reference.
    pub fn from_retained(
        manifest_json: &str,
        mut documents: Vec<RetainedDefinitionDocument>,
        limits: DefinitionGraphLimits,
    ) -> Result<(Self, VerifiedDefinitionGraph), StorageError> {
        limits.validate()?;
        if manifest_json.len() > limits.serialized_bytes || documents.len() > limits.definitions {
            return Err(invalid());
        }
        let manifest = DefinitionManifest::decode(manifest_json)?;
        let semantic_sha = manifest.sha256()?;
        documents.sort_by(|a, b| a.sequence_sha256.cmp(&b.sequence_sha256));
        let mut payload = String::from("{\"manifest\":");
        payload.push_str(manifest_json);
        payload.push_str(",\"definitions\":[");
        for (index, document) in documents.into_iter().enumerate() {
            if document.sequence_sha256.len() != 64
                || !document
                    .sequence_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                || document.definition_json.len() > limits.serialized_bytes
            {
                return Err(invalid());
            }
            let header = format!(
                "{{\"version\":1,\"sequence_sha256\":{},\"definition\":",
                serde_json::to_string(&document.sequence_sha256)?
            );
            let addition =
                usize::from(index > 0) + header.len() + document.definition_json.len() + 3;
            if payload.len().saturating_add(addition) > limits.serialized_bytes {
                return Err(invalid());
            }
            // A row is one standalone JSON object, never a fragment capable of
            // injecting another document into the outer wrapper.
            if !document.definition_json.trim_start().starts_with('{') {
                return Err(invalid());
            }
            let _: serde::de::IgnoredAny =
                serde_json::from_str(&document.definition_json).map_err(|_| invalid())?;
            if index > 0 {
                payload.push(',');
            }
            payload.push_str(&header);
            payload.push_str(&document.definition_json);
            payload.push('}');
        }
        payload.push_str("]}");
        let payload = payload.into_bytes();
        let verified = decode_payload(&payload, &semantic_sha, limits).map_err(|_| invalid())?;
        let graph = verified.graph();
        let reference = DefinitionBundleReference {
            version: 1,
            sequence_sha256: manifest.root_sha256,
            bundle_sha256: semantic_sha,
            payload_sha256: bytes_sha256(&payload),
            payload_bytes: u32::try_from(payload.len()).map_err(|_| invalid())?,
            definition_count: u32::try_from(graph.definitions.len()).map_err(|_| invalid())?,
            selector_count: u32::try_from(graph.manifest.bindings.len()).map_err(|_| invalid())?,
        };
        reference.validate(limits)?;
        let expanded_bytes = verified.expanded_bytes();
        Ok((
            Self {
                reference,
                payload,
                expanded_bytes,
            },
            verified,
        ))
    }
    pub fn reference(&self) -> &DefinitionBundleReference {
        &self.reference
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn expanded_bytes(&self) -> u32 {
        self.expanded_bytes
    }
}

pub struct VerifiedDefinitionGraph(FrozenDefinitionGraph, u32);
impl std::fmt::Debug for VerifiedDefinitionGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedDefinitionGraph")
            .field("root_sha256", &self.0.manifest.root_sha256)
            .field("definition_count", &self.0.definitions.len())
            .finish_non_exhaustive()
    }
}
impl VerifiedDefinitionGraph {
    pub fn expanded_bytes(&self) -> u32 {
        self.1
    }
    pub fn graph(&self) -> &FrozenDefinitionGraph {
        &self.0
    }
    pub fn into_graph(self) -> FrozenDefinitionGraph {
        self.0
    }
    pub fn root(&self) -> &FrozenDelegationDefinition {
        self.0
            .definitions
            .iter()
            .find(|d| d.sequence_sha256 == self.0.manifest.root_sha256)
            .expect("verified closure has its root")
    }
}

struct RawGraph {
    manifest: DefinitionManifest,
    definitions: Vec<RawDocument>,
    expanded_bytes: usize,
}
struct RawDocument {
    version: u32,
    sequence_sha256: String,
    definition: serde_json::Value,
}

fn decode_payload(
    payload: &[u8],
    expected: &str,
    limits: DefinitionGraphLimits,
) -> Result<VerifiedDefinitionGraph, StorageError> {
    limits.validate()?;
    if payload.len() > limits.serialized_bytes {
        return Err(invalid());
    }
    let mut decoder = serde_json::Deserializer::from_slice(payload);
    let raw = decode::GraphSeed::new(limits).deserialize(&mut decoder)?;
    decoder.end()?;
    let mut definitions = Vec::with_capacity(raw.definitions.len());
    for document in raw.definitions {
        let frozen = definition_from_value(document.definition)?;
        if document.version != 1 || frozen.sequence_sha256 != document.sequence_sha256 {
            return Err(invalid());
        }
        definitions.push(frozen);
    }
    let graph = FrozenDefinitionGraph {
        manifest: raw.manifest,
        definitions,
    };
    // Hash the retained shape above, never a reserialized current struct with
    // newly defaulted fields. Typed validation still verifies the complete DSL.
    graph.verify_inner(expected, limits, false)?;
    Ok(VerifiedDefinitionGraph(
        graph,
        u32::try_from(raw.expanded_bytes).map_err(|_| invalid())?,
    ))
}

mod epoch_text {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};
    // Serde's field serializer hook requires a reference to the field.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S: Serializer>(epoch: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&epoch.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let text = String::deserialize(deserializer)?;
        let epoch = text.parse::<u64>().map_err(D::Error::custom)?;
        if text != epoch.to_string() {
            return Err(D::Error::custom(
                "claim epoch must be canonical decimal text",
            ));
        }
        Ok(epoch)
    }
}

#[cfg(test)]
mod tests;
