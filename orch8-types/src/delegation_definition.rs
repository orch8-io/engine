//! Bounded frozen workflow documents and pure dependency verification.
//! Available without SQL/storage features for native and WebAssembly consumers.
use crate::{
    error::StorageError,
    sequence::{BlockDefinition, SequenceDefinition},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

mod bounded;
pub mod claim;
pub mod input;
pub mod materialize;
pub mod protocol;
mod value_budget;
pub mod wire;

/// Leaves room for the envelope below the remote client's 8 MiB response cap.
pub const MAX_DEFINITION_BYTES: usize = 4 * 1024 * 1024;
/// Logical decoded JSON budget: node/key charges plus UTF-8 string bytes.
pub const MAX_DEFINITION_EXPANDED_BYTES: usize = 16 * 1024 * 1024;

/// One catalog document. A dependency manifest and execution authority are
/// separate contracts; this document alone does not certify a runnable closure.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenDelegationDefinition {
    pub version: u32,
    pub sequence_sha256: String,
    pub definition: SequenceDefinition,
}
impl std::fmt::Debug for FrozenDelegationDefinition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrozenDelegationDefinition")
            .field("version", &self.version)
            .field("sequence_sha256", &self.sequence_sha256)
            .finish_non_exhaustive()
    }
}
impl FrozenDelegationDefinition {
    pub fn new(definition: SequenceDefinition) -> Result<Self, StorageError> {
        let record = serde_json::to_string(&definition)?;
        if record.len() > MAX_DEFINITION_BYTES {
            return Err(invalid());
        }
        Ok(Self {
            version: 1,
            sequence_sha256: sequence_sha256(&definition)?,
            definition,
        })
    }
}

/// Decode retained bytes while preserving their semantic identity. New defaulted
/// struct fields must not change the fingerprint of an older stored document.
pub fn decode_retained_definition(
    record: &str,
) -> Result<FrozenDelegationDefinition, StorageError> {
    if record.len() > MAX_DEFINITION_BYTES {
        return Err(invalid());
    }
    let remaining = std::cell::Cell::new(MAX_DEFINITION_EXPANDED_BYTES);
    let mut decoder = serde_json::Deserializer::from_str(record);
    let value = serde::de::DeserializeSeed::deserialize(
        value_budget::ValueSeed {
            remaining: &remaining,
        },
        &mut decoder,
    )
    .map_err(|_| invalid())?;
    decoder.end().map_err(|_| invalid())?;
    definition_from_value(value).map_err(|_| invalid())
}
fn definition_from_value(
    mut value: serde_json::Value,
) -> Result<FrozenDelegationDefinition, StorageError> {
    // Exclude creation metadata without duplicating the bounded decoded graph.
    // Restore the original field before typed decoding applies schema defaults.
    let created_at = value
        .as_object_mut()
        .ok_or_else(invalid)?
        .remove("created_at");
    let sequence_sha256 = semantic_json_sha256(&value);
    if let Some(created_at) = created_at {
        value
            .as_object_mut()
            .ok_or_else(invalid)?
            .insert("created_at".into(), created_at);
    }
    Ok(FrozenDelegationDefinition {
        version: 1,
        sequence_sha256,
        definition: serde_json::from_value(value)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefinitionSelector {
    pub name: String,
    pub version: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefinitionBinding {
    pub selector: DefinitionSelector,
    pub sequence_sha256: String,
}

/// Separate identity from the root hash: an unversioned child can select a
/// different revision while the root document remains identical.
#[derive(Debug, Clone, Serialize)]
pub struct DefinitionManifest {
    pub version: u32,
    pub root_sha256: String,
    pub bindings: Vec<DefinitionBinding>,
}
impl<'de> Deserialize<'de> for DefinitionManifest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        serde::de::DeserializeSeed::deserialize(
            bounded::ManifestSeed {
                selectors: DefinitionGraphLimits::default().selectors,
            },
            deserializer,
        )
    }
}
impl DefinitionManifest {
    pub fn sha256(&self) -> Result<String, StorageError> {
        Ok(semantic_json_sha256(&serde_json::to_value(self)?))
    }

    pub fn decode(record: &str) -> Result<Self, StorageError> {
        if record.len() > MAX_DEFINITION_BYTES {
            return Err(invalid());
        }
        let manifest: Self = serde_json::from_str(record).map_err(|_| invalid())?;
        manifest.document_hashes()?;
        Ok(manifest)
    }

    pub fn document_hashes(&self) -> Result<BTreeSet<String>, StorageError> {
        let limits = DefinitionGraphLimits::default();
        if self.version != 1 || self.bindings.len() > limits.selectors {
            return Err(invalid());
        }
        let hashes: BTreeSet<_> = std::iter::once(self.root_sha256.clone())
            .chain(self.bindings.iter().map(|b| b.sequence_sha256.clone()))
            .collect();
        if hashes.len() > limits.definitions
            || hashes.iter().any(|hash| {
                hash.len() != 64
                    || !hash
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
        {
            return Err(invalid());
        }
        Ok(hashes)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenDefinitionGraph {
    pub manifest: DefinitionManifest,
    #[serde(deserialize_with = "bounded::definitions")]
    pub definitions: Vec<FrozenDelegationDefinition>,
}
impl std::fmt::Debug for FrozenDefinitionGraph {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FrozenDefinitionGraph")
            .field("root_sha256", &self.manifest.root_sha256)
            .field("definition_count", &self.definitions.len())
            .field("selector_count", &self.manifest.bindings.len())
            .finish_non_exhaustive()
    }
}

/// Local resource limits may tighten the hard wire limits, never enlarge them.
#[derive(Debug, Clone, Copy)]
pub struct DefinitionGraphLimits {
    pub definitions: usize,
    pub selectors: usize,
    pub serialized_bytes: usize,
    /// Independent of serialized bytes; a small JSON array can expand greatly.
    pub expanded_bytes: usize,
}
impl Default for DefinitionGraphLimits {
    fn default() -> Self {
        Self {
            definitions: 256,
            selectors: 1024,
            serialized_bytes: MAX_DEFINITION_BYTES,
            expanded_bytes: MAX_DEFINITION_EXPANDED_BYTES,
        }
    }
}
impl DefinitionGraphLimits {
    pub fn validate(self) -> Result<(), StorageError> {
        let hard = Self::default();
        if self.definitions == 0
            || self.definitions > hard.definitions
            || self.selectors > hard.selectors
            || self.serialized_bytes == 0
            || self.serialized_bytes > hard.serialized_bytes
            || self.expanded_bytes == 0
            || self.expanded_bytes > hard.expanded_bytes
        {
            return Err(invalid());
        }
        Ok(())
    }
}

impl FrozenDefinitionGraph {
    pub fn sha256(&self) -> Result<String, StorageError> {
        self.manifest.sha256()
    }

    /// Verify against the separately trusted publication identity. Rehashing a
    /// downloaded body alone cannot establish its provenance.
    pub fn verify(
        &self,
        expected_sha256: &str,
        limits: DefinitionGraphLimits,
    ) -> Result<(), StorageError> {
        self.verify_inner(expected_sha256, limits, true)
    }

    fn verify_inner(
        &self,
        expected_sha256: &str,
        limits: DefinitionGraphLimits,
        rehash_typed_bodies: bool,
    ) -> Result<(), StorageError> {
        limits.validate()?;
        if self.manifest.version != 1
            || self.sha256()? != expected_sha256
            || self.definitions.is_empty()
            || self.definitions.len() > limits.definitions
            || self.manifest.bindings.len() > limits.selectors
            || (rehash_typed_bodies && serde_json::to_vec(self)?.len() > limits.serialized_bytes)
        {
            return Err(invalid());
        }
        let mut definitions = BTreeMap::new();
        let remaining = std::cell::Cell::new(limits.expanded_bytes);
        let mut ids = BTreeSet::new();
        let mut versions = BTreeSet::new();
        for document in &self.definitions {
            if rehash_typed_bodies {
                let mut value = serde_json::to_value(&document.definition)?;
                value_budget::check(&value, &remaining, 0)?;
                value
                    .as_object_mut()
                    .ok_or_else(invalid)?
                    .remove("created_at");
                if semantic_json_sha256(&value) != document.sequence_sha256 {
                    return Err(invalid());
                }
            }
            if document.version != 1
                || !ids.insert(document.definition.id.into_uuid())
                || !versions.insert((
                    document.definition.name.clone(),
                    document.definition.version,
                ))
                || definitions
                    .insert(document.sequence_sha256.clone(), &document.definition)
                    .is_some()
            {
                return Err(invalid());
            }
        }
        let root = definitions
            .get(&self.manifest.root_sha256)
            .ok_or_else(invalid)?;
        let mut bindings = BTreeMap::new();
        for binding in &self.manifest.bindings {
            let target = definitions
                .get(&binding.sequence_sha256)
                .ok_or_else(invalid)?;
            if target.tenant_id != root.tenant_id
                || target.namespace != root.namespace
                || !matches_selector(target, &binding.selector)
                || bindings
                    .insert(&binding.selector, binding.sequence_sha256.as_str())
                    .is_some()
            {
                return Err(invalid());
            }
        }
        let mut visited = BTreeSet::new();
        let mut required = BTreeSet::new();
        let mut pending = vec![self.manifest.root_sha256.as_str()];
        while let Some(hash) = pending.pop() {
            if !visited.insert(hash) {
                continue;
            }
            // Bodies were hash-verified above. Traverse by identity so a cycle
            // or a shared large child does not rehash its body for every edge.
            let document = definitions.get(hash).ok_or_else(invalid)?;
            for selector in definition_references(document)? {
                let target = bindings.get(&selector).ok_or_else(invalid)?;
                required.insert(selector);
                pending.push(*target);
            }
        }
        if visited.len() != definitions.len() || required.len() != bindings.len() {
            return Err(invalid());
        }
        Ok(())
    }
}

pub fn matches_selector(definition: &SequenceDefinition, selector: &DefinitionSelector) -> bool {
    definition.name == selector.name
        && selector
            .version
            .map_or(!definition.deprecated, |v| v == definition.version)
}

/// Walk the DSL, including lifecycle and saga cleanup. Arbitrary handler input,
/// interceptor parameters and JSON metadata are deliberately not DSL references.
pub fn definition_references(
    definition: &SequenceDefinition,
) -> Result<BTreeSet<DefinitionSelector>, StorageError> {
    definition.validate().map_err(|_| invalid())?;
    let mut pending: Vec<_> = definition
        .blocks
        .iter()
        .chain(definition.on_failure.iter().flatten())
        .chain(definition.on_cancel.iter().flatten())
        .collect();
    let mut found = BTreeSet::new();
    while let Some(block) = pending.pop() {
        match block {
            BlockDefinition::Step(_) => {}
            BlockDefinition::SubSequence(s) => {
                found.insert(DefinitionSelector {
                    name: s.sequence_name.clone(),
                    version: s.version,
                });
            }
            BlockDefinition::Parallel(p) => pending.extend(p.branches.iter().flatten()),
            BlockDefinition::Race(r) => pending.extend(r.branches.iter().flatten()),
            BlockDefinition::Loop(l) => pending.extend(&l.body),
            BlockDefinition::ForEach(f) => pending.extend(&f.body),
            BlockDefinition::Router(r) => {
                pending.extend(r.routes.iter().flat_map(|r| &r.blocks));
                pending.extend(r.default.iter().flatten());
            }
            BlockDefinition::TryCatch(t) => {
                pending.extend(&t.try_block);
                pending.extend(&t.catch_block);
                pending.extend(t.finally_block.iter().flatten());
            }
            BlockDefinition::ABSplit(a) => {
                pending.extend(a.variants.iter().flat_map(|v| &v.blocks));
            }
            BlockDefinition::CancellationScope(c) => pending.extend(&c.blocks),
            BlockDefinition::Saga(s) => {
                for step in &s.steps {
                    pending.push(&step.action);
                    pending.extend(step.compensation.as_deref());
                }
            }
        }
    }
    Ok(found)
}

/// Preserve the published semantic receipt identity: sorted JSON, excluding only
/// catalog creation time. Wire-byte identities are a separate protocol field.
pub fn sequence_sha256(definition: &SequenceDefinition) -> Result<String, StorageError> {
    let mut value = serde_json::to_value(definition)?;
    value
        .as_object_mut()
        .ok_or_else(invalid)?
        .remove("created_at");
    Ok(semantic_json_sha256(&value))
}
pub(crate) fn semantic_json_sha256(value: &serde_json::Value) -> String {
    bytes_sha256(crate::ai::canonical_json(value).as_bytes())
}
pub(crate) fn bytes_sha256(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut result, "{byte:02x}").expect("writing to String cannot fail");
    }
    result
}
pub(crate) fn invalid() -> StorageError {
    StorageError::Constraint("missing, oversized or corrupt frozen definition bundle".into())
}
