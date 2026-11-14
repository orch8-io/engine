//! Stored frozen documents and publication-scoped server adapters.
use crate::delegation_publication::DelegationPublication;
use orch8_types::delegation_definition::wire::{EncodedDefinitionBundle, VerifiedDefinitionGraph};
pub use orch8_types::delegation_definition::{FrozenDelegationDefinition, MAX_DEFINITION_BYTES};
use orch8_types::{error::StorageError, ids::TenantId, sequence::SequenceDefinition};
use serde::Serialize;
use uuid::Uuid;
pub mod graph;

/// A catalog that cannot form a supported frozen closure is a publication
/// precondition failure, not an outage. Preserve actual backend errors so
/// transient failures retain their existing retry handling.
pub(crate) fn publication_error(error: StorageError) -> StorageError {
    match error {
        StorageError::Constraint(_) | StorageError::Serialization(_) => StorageError::Conflict(
            "delegation definitions are invalid or exceed supported limits".into(),
        ),
        other => other,
    }
}

pub(crate) fn decode_document(
    tenant: &TenantId,
    expected_sha256: &str,
    sequence_id: Uuid,
    version: i64,
    record: &str,
) -> Result<FrozenDelegationDefinition, StorageError> {
    if version != 1 || record.len() > MAX_DEFINITION_BYTES {
        return Err(invalid());
    }
    let frozen = orch8_types::delegation_definition::decode_retained_definition(record)?;
    if frozen.definition.tenant_id != *tenant
        || frozen.definition.id.into_uuid() != sequence_id
        || frozen.sequence_sha256 != expected_sha256
    {
        return Err(invalid());
    }
    Ok(frozen)
}

/// Server-side decoded closure plus the exact-byte transport retained with it.
#[derive(Clone, Serialize)]
pub struct PublishedDelegationDefinition {
    pub version: u32,
    pub sequence_sha256: String,
    pub definition: SequenceDefinition,
    pub bundle_sha256: String,
    pub manifest: graph::DefinitionManifest,
    pub dependencies: Vec<FrozenDelegationDefinition>,
    #[serde(skip)]
    wire: EncodedDefinitionBundle,
}
impl std::fmt::Debug for PublishedDelegationDefinition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublishedDelegationDefinition")
            .field("version", &self.version)
            .field("sequence_sha256", &self.sequence_sha256)
            .field("bundle_sha256", &self.bundle_sha256)
            .field("dependency_count", &self.dependencies.len())
            .finish_non_exhaustive()
    }
}
impl PublishedDelegationDefinition {
    pub fn handler_names(&self) -> Vec<String> {
        std::iter::once(&self.definition)
            .chain(self.dependencies.iter().map(|d| &d.definition))
            .flat_map(SequenceDefinition::handler_names)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(crate) fn from_verified_graph(
        verified: VerifiedDefinitionGraph,
        wire: EncodedDefinitionBundle,
        tenant: &TenantId,
        task: Uuid,
        publication: &DelegationPublication,
    ) -> Result<Self, StorageError> {
        publication.validate()?;
        let mut graph = verified.into_graph();
        let expected_sha256 = wire.reference().bundle_sha256.clone();
        if publication.task_id != task
            || publication.delegation.tenant_id != *tenant
            || graph.manifest.root_sha256 != publication.sequence_sha256
            || graph.sha256()? != expected_sha256
        {
            return Err(invalid());
        }
        let index = graph
            .definitions
            .iter()
            .position(|d| d.sequence_sha256 == graph.manifest.root_sha256)
            .ok_or_else(invalid)?;
        let root = graph.definitions.remove(index);
        if root.definition.tenant_id != *tenant
            || root.definition.id != publication.delegation.sub_sequence_id
        {
            return Err(invalid());
        }
        Ok(Self {
            version: root.version,
            sequence_sha256: root.sequence_sha256,
            definition: root.definition,
            bundle_sha256: expected_sha256,
            manifest: graph.manifest,
            dependencies: graph.definitions,
            wire,
        })
    }

    pub fn wire(&self) -> &EncodedDefinitionBundle {
        &self.wire
    }

    /// Reconstruct the common verifier input without consulting a live catalog.
    pub fn into_graph(self) -> graph::FrozenDefinitionGraph {
        let mut definitions = self.dependencies;
        definitions.push(FrozenDelegationDefinition {
            version: self.version,
            sequence_sha256: self.sequence_sha256,
            definition: self.definition,
        });
        graph::FrozenDefinitionGraph {
            manifest: self.manifest,
            definitions,
        }
    }
}
pub(crate) fn invalid() -> StorageError {
    StorageError::Constraint("missing, oversized or corrupt frozen delegation definition".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frozen_definition_budget_counts_exact_serialized_utf8_bytes() {
        let mut sequence = crate::testing::fixtures::SequenceBuilder::new("definition-budget")
            .step("work", "noop")
            .build();
        if let orch8_types::sequence::BlockDefinition::Step(step) = &mut sequence.blocks[0] {
            step.params = serde_json::json!({"padding":""});
        }
        let empty = serde_json::to_string(&sequence).unwrap().len();
        if let orch8_types::sequence::BlockDefinition::Step(step) = &mut sequence.blocks[0] {
            step.params["padding"] = serde_json::json!("x".repeat(MAX_DEFINITION_BYTES - empty));
        }
        assert_eq!(
            serde_json::to_string(&sequence).unwrap().len(),
            MAX_DEFINITION_BYTES
        );
        assert!(FrozenDelegationDefinition::new(sequence.clone()).is_ok());
        if let orch8_types::sequence::BlockDefinition::Step(step) = &mut sequence.blocks[0] {
            step.params["padding"] =
                serde_json::json!(format!("{}x", step.params["padding"].as_str().unwrap()));
        }
        assert_eq!(
            serde_json::to_string(&sequence).unwrap().len(),
            MAX_DEFINITION_BYTES + 1
        );
        assert!(FrozenDelegationDefinition::new(sequence.clone()).is_err());
        if let orch8_types::sequence::BlockDefinition::Step(step) = &mut sequence.blocks[0] {
            let mut padding = step.params["padding"].as_str().unwrap().to_owned();
            padding.pop();
            padding.push('λ');
            step.params["padding"] = serde_json::json!(padding);
        }
        assert_eq!(
            serde_json::to_string(&sequence).unwrap().len(),
            MAX_DEFINITION_BYTES + 2
        );
        assert!(FrozenDelegationDefinition::new(sequence).is_err());
    }
}
