//! Resolve a bounded dependency closure against a coherent catalog cut.
use super::{FrozenDelegationDefinition, invalid};
use async_trait::async_trait;
pub use orch8_types::delegation_definition::{
    DefinitionBinding, DefinitionGraphLimits, DefinitionManifest, DefinitionSelector,
    FrozenDefinitionGraph,
};
use orch8_types::delegation_definition::{definition_references as references, matches_selector};
use orch8_types::{
    error::StorageError,
    ids::{Namespace, TenantId},
    sequence::SequenceDefinition,
};
use std::collections::BTreeMap;

#[async_trait]
pub trait DefinitionResolver: Send {
    /// Resolve by the supplied tenant and namespace, preserving explicit versions
    /// and choosing latest non-deprecated only for an omitted version. All calls
    /// in one capture must observe the same transactional catalog cut.
    async fn resolve(
        &mut self,
        tenant: &TenantId,
        namespace: &Namespace,
        selector: &DefinitionSelector,
    ) -> Result<Option<SequenceDefinition>, StorageError>;
}

pub async fn capture(
    root: SequenceDefinition,
    resolver: &mut impl DefinitionResolver,
    limits: DefinitionGraphLimits,
) -> Result<FrozenDefinitionGraph, StorageError> {
    limits.validate()?;
    let tenant = root.tenant_id.clone();
    let namespace = root.namespace.clone();
    let pending = references(&root)?;
    let root = FrozenDelegationDefinition::new(root)?;
    let root_sha256 = root.sequence_sha256.clone();
    let mut definitions = BTreeMap::from([(root_sha256.clone(), root)]);
    let mut pending = pending;
    let mut bindings = BTreeMap::new();
    let mut bytes = serde_json::to_vec(&definitions)?.len();
    while let Some(selector) = pending.pop_first() {
        if bindings.contains_key(&selector) {
            continue;
        }
        if bindings.len() >= limits.selectors || pending.len() > limits.selectors {
            return Err(invalid());
        }
        let child = resolver
            .resolve(&tenant, &namespace, &selector)
            .await?
            .ok_or_else(invalid)?;
        if child.tenant_id != tenant
            || child.namespace != namespace
            || !matches_selector(&child, &selector)
        {
            return Err(invalid());
        }
        let frozen = FrozenDelegationDefinition::new(child)?;
        bindings.insert(selector, frozen.sequence_sha256.clone());
        if !definitions.contains_key(&frozen.sequence_sha256) {
            if definitions.len() >= limits.definitions {
                return Err(invalid());
            }
            bytes = bytes.saturating_add(serde_json::to_vec(&frozen)?.len());
            if bytes > limits.serialized_bytes {
                return Err(invalid());
            }
            pending.extend(references(&frozen.definition)?);
            definitions.insert(frozen.sequence_sha256.clone(), frozen);
        }
    }
    let graph = FrozenDefinitionGraph {
        manifest: DefinitionManifest {
            version: 1,
            root_sha256,
            bindings: bindings
                .into_iter()
                .map(|(selector, sequence_sha256)| DefinitionBinding {
                    selector,
                    sequence_sha256,
                })
                .collect(),
        },
        definitions: definitions.into_values().collect(),
    };
    graph.verify(&graph.sha256()?, limits)?;
    Ok(graph)
}

#[cfg(test)]
mod tests;
