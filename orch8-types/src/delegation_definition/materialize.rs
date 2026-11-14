//! Private local catalog preparation. This is not execution authorization.
use super::{DefinitionSelector, invalid, wire::VerifiedDefinitionGraph};
use crate::{
    error::StorageError,
    ids::{Namespace, SequenceId, TenantId},
    sequence::{BlockDefinition, SequenceDefinition},
};
use std::collections::{BTreeMap, BTreeSet};

/// An ephemeral plan. Store its definitions and source mappings atomically in
/// an explicitly account/catalog/server-qualified receipt before creating runs.
/// Reuse that receipt on retry; preparing again deliberately mints fresh IDs.
pub struct PreparedDefinitionGraph {
    bundle_sha256: String,
    namespace: Namespace,
    root_id: SequenceId,
    entries: Vec<PreparedDefinition>,
}
/// Source identity stays separate from the relocated definition's new hash.
pub struct PreparedDefinition {
    source_sha256: String,
    definition: SequenceDefinition,
}
impl VerifiedDefinitionGraph {
    /// Prepare definitions in a fresh private namespace under a trusted local
    /// tenant. Remote IDs never select physical local rows. This does not check
    /// claim authority, grant permission, or persist a durable retry receipt.
    pub fn prepare_local(self, tenant: &TenantId) -> Result<PreparedDefinitionGraph, StorageError> {
        if tenant.as_str().trim().is_empty() {
            return Err(invalid());
        }
        let graph = self.into_graph();
        let bundle_sha256 = graph.sha256()?;
        let versions: BTreeMap<_, _> = graph
            .definitions
            .iter()
            .map(|d| (d.sequence_sha256.as_str(), d.definition.version))
            .collect();
        let bindings: BTreeMap<_, _> = graph
            .manifest
            .bindings
            .iter()
            .map(|b| {
                versions
                    .get(b.sequence_sha256.as_str())
                    .copied()
                    .map(|v| (b.selector.clone(), v))
                    .ok_or_else(invalid)
            })
            .collect::<Result<_, _>>()?;
        let namespace = Namespace::new(format!("delegated-bundle-{}", SequenceId::new()));
        let mut ids: BTreeSet<_> = graph
            .definitions
            .iter()
            .map(|d| d.definition.id.into_uuid())
            .collect();
        let mut root_id = None;
        let mut entries = Vec::with_capacity(graph.definitions.len());
        for document in graph.definitions {
            let id = SequenceId::new();
            if !ids.insert(id.into_uuid()) {
                return Err(invalid());
            }
            if document.sequence_sha256 == graph.manifest.root_sha256 {
                root_id = Some(id);
            }
            let mut definition = document.definition;
            definition.id = id;
            definition.tenant_id = tenant.clone();
            definition.namespace = namespace.clone();
            definition.sub_tenant = None;
            definition.embed = None;
            pin_dependencies(&mut definition, &bindings)?;
            entries.push(PreparedDefinition {
                source_sha256: document.sequence_sha256,
                definition,
            });
        }
        Ok(PreparedDefinitionGraph {
            bundle_sha256,
            namespace,
            root_id: root_id.ok_or_else(invalid)?,
            entries,
        })
    }
}
impl PreparedDefinitionGraph {
    pub fn bundle_sha256(&self) -> &str {
        &self.bundle_sha256
    }
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }
    pub fn root_id(&self) -> SequenceId {
        self.root_id
    }
    pub fn entries(&self) -> &[PreparedDefinition] {
        &self.entries
    }
    pub fn into_entries(self) -> Vec<PreparedDefinition> {
        self.entries
    }
}
impl PreparedDefinition {
    pub fn source_sha256(&self) -> &str {
        &self.source_sha256
    }
    pub fn definition(&self) -> &SequenceDefinition {
        &self.definition
    }
    pub fn into_parts(self) -> (String, SequenceDefinition) {
        (self.source_sha256, self.definition)
    }
}
#[cfg(test)]
mod tests;

// This exhaustive typed traversal deliberately never inspects handler JSON.
fn pin_dependencies(
    definition: &mut SequenceDefinition,
    bindings: &BTreeMap<DefinitionSelector, i32>,
) -> Result<(), StorageError> {
    let mut pending: Vec<_> = definition
        .blocks
        .iter_mut()
        .chain(definition.on_failure.iter_mut().flatten())
        .chain(definition.on_cancel.iter_mut().flatten())
        .collect();
    while let Some(block) = pending.pop() {
        match block {
            BlockDefinition::Step(_) => {}
            BlockDefinition::SubSequence(s) => {
                let selector = DefinitionSelector {
                    name: s.sequence_name.clone(),
                    version: s.version,
                };
                s.version = Some(*bindings.get(&selector).ok_or_else(invalid)?);
            }
            BlockDefinition::Parallel(p) => pending.extend(p.branches.iter_mut().flatten()),
            BlockDefinition::Race(r) => pending.extend(r.branches.iter_mut().flatten()),
            BlockDefinition::Loop(l) => pending.extend(&mut l.body),
            BlockDefinition::ForEach(f) => pending.extend(&mut f.body),
            BlockDefinition::Router(r) => {
                pending.extend(r.routes.iter_mut().flat_map(|r| &mut r.blocks));
                pending.extend(r.default.iter_mut().flatten());
            }
            BlockDefinition::TryCatch(t) => {
                pending.extend(&mut t.try_block);
                pending.extend(&mut t.catch_block);
                pending.extend(t.finally_block.iter_mut().flatten());
            }
            BlockDefinition::ABSplit(a) => {
                pending.extend(a.variants.iter_mut().flat_map(|v| &mut v.blocks));
            }
            BlockDefinition::CancellationScope(c) => pending.extend(&mut c.blocks),
            BlockDefinition::Saga(s) => {
                for step in &mut s.steps {
                    pending.push(&mut step.action);
                    pending.extend(step.compensation.as_deref_mut());
                }
            }
        }
    }
    Ok(())
}
