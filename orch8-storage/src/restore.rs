//! Atomic logical instance imports, independent of archive representation.
use crate::externalizing::ExternalizedEntry;
use orch8_types::{
    error::StorageError, execution::ExecutionNode, instance::TaskInstance, output::BlockOutput,
};
use std::collections::{HashMap, HashSet, VecDeque};

/// One instance and all of its archived dependent rows.
#[derive(Debug, Clone)]
pub struct InstanceSnapshot {
    pub instance: TaskInstance,
    pub nodes: Vec<ExecutionNode>,
    pub outputs: Vec<BlockOutput>,
    pub payloads: Vec<ExternalizedEntry>,
}

impl InstanceSnapshot {
    /// Check ownership, duplicate identities and tree structure before import.
    pub fn validate(&self) -> Result<(), StorageError> {
        self.checked_nodes().map(|_| ())
    }

    /// Validate ownership and uniqueness, then order parents before children.
    pub(crate) fn checked_nodes(&self) -> Result<Vec<&ExecutionNode>, StorageError> {
        let id = self.instance.id;
        if self.nodes.iter().any(|node| node.instance_id != id)
            || self.outputs.iter().any(|output| output.instance_id != id)
            || self.payloads.iter().any(|entry| entry.instance_id != id)
        {
            return Err(StorageError::Query(
                "snapshot contains rows owned by another instance".into(),
            ));
        }
        let index: HashMap<_, _> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (node.id, i))
            .collect();
        let output_ids: HashSet<_> = self.outputs.iter().map(|output| output.id).collect();
        let references: HashSet<_> = self.payloads.iter().map(|entry| &entry.ref_key).collect();
        if index.len() != self.nodes.len()
            || output_ids.len() != self.outputs.len()
            || references.len() != self.payloads.len()
        {
            return Err(StorageError::Query(
                "snapshot contains duplicate row identities".into(),
            ));
        }
        let mut children = vec![Vec::new(); self.nodes.len()];
        let mut ready = VecDeque::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if let Some(parent) = node.parent_id {
                let parent = index.get(&parent).ok_or_else(|| {
                    StorageError::Query("snapshot execution node has no parent".into())
                })?;
                children[*parent].push(i);
            } else {
                ready.push_back(i);
            }
        }
        let mut ordered = Vec::with_capacity(self.nodes.len());
        while let Some(i) = ready.pop_front() {
            ordered.push(&self.nodes[i]);
            ready.extend(children[i].iter().copied());
        }
        if ordered.len() != self.nodes.len() {
            return Err(StorageError::Query(
                "snapshot execution tree contains a cycle".into(),
            ));
        }
        Ok(ordered)
    }
}

/// Check ownership before opening the capsule publication transaction.
pub(crate) fn validate_capsule_output_import(
    instance: &orch8_types::instance::TaskInstance,
    checkpoint: &orch8_types::checkpoint::Checkpoint,
    outputs: &[orch8_types::output::BlockOutput],
) -> Result<(), orch8_types::error::StorageError> {
    use orch8_types::error::StorageError;
    let mut ids = std::collections::HashSet::with_capacity(outputs.len());
    if checkpoint.instance_id != instance.id
        || outputs
            .iter()
            .any(|output| output.instance_id != instance.id || !ids.insert(output.id))
    {
        return Err(StorageError::Constraint(
            "capsule checkpoint/output ownership or duplicate output identity".into(),
        ));
    }
    Ok(())
}
