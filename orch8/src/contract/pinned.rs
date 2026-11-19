use std::collections::{HashMap, HashSet};

use orch8_types::delegation_definition::definition_references;
use orch8_types::ids::SequenceId;
use orch8_types::sequence::SequenceDefinition;

use crate::error::Error;

pub(super) fn resolve<'a>(
    root: &'a SequenceDefinition,
    children: impl IntoIterator<Item = &'a SequenceDefinition>,
) -> Result<Vec<&'a SequenceDefinition>, Error> {
    let mut revisions = HashMap::new();
    let mut ids = HashSet::new();
    for definition in std::iter::once(root).chain(children) {
        if definition.tenant_id != root.tenant_id || definition.namespace != root.namespace {
            return Err(Error::Config(format!(
                "contract child '{}' must share the root tenant and namespace",
                definition.name
            )));
        }
        if !ids.insert(definition.id)
            || revisions
                .insert((definition.name.as_str(), definition.version), definition)
                .is_some()
        {
            return Err(Error::Config(format!(
                "duplicate contract definition identity for '{}' v{}",
                definition.name, definition.version
            )));
        }
    }

    let mut graph = Graph {
        revisions,
        path: HashSet::new(),
        heights: HashMap::new(),
        resolved: Vec::new(),
    };
    graph.visit(root, 0)?;
    // Only install definitions that can be reached from the root.
    graph.resolved.pop();
    Ok(graph.resolved)
}

struct Graph<'a> {
    revisions: HashMap<(&'a str, i32), &'a SequenceDefinition>,
    path: HashSet<SequenceId>,
    heights: HashMap<SequenceId, usize>,
    resolved: Vec<&'a SequenceDefinition>,
}

impl<'a> Graph<'a> {
    fn visit(&mut self, definition: &'a SequenceDefinition, depth: usize) -> Result<usize, Error> {
        // Keep sandbox validation within the scheduler's supported child depth.
        const MAX_DEPTH: usize = 16;
        let depth_error = || {
            Error::Config(format!(
                "contract sub_sequence depth exceeds {MAX_DEPTH} at '{}'",
                definition.name
            ))
        };
        if depth > MAX_DEPTH {
            return Err(depth_error());
        }
        if let Some(&height) = self.heights.get(&definition.id) {
            return if depth + height <= MAX_DEPTH {
                Ok(height)
            } else {
                Err(depth_error())
            };
        }
        if !self.path.insert(definition.id) {
            return Err(Error::Config(format!(
                "contract sub_sequence cycle at '{}' v{}",
                definition.name, definition.version
            )));
        }
        definition
            .validate()
            .map_err(|error| Error::InvalidSequence(error.to_string()))?;
        let mut height = 0;
        for child in definition_references(definition)? {
            let version = child.version.ok_or_else(|| {
                Error::Config(format!(
                    "contract sub_sequence '{}' in '{}' needs an explicit version",
                    child.name, definition.name
                ))
            })?;
            let target = self
                .revisions
                .get(&(child.name.as_str(), version))
                .ok_or_else(|| {
                    Error::Config(format!(
                        "missing contract sub_sequence '{}' v{version} referenced by '{}'",
                        child.name, definition.name
                    ))
                })?;
            height = height.max(1 + self.visit(target, depth + 1)?);
        }
        self.path.remove(&definition.id);
        self.heights.insert(definition.id, height);
        self.resolved.push(definition);
        Ok(height)
    }
}
