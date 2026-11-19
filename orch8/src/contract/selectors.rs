use std::collections::HashMap;

use orch8_engine::evaluator::find_block;
use orch8_types::contract::{AssertionSubject, ContractCase, ContractDefinition, MockPolicy};
use orch8_types::ids::{BlockId, SequenceId};
use orch8_types::sequence::SequenceDefinition;

use crate::error::Error;

pub(super) struct CaseSelectors {
    pub(super) by_handler: HashMap<String, MockPolicy>,
    pub(super) by_block: HashMap<(SequenceId, String), MockPolicy>,
    pub(super) assertion_definitions: Vec<SequenceId>,
}

impl CaseSelectors {
    pub(super) fn resolve(
        root: &SequenceDefinition,
        children: &[&SequenceDefinition],
        case: &ContractCase,
    ) -> Result<Self, Error> {
        let invalid = |reason: String| {
            Error::Config(format!("invalid contract case '{}': {reason}", case.name))
        };
        let select = |block: &str, scope: Option<&ContractDefinition>| {
            resolve_block(root, children, block, scope).map_err(invalid)
        };
        let mut by_handler = HashMap::new();
        let mut by_block = HashMap::new();
        for mock in &case.mocks {
            if let Some(handler) = &mock.handler
                && by_handler
                    .insert(handler.clone(), mock.policy.clone())
                    .is_some()
            {
                return Err(invalid(format!("duplicate mock for handler '{handler}'")));
            }
            if let Some(block) = &mock.block {
                let sequence_id = select(block, mock.definition.as_ref())?.unwrap_or(root.id);
                if by_block
                    .insert((sequence_id, block.clone()), mock.policy.clone())
                    .is_some()
                {
                    return Err(invalid(format!("duplicate mock for block '{block}'")));
                }
            }
        }
        let assertion_definitions = case
            .expect
            .assertions
            .iter()
            .map(|assertion| match &assertion.subject {
                AssertionSubject::Output {
                    block, definition, ..
                } => {
                    let selected = select(block, definition.as_ref())?;
                    // Bare output assertions historically read the root. To
                    // assert a child output explicitly, name its definition.
                    Ok(if definition.is_some() {
                        selected.unwrap_or(root.id)
                    } else {
                        root.id
                    })
                }
                AssertionSubject::Context { .. } => Ok(root.id),
            })
            .collect::<Result<_, Error>>()?;
        Ok(Self {
            by_handler,
            by_block,
            assertion_definitions,
        })
    }
}

fn resolve_block(
    root: &SequenceDefinition,
    children: &[&SequenceDefinition],
    block: &str,
    scope: Option<&ContractDefinition>,
) -> Result<Option<SequenceId>, String> {
    let id = BlockId::new(block);
    let definitions = std::iter::once(root).chain(children.iter().copied());
    let has_block = |definition: &SequenceDefinition| {
        std::iter::once(definition.blocks.as_slice())
            .chain(definition.on_failure.as_deref())
            .chain(definition.on_cancel.as_deref())
            .any(|blocks| find_block(blocks, &id).is_some())
    };
    if let Some(scope) = scope {
        let definition = definitions
            .filter(|definition| {
                definition.name == scope.name && definition.version == scope.version
            })
            .find(|definition| has_block(definition))
            .ok_or_else(|| {
                format!(
                    "block '{block}' does not exist in reachable definition '{}' v{}",
                    scope.name, scope.version
                )
            })?;
        return Ok(Some(definition.id));
    }
    let mut matches = definitions.filter(|definition| has_block(definition));
    let first = matches.next();
    if let Some(second) = matches.next()
        && let Some(first) = first
    {
        return Err(format!(
            "block '{block}' is ambiguous in '{}' v{} and '{}' v{}; specify its definition",
            first.name, first.version, second.name, second.version
        ));
    }
    Ok(first.map(|definition| definition.id))
}
