//! Conservative workflow optimization IR.
//!
//! The pass never rewrites the durable workflow definition. It compiles an
//! immutable sidecar plan whose source hash proves exact semantic identity,
//! while hoisting repeated constants and hot-path structural decisions.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use orch8_types::ids::BlockId;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::dataflow::{self, DataflowReport};

pub const OPTIMIZER_VERSION: &str = "orch8-optimizer-v1";
const MAX_OPTIMIZATION_NODES: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizedBlockKind {
    Step,
    Parallel,
    Race,
    Loop,
    ForEach,
    Router,
    TryCatch,
    SubSequence,
    AbSplit,
    CancellationScope,
    Saga,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardPlan {
    Always,
    Never,
    Dynamic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptimizationEdge {
    /// Stable semantic edge name such as `branch:0`, `route:1`, or `body`.
    pub role: String,
    pub child: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptimizedBlock {
    pub id: BlockId,
    pub kind: OptimizedBlockKind,
    pub handler: Option<String>,
    pub guard: GuardPlan,
    /// Indices into `OptimizationIr::constant_pool`.
    pub constants: Vec<usize>,
    pub edges: Vec<OptimizationEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationIr {
    pub optimizer_version: String,
    /// SHA-256 of the canonical, unmodified `SequenceDefinition`.
    pub source_sha256: String,
    pub nodes: Vec<OptimizedBlock>,
    /// Canonical constants shared by all nodes. Repeated handler parameters,
    /// schemas, and sub-sequence inputs occupy one allocation in the plan.
    pub constant_pool: Vec<Value>,
    pub roots: Vec<usize>,
    pub top_level_has_composite: bool,
    pub top_level_has_plugin_handler: bool,
    pub dataflow: DataflowReport,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OptimizationError {
    #[error("workflow is invalid: {0}")]
    InvalidWorkflow(String),
    #[error("workflow cannot be canonicalized: {0}")]
    Serialization(String),
    #[error("workflow exceeds the {MAX_OPTIMIZATION_NODES}-node optimization limit")]
    NodeLimit,
}

impl OptimizationIr {
    /// Verify that this plan was compiled from the exact supplied definition.
    /// This is an executable equivalence proof, not a best-effort comparison.
    pub fn verify_equivalent(
        &self,
        sequence: &SequenceDefinition,
    ) -> Result<(), OptimizationError> {
        let actual = source_hash(sequence)?;
        if actual != self.source_sha256 {
            return Err(OptimizationError::InvalidWorkflow(
                "optimized plan source hash does not match workflow definition".into(),
            ));
        }
        Ok(())
    }
}

/// Compile a semantics-preserving sidecar plan for a validated workflow.
pub fn optimize(sequence: &SequenceDefinition) -> Result<OptimizationIr, OptimizationError> {
    sequence
        .validate()
        .map_err(|error| OptimizationError::InvalidWorkflow(error.to_string()))?;
    let source_sha256 = source_hash(sequence)?;
    let mut compiler = Compiler::default();
    let roots = compiler.blocks(&sequence.blocks, "root")?;
    Ok(OptimizationIr {
        optimizer_version: OPTIMIZER_VERSION.into(),
        source_sha256,
        nodes: compiler.nodes,
        constant_pool: compiler.constant_pool,
        roots,
        top_level_has_composite: sequence
            .blocks
            .iter()
            .any(|block| !matches!(block, BlockDefinition::Step(_))),
        top_level_has_plugin_handler: sequence.blocks.iter().any(is_plugin_step),
        dataflow: dataflow::compile(sequence),
    })
}

fn source_hash(sequence: &SequenceDefinition) -> Result<String, OptimizationError> {
    let canonical = orch8_publisher::manifest::canonical_json(sequence)
        .map_err(|error| OptimizationError::Serialization(error.to_string()))?;
    let digest = Sha256::digest(canonical.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

fn is_plugin_step(block: &BlockDefinition) -> bool {
    let BlockDefinition::Step(step) = block else {
        return false;
    };
    crate::handlers::activepieces::is_ap_handler(&step.handler)
        || crate::handlers::grpc_plugin::is_grpc_handler(&step.handler)
        || crate::handlers::wasm_plugin::is_wasm_handler(&step.handler)
}

#[derive(Default)]
struct Compiler {
    nodes: Vec<OptimizedBlock>,
    constant_pool: Vec<Value>,
    constant_index: BTreeMap<String, usize>,
}

impl Compiler {
    fn blocks(
        &mut self,
        blocks: &[BlockDefinition],
        role_prefix: &str,
    ) -> Result<Vec<usize>, OptimizationError> {
        blocks
            .iter()
            .enumerate()
            .map(|(index, block)| self.block(block, &format!("{role_prefix}:{index}")))
            .collect()
    }

    fn block(&mut self, block: &BlockDefinition, _role: &str) -> Result<usize, OptimizationError> {
        if self.nodes.len() >= MAX_OPTIMIZATION_NODES {
            return Err(OptimizationError::NodeLimit);
        }
        let (id, kind, handler, guard, constants) = match block {
            BlockDefinition::Step(step) => {
                let mut constants = vec![self.intern(&step.params)?];
                if let Some(schema) = &step.output_schema {
                    constants.push(self.intern(schema)?);
                }
                (
                    step.id.clone(),
                    OptimizedBlockKind::Step,
                    Some(step.handler.clone()),
                    classify_guard(step.when.as_deref()),
                    constants,
                )
            }
            BlockDefinition::Parallel(block) => (
                block.id.clone(),
                OptimizedBlockKind::Parallel,
                None,
                GuardPlan::Always,
                Vec::new(),
            ),
            BlockDefinition::Race(block) => (
                block.id.clone(),
                OptimizedBlockKind::Race,
                None,
                GuardPlan::Always,
                Vec::new(),
            ),
            BlockDefinition::Loop(block) => (
                block.id.clone(),
                OptimizedBlockKind::Loop,
                None,
                classify_guard(Some(&block.condition)),
                Vec::new(),
            ),
            BlockDefinition::ForEach(block) => (
                block.id.clone(),
                OptimizedBlockKind::ForEach,
                None,
                GuardPlan::Dynamic,
                Vec::new(),
            ),
            BlockDefinition::Router(block) => (
                block.id.clone(),
                OptimizedBlockKind::Router,
                None,
                GuardPlan::Dynamic,
                Vec::new(),
            ),
            BlockDefinition::TryCatch(block) => (
                block.id.clone(),
                OptimizedBlockKind::TryCatch,
                None,
                GuardPlan::Always,
                Vec::new(),
            ),
            BlockDefinition::SubSequence(block) => (
                block.id.clone(),
                OptimizedBlockKind::SubSequence,
                None,
                GuardPlan::Always,
                vec![self.intern(&block.input)?],
            ),
            BlockDefinition::ABSplit(block) => (
                block.id.clone(),
                OptimizedBlockKind::AbSplit,
                None,
                GuardPlan::Dynamic,
                Vec::new(),
            ),
            BlockDefinition::CancellationScope(block) => (
                block.id.clone(),
                OptimizedBlockKind::CancellationScope,
                None,
                GuardPlan::Always,
                Vec::new(),
            ),
            BlockDefinition::Saga(block) => (
                block.id.clone(),
                OptimizedBlockKind::Saga,
                None,
                GuardPlan::Always,
                Vec::new(),
            ),
        };
        let index = self.nodes.len();
        self.nodes.push(OptimizedBlock {
            id,
            kind,
            handler,
            guard,
            constants,
            edges: Vec::new(),
        });

        let edges = self.child_edges(block)?;
        self.nodes[index].edges = edges;
        Ok(index)
    }

    fn child_edges(
        &mut self,
        block: &BlockDefinition,
    ) -> Result<Vec<OptimizationEdge>, OptimizationError> {
        let mut edges = Vec::new();
        match block {
            BlockDefinition::Step(_) | BlockDefinition::SubSequence(_) => {}
            BlockDefinition::Parallel(block) => {
                for (branch, blocks) in block.branches.iter().enumerate() {
                    self.extend_edges(&mut edges, blocks, &format!("branch:{branch}"))?;
                }
            }
            BlockDefinition::Race(block) => {
                for (branch, blocks) in block.branches.iter().enumerate() {
                    self.extend_edges(&mut edges, blocks, &format!("branch:{branch}"))?;
                }
            }
            BlockDefinition::Loop(block) => self.extend_edges(&mut edges, &block.body, "body")?,
            BlockDefinition::ForEach(block) => {
                self.extend_edges(&mut edges, &block.body, "body")?;
            }
            BlockDefinition::Router(block) => {
                for (route, route_def) in block.routes.iter().enumerate() {
                    self.extend_edges(&mut edges, &route_def.blocks, &format!("route:{route}"))?;
                }
                if let Some(default) = &block.default {
                    self.extend_edges(&mut edges, default, "default")?;
                }
            }
            BlockDefinition::TryCatch(block) => {
                self.extend_edges(&mut edges, &block.try_block, "try")?;
                self.extend_edges(&mut edges, &block.catch_block, "catch")?;
                if let Some(finally) = &block.finally_block {
                    self.extend_edges(&mut edges, finally, "finally")?;
                }
            }
            BlockDefinition::ABSplit(block) => {
                for (variant, variant_def) in block.variants.iter().enumerate() {
                    self.extend_edges(
                        &mut edges,
                        &variant_def.blocks,
                        &format!("variant:{variant}"),
                    )?;
                }
            }
            BlockDefinition::CancellationScope(block) => {
                self.extend_edges(&mut edges, &block.blocks, "scope")?;
            }
            BlockDefinition::Saga(block) => {
                for (step, saga_step) in block.steps.iter().enumerate() {
                    let action = self.block(&saga_step.action, &format!("action:{step}"))?;
                    edges.push(OptimizationEdge {
                        role: format!("action:{step}"),
                        child: action,
                    });
                    if let Some(compensation) = &saga_step.compensation {
                        let child = self.block(compensation, &format!("compensation:{step}"))?;
                        edges.push(OptimizationEdge {
                            role: format!("compensation:{step}"),
                            child,
                        });
                    }
                }
            }
        }
        Ok(edges)
    }

    fn extend_edges(
        &mut self,
        edges: &mut Vec<OptimizationEdge>,
        blocks: &[BlockDefinition],
        role: &str,
    ) -> Result<(), OptimizationError> {
        for (position, child) in blocks.iter().enumerate() {
            let child = self.block(child, role)?;
            edges.push(OptimizationEdge {
                role: format!("{role}:{position}"),
                child,
            });
        }
        Ok(())
    }

    fn intern(&mut self, value: &Value) -> Result<usize, OptimizationError> {
        let canonical = orch8_publisher::manifest::canonical_json(value)
            .map_err(|error| OptimizationError::Serialization(error.to_string()))?;
        if let Some(index) = self.constant_index.get(&canonical) {
            return Ok(*index);
        }
        let index = self.constant_pool.len();
        self.constant_pool.push(value.clone());
        self.constant_index.insert(canonical, index);
        Ok(index)
    }
}

fn classify_guard(guard: Option<&str>) -> GuardPlan {
    match guard.map(str::trim) {
        None | Some("true") => GuardPlan::Always,
        Some("false") => GuardPlan::Never,
        Some(_) => GuardPlan::Dynamic,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use orch8_types::ids::{Namespace, SequenceId, TenantId};
    use orch8_types::sequence::{SequenceStatus, StepDef};
    use serde_json::json;

    use super::*;

    fn step(id: &str, when: Option<&str>) -> BlockDefinition {
        BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new(id),
            handler: "noop".into(),
            params: json!({"shared": [1, 2, 3]}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
            output_schema: None,
            when: when.map(str::to_owned),
            compensation: None,
        }))
    }

    fn sequence() -> SequenceDefinition {
        SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("tenant"),
            namespace: Namespace::new("default"),
            name: "optimized".into(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::Production,
            blocks: vec![step("one", Some("true")), step("two", Some("false"))],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn pools_constants_and_folds_only_literal_guards() {
        let ir = optimize(&sequence()).unwrap();
        assert_eq!(ir.constant_pool.len(), 1);
        assert_eq!(ir.nodes[0].constants, ir.nodes[1].constants);
        assert_eq!(ir.nodes[0].guard, GuardPlan::Always);
        assert_eq!(ir.nodes[1].guard, GuardPlan::Never);
        assert!(!ir.top_level_has_composite);
    }

    #[test]
    fn exact_source_hash_is_an_equivalence_proof() {
        let sequence = sequence();
        let ir = optimize(&sequence).unwrap();
        ir.verify_equivalent(&sequence).unwrap();

        let mut changed = sequence;
        let BlockDefinition::Step(step) = &mut changed.blocks[0] else {
            unreachable!();
        };
        step.handler = "log".into();
        assert!(ir.verify_equivalent(&changed).is_err());
    }

    #[test]
    fn optimization_is_deterministic_and_does_not_mutate_source() {
        let sequence = sequence();
        let before = orch8_publisher::manifest::canonical_json(&sequence).unwrap();
        let first = optimize(&sequence).unwrap();
        let second = optimize(&sequence).unwrap();
        let after = orch8_publisher::manifest::canonical_json(&sequence).unwrap();
        assert_eq!(before, after);
        assert_eq!(first.source_sha256, second.source_sha256);
        assert_eq!(first.nodes, second.nodes);
        assert_eq!(first.constant_pool, second.constant_pool);
    }
}
