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
    /// Stable semantic edge name: the container role plus the child position,
    /// such as `branch:0:1` (branch 0, second child), `route:1:0`, `body:0`,
    /// or `action:0` / `compensation:0` for saga steps.
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
    crate::handlers::PluginKind::detect(&step.handler).is_some()
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
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{Namespace, SequenceId, TenantId};
    use orch8_types::sequence::{
        ABSplitDef, ABVariant, CancellationScopeDef, ForEachDef, LoopDef, ParallelDef, RaceDef,
        RaceSemantics, Route, RouterDef, SagaDef, SagaStep, SequenceStatus, StepDef,
        SubSequenceDef, TryCatchDef,
    };
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
            rate_budget: None,
            placement: None,
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
            embed: None,
            sub_tenant: None,
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
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
            placement: None,
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

    fn sequence_with(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
        SequenceDefinition {
            blocks,
            ..sequence()
        }
    }

    fn plain(id: &str) -> BlockDefinition {
        let mut block = step(id, None);
        let BlockDefinition::Step(def) = &mut block else {
            unreachable!();
        };
        def.params = json!({});
        block
    }

    fn with_step(mut block: BlockDefinition, edit: impl FnOnce(&mut StepDef)) -> BlockDefinition {
        let BlockDefinition::Step(def) = &mut block else {
            unreachable!();
        };
        edit(def);
        block
    }

    fn loop_block(condition: &str) -> BlockDefinition {
        BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("l"),
            condition: condition.into(),
            body: vec![plain("b1"), plain("b2")],
            max_iterations: 3,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        }))
    }

    /// Edges as `(role, child block id)` pairs.
    fn edges_of(ir: &OptimizationIr, node: usize) -> Vec<(String, String)> {
        ir.nodes[node]
            .edges
            .iter()
            .map(|edge| {
                (
                    edge.role.clone(),
                    ir.nodes[edge.child].id.as_str().to_owned(),
                )
            })
            .collect()
    }

    #[test]
    fn guard_folding_agrees_with_the_runtime_when_evaluator() {
        // Folding is only safe if every guard the optimizer folds evaluates
        // to the same constant at runtime, whatever the instance state is.
        let contexts = [
            ExecutionContext::default(),
            ExecutionContext {
                data: json!({"ready": true, "value": 1, "true": false, "false": true}),
                ..ExecutionContext::default()
            },
        ];
        let outputs = [json!({}), json!({"check": {"ok": false}})];
        let cases = [
            ("true", GuardPlan::Always),
            (" true", GuardPlan::Always),
            ("\ttrue\n", GuardPlan::Always),
            ("false", GuardPlan::Never),
            (" false ", GuardPlan::Never),
            ("\nfalse\t", GuardPlan::Never),
            ("", GuardPlan::Dynamic),
            ("   ", GuardPlan::Dynamic),
            ("TRUE", GuardPlan::Dynamic),
            ("1", GuardPlan::Dynamic),
            ("\"true\"", GuardPlan::Dynamic),
            ("true # comment", GuardPlan::Dynamic),
            ("true_value", GuardPlan::Dynamic),
            ("(true)", GuardPlan::Dynamic),
            ("{{ data.ready }}", GuardPlan::Dynamic),
            ("data.value == 1", GuardPlan::Dynamic),
        ];
        assert_eq!(classify_guard(None), GuardPlan::Always, "absent guard");
        for (guard, expected) in cases {
            assert_eq!(classify_guard(Some(guard)), expected, "guard {guard:?}");
            let folded = match expected {
                GuardPlan::Always => true,
                GuardPlan::Never => false,
                GuardPlan::Dynamic => continue,
            };
            for context in &contexts {
                for output in &outputs {
                    assert_eq!(
                        crate::expression::evaluate_condition_strict(guard, context, output),
                        Ok(folded),
                        "guard {guard:?} folded to {expected:?} but runtime disagrees"
                    );
                    assert_eq!(
                        crate::expression::evaluate_condition(guard, context, output),
                        folded,
                        "loop condition {guard:?} folded to {expected:?} but runtime disagrees"
                    );
                }
            }
        }
    }

    #[test]
    fn step_and_loop_guards_are_folded_and_other_composites_stay_dynamic() {
        let cases = [
            (loop_block("true"), GuardPlan::Always),
            (loop_block("false"), GuardPlan::Never),
            (loop_block("data.keep_going"), GuardPlan::Dynamic),
            (
                BlockDefinition::ForEach(Box::new(ForEachDef {
                    id: BlockId::new("f"),
                    collection: "items".into(),
                    item_var: "item".into(),
                    body: vec![plain("a")],
                    max_iterations: 3,
                    retain_iterations: None,
                })),
                GuardPlan::Dynamic,
            ),
            (
                BlockDefinition::Router(Box::new(RouterDef {
                    id: BlockId::new("r"),
                    routes: vec![Route {
                        condition: "true".into(),
                        blocks: vec![plain("a")],
                    }],
                    default: None,
                })),
                GuardPlan::Dynamic,
            ),
        ];
        for (block, expected) in cases {
            let ir = optimize(&sequence_with(vec![block])).unwrap();
            assert_eq!(ir.nodes[0].guard, expected, "{:?}", ir.nodes[0].kind);
        }
    }

    #[test]
    fn source_hash_ignores_json_key_order_but_tracks_name_params_and_guard() {
        let base = sequence_with(vec![with_step(plain("s"), |def| {
            def.params = serde_json::from_str(r#"{"a": 1, "b": {"x": 1, "y": 2}}"#).unwrap();
            def.when = Some("data.ready".into());
        })]);
        let base_hash = source_hash(&base).unwrap();
        let rehash = |edit: &dyn Fn(&mut StepDef)| {
            let mut changed = base.clone();
            let BlockDefinition::Step(def) = &mut changed.blocks[0] else {
                unreachable!();
            };
            edit(def);
            source_hash(&changed).unwrap()
        };

        let reordered = rehash(&|def| {
            def.params = serde_json::from_str(r#"{"b": {"y": 2, "x": 1}, "a": 1}"#).unwrap();
        });
        let renamed = {
            let mut changed = base.clone();
            changed.name = "renamed".into();
            source_hash(&changed).unwrap()
        };
        let new_param = rehash(&|def| def.params["a"] = json!(2));
        let new_nested_param = rehash(&|def| def.params["b"]["y"] = json!(3));
        let new_guard = rehash(&|def| def.when = Some("data.other".into()));
        let no_guard = rehash(&|def| def.when = None);

        assert_eq!(reordered, base_hash, "key order is not semantic");
        for (case, hash) in [
            ("name", renamed),
            ("param", new_param),
            ("nested param", new_nested_param),
            ("guard", new_guard),
            ("removed guard", no_guard),
        ] {
            assert_ne!(hash, base_hash, "{case} change must change the identity");
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)] // one table row per block kind
    fn child_edges_point_at_each_child_with_semantic_roles() {
        let pair = |role: &str, child: &str| (role.to_owned(), child.to_owned());
        let cases = vec![
            (
                BlockDefinition::Parallel(Box::new(ParallelDef {
                    id: BlockId::new("p"),
                    branches: vec![vec![plain("a"), plain("a2")], vec![plain("b")]],
                })),
                OptimizedBlockKind::Parallel,
                vec![
                    pair("branch:0:0", "a"),
                    pair("branch:0:1", "a2"),
                    pair("branch:1:0", "b"),
                ],
            ),
            (
                BlockDefinition::Race(Box::new(RaceDef {
                    id: BlockId::new("r"),
                    branches: vec![vec![plain("a")], vec![plain("b")]],
                    semantics: RaceSemantics::default(),
                })),
                OptimizedBlockKind::Race,
                vec![pair("branch:0:0", "a"), pair("branch:1:0", "b")],
            ),
            (
                loop_block("true"),
                OptimizedBlockKind::Loop,
                vec![pair("body:0", "b1"), pair("body:1", "b2")],
            ),
            (
                BlockDefinition::ForEach(Box::new(ForEachDef {
                    id: BlockId::new("f"),
                    collection: "items".into(),
                    item_var: "item".into(),
                    body: vec![plain("a")],
                    max_iterations: 3,
                    retain_iterations: None,
                })),
                OptimizedBlockKind::ForEach,
                vec![pair("body:0", "a")],
            ),
            (
                BlockDefinition::Router(Box::new(RouterDef {
                    id: BlockId::new("r"),
                    routes: vec![
                        Route {
                            condition: "a".into(),
                            blocks: vec![plain("ra")],
                        },
                        Route {
                            condition: "b".into(),
                            blocks: vec![plain("rb")],
                        },
                    ],
                    default: Some(vec![plain("rd")]),
                })),
                OptimizedBlockKind::Router,
                vec![
                    pair("route:0:0", "ra"),
                    pair("route:1:0", "rb"),
                    pair("default:0", "rd"),
                ],
            ),
            (
                BlockDefinition::TryCatch(Box::new(TryCatchDef {
                    id: BlockId::new("t"),
                    try_block: vec![plain("t1")],
                    catch_block: vec![plain("c1")],
                    finally_block: Some(vec![plain("f1")]),
                })),
                OptimizedBlockKind::TryCatch,
                vec![
                    pair("try:0", "t1"),
                    pair("catch:0", "c1"),
                    pair("finally:0", "f1"),
                ],
            ),
            (
                BlockDefinition::ABSplit(Box::new(ABSplitDef {
                    id: BlockId::new("ab"),
                    variants: vec![
                        ABVariant {
                            name: "control".into(),
                            weight: 50,
                            blocks: vec![plain("va")],
                        },
                        ABVariant {
                            name: "treatment".into(),
                            weight: 50,
                            blocks: vec![plain("vb")],
                        },
                    ],
                })),
                OptimizedBlockKind::AbSplit,
                vec![pair("variant:0:0", "va"), pair("variant:1:0", "vb")],
            ),
            (
                BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
                    id: BlockId::new("cs"),
                    blocks: vec![plain("s1")],
                })),
                OptimizedBlockKind::CancellationScope,
                vec![pair("scope:0", "s1")],
            ),
            (
                BlockDefinition::Saga(Box::new(SagaDef {
                    id: BlockId::new("sg"),
                    steps: vec![
                        SagaStep {
                            id: BlockId::new("sg1"),
                            action: Box::new(plain("a1")),
                            compensation: Some(Box::new(plain("c1"))),
                        },
                        SagaStep {
                            id: BlockId::new("sg2"),
                            action: Box::new(plain("a2")),
                            compensation: None,
                        },
                    ],
                })),
                OptimizedBlockKind::Saga,
                vec![
                    pair("action:0", "a1"),
                    pair("compensation:0", "c1"),
                    pair("action:1", "a2"),
                ],
            ),
            (
                BlockDefinition::SubSequence(Box::new(SubSequenceDef {
                    id: BlockId::new("sub"),
                    sequence_name: "child".into(),
                    version: None,
                    input: json!({}),
                })),
                OptimizedBlockKind::SubSequence,
                vec![],
            ),
            (plain("s"), OptimizedBlockKind::Step, vec![]),
        ];
        for (block, kind, expected_edges) in cases {
            let ir = optimize(&sequence_with(vec![block])).unwrap();
            assert_eq!(ir.roots, vec![0], "{kind:?}");
            assert_eq!(ir.nodes[0].kind, kind);
            assert_eq!(edges_of(&ir, 0), expected_edges, "{kind:?}");
            assert_eq!(ir.nodes.len(), expected_edges.len() + 1, "{kind:?}");
        }
    }

    #[test]
    fn compiler_rejects_the_node_past_the_optimization_limit() {
        let at_limit: Vec<_> = (0..MAX_OPTIMIZATION_NODES)
            .map(|index| plain(&format!("s{index}")))
            .collect();
        let mut over_limit = at_limit.clone();
        over_limit.push(plain("one-too-many"));

        let accepted = Compiler::default().blocks(&at_limit, "root");
        let rejected = Compiler::default().blocks(&over_limit, "root");

        assert_eq!(
            accepted.map(|roots| roots.len()),
            Ok(MAX_OPTIMIZATION_NODES)
        );
        assert_eq!(rejected, Err(OptimizationError::NodeLimit));
    }

    #[test]
    fn optimize_rejects_oversized_workflows_at_validation_before_compiling() {
        // Sequence validation caps a workflow well below the optimizer's node
        // limit, so through the public entry point the limit is defence in depth.
        let blocks = (0..=MAX_OPTIMIZATION_NODES)
            .map(|index| plain(&format!("s{index}")))
            .collect();

        let error = optimize(&sequence_with(blocks)).unwrap_err();

        assert!(
            matches!(&error, OptimizationError::InvalidWorkflow(message) if message.contains("blocks")),
            "{error:?}"
        );
    }

    #[test]
    fn output_schema_is_interned_after_the_step_params() {
        let ir = optimize(&sequence_with(vec![with_step(plain("s"), |def| {
            def.output_schema = Some(json!({"type": "object"}));
        })]))
        .unwrap();

        assert_eq!(ir.nodes[0].constants.len(), 2);
        assert_eq!(ir.constant_pool[ir.nodes[0].constants[0]], json!({}));
        assert_eq!(
            ir.constant_pool[ir.nodes[0].constants[1]],
            json!({"type": "object"})
        );
    }

    #[test]
    fn distinct_constants_occupy_distinct_pool_entries() {
        let sub = |id: &str, input: Value| {
            BlockDefinition::SubSequence(Box::new(SubSequenceDef {
                id: BlockId::new(id),
                sequence_name: "child".into(),
                version: None,
                input,
            }))
        };
        let ir = optimize(&sequence_with(vec![
            sub("s1", json!({"x": 1})),
            sub("s2", json!({"x": 1})),
            sub("s3", json!({"x": 2})),
            with_step(plain("st"), |def| def.params = json!({"x": 2})),
        ]))
        .unwrap();

        assert_eq!(ir.constant_pool.len(), 2);
        assert_eq!(ir.nodes[0].constants, ir.nodes[1].constants);
        assert_ne!(ir.nodes[0].constants, ir.nodes[2].constants);
        assert_eq!(
            ir.nodes[2].constants, ir.nodes[3].constants,
            "steps and sub-sequences share the pool"
        );
    }

    #[test]
    fn roots_follow_top_level_order() {
        let ir = optimize(&sequence_with(vec![plain("a"), plain("b"), plain("c")])).unwrap();

        let root_ids: Vec<_> = ir.roots.iter().map(|&i| ir.nodes[i].id.as_str()).collect();
        assert_eq!(root_ids, ["a", "b", "c"]);
    }

    #[test]
    fn top_level_composite_flag_is_set_only_by_non_step_blocks() {
        let flat = optimize(&sequence_with(vec![plain("a"), plain("b")])).unwrap();
        let composite = optimize(&sequence_with(vec![
            plain("a"),
            BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
                id: BlockId::new("cs"),
                blocks: vec![plain("b")],
            })),
        ]))
        .unwrap();

        assert!(!flat.top_level_has_composite);
        assert!(composite.top_level_has_composite);
    }

    #[test]
    fn plugin_handler_flag_detects_each_plugin_scheme_only() {
        let cases = [
            ("ap://gmail.send", true),
            ("grpc://plugin", true),
            ("grpcs://plugin", true),
            ("wasm://plugin", true),
            ("noop", false),
        ];
        for (handler, expected) in cases {
            let ir = optimize(&sequence_with(vec![with_step(plain("s"), |def| {
                def.handler = handler.into();
            })]))
            .unwrap();
            assert_eq!(ir.top_level_has_plugin_handler, expected, "{handler}");
        }
    }

    #[test]
    fn verify_equivalent_rejects_a_renamed_workflow_as_invalid() {
        let seq = sequence_with(vec![plain("a")]);
        let ir = optimize(&seq).unwrap();
        let mut changed = seq.clone();
        changed.name = "renamed".into();

        let error = ir.verify_equivalent(&changed).unwrap_err();

        assert_eq!(
            error,
            OptimizationError::InvalidWorkflow(
                "optimized plan source hash does not match workflow definition".into()
            )
        );
    }

    #[test]
    fn empty_workflow_is_rejected_as_invalid() {
        let error = optimize(&sequence_with(Vec::new())).unwrap_err();

        assert!(
            matches!(error, OptimizationError::InvalidWorkflow(_)),
            "{error:?}"
        );
    }
}
