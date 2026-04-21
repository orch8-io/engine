//! Unit tests for the evaluator module.

use super::*;
use orch8_types::sequence::StepDef;

#[test]
fn find_block_in_flat_list() {
    let blocks = vec![BlockDefinition::Step(Box::new(StepDef {
        id: BlockId("step_1".into()),
        handler: "test".into(),
        params: serde_json::Value::Null,
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
    }))];

    let found = find_block(&blocks, &BlockId("step_1".into()));
    assert!(found.is_some());

    let not_found = find_block(&blocks, &BlockId("step_999".into()));
    assert!(not_found.is_none());
}

#[test]
fn find_block_nested_in_parallel() {
    let blocks = vec![BlockDefinition::Parallel(Box::new(
        orch8_types::sequence::ParallelDef {
            id: BlockId("par_1".into()),
            branches: vec![vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId("nested_step".into()),
                handler: "test".into(),
                params: serde_json::Value::Null,
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
            }))]],
        },
    ))];

    let found = find_block(&blocks, &BlockId("nested_step".into()));
    assert!(found.is_some());
}

#[test]
fn block_meta_returns_correct_types() {
    let step = BlockDefinition::Step(Box::new(StepDef {
        id: BlockId("s".into()),
        handler: "h".into(),
        params: serde_json::Value::Null,
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
    }));
    let (id, bt) = block_meta(&step);
    assert_eq!(id.0, "s");
    assert_eq!(bt, BlockType::Step);
}

fn mk_step(id: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId(id.into()),
        handler: "h".into(),
        params: serde_json::Value::Null,
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
    }))
}

fn mk_node(
    id: ExecutionNodeId,
    parent: Option<ExecutionNodeId>,
    block_id: &str,
    bt: BlockType,
    state: NodeState,
    branch_index: Option<i16>,
) -> ExecutionNode {
    ExecutionNode {
        id,
        instance_id: orch8_types::ids::InstanceId::new(),
        parent_id: parent,
        block_id: BlockId(block_id.into()),
        block_type: bt,
        branch_index,
        state,
        started_at: None,
        completed_at: None,
    }
}

#[test]
fn find_block_nested_in_loop_body() {
    use orch8_types::sequence::LoopDef;
    let loop_block = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("loop".into()),
        condition: "true".into(),
        body: vec![mk_step("inner")],
        max_iterations: 5,
    }));
    assert!(find_block(std::slice::from_ref(&loop_block), &BlockId("inner".into())).is_some());
    assert!(find_block(&[loop_block], &BlockId("loop".into())).is_some());
}

#[test]
fn find_block_nested_in_for_each_and_router() {
    use orch8_types::sequence::{ForEachDef, Route, RouterDef};
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId("fe".into()),
        collection: "xs".into(),
        item_var: "item".into(),
        body: vec![mk_step("fe-child")],
        max_iterations: 5,
    }));
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId("r".into()),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![mk_step("route-child")],
        }],
        default: Some(vec![mk_step("default-child")]),
    }));
    assert!(find_block(std::slice::from_ref(&fe), &BlockId("fe-child".into())).is_some());
    assert!(find_block(
        std::slice::from_ref(&router),
        &BlockId("route-child".into())
    )
    .is_some());
    assert!(find_block(&[router], &BlockId("default-child".into())).is_some());
}

#[test]
fn find_block_nested_in_try_catch_finally() {
    use orch8_types::sequence::TryCatchDef;
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![mk_step("t")],
        catch_block: vec![mk_step("c")],
        finally_block: Some(vec![mk_step("f")]),
    }));
    assert!(find_block(std::slice::from_ref(&tc), &BlockId("t".into())).is_some());
    assert!(find_block(std::slice::from_ref(&tc), &BlockId("c".into())).is_some());
    assert!(find_block(&[tc], &BlockId("f".into())).is_some());
}

#[test]
fn find_block_nested_in_ab_split_and_cancellation_scope() {
    use orch8_types::sequence::{ABSplitDef, ABVariant, CancellationScopeDef};
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId("ab".into()),
        variants: vec![ABVariant {
            name: "control".into(),
            weight: 50,
            blocks: vec![mk_step("ab-inner")],
        }],
    }));
    let cs = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId("cs".into()),
        blocks: vec![mk_step("cs-inner")],
    }));
    assert!(find_block(&[ab], &BlockId("ab-inner".into())).is_some());
    assert!(find_block(&[cs], &BlockId("cs-inner".into())).is_some());
}

#[test]
fn find_block_sub_sequence_only_finds_root_not_internals() {
    // SubSequence's child sequence lives in another definition; find_block
    // inside the *parent* definition must not descend into it.
    use orch8_types::sequence::SubSequenceDef;
    let sub = BlockDefinition::SubSequence(Box::new(SubSequenceDef {
        id: BlockId("sub".into()),
        sequence_name: "child".into(),
        version: None,
        input: serde_json::Value::Null,
    }));
    assert!(find_block(std::slice::from_ref(&sub), &BlockId("sub".into())).is_some());
    assert!(find_block(&[sub], &BlockId("child-step".into())).is_none());
}

#[test]
fn find_block_returns_none_for_unknown_id() {
    let blocks = vec![mk_step("a"), mk_step("b")];
    assert!(find_block(&blocks, &BlockId("not-there".into())).is_none());
}

#[test]
fn all_terminal_true_for_mixed_terminal_states() {
    let a = mk_node(
        ExecutionNodeId::new(),
        None,
        "a",
        BlockType::Step,
        NodeState::Completed,
        None,
    );
    let b = mk_node(
        ExecutionNodeId::new(),
        None,
        "b",
        BlockType::Step,
        NodeState::Failed,
        None,
    );
    let c = mk_node(
        ExecutionNodeId::new(),
        None,
        "c",
        BlockType::Step,
        NodeState::Skipped,
        None,
    );
    let d = mk_node(
        ExecutionNodeId::new(),
        None,
        "d",
        BlockType::Step,
        NodeState::Cancelled,
        None,
    );
    let refs = vec![&a, &b, &c, &d];
    assert!(all_terminal(&refs));
}

#[test]
fn all_terminal_false_when_any_running_or_waiting() {
    let a = mk_node(
        ExecutionNodeId::new(),
        None,
        "a",
        BlockType::Step,
        NodeState::Completed,
        None,
    );
    let b = mk_node(
        ExecutionNodeId::new(),
        None,
        "b",
        BlockType::Step,
        NodeState::Running,
        None,
    );
    assert!(!all_terminal(&[&a, &b]));
    let c = mk_node(
        ExecutionNodeId::new(),
        None,
        "c",
        BlockType::Step,
        NodeState::Waiting,
        None,
    );
    assert!(!all_terminal(&[&a, &c]));
    let d = mk_node(
        ExecutionNodeId::new(),
        None,
        "d",
        BlockType::Step,
        NodeState::Pending,
        None,
    );
    assert!(!all_terminal(&[&a, &d]));
}

#[test]
fn any_completed_and_all_completed_contract() {
    let a = mk_node(
        ExecutionNodeId::new(),
        None,
        "a",
        BlockType::Step,
        NodeState::Completed,
        None,
    );
    let b = mk_node(
        ExecutionNodeId::new(),
        None,
        "b",
        BlockType::Step,
        NodeState::Failed,
        None,
    );
    assert!(any_completed(&[&a, &b]));
    assert!(!all_completed(&[&a, &b]));
    assert!(all_completed(&[&a]));
    assert!(!any_completed(&[&b]));
    // empty slice — all_completed is vacuously true; any_completed is false.
    assert!(all_completed(&[]));
    assert!(!any_completed(&[]));
}

#[test]
fn any_failed_and_has_waiting_nodes() {
    let ok = mk_node(
        ExecutionNodeId::new(),
        None,
        "ok",
        BlockType::Step,
        NodeState::Completed,
        None,
    );
    let bad = mk_node(
        ExecutionNodeId::new(),
        None,
        "bad",
        BlockType::Step,
        NodeState::Failed,
        None,
    );
    let wait = mk_node(
        ExecutionNodeId::new(),
        None,
        "w",
        BlockType::Step,
        NodeState::Waiting,
        None,
    );
    assert!(any_failed(&[&ok, &bad]));
    assert!(!any_failed(&[&ok]));
    let tree = vec![ok.clone(), wait.clone()];
    assert!(has_waiting_nodes(&tree));
    assert!(!has_waiting_nodes(&[ok]));
}

#[test]
fn children_of_filters_by_parent_and_branch() {
    let parent_id = ExecutionNodeId::new();
    let c1 = mk_node(
        ExecutionNodeId::new(),
        Some(parent_id),
        "c1",
        BlockType::Step,
        NodeState::Pending,
        Some(0),
    );
    let c2 = mk_node(
        ExecutionNodeId::new(),
        Some(parent_id),
        "c2",
        BlockType::Step,
        NodeState::Pending,
        Some(1),
    );
    let unrelated = mk_node(
        ExecutionNodeId::new(),
        Some(ExecutionNodeId::new()),
        "u",
        BlockType::Step,
        NodeState::Pending,
        None,
    );
    let tree = vec![c1.clone(), c2.clone(), unrelated.clone()];
    assert_eq!(children_of(&tree, parent_id, None).len(), 2);
    assert_eq!(children_of(&tree, parent_id, Some(0)).len(), 1);
    assert_eq!(children_of(&tree, parent_id, Some(1)).len(), 1);
    assert_eq!(children_of(&tree, parent_id, Some(2)).len(), 0);
}

#[test]
fn children_of_returns_empty_for_unknown_parent() {
    let tree: Vec<ExecutionNode> = vec![];
    assert!(children_of(&tree, ExecutionNodeId::new(), None).is_empty());
}

#[test]
fn count_ancestors_measures_depth() {
    let root_id = ExecutionNodeId::new();
    let mid_id = ExecutionNodeId::new();
    let leaf_id = ExecutionNodeId::new();
    let root = mk_node(
        root_id,
        None,
        "r",
        BlockType::Parallel,
        NodeState::Running,
        None,
    );
    let mid = mk_node(
        mid_id,
        Some(root_id),
        "m",
        BlockType::Parallel,
        NodeState::Running,
        None,
    );
    let leaf = mk_node(
        leaf_id,
        Some(mid_id),
        "l",
        BlockType::Step,
        NodeState::Running,
        None,
    );
    let tree = vec![root, mid, leaf];
    let pm = build_parent_map(&tree);
    assert_eq!(count_ancestors(&pm, root_id), 0);
    assert_eq!(count_ancestors(&pm, mid_id), 1);
    assert_eq!(count_ancestors(&pm, leaf_id), 2);
    // Unknown id returns 0 (no parent found).
    assert_eq!(count_ancestors(&pm, ExecutionNodeId::new()), 0);
}

#[test]
fn block_meta_recognizes_each_variant() {
    use orch8_types::sequence::{
        ABSplitDef, ABVariant, CancellationScopeDef, ForEachDef, LoopDef, ParallelDef, RaceDef,
        RaceSemantics, RouterDef, SubSequenceDef, TryCatchDef,
    };
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("p".into()),
        branches: vec![],
    }));
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId("race".into()),
        branches: vec![],
        semantics: RaceSemantics::default(),
    }));
    let lp = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId("lp".into()),
        condition: "false".into(),
        body: vec![],
        max_iterations: 1,
    }));
    let fe = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId("fe".into()),
        collection: "x".into(),
        item_var: "i".into(),
        body: vec![],
        max_iterations: 1,
    }));
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId("r".into()),
        routes: vec![],
        default: None,
    }));
    let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId("tc".into()),
        try_block: vec![],
        catch_block: vec![],
        finally_block: None,
    }));
    let sub = BlockDefinition::SubSequence(Box::new(SubSequenceDef {
        id: BlockId("sub".into()),
        sequence_name: "s".into(),
        version: None,
        input: serde_json::Value::Null,
    }));
    let ab = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId("ab".into()),
        variants: vec![ABVariant {
            name: "a".into(),
            weight: 1,
            blocks: vec![],
        }],
    }));
    let cs = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId("cs".into()),
        blocks: vec![],
    }));
    assert_eq!(block_meta(&par).1, BlockType::Parallel);
    assert_eq!(block_meta(&race).1, BlockType::Race);
    assert_eq!(block_meta(&lp).1, BlockType::Loop);
    assert_eq!(block_meta(&fe).1, BlockType::ForEach);
    assert_eq!(block_meta(&router).1, BlockType::Router);
    assert_eq!(block_meta(&tc).1, BlockType::TryCatch);
    assert_eq!(block_meta(&sub).1, BlockType::SubSequence);
    assert_eq!(block_meta(&ab).1, BlockType::ABSplit);
    assert_eq!(block_meta(&cs).1, BlockType::CancellationScope);
}

#[test]
fn find_block_nested_in_race_branches() {
    use orch8_types::sequence::{RaceDef, RaceSemantics};
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId("race".into()),
        branches: vec![vec![mk_step("fast")], vec![mk_step("slow")]],
        semantics: RaceSemantics::default(),
    }));
    assert!(find_block(std::slice::from_ref(&race), &BlockId("fast".into())).is_some());
    assert!(find_block(&[race], &BlockId("slow".into())).is_some());
}

#[test]
fn find_running_step_prefers_step_over_composite() {
    use orch8_types::sequence::ParallelDef;
    let par_node_id = ExecutionNodeId::new();
    let step_node_id = ExecutionNodeId::new();
    let par = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId("p".into()),
        branches: vec![vec![mk_step("s")]],
    }));
    let blocks = vec![par];
    let tree = vec![
        mk_node(
            par_node_id,
            None,
            "p",
            BlockType::Parallel,
            NodeState::Running,
            None,
        ),
        mk_node(
            step_node_id,
            Some(par_node_id),
            "s",
            BlockType::Step,
            NodeState::Running,
            None,
        ),
    ];
    let handlers = crate::handlers::HandlerRegistry::new();
    let pm = build_parent_map(&tree);
    let block_map = flatten_blocks(&blocks);
    let (found_node, found_block) =
        find_running_step(&tree, &block_map, &handlers, &pm).expect("should find the running step");
    assert_eq!(found_node.block_id.0, "s");
    assert!(matches!(found_block, BlockDefinition::Step(_)));
}

// ------------------------------------------------------------------
// Async helper tests (EV1-EV10)
// ------------------------------------------------------------------

use chrono::Utc;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};

async fn seed_instance_ev(s: &SqliteStorage, id: InstanceId) {
    let now = Utc::now();
    let inst = TaskInstance {
        id,
        sequence_id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        state: InstanceState::Running,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context: orch8_types::context::ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    };
    s.create_instance(&inst).await.unwrap();
}

fn mk_exec_node(
    id: ExecutionNodeId,
    parent: Option<ExecutionNodeId>,
    bid: &str,
    bt: BlockType,
    state: NodeState,
    inst: InstanceId,
) -> ExecutionNode {
    ExecutionNode {
        id,
        instance_id: inst,
        parent_id: parent,
        block_id: BlockId(bid.into()),
        block_type: bt,
        branch_index: None,
        state,
        started_at: None,
        completed_at: None,
    }
}

// EV1: complete_node transitions Pending → Completed.
#[tokio::test]
async fn complete_node_marks_completed() {
    let s = SqliteStorage::in_memory().await.unwrap();
    let inst_id = InstanceId::new();
    seed_instance_ev(&s, inst_id).await;
    let n = mk_exec_node(
        ExecutionNodeId::new(),
        None,
        "n",
        BlockType::Step,
        NodeState::Pending,
        inst_id,
    );
    s.create_execution_nodes_batch(std::slice::from_ref(&n))
        .await
        .unwrap();
    complete_node(&s, n.id).await.unwrap();
    let tree = s.get_execution_tree(inst_id).await.unwrap();
    assert_eq!(tree[0].state, NodeState::Completed);
}

// EV2: fail_node transitions Running → Failed.
#[tokio::test]
async fn fail_node_marks_failed() {
    let s = SqliteStorage::in_memory().await.unwrap();
    let inst_id = InstanceId::new();
    seed_instance_ev(&s, inst_id).await;
    let n = mk_exec_node(
        ExecutionNodeId::new(),
        None,
        "n",
        BlockType::Step,
        NodeState::Running,
        inst_id,
    );
    s.create_execution_nodes_batch(std::slice::from_ref(&n))
        .await
        .unwrap();
    fail_node(&s, n.id).await.unwrap();
    let tree = s.get_execution_tree(inst_id).await.unwrap();
    assert_eq!(tree[0].state, NodeState::Failed);
}

// EV3: activate_pending_children flips Pending → Running, leaves others alone.
#[tokio::test]
async fn activate_pending_children_flips_only_pending() {
    let s = SqliteStorage::in_memory().await.unwrap();
    let inst_id = InstanceId::new();
    seed_instance_ev(&s, inst_id).await;
    let parent_id = ExecutionNodeId::new();
    let parent = mk_exec_node(
        parent_id,
        None,
        "p",
        BlockType::Parallel,
        NodeState::Running,
        inst_id,
    );
    let c_pending = mk_exec_node(
        ExecutionNodeId::new(),
        Some(parent_id),
        "cp",
        BlockType::Step,
        NodeState::Pending,
        inst_id,
    );
    let c_completed = mk_exec_node(
        ExecutionNodeId::new(),
        Some(parent_id),
        "cc",
        BlockType::Step,
        NodeState::Completed,
        inst_id,
    );
    let c_running = mk_exec_node(
        ExecutionNodeId::new(),
        Some(parent_id),
        "cr",
        BlockType::Step,
        NodeState::Running,
        inst_id,
    );
    s.create_execution_nodes_batch(&[
        parent,
        c_pending.clone(),
        c_completed.clone(),
        c_running.clone(),
    ])
    .await
    .unwrap();
    let tree = s.get_execution_tree(inst_id).await.unwrap();
    let children = children_of(&tree, parent_id, None);
    activate_pending_children(&s, &children).await.unwrap();
    let after = s.get_execution_tree(inst_id).await.unwrap();
    assert_eq!(
        after.iter().find(|n| n.id == c_pending.id).unwrap().state,
        NodeState::Running
    );
    assert_eq!(
        after.iter().find(|n| n.id == c_completed.id).unwrap().state,
        NodeState::Completed
    );
    assert_eq!(
        after.iter().find(|n| n.id == c_running.id).unwrap().state,
        NodeState::Running
    );
}

// EV4: activate_pending_children with empty slice is a no-op.
#[tokio::test]
async fn activate_pending_children_empty_is_noop() {
    let s = SqliteStorage::in_memory().await.unwrap();
    activate_pending_children(&s, &[]).await.unwrap();
}

// EV5: merged_blocks returns borrowed slice when no injection.
#[tokio::test]
async fn merged_blocks_no_injection_borrows() {
    use orch8_types::sequence::SequenceDefinition;
    let s = SqliteStorage::in_memory().await.unwrap();
    let inst_id = InstanceId::new();
    seed_instance_ev(&s, inst_id).await;
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "test".into(),
        version: 1,
        deprecated: false,
        blocks: vec![mk_step("a"), mk_step("b")],
        interceptors: None,
        created_at: Utc::now(),
    };
    let merged = merged_blocks(&s, inst_id, &seq).await.unwrap();
    assert!(matches!(merged, std::borrow::Cow::Borrowed(_)));
    assert_eq!(merged.len(), 2);
}

// EV6: merged_blocks appends injected blocks and returns Owned.
#[tokio::test]
async fn merged_blocks_with_injection_owns() {
    use orch8_types::sequence::SequenceDefinition;
    let s = SqliteStorage::in_memory().await.unwrap();
    let inst_id = InstanceId::new();
    seed_instance_ev(&s, inst_id).await;
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "test".into(),
        version: 1,
        deprecated: false,
        blocks: vec![mk_step("a")],
        interceptors: None,
        created_at: Utc::now(),
    };
    let injected = vec![mk_step("inj")];
    s.inject_blocks(inst_id, &serde_json::to_value(&injected).unwrap())
        .await
        .unwrap();
    let merged = merged_blocks(&s, inst_id, &seq).await.unwrap();
    assert!(matches!(merged, std::borrow::Cow::Owned(_)));
    assert_eq!(merged.len(), 2);
}

// EV7: merged_blocks with empty injected array borrows (no cloning).
#[tokio::test]
async fn merged_blocks_empty_injection_still_borrows() {
    use orch8_types::sequence::SequenceDefinition;
    let s = SqliteStorage::in_memory().await.unwrap();
    let inst_id = InstanceId::new();
    seed_instance_ev(&s, inst_id).await;
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "test".into(),
        version: 1,
        deprecated: false,
        blocks: vec![mk_step("a")],
        interceptors: None,
        created_at: Utc::now(),
    };
    s.inject_blocks(inst_id, &serde_json::json!([]))
        .await
        .unwrap();
    let merged = merged_blocks(&s, inst_id, &seq).await.unwrap();
    assert!(matches!(merged, std::borrow::Cow::Borrowed(_)));
    assert_eq!(merged.len(), 1);
}

// EV8: ensure_execution_tree is idempotent on subsequent calls.
#[tokio::test]
async fn ensure_execution_tree_idempotent() {
    let s = SqliteStorage::in_memory().await.unwrap();
    let inst_id = InstanceId::new();
    seed_instance_ev(&s, inst_id).await;
    let inst = s.get_instance(inst_id).await.unwrap().unwrap();
    let blocks = vec![mk_step("a"), mk_step("b")];
    let first = ensure_execution_tree(&s, &inst, &blocks).await.unwrap();
    let second = ensure_execution_tree(&s, &inst, &blocks).await.unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);
    // Node IDs must be preserved across calls.
    let first_ids: std::collections::HashSet<_> = first.iter().map(|n| n.id).collect();
    let second_ids: std::collections::HashSet<_> = second.iter().map(|n| n.id).collect();
    assert_eq!(first_ids, second_ids);
}

// EV9: ensure_execution_tree adds nodes for newly-injected blocks.
#[tokio::test]
async fn ensure_execution_tree_adds_injected_nodes() {
    let s = SqliteStorage::in_memory().await.unwrap();
    let inst_id = InstanceId::new();
    seed_instance_ev(&s, inst_id).await;
    let inst = s.get_instance(inst_id).await.unwrap().unwrap();
    ensure_execution_tree(&s, &inst, &[mk_step("a")])
        .await
        .unwrap();
    // Now pretend an extra block was injected; pass merged slice.
    let after = ensure_execution_tree(&s, &inst, &[mk_step("a"), mk_step("new")])
        .await
        .unwrap();
    assert_eq!(after.len(), 2);
    let block_ids: Vec<&str> = after.iter().map(|n| n.block_id.0.as_str()).collect();
    assert!(block_ids.contains(&"a"));
    assert!(block_ids.contains(&"new"));
}

// EV10: is_inside_decided_race returns false when no sibling completed.
#[test]
fn is_inside_decided_race_no_winner() {
    use orch8_types::sequence::{RaceDef, RaceSemantics};
    let race_id = ExecutionNodeId::new();
    let br0_id = ExecutionNodeId::new();
    let br1_id = ExecutionNodeId::new();
    let leaf_id = ExecutionNodeId::new();
    let race_block = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId("race".into()),
        branches: vec![vec![mk_step("b0")], vec![mk_step("b1")]],
        semantics: RaceSemantics::default(),
    }));
    let blocks = vec![race_block];
    let tree = vec![
        mk_node(
            race_id,
            None,
            "race",
            BlockType::Race,
            NodeState::Running,
            None,
        ),
        mk_node(
            br0_id,
            Some(race_id),
            "b0",
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
        mk_node(
            br1_id,
            Some(race_id),
            "b1",
            BlockType::Step,
            NodeState::Running,
            Some(1),
        ),
        mk_node(
            leaf_id,
            Some(br0_id),
            "leaf",
            BlockType::Step,
            NodeState::Running,
            None,
        ),
    ];
    let leaf = tree.iter().find(|n| n.id == leaf_id).unwrap();
    let pm = build_parent_map(&tree);
    let node_map: std::collections::HashMap<_, _> = tree.iter().map(|n| (n.id, n)).collect();
    let block_map = flatten_blocks(&blocks);
    assert!(!is_inside_decided_race(
        &tree, &block_map, leaf, &pm, &node_map
    ));
}

// EV11: is_inside_decided_race returns true when sibling branch completed.
#[test]
fn is_inside_decided_race_with_winner() {
    use orch8_types::sequence::{RaceDef, RaceSemantics};
    let race_id = ExecutionNodeId::new();
    let br0_id = ExecutionNodeId::new();
    let br1_id = ExecutionNodeId::new();
    let race_block = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId("race".into()),
        branches: vec![vec![mk_step("b0")], vec![mk_step("b1")]],
        semantics: RaceSemantics::default(),
    }));
    let blocks = vec![race_block];
    let tree = vec![
        mk_node(
            race_id,
            None,
            "race",
            BlockType::Race,
            NodeState::Running,
            None,
        ),
        mk_node(
            br0_id,
            Some(race_id),
            "b0",
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
        // Sibling branch completed.
        mk_node(
            br1_id,
            Some(race_id),
            "b1",
            BlockType::Step,
            NodeState::Completed,
            Some(1),
        ),
    ];
    let loser = tree.iter().find(|n| n.id == br0_id).unwrap();
    let pm = build_parent_map(&tree);
    let node_map: std::collections::HashMap<_, _> = tree.iter().map(|n| (n.id, n)).collect();
    let block_map = flatten_blocks(&blocks);
    assert!(is_inside_decided_race(
        &tree, &block_map, loser, &pm, &node_map
    ));
}
